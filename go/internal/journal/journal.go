// Package journal contains the local, metadata-only SQLite custody journal.
//
// This package deliberately has no knowledge of app-server or CLI types.  It
// records identities, routes, digests and evidence transitions, never message
// or reply bodies.  Callers must close every transaction before making an RPC.
package journal

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const (
	defaultBusyTimeout = 2500 * time.Millisecond
	leaseDuration      = 30 * time.Second

	// Evidence states are persisted strings and are intentionally stable.
	StateNotSent             EvidenceState = "not_sent"
	StatePrepared            EvidenceState = "prepared"
	StateDispatchStarted     EvidenceState = "dispatch_started"
	StateReplyClaimed        EvidenceState = "reply_dispatch_claimed"
	StateReplyOutcomeUnknown EvidenceState = "reply_outcome_unknown"
	StateRejected            EvidenceState = "rejected"
	StateAccepted            EvidenceState = "accepted"
	StateOutcomeUnknown      EvidenceState = "outcome_unknown"
	StateReplyAccepted       EvidenceState = "reply_accepted"
	StateReplyObserved       EvidenceState = "reply_observed"
	StateManuallyResolved    EvidenceState = "manually_resolved"
)

type EvidenceState string

var (
	ErrNotFound          = errors.New("journal: record not found")
	ErrIdentityConflict  = errors.New("journal: message identity conflict")
	ErrClaimNotOwned     = errors.New("journal: claim is not owned by token")
	ErrClaimExpired      = errors.New("journal: claim expired")
	ErrAlreadyWon        = errors.New("journal: a different reply already won")
	ErrInvalidTransition = errors.New("journal: invalid evidence transition")
	ErrCorrupt           = errors.New("journal: corrupt or unreadable database")
)

// Options controls state location and custody timing. A zero StateDir uses
// $MEKTUP_STATE_DIR when present and otherwise ~/.local/share/mektup.
type Options struct {
	StateDir      string
	BusyTimeout   time.Duration
	LeaseDuration time.Duration
	Now           func() time.Time
}

type Journal struct {
	db            *sql.DB
	stateDir      string
	storeID       string
	busyTimeout   time.Duration
	leaseDuration time.Duration
	now           func() time.Time
	mu            sync.RWMutex
}

func Open(ctx context.Context, opts Options) (*Journal, error) {
	dir, err := statePath(opts.StateDir)
	if err != nil {
		return nil, err
	}
	if err := secureDir(dir); err != nil {
		return nil, err
	}
	timeout := opts.BusyTimeout
	if timeout <= 0 {
		timeout = defaultBusyTimeout
	}
	lease := opts.LeaseDuration
	if lease <= 0 {
		lease = leaseDuration
	}
	dbPath := filepath.Join(dir, "journal.sqlite3")
	// URI pragmas apply to every connection in database/sql's pool. WAL is
	// required for concurrent swarm processes; FK and busy handling are not
	// optional safety settings.
	dsn := "file:" + dbPath + "?_pragma=busy_timeout(" + fmt.Sprint(timeout.Milliseconds()) + ")&_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(8)
	j := &Journal{db: db, stateDir: dir, busyTimeout: timeout, leaseDuration: lease, now: opts.Now}
	if j.now == nil {
		j.now = time.Now
	}
	if err := j.init(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return j, nil
}

func statePath(requested string) (string, error) {
	if requested == "" {
		requested = os.Getenv("MEKTUP_STATE_DIR")
	}
	if requested == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		requested = filepath.Join(home, ".local", "share", "mektup")
	}
	requested, err := filepath.Abs(requested)
	if err != nil {
		return "", err
	}
	return requested, nil
}

func secureDir(dir string) error {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return err
	}
	return nil
}

func (j *Journal) init(ctx context.Context) error {
	if err := j.db.PingContext(ctx); err != nil {
		return fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	// Set pragmas explicitly as well as in the URI. This gives deterministic
	// behavior for drivers that ignore URI pragma parameters.
	for _, pragma := range []string{
		"PRAGMA foreign_keys=ON", fmt.Sprintf("PRAGMA busy_timeout=%d", j.busyTimeout.Milliseconds()), "PRAGMA journal_mode=WAL", "PRAGMA synchronous=FULL",
	} {
		if _, err := j.db.ExecContext(ctx, pragma); err != nil {
			return fmt.Errorf("journal pragma: %w", err)
		}
	}
	tx, err := j.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("journal migration: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()
	var version int
	if err = tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	if version > 1 {
		return fmt.Errorf("journal: unsupported schema version %d", version)
	}
	if version == 0 {
		if _, err = tx.ExecContext(ctx, schemaV1); err != nil {
			return fmt.Errorf("journal migration: %w", err)
		}
		if _, err = tx.ExecContext(ctx, "PRAGMA user_version=1"); err != nil {
			return fmt.Errorf("journal migration: %w", err)
		}
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("journal migration commit: %w", err)
	}
	if err = j.loadOrCreateStoreID(ctx); err != nil {
		return err
	}
	if err := j.secureFiles(); err != nil {
		return err
	}
	return nil
}

func (j *Journal) secureFiles() error {
	for _, suffix := range []string{"", "-wal", "-shm"} {
		p := filepath.Join(j.stateDir, "journal.sqlite3"+suffix)
		if _, err := os.Stat(p); err == nil {
			if err := os.Chmod(p, 0600); err != nil {
				return err
			}
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

const schemaV1 = `
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS operations (
 operation_id TEXT PRIMARY KEY, message_id TEXT NOT NULL UNIQUE,
 source_route TEXT NOT NULL, target_route TEXT NOT NULL, semantics TEXT NOT NULL,
 digest TEXT NOT NULL, body_size INTEGER NOT NULL CHECK(body_size >= 0),
 state TEXT NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
 dispatch_started_at INTEGER, terminal_at INTEGER, error_code TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS attempts (
 operation_id TEXT PRIMARY KEY REFERENCES operations(operation_id) ON DELETE CASCADE,
 state TEXT NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS reply_claims (
 reply_id TEXT PRIMARY KEY, original_id TEXT NOT NULL,
 digest TEXT NOT NULL, body_size INTEGER NOT NULL CHECK(body_size >= 0),
 status TEXT NOT NULL CHECK(status IN ('success','error')),
 reply_route TEXT NOT NULL, custody_route TEXT NOT NULL,
 owner TEXT NOT NULL, token TEXT NOT NULL, lease_until INTEGER NOT NULL,
 state TEXT NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
 accepted_at INTEGER, commit_seq INTEGER, error_code TEXT NOT NULL DEFAULT '',
 UNIQUE(reply_id, original_id)
);
CREATE INDEX IF NOT EXISTS reply_claims_original ON reply_claims(original_id);
CREATE TABLE IF NOT EXISTS reply_winners (
 original_id TEXT PRIMARY KEY, reply_id TEXT NOT NULL REFERENCES reply_claims(reply_id),
 committed_at INTEGER NOT NULL, commit_seq INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS observations (
 reply_id TEXT PRIMARY KEY REFERENCES reply_claims(reply_id) ON DELETE CASCADE,
 native_item_id TEXT NOT NULL, observed_at INTEGER NOT NULL,
 digest TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
 seq INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL,
 operation_id TEXT, reply_id TEXT, state TEXT NOT NULL, at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS events_operation ON events(operation_id, seq);
`

func (j *Journal) loadOrCreateStoreID(ctx context.Context) error {
	var id string
	err := j.db.QueryRowContext(ctx, "SELECT value FROM meta WHERE key='store_id'").Scan(&id)
	if err == nil && id != "" {
		j.storeID = id
		return nil
	}
	if err != sql.ErrNoRows && err != nil {
		return fmt.Errorf("journal store identity: %w", err)
	}
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return err
	}
	id = "store_" + hex.EncodeToString(b)
	if _, err := j.db.ExecContext(ctx, "INSERT INTO meta(key,value) VALUES('store_id',?)", id); err != nil {
		// A concurrent opener may have won creation. Read it back rather than
		// generating an unstable identity.
		if scanErr := j.db.QueryRowContext(ctx, "SELECT value FROM meta WHERE key='store_id'").Scan(&id); scanErr != nil {
			return fmt.Errorf("journal store identity: %w", err)
		}
	}
	j.storeID = id
	return nil
}

func (j *Journal) Close() error     { return j.db.Close() }
func (j *Journal) StateDir() string { return j.stateDir }
func (j *Journal) StoreID() string  { j.mu.RLock(); defer j.mu.RUnlock(); return j.storeID }
func (j *Journal) nowUnix() int64   { return j.now().UTC().UnixNano() }

func (j *Journal) withTx(ctx context.Context, fn func(*sql.Tx) error) error {
	deadline := time.Now().Add(j.busyTimeout)
	for {
		tx, err := j.db.BeginTx(ctx, nil)
		if err != nil {
			if !isBusy(err) || time.Now().After(deadline) {
				return err
			}
			if err := waitBusy(ctx, deadline); err != nil {
				return err
			}
			continue
		}
		err = fn(tx)
		if err != nil {
			_ = tx.Rollback()
			if !isBusy(err) || time.Now().After(deadline) {
				return err
			}
			if err := waitBusy(ctx, deadline); err != nil {
				return err
			}
			continue
		}
		// A busy COMMIT is deliberately returned rather than replaying the
		// closure: SQLite may have committed the transaction despite the error.
		return tx.Commit()
	}
}

func isBusy(err error) bool {
	s := err.Error()
	return strings.Contains(s, "SQLITE_BUSY") || strings.Contains(s, "database is locked")
}

func waitBusy(ctx context.Context, deadline time.Time) error {
	d := 5 * time.Millisecond
	if left := time.Until(deadline); left < d {
		d = left
	}
	if d <= 0 {
		return context.DeadlineExceeded
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func emit(tx *sql.Tx, kind, operationID, replyID string, state EvidenceState, now int64) error {
	_, err := tx.Exec("INSERT INTO events(kind,operation_id,reply_id,state,at) VALUES(?,?,?,?,?)", kind, operationID, replyID, string(state), now)
	return err
}
