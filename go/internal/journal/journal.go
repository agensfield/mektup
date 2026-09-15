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
	"net/url"
	"os"
	"path/filepath"
	"sort"
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
	dsn := "file:" + escapedSQLitePath(dbPath) + "?_pragma=busy_timeout(" + fmt.Sprint(timeout.Milliseconds()) + ")&_pragma=foreign_keys(1)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_txlock=immediate"
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

func escapedSQLitePath(path string) string {
	parts := strings.Split(path, string(os.PathSeparator))
	for i, part := range parts {
		parts[i] = url.PathEscape(part)
	}
	return strings.Join(parts, string(os.PathSeparator))
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
	var sqliteVersion string
	if err := j.db.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&sqliteVersion); err != nil {
		return fmt.Errorf("journal sqlite version: %w", err)
	}
	if !atLeastSQLite(sqliteVersion, 3, 51, 3) {
		return fmt.Errorf("journal: SQLite %s is below required 3.51.3", sqliteVersion)
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
	// Rollback is harmless after Commit and guarantees every validation or
	// migration error closes the transaction, including future-schema exits.
	defer func() { _ = tx.Rollback() }()
	var version int
	if err = tx.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	if version > 4 {
		return fmt.Errorf("journal: unsupported schema version %d", version)
	}
	if version == 0 {
		if _, err = tx.ExecContext(ctx, schemaV1); err != nil {
			return fmt.Errorf("journal migration: %w", err)
		}
		if _, err = tx.ExecContext(ctx, "PRAGMA user_version=4"); err != nil {
			return fmt.Errorf("journal migration: %w", err)
		}
	} else if version == 1 {
		if err = migrateV1ToV4(ctx, tx, j.leaseDuration); err != nil {
			return err
		}
	} else if version == 2 {
		if err = migrateV2ToV4(ctx, tx); err != nil {
			return err
		}
	} else if version == 3 {
		if err = migrateV3ToV4(ctx, tx); err != nil {
			return err
		}
	} else if version == 4 {
		if err = validateV4Schema(ctx, tx); err != nil {
			return err
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

func atLeastSQLite(got string, wantMajor, wantMinor, wantPatch int) bool {
	var major, minor, patch int
	if _, err := fmt.Sscanf(got, "%d.%d.%d", &major, &minor, &patch); err != nil {
		return false
	}
	if major != wantMajor {
		return major > wantMajor
	}
	if minor != wantMinor {
		return minor > wantMinor
	}
	return patch >= wantPatch
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
CREATE TABLE IF NOT EXISTS store_id_aliases (alias TEXT PRIMARY KEY, store_id TEXT NOT NULL, created_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS operations (
 operation_id TEXT PRIMARY KEY, message_id TEXT NOT NULL UNIQUE,
 source_route TEXT NOT NULL, target_route TEXT NOT NULL, semantics TEXT NOT NULL,
 reply_route TEXT NOT NULL DEFAULT '', custody_route TEXT NOT NULL DEFAULT '', custody_store_id TEXT NOT NULL DEFAULT '',
 digest TEXT NOT NULL, body_size INTEGER NOT NULL CHECK(body_size >= 0),
 state TEXT NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
 dispatch_started_at INTEGER, terminal_at INTEGER, error_code TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS attempts (
 operation_id TEXT PRIMARY KEY REFERENCES operations(operation_id) ON DELETE CASCADE,
 state TEXT NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
 owner TEXT NOT NULL, token TEXT NOT NULL, lease_until INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS reply_claims (
 reply_id TEXT PRIMARY KEY, original_id TEXT NOT NULL,
 digest TEXT NOT NULL, body_size INTEGER NOT NULL CHECK(body_size >= 0),
 status TEXT NOT NULL CHECK(status IN ('success','error')),
 reply_route TEXT NOT NULL, custody_route TEXT NOT NULL,
 custody_store_id TEXT NOT NULL DEFAULT '',
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
CREATE TABLE IF NOT EXISTS manual_resolutions (
 operation_id TEXT PRIMARY KEY REFERENCES operations(operation_id) ON DELETE CASCADE,
 assertion TEXT NOT NULL, actor TEXT NOT NULL, reason TEXT NOT NULL,
 evidence_ref TEXT NOT NULL, resolved_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS operation_acceptances (
 operation_id TEXT PRIMARY KEY REFERENCES operations(operation_id) ON DELETE CASCADE,
 evidence_ref TEXT NOT NULL, recorded_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS reply_acceptances (
 reply_id TEXT PRIMARY KEY REFERENCES reply_claims(reply_id) ON DELETE CASCADE,
 evidence_ref TEXT NOT NULL, recorded_at INTEGER NOT NULL
);
`

func migrateV1ToV4(ctx context.Context, tx *sql.Tx, leaseDuration time.Duration) error {
	stmts := []string{
		`ALTER TABLE attempts ADD COLUMN owner TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE attempts ADD COLUMN token TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE attempts ADD COLUMN lease_until INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE operations ADD COLUMN reply_route TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE operations ADD COLUMN custody_route TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE operations ADD COLUMN custody_store_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE reply_claims ADD COLUMN custody_store_id TEXT NOT NULL DEFAULT ''`,
		`CREATE TABLE IF NOT EXISTS manual_resolutions (operation_id TEXT PRIMARY KEY REFERENCES operations(operation_id) ON DELETE CASCADE, assertion TEXT NOT NULL, actor TEXT NOT NULL, reason TEXT NOT NULL, evidence_ref TEXT NOT NULL, resolved_at INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS store_id_aliases (alias TEXT PRIMARY KEY, store_id TEXT NOT NULL, created_at INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS operation_acceptances (operation_id TEXT PRIMARY KEY REFERENCES operations(operation_id) ON DELETE CASCADE, evidence_ref TEXT NOT NULL, recorded_at INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS reply_acceptances (reply_id TEXT PRIMARY KEY REFERENCES reply_claims(reply_id) ON DELETE CASCADE, evidence_ref TEXT NOT NULL, recorded_at INTEGER NOT NULL)`,
		`PRAGMA user_version=4`,
	}
	for _, stmt := range stmts {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("journal migration: %w", err)
		}
	}
	rows, err := tx.QueryContext(ctx, "SELECT operation_id,updated_at FROM attempts WHERE lease_until=0")
	if err != nil {
		return fmt.Errorf("journal migration: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var operationID string
		var updatedAt int64
		if err := rows.Scan(&operationID, &updatedAt); err != nil {
			return fmt.Errorf("journal migration: %w", err)
		}
		token, err := randomToken()
		if err != nil {
			return err
		}
		owner := "legacy-" + token[:12]
		if _, err := tx.ExecContext(ctx, "UPDATE attempts SET owner=?,token=?,lease_until=? WHERE operation_id=? AND lease_until=0", owner, token, updatedAt+leaseDuration.Nanoseconds(), operationID); err != nil {
			return fmt.Errorf("journal migration: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("journal migration: %w", err)
	}
	return nil
}

func migrateV2ToV4(ctx context.Context, tx *sql.Tx) error {
	for _, stmt := range []string{
		`ALTER TABLE operations ADD COLUMN reply_route TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE operations ADD COLUMN custody_route TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE operations ADD COLUMN custody_store_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE reply_claims ADD COLUMN custody_store_id TEXT NOT NULL DEFAULT ''`,
		`CREATE TABLE IF NOT EXISTS manual_resolutions (operation_id TEXT PRIMARY KEY REFERENCES operations(operation_id) ON DELETE CASCADE, assertion TEXT NOT NULL, actor TEXT NOT NULL, reason TEXT NOT NULL, evidence_ref TEXT NOT NULL, resolved_at INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS store_id_aliases (alias TEXT PRIMARY KEY, store_id TEXT NOT NULL, created_at INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS operation_acceptances (operation_id TEXT PRIMARY KEY REFERENCES operations(operation_id) ON DELETE CASCADE, evidence_ref TEXT NOT NULL, recorded_at INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS reply_acceptances (reply_id TEXT PRIMARY KEY REFERENCES reply_claims(reply_id) ON DELETE CASCADE, evidence_ref TEXT NOT NULL, recorded_at INTEGER NOT NULL)`,
		`PRAGMA user_version=4`,
	} {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("journal migration: %w", err)
		}
	}
	return nil
}

func ensureV4Tables(ctx context.Context, tx *sql.Tx) error {
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS store_id_aliases (alias TEXT PRIMARY KEY, store_id TEXT NOT NULL, created_at INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS operation_acceptances (operation_id TEXT PRIMARY KEY REFERENCES operations(operation_id) ON DELETE CASCADE, evidence_ref TEXT NOT NULL, recorded_at INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS reply_acceptances (reply_id TEXT PRIMARY KEY REFERENCES reply_claims(reply_id) ON DELETE CASCADE, evidence_ref TEXT NOT NULL, recorded_at INTEGER NOT NULL)`,
		`PRAGMA user_version=4`,
	} {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("journal migration: %w", err)
		}
	}
	return nil
}

func migrateV3ToV4(ctx context.Context, tx *sql.Tx) error {
	if err := ensureV4Tables(ctx, tx); err != nil {
		return err
	}
	return repairV3PositiveReplies(ctx, tx)
}

func validateV4Schema(ctx context.Context, tx *sql.Tx) error {
	required := []string{"meta", "store_id_aliases", "operations", "attempts", "reply_claims", "reply_winners", "observations", "events", "manual_resolutions", "operation_acceptances", "reply_acceptances"}
	for _, name := range required {
		var n int
		if err := tx.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", name).Scan(&n); err != nil {
			return fmt.Errorf("journal schema validation: %w", err)
		}
		if n != 1 {
			return fmt.Errorf("%w: required v4 table %s is missing", ErrCorrupt, name)
		}
	}
	return nil
}

type legacyPositiveReply struct {
	replyID, originalID                            string
	state                                          EvidenceState
	seq, acceptedAt, eventSeq, observedAt, eventAt int64
}

// repairV3PositiveReplies reconstructs the custody order lost by the earlier
// reconciliation implementation. It refuses to guess when no durable event
// or observation evidence exists.
func repairV3PositiveReplies(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `SELECT c.reply_id,c.original_id,c.state,COALESCE(c.commit_seq,0),COALESCE(c.accepted_at,0),
COALESCE((SELECT MAX(seq) FROM events e WHERE e.reply_id=c.reply_id AND e.kind IN ('reply.accepted','reply.reconciled')),0),
COALESCE((SELECT MAX(observed_at) FROM observations o WHERE o.reply_id=c.reply_id),0),
COALESCE((SELECT MAX(at) FROM events e WHERE e.reply_id=c.reply_id AND e.kind IN ('reply.accepted','reply.reconciled')),0)
FROM reply_claims c WHERE c.state IN (?,?)`, string(StateReplyAccepted), string(StateReplyObserved))
	if err != nil {
		return fmt.Errorf("%w: inspect legacy positive replies: %v", ErrCorrupt, err)
	}
	defer rows.Close()
	var positives []legacyPositiveReply
	var maxSeq int64
	for rows.Next() {
		var p legacyPositiveReply
		if err := rows.Scan(&p.replyID, &p.originalID, &p.state, &p.seq, &p.acceptedAt, &p.eventSeq, &p.observedAt, &p.eventAt); err != nil {
			return fmt.Errorf("%w: inspect legacy positive replies: %v", ErrCorrupt, err)
		}
		if p.seq > maxSeq {
			maxSeq = p.seq
		}
		if p.seq == 0 && p.acceptedAt == 0 && p.eventSeq == 0 && p.observedAt == 0 && p.eventAt == 0 {
			return fmt.Errorf("%w: positive reply %s has no acceptance evidence", ErrCorrupt, p.replyID)
		}
		positives = append(positives, p)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("%w: inspect legacy positive replies: %v", ErrCorrupt, err)
	}
	sort.Slice(positives, func(i, j int) bool { return evidenceKey(positives[i]) < evidenceKey(positives[j]) })
	for i := 1; i < len(positives); i++ {
		if positives[i].seq == 0 && positives[i-1].seq == 0 && evidenceKey(positives[i]) == evidenceKey(positives[i-1]) {
			return fmt.Errorf("%w: tied acceptance evidence for replies %s and %s", ErrCorrupt, positives[i-1].replyID, positives[i].replyID)
		}
	}
	for _, p := range positives {
		seq := p.seq
		if seq == 0 {
			maxSeq++
			seq = maxSeq
		}
		at := p.acceptedAt
		if at == 0 {
			at = p.observedAt
			if at == 0 {
				at = p.eventAt
			}
		}
		if at == 0 {
			return fmt.Errorf("%w: positive reply %s has no acceptance timestamp", ErrCorrupt, p.replyID)
		}
		if _, err := tx.ExecContext(ctx, "UPDATE reply_claims SET accepted_at=?,commit_seq=? WHERE reply_id=?", at, seq, p.replyID); err != nil {
			return fmt.Errorf("%w: repair positive reply %s: %v", ErrCorrupt, p.replyID, err)
		}
		if _, err := tx.ExecContext(ctx, "INSERT OR IGNORE INTO reply_winners(original_id,reply_id,committed_at,commit_seq) VALUES(?,?,?,?)", p.originalID, p.replyID, at, seq); err != nil {
			return fmt.Errorf("%w: repair winner %s: %v", ErrCorrupt, p.replyID, err)
		}
	}
	return nil
}

func evidenceKey(p legacyPositiveReply) int64 {
	if p.eventSeq != 0 {
		return p.eventSeq
	}
	if p.acceptedAt != 0 {
		return p.acceptedAt
	}
	if p.observedAt != 0 {
		return p.observedAt
	}
	return p.eventAt
}

func (j *Journal) loadOrCreateStoreID(ctx context.Context) error {
	var id string
	err := j.db.QueryRowContext(ctx, "SELECT value FROM meta WHERE key='store_id'").Scan(&id)
	if err == nil && id != "" {
		if validStoreID(id) {
			j.storeID = id
			return nil
		}
		newID, genErr := newStoreID()
		if genErr != nil {
			return genErr
		}
		tx, txErr := j.db.BeginTx(ctx, nil)
		if txErr != nil {
			return fmt.Errorf("journal store identity: %w", txErr)
		}
		res, updateErr := tx.ExecContext(ctx, "UPDATE meta SET value=? WHERE key='store_id' AND value=?", newID, id)
		if updateErr != nil {
			_ = tx.Rollback()
			return fmt.Errorf("journal store identity: %w", updateErr)
		}
		if n, _ := res.RowsAffected(); n == 0 {
			if scanErr := tx.QueryRowContext(ctx, "SELECT value FROM meta WHERE key='store_id'").Scan(&newID); scanErr != nil {
				_ = tx.Rollback()
				return scanErr
			}
		} else if _, aliasErr := tx.ExecContext(ctx, "INSERT OR IGNORE INTO store_id_aliases(alias,store_id,created_at) VALUES(?,?,?)", id, newID, j.nowUnix()); aliasErr != nil {
			_ = tx.Rollback()
			return fmt.Errorf("journal store identity alias: %w", aliasErr)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("journal store identity: %w", err)
		}
		j.storeID = newID
		return nil
	}
	if err != sql.ErrNoRows && err != nil {
		return fmt.Errorf("journal store identity: %w", err)
	}
	id, err = newStoreID()
	if err != nil {
		return err
	}
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

func newStoreID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	ms := uint64(time.Now().UnixMilli())
	b[0], b[1], b[2], b[3], b[4], b[5] = byte(ms>>40), byte(ms>>32), byte(ms>>24), byte(ms>>16), byte(ms>>8), byte(ms)
	b[6] = (b[6] & 0x0f) | 0x70
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b[:])
	return "store_" + h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32], nil
}

func validStoreID(id string) bool {
	if !strings.HasPrefix(id, "store_") || len(id) != len("store_")+36 {
		return false
	}
	b, err := hex.DecodeString(strings.ReplaceAll(id[len("store_"):], "-", ""))
	return err == nil && len(b) == 16 && b[6]>>4 == 7 && b[8]>>6 == 2
}

func (j *Journal) Close() error     { return j.db.Close() }
func (j *Journal) StateDir() string { return j.stateDir }
func (j *Journal) StoreID() string  { j.mu.RLock(); defer j.mu.RUnlock(); return j.storeID }
func (j *Journal) nowUnix() int64   { return j.now().UTC().UnixNano() }

// ResolveStoreID preserves custody references issued by the pre-UUIDv7
// journal while making the new canonical identity explicit.
func (j *Journal) ResolveStoreID(ctx context.Context, id string) (string, error) {
	if id == "" {
		return "", ErrNotFound
	}
	if id == j.StoreID() {
		return id, nil
	}
	var canonical string
	if err := j.db.QueryRowContext(ctx, "SELECT store_id FROM store_id_aliases WHERE alias=?", id).Scan(&canonical); err != nil {
		if err == sql.ErrNoRows {
			return "", ErrNotFound
		}
		return "", err
	}
	return canonical, nil
}

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
