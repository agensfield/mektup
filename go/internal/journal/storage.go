package journal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// RetentionAge is the minimum age before a completed record can be removed.
// Unknown and active records are never eligible solely because of age.
const RetentionAge = 30 * 24 * time.Hour

const retentionOperationStates = `o.state IN (?,?,?,?) AND o.terminal_at IS NOT NULL AND o.terminal_at > 0 AND o.terminal_at <= ?`

// retentionOperationPredicate keeps an accepted request-reply operation
// alive until custody has a durable winner and every related claim is itself
// terminal and old. This prevents an accepted outbound send from erasing the
// relationship a later wait needs to inspect.
const retentionOperationPredicate = retentionOperationStates + ` AND (
	o.reply_route = '' OR (
		o.state IN (?,?,?) AND NOT EXISTS (
			SELECT 1 FROM reply_claims related
			WHERE related.original_id = o.message_id
			  AND (related.state NOT IN (?,?) OR related.updated_at > ?)
		)
	) OR (
		o.state = ? AND
		EXISTS (
			SELECT 1 FROM reply_winners w
			JOIN reply_claims winner ON winner.reply_id = w.reply_id
			WHERE w.original_id = o.message_id
			  AND winner.state IN (?,?)
			  AND winner.updated_at <= ?
		)
		AND NOT EXISTS (
			SELECT 1 FROM reply_claims related
			WHERE related.original_id = o.message_id
			  AND (related.state NOT IN (?,?) OR related.updated_at > ?)
		)
	)
)`

var (
	ErrStorageBusy = errors.New("journal: storage busy")
	// ErrStorageCorrupt is an alias for the journal's established corruption
	// sentinel so callers do not need separate error handling for check paths.
	ErrStorageCorrupt = ErrCorrupt
)

// StorageStatus is bounded, metadata-only information about the journal.
// Counts are grouped by persisted evidence state and never include bodies.
type StorageStatus struct {
	StateDir          string           `json:"stateDir"`
	DatabasePath      string           `json:"databasePath"`
	WALPath           string           `json:"walPath"`
	SHMPath           string           `json:"shmPath"`
	DatabaseBytes     int64            `json:"databaseBytes"`
	WALBytes          int64            `json:"walBytes"`
	SHMBytes          int64            `json:"shmBytes"`
	DatabaseModified  time.Time        `json:"databaseModified,omitempty"`
	SchemaVersion     int              `json:"schemaVersion"`
	SQLiteVersion     string           `json:"sqliteVersion"`
	JournalMode       string           `json:"journalMode"`
	Counts            map[string]int64 `json:"counts"`
	OperationCounts   map[string]int64 `json:"operationCounts"`
	ReplyCounts       map[string]int64 `json:"replyCounts"`
	RetentionCutoff   time.Time        `json:"retentionCutoff"`
	RetentionEligible int64            `json:"retentionEligible"`
}

// StorageCheck is a read-only integrity result. An empty Issues slice is a
// successful check; the ReadOnly flag is included in receipts to make the
// no-write guarantee explicit to callers.
type StorageCheck struct {
	ReadOnly           bool     `json:"readOnly"`
	Integrity          string   `json:"integrity"`
	ForeignKeyIssues   []string `json:"foreignKeyIssues,omitempty"`
	RelationshipIssues []string `json:"relationshipIssues,omitempty"`
}

// MaintenanceOptions controls the explicit maintenance operation.
type MaintenanceOptions struct {
	// Before is an optional caller-provided upper bound. It is clamped to the
	// mandatory 30-day retention boundary and can never make newer records
	// eligible.
	Before time.Time
	DryRun bool
	Now    func() time.Time
}

type MaintenanceAction struct {
	Kind      string `json:"kind"`
	Attempted bool   `json:"attempted"`
	Applied   bool   `json:"applied"`
	Eligible  int64  `json:"eligible,omitempty"`
	Changed   int64  `json:"changed,omitempty"`
	Busy      bool   `json:"busy,omitempty"`
	Error     string `json:"error,omitempty"`
}

// MaintenanceReceipt separately describes expiry, pruning, WAL checkpoint,
// and optimization. This is intentionally a result/receipt, not a journal
// event: recording the receipt must not introduce a second write transaction
// that could make a failed maintenance action look successful.
type MaintenanceReceipt struct {
	DryRun          bool                `json:"dryRun"`
	RetentionCutoff time.Time           `json:"retentionCutoff"`
	Actions         []MaintenanceAction `json:"actions"`
}

// VacuumReceipt reports the explicit blocking rewrite and its size effect.
type VacuumReceipt struct {
	DatabasePath string `json:"databasePath"`
	BeforeBytes  int64  `json:"beforeBytes"`
	AfterBytes   int64  `json:"afterBytes"`
	Applied      bool   `json:"applied"`
}

func (j *Journal) StorageStatus(ctx context.Context) (StorageStatus, error) {
	status := StorageStatus{
		StateDir:     j.stateDir,
		DatabasePath: filepath.Join(j.stateDir, "journal.sqlite3"),
		WALPath:      filepath.Join(j.stateDir, "journal.sqlite3-wal"),
		SHMPath:      filepath.Join(j.stateDir, "journal.sqlite3-shm"),
		Counts:       make(map[string]int64), OperationCounts: make(map[string]int64), ReplyCounts: make(map[string]int64),
	}
	if err := statFile(status.DatabasePath, &status.DatabaseBytes, &status.DatabaseModified); err != nil {
		return status, classifyStorageError(err)
	}
	if err := statFile(status.WALPath, &status.WALBytes, nil); err != nil {
		return status, classifyStorageError(err)
	}
	if err := statFile(status.SHMPath, &status.SHMBytes, nil); err != nil {
		return status, classifyStorageError(err)
	}
	if err := j.db.QueryRowContext(ctx, "SELECT sqlite_version() ").Scan(&status.SQLiteVersion); err != nil {
		return status, classifyStorageError(err)
	}
	if err := j.db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&status.SchemaVersion); err != nil {
		return status, classifyStorageError(err)
	}
	if err := j.db.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&status.JournalMode); err != nil {
		return status, classifyStorageError(err)
	}
	for _, table := range []string{"operations", "reply_claims", "events", "observations", "manual_resolutions", "receipts", "blockers"} {
		var count int64
		err := j.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table).Scan(&count)
		if err != nil {
			// blockers is not part of the custody foundation yet. It is still
			// useful to expose a stable zero count when absent.
			if table == "blockers" && strings.Contains(err.Error(), "no such table") {
				continue
			}
			return status, classifyStorageError(err)
		}
		status.Counts[table] = count
	}
	for _, table := range []struct {
		table  string
		target map[string]int64
	}{
		{"operations", status.OperationCounts},
		{"reply_claims", status.ReplyCounts},
	} {
		rows, err := j.db.QueryContext(ctx, "SELECT state,COUNT(*) FROM "+table.table+" GROUP BY state")
		if err != nil {
			return status, classifyStorageError(err)
		}
		for rows.Next() {
			var state string
			var count int64
			if err := rows.Scan(&state, &count); err != nil {
				rows.Close()
				return status, classifyStorageError(err)
			}
			table.target[state] = count
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return status, classifyStorageError(err)
		}
		rows.Close()
	}
	status.RetentionCutoff = time.Now().UTC().Add(-RetentionAge)
	if j.now != nil {
		status.RetentionCutoff = j.now().UTC().Add(-RetentionAge)
	}
	if err := j.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM operations o WHERE `+retentionOperationPredicate, retentionOperationArgs(status.RetentionCutoff.UnixNano())...).Scan(&status.RetentionEligible); err != nil {
		return status, classifyStorageError(err)
	}
	var replies int64
	if err := j.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM reply_claims c WHERE `+retentionReplyPredicate, retentionReplyArgs(status.RetentionCutoff.UnixNano())...).Scan(&replies); err != nil {
		return status, classifyStorageError(err)
	}
	status.RetentionEligible += replies
	var receipts, blockers int64
	if err := j.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM receipts WHERE `+retentionReceiptPredicate, retentionReceiptArgs(status.RetentionCutoff.UnixNano())...).Scan(&receipts); err != nil {
		return status, classifyStorageError(err)
	}
	if err := j.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM blockers WHERE resolved_at IS NOT NULL AND resolved_at <= ?`, status.RetentionCutoff.UnixNano()).Scan(&blockers); err != nil {
		return status, classifyStorageError(err)
	}
	status.RetentionEligible += receipts + blockers
	return status, nil
}

func statFile(path string, bytes *int64, modified *time.Time) error {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			*bytes = 0
			return nil
		}
		return err
	}
	if info.IsDir() {
		return fmt.Errorf("journal: storage path is a directory: %s", path)
	}
	*bytes = info.Size()
	if modified != nil {
		*modified = info.ModTime().UTC()
	}
	return nil
}

func retentionOperationArgs(cutoff int64) []any {
	return []any{
		string(StateAccepted), string(StateRejected), string(StateNotSent), string(StateManuallyResolved), cutoff,
		string(StateRejected), string(StateNotSent), string(StateManuallyResolved),
		string(StateReplyAccepted), string(StateReplyObserved), cutoff,
		string(StateAccepted), string(StateReplyAccepted), string(StateReplyObserved), cutoff,
		string(StateReplyAccepted), string(StateReplyObserved), cutoff,
	}
}

const retentionReplyPredicate = `c.state IN (?,?) AND c.updated_at <= ? AND EXISTS (
	SELECT 1 FROM operations o WHERE o.message_id = c.original_id AND ` + retentionOperationPredicate + `
)`

const retentionReceiptPredicate = `state IN (?,?,?,?,?,?) AND updated_at <= ?`

func retentionReceiptArgs(cutoff int64) []any {
	return []any{string(StateAccepted), string(StateRejected), string(StateNotSent), string(StateManuallyResolved), string(StateReplyAccepted), string(StateReplyObserved), cutoff}
}

func retentionReplyArgs(cutoff int64) []any {
	args := []any{string(StateReplyAccepted), string(StateReplyObserved), cutoff}
	return append(args, retentionOperationArgs(cutoff)...)
}

// StorageCheck performs only SELECTs and read-only SQLite pragmas. It never
// creates a directory, repairs schema, changes pragmas, or rebuilds files.
func (j *Journal) StorageCheck(ctx context.Context) (StorageCheck, error) {
	return checkDB(ctx, j.db)
}

func checkDB(ctx context.Context, db *sql.DB) (StorageCheck, error) {
	result := StorageCheck{ReadOnly: true}
	if err := db.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&result.Integrity); err != nil {
		return result, classifyStorageError(err)
	}
	if result.Integrity != "ok" {
		return result, fmt.Errorf("%w: integrity_check: %s", ErrStorageCorrupt, result.Integrity)
	}
	if err := validateReadOnlySchema(ctx, db); err != nil {
		return result, err
	}
	rows, err := db.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return result, classifyStorageError(err)
	}
	for rows.Next() {
		var table string
		var rowid, parent, fkid any
		if err := rows.Scan(&table, &rowid, &parent, &fkid); err != nil {
			rows.Close()
			return result, classifyStorageError(err)
		}
		result.ForeignKeyIssues = append(result.ForeignKeyIssues, fmt.Sprintf("%s row=%v parent=%v fk=%v", table, rowid, parent, fkid))
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return result, classifyStorageError(err)
	}
	rows.Close()
	checks := []struct {
		name  string
		query string
	}{
		{"orphan_attempt", `SELECT COUNT(*) FROM attempts a LEFT JOIN operations o ON o.operation_id=a.operation_id WHERE o.operation_id IS NULL`},
		{"orphan_reply_claim", `SELECT COUNT(*) FROM reply_claims c LEFT JOIN operations o ON o.message_id=c.original_id WHERE o.operation_id IS NULL`},
		{"orphan_observation", `SELECT COUNT(*) FROM observations x LEFT JOIN reply_claims c ON c.reply_id=x.reply_id WHERE c.reply_id IS NULL`},
		{"orphan_winner", `SELECT COUNT(*) FROM reply_winners w LEFT JOIN reply_claims c ON c.reply_id=w.reply_id WHERE c.reply_id IS NULL`},
	}
	for _, check := range checks {
		var count int64
		if err := db.QueryRowContext(ctx, check.query).Scan(&count); err != nil {
			return result, classifyStorageError(err)
		}
		if count != 0 {
			result.RelationshipIssues = append(result.RelationshipIssues, fmt.Sprintf("%s=%d", check.name, count))
		}
	}
	if len(result.ForeignKeyIssues) != 0 || len(result.RelationshipIssues) != 0 {
		return result, fmt.Errorf("%w: relationship checks failed", ErrStorageCorrupt)
	}
	return result, nil
}

func validateReadOnlySchema(ctx context.Context, db *sql.DB) error {
	var version int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return classifyStorageError(err)
	}
	if version != 4 {
		return fmt.Errorf("%w: unsupported schema version %d", ErrStorageCorrupt, version)
	}
	for _, name := range []string{"meta", "store_id_aliases", "operations", "attempts", "reply_claims", "reply_winners", "observations", "events", "manual_resolutions", "operation_acceptances", "reply_acceptances"} {
		var count int
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", name).Scan(&count); err != nil {
			return classifyStorageError(err)
		}
		if count != 1 {
			return fmt.Errorf("%w: required v4 table %s is missing", ErrStorageCorrupt, name)
		}
	}
	return nil
}

// CheckPath opens an existing database read-only. Unlike Open, it does not
// create directories, run migrations, set pragmas, or create a store ID.
func CheckPath(ctx context.Context, path string) (StorageCheck, error) {
	if path == "" {
		return StorageCheck{ReadOnly: true}, fmt.Errorf("%w: empty database path", ErrStorageCorrupt)
	}
	db, err := sql.Open("sqlite", "file:"+escapedSQLitePath(path)+"?mode=ro&_pragma=busy_timeout(2500)")
	if err != nil {
		return StorageCheck{ReadOnly: true}, classifyStorageError(err)
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return StorageCheck{ReadOnly: true}, classifyStorageError(err)
	}
	return checkDB(ctx, db)
}

// StorageMaintain expires abandoned claims, prunes only records that have
// passed the 30-day boundary, checkpoints WAL, and runs safe optimization.
func (j *Journal) StorageMaintain(ctx context.Context, opts MaintenanceOptions) (MaintenanceReceipt, error) {
	now := time.Now().UTC()
	if opts.Now != nil {
		now = opts.Now().UTC()
	} else if j.now != nil {
		now = j.now().UTC()
	}
	cutoff := now.Add(-RetentionAge)
	if !opts.Before.IsZero() && opts.Before.Before(cutoff) {
		cutoff = opts.Before.UTC()
	}
	receipt := MaintenanceReceipt{DryRun: opts.DryRun, RetentionCutoff: cutoff, Actions: []MaintenanceAction{
		{Kind: "expiry"},
		{Kind: "prune"},
		{Kind: "wal_checkpoint"},
		{Kind: "optimize"},
	}}
	var eligibleOps, eligibleReplies, eligibleReceipts, eligibleBlockers, activeExpired int64
	if err := j.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM operations o WHERE `+retentionOperationPredicate, retentionOperationArgs(cutoff.UnixNano())...).Scan(&eligibleOps); err != nil {
		return receipt, classifyStorageError(err)
	}
	if err := j.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM reply_claims c WHERE `+retentionReplyPredicate, retentionReplyArgs(cutoff.UnixNano())...).Scan(&eligibleReplies); err != nil {
		return receipt, classifyStorageError(err)
	}
	if err := j.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM receipts WHERE `+retentionReceiptPredicate, retentionReceiptArgs(cutoff.UnixNano())...).Scan(&eligibleReceipts); err != nil {
		return receipt, classifyStorageError(err)
	}
	if err := j.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM blockers WHERE resolved_at IS NOT NULL AND resolved_at <= ?`, cutoff.UnixNano()).Scan(&eligibleBlockers); err != nil {
		return receipt, classifyStorageError(err)
	}
	receipt.Actions[1].Eligible = eligibleOps + eligibleReplies + eligibleReceipts + eligibleBlockers
	if err := j.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM reply_claims WHERE state=? AND lease_until <= ?`, string(StateReplyClaimed), now.UnixNano()).Scan(&activeExpired); err != nil {
		return receipt, classifyStorageError(err)
	}
	receipt.Actions[0].Eligible = activeExpired
	if opts.DryRun {
		return receipt, nil
	}
	receipt.Actions[0].Attempted = true
	changed, err := j.expireClaimsAt(ctx, now.UnixNano())
	if err != nil {
		receipt.Actions[0].Error = err.Error()
		receipt.Actions[0].Busy = isBusy(err)
		return receipt, classifyStorageError(err)
	}
	receipt.Actions[0].Changed = changed
	receipt.Actions[0].Applied = true
	receipt.Actions[1].Attempted = true
	var pruned int64
	err = j.withTx(ctx, func(tx *sql.Tx) error {
		args := retentionOperationArgs(cutoff.UnixNano())
		if _, err := tx.Exec(`CREATE TEMP TABLE mektup_prune_operations(operation_id TEXT PRIMARY KEY)`); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO mektup_prune_operations SELECT o.operation_id FROM operations o WHERE `+retentionOperationPredicate, args...); err != nil {
			return err
		}
		replyArgs := retentionReplyArgs(cutoff.UnixNano())
		if _, err := tx.Exec(`CREATE TEMP TABLE mektup_prune_replies(reply_id TEXT PRIMARY KEY)`); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO mektup_prune_replies SELECT c.reply_id FROM reply_claims c WHERE `+retentionReplyPredicate, replyArgs...); err != nil {
			return err
		}
		if _, err := tx.Exec(`DELETE FROM events WHERE operation_id IN (SELECT operation_id FROM mektup_prune_operations)`); err != nil {
			return err
		}
		res, err := tx.Exec(`DELETE FROM operations WHERE operation_id IN (SELECT operation_id FROM mektup_prune_operations)`)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		pruned += n
		if _, err := tx.Exec(`DELETE FROM events WHERE reply_id IN (SELECT reply_id FROM mektup_prune_replies)`); err != nil {
			return err
		}
		if _, err := tx.Exec(`DELETE FROM reply_winners WHERE reply_id IN (SELECT reply_id FROM mektup_prune_replies)`); err != nil {
			return err
		}
		res, err = tx.Exec(`DELETE FROM reply_claims WHERE reply_id IN (SELECT reply_id FROM mektup_prune_replies)`)
		if err != nil {
			return err
		}
		n, _ = res.RowsAffected()
		pruned += n
		res, err = tx.Exec(`DELETE FROM receipts WHERE `+retentionReceiptPredicate, retentionReceiptArgs(cutoff.UnixNano())...)
		if err != nil {
			return err
		}
		n, _ = res.RowsAffected()
		pruned += n
		res, err = tx.Exec(`DELETE FROM blockers WHERE resolved_at IS NOT NULL AND resolved_at <= ?`, cutoff.UnixNano())
		if err != nil {
			return err
		}
		n, _ = res.RowsAffected()
		pruned += n
		_, err = tx.Exec(`DROP TABLE mektup_prune_replies`)
		if err != nil {
			return err
		}
		_, err = tx.Exec(`DROP TABLE mektup_prune_operations`)
		return err
	})
	if err != nil {
		receipt.Actions[1].Error = err.Error()
		receipt.Actions[1].Busy = isBusy(err)
		return receipt, classifyStorageError(err)
	}
	receipt.Actions[1].Changed = pruned
	receipt.Actions[1].Applied = true
	receipt.Actions[2].Attempted = true
	var busy, logPages, checkpointed int64
	if err := j.db.QueryRowContext(ctx, "PRAGMA wal_checkpoint(PASSIVE)").Scan(&busy, &logPages, &checkpointed); err != nil {
		receipt.Actions[2].Error = err.Error()
		receipt.Actions[2].Busy = isBusy(err)
		return receipt, classifyStorageError(err)
	}
	receipt.Actions[2].Changed = checkpointed
	if busy != 0 {
		receipt.Actions[2].Busy = true
	}
	receipt.Actions[2].Applied = true
	receipt.Actions[3].Attempted = true
	if _, err := j.db.ExecContext(ctx, "PRAGMA optimize"); err != nil {
		receipt.Actions[3].Error = err.Error()
		receipt.Actions[3].Busy = isBusy(err)
		return receipt, classifyStorageError(err)
	}
	receipt.Actions[3].Applied = true
	return receipt, nil
}

func (j *Journal) StorageVacuum(ctx context.Context) (VacuumReceipt, error) {
	path := filepath.Join(j.stateDir, "journal.sqlite3")
	receipt := VacuumReceipt{DatabasePath: path}
	if err := statFile(path, &receipt.BeforeBytes, nil); err != nil {
		return receipt, classifyStorageError(err)
	}
	if _, err := j.db.ExecContext(ctx, "VACUUM"); err != nil {
		return receipt, classifyStorageError(err)
	}
	if err := statFile(path, &receipt.AfterBytes, nil); err != nil {
		return receipt, classifyStorageError(err)
	}
	receipt.Applied = true
	return receipt, nil
}

func classifyStorageError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: %v", ErrStorageBusy, err)
	}
	if isBusy(err) {
		return fmt.Errorf("%w: %v", ErrStorageBusy, err)
	}
	lower := strings.ToLower(err.Error())
	if errors.Is(err, ErrCorrupt) || strings.Contains(lower, "malformed") || strings.Contains(lower, "not a database") || strings.Contains(lower, "no such table") {
		return fmt.Errorf("%w: %v", ErrStorageCorrupt, err)
	}
	return err
}
