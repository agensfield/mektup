package journal

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	mektup "github.com/agensfield/mektup/go"
)

// Operation describes metadata persisted before a request can be dispatched.
type Operation struct {
	OperationID      string
	MessageID        string
	SourceRoute      string
	TargetRoute      string
	Semantics        string
	SourceEndpointID string
	TargetEndpointID string
	ReplyRoute       string
	ReplyEndpointID  string
	ReplyThreadID    string
	CustodyRoute     string
	CustodyStoreID   string
	AttemptOwner     string
	Digest           string
	BodySize         int64
}

type OperationRecord struct {
	Operation
	State             EvidenceState
	CreatedAt         int64
	UpdatedAt         int64
	DispatchStartedAt int64
	TerminalAt        int64
	ErrorCode         string
	AttemptToken      string
	AttemptLeaseUntil int64
	ManualResolution  *ManualResolutionRecord
}

type ManualResolution struct {
	Assertion    string
	Actor        string
	Reason       string
	EvidenceRef  string
	Presentation string
	Timestamp    time.Time
}

type ManualResolutionRecord struct {
	Assertion    string
	Actor        string
	Reason       string
	EvidenceRef  string
	Presentation string
	ResolvedAt   time.Time
}

const MaxOperationQueryLimit = 100

type OperationQuery struct {
	State EvidenceState
	Since time.Time
	Limit int
}

// ImportOperation records metadata for a successor wait without creating an
// attempts row, owner, lease, or fencing token.
func (j *Journal) ImportOperation(ctx context.Context, op Operation, state EvidenceState, errorCode string) error {
	if op.OperationID == "" || op.MessageID == "" || op.SourceRoute == "" || op.TargetRoute == "" || op.Digest == "" || op.BodySize < 0 || !state.Valid() {
		return fmt.Errorf("journal: invalid imported operation metadata")
	}
	if (op.ReplyRoute == "") != (op.CustodyRoute == "") || (op.ReplyRoute != "" && op.CustodyStoreID == "") {
		return fmt.Errorf("journal: incomplete imported reply custody relationship")
	}
	now := j.nowUnix()
	return j.withTx(ctx, func(tx *sql.Tx) error {
		var existing OperationRecord
		err := scanOperation(tx.QueryRow(operationSelect+"o.operation_id=?", op.OperationID), &existing)
		if err == nil {
			if existing.OperationID != op.OperationID || existing.MessageID != op.MessageID || existing.SourceRoute != op.SourceRoute || existing.TargetRoute != op.TargetRoute || existing.Semantics != op.Semantics || existing.SourceEndpointID != op.SourceEndpointID || existing.TargetEndpointID != op.TargetEndpointID || existing.ReplyRoute != op.ReplyRoute || existing.ReplyEndpointID != op.ReplyEndpointID || existing.ReplyThreadID != op.ReplyThreadID || existing.CustodyRoute != op.CustodyRoute || existing.CustodyStoreID != op.CustodyStoreID || existing.Digest != op.Digest || existing.BodySize != op.BodySize {
				return ErrIdentityConflict
			}
			return nil
		}
		if err != sql.ErrNoRows {
			return err
		}
		var byMessage OperationRecord
		if err := scanOperation(tx.QueryRow(operationSelect+"o.message_id=?", op.MessageID), &byMessage); err == nil {
			return ErrIdentityConflict
		} else if err != sql.ErrNoRows {
			return err
		}
		_, err = tx.Exec("INSERT INTO operations(operation_id,message_id,source_route,target_route,semantics,source_endpoint_id,target_endpoint_id,reply_route,reply_endpoint_id,reply_thread_id,custody_route,custody_store_id,digest,body_size,state,created_at,updated_at,terminal_at,error_code) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", op.OperationID, op.MessageID, op.SourceRoute, op.TargetRoute, op.Semantics, op.SourceEndpointID, op.TargetEndpointID, op.ReplyRoute, op.ReplyEndpointID, op.ReplyThreadID, op.CustodyRoute, op.CustodyStoreID, op.Digest, op.BodySize, string(state), now, now, now, errorCode)
		if err != nil {
			return err
		}
		return emit(tx, "operation.imported", op.OperationID, "", state, now)
	})
}

const operationSelect = "SELECT o.operation_id,o.message_id,o.source_route,o.target_route,o.semantics,o.source_endpoint_id,o.target_endpoint_id,o.reply_route,o.reply_endpoint_id,o.reply_thread_id,o.custody_route,o.custody_store_id,o.digest,o.body_size,o.state,o.created_at,o.updated_at,COALESCE(o.dispatch_started_at,0),COALESCE(o.terminal_at,0),o.error_code,COALESCE(a.owner,''),COALESCE(a.token,''),COALESCE(a.lease_until,0) FROM operations o LEFT JOIN attempts a ON a.operation_id=o.operation_id WHERE "

// Prepare durably establishes the original relationship. It must be called
// before any transport can write a request. Bodies are intentionally absent.
func (j *Journal) Prepare(ctx context.Context, op Operation) (OperationRecord, error) {
	if op.OperationID == "" || op.MessageID == "" || op.SourceRoute == "" || op.TargetRoute == "" || op.Digest == "" || op.BodySize < 0 {
		return OperationRecord{}, fmt.Errorf("journal: invalid operation metadata")
	}
	if (op.ReplyRoute == "") != (op.CustodyRoute == "") || (op.ReplyRoute != "" && op.CustodyStoreID == "") {
		return OperationRecord{}, fmt.Errorf("journal: incomplete reply custody relationship")
	}
	creator := false
	err := j.withTx(ctx, func(tx *sql.Tx) error {
		now := j.nowUnix()
		var existing OperationRecord
		err := scanOperation(tx.QueryRow(operationSelect+"o.message_id=?", op.MessageID), &existing)
		if err == nil {
			if existing.OperationID == op.OperationID && existing.Digest == op.Digest && existing.BodySize == op.BodySize && existing.SourceRoute == op.SourceRoute && existing.TargetRoute == op.TargetRoute && existing.Semantics == op.Semantics && existing.SourceEndpointID == op.SourceEndpointID && existing.TargetEndpointID == op.TargetEndpointID && existing.ReplyRoute == op.ReplyRoute && existing.ReplyEndpointID == op.ReplyEndpointID && existing.ReplyThreadID == op.ReplyThreadID && existing.CustodyRoute == op.CustodyRoute && existing.CustodyStoreID == op.CustodyStoreID {
				creator = op.AttemptOwner != "" && op.AttemptOwner == existing.AttemptOwner
				return nil
			}
			return ErrIdentityConflict
		}
		if err != sql.ErrNoRows {
			return err
		}
		attemptToken, tokenErr := randomToken()
		if tokenErr != nil {
			return tokenErr
		}
		attemptOwner := op.AttemptOwner
		if attemptOwner == "" {
			attemptOwner = "local-" + attemptToken[:12]
		}
		lease := now + j.leaseDuration.Nanoseconds()
		_, err = tx.Exec("INSERT INTO operations(operation_id,message_id,source_route,target_route,semantics,source_endpoint_id,target_endpoint_id,reply_route,reply_endpoint_id,reply_thread_id,custody_route,custody_store_id,digest,body_size,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", op.OperationID, op.MessageID, op.SourceRoute, op.TargetRoute, op.Semantics, op.SourceEndpointID, op.TargetEndpointID, op.ReplyRoute, op.ReplyEndpointID, op.ReplyThreadID, op.CustodyRoute, op.CustodyStoreID, op.Digest, op.BodySize, string(StatePrepared), now, now)
		if err != nil {
			return err
		}
		if _, err = tx.Exec("INSERT INTO attempts(operation_id,state,created_at,updated_at,owner,token,lease_until) VALUES(?,?,?,?,?,?,?)", op.OperationID, string(StatePrepared), now, now, attemptOwner, attemptToken, lease); err != nil {
			return err
		}
		creator = true
		return emit(tx, "operation.prepared", op.OperationID, "", StatePrepared, now)
	})
	if err != nil {
		return OperationRecord{}, err
	}
	return j.operation(ctx, op.OperationID, creator)
}

func scanOperation(row interface{ Scan(...any) error }, out *OperationRecord) error {
	return row.Scan(&out.OperationID, &out.MessageID, &out.SourceRoute, &out.TargetRoute, &out.Semantics, &out.SourceEndpointID, &out.TargetEndpointID, &out.ReplyRoute, &out.ReplyEndpointID, &out.ReplyThreadID, &out.CustodyRoute, &out.CustodyStoreID, &out.Digest, &out.BodySize, &out.State, &out.CreatedAt, &out.UpdatedAt, &out.DispatchStartedAt, &out.TerminalAt, &out.ErrorCode, &out.AttemptOwner, &out.AttemptToken, &out.AttemptLeaseUntil)
}

func (j *Journal) loadManualResolution(ctx context.Context, operationID string, out *OperationRecord) error {
	var record ManualResolutionRecord
	var resolved int64
	err := j.db.QueryRowContext(ctx, `SELECT assertion,actor,reason,evidence_ref,presentation,resolved_at FROM manual_resolutions WHERE operation_id=?`, operationID).Scan(&record.Assertion, &record.Actor, &record.Reason, &record.EvidenceRef, &record.Presentation, &resolved)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	record.ResolvedAt = time.Unix(0, resolved).UTC()
	out.ManualResolution = &record
	return nil
}

func (j *Journal) Operation(ctx context.Context, operationID string) (OperationRecord, error) {
	return j.operation(ctx, operationID, false)
}

// OperationByMessage resolves the durable operation identity without exposing
// the SQL schema to semantic callers.
func (j *Journal) OperationByMessage(ctx context.Context, messageID string) (OperationRecord, error) {
	var out OperationRecord
	err := scanOperation(j.db.QueryRowContext(ctx, operationSelect+"o.message_id=?", messageID), &out)
	if err == sql.ErrNoRows {
		return out, ErrNotFound
	}
	// Message lookup is read-only and must never grant the live dispatch
	// fencing token. Owned-token recovery remains confined to Prepare.
	out.AttemptToken = ""
	if err == nil {
		err = j.loadManualResolution(ctx, out.OperationID, &out)
	}
	return out, err
}

func (j *Journal) operation(ctx context.Context, operationID string, includeToken bool) (OperationRecord, error) {
	var out OperationRecord
	err := scanOperation(j.db.QueryRowContext(ctx, operationSelect+"o.operation_id=?", operationID), &out)
	if err == sql.ErrNoRows {
		return out, ErrNotFound
	}
	if !includeToken {
		out.AttemptToken = ""
	}
	if err == nil {
		err = j.loadManualResolution(ctx, out.OperationID, &out)
	}
	return out, err
}

func (j *Journal) ListOperations(ctx context.Context, query OperationQuery) ([]OperationRecord, error) {
	limit := query.Limit
	if limit < 0 || limit > MaxOperationQueryLimit {
		return nil, fmt.Errorf("journal: operation limit must be between 0 and %d", MaxOperationQueryLimit)
	}
	if limit == 0 {
		limit = 50
	}
	where := "1=1"
	args := make([]any, 0, 3)
	if query.State != "" {
		if !query.State.Valid() {
			return nil, fmt.Errorf("journal: invalid operation state %q", query.State)
		}
		where += " AND o.state=?"
		args = append(args, string(query.State))
	}
	if !query.Since.IsZero() {
		where += " AND o.created_at>=?"
		args = append(args, query.Since.UTC().UnixNano())
	}
	args = append(args, limit)
	rows, err := j.db.QueryContext(ctx, operationSelect+where+` ORDER BY o.created_at DESC,o.operation_id DESC LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OperationRecord
	for rows.Next() {
		var record OperationRecord
		if err := scanOperation(rows, &record); err != nil {
			return nil, err
		}
		record.AttemptToken = ""
		if err := j.loadManualResolution(ctx, record.OperationID, &record); err != nil {
			return nil, err
		}
		out = append(out, record)
	}
	return out, rows.Err()
}

// MarkDispatchStarted commits the dispatch fence before handing bytes to a
// transport. An orphaned dispatch-started operation is always unknown.
func (j *Journal) MarkDispatchStarted(ctx context.Context, operationID string) error {
	rec, err := j.operation(ctx, operationID, true)
	if err != nil {
		return err
	}
	return j.MarkDispatchStartedOwned(ctx, operationID, rec.AttemptOwner, rec.AttemptToken)
}

// MarkDispatchStartedOwned is the fenced variant for delivery processes. The
// legacy MarkDispatchStarted remains available for callers that already own
// the prepared invocation, while this form makes that ownership explicit.
func (j *Journal) MarkDispatchStartedOwned(ctx context.Context, operationID, owner, token string) error {
	return j.withTx(ctx, func(tx *sql.Tx) error {
		now := j.nowUnix()
		res, err := tx.Exec("UPDATE operations SET state=?,dispatch_started_at=?,updated_at=? WHERE operation_id=? AND state=? AND EXISTS (SELECT 1 FROM attempts WHERE operation_id=? AND owner=? AND token=? AND lease_until>?)", string(StateDispatchStarted), now, now, operationID, string(StatePrepared), operationID, owner, token, now)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			return ErrClaimNotOwned
		}
		if _, err = tx.Exec("UPDATE attempts SET state=?,updated_at=? WHERE operation_id=? AND owner=? AND token=?", string(StateDispatchStarted), now, operationID, owner, token); err != nil {
			return err
		}
		return emit(tx, "operation.dispatch_started", operationID, "", StateDispatchStarted, now)
	})
}

// HeartbeatAttempt proves that the invocation process is still alive. Recovery
// only considers attempts whose lease has actually expired.
func (j *Journal) HeartbeatAttempt(ctx context.Context, operationID, owner, token string) (OperationRecord, error) {
	err := j.withTx(ctx, func(tx *sql.Tx) error {
		now := j.nowUnix()
		lease := now + j.leaseDuration.Nanoseconds()
		res, err := tx.Exec("UPDATE attempts SET lease_until=?,updated_at=? WHERE operation_id=? AND owner=? AND token=? AND state IN (?,?) AND lease_until>?", lease, now, operationID, owner, token, string(StatePrepared), string(StateDispatchStarted), now)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			return ErrClaimNotOwned
		}
		return nil
	})
	if err != nil {
		return OperationRecord{}, err
	}
	return j.Operation(ctx, operationID)
}

func (j *Journal) RecordResult(ctx context.Context, operationID string, state EvidenceState, errorCode string) error {
	if state != StateAccepted && state != StateRejected && state != StateOutcomeUnknown && state != StateNotSent {
		return ErrInvalidTransition
	}
	now := j.nowUnix()
	return j.withTx(ctx, func(tx *sql.Tx) error {
		var current EvidenceState
		if err := tx.QueryRow("SELECT state FROM operations WHERE operation_id=?", operationID).Scan(&current); err != nil {
			if err == sql.ErrNoRows {
				return ErrNotFound
			}
			return err
		}
		valid := (current == StateDispatchStarted && (state == StateAccepted || state == StateRejected || state == StateOutcomeUnknown)) || (current == StateOutcomeUnknown && state == StateAccepted) || (current == StatePrepared && state == StateNotSent)
		if !valid {
			return ErrInvalidTransition
		}
		res, err := tx.Exec("UPDATE operations SET state=?,error_code=?,updated_at=?,terminal_at=? WHERE operation_id=? AND state=?", string(state), errorCode, now, now, operationID, string(current))
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			return ErrInvalidTransition
		}
		if _, err = tx.Exec("UPDATE attempts SET state=?,updated_at=? WHERE operation_id=?", string(state), now, operationID); err != nil {
			return err
		}
		return emit(tx, "operation.result", operationID, "", state, now)
	})
}

// RecordAccepted is the explicit positive-evidence strengthening path for an
// outcome-unknown operation. evidenceRef is metadata only, such as an
// app-server response or reconciled native item identifier.
func (j *Journal) RecordAccepted(ctx context.Context, operationID, evidenceRef string) error {
	if evidenceRef == "" {
		return fmt.Errorf("journal: acceptance evidence reference required")
	}
	return j.withTx(ctx, func(tx *sql.Tx) error {
		now := j.nowUnix()
		var current EvidenceState
		if err := tx.QueryRow("SELECT state FROM operations WHERE operation_id=?", operationID).Scan(&current); err != nil {
			if err == sql.ErrNoRows {
				return ErrNotFound
			}
			return err
		}
		if current != StateDispatchStarted && current != StateOutcomeUnknown {
			return ErrInvalidTransition
		}
		if _, err := tx.Exec("UPDATE operations SET state=?,updated_at=?,terminal_at=? WHERE operation_id=? AND state=?", string(StateAccepted), now, now, operationID, string(current)); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT OR REPLACE INTO operation_acceptances(operation_id,evidence_ref,recorded_at) VALUES(?,?,?)", operationID, evidenceRef, now); err != nil {
			return err
		}
		if _, err := tx.Exec("UPDATE attempts SET state=?,updated_at=? WHERE operation_id=?", string(StateAccepted), now, operationID); err != nil {
			return err
		}
		return emit(tx, "operation.accepted", operationID, "", StateAccepted, now)
	})
}

// RecordManualResolution is the only non-observational way to close an
// unknown operation. The original unknown event remains in the event log.
func (j *Journal) RecordManualResolution(ctx context.Context, operationID string, resolution ManualResolution) error {
	if (resolution.Assertion != "accepted" && resolution.Assertion != "not_delivered") || resolution.Actor == "" || resolution.Reason == "" || resolution.EvidenceRef == "" {
		return fmt.Errorf("journal: incomplete manual resolution")
	}
	return j.withTx(ctx, func(tx *sql.Tx) error {
		now := j.nowUnix()
		resolvedAt := now
		if !resolution.Timestamp.IsZero() {
			resolvedAt = resolution.Timestamp.UTC().UnixNano()
		}
		var state EvidenceState
		if err := tx.QueryRow("SELECT state FROM operations WHERE operation_id=?", operationID).Scan(&state); err != nil {
			if err == sql.ErrNoRows {
				return ErrNotFound
			}
			return err
		}
		if state != StateOutcomeUnknown {
			return ErrInvalidTransition
		}
		if _, err := tx.Exec("UPDATE operations SET state=?,updated_at=?,terminal_at=?,error_code=? WHERE operation_id=? AND state=?", string(StateManuallyResolved), now, now, resolution.Assertion, operationID, string(StateOutcomeUnknown)); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT OR REPLACE INTO manual_resolutions(operation_id,assertion,actor,reason,evidence_ref,presentation,resolved_at) VALUES(?,?,?,?,?,?,?)", operationID, resolution.Assertion, resolution.Actor, resolution.Reason, resolution.EvidenceRef, resolution.Presentation, resolvedAt); err != nil {
			return err
		}
		if _, err := tx.Exec("UPDATE attempts SET state=?,updated_at=? WHERE operation_id=?", string(StateManuallyResolved), now, operationID); err != nil {
			return err
		}
		return emit(tx, "operation.manually_resolved", operationID, "", StateManuallyResolved, now)
	})
}

// ResolveWithReceipt atomically commits a caller assertion, its evidence
// transition, and the bodyless receipt projection. Human authorization is
// intentionally performed by the caller before entering this transaction.
// Repeated callers observe the already committed projection rather than
// clobbering it with an older assertion.
func (j *Journal) ResolveWithReceipt(ctx context.Context, operationID string, resolution ManualResolution, receipt mektup.Receipt) (mektup.Receipt, error) {
	if operationID == "" || receipt.OperationID != operationID || resolution.Actor == "" || resolution.Reason == "" || resolution.EvidenceRef == "" || (resolution.Assertion != "accepted" && resolution.Assertion != "not_delivered") {
		return mektup.Receipt{}, fmt.Errorf("journal: incomplete atomic manual resolution")
	}
	if receipt.State != mektup.StateManuallyResolved {
		return mektup.Receipt{}, fmt.Errorf("journal: atomic manual receipt must be manually_resolved")
	}
	var committed mektup.Receipt
	err := j.withTx(ctx, func(tx *sql.Tx) error {
		now := j.nowUnix()
		var state EvidenceState
		if err := tx.QueryRow("SELECT state FROM operations WHERE operation_id=?", operationID).Scan(&state); err != nil {
			if err == sql.ErrNoRows {
				return ErrNotFound
			}
			return err
		}
		if state == StateManuallyResolved {
			var document string
			if err := tx.QueryRow("SELECT document FROM receipts WHERE receipt_id=?", receipt.ReceiptID).Scan(&document); err != nil {
				if err == sql.ErrNoRows {
					return ErrNotFound
				}
				return err
			}
			var err error
			committed, err = parseStoredReceipt(document)
			return err
		}
		if state != StateOutcomeUnknown {
			return ErrInvalidTransition
		}
		resolvedAt := now
		if !resolution.Timestamp.IsZero() {
			resolvedAt = resolution.Timestamp.UTC().UnixNano()
		}
		if _, err := tx.Exec("UPDATE operations SET state=?,updated_at=?,terminal_at=?,error_code=? WHERE operation_id=? AND state=?", string(StateManuallyResolved), now, now, resolution.Assertion, operationID, string(StateOutcomeUnknown)); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT OR REPLACE INTO manual_resolutions(operation_id,assertion,actor,reason,evidence_ref,presentation,resolved_at) VALUES(?,?,?,?,?,?,?)", operationID, resolution.Assertion, resolution.Actor, resolution.Reason, resolution.EvidenceRef, resolution.Presentation, resolvedAt); err != nil {
			return err
		}
		if _, err := tx.Exec("UPDATE attempts SET state=?,updated_at=? WHERE operation_id=?", string(StateManuallyResolved), now, operationID); err != nil {
			return err
		}
		if err := emit(tx, "operation.manually_resolved", operationID, "", StateManuallyResolved, now); err != nil {
			return err
		}
		if err := j.putReceiptTx(ctx, tx, receipt); err != nil {
			return err
		}
		committed = receipt
		return nil
	})
	if err != nil {
		return mektup.Receipt{}, err
	}
	return committed, nil
}

// RecoverOrphans is conservative and idempotent. Prepared intent is proven
// not sent; a dispatch fence means bytes may have left the process.
func (j *Journal) RecoverOrphans(ctx context.Context) error {
	now := j.nowUnix()
	return j.withTx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.Query("SELECT o.operation_id,o.state FROM operations o JOIN attempts a ON a.operation_id=o.operation_id WHERE o.state IN (?,?) AND a.lease_until>0 AND a.lease_until<=?", string(StatePrepared), string(StateDispatchStarted), now)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id, state string
			if err := rows.Scan(&id, &state); err != nil {
				return err
			}
			next := StateNotSent
			if state == string(StateDispatchStarted) {
				next = StateOutcomeUnknown
			}
			if _, err := tx.Exec("UPDATE operations SET state=?,updated_at=?,terminal_at=? WHERE operation_id=?", string(next), now, now, id); err != nil {
				return err
			}
			if _, err := tx.Exec("UPDATE attempts SET state=?,updated_at=? WHERE operation_id=?", string(next), now, id); err != nil {
				return err
			}
			if err := emit(tx, "operation.recovered", id, "", next, now); err != nil {
				return err
			}
		}
		return rows.Err()
	})
}
