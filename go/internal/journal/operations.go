package journal

import (
	"context"
	"database/sql"
	"fmt"
)

// Operation describes metadata persisted before a request can be dispatched.
type Operation struct {
	OperationID    string
	MessageID      string
	SourceRoute    string
	TargetRoute    string
	Semantics      string
	ReplyRoute     string
	CustodyRoute   string
	CustodyStoreID string
	AttemptOwner   string
	Digest         string
	BodySize       int64
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
}

type ManualResolution struct {
	Assertion   string
	Actor       string
	Reason      string
	EvidenceRef string
}

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
		err := scanOperation(tx.QueryRow("SELECT o.operation_id,o.message_id,o.source_route,o.target_route,o.semantics,o.reply_route,o.custody_route,o.custody_store_id,o.digest,o.body_size,o.state,o.created_at,o.updated_at,COALESCE(o.dispatch_started_at,0),COALESCE(o.terminal_at,0),o.error_code,COALESCE(a.owner,''),COALESCE(a.token,''),COALESCE(a.lease_until,0) FROM operations o LEFT JOIN attempts a ON a.operation_id=o.operation_id WHERE o.message_id=?", op.MessageID), &existing)
		if err == nil {
			if existing.OperationID == op.OperationID && existing.Digest == op.Digest && existing.BodySize == op.BodySize && existing.SourceRoute == op.SourceRoute && existing.TargetRoute == op.TargetRoute && existing.Semantics == op.Semantics && existing.ReplyRoute == op.ReplyRoute && existing.CustodyRoute == op.CustodyRoute && existing.CustodyStoreID == op.CustodyStoreID {
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
		_, err = tx.Exec("INSERT INTO operations(operation_id,message_id,source_route,target_route,semantics,reply_route,custody_route,custody_store_id,digest,body_size,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", op.OperationID, op.MessageID, op.SourceRoute, op.TargetRoute, op.Semantics, op.ReplyRoute, op.CustodyRoute, op.CustodyStoreID, op.Digest, op.BodySize, string(StatePrepared), now, now)
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
	return row.Scan(&out.OperationID, &out.MessageID, &out.SourceRoute, &out.TargetRoute, &out.Semantics, &out.ReplyRoute, &out.CustodyRoute, &out.CustodyStoreID, &out.Digest, &out.BodySize, &out.State, &out.CreatedAt, &out.UpdatedAt, &out.DispatchStartedAt, &out.TerminalAt, &out.ErrorCode, &out.AttemptOwner, &out.AttemptToken, &out.AttemptLeaseUntil)
}

func (j *Journal) Operation(ctx context.Context, operationID string) (OperationRecord, error) {
	return j.operation(ctx, operationID, false)
}

func (j *Journal) operation(ctx context.Context, operationID string, includeToken bool) (OperationRecord, error) {
	var out OperationRecord
	err := scanOperation(j.db.QueryRowContext(ctx, "SELECT o.operation_id,o.message_id,o.source_route,o.target_route,o.semantics,o.reply_route,o.custody_route,o.custody_store_id,o.digest,o.body_size,o.state,o.created_at,o.updated_at,COALESCE(o.dispatch_started_at,0),COALESCE(o.terminal_at,0),o.error_code,COALESCE(a.owner,''),COALESCE(a.token,''),COALESCE(a.lease_until,0) FROM operations o LEFT JOIN attempts a ON a.operation_id=o.operation_id WHERE o.operation_id=?", operationID), &out)
	if err == sql.ErrNoRows {
		return out, ErrNotFound
	}
	if !includeToken {
		out.AttemptToken = ""
	}
	return out, err
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
		if _, err := tx.Exec("INSERT OR REPLACE INTO manual_resolutions(operation_id,assertion,actor,reason,evidence_ref,resolved_at) VALUES(?,?,?,?,?,?)", operationID, resolution.Assertion, resolution.Actor, resolution.Reason, resolution.EvidenceRef, now); err != nil {
			return err
		}
		if _, err := tx.Exec("UPDATE attempts SET state=?,updated_at=? WHERE operation_id=?", string(StateManuallyResolved), now, operationID); err != nil {
			return err
		}
		return emit(tx, "operation.manually_resolved", operationID, "", StateManuallyResolved, now)
	})
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
