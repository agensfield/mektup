package journal

import (
	"context"
	"database/sql"
	"fmt"
)

// Operation describes metadata persisted before a request can be dispatched.
type Operation struct {
	OperationID string
	MessageID   string
	SourceRoute string
	TargetRoute string
	Semantics   string
	Digest      string
	BodySize    int64
}

type OperationRecord struct {
	Operation
	State             EvidenceState
	CreatedAt         int64
	UpdatedAt         int64
	DispatchStartedAt int64
	TerminalAt        int64
	ErrorCode         string
}

// Prepare durably establishes the original relationship. It must be called
// before any transport can write a request. Bodies are intentionally absent.
func (j *Journal) Prepare(ctx context.Context, op Operation) (OperationRecord, error) {
	if op.OperationID == "" || op.MessageID == "" || op.SourceRoute == "" || op.TargetRoute == "" || op.Digest == "" || op.BodySize < 0 {
		return OperationRecord{}, fmt.Errorf("journal: invalid operation metadata")
	}
	now := j.nowUnix()
	err := j.withTx(ctx, func(tx *sql.Tx) error {
		var existing OperationRecord
		err := scanOperation(tx.QueryRow("SELECT operation_id,message_id,source_route,target_route,semantics,digest,body_size,state,created_at,updated_at,COALESCE(dispatch_started_at,0),COALESCE(terminal_at,0),error_code FROM operations WHERE message_id=?", op.MessageID), &existing)
		if err == nil {
			if existing.OperationID == op.OperationID && existing.Digest == op.Digest && existing.BodySize == op.BodySize && existing.SourceRoute == op.SourceRoute && existing.TargetRoute == op.TargetRoute && existing.Semantics == op.Semantics {
				return nil
			}
			return ErrIdentityConflict
		}
		if err != sql.ErrNoRows {
			return err
		}
		_, err = tx.Exec("INSERT INTO operations(operation_id,message_id,source_route,target_route,semantics,digest,body_size,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)", op.OperationID, op.MessageID, op.SourceRoute, op.TargetRoute, op.Semantics, op.Digest, op.BodySize, string(StatePrepared), now, now)
		if err != nil {
			return err
		}
		if _, err = tx.Exec("INSERT INTO attempts(operation_id,state,created_at,updated_at) VALUES(?,?,?,?)", op.OperationID, string(StatePrepared), now, now); err != nil {
			return err
		}
		return emit(tx, "operation.prepared", op.OperationID, "", StatePrepared, now)
	})
	if err != nil {
		return OperationRecord{}, err
	}
	return j.Operation(ctx, op.OperationID)
}

func scanOperation(row interface{ Scan(...any) error }, out *OperationRecord) error {
	return row.Scan(&out.OperationID, &out.MessageID, &out.SourceRoute, &out.TargetRoute, &out.Semantics, &out.Digest, &out.BodySize, &out.State, &out.CreatedAt, &out.UpdatedAt, &out.DispatchStartedAt, &out.TerminalAt, &out.ErrorCode)
}

func (j *Journal) Operation(ctx context.Context, operationID string) (OperationRecord, error) {
	var out OperationRecord
	err := scanOperation(j.db.QueryRowContext(ctx, "SELECT operation_id,message_id,source_route,target_route,semantics,digest,body_size,state,created_at,updated_at,COALESCE(dispatch_started_at,0),COALESCE(terminal_at,0),error_code FROM operations WHERE operation_id=?", operationID), &out)
	if err == sql.ErrNoRows {
		return out, ErrNotFound
	}
	return out, err
}

// MarkDispatchStarted commits the dispatch fence before handing bytes to a
// transport. An orphaned dispatch-started operation is always unknown.
func (j *Journal) MarkDispatchStarted(ctx context.Context, operationID string) error {
	now := j.nowUnix()
	return j.withTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.Exec("UPDATE operations SET state=?,dispatch_started_at=?,updated_at=? WHERE operation_id=? AND state=?", string(StateDispatchStarted), now, now, operationID, string(StatePrepared))
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			return ErrInvalidTransition
		}
		if _, err = tx.Exec("UPDATE attempts SET state=?,updated_at=? WHERE operation_id=?", string(StateDispatchStarted), now, operationID); err != nil {
			return err
		}
		return emit(tx, "operation.dispatch_started", operationID, "", StateDispatchStarted, now)
	})
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
		valid := current == StateDispatchStarted || (current == StateOutcomeUnknown && (state == StateAccepted || state == StateRejected)) || (current == StatePrepared && state == StateNotSent)
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

// RecoverOrphans is conservative and idempotent. Prepared intent is proven
// not sent; a dispatch fence means bytes may have left the process.
func (j *Journal) RecoverOrphans(ctx context.Context) error {
	now := j.nowUnix()
	return j.withTx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.Query("SELECT operation_id,state FROM operations WHERE state IN (?,?)", string(StatePrepared), string(StateDispatchStarted))
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
