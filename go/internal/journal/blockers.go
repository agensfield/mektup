package journal

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

const MaxBlockerQueryLimit = 100

// Blocker is the metadata projection of one distinct server request. It
// intentionally has no command, question, permission, or tool payload.
type Blocker struct {
	Method        string
	CorrelationID string
	Generation    string
	FirstSeen     time.Time
	LastSeen      time.Time
	ResolvedAt    *time.Time
	EndpointID    string
	ThreadID      string
	TurnID        string
	ItemID        string
	OperationID   string
	MessageID     string
}

type BlockerObservation struct {
	Method        string
	CorrelationID string
	Generation    string
	SeenAt        time.Time
	ResolvedAt    *time.Time
	EndpointID    string
	ThreadID      string
	TurnID        string
	ItemID        string
	OperationID   string
	MessageID     string
}

type BlockerQuery struct {
	EndpointID, Generation, ThreadID, OperationID string
	Limit                                         int
}

func (q BlockerQuery) limit() (int, error) {
	if q.Limit < 0 || q.Limit > MaxBlockerQueryLimit {
		return 0, fmt.Errorf("journal: blocker limit must be between 0 and %d", MaxBlockerQueryLimit)
	}
	if q.Limit == 0 {
		return 50, nil
	}
	return q.Limit, nil
}

// UpsertBlocker updates the last-seen metadata for one correlation key. It
// deliberately emits no journal event, so polling does not append history.
func (j *Journal) UpsertBlocker(ctx context.Context, observation BlockerObservation) error {
	if strings.TrimSpace(observation.Method) == "" || strings.TrimSpace(observation.CorrelationID) == "" {
		return fmt.Errorf("journal: blocker method and correlation ID are required")
	}
	seen := observation.SeenAt
	if seen.IsZero() {
		seen = j.now().UTC()
	} else {
		seen = seen.UTC()
	}
	var resolved any
	if observation.ResolvedAt != nil {
		resolved = observation.ResolvedAt.UTC().UnixNano()
	}
	_, err := j.db.ExecContext(ctx, `INSERT INTO blockers(method,correlation_id,generation,first_seen,last_seen,resolved_at,endpoint_id,thread_id,turn_id,item_id,operation_id,message_id)
VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(endpoint_id,generation,method,correlation_id) DO UPDATE SET
 last_seen=CASE WHEN excluded.last_seen>blockers.last_seen THEN excluded.last_seen ELSE blockers.last_seen END,
 resolved_at=COALESCE(excluded.resolved_at,blockers.resolved_at),
 endpoint_id=CASE WHEN excluded.endpoint_id<>'' THEN excluded.endpoint_id ELSE blockers.endpoint_id END,
 thread_id=CASE WHEN excluded.thread_id<>'' THEN excluded.thread_id ELSE blockers.thread_id END,
 turn_id=CASE WHEN excluded.turn_id<>'' THEN excluded.turn_id ELSE blockers.turn_id END,
 item_id=CASE WHEN excluded.item_id<>'' THEN excluded.item_id ELSE blockers.item_id END,
 operation_id=CASE WHEN excluded.operation_id<>'' THEN excluded.operation_id ELSE blockers.operation_id END,
 message_id=CASE WHEN excluded.message_id<>'' THEN excluded.message_id ELSE blockers.message_id END`,
		observation.Method, observation.CorrelationID, observation.Generation, seen.UnixNano(), seen.UnixNano(), resolved,
		observation.EndpointID, observation.ThreadID, observation.TurnID, observation.ItemID, observation.OperationID, observation.MessageID)
	return err
}

// ResolveBlocker marks the matching connection-generation request resolved.
// The app-server resolved notification carries no method, so request IDs must
// be unique within one connection generation and endpoint. No request payload
// is accepted or persisted here.
func (j *Journal) ResolveBlocker(ctx context.Context, endpointID, generation, correlationID string, resolvedAt time.Time) error {
	if strings.TrimSpace(endpointID) == "" || strings.TrimSpace(generation) == "" || strings.TrimSpace(correlationID) == "" {
		return fmt.Errorf("journal: blocker endpoint, generation, and correlation ID are required")
	}
	if resolvedAt.IsZero() {
		resolvedAt = j.now().UTC()
	} else {
		resolvedAt = resolvedAt.UTC()
	}
	_, err := j.db.ExecContext(ctx, `UPDATE blockers SET resolved_at=COALESCE(resolved_at,?) WHERE endpoint_id=? AND generation=? AND correlation_id=?`, resolvedAt.UnixNano(), endpointID, generation, correlationID)
	return err
}

func (j *Journal) ListBlockers(ctx context.Context, query BlockerQuery) ([]Blocker, error) {
	limit, err := query.limit()
	if err != nil {
		return nil, err
	}
	where := []string{"1=1"}
	args := make([]any, 0, 4)
	if query.EndpointID != "" {
		where = append(where, "endpoint_id=?")
		args = append(args, query.EndpointID)
	}
	if query.Generation != "" {
		where = append(where, "generation=?")
		args = append(args, query.Generation)
	}
	if query.ThreadID != "" {
		where = append(where, "thread_id=?")
		args = append(args, query.ThreadID)
	}
	if query.OperationID != "" {
		where = append(where, "operation_id=?")
		args = append(args, query.OperationID)
	}
	args = append(args, limit)
	rows, err := j.db.QueryContext(ctx, `SELECT method,correlation_id,generation,first_seen,last_seen,resolved_at,endpoint_id,thread_id,turn_id,item_id,operation_id,message_id FROM blockers WHERE `+strings.Join(where, " AND ")+` ORDER BY last_seen DESC, method, correlation_id LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Blocker
	for rows.Next() {
		var b Blocker
		var first, last int64
		var resolved sql.NullInt64
		if err := rows.Scan(&b.Method, &b.CorrelationID, &b.Generation, &first, &last, &resolved, &b.EndpointID, &b.ThreadID, &b.TurnID, &b.ItemID, &b.OperationID, &b.MessageID); err != nil {
			return nil, err
		}
		b.FirstSeen = time.Unix(0, first).UTC()
		b.LastSeen = time.Unix(0, last).UTC()
		if resolved.Valid {
			at := time.Unix(0, resolved.Int64).UTC()
			b.ResolvedAt = &at
		}
		out = append(out, b)
	}
	return out, rows.Err()
}
