package journal

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	mektup "github.com/agensfield/mektup/go"
)

const MaxReceiptQueryLimit = 100

var (
	ErrReceiptBody     = errors.New("journal: receipt contains body content")
	ErrReceiptConflict = errors.New("journal: receipt reference is ambiguous")
)

// ReceiptQuery bounds metadata-only journal reads. Since applies to creation
// time. A zero limit uses the package default; values over the hard bound fail
// instead of silently producing an incomplete answer.
type ReceiptQuery struct {
	State      mektup.EvidenceState
	Since      time.Time
	EndpointID string
	ThreadID   string
	Limit      int
}

func (q ReceiptQuery) limit() (int, error) {
	if q.Limit < 0 || q.Limit > MaxReceiptQueryLimit {
		return 0, fmt.Errorf("journal: receipt limit must be between 0 and %d", MaxReceiptQueryLimit)
	}
	if q.Limit == 0 {
		return 50, nil
	}
	return q.Limit, nil
}

// PutReceipt stores only the validated, bodyless portable representation.
// Receipt.MarshalJSON deliberately has no body field; this additional shape
// check protects the journal if additive evidence maps are supplied by a
// future caller.
func (j *Journal) PutReceipt(ctx context.Context, receipt mektup.Receipt) error {
	if err := receipt.Validate(); err != nil {
		return fmt.Errorf("journal: invalid receipt: %w", err)
	}
	document, err := json.Marshal(receipt)
	if err != nil {
		return fmt.Errorf("journal: marshal receipt: %w", err)
	}
	if containsBodyKey(document) {
		return ErrReceiptBody
	}
	created, err := parseReceiptTime(receipt.CreatedAt)
	if err != nil {
		return err
	}
	updated, err := parseReceiptTime(receipt.UpdatedAt)
	if err != nil {
		return err
	}
	_, err = j.db.ExecContext(ctx, `INSERT INTO receipts(receipt_id,operation_id,message_id,state,created_at,updated_at,source_endpoint_id,source_thread_id,target_endpoint_id,target_thread_id,document)
VALUES(?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(receipt_id) DO UPDATE SET operation_id=excluded.operation_id,message_id=excluded.message_id,state=excluded.state,created_at=excluded.created_at,updated_at=excluded.updated_at,source_endpoint_id=excluded.source_endpoint_id,source_thread_id=excluded.source_thread_id,target_endpoint_id=excluded.target_endpoint_id,target_thread_id=excluded.target_thread_id,document=excluded.document`,
		receipt.ReceiptID, receipt.OperationID, receipt.Message.MessageID, string(receipt.State), created.UnixNano(), updated.UnixNano(), receipt.Source.EndpointID, receipt.Source.ThreadID, receipt.Target.EndpointID, receipt.Target.ThreadID, string(document))
	return err
}

func (j *Journal) Receipt(ctx context.Context, reference string) (mektup.Receipt, error) {
	var exact string
	err := j.db.QueryRowContext(ctx, `SELECT document FROM receipts WHERE receipt_id=?`, reference).Scan(&exact)
	if err == nil {
		return parseStoredReceipt(exact)
	}
	if err != sql.ErrNoRows {
		return mektup.Receipt{}, err
	}
	rows, err := j.db.QueryContext(ctx, `SELECT document FROM receipts WHERE operation_id=? OR message_id=? ORDER BY updated_at DESC, receipt_id DESC`, reference, reference)
	if err != nil {
		return mektup.Receipt{}, err
	}
	defer rows.Close()
	var out mektup.Receipt
	count := 0
	for rows.Next() {
		var document string
		if err := rows.Scan(&document); err != nil {
			return mektup.Receipt{}, err
		}
		receipt, err := parseStoredReceipt(document)
		if err != nil {
			return mektup.Receipt{}, err
		}
		if count == 0 {
			out = receipt
		} else if receipt.ReceiptID != out.ReceiptID {
			return mektup.Receipt{}, ErrReceiptConflict
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return mektup.Receipt{}, err
	}
	if count == 0 {
		return mektup.Receipt{}, ErrNotFound
	}
	return out, nil
}

func parseStoredReceipt(document string) (mektup.Receipt, error) {
	receipt, err := mektup.ParseReceipt([]byte(document))
	if err != nil {
		return mektup.Receipt{}, fmt.Errorf("%w: invalid stored receipt: %v", ErrCorrupt, err)
	}
	return receipt, nil
}

func (j *Journal) ListReceipts(ctx context.Context, query ReceiptQuery) ([]mektup.Receipt, error) {
	limit, err := query.limit()
	if err != nil {
		return nil, err
	}
	where := []string{"1=1"}
	args := make([]any, 0, 3)
	if query.State != "" {
		if !query.State.Valid() {
			return nil, fmt.Errorf("journal: invalid receipt state %q", query.State)
		}
		where = append(where, "state=?")
		args = append(args, string(query.State))
	}
	if !query.Since.IsZero() {
		where = append(where, "created_at>=?")
		args = append(args, query.Since.UTC().UnixNano())
	}
	if query.EndpointID != "" {
		where = append(where, "(source_endpoint_id=? OR target_endpoint_id=?)")
		args = append(args, query.EndpointID, query.EndpointID)
	}
	if query.ThreadID != "" {
		where = append(where, "(source_thread_id=? OR target_thread_id=?)")
		args = append(args, query.ThreadID, query.ThreadID)
	}
	args = append(args, limit)
	rows, err := j.db.QueryContext(ctx, `SELECT document FROM receipts WHERE `+strings.Join(where, " AND ")+` ORDER BY created_at DESC, receipt_id DESC LIMIT ?`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []mektup.Receipt
	for rows.Next() {
		var document string
		if err := rows.Scan(&document); err != nil {
			return nil, err
		}
		receipt, err := parseStoredReceipt(document)
		if err != nil {
			return nil, err
		}
		out = append(out, receipt)
	}
	return out, rows.Err()
}

func parseReceiptTime(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("journal: invalid receipt timestamp: %w", err)
	}
	return parsed.UTC(), nil
}

func containsBodyKey(document []byte) bool {
	var value any
	if json.Unmarshal(document, &value) != nil {
		return true
	}
	var walk func(any) bool
	walk = func(current any) bool {
		switch typed := current.(type) {
		case map[string]any:
			for key, child := range typed {
				switch strings.ToLower(key) {
				case "body", "bodytext", "replybody", "payload", "payloadtext":
					return true
				}
				if walk(child) {
					return true
				}
			}
		case []any:
			for _, child := range typed {
				if walk(child) {
					return true
				}
			}
		}
		return false
	}
	return walk(value)
}
