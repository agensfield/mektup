package journal

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
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
	AnchorAt   time.Time
	AnchorID   string
	BeforeAt   time.Time
	BeforeID   string
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
	return j.withTx(ctx, func(tx *sql.Tx) error { return j.putReceiptTx(ctx, tx, receipt) })
}

type receiptStore interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (j *Journal) putReceiptTx(ctx context.Context, execer receiptStore, receipt mektup.Receipt) error {
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
	var existingDocument string
	err = execer.QueryRowContext(ctx, `SELECT document FROM receipts WHERE receipt_id=?`, receipt.ReceiptID).Scan(&existingDocument)
	if err == nil {
		existing, parseErr := parseStoredReceipt(existingDocument)
		if parseErr != nil {
			return parseErr
		}
		merged, mergeErr := mergeReceiptProjection(existing, receipt)
		if mergeErr != nil {
			return mergeErr
		}
		receipt = merged
		document, err = json.Marshal(receipt)
		if err != nil {
			return fmt.Errorf("journal: marshal receipt: %w", err)
		}
		created, err = parseReceiptTime(receipt.CreatedAt)
		if err != nil {
			return err
		}
		updated, err = parseReceiptTime(receipt.UpdatedAt)
		if err != nil {
			return err
		}
	} else if err != sql.ErrNoRows {
		return err
	}
	_, err = execer.ExecContext(ctx, `INSERT INTO receipts(receipt_id,operation_id,message_id,state,created_at,updated_at,source_endpoint_id,source_thread_id,target_endpoint_id,target_thread_id,document)
VALUES(?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(receipt_id) DO UPDATE SET operation_id=excluded.operation_id,message_id=excluded.message_id,state=excluded.state,created_at=excluded.created_at,updated_at=excluded.updated_at,source_endpoint_id=excluded.source_endpoint_id,source_thread_id=excluded.source_thread_id,target_endpoint_id=excluded.target_endpoint_id,target_thread_id=excluded.target_thread_id,document=excluded.document`,
		receipt.ReceiptID, receipt.OperationID, receipt.Message.MessageID, string(receipt.State), created.UnixNano(), updated.UnixNano(), receipt.Source.EndpointID, receipt.Source.ThreadID, receipt.Target.EndpointID, receipt.Target.ThreadID, string(document))
	return err
}

func mergeReceiptProjection(existing, incoming mektup.Receipt) (mektup.Receipt, error) {
	if existing.Schema != incoming.Schema || existing.ReceiptID != incoming.ReceiptID || existing.OperationID != incoming.OperationID || existing.Operation != incoming.Operation || existing.Message.MessageID != incoming.Message.MessageID || existing.Message.InReplyTo != incoming.Message.InReplyTo || existing.Message.Kind != incoming.Message.Kind || existing.Message.ReplyRequested != incoming.Message.ReplyRequested || existing.Message.ClientMessageID != incoming.Message.ClientMessageID || existing.Message.PayloadBytes != incoming.Message.PayloadBytes || existing.Message.PayloadSHA256 != incoming.Message.PayloadSHA256 || existing.Source.EndpointID != incoming.Source.EndpointID || existing.Source.ThreadID != incoming.Source.ThreadID || existing.Source.Resolved != incoming.Source.Resolved || existing.Target.EndpointID != incoming.Target.EndpointID || existing.Target.ThreadID != incoming.Target.ThreadID || existing.Target.Resolved != incoming.Target.Resolved || existing.CreatedAt != incoming.CreatedAt {
		return mektup.Receipt{}, ErrIdentityConflict
	}
	mergedRef, err := mergeContentRef(existing.ContentRef, incoming.ContentRef)
	if err != nil {
		return mektup.Receipt{}, err
	}
	incoming.ContentRef = mergedRef
	incoming.Warnings = mergeWarnings(existing.Warnings, incoming.Warnings)
	if incoming.ManualResolution == nil {
		incoming.ManualResolution = existing.ManualResolution
	}
	for _, prior := range existing.Evidence {
		found := false
		for _, next := range incoming.Evidence {
			if reflect.DeepEqual(prior, next) {
				found = true
				break
			}
		}
		if !found {
			incoming.Evidence = append([]mektup.EvidenceRecord{prior}, incoming.Evidence...)
		}
	}
	return incoming, nil
}

func mergeContentRef(existing, incoming *mektup.ContentRef) (*mektup.ContentRef, error) {
	if existing == nil {
		return incoming, nil
	}
	if incoming == nil {
		copy := *existing
		return &copy, nil
	}
	if existing.EndpointID != incoming.EndpointID || existing.ThreadID != incoming.ThreadID || existing.PayloadBytes != incoming.PayloadBytes || existing.PayloadSHA256 != incoming.PayloadSHA256 {
		return nil, ErrIdentityConflict
	}
	merged := *existing
	if incoming.TurnID != "" {
		if merged.TurnID != "" && merged.TurnID != incoming.TurnID {
			return nil, ErrIdentityConflict
		}
		merged.TurnID = incoming.TurnID
	}
	if incoming.ItemID != "" {
		if merged.ItemID != "" && merged.ItemID != incoming.ItemID {
			return nil, ErrIdentityConflict
		}
		merged.ItemID = incoming.ItemID
	}
	if incoming.ClientMessageID != "" {
		if merged.ClientMessageID != "" && merged.ClientMessageID != incoming.ClientMessageID {
			return nil, ErrIdentityConflict
		}
		merged.ClientMessageID = incoming.ClientMessageID
	}
	return &merged, nil
}

func mergeWarnings(existing, incoming []mektup.Warning) []mektup.Warning {
	merged := append([]mektup.Warning(nil), existing...)
	for _, warning := range incoming {
		found := false
		for _, prior := range merged {
			if reflect.DeepEqual(prior, warning) {
				found = true
				break
			}
		}
		if !found {
			merged = append(merged, warning)
		}
	}
	return merged
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
	where, args, err := receiptPredicates(query)
	if err != nil {
		return nil, err
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

// HasReceiptAfter checks continuation without fetching and discarding a row
// beyond the caller's exact page limit.
func (j *Journal) HasReceiptAfter(ctx context.Context, query ReceiptQuery) (bool, error) {
	where, args, err := receiptPredicates(query)
	if err != nil {
		return false, err
	}
	var exists int
	err = j.db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM receipts WHERE `+strings.Join(where, " AND ")+`)`, args...).Scan(&exists)
	return exists != 0, err
}

func receiptPredicates(query ReceiptQuery) ([]string, []any, error) {
	where := []string{"1=1"}
	args := make([]any, 0, 12)
	if query.State != "" {
		if !query.State.Valid() {
			return nil, nil, fmt.Errorf("journal: invalid receipt state %q", query.State)
		}
		where = append(where, "state=?")
		args = append(args, string(query.State))
	}
	if !query.Since.IsZero() {
		where = append(where, "created_at>=?")
		args = append(args, query.Since.UTC().UnixNano())
	}
	if query.EndpointID != "" && query.ThreadID != "" {
		where = append(where, "((source_endpoint_id=? AND source_thread_id=?) OR (target_endpoint_id=? AND target_thread_id=?))")
		args = append(args, query.EndpointID, query.ThreadID, query.EndpointID, query.ThreadID)
	} else if query.EndpointID != "" {
		where = append(where, "(source_endpoint_id=? OR target_endpoint_id=?)")
		args = append(args, query.EndpointID, query.EndpointID)
	} else if query.ThreadID != "" {
		where = append(where, "(source_thread_id=? OR target_thread_id=?)")
		args = append(args, query.ThreadID, query.ThreadID)
	}
	if !query.AnchorAt.IsZero() {
		where = append(where, "(created_at<? OR (created_at=? AND receipt_id<=?))")
		anchor := query.AnchorAt.UTC().UnixNano()
		args = append(args, anchor, anchor, query.AnchorID)
	}
	if !query.BeforeAt.IsZero() {
		where = append(where, "(created_at<? OR (created_at=? AND receipt_id<?))")
		before := query.BeforeAt.UTC().UnixNano()
		args = append(args, before, before, query.BeforeID)
	}
	return where, args, nil
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
