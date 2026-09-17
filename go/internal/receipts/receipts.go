// Package receipts contains the metadata-only receipt, inspection, and
// reconciliation domain. It deliberately exposes no transport executor and
// never treats a portable claim as endpoint authority.
package receipts

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	mektup "github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/internal/journal"
)

const (
	DefaultLimit = 50
	MaxLimit     = 100
)

var (
	ErrInvalidArguments    = errors.New("receipts: invalid arguments")
	ErrNotFound            = errors.New("receipts: receipt not found")
	ErrAmbiguous           = errors.New("receipts: reference is ambiguous")
	ErrLimit               = errors.New("receipts: query limit exceeds bound")
	ErrUntrusted           = errors.New("receipts: portable receipt is untrusted")
	ErrHumanGateRequired   = errors.New("receipts: explicit human gate is required")
	ErrIdentityMismatch    = errors.New("receipts: exact identity mismatch")
	ErrDigestMismatch      = errors.New("receipts: body digest mismatch")
	ErrContentUnavailable  = errors.New("receipts: content unavailable")
	ErrOutputTooLarge      = errors.New("receipts: content exceeds inline bound")
	ErrReconcileIncomplete = errors.New("receipts: reconciliation incomplete")
	ErrRouteUnavailable    = errors.New("receipts: exact endpoint route unavailable")
	ErrPortableContent     = errors.New("receipts: portable and content views are mutually exclusive")
)

// Journal is the narrow metadata journal seam used by this package. The
// concrete approved implementation is *journal.Journal; tests may provide a
// fake without importing SQL.
type Journal interface {
	PutReceipt(context.Context, mektup.Receipt) error
	Receipt(context.Context, string) (mektup.Receipt, error)
	ListReceipts(context.Context, journal.ReceiptQuery) ([]mektup.Receipt, error)
	ListOperations(context.Context, journal.OperationQuery) ([]journal.OperationRecord, error)
	Operation(context.Context, string) (journal.OperationRecord, error)
	OperationByMessage(context.Context, string) (journal.OperationRecord, error)
	RecordManualResolution(context.Context, string, journal.ManualResolution) error
	Reply(context.Context, string) (journal.ReplyClaim, error)
	RepliesFor(context.Context, string) ([]journal.ReplyClaim, error)
	OriginalStatus(context.Context, string) (journal.OriginalStatusResult, error)
	RecordAccepted(context.Context, string, string) error
	ReconcileReplyObservation(context.Context, string, string, string) error
	UpsertBlocker(context.Context, journal.BlockerObservation) error
	ListBlockers(context.Context, journal.BlockerQuery) ([]journal.Blocker, error)
}

type Store struct {
	Journal Journal
	Now     func() time.Time
}

func (s Store) valid() error {
	if s.Journal == nil {
		return fmt.Errorf("%w: journal is required", ErrInvalidArguments)
	}
	return nil
}

func (s Store) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

type ListOptions struct {
	State      mektup.EvidenceState
	Since      time.Time
	Limit      int
	Cursor     string
	EndpointID string
	ThreadID   string
}

type ListPage struct {
	Receipts   []mektup.Receipt
	NextCursor string
}

type receiptCursor struct {
	Version    int    `json:"v"`
	StoreID    string `json:"store"`
	State      string `json:"state,omitempty"`
	Since      int64  `json:"since,omitempty"`
	EndpointID string `json:"endpointId,omitempty"`
	ThreadID   string `json:"threadId,omitempty"`
	AnchorAt   int64  `json:"anchorAt"`
	AnchorID   string `json:"anchorId"`
	LastAt     int64  `json:"lastAt"`
	LastID     string `json:"lastId"`
}

type storeIdentity interface{ StoreID() string }
type receiptContinuation interface {
	HasReceiptAfter(context.Context, journal.ReceiptQuery) (bool, error)
}

func (s Store) List(ctx context.Context, options ListOptions) ([]mektup.Receipt, error) {
	if err := s.valid(); err != nil {
		return nil, err
	}
	if options.Cursor != "" {
		page, err := s.ListPage(ctx, options)
		return page.Receipts, err
	}
	limit, err := boundedLimit(options.Limit)
	if err != nil {
		return nil, err
	}
	if options.State != "" && !options.State.Valid() {
		return nil, fmt.Errorf("%w: invalid state %q", ErrInvalidArguments, options.State)
	}
	return s.Journal.ListReceipts(ctx, journal.ReceiptQuery{State: options.State, Since: options.Since, EndpointID: options.EndpointID, ThreadID: options.ThreadID, Limit: limit})
}

func (s Store) ListPage(ctx context.Context, options ListOptions) (ListPage, error) {
	if err := s.valid(); err != nil {
		return ListPage{}, err
	}
	limit, err := boundedLimit(options.Limit)
	if err != nil {
		return ListPage{}, err
	}
	if options.State != "" && !options.State.Valid() {
		return ListPage{}, fmt.Errorf("%w: invalid state %q", ErrInvalidArguments, options.State)
	}
	identity, ok := s.Journal.(storeIdentity)
	if !ok || identity.StoreID() == "" {
		return ListPage{}, fmt.Errorf("%w: receipt store identity is unavailable", ErrInvalidArguments)
	}
	query := journal.ReceiptQuery{State: options.State, Since: options.Since, EndpointID: options.EndpointID, ThreadID: options.ThreadID, Limit: limit}
	var cursor receiptCursor
	if options.Cursor != "" {
		cursor, err = decodeReceiptCursor(options.Cursor)
		if err != nil || cursor.Version != 1 || cursor.StoreID != identity.StoreID() || cursor.State != string(options.State) || cursor.Since != unixNano(options.Since) || cursor.EndpointID != options.EndpointID || cursor.ThreadID != options.ThreadID || cursor.AnchorAt <= 0 || cursor.AnchorID == "" || cursor.LastAt <= 0 || cursor.LastID == "" {
			return ListPage{}, fmt.Errorf("%w: receipt cursor does not match this store and query", ErrInvalidArguments)
		}
		query.AnchorAt, query.AnchorID = time.Unix(0, cursor.AnchorAt).UTC(), cursor.AnchorID
		query.BeforeAt, query.BeforeID = time.Unix(0, cursor.LastAt).UTC(), cursor.LastID
	}
	items, err := s.Journal.ListReceipts(ctx, query)
	if err != nil {
		return ListPage{}, err
	}
	hasMore := false
	if len(items) == limit && len(items) != 0 {
		lastAt, parseErr := receiptCreatedAt(items[len(items)-1].CreatedAt)
		if parseErr != nil {
			return ListPage{}, parseErr
		}
		continuation, ok := s.Journal.(receiptContinuation)
		if !ok {
			return ListPage{}, fmt.Errorf("%w: receipt continuation query is unavailable", ErrInvalidArguments)
		}
		probe := query
		probe.BeforeAt, probe.BeforeID = lastAt, items[len(items)-1].ReceiptID
		hasMore, err = continuation.HasReceiptAfter(ctx, probe)
		if err != nil {
			return ListPage{}, err
		}
	}
	page := ListPage{Receipts: items}
	if !hasMore || len(items) == 0 {
		return page, nil
	}
	firstAt, err := receiptCreatedAt(items[0].CreatedAt)
	if err != nil {
		return ListPage{}, err
	}
	if options.Cursor == "" {
		cursor = receiptCursor{Version: 1, StoreID: identity.StoreID(), State: string(options.State), Since: unixNano(options.Since), EndpointID: options.EndpointID, ThreadID: options.ThreadID, AnchorAt: firstAt.UnixNano(), AnchorID: items[0].ReceiptID}
	}
	lastAt, err := receiptCreatedAt(items[len(items)-1].CreatedAt)
	if err != nil {
		return ListPage{}, err
	}
	cursor.LastAt, cursor.LastID = lastAt.UnixNano(), items[len(items)-1].ReceiptID
	page.NextCursor, err = encodeReceiptCursor(cursor)
	if err != nil {
		return ListPage{}, err
	}
	return page, nil
}

func encodeReceiptCursor(cursor receiptCursor) (string, error) {
	document, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	return "rc1." + base64.RawURLEncoding.EncodeToString(document), nil
}

func decodeReceiptCursor(value string) (receiptCursor, error) {
	var cursor receiptCursor
	if len(value) > 8192 {
		return cursor, ErrInvalidArguments
	}
	encoded, ok := strings.CutPrefix(value, "rc1.")
	if !ok {
		return cursor, ErrInvalidArguments
	}
	document, err := base64.RawURLEncoding.DecodeString(encoded)
	if err != nil || len(document) > 4096 {
		return cursor, ErrInvalidArguments
	}
	if err := json.Unmarshal(document, &cursor); err != nil {
		return cursor, ErrInvalidArguments
	}
	return cursor, nil
}

func unixNano(value time.Time) int64 {
	if value.IsZero() {
		return 0
	}
	return value.UTC().UnixNano()
}

func receiptCreatedAt(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("%w: invalid receipt createdAt", ErrInvalidArguments)
	}
	return parsed.UTC(), nil
}

type ShowOptions struct {
	Portable bool
	Content  bool
}

func (s Store) Show(ctx context.Context, reference string, options ShowOptions) (mektup.Receipt, error) {
	if err := s.valid(); err != nil {
		return mektup.Receipt{}, err
	}
	if reference == "" {
		return mektup.Receipt{}, fmt.Errorf("%w: receipt reference is required", ErrInvalidArguments)
	}
	if options.Portable && options.Content {
		return mektup.Receipt{}, ErrPortableContent
	}
	receipt, err := s.Journal.Receipt(ctx, reference)
	if errors.Is(err, journal.ErrNotFound) {
		return mektup.Receipt{}, ErrNotFound
	}
	if errors.Is(err, journal.ErrReceiptConflict) {
		return mektup.Receipt{}, ErrAmbiguous
	}
	if err != nil {
		return mektup.Receipt{}, err
	}
	if options.Portable {
		return PortableProjection(receipt)
	}
	return receipt, nil
}

// PortableProjection normalizes a durable receipt through the public
// metadata-only wire contract. This is deliberately a projection operation,
// rather than a trust operation: callers still have to verify the resulting
// identity against their independently established route and custody.
func PortableProjection(receipt mektup.Receipt) (mektup.Receipt, error) {
	document, err := json.Marshal(receipt)
	if err != nil {
		return mektup.Receipt{}, err
	}
	projected, err := mektup.ParseReceipt(document)
	if err != nil {
		return mektup.Receipt{}, err
	}
	return projected, nil
}

// Import validates only the portable shape. It does not persist the claim,
// register an endpoint, select custody, or alter any observed journal state.
type Imported struct {
	Receipt mektup.Receipt
	Trusted bool
}

func Import(data []byte) (Imported, error) {
	if containsBodyKey(data) {
		return Imported{}, ErrUntrusted
	}
	receipt, err := mektup.ParseReceipt(data)
	if err != nil {
		return Imported{}, fmt.Errorf("%w: %v", ErrUntrusted, err)
	}
	return Imported{Receipt: receipt, Trusted: false}, nil
}

func containsBodyKey(data []byte) bool {
	var value any
	if json.Unmarshal(data, &value) != nil {
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

func (s Store) Save(ctx context.Context, receipt mektup.Receipt) error {
	if err := s.valid(); err != nil {
		return err
	}
	return s.Journal.PutReceipt(ctx, receipt)
}

type ResolveIntent struct {
	OperationID  string
	Assertion    string
	Actor        string
	Reason       string
	EvidenceRef  string
	Presentation string
	Intent       string
}

// HumanGate is deliberately an intent/token seam. A caller or CLI owns the
// actual prompt/policy; this package only requires that it authorize this
// exact operation before writing a manual assertion.
type HumanGate interface {
	Authorize(context.Context, ResolveIntent) (string, error)
}

type ResolveRequest struct {
	Reference    string
	Assertion    string
	Actor        string
	Reason       string
	EvidenceRef  string
	Presentation string
	Intent       string
	Gate         HumanGate
}

func (s Store) Resolve(ctx context.Context, request ResolveRequest) (mektup.Receipt, error) {
	if err := s.valid(); err != nil {
		return mektup.Receipt{}, err
	}
	if request.Reference == "" || request.Actor == "" || request.Reason == "" || request.EvidenceRef == "" || request.Intent != "receipt.resolve" || request.Gate == nil {
		return mektup.Receipt{}, ErrHumanGateRequired
	}
	if request.Assertion != "accepted" && request.Assertion != "not_delivered" {
		return mektup.Receipt{}, fmt.Errorf("%w: unsupported assertion", ErrInvalidArguments)
	}
	if request.Presentation != "human" && request.Presentation != "agent" {
		return mektup.Receipt{}, fmt.Errorf("%w: unsupported presentation", ErrInvalidArguments)
	}
	receipt, err := s.Show(ctx, request.Reference, ShowOptions{})
	if err != nil {
		return mektup.Receipt{}, err
	}
	intent := ResolveIntent{OperationID: receipt.OperationID, Assertion: request.Assertion, Actor: request.Actor, Reason: request.Reason, EvidenceRef: request.EvidenceRef, Presentation: request.Presentation, Intent: request.Intent}
	token, err := request.Gate.Authorize(ctx, intent)
	if err != nil || token == "" {
		if err != nil {
			return mektup.Receipt{}, fmt.Errorf("%w: %v", ErrHumanGateRequired, err)
		}
		return mektup.Receipt{}, ErrHumanGateRequired
	}
	op, opErr := s.Journal.Operation(ctx, receipt.OperationID)
	if opErr != nil {
		return mektup.Receipt{}, opErr
	}
	// Recover a crash between the projection-first write and the journal
	// transition. The stored assertion is retained and replayed; no new
	// external effect or route resolution is introduced.
	if receipt.State == mektup.StateManuallyResolved && op.State == journal.StateOutcomeUnknown && receipt.ManualResolution != nil {
		resolution := receipt.ManualResolution
		if err := s.Journal.RecordManualResolution(ctx, receipt.OperationID, journal.ManualResolution{Assertion: resolution.Assertion, Actor: resolution.Actor, Reason: resolution.Reason, EvidenceRef: resolution.EvidenceRef, Presentation: resolution.Presentation, Timestamp: parseResolutionTime(resolution.Timestamp)}); err != nil {
			return mektup.Receipt{}, err
		}
		return receipt, nil
	}
	if receipt.State == mektup.StateOutcomeUnknown && op.State == journal.StateManuallyResolved && op.ManualResolution != nil {
		receipt.State = mektup.StateManuallyResolved
		receipt.UpdatedAt = s.now().Format(time.RFC3339Nano)
		receipt.ManualResolution = &mektup.ManualResolution{Assertion: op.ManualResolution.Assertion, Actor: op.ManualResolution.Actor, Reason: op.ManualResolution.Reason, EvidenceRef: op.ManualResolution.EvidenceRef, Presentation: op.ManualResolution.Presentation, Timestamp: op.ManualResolution.ResolvedAt.Format(time.RFC3339Nano)}
		if err := s.Journal.PutReceipt(ctx, receipt); err != nil {
			return mektup.Receipt{}, err
		}
		return receipt, nil
	}
	if receipt.State != mektup.StateOutcomeUnknown {
		return mektup.Receipt{}, fmt.Errorf("%w: receipt is not outcome-unknown", journal.ErrInvalidTransition)
	}
	now := s.now()
	previous := receipt
	receipt.State = mektup.StateManuallyResolved
	receipt.UpdatedAt = now.Format(time.RFC3339Nano)
	receipt.ManualResolution = &mektup.ManualResolution{Assertion: request.Assertion, Actor: request.Actor, Reason: request.Reason, EvidenceRef: request.EvidenceRef, Presentation: request.Presentation, Timestamp: now.Format(time.RFC3339Nano)}
	resolution := journal.ManualResolution{Assertion: request.Assertion, Actor: request.Actor, Reason: request.Reason, EvidenceRef: request.EvidenceRef, Presentation: request.Presentation, Timestamp: now}
	if concrete, ok := s.Journal.(*journal.Journal); ok {
		return concrete.ResolveWithReceipt(ctx, receipt.OperationID, resolution, receipt)
	}
	// The journal's transition API predates receipt projections. Write the
	// projection first so an injected/failed receipt write leaves the operation
	// outcome-unknown and retryable. If the transition then fails, restore the
	// previous projection. The concrete SQLite journal keeps each write fenced;
	// callers never observe a successful resolve without both records.
	if err := s.Journal.PutReceipt(ctx, receipt); err != nil {
		return mektup.Receipt{}, err
	}
	if err := s.Journal.RecordManualResolution(ctx, receipt.OperationID, resolution); err != nil {
		latestOp, latestOpErr := s.Journal.Operation(ctx, receipt.OperationID)
		latestReceipt, latestReceiptErr := s.Show(ctx, receipt.ReceiptID, ShowOptions{})
		if latestOpErr == nil && latestReceiptErr == nil && latestOp.State == journal.StateManuallyResolved && latestReceipt.State == mektup.StateManuallyResolved {
			return latestReceipt, nil
		}
		if rollbackErr := s.Journal.PutReceipt(ctx, previous); rollbackErr != nil {
			return mektup.Receipt{}, fmt.Errorf("%w: projection rollback failed: %v (transition: %v)", ErrReconcileIncomplete, rollbackErr, err)
		}
		return mektup.Receipt{}, err
	}
	return receipt, nil
}

func boundedLimit(limit int) (int, error) {
	if limit < 0 || limit > MaxLimit {
		return 0, ErrLimit
	}
	if limit == 0 {
		return DefaultLimit, nil
	}
	return limit, nil
}

func bodyDigest(body []byte) string {
	sum := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func parseResolutionTime(value string) time.Time {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}
	}
	return parsed
}
