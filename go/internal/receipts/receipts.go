// Package receipts contains the metadata-only receipt, inspection, and
// reconciliation domain. It deliberately exposes no transport executor and
// never treats a portable claim as endpoint authority.
package receipts

import (
	"context"
	"crypto/sha256"
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
	State mektup.EvidenceState
	Since time.Time
	Limit int
}

func (s Store) List(ctx context.Context, options ListOptions) ([]mektup.Receipt, error) {
	if err := s.valid(); err != nil {
		return nil, err
	}
	limit, err := boundedLimit(options.Limit)
	if err != nil {
		return nil, err
	}
	if options.State != "" && !options.State.Valid() {
		return nil, fmt.Errorf("%w: invalid state %q", ErrInvalidArguments, options.State)
	}
	return s.Journal.ListReceipts(ctx, journal.ReceiptQuery{State: options.State, Since: options.Since, Limit: limit})
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
	return receipt, nil
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
	if receipt.State != mektup.StateOutcomeUnknown {
		return mektup.Receipt{}, fmt.Errorf("%w: receipt is not outcome-unknown", journal.ErrInvalidTransition)
	}
	now := s.now()
	if err := s.Journal.RecordManualResolution(ctx, receipt.OperationID, journal.ManualResolution{Assertion: request.Assertion, Actor: request.Actor, Reason: request.Reason, EvidenceRef: request.EvidenceRef, Presentation: request.Presentation, Timestamp: now}); err != nil {
		return mektup.Receipt{}, err
	}
	receipt.State = mektup.StateManuallyResolved
	receipt.UpdatedAt = now.Format(time.RFC3339Nano)
	receipt.ManualResolution = &mektup.ManualResolution{Assertion: request.Assertion, Actor: request.Actor, Reason: request.Reason, EvidenceRef: request.EvidenceRef, Presentation: request.Presentation, Timestamp: now.Format(time.RFC3339Nano)}
	if err := s.Journal.PutReceipt(ctx, receipt); err != nil {
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
