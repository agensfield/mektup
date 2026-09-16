package receipts

import (
	"context"
	"fmt"
	"strings"

	mektup "github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/internal/journal"
)

// TargetIdentity is intentionally a bounded runtime projection. An inspector
// adapter may obtain it from an already initialized endpoint, but this seam
// cannot resume a thread, subscribe, repair, or return bodies.
type TargetIdentity struct {
	EndpointID    string
	EndpointAlias string
	Transport     string
	ServerVersion string
	Compatibility string
	ThreadID      string
	Requested     string
	Resolved      string
	Loaded        bool
	Status        string
	ActiveTurnID  string
	HerdrEvidence map[string]any
}

type TargetInspector interface {
	Inspect(context.Context, string) (TargetIdentity, error)
}

type InspectOptions struct {
	ReceiptLimit  int
	ReceiptCursor string
	Blockers      bool
}

type InspectResult struct {
	Target             TargetIdentity
	Receipts           []mektup.Receipt
	ReceiptsNextCursor string
	Blockers           []journal.Blocker
}

func (s Store) Inspect(ctx context.Context, target string, inspector TargetInspector, options InspectOptions) (InspectResult, error) {
	if err := s.valid(); err != nil {
		return InspectResult{}, err
	}
	if strings.TrimSpace(target) == "" || inspector == nil {
		return InspectResult{}, fmt.Errorf("%w: target and read-only inspector are required", ErrInvalidArguments)
	}
	if options.ReceiptLimit < 0 || options.ReceiptLimit > MaxLimit {
		return InspectResult{}, ErrLimit
	}
	identity, err := inspector.Inspect(ctx, target)
	if err != nil {
		return InspectResult{}, err
	}
	related := make([]mektup.Receipt, 0)
	var receiptsNextCursor string
	if options.ReceiptLimit > 0 {
		page, pageErr := s.ListPage(ctx, ListOptions{Limit: options.ReceiptLimit, Cursor: options.ReceiptCursor, EndpointID: identity.EndpointID, ThreadID: identity.ThreadID})
		related, receiptsNextCursor, err = page.Receipts, page.NextCursor, pageErr
		if err != nil {
			return InspectResult{}, err
		}
	} else if options.ReceiptCursor != "" {
		return InspectResult{}, fmt.Errorf("%w: receipt cursor requires a positive receipt limit", ErrInvalidArguments)
	}
	result := InspectResult{Target: identity, Receipts: related, ReceiptsNextCursor: receiptsNextCursor}
	if options.Blockers {
		blockerLimit := options.ReceiptLimit
		if blockerLimit == 0 {
			blockerLimit = 10
		}
		result.Blockers, err = s.Journal.ListBlockers(ctx, journal.BlockerQuery{EndpointID: identity.EndpointID, ThreadID: identity.ThreadID, Limit: blockerLimit})
		if err != nil {
			return InspectResult{}, err
		}
	}
	return result, nil
}
