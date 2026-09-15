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
	ReceiptLimit int
	Blockers     bool
}

type InspectResult struct {
	Target   TargetIdentity
	Receipts []mektup.Receipt
	Blockers []journal.Blocker
}

func (s Store) Inspect(ctx context.Context, target string, inspector TargetInspector, options InspectOptions) (InspectResult, error) {
	if err := s.valid(); err != nil {
		return InspectResult{}, err
	}
	if strings.TrimSpace(target) == "" || inspector == nil {
		return InspectResult{}, fmt.Errorf("%w: target and read-only inspector are required", ErrInvalidArguments)
	}
	limit, err := boundedLimit(options.ReceiptLimit)
	if err != nil {
		return InspectResult{}, err
	}
	identity, err := inspector.Inspect(ctx, target)
	if err != nil {
		return InspectResult{}, err
	}
	receipts, err := s.Journal.ListReceipts(ctx, journal.ReceiptQuery{Limit: limit})
	if err != nil {
		return InspectResult{}, err
	}
	// The journal query itself is bounded. Filter only metadata identity fields
	// and never hydrate transcript content as an inspection side effect.
	receipts = filterRelatedReceipts(receipts, identity)
	result := InspectResult{Target: identity, Receipts: receipts}
	if options.Blockers {
		result.Blockers, err = s.Journal.ListBlockers(ctx, journal.BlockerQuery{EndpointID: identity.EndpointID, ThreadID: identity.ThreadID, Limit: limit})
		if err != nil {
			return InspectResult{}, err
		}
	}
	return result, nil
}

func filterRelatedReceipts(receipts []mektup.Receipt, identity TargetIdentity) []mektup.Receipt {
	if identity.EndpointID == "" && identity.ThreadID == "" {
		return receipts
	}
	out := make([]mektup.Receipt, 0, len(receipts))
	for _, receipt := range receipts {
		matchesSource := (identity.EndpointID == "" || receipt.Source.EndpointID == identity.EndpointID) && (identity.ThreadID == "" || receipt.Source.ThreadID == identity.ThreadID)
		matchesTarget := (identity.EndpointID == "" || receipt.Target.EndpointID == identity.EndpointID) && (identity.ThreadID == "" || receipt.Target.ThreadID == identity.ThreadID)
		if matchesSource || matchesTarget {
			out = append(out, receipt)
		}
	}
	return out
}
