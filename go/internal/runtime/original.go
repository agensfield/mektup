package runtime

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/internal/service"
)

// OriginalResolver performs exact lookup against one already pinned current
// thread. It never invokes search, ranks candidates, or resolves a Herdr alias.
type OriginalResolver struct {
	Observe     service.ObservationPort
	Target      service.ResolvedTarget
	ValidateURI func(context.Context, string, string) error
	// CandidateLookup locates native items cheaply, but resolveItems still
	// validates their complete envelope and pinned relationship. A search miss
	// is incomplete evidence, never proof of absence or a reason to scan an
	// unbounded thread. Only an unsupported search method uses bounded history.
	CandidateLookup func(context.Context, service.ResolvedTarget, string) ([]service.ObservedItem, error)
}

var ErrTargetedOriginalLookupUnsupported = errors.New("runtime: targeted original lookup is unsupported")

func (r OriginalResolver) ResolveOriginal(ctx context.Context, reference string) (service.OriginalMessage, error) {
	if r.Observe == nil || r.Target.ThreadID == "" || r.Target.URI == "" {
		return service.OriginalMessage{}, errors.New("runtime: original resolver is not bound to a current thread")
	}
	if strings.TrimSpace(reference) != reference || reference == "" {
		return service.OriginalMessage{}, errors.New("runtime: exact original reference is empty or has surrounding whitespace")
	}
	if r.CandidateLookup != nil {
		candidates, err := r.CandidateLookup(ctx, r.Target, reference)
		if err == nil && len(candidates) != 0 {
			original, resolveErr := r.resolveItems(ctx, reference, candidates)
			if resolveErr == nil || errors.Is(resolveErr, service.ErrOriginalIdentityConflict) {
				return original, resolveErr
			}
			return service.OriginalMessage{}, fmt.Errorf("%w: search results did not verify the original", service.ErrOriginalLookupIncomplete)
		}
		if !errors.Is(err, ErrTargetedOriginalLookupUnsupported) {
			if err == nil {
				return service.OriginalMessage{}, fmt.Errorf("%w: indexed search found no verifiable original", service.ErrOriginalLookupIncomplete)
			}
			return service.OriginalMessage{}, fmt.Errorf("%w: indexed original lookup: %v", service.ErrOriginalLookupIncomplete, err)
		}
	}
	items, err := r.Observe.FullHistory(ctx, r.Target)
	if err != nil {
		return service.OriginalMessage{}, fmt.Errorf("%w: %v", service.ErrOriginalLookupIncomplete, err)
	}
	return r.resolveItems(ctx, reference, items)
}

func (r OriginalResolver) resolveItems(ctx context.Context, reference string, items []service.ObservedItem) (service.OriginalMessage, error) {
	var match *service.OriginalMessage
	for _, item := range items {
		if item.ThreadID != "" && item.ThreadID != r.Target.ThreadID {
			continue
		}
		if item.NativeType != "" && item.NativeType != "userMessage" {
			continue
		}
		envelope, parseErr := mektup.ParseEnvelopeString(item.Text)
		if parseErr != nil || (envelope.Kind != mektup.KindMessage && envelope.Kind != mektup.KindReply) {
			continue
		}
		if !matchesExact(reference, envelope, item.ClientMessageID) {
			continue
		}
		if err := envelope.Validate(); err != nil {
			continue
		}
		// Both the URI and the native thread ID are checked. This is the fork
		// guard: an ancestor envelope remains ordinary copied history in a
		// descendant and cannot become a reply target.
		envelopeAddress, envelopeAddressErr := mektup.ParseThreadURI(envelope.To)
		targetAddress, targetAddressErr := mektup.ParseThreadURI(r.Target.URI)
		if envelope.ToEndpointID != r.Target.EndpointID || envelopeAddressErr != nil || targetAddressErr != nil || envelopeAddress.ThreadID != targetAddress.ThreadID {
			continue
		}
		if envelope.To != r.Target.URI && (r.ValidateURI == nil || r.ValidateURI(ctx, r.Target.EndpointID, envelope.To) != nil) {
			continue
		}
		candidate := service.OriginalMessage{Envelope: envelope, CurrentThread: r.Target.URI}
		// Envelope is a scalar-only immutable value. Compare the complete
		// relationship tuple, including sender, reply route, custody route,
		// status, timestamp, and body, rather than selecting the later history
		// presentation when a return route was changed.
		if match != nil && match.Envelope != candidate.Envelope {
			return service.OriginalMessage{}, service.ErrOriginalIdentityConflict
		}
		copy := candidate
		match = &copy
	}
	if match == nil {
		return service.OriginalMessage{}, fmt.Errorf("%w: exact original %q was not found in the pinned thread", service.ErrOriginalNotFound, reference)
	}
	return *match, nil
}

func matchesExact(reference string, envelope mektup.Envelope, clientMessageID string) bool {
	return reference == envelope.MessageID || reference == clientMessageID || (strings.HasPrefix(reference, "sha256:") && reference == envelope.PayloadSHA256)
}

var _ service.OriginalResolver = OriginalResolver{}
