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
	Observe service.ObservationPort
	Target  service.ResolvedTarget
}

func (r OriginalResolver) ResolveOriginal(ctx context.Context, reference string) (service.OriginalMessage, error) {
	if r.Observe == nil || r.Target.ThreadID == "" || r.Target.URI == "" {
		return service.OriginalMessage{}, errors.New("runtime: original resolver is not bound to a current thread")
	}
	if strings.TrimSpace(reference) != reference || reference == "" {
		return service.OriginalMessage{}, errors.New("runtime: exact original reference is empty or has surrounding whitespace")
	}
	items, err := r.Observe.FullHistory(ctx, r.Target)
	if err != nil {
		return service.OriginalMessage{}, err
	}
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
		if envelope.To != r.Target.URI || envelope.ToEndpointID != r.Target.EndpointID || envelope.ValidateAddressToThread(r.Target.URI) != nil {
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
		return service.OriginalMessage{}, fmt.Errorf("runtime: exact original %q was not found in the pinned thread", reference)
	}
	return *match, nil
}

func matchesExact(reference string, envelope mektup.Envelope, clientMessageID string) bool {
	return reference == envelope.MessageID || reference == clientMessageID || (strings.HasPrefix(reference, "sha256:") && reference == envelope.PayloadSHA256)
}

var _ service.OriginalResolver = OriginalResolver{}
