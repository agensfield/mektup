package runtime

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/internal/endpoint"
	"github.com/agensfield/mektup/go/internal/service"
)

// ResolverAdapter is an invocation-scoped resolver. It captures endpoint
// identity and the observed source at construction time; service retries use
// the returned service.ResolvedTarget and never call this resolver again.
type ResolverAdapter struct {
	Store             endpoint.EndpointStore
	Herdr             *endpoint.HerdrResolver
	EndpointOverride  string
	CodexHome         string
	CurrentThreadID   string
	ReplyTo           string
	CustodyEndpointID string
	CustodyStoreID    string
	StateProbe        ThreadStateProbe
}

// ThreadStateProbe is an explicit preflight seam for authoritative loaded and
// persistence facts. Without one, the adapter conservatively treats a direct
// target as already loaded and never invents a resume effect from stale local
// metadata.
type ThreadStateProbe func(context.Context, string, string) (loaded, persistent bool, err error)

func (r ResolverAdapter) Resolve(ctx context.Context, selector string) (service.ResolvedTarget, error) {
	target, err := endpoint.ParseTarget(selector)
	if err != nil {
		return service.ResolvedTarget{}, err
	}
	resolved, err := r.Store.ResolveDestination(ctx, target, r.EndpointOverride, r.CodexHome, r.Herdr)
	if err != nil {
		return service.ResolvedTarget{}, err
	}
	if resolved.ThreadID == "" {
		return service.ResolvedTarget{}, fmt.Errorf("runtime: resolver returned an empty Codex thread ID")
	}
	uri := codexURI(resolved.Endpoint.ID, resolved.ThreadID)
	loaded, persistent := true, true
	if r.StateProbe != nil {
		loaded, persistent, err = r.StateProbe(ctx, resolved.Endpoint.ID, resolved.ThreadID)
		if err != nil {
			return service.ResolvedTarget{}, fmt.Errorf("runtime: target runtime-state preflight failed: %w", err)
		}
	}
	return service.ResolvedTarget{Requested: selector, EndpointID: resolved.Endpoint.ID, URI: uri, ThreadID: resolved.ThreadID, Loaded: loaded, Persistent: persistent}, nil
}

func (r ResolverAdapter) ResolveSource(ctx context.Context, source string) (service.SourceIdentity, error) {
	resolved, err := r.Store.ResolveSource(endpoint.SourceOptions{CurrentThreadID: r.CurrentThreadID, ReplyTo: firstNonEmpty(source, r.ReplyTo), CodexHome: r.CodexHome})
	if err != nil {
		return service.SourceIdentity{}, err
	}
	uri := codexURI(resolved.Endpoint.ID, resolved.ThreadID)
	herdrURI := ""
	if r.Herdr != nil && resolved.Endpoint.HerdrEnabled() {
		if provenance, provenanceErr := r.Herdr.ResolveThreadEndpoint(ctx, resolved.Endpoint, resolved.ThreadID); provenanceErr == nil {
			herdrURI = "herdr://" + resolved.Endpoint.ID + "/pane/" + url.PathEscape(provenance.Pane)
		}
	}
	return service.SourceIdentity{EndpointID: resolved.Endpoint.ID, URI: uri, Herdr: herdrURI, Human: false, CustodyEndpointID: firstNonEmpty(r.CustodyEndpointID, resolved.Endpoint.ID), CustodyStoreID: r.CustodyStoreID}, nil
}

func (r ResolverAdapter) ResolvePinned(ctx context.Context, endpointID, uri string) (service.ResolvedTarget, error) {
	if endpointID == "" || uri == "" {
		return service.ResolvedTarget{}, fmt.Errorf("runtime: pinned endpoint and URI are required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	address, err := mektup.ParseThreadURI(uri)
	if err != nil {
		return service.ResolvedTarget{}, err
	}
	ep, err := r.Store.ResolveEndpointID(endpointID, r.CodexHome)
	if err != nil {
		return service.ResolvedTarget{}, err
	}
	// Preserve and validate the supplied selector. A stable-ID selector must
	// name the independently resolved endpoint exactly; an alias selector must
	// be locally configured and map to that same stable endpoint. Neither form
	// can be silently rewritten into a different relationship tuple.
	if mektup.ValidateID(address.Endpoint, mektup.EndpointIDPrefix) == nil {
		if address.Endpoint != endpointID {
			return service.ResolvedTarget{}, fmt.Errorf("runtime: pinned stable URI selector %q does not match endpoint %q", address.Endpoint, endpointID)
		}
	} else {
		mapped, mapErr := r.Store.ResolveEndpoint(address.Endpoint, r.CodexHome)
		if mapErr != nil || mapped.ID != ep.ID {
			if mapErr != nil {
				return service.ResolvedTarget{}, fmt.Errorf("runtime: pinned URI selector %q is not locally mapped to endpoint %q: %w", address.Endpoint, endpointID, mapErr)
			}
			return service.ResolvedTarget{}, fmt.Errorf("runtime: pinned URI selector %q maps to endpoint %q, want %q", address.Endpoint, mapped.ID, endpointID)
		}
	}
	loaded, persistent := true, true
	if r.StateProbe != nil {
		loaded, persistent, err = r.StateProbe(ctx, ep.ID, address.ThreadID)
		if err != nil {
			return service.ResolvedTarget{}, fmt.Errorf("runtime: pinned target runtime-state preflight failed: %w", err)
		}
	}
	return service.ResolvedTarget{Requested: uri, EndpointID: ep.ID, URI: uri, ThreadID: address.ThreadID, Loaded: loaded, Persistent: persistent}, nil
}

func codexURI(alias, threadID string) string {
	return "codex://" + alias + "/thread/" + threadID
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

var _ service.Resolver = ResolverAdapter{}
