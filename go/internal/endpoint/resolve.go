package endpoint

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

var ErrSourceConflict = errors.New("explicit source conflicts with observed Codex identity")

type ResolvedTarget struct {
	Endpoint Endpoint
	Target   Target
	ThreadID string
	Herdr    *HerdrResolution
}

func (s EndpointStore) ResolveEndpoint(selector, codexHome string) (Endpoint, error) {
	if selector == "local" {
		return s.EnsureBuiltinLocal(codexHome)
	}
	if builtin, ok, err := s.builtinByID(selector); err != nil {
		return Endpoint{}, err
	} else if ok {
		return builtin, nil
	}
	cfg, err := s.Load()
	if err != nil {
		return Endpoint{}, err
	}
	if selector == "" {
		selector = cfg.Default
		if selector == "" {
			return s.EnsureBuiltinLocal(codexHome)
		}
	}
	for _, endpoint := range cfg.Endpoints {
		if endpoint.Alias == selector || endpoint.ID == selector {
			return endpoint, nil
		}
	}
	return Endpoint{}, fmt.Errorf("%w: %s", ErrEndpointNotFound, selector)
}

// ResolveEndpointID resolves a portable stable endpoint identity without
// treating an alias as authority. Built-in identities are looked up from the
// owner-private identity registry; configured endpoints are matched by ID.
func (s EndpointStore) ResolveEndpointID(id, codexHome string) (Endpoint, error) {
	if id == "" {
		return Endpoint{}, ErrEndpointNotFound
	}
	if builtin, ok, err := s.builtinByID(id); err != nil {
		return Endpoint{}, err
	} else if ok {
		return builtin, nil
	}
	cfg, err := s.Load()
	if err != nil {
		return Endpoint{}, err
	}
	for _, configured := range cfg.Endpoints {
		if configured.ID == id {
			return configured, nil
		}
	}
	return Endpoint{}, fmt.Errorf("%w: %s", ErrEndpointNotFound, id)
}

// ResolveDestination resolves only the destination selector. It does not
// inspect CODEX_THREAD_ID and cannot accidentally rebind source identity when
// the caller supplies an endpoint override.
func (s EndpointStore) ResolveDestination(ctx context.Context, target Target, endpointOverride, codexHome string, herdr *HerdrResolver) (ResolvedTarget, error) {
	selector := endpointOverride
	if target.Explicit {
		if target.Endpoint == "" {
			return ResolvedTarget{}, ErrEndpointRequired
		}
		selector = target.Endpoint
	}
	endpoint, err := s.ResolveEndpoint(selector, codexHome)
	if err != nil {
		return ResolvedTarget{}, err
	}
	result := ResolvedTarget{Endpoint: endpoint, Target: target}
	switch target.Kind {
	case TargetCodex:
		result.ThreadID = target.ThreadID
	case TargetAgent, TargetPane, TargetBare:
		if herdr == nil || !endpoint.HerdrEnabled() {
			return ResolvedTarget{}, fmt.Errorf("%w: Herdr is disabled for endpoint %s", ErrResolverUnavailable, endpoint.Alias)
		}
		selectorTarget := target
		if target.Kind == TargetBare {
			selectorTarget = Target{Kind: TargetAgent, Name: target.Name}
		}
		resolved, resolveErr := herdr.ResolveEndpoint(ctx, endpoint, selectorTarget)
		if resolveErr != nil {
			return ResolvedTarget{}, resolveErr
		}
		result.Herdr = &resolved
		result.ThreadID = resolved.ThreadID
	default:
		return ResolvedTarget{}, fmt.Errorf("%w: unknown target kind", ErrInvalidTarget)
	}
	return result, nil
}

type SourceOptions struct {
	CurrentThreadID string
	ReplyTo         string
	CodexHome       string
}

// ResolveSource derives source identity from the current local Codex home and
// detected thread, unless the caller explicitly provides --reply-to. It never
// uses the destination override as a source route.
func (s EndpointStore) ResolveSource(options SourceOptions) (ResolvedTarget, error) {
	if strings.TrimSpace(options.ReplyTo) != "" {
		target, err := ParseTarget(options.ReplyTo)
		if err != nil {
			return ResolvedTarget{}, err
		}
		if target.Kind != TargetCodex {
			return ResolvedTarget{}, errors.New("reply-to source must be a direct codex thread URI")
		}
		if strings.TrimSpace(options.CurrentThreadID) != "" {
			observed, observedErr := s.EnsureBuiltinLocal(options.CodexHome)
			if observedErr != nil {
				return ResolvedTarget{}, observedErr
			}
			explicit, explicitErr := s.ResolveEndpoint(target.Endpoint, options.CodexHome)
			if explicitErr != nil {
				return ResolvedTarget{}, explicitErr
			}
			if target.ThreadID != options.CurrentThreadID || explicit.ID != observed.ID {
				return ResolvedTarget{}, ErrSourceConflict
			}
		}
		endpoint, err := s.ResolveEndpoint(target.Endpoint, options.CodexHome)
		if err != nil {
			return ResolvedTarget{}, err
		}
		return ResolvedTarget{Endpoint: endpoint, Target: target, ThreadID: target.ThreadID}, nil
	}
	if strings.TrimSpace(options.CurrentThreadID) == "" {
		return ResolvedTarget{}, errors.New("source Codex thread identity is unavailable")
	}
	endpoint, err := s.EnsureBuiltinLocal(options.CodexHome)
	if err != nil {
		return ResolvedTarget{}, err
	}
	target := Target{Kind: TargetCodex, Endpoint: endpoint.Alias, ThreadID: options.CurrentThreadID, Explicit: true}
	return ResolvedTarget{Endpoint: endpoint, Target: target, ThreadID: options.CurrentThreadID}, nil
}
