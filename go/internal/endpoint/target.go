// Package endpoint contains the private endpoint, target, and resolver
// adapters used by Mektup.  Its types deliberately do not expose CLI or
// transport lifecycles.
package endpoint

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

type TargetKind string

const (
	TargetBare  TargetKind = "bare"
	TargetCodex TargetKind = "codex-thread"
	TargetAgent TargetKind = "herdr-agent"
	TargetPane  TargetKind = "herdr-pane"
)

var (
	ErrInvalidTarget       = errors.New("invalid endpoint target")
	ErrEndpointMismatch    = errors.New("target endpoint does not match selected endpoint")
	ErrEndpointRequired    = errors.New("target endpoint is required")
	ErrEndpointNotFound    = errors.New("endpoint is not configured")
	ErrDuplicateAlias      = errors.New("endpoint alias is already configured")
	ErrDuplicateEndpointID = errors.New("endpoint ID is already configured")
	ErrBuiltinImmutable    = errors.New("built-in local endpoint is immutable")
)

// Target is the typed form of a direct Codex or Herdr selector.  Name is only
// populated for bare and agent targets; ThreadID and PaneID are the exact
// native identifiers for their respective target kinds.
type Target struct {
	Raw      string
	Kind     TargetKind
	Endpoint string
	Name     string
	ThreadID string
	PaneID   string
	Explicit bool
}

// ParseTarget accepts the v1 URI forms and a bare Herdr agent name.  Path
// components are unescaped exactly once, so names containing spaces, UTF-8,
// or a literal percent sign round-trip without being interpreted as syntax.
func ParseTarget(raw string) (Target, error) {
	if raw == "" || strings.TrimSpace(raw) != raw || strings.ContainsRune(raw, '\x00') {
		return Target{}, fmt.Errorf("%w: empty, whitespace, or NUL", ErrInvalidTarget)
	}
	if !strings.Contains(raw, "://") {
		if strings.ContainsAny(raw, "/?#") {
			return Target{}, fmt.Errorf("%w: malformed bare name", ErrInvalidTarget)
		}
		return Target{Raw: raw, Kind: TargetBare, Name: raw}, nil
	}

	u, err := url.Parse(raw)
	if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return Target{}, fmt.Errorf("%w: malformed URI", ErrInvalidTarget)
	}
	if u.Host == "" || strings.ContainsAny(u.Host, "/?#%") {
		return Target{}, fmt.Errorf("%w: endpoint alias is invalid", ErrInvalidTarget)
	}
	parts := strings.Split(strings.TrimPrefix(u.EscapedPath(), "/"), "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return Target{}, fmt.Errorf("%w: expected /kind/value", ErrInvalidTarget)
	}
	value, err := url.PathUnescape(parts[1])
	if err != nil || value == "" || strings.ContainsAny(value, "\x00\r\n") {
		return Target{}, fmt.Errorf("%w: invalid escaped value", ErrInvalidTarget)
	}
	target := Target{Raw: raw, Endpoint: u.Host, Explicit: true}
	switch strings.ToLower(u.Scheme) {
	case "codex":
		if parts[0] != "thread" || strings.ContainsAny(value, " \t") {
			return Target{}, fmt.Errorf("%w: codex target must be /thread/<id>", ErrInvalidTarget)
		}
		target.Kind, target.ThreadID = TargetCodex, value
	case "herdr":
		switch parts[0] {
		case "agent":
			target.Kind, target.Name = TargetAgent, value
		case "pane":
			if !fullyQualifiedPane(value) {
				return Target{}, fmt.Errorf("%w: pane must be fully qualified (workspace:pane)", ErrInvalidTarget)
			}
			target.Kind, target.PaneID = TargetPane, value
		default:
			return Target{}, fmt.Errorf("%w: Herdr target must be /agent/<name> or /pane/<id>", ErrInvalidTarget)
		}
	default:
		return Target{}, fmt.Errorf("%w: unsupported scheme %q", ErrInvalidTarget, u.Scheme)
	}
	return target, nil
}

func fullyQualifiedPane(value string) bool {
	colon := strings.IndexByte(value, ':')
	return colon > 0 && colon < len(value)-2 && !strings.ContainsAny(value, " /\\") &&
		strings.HasPrefix(value[colon+1:], "p")
}

func (t Target) String() string {
	if t.Raw != "" {
		return t.Raw
	}
	if t.Kind == TargetBare {
		return t.Name
	}
	var kind, value string
	switch t.Kind {
	case TargetCodex:
		kind, value = "codex", "thread/"+url.PathEscape(t.ThreadID)
	case TargetAgent:
		kind, value = "herdr", "agent/"+url.PathEscape(t.Name)
	case TargetPane:
		kind, value = "herdr", "pane/"+url.PathEscape(t.PaneID)
	default:
		return ""
	}
	return kind + "://" + t.Endpoint + "/" + value
}

// PortableTarget contains no local route paths or executable details.
type PortableTarget struct {
	Kind     TargetKind `json:"kind"`
	Endpoint string     `json:"endpoint"`
	Name     string     `json:"name,omitempty"`
	ThreadID string     `json:"threadId,omitempty"`
	PaneID   string     `json:"paneId,omitempty"`
}

func (t Target) Portable() PortableTarget {
	return PortableTarget{Kind: t.Kind, Endpoint: t.Endpoint, Name: t.Name, ThreadID: t.ThreadID, PaneID: t.PaneID}
}
