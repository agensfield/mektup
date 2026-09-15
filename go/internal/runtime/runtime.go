// Package runtime contains the concrete same-host adapters for service.
//
// The package deliberately does not expose CLI policy or start a Codex daemon.
// It joins an already-running endpoint, keeps its endpoint identity pinned for
// the operation, and translates the low-level app-server evidence into the
// service ports.
package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/agensfield/mektup/go/appserver"
	"github.com/agensfield/mektup/go/internal/codexapi"
	"github.com/agensfield/mektup/go/internal/connection"
	"github.com/agensfield/mektup/go/internal/endpoint"
	"github.com/agensfield/mektup/go/internal/service"
)

var (
	ErrNoSessionFactory = errors.New("runtime: session factory is required")
	ErrEndpointMismatch = errors.New("runtime: session endpoint does not match pinned target")
)

// Session is the smallest concrete app-server seam needed by the runtime
// adapters. A session is tied to one endpoint ID for its whole lifetime.
type Session interface {
	EndpointID() string
	StartOrSteer(context.Context, string, string, string) (TurnResult, error)
	Resume(context.Context, string) error
	Unsubscribe(context.Context, string) error
	History(context.Context, string) ([]codexapi.Turn, error)
	NextEvent(context.Context) (appserver.Event, error)
	Detach(context.Context) error
}

type itemHistorySession interface {
	ItemsHistory(context.Context, string) ([]codexapi.ItemEntry, error)
}

// TurnResult is deliberately smaller than codexapi's response. The adapter
// only needs the associated turn and raw acceptance evidence.
type TurnResult struct {
	TurnID   string
	Evidence string
}

// SessionFactory opens one already-running app-server route. Implementations
// must not start, restart, or replace a daemon.
type SessionFactory interface {
	Open(context.Context, endpoint.Endpoint) (Session, error)
}

type SessionFactoryFunc func(context.Context, endpoint.Endpoint) (Session, error)

func (f SessionFactoryFunc) Open(ctx context.Context, ep endpoint.Endpoint) (Session, error) {
	if f == nil {
		return nil, ErrNoSessionFactory
	}
	return f(ctx, ep)
}

type EndpointLookup func(string) (endpoint.Endpoint, error)

// ConnectionPool retains one initialized connection per stable endpoint ID.
// It never uses an alias as a cache key, so an alias change cannot move a
// pinned operation to another endpoint.
type ConnectionPool struct {
	Factory  SessionFactory
	Lookup   EndpointLookup
	mu       sync.Mutex
	sessions map[string]Session
}

func NewConnectionPool(factory SessionFactory, lookup EndpointLookup) *ConnectionPool {
	return &ConnectionPool{Factory: factory, Lookup: lookup, sessions: make(map[string]Session)}
}

func (p *ConnectionPool) session(ctx context.Context, target service.ResolvedTarget) (Session, error) {
	if p == nil || p.Factory == nil {
		return nil, ErrNoSessionFactory
	}
	if target.EndpointID == "" || target.ThreadID == "" || target.URI == "" {
		return nil, fmt.Errorf("runtime: incomplete pinned target")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if current := p.sessions[target.EndpointID]; current != nil {
		if current.EndpointID() != target.EndpointID {
			return nil, ErrEndpointMismatch
		}
		return current, nil
	}
	// Opening happens under the pool lock. This intentionally serializes the
	// first operation for an endpoint and prevents two same-endpoint daemons
	// from being created by racing callers.
	if p.Lookup == nil {
		return nil, errors.New("runtime: endpoint lookup is required")
	}
	ep, err := p.Lookup(target.EndpointID)
	if err != nil {
		return nil, err
	}
	if ep.ID != target.EndpointID {
		return nil, ErrEndpointMismatch
	}
	session, err := p.Factory.Open(ctx, ep)
	if err != nil {
		return nil, err
	}
	if session == nil {
		return nil, ErrEndpointMismatch
	}
	if session.EndpointID() != target.EndpointID {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = session.Detach(cleanupCtx)
		cancel()
		return nil, ErrEndpointMismatch
	}
	p.sessions[target.EndpointID] = session
	return session, nil
}

func (p *ConnectionPool) close(ctx context.Context) error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	sessions := make([]Session, 0, len(p.sessions))
	for _, session := range p.sessions {
		sessions = append(sessions, session)
	}
	p.sessions = make(map[string]Session)
	p.mu.Unlock()
	var joined error
	for _, session := range sessions {
		joined = errors.Join(joined, session.Detach(ctx))
	}
	return joined
}

func (p *ConnectionPool) forget(endpointID string, session Session) {
	if p == nil {
		return
	}
	p.mu.Lock()
	if current := p.sessions[endpointID]; current == session {
		delete(p.sessions, endpointID)
	}
	p.mu.Unlock()
}

// ConnectionFactory is the production factory over internal/connection.
type ConnectionFactory struct {
	Options connection.Options
}

func (f ConnectionFactory) Open(ctx context.Context, ep endpoint.Endpoint) (Session, error) {
	if err := ep.Validate(); err != nil {
		return nil, err
	}
	conn, err := connection.Connect(ctx, ep.Route, f.Options)
	if err != nil {
		return nil, err
	}
	return &connectionSession{endpointID: ep.ID, conn: conn, api: codexapi.New(conn, codexapi.Options{Capabilities: conn.Capabilities()})}, nil
}

type connectionSession struct {
	endpointID string
	conn       *connection.Connection
	api        *codexapi.Client
}

func (s *connectionSession) EndpointID() string { return s.endpointID }

func (s *connectionSession) StartOrSteer(ctx context.Context, threadID, text, clientID string) (TurnResult, error) {
	if threadID == "" || text == "" || clientID == "" {
		return TurnResult{}, &service.DeliveryError{Err: errors.New("runtime: turn/start identity or text is empty"), Phase: service.WriteProvenBeforeWrite}
	}
	params := struct {
		ThreadID            string `json:"threadId"`
		ClientUserMessageID string `json:"clientUserMessageId"`
		Input               []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"input"`
	}{ThreadID: threadID, ClientUserMessageID: clientID, Input: []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}{{Type: "text", Text: text}}}
	rawParams, err := json.Marshal(params)
	if err != nil {
		return TurnResult{}, &service.DeliveryError{Err: err, Phase: service.WriteProvenBeforeWrite}
	}
	raw, err := s.conn.CallDetailed(ctx, "turn/start", rawParams)
	if err != nil {
		return TurnResult{}, deliveryErrorFromCall(err, service.WriteMayHaveWritten)
	}
	if raw.ServerError != nil {
		return TurnResult{}, &service.DeliveryError{Server: raw.ServerError, Phase: mapWritePhase(raw.Evidence.Phase)}
	}
	var response struct {
		Turn struct {
			ID string `json:"id"`
		} `json:"turn"`
	}
	if err := json.Unmarshal(raw.Result, &response); err != nil || response.Turn.ID == "" {
		if err == nil {
			err = errors.New("runtime: turn/start response omitted turn.id")
		}
		return TurnResult{}, &service.DeliveryError{Err: err, Phase: mapWritePhase(raw.Evidence.Phase)}
	}
	return TurnResult{TurnID: response.Turn.ID, Evidence: "turn/start response"}, nil
}

func mapWritePhase(phase appserver.WritePhase) service.WritePhase {
	switch phase {
	case appserver.WriteProvenBeforeWrite:
		return service.WriteProvenBeforeWrite
	case appserver.WriteMayHaveWritten:
		return service.WriteMayHaveWritten
	case appserver.WriteComplete:
		return service.WriteComplete
	default:
		return service.WriteNotStarted
	}
}

func deliveryErrorFromCall(err error, fallback service.WritePhase) error {
	var low *connection.CallError
	if errors.As(err, &low) {
		return &service.DeliveryError{Err: err, Phase: mapWritePhase(low.Evidence.Phase)}
	}
	return &service.DeliveryError{Err: err, Phase: fallback}
}

func (s *connectionSession) Resume(ctx context.Context, threadID string) error {
	_, err := s.api.ThreadResume(ctx, codexapi.ResumeOptions{ThreadID: threadID})
	return err
}

func (s *connectionSession) Unsubscribe(ctx context.Context, threadID string) error {
	_, err := s.api.ThreadUnsubscribe(ctx, threadID)
	return err
}

func (s *connectionSession) History(ctx context.Context, threadID string) ([]codexapi.Turn, error) {
	result, err := s.api.ReadFullTurns(ctx, threadID)
	if err != nil {
		return nil, err
	}
	return result.Data, nil
}

func (s *connectionSession) ItemsHistory(ctx context.Context, threadID string) ([]codexapi.ItemEntry, error) {
	var all []codexapi.ItemEntry
	cursor := ""
	for pageNo := 0; pageNo < codexapi.MaxReconciliationPages; pageNo++ {
		page, err := s.api.ThreadItems(ctx, codexapi.ItemsOptions{ThreadID: threadID, Cursor: cursor, Limit: codexapi.MaxItemsPageLimit})
		if err != nil {
			return nil, fmt.Errorf("runtime: item history page %d: %w", pageNo+1, err)
		}
		if len(all)+len(page.Data) > codexapi.MaxReconciliationItems {
			return nil, codexapi.ErrPaginationExceeded
		}
		all = append(all, page.Data...)
		if page.NextCursor == "" {
			return all, nil
		}
		if page.NextCursor == cursor {
			return nil, codexapi.ErrPaginationStalled
		}
		cursor = page.NextCursor
	}
	return nil, codexapi.ErrPaginationExceeded
}

func (s *connectionSession) NextEvent(ctx context.Context) (appserver.Event, error) {
	return s.conn.NextEvent(ctx)
}

func (s *connectionSession) Detach(ctx context.Context) error { return s.conn.Detach(ctx) }

var _ SessionFactory = ConnectionFactory{}
var _ service.DeliveryPort = (*DeliveryAdapter)(nil)
