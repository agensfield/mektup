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
	ErrPoolClosed       = errors.New("runtime: connection pool is closed")
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

type threadStateSession interface {
	ThreadState(context.Context, string) (string, *bool, error)
}

type loadedThreadsSession interface {
	LoadedThreads(context.Context) ([]string, error)
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
	Factory       SessionFactory
	Lookup        EndpointLookup
	mu            sync.Mutex
	sessions      map[string]Session
	subscriptions map[string]int
	openings      map[string]*sessionOpening
	gates         map[string]chan struct{}
	closed        bool
}

type sessionOpening struct {
	done    chan struct{}
	cancel  context.CancelFunc
	session Session
	err     error
}

func NewConnectionPool(factory SessionFactory, lookup EndpointLookup) *ConnectionPool {
	return &ConnectionPool{Factory: factory, Lookup: lookup, sessions: make(map[string]Session), subscriptions: make(map[string]int), openings: make(map[string]*sessionOpening), gates: make(map[string]chan struct{})}
}

func (p *ConnectionPool) session(ctx context.Context, target service.ResolvedTarget) (Session, error) {
	if p == nil || p.Factory == nil {
		return nil, ErrNoSessionFactory
	}
	if target.EndpointID == "" || target.ThreadID == "" || target.URI == "" {
		return nil, fmt.Errorf("runtime: incomplete pinned target")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrPoolClosed
	}
	if current := p.sessions[target.EndpointID]; current != nil {
		p.mu.Unlock()
		if current.EndpointID() != target.EndpointID {
			return nil, ErrEndpointMismatch
		}
		return current, nil
	}
	if opening := p.openings[target.EndpointID]; opening != nil {
		done := opening.done
		p.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-done:
			if opening.err != nil {
				return nil, opening.err
			}
			return opening.session, nil
		}
	}
	setupCtx, setupCancel := context.WithCancel(ctx)
	opening := &sessionOpening{done: make(chan struct{}), cancel: setupCancel}
	p.openings[target.EndpointID] = opening
	p.mu.Unlock()

	session, err := p.openEndpoint(setupCtx, target)
	setupCancel()
	p.mu.Lock()
	delete(p.openings, target.EndpointID)
	if p.closed && err == nil {
		err = ErrPoolClosed
	}
	if err == nil {
		p.sessions[target.EndpointID] = session
	}
	opening.session, opening.err = session, err
	close(opening.done)
	p.mu.Unlock()
	if err != nil && session != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = session.Detach(cleanupCtx)
		cancel()
	}
	return session, err
}

// ProbeThreadState combines exact native loaded enumeration with thread/read
// persistence metadata. Missing either capability fails closed.
func (p *ConnectionPool) ProbeThreadState(ctx context.Context, endpointID, threadID string) (bool, bool, error) {
	if p == nil || p.Lookup == nil {
		return false, false, errors.New("runtime: thread-state lookup is unavailable")
	}
	ep, err := p.Lookup(endpointID)
	if err != nil {
		return false, false, err
	}
	target := service.ResolvedTarget{EndpointID: endpointID, URI: codexURI(ep.Alias, threadID), ThreadID: threadID, Loaded: false, Persistent: true}
	session, err := p.session(ctx, target)
	if err != nil {
		return false, false, err
	}
	reader, stateOK := session.(threadStateSession)
	loadedReader, loadedOK := session.(loadedThreadsSession)
	if !stateOK || !loadedOK {
		return false, false, errors.New("runtime: session cannot authoritatively probe loaded and persistent thread state")
	}
	status, ephemeral, err := reader.ThreadState(ctx, threadID)
	if err != nil {
		return false, false, err
	}
	if status == "" || ephemeral == nil {
		return false, false, errors.New("runtime: thread/read omitted status or persistence metadata")
	}
	loadedIDs, err := loadedReader.LoadedThreads(ctx)
	if err != nil {
		return false, false, fmt.Errorf("runtime: loaded thread enumeration failed: %w", err)
	}
	for _, id := range loadedIDs {
		if id == threadID {
			return true, !*ephemeral, nil
		}
	}
	return false, !*ephemeral, nil
}

func (p *ConnectionPool) openEndpoint(ctx context.Context, target service.ResolvedTarget) (Session, error) {
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
	if session == nil || session.EndpointID() != target.EndpointID {
		return session, ErrEndpointMismatch
	}
	return session, nil
}

func (p *ConnectionPool) gate(key string) chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.gates[key] == nil {
		p.gates[key] = make(chan struct{}, 1)
		p.gates[key] <- struct{}{}
	}
	return p.gates[key]
}

// openIsolated creates an observer-owned connection. Observer streams consume
// a connection's single event queue, so sharing the delivery session would
// let one thread discard another thread's notifications. The returned session
// is never inserted into the pool and must be detached by its owner.
func (p *ConnectionPool) openIsolated(ctx context.Context, target service.ResolvedTarget) (Session, error) {
	if p == nil || p.Factory == nil {
		return nil, ErrNoSessionFactory
	}
	if target.EndpointID == "" || target.ThreadID == "" || target.URI == "" {
		return nil, fmt.Errorf("runtime: incomplete pinned target")
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrPoolClosed
	}
	p.mu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	session, err := p.openEndpoint(ctx, target)
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		if session != nil {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = session.Detach(cleanupCtx)
			cancel()
		}
		return nil, ErrPoolClosed
	}
	if err != nil {
		if session != nil {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_ = session.Detach(cleanupCtx)
			cancel()
		}
		return nil, err
	}
	return session, nil
}

func (p *ConnectionPool) Close(ctx context.Context) error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	sessions := make([]Session, 0, len(p.sessions))
	cancels := make([]context.CancelFunc, 0, len(p.openings))
	for _, opening := range p.openings {
		if opening.cancel != nil {
			cancels = append(cancels, opening.cancel)
		}
	}
	for _, session := range p.sessions {
		sessions = append(sessions, session)
	}
	p.sessions = make(map[string]Session)
	p.subscriptions = make(map[string]int)
	p.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
	var joined error
	for _, session := range sessions {
		joined = errors.Join(joined, session.Detach(ctx))
	}
	return joined
}

func (p *ConnectionPool) close(ctx context.Context) error { return p.Close(ctx) }

func subscriptionKey(target service.ResolvedTarget) string {
	return target.EndpointID + "\x00" + target.ThreadID
}

// subscribe acquires a reference-counted per-thread subscription on the
// pooled endpoint session. Codex subscriptions belong to the connection, not
// to an individual operation, so only the first lease resumes and only the
// last release unsubscribes.
func (p *ConnectionPool) subscribe(ctx context.Context, target service.ResolvedTarget) (Session, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	gate := p.gate(subscriptionKey(target))
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-gate:
	}
	defer func() { gate <- struct{}{} }()
	session, err := p.session(ctx, target)
	if err != nil {
		return nil, err
	}
	key := subscriptionKey(target)
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrPoolClosed
	}
	if current := p.sessions[target.EndpointID]; current != session {
		p.mu.Unlock()
		return nil, ErrEndpointMismatch
	}
	if p.subscriptions[key] > 0 {
		p.subscriptions[key]++
		p.mu.Unlock()
		return session, nil
	}
	p.mu.Unlock()
	if err := session.Resume(ctx, target.ThreadID); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrPoolClosed
	}
	if p.sessions[target.EndpointID] != session {
		return nil, ErrEndpointMismatch
	}
	p.subscriptions[key] = 1
	return session, nil
}

func (p *ConnectionPool) unsubscribe(ctx context.Context, target service.ResolvedTarget) error {
	if p == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	key := subscriptionKey(target)
	gate := p.gate(key)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-gate:
	}
	defer func() { gate <- struct{}{} }()
	p.mu.Lock()
	session := p.sessions[target.EndpointID]
	count := p.subscriptions[key]
	if count <= 0 || session == nil {
		p.mu.Unlock()
		return nil
	}
	if count > 1 {
		p.subscriptions[key] = count - 1
		p.mu.Unlock()
		return nil
	}
	delete(p.subscriptions, key)
	// Hold the pool lock while releasing the last lease so another subscriber
	// cannot race a new Resume between the count check and unsubscribe.
	p.mu.Unlock()
	return session.Unsubscribe(ctx, target.ThreadID)
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

func (s *connectionSession) ThreadExists(ctx context.Context, threadID string) (bool, error) {
	_, err := s.api.ThreadRead(ctx, codexapi.ThreadReadOptions{ThreadID: threadID})
	return err == nil, err
}

func (s *connectionSession) ThreadState(ctx context.Context, threadID string) (string, *bool, error) {
	result, err := s.api.ThreadRead(ctx, codexapi.ThreadReadOptions{ThreadID: threadID})
	if err != nil {
		return "", nil, err
	}
	ephemeralRaw, ok := result.Thread.Fields["ephemeral"]
	if !ok {
		return result.Thread.Status, nil, nil
	}
	var ephemeral bool
	if err := json.Unmarshal(ephemeralRaw, &ephemeral); err != nil {
		return result.Thread.Status, nil, err
	}
	return result.Thread.Status, &ephemeral, nil
}

func (s *connectionSession) LoadedThreads(ctx context.Context) ([]string, error) {
	var all []string
	cursor := ""
	for pageNo := 0; pageNo < codexapi.MaxReconciliationPages; pageNo++ {
		result, err := s.api.LoadedThreads(ctx, cursor, codexapi.MaxThreadPageLimit)
		if err != nil {
			return nil, err
		}
		if len(all)+len(result.Data) > codexapi.MaxReconciliationItems {
			return nil, codexapi.ErrPaginationExceeded
		}
		all = append(all, result.Data...)
		if result.NextCursor == "" {
			return all, nil
		}
		if result.NextCursor == cursor {
			return nil, codexapi.ErrPaginationStalled
		}
		cursor = result.NextCursor
	}
	return nil, codexapi.ErrPaginationExceeded
}

func (s *connectionSession) ItemsHistory(ctx context.Context, threadID string) ([]codexapi.ItemEntry, error) {
	var all []codexapi.ItemEntry
	cursor := ""
	totalBytes := 0
	for pageNo := 0; pageNo < codexapi.MaxReconciliationPages; pageNo++ {
		page, err := s.api.ThreadItems(ctx, codexapi.ItemsOptions{ThreadID: threadID, Cursor: cursor, Limit: codexapi.MaxItemsPageLimit})
		if err != nil {
			return nil, fmt.Errorf("runtime: item history page %d: %w", pageNo+1, err)
		}
		if len(all)+len(page.Data) > codexapi.MaxReconciliationItems {
			return nil, codexapi.ErrPaginationExceeded
		}
		totalBytes += len(page.Raw)
		if totalBytes > codexapi.MaxReconciliationBytes {
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
