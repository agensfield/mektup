package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	mektup "github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/appserver"
	"github.com/agensfield/mektup/go/internal/codexapi"
	"github.com/agensfield/mektup/go/internal/endpoint"
	"github.com/agensfield/mektup/go/internal/service"
)

const (
	testSourceEndpoint = "ep_01999999-9999-7999-8999-999999999991"
	testTargetEndpoint = "ep_01999999-9999-7999-8999-999999999992"
)

type fakeSession struct {
	endpoint   string
	start      func(context.Context, string, string, string) (TurnResult, error)
	events     chan appserver.Event
	history    []codexapi.Turn
	items      []codexapi.ItemEntry
	historyErr error
	mu         sync.Mutex
	calls      []string
	unsub      int
	detach     int
}

func (s *fakeSession) EndpointID() string { return s.endpoint }
func (s *fakeSession) StartOrSteer(ctx context.Context, thread, text, id string) (TurnResult, error) {
	if s.start != nil {
		return s.start(ctx, thread, text, id)
	}
	return TurnResult{TurnID: "turn-1", Evidence: "fake accepted"}, nil
}
func (s *fakeSession) Resume(context.Context, string) error {
	s.mu.Lock()
	s.calls = append(s.calls, "resume")
	s.mu.Unlock()
	return nil
}
func (s *fakeSession) Unsubscribe(context.Context, string) error {
	s.mu.Lock()
	s.unsub++
	s.mu.Unlock()
	return nil
}
func (s *fakeSession) History(context.Context, string) ([]codexapi.Turn, error) {
	return s.history, s.historyErr
}
func (s *fakeSession) ItemsHistory(context.Context, string) ([]codexapi.ItemEntry, error) {
	return s.items, nil
}
func (s *fakeSession) NextEvent(ctx context.Context) (appserver.Event, error) {
	select {
	case event, ok := <-s.events:
		if !ok {
			return appserver.Event{}, io.EOF
		}
		return event, nil
	case <-ctx.Done():
		return appserver.Event{}, ctx.Err()
	}
}
func (s *fakeSession) Detach(context.Context) error {
	s.mu.Lock()
	s.detach++
	s.mu.Unlock()
	return nil
}

type fakeFactory struct {
	session *fakeSession
	opens   int
	got     []endpoint.Endpoint
}

func (f *fakeFactory) Open(_ context.Context, ep endpoint.Endpoint) (Session, error) {
	f.opens++
	f.got = append(f.got, ep)
	return f.session, nil
}

func testTarget() service.ResolvedTarget {
	return service.ResolvedTarget{EndpointID: testTargetEndpoint, URI: "codex://target/thread/thread-1", ThreadID: "thread-1", Loaded: true, Persistent: true}
}

func TestDeliveryPoolPinsStableEndpointAndUsesOneSession(t *testing.T) {
	factory := &fakeFactory{session: &fakeSession{endpoint: testTargetEndpoint}}
	pool := NewConnectionPool(factory, func(id string) (endpoint.Endpoint, error) {
		if id != testTargetEndpoint {
			t.Fatalf("lookup used unexpected endpoint ID %q", id)
		}
		return endpoint.Endpoint{ID: id, Alias: "target", Route: endpoint.Route{Kind: endpoint.RouteUnix, UnixSocket: "/tmp/daemon.sock"}, Herdr: endpoint.HerdrDisabled}, nil
	})
	delivery := &DeliveryAdapter{Pool: pool}
	for i := 0; i < 2; i++ {
		result, err := delivery.Send(context.Background(), testTarget(), "body", "msg_01999999-9999-7999-8999-999999999993")
		if err != nil || !result.Accepted || result.TurnID != "turn-1" {
			t.Fatalf("delivery %d = %#v, %v", i, result, err)
		}
	}
	if factory.opens != 1 {
		t.Fatalf("opened %d sessions for one endpoint", factory.opens)
	}
}

func TestDeliveryPreservesServiceWriteEvidenceAndServerError(t *testing.T) {
	server := &codexapi.ServerError{Code: -32603, Message: "internal"}
	session := &fakeSession{endpoint: testTargetEndpoint, start: func(context.Context, string, string, string) (TurnResult, error) {
		return TurnResult{}, &service.DeliveryError{Server: server, Phase: service.WriteComplete}
	}}
	pool := NewConnectionPool(&fakeFactory{session: session}, func(string) (endpoint.Endpoint, error) { return endpoint.Endpoint{ID: testTargetEndpoint}, nil })
	_, err := (&DeliveryAdapter{Pool: pool}).Send(context.Background(), testTarget(), "body", "msg_01999999-9999-7999-8999-999999999996")
	var deliveryErr *service.DeliveryError
	if !errors.As(err, &deliveryErr) || deliveryErr.Server != server || deliveryErr.Phase != service.WriteComplete {
		t.Fatalf("delivery evidence = %#v, %v", deliveryErr, err)
	}
}

func TestConnectionPoolDetachesMismatchedOpenedSession(t *testing.T) {
	session := &fakeSession{endpoint: "ep_01999999-9999-7999-8999-999999999997"}
	pool := NewConnectionPool(&fakeFactory{session: session}, func(string) (endpoint.Endpoint, error) { return endpoint.Endpoint{ID: testTargetEndpoint}, nil })
	if _, err := pool.session(context.Background(), testTarget()); !errors.Is(err, ErrEndpointMismatch) {
		t.Fatalf("mismatched session error = %v", err)
	}
	session.mu.Lock()
	detach := session.detach
	session.mu.Unlock()
	if detach != 1 {
		t.Fatalf("mismatched session detach count = %d, want one", detach)
	}
}

func TestObservationIgnoresServerRequestsAndMapsVisibleCompletedItem(t *testing.T) {
	events := make(chan appserver.Event, 2)
	events <- appserver.Event{Kind: appserver.EventServerRequest, Request: &appserver.ServerRequest{Method: "item/permissions/requestApproval"}}
	params, _ := json.Marshal(map[string]any{
		"threadId": "thread-1", "turnId": "turn-1",
		"item": map[string]any{"id": "item-1", "type": "userMessage", "clientId": "msg-1", "content": []any{map[string]any{"type": "text", "text": "hello"}}},
	})
	events <- appserver.Event{Kind: appserver.EventNotification, Notification: &appserver.RPCNotification{Method: "item/completed", Params: params}}
	session := &fakeSession{endpoint: testTargetEndpoint, events: events}
	pool := NewConnectionPool(&fakeFactory{session: session}, func(string) (endpoint.Endpoint, error) { return endpoint.Endpoint{ID: testTargetEndpoint}, nil })
	stream, err := (&ObservationAdapter{Pool: pool}).Subscribe(context.Background(), testTarget())
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	event, err := stream.Next(context.Background())
	if err != nil || event.Gap || event.Item == nil {
		t.Fatalf("event = %#v, %v", event, err)
	}
	if event.Item.Text != "hello" || event.Item.ClientMessageID != "msg-1" || event.Item.NativeItemID != "item-1" {
		t.Fatalf("mapped item = %#v", event.Item)
	}
}

func TestObservationSubscribesLoadedThreadBeforeReturningStream(t *testing.T) {
	events := make(chan appserver.Event)
	close(events)
	session := &fakeSession{endpoint: testTargetEndpoint, events: events}
	pool := NewConnectionPool(&fakeFactory{session: session}, func(string) (endpoint.Endpoint, error) { return endpoint.Endpoint{ID: testTargetEndpoint}, nil })
	stream, err := (&ObservationAdapter{Pool: pool}).Subscribe(context.Background(), testTarget())
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	session.mu.Lock()
	calls := append([]string(nil), session.calls...)
	session.mu.Unlock()
	if len(calls) != 1 || calls[0] != "resume" {
		t.Fatalf("subscription established without explicit resume: %v", calls)
	}
}

func TestObservationFullHistoryUsesVisibleNativeItems(t *testing.T) {
	turnItems := []map[string]any{
		{"id": "hidden", "type": "reasoning", "text": "no"},
		{"id": "user", "type": "userMessage", "content": []any{map[string]any{"type": "text", "text": "question"}}},
		{"id": "assistant", "type": "agentMessage", "text": "answer"},
	}
	raw, _ := json.Marshal(turnItems)
	session := &fakeSession{endpoint: testTargetEndpoint, history: []codexapi.Turn{{RawObject: codexapi.RawObject{Fields: map[string]json.RawMessage{"items": raw}}, ID: "turn-1"}}}
	pool := NewConnectionPool(&fakeFactory{session: session}, func(string) (endpoint.Endpoint, error) { return endpoint.Endpoint{ID: testTargetEndpoint}, nil })
	items, err := (&ObservationAdapter{Pool: pool}).FullHistory(context.Background(), testTarget())
	if err != nil || len(items) != 2 {
		t.Fatalf("history = %#v, %v", items, err)
	}
	if items[0].Text != "question" || items[1].Text != "answer" {
		t.Fatalf("visible items = %#v", items)
	}
}

func TestObservationFallsBackToBoundedItemHistory(t *testing.T) {
	raw, _ := json.Marshal(map[string]any{"id": "user", "type": "userMessage", "content": []any{map[string]any{"type": "text", "text": "legacy"}}})
	session := &fakeSession{endpoint: testTargetEndpoint, historyErr: errors.New("thread turns unavailable"), items: []codexapi.ItemEntry{{TurnID: "turn-legacy", Item: codexapi.Item{RawObject: codexapi.RawObject{Raw: raw}}}}}
	pool := NewConnectionPool(&fakeFactory{session: session}, func(string) (endpoint.Endpoint, error) { return endpoint.Endpoint{ID: testTargetEndpoint}, nil })
	items, err := (&ObservationAdapter{Pool: pool}).FullHistory(context.Background(), testTarget())
	if err != nil || len(items) != 1 || items[0].Text != "legacy" {
		t.Fatalf("legacy item fallback = %#v, %v", items, err)
	}
}

func TestOriginalResolverExactLookupRejectsForkAncestor(t *testing.T) {
	original := mektup.Envelope{MessageID: "msg_01999999-9999-7999-8999-999999999994", Kind: mektup.KindMessage,
		FromEndpointID: testSourceEndpoint, From: "codex://source/thread/source", FromKind: "agent",
		ToEndpointID: testTargetEndpoint, To: "codex://target/thread/thread-1", RequestedTarget: "target",
		Body: "question", Provenance: "observed", SentAt: time.Now().UTC().Format(time.RFC3339Nano)}
	rendered, err := mektup.RenderEnvelope(original)
	if err != nil {
		t.Fatal(err)
	}
	observe := &historyPort{items: []service.ObservedItem{{ThreadID: "thread-1", ClientMessageID: original.MessageID, Text: string(rendered)}}}
	resolver := OriginalResolver{Observe: observe, Target: testTarget()}
	got, err := resolver.ResolveOriginal(context.Background(), original.MessageID)
	if err != nil || got.Envelope.MessageID != original.MessageID {
		t.Fatalf("exact lookup = %#v, %v", got, err)
	}

	ancestor := original
	ancestor.To = "codex://target/thread/ancestor"
	ancestor.ToEndpointID = testTargetEndpoint
	ancestorText, _ := mektup.RenderEnvelope(ancestor)
	observe.items = []service.ObservedItem{{ThreadID: "thread-1", ClientMessageID: ancestor.MessageID, Text: string(ancestorText)}}
	if _, err := resolver.ResolveOriginal(context.Background(), ancestor.MessageID); err == nil {
		t.Fatal("fork-inherited ancestor envelope resolved in descendant")
	}
}

type historyPort struct{ items []service.ObservedItem }

func (p *historyPort) Subscribe(context.Context, service.ResolvedTarget) (service.EventStream, error) {
	return nil, errors.New("not needed")
}
func (p *historyPort) FullHistory(context.Context, service.ResolvedTarget) ([]service.ObservedItem, error) {
	return p.items, nil
}
