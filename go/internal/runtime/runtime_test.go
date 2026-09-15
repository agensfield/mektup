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
	endpoint    string
	start       func(context.Context, string, string, string) (TurnResult, error)
	events      chan appserver.Event
	history     []codexapi.Turn
	items       []codexapi.ItemEntry
	historyErr  error
	mu          sync.Mutex
	calls       []string
	unsub       int
	detach      int
	nextStarted chan struct{}
	nextOnce    sync.Once
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
	s.nextOnce.Do(func() {
		if s.nextStarted != nil {
			close(s.nextStarted)
		}
	})
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
	session     *fakeSession
	open        func() *fakeSession
	openStarted chan struct{}
	openBlock   chan struct{}
	openOnce    sync.Once
	opens       int
	got         []endpoint.Endpoint
}

func (f *fakeFactory) Open(_ context.Context, ep endpoint.Endpoint) (Session, error) {
	f.opens++
	f.got = append(f.got, ep)
	f.openOnce.Do(func() {
		if f.openStarted != nil {
			close(f.openStarted)
		}
	})
	if f.openBlock != nil {
		<-f.openBlock
	}
	if f.open != nil {
		return f.open(), nil
	}
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

func TestConnectionPoolCloseIsIdempotentAndFencesReopen(t *testing.T) {
	session := &fakeSession{endpoint: testTargetEndpoint, events: make(chan appserver.Event)}
	factory := &fakeFactory{session: session}
	pool := NewConnectionPool(factory, func(string) (endpoint.Endpoint, error) { return endpoint.Endpoint{ID: testTargetEndpoint}, nil })
	if _, err := pool.session(context.Background(), testTarget()); err != nil {
		t.Fatal(err)
	}
	if err := pool.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := pool.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.session(context.Background(), testTarget()); !errors.Is(err, ErrPoolClosed) {
		t.Fatalf("reopen after close = %v", err)
	}
	if factory.opens != 1 {
		t.Fatalf("factory opened %d sessions after close", factory.opens)
	}
}

func TestConnectionPoolCanceledWaiterDoesNotBlockBehindEndpointOpen(t *testing.T) {
	session := &fakeSession{endpoint: testTargetEndpoint, events: make(chan appserver.Event)}
	started, block := make(chan struct{}), make(chan struct{})
	factory := &fakeFactory{session: session, openStarted: started, openBlock: block}
	pool := NewConnectionPool(factory, func(string) (endpoint.Endpoint, error) { return endpoint.Endpoint{ID: testTargetEndpoint}, nil })
	firstDone := make(chan error, 1)
	go func() {
		_, err := pool.session(context.Background(), testTarget())
		firstDone <- err
	}()
	<-started
	waitCtx, cancel := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() {
		_, err := pool.session(waitCtx, testTarget())
		secondDone <- err
	}()
	cancel()
	select {
	case err := <-secondDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled waiter error = %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("canceled waiter remained blocked behind endpoint setup")
	}
	close(block)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if factory.opens != 1 {
		t.Fatalf("endpoint setup duplicated: %d opens", factory.opens)
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

func TestIsolatedObserversDoNotStealOtherThreadEvents(t *testing.T) {
	makeSession := func(thread, text string) *fakeSession {
		events := make(chan appserver.Event, 1)
		params, _ := json.Marshal(map[string]any{"threadId": thread, "turnId": "turn-1", "item": map[string]any{"id": "item-" + thread, "type": "userMessage", "clientId": "msg-" + thread, "content": []any{map[string]any{"type": "text", "text": text}}}})
		events <- appserver.Event{Kind: appserver.EventNotification, Notification: &appserver.RPCNotification{Method: "item/completed", Params: params}}
		return &fakeSession{endpoint: testTargetEndpoint, events: events}
	}
	first, second := makeSession("thread-1", "one"), makeSession("thread-2", "two")
	opened := 0
	factory := &fakeFactory{open: func() *fakeSession {
		opened++
		if opened == 1 {
			return first
		}
		return second
	}}
	pool := NewConnectionPool(factory, func(string) (endpoint.Endpoint, error) { return endpoint.Endpoint{ID: testTargetEndpoint}, nil })
	adapter := &ObservationAdapter{Pool: pool}
	secondTarget := testTarget()
	secondTarget.ThreadID = "thread-2"
	secondTarget.URI = "codex://target/thread/thread-2"
	one, err := adapter.Subscribe(context.Background(), testTarget())
	if err != nil {
		t.Fatal(err)
	}
	two, err := adapter.Subscribe(context.Background(), secondTarget)
	if err != nil {
		t.Fatal(err)
	}
	defer one.Close()
	defer two.Close()
	oneEvent, err := one.Next(context.Background())
	if err != nil || oneEvent.Item == nil || oneEvent.Item.Text != "one" {
		t.Fatalf("first observer event = %#v, %v", oneEvent, err)
	}
	twoEvent, err := two.Next(context.Background())
	if err != nil || twoEvent.Item == nil || twoEvent.Item.Text != "two" {
		t.Fatalf("second observer event = %#v, %v", twoEvent, err)
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

func TestEventStreamConcurrentNextAndCloseIsIdempotent(t *testing.T) {
	nextStarted := make(chan struct{})
	session := &fakeSession{endpoint: testTargetEndpoint, events: make(chan appserver.Event), nextStarted: nextStarted}
	pool := NewConnectionPool(&fakeFactory{session: session}, func(string) (endpoint.Endpoint, error) { return endpoint.Endpoint{ID: testTargetEndpoint}, nil })
	stream, err := (&ObservationAdapter{Pool: pool}).Subscribe(context.Background(), testTarget())
	if err != nil {
		t.Fatal(err)
	}
	nextCtx, cancel := context.WithCancel(context.Background())
	nextDone := make(chan error, 1)
	go func() {
		_, nextErr := stream.Next(nextCtx)
		nextDone <- nextErr
	}()
	<-nextStarted
	closeDone := make(chan error, 1)
	go func() { closeDone <- stream.Close() }()
	cancel()
	if err := <-closeDone; err != nil {
		t.Fatalf("close = %v", err)
	}
	if err := <-nextDone; err == nil {
		t.Fatal("Next returned without cancellation or a terminal event")
	}
	if err := stream.Close(); err != nil {
		t.Fatalf("second close = %v", err)
	}
}

func TestObservationCloseDoesNotDetachPooledDeliverySession(t *testing.T) {
	deliverySession := &fakeSession{endpoint: testTargetEndpoint, events: make(chan appserver.Event)}
	observerSession := &fakeSession{endpoint: testTargetEndpoint, events: make(chan appserver.Event)}
	opened := 0
	factory := &fakeFactory{open: func() *fakeSession {
		opened++
		if opened == 1 {
			return deliverySession
		}
		return observerSession
	}}
	pool := NewConnectionPool(factory, func(string) (endpoint.Endpoint, error) { return endpoint.Endpoint{ID: testTargetEndpoint}, nil })
	delivery := &DeliveryAdapter{Pool: pool}
	if _, err := delivery.Send(context.Background(), testTarget(), "body", "msg_01999999-9999-7999-8999-999999999998"); err != nil {
		t.Fatal(err)
	}
	stream, err := (&ObservationAdapter{Pool: pool}).Subscribe(context.Background(), testTarget())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	deliverySession.mu.Lock()
	detach := deliverySession.detach
	deliverySession.mu.Unlock()
	observerSession.mu.Lock()
	observerDetach := observerSession.detach
	observerSession.mu.Unlock()
	if detach != 0 || observerDetach != 1 {
		t.Fatalf("observer lifetime: pooled delivery detach=%d observer detach=%d", detach, observerDetach)
	}
}

func TestPooledSubscriptionReferenceCountingProtectsConcurrentUsers(t *testing.T) {
	// Delivery leases share one pooled session. Observer streams use an
	// isolated session and therefore do not participate in this count.
	deliverySession := &fakeSession{endpoint: testTargetEndpoint, events: make(chan appserver.Event)}
	observerSession := &fakeSession{endpoint: testTargetEndpoint, events: make(chan appserver.Event)}
	opened := 0
	factory := &fakeFactory{open: func() *fakeSession {
		opened++
		if opened == 1 {
			return deliverySession
		}
		return observerSession
	}}
	pool := NewConnectionPool(factory, func(string) (endpoint.Endpoint, error) { return endpoint.Endpoint{ID: testTargetEndpoint}, nil })
	delivery := &DeliveryAdapter{Pool: pool}
	if _, err := delivery.Resume(context.Background(), testTarget()); err != nil {
		t.Fatal(err)
	}
	stream, err := (&ObservationAdapter{Pool: pool}).Subscribe(context.Background(), testTarget())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	deliverySession.mu.Lock()
	resumeCalls := 0
	for _, call := range deliverySession.calls {
		if call == "resume" {
			resumeCalls++
		}
	}
	unsub := deliverySession.unsub
	deliverySession.mu.Unlock()
	if resumeCalls != 1 || unsub != 0 {
		t.Fatalf("delivery lease changed by observer: resumes=%d unsubscribes=%d", resumeCalls, unsub)
	}
	if err := delivery.Detach(context.Background(), testTarget()); err != nil {
		t.Fatal(err)
	}
	deliverySession.mu.Lock()
	unsub = deliverySession.unsub
	deliverySession.mu.Unlock()
	if unsub != 1 {
		t.Fatalf("final subscription release unsubscribes=%d, want one", unsub)
	}
}

func TestObservationFullHistoryUsesVisibleNativeItems(t *testing.T) {
	turnItems := []map[string]any{
		{"id": "hidden", "type": "reasoning", "text": "no"},
		{"id": "user", "type": "userMessage", "clientId": nil, "content": []any{map[string]any{"type": "text", "text": "question"}}},
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
	raw, _ := json.Marshal(map[string]any{"id": "user", "type": "userMessage", "clientId": nil, "content": []any{map[string]any{"type": "text", "text": "legacy"}}})
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

func TestOriginalResolverConflictsOnChangedReplyRoute(t *testing.T) {
	original := mektup.Envelope{MessageID: "msg_01999999-9999-7999-8999-999999999999", Kind: mektup.KindMessage,
		FromEndpointID: testSourceEndpoint, From: "codex://source/thread/source", FromKind: "agent",
		ToEndpointID: testTargetEndpoint, To: "codex://target/thread/thread-1", RequestedTarget: "target",
		ReplyRequested: true, ReplyEndpointID: testSourceEndpoint, ReplyTo: "codex://source/thread/source",
		ReplyCustodyEndpointID: testSourceEndpoint, ReplyCustodyStoreID: "store_01999999-9999-7999-8999-999999999991",
		Body: "question", Provenance: "observed", SentAt: time.Now().UTC().Format(time.RFC3339Nano)}
	first, err := mektup.RenderEnvelope(original)
	if err != nil {
		t.Fatal(err)
	}
	secondEnvelope := original
	secondEnvelope.ReplyTo = "codex://source/thread/other"
	second, err := mektup.RenderEnvelope(secondEnvelope)
	if err != nil {
		t.Fatal(err)
	}
	resolver := OriginalResolver{Observe: &historyPort{items: []service.ObservedItem{
		{ThreadID: "thread-1", ClientMessageID: original.MessageID, Text: string(first)},
		{ThreadID: "thread-1", ClientMessageID: original.MessageID, Text: string(second)},
	}}, Target: testTarget()}
	if _, err := resolver.ResolveOriginal(context.Background(), original.MessageID); !errors.Is(err, service.ErrOriginalIdentityConflict) {
		t.Fatalf("changed reply route did not conflict: %v", err)
	}
}

func TestVisibleItemRejectsMalformedPinnedShapes(t *testing.T) {
	for _, raw := range []string{
		`{"type":"userMessage","content":[]}`,
		`{"id":"u","type":"userMessage","content":[]}`,
		`{"id":"u","type":"userMessage","clientId":42,"content":[]}`,
		`{"id":"u","type":"userMessage","content":["body"]}`,
		`{"id":"u","type":"userMessage","clientId":null,"content":[{"type":"future"}]}`,
	} {
		if _, ok := visibleItem(json.RawMessage(raw), "thread-1", "turn-1", ""); ok {
			t.Fatalf("malformed native item accepted: %s", raw)
		}
	}
	valid, ok := visibleItem(json.RawMessage(`{"id":"u","type":"userMessage","clientId":"msg-1","text":"phantom","content":[{"type":"text","text":"body"}]}`), "thread-1", "turn-1", "")
	if !ok || valid.NativeType != "userMessage" || valid.ClientMessageID != "msg-1" || valid.Text != "body" {
		t.Fatalf("valid native user item rejected: %#v", valid)
	}
}

func TestOriginalResolverRejectsAssistantAuthoredEnvelope(t *testing.T) {
	envelope := mektup.Envelope{MessageID: "msg_01999999-9999-7999-8999-999999999998", Kind: mektup.KindMessage,
		FromEndpointID: testSourceEndpoint, From: "codex://source/thread/source", FromKind: "agent",
		ToEndpointID: testTargetEndpoint, To: "codex://target/thread/thread-1", RequestedTarget: "target",
		Body: "quoted", Provenance: "observed", SentAt: time.Now().UTC().Format(time.RFC3339Nano)}
	raw, err := mektup.RenderEnvelope(envelope)
	if err != nil {
		t.Fatal(err)
	}
	resolver := OriginalResolver{Observe: &historyPort{items: []service.ObservedItem{{ThreadID: "thread-1", NativeType: "agentMessage", Text: string(raw)}}}, Target: testTarget()}
	if _, err := resolver.ResolveOriginal(context.Background(), envelope.MessageID); err == nil {
		t.Fatal("assistant-authored envelope resolved as delivered original")
	}
}

type historyPort struct{ items []service.ObservedItem }

func (p *historyPort) Subscribe(context.Context, service.ResolvedTarget) (service.EventStream, error) {
	return nil, errors.New("not needed")
}
func (p *historyPort) FullHistory(context.Context, service.ResolvedTarget) ([]service.ObservedItem, error) {
	return p.items, nil
}
