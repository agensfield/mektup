package runtime

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	mektup "github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/internal/codexapi"
	"github.com/agensfield/mektup/go/internal/connection"
	"github.com/agensfield/mektup/go/internal/endpoint"
	"github.com/agensfield/mektup/go/internal/journal"
	"github.com/agensfield/mektup/go/internal/service"
)

// TestLiveBodyCarryAcceptance is deliberately opt-in. It creates two fresh
// persistent threads and sends exactly one standard envelope between them;
// without MEKTUP_ACCEPT_BODY=1 it cannot mutate the user's daemon or threads.
func TestLiveBodyCarryAcceptance(t *testing.T) {
	if os.Getenv("MEKTUP_ACCEPT_BODY") != "1" {
		t.Skip("set MEKTUP_ACCEPT_BODY=1 to run dedicated live body-carry acceptance")
	}
	socket := os.Getenv("MEKTUP_ACCEPT_SOCKET")
	if socket == "" {
		t.Skip("set MEKTUP_ACCEPT_SOCKET to the already-running daemon socket")
	}
	route, err := endpoint.UnixRoute(socket)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	admin, err := connection.Connect(ctx, route, connection.Options{ClientName: "mektup-live-body-acceptance", ClientVersion: "1.0.0-dev", HandshakeTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = admin.Detach(closeCtx)
		closeCancel()
	}()
	api := codexapi.New(admin, codexapi.Options{Capabilities: admin.Capabilities()})
	source, err := api.ThreadStart(ctx, codexapi.StartOptions{})
	if err != nil {
		t.Fatalf("create dedicated source thread: %v", err)
	}
	target, err := api.ThreadStart(ctx, codexapi.StartOptions{})
	if err != nil {
		t.Fatalf("create dedicated target thread: %v", err)
	}
	if source.Thread.ID == "" || target.Thread.ID == "" || source.Thread.ID == target.Thread.ID {
		t.Fatalf("dedicated thread IDs = %q, %q", source.Thread.ID, target.Thread.ID)
	}

	endpointID := "ep_01999999-9999-7999-8999-999999999995"
	ep := endpoint.Endpoint{ID: endpointID, Alias: "live", Route: route, Herdr: endpoint.HerdrDisabled}
	factory := ConnectionFactory{Options: connection.Options{ClientName: "mektup-live-body-adapter", ClientVersion: "1.0.0-dev", HandshakeTimeout: 5 * time.Second}}
	pool := NewConnectionPool(factory, func(id string) (endpoint.Endpoint, error) {
		if id != endpointID {
			return endpoint.Endpoint{}, errors.New("unexpected endpoint identity")
		}
		return ep, nil
	})
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 3*time.Second)
		_ = pool.Close(closeCtx)
		closeCancel()
	}()
	store, err := journal.Open(ctx, journal.Options{StateDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	registry := service.NewMemoryIdentityRegistry()
	journalAdapter, err := NewJournalAdapter(store, registry)
	if err != nil {
		t.Fatal(err)
	}
	sourceURI := "codex://live/thread/" + source.Thread.ID
	targetURI := "codex://live/thread/" + target.Thread.ID
	resolver := liveResolver{source: service.SourceIdentity{EndpointID: endpointID, URI: sourceURI, CustodyEndpointID: endpointID, CustodyStoreID: store.StoreID()}, target: service.ResolvedTarget{EndpointID: endpointID, URI: targetURI, ThreadID: target.Thread.ID, Loaded: false, Persistent: true}}
	observe := &ObservationAdapter{Pool: pool}
	gatedObserve := &custodyFirstObservation{inner: observe, release: make(chan struct{})}
	sender := &service.Service{Resolver: resolver, Delivery: &DeliveryAdapter{Pool: pool}, Journal: journalAdapter, Observe: gatedObserve}
	receiver := &service.Service{Resolver: resolver, Delivery: &DeliveryAdapter{Pool: pool}, Journal: journalAdapter, Observe: observe}
	originalResolver := OriginalResolver{Observe: observe, Target: resolver.target}

	started := time.Now()
	sendDone := make(chan struct {
		result service.SendResult
		err    error
	}, 1)
	go func() {
		result, sendErr := sender.Send(ctx, service.SendRequest{Target: targetURI, Body: "a dedicated live body-carry probe", Source: sourceURI, Wait: true, WaitTimeout: 15 * time.Second})
		sendDone <- struct {
			result service.SendResult
			err    error
		}{result: result, err: sendErr}
	}()

	var original service.OriginalMessage
	if err := eventually(ctx, 10*time.Second, func() error {
		items, historyErr := observe.FullHistory(ctx, resolver.target)
		if historyErr != nil {
			return historyErr
		}
		for _, item := range items {
			parsed, parseErr := mektup.ParseEnvelopeString(item.Text)
			if parseErr != nil || parsed.Kind != mektup.KindMessage || parsed.Body != "a dedicated live body-carry probe" {
				continue
			}
			var resolveErr error
			original, resolveErr = originalResolver.ResolveOriginal(ctx, parsed.MessageID)
			if resolveErr == nil {
				return nil
			}
		}
		return errors.New("dedicated original not visible")
	}); err != nil {
		t.Fatalf("dedicated original did not become visible: %v", err)
	}
	if _, err := receiver.Reply(ctx, originalResolver, service.ReplyRequest{Reference: original.Envelope.MessageID, Body: "one dedicated live reply"}); err != nil {
		t.Fatalf("reply delivery: %v", err)
	}
	select {
	case outcome := <-sendDone:
		if outcome.err != nil {
			t.Fatalf("sender wait did not wake from custody: %v", outcome.err)
		}
		if outcome.result.Wait == nil || outcome.result.Wait.ReplyID == "" {
			t.Fatalf("sender wait result lacks reply metadata: %#v", outcome.result.Wait)
		}
		if time.Since(started) > 10*time.Second {
			t.Fatalf("custody wake took too long: %s", time.Since(started))
		}
		close(gatedObserve.release)
	case <-ctx.Done():
		t.Fatal("sender wait did not wake before native observation")
	}

	sourceTarget := service.ResolvedTarget{EndpointID: endpointID, URI: sourceURI, ThreadID: source.Thread.ID, Loaded: true, Persistent: true}
	items, err := eventuallyItems(ctx, 10*time.Second, observe, sourceTarget)
	if err != nil {
		t.Fatal(err)
	}
	replyCount := 0
	for _, item := range items {
		parsed, parseErr := mektup.ParseEnvelopeString(item.Text)
		if parseErr == nil && parsed.Kind == mektup.KindReply && parsed.InReplyTo == original.Envelope.MessageID && parsed.Body == "one dedicated live reply" {
			replyCount++
		}
	}
	if replyCount != 1 {
		t.Fatalf("native source history contains %d exact reply bodies, want one", replyCount)
	}
}

type liveResolver struct {
	source service.SourceIdentity
	target service.ResolvedTarget
}

func (r liveResolver) Resolve(context.Context, string) (service.ResolvedTarget, error) {
	return r.target, nil
}
func (r liveResolver) ResolveSource(context.Context, string) (service.SourceIdentity, error) {
	return r.source, nil
}
func (r liveResolver) ResolvePinned(_ context.Context, endpointID, uri string) (service.ResolvedTarget, error) {
	if endpointID != r.source.EndpointID || uri != r.source.URI {
		return service.ResolvedTarget{}, errors.New("pinned source mismatch")
	}
	return service.ResolvedTarget{EndpointID: endpointID, URI: uri, ThreadID: r.source.URI[strings.LastIndex(r.source.URI, "/")+1:], Loaded: true, Persistent: true}, nil
}

type custodyFirstObservation struct {
	inner   service.ObservationPort
	release chan struct{}
}

func (o *custodyFirstObservation) Subscribe(ctx context.Context, target service.ResolvedTarget) (service.EventStream, error) {
	stream, err := o.inner.Subscribe(ctx, target)
	if err != nil {
		return nil, err
	}
	return &custodyFirstStream{inner: stream, ctx: ctx, release: o.release}, nil
}
func (o *custodyFirstObservation) FullHistory(ctx context.Context, target service.ResolvedTarget) ([]service.ObservedItem, error) {
	select {
	case <-o.release:
		return o.inner.FullHistory(ctx, target)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type custodyFirstStream struct {
	inner   service.EventStream
	ctx     context.Context
	release chan struct{}
}

func (s *custodyFirstStream) Next(ctx context.Context) (service.Event, error) {
	for {
		event, err := s.inner.Next(ctx)
		if err != nil {
			return service.Event{}, err
		}
		if event.Item == nil {
			return event, nil
		}
		select {
		case <-s.release:
			return event, nil
		case <-s.ctx.Done():
			return service.Event{}, s.ctx.Err()
		case <-ctx.Done():
			return service.Event{}, ctx.Err()
		}
	}
}
func (s *custodyFirstStream) Close() error { return s.inner.Close() }

func eventually(ctx context.Context, timeout time.Duration, fn func() error) error {
	deadline := time.NewTimer(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer deadline.Stop()
	defer ticker.Stop()
	for {
		if err := fn(); err == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return errors.New("timed out")
		case <-ticker.C:
		}
	}
}

func eventuallyItems(ctx context.Context, timeout time.Duration, observe service.ObservationPort, target service.ResolvedTarget) ([]service.ObservedItem, error) {
	var items []service.ObservedItem
	err := eventually(ctx, timeout, func() error {
		var err error
		items, err = observe.FullHistory(ctx, target)
		if err != nil {
			return err
		}
		for _, item := range items {
			if parsed, parseErr := mektup.ParseEnvelopeString(item.Text); parseErr == nil && parsed.Kind == mektup.KindReply {
				return nil
			}
		}
		return errors.New("reply not visible")
	})
	return items, err
}
