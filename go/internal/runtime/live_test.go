package runtime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
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
	t.Logf("dedicated live threads source=%s target=%s", source.Thread.ID, target.Thread.ID)

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
	targetResolved := service.ResolvedTarget{EndpointID: endpointID, URI: targetURI, ThreadID: target.Thread.ID, Loaded: true, Persistent: true}
	senderResolver := liveResolver{source: service.SourceIdentity{EndpointID: endpointID, URI: sourceURI, CustodyEndpointID: endpointID, CustodyStoreID: store.StoreID()}, target: targetResolved}
	receiverResolver := liveResolver{source: service.SourceIdentity{EndpointID: endpointID, URI: targetURI, CustodyEndpointID: endpointID, CustodyStoreID: store.StoreID()}, target: targetResolved, replyURI: sourceURI}
	observe := &ObservationAdapter{Pool: pool}
	gatedObserve := &custodyFirstObservation{inner: observe, release: make(chan struct{})}
	sender := &service.Service{Resolver: senderResolver, Delivery: &DeliveryAdapter{Pool: pool}, Journal: journalAdapter, Observe: gatedObserve}
	receiver := &service.Service{Resolver: receiverResolver, Delivery: &DeliveryAdapter{Pool: pool}, Journal: journalAdapter, Observe: observe}
	originalResolver := OriginalResolver{Observe: observe, Target: targetResolved}

	started := time.Now()
	sendDone := make(chan liveSendOutcome, 1)
	go func() {
		result, sendErr := sender.Send(ctx, service.SendRequest{Target: targetURI, Body: "a dedicated live body-carry probe", Source: sourceURI, Wait: true, WaitTimeout: 15 * time.Second})
		sendDone <- liveSendOutcome{result: result, err: sendErr}
	}()

	original, err := discoverLiveOriginal(ctx, 10*time.Second, observe, targetResolved, originalResolver, sendDone, "a dedicated live body-carry probe")
	if err != nil {
		t.Fatalf("dedicated original did not become visible: %v", err)
	}
	replyResult, err := receiver.Reply(ctx, originalResolver, service.ReplyRequest{Reference: original.Envelope.MessageID, Body: "one dedicated live reply"})
	if err != nil {
		t.Fatalf("reply delivery: %v", err)
	}
	replyID := replyResult.Receipt.Message.MessageID
	if replyID == "" {
		t.Fatal("reply receipt omitted message identity")
	}
	var sendOutcome liveSendOutcome
	select {
	case outcome := <-sendDone:
		sendOutcome = outcome
		logLiveSendOutcome(t, outcome)
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
	if sendOutcome.result.Wait.ReplyID != replyID || replyResult.Receipt.Message.InReplyTo != original.Envelope.MessageID {
		t.Fatalf("custody reply ID %q differs from delivered reply %q", sendOutcome.result.Wait.ReplyID, replyID)
	}
	originalCount := 0
	targetItems, historyErr := mustHistory(ctx, observe, targetResolved)
	if historyErr != nil {
		t.Fatalf("target history verification: %v", historyErr)
	}
	for _, item := range targetItems {
		if item.NativeType != "userMessage" || item.ClientMessageID != original.Envelope.MessageID {
			continue
		}
		parsed, parseErr := mektup.ParseEnvelopeString(item.Text)
		if parseErr == nil && parsed.Kind == mektup.KindMessage && parsed.MessageID == original.Envelope.MessageID && parsed.FromEndpointID == endpointID && parsed.ToEndpointID == endpointID && parsed.From == sourceURI && parsed.To == targetURI && parsed.Body == original.Envelope.Body && parsed.PayloadSHA256 == original.Envelope.PayloadSHA256 {
			originalCount++
		}
	}
	if originalCount != 1 {
		t.Fatalf("native target history contains %d exact original user messages, want one", originalCount)
	}
	exactReplyCount := 0
	for _, item := range items {
		if item.NativeType != "userMessage" || item.ClientMessageID != replyID {
			continue
		}
		parsed, parseErr := mektup.ParseEnvelopeString(item.Text)
		if parseErr == nil && parsed.Kind == mektup.KindReply && parsed.MessageID == replyID && parsed.FromEndpointID == endpointID && parsed.ToEndpointID == endpointID && parsed.From == targetURI && parsed.To == sourceURI && parsed.InReplyTo == original.Envelope.MessageID && parsed.Body == "one dedicated live reply" && parsed.PayloadSHA256 == replyResult.Receipt.Message.PayloadSHA256 && parsed.PayloadBytes == replyResult.Receipt.Message.PayloadBytes {
			exactReplyCount++
		}
	}
	if exactReplyCount != 1 {
		t.Fatalf("native source history contains %d exact correlated reply user messages, want one", exactReplyCount)
	}
}

func mustHistory(ctx context.Context, observe service.ObservationPort, target service.ResolvedTarget) ([]service.ObservedItem, error) {
	return observe.FullHistory(ctx, target)
}

func TestMustHistoryHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := mustHistory(ctx, &slowHistoryObservation{started: make(chan struct{})}, service.ResolvedTarget{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("history cancellation error = %v", err)
	}
}

type liveSendOutcome struct {
	result service.SendResult
	err    error
}

type liveHistoryResult struct {
	items []service.ObservedItem
	err   error
}

func logLiveSendOutcome(t *testing.T, outcome liveSendOutcome) {
	t.Helper()
	t.Logf("send metadata operation=%s message=%s state=%s", outcome.result.Receipt.OperationID, outcome.result.Receipt.Message.MessageID, outcome.result.Receipt.State)
	if outcome.result.Wait != nil {
		t.Logf("wait metadata state=%s reply=%s", outcome.result.Wait.State, outcome.result.Wait.ReplyID)
	}
}

func discoverLiveOriginal(ctx context.Context, timeout time.Duration, observe service.ObservationPort, target service.ResolvedTarget, resolver OriginalResolver, sendDone <-chan liveSendOutcome, expectedBody string) (service.OriginalMessage, error) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	scanCtx, scanCancel := context.WithCancel(ctx)
	workerDone := make(chan struct{})
	defer func() {
		scanCancel()
		<-workerDone
	}()
	historyCh := make(chan liveHistoryResult, 1)
	go func() {
		defer close(workerDone)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			items, historyErr := observe.FullHistory(scanCtx, target)
			select {
			case historyCh <- liveHistoryResult{items: items, err: historyErr}:
			case <-scanCtx.Done():
				return
			}
			select {
			case <-ticker.C:
			case <-scanCtx.Done():
				return
			}
		}
	}()
	sendCh := sendDone
	for {
		select {
		case outcome := <-sendCh:
			sendCh = nil
			if outcome.err != nil {
				return service.OriginalMessage{}, fmt.Errorf("send failed before original discovery operation=%s message=%s state=%s: %w", outcome.result.Receipt.OperationID, outcome.result.Receipt.Message.MessageID, outcome.result.Receipt.State, outcome.err)
			}
			return service.OriginalMessage{}, fmt.Errorf("send completed before original discovery operation=%s message=%s state=%s", outcome.result.Receipt.OperationID, outcome.result.Receipt.Message.MessageID, outcome.result.Receipt.State)
		case history := <-historyCh:
			if history.err == nil {
				for _, item := range history.items {
					parsed, parseErr := mektup.ParseEnvelopeString(item.Text)
					if parseErr != nil || parsed.Kind != mektup.KindMessage || parsed.Body != expectedBody {
						continue
					}
					if original, resolveErr := resolver.resolveItems(parsed.MessageID, history.items); resolveErr == nil {
						return original, nil
					}
				}
			}
		case <-ctx.Done():
			return service.OriginalMessage{}, ctx.Err()
		case <-deadline.C:
			return service.OriginalMessage{}, errors.New("timed out discovering dedicated original")
		}
	}
}

func TestLiveOriginalDiscoveryReportsEarlySendFailure(t *testing.T) {
	sendErr := errors.New("pre-write failure")
	sendDone := make(chan liveSendOutcome, 1)
	sendDone <- liveSendOutcome{err: sendErr}
	_, err := discoverLiveOriginal(context.Background(), time.Second, &historyPort{}, service.ResolvedTarget{}, OriginalResolver{}, sendDone, "body")
	if !errors.Is(err, sendErr) {
		t.Fatalf("early send error = %v", err)
	}
}

func TestLiveOriginalDiscoveryDoesNotOverlapSlowHistoryScans(t *testing.T) {
	observe := &slowHistoryObservation{started: make(chan struct{})}
	sendDone := make(chan liveSendOutcome, 1)
	go func() {
		time.Sleep(20 * time.Millisecond)
		sendDone <- liveSendOutcome{err: errors.New("early send failure")}
	}()
	started := time.Now()
	_, err := discoverLiveOriginal(context.Background(), time.Second, observe, service.ResolvedTarget{}, OriginalResolver{}, sendDone, "body")
	if err == nil || time.Since(started) > 200*time.Millisecond {
		t.Fatalf("slow discovery did not fail promptly: elapsed=%s err=%v", time.Since(started), err)
	}
	select {
	case <-observe.started:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("history worker did not start")
	}
	deadline := time.NewTimer(200 * time.Millisecond)
	defer deadline.Stop()
	for observe.inFlight.Load() != 0 {
		select {
		case <-deadline.C:
			t.Fatal("slow history worker remained in flight after cancellation")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if observe.maxInFlight.Load() > 1 {
		t.Fatalf("overlapping history scans: max=%d", observe.maxInFlight.Load())
	}
}

type slowHistoryObservation struct {
	inFlight    atomic.Int32
	maxInFlight atomic.Int32
	started     chan struct{}
	startedOnce sync.Once
}

func (o *slowHistoryObservation) Subscribe(context.Context, service.ResolvedTarget) (service.EventStream, error) {
	return nil, errors.New("not used")
}

func (o *slowHistoryObservation) FullHistory(ctx context.Context, _ service.ResolvedTarget) ([]service.ObservedItem, error) {
	current := o.inFlight.Add(1)
	for {
		max := o.maxInFlight.Load()
		if current <= max || o.maxInFlight.CompareAndSwap(max, current) {
			break
		}
	}
	o.startedOnce.Do(func() { close(o.started) })
	defer o.inFlight.Add(-1)
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

type liveResolver struct {
	source   service.SourceIdentity
	target   service.ResolvedTarget
	replyURI string
}

func TestReceiverResolverAcceptsOriginalReturnRoute(t *testing.T) {
	ep := "ep_01999999-9999-7999-8999-999999999995"
	r := liveResolver{source: service.SourceIdentity{EndpointID: ep, URI: "codex://live/thread/target"}, target: service.ResolvedTarget{EndpointID: ep, URI: "codex://live/thread/target", ThreadID: "target", Loaded: true}}
	if _, err := r.ResolvePinned(context.Background(), ep, "codex://live/thread/source"); err != nil {
		t.Fatalf("receiver config rejects original return route: %v", err)
	}
}

func (r liveResolver) Resolve(context.Context, string) (service.ResolvedTarget, error) {
	return r.target, nil
}
func (r liveResolver) ResolveSource(context.Context, string) (service.SourceIdentity, error) {
	return r.source, nil
}
func (r liveResolver) ResolvePinned(_ context.Context, endpointID, uri string) (service.ResolvedTarget, error) {
	if endpointID != r.source.EndpointID {
		return service.ResolvedTarget{}, errors.New("pinned source mismatch")
	}
	if r.replyURI != "" && uri != r.replyURI {
		return service.ResolvedTarget{}, errors.New("pinned reply destination mismatch")
	}
	return service.ResolvedTarget{EndpointID: endpointID, URI: uri, ThreadID: uri[strings.LastIndex(uri, "/")+1:], Loaded: true, Persistent: true}, nil
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
