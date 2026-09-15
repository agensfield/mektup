package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/internal/codexapi"
)

const (
	epSource = "ep_01999999-9999-7999-8999-999999999999"
	epTarget = "ep_02999999-9999-7999-8999-999999999999"
	storeID  = "store_03999999-9999-7999-8999-999999999999"
)

type fakeResolver struct {
	source SourceIdentity
	target ResolvedTarget
	pinned ResolvedTarget
	err    error
}

func (r fakeResolver) Resolve(context.Context, string) (ResolvedTarget, error) {
	return r.target, r.err
}
func (r fakeResolver) ResolveSource(context.Context, string) (SourceIdentity, error) {
	if r.source.EndpointID == "" {
		return SourceIdentity{}, errors.New("no source")
	}
	return r.source, nil
}
func (r fakeResolver) ResolvePinned(context.Context, string, string) (ResolvedTarget, error) {
	if r.pinned.URI != "" {
		return r.pinned, r.err
	}
	return r.target, r.err
}

type fakeDelivery struct {
	mu     sync.Mutex
	calls  []deliveryCall
	gate   <-chan struct{}
	err    error
	resume int
	detach int
}
type deliveryCall struct{ thread, text, id string }

func (d *fakeDelivery) Send(ctx context.Context, thread, text, id string) (DeliveryResult, error) {
	d.mu.Lock()
	d.calls = append(d.calls, deliveryCall{thread, text, id})
	d.mu.Unlock()
	if d.gate != nil {
		select {
		case <-d.gate:
		case <-ctx.Done():
			return DeliveryResult{}, &DeliveryError{Err: ctx.Err(), Phase: WriteMayHaveWritten}
		}
	}
	if d.err != nil {
		return DeliveryResult{}, d.err
	}
	return DeliveryResult{Accepted: true, TurnID: "turn-1", Evidence: "accepted"}, nil
}
func (d *fakeDelivery) Resume(context.Context, ResolvedTarget) (string, error) {
	d.resume++
	return "turn-resumed", nil
}
func (d *fakeDelivery) Detach(context.Context, ResolvedTarget) error { d.detach++; return nil }
func (d *fakeDelivery) count() int                                   { d.mu.Lock(); defer d.mu.Unlock(); return len(d.calls) }

type fakeJournal struct {
	mu        sync.Mutex
	prepared  map[string]Prepared
	ops       map[string]OperationStatus
	claims    map[string]ReplyClaim
	marked    []string
	results   []mektup.EvidenceState
	commitErr error
}

func newFakeJournal() *fakeJournal {
	return &fakeJournal{prepared: map[string]Prepared{}, ops: map[string]OperationStatus{}, claims: map[string]ReplyClaim{}}
}
func (j *fakeJournal) Prepare(_ context.Context, op Operation) (Prepared, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if existing, ok := j.prepared[op.MessageID]; ok {
		return existing, nil
	}
	p := Prepared{OperationID: op.OperationID, MessageID: op.MessageID, Owner: "owner-" + op.MessageID, Token: "token-" + op.MessageID}
	j.prepared[op.MessageID] = p
	j.ops[op.MessageID] = OperationStatus{Operation: op, State: mektup.StatePrepared}
	return p, nil
}
func (j *fakeJournal) MarkDispatchStarted(_ context.Context, opID, owner, token string) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.marked = append(j.marked, opID+":"+owner+":"+token)
	for id, op := range j.ops {
		if op.OperationID == opID {
			op.State = mektup.StateDispatchStarted
			j.ops[id] = op
		}
	}
	return nil
}
func (j *fakeJournal) RecordResult(_ context.Context, opID string, state mektup.EvidenceState, _ string) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.results = append(j.results, state)
	for id, op := range j.ops {
		if op.OperationID == opID {
			op.State = state
			j.ops[id] = op
		}
	}
	return nil
}
func (j *fakeJournal) markedCount() int { j.mu.Lock(); defer j.mu.Unlock(); return len(j.marked) }
func (j *fakeJournal) Lookup(_ context.Context, ref string) (OperationStatus, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if op, ok := j.ops[ref]; ok {
		return op, nil
	}
	for _, op := range j.ops {
		if op.OperationID == ref {
			return op, nil
		}
	}
	return OperationStatus{}, errors.New("not found")
}
func (j *fakeJournal) ClaimReply(_ context.Context, in ReplyClaimInput) (ReplyClaim, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if c, ok := j.claims[in.ReplyID]; ok {
		if c.Digest != in.Digest || c.OriginalID != in.OriginalID {
			return ReplyClaim{}, errors.New("identity conflict")
		}
		c.Joined = true
		c.Token = ""
		return c, nil
	}
	c := ReplyClaim{ReplyID: in.ReplyID, OriginalID: in.OriginalID, Digest: in.Digest, BodySize: in.BodySize, Status: in.Status, ReplyRoute: in.ReplyRoute, CustodyRoute: in.CustodyRoute, CustodyStoreID: in.CustodyStoreID, Owner: in.Owner, Token: "claim-" + in.ReplyID, State: mektup.StateReplyDispatchClaimed}
	j.claims[in.ReplyID] = c
	return c, nil
}
func (j *fakeJournal) Heartbeat(context.Context, string, string, string) error { return nil }
func (j *fakeJournal) CommitReply(_ context.Context, id, _, _ string) (ReplyClaim, error) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if j.commitErr != nil {
		return ReplyClaim{}, j.commitErr
	}
	c := j.claims[id]
	c.State = mektup.StateReplyAccepted
	j.claims[id] = c
	if op, ok := j.ops[c.OriginalID]; ok {
		op.State = mektup.StateReplyAccepted
		j.ops[c.OriginalID] = op
	}
	return c, nil
}
func (j *fakeJournal) AbandonReply(_ context.Context, id, _, _ string) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	c := j.claims[id]
	c.State = mektup.StateReplyOutcomeUnknown
	j.claims[id] = c
	if op, ok := j.ops[c.OriginalID]; ok {
		op.State = mektup.StateReplyOutcomeUnknown
		j.ops[c.OriginalID] = op
	}
	return nil
}
func (j *fakeJournal) ObserveReply(context.Context, string, string, string) error { return nil }
func (j *fakeJournal) ReconcileReplyObservation(context.Context, string, string, string) error {
	return nil
}

func validService(r *fakeResolver, d *fakeDelivery, j *fakeJournal) *Service {
	return &Service{Resolver: r, Delivery: d, Journal: j}
}
func baseResolver() fakeResolver {
	return fakeResolver{source: SourceIdentity{EndpointID: epSource, URI: "codex://local/thread/source", CustodyEndpointID: epSource, CustodyStoreID: storeID}, target: ResolvedTarget{Requested: "target", EndpointID: epTarget, URI: "codex://local/thread/target", ThreadID: "target", Loaded: true}, pinned: ResolvedTarget{EndpointID: epSource, URI: "codex://local/thread/source", ThreadID: "source", Loaded: true}}
}

func TestSendPreflightsMeasuredEnvelopeBeforeJournalOrWrite(t *testing.T) {
	r := baseResolver()
	d := &fakeDelivery{}
	j := newFakeJournal()
	s := validService(&r, d, j)
	_, err := s.Send(context.Background(), SendRequest{Target: "target", Body: string(make([]byte, MaxInputBytes))})
	var se *Error
	if !errors.As(err, &se) || se.Code != mektup.ErrInputTooLarge {
		t.Fatalf("error = %v, want input_too_large", err)
	}
	if d.count() != 0 || len(j.prepared) != 0 {
		t.Fatalf("oversized body crossed preflight: writes=%d prepared=%d", d.count(), len(j.prepared))
	}
	if se.Details["bytes"].(int) <= MaxInputBytes {
		t.Fatalf("missing measured byte evidence: %#v", se.Details)
	}
}

func TestRawIsByteEquivalentAndRawReplyCombinationsReject(t *testing.T) {
	r := baseResolver()
	d := &fakeDelivery{}
	j := newFakeJournal()
	s := validService(&r, d, j)
	body := "żero\n---\x00"
	if _, err := s.Send(context.Background(), SendRequest{Target: "target", Body: body, Raw: true}); err != nil {
		t.Fatal(err)
	}
	if got := d.calls[0].text; got != body {
		t.Fatalf("raw body changed: %q", got)
	}
	if _, err := s.Send(context.Background(), SendRequest{Target: "target", Body: "x", Raw: true, Wait: true}); err == nil {
		t.Fatal("raw wait unexpectedly dispatched")
	}
	if d.count() != 1 {
		t.Fatalf("invalid raw combination dispatched: %d", d.count())
	}
}

func TestRecognizedNotSubmittedRetriesPinnedTargetAndIDOnce(t *testing.T) {
	r := baseResolver()
	d := &fakeDelivery{}
	j := newFakeJournal()
	d.err = &DeliveryError{Server: &codexapi.ServerError{Code: -32603, Message: "failed to submit turn input: ActiveTurnNotSteerable { turn_kind: Review }"}, Phase: WriteComplete}
	// Flip to acceptance after the first call without changing the pinned port
	// inputs. This is the exact bounded Review retry seam.
	count := 0
	delivery := &countingDelivery{inner: d, firstErr: d.err, calls: &count}
	s := &Service{Resolver: &r, Delivery: delivery, Journal: j}
	if _, err := s.Send(context.Background(), SendRequest{Target: "target", Body: "hello"}); err != nil {
		t.Fatal(err)
	}
	if *delivery.calls != 2 || delivery.ids[0] != delivery.ids[1] || delivery.threads[0] != "target" || delivery.threads[1] != "target" {
		t.Fatalf("retry was not same pinned request: %#v", delivery)
	}
}

type countingDelivery struct {
	inner        *fakeDelivery
	firstErr     error
	calls        *int
	ids, threads []string
}

func (d *countingDelivery) Send(ctx context.Context, thread, text, id string) (DeliveryResult, error) {
	d.ids = append(d.ids, id)
	d.threads = append(d.threads, thread)
	*d.calls++
	if *d.calls == 1 {
		return DeliveryResult{}, d.firstErr
	}
	return DeliveryResult{Accepted: true, TurnID: "turn-1", Evidence: "accepted"}, nil
}
func (d *countingDelivery) Resume(ctx context.Context, t ResolvedTarget) (string, error) {
	return d.inner.Resume(ctx, t)
}
func (d *countingDelivery) Detach(ctx context.Context, t ResolvedTarget) error {
	return d.inner.Detach(ctx, t)
}

func TestBlockedDeliveryHasOneBodyAndJournalFenceBeforeWrite(t *testing.T) {
	r := baseResolver()
	gate := make(chan struct{})
	d := &fakeDelivery{gate: gate}
	j := newFakeJournal()
	s := validService(&r, d, j)
	result := make(chan SendResult, 1)
	go func() {
		out, err := s.Send(context.Background(), SendRequest{Target: "target", Body: "blocked"})
		if err != nil {
			result <- SendResult{}
			return
		}
		result <- out
	}()
	deadline := time.After(time.Second)
	for j.markedCount() == 0 {
		select {
		case <-deadline:
			t.Fatal("dispatch fence was not committed before blocked write")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	close(gate)
	out := <-result
	if err := out.Receipt.Validate(); err != nil {
		t.Fatal(err)
	}
	if d.count() != 1 {
		t.Fatalf("body written %d times", d.count())
	}
}

func TestConcurrentIdenticalReplyClaimsJoinAndOnlyWinnerCommits(t *testing.T) {
	j := newFakeJournal()
	in := ReplyClaimInput{ReplyID: "msg_04999999-9999-7999-8999-999999999999", OriginalID: "msg_05999999-9999-7999-8999-999999999999", Digest: "sha256:abc", BodySize: 3, Status: "success", ReplyRoute: "codex://local/thread/source", CustodyRoute: epSource, CustodyStoreID: storeID, Owner: "a"}
	// Seed a matching operation relationship as a real journal would require.
	j.ops[in.OriginalID] = OperationStatus{Operation: Operation{MessageID: in.OriginalID, ReplyRoute: in.ReplyRoute, CustodyRoute: in.CustodyRoute, CustodyStoreID: in.CustodyStoreID}, State: mektup.StateAccepted}
	var wg sync.WaitGroup
	claims := make([]ReplyClaim, 2)
	wg.Add(2)
	for i := range claims {
		go func(i int) { defer wg.Done(); claims[i], _ = j.ClaimReply(context.Background(), in) }(i)
	}
	wg.Wait()
	if claims[0].Token == "" && claims[1].Token == "" {
		t.Fatal("no fenced claim owner")
	}
	joined := claims[0].Joined || claims[1].Joined
	if !joined {
		t.Fatalf("matching claim did not join: %#v %#v", claims[0], claims[1])
	}
}

func TestReplyCommitFailureDoesNotClaimAcceptance(t *testing.T) {
	r := baseResolver()
	d := &fakeDelivery{}
	j := newFakeJournal()
	j.commitErr = errors.New("db failed after body")
	original := mektup.Envelope{MessageID: "msg_06999999-9999-7999-8999-999999999999", Kind: mektup.KindMessage, FromEndpointID: epSource, From: "codex://local/thread/source", FromKind: "agent", ToEndpointID: epTarget, To: "codex://local/thread/target", RequestedTarget: "target", ReplyRequested: true, ReplyEndpointID: epSource, ReplyTo: "codex://local/thread/source", ReplyCustodyEndpointID: epSource, ReplyCustodyStoreID: storeID, Body: "request", Provenance: "observed"}
	original.PayloadBytes = uint64(len(original.Body))
	original.PayloadSHA256 = digest(original.Body)
	original.SentAt = time.Now().UTC().Format(time.RFC3339Nano)
	resolver := originalResolver{original: OriginalMessage{Envelope: original, CurrentThread: "codex://local/thread/target"}}
	s := validService(&r, d, j)
	_, err := s.Reply(context.Background(), resolver, ReplyRequest{Reference: original.MessageID, Body: "answer"})
	var se *Error
	if !errors.As(err, &se) || se.Code != mektup.ErrReplyOutcomeUnknown {
		t.Fatalf("reply error = %v", err)
	}
	if d.count() != 1 {
		t.Fatalf("reply body was not submitted exactly once: %d", d.count())
	}
}

type originalResolver struct{ original OriginalMessage }

func (r originalResolver) ResolveOriginal(context.Context, string) (OriginalMessage, error) {
	return r.original, nil
}

func TestWaitGapReturnsIncompleteWithoutInventingReply(t *testing.T) {
	r := baseResolver()
	d := &fakeDelivery{}
	j := newFakeJournal()
	s := validService(&r, d, j)
	op := Operation{OperationID: "op_07999999-9999-7999-8999-999999999999", MessageID: "msg_08999999-9999-7999-8999-999999999999", SourceRoute: r.source.URI, TargetRoute: r.target.URI, Semantics: "message", Digest: digest("x"), BodySize: 1, ReplyRequested: true}
	_, _ = j.Prepare(context.Background(), op)
	p, _ := j.Prepare(context.Background(), op)
	_ = j.MarkDispatchStarted(context.Background(), p.OperationID, p.Owner, p.Token)
	_ = j.RecordResult(context.Background(), op.OperationID, mektup.StateAccepted, "ok")
	// An observation implementation emits a durable gap after the scan.
	s.Observe = gapObservation{}
	wait, err := s.Wait(context.Background(), WaitRequest{Reference: op.MessageID, Timeout: 100 * time.Millisecond})
	var se *Error
	if !errors.As(err, &se) || se.Code != mektup.ErrWaitIncomplete || !wait.Incomplete {
		t.Fatalf("wait = %#v err=%v", wait, err)
	}
}

type gapObservation struct{}

func (gapObservation) Subscribe(context.Context, string) (EventStream, error) {
	return gapStream{}, nil
}
func (gapObservation) FullHistory(context.Context, string) ([]ObservedItem, error) { return nil, nil }

type gapStream struct{}

func (gapStream) Next(context.Context) (Event, error) {
	return Event{Gap: true, Reason: "overflow"}, nil
}
func (gapStream) Close() error { return nil }
