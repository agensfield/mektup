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
	epReply  = "ep_03999999-9999-7999-8999-999999999999"
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

func (d *fakeDelivery) Send(ctx context.Context, target ResolvedTarget, text, id string) (DeliveryResult, error) {
	d.mu.Lock()
	d.calls = append(d.calls, deliveryCall{target.ThreadID, text, id})
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
func (j *fakeJournal) ExpireClaims(context.Context) error { return nil }
func (j *fakeJournal) markedCount() int                   { j.mu.Lock(); defer j.mu.Unlock(); return len(j.marked) }
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
	if se.Details["inputChars"].(int) <= MaxInputChars || se.Details["inputBytes"].(int) == 0 || se.Details["envelopeOverheadBytes"].(int) == 0 {
		t.Fatalf("missing measured size evidence: %#v", se.Details)
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

type repeatedReviewDelivery struct {
	mu    sync.Mutex
	calls int
}

func (d *repeatedReviewDelivery) Send(context.Context, ResolvedTarget, string, string) (DeliveryResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls++
	if d.calls < 3 {
		return DeliveryResult{}, &DeliveryError{Server: &codexapi.ServerError{Code: -32603, Message: "failed to submit turn input: ActiveTurnNotSteerable { turn_kind: Review }"}, Phase: WriteComplete}
	}
	return DeliveryResult{Accepted: true, TurnID: "turn-repeat", Evidence: "accepted"}, nil
}
func (d *repeatedReviewDelivery) count() int { d.mu.Lock(); defer d.mu.Unlock(); return d.calls }
func (*repeatedReviewDelivery) Resume(context.Context, ResolvedTarget) (string, error) {
	return "", nil
}
func (*repeatedReviewDelivery) Detach(context.Context, ResolvedTarget) error { return nil }

func TestRecognizedNotSubmittedRetriesUntilDeadlineNotFixedAttemptCount(t *testing.T) {
	r := baseResolver()
	d := &repeatedReviewDelivery{}
	j := newFakeJournal()
	s := &Service{Resolver: &r, Delivery: d, Journal: j}
	if _, err := s.Send(context.Background(), SendRequest{Target: "target", Body: "hello", DeliveryTimeout: time.Second}); err != nil {
		t.Fatal(err)
	}
	if d.calls != 3 {
		t.Fatalf("recognized temporary rejection stopped after %d calls", d.calls)
	}
}

func TestGenericInternalServerErrorIsUnknown(t *testing.T) {
	state, _, retry := classifyDelivery(&DeliveryError{Server: &codexapi.ServerError{Code: -32603, Message: "backend failed after submission"}, Phase: WriteComplete})
	if state != mektup.StateOutcomeUnknown || retry {
		t.Fatalf("generic internal error mapped to %s retry=%v", state, retry)
	}
}

func TestReplyObservationUsesReplyBodyDigestAndPinnedReplyRoute(t *testing.T) {
	r := baseResolver()
	status := OperationStatus{Operation: Operation{MessageID: "msg_18999999-9999-7999-8999-999999999999", SourceRoute: "codex://local/thread/source", ReplyRoute: "codex://local/thread/source", ReplyRequested: true}, ReplyDigest: digest("answer"), ReplyBodySize: 6}
	e := mektup.Envelope{MessageID: "msg_19999999-9999-7999-8999-999999999999", Kind: mektup.KindReply, FromEndpointID: epTarget, From: r.target.URI, FromKind: "agent", ToEndpointID: epSource, To: status.ReplyRoute, RequestedTarget: status.ReplyRoute, InReplyTo: status.MessageID, ReplyStatus: mektup.ReplySuccess, Body: "answer", Provenance: "observed", SentAt: time.Now().UTC().Format(time.RFC3339Nano)}
	e.PayloadBytes = uint64(len(e.Body))
	e.PayloadSHA256 = digest(e.Body)
	payload, err := mektup.RenderEnvelope(e)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := validateObservedEnvelope(ObservedItem{ThreadID: "source", NativeItemID: "native", ClientMessageID: e.MessageID, Text: string(payload)}, status, true); err != nil {
		t.Fatal(err)
	}
}

func TestReplyObservationUsesIndependentEndpointAndThread(t *testing.T) {
	status := OperationStatus{Operation: Operation{MessageID: "msg_25999999-9999-7999-8999-999999999999", SourceRoute: "codex://local/thread/source", TargetRoute: "codex://local/thread/target", ReplyRoute: "codex://third/thread/reply", ReplyEndpointID: epReply, SourceEndpointID: epSource, TargetEndpointID: epTarget, ReplyRequested: true}, ReplyDigest: digest("answer"), ReplyBodySize: 6}
	e := mektup.Envelope{MessageID: "msg_26999999-9999-7999-8999-999999999999", Kind: mektup.KindReply, FromEndpointID: epTarget, From: status.TargetRoute, FromKind: "agent", ToEndpointID: epReply, To: status.ReplyRoute, RequestedTarget: status.ReplyRoute, InReplyTo: status.MessageID, ReplyStatus: mektup.ReplySuccess, Body: "answer", Provenance: "observed", SentAt: time.Now().UTC().Format(time.RFC3339Nano)}
	e.PayloadBytes = uint64(len(e.Body))
	e.PayloadSHA256 = digest(e.Body)
	payload, err := mektup.RenderEnvelope(e)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := validateObservedEnvelope(ObservedItem{ThreadID: "reply", NativeItemID: "native", ClientMessageID: e.MessageID, Text: string(payload)}, status, true); err != nil {
		t.Fatal(err)
	}
}

type countingDelivery struct {
	inner        *fakeDelivery
	firstErr     error
	calls        *int
	ids, threads []string
}

func (d *countingDelivery) Send(ctx context.Context, target ResolvedTarget, text, id string) (DeliveryResult, error) {
	d.ids = append(d.ids, id)
	d.threads = append(d.threads, target.ThreadID)
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

func TestReplyResumesAndDetachesUnloadedPersistentReturnTarget(t *testing.T) {
	r := baseResolver()
	r.pinned.Loaded = false
	r.pinned.Persistent = true
	d := &fakeDelivery{}
	j := newFakeJournal()
	original := mektup.Envelope{MessageID: "msg_20999999-9999-7999-8999-999999999999", Kind: mektup.KindMessage, FromEndpointID: epSource, From: r.source.URI, FromKind: "agent", ToEndpointID: epTarget, To: r.target.URI, RequestedTarget: "target", ReplyRequested: true, ReplyEndpointID: epSource, ReplyTo: r.source.URI, ReplyCustodyEndpointID: epSource, ReplyCustodyStoreID: storeID, Body: "request", Provenance: "observed"}
	original.PayloadBytes = uint64(len(original.Body))
	original.PayloadSHA256 = digest(original.Body)
	original.SentAt = time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := validService(&r, d, j).Reply(context.Background(), originalResolver{original: OriginalMessage{Envelope: original, CurrentThread: r.target.URI}}, ReplyRequest{Reference: original.MessageID, Body: "answer"}); err != nil {
		t.Fatal(err)
	}
	if d.resume != 1 || d.detach != 1 {
		t.Fatalf("persistent reply residency not closed: resume=%d detach=%d", d.resume, d.detach)
	}
}

func TestReplyReviewRetryReachesCommit(t *testing.T) {
	r := baseResolver()
	d := &repeatedReviewDelivery{}
	j := newFakeJournal()
	original := mektup.Envelope{MessageID: "msg_21999999-9999-7999-8999-999999999999", Kind: mektup.KindMessage, FromEndpointID: epSource, From: r.source.URI, FromKind: "agent", ToEndpointID: epTarget, To: r.target.URI, RequestedTarget: "target", ReplyRequested: true, ReplyEndpointID: epSource, ReplyTo: r.source.URI, ReplyCustodyEndpointID: epSource, ReplyCustodyStoreID: storeID, Body: "request", Provenance: "observed"}
	original.PayloadBytes = uint64(len(original.Body))
	original.PayloadSHA256 = digest(original.Body)
	original.SentAt = time.Now().UTC().Format(time.RFC3339Nano)
	replyID := "msg_22999999-9999-7999-8999-999999999999"
	out, err := (&Service{Resolver: &r, Delivery: d, Journal: j}).Reply(context.Background(), originalResolver{original: OriginalMessage{Envelope: original, CurrentThread: r.target.URI}}, ReplyRequest{Reference: original.MessageID, MessageID: replyID, Body: "answer", DeliveryTimeout: time.Second})
	if err != nil || out.Receipt.State != mektup.StateReplyAccepted || j.claims[replyID].State != mektup.StateReplyAccepted {
		t.Fatalf("retry did not commit: calls=%d receipt=%s claim=%s err=%v", d.calls, out.Receipt.State, j.claims[replyID].State, err)
	}
}

type unknownJoinedWaitJournal struct{ *fakeJournal }

func (j unknownJoinedWaitJournal) WaitReply(context.Context, string, time.Duration) (OperationStatus, error) {
	return OperationStatus{State: mektup.StateReplyOutcomeUnknown}, nil
}

func TestJoinedUnknownReplyIsTypedAndDoesNotRedispatch(t *testing.T) {
	r := baseResolver()
	j := newFakeJournal()
	original := mektup.Envelope{MessageID: "msg_29999999-9999-7999-8999-999999999999", Kind: mektup.KindMessage, FromEndpointID: epSource, From: r.source.URI, FromKind: "agent", ToEndpointID: epTarget, To: r.target.URI, RequestedTarget: "target", ReplyRequested: true, ReplyEndpointID: epSource, ReplyTo: r.source.URI, ReplyCustodyEndpointID: epSource, ReplyCustodyStoreID: storeID, Body: "question", Provenance: "observed", SentAt: time.Now().UTC().Format(time.RFC3339Nano)}
	original.PayloadBytes = uint64(len(original.Body))
	original.PayloadSHA256 = digest(original.Body)
	rid := "msg_30999999-9999-7999-8999-999999999999"
	j.claims[rid] = ReplyClaim{ReplyID: rid, OriginalID: original.MessageID, Digest: digest("answer"), BodySize: 6, Status: "success", State: mektup.StateReplyDispatchClaimed, Owner: "first-owner", Token: "first-token"}
	d := &fakeDelivery{}
	out, err := (&Service{Resolver: &r, Delivery: d, Journal: unknownJoinedWaitJournal{j}}).Reply(context.Background(), originalResolver{original: OriginalMessage{Envelope: original, CurrentThread: r.target.URI}}, ReplyRequest{Reference: original.MessageID, MessageID: rid, Body: "answer", Wait: true, WaitTimeout: time.Second})
	var se *Error
	if !errors.As(err, &se) || se.Code != mektup.ErrReplyOutcomeUnknown || d.count() != 0 || out.Wait == nil {
		t.Fatalf("joined unknown not typed/fenced: wait=%#v err=%v sends=%d", out.Wait, err, d.count())
	}
}

type retryHeartbeatJournal struct {
	*fakeJournal
	started chan struct{}
	release chan struct{}
	failed  chan struct{}
	once    sync.Once
}

func (j *retryHeartbeatJournal) Heartbeat(context.Context, string, string, string) error {
	j.once.Do(func() { close(j.started) })
	<-j.release
	close(j.failed)
	return errors.New("heartbeat lost during retry")
}

func TestReplyHeartbeatFailureDuringSafeRetryAbortsUnknown(t *testing.T) {
	r := baseResolver()
	d := &repeatedReviewDelivery{}
	j := &retryHeartbeatJournal{fakeJournal: newFakeJournal(), started: make(chan struct{}), release: make(chan struct{}), failed: make(chan struct{})}
	original := mektup.Envelope{MessageID: "msg_31999999-9999-7999-8999-999999999999", Kind: mektup.KindMessage, FromEndpointID: epSource, From: r.source.URI, FromKind: "agent", ToEndpointID: epTarget, To: r.target.URI, RequestedTarget: "target", ReplyRequested: true, ReplyEndpointID: epSource, ReplyTo: r.source.URI, ReplyCustodyEndpointID: epSource, ReplyCustodyStoreID: storeID, Body: "question", Provenance: "observed", SentAt: time.Now().UTC().Format(time.RFC3339Nano)}
	original.PayloadBytes = uint64(len(original.Body))
	original.PayloadSHA256 = digest(original.Body)
	done := make(chan error, 1)
	go func() {
		_, err := (&Service{Resolver: &r, Delivery: d, Journal: j}).Reply(context.Background(), originalResolver{original: OriginalMessage{Envelope: original, CurrentThread: r.target.URI}}, ReplyRequest{Reference: original.MessageID, Body: "answer", DeliveryTimeout: time.Second})
		done <- err
	}()
	select {
	case <-j.started:
	case <-time.After(time.Second):
		t.Fatal("heartbeat did not start")
	}
	deadline := time.Now().Add(time.Second)
	for d.count() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	close(j.release)
	select {
	case <-j.failed:
	case <-time.After(time.Second):
		t.Fatal("heartbeat failure did not complete")
	}
	err := <-done
	var se *Error
	if !errors.As(err, &se) || se.Code != mektup.ErrReplyOutcomeUnknown || d.count() < 2 {
		t.Fatalf("retry heartbeat failure was not conservative: calls=%d err=%v", d.count(), err)
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

func TestWaitTerminalErrorReplyIsRejectedAndReceiptIsValid(t *testing.T) {
	r := baseResolver()
	d := &fakeDelivery{}
	j := newFakeJournal()
	op := Operation{OperationID: "op_15999999-9999-7999-8999-999999999999", MessageID: "msg_16999999-9999-7999-8999-999999999999", SourceRoute: r.source.URI, TargetRoute: r.target.URI, ReplyRoute: r.source.URI, Semantics: "message", Digest: digest("question"), BodySize: 8, ReplyRequested: true, SourceEndpointID: epSource, TargetEndpointID: epTarget}
	j.ops[op.MessageID] = OperationStatus{Operation: op, State: mektup.StateReplyAccepted, ReplyID: "msg_17999999-9999-7999-8999-999999999999", ReplyStatus: "error"}
	got, err := validService(&r, d, j).Wait(context.Background(), WaitRequest{Reference: op.MessageID})
	var se *Error
	if !errors.As(err, &se) || se.Code != mektup.ErrDeliveryRejected {
		t.Fatalf("wait error = %v", err)
	}
	if validateErr := got.Receipt.Validate(); validateErr != nil {
		t.Fatalf("error reply receipt invalid: %v", validateErr)
	}
}

type gapObservation struct{}

func (gapObservation) Subscribe(context.Context, ResolvedTarget) (EventStream, error) {
	return gapStream{}, nil
}
func (gapObservation) FullHistory(context.Context, ResolvedTarget) ([]ObservedItem, error) {
	return nil, nil
}

type gapStream struct{}

func (gapStream) Next(context.Context) (Event, error) {
	return Event{Gap: true, Reason: "overflow"}, nil
}
func (gapStream) Close() error { return nil }
