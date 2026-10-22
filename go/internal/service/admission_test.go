package service

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	mektup "github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/internal/codexapi"
	"github.com/agensfield/mektup/go/internal/journal"
	"github.com/agensfield/mektup/go/internal/sshproxy"
)

type admissionResolver struct {
	fakeResolver
	stall          string
	preflightErr   error
	preflightCalls int
}

func (r *admissionResolver) Resolve(ctx context.Context, target string) (ResolvedTarget, error) {
	if r.stall == "target" {
		<-ctx.Done()
		return ResolvedTarget{}, ctx.Err()
	}
	return r.fakeResolver.Resolve(ctx, target)
}
func (r *admissionResolver) ResolveSource(ctx context.Context, source string) (SourceIdentity, error) {
	if r.stall == "source" {
		<-ctx.Done()
		return SourceIdentity{}, ctx.Err()
	}
	return r.fakeResolver.ResolveSource(ctx, source)
}
func (r *admissionResolver) ResolvePinned(ctx context.Context, endpoint, uri string) (ResolvedTarget, error) {
	if r.stall == "pinned" {
		<-ctx.Done()
		return ResolvedTarget{}, ctx.Err()
	}
	return r.fakeResolver.ResolvePinned(ctx, endpoint, uri)
}
func (r *admissionResolver) PreflightReply(ctx context.Context, _ ResolvedTarget, _ SourceIdentity) error {
	r.preflightCalls++
	if r.stall == "preflight" {
		<-ctx.Done()
		return ctx.Err()
	}
	return r.preflightErr
}

type admissionOriginal struct {
	originalResolver
	stall bool
}

func (r admissionOriginal) ResolveOriginal(ctx context.Context, ref string) (OriginalMessage, error) {
	if r.stall {
		<-ctx.Done()
		return OriginalMessage{}, ctx.Err()
	}
	return r.originalResolver.ResolveOriginal(ctx, ref)
}

func admissionOriginalFor(r fakeResolver) admissionOriginal {
	e := mektup.Envelope{MessageID: "msg_42999999-9999-7999-8999-999999999999", Kind: mektup.KindMessage, FromEndpointID: epSource, From: r.source.URI, FromKind: "agent", ToEndpointID: epTarget, To: r.target.URI, RequestedTarget: "target", ReplyRequested: true, ReplyEndpointID: epSource, ReplyTo: r.source.URI, ReplyCustodyEndpointID: epSource, ReplyCustodyStoreID: storeID, Body: "question", Provenance: "observed", SentAt: time.Now().UTC().Format(time.RFC3339Nano)}
	e.PayloadBytes, e.PayloadSHA256 = uint64(len(e.Body)), digest(e.Body)
	return admissionOriginal{originalResolver: originalResolver{original: OriginalMessage{Envelope: e, CurrentThread: r.target.URI}}}
}

type admissionDelivery struct {
	fakeDelivery
	stallResume    bool
	cancelOnAccept context.CancelFunc
	resumeErr      error
	detachErr      error
}

func (d *admissionDelivery) Resume(ctx context.Context, target ResolvedTarget) (string, error) {
	if d.resumeErr != nil {
		return "", d.resumeErr
	}
	if d.stallResume {
		<-ctx.Done()
		return "", ctx.Err()
	}
	return d.fakeDelivery.Resume(ctx, target)
}
func (d *admissionDelivery) Detach(ctx context.Context, target ResolvedTarget) error {
	if d.detachErr != nil {
		return d.detachErr
	}
	return d.fakeDelivery.Detach(ctx, target)
}
func (d *admissionDelivery) Send(ctx context.Context, target ResolvedTarget, body, id string) (DeliveryResult, error) {
	out, err := d.fakeDelivery.Send(ctx, target, body, id)
	if d.cancelOnAccept != nil {
		d.cancelOnAccept()
	}
	return out, err
}

type admissionJournal struct {
	*fakeJournal
	stallClaim bool
	claimErr   error
	fenceErr   error
	recordErr  error
}

func (j *admissionJournal) ClaimReply(ctx context.Context, input ReplyClaimInput) (ReplyClaim, error) {
	if j.stallClaim {
		<-ctx.Done()
		return ReplyClaim{}, ctx.Err()
	}
	if j.claimErr != nil {
		return ReplyClaim{}, j.claimErr
	}
	return j.fakeJournal.ClaimReply(ctx, input)
}
func (j *admissionJournal) MarkDispatchStarted(ctx context.Context, id, owner, token string) error {
	if j.fenceErr != nil {
		return j.fenceErr
	}
	return j.fakeJournal.MarkDispatchStarted(ctx, id, owner, token)
}
func (j *admissionJournal) RecordResult(ctx context.Context, id string, state mektup.EvidenceState, code string) error {
	if j.recordErr != nil {
		return j.recordErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, ok := ctx.Deadline(); !ok {
		return errors.New("custody write is unbounded")
	}
	return j.fakeJournal.RecordResult(ctx, id, state, code)
}
func (j *admissionJournal) CommitReply(ctx context.Context, id, owner, token string) (ReplyClaim, error) {
	if err := ctx.Err(); err != nil {
		return ReplyClaim{}, err
	}
	if _, ok := ctx.Deadline(); !ok {
		return ReplyClaim{}, errors.New("custody commit is unbounded")
	}
	return j.fakeJournal.CommitReply(ctx, id, owner, token)
}

func TestAdmissionDeadlineBoundsResolutionClaimAndResume(t *testing.T) {
	for _, reply := range []bool{false, true} {
		stages := []string{"target", "source", "preflight", "resume"}
		if reply {
			stages = []string{"original", "source", "pinned", "preflight", "claim", "resume"}
		}
		for _, stage := range stages {
			name := "send/" + stage
			if reply {
				name = "reply/" + stage
			}
			t.Run(name, func(t *testing.T) {
				r := &admissionResolver{fakeResolver: baseResolver(), stall: stage}
				d := &admissionDelivery{stallResume: stage == "resume"}
				j := &admissionJournal{fakeJournal: newFakeJournal(), stallClaim: stage == "claim"}
				if stage == "resume" {
					r.target.Loaded = false
					r.target.Persistent = true
					r.pinned.Loaded = false
					r.pinned.Persistent = true
				}
				s := &Service{Resolver: r, Delivery: d, Journal: j}
				// The outer deadline prevents a regression from hanging the test.
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				started := time.Now()
				var err error
				if reply {
					original := admissionOriginalFor(r.fakeResolver)
					original.stall = stage == "original"
					_, err = s.Reply(ctx, original, ReplyRequest{Reference: original.original.Envelope.MessageID, Body: "answer", Wait: true, DeliveryTimeout: 15 * time.Millisecond})
				} else {
					_, err = s.Send(ctx, SendRequest{Target: "target", Body: "question", RequestReply: true, DeliveryTimeout: 15 * time.Millisecond})
				}
				if err == nil || !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("error=%v", err)
				}
				if elapsed := time.Since(started); elapsed > 500*time.Millisecond {
					t.Fatalf("admission deadline missed: %s", elapsed)
				}
				if d.count() != 0 {
					t.Fatalf("body dispatched after %s timed out", stage)
				}
				if stage == "claim" || stage == "resume" {
					for _, op := range j.ops {
						if op.State != mektup.StateNotSent {
							t.Fatalf("prepared failure state=%s", op.State)
						}
					}
				} else if len(j.prepared) != 0 {
					t.Fatalf("prepared before admission validation completed")
				}
			})
		}
	}
}

func TestReplyClaimErrorsRetainTypedCauseAndDurableIdentities(t *testing.T) {
	transportCause := &sshproxy.Failure{Kind: sshproxy.FailurePossibleWrite, Evidence: sshproxy.WriteMayHaveWritten, Err: context.DeadlineExceeded}
	typedCause := semantic(mektup.ErrStorageBusy, "remote custody busy", map[string]any{"remoteDetail": "busy"}, errors.New("remote"))
	typedCause.Retryable = true
	for _, tt := range []struct {
		name       string
		cause      error
		code       mektup.ErrorCode
		claimState string
	}{
		{"typed", typedCause, mektup.ErrStorageBusy, "outcome_unknown"},
		{"conflict", journal.ErrIdentityConflict, mektup.ErrMessageIdentityConflict, "rejected"},
		{"expired", journal.ErrClaimExpired, mektup.ErrReplyOutcomeUnknown, "outcome_unknown"},
		{"transport", transportCause, mektup.ErrReplyOutcomeUnknown, "outcome_unknown"},
		{"before_write", &sshproxy.Failure{Kind: sshproxy.FailureSpawn, Evidence: sshproxy.WriteNotStarted, Err: errors.New("spawn")}, mektup.ErrDeliveryTemporarilyUnavailable, "not_attempted"},
		{"route", ErrRemoteCustodyUnavailable, mektup.ErrReplyRouteUnavailable, "not_attempted"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r := baseResolver()
			d := &fakeDelivery{}
			j := &admissionJournal{fakeJournal: newFakeJournal(), claimErr: tt.cause}
			original := admissionOriginalFor(r)
			out, err := (&Service{Resolver: &r, Delivery: d, Journal: j}).Reply(context.Background(), original, ReplyRequest{Reference: original.original.Envelope.MessageID, Body: "answer"})
			var se *Error
			if !errors.As(err, &se) || se.Code != tt.code || !errors.Is(err, tt.cause) {
				t.Fatalf("error=%#v cause=%v", se, err)
			}
			if se.Retryable || se.Details["bodySent"] != false || se.Details["claimState"] != tt.claimState {
				t.Fatalf("unsafe error details: %#v", se)
			}
			if se.Details["operationId"] != out.Receipt.OperationID || se.Details["messageId"] != out.Receipt.Message.MessageID {
				t.Fatalf("lost durable identities: %#v %#v", se.Details, out.Receipt)
			}
			if err := out.Receipt.Validate(); err != nil {
				t.Fatal(err)
			}
			if out.Receipt.State != mektup.StateNotSent || d.count() != 0 || len(j.results) != 1 || j.results[0] != mektup.StateNotSent {
				t.Fatalf("receipt=%s sends=%d results=%v", out.Receipt.State, d.count(), j.results)
			}
			if tt.name == "typed" && se.Details["remoteDetail"] != "busy" {
				t.Fatalf("lost typed detail: %#v", se.Details)
			}
		})
	}
}

func TestAdmissionPreflightRejectsBeforePrepare(t *testing.T) {
	cause := semantic(mektup.ErrReplyRouteUnavailable, "return path cannot resolve sender", nil, nil)
	r := &admissionResolver{fakeResolver: baseResolver(), preflightErr: cause}
	j := newFakeJournal()
	d := &fakeDelivery{}
	s := &Service{Resolver: r, Journal: j, Delivery: d}
	_, err := s.Send(context.Background(), SendRequest{Target: "target", Body: "question", RequestReply: true})
	if err != cause || len(j.prepared) != 0 || d.count() != 0 || r.preflightCalls != 1 {
		t.Fatalf("err=%v prepared=%d writes=%d checks=%d", err, len(j.prepared), d.count(), r.preflightCalls)
	}
	_, err = s.Send(context.Background(), SendRequest{Target: "target", Body: "one-way"})
	if err != nil || r.preflightCalls != 1 {
		t.Fatalf("one-way send preflighted: err=%v checks=%d", err, r.preflightCalls)
	}
}

func TestAcceptedEffectSurvivesCallerCancellation(t *testing.T) {
	for _, reply := range []bool{false, true} {
		t.Run(map[bool]string{false: "send", true: "reply"}[reply], func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r := baseResolver()
			d := &admissionDelivery{cancelOnAccept: cancel}
			j := &admissionJournal{fakeJournal: newFakeJournal()}
			s := &Service{Resolver: &r, Delivery: d, Journal: j}
			var state mektup.EvidenceState
			var err error
			if reply {
				original := admissionOriginalFor(r)
				var out ReplyResult
				out, err = s.Reply(ctx, original, ReplyRequest{Reference: original.original.Envelope.MessageID, Body: "answer"})
				state = out.Receipt.State
			} else {
				var out SendResult
				out, err = s.Send(ctx, SendRequest{Target: "target", Body: "question"})
				state = out.Receipt.State
			}
			want := mektup.StateAccepted
			if reply {
				want = mektup.StateReplyAccepted
			}
			if err != nil || state != want || len(j.results) != 1 || j.results[0] != mektup.StateAccepted {
				t.Fatalf("state=%s err=%v results=%v", state, err, j.results)
			}
		})
	}
}

type delayedReplyJournal struct{ *admissionJournal }

func (j delayedReplyJournal) Lookup(ctx context.Context, ref string) (OperationStatus, error) {
	select {
	case <-time.After(40 * time.Millisecond):
	case <-ctx.Done():
		return OperationStatus{}, ctx.Err()
	}
	op, err := j.fakeJournal.Lookup(ctx, ref)
	op.State = mektup.StateReplyAccepted
	op.ReplyID = "msg_43999999-9999-7999-8999-999999999999"
	op.ReplyStatus = "success"
	return op, err
}
func TestWaitBudgetIsIndependentOfAdmissionDeadline(t *testing.T) {
	for _, reply := range []bool{false, true} {
		t.Run(map[bool]string{false: "send", true: "reply"}[reply], func(t *testing.T) {
			r := baseResolver()
			j := delayedReplyJournal{&admissionJournal{fakeJournal: newFakeJournal()}}
			s := &Service{Resolver: &r, Delivery: &fakeDelivery{}, Journal: j}
			var wait *WaitResult
			var err error
			if reply {
				original := admissionOriginalFor(r)
				var out ReplyResult
				out, err = s.Reply(context.Background(), original, ReplyRequest{Reference: original.original.Envelope.MessageID, Body: "answer", Wait: true, DeliveryTimeout: 10 * time.Millisecond, WaitTimeout: 200 * time.Millisecond})
				wait = out.Wait
			} else {
				var out SendResult
				out, err = s.Send(context.Background(), SendRequest{Target: "target", Body: "question", Wait: true, DeliveryTimeout: 10 * time.Millisecond, WaitTimeout: 200 * time.Millisecond})
				wait = out.Wait
			}
			if err != nil || wait == nil || wait.Incomplete || wait.State != mektup.StateReplyAccepted {
				t.Fatalf("wait=%#v err=%v", wait, err)
			}
		})
	}
}

func TestPreparedFailuresReturnDurableReceipt(t *testing.T) {
	for _, reply := range []bool{false, true} {
		for _, stage := range []string{"resume", "fence", "rejected", "unknown", "record", "cleanup", "commit"} {
			if stage == "commit" && !reply {
				continue
			}
			name := map[bool]string{false: "send/", true: "reply/"}[reply] + stage
			t.Run(name, func(t *testing.T) {
				cause := errors.New(stage + " failed")
				r := baseResolver()
				d := &admissionDelivery{}
				j := &admissionJournal{fakeJournal: newFakeJournal()}
				want := mektup.StateOutcomeUnknown
				switch stage {
				case "resume":
					d.resumeErr = cause
					want = mektup.StateNotSent
				case "fence":
					j.fenceErr = cause
				case "rejected":
					d.err = &DeliveryError{Err: cause, Server: &codexapi.ServerError{Code: -32602}}
					want = mektup.StateRejected
				case "unknown":
					d.err = &DeliveryError{Err: cause, Phase: WriteMayHaveWritten}
				case "record":
					j.recordErr = cause
					want = mektup.StateAccepted
				case "cleanup":
					d.detachErr = cause
					want = mektup.StateAccepted
				case "commit":
					j.commitErr = cause
				}
				if stage == "resume" || stage == "cleanup" {
					r.target.Loaded = false
					r.target.Persistent = true
					r.pinned.Loaded = false
					r.pinned.Persistent = true
				}
				if reply && want == mektup.StateAccepted {
					want = mektup.StateReplyAccepted
				}
				s := &Service{Resolver: &r, Delivery: d, Journal: j}
				var receipt mektup.Receipt
				var err error
				if reply {
					original := admissionOriginalFor(r)
					var out ReplyResult
					out, err = s.Reply(context.Background(), original, ReplyRequest{Reference: original.original.Envelope.MessageID, Body: "answer"})
					receipt = out.Receipt
				} else {
					var out SendResult
					out, err = s.Send(context.Background(), SendRequest{Target: "target", Body: "question"})
					receipt = out.Receipt
				}
				var se *Error
				if !errors.As(err, &se) || !errors.Is(err, cause) {
					t.Fatalf("error=%v", err)
				}
				if err := receipt.Validate(); err != nil {
					t.Fatal(err)
				}
				prepared, ok := j.prepared[receipt.Message.MessageID]
				if !ok || prepared.OperationID != receipt.OperationID || se.Details["operationId"] != receipt.OperationID || se.Details["messageId"] != receipt.Message.MessageID {
					t.Fatalf("receipt identity not durable: receipt=%+v error=%+v", receipt, se)
				}
				if receipt.State != want {
					t.Fatalf("state=%s want=%s", receipt.State, want)
				}
			})
		}
	}
}

func TestDeliveryDeadlineDefaultsAndDisableContract(t *testing.T) {
	ctx, cancel := deliveryContext(context.Background(), 0, false)
	defer cancel()
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) < 9*time.Second || time.Until(deadline) > 10*time.Second {
		t.Fatalf("default deadline=%v present=%v", deadline, ok)
	}
	disabled, stop := deliveryContext(context.Background(), time.Millisecond, true)
	defer stop()
	if _, ok := disabled.Deadline(); ok {
		t.Fatal("disabled delivery deadline still active")
	}
	parent, parentCancel := context.WithTimeout(context.Background(), time.Hour)
	defer parentCancel()
	child, childCancel := deliveryContext(parent, 0, true)
	defer childCancel()
	parentDeadline, _ := parent.Deadline()
	childDeadline, ok := child.Deadline()
	if !ok || !parentDeadline.Equal(childDeadline) {
		t.Fatal("disabled delivery timeout discarded caller deadline")
	}
}

type durableClaimFailure struct {
	SQLiteJournal
	failed bool
}

func (j *durableClaimFailure) ClaimReply(_ context.Context, in ReplyClaimInput) (ReplyClaim, error) {
	if !j.failed {
		j.failed = true
		return ReplyClaim{}, context.DeadlineExceeded
	}
	return ReplyClaim{ReplyID: in.ReplyID, Owner: in.Owner, Token: "remote-claim", State: mektup.StateReplyDispatchClaimed}, nil
}
func (j *durableClaimFailure) Heartbeat(context.Context, string, string, string) error    { return nil }
func (j *durableClaimFailure) AbandonReply(context.Context, string, string, string) error { return nil }

func TestUnknownClaimClosesLocalAttemptAgainstSameOperationReplay(t *testing.T) {
	ctx := context.Background()
	inner, err := journal.Open(ctx, journal.Options{StateDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	j := &durableClaimFailure{SQLiteJournal: SQLiteJournal{Inner: inner, Registry: NewMemoryIdentityRegistry()}}
	r := baseResolver()
	d := &fakeDelivery{}
	s := &Service{Resolver: &r, Delivery: d, Journal: j}
	original := admissionOriginalFor(r)
	req := ReplyRequest{Reference: original.original.Envelope.MessageID, Body: "answer", MessageID: "msg_44999999-9999-7999-8999-999999999999", OperationID: "op_44999999-9999-7999-8999-999999999999"}
	out, err := s.Reply(ctx, original, req)
	if err == nil || out.Receipt.State != mektup.StateNotSent {
		t.Fatalf("first receipt=%s err=%v", out.Receipt.State, err)
	}
	persisted, err := inner.Operation(ctx, req.OperationID)
	if err != nil || persisted.State != journal.StateNotSent || persisted.ErrorCode != "reply_claim_failed" {
		t.Fatalf("durable operation=%+v err=%v", persisted, err)
	}
	_, err = s.Reply(ctx, original, req)
	if err == nil || d.count() != 0 {
		t.Fatalf("failed claim was replayed: sends=%d err=%v", d.count(), err)
	}
}

type concurrentPreparedJournal struct {
	SQLiteJournal
	prepared       chan struct{}
	releasePrepare <-chan struct{}
	owners         chan string
	claims         atomic.Int32
}

func (j *concurrentPreparedJournal) Prepare(ctx context.Context, op Operation) (Prepared, error) {
	j.owners <- op.AttemptOwner
	p, err := j.SQLiteJournal.Prepare(ctx, op)
	if err == nil && p.Token != "" {
		close(j.prepared)
		if j.releasePrepare != nil {
			select {
			case <-j.releasePrepare:
			case <-ctx.Done():
				return Prepared{}, ctx.Err()
			}
		}
	}
	return p, err
}
func (j *concurrentPreparedJournal) ClaimReply(_ context.Context, in ReplyClaimInput) (ReplyClaim, error) {
	if j.claims.Add(1) > 1 {
		return ReplyClaim{}, errors.New("duplicate claim failed")
	}
	return ReplyClaim{ReplyID: in.ReplyID, Owner: in.Owner, Token: "owner-claim", State: mektup.StateReplyDispatchClaimed}, nil
}
func (j *concurrentPreparedJournal) Heartbeat(context.Context, string, string, string) error {
	return nil
}
func (j *concurrentPreparedJournal) CommitReply(_ context.Context, id, owner, token string) (ReplyClaim, error) {
	return ReplyClaim{ReplyID: id, Owner: owner, Token: token, State: mektup.StateReplyAccepted}, nil
}
func (j *concurrentPreparedJournal) AbandonReply(context.Context, string, string, string) error {
	return errors.New("duplicate must not abandon owner claim")
}

type signaledAdmissionDelivery struct {
	fakeDelivery
	entered chan struct{}
}

func (d *signaledAdmissionDelivery) Send(ctx context.Context, target ResolvedTarget, body, id string) (DeliveryResult, error) {
	close(d.entered)
	return d.fakeDelivery.Send(ctx, target, body, id)
}

func TestConcurrentSameReplyIDsCannotStealOrCloseOwnerAttempt(t *testing.T) {
	for _, phase := range []string{"prepared", "dispatch_started"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			inner, err := journal.Open(ctx, journal.Options{StateDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer inner.Close()
			release := make(chan struct{})
			j := &concurrentPreparedJournal{SQLiteJournal: SQLiteJournal{Inner: inner, Registry: NewMemoryIdentityRegistry()}, prepared: make(chan struct{}), owners: make(chan string, 2)}
			d := &signaledAdmissionDelivery{entered: make(chan struct{})}
			if phase == "prepared" {
				j.releasePrepare = release
			} else {
				d.gate = release
			}
			r := baseResolver()
			s := &Service{Resolver: &r, Journal: j, Delivery: d}
			original := admissionOriginalFor(r)
			req := ReplyRequest{Reference: original.original.Envelope.MessageID, Body: "answer", MessageID: "msg_45999999-9999-7999-8999-999999999999", OperationID: "op_45999999-9999-7999-8999-999999999999"}
			type outcome struct {
				out ReplyResult
				err error
			}
			first := make(chan outcome, 1)
			go func() { out, err := s.Reply(ctx, original, req); first <- outcome{out, err} }()
			ready := j.prepared
			if phase == "dispatch_started" {
				ready = d.entered
			}
			select {
			case <-ready:
			case <-ctx.Done():
				t.Fatal("first caller did not reach gate")
			}
			second, err := s.Reply(ctx, original, req)
			if err == nil {
				t.Fatal("duplicate unexpectedly accepted")
			}
			want := mektup.StatePrepared
			if phase == "dispatch_started" {
				want = mektup.StateDispatchStarted
			}
			if second.Receipt.State != want {
				t.Fatalf("duplicate mislabeled existing operation: state=%s want=%s err=%v", second.Receipt.State, want, err)
			}
			if second.Receipt.OperationID != req.OperationID || second.Receipt.Message.MessageID != req.MessageID {
				t.Fatalf("duplicate lost identity: %+v", second.Receipt)
			}
			one, two := <-j.owners, <-j.owners
			if one == two || one == "" || two == "" {
				t.Fatalf("attempt owners are not unique: %q %q", one, two)
			}
			persisted, err := inner.Operation(ctx, req.OperationID)
			if err != nil || mektup.EvidenceState(persisted.State) != want || persisted.AttemptOwner != one {
				t.Fatalf("duplicate changed owner operation: %+v err=%v", persisted, err)
			}
			wantClaims := int32(0)
			if phase == "dispatch_started" {
				wantClaims = 1
			}
			if j.claims.Load() != wantClaims {
				t.Fatalf("duplicate attempted custody mutation: claims=%d", j.claims.Load())
			}
			close(release)
			select {
			case result := <-first:
				if result.err != nil || result.out.Receipt.State != mektup.StateReplyAccepted {
					t.Fatalf("owner lost authority: receipt=%s err=%v", result.out.Receipt.State, result.err)
				}
			case <-ctx.Done():
				t.Fatal("owner did not complete")
			}
			if j.claims.Load() != 1 || d.count() != 1 {
				t.Fatalf("claims=%d bodies=%d", j.claims.Load(), d.count())
			}
			persisted, err = inner.Operation(ctx, req.OperationID)
			if err != nil || persisted.State != journal.StateAccepted {
				t.Fatalf("owner acceptance not durable: %+v err=%v", persisted, err)
			}
			joined, err := s.Reply(ctx, original, req)
			if err != nil || joined.Receipt.State != mektup.StateReplyAccepted || j.claims.Load() != 1 || d.count() != 1 {
				t.Fatalf("accepted duplicate changed state or dispatched: state=%s claims=%d bodies=%d err=%v", joined.Receipt.State, j.claims.Load(), d.count(), err)
			}
		})
	}
}

type joinSnapshotJournal struct {
	*fakeJournal
	states      []mektup.EvidenceState
	lookups     int
	claimCalls  int
	expiryCalls int
}

func (j *joinSnapshotJournal) Prepare(ctx context.Context, op Operation) (Prepared, error) {
	p, err := j.fakeJournal.Prepare(ctx, op)
	p.Token = ""
	return p, err
}
func (j *joinSnapshotJournal) Lookup(ctx context.Context, ref string) (OperationStatus, error) {
	op, err := j.fakeJournal.Lookup(ctx, ref)
	index := j.lookups
	if index >= len(j.states) {
		index = len(j.states) - 1
	}
	j.lookups++
	op.State = j.states[index]
	if op.State == mektup.StateReplyAccepted {
		op.ReplyStatus = "success"
	}
	return op, err
}
func (j *joinSnapshotJournal) ClaimReply(context.Context, ReplyClaimInput) (ReplyClaim, error) {
	j.claimCalls++
	return ReplyClaim{}, errors.New("joiner must not claim")
}
func (j *joinSnapshotJournal) ExpireClaims(context.Context) error { j.expiryCalls++; return nil }

func TestTokenlessReplyJoinPreservesTerminalUnknown(t *testing.T) {
	for _, state := range []mektup.EvidenceState{mektup.StateOutcomeUnknown, mektup.StateReplyOutcomeUnknown} {
		t.Run(string(state), func(t *testing.T) {
			r := baseResolver()
			j := &joinSnapshotJournal{fakeJournal: newFakeJournal(), states: []mektup.EvidenceState{state}}
			d := &fakeDelivery{}
			original := admissionOriginalFor(r)
			out, err := (&Service{Resolver: &r, Journal: j, Delivery: d}).Reply(context.Background(), original, ReplyRequest{Reference: original.original.Envelope.MessageID, Body: "answer"})
			var se *Error
			if !errors.As(err, &se) || se.Code != mektup.ErrReplyOutcomeUnknown || se.Retryable || out.Receipt.State != state {
				t.Fatalf("state=%s error=%+v", out.Receipt.State, se)
			}
			if j.claimCalls != 0 || j.expiryCalls != 0 || d.count() != 0 {
				t.Fatalf("join mutated custody: claims=%d expiry=%d bodies=%d", j.claimCalls, j.expiryCalls, d.count())
			}
		})
	}
}

func TestTokenlessPendingReplyWaitIsBoundedAndReadOnly(t *testing.T) {
	r := baseResolver()
	j := &joinSnapshotJournal{fakeJournal: newFakeJournal(), states: []mektup.EvidenceState{mektup.StatePrepared}}
	d := &fakeDelivery{}
	original := admissionOriginalFor(r)
	started := time.Now()
	out, err := (&Service{Resolver: &r, Journal: j, Delivery: d}).Reply(context.Background(), original, ReplyRequest{Reference: original.original.Envelope.MessageID, Body: "answer", Wait: true, WaitTimeout: 25 * time.Millisecond})
	var se *Error
	if !errors.As(err, &se) || se.Code != mektup.ErrWaitIncomplete || !errors.Is(err, context.DeadlineExceeded) || out.Wait == nil || !out.Wait.Incomplete || out.Receipt.State != mektup.StatePrepared {
		t.Fatalf("result=%+v error=%v", out, err)
	}
	if elapsed := time.Since(started); elapsed < 20*time.Millisecond || elapsed > time.Second {
		t.Fatalf("wait budget not honored: %s", elapsed)
	}
	if j.claimCalls != 0 || j.expiryCalls != 0 || d.count() != 0 {
		t.Fatalf("pending join mutated custody: claims=%d expiry=%d bodies=%d", j.claimCalls, j.expiryCalls, d.count())
	}
}

func TestTokenlessPendingReplyCanObserveOwnerAcceptance(t *testing.T) {
	r := baseResolver()
	j := &joinSnapshotJournal{fakeJournal: newFakeJournal(), states: []mektup.EvidenceState{mektup.StatePrepared, mektup.StateAccepted, mektup.StateReplyAccepted}}
	d := &fakeDelivery{}
	original := admissionOriginalFor(r)
	callbacks := 0
	out, err := (&Service{Resolver: &r, Journal: j, Delivery: d}).ReplyWithAcceptance(context.Background(), original, ReplyRequest{Reference: original.original.Envelope.MessageID, Body: "answer", Wait: true, WaitTimeout: 250 * time.Millisecond}, func(ReplyResult) error { callbacks++; return nil })
	if err != nil || out.Receipt.State != mektup.StateReplyAccepted || out.Wait == nil || out.Wait.State != mektup.StateReplyAccepted || callbacks != 1 {
		t.Fatalf("result=%+v callback=%d err=%v", out, callbacks, err)
	}
	if j.claimCalls != 0 || d.count() != 0 {
		t.Fatalf("joiner sent: claims=%d bodies=%d", j.claimCalls, d.count())
	}
}
