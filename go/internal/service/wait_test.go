package service

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/agensfield/mektup/go"
)

type waitObservation struct {
	subscribe func(context.Context) (EventStream, error)
	history   func(context.Context) ([]ObservedItem, error)
}

func (o waitObservation) Subscribe(ctx context.Context, _ ResolvedTarget) (EventStream, error) {
	if o.subscribe != nil {
		return o.subscribe(ctx)
	}
	return waitStream{}, nil
}

func (o waitObservation) FullHistory(ctx context.Context, _ ResolvedTarget) ([]ObservedItem, error) {
	if o.history != nil {
		return o.history(ctx)
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

type waitStream struct {
	next  func(context.Context) (Event, error)
	close func() error
}

func (s waitStream) Next(ctx context.Context) (Event, error) {
	if s.next != nil {
		return s.next(ctx)
	}
	<-ctx.Done()
	return Event{}, ctx.Err()
}

func (s waitStream) Close() error {
	if s.close != nil {
		return s.close()
	}
	return nil
}

func waitFixture() (*Service, *fakeJournal, OperationStatus) {
	r := baseResolver()
	j := newFakeJournal()
	status := OperationStatus{Operation: Operation{
		OperationID: "op_18999999-9999-7999-8999-999999999999",
		MessageID:   "msg_18999999-9999-7999-8999-999999999998",
		SourceRoute: r.source.URI, TargetRoute: r.target.URI, ReplyRoute: r.source.URI,
		SourceEndpointID: epSource, TargetEndpointID: epTarget, ReplyEndpointID: epSource,
		CustodyRoute: epSource, CustodyStoreID: storeID,
		Semantics: "message", ReplyRequested: true, Digest: digest("question"), BodySize: 8,
	}, State: mektup.StateAccepted}
	j.ops[status.MessageID] = status
	return validService(&r, &fakeDelivery{}, j), j, status
}

func setWaitState(j *fakeJournal, status OperationStatus, state mektup.EvidenceState, replyStatus string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	status.State = state
	status.ReplyID = "msg_18999999-9999-7999-8999-999999999997"
	status.ReplyStatus = replyStatus
	status.ReplyDigest = digest("answer")
	status.ReplyBodySize = 6
	status.ReplyCommitSeq = 1
	j.ops[status.MessageID] = status
}

func assertWaitOutcome(t *testing.T, result WaitResult, err error, state mektup.EvidenceState, code mektup.ErrorCode, gap string) {
	t.Helper()
	if result.Incomplete || result.State != state || result.ReplyID == "" || result.GapReason != gap {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if code == "" {
		if err != nil {
			t.Fatal(err)
		}
	} else {
		var se *Error
		if !errors.As(err, &se) || se.Code != code {
			t.Fatalf("err=%v, want %s", err, code)
		}
	}
	if gap != "" && result.Receipt.Evidence[0].Details["gap"] != gap {
		t.Fatalf("receipt lost gap diagnostic: %+v", result.Receipt.Evidence)
	}
	if err := result.Receipt.Validate(); err != nil {
		t.Fatalf("invalid receipt: %v", err)
	}
}

func TestWaitFreshTerminalCustodyWinsObservationGap(t *testing.T) {
	cases := []struct {
		name, status string
		state        mektup.EvidenceState
		code         mektup.ErrorCode
	}{
		{"accepted", "success", mektup.StateReplyAccepted, ""},
		{"observed", "success", mektup.StateReplyObserved, ""},
		{"error", "error", mektup.StateReplyAccepted, mektup.ErrDeliveryRejected},
		{"unknown", "success", mektup.StateReplyOutcomeUnknown, mektup.ErrReplyOutcomeUnknown},
	}
	for _, tc := range cases {
		for _, source := range []string{"history", "stream", "subscribe"} {
			t.Run(tc.name+"/"+source, func(t *testing.T) {
				s, j, status := waitFixture()
				commit := func() { setWaitState(j, status, tc.state, tc.status) }
				o := waitObservation{}
				gap := "history: unavailable"
				switch source {
				case "history":
					o.history = func(context.Context) ([]ObservedItem, error) {
						commit()
						return nil, errors.New("unavailable")
					}
				case "stream":
					gap = "overflow"
					o.subscribe = func(context.Context) (EventStream, error) {
						return waitStream{next: func(context.Context) (Event, error) {
							commit()
							return Event{Gap: true, Reason: gap}, nil
						}}, nil
					}
				case "subscribe":
					gap = "event_stream: unavailable"
					o.subscribe = func(context.Context) (EventStream, error) {
						commit()
						return nil, errors.New("unavailable")
					}
				}
				s.Observe = o
				result, err := s.Wait(context.Background(), WaitRequest{Reference: status.OperationID})
				assertWaitOutcome(t, result, err, tc.state, tc.code, gap)
			})
		}
	}
}

type waitLookupJournal struct {
	*fakeJournal
	lookup func(context.Context, string) (OperationStatus, error)
}

func (j waitLookupJournal) Lookup(ctx context.Context, ref string) (OperationStatus, error) {
	return j.lookup(ctx, ref)
}

func TestWaitGapAllowsDelayedCustodyWithinCallerDeadline(t *testing.T) {
	for _, parentDeadline := range []bool{false, true} {
		t.Run(map[bool]string{false: "request_timeout", true: "parent_deadline"}[parentDeadline], func(t *testing.T) {
			s, j, status := waitFixture()
			pending := make(chan struct{})
			var once sync.Once
			lookups := 0
			s.Journal = waitLookupJournal{j, func(ctx context.Context, ref string) (OperationStatus, error) {
				lookups++
				fresh, err := j.Lookup(ctx, ref)
				if lookups > 1 {
					once.Do(func() { close(pending) })
				}
				return fresh, err
			}}
			s.Observe = waitObservation{history: func(context.Context) ([]ObservedItem, error) {
				return nil, errors.New("unavailable")
			}}
			committed := make(chan struct{})
			go func() {
				<-pending
				setWaitState(j, status, mektup.StateReplyAccepted, "success")
				close(committed)
			}()
			ctx := context.Background()
			req := WaitRequest{Reference: status.MessageID, Timeout: time.Second}
			if parentDeadline {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, time.Second)
				defer cancel()
				req.Timeout = 0
			}
			result, err := s.Wait(ctx, req)
			<-committed
			assertWaitOutcome(t, result, err, mektup.StateReplyAccepted, "", "history: unavailable")
		})
	}
}

func TestWaitKnownGapWithoutDeadlineIsBounded(t *testing.T) {
	s, _, status := waitFixture()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s.Observe = waitObservation{history: func(context.Context) ([]ObservedItem, error) {
		return nil, errors.New("unavailable")
	}}
	done := make(chan struct{})
	go func() {
		defer close(done)
		result, err := s.Wait(ctx, WaitRequest{Reference: status.MessageID})
		var se *Error
		if !errors.As(err, &se) || se.Code != mektup.ErrWaitIncomplete || !result.Incomplete || result.GapReason == "" {
			t.Errorf("result=%+v err=%v", result, err)
		}
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("wait continued indefinitely after a known gap without a deadline")
	}
}

func TestWaitDeadlineKeepsLatestCustodyAndGap(t *testing.T) {
	for _, terminal := range []bool{false, true} {
		t.Run(map[bool]string{false: "pending", true: "terminal"}[terminal], func(t *testing.T) {
			s, j, status := waitFixture()
			lookups := 0
			state := mektup.StateReplyDispatchClaimed
			if terminal {
				state = mektup.StateReplyObserved
			}
			s.Journal = waitLookupJournal{j, func(ctx context.Context, ref string) (OperationStatus, error) {
				lookups++
				if lookups == 2 {
					<-ctx.Done()
					setWaitState(j, status, state, "success")
					return OperationStatus{}, ctx.Err()
				}
				if err := ctx.Err(); err != nil {
					return OperationStatus{}, err
				}
				return j.Lookup(ctx, ref)
			}}
			s.Observe = waitObservation{history: func(context.Context) ([]ObservedItem, error) {
				return nil, errors.New("unavailable")
			}}
			result, err := s.Wait(context.Background(), WaitRequest{Reference: status.MessageID, Timeout: 20 * time.Millisecond})
			if terminal {
				assertWaitOutcome(t, result, err, state, "", "history: unavailable")
			} else {
				var se *Error
				if !result.Incomplete || result.State != state || result.GapReason != "history: unavailable" || !errors.As(err, &se) || se.Code != mektup.ErrWaitIncomplete || !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("result=%+v err=%v", result, err)
				}
			}
		})
	}
}

func TestWaitCancelsAndJoinsObservationWorkers(t *testing.T) {
	s, _, status := waitFixture()
	nextDone, historyDone := make(chan struct{}), make(chan struct{})
	s.Observe = waitObservation{
		subscribe: func(context.Context) (EventStream, error) {
			return waitStream{next: func(ctx context.Context) (Event, error) {
				defer close(nextDone)
				<-ctx.Done()
				return Event{}, ctx.Err()
			}}, nil
		},
		history: func(ctx context.Context) ([]ObservedItem, error) {
			defer close(historyDone)
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
	result, err := s.Wait(context.Background(), WaitRequest{Reference: status.MessageID, Timeout: 20 * time.Millisecond})
	if !result.Incomplete || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	for name, done := range map[string]<-chan struct{}{"stream": nextDone, "history": historyDone} {
		select {
		case <-done:
		default:
			t.Errorf("%s worker still running", name)
		}
	}
}

func TestWaitCancellationRefreshesCustody(t *testing.T) {
	for _, state := range []mektup.EvidenceState{mektup.StateReplyAccepted, mektup.StateReplyDispatchClaimed} {
		for _, gap := range []bool{false, true} {
			t.Run(string(state)+map[bool]string{false: "/poll", true: "/gap"}[gap], func(t *testing.T) {
				s, j, status := waitFixture()
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				lookups := 0
				s.Journal = waitLookupJournal{j, func(ctx context.Context, ref string) (OperationStatus, error) {
					lookups++
					if lookups == 2 {
						setWaitState(j, status, state, "success")
						cancel()
						return OperationStatus{}, ctx.Err()
					}
					if err := ctx.Err(); err != nil {
						return OperationStatus{}, err
					}
					return j.Lookup(ctx, ref)
				}}
				gapReason := ""
				if gap {
					// A finite parent deadline enables custody polling after a gap.
					var timeoutCancel context.CancelFunc
					ctx, timeoutCancel = context.WithTimeout(ctx, time.Second)
					defer timeoutCancel()
					s.Observe = waitObservation{history: func(context.Context) ([]ObservedItem, error) {
						return nil, errors.New("unavailable")
					}}
					gapReason = "history: unavailable"
				}
				result, err := s.Wait(ctx, WaitRequest{Reference: status.MessageID})
				if state == mektup.StateReplyAccepted {
					assertWaitOutcome(t, result, err, state, "", gapReason)
				} else {
					var se *Error
					if !result.Incomplete || result.State != state || result.GapReason != gapReason || !errors.As(err, &se) || se.Code != mektup.ErrWaitIncomplete || !errors.Is(err, context.Canceled) {
						t.Fatalf("result=%+v err=%v", result, err)
					}
				}
			})
		}
	}
}

func TestWaitBoundsUncooperativeObservationCleanup(t *testing.T) {
	s, _, status := waitFixture()
	release := make(chan struct{})
	defer close(release)
	s.Observe = waitObservation{
		subscribe: func(context.Context) (EventStream, error) {
			return waitStream{
				next:  func(context.Context) (Event, error) { <-release; return Event{}, errors.New("closed") },
				close: func() error { <-release; return nil },
			}, nil
		},
		history: func(context.Context) ([]ObservedItem, error) { <-release; return nil, nil },
	}
	started := time.Now()
	result, err := s.Wait(context.Background(), WaitRequest{Reference: status.MessageID, Timeout: 20 * time.Millisecond})
	if !result.Incomplete || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("cleanup exceeded bound: %s", elapsed)
	}
}
