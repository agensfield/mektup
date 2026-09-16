package service

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/agensfield/mektup/go"
)

type OriginalMessage struct {
	Envelope      mektup.Envelope
	CurrentThread string
}

// ErrOriginalIdentityConflict means exact lookup found more than one
// materially different valid envelope for the requested identity. Callers
// must not choose a fuzzy or ranked candidate.
var ErrOriginalIdentityConflict = errors.New("service: original message identity conflict")

// OriginalResolver is deliberately separate from endpoint resolution. It may
// use current history, a local receipt, or an explicitly imported receipt, but
// it must prove that the envelope addresses the current thread.
type OriginalResolver interface {
	ResolveOriginal(context.Context, string) (OriginalMessage, error)
}

type ReplyRequest struct {
	Reference string
	// MessageID and OperationID are optional recovery inputs. Normal callers
	// leave them empty and the service allocates UUIDv7 identities; a receiver
	// retry may supply the original pair so ClaimReply can join the fenced
	// attempt instead of creating another body.
	MessageID              string
	OperationID            string
	Body                   string
	Status                 mektup.ReplyStatus
	ErrorCode              string
	Source                 string
	Wait                   bool
	DeliveryTimeout        time.Duration
	WaitTimeout            time.Duration
	DisableDeliveryTimeout bool
}

type ReplyResult struct {
	Receipt mektup.Receipt
	Wait    *WaitResult
}

func (s *Service) Reply(ctx context.Context, resolver OriginalResolver, req ReplyRequest) (ReplyResult, error) {
	return s.reply(ctx, resolver, req, nil)
}

// ReplyWithAcceptance exposes the durable reply acceptance boundary before an
// optional correlated child-reply wait. It reuses the ordinary fenced claim,
// delivery, heartbeat, commit, and cleanup path.
type ReplyAcceptanceCallback func(ReplyResult) error

func (s *Service) ReplyWithAcceptance(ctx context.Context, resolver OriginalResolver, req ReplyRequest, onAccepted ReplyAcceptanceCallback) (ReplyResult, error) {
	return s.reply(ctx, resolver, req, onAccepted)
}

func (s *Service) reply(ctx context.Context, resolver OriginalResolver, req ReplyRequest, onAccepted ReplyAcceptanceCallback) (ReplyResult, error) {
	if err := s.validate(); err != nil {
		return ReplyResult{}, err
	}
	if resolver == nil {
		return ReplyResult{}, semantic(mektup.ErrMessageNotFound, "original message resolver is required", nil, nil)
	}
	if req.Reference == "" || req.Body == "" {
		return ReplyResult{}, semantic(mektup.ErrInvalidArguments, "original reference and non-empty body are required", nil, nil)
	}
	if req.Status == "" {
		req.Status = mektup.ReplySuccess
	}
	if req.Status != mektup.ReplySuccess && req.Status != mektup.ReplyError {
		return ReplyResult{}, semantic(mektup.ErrInvalidArguments, "reply status must be success or error", nil, nil)
	}
	if req.Status == mektup.ReplySuccess && req.ErrorCode != "" {
		return ReplyResult{}, semantic(mektup.ErrInvalidArguments, "error code requires an error reply", nil, nil)
	}

	original, err := resolver.ResolveOriginal(ctx, req.Reference)
	if err != nil {
		if errors.Is(err, ErrOriginalIdentityConflict) {
			return ReplyResult{}, semantic(mektup.ErrMessageIdentityConflict, "original message identity is ambiguous", nil, err)
		}
		return ReplyResult{}, semantic(mektup.ErrMessageNotFound, "original message could not be resolved", nil, err)
	}
	if err := original.Envelope.Validate(); err != nil {
		return ReplyResult{}, semantic(mektup.ErrMessageNotFound, "original envelope is invalid", nil, err)
	}
	if err := original.Envelope.ValidateAddressToThread(original.CurrentThread); err != nil {
		return ReplyResult{}, semantic(mektup.ErrMessageNotAddressedThread, "original envelope belongs to another thread", nil, err)
	}
	if !original.Envelope.ReplyRequested {
		return ReplyResult{}, semantic(mektup.ErrReplyRouteUnavailable, "original message did not carry a reply route", nil, nil)
	}

	source, err := s.Resolver.ResolveSource(ctx, req.Source)
	if err != nil {
		return ReplyResult{}, semantic(mektup.ErrReplyRouteUnavailable, "reply source could not be resolved", nil, err)
	}
	target, err := s.Resolver.ResolvePinned(ctx, original.Envelope.ReplyEndpointID, original.Envelope.ReplyTo)
	if err != nil {
		return ReplyResult{}, semantic(mektup.ErrReplyRouteUnavailable, "pinned reply route is unavailable", nil, err)
	}
	if target.EndpointID != original.Envelope.ReplyEndpointID || target.URI != original.Envelope.ReplyTo {
		return ReplyResult{}, semantic(mektup.ErrReplyRouteUnavailable, "resolved reply route does not match the pinned original", nil, nil)
	}
	if source.URI == "" || source.EndpointID == "" {
		return ReplyResult{}, semantic(mektup.ErrReplyRouteUnavailable, "reply source identity is incomplete", nil, nil)
	}

	replyID := req.MessageID
	if replyID == "" {
		replyID, err = mektup.NewMessageIDChecked()
		if err != nil {
			return ReplyResult{}, semantic(mektup.ErrInternal, "cannot allocate reply identity", nil, err)
		}
	} else if err := mektup.ValidateID(replyID, mektup.MessageIDPrefix); err != nil {
		return ReplyResult{}, semantic(mektup.ErrInvalidArguments, "reply message identity is invalid", nil, err)
	}
	e := mektup.Envelope{MessageID: replyID, Kind: mektup.KindReply, FromEndpointID: source.EndpointID, From: source.URI, FromKind: kindOf(source), FromHerdr: source.Herdr,
		ToEndpointID: original.Envelope.ReplyEndpointID, To: original.Envelope.ReplyTo, RequestedTarget: original.Envelope.ReplyTo, InReplyTo: original.Envelope.MessageID,
		ReplyStatus: req.Status, ReplyErrorCode: req.ErrorCode, Body: req.Body, Provenance: "observed"}
	if req.Wait {
		e.ReplyRequested = true
		e.ReplyEndpointID = source.EndpointID
		e.ReplyTo = source.URI
		e.ReplyCustodyEndpointID = source.CustodyEndpointID
		e.ReplyCustodyStoreID = source.CustodyStoreID
		if e.ReplyEndpointID == "" || e.ReplyTo == "" || e.ReplyCustodyEndpointID == "" || e.ReplyCustodyStoreID == "" {
			return ReplyResult{}, semantic(mektup.ErrReplyRouteRequired, "a routable source is required when waiting for a reply", nil, nil)
		}
	}
	e.PayloadBytes = uint64(len([]byte(req.Body)))
	e.PayloadSHA256 = digest(req.Body)
	e.SentAt = s.now().UTC().Format(time.RFC3339Nano)
	payload, err := mektup.RenderEnvelope(e)
	if err != nil {
		return ReplyResult{}, semantic(mektup.ErrInvalidArguments, "cannot render reply envelope", nil, err)
	}
	if err := preflight(payload, req.Body); err != nil {
		return ReplyResult{}, err
	}

	opID := req.OperationID
	if opID == "" {
		opID, err = mektup.NewOperationIDChecked()
		if err != nil {
			return ReplyResult{}, semantic(mektup.ErrInternal, "cannot allocate reply operation identity", nil, err)
		}
	} else if err := mektup.ValidateID(opID, mektup.OperationIDPrefix); err != nil {
		return ReplyResult{}, semantic(mektup.ErrInvalidArguments, "reply operation identity is invalid", nil, err)
	}
	op := Operation{OperationID: opID, MessageID: replyID, InReplyTo: original.Envelope.MessageID, SourceRoute: source.URI, TargetRoute: target.URI, Semantics: "reply", AttemptOwner: "reply-" + replyID, ReplyRoute: e.ReplyTo, ReplyEndpointID: e.ReplyEndpointID, CustodyRoute: e.ReplyCustodyEndpointID, CustodyStoreID: e.ReplyCustodyStoreID,
		Digest: digest(req.Body), BodySize: int64(len([]byte(req.Body))), ReplyRequested: req.Wait, SourceEndpointID: source.EndpointID, TargetEndpointID: target.EndpointID}
	prepared, err := s.Journal.Prepare(ctx, op)
	if err != nil {
		return ReplyResult{}, semantic(mektup.ErrMessageIdentityConflict, "reply relationship could not be prepared", nil, err)
	}
	owner := prepared.Owner
	if owner == "" {
		owner = "reply-" + replyID
	}
	claim, err := s.Journal.ClaimReply(ctx, ReplyClaimInput{ReplyID: replyID, OriginalID: original.Envelope.MessageID, Digest: digest(req.Body), BodySize: int64(len([]byte(req.Body))), Status: string(req.Status), ErrorCode: req.ErrorCode, ReplyRoute: original.Envelope.ReplyTo, CustodyRoute: original.Envelope.ReplyCustodyEndpointID, CustodyStoreID: original.Envelope.ReplyCustodyStoreID, Owner: owner})
	if err != nil {
		if errors.Is(err, ErrClaimExpired) {
			return ReplyResult{}, semantic(mektup.ErrReplyOutcomeUnknown, "reply claim expired and will not be replayed", nil, err)
		}
		return ReplyResult{}, semantic(mektup.ErrMessageIdentityConflict, "reply claim failed", nil, err)
	}
	if claim.Joined || claim.State == mektup.StateReplyAccepted || claim.State == mektup.StateReplyObserved {
		out := ReplyResult{Receipt: receiptFor(op, e, claim.State, "")}
		if claim.State == mektup.StateReplyAccepted || claim.State == mektup.StateReplyObserved {
			if onAccepted != nil {
				if callbackErr := onAccepted(out); callbackErr != nil {
					return out, callbackErr
				}
			}
			if req.Wait {
				wait, waitErr := s.Wait(ctx, WaitRequest{Reference: replyID, Timeout: req.WaitTimeout})
				out.Wait = &wait
				return out, waitErr
			}
			return out, nil
		}
		if req.Wait {
			if waiter, ok := s.Journal.(JoinedReplyWaiter); ok {
				waitCtx := ctx
				if req.WaitTimeout > 0 {
					var cancel context.CancelFunc
					waitCtx, cancel = context.WithTimeout(ctx, req.WaitTimeout)
					defer cancel()
				}
				waitStatus, waitErr := waiter.WaitReply(waitCtx, replyID, req.WaitTimeout)
				if waitErr == nil {
					waitErr = waitStateError(waitStatus)
				}
				if waitErr != nil && waitStatus.State != mektup.StateReplyOutcomeUnknown && waitStatus.State != mektup.StateReplyAccepted && waitStatus.State != mektup.StateReplyObserved {
					waitErr = semantic(mektup.ErrWaitIncomplete, "joined reply wait ended before a correlated child reply", nil, waitErr)
				}
				out.Wait = &WaitResult{Receipt: receiptFor(op, e, waitStatus.State, ""), State: waitStatus.State, ReplyID: waitStatus.ReplyID, ReplyStatus: waitStatus.ReplyStatus, Incomplete: waitErr != nil}
				return out, waitErr
			}
			out.Wait = &WaitResult{Receipt: out.Receipt, State: claim.State, Incomplete: true, GapReason: "reply attempt is already in flight"}
			return out, semantic(mektup.ErrWaitIncomplete, "joined reply attempt has not reached a terminal custody state", map[string]any{"replyState": claim.State}, nil)
		}
		return out, semantic(mektup.ErrWaitIncomplete, "reply dispatch is already in flight", map[string]any{"replyState": claim.State}, nil)
	}
	resumed := false
	if !target.Loaded && target.Persistent {
		if _, err := s.Delivery.Resume(ctx, target); err != nil {
			recordCtx, recordCancel := custodyContext(ctx)
			if recordErr := s.Journal.RecordResult(recordCtx, prepared.OperationID, mektup.StateNotSent, "resume_failed"); recordErr != nil {
				recordCancel()
				return ReplyResult{}, semantic(mektup.ErrInternal, "resume failure evidence could not be journaled", nil, recordErr)
			}
			if abandonErr := s.Journal.AbandonReply(recordCtx, claim.ReplyID, claim.Owner, claim.Token); abandonErr != nil {
				recordCancel()
				return ReplyResult{}, semantic(mektup.ErrInternal, "resume failure and reply claim cleanup could not be journaled", nil, errors.Join(err, abandonErr))
			}
			recordCancel()
			return ReplyResult{}, semantic(mektup.ErrDeliveryRejected, "persistent reply target could not be resumed", nil, err)
		}
		resumed = true
	}

	// Heartbeat is independent of the body call. The body path never runs
	// inside a journal transaction and a late commit is fenced by token/lease.
	heartbeatCtx, stopHeartbeat := context.WithCancel(context.Background())
	defer stopHeartbeat()
	heartbeatMonitor := newHeartbeatMonitor()
	heartbeatErrors := make(chan error, 1)
	interval := s.heartbeatInterval
	if interval <= 0 {
		interval = 5 * time.Second
	}
	go heartbeat(heartbeatCtx, s.Journal, claim.ReplyID, claim.Owner, claim.Token, heartbeatErrors, heartbeatMonitor, interval)
	if prepared.Token == "" {
		if resumed {
			_ = detachTarget(s.Delivery, target)
		}
		recordCtx, recordCancel := custodyContext(ctx)
		if abandonErr := s.Journal.AbandonReply(recordCtx, claim.ReplyID, claim.Owner, claim.Token); abandonErr != nil {
			recordCancel()
			return ReplyResult{}, semantic(mektup.ErrInternal, "unowned reply attempt cleanup could not be journaled", nil, abandonErr)
		}
		recordCancel()
		return ReplyResult{}, semantic(mektup.ErrInternal, "reply dispatch attempt is not owned; body was not sent", nil, nil)
	}
	if err := s.Journal.MarkDispatchStarted(ctx, prepared.OperationID, prepared.Owner, prepared.Token); err != nil {
		if resumed {
			_ = detachTarget(s.Delivery, target)
		}
		recordCtx, recordCancel := custodyContext(ctx)
		if abandonErr := s.Journal.AbandonReply(recordCtx, claim.ReplyID, claim.Owner, claim.Token); abandonErr != nil {
			recordCancel()
			return ReplyResult{}, semantic(mektup.ErrInternal, "reply fence failure cleanup could not be journaled", nil, errors.Join(err, abandonErr))
		}
		recordCancel()
		return ReplyResult{}, semantic(mektup.ErrInternal, "reply dispatch fence could not be committed", nil, err)
	}
	deliveryCtx, cancel := deliveryContext(ctx, req.DeliveryTimeout, req.DisableDeliveryTimeout)
	defer cancel()
	delivery, deliveryErr := s.dispatch(deliveryCtx, target, string(payload), replyID)
	if monitorErr := heartbeatMonitor.wait(deliveryCtx); monitorErr != nil {
		return ReplyResult{}, s.abortReplyHeartbeat(ctx, target, resumed, claim, prepared, monitorErr)
	}
	var heartbeatErr error
	select {
	case heartbeatErr = <-heartbeatErrors:
	default:
	}
	if heartbeatErr != nil {
		return ReplyResult{}, s.abortReplyHeartbeat(ctx, target, resumed, claim, prepared, heartbeatErr)
	}
	if deliveryErr != nil {
		state, code, retry := classifyDelivery(deliveryErr)
		if retry {
			delivery, deliveryErr, state, code = s.retryNotSubmitted(deliveryCtx, target, string(payload), replyID, deliveryErr, state, code, func() error { return heartbeatMonitor.wait(deliveryCtx) })
		}
		if heartbeatErr = readHeartbeatError(heartbeatErrors); heartbeatErr != nil {
			return ReplyResult{}, s.abortReplyHeartbeat(ctx, target, resumed, claim, prepared, heartbeatErr)
		}
		if deliveryErr == nil {
			// The exact NotSubmitted retry proved non-admission. A successful
			// retry now follows the ordinary acceptance/commit path below.
			goto replyAccepted
		}
		if state == mektup.StateNotSent {
			state, code = mektup.StateOutcomeUnknown, "outcome_unknown_after_dispatch_fence"
		}
		recordCtx, recordCancel := custodyContext(ctx)
		if abandonErr := s.Journal.AbandonReply(recordCtx, claim.ReplyID, claim.Owner, claim.Token); abandonErr != nil {
			recordCancel()
			if resumed {
				_ = detachTarget(s.Delivery, target)
			}
			return ReplyResult{}, semantic(mektup.ErrInternal, "reply uncertainty could not be journaled", map[string]any{"deliveryState": state}, abandonErr)
		}
		if recordErr := s.Journal.RecordResult(recordCtx, prepared.OperationID, state, code); recordErr != nil {
			recordCancel()
			if resumed {
				_ = detachTarget(s.Delivery, target)
			}
			return ReplyResult{}, semantic(mektup.ErrInternal, "reply delivery evidence could not be journaled", map[string]any{"deliveryState": state}, recordErr)
		}
		recordCancel()
		if state == mektup.StateRejected || state == mektup.StateNotSent {
			if resumed {
				_ = detachTarget(s.Delivery, target)
			}
			return ReplyResult{}, semantic(mektup.ErrDeliveryRejected, "reply body was not accepted", map[string]any{"evidenceState": state}, deliveryErr)
		}
		if resumed {
			_ = detachTarget(s.Delivery, target)
		}
		return ReplyResult{}, semantic(mektup.ErrReplyOutcomeUnknown, "reply body outcome is unknown; it will not be replayed", nil, deliveryErr)
	}
replyAccepted:
	if monitorErr := heartbeatMonitor.wait(deliveryCtx); monitorErr != nil {
		return ReplyResult{}, s.abortReplyHeartbeat(ctx, target, resumed, claim, prepared, monitorErr)
	}
	if heartbeatErr = readHeartbeatError(heartbeatErrors); heartbeatErr != nil {
		return ReplyResult{}, s.abortReplyHeartbeat(ctx, target, resumed, claim, prepared, heartbeatErr)
	}
	commitCtx, commitCancel := custodyContext(ctx)
	if _, err := s.Journal.CommitReply(commitCtx, claim.ReplyID, claim.Owner, claim.Token); err != nil {
		commitCancel()
		if resumed {
			_ = detachTarget(s.Delivery, target)
		}
		fallbackCtx, fallbackCancel := custodyContext(ctx)
		if recordErr := s.Journal.RecordResult(fallbackCtx, prepared.OperationID, mektup.StateOutcomeUnknown, "reply_acceptance_commit_failed"); recordErr != nil {
			fallbackCancel()
			return ReplyResult{}, semantic(mektup.ErrInternal, "reply acceptance and uncertainty evidence could not be journaled", nil, errors.Join(err, recordErr))
		}
		fallbackCancel()
		return ReplyResult{}, semantic(mektup.ErrReplyOutcomeUnknown, "reply acceptance commit was fenced or unavailable", nil, err)
	}
	commitCancel()
	recordCtx, recordCancel := custodyContext(ctx)
	if err := s.Journal.RecordResult(recordCtx, prepared.OperationID, mektup.StateAccepted, delivery.Evidence); err != nil {
		recordCancel()
		if resumed {
			_ = detachTarget(s.Delivery, target)
		}
		return ReplyResult{Receipt: receiptFor(op, e, mektup.StateReplyAccepted, delivery.TurnID)}, semantic(mektup.ErrInternal, "reply operation acceptance could not be journaled", nil, err)
	}
	recordCancel()
	if resumed {
		if detachErr := detachTarget(s.Delivery, target); detachErr != nil {
			receipt := receiptFor(op, e, mektup.StateReplyAccepted, delivery.TurnID)
			receipt.Warnings = append(receipt.Warnings, mektup.Warning{Code: mektup.WarningCleanupIncomplete, Message: "persistent reply target detach did not complete", Details: map[string]any{"error": detachErr.Error()}})
			return ReplyResult{Receipt: receipt}, semantic(mektup.ErrInternal, "reply accepted but target cleanup is incomplete", nil, detachErr)
		}
	}
	out := ReplyResult{Receipt: receiptFor(op, e, mektup.StateReplyAccepted, delivery.TurnID)}
	if onAccepted != nil {
		if callbackErr := onAccepted(out); callbackErr != nil {
			return out, callbackErr
		}
	}
	if req.Wait {
		wait, waitErr := s.Wait(ctx, WaitRequest{Reference: replyID, Timeout: req.WaitTimeout})
		out.Wait = &wait
		if waitErr != nil {
			return out, waitErr
		}
	}
	return out, nil
}

func readHeartbeatError(failures <-chan error) error {
	select {
	case err := <-failures:
		return err
	default:
		return nil
	}
}

func (s *Service) abortReplyHeartbeat(ctx context.Context, target ResolvedTarget, resumed bool, claim ReplyClaim, prepared Prepared, heartbeatErr error) error {
	recordCtx, recordCancel := custodyContext(ctx)
	abandonErr := s.Journal.AbandonReply(recordCtx, claim.ReplyID, claim.Owner, claim.Token)
	resultErr := s.Journal.RecordResult(recordCtx, prepared.OperationID, mektup.StateOutcomeUnknown, "heartbeat_failed")
	recordCancel()
	if resumed {
		if detachErr := detachTarget(s.Delivery, target); detachErr != nil {
			abandonErr = errors.Join(abandonErr, detachErr)
		}
	}
	if abandonErr != nil || resultErr != nil {
		return semantic(mektup.ErrInternal, "heartbeat failure and conservative outcome could not be journaled", nil, errors.Join(heartbeatErr, abandonErr, resultErr))
	}
	return semantic(mektup.ErrReplyOutcomeUnknown, "reply claim heartbeat failed; body outcome is conservatively unknown", nil, heartbeatErr)
}

type heartbeatMonitor struct {
	mu     sync.Mutex
	active int
	err    error
	idle   chan struct{}
}

func newHeartbeatMonitor() *heartbeatMonitor {
	idle := make(chan struct{})
	close(idle)
	return &heartbeatMonitor{idle: idle}
}
func (m *heartbeatMonitor) begin() {
	m.mu.Lock()
	if m.active == 0 {
		m.idle = make(chan struct{})
	}
	m.active++
	m.mu.Unlock()
}
func (m *heartbeatMonitor) end(err error) {
	m.mu.Lock()
	if err != nil && m.err == nil {
		m.err = err
	}
	m.active--
	if m.active == 0 {
		close(m.idle)
	}
	m.mu.Unlock()
}
func (m *heartbeatMonitor) wait(ctx context.Context) error {
	m.mu.Lock()
	idle := m.idle
	m.mu.Unlock()
	select {
	case <-idle:
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func heartbeat(ctx context.Context, journal JournalPort, replyID, owner, token string, failures chan<- error, monitor *heartbeatMonitor, interval time.Duration) {
	monitor.begin()
	err := journal.Heartbeat(ctx, replyID, owner, token)
	monitor.end(err)
	if err != nil {
		select {
		case failures <- err:
		default:
		}
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			monitor.begin()
			err := journal.Heartbeat(ctx, replyID, owner, token)
			monitor.end(err)
			if err != nil {
				select {
				case failures <- err:
				default:
				}
				return
			}
		}
	}
}

var ErrClaimExpired = errors.New("reply claim expired")
