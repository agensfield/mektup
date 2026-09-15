package service

import (
	"context"
	"errors"
	"time"

	"github.com/agensfield/mektup/go"
)

type OriginalMessage struct {
	Envelope      mektup.Envelope
	CurrentThread string
}

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
	MessageID       string
	OperationID     string
	Body            string
	Status          mektup.ReplyStatus
	ErrorCode       string
	Source          string
	Wait            bool
	DeliveryTimeout time.Duration
	WaitTimeout     time.Duration
}

type ReplyResult struct {
	Receipt mektup.Receipt
	Wait    *WaitResult
}

func (s *Service) Reply(ctx context.Context, resolver OriginalResolver, req ReplyRequest) (ReplyResult, error) {
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
	op := Operation{OperationID: opID, MessageID: replyID, SourceRoute: source.URI, TargetRoute: target.URI, Semantics: "reply", ReplyRoute: e.ReplyTo, CustodyRoute: e.ReplyCustodyEndpointID, CustodyStoreID: e.ReplyCustodyStoreID,
		Digest: digest(req.Body), BodySize: int64(len([]byte(req.Body))), ReplyRequested: req.Wait, SourceEndpointID: source.EndpointID, TargetEndpointID: target.EndpointID}
	prepared, err := s.Journal.Prepare(ctx, op)
	if err != nil {
		return ReplyResult{}, semantic(mektup.ErrMessageIdentityConflict, "reply relationship could not be prepared", nil, err)
	}
	owner := prepared.Owner
	if owner == "" {
		owner = "reply-" + replyID
	}
	claim, err := s.Journal.ClaimReply(ctx, ReplyClaimInput{ReplyID: replyID, OriginalID: original.Envelope.MessageID, Digest: digest(req.Body), BodySize: int64(len([]byte(req.Body))), Status: string(req.Status), ReplyRoute: original.Envelope.ReplyTo, CustodyRoute: original.Envelope.ReplyCustodyEndpointID, CustodyStoreID: original.Envelope.ReplyCustodyStoreID, Owner: owner})
	if err != nil {
		if errors.Is(err, ErrClaimExpired) {
			return ReplyResult{}, semantic(mektup.ErrReplyOutcomeUnknown, "reply claim expired and will not be replayed", nil, err)
		}
		return ReplyResult{}, semantic(mektup.ErrMessageIdentityConflict, "reply claim failed", nil, err)
	}
	if claim.Joined || claim.State == mektup.StateReplyAccepted || claim.State == mektup.StateReplyObserved {
		return ReplyResult{Receipt: receiptFor(op, e, claim.State, "")}, nil
	}

	// Heartbeat is independent of the body call. The body path never runs
	// inside a journal transaction and a late commit is fenced by token/lease.
	heartbeatCtx, stopHeartbeat := context.WithCancel(context.Background())
	defer stopHeartbeat()
	go heartbeat(heartbeatCtx, s.Journal, claim.ReplyID, claim.Owner, claim.Token)
	if prepared.Token != "" {
		if err := s.Journal.MarkDispatchStarted(ctx, prepared.OperationID, prepared.Owner, prepared.Token); err != nil {
			_ = s.Journal.AbandonReply(ctx, claim.ReplyID, claim.Owner, claim.Token)
			return ReplyResult{}, semantic(mektup.ErrInternal, "reply dispatch fence could not be committed", nil, err)
		}
	}
	deliveryCtx := ctx
	var cancel context.CancelFunc
	if req.DeliveryTimeout > 0 {
		deliveryCtx, cancel = context.WithTimeout(ctx, req.DeliveryTimeout)
		defer cancel()
	}
	delivery, deliveryErr := s.dispatch(deliveryCtx, target, string(payload), replyID)
	if deliveryErr != nil {
		_ = s.Journal.AbandonReply(ctx, claim.ReplyID, claim.Owner, claim.Token)
		state, _, _ := classifyDelivery(deliveryErr)
		if state == mektup.StateRejected || state == mektup.StateNotSent {
			return ReplyResult{}, semantic(mektup.ErrDeliveryRejected, "reply body was not accepted", map[string]any{"evidenceState": state}, deliveryErr)
		}
		return ReplyResult{}, semantic(mektup.ErrReplyOutcomeUnknown, "reply body outcome is unknown; it will not be replayed", nil, deliveryErr)
	}
	if _, err := s.Journal.CommitReply(ctx, claim.ReplyID, claim.Owner, claim.Token); err != nil {
		return ReplyResult{}, semantic(mektup.ErrReplyOutcomeUnknown, "reply acceptance commit was fenced or unavailable", nil, err)
	}
	_ = s.Journal.RecordResult(ctx, prepared.OperationID, mektup.StateAccepted, delivery.Evidence)
	out := ReplyResult{Receipt: receiptFor(op, e, mektup.StateReplyAccepted, delivery.TurnID)}
	if req.Wait {
		wait, waitErr := s.Wait(ctx, WaitRequest{Reference: replyID, Timeout: req.WaitTimeout})
		out.Wait = &wait
		if waitErr != nil {
			return out, waitErr
		}
	}
	return out, nil
}

func heartbeat(ctx context.Context, journal JournalPort, replyID, owner, token string) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			_ = journal.Heartbeat(ctx, replyID, owner, token)
		}
	}
}

var ErrClaimExpired = errors.New("reply claim expired")
