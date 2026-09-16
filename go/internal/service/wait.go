package service

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/agensfield/mektup/go"
)

type WaitRequest struct {
	Reference string
	Timeout   time.Duration
}

type WaitResult struct {
	Receipt     mektup.Receipt
	State       mektup.EvidenceState
	ReplyID     string
	ReplyStatus string
	Incomplete  bool
	GapReason   string
}

func (s *Service) Wait(ctx context.Context, req WaitRequest) (WaitResult, error) {
	if err := s.validate(); err != nil {
		return WaitResult{}, err
	}
	if req.Reference == "" {
		return WaitResult{}, semantic(mektup.ErrInvalidArguments, "receipt or message reference is required", nil, nil)
	}
	if req.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, req.Timeout)
		defer cancel()
	}
	if err := s.Journal.ExpireClaims(ctx); err != nil {
		return WaitResult{}, semantic(mektup.ErrInternal, "custody expiry could not be processed", nil, err)
	}
	status, err := s.Journal.Lookup(ctx, req.Reference)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return WaitResult{}, semantic(mektup.ErrWaitIncomplete, "wait ended before custody lookup completed", nil, err)
		}
		return WaitResult{}, semantic(mektup.ErrMessageNotFound, "operation receipt was not found", nil, err)
	}
	if !status.ReplyRequested || status.Semantics == "raw" {
		return WaitResult{}, semantic(mektup.ErrReplyNotRequested, "operation did not request a correlated reply", nil, nil)
	}
	if terminalWaitState(status.State) {
		return s.waitResult(status, false, ""), waitStateError(status)
	}
	pinnedStatus := status

	// ReplyRoute is independently pinned from source identity. It is the
	// native thread in which a correlated body must be observed.
	pinnedRoute := status.ReplyRoute
	if pinnedRoute == "" {
		pinnedRoute = status.SourceRoute
	}
	pinnedThread := threadID(pinnedRoute)
	if pinnedThread == "" {
		return s.waitResult(status, true, "reply route has no native thread identity"), semantic(mektup.ErrReplyRouteUnavailable, "reply route has no native thread identity", nil, nil)
	}
	if s.Observe == nil {
		return s.pollCustody(ctx, status)
	}
	pinnedTarget := ResolvedTarget{EndpointID: status.ReplyEndpointID, URI: pinnedRoute, ThreadID: pinnedThread, Loaded: true, Persistent: true}
	if pinnedTarget.EndpointID == "" {
		pinnedTarget.EndpointID = status.SourceEndpointID
	}

	scanCtx, stopScan := context.WithCancel(ctx)
	stream, err := s.Observe.Subscribe(scanCtx, pinnedTarget)
	if err != nil {
		stopScan()
		return s.pollCustody(ctx, status)
	}
	history := make(chan historyResult, 1)
	events := make(chan Event, 16)
	workerDone := make(chan struct{})
	var stopOnce sync.Once
	stopWorkers := func() {
		stopOnce.Do(func() {
			stopScan()
			_ = stream.Close()
			select {
			case <-workerDone:
			case <-time.After(250 * time.Millisecond):
			}
		})
	}
	defer stopWorkers()

	go func() {
		defer close(events)
		for {
			e, nextErr := stream.Next(scanCtx)
			if nextErr != nil {
				select {
				case events <- Event{Gap: true, Reason: "event_stream: " + nextErr.Error()}:
				case <-scanCtx.Done():
				}
				return
			}
			select {
			case events <- e:
			case <-scanCtx.Done():
				return
			}
		}
	}()
	go func() {
		defer close(workerDone)
		items, scanErr := s.Observe.FullHistory(scanCtx, pinnedTarget)
		select {
		case history <- historyResult{items: items, err: scanErr}:
		case <-scanCtx.Done():
		}
	}()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	seen := make(map[string]struct{})
	historyDone := false
	for {
		select {
		case <-ctx.Done():
			stopWorkers()
			fresh, lookupErr := s.lookupCustodyContext(ctx, req.Reference)
			if lookupErr != nil {
				fresh = status
			}
			return s.waitResult(fresh, true, ""), semantic(mektup.ErrWaitIncomplete, "wait ended before correlated reply evidence was complete", nil, ctx.Err())
		case result := <-history:
			if historyDone {
				continue
			}
			historyDone = true
			if result.err != nil {
				return s.incompleteAfterGap(req.Reference, status, "history: "+result.err.Error(), stopWorkers)
			}
			for _, item := range result.items {
				if observeErr := s.observeCandidate(ctx, pinnedStatus, item, seen, true); observeErr != nil {
					return s.incompleteAfterGap(req.Reference, status, "observation: "+observeErr.Error(), stopWorkers)
				}
			}
		case event, ok := <-events:
			if !ok {
				return s.incompleteAfterGap(req.Reference, status, "event_stream: closed", stopWorkers)
			}
			if event.Gap {
				return s.incompleteAfterGap(req.Reference, status, event.Reason, stopWorkers)
			}
			if event.Item != nil {
				if observeErr := s.observeCandidate(ctx, pinnedStatus, *event.Item, seen, false); observeErr != nil {
					return s.incompleteAfterGap(req.Reference, status, "observation: "+observeErr.Error(), stopWorkers)
				}
			}
		case <-ticker.C:
			fresh, lookupErr := s.lookupCustodyContext(ctx, req.Reference)
			if lookupErr == nil {
				status = fresh
				if pinnedStatus.ReplyDigest == "" {
					pinnedStatus.ReplyDigest, pinnedStatus.ReplyBodySize = fresh.ReplyDigest, fresh.ReplyBodySize
				}
				if terminalWaitState(status.State) {
					return s.waitResult(status, false, ""), waitStateError(status)
				}
			}
		}
	}
}

type historyResult struct {
	items []ObservedItem
	err   error
}

func (s *Service) lookupCustody(reference string) (OperationStatus, error) {
	return s.lookupCustodyContext(context.Background(), reference)
}

func (s *Service) lookupCustodyContext(parent context.Context, reference string) (OperationStatus, error) {
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	if err := s.Journal.ExpireClaims(ctx); err != nil {
		return OperationStatus{}, err
	}
	return s.Journal.Lookup(ctx, reference)
}

func (s *Service) incompleteAfterGap(reference string, status OperationStatus, reason string, stop func()) (WaitResult, error) {
	stop()
	fresh, err := s.lookupCustody(reference)
	if err != nil {
		fresh = status
	}
	return s.waitResult(fresh, true, reason), semantic(mektup.ErrWaitIncomplete, "event/history reconciliation has an explicit gap", map[string]any{"gap": reason}, err)
}

func (s *Service) pollCustody(ctx context.Context, status OperationStatus) (WaitResult, error) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		fresh, err := s.lookupCustodyContext(ctx, status.MessageID)
		if err == nil && terminalWaitState(fresh.State) {
			return s.waitResult(fresh, false, ""), waitStateError(fresh)
		}
		select {
		case <-ctx.Done():
			return s.waitResult(status, true, ""), semantic(mektup.ErrWaitIncomplete, "wait ended before correlated reply evidence was complete", nil, ctx.Err())
		case <-ticker.C:
		}
	}
}

func (s *Service) observeCandidate(ctx context.Context, status OperationStatus, item ObservedItem, seen map[string]struct{}, historical bool) error {
	e, err := validateObservedEnvelope(item, status, true)
	if err != nil {
		return nil
	}
	key := item.NativeItemID + "\x00" + e.MessageID
	if key == "\x00" {
		key = e.MessageID + "\x00" + e.PayloadSHA256
	}
	if _, ok := seen[key]; ok {
		return nil
	}
	if verified, ok := s.Journal.(VerifiedObservation); ok {
		if err := verified.ObserveVerifiedReply(ctx, status, item, e); err != nil {
			return err
		}
		seen[key] = struct{}{}
		return nil
	}
	var observeErr error
	if historical {
		observeErr = s.Journal.ReconcileReplyObservation(ctx, e.MessageID, item.NativeItemID, e.PayloadSHA256)
	} else {
		observeErr = s.Journal.ObserveReply(ctx, e.MessageID, item.NativeItemID, e.PayloadSHA256)
		if observeErr != nil {
			observeErr = s.Journal.ReconcileReplyObservation(ctx, e.MessageID, item.NativeItemID, e.PayloadSHA256)
		}
	}
	if observeErr != nil {
		return observeErr
	}
	seen[key] = struct{}{}
	return nil
}

func terminalWaitState(state mektup.EvidenceState) bool {
	return state == mektup.StateReplyAccepted || state == mektup.StateReplyObserved || state == mektup.StateReplyOutcomeUnknown
}

func waitStateError(status OperationStatus) error {
	if status.ReplyStatus == string(mektup.ReplyError) {
		return semantic(mektup.ErrDeliveryRejected, "a terminal error reply was accepted", map[string]any{"replyId": status.ReplyID, "replyStatus": status.ReplyStatus}, nil)
	}
	if status.State == mektup.StateReplyOutcomeUnknown {
		return semantic(mektup.ErrReplyOutcomeUnknown, "reply delivery outcome is unknown", nil, nil)
	}
	return nil
}

func (s *Service) waitResult(status OperationStatus, incomplete bool, gap string) WaitResult {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	kind := string(mektup.KindMessage)
	if status.Semantics == "reply" {
		kind = string(mektup.KindReply)
	}
	acceptedAt := ""
	if status.State == mektup.StateReplyAccepted || status.State == mektup.StateReplyObserved {
		acceptedAt = now
	}
	message := mektup.ReceiptMessage{MessageID: status.MessageID, InReplyTo: status.InReplyTo, Kind: kind, ReplyRequested: status.ReplyRequested, PayloadBytes: uint64(status.BodySize), PayloadSHA256: status.Digest, TurnID: status.TurnID, AcceptedAt: acceptedAt}
	if status.ReplyID != "" {
		message.ReplyAt = now
	}
	receipt := mektup.Receipt{Schema: mektup.ReceiptSchema, ReceiptID: mektup.NewReceiptID(), OperationID: status.OperationID, Operation: status.Semantics, State: status.State,
		Source: mektup.ReceiptIdentity{EndpointID: status.SourceEndpointID, ThreadID: threadID(status.SourceRoute), Resolved: status.SourceRoute},
		Target: mektup.ReceiptIdentity{EndpointID: status.TargetEndpointID, ThreadID: threadID(status.TargetRoute), Resolved: status.TargetRoute}, Message: message,
		Evidence: []mektup.EvidenceRecord{{State: status.State, At: now, Reference: status.ReplyID, Details: map[string]any{"replyStatus": status.ReplyStatus, "custodyRoute": status.CustodyRoute, "custodyStoreId": status.CustodyStoreID}}}, CreatedAt: now, UpdatedAt: now}
	return WaitResult{State: status.State, ReplyID: status.ReplyID, ReplyStatus: status.ReplyStatus, Incomplete: incomplete, GapReason: gap, Receipt: receipt}
}
