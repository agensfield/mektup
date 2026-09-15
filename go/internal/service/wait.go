package service

import (
	"context"
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
	// Custody is consulted first. This makes a durable reply-accepted wake
	// visible even when the native item is delayed or suppressed.
	status, err := s.Journal.Lookup(ctx, req.Reference)
	if err != nil {
		return WaitResult{}, semantic(mektup.ErrMessageNotFound, "operation receipt was not found", nil, err)
	}
	if !status.ReplyRequested || status.Semantics == "raw" {
		return WaitResult{}, semantic(mektup.ErrReplyNotRequested, "operation did not request a correlated reply", nil, nil)
	}
	if terminalWaitState(status.State) {
		return s.waitResult(status, false, ""), waitStateError(status.State)
	}
	if s.Observe == nil {
		return s.pollCustody(ctx, status)
	}

	stream, err := s.Observe.Subscribe(ctx, threadID(status.SourceRoute))
	if err != nil {
		return s.pollCustody(ctx, status)
	}
	defer stream.Close()
	events := make(chan Event, 16)
	go func() {
		defer close(events)
		for {
			e, err := stream.Next(ctx)
			if err != nil {
				return
			}
			select {
			case events <- e:
			case <-ctx.Done():
				return
			}
		}
	}()
	history := make(chan historyResult, 1)
	go func() {
		items, err := s.Observe.FullHistory(ctx, threadID(status.SourceRoute))
		history <- historyResult{items, err}
	}()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	seen := make(map[string]struct{})
	historyDone := false
	gapReason := ""
	for {
		select {
		case <-ctx.Done():
			fresh, _ := s.Journal.Lookup(context.Background(), req.Reference)
			return s.waitResult(fresh, true, gapReason), semantic(mektup.ErrWaitIncomplete, "wait ended before correlated reply evidence was complete", map[string]any{"gap": gapReason}, ctx.Err())
		case result := <-history:
			if historyDone {
				continue
			}
			historyDone = true
			if result.err != nil {
				gapReason = result.err.Error()
			} else {
				for _, item := range result.items {
					s.observeCandidate(ctx, status, item, seen, true)
				}
			}
		case event, ok := <-events:
			if !ok {
				continue
			}
			if event.Gap {
				gapReason = event.Reason
				continue
			}
			if event.Item != nil {
				s.observeCandidate(ctx, status, *event.Item, seen, false)
			}
		case <-ticker.C:
			fresh, lookupErr := s.Journal.Lookup(ctx, req.Reference)
			if lookupErr == nil {
				status = fresh
				if terminalWaitState(status.State) {
					return s.waitResult(status, false, gapReason), waitStateError(status.State)
				}
			}
			if historyDone && gapReason != "" {
				return s.waitResult(status, true, gapReason), semantic(mektup.ErrWaitIncomplete, "event/history reconciliation has an explicit gap", map[string]any{"gap": gapReason}, nil)
			}
		}
	}
}

type historyResult struct {
	items []ObservedItem
	err   error
}

func (s *Service) pollCustody(ctx context.Context, status OperationStatus) (WaitResult, error) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		fresh, err := s.Journal.Lookup(ctx, status.MessageID)
		if err == nil && terminalWaitState(fresh.State) {
			return s.waitResult(fresh, false, ""), waitStateError(fresh.State)
		}
		select {
		case <-ctx.Done():
			return s.waitResult(status, true, ""), semantic(mektup.ErrWaitIncomplete, "wait ended before correlated reply evidence was complete", nil, ctx.Err())
		case <-ticker.C:
		}
	}
}

func (s *Service) observeCandidate(ctx context.Context, status OperationStatus, item ObservedItem, seen map[string]struct{}, historical bool) {
	key := item.NativeItemID + "\x00" + item.ClientMessageID
	if key == "\x00" {
		key = item.Text
	}
	if _, ok := seen[key]; ok {
		return
	}
	seen[key] = struct{}{}
	e, err := validateObservedEnvelope(item, status, true)
	if err != nil {
		return
	}
	if historical {
		_ = s.Journal.ReconcileReplyObservation(ctx, e.MessageID, item.NativeItemID, e.PayloadSHA256)
	} else if err := s.Journal.ObserveReply(ctx, e.MessageID, item.NativeItemID, e.PayloadSHA256); err != nil {
		// A native item may be the first positive evidence after a fenced
		// claim expired. Strengthen unknown separately without reviving its
		// dispatch token.
		_ = s.Journal.ReconcileReplyObservation(ctx, e.MessageID, item.NativeItemID, e.PayloadSHA256)
	}
}

func terminalWaitState(state mektup.EvidenceState) bool {
	return state == mektup.StateReplyAccepted || state == mektup.StateReplyObserved || state == mektup.StateReplyOutcomeUnknown
}
func waitStateError(state mektup.EvidenceState) error {
	switch state {
	case mektup.StateReplyOutcomeUnknown:
		return semantic(mektup.ErrReplyOutcomeUnknown, "reply delivery outcome is unknown", nil, nil)
	case mektup.StateReplyAccepted, mektup.StateReplyObserved:
		return nil
	default:
		return nil
	}
}
func (s *Service) waitResult(status OperationStatus, incomplete bool, gap string) WaitResult {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return WaitResult{State: status.State, ReplyID: status.ReplyID, ReplyStatus: status.ReplyStatus, Incomplete: incomplete, GapReason: gap, Receipt: mektup.Receipt{Schema: mektup.ReceiptSchema, ReceiptID: mektup.NewReceiptID(), OperationID: status.OperationID, Operation: status.Semantics, State: status.State, Message: mektup.ReceiptMessage{MessageID: status.MessageID, Kind: "message", ReplyRequested: status.ReplyRequested, PayloadBytes: uint64(status.BodySize), PayloadSHA256: status.Digest, TurnID: status.TurnID}, Evidence: []mektup.EvidenceRecord{{State: status.State, At: now}}, CreatedAt: now, UpdatedAt: now}}
}
