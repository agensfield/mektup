package receipts

import (
	"context"
	"fmt"
	"time"

	mektup "github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/internal/journal"
)

// Reconcile performs one exact full-history read and, when the matching item
// is proven, strengthens the local journal. It has no resend path by design.
func (s Store) Reconcile(ctx context.Context, reference string, history HistoryPort) (mektup.Receipt, error) {
	if err := s.valid(); err != nil {
		return mektup.Receipt{}, err
	}
	if reference == "" || history == nil {
		return mektup.Receipt{}, fmt.Errorf("%w: reference and exact history port are required", ErrInvalidArguments)
	}
	receipt, err := s.Show(ctx, reference, ShowOptions{})
	if err != nil {
		return mektup.Receipt{}, err
	}
	if receipt.ContentRef == nil && isReplyWaitReceipt(receipt.State) {
		return s.reconcileDurableReply(ctx, receipt, history)
	}
	previous := receipt
	previous.Evidence = append([]mektup.EvidenceRecord(nil), receipt.Evidence...)
	// Reply content locators carry their own pinned native destination. For a
	// wait receipt without ContentRef, the durable operation's reply route is
	// resolved by reconcileDurableReply above; ordinary receipts use Target.
	endpointID, threadID := receipt.Target.EndpointID, receipt.Target.ThreadID
	if receipt.ContentRef != nil {
		endpointID, threadID = receipt.ContentRef.EndpointID, receipt.ContentRef.ThreadID
	}
	if endpointID == "" || threadID == "" {
		return mektup.Receipt{}, ErrRouteUnavailable
	}
	items, err := history.FullHistory(ctx, endpointID, threadID)
	if err != nil {
		return mektup.Receipt{}, fmt.Errorf("%w: %v", ErrReconcileIncomplete, err)
	}
	var match *HistoryItem
	expectedDigest, expectedBytes := receipt.Message.PayloadSHA256, receipt.Message.PayloadBytes
	if receipt.ContentRef != nil {
		expectedDigest, expectedBytes = receipt.ContentRef.PayloadSHA256, receipt.ContentRef.PayloadBytes
	}
	for i := range items {
		item := &items[i]
		if item.EndpointID != "" && item.EndpointID != endpointID {
			continue
		}
		if item.ThreadID != "" && item.ThreadID != threadID {
			continue
		}
		if receipt.ContentRef != nil {
			if !contentIdentityMatches(*item, receipt.Message.MessageID, receipt.ContentRef) {
				continue
			}
		} else if item.MessageID != receipt.Message.MessageID && item.ClientMessageID != receipt.Message.MessageID {
			continue
		}
		if item.TurnID != "" && receipt.Message.TurnID != "" && item.TurnID != receipt.Message.TurnID {
			continue
		}
		// Full-history reconciliation is positive evidence only when the full
		// body is present. Advertised native metadata is an untrusted claim and
		// must never substitute for hashing the actual bytes.
		if item.Body == nil || bodyDigest(item.Body) != expectedDigest {
			continue
		}
		if uint64(len(item.Body)) != expectedBytes {
			continue
		}
		if match != nil {
			return mektup.Receipt{}, fmt.Errorf("%w: multiple exact history items", ErrIdentityMismatch)
		}
		match = item
	}
	if match == nil {
		return receipt, ErrReconcileIncomplete
	}

	// A reply claim is strengthened through its dedicated monotonic transition;
	// ordinary operation acceptance uses a separate journal transition. A
	// portable reply receipt names the original operation, so resolve its
	// candidate claim by the stored operation relationship and digest.
	var replyID string
	if receipt.ContentRef != nil {
		if receipt.ContentRef.ClientMessageID == "" {
			return mektup.Receipt{}, fmt.Errorf("%w: reply claim identity is absent", ErrIdentityMismatch)
		}
		originalID := receipt.Message.MessageID
		if op, opErr := s.Journal.Operation(ctx, receipt.OperationID); opErr == nil && op.MessageID != "" {
			originalID = op.MessageID
		}
		claims, claimsErr := s.Journal.RepliesFor(ctx, originalID)
		if claimsErr != nil {
			return mektup.Receipt{}, claimsErr
		}
		for _, claim := range claims {
			if claim.ReplyID == receipt.ContentRef.ClientMessageID {
				if claim.OriginalID != originalID || claim.Digest != expectedDigest {
					return mektup.Receipt{}, ErrIdentityMismatch
				}
				if replyID != "" {
					return mektup.Receipt{}, fmt.Errorf("%w: multiple reply claims", ErrIdentityMismatch)
				}
				replyID = claim.ReplyID
			}
		}
		if replyID == "" {
			return mektup.Receipt{}, ErrIdentityMismatch
		}
	}
	var transition func() error
	if replyID != "" {
		receipt.State = mektup.StateReplyObserved
		transition = func() error {
			return s.Journal.ReconcileReplyObservation(ctx, replyID, match.ItemID, expectedDigest)
		}
	} else {
		op, opErr := s.Journal.Operation(ctx, receipt.OperationID)
		if opErr != nil {
			return mektup.Receipt{}, opErr
		}
		if op.State == journal.StateOutcomeUnknown || op.State == journal.StateDispatchStarted {
			transition = func() error {
				return s.Journal.RecordAccepted(ctx, receipt.OperationID, historyEvidenceReference(*match))
			}
		}
		if receipt.State == mektup.StateOutcomeUnknown || receipt.State == mektup.StateDispatchStarted {
			receipt.State = mektup.StateAccepted
		}
	}
	now := s.now().Format(time.RFC3339Nano)
	receipt.UpdatedAt = now
	receipt.Evidence = append(receipt.Evidence, mektup.EvidenceRecord{State: receipt.State, At: now, Kind: "exact_history", Reference: historyEvidenceReference(*match)})
	if receipt.ContentRef == nil && receipt.Message.InReplyTo != "" {
		receipt.ContentRef = &mektup.ContentRef{EndpointID: endpointID, ThreadID: threadID, TurnID: match.TurnID, ItemID: match.ItemID, ClientMessageID: match.ClientMessageID, PayloadBytes: receipt.Message.PayloadBytes, PayloadSHA256: receipt.Message.PayloadSHA256}
	}
	if err := s.Journal.PutReceipt(ctx, receipt); err != nil {
		return mektup.Receipt{}, err
	}
	if transition != nil {
		if err := transition(); err != nil {
			if rollbackErr := s.Journal.PutReceipt(ctx, previous); rollbackErr != nil {
				return mektup.Receipt{}, fmt.Errorf("%w: projection rollback failed: %v (transition: %v)", ErrReconcileIncomplete, rollbackErr, err)
			}
			return mektup.Receipt{}, err
		}
	}
	return receipt, nil
}

func isReplyWaitReceipt(state mektup.EvidenceState) bool {
	return state == mektup.StateReplyAccepted || state == mektup.StateReplyOutcomeUnknown || state == mektup.StateReplyObserved
}

// reconcileDurableReply resolves the selected reply from the authoritative
// custody journal, then reads the pinned reply destination. Receipt evidence
// fields are presentation only and never select a claim or digest.
func (s Store) reconcileDurableReply(ctx context.Context, receipt mektup.Receipt, history HistoryPort) (mektup.Receipt, error) {
	op, err := s.Journal.Operation(ctx, receipt.OperationID)
	if err != nil {
		return mektup.Receipt{}, err
	}
	status, err := s.Journal.OriginalStatus(ctx, op.MessageID)
	if err != nil {
		return mektup.Receipt{}, err
	}
	if status.Selection != journal.OriginalStatusWinner && status.Selection != journal.OriginalStatusTerminalUnknown {
		return mektup.Receipt{}, ErrReconcileIncomplete
	}
	claim := status.Claim
	endpointID := op.ReplyEndpointID
	if endpointID == "" {
		endpointID = receipt.Source.EndpointID
	}
	threadID := op.ReplyThreadID
	if threadID == "" {
		if parsed, parseErr := mektup.ParseThreadURI(op.ReplyRoute); parseErr == nil {
			threadID = parsed.ThreadID
		}
	}
	if threadID == "" {
		threadID = receipt.Source.ThreadID
	}
	if endpointID == "" || threadID == "" {
		return mektup.Receipt{}, ErrRouteUnavailable
	}
	items, err := history.FullHistory(ctx, endpointID, threadID)
	if err != nil {
		return mektup.Receipt{}, fmt.Errorf("%w: %v", ErrReconcileIncomplete, err)
	}
	var match *HistoryItem
	for i := range items {
		item := &items[i]
		if !replyHistoryMatch(*item, endpointID, threadID, claim, op) {
			continue
		}
		if match != nil {
			return mektup.Receipt{}, fmt.Errorf("%w: multiple exact reply items", ErrIdentityMismatch)
		}
		match = item
	}
	if match == nil {
		return mektup.Receipt{}, ErrReconcileIncomplete
	}
	now := s.now().Format(time.RFC3339Nano)
	updated := receipt
	updated.State = mektup.StateReplyObserved
	updated.UpdatedAt = now
	updated.ContentRef = &mektup.ContentRef{EndpointID: endpointID, ThreadID: threadID, TurnID: match.TurnID, ItemID: match.ItemID, ClientMessageID: claim.ReplyID, PayloadBytes: uint64(claim.BodySize), PayloadSHA256: claim.Digest}
	updated.Evidence = append(updated.Evidence, mektup.EvidenceRecord{State: mektup.StateReplyObserved, At: now, Reference: claim.ReplyID, Details: map[string]any{"replyStatus": claim.Status, "replyDigest": claim.Digest, "replyBodyBytes": claim.BodySize, "commitSeq": claim.CommitSeq}})
	if concrete, ok := s.Journal.(*journal.Journal); ok {
		if err := concrete.ReconcileReplyObservationWithReceipt(ctx, claim.ReplyID, match.ItemID, claim.Digest, updated); err != nil {
			return mektup.Receipt{}, err
		}
		return updated, nil
	}
	if err := s.Journal.ReconcileReplyObservation(ctx, claim.ReplyID, match.ItemID, claim.Digest); err != nil {
		return mektup.Receipt{}, err
	}
	if err := s.Journal.PutReceipt(ctx, updated); err != nil {
		return mektup.Receipt{}, err
	}
	return updated, nil
}

func replyHistoryMatch(item HistoryItem, endpointID, threadID string, claim journal.ReplyClaim, op journal.OperationRecord) bool {
	if item.EndpointID != endpointID || item.ThreadID != threadID || item.MessageID != claim.ReplyID || item.ClientMessageID != claim.ReplyID || item.InReplyTo != claim.OriginalID || item.Body == nil {
		return false
	}
	if item.EnvelopeToEndpointID != endpointID || threadIdentity(item.EnvelopeTo) != threadID {
		return false
	}
	if op.TargetEndpointID != "" && item.EnvelopeFromEndpointID != op.TargetEndpointID {
		return false
	}
	if op.TargetRoute != "" && threadIdentity(op.TargetRoute) != "" && threadIdentity(item.EnvelopeFrom) != threadIdentity(op.TargetRoute) {
		return false
	}
	if bodyDigest(item.Body) != claim.Digest || uint64(len(item.Body)) != uint64(claim.BodySize) {
		return false
	}
	if item.PayloadSHA256 != "" && item.PayloadSHA256 != claim.Digest {
		return false
	}
	if item.ReplyStatus != claim.Status {
		return false
	}
	if claim.ReplyErrorCode != "" {
		return item.ReplyErrorCode == claim.ReplyErrorCode
	}
	// An error reply may omit its code only when custody also omitted it.
	// Successful replies never carry an error code.
	return item.ReplyErrorCode == ""
}

func threadIdentity(uri string) string {
	parsed, err := mektup.ParseThreadURI(uri)
	if err != nil {
		return ""
	}
	return parsed.ThreadID
}

func historyEvidenceReference(item HistoryItem) string {
	if item.ItemID != "" {
		return item.ItemID
	}
	if item.ClientMessageID != "" {
		return item.ClientMessageID
	}
	return item.MessageID
}
