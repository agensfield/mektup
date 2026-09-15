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
	endpointID, threadID := receipt.Target.EndpointID, receipt.Target.ThreadID
	if receipt.Message.InReplyTo != "" || receipt.State == mektup.StateReplyOutcomeUnknown || receipt.State == mektup.StateReplyAccepted {
		endpointID, threadID = receipt.Source.EndpointID, receipt.Source.ThreadID
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
		if item.PayloadSHA256 == "" || item.PayloadSHA256 != expectedDigest {
			if len(item.Body) == 0 || bodyDigest(item.Body) != expectedDigest {
				continue
			}
		}
		if uint64(len(item.Body)) != expectedBytes && len(item.Body) != 0 {
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
	originalID := receipt.Message.MessageID
	if op, opErr := s.Journal.Operation(ctx, receipt.OperationID); opErr == nil && op.MessageID != "" {
		originalID = op.MessageID
	}
	if claims, claimsErr := s.Journal.RepliesFor(ctx, originalID); claimsErr == nil {
		for _, claim := range claims {
			if claim.Digest == expectedDigest && (receipt.ContentRef == nil || receipt.ContentRef.ClientMessageID == "" || claim.ReplyID == receipt.ContentRef.ClientMessageID) {
				replyID = claim.ReplyID
				break
			}
		}
	}
	if replyID == "" {
		if _, claimErr := s.Journal.Reply(ctx, receipt.Message.MessageID); claimErr == nil {
			replyID = receipt.Message.MessageID
		}
	}
	if replyID != "" {
		if err := s.Journal.ReconcileReplyObservation(ctx, replyID, match.ItemID, expectedDigest); err != nil {
			return mektup.Receipt{}, err
		}
		receipt.State = mektup.StateReplyObserved
	} else {
		if receipt.State == mektup.StateReplyAccepted || receipt.State == mektup.StateReplyOutcomeUnknown {
			return mektup.Receipt{}, ErrReconcileIncomplete
		}
		op, opErr := s.Journal.Operation(ctx, receipt.OperationID)
		if opErr != nil {
			return mektup.Receipt{}, opErr
		}
		if op.State == journal.StateOutcomeUnknown || op.State == journal.StateDispatchStarted {
			if err := s.Journal.RecordAccepted(ctx, receipt.OperationID, historyEvidenceReference(*match)); err != nil {
				return mektup.Receipt{}, err
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
	return receipt, nil
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
