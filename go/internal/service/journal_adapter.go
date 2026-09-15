package service

import (
	"context"
	"errors"
	"sort"
	"time"

	"github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/internal/journal"
)

// SQLiteJournal adapts the approved metadata-only journal to JournalPort.
// Endpoint IDs are supplied by the resolver layer because the journal
// intentionally stores routes, not endpoint registry records.
type SQLiteJournal struct {
	Inner            *journal.Journal
	SourceEndpointID string
	TargetEndpointID string
	ReplyEndpointID  string
}

func (a SQLiteJournal) valid() error {
	if a.Inner == nil {
		return errors.New("service: nil journal adapter")
	}
	return nil
}

func (a SQLiteJournal) Prepare(ctx context.Context, op Operation) (Prepared, error) {
	if err := a.valid(); err != nil {
		return Prepared{}, err
	}
	r, err := a.Inner.Prepare(ctx, journal.Operation{OperationID: op.OperationID, MessageID: op.MessageID, SourceRoute: op.SourceRoute, TargetRoute: op.TargetRoute, Semantics: op.Semantics, ReplyRoute: op.ReplyRoute, CustodyRoute: op.CustodyRoute, CustodyStoreID: op.CustodyStoreID, AttemptOwner: op.AttemptOwner, Digest: op.Digest, BodySize: op.BodySize})
	if err != nil {
		return Prepared{}, err
	}
	return Prepared{OperationID: r.OperationID, MessageID: r.MessageID, Owner: r.AttemptOwner, Token: r.AttemptToken}, nil
}

func (a SQLiteJournal) MarkDispatchStarted(ctx context.Context, id, owner, token string) error {
	if err := a.valid(); err != nil {
		return err
	}
	return a.Inner.MarkDispatchStartedOwned(ctx, id, owner, token)
}
func (a SQLiteJournal) RecordResult(ctx context.Context, id string, state mektup.EvidenceState, code string) error {
	if err := a.valid(); err != nil {
		return err
	}
	return a.Inner.RecordResult(ctx, id, journal.EvidenceState(state), code)
}
func (a SQLiteJournal) ExpireClaims(ctx context.Context) error {
	if err := a.valid(); err != nil {
		return err
	}
	return a.Inner.ExpireClaims(ctx)
}

func (a SQLiteJournal) Lookup(ctx context.Context, ref string) (OperationStatus, error) {
	if err := a.valid(); err != nil {
		return OperationStatus{}, err
	}
	r, err := a.Inner.Operation(ctx, ref)
	if errors.Is(err, journal.ErrNotFound) {
		r, err = a.Inner.OperationByMessage(ctx, ref)
	}
	if err != nil {
		return OperationStatus{}, err
	}
	status := operationStatus(r, a.SourceEndpointID, a.TargetEndpointID)
	status.ReplyEndpointID = a.ReplyEndpointID
	// A reply operation is itself a claimed reply. Recover its original
	// relationship from the claim so restart receipts retain InReplyTo without
	// putting bodies or relationship prose in the journal schema.
	if ownClaim, claimErr := a.Inner.Reply(ctx, r.MessageID); claimErr == nil {
		status.InReplyTo = ownClaim.OriginalID
	}
	claims, err := a.Inner.RepliesFor(ctx, r.MessageID)
	if err != nil {
		return OperationStatus{}, err
	}
	sort.SliceStable(claims, func(i, j int) bool { return claims[i].CommitSeq < claims[j].CommitSeq })
	for _, claim := range claims {
		if claim.State == journal.StateReplyAccepted || claim.State == journal.StateReplyObserved {
			if status.State != mektup.StateReplyAccepted && status.State != mektup.StateReplyObserved {
				status.State = mektup.EvidenceState(claim.State)
				status.ReplyID, status.ReplyStatus = claim.ReplyID, claim.Status
				status.ReplyDigest, status.ReplyBodySize = claim.Digest, claim.BodySize
			}
		} else if claim.State == journal.StateReplyOutcomeUnknown && status.State != mektup.StateReplyAccepted && status.State != mektup.StateReplyObserved {
			status.State = mektup.StateReplyOutcomeUnknown
			status.ReplyID, status.ReplyStatus = claim.ReplyID, claim.Status
			status.ReplyDigest, status.ReplyBodySize = claim.Digest, claim.BodySize
		}
	}
	return status, nil
}

func operationStatus(r journal.OperationRecord, sourceID, targetID string) OperationStatus {
	return OperationStatus{Operation: Operation{OperationID: r.OperationID, MessageID: r.MessageID, SourceRoute: r.SourceRoute, TargetRoute: r.TargetRoute, Semantics: r.Semantics, ReplyRoute: r.ReplyRoute, CustodyRoute: r.CustodyRoute, CustodyStoreID: r.CustodyStoreID, Digest: r.Digest, BodySize: r.BodySize, ReplyRequested: r.ReplyRoute != "", SourceEndpointID: sourceID, TargetEndpointID: targetID}, State: mektup.EvidenceState(r.State), ErrorCode: r.ErrorCode}
}

func (a SQLiteJournal) ClaimReply(ctx context.Context, in ReplyClaimInput) (ReplyClaim, error) {
	if err := a.valid(); err != nil {
		return ReplyClaim{}, err
	}
	c, err := a.Inner.ClaimReply(ctx, journal.ClaimInput{ReplyID: in.ReplyID, OriginalID: in.OriginalID, Digest: in.Digest, BodySize: in.BodySize, Status: in.Status, ReplyRoute: in.ReplyRoute, CustodyRoute: in.CustodyRoute, CustodyStoreID: in.CustodyStoreID, Owner: in.Owner})
	if err != nil {
		return ReplyClaim{}, err
	}
	return replyClaim(c), nil
}
func (a SQLiteJournal) Heartbeat(ctx context.Context, id, owner, token string) error {
	if err := a.valid(); err != nil {
		return err
	}
	_, err := a.Inner.Heartbeat(ctx, id, owner, token)
	return err
}
func (a SQLiteJournal) CommitReply(ctx context.Context, id, owner, token string) (ReplyClaim, error) {
	if err := a.valid(); err != nil {
		return ReplyClaim{}, err
	}
	r, err := a.Inner.CommitReply(ctx, id, owner, token)
	if err != nil {
		return ReplyClaim{}, err
	}
	return replyClaim(r.Claim), nil
}
func (a SQLiteJournal) AbandonReply(ctx context.Context, id, owner, token string) error {
	if err := a.valid(); err != nil {
		return err
	}
	return a.Inner.AbandonReply(ctx, id, owner, token)
}
func (a SQLiteJournal) ObserveReply(ctx context.Context, id, native, digest string) error {
	if err := a.valid(); err != nil {
		return err
	}
	return a.Inner.ObserveReply(ctx, id, native, digest)
}
func (a SQLiteJournal) ReconcileReplyObservation(ctx context.Context, id, native, digest string) error {
	if err := a.valid(); err != nil {
		return err
	}
	return a.Inner.ReconcileReplyObservation(ctx, id, native, digest)
}

func (a SQLiteJournal) WaitReply(ctx context.Context, replyID string, timeout time.Duration) (OperationStatus, error) {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := a.ExpireClaims(ctx); err != nil {
			return OperationStatus{}, err
		}
		if ownClaim, ownErr := a.Inner.Reply(ctx, replyID); ownErr == nil && ownClaim.State == journal.StateReplyOutcomeUnknown {
			status, lookupErr := a.Lookup(ctx, replyID)
			if lookupErr != nil {
				return OperationStatus{}, lookupErr
			}
			status.State = mektup.StateReplyOutcomeUnknown
			return status, nil
		}
		status, err := a.Lookup(ctx, replyID)
		if err != nil {
			return OperationStatus{}, err
		}
		if status.State == mektup.StateReplyAccepted || status.State == mektup.StateReplyObserved || status.State == mektup.StateReplyOutcomeUnknown {
			return status, nil
		}
		select {
		case <-ctx.Done():
			return status, ctx.Err()
		case <-ticker.C:
		}
	}
}

func replyClaim(c journal.ReplyClaim) ReplyClaim {
	return ReplyClaim{ReplyID: c.ReplyID, OriginalID: c.OriginalID, Digest: c.Digest, BodySize: c.BodySize, Status: c.Status, ReplyRoute: c.ReplyRoute, CustodyRoute: c.CustodyRoute, CustodyStoreID: c.CustodyStoreID, Owner: c.Owner, Token: c.Token, State: mektup.EvidenceState(c.State), Joined: c.Joined, Won: c.Won}
}

var _ JournalPort = SQLiteJournal{}
