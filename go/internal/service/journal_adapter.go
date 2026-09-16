package service

import (
	"context"
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/internal/journal"
)

// IdentityBinding is the endpoint identity context for one durable operation.
// The identity registry remains the service-level endpoint capability seam.
// It is deliberately keyed by operation/message identity, never by a mutable
// adapter-wide "current endpoint".
type IdentityBinding struct {
	SourceEndpointID string
	TargetEndpointID string
	ReplyEndpointID  string
}

// IdentityRegistry is the safe seam for reconstructing endpoint identity on a
// later process invocation. A durable implementation may back it with a
// separate owner-private registry; missing entries must fail closed.
type IdentityRegistry interface {
	Bind(context.Context, string, string, IdentityBinding) error
	Lookup(context.Context, string, string) (IdentityBinding, error)
}

// MemoryIdentityRegistry is useful for one process and deterministic tests. A
// binding is immutable after first insertion, which prevents a later alias or
// operation from rebinding an existing receipt.
type MemoryIdentityRegistry struct {
	mu      sync.RWMutex
	entries map[string]IdentityBinding
}

func NewMemoryIdentityRegistry() *MemoryIdentityRegistry {
	return &MemoryIdentityRegistry{entries: make(map[string]IdentityBinding)}
}

func identityKey(operationID, messageID string) string { return operationID + "\x00" + messageID }

func (r *MemoryIdentityRegistry) Bind(_ context.Context, operationID, messageID string, binding IdentityBinding) error {
	if r == nil || operationID == "" || messageID == "" || binding.SourceEndpointID == "" || binding.TargetEndpointID == "" {
		return errors.New("service: incomplete operation identity binding")
	}
	k := identityKey(operationID, messageID)
	r.mu.Lock()
	defer r.mu.Unlock()
	if existing, ok := r.entries[k]; ok {
		if existing != binding {
			return errors.New("service: operation identity binding conflict")
		}
		return nil
	}
	r.entries[k] = binding
	return nil
}

func (r *MemoryIdentityRegistry) Lookup(_ context.Context, operationID, messageID string) (IdentityBinding, error) {
	if r == nil {
		return IdentityBinding{}, errors.New("service: identity registry is unavailable")
	}
	r.mu.RLock()
	binding, ok := r.entries[identityKey(operationID, messageID)]
	r.mu.RUnlock()
	if !ok {
		return IdentityBinding{}, errors.New("service: operation endpoint identity is unavailable")
	}
	return binding, nil
}

// SQLiteJournal adapts the approved metadata-only journal to JournalPort.
// Endpoint IDs are pinned as metadata alongside the reply route; the
// identity registry remains the service-level endpoint capability seam.
type SQLiteJournal struct {
	Inner    *journal.Journal
	Registry IdentityRegistry
	// The legacy fields are retained for source compatibility with the early
	// service tests. Runtime construction MUST use Registry; they are not used
	// when a registry is present and are not safe as restart identity.
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
	if op.SourceEndpointID == "" {
		op.SourceEndpointID = a.SourceEndpointID
	}
	if op.TargetEndpointID == "" {
		op.TargetEndpointID = a.TargetEndpointID
	}
	if op.ReplyEndpointID == "" {
		op.ReplyEndpointID = a.ReplyEndpointID
	}
	if op.SourceEndpointID == "" || op.TargetEndpointID == "" {
		return Prepared{}, errors.New("service: operation endpoint identity is required")
	}
	binding := IdentityBinding{SourceEndpointID: op.SourceEndpointID, TargetEndpointID: op.TargetEndpointID, ReplyEndpointID: op.ReplyEndpointID}
	if a.Registry != nil {
		if err := a.Registry.Bind(ctx, op.OperationID, op.MessageID, binding); err != nil {
			return Prepared{}, err
		}
	}
	r, err := a.Inner.Prepare(ctx, journal.Operation{OperationID: op.OperationID, MessageID: op.MessageID, SourceRoute: op.SourceRoute, TargetRoute: op.TargetRoute, Semantics: op.Semantics, SourceEndpointID: op.SourceEndpointID, TargetEndpointID: op.TargetEndpointID, ReplyRoute: op.ReplyRoute, ReplyEndpointID: op.ReplyEndpointID, ReplyThreadID: threadID(op.ReplyRoute), CustodyRoute: op.CustodyRoute, CustodyStoreID: op.CustodyStoreID, AttemptOwner: op.AttemptOwner, Digest: op.Digest, BodySize: op.BodySize})
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
	binding, bindingErr := a.binding(ctx, r)
	if bindingErr != nil {
		return OperationStatus{}, bindingErr
	}
	status := operationStatus(r, binding.SourceEndpointID, binding.TargetEndpointID)
	status.ReplyEndpointID = binding.ReplyEndpointID
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

func (a SQLiteJournal) binding(ctx context.Context, r journal.OperationRecord) (IdentityBinding, error) {
	if r.SourceEndpointID == "" || r.TargetEndpointID == "" {
		return IdentityBinding{}, errors.New("service: durable operation endpoint identity is unavailable")
	}
	durable := IdentityBinding{SourceEndpointID: r.SourceEndpointID, TargetEndpointID: r.TargetEndpointID, ReplyEndpointID: r.ReplyEndpointID}
	if a.Registry != nil {
		registered, err := a.Registry.Lookup(ctx, r.OperationID, r.MessageID)
		if err != nil {
			return IdentityBinding{}, err
		}
		if registered != durable {
			return IdentityBinding{}, errors.New("service: durable and registry endpoint identity conflict")
		}
	}
	return durable, nil
}

func operationStatus(r journal.OperationRecord, sourceID, targetID string) OperationStatus {
	return OperationStatus{Operation: Operation{OperationID: r.OperationID, MessageID: r.MessageID, SourceRoute: r.SourceRoute, TargetRoute: r.TargetRoute, Semantics: r.Semantics, ReplyRoute: r.ReplyRoute, ReplyEndpointID: r.ReplyEndpointID, CustodyRoute: r.CustodyRoute, CustodyStoreID: r.CustodyStoreID, Digest: r.Digest, BodySize: r.BodySize, ReplyRequested: r.ReplyRoute != "", SourceEndpointID: sourceID, TargetEndpointID: targetID}, State: mektup.EvidenceState(r.State), ErrorCode: r.ErrorCode}
}

func (a SQLiteJournal) ClaimReply(ctx context.Context, in ReplyClaimInput) (ReplyClaim, error) {
	if err := a.valid(); err != nil {
		return ReplyClaim{}, err
	}
	c, err := a.Inner.ClaimReply(ctx, journal.ClaimInput{ReplyID: in.ReplyID, OriginalID: in.OriginalID, Digest: in.Digest, BodySize: in.BodySize, Status: in.Status, ReplyRoute: in.ReplyRoute, CustodyRoute: in.CustodyRoute, CustodyStoreID: in.CustodyStoreID, Owner: in.Owner, ErrorCode: in.ErrorCode})
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
