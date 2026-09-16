// Package controlreceiver implements the one-shot metadata-only custody
// receiver used by SSH control calls. It has no CLI wiring and never dispatches
// an app-server body.
package controlreceiver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/agensfield/mektup/go/internal/journal"
	"github.com/agensfield/mektup/go/internal/sshproxy"
)

const (
	maxControlInput  = 1 << 20
	maxControlOutput = 1 << 20
)

var (
	ErrRelationshipMismatch = errors.New("control receiver: custody relationship mismatch")
	ErrControlOutput        = errors.New("control receiver: response exceeds bound")
)

type Receiver struct {
	Registry        Resolver
	LocalEndpointID string
	Destination     DestinationResolver
	MaxInput        int64
	MaxOutput       int64
}

// DestinationResolver is trusted local endpoint metadata. A control document
// may select an endpoint ID only when the ID, URI, and thread identity are an
// established route. It is separate from custody-store resolution because
// reply body and custody endpoints may differ.
type DestinationResolver interface {
	ValidateDestination(context.Context, string, string, string) error
}

type DestinationResolverFunc func(context.Context, string, string, string) error

func (f DestinationResolverFunc) ValidateDestination(ctx context.Context, endpointID, uri, threadID string) error {
	return f(ctx, endpointID, uri, threadID)
}

// Serve reads exactly one JSON control document and writes exactly one result
// document on success. It does not accept a body, path, executable, or shell
// fragment, and it never sends a Codex/app-server request.
func (r Receiver) Serve(ctx context.Context, input io.Reader, output io.Writer) error {
	if ctx == nil {
		ctx = context.Background()
	}
	limit := r.MaxInput
	if limit <= 0 {
		limit = maxControlInput
	}
	data, err := io.ReadAll(io.LimitReader(input, limit+1))
	if err != nil {
		return err
	}
	if int64(len(data)) > limit {
		return fmt.Errorf("%w: input exceeds bound", sshproxy.ErrControlValidation)
	}
	response, err := r.Receive(ctx, data)
	if err != nil {
		return err
	}
	written, err := output.Write(append(response, '\n'))
	if err == nil && written != len(response)+1 {
		return io.ErrShortWrite
	}
	return err
}

func (r Receiver) Receive(ctx context.Context, data []byte) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	request, err := sshproxy.ValidateControlRequest(data)
	if err != nil {
		return nil, err
	}
	if request.Kind != "request" {
		return nil, fmt.Errorf("%w: receiver accepts request documents only", sshproxy.ErrControlValidation)
	}
	if r.Registry == nil || r.LocalEndpointID == "" {
		return nil, ErrStoreUnavailable
	}
	store, err := r.Registry.Resolve(ctx, request.Custody.EndpointID, request.Custody.StoreID)
	if err != nil {
		return nil, err
	}
	defer store.Close()
	if store.Journal == nil || store.EndpointID != r.LocalEndpointID {
		return nil, ErrRelationshipMismatch
	}
	canonical, err := store.Journal.ResolveStoreID(ctx, request.Custody.StoreID)
	if err != nil || canonical != store.Journal.StoreID() {
		return nil, ErrStoreUnavailable
	}
	if err := r.validateOriginal(ctx, store.Journal, request, canonical); err != nil {
		return nil, err
	}
	if request.Operation == "heartbeat" || request.Operation == "commit" || request.Operation == "abandon" {
		if err := validateClaimTuple(ctx, store.Journal, request, canonical); err != nil {
			return nil, err
		}
	} else if request.Operation == "observe" {
		if err := validateObserveTuple(ctx, store.Journal, request, canonical); err != nil {
			return nil, err
		}
	} else if request.Operation == "status" || request.Operation == "reconcile" {
		if err := validateClaimIdentity(ctx, store.Journal, request, canonical); err != nil {
			return nil, err
		}
	}

	result, err := apply(ctx, store.Journal, request, canonical)
	if err != nil {
		return nil, err
	}
	response := request
	response.Kind = "result"
	response.Result = result
	response.BodyBytes = nil
	response.BodySHA256 = ""
	response.ReplyStatus = ""
	response.ReplyErrorCode = ""
	response.NativeItemID = ""
	response.RequestedLease = nil
	response.FencingToken = ""
	response.Lease = nil
	response.AttemptOwner = ""
	response.RequestedAt = ""
	encoded, err := json.Marshal(response)
	if err != nil {
		return nil, err
	}
	if request.Operation == "originalStatus" {
		var document map[string]json.RawMessage
		if err := json.Unmarshal(encoded, &document); err != nil {
			return nil, err
		}
		delete(document, "replyMessageId")
		encoded, err = json.Marshal(document)
		if err != nil {
			return nil, err
		}
	}
	limit := r.MaxOutput
	if limit <= 0 {
		limit = maxControlOutput
	}
	if int64(len(encoded)) > limit {
		return nil, ErrControlOutput
	}
	return encoded, nil
}

func (r Receiver) validateOriginal(ctx context.Context, j *journal.Journal, req sshproxy.ControlRequest, storeID string) error {
	if err := validateDestinationThread(req); err != nil {
		return err
	}
	if r.Destination == nil {
		return ErrRelationshipMismatch
	}
	if err := r.Destination.ValidateDestination(ctx, req.ReplyDestination.EndpointID, req.ReplyDestination.URI, req.ReplyDestination.ThreadID); err != nil {
		return ErrRelationshipMismatch
	}
	op, err := j.OperationByMessage(ctx, req.OriginalMessageID)
	if err != nil {
		if errors.Is(err, journal.ErrNotFound) {
			return journal.ErrNotFound
		}
		return fmt.Errorf("%w: original operation unavailable: %v", ErrRelationshipMismatch, err)
	}
	if (req.Operation == "originalStatus" && op.OperationID != req.OperationID) || op.CustodyRoute != req.Custody.EndpointID || op.CustodyStoreID != storeID || op.ReplyRoute == "" || op.ReplyRoute != req.ReplyDestination.URI || op.ReplyEndpointID == "" || op.ReplyEndpointID != req.ReplyDestination.EndpointID || op.ReplyThreadID == "" || op.ReplyThreadID != req.ReplyDestination.ThreadID {
		return ErrRelationshipMismatch
	}
	return nil
}

func validateDestinationThread(req sshproxy.ControlRequest) error {
	u, err := url.Parse(req.ReplyDestination.URI)
	if err != nil || u.Scheme == "" || u.Host == "" {
		return ErrRelationshipMismatch
	}
	segment, err := url.PathUnescape(path.Base(strings.TrimSuffix(u.Path, "/")))
	if err != nil || segment == "." || segment == "/" || segment == "" {
		return ErrRelationshipMismatch
	}
	// The stable thread identity is the decoded final URI segment. Display
	// aliases or presentation prefixes are not identity evidence.
	if req.ReplyDestination.ThreadID != segment {
		return ErrRelationshipMismatch
	}
	return nil
}

func validateClaimTuple(ctx context.Context, j *journal.Journal, req sshproxy.ControlRequest, storeID string) error {
	if err := validateClaimJoinTuple(ctx, j, req, storeID); err != nil {
		return err
	}
	claim, err := j.Reply(ctx, req.ReplyMessageID)
	if err != nil {
		if errors.Is(err, journal.ErrNotFound) {
			return journal.ErrNotFound
		}
		return fmt.Errorf("%w: selected claim unavailable: %v", ErrRelationshipMismatch, err)
	}
	if req.AttemptOwner != "" && claim.Owner != req.AttemptOwner {
		return ErrRelationshipMismatch
	}
	return nil
}

func validateClaimJoinTuple(ctx context.Context, j *journal.Journal, req sshproxy.ControlRequest, storeID string) error {
	claim, err := j.Reply(ctx, req.ReplyMessageID)
	if err != nil {
		if errors.Is(err, journal.ErrNotFound) {
			return journal.ErrNotFound
		}
		return fmt.Errorf("%w: selected claim unavailable: %v", ErrRelationshipMismatch, err)
	}
	if claim.Digest != req.BodySHA256 {
		return journal.ErrIdentityConflict
	}
	if claim.OriginalID != req.OriginalMessageID ||
		(req.BodyBytes == nil || claim.BodySize != *req.BodyBytes) || claim.Status != req.ReplyStatus ||
		claim.ReplyRoute != req.ReplyDestination.URI || claim.CustodyRoute != req.Custody.EndpointID ||
		claim.CustodyStoreID != storeID {
		return ErrRelationshipMismatch
	}
	return nil
}

func validateObserveTuple(ctx context.Context, j *journal.Journal, req sshproxy.ControlRequest, storeID string) error {
	if err := validateClaimJoinTuple(ctx, j, req, storeID); err != nil {
		return err
	}
	code, err := j.ReplyErrorCode(ctx, req.ReplyMessageID)
	if err != nil {
		if errors.Is(err, journal.ErrNotFound) {
			return journal.ErrNotFound
		}
		return fmt.Errorf("%w: selected claim error metadata unavailable: %v", ErrRelationshipMismatch, err)
	}
	if code != req.ReplyErrorCode {
		return ErrRelationshipMismatch
	}
	return nil
}

func validateClaimIdentity(ctx context.Context, j *journal.Journal, req sshproxy.ControlRequest, storeID string) error {
	claim, err := j.Reply(ctx, req.ReplyMessageID)
	if err != nil {
		if errors.Is(err, journal.ErrNotFound) {
			return journal.ErrNotFound
		}
		return fmt.Errorf("%w: selected claim unavailable: %v", ErrRelationshipMismatch, err)
	}
	if claim.OriginalID != req.OriginalMessageID || claim.ReplyRoute != req.ReplyDestination.URI || claim.CustodyRoute != req.Custody.EndpointID || claim.CustodyStoreID != storeID {
		return ErrRelationshipMismatch
	}
	return nil
}

func apply(ctx context.Context, j *journal.Journal, req sshproxy.ControlRequest, storeID string) (json.RawMessage, error) {
	switch req.Operation {
	case "claim":
		if _, inspectErr := j.Reply(ctx, req.ReplyMessageID); inspectErr == nil {
			if err := validateClaimJoinTuple(ctx, j, req, storeID); err != nil {
				return nil, journal.ErrIdentityConflict
			}
		}
		claim, err := j.ClaimReply(ctx, journal.ClaimInput{ReplyID: req.ReplyMessageID, OriginalID: req.OriginalMessageID, Digest: req.BodySHA256, BodySize: deref(req.BodyBytes), Status: req.ReplyStatus, ReplyRoute: req.ReplyDestination.URI, CustodyRoute: req.Custody.EndpointID, CustodyStoreID: storeID, Owner: req.AttemptOwner, ErrorCode: req.ReplyErrorCode})
		if err != nil {
			if errors.Is(err, journal.ErrClaimExpired) {
				if terminal, inspectErr := j.Reply(ctx, req.ReplyMessageID); inspectErr == nil {
					if tupleErr := validateClaimJoinTuple(ctx, j, req, storeID); tupleErr == nil {
						return existingClaimResult(terminal)
					}
				}
			}
			return nil, err
		}
		if claim.Joined {
			// Matching retries receive status-only metadata. The existing
			// disposition never authorizes body dispatch or fencing operations.
			return existingClaimResult(claim)
		}
		return resultJSON(map[string]any{"disposition": "claimed", "state": claim.State, "fencingToken": claim.Token, "lease": leaseJSON(claim.LeaseUntil)})
	case "heartbeat":
		claim, err := j.Heartbeat(ctx, req.ReplyMessageID, req.AttemptOwner, req.FencingToken)
		if err != nil {
			return nil, err
		}
		return resultJSON(map[string]any{"state": claim.State, "lease": leaseJSON(claim.LeaseUntil)})
	case "commit":
		committed, err := j.CommitReply(ctx, req.ReplyMessageID, req.AttemptOwner, req.FencingToken)
		if err != nil {
			return nil, err
		}
		return resultJSON(map[string]any{"state": committed.Claim.State, "wakeRecorded": true, "won": committed.Won})
	case "abandon":
		if err := j.AbandonReply(ctx, req.ReplyMessageID, req.AttemptOwner, req.FencingToken); err != nil {
			return nil, err
		}
		return resultJSON(map[string]any{"state": journal.StateReplyOutcomeUnknown})
	case "observe":
		if err := j.ObserveReplyWithProvenance(ctx, req.ReplyMessageID, req.NativeItemID, req.BodySHA256, req.ReplyDestination.EndpointID, req.Custody.EndpointID); err != nil {
			return nil, err
		}
		claim, err := j.Reply(ctx, req.ReplyMessageID)
		if err != nil {
			return nil, err
		}
		observedNative, _, observedEndpoint, observedRoute, err := j.Observation(ctx, req.ReplyMessageID)
		if err != nil {
			return nil, err
		}
		winnerClaim, winnerNative, winnerSeq, winnerErr := j.WinnerDetails(ctx, claim.OriginalID)
		if winnerErr != nil && !errors.Is(winnerErr, journal.ErrNotFound) {
			return nil, winnerErr
		}
		result := map[string]any{"state": claim.State, "status": "observed", "provenance": map[string]any{"endpointId": observedEndpoint, "controlRoute": observedRoute}}
		if winnerErr == nil {
			winner := map[string]any{"replyMessageId": winnerClaim.ReplyID, "commitSeq": winnerSeq, "status": winnerClaim.Status, "bodyBytes": winnerClaim.BodySize, "bodySha256": winnerClaim.Digest}
			if winnerClaim.ReplyErrorCode != "" {
				winner["replyErrorCode"] = winnerClaim.ReplyErrorCode
			}
			if winnerNative != "" {
				winner["nativeItemId"] = winnerNative
			}
			if winnerClaim.ReplyID == claim.ReplyID && observedNative != "" && winnerNative == "" {
				winner["nativeItemId"] = observedNative
			}
			result["winner"] = winner
		}
		return resultJSON(result)
	case "status", "reconcile":
		if req.Operation == "reconcile" {
			if err := j.ExpireClaims(ctx); err != nil {
				return nil, err
			}
		}
		claim, err := j.Reply(ctx, req.ReplyMessageID)
		if err != nil {
			return nil, err
		}
		return resultJSON(map[string]any{"state": claim.State, "replyStatus": claim.Status, "replyErrorCode": claim.ReplyErrorCode, "commitSeq": claim.CommitSeq})
	case "originalStatus":
		status, err := j.OriginalStatus(ctx, req.OriginalMessageID)
		if err != nil {
			return nil, err
		}
		result := map[string]any{"selection": status.Selection}
		switch status.Selection {
		case "winner":
			result["replyMessageId"] = status.Claim.ReplyID
			result["replyStatus"] = status.Claim.Status
			result["bodyBytes"] = status.Claim.BodySize
			result["bodySha256"] = status.Claim.Digest
			result["state"] = status.Claim.State
			result["commitSeq"] = status.EventSeq
			if status.Claim.ReplyErrorCode != "" {
				result["replyErrorCode"] = status.Claim.ReplyErrorCode
			}
			if status.NativeItemID != "" {
				result["nativeItemId"] = status.NativeItemID
			}
		case "terminal_unknown":
			result["replyMessageId"] = status.Claim.ReplyID
			result["replyStatus"] = status.Claim.Status
			result["bodyBytes"] = status.Claim.BodySize
			result["bodySha256"] = status.Claim.Digest
			result["state"] = status.Claim.State
			result["eventSeq"] = status.TerminalEventSeq
			if status.Claim.ReplyErrorCode != "" {
				result["replyErrorCode"] = status.Claim.ReplyErrorCode
			}
		}
		return resultJSON(result)
	default:
		return nil, fmt.Errorf("%w: unsupported operation", sshproxy.ErrControlValidation)
	}
}

func existingClaimResult(claim journal.ReplyClaim) (json.RawMessage, error) {
	return resultJSON(map[string]any{"disposition": "existing", "state": claim.State, "replyStatus": claim.Status, "commitSeq": claim.CommitSeq})
}

func resultJSON(value map[string]any) (json.RawMessage, error) {
	data, err := json.Marshal(value)
	return json.RawMessage(data), err
}

func leaseJSON(until int64) map[string]string {
	return map[string]string{"expiresAt": time.Unix(0, until).UTC().Format(time.RFC3339Nano)}
}

func deref(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}
