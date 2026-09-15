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
	MaxInput        int64
	MaxOutput       int64
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
	if store.Journal == nil || store.EndpointID != r.LocalEndpointID || store.EndpointID != request.Custody.EndpointID || request.ReplyDestination.EndpointID != r.LocalEndpointID {
		return nil, ErrRelationshipMismatch
	}
	canonical, err := store.Journal.ResolveStoreID(ctx, request.Custody.StoreID)
	if err != nil || canonical != store.Journal.StoreID() {
		return nil, ErrStoreUnavailable
	}
	if err := validateOriginal(ctx, store.Journal, request, canonical); err != nil {
		return nil, err
	}
	if request.Operation == "heartbeat" || request.Operation == "commit" || request.Operation == "abandon" {
		if err := validateClaimTuple(ctx, store.Journal, request, canonical); err != nil {
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
	response.RequestedLease = nil
	response.FencingToken = ""
	response.Lease = nil
	response.AttemptOwner = ""
	response.RequestedAt = ""
	encoded, err := json.Marshal(response)
	if err != nil {
		return nil, err
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

func validateOriginal(ctx context.Context, j *journal.Journal, req sshproxy.ControlRequest, storeID string) error {
	if err := validateDestinationThread(req); err != nil {
		return err
	}
	op, err := j.OperationByMessage(ctx, req.OriginalMessageID)
	if err != nil {
		return fmt.Errorf("%w: original operation unavailable: %v", ErrRelationshipMismatch, err)
	}
	if op.CustodyRoute != req.Custody.EndpointID || op.CustodyStoreID != storeID || op.ReplyRoute == "" || op.ReplyRoute != req.ReplyDestination.URI {
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
	claim, err := j.Reply(ctx, req.ReplyMessageID)
	if err != nil {
		return fmt.Errorf("%w: selected claim unavailable: %v", ErrRelationshipMismatch, err)
	}
	if claim.OriginalID != req.OriginalMessageID || claim.Digest != req.BodySHA256 ||
		(req.BodyBytes == nil || claim.BodySize != *req.BodyBytes) || claim.Status != req.ReplyStatus ||
		claim.ReplyRoute != req.ReplyDestination.URI || claim.CustodyRoute != req.Custody.EndpointID ||
		claim.CustodyStoreID != storeID || (req.AttemptOwner != "" && claim.Owner != req.AttemptOwner) {
		return ErrRelationshipMismatch
	}
	return nil
}

func apply(ctx context.Context, j *journal.Journal, req sshproxy.ControlRequest, storeID string) (json.RawMessage, error) {
	switch req.Operation {
	case "claim":
		claim, err := j.ClaimReply(ctx, journal.ClaimInput{ReplyID: req.ReplyMessageID, OriginalID: req.OriginalMessageID, Digest: req.BodySHA256, BodySize: deref(req.BodyBytes), Status: req.ReplyStatus, ReplyRoute: req.ReplyDestination.URI, CustodyRoute: req.Custody.EndpointID, CustodyStoreID: storeID, Owner: req.AttemptOwner})
		if err != nil {
			return nil, err
		}
		if claim.Joined {
			// Matching retries receive status-only metadata. The existing
			// disposition never authorizes body dispatch or fencing operations.
			return resultJSON(map[string]any{"disposition": "existing", "state": claim.State, "replyStatus": claim.Status, "won": claim.Won, "commitSeq": claim.CommitSeq})
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
		return resultJSON(map[string]any{"state": claim.State, "replyStatus": claim.Status, "commitSeq": claim.CommitSeq})
	default:
		return nil, fmt.Errorf("%w: unsupported operation", sshproxy.ErrControlValidation)
	}
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
