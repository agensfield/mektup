package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	mektup "github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/internal/endpoint"
	"github.com/agensfield/mektup/go/internal/sshproxy"
)

var (
	ErrRemoteCustodyUnavailable = errors.New("service: remote custody is unavailable")
	ErrRemoteCustodyBinding     = errors.New("service: remote custody relationship is not bound")
)

// ControlInvoker is the transport seam. Production uses SSHControlInvoker;
// tests inject a deterministic function and never need a real SSH process.
type ControlInvoker func(context.Context, endpoint.Route, sshproxy.ControlRequest) ([]byte, error)

func SSHControlInvoker(ctx context.Context, route endpoint.Route, request sshproxy.ControlRequest) ([]byte, error) {
	if route.Kind != endpoint.RouteSSH || route.SSHHost == "" {
		return nil, fmt.Errorf("%w: remote custody requires an SSH route", ErrRemoteCustodyUnavailable)
	}
	return sshproxy.InvokeControl(ctx, sshproxy.Config{Host: route.SSHHost}, request, nil, nil)
}

// RemoteJournal preserves local SQLiteJournal semantics while routing only
// custody transitions to a configured SSH endpoint. It never carries a body,
// path, executable, or fencing token to a join that did not receive authority.
type RemoteJournal struct {
	Local           *SQLiteJournal
	Endpoints       endpoint.EndpointStore
	LocalEndpointID string
	Invoke          ControlInvoker
	PollInterval    time.Duration
	Now             func() time.Time

	mu         sync.Mutex
	operations map[string]Operation
	claims     map[string]remoteClaim
}

type remoteClaim struct {
	input     ReplyClaimInput
	operation Operation
	request   sshproxy.ControlRequest
	route     endpoint.Route
	lease     *sshproxy.Lease
}

func (r *RemoteJournal) init() error {
	if r == nil || r.Local == nil {
		return errors.New("service: nil local journal")
	}
	if r.Invoke == nil {
		r.Invoke = SSHControlInvoker
	}
	if r.Now == nil {
		r.Now = time.Now
	}
	if r.PollInterval <= 0 {
		r.PollInterval = 100 * time.Millisecond
	}
	if r.operations == nil {
		r.operations = make(map[string]Operation)
	}
	if r.claims == nil {
		r.claims = make(map[string]remoteClaim)
	}
	return nil
}

func (r *RemoteJournal) Prepare(ctx context.Context, op Operation) (Prepared, error) {
	if err := r.init(); err != nil {
		return Prepared{}, err
	}
	prepared, err := r.Local.Prepare(ctx, op)
	if err != nil {
		return Prepared{}, err
	}
	r.mu.Lock()
	r.operations[op.MessageID] = op
	r.mu.Unlock()
	return prepared, nil
}
func (r *RemoteJournal) MarkDispatchStarted(ctx context.Context, id, owner, token string) error {
	if err := r.init(); err != nil {
		return err
	}
	return r.Local.MarkDispatchStarted(ctx, id, owner, token)
}
func (r *RemoteJournal) RecordResult(ctx context.Context, id string, state mektup.EvidenceState, code string) error {
	if err := r.init(); err != nil {
		return err
	}
	return r.Local.RecordResult(ctx, id, state, code)
}
func (r *RemoteJournal) ExpireClaims(ctx context.Context) error {
	if err := r.init(); err != nil {
		return err
	}
	return r.Local.ExpireClaims(ctx)
}
func (r *RemoteJournal) Lookup(ctx context.Context, ref string) (OperationStatus, error) {
	if err := r.init(); err != nil {
		return OperationStatus{}, err
	}
	return r.Local.Lookup(ctx, ref)
}

func (r *RemoteJournal) ClaimReply(ctx context.Context, input ReplyClaimInput) (ReplyClaim, error) {
	if err := r.init(); err != nil {
		return ReplyClaim{}, err
	}
	op, err := r.operationFor(ctx, input.OriginalID)
	if err != nil {
		return ReplyClaim{}, err
	}
	if op.CustodyRoute != input.CustodyRoute || op.CustodyStoreID != input.CustodyStoreID || op.ReplyRoute != input.ReplyRoute || op.ReplyEndpointID == "" {
		return ReplyClaim{}, ErrRemoteCustodyBinding
	}
	route, remote, err := r.route(input.CustodyRoute)
	if err != nil {
		return ReplyClaim{}, err
	}
	if !remote {
		return r.Local.ClaimReply(ctx, input)
	}
	request, err := r.request(op, input, "claim")
	if err != nil {
		return ReplyClaim{}, err
	}
	response, err := r.invoke(ctx, route, request)
	if err != nil {
		return ReplyClaim{}, err
	}
	claim, err := decodeClaimResult(response, input)
	if err != nil {
		return ReplyClaim{}, err
	}
	r.mu.Lock()
	r.claims[input.ReplyID] = remoteClaim{input: input, operation: op, request: request, route: route, lease: claimLease(response)}
	r.mu.Unlock()
	return claim, nil
}

func (r *RemoteJournal) Heartbeat(ctx context.Context, replyID, owner, token string) error {
	claim, remote, err := r.claimFor(replyID)
	if err != nil {
		return err
	}
	if !remote {
		return r.Local.Heartbeat(ctx, replyID, owner, token)
	}
	request := claim.request
	request.Operation = "heartbeat"
	request.AttemptOwner = owner
	request.FencingToken = token
	request.Lease = claim.lease
	response, err := r.invoke(ctx, claim.route, request)
	if err != nil {
		return err
	}
	lease, err := decodeLeaseResult(response)
	if err != nil {
		return err
	}
	r.mu.Lock()
	current := r.claims[replyID]
	current.lease = lease
	r.claims[replyID] = current
	r.mu.Unlock()
	return nil
}

func (r *RemoteJournal) CommitReply(ctx context.Context, replyID, owner, token string) (ReplyClaim, error) {
	claim, remote, err := r.claimFor(replyID)
	if err != nil {
		return ReplyClaim{}, err
	}
	if !remote {
		return r.Local.CommitReply(ctx, replyID, owner, token)
	}
	request := claim.request
	request.Operation = "commit"
	request.AttemptOwner = owner
	request.FencingToken = token
	request.Lease = claim.lease
	response, err := r.invoke(ctx, claim.route, request)
	if err != nil {
		return ReplyClaim{}, err
	}
	return decodeMutationResult(response, claim.input, false)
}

func (r *RemoteJournal) AbandonReply(ctx context.Context, replyID, owner, token string) error {
	claim, remote, err := r.claimFor(replyID)
	if err != nil {
		return err
	}
	if !remote {
		return r.Local.AbandonReply(ctx, replyID, owner, token)
	}
	request := claim.request
	request.Operation = "abandon"
	request.AttemptOwner = owner
	request.FencingToken = token
	request.Lease = claim.lease
	_, err = r.invoke(ctx, claim.route, request)
	return err
}

func (r *RemoteJournal) ObserveReply(ctx context.Context, replyID, nativeID, digest string) error {
	claim, remote, err := r.claimFor(replyID)
	if err != nil {
		return err
	}
	if !remote {
		return r.Local.ObserveReply(ctx, replyID, nativeID, digest)
	}
	_, err = r.status(ctx, claim, false)
	return err
}

func (r *RemoteJournal) ReconcileReplyObservation(ctx context.Context, replyID, nativeID, digest string) error {
	claim, remote, err := r.claimFor(replyID)
	if err != nil {
		return err
	}
	if !remote {
		return r.Local.ReconcileReplyObservation(ctx, replyID, nativeID, digest)
	}
	_, err = r.status(ctx, claim, true)
	return err
}

func (r *RemoteJournal) WaitReply(ctx context.Context, replyID string, timeout time.Duration) (OperationStatus, error) {
	claim, remote, err := r.claimFor(replyID)
	if err != nil {
		return OperationStatus{}, err
	}
	if !remote {
		return r.Local.Lookup(ctx, replyID)
	}
	waitCtx := ctx
	if timeout > 0 {
		var cancel context.CancelFunc
		waitCtx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	for {
		status, err := r.status(waitCtx, claim, false)
		if err != nil {
			return status, err
		}
		if status.State == mektup.StateReplyAccepted || status.State == mektup.StateReplyObserved || status.State == mektup.StateReplyOutcomeUnknown {
			return status, nil
		}
		timer := time.NewTimer(r.PollInterval)
		select {
		case <-waitCtx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return status, waitCtx.Err()
		case <-timer.C:
		}
	}
}

func (r *RemoteJournal) operationFor(ctx context.Context, messageID string) (Operation, error) {
	r.mu.Lock()
	op, ok := r.operations[messageID]
	r.mu.Unlock()
	if ok {
		return op, nil
	}
	status, err := r.Local.Lookup(ctx, messageID)
	if err != nil {
		return Operation{}, err
	}
	r.mu.Lock()
	r.operations[messageID] = status.Operation
	r.mu.Unlock()
	return status.Operation, nil
}

func (r *RemoteJournal) route(endpointID string) (endpoint.Route, bool, error) {
	if endpointID == "" {
		return endpoint.Route{}, false, ErrRemoteCustodyBinding
	}
	resolved, err := r.Endpoints.ResolveEndpointID(endpointID, "")
	if err != nil {
		return endpoint.Route{}, false, fmt.Errorf("%w: endpoint mapping: %v", ErrRemoteCustodyUnavailable, err)
	}
	if resolved.Route.Kind == endpoint.RouteUnix {
		if r.LocalEndpointID != "" && endpointID == r.LocalEndpointID {
			return resolved.Route, false, nil
		}
		return endpoint.Route{}, false, fmt.Errorf("%w: non-local Unix custody endpoint", ErrRemoteCustodyUnavailable)
	}
	if resolved.Route.Kind != endpoint.RouteSSH || resolved.Route.SSHHost == "" {
		return endpoint.Route{}, false, ErrRemoteCustodyUnavailable
	}
	return resolved.Route, true, nil
}

func (r *RemoteJournal) claimFor(replyID string) (remoteClaim, bool, error) {
	if err := r.init(); err != nil {
		return remoteClaim{}, false, err
	}
	r.mu.Lock()
	claim, ok := r.claims[replyID]
	r.mu.Unlock()
	if !ok {
		return remoteClaim{}, false, ErrRemoteCustodyBinding
	}
	_, remote, err := r.route(claim.input.CustodyRoute)
	return claim, remote, err
}

func (r *RemoteJournal) request(op Operation, input ReplyClaimInput, operation string) (sshproxy.ControlRequest, error) {
	thread, err := mektup.ParseThreadURI(input.ReplyRoute)
	if err != nil {
		return sshproxy.ControlRequest{}, fmt.Errorf("%w: reply route: %v", ErrRemoteCustodyBinding, err)
	}
	now := r.Now().UTC().Format(time.RFC3339Nano)
	bytes := input.BodySize
	return sshproxy.ControlRequest{Schema: "mektup/control/v1", Kind: "request", Operation: operation, OperationID: op.OperationID, ReplyMessageID: input.ReplyID, OriginalMessageID: input.OriginalID, Custody: sshproxy.CustodyRef{EndpointID: input.CustodyRoute, StoreID: input.CustodyStoreID}, ReplyDestination: sshproxy.DestinationRef{EndpointID: op.ReplyEndpointID, ThreadID: thread.ThreadID, URI: input.ReplyRoute}, BodyBytes: &bytes, BodySHA256: input.Digest, ReplyStatus: input.Status, AttemptOwner: input.Owner, RequestedAt: now}, nil
}

func (r *RemoteJournal) invoke(ctx context.Context, route endpoint.Route, request sshproxy.ControlRequest) (sshproxy.ControlRequest, error) {
	if route.Kind != endpoint.RouteSSH {
		return sshproxy.ControlRequest{}, ErrRemoteCustodyUnavailable
	}
	raw, err := r.Invoke(ctx, route, request)
	if err != nil {
		return sshproxy.ControlRequest{}, err
	}
	response, err := sshproxy.ValidateControlRequest(raw)
	if err != nil {
		return sshproxy.ControlRequest{}, err
	}
	if response.Kind != "result" || response.Operation != request.Operation || response.OperationID != request.OperationID || response.ReplyMessageID != request.ReplyMessageID || response.OriginalMessageID != request.OriginalMessageID || response.Custody != request.Custody || response.ReplyDestination != request.ReplyDestination {
		return sshproxy.ControlRequest{}, fmt.Errorf("%w: remote response identity mismatch", sshproxy.ErrControlValidation)
	}
	return response, nil
}

func decodeClaimResult(response sshproxy.ControlRequest, input ReplyClaimInput) (ReplyClaim, error) {
	var result struct {
		Disposition  string               `json:"disposition"`
		State        mektup.EvidenceState `json:"state"`
		FencingToken string               `json:"fencingToken"`
		ReplyStatus  string               `json:"replyStatus"`
	}
	if err := json.Unmarshal(response.Result, &result); err != nil {
		return ReplyClaim{}, fmt.Errorf("%w: claim result: %v", sshproxy.ErrControlValidation, err)
	}
	if !result.State.Valid() {
		return ReplyClaim{}, sshproxy.ErrControlValidation
	}
	if result.Disposition == "claimed" && result.FencingToken == "" {
		return ReplyClaim{}, sshproxy.ErrControlValidation
	}
	if result.Disposition != "claimed" && result.Disposition != "existing" {
		return ReplyClaim{}, sshproxy.ErrControlValidation
	}
	return ReplyClaim{ReplyID: input.ReplyID, OriginalID: input.OriginalID, Digest: input.Digest, BodySize: input.BodySize, Status: input.Status, ReplyRoute: input.ReplyRoute, CustodyRoute: input.CustodyRoute, CustodyStoreID: input.CustodyStoreID, Owner: input.Owner, Token: result.FencingToken, State: result.State, Joined: result.Disposition == "existing"}, nil
}

func decodeLeaseResult(response sshproxy.ControlRequest) (*sshproxy.Lease, error) {
	var result struct {
		Lease *sshproxy.Lease `json:"lease"`
	}
	if err := json.Unmarshal(response.Result, &result); err != nil || result.Lease == nil {
		return nil, sshproxy.ErrControlValidation
	}
	return result.Lease, nil
}
func claimLease(response sshproxy.ControlRequest) *sshproxy.Lease {
	var result struct {
		Lease *sshproxy.Lease `json:"lease"`
	}
	_ = json.Unmarshal(response.Result, &result)
	return result.Lease
}
func decodeMutationResult(response sshproxy.ControlRequest, input ReplyClaimInput, joined bool) (ReplyClaim, error) {
	var result struct {
		State mektup.EvidenceState `json:"state"`
		Won   bool                 `json:"won"`
	}
	if err := json.Unmarshal(response.Result, &result); err != nil || !result.State.Valid() {
		return ReplyClaim{}, sshproxy.ErrControlValidation
	}
	return ReplyClaim{ReplyID: input.ReplyID, OriginalID: input.OriginalID, Digest: input.Digest, BodySize: input.BodySize, Status: input.Status, ReplyRoute: input.ReplyRoute, CustodyRoute: input.CustodyRoute, CustodyStoreID: input.CustodyStoreID, Owner: input.Owner, State: result.State, Joined: joined, Won: result.Won}, nil
}

func (r *RemoteJournal) status(ctx context.Context, claim remoteClaim, reconcile bool) (OperationStatus, error) {
	op := claim.operation
	request := claim.request
	if reconcile {
		request.Operation = "reconcile"
	} else {
		request.Operation = "status"
	}
	request.BodyBytes = nil
	request.BodySHA256 = ""
	request.ReplyStatus = ""
	request.AttemptOwner = ""
	response, err := r.invoke(ctx, claim.route, request)
	if err != nil {
		return OperationStatus{}, err
	}
	var result struct {
		State       mektup.EvidenceState `json:"state"`
		ReplyStatus string               `json:"replyStatus"`
	}
	if err := json.Unmarshal(response.Result, &result); err != nil || !result.State.Valid() {
		return OperationStatus{}, sshproxy.ErrControlValidation
	}
	return OperationStatus{Operation: op, State: result.State, ReplyID: claim.input.ReplyID, ReplyStatus: result.ReplyStatus, ReplyDigest: claim.input.Digest, ReplyBodySize: claim.input.BodySize}, nil
}
