package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	mektup "github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/internal/endpoint"
	"github.com/agensfield/mektup/go/internal/journal"
	"github.com/agensfield/mektup/go/internal/sshproxy"
)

var (
	ErrRemoteCustodyUnavailable     = errors.New("service: remote custody is unavailable")
	ErrRemoteCustodyBinding         = errors.New("service: remote custody relationship is not bound")
	ErrRemoteObservationUnsupported = errors.New("service: remote native observation is unsupported by control v1")
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
	initMu     sync.Mutex
	ready      bool
	operations map[string]Operation
	claims     map[string]remoteClaim
}

type remoteClaim struct {
	input     ReplyClaimInput
	operation Operation
	request   sshproxy.ControlRequest
	route     endpoint.Route
	lease     *sshproxy.Lease
	state     mektup.EvidenceState
	joined    bool
}

type OriginalStatusResult struct {
	Selection    string
	ReplyID      string
	Digest       string
	BodySize     int64
	Status       string
	ErrorCode    string
	State        mektup.EvidenceState
	CommitSeq    int64
	EventSeq     int64
	NativeItemID string
}

func (c remoteClaim) activeOwner() bool { return c.input.Owner != "" && c.lease != nil }

func (r *RemoteJournal) init() error {
	if r == nil {
		return errors.New("service: nil remote journal")
	}
	r.initMu.Lock()
	defer r.initMu.Unlock()
	if r.Local == nil {
		return errors.New("service: nil local journal")
	}
	if r.ready {
		return nil
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
	r.mu.Lock()
	if r.operations == nil {
		r.operations = make(map[string]Operation)
	}
	if r.claims == nil {
		r.claims = make(map[string]remoteClaim)
	}
	r.mu.Unlock()
	r.ready = true
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
	status, err := r.Local.Lookup(ctx, ref)
	if err != nil {
		return OperationStatus{}, err
	}
	if status.ReplyID != "" && status.ReplyCommitSeq > 0 && (status.State == mektup.StateReplyAccepted || status.State == mektup.StateReplyObserved) {
		return status, nil
	}
	claims := r.claimsForOriginal(status.MessageID)
	var terminal []OperationStatus
	for _, claim := range claims {
		_, remote, routeErr := r.route(claim.input.CustodyRoute)
		if routeErr != nil {
			return OperationStatus{}, routeErr
		}
		if !remote {
			continue
		}
		remoteStatus, statusErr := r.status(ctx, claim, false)
		if statusErr != nil {
			return OperationStatus{}, statusErr
		}
		if remoteStatus.State == mektup.StateReplyAccepted || remoteStatus.State == mektup.StateReplyObserved || remoteStatus.State == mektup.StateReplyOutcomeUnknown {
			terminal = append(terminal, remoteStatus)
		}
	}
	// A cacheless/restarted portable wait has no in-memory claim to drive the
	// ordinary status path.  The original-scoped control operation is the
	// durable authority in that case.  It is metadata-only and can never create
	// a dispatch attempt.
	needsOriginalStatus := len(terminal) == 0
	for _, candidate := range terminal {
		if candidate.State == mektup.StateReplyOutcomeUnknown && candidate.ReplyCommitSeq == 0 {
			needsOriginalStatus = true
			break
		}
	}
	if needsOriginalStatus && status.Operation.ReplyRoute != "" && status.Operation.CustodyRoute != "" {
		_, remote, routeErr := r.route(status.Operation.CustodyRoute)
		if routeErr != nil {
			return OperationStatus{}, routeErr
		}
		if remote {
			original, statusErr := r.OriginalStatus(ctx, status.Operation)
			if statusErr != nil {
				return OperationStatus{}, statusErr
			}
			if original.Selection != "pending" {
				if statusErr := r.PersistOriginalStatus(ctx, status.Operation, original); statusErr != nil {
					return OperationStatus{}, statusErr
				}
				status.State = original.State
				status.ReplyID = original.ReplyID
				status.ReplyStatus = original.Status
				status.ReplyErrorCode = original.ErrorCode
				status.ReplyDigest = original.Digest
				status.ReplyBodySize = original.BodySize
				status.ReplyCommitSeq = original.CommitSeq
				status.ReplyNativeID = original.NativeItemID
				terminal = nil
			} else if len(terminal) > 0 {
				return OperationStatus{}, journal.ErrIdentityConflict
			}
		}
	}
	if len(terminal) > 0 {
		sort.SliceStable(terminal, func(i, j int) bool {
			leftSeq, rightSeq := terminal[i].ReplyCommitSeq, terminal[j].ReplyCommitSeq
			if leftSeq == 0 {
				return false
			}
			if rightSeq == 0 {
				return true
			}
			if leftSeq != rightSeq {
				return leftSeq < rightSeq
			}
			return terminal[i].ReplyID < terminal[j].ReplyID
		})
		winner := terminal[0]
		if winner.ReplyCommitSeq > 0 {
			for _, candidate := range terminal[1:] {
				if candidate.ReplyCommitSeq == winner.ReplyCommitSeq && candidate.ReplyID != winner.ReplyID {
					return OperationStatus{}, journal.ErrIdentityConflict
				}
			}
		}
		status.State, status.ReplyID, status.ReplyStatus, status.ReplyErrorCode = winner.State, winner.ReplyID, winner.ReplyStatus, winner.ReplyErrorCode
		status.ReplyDigest, status.ReplyBodySize, status.ReplyCommitSeq, status.ReplyNativeID = winner.ReplyDigest, winner.ReplyBodySize, winner.ReplyCommitSeq, winner.ReplyNativeID
		if err := r.persistRemoteStatus(ctx, status.Operation, winner); err != nil {
			return OperationStatus{}, err
		}
	}
	return status, nil
}

func (r *RemoteJournal) claimsForOriginal(originalID string) []remoteClaim {
	r.mu.Lock()
	defer r.mu.Unlock()
	claims := make([]remoteClaim, 0)
	for _, claim := range r.claims {
		if claim.input.OriginalID == originalID {
			claims = append(claims, claim)
		}
	}
	sort.SliceStable(claims, func(i, j int) bool { return claims[i].input.ReplyID < claims[j].input.ReplyID })
	return claims
}

func (r *RemoteJournal) ClaimReply(ctx context.Context, input ReplyClaimInput) (ReplyClaim, error) {
	if err := r.init(); err != nil {
		return ReplyClaim{}, err
	}
	op, err := r.operationFor(ctx, input.OriginalID, input.ReplyID)
	if err != nil {
		return ReplyClaim{}, err
	}
	if op.InReplyTo != "" && op.InReplyTo != input.OriginalID {
		return ReplyClaim{}, ErrRemoteCustodyBinding
	}
	replyEndpointID := op.ReplyEndpointID
	if op.InReplyTo != "" {
		// A reply operation's Target tuple is the original envelope's body
		// destination. ReplyEndpointID/ReplyRoute/Custody* belong to the
		// optional child reply requested by this reply operation.
		replyEndpointID = op.TargetEndpointID
		if replyEndpointID == "" || op.TargetRoute == "" || input.ReplyRoute != op.TargetRoute {
			return ReplyClaim{}, ErrRemoteCustodyBinding
		}
	} else if replyEndpointID == "" || (op.CustodyRoute != "" && (op.CustodyRoute != input.CustodyRoute || op.CustodyStoreID != input.CustodyStoreID)) {
		return ReplyClaim{}, ErrRemoteCustodyBinding
	}
	route, remote, err := r.route(input.CustodyRoute)
	if err != nil {
		return ReplyClaim{}, err
	}
	if !remote {
		claim, err := r.Local.ClaimReply(ctx, input)
		if err != nil {
			return ReplyClaim{}, err
		}
		r.mu.Lock()
		current, exists := r.claims[input.ReplyID]
		if !exists || !current.activeOwner() || !claim.Joined {
			r.claims[input.ReplyID] = remoteClaim{input: input, operation: op, state: mektup.EvidenceState(claim.State)}
		}
		r.mu.Unlock()
		return claim, nil
	}
	op.ReplyEndpointID = replyEndpointID
	request, err := r.request(op, input, "claim", replyEndpointID)
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
	current, exists := r.claims[input.ReplyID]
	if !(exists && current.activeOwner() && claim.Joined) {
		r.claims[input.ReplyID] = remoteClaim{input: input, operation: op, request: request, route: route, lease: claimLease(response), state: mektup.EvidenceState(claim.State), joined: claim.Joined}
	}
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
	current.state = mektup.StateReplyDispatchClaimed
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
	result, err := decodeMutationResult(response, claim.input, false)
	if err == nil {
		r.mu.Lock()
		current := r.claims[replyID]
		current.state = result.State
		r.claims[replyID] = current
		r.mu.Unlock()
	}
	return result, err
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
	return r.observeRemote(ctx, claim, nativeID, digest)
}

// ObserveVerifiedReply builds a tokenless observation directly from an exact
// native envelope and the sender's durable original relationship. It does not
// require a prior in-process ClaimReply cache and never creates dispatch
// authority.
func (r *RemoteJournal) ObserveVerifiedReply(ctx context.Context, status OperationStatus, item ObservedItem, envelope mektup.Envelope) error {
	if err := r.init(); err != nil {
		return err
	}
	input := ReplyClaimInput{ReplyID: envelope.MessageID, OriginalID: status.MessageID, Digest: envelope.PayloadSHA256, BodySize: int64(envelope.PayloadBytes), Status: string(envelope.ReplyStatus), ErrorCode: envelope.ReplyErrorCode, ReplyRoute: status.ReplyRoute, CustodyRoute: status.CustodyRoute, CustodyStoreID: status.CustodyStoreID}
	if input.ReplyRoute == "" {
		input.ReplyRoute = status.SourceRoute
	}
	if input.Status == "" {
		input.Status = status.ReplyStatus
	}
	if input.ReplyID == "" || item.NativeItemID == "" {
		return sshproxy.ErrControlValidation
	}
	route, remote, err := r.route(input.CustodyRoute)
	if err != nil {
		return err
	}
	if !remote {
		return r.Local.RecordObservedReply(ctx, input, item.NativeItemID, status.ReplyEndpointID, status.CustodyRoute)
	}
	replyEndpointID := status.ReplyEndpointID
	if replyEndpointID == "" {
		replyEndpointID = status.SourceEndpointID
	}
	request, err := r.request(status.Operation, input, "observe", replyEndpointID)
	if err != nil {
		return err
	}
	request.NativeItemID = item.NativeItemID
	request.AttemptOwner = ""
	request.FencingToken = ""
	request.Lease = nil
	request.RequestedLease = nil
	response, err := r.invoke(ctx, route, request)
	if err != nil {
		return err
	}
	result, err := decodeObserveResult(response, request)
	if err != nil {
		return err
	}
	if result.WinnerReplyID == input.ReplyID && (result.WinnerDigest != input.Digest || result.WinnerBodySize != input.BodySize || result.WinnerStatus != input.Status || result.WinnerErrorCode != input.ErrorCode || result.WinnerNativeID != item.NativeItemID) {
		return journal.ErrIdentityConflict
	}
	if err := r.Local.Inner.RecordObservedReplyAndWinner(ctx, journal.ClaimInput{ReplyID: input.ReplyID, OriginalID: input.OriginalID, Digest: input.Digest, BodySize: input.BodySize, Status: input.Status, ErrorCode: input.ErrorCode, ReplyRoute: input.ReplyRoute, CustodyRoute: input.CustodyRoute, CustodyStoreID: input.CustodyStoreID}, item.NativeItemID, status.ReplyEndpointID, status.CustodyRoute, result.WinnerReplyID, result.WinnerDigest, result.WinnerStatus, result.WinnerErrorCode, result.WinnerNativeID, result.WinnerCommitSeq, result.WinnerBodySize); err != nil {
		return err
	}
	r.mu.Lock()
	r.claims[input.ReplyID] = remoteClaim{input: input, operation: status.Operation, request: request, route: route, state: mektup.StateReplyObserved}
	r.mu.Unlock()
	return nil
}

func (r *RemoteJournal) PersistOriginalStatus(ctx context.Context, op Operation, result OriginalStatusResult) error {
	if r == nil || r.Local == nil || r.Local.Inner == nil {
		return ErrRemoteCustodyUnavailable
	}
	return r.Local.Inner.ImportOriginalStatus(ctx, journal.OriginalStatusImport{
		Operation: journal.Operation{
			OperationID:      op.OperationID,
			MessageID:        op.MessageID,
			SourceRoute:      op.SourceRoute,
			TargetRoute:      op.TargetRoute,
			Semantics:        op.Semantics,
			SourceEndpointID: op.SourceEndpointID,
			TargetEndpointID: op.TargetEndpointID,
			ReplyRoute:       op.ReplyRoute,
			ReplyEndpointID:  op.ReplyEndpointID,
			ReplyThreadID:    threadID(op.ReplyRoute),
			CustodyRoute:     op.CustodyRoute,
			CustodyStoreID:   op.CustodyStoreID,
			Digest:           op.Digest,
			BodySize:         op.BodySize,
		},
		Selection:    result.Selection,
		ReplyID:      result.ReplyID,
		Digest:       result.Digest,
		BodySize:     result.BodySize,
		Status:       result.Status,
		ErrorCode:    result.ErrorCode,
		CommitSeq:    result.CommitSeq,
		EventSeq:     result.EventSeq,
		NativeItemID: result.NativeItemID,
	})
}

func (r *RemoteJournal) persistRemoteStatus(ctx context.Context, op Operation, status OperationStatus) error {
	if status.State != mektup.StateReplyAccepted && status.State != mektup.StateReplyObserved && status.State != mektup.StateReplyOutcomeUnknown {
		return nil
	}
	in := journal.ClaimInput{ReplyID: status.ReplyID, OriginalID: op.MessageID, Digest: status.ReplyDigest, BodySize: status.ReplyBodySize, Status: status.ReplyStatus, ErrorCode: status.ReplyErrorCode, ReplyRoute: op.ReplyRoute, CustodyRoute: op.CustodyRoute, CustodyStoreID: op.CustodyStoreID}
	if status.State == mektup.StateReplyOutcomeUnknown {
		return r.Local.Inner.ImportTerminalUnknown(ctx, in)
	}
	return r.Local.Inner.RecordObservedWinner(ctx, op.MessageID, status.ReplyID, status.ReplyDigest, status.ReplyStatus, status.ReplyErrorCode, op.ReplyRoute, op.CustodyRoute, op.CustodyStoreID, status.ReplyNativeID, op.ReplyEndpointID, op.CustodyRoute, status.ReplyCommitSeq, status.ReplyBodySize)
}

func (r *RemoteJournal) OriginalStatus(ctx context.Context, op Operation) (OriginalStatusResult, error) {
	if err := r.init(); err != nil {
		return OriginalStatusResult{}, err
	}
	if op.OperationID == "" || op.MessageID == "" || op.ReplyRoute == "" || op.ReplyEndpointID == "" || op.CustodyRoute == "" || op.CustodyStoreID == "" {
		return OriginalStatusResult{}, ErrRemoteCustodyBinding
	}
	route, remote, err := r.route(op.CustodyRoute)
	if err != nil {
		return OriginalStatusResult{}, err
	}
	if !remote {
		return OriginalStatusResult{}, ErrRemoteCustodyUnavailable
	}
	thread, err := mektup.ParseThreadURI(op.ReplyRoute)
	if err != nil {
		return OriginalStatusResult{}, ErrRemoteCustodyBinding
	}
	request := sshproxy.ControlRequest{Schema: "mektup/control/v1", Kind: "request", Operation: "originalStatus", OperationID: op.OperationID, OriginalMessageID: op.MessageID, Custody: sshproxy.CustodyRef{EndpointID: op.CustodyRoute, StoreID: op.CustodyStoreID}, ReplyDestination: sshproxy.DestinationRef{EndpointID: op.ReplyEndpointID, ThreadID: thread.ThreadID, URI: op.ReplyRoute}, RequestedAt: r.Now().UTC().Format(time.RFC3339Nano)}
	response, err := r.invoke(ctx, route, request)
	if err != nil {
		return OriginalStatusResult{}, err
	}
	return decodeCanonicalOriginalStatus(response.Result)
}

func decodeCanonicalOriginalStatus(data json.RawMessage) (OriginalStatusResult, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil || raw == nil {
		return OriginalStatusResult{}, sshproxy.ErrControlValidation
	}
	var selection string
	if value, ok := raw["selection"]; !ok || json.Unmarshal(value, &selection) != nil || (selection != "winner" && selection != "terminal_unknown" && selection != "pending") {
		return OriginalStatusResult{}, sshproxy.ErrControlValidation
	}
	if selection == "pending" {
		// The wire contract is forward-compatible with additive result
		// metadata. Reject every known field that could smuggle selected
		// evidence or authority, while preserving unknown extensions.
		for _, field := range []string{
			"body", "bodyText", "bodyContent", "replyBody",
			"fencingToken", "lease", "requestedLease", "attemptOwner",
			"replyMessageId", "replyStatus", "replyErrorCode", "status",
			"bodyBytes", "bodySha256", "commitSeq", "eventSeq", "nativeItemId", "state",
		} {
			if _, present := raw[field]; present {
				return OriginalStatusResult{}, sshproxy.ErrControlValidation
			}
		}
		return OriginalStatusResult{Selection: selection}, nil
	}
	getString := func(name string) (string, error) {
		value, ok := raw[name]
		if !ok || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return "", sshproxy.ErrControlValidation
		}
		var text string
		if json.Unmarshal(value, &text) != nil || text == "" {
			return "", sshproxy.ErrControlValidation
		}
		return text, nil
	}
	replyID, err := getString("replyMessageId")
	if err != nil || mektup.ValidateID(replyID, mektup.MessageIDPrefix) != nil {
		return OriginalStatusResult{}, sshproxy.ErrControlValidation
	}
	status, err := getString("replyStatus")
	if err != nil || (status != "success" && status != "error") {
		return OriginalStatusResult{}, sshproxy.ErrControlValidation
	}
	digest, err := getString("bodySha256")
	if err != nil || !validRemoteDigest(digest) {
		return OriginalStatusResult{}, sshproxy.ErrControlValidation
	}
	_, err = getString("state")
	if err != nil {
		return OriginalStatusResult{}, err
	}
	var state mektup.EvidenceState
	if json.Unmarshal(raw["state"], &state) != nil || !state.Valid() {
		return OriginalStatusResult{}, sshproxy.ErrControlValidation
	}
	var bodySize int64
	if value, ok := raw["bodyBytes"]; !ok || json.Unmarshal(value, &bodySize) != nil || bodySize < 0 {
		return OriginalStatusResult{}, sshproxy.ErrControlValidation
	}
	out := OriginalStatusResult{Selection: selection, ReplyID: replyID, Digest: digest, BodySize: bodySize, Status: status, State: state}
	if value, ok := raw["replyErrorCode"]; ok {
		if status != "error" || json.Unmarshal(value, &out.ErrorCode) != nil || out.ErrorCode == "" {
			return OriginalStatusResult{}, sshproxy.ErrControlValidation
		}
	}
	if selection == "winner" {
		if state != mektup.StateReplyAccepted && state != mektup.StateReplyObserved {
			return OriginalStatusResult{}, sshproxy.ErrControlValidation
		}
		if value, ok := raw["commitSeq"]; !ok || json.Unmarshal(value, &out.CommitSeq) != nil || out.CommitSeq < 1 {
			return OriginalStatusResult{}, sshproxy.ErrControlValidation
		}
		if _, present := raw["eventSeq"]; present {
			return OriginalStatusResult{}, sshproxy.ErrControlValidation
		}
		if value, ok := raw["nativeItemId"]; ok {
			if json.Unmarshal(value, &out.NativeItemID) != nil || out.NativeItemID == "" {
				return OriginalStatusResult{}, sshproxy.ErrControlValidation
			}
		}
		if state == mektup.StateReplyObserved {
			if out.NativeItemID == "" {
				return OriginalStatusResult{}, sshproxy.ErrControlValidation
			}
		}
	} else {
		if state != mektup.StateReplyOutcomeUnknown {
			return OriginalStatusResult{}, sshproxy.ErrControlValidation
		}
		if value, ok := raw["eventSeq"]; !ok || json.Unmarshal(value, &out.EventSeq) != nil || out.EventSeq < 1 {
			return OriginalStatusResult{}, sshproxy.ErrControlValidation
		}
		if _, present := raw["commitSeq"]; present {
			return OriginalStatusResult{}, sshproxy.ErrControlValidation
		}
		if _, present := raw["nativeItemId"]; present {
			return OriginalStatusResult{}, sshproxy.ErrControlValidation
		}
	}
	return out, nil
}

func (r *RemoteJournal) ReconcileReplyObservation(ctx context.Context, replyID, nativeID, digest string) error {
	claim, remote, err := r.claimFor(replyID)
	if err != nil {
		return err
	}
	if !remote {
		return r.Local.ReconcileReplyObservation(ctx, replyID, nativeID, digest)
	}
	// The sender has already performed exact full-history/live native-item
	// validation. Remote control has one evidence transition for both paths;
	// this method name is retained for the local JournalPort seam, while the
	// wire operation remains the explicit tokenless observe operation.
	return r.observeRemote(ctx, claim, nativeID, digest)
}

type observeResult struct {
	State           mektup.EvidenceState
	WinnerReplyID   string
	WinnerNativeID  string
	WinnerCommitSeq int64
	WinnerStatus    string
	WinnerDigest    string
	WinnerBodySize  int64
	WinnerErrorCode string
}

func (r *RemoteJournal) observeRemote(ctx context.Context, claim remoteClaim, nativeID, digest string) error {
	if nativeID == "" || digest == "" {
		return sshproxy.ErrControlValidation
	}
	request := claim.request
	request.Operation = "observe"
	request.NativeItemID = nativeID
	request.BodySHA256 = digest
	request.AttemptOwner = ""
	request.FencingToken = ""
	request.Lease = nil
	request.RequestedLease = nil
	response, err := r.invoke(ctx, claim.route, request)
	if err != nil {
		return err
	}
	result, err := decodeObserveResult(response, request)
	if err != nil {
		return err
	}
	if result.WinnerReplyID == claim.input.ReplyID && (result.WinnerDigest != claim.input.Digest || result.WinnerBodySize != claim.input.BodySize || result.WinnerStatus != claim.input.Status || result.WinnerErrorCode != claim.input.ErrorCode || result.WinnerNativeID != nativeID) {
		return journal.ErrIdentityConflict
	}
	if err := r.Local.Inner.RecordObservedReplyAndWinner(ctx, journal.ClaimInput{ReplyID: claim.input.ReplyID, OriginalID: claim.input.OriginalID, Digest: claim.input.Digest, BodySize: claim.input.BodySize, Status: claim.input.Status, ErrorCode: claim.input.ErrorCode, ReplyRoute: claim.input.ReplyRoute, CustodyRoute: claim.input.CustodyRoute, CustodyStoreID: claim.input.CustodyStoreID}, nativeID, request.ReplyDestination.EndpointID, request.Custody.EndpointID, result.WinnerReplyID, result.WinnerDigest, result.WinnerStatus, result.WinnerErrorCode, result.WinnerNativeID, result.WinnerCommitSeq, result.WinnerBodySize); err != nil {
		return err
	}
	r.mu.Lock()
	current := r.claims[claim.input.ReplyID]
	current.state = result.State
	r.claims[claim.input.ReplyID] = current
	r.mu.Unlock()
	return nil
}

func (r *RemoteJournal) WaitReply(ctx context.Context, replyID string, timeout time.Duration) (OperationStatus, error) {
	claim, remote, err := r.claimFor(replyID)
	if err != nil {
		return OperationStatus{}, err
	}
	if !remote {
		return r.Local.WaitReply(ctx, replyID, timeout)
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
			if status.State == mektup.StateReplyOutcomeUnknown {
				return status, nil
			}
			status.State = mektup.StateReplyDispatchClaimed
			status.ReplyID = ""
			return status, errors.New("reply attempt acceptance has no correlated child reply")
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

func (r *RemoteJournal) operationFor(ctx context.Context, messageID, replyID string) (Operation, error) {
	r.mu.Lock()
	op, ok := r.operations[replyID]
	if !ok {
		var candidate Operation
		matches := 0
		for _, item := range r.operations {
			if item.InReplyTo == messageID {
				candidate = item
				matches++
			}
		}
		if matches == 1 {
			op, ok = candidate, true
		}
	}
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

func (r *RemoteJournal) request(op Operation, input ReplyClaimInput, operation, replyEndpointID string) (sshproxy.ControlRequest, error) {
	thread, err := mektup.ParseThreadURI(input.ReplyRoute)
	if err != nil {
		return sshproxy.ControlRequest{}, fmt.Errorf("%w: reply route: %v", ErrRemoteCustodyBinding, err)
	}
	now := r.Now().UTC().Format(time.RFC3339Nano)
	bytes := input.BodySize
	return sshproxy.ControlRequest{Schema: "mektup/control/v1", Kind: "request", Operation: operation, OperationID: op.OperationID, ReplyMessageID: input.ReplyID, OriginalMessageID: input.OriginalID, Custody: sshproxy.CustodyRef{EndpointID: input.CustodyRoute, StoreID: input.CustodyStoreID}, ReplyDestination: sshproxy.DestinationRef{EndpointID: replyEndpointID, ThreadID: thread.ThreadID, URI: input.ReplyRoute}, BodyBytes: &bytes, BodySHA256: input.Digest, ReplyStatus: input.Status, ReplyErrorCode: input.ErrorCode, AttemptOwner: input.Owner, RequestedAt: now}, nil
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
	return ReplyClaim{ReplyID: input.ReplyID, OriginalID: input.OriginalID, Digest: input.Digest, BodySize: input.BodySize, Status: input.Status, ReplyErrorCode: input.ErrorCode, ReplyRoute: input.ReplyRoute, CustodyRoute: input.CustodyRoute, CustodyStoreID: input.CustodyStoreID, Owner: input.Owner, Token: result.FencingToken, State: result.State, Joined: result.Disposition == "existing"}, nil
}

func decodeLeaseResult(response sshproxy.ControlRequest) (*sshproxy.Lease, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(response.Result, &raw); err != nil {
		return nil, sshproxy.ErrControlValidation
	}
	leaseRaw, ok := raw["lease"]
	if !ok || string(bytes.TrimSpace(leaseRaw)) == "null" {
		return nil, sshproxy.ErrControlValidation
	}
	var leaseObject map[string]json.RawMessage
	if err := json.Unmarshal(leaseRaw, &leaseObject); err != nil || leaseObject == nil {
		return nil, sshproxy.ErrControlValidation
	}
	for _, field := range []string{"expiresAt", "acquiredAt", "heartbeatAt"} {
		value, present := leaseObject[field]
		if !present {
			if field == "expiresAt" {
				return nil, sshproxy.ErrControlValidation
			}
			continue
		}
		if string(bytes.TrimSpace(value)) == "null" {
			return nil, sshproxy.ErrControlValidation
		}
		var text string
		if err := json.Unmarshal(value, &text); err != nil || !validRemoteTimestamp(text) {
			return nil, sshproxy.ErrControlValidation
		}
	}
	var result struct {
		Lease *sshproxy.Lease `json:"lease"`
	}
	if err := json.Unmarshal(response.Result, &result); err != nil || result.Lease == nil || !validRemoteLease(*result.Lease) {
		return nil, sshproxy.ErrControlValidation
	}
	return result.Lease, nil
}

func validRemoteLease(lease sshproxy.Lease) bool {
	if !validRemoteTimestamp(lease.ExpiresAt) {
		return false
	}
	return (lease.AcquiredAt == "" || validRemoteTimestamp(lease.AcquiredAt)) && (lease.HeartbeatAt == "" || validRemoteTimestamp(lease.HeartbeatAt))
}

func validRemoteTimestamp(value string) bool {
	if len(value) < len("2006-01-02T15:04:05.0Z") || len(value) <= 19 || value[19] != '.' {
		return false
	}
	_, err := time.Parse(time.RFC3339Nano, value)
	return err == nil
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
		State        mektup.EvidenceState `json:"state"`
		Won          bool                 `json:"won"`
		WakeRecorded bool                 `json:"wakeRecorded"`
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(response.Result, &raw); err != nil {
		return ReplyClaim{}, sshproxy.ErrControlValidation
	}
	if _, ok := raw["wakeRecorded"]; !ok {
		return ReplyClaim{}, sshproxy.ErrControlValidation
	}
	if err := json.Unmarshal(response.Result, &result); err != nil || !result.State.Valid() || !result.WakeRecorded || (result.State != mektup.StateReplyAccepted && result.State != mektup.StateReplyObserved) {
		return ReplyClaim{}, sshproxy.ErrControlValidation
	}
	return ReplyClaim{ReplyID: input.ReplyID, OriginalID: input.OriginalID, Digest: input.Digest, BodySize: input.BodySize, Status: input.Status, ReplyErrorCode: input.ErrorCode, ReplyRoute: input.ReplyRoute, CustodyRoute: input.CustodyRoute, CustodyStoreID: input.CustodyStoreID, Owner: input.Owner, State: result.State, Joined: joined, Won: result.Won}, nil
}

func decodeObserveResult(response sshproxy.ControlRequest, request sshproxy.ControlRequest) (observeResult, error) {
	var out observeResult
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(response.Result, &raw); err != nil || raw == nil {
		return out, sshproxy.ErrControlValidation
	}
	stateRaw, ok := raw["state"]
	if !ok || json.Unmarshal(stateRaw, &out.State) != nil || out.State != mektup.StateReplyObserved {
		return out, sshproxy.ErrControlValidation
	}
	statusRaw, ok := raw["status"]
	var status string
	if !ok || json.Unmarshal(statusRaw, &status) != nil || status == "" {
		return out, sshproxy.ErrControlValidation
	}
	winnerRaw, ok := raw["winner"]
	if !ok || string(bytes.TrimSpace(winnerRaw)) == "null" {
		return out, sshproxy.ErrControlValidation
	}
	var winner map[string]json.RawMessage
	if json.Unmarshal(winnerRaw, &winner) != nil || winner == nil {
		return out, sshproxy.ErrControlValidation
	}
	if nativeRaw, ok := winner["nativeItemId"]; ok {
		var winnerNative string
		if json.Unmarshal(nativeRaw, &winnerNative) != nil || winnerNative == "" {
			return out, sshproxy.ErrControlValidation
		}
		out.WinnerNativeID = winnerNative
	}
	if raw, ok := winner["replyMessageId"]; !ok || json.Unmarshal(raw, &out.WinnerReplyID) != nil || mektup.ValidateID(out.WinnerReplyID, mektup.MessageIDPrefix) != nil {
		return out, sshproxy.ErrControlValidation
	}
	if raw, ok := winner["commitSeq"]; !ok || json.Unmarshal(raw, &out.WinnerCommitSeq) != nil || out.WinnerCommitSeq < 1 {
		return out, sshproxy.ErrControlValidation
	}
	if raw, ok := winner["status"]; !ok || json.Unmarshal(raw, &out.WinnerStatus) != nil || out.WinnerStatus == "" {
		return out, sshproxy.ErrControlValidation
	}
	if raw, ok := winner["bodySha256"]; !ok || json.Unmarshal(raw, &out.WinnerDigest) != nil || !validWinnerDigest(out.WinnerDigest) {
		return out, sshproxy.ErrControlValidation
	}
	if raw, ok := winner["bodyBytes"]; !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return out, sshproxy.ErrControlValidation
	}
	if raw, ok := winner["bodyBytes"]; ok {
		var size int64
		if err := json.Unmarshal(raw, &size); err != nil || size < 0 {
			return out, sshproxy.ErrControlValidation
		}
		out.WinnerBodySize = size
	}
	if raw, ok := winner["replyErrorCode"]; ok {
		if json.Unmarshal(raw, &out.WinnerErrorCode) != nil || out.WinnerErrorCode == "" {
			return out, sshproxy.ErrControlValidation
		}
	}
	if out.WinnerStatus == "success" && out.WinnerErrorCode != "" {
		return out, sshproxy.ErrControlValidation
	}
	provenanceRaw, ok := raw["provenance"]
	if !ok || string(bytes.TrimSpace(provenanceRaw)) == "null" {
		return out, sshproxy.ErrControlValidation
	}
	var provenance struct {
		EndpointID   string `json:"endpointId"`
		ControlRoute string `json:"controlRoute"`
	}
	if json.Unmarshal(provenanceRaw, &provenance) != nil || provenance.EndpointID != request.ReplyDestination.EndpointID || provenance.ControlRoute != request.Custody.EndpointID {
		return out, sshproxy.ErrControlValidation
	}
	return out, nil
}

func validWinnerDigest(value string) bool {
	if len(value) != len("sha256:")+64 || !strings.HasPrefix(value, "sha256:") || strings.ToLower(value) != value {
		return false
	}
	for _, r := range value[len("sha256:"):] {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
			return false
		}
	}
	return true
}

func (r *RemoteJournal) status(ctx context.Context, claim remoteClaim, reconcile bool) (OperationStatus, error) {
	op := claim.operation
	op.MessageID = claim.input.ReplyID
	op.InReplyTo = claim.input.OriginalID
	request := claim.request
	if reconcile {
		request.Operation = "reconcile"
	} else {
		request.Operation = "status"
	}
	request.BodyBytes = nil
	request.BodySHA256 = ""
	request.ReplyStatus = ""
	request.ReplyErrorCode = ""
	request.AttemptOwner = ""
	response, err := r.invoke(ctx, claim.route, request)
	if err != nil {
		return OperationStatus{}, err
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(response.Result, &raw); err != nil || raw == nil {
		return OperationStatus{}, sshproxy.ErrControlValidation
	}
	getString := func(name string) (string, bool, error) {
		value, ok := raw[name]
		if !ok {
			return "", false, nil
		}
		var result string
		if json.Unmarshal(value, &result) != nil || result == "" {
			return "", true, sshproxy.ErrControlValidation
		}
		return result, true, nil
	}
	stateText, ok, err := getString("state")
	if err != nil || !ok {
		return OperationStatus{}, sshproxy.ErrControlValidation
	}
	state := mektup.EvidenceState(stateText)
	if !state.Valid() {
		return OperationStatus{}, sshproxy.ErrControlValidation
	}
	replyID, present, err := getString("replyId")
	if err != nil {
		return OperationStatus{}, sshproxy.ErrControlValidation
	}
	if present && replyID != claim.input.ReplyID {
		return OperationStatus{}, journal.ErrIdentityConflict
	}
	if !present {
		replyID = claim.input.ReplyID
	}
	replyStatus, present, err := getString("replyStatus")
	if err != nil {
		return OperationStatus{}, sshproxy.ErrControlValidation
	}
	if present && replyStatus != "success" && replyStatus != "error" {
		return OperationStatus{}, sshproxy.ErrControlValidation
	}
	if !present {
		replyStatus = claim.input.Status
	}
	errorCode := ""
	errorPresent := false
	if rawError, present := raw["replyErrorCode"]; present {
		errorPresent = true
		if json.Unmarshal(rawError, &errorCode) != nil {
			return OperationStatus{}, sshproxy.ErrControlValidation
		}
	}
	if errorPresent && errorCode != "" && claim.input.ErrorCode != "" && errorCode != claim.input.ErrorCode {
		return OperationStatus{}, journal.ErrIdentityConflict
	}
	if errorPresent && errorCode == "" && claim.input.ErrorCode != "" {
		return OperationStatus{}, journal.ErrIdentityConflict
	}
	if !errorPresent {
		errorCode = claim.input.ErrorCode
	}
	digest, present, err := getString("bodySha256")
	if err != nil {
		return OperationStatus{}, sshproxy.ErrControlValidation
	}
	if present && !validRemoteDigest(digest) {
		return OperationStatus{}, sshproxy.ErrControlValidation
	}
	if present && claim.input.Digest != "" && digest != claim.input.Digest {
		return OperationStatus{}, journal.ErrIdentityConflict
	}
	if !present {
		digest = claim.input.Digest
	}
	bodySize := claim.input.BodySize
	if rawBody, present := raw["bodyBytes"]; present {
		var value int64
		if json.Unmarshal(rawBody, &value) != nil || value < 0 {
			return OperationStatus{}, sshproxy.ErrControlValidation
		}
		if claim.input.BodySizeKnown && value != claim.input.BodySize {
			return OperationStatus{}, journal.ErrIdentityConflict
		}
		bodySize = value
	}
	commitSeq := int64(0)
	if rawCommit, present := raw["commitSeq"]; present {
		if json.Unmarshal(rawCommit, &commitSeq) != nil || commitSeq < 0 {
			return OperationStatus{}, sshproxy.ErrControlValidation
		}
	}
	nativeID := ""
	if rawNative, present := raw["nativeItemId"]; present {
		if json.Unmarshal(rawNative, &nativeID) != nil || nativeID == "" {
			return OperationStatus{}, sshproxy.ErrControlValidation
		}
	}
	return OperationStatus{Operation: op, State: state, ReplyID: replyID, ReplyStatus: replyStatus, ReplyErrorCode: errorCode, ReplyDigest: digest, ReplyBodySize: bodySize, ReplyCommitSeq: commitSeq, ReplyNativeID: nativeID}, nil
}

func validRemoteDigest(value string) bool {
	if len(value) != len("sha256:")+64 || !strings.HasPrefix(value, "sha256:") || strings.ToLower(value) != value {
		return false
	}
	for _, char := range value[len("sha256:"):] {
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f')) {
			return false
		}
	}
	return true
}
