package sshproxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// ControlRequest is the metadata-only seam between SSH transport and the
// journal/custody implementation. The transport validates the wire shape and
// opaque identity relationships, but deliberately does not know journal
// tables, state paths, leases, or fencing rules.
type ControlRequest struct {
	Schema            string         `json:"schema"`
	Kind              string         `json:"kind"`
	Operation         string         `json:"operation"`
	OperationID       string         `json:"operationId"`
	ReceiptID         string         `json:"receiptId,omitempty"`
	ReplyMessageID    string         `json:"replyMessageId"`
	OriginalMessageID string         `json:"originalMessageId"`
	Custody           CustodyRef     `json:"custody"`
	ReplyDestination  DestinationRef `json:"replyDestination"`
	BodyBytes         *int64         `json:"bodyBytes,omitempty"`
	BodySHA256        string         `json:"bodySha256,omitempty"`
	ReplyStatus       string         `json:"replyStatus,omitempty"`
	ReplyErrorCode    string         `json:"replyErrorCode,omitempty"`
	RequestedLease    *LeaseRequest  `json:"requestedLease,omitempty"`
	FencingToken      string         `json:"fencingToken,omitempty"`
	Lease             *Lease         `json:"lease,omitempty"`
	AttemptOwner      string         `json:"attemptOwner,omitempty"`
	RequestedAt       string         `json:"requestedAt,omitempty"`
}

type CustodyRef struct {
	EndpointID string `json:"endpointId"`
	StoreID    string `json:"storeId"`
}

type DestinationRef struct {
	EndpointID string `json:"endpointId"`
	ThreadID   string `json:"threadId"`
	URI        string `json:"uri,omitempty"`
}

type LeaseRequest struct {
	DurationMS int64 `json:"durationMs"`
}
type Lease struct {
	AcquiredAt  string `json:"acquiredAt,omitempty"`
	ExpiresAt   string `json:"expiresAt"`
	HeartbeatAt string `json:"heartbeatAt,omitempty"`
}

// ControlValidator is the safe seam for journal ownership. A journal adapter
// may validate that the endpoint/store/message relationships already exist;
// this package will call it only after its own metadata-only validation. The
// callback must not derive paths or executables from request data.
type ControlValidator interface {
	ValidateControl(context.Context, ControlRequest) error
}

// ControlValidatorFunc adapts a function without coupling this package to the
// journal implementation.
type ControlValidatorFunc func(context.Context, ControlRequest) error

func (f ControlValidatorFunc) ValidateControl(ctx context.Context, req ControlRequest) error {
	return f(ctx, req)
}

// ValidateControlRequest checks the versioned stdin protocol. It rejects path,
// executable, shell, and body-bearing fields even though the schema allows
// unknown fields for forward compatibility. Store IDs are opaque selectors,
// never paths or instructions.
func ValidateControlRequest(data []byte) (ControlRequest, error) {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		return ControlRequest{}, fmt.Errorf("%w: invalid JSON: %v", ErrControlValidation, err)
	}
	for _, forbidden := range []string{"body", "bodyText", "bodyContent", "path", "executable", "shell"} {
		if _, ok := raw[forbidden]; ok {
			return ControlRequest{}, fmt.Errorf("%w: field %q is not permitted", ErrControlValidation, forbidden)
		}
	}
	var req ControlRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return ControlRequest{}, fmt.Errorf("%w: %v", ErrControlValidation, err)
	}
	if err := req.Validate(); err != nil {
		return ControlRequest{}, err
	}
	return req, nil
}

func (r ControlRequest) Validate() error {
	if r.Schema != "mektup/control/v1" || r.Kind != "request" {
		return fmt.Errorf("%w: schema and kind must identify a v1 request", ErrControlValidation)
	}
	if !opaqueID(r.OperationID, "op_") || !opaqueID(r.ReplyMessageID, "msg_") || !opaqueID(r.OriginalMessageID, "msg_") {
		return fmt.Errorf("%w: invalid operation or message identity", ErrControlValidation)
	}
	if r.ReceiptID != "" && !opaqueID(r.ReceiptID, "rcpt_") {
		return fmt.Errorf("%w: invalid receipt identity", ErrControlValidation)
	}
	if !opaqueID(r.Custody.EndpointID, "ep_") || !opaqueID(r.Custody.StoreID, "store_") {
		return fmt.Errorf("%w: custody must use opaque endpoint and store IDs", ErrControlValidation)
	}
	if !opaqueID(r.ReplyDestination.EndpointID, "ep_") || !opaqueID(r.ReplyDestination.ThreadID, "thread_") {
		return fmt.Errorf("%w: reply destination must use opaque endpoint and thread IDs", ErrControlValidation)
	}
	switch r.Operation {
	case "claim":
		if r.FencingToken != "" || r.Lease != nil {
			return fmt.Errorf("%w: claim cannot choose a fencing token or lease", ErrControlValidation)
		}
		if r.BodyBytes == nil || r.BodySHA256 == "" || r.ReplyStatus == "" || r.AttemptOwner == "" {
			return fmt.Errorf("%w: claim requires body digest, status, and attempt owner", ErrControlValidation)
		}
	case "heartbeat", "commit", "abandon":
		if r.FencingToken == "" || r.Lease == nil || r.Lease.ExpiresAt == "" {
			return fmt.Errorf("%w: %s requires an issued fencing token and lease", ErrControlValidation, r.Operation)
		}
	case "status", "reconcile":
	default:
		return fmt.Errorf("%w: unsupported operation %q", ErrControlValidation, r.Operation)
	}
	if r.BodyBytes != nil && *r.BodyBytes < 0 {
		return fmt.Errorf("%w: bodyBytes cannot be negative", ErrControlValidation)
	}
	if r.BodySHA256 != "" && !validSHA256(r.BodySHA256) {
		return fmt.Errorf("%w: bodySha256 must be sha256:<64 lowercase hex>", ErrControlValidation)
	}
	if r.RequestedLease != nil && r.RequestedLease.DurationMS <= 0 {
		return fmt.Errorf("%w: requested lease must be positive", ErrControlValidation)
	}
	if r.ReplyStatus != "" && r.ReplyStatus != "success" && r.ReplyStatus != "error" {
		return fmt.Errorf("%w: invalid reply status", ErrControlValidation)
	}
	if r.Custody.EndpointID == r.ReplyDestination.EndpointID && r.Custody.StoreID == "" {
		return fmt.Errorf("%w: custody store ID is required", ErrControlValidation)
	}
	return nil
}

func opaqueID(value, prefix string) bool {
	if !strings.HasPrefix(value, prefix) || len(value) <= len(prefix) {
		return false
	}
	for _, r := range value[len(prefix):] {
		if !(r == '-' || r == '_' || r == '.' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')) {
			return false
		}
	}
	return true
}

func validSHA256(value string) bool {
	if len(value) != len("sha256:")+64 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	for _, r := range value[len("sha256:"):] {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
			return false
		}
	}
	return true
}

// InvokeControl runs a fixed one-shot receiver and sends one validated
// metadata document through stdin. It returns the complete JSON response,
// bounded by Config.ControlLimit. Response semantics belong to the journal
// adapter; transport only preserves bytes and process evidence.
func InvokeControl(ctx context.Context, cfg Config, req ControlRequest, factory ProcessFactory, validator ControlValidator) (response []byte, returnErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if validator != nil {
		if err := validator.ValidateControl(ctx, req); err != nil {
			return nil, err
		}
	}
	argv, err := cfg.ControlArgv()
	if err != nil {
		return nil, err
	}
	child, err := startChild(argv, cfg, factory)
	if err != nil {
		return nil, err
	}
	cleanup := func() { _ = child.close() }
	defer func() {
		if cleanupErr := child.close(); cleanupErr != nil && returnErr == nil {
			returnErr = cleanupErr
		}
	}()
	watchDone := make(chan struct{})
	defer close(watchDone)
	go func() {
		select {
		case <-ctx.Done():
			cleanup()
		case <-watchDone:
		}
	}()

	data, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("encode control request: %w", err)
	}
	data = append(data, '\n')
	writeDone := make(chan error, 1)
	go func() {
		_, writeErr := child.stdin.Write(data)
		closeErr := child.stdin.Close()
		if writeErr != nil {
			writeDone <- &Failure{Kind: FailurePossibleWrite, Cause: FailureProxy, Evidence: WriteMayHaveWritten, Err: writeErr}
			return
		}
		writeDone <- closeErr
	}()
	select {
	case err := <-writeDone:
		if err != nil {
			return nil, err
		}
	case <-ctx.Done():
		return nil, &Failure{Kind: FailurePossibleWrite, Cause: FailureCanceled, Evidence: WriteMayHaveWritten, Err: ctx.Err()}
	}

	outputDone := make(chan controlReadResult, 1)
	go func() {
		body, readErr := io.ReadAll(io.LimitReader(child.stdout, cfg.normalized().ControlLimit+1))
		outputDone <- controlReadResult{body: body, err: readErr}
	}()
	select {
	case result := <-outputDone:
		if result.err != nil {
			return nil, &Failure{Kind: FailureEOF, Cause: FailureEOF, Evidence: WriteComplete, Err: result.err}
		}
		<-child.waitDone
		<-child.stderrDone
		if childErr, stderr, trunc := child.status(); childErr != nil {
			kind := classifyChildFailure(stderr)
			return nil, &Failure{Kind: FailurePossibleWrite, Cause: kind, Evidence: WriteComplete, Err: childErr, ExitCode: processExitCode(childErr), Stderr: stderr, StderrTrunc: trunc}
		}
		limit := cfg.normalized().ControlLimit
		if int64(len(result.body)) > limit {
			return nil, &Failure{Kind: FailureProxy, Cause: FailureProxy, Evidence: WriteComplete, Err: ErrControlTooLarge}
		}
		return result.body, nil
	case <-ctx.Done():
		return nil, &Failure{Kind: FailurePossibleWrite, Cause: FailureCanceled, Evidence: WriteComplete, Err: ctx.Err()}
	}
}

type controlReadResult struct {
	body []byte
	err  error
}
