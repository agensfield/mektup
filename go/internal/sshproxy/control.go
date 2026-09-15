package sshproxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
)

// ControlRequest is the metadata-only seam between SSH transport and the
// journal/custody implementation. The transport validates the wire shape and
// opaque identity relationships, but deliberately does not know journal
// tables, state paths, leases, or fencing rules.
type ControlRequest struct {
	Schema            string          `json:"schema"`
	Kind              string          `json:"kind"`
	Operation         string          `json:"operation"`
	OperationID       string          `json:"operationId"`
	ReceiptID         string          `json:"receiptId,omitempty"`
	ReplyMessageID    string          `json:"replyMessageId"`
	OriginalMessageID string          `json:"originalMessageId"`
	Custody           CustodyRef      `json:"custody"`
	ReplyDestination  DestinationRef  `json:"replyDestination"`
	BodyBytes         *int64          `json:"bodyBytes,omitempty"`
	BodySHA256        string          `json:"bodySha256,omitempty"`
	ReplyStatus       string          `json:"replyStatus,omitempty"`
	ReplyErrorCode    string          `json:"replyErrorCode,omitempty"`
	RequestedLease    *LeaseRequest   `json:"requestedLease,omitempty"`
	FencingToken      string          `json:"fencingToken,omitempty"`
	Lease             *Lease          `json:"lease,omitempty"`
	AttemptOwner      string          `json:"attemptOwner,omitempty"`
	RequestedAt       string          `json:"requestedAt,omitempty"`
	Result            json.RawMessage `json:"result,omitempty"`
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
	if err := validateKnownFields(raw); err != nil {
		return ControlRequest{}, err
	}
	var req ControlRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return ControlRequest{}, fmt.Errorf("%w: %v", ErrControlValidation, err)
	}
	if err := req.Validate(); err != nil {
		return ControlRequest{}, err
	}
	if req.Kind == "request" && req.Operation == "claim" {
		for _, field := range []string{"lease", "fencingToken"} {
			if _, present := raw[field]; present {
				return ControlRequest{}, fmt.Errorf("%w: claim cannot contain %s, including null", ErrControlValidation, field)
			}
		}
	}
	if req.Kind == "request" && (req.Operation == "heartbeat" || req.Operation == "commit" || req.Operation == "abandon") {
		if _, present := raw["requestedLease"]; present {
			return ControlRequest{}, fmt.Errorf("%w: %s cannot contain requestedLease, including null", ErrControlValidation, req.Operation)
		}
	}
	if req.Kind == "result" {
		for _, field := range []string{"lease", "fencingToken"} {
			if _, present := raw[field]; present {
				return ControlRequest{}, fmt.Errorf("%w: result cannot contain top-level %s", ErrControlValidation, field)
			}
		}
	}
	return req, nil
}

func validateKnownFields(raw map[string]json.RawMessage) error {
	stringChecks := []struct {
		field string
		check func(string) bool
	}{
		{"schema", func(v string) bool { return v == "mektup/control/v1" }},
		{"kind", func(v string) bool { return v == "request" || v == "result" }},
		{"operation", func(v string) bool {
			switch v {
			case "claim", "heartbeat", "commit", "abandon", "status", "reconcile":
				return true
			}
			return false
		}},
		{"operationId", func(v string) bool { return validID(v, "op_") }},
		{"receiptId", func(v string) bool { return validID(v, "rcpt_") }},
		{"replyMessageId", func(v string) bool { return validID(v, "msg_") }},
		{"originalMessageId", func(v string) bool { return validID(v, "msg_") }},
		{"bodySha256", validSHA256},
		{"replyStatus", func(v string) bool { return v == "success" || v == "error" }},
		{"replyErrorCode", func(v string) bool { return v != "" }},
		{"fencingToken", func(v string) bool { return v != "" }},
		{"attemptOwner", func(v string) bool { return v != "" }},
		{"requestedAt", validTimestamp},
	}
	for _, item := range stringChecks {
		if value, present := raw[item.field]; present {
			textValue, err := rawString(value, item.field)
			if err != nil || !item.check(textValue) {
				return fmt.Errorf("%w: invalid known field %s", ErrControlValidation, item.field)
			}
		}
	}
	if value, present := raw["bodyBytes"]; present {
		if number, err := rawInt(value, "bodyBytes"); err != nil || number < 0 {
			return fmt.Errorf("%w: bodyBytes must be a nonnegative integer", ErrControlValidation)
		}
	}
	if value, present := raw["requestedLease"]; present {
		object, err := rawObject(value, "requestedLease")
		if err != nil {
			return err
		}
		if duration, ok := object["durationMs"]; !ok {
			return fmt.Errorf("%w: requestedLease requires durationMs", ErrControlValidation)
		} else if number, err := rawInt(duration, "requestedLease.durationMs"); err != nil || number <= 0 {
			return fmt.Errorf("%w: requestedLease durationMs must be positive", ErrControlValidation)
		}
	}
	if value, present := raw["lease"]; present {
		if err := validateLeaseObject(value); err != nil {
			return err
		}
	}
	if value, present := raw["custody"]; present {
		object, err := rawObject(value, "custody")
		if err != nil {
			return err
		}
		if err := validateRawID(object, "endpointId", "ep_"); err != nil {
			return err
		}
		if err := validateRawID(object, "storeId", "store_"); err != nil {
			return err
		}
	}
	if value, present := raw["replyDestination"]; present {
		object, err := rawObject(value, "replyDestination")
		if err != nil {
			return err
		}
		if err := validateRawID(object, "endpointId", "ep_"); err != nil {
			return err
		}
		if thread, ok := object["threadId"]; !ok {
			return fmt.Errorf("%w: replyDestination requires threadId", ErrControlValidation)
		} else if textValue, err := rawString(thread, "replyDestination.threadId"); err != nil || textValue == "" {
			return fmt.Errorf("%w: invalid replyDestination.threadId", ErrControlValidation)
		}
		if uri, ok := object["uri"]; ok {
			textValue, err := rawString(uri, "replyDestination.uri")
			if err != nil || !validURI(textValue) {
				return fmt.Errorf("%w: invalid replyDestination.uri", ErrControlValidation)
			}
		}
	}
	if value, present := raw["result"]; present {
		if _, err := rawObject(value, "result"); err != nil {
			return err
		}
	}
	return nil
}

func rawString(value json.RawMessage, field string) (string, error) {
	if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		return "", fmt.Errorf("%w: %s cannot be null", ErrControlValidation, field)
	}
	var result string
	if err := json.Unmarshal(value, &result); err != nil || result == "" {
		return "", fmt.Errorf("%w: %s must be a nonempty string", ErrControlValidation, field)
	}
	return result, nil
}

func rawInt(value json.RawMessage, field string) (int64, error) {
	if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		return 0, fmt.Errorf("%w: %s cannot be null", ErrControlValidation, field)
	}
	var result int64
	if err := json.Unmarshal(value, &result); err != nil {
		return 0, fmt.Errorf("%w: %s must be an integer", ErrControlValidation, field)
	}
	return result, nil
}

func rawObject(value json.RawMessage, field string) (map[string]json.RawMessage, error) {
	if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
		return nil, fmt.Errorf("%w: %s cannot be null", ErrControlValidation, field)
	}
	var result map[string]json.RawMessage
	if err := json.Unmarshal(value, &result); err != nil || result == nil {
		return nil, fmt.Errorf("%w: %s must be an object", ErrControlValidation, field)
	}
	return result, nil
}

func validateRawID(object map[string]json.RawMessage, field, prefix string) error {
	value, ok := object[field]
	if !ok {
		return fmt.Errorf("%w: missing %s", ErrControlValidation, field)
	}
	textValue, err := rawString(value, field)
	if err != nil || !validID(textValue, prefix) {
		return fmt.Errorf("%w: invalid %s", ErrControlValidation, field)
	}
	return nil
}

func validateLeaseObject(value json.RawMessage) error {
	object, err := rawObject(value, "lease")
	if err != nil {
		return err
	}
	for _, item := range []struct {
		field    string
		required bool
	}{{"expiresAt", true}, {"acquiredAt", false}, {"heartbeatAt", false}} {
		value, present := object[item.field]
		if !present {
			if item.required {
				return fmt.Errorf("%w: lease requires expiresAt", ErrControlValidation)
			}
			continue
		}
		textValue, valueErr := rawString(value, "lease."+item.field)
		if valueErr != nil || !validTimestamp(textValue) {
			return fmt.Errorf("%w: invalid lease.%s", ErrControlValidation, item.field)
		}
	}
	return nil
}

func (r ControlRequest) Validate() error {
	if r.Schema != "mektup/control/v1" || (r.Kind != "request" && r.Kind != "result") {
		return fmt.Errorf("%w: schema and kind must identify a v1 document", ErrControlValidation)
	}
	if !validID(r.OperationID, "op_") || !validID(r.ReplyMessageID, "msg_") || !validID(r.OriginalMessageID, "msg_") {
		return fmt.Errorf("%w: invalid operation or message identity", ErrControlValidation)
	}
	if r.ReceiptID != "" && !validID(r.ReceiptID, "rcpt_") {
		return fmt.Errorf("%w: invalid receipt identity", ErrControlValidation)
	}
	if !validID(r.Custody.EndpointID, "ep_") || !validID(r.Custody.StoreID, "store_") {
		return fmt.Errorf("%w: custody must use opaque endpoint and store IDs", ErrControlValidation)
	}
	if !validID(r.ReplyDestination.EndpointID, "ep_") || r.ReplyDestination.ThreadID == "" {
		return fmt.Errorf("%w: reply destination must use opaque endpoint and nonempty thread ID", ErrControlValidation)
	}
	if r.ReplyDestination.URI != "" && !validURI(r.ReplyDestination.URI) {
		return fmt.Errorf("%w: invalid reply destination URI", ErrControlValidation)
	}
	if r.RequestedAt != "" && !validTimestamp(r.RequestedAt) {
		return fmt.Errorf("%w: invalid requestedAt timestamp", ErrControlValidation)
	}
	if r.Lease != nil && !r.Lease.valid() {
		return fmt.Errorf("%w: invalid lease timestamp", ErrControlValidation)
	}
	if r.ReplyErrorCode != "" && r.ReplyStatus != "error" {
		return fmt.Errorf("%w: replyErrorCode requires error status", ErrControlValidation)
	}
	if r.BodyBytes != nil && *r.BodyBytes < 0 {
		return fmt.Errorf("%w: bodyBytes cannot be negative", ErrControlValidation)
	}
	if r.RequestedLease != nil && r.RequestedLease.DurationMS <= 0 {
		return fmt.Errorf("%w: requested lease must be positive", ErrControlValidation)
	}
	if r.BodySHA256 != "" && !validSHA256(r.BodySHA256) {
		return fmt.Errorf("%w: bodySha256 must be sha256:<64 lowercase hex>", ErrControlValidation)
	}
	if r.ReplyStatus != "" && r.ReplyStatus != "success" && r.ReplyStatus != "error" {
		return fmt.Errorf("%w: invalid reply status", ErrControlValidation)
	}
	if r.Operation != "claim" && r.Operation != "status" && r.Operation != "reconcile" && r.RequestedLease != nil {
		return fmt.Errorf("%w: %s cannot contain requestedLease", ErrControlValidation, r.Operation)
	}
	switch r.Operation {
	case "claim", "heartbeat", "commit", "abandon", "status", "reconcile":
	default:
		return fmt.Errorf("%w: unsupported operation %q", ErrControlValidation, r.Operation)
	}
	if r.Kind == "result" {
		if len(r.Result) == 0 || string(r.Result) == "null" || r.FencingToken != "" || r.Lease != nil {
			return fmt.Errorf("%w: result requires result object and no top-level claim lease", ErrControlValidation)
		}
		var resultObject map[string]json.RawMessage
		if err := json.Unmarshal(r.Result, &resultObject); err != nil || resultObject == nil {
			return fmt.Errorf("%w: result must be an object", ErrControlValidation)
		}
		if r.Operation == "claim" {
			var result struct {
				FencingToken string `json:"fencingToken"`
				Lease        Lease  `json:"lease"`
			}
			if err := json.Unmarshal(r.Result, &result); err != nil || result.FencingToken == "" || !result.Lease.valid() {
				return fmt.Errorf("%w: claim result requires fencingToken and lease", ErrControlValidation)
			}
		}
		return nil
	}
	if r.Operation == "claim" && r.Result != nil {
		return fmt.Errorf("%w: claim request cannot contain result", ErrControlValidation)
	}
	switch r.Operation {
	case "claim", "heartbeat", "commit", "abandon":
		if r.BodyBytes == nil || r.BodySHA256 == "" || r.ReplyStatus == "" || r.AttemptOwner == "" {
			return fmt.Errorf("%w: %s requires body digest, status, and attempt owner", ErrControlValidation, r.Operation)
		}
		if r.Operation == "claim" && (r.FencingToken != "" || r.Lease != nil) {
			return fmt.Errorf("%w: claim cannot choose a fencing token or lease", ErrControlValidation)
		}
		if r.FencingToken == "" || r.Lease == nil || r.Lease.ExpiresAt == "" {
			if r.Operation != "claim" {
				return fmt.Errorf("%w: %s requires an issued fencing token and lease", ErrControlValidation, r.Operation)
			}
		}
	case "status", "reconcile":
	}
	return nil
}

func validID(value, prefix string) bool {
	// This mirrors the public mektup.ValidateID contract used by the
	// integration branch. The appserver foundation branch intentionally does
	// not yet contain that public package, so the transport keeps the same
	// UUIDv7 shape locally until integration supplies the shared helper.
	if !strings.HasPrefix(value, prefix) || len(value) != len(prefix)+36 {
		return false
	}
	uuid := value[len(prefix):]
	for i, r := range uuid {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if r != '-' {
				return false
			}
			continue
		}
		if i == 14 && r != '7' {
			return false
		}
		if i == 19 && r != '8' && r != '9' && r != 'a' && r != 'b' {
			return false
		}
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
			return false
		}
	}
	return true
}

func validTimestamp(value string) bool {
	if len(value) < len("2006-01-02T15:04:05.0Z") || !strings.HasSuffix(value, "Z") {
		return false
	}
	if len(value) <= 19 || value[19] != '.' {
		return false
	}
	_, err := time.Parse(time.RFC3339Nano, value)
	return err == nil
}

func validURI(value string) bool {
	colon := strings.Index(value, "://")
	if colon < 1 || colon+3 >= len(value) || strings.IndexAny(value, " \t\r\n") >= 0 {
		return false
	}
	for i, r := range value[:colon] {
		if i == 0 && !(r >= 'a' && r <= 'z') {
			return false
		}
		if i > 0 && !(r == '+' || r == '-' || r == '.' || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')) {
			return false
		}
	}
	return true
}

func (l Lease) valid() bool {
	if l.ExpiresAt == "" || !validTimestamp(l.ExpiresAt) {
		return false
	}
	return (l.AcquiredAt == "" || validTimestamp(l.AcquiredAt)) && (l.HeartbeatAt == "" || validTimestamp(l.HeartbeatAt))
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
	if err := ctx.Err(); err != nil {
		return nil, &Failure{Kind: FailureCanceled, Cause: FailureCanceled, Evidence: WriteNotStarted, Err: err}
	}
	if err := req.Validate(); err != nil {
		return nil, err
	}
	if req.Kind != "request" {
		return nil, fmt.Errorf("%w: control receiver accepts requests only", ErrControlValidation)
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
		written, writeErr := child.stdin.Write(data)
		if writeErr == nil && written != len(data) {
			writeErr = io.ErrShortWrite
		}
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
		defer child.markStdoutDone()
		body, readErr := io.ReadAll(io.LimitReader(child.stdout, cfg.normalized().ControlLimit+1))
		outputDone <- controlReadResult{body: body, err: readErr}
	}()
	select {
	case result := <-outputDone:
		if result.err != nil {
			return nil, &Failure{Kind: FailureEOF, Cause: FailureEOF, Evidence: WriteComplete, Err: result.err}
		}
		limit := cfg.normalized().ControlLimit
		if int64(len(result.body)) > limit {
			_ = child.close()
			return nil, &Failure{Kind: FailureProxy, Cause: FailureProxy, Evidence: WriteComplete, Err: ErrControlTooLarge}
		}
		waitTimer := time.NewTimer(cfg.normalized().CleanupTimeout)
		select {
		case <-child.waitDone:
			if !waitTimer.Stop() {
				select {
				case <-waitTimer.C:
				default:
				}
			}
		case <-ctx.Done():
			if !waitTimer.Stop() {
				select {
				case <-waitTimer.C:
				default:
				}
			}
			return nil, &Failure{Kind: FailurePossibleWrite, Cause: FailureCanceled, Evidence: WriteComplete, Err: ctx.Err()}
		case <-waitTimer.C:
			_ = child.close()
			return nil, &Failure{Kind: FailureProxy, Cause: FailureProxy, Evidence: WriteComplete, Err: ErrCleanupTimeout}
		}
		if childErr, stderr, trunc := child.status(); childErr != nil {
			kind := classifyChildFailure(stderr)
			return nil, &Failure{Kind: FailurePossibleWrite, Cause: kind, Evidence: WriteComplete, Err: childErr, ExitCode: processExitCode(childErr), Stderr: stderr, StderrTrunc: trunc}
		}
		if _, validationErr := validateControlResult(result.body, req); validationErr != nil {
			return nil, &Failure{Kind: FailureProxy, Cause: FailureProxy, Evidence: WriteComplete, Err: validationErr}
		}
		return result.body, nil
	case <-ctx.Done():
		return nil, &Failure{Kind: FailurePossibleWrite, Cause: FailureCanceled, Evidence: WriteComplete, Err: ctx.Err()}
	}
}

func validateControlResult(data []byte, request ControlRequest) (ControlRequest, error) {
	result, err := ValidateControlRequest(data)
	if err != nil {
		return ControlRequest{}, err
	}
	if result.Kind != "result" {
		return ControlRequest{}, fmt.Errorf("%w: control response kind is %q", ErrControlValidation, result.Kind)
	}
	if result.Operation != request.Operation || result.OperationID != request.OperationID ||
		result.ReplyMessageID != request.ReplyMessageID || result.OriginalMessageID != request.OriginalMessageID ||
		result.Custody != request.Custody || result.ReplyDestination != request.ReplyDestination ||
		result.ReceiptID != request.ReceiptID {
		return ControlRequest{}, fmt.Errorf("%w: control response identity does not match request", ErrControlValidation)
	}
	return result, nil
}

type controlReadResult struct {
	body []byte
	err  error
}
