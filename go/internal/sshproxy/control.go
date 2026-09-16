package sshproxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode"

	mektup "github.com/agensfield/mektup/go"
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
	NativeItemID      string          `json:"nativeItemId,omitempty"`
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
	if req.Kind == "result" && req.Operation == "originalStatus" {
		if _, present := raw["replyMessageId"]; present {
			return ControlRequest{}, fmt.Errorf("%w: originalStatus result forbids top-level replyMessageId", ErrControlValidation)
		}
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
	if req.Kind == "request" && req.Operation == "originalStatus" {
		for _, field := range []string{"replyMessageId", "bodyBytes", "bodySha256", "replyStatus", "replyErrorCode", "nativeItemId", "fencingToken", "lease", "requestedLease", "attemptOwner", "result"} {
			if _, present := raw[field]; present {
				return ControlRequest{}, fmt.Errorf("%w: originalStatus cannot contain %s, including null", ErrControlValidation, field)
			}
		}
	}
	if req.Operation == "observe" {
		for _, field := range []string{"fencingToken", "lease", "requestedLease", "attemptOwner"} {
			if _, present := raw[field]; present {
				return ControlRequest{}, fmt.Errorf("%w: observe cannot contain %s, including null", ErrControlValidation, field)
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
		if req.Operation == "originalStatus" {
			for _, field := range []string{"replyMessageId", "nativeItemId", "bodyBytes", "bodySha256", "replyStatus", "replyErrorCode", "requestedLease", "attemptOwner"} {
				if _, present := raw[field]; present {
					return ControlRequest{}, fmt.Errorf("%w: originalStatus result forbids top-level %s", ErrControlValidation, field)
				}
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
			case "claim", "heartbeat", "commit", "abandon", "status", "reconcile", "observe", "originalStatus":
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
		{"nativeItemId", func(v string) bool { return v != "" }},
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
		} else {
			return fmt.Errorf("%w: replyDestination requires uri", ErrControlValidation)
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

func validateOriginalStatusResult(result map[string]json.RawMessage) error {
	for _, field := range []string{"body", "bodyText", "bodyContent", "replyBody"} {
		if _, present := result[field]; present {
			return fmt.Errorf("%w: originalStatus result forbids body field %s, including null", ErrControlValidation, field)
		}
	}
	for _, field := range []string{"fencingToken", "lease", "requestedLease", "attemptOwner"} {
		if _, present := result[field]; present {
			return fmt.Errorf("%w: originalStatus result forbids authority field %s, including null", ErrControlValidation, field)
		}
	}
	selection, ok := result["selection"]
	if !ok {
		return fmt.Errorf("%w: originalStatus result requires selection", ErrControlValidation)
	}
	selectionValue, err := rawString(selection, "result.selection")
	if err != nil {
		return err
	}
	requireString := func(field, prefix string) error {
		value, present := result[field]
		if !present {
			return fmt.Errorf("%w: originalStatus %s requires %s", ErrControlValidation, selectionValue, field)
		}
		textValue, err := rawString(value, "result."+field)
		if err != nil || (prefix != "" && !validID(textValue, prefix)) {
			return fmt.Errorf("%w: invalid originalStatus result %s", ErrControlValidation, field)
		}
		return nil
	}
	requirePositive := func(field string) error {
		value, present := result[field]
		if !present {
			return fmt.Errorf("%w: originalStatus %s requires %s", ErrControlValidation, selectionValue, field)
		}
		number, err := rawInt(value, "result."+field)
		if err != nil || number <= 0 {
			return fmt.Errorf("%w: originalStatus result %s must be positive", ErrControlValidation, field)
		}
		return nil
	}
	validateReplyEvidence := func() error {
		if err := requireString("replyMessageId", "msg_"); err != nil {
			return err
		}
		status, present := result["replyStatus"]
		if !present {
			return fmt.Errorf("%w: originalStatus result requires replyStatus", ErrControlValidation)
		}
		statusValue, err := rawString(status, "result.replyStatus")
		if err != nil || (statusValue != "success" && statusValue != "error") {
			return fmt.Errorf("%w: originalStatus result has invalid replyStatus", ErrControlValidation)
		}
		bodyBytes, present := result["bodyBytes"]
		if !present {
			return fmt.Errorf("%w: originalStatus result requires bodyBytes", ErrControlValidation)
		}
		if number, err := rawInt(bodyBytes, "result.bodyBytes"); err != nil || number < 0 {
			return fmt.Errorf("%w: originalStatus result bodyBytes must be nonnegative", ErrControlValidation)
		}
		bodyDigest, present := result["bodySha256"]
		if !present {
			return fmt.Errorf("%w: originalStatus result requires bodySha256", ErrControlValidation)
		}
		digestValue, err := rawString(bodyDigest, "result.bodySha256")
		if err != nil || !validSHA256(digestValue) {
			return fmt.Errorf("%w: originalStatus result bodySha256 is invalid", ErrControlValidation)
		}
		if errorCode, present := result["replyErrorCode"]; present {
			code, err := rawString(errorCode, "result.replyErrorCode")
			if err != nil || code == "" || statusValue != "error" {
				return fmt.Errorf("%w: replyErrorCode requires error replyStatus", ErrControlValidation)
			}
		}
		return nil
	}
	forbiddenSelected := []string{"replyMessageId", "replyStatus", "replyErrorCode", "status", "bodyBytes", "bodySha256", "commitSeq", "eventSeq", "nativeItemId", "fencingToken", "lease", "requestedLease", "attemptOwner", "state"}
	switch selectionValue {
	case "winner":
		if err := requireString("state", ""); err != nil {
			return err
		}
		state, _ := rawString(result["state"], "result.state")
		if state != "reply_accepted" && state != "reply_observed" {
			return fmt.Errorf("%w: winner state must be reply_accepted or reply_observed", ErrControlValidation)
		}
		if err := validateReplyEvidence(); err != nil {
			return err
		}
		if err := requirePositive("commitSeq"); err != nil {
			return err
		}
		if state == "reply_observed" {
			if err := requireString("nativeItemId", ""); err != nil {
				return err
			}
		} else if native, present := result["nativeItemId"]; present {
			if _, err := rawString(native, "result.nativeItemId"); err != nil {
				return err
			}
		}
		if _, present := result["eventSeq"]; present {
			return fmt.Errorf("%w: winner forbids eventSeq", ErrControlValidation)
		}
	case "terminal_unknown":
		if err := requireString("state", ""); err != nil {
			return err
		}
		state, _ := rawString(result["state"], "result.state")
		if state != "reply_outcome_unknown" {
			return fmt.Errorf("%w: terminal_unknown state must be reply_outcome_unknown", ErrControlValidation)
		}
		if err := validateReplyEvidence(); err != nil {
			return err
		}
		if err := requirePositive("eventSeq"); err != nil {
			return err
		}
		if _, present := result["commitSeq"]; present {
			return fmt.Errorf("%w: terminal_unknown forbids commitSeq", ErrControlValidation)
		}
		if _, present := result["nativeItemId"]; present {
			return fmt.Errorf("%w: terminal_unknown forbids nativeItemId", ErrControlValidation)
		}
	case "pending":
		for _, field := range forbiddenSelected {
			if _, present := result[field]; present {
				return fmt.Errorf("%w: pending forbids %s", ErrControlValidation, field)
			}
		}
	default:
		return fmt.Errorf("%w: unsupported originalStatus selection %q", ErrControlValidation, selectionValue)
	}
	return nil
}

func (r ControlRequest) Validate() error {
	if r.Schema != "mektup/control/v1" || (r.Kind != "request" && r.Kind != "result") {
		return fmt.Errorf("%w: schema and kind must identify a v1 document", ErrControlValidation)
	}
	if !validID(r.OperationID, "op_") || !validID(r.OriginalMessageID, "msg_") || (r.Operation != "originalStatus" && !validID(r.ReplyMessageID, "msg_")) {
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
	if !validURI(r.ReplyDestination.URI) {
		return fmt.Errorf("%w: reply destination URI is required and must be valid", ErrControlValidation)
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
	if r.Operation != "claim" && r.Operation != "status" && r.Operation != "reconcile" && r.Operation != "observe" && r.Operation != "originalStatus" && r.RequestedLease != nil {
		return fmt.Errorf("%w: %s cannot contain requestedLease", ErrControlValidation, r.Operation)
	}
	switch r.Operation {
	case "claim", "heartbeat", "commit", "abandon", "status", "reconcile", "observe", "originalStatus":
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
		if r.Operation == "originalStatus" {
			return validateOriginalStatusResult(resultObject)
		}
		if r.Operation == "claim" {
			var resultObject map[string]json.RawMessage
			if err := json.Unmarshal(r.Result, &resultObject); err != nil || resultObject == nil {
				return fmt.Errorf("%w: claim result must be an object", ErrControlValidation)
			}
			var result struct {
				Disposition  string          `json:"disposition"`
				State        string          `json:"state"`
				FencingToken string          `json:"fencingToken"`
				Lease        *Lease          `json:"lease"`
				Status       string          `json:"status"`
				Winner       json.RawMessage `json:"winner"`
			}
			if err := json.Unmarshal(r.Result, &result); err != nil || result.Disposition == "" || !mektup.EvidenceState(result.State).Valid() {
				return fmt.Errorf("%w: claim result requires disposition and valid state", ErrControlValidation)
			}
			switch result.Disposition {
			case "claimed":
				leaseRaw, present := resultObject["lease"]
				if !present || result.FencingToken == "" || result.Lease == nil || !result.Lease.valid() || validateLeaseObject(leaseRaw) != nil {
					return fmt.Errorf("%w: claimed result requires fencingToken and lease", ErrControlValidation)
				}
			case "existing":
				if _, present := resultObject["fencingToken"]; present {
					return fmt.Errorf("%w: existing result forbids fencingToken and lease", ErrControlValidation)
				}
				if _, present := resultObject["lease"]; present {
					return fmt.Errorf("%w: existing result forbids fencingToken and lease", ErrControlValidation)
				}
				if raw, present := resultObject["status"]; present {
					if text, err := rawString(raw, "result.status"); err != nil || text == "" {
						return fmt.Errorf("%w: existing result status must be a nonempty string", ErrControlValidation)
					}
				}
				if raw, present := resultObject["winner"]; present {
					if _, err := rawObject(raw, "result.winner"); err != nil {
						return fmt.Errorf("%w: existing result winner must be an object", ErrControlValidation)
					}
				}
			default:
				return fmt.Errorf("%w: unsupported claim result disposition %q", ErrControlValidation, result.Disposition)
			}
		} else if r.Operation == "observe" {
			var result struct {
				State string `json:"state"`
			}
			if err := json.Unmarshal(r.Result, &result); err != nil || !mektup.EvidenceState(result.State).Valid() {
				return fmt.Errorf("%w: observe result requires valid state", ErrControlValidation)
			}
			for _, field := range []string{"fencingToken", "lease"} {
				if _, present := resultObject[field]; present {
					return fmt.Errorf("%w: observe result forbids %s", ErrControlValidation, field)
				}
			}
			if raw, present := resultObject["status"]; present {
				if text, err := rawString(raw, "result.status"); err != nil || text == "" {
					return fmt.Errorf("%w: observe result status must be a nonempty string", ErrControlValidation)
				}
			}
			if raw, present := resultObject["winner"]; present {
				if _, err := rawObject(raw, "result.winner"); err != nil {
					return fmt.Errorf("%w: observe result winner must be an object", ErrControlValidation)
				}
			}
			if raw, present := resultObject["provenance"]; present {
				provenance, err := rawObject(raw, "result.provenance")
				if err != nil {
					return err
				}
				for _, field := range []string{"endpointId", "controlRoute"} {
					if value, exists := provenance[field]; exists {
						text, textErr := rawString(value, "result.provenance."+field)
						if textErr != nil || text == "" {
							return fmt.Errorf("%w: observe result provenance.%s must be a nonempty string", ErrControlValidation, field)
						}
					}
				}
			}
		} else if r.Operation == "originalStatus" {
			if _, present := resultObject["status"]; present {
				return fmt.Errorf("%w: originalStatus result forbids status", ErrControlValidation)
			}
			if _, present := resultObject["winner"]; present {
				return fmt.Errorf("%w: originalStatus result forbids winner", ErrControlValidation)
			}
			if err := validateOriginalStatusResult(resultObject); err != nil {
				return err
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
	case "originalStatus":
		if r.ReplyMessageID != "" || r.BodyBytes != nil || r.BodySHA256 != "" || r.ReplyStatus != "" || r.ReplyErrorCode != "" || r.NativeItemID != "" || r.FencingToken != "" || r.Lease != nil || r.RequestedLease != nil || r.AttemptOwner != "" || len(r.Result) != 0 {
			return fmt.Errorf("%w: originalStatus is metadata-only and original-scoped", ErrControlValidation)
		}
	case "observe":
		if r.NativeItemID == "" || r.BodyBytes == nil || r.BodySHA256 == "" || r.ReplyStatus == "" || r.FencingToken != "" || r.Lease != nil || r.RequestedLease != nil || r.AttemptOwner != "" {
			return fmt.Errorf("%w: observe requires native item evidence without dispatch authority", ErrControlValidation)
		}
	}
	return nil
}

func validateOriginalStatusResult(result map[string]json.RawMessage) error {
	for _, field := range []string{"body", "bodyText", "bodyContent"} {
		if _, ok := result[field]; ok {
			return fmt.Errorf("%w: originalStatus result forbids %s", ErrControlValidation, field)
		}
	}
	for _, field := range []string{"fencingToken", "lease", "requestedLease", "attemptOwner"} {
		if _, ok := result[field]; ok {
			return fmt.Errorf("%w: originalStatus result forbids %s", ErrControlValidation, field)
		}
	}
	selectionRaw, ok := result["selection"]
	if !ok {
		return fmt.Errorf("%w: originalStatus result requires selection", ErrControlValidation)
	}
	selection, err := rawString(selectionRaw, "result.selection")
	if err != nil {
		return err
	}
	selected := []string{"replyMessageId", "replyStatus", "bodyBytes", "bodySha256", "state", "commitSeq", "eventSeq", "replyErrorCode", "nativeItemId"}
	if selection == "pending" {
		for _, field := range selected {
			if _, ok := result[field]; ok {
				return fmt.Errorf("%w: pending originalStatus forbids %s", ErrControlValidation, field)
			}
		}
		return nil
	}
	if selection != "winner" && selection != "terminal_unknown" {
		return fmt.Errorf("%w: invalid originalStatus selection", ErrControlValidation)
	}
	for _, field := range []string{"replyMessageId", "replyStatus", "bodyBytes", "bodySha256", "state"} {
		if _, ok := result[field]; !ok {
			return fmt.Errorf("%w: selected originalStatus result requires %s", ErrControlValidation, field)
		}
	}
	replyID, err := rawString(result["replyMessageId"], "result.replyMessageId")
	if err != nil || !validID(replyID, "msg_") {
		return fmt.Errorf("%w: invalid result.replyMessageId", ErrControlValidation)
	}
	status, err := rawString(result["replyStatus"], "result.replyStatus")
	if err != nil || (status != "success" && status != "error") {
		return fmt.Errorf("%w: invalid result.replyStatus", ErrControlValidation)
	}
	bytesValue, err := rawInt(result["bodyBytes"], "result.bodyBytes")
	if err != nil || bytesValue < 0 {
		return fmt.Errorf("%w: invalid result.bodyBytes", ErrControlValidation)
	}
	digest, err := rawString(result["bodySha256"], "result.bodySha256")
	if err != nil || !validSHA256(digest) {
		return fmt.Errorf("%w: invalid result.bodySha256", ErrControlValidation)
	}
	state, err := rawString(result["state"], "result.state")
	if err != nil {
		return err
	}
	if selection == "winner" {
		if state != "reply_accepted" && state != "reply_observed" {
			return fmt.Errorf("%w: invalid winner state", ErrControlValidation)
		}
	} else if state != "reply_outcome_unknown" {
		return fmt.Errorf("%w: invalid unknown state", ErrControlValidation)
	}
	seqField := "commitSeq"
	if selection == "terminal_unknown" {
		seqField = "eventSeq"
	}
	seqRaw, ok := result[seqField]
	if !ok {
		return fmt.Errorf("%w: selected result requires %s", ErrControlValidation, seqField)
	}
	seq, err := rawInt(seqRaw, "result."+seqField)
	if err != nil || seq <= 0 {
		return fmt.Errorf("%w: invalid result.%s", ErrControlValidation, seqField)
	}
	if codeRaw, ok := result["replyErrorCode"]; ok {
		code, err := rawString(codeRaw, "result.replyErrorCode")
		if err != nil || status != "error" || code == "" {
			return fmt.Errorf("%w: invalid result.replyErrorCode", ErrControlValidation)
		}
	} else if status == "success" { /* optional field omitted */
	}
	if nativeRaw, ok := result["nativeItemId"]; ok {
		if selection != "winner" || state != "reply_observed" {
			return fmt.Errorf("%w: nativeItemId only applies to observed winners", ErrControlValidation)
		}
		if _, err := rawString(nativeRaw, "result.nativeItemId"); err != nil {
			return err
		}
	} else if selection == "winner" && state == "reply_observed" {
		return fmt.Errorf("%w: observed winner requires nativeItemId", ErrControlValidation)
	}
	return nil
}

func validID(value, prefix string) bool {
	return mektup.ValidateID(value, prefix) == nil
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
	if colon < 1 || colon+3 >= len(value) || strings.IndexFunc(value, unicode.IsSpace) >= 0 {
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
