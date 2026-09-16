// Package rawrpc is the safety boundary for Mektup's raw app-server escape
// hatch. It classifies and gates a request before handing it to the low-level
// app-server client, and keeps the result raw so newer server fields are not
// lost at this boundary.
package rawrpc

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"

	mektup "github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/appserver"
	"github.com/agensfield/mektup/go/internal/artifact"
	"github.com/agensfield/mektup/go/internal/rpcmeta"
)

const (
	// DefaultInlineLimit is deliberately bounded. Callers can choose a lower
	// limit for a presentation surface with a smaller stdout budget.
	DefaultInlineLimit int64 = 1 << 20

	ParamsOmitted ParamsSource = ""
	ParamsInline  ParamsSource = "inline"
	ParamsFile    ParamsSource = "file"
	ParamsStdin   ParamsSource = "stdin"
)

// ParamsSource records which already-bounded source supplied Params. Reading
// files and stdin belongs to the caller, so this package never opens an
// untrusted path or consumes an unbounded stream.
type ParamsSource string

func (s ParamsSource) valid() bool {
	switch s {
	case ParamsOmitted, ParamsInline, ParamsFile, ParamsStdin:
		return true
	default:
		return false
	}
}

// Request is one raw RPC invocation. Params is passed byte-for-byte to the
// caller. An omitted source with non-empty Params is treated as inline for
// library callers; the CLI remains responsible for rejecting multiple source
// flags before constructing this value.
type Request struct {
	Method       string
	Params       json.RawMessage
	ParamsSource ParamsSource
	ID           appserver.RequestID
	Grants       []rpcmeta.EffectClass
	Output       OutputOptions
}

// OutputOptions controls bounded response retention and optional complete
// output publication. Store is used for managed private artifacts; Path is an
// explicit caller-owned destination and may be outside Store.Root().
type OutputOptions struct {
	InlineLimit int64
	Store       *artifact.Store
	Name        string
	Path        string
	Force       bool
	MaxBytes    int64
}

// Caller is intentionally the same narrow boundary as appserver.Client. A
// connection adapter may provide initialization/capability negotiation around
// this call without making rawrpc depend on its transport implementation.
type Caller interface {
	Call(context.Context, appserver.RPCRequest) (*appserver.RPCResult, error)
}

// ExperimentalCapability is the positive connection-level proof required for
// experimental methods or selected experimental fields. A request boolean is
// deliberately not accepted as proof: the connection adapter must report the
// capability it actually negotiated during initialization.
type ExperimentalCapability interface {
	ExperimentalAPIEnabled() bool
}

// FuncCaller adapts a function to Caller and is useful for deterministic
// tests or a thin connection-layer adapter.
type FuncCaller func(context.Context, appserver.RPCRequest) (*appserver.RPCResult, error)

func (f FuncCaller) Call(ctx context.Context, request appserver.RPCRequest) (*appserver.RPCResult, error) {
	return f(ctx, request)
}

// Plan is the complete pre-dispatch decision. ExperimentalAPIRequired is a
// connection capability request, not an effect grant and is never persisted
// as an invocation authorization.
type Plan struct {
	Request                 Request
	Decision                rpcmeta.Decision
	ExperimentalAPIRequired bool
	NetworkRead             bool
	RetrySafety             rpcmeta.RetrySafety
	Missing                 []rpcmeta.EffectClass
}

// Response retains the result value exactly when it fits the inline bound.
// For a spilled response, Artifact is complete and Raw is intentionally nil.
type Response struct {
	Method                  string
	ID                      appserver.RequestID
	Raw                     json.RawMessage
	Artifact                *artifact.Receipt
	Effects                 []rpcmeta.EffectClass
	Experimental            bool
	ExperimentalFields      []string
	ExperimentalAPIRequired bool
	NetworkRead             bool
	RetrySafety             rpcmeta.RetrySafety
	WriteEvidence           appserver.WriteEvidence
	EffectState             mektup.EvidenceState
}

// ServerErrorEvidence preserves the JSON-RPC error fields without replacing
// the stable Mektup error code. Data is copied, never decoded and re-encoded.
type ServerErrorEvidence struct {
	ID               appserver.RequestID `json:"id,omitempty"`
	Code             int64               `json:"code"`
	Message          string              `json:"message,omitempty"`
	MessageBytes     int64               `json:"messageBytes,omitempty"`
	MessageSHA256    string              `json:"messageSHA256,omitempty"`
	Data             json.RawMessage     `json:"data,omitempty"`
	DataBytes        int64               `json:"dataBytes,omitempty"`
	DataSHA256       string              `json:"dataSHA256,omitempty"`
	DataArtifact     *artifact.Receipt   `json:"dataArtifact,omitempty"`
	EvidenceBytes    int64               `json:"evidenceBytes,omitempty"`
	EvidenceSHA256   string              `json:"evidenceSHA256,omitempty"`
	EvidenceArtifact *artifact.Receipt   `json:"evidenceArtifact,omitempty"`
	Generation       uint64              `json:"generation,omitempty"`
}

// Error is a stable Mektup terminal error with optional untouched app-server
// evidence. Code and EffectState remain authoritative for callers and wire
// serializers; Server is nested evidence only.
type Error struct {
	Code        mektup.ErrorCode `json:"code"`
	Message     string           `json:"message"`
	Retryable   bool             `json:"retryable"`
	EffectState string           `json:"effectState"`
	Details     map[string]any   `json:"details,omitempty"`
	// Mektup is the canonical wire-shaped error. The adjacent fields make
	// inspection pleasant without embedding a type that has Error() itself.
	Mektup mektup.Error         `json:"-"`
	Server *ServerErrorEvidence `json:"server,omitempty"`
	Cause  error                `json:"-"`
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// Stable returns the canonical wire-shaped error value.
func (e *Error) Stable() mektup.Error {
	if e == nil {
		return mektup.Error{}
	}
	return e.Mektup
}

// Prepare classifies and gates a request. It returns the plan even when the
// request is rejected, allowing a connection layer to observe that an
// experimental capability is needed without dispatching the request.
func Prepare(request Request) (Plan, error) {
	plan := Plan{Request: request}
	if err := validateRequest(&plan.Request); err != nil {
		return plan, newError(mektup.ErrInvalidArguments, err.Error(), mektup.StateNotSent, false, nil, err)
	}
	plan.Request.Params = append(json.RawMessage(nil), plan.Request.Params...)
	plan.Decision = rpcmeta.Evaluate(plan.Request.Method, plan.Request.Params)
	plan.ExperimentalAPIRequired = plan.Decision.Experimental
	plan.NetworkRead = hasEffect(plan.Decision.Effects, rpcmeta.EffectNetworkRead)
	plan.RetrySafety = plan.Decision.Metadata.RetrySafety
	plan.Missing = missingEffects(plan.Decision.Effects, plan.Request.Grants)
	if len(plan.Missing) != 0 {
		gate := &rpcmeta.GateError{
			Method:       plan.Request.Method,
			Missing:      append([]rpcmeta.EffectClass(nil), plan.Missing...),
			Effects:      append([]rpcmeta.EffectClass(nil), plan.Decision.Effects...),
			Experimental: plan.Decision.Experimental,
		}
		details := map[string]any{
			"method":       plan.Request.Method,
			"effects":      effectsStrings(plan.Decision.Effects),
			"missing":      effectsStrings(plan.Missing),
			"experimental": plan.Decision.Experimental,
		}
		if len(plan.Decision.ExperimentalFields) != 0 {
			details["experimentalFields"] = append([]string(nil), plan.Decision.ExperimentalFields...)
		}
		return plan, newError(mektup.ErrEffectAcknowledgmentRequired, gate.Error(), mektup.StateNotSent, false, details, gate)
	}
	return plan, nil
}

// Execute gates before Caller.Call, then performs exactly one call. It never
// retries: an unknown or possibly mutating write remains unknown for the
// caller to reconcile explicitly.
func Execute(ctx context.Context, caller Caller, request Request) (*Response, error) {
	plan, err := Prepare(request)
	if err != nil {
		return nil, err
	}
	if caller == nil {
		return nil, newError(mektup.ErrInternal, "raw RPC caller is required", mektup.StateNotSent, false, nil, errors.New("nil caller"))
	}
	if plan.ExperimentalAPIRequired {
		capability, ok := caller.(ExperimentalCapability)
		if !ok || !capability.ExperimentalAPIEnabled() {
			return nil, newError(mektup.ErrExperimentalMethodUnavailable, "raw RPC requires an initialized experimental API capability", mektup.StateNotSent, false,
				map[string]any{"method": plan.Request.Method, "experimental": true, "experimentalFields": append([]string(nil), plan.Decision.ExperimentalFields...)}, errors.New("experimentalApi capability was not positively negotiated"))
		}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	id := plan.Request.ID
	if id == nil {
		id = fmt.Sprintf("mektup-rawrpc-%d", atomic.AddUint64(&nextID, 1))
	}
	result, callErr := caller.Call(ctx, appserver.RPCRequest{ID: id, Method: plan.Request.Method, Params: plan.Request.Params})
	if callErr != nil {
		return nil, classifyCallError(ctx, plan, callErr)
	}
	if result == nil {
		return nil, newError(mektup.ErrInternal, "raw RPC caller returned no result", mektup.StateOutcomeUnknown, false, nil, errors.New("nil RPC result"))
	}
	response := &Response{
		Method:                  plan.Request.Method,
		ID:                      result.ID,
		Effects:                 append([]rpcmeta.EffectClass(nil), plan.Decision.Effects...),
		Experimental:            plan.Decision.Experimental,
		ExperimentalFields:      append([]string(nil), plan.Decision.ExperimentalFields...),
		ExperimentalAPIRequired: plan.ExperimentalAPIRequired,
		NetworkRead:             plan.NetworkRead,
		RetrySafety:             plan.RetrySafety,
		WriteEvidence:           result.Evidence,
		EffectState:             mektup.StateAccepted,
	}
	if err := retainResult(ctx, response, result.Value, plan.Request.Output); err != nil {
		return response, err
	}
	return response, nil
}

var nextID uint64

func validateRequest(request *Request) error {
	if request == nil || strings.TrimSpace(request.Method) == "" {
		return errors.New("raw RPC method is required")
	}
	if !request.ParamsSource.valid() {
		return fmt.Errorf("raw RPC params source %q is invalid", request.ParamsSource)
	}
	trimmed := bytes.TrimSpace(request.Params)
	if len(trimmed) == 0 {
		if request.ParamsSource != ParamsOmitted {
			return errors.New("raw RPC params source is empty")
		}
		return nil
	}
	if !json.Valid(request.Params) {
		return errors.New("raw RPC params are not valid JSON")
	}
	if request.ParamsSource == ParamsOmitted {
		request.ParamsSource = ParamsInline
	}
	return nil
}

func missingEffects(effects, grants []rpcmeta.EffectClass) []rpcmeta.EffectClass {
	allowed := make(map[rpcmeta.EffectClass]struct{}, len(grants))
	for _, grant := range grants {
		allowed[grant] = struct{}{}
	}
	missing := make([]rpcmeta.EffectClass, 0, len(effects))
	for _, effect := range effects {
		if effect == rpcmeta.EffectRead || effect == rpcmeta.EffectNetworkRead {
			continue
		}
		if _, ok := allowed[effect]; !ok && !hasEffect(missing, effect) {
			missing = append(missing, effect)
		}
	}
	return missing
}

func classifyCallError(ctx context.Context, plan Plan, cause error) error {
	var callErr *appserver.CallError
	if !errors.As(cause, &callErr) || callErr == nil {
		return newError(mektup.ErrOutcomeUnknown, "raw RPC outcome is unknown", mektup.StateOutcomeUnknown, false,
			map[string]any{"method": plan.Request.Method, "effects": effectsStrings(plan.Decision.Effects)}, cause)
	}
	state := stateFor(callErr.Evidence, callErr.Server != nil)
	code := mektup.ErrOutcomeUnknown
	message := "raw RPC outcome is unknown"
	if callErr.Server != nil {
		code = mektup.ErrDeliveryRejected
		message = "app-server rejected raw RPC request"
	}
	if callErr.Evidence.Phase == appserver.WriteProvenBeforeWrite || callErr.Evidence.Phase == appserver.WriteNotStarted {
		if callErr.Server == nil {
			code = mektup.ErrEndpointUnavailable
			message = "raw RPC was not written"
		}
	}
	details := map[string]any{
		"method":       plan.Request.Method,
		"effects":      effectsStrings(plan.Decision.Effects),
		"writePhase":   callErr.Evidence.Phase.String(),
		"retrySafety":  string(plan.RetrySafety),
		"experimental": plan.ExperimentalAPIRequired,
	}
	var serverEvidence *ServerErrorEvidence
	if callErr.Server != nil {
		serverEvidence = &ServerErrorEvidence{ID: callErr.Server.ID, Code: callErr.Server.Code, Message: callErr.Server.Message, Generation: callErr.Server.Generation}
		retentionErr := retainServerError(ctx, serverEvidence, callErr.Server.Data, plan.Request.Output)
		details["serverError"] = serverEvidenceDetails(serverEvidence)
		if retentionErr != nil {
			details["serverErrorRetention"] = retentionErr.Error()
			return stableError(mektup.ErrOutputTooLarge, "app-server error data could not be retained completely", state, false, details, serverEvidence, cause)
		}
	}
	return stableError(code, message, state, false, details, serverEvidence, cause)
}

func stateFor(evidence appserver.WriteEvidence, serverError bool) mektup.EvidenceState {
	switch evidence.Phase {
	case appserver.WriteNotStarted, appserver.WriteProvenBeforeWrite:
		return mektup.StateNotSent
	case appserver.WriteComplete:
		if serverError {
			return mektup.StateRejected
		}
		return mektup.StateOutcomeUnknown
	case appserver.WriteMayHaveWritten:
		return mektup.StateOutcomeUnknown
	default:
		return mektup.StateOutcomeUnknown
	}
}

func retainResult(ctx context.Context, response *Response, raw json.RawMessage, options OutputOptions) error {
	// An explicit output path is an output request, not merely a spill
	// fallback. Publish the complete response even when it fits inline.
	if options.Path != "" {
		receipt, err := writeArtifact(ctx, raw, options, "raw-rpc-output.json")
		if err != nil {
			return newError(mektup.ErrOutputTooLarge, "raw RPC response could not be retained completely", response.EffectState, false,
				map[string]any{"bytes": len(raw), "outputPath": options.Path, "error": err.Error()}, err)
		}
		response.Artifact = &receipt
		return nil
	}
	limit := options.InlineLimit
	if limit == 0 {
		limit = DefaultInlineLimit
	}
	if limit < 1 {
		return newError(mektup.ErrOutputTooLarge, "raw RPC inline output limit must be positive", response.EffectState, false, nil, nil)
	}
	if int64(len(raw)) <= limit {
		response.Raw = append(json.RawMessage(nil), raw...)
		return nil
	}
	name := options.Name
	digest := sha256.Sum256(raw)
	if name == "" {
		name = "raw-rpc-" + hex.EncodeToString(digest[:]) + ".json"
	}
	spillOptions := options
	spillOptions.Name = name
	receipt, err := writeArtifact(ctx, raw, spillOptions, name)
	if err != nil {
		return newError(mektup.ErrOutputTooLarge, "raw RPC response could not be retained completely", response.EffectState, false,
			map[string]any{"bytes": len(raw), "inlineLimit": limit, "error": err.Error()}, err)
	}
	response.Artifact = &receipt
	return nil
}

func retainServerError(ctx context.Context, evidence *ServerErrorEvidence, raw json.RawMessage, options OutputOptions) error {
	messageDigest := sha256.Sum256([]byte(evidence.Message))
	evidence.MessageBytes = int64(len(evidence.Message))
	evidence.MessageSHA256 = "sha256:" + hex.EncodeToString(messageDigest[:])
	if len(raw) != 0 {
		dataDigest := sha256.Sum256(raw)
		evidence.DataBytes = int64(len(raw))
		evidence.DataSHA256 = "sha256:" + hex.EncodeToString(dataDigest[:])
	}

	limit := options.InlineLimit
	if limit == 0 {
		limit = DefaultInlineLimit
	}
	if options.Path == "" && limit < 1 {
		return fmt.Errorf("raw RPC server error inline output limit must be positive")
	}
	inlineBytes := int64(len(evidence.Message) + len(raw))
	if options.Path == "" && inlineBytes <= limit {
		evidence.Data = append(json.RawMessage(nil), raw...)
		return nil
	}

	// A message that does not fit inline cannot remain duplicated across the
	// stable details and nested server evidence. Preserve the complete error as
	// one sensitive artifact and leave only its digest/size metadata inline.
	if int64(len(evidence.Message)) > limit {
		document, err := json.Marshal(struct {
			ID      appserver.RequestID `json:"id,omitempty"`
			Code    int64               `json:"code"`
			Message string              `json:"message"`
			Data    json.RawMessage     `json:"data,omitempty"`
		}{ID: evidence.ID, Code: evidence.Code, Message: evidence.Message, Data: raw})
		if err != nil {
			return fmt.Errorf("encode raw RPC server error evidence: %w", err)
		}
		digest := sha256.Sum256(document)
		evidence.EvidenceBytes = int64(len(document))
		evidence.EvidenceSHA256 = "sha256:" + hex.EncodeToString(digest[:])
		spillOptions := options
		if spillOptions.Name == "" {
			spillOptions.Name = "raw-rpc-error-" + hex.EncodeToString(digest[:]) + ".json"
		}
		receipt, err := writeArtifact(ctx, document, spillOptions, spillOptions.Name)
		if err != nil {
			return err
		}
		evidence.Message = ""
		evidence.Data = nil
		evidence.EvidenceArtifact = &receipt
		return nil
	}

	// The message fits within the caller's inline budget, but the combined
	// evidence does not. Retain the exact data separately without weakening the
	// message bound.
	if len(raw) == 0 {
		return nil
	}
	dataDigest := sha256.Sum256(raw)
	spillOptions := options
	if spillOptions.Name == "" {
		spillOptions.Name = "raw-rpc-error-data-" + hex.EncodeToString(dataDigest[:]) + ".json"
	}
	receipt, err := writeArtifact(ctx, raw, spillOptions, spillOptions.Name)
	if err != nil {
		return err
	}
	evidence.DataArtifact = &receipt
	return nil
}

func writeArtifact(ctx context.Context, raw json.RawMessage, options OutputOptions, defaultName string) (artifact.Receipt, error) {
	artifactOptions := artifact.RPCOutputOptions{MaxBytes: options.MaxBytes, MediaType: "application/json", SensitiveOutputPossible: true, Force: options.Force}
	if options.Path != "" {
		if options.Store != nil {
			return options.Store.WriteRPCOutputPath(ctx, options.Path, bytes.NewReader(raw), artifactOptions)
		}
		return artifact.WriteRPCOutputPath(ctx, options.Path, bytes.NewReader(raw), artifactOptions)
	}
	name := options.Name
	if name == "" {
		name = defaultName
	}
	if options.Store == nil {
		return artifact.Receipt{}, errors.New("raw RPC response exceeds inline limit and no artifact destination was supplied")
	}
	return options.Store.Spill(ctx, name, bytes.NewReader(raw), artifact.Options{MaxBytes: options.MaxBytes, MediaType: "application/json", SensitiveOutputPossible: true})
}

func serverEvidenceDetails(evidence *ServerErrorEvidence) map[string]any {
	details := map[string]any{"id": evidence.ID, "code": evidence.Code, "messageBytes": evidence.MessageBytes, "messageSHA256": evidence.MessageSHA256}
	if evidence.Generation != 0 {
		details["generation"] = evidence.Generation
	}
	if evidence.DataBytes != 0 {
		details["dataBytes"] = evidence.DataBytes
		details["dataSHA256"] = evidence.DataSHA256
	}
	if evidence.DataArtifact != nil {
		details["dataArtifact"] = evidence.DataArtifact
	}
	if evidence.EvidenceBytes != 0 {
		details["evidenceBytes"] = evidence.EvidenceBytes
		details["evidenceSHA256"] = evidence.EvidenceSHA256
	}
	if evidence.EvidenceArtifact != nil {
		details["evidenceArtifact"] = evidence.EvidenceArtifact
	}
	return details
}

func newError(code mektup.ErrorCode, message string, state mektup.EvidenceState, retryable bool, details map[string]any, cause error) *Error {
	return stableError(code, message, state, retryable, details, nil, cause)
}

func stableError(code mektup.ErrorCode, message string, state mektup.EvidenceState, retryable bool, details map[string]any, server *ServerErrorEvidence, cause error) *Error {
	stable := mektup.Error{Code: code, Message: message, Retryable: retryable, EffectState: string(state), Details: details}
	return &Error{Code: code, Message: message, Retryable: retryable, EffectState: string(state), Details: details, Mektup: stable, Server: server, Cause: cause}
}

func hasEffect(effects []rpcmeta.EffectClass, wanted rpcmeta.EffectClass) bool {
	for _, effect := range effects {
		if effect == wanted {
			return true
		}
	}
	return false
}

func effectsStrings(effects []rpcmeta.EffectClass) []string {
	result := make([]string, len(effects))
	for i, effect := range effects {
		result[i] = string(effect)
	}
	return result
}
