// Package service contains Mektup's semantic messaging engine.
//
// The package deliberately depends on narrow ports.  It owns envelope
// construction, identity pinning, journal ordering, reply claims, and wait
// reconciliation, but not sockets, SSH, SQLite, or CLI presentation.
package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/internal/codexapi"
)

const (
	// MaxInputBytes and MaxInputChars are the conservative app-server text
	// input preflight bounds.  Both measurements are retained in errors.
	MaxInputBytes          = 1 << 20
	MaxInputChars          = 1 << 20
	maxNotSubmittedRetries = 1
)

var (
	ErrNoResolver = errors.New("mektup service: resolver port is required")
	ErrNoDelivery = errors.New("mektup service: delivery port is required")
	ErrNoJournal  = errors.New("mektup service: journal port is required")
)

// Error is the semantic error returned by the service.  Transport and server
// evidence remains available through Unwrap and Details; callers should use
// Code for stable handling rather than matching prose.
type Error struct {
	Code      mektup.ErrorCode
	Message   string
	Retryable bool
	Details   map[string]any
	Cause     error
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	if e.Message != "" {
		return string(e.Code) + ": " + e.Message
	}
	return string(e.Code)
}
func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

func semantic(code mektup.ErrorCode, message string, details map[string]any, cause error) *Error {
	return &Error{Code: code, Message: message, Details: details, Cause: cause}
}

// WritePhase is deliberately independent of appserver.Transport.  Adapters
// map concrete transport evidence to this small semantic vocabulary.
type WritePhase uint8

const (
	WriteNotStarted WritePhase = iota
	WriteProvenBeforeWrite
	WriteMayHaveWritten
	WriteComplete
)

// DeliveryError carries enough evidence for conservative operation mapping.
// A server error is conclusive rejection unless it is one of Codex's exact
// recognized NotSubmitted Review/Compact forms.
type DeliveryError struct {
	Err    error
	Server *codexapi.ServerError
	Phase  WritePhase
}

func (e *DeliveryError) Error() string {
	if e == nil {
		return ""
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	if e.Server != nil {
		return e.Server.Error()
	}
	return "delivery failed"
}
func (e *DeliveryError) Unwrap() error {
	if e == nil {
		return nil
	}
	if e.Err != nil {
		return e.Err
	}
	return e.Server
}

// ResolvedTarget is a target identity after resolution.  URI and ThreadID are
// pinned for the entire operation; retries never invoke a resolver again.
type ResolvedTarget struct {
	Requested  string
	EndpointID string
	URI        string
	ThreadID   string
	Loaded     bool
	Persistent bool
}

type SourceIdentity struct {
	EndpointID        string
	URI               string
	Human             bool
	Herdr             string
	CustodyEndpointID string
	CustodyStoreID    string
}

// Resolver resolves a destination and source independently. ResolvePinned is
// used for reply routes and must reject missing local endpoint mappings.
type Resolver interface {
	Resolve(context.Context, string) (ResolvedTarget, error)
	ResolveSource(context.Context, string) (SourceIdentity, error)
	ResolvePinned(context.Context, string, string) (ResolvedTarget, error)
}

type DeliveryResult struct {
	TurnID   string
	Accepted bool
	Evidence string
}

// DeliveryPort is the only body-write seam. Resume and Detach are separate
// from Send so unloaded persistent targets can be receipted independently.
type DeliveryPort interface {
	Send(context.Context, string, string, string) (DeliveryResult, error)
	Resume(context.Context, ResolvedTarget) (string, error)
	Detach(context.Context, ResolvedTarget) error
}

// ObservedItem contains only native observation metadata and the target text;
// it is never handed to JournalPort and is not persisted by this package.
type ObservedItem struct {
	ThreadID        string
	TurnID          string
	NativeItemID    string
	ClientMessageID string
	Text            string
}

type Event struct {
	Item   *ObservedItem
	Gap    bool
	Reason string
}

type EventStream interface {
	Next(context.Context) (Event, error)
	Close() error
}

type ObservationPort interface {
	Subscribe(context.Context, string) (EventStream, error)
	FullHistory(context.Context, string) ([]ObservedItem, error)
}

// Operation is metadata only. Body bytes remain in the native envelope and
// are intentionally absent here and from every journal-facing type.
type Operation struct {
	OperationID      string
	MessageID        string
	SourceRoute      string
	TargetRoute      string
	Semantics        string
	ReplyRoute       string
	CustodyRoute     string
	CustodyStoreID   string
	Digest           string
	BodySize         int64
	ReplyRequested   bool
	SourceEndpointID string
	TargetEndpointID string
}

type Prepared struct {
	OperationID string
	MessageID   string
	Owner       string
	Token       string
}

type OperationStatus struct {
	Operation
	State       mektup.EvidenceState
	TurnID      string
	ErrorCode   string
	ReplyID     string
	ReplyStatus string
}

type ReplyClaimInput struct {
	ReplyID        string
	OriginalID     string
	Digest         string
	BodySize       int64
	Status         string
	ReplyRoute     string
	CustodyRoute   string
	CustodyStoreID string
	Owner          string
}

type ReplyClaim struct {
	ReplyID        string
	OriginalID     string
	Digest         string
	BodySize       int64
	Status         string
	ReplyRoute     string
	CustodyRoute   string
	CustodyStoreID string
	Owner          string
	Token          string
	State          mektup.EvidenceState
	Joined         bool
	Won            bool
}

// JournalPort describes the explicit transition surface required by the
// semantic layer. Implementations may be SQLite, a remote custody receiver,
// or a deterministic fake. No transaction is held across a DeliveryPort call.
type JournalPort interface {
	Prepare(context.Context, Operation) (Prepared, error)
	MarkDispatchStarted(context.Context, string, string, string) error
	RecordResult(context.Context, string, mektup.EvidenceState, string) error
	Lookup(context.Context, string) (OperationStatus, error)
	ClaimReply(context.Context, ReplyClaimInput) (ReplyClaim, error)
	Heartbeat(context.Context, string, string, string) error
	CommitReply(context.Context, string, string, string) (ReplyClaim, error)
	AbandonReply(context.Context, string, string, string) error
	ObserveReply(context.Context, string, string, string) error
	ReconcileReplyObservation(context.Context, string, string, string) error
}

type Service struct {
	Resolver Resolver
	Delivery DeliveryPort
	Journal  JournalPort
	Observe  ObservationPort
	Now      func() time.Time
}

func (s *Service) validate() error {
	if s == nil || s.Resolver == nil {
		return ErrNoResolver
	}
	if s.Delivery == nil {
		return ErrNoDelivery
	}
	if s.Journal == nil {
		return ErrNoJournal
	}
	return nil
}

type SendRequest struct {
	Target          string
	Body            string
	Raw             bool
	RequestReply    bool
	Wait            bool
	Source          string
	DeliveryTimeout time.Duration
	WaitTimeout     time.Duration
}

type SendResult struct {
	Receipt mektup.Receipt
	Wait    *WaitResult
}

func (s *Service) Send(ctx context.Context, req SendRequest) (SendResult, error) {
	if err := s.validate(); err != nil {
		return SendResult{}, err
	}
	if strings.TrimSpace(req.Target) == "" || req.Body == "" {
		return SendResult{}, semantic(mektup.ErrInvalidArguments, "target and non-empty body are required", nil, nil)
	}
	if !utf8.ValidString(req.Body) {
		return SendResult{}, semantic(mektup.ErrInvalidArguments, "body is not valid UTF-8", nil, nil)
	}
	if req.Wait {
		req.RequestReply = true
	}
	if req.Raw && req.Wait {
		return SendResult{}, semantic(mektup.ErrInvalidRawWait, "raw delivery cannot wait for a correlated reply", nil, nil)
	}
	if req.Raw && req.RequestReply {
		return SendResult{}, semantic(mektup.ErrInvalidRawReplyRequest, "raw delivery cannot request a correlated reply", nil, nil)
	}

	target, err := s.Resolver.Resolve(ctx, req.Target)
	if err != nil {
		return SendResult{}, semantic(mektup.ErrInvalidTarget, "destination resolution failed", nil, err)
	}
	source, err := s.Resolver.ResolveSource(ctx, req.Source)
	if err != nil {
		if req.RequestReply {
			return SendResult{}, semantic(mektup.ErrReplyRouteRequired, "a routable source is required for request-reply", nil, err)
		}
		// Human one-way is explicitly allowed without a source thread.
		humanEndpoint, idErr := mektup.NewEndpointIDChecked()
		if idErr != nil {
			return SendResult{}, semantic(mektup.ErrInternal, "cannot allocate human source identity", nil, idErr)
		}
		source = SourceIdentity{EndpointID: humanEndpoint, Human: true}
	}
	if req.RequestReply && (source.URI == "" || source.EndpointID == "" || source.CustodyEndpointID == "" || source.CustodyStoreID == "") {
		return SendResult{}, semantic(mektup.ErrReplyRouteRequired, "reply source and custody identities are incomplete", nil, nil)
	}

	messageID, err := mektup.NewMessageIDChecked()
	if err != nil {
		return SendResult{}, semantic(mektup.ErrInternal, "cannot allocate message identity", nil, err)
	}
	envelope := mektup.Envelope{MessageID: messageID, Kind: mektup.KindMessage,
		FromEndpointID: source.EndpointID, From: source.URI, FromKind: kindOf(source), FromHerdr: source.Herdr,
		ToEndpointID: target.EndpointID, To: target.URI, RequestedTarget: req.Target,
		ReplyRequested: req.RequestReply, Body: req.Body, Provenance: "observed"}
	if req.RequestReply {
		envelope.ReplyEndpointID = source.EndpointID
		envelope.ReplyTo = source.URI
		envelope.ReplyCustodyEndpointID = source.CustodyEndpointID
		envelope.ReplyCustodyStoreID = source.CustodyStoreID
	}
	envelope.PayloadBytes = uint64(len([]byte(req.Body)))
	envelope.PayloadSHA256 = digest(req.Body)
	envelope.SentAt = s.now().UTC().Format(time.RFC3339Nano)
	var payload []byte
	if req.Raw {
		payload = []byte(req.Body)
	} else {
		payload, err = mektup.RenderEnvelope(envelope)
		if err != nil {
			return SendResult{}, semantic(mektup.ErrInvalidArguments, "cannot render message envelope", nil, err)
		}
	}
	if err := preflight(payload, req.Body); err != nil {
		return SendResult{}, err
	}

	opID, err := mektup.NewOperationIDChecked()
	if err != nil {
		return SendResult{}, semantic(mektup.ErrInternal, "cannot allocate operation identity", nil, err)
	}
	digest := digest(req.Body)
	sourceRoute := source.URI
	if sourceRoute == "" {
		sourceRoute = "human://" + source.EndpointID
	}
	op := Operation{OperationID: opID, MessageID: messageID, SourceRoute: sourceRoute, TargetRoute: target.URI,
		Semantics: semantics(req.Raw), ReplyRoute: envelope.ReplyTo, CustodyRoute: envelope.ReplyCustodyEndpointID,
		CustodyStoreID: envelope.ReplyCustodyStoreID, Digest: digest, BodySize: int64(len([]byte(req.Body))),
		ReplyRequested: req.RequestReply, SourceEndpointID: source.EndpointID, TargetEndpointID: target.EndpointID}
	prepared, err := s.Journal.Prepare(ctx, op)
	if err != nil {
		return SendResult{}, semantic(mektup.ErrMessageIdentityConflict, "message relationship could not be prepared", nil, err)
	}

	if !target.Loaded && target.Persistent {
		if _, err := s.Delivery.Resume(ctx, target); err != nil {
			_ = s.Journal.RecordResult(ctx, opID, mektup.StateNotSent, "resume_failed")
			return SendResult{}, semantic(mektup.ErrDeliveryRejected, "persistent target could not be resumed", nil, err)
		}
	}
	if err := s.Journal.MarkDispatchStarted(ctx, prepared.OperationID, prepared.Owner, prepared.Token); err != nil {
		return SendResult{}, semantic(mektup.ErrInternal, "dispatch fence could not be committed", nil, err)
	}

	deliveryCtx := ctx
	var cancel context.CancelFunc
	if req.DeliveryTimeout > 0 {
		deliveryCtx, cancel = context.WithTimeout(ctx, req.DeliveryTimeout)
		defer cancel()
	}
	result, dispatchErr := s.dispatch(deliveryCtx, target, string(payload), messageID)
	if dispatchErr != nil {
		state, code, retry := classifyDelivery(dispatchErr)
		if retry {
			for i := 0; i < maxNotSubmittedRetries; i++ {
				result, dispatchErr = s.dispatch(deliveryCtx, target, string(payload), messageID)
				if dispatchErr == nil {
					break
				}
				state, code, _ = classifyDelivery(dispatchErr)
			}
		}
		if dispatchErr != nil {
			_ = s.Journal.RecordResult(ctx, opID, state, code)
			return SendResult{}, deliveryError(dispatchErr, state, code)
		}
	}
	if err := s.Journal.RecordResult(ctx, opID, mektup.StateAccepted, result.Evidence); err != nil {
		return SendResult{}, semantic(mektup.ErrInternal, "acceptance evidence could not be journaled", nil, err)
	}
	if !target.Loaded && target.Persistent {
		_ = s.Delivery.Detach(context.Background(), target)
	}
	receipt := receiptFor(op, envelope, mektup.StateAccepted, result.TurnID)
	out := SendResult{Receipt: receipt}
	if req.Wait {
		wait, waitErr := s.Wait(ctx, WaitRequest{Reference: messageID, Timeout: req.WaitTimeout})
		out.Wait = &wait
		if waitErr != nil {
			return out, waitErr
		}
	}
	return out, nil
}

func (s *Service) dispatch(ctx context.Context, target ResolvedTarget, text, messageID string) (DeliveryResult, *DeliveryError) {
	result, err := s.Delivery.Send(ctx, target.ThreadID, text, messageID)
	if err == nil {
		if !result.Accepted {
			return result, &DeliveryError{Err: errors.New("delivery did not report acceptance"), Phase: WriteComplete}
		}
		return result, nil
	}
	var de *DeliveryError
	if errors.As(err, &de) {
		return result, de
	}
	return result, &DeliveryError{Err: err, Phase: WriteMayHaveWritten}
}

func (s *Service) now() time.Time {
	if s != nil && s.Now != nil {
		return s.Now()
	}
	return time.Now()
}

func classifyDelivery(err *DeliveryError) (mektup.EvidenceState, string, bool) {
	if err == nil {
		return mektup.StateAccepted, "", false
	}
	if err.Server != nil {
		if c := codexapi.ClassifyTurnStartNotSubmitted(err.Server); c.Retry {
			return mektup.StateRejected, "not_submitted", true
		}
		return mektup.StateRejected, fmt.Sprintf("server_%d", err.Server.Code), false
	}
	if err.Phase <= WriteProvenBeforeWrite {
		return mektup.StateNotSent, "not_sent", false
	}
	return mektup.StateOutcomeUnknown, "outcome_unknown", false
}

func deliveryError(err *DeliveryError, state mektup.EvidenceState, code string) error {
	if state == mektup.StateRejected {
		return semantic(mektup.ErrDeliveryRejected, "destination rejected the message", map[string]any{"evidenceState": state, "errorCode": code}, err)
	}
	if state == mektup.StateNotSent {
		return semantic(mektup.ErrDeliveryTemporarilyUnavailable, "message was not written", map[string]any{"evidenceState": state}, err)
	}
	return semantic(mektup.ErrOutcomeUnknown, "message outcome is unknown; it will not be replayed automatically", map[string]any{"evidenceState": state}, err)
}

func kindOf(source SourceIdentity) string {
	if source.Human || source.URI == "" {
		return "human"
	}
	return "agent"
}
func semantics(raw bool) string {
	if raw {
		return "raw"
	}
	return "message"
}
func digest(body string) string {
	sum := sha256.Sum256([]byte(body))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func receiptFor(op Operation, e mektup.Envelope, state mektup.EvidenceState, turnID string) mektup.Receipt {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return mektup.Receipt{Schema: mektup.ReceiptSchema, ReceiptID: mektup.NewReceiptID(), OperationID: op.OperationID, Operation: op.Semantics, State: state,
		Source: mektup.ReceiptIdentity{EndpointID: op.SourceEndpointID, ThreadID: threadID(e.From)}, Target: mektup.ReceiptIdentity{EndpointID: op.TargetEndpointID, ThreadID: threadID(e.To), Requested: e.RequestedTarget, Resolved: e.To},
		Message: mektup.ReceiptMessage{MessageID: e.MessageID, Kind: string(e.Kind), ReplyRequested: e.ReplyRequested, PayloadBytes: e.PayloadBytes, PayloadSHA256: e.PayloadSHA256, ClientMessageID: e.MessageID, TurnID: turnID, AcceptedAt: now}, Evidence: []mektup.EvidenceRecord{{State: state, At: now, Reference: turnID}}, CreatedAt: now, UpdatedAt: now}
}

func threadID(uri string) string {
	if a, err := mektup.ParseThreadURI(uri); err == nil {
		return a.ThreadID
	}
	return ""
}
