package mektup

import "fmt"

// EvidenceState is a durable, monotonic delivery fact.
type EvidenceState string

const (
	StateNotSent              EvidenceState = "not_sent"
	StatePrepared             EvidenceState = "prepared"
	StateDispatchStarted      EvidenceState = "dispatch_started"
	StateReplyDispatchClaimed EvidenceState = "reply_dispatch_claimed"
	StateReplyOutcomeUnknown  EvidenceState = "reply_outcome_unknown"
	StateRejected             EvidenceState = "rejected"
	StateAccepted             EvidenceState = "accepted"
	StateOutcomeUnknown       EvidenceState = "outcome_unknown"
	StateReplyAccepted        EvidenceState = "reply_accepted"
	StateReplyObserved        EvidenceState = "reply_observed"
	StateManuallyResolved     EvidenceState = "manually_resolved"
)

// Short aliases make state switches readable to callers.
const (
	NotSent              = StateNotSent
	Prepared             = StatePrepared
	DispatchStarted      = StateDispatchStarted
	ReplyDispatchClaimed = StateReplyDispatchClaimed
	ReplyOutcomeUnknown  = StateReplyOutcomeUnknown
	Rejected             = StateRejected
	Accepted             = StateAccepted
	OutcomeUnknown       = StateOutcomeUnknown
	ReplyAccepted        = StateReplyAccepted
	ReplyObserved        = StateReplyObserved
	ManuallyResolved     = StateManuallyResolved
)

func (s EvidenceState) Valid() bool {
	switch s {
	case StateNotSent, StatePrepared, StateDispatchStarted, StateReplyDispatchClaimed,
		StateReplyOutcomeUnknown, StateRejected, StateAccepted, StateOutcomeUnknown,
		StateReplyAccepted, StateReplyObserved, StateManuallyResolved:
		return true
	default:
		return false
	}
}

// ErrorCode is the stable Mektup error authority. App-server errors belong in
// nested evidence and must not replace this code.
type ErrorCode string

const (
	ErrInvalidArguments               ErrorCode = "invalid_arguments"
	ErrInvalidTarget                  ErrorCode = "invalid_target"
	ErrResolverUnavailable            ErrorCode = "resolver_unavailable"
	ErrTargetAmbiguous                ErrorCode = "target_ambiguous"
	ErrRouteUnavailable               ErrorCode = "route_unavailable"
	ErrEndpointUnavailable            ErrorCode = "endpoint_unavailable"
	ErrUnsupportedServerVersion       ErrorCode = "unsupported_server_version"
	ErrInputTooLarge                  ErrorCode = "input_too_large"
	ErrInvalidUTF8                    ErrorCode = "invalid_utf8"
	ErrMessageNotFound                ErrorCode = "message_not_found"
	ErrMessageIdentityConflict        ErrorCode = "message_identity_conflict"
	ErrReplyRouteRequired             ErrorCode = "reply_route_required"
	ErrReplyRouteUnavailable          ErrorCode = "reply_route_unavailable"
	ErrReplyNotRequested              ErrorCode = "reply_not_requested"
	ErrInvalidRawWait                 ErrorCode = "invalid_raw_wait"
	ErrInvalidRawReplyRequest         ErrorCode = "invalid_raw_reply_request"
	ErrReplyOutcomeUnknown            ErrorCode = "reply_outcome_unknown"
	ErrMessageNotAddressedThread      ErrorCode = "message_not_addressed_to_thread"
	ErrContentUnavailable             ErrorCode = "content_unavailable"
	ErrDeliveryRejected               ErrorCode = "delivery_rejected"
	ErrDeliveryTemporarilyUnavailable ErrorCode = "delivery_temporarily_unavailable"
	ErrOutcomeUnknown                 ErrorCode = "outcome_unknown"
	ErrWaitIncomplete                 ErrorCode = "wait_incomplete"
	ErrWaitInterrupted                ErrorCode = "wait_interrupted"
	ErrEffectAcknowledgmentRequired   ErrorCode = "effect_acknowledgment_required"
	ErrExperimentalMethodUnavailable  ErrorCode = "experimental_method_unavailable"
	ErrOutputTooLarge                 ErrorCode = "output_too_large"
	ErrCompactOutputTooLarge          ErrorCode = "compact_output_too_large"
	ErrStorageBusy                    ErrorCode = "storage_busy"
	ErrStorageCorrupt                 ErrorCode = "storage_corrupt"
	ErrStorageMigrationRequired       ErrorCode = "storage_migration_required"
	ErrRepairRequired                 ErrorCode = "repair_required"
	ErrInternal                       ErrorCode = "internal_error"
)

const (
	ErrorInvalidArguments = ErrInvalidArguments
	ErrorOutcomeUnknown   = ErrOutcomeUnknown
)

func (c ErrorCode) Valid() bool {
	switch c {
	case ErrInvalidArguments, ErrInvalidTarget, ErrResolverUnavailable, ErrTargetAmbiguous,
		ErrRouteUnavailable, ErrEndpointUnavailable, ErrUnsupportedServerVersion, ErrInputTooLarge, ErrInvalidUTF8,
		ErrMessageNotFound, ErrMessageIdentityConflict, ErrReplyRouteRequired, ErrReplyRouteUnavailable,
		ErrReplyNotRequested, ErrInvalidRawWait, ErrInvalidRawReplyRequest, ErrReplyOutcomeUnknown,
		ErrMessageNotAddressedThread, ErrContentUnavailable, ErrDeliveryRejected,
		ErrDeliveryTemporarilyUnavailable, ErrOutcomeUnknown, ErrWaitIncomplete, ErrWaitInterrupted,
		ErrEffectAcknowledgmentRequired, ErrExperimentalMethodUnavailable, ErrOutputTooLarge, ErrCompactOutputTooLarge,
		ErrStorageBusy, ErrStorageCorrupt, ErrStorageMigrationRequired, ErrRepairRequired, ErrInternal:
		return true
	default:
		return false
	}
}

// Exit classes are intentionally small and stable across CLI implementations.
const (
	ExitSuccess    = 0
	ExitInternal   = 1
	ExitUsage      = 2
	ExitRejected   = 3
	ExitUnknown    = 4
	ExitIncomplete = 5
)

const (
	ExitClassSuccess    = ExitSuccess
	ExitClassInternal   = ExitInternal
	ExitClassUsage      = ExitUsage
	ExitClassRejected   = ExitRejected
	ExitClassUnknown    = ExitUnknown
	ExitClassIncomplete = ExitIncomplete
)

// WarningCode identifies machine-actionable warning categories.
type WarningCode string

const (
	WarningUntestedServerVersion WarningCode = "untested_server_version"
	WarningServerVersionUnknown  WarningCode = "server_version_unknown"
	WarningEvidenceGap           WarningCode = "evidence_gap"
	WarningProjectionMayLag      WarningCode = "projection_may_lag"
	WarningAuditLoggingEnabled   WarningCode = "audit_logging_enabled"
	WarningOutputSpilled         WarningCode = "output_spilled"
	WarningResolverDegraded      WarningCode = "resolver_degraded"
	WarningCleanupIncomplete     WarningCode = "cleanup_incomplete"
	WarningManualResolution      WarningCode = "manual_resolution"
)

// Error is a terminal machine-readable failure payload.
type Error struct {
	Code        ErrorCode      `json:"code"`
	Message     string         `json:"message"`
	Retryable   bool           `json:"retryable"`
	EffectState string         `json:"effectState"`
	Details     map[string]any `json:"details,omitempty"`
}

func (e Error) Validate() error {
	if !e.Code.Valid() {
		return fmt.Errorf("mektup: unknown error code %q", e.Code)
	}
	if e.Message == "" {
		return fmt.Errorf("mektup: error message is required")
	}
	if e.EffectState == "" {
		return fmt.Errorf("mektup: error effectState is required")
	}
	return nil
}

type Warning struct {
	Code    WarningCode    `json:"code"`
	Message string         `json:"message"`
	Details map[string]any `json:"details,omitempty"`
}

func (w Warning) Validate() error {
	if w.Code == "" || w.Message == "" {
		return fmt.Errorf("mektup: warning code and message are required")
	}
	return nil
}
