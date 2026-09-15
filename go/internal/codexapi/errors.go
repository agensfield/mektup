package codexapi

import (
	"bytes"
	"encoding/json"
)

// NotSubmittedKind is deliberately tiny. Codex uses -32603 for many
// unrelated failures; only these two pinned turn/start shapes prove that the
// input was not admitted and may be retried after reconciliation.
type NotSubmittedKind string

const (
	NotSubmittedNone    NotSubmittedKind = ""
	NotSubmittedReview  NotSubmittedKind = "review"
	NotSubmittedCompact NotSubmittedKind = "compact"
)

type NotSubmittedClassification struct {
	Kind     NotSubmittedKind
	Retry    bool
	Evidence json.RawMessage
}

func (c NotSubmittedClassification) Recognized() bool { return c.Kind != NotSubmittedNone }

// ClassifyNotSubmitted accepts only the exact pinned -32603 message shape.
// The actual 0.154.0 producer omits error data; if a future server attaches
// data, it must match the nested shape before it can strengthen this evidence.
// Generic internal errors remain unknown.
func ClassifyNotSubmitted(err *ServerError) NotSubmittedClassification {
	result := NotSubmittedClassification{}
	if err == nil || err.Code != -32603 {
		return result
	}
	var kind NotSubmittedKind
	switch err.Message {
	case "failed to submit turn input: ActiveTurnNotSteerable { turn_kind: Review }":
		kind = NotSubmittedReview
	case "failed to submit turn input: ActiveTurnNotSteerable { turn_kind: Compact }":
		kind = NotSubmittedCompact
	default:
		return result
	}
	// Future-only nested data must not weaken the actual omitted-data contract.
	if len(bytes.TrimSpace(err.Data)) != 0 {
		data, ok := objectForClassification(err.Data)
		if !ok {
			return result
		}
		info, ok := objectForClassification(data["codexErrorInfo"])
		if !ok {
			return result
		}
		notSteerable, ok := objectForClassification(info["activeTurnNotSteerable"])
		if !ok {
			return result
		}
		var turnKind string
		if json.Unmarshal(notSteerable["turnKind"], &turnKind) != nil || (kind == NotSubmittedReview && turnKind != "review") || (kind == NotSubmittedCompact && turnKind != "compact") {
			return result
		}
	}
	result.Kind, result.Retry = kind, true
	result.Evidence = append(json.RawMessage(nil), err.Raw...)
	return result
}

func objectForClassification(raw json.RawMessage) (map[string]json.RawMessage, bool) {
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, false
	}
	var m map[string]json.RawMessage
	if json.Unmarshal(raw, &m) != nil || m == nil {
		return nil, false
	}
	return m, true
}

func IsRetryableNotSubmitted(err *ServerError) bool { return ClassifyNotSubmitted(err).Retry }

func ClassifyTurnStartNotSubmitted(err *ServerError) NotSubmittedClassification {
	return ClassifyNotSubmitted(err)
}

func RetryableNotSubmitted(err *ServerError) bool { return IsRetryableNotSubmitted(err) }
