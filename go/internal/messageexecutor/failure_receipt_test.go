package messageexecutor

import (
	"testing"

	mektup "github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/internal/cli"
	"github.com/agensfield/mektup/go/internal/service"
)

func TestFailureReceiptRetainsIdentityAndEvidence(t *testing.T) {
	for _, tc := range []struct {
		name    string
		state   mektup.EvidenceState
		code    mektup.ErrorCode
		details map[string]any
		exit    cli.ExitCode
		effect  string
	}{
		{"route", mektup.StateNotSent, mektup.ErrReplyRouteUnavailable, nil, cli.ExitRejected, "not_sent"},
		{"identity", mektup.StateNotSent, mektup.ErrMessageIdentityConflict, nil, cli.ExitRejected, "not_sent"},
		{"claim unknown", mektup.StateNotSent, mektup.ErrReplyOutcomeUnknown, map[string]any{"claimState": "outcome_unknown", "bodySent": false}, cli.ExitUnknown, "unknown"},
		{"body unknown", mektup.StateOutcomeUnknown, mektup.ErrOutcomeUnknown, nil, cli.ExitUnknown, "outcome_unknown"},
		{"accepted cleanup", mektup.StateAccepted, mektup.ErrInternal, nil, cli.ExitInternal, "accepted"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			receipt := testReceipt(tc.state)
			result, err := New(Ports{}).messagingResult("reply", receipt, nil, &service.Error{Code: tc.code, Message: tc.name, Details: tc.details})
			if err != nil || result.Exit != tc.exit || len(result.Events) != 1 {
				t.Fatalf("result=%+v err=%v", result, err)
			}
			event := result.Events[0].Machine.(map[string]any)
			data := event["data"].(map[string]any)
			got := data["receipt"].(mektup.Receipt)
			failure := data["error"].(map[string]any)
			if got.ReceiptID != receipt.ReceiptID || event["operationId"] != receipt.OperationID || got.Message.MessageID != receipt.Message.MessageID {
				t.Fatalf("lost durable identity: %+v", event)
			}
			if got.State != tc.state || failure["effectState"] != tc.effect || failure["retryable"] != false {
				t.Fatalf("wrong evidence or retry authority: %+v", event)
			}
		})
	}
}
