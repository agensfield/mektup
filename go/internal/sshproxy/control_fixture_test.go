package sshproxy

import (
	"encoding/json"
	"errors"
	"testing"
)

// These are the control-v1 fixtures from contracts/fixtures/v1. They stay
// inline here because this transport worktree intentionally does not vendor
// the integration branch's full contracts tree.
func TestControlV1Fixtures(t *testing.T) {
	claim := validControlRequest()
	claim.RequestedLease = &LeaseRequest{DurationMS: 30000}
	claim.RequestedAt = "2026-09-15T03:00:01.800000Z"

	commit := claim
	commit.Operation = "commit"
	commit.FencingToken = "fence-01"
	commit.Lease = &Lease{ExpiresAt: "2026-09-15T03:00:31.900000Z", HeartbeatAt: "2026-09-15T03:00:02.000000Z"}
	commit.RequestedLease = nil
	status := claim
	status.Operation = "status"
	status.RequestedLease = nil

	for _, fixture := range []struct {
		name string
		data []byte
		ok   bool
	}{
		{name: "control-claim", data: mustJSON(t, claim), ok: true},
		{name: "control-commit", data: mustJSON(t, commit), ok: true},
		{name: "control-status-without-receipt", data: mustJSON(t, status), ok: true},
		{name: "control-claim-with-token.invalid", data: mustJSON(t, func() ControlRequest { r := claim; r.FencingToken = "caller-selected-token"; return r }()), ok: false},
		{name: "control-heartbeat-without-lease.invalid", data: mustJSON(t, func() ControlRequest { r := commit; r.Operation = "heartbeat"; r.Lease = nil; return r }()), ok: false},
		{name: "control-commit-without-token.invalid", data: mustJSON(t, func() ControlRequest { r := commit; r.FencingToken = ""; return r }()), ok: false},
		{name: "control-abandon-without-token.invalid", data: mustJSON(t, func() ControlRequest { r := commit; r.Operation = "abandon"; r.FencingToken = ""; return r }()), ok: false},
	} {
		t.Run(fixture.name, func(t *testing.T) {
			_, err := ValidateControlRequest(fixture.data)
			if fixture.ok && err != nil {
				t.Fatalf("fixture rejected: %v", err)
			}
			if !fixture.ok && !errors.Is(err, ErrControlValidation) {
				t.Fatalf("fixture error = %v", err)
			}
		})
	}

	result := []byte(`{"schema":"mektup/control/v1","kind":"result","operation":"claim","operationId":"op_0198f0e0-0000-7000-8000-00000000000c","replyMessageId":"msg_0198f0e0-0000-7000-8000-000000000007","originalMessageId":"msg_0198f0e0-0000-7000-8000-000000000003","custody":{"endpointId":"ep_0198f0e0-0000-7000-8000-000000000001","storeId":"store_0198f0e0-0000-7000-8000-000000000002"},"replyDestination":{"endpointId":"ep_0198f0e0-0000-7000-8000-000000000001","threadId":"thread-local-001","uri":"codex://local/thread/thread-local-001"},"result":{"state":"reply_dispatch_claimed","fencingToken":"fence-01","lease":{"acquiredAt":"2026-09-15T03:00:01.900000Z","expiresAt":"2026-09-15T03:00:31.900000Z"}}}`)
	parsed, err := ValidateControlRequest(result)
	if err != nil {
		t.Fatalf("claim result rejected: %v", err)
	}
	if string(parsed.Result) == "" {
		t.Fatal("claim result was silently dropped")
	}
}

func mustJSON(t *testing.T, value ControlRequest) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
