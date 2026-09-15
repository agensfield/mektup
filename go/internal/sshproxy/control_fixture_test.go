package sshproxy

import (
	"bytes"
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

	result := []byte(`{"schema":"mektup/control/v1","kind":"result","operation":"claim","operationId":"op_0198f0e0-0000-7000-8000-00000000000c","replyMessageId":"msg_0198f0e0-0000-7000-8000-000000000007","originalMessageId":"msg_0198f0e0-0000-7000-8000-000000000003","custody":{"endpointId":"ep_0198f0e0-0000-7000-8000-000000000001","storeId":"store_0198f0e0-0000-7000-8000-000000000002"},"replyDestination":{"endpointId":"ep_0198f0e0-0000-7000-8000-000000000001","threadId":"thread-local-001","uri":"codex://local/thread/thread-local-001"},"result":{"disposition":"claimed","state":"reply_dispatch_claimed","fencingToken":"fence-01","lease":{"acquiredAt":"2026-09-15T03:00:01.900000Z","expiresAt":"2026-09-15T03:00:31.900000Z"}}}`)
	parsed, err := ValidateControlRequest(result)
	if err != nil {
		t.Fatalf("claim result rejected: %v", err)
	}
	if string(parsed.Result) == "" {
		t.Fatal("claim result was silently dropped")
	}
}

func TestControlClaimExistingResultIsTokenless(t *testing.T) {
	result := []byte(`{"schema":"mektup/control/v1","kind":"result","operation":"claim","operationId":"op_0198f0e0-0000-7000-8000-000000000013","replyMessageId":"msg_0198f0e0-0000-7000-8000-000000000007","originalMessageId":"msg_0198f0e0-0000-7000-8000-000000000003","custody":{"endpointId":"ep_0198f0e0-0000-7000-8000-000000000001","storeId":"store_0198f0e0-0000-7000-8000-000000000002"},"replyDestination":{"endpointId":"ep_0198f0e0-0000-7000-8000-000000000001","threadId":"thread-local-001"},"result":{"disposition":"existing","state":"reply_accepted","status":"accepted","winner":{"replyMessageId":"msg_0198f0e0-0000-7000-8000-000000000007"}}}`)
	if _, err := ValidateControlRequest(result); err != nil {
		t.Fatalf("existing claim result rejected: %v", err)
	}
	withToken := []byte(`{"schema":"mektup/control/v1","kind":"result","operation":"claim","operationId":"op_0198f0e0-0000-7000-8000-000000000013","replyMessageId":"msg_0198f0e0-0000-7000-8000-000000000007","originalMessageId":"msg_0198f0e0-0000-7000-8000-000000000003","custody":{"endpointId":"ep_0198f0e0-0000-7000-8000-000000000001","storeId":"store_0198f0e0-0000-7000-8000-000000000002"},"replyDestination":{"endpointId":"ep_0198f0e0-0000-7000-8000-000000000001","threadId":"thread-local-001"},"result":{"disposition":"existing","state":"reply_accepted","fencingToken":"owner-token"}}`)
	if _, err := ValidateControlRequest(withToken); !errors.Is(err, ErrControlValidation) {
		t.Fatalf("existing claim result with token accepted: %v", err)
	}
	for name, tampered := range map[string][]byte{
		"null fencingToken": bytes.Replace(result, []byte(`"result":{"disposition"`), []byte(`"result":{"fencingToken":null,"disposition"`), 1),
		"null lease":        bytes.Replace(result, []byte(`"result":{"disposition"`), []byte(`"result":{"lease":null,"disposition"`), 1),
		"null status":       bytes.Replace(result, []byte(`"status":"accepted"`), []byte(`"status":null`), 1),
		"null winner":       bytes.Replace(result, []byte(`"winner":{"replyMessageId":"msg_0198f0e0-0000-7000-8000-000000000007"}`), []byte(`"winner":null`), 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ValidateControlRequest(tampered); !errors.Is(err, ErrControlValidation) {
				t.Fatalf("existing claim result with explicit null accepted: %v", err)
			}
		})
	}
}

func TestControlClaimResultLeaseTimestampsAreStrict(t *testing.T) {
	claimed := []byte(`{"schema":"mektup/control/v1","kind":"result","operation":"claim","operationId":"op_0198f0e0-0000-7000-8000-00000000000c","replyMessageId":"msg_0198f0e0-0000-7000-8000-000000000007","originalMessageId":"msg_0198f0e0-0000-7000-8000-000000000003","custody":{"endpointId":"ep_0198f0e0-0000-7000-8000-000000000001","storeId":"store_0198f0e0-0000-7000-8000-000000000002"},"replyDestination":{"endpointId":"ep_0198f0e0-0000-7000-8000-000000000001","threadId":"thread-local-001"},"result":{"disposition":"claimed","state":"reply_dispatch_claimed","fencingToken":"fence-01","lease":{"acquiredAt":"2026-09-15T03:00:01.900000Z","expiresAt":"2026-09-15T03:00:31.900000Z"}}}`)
	for name, tampered := range map[string][]byte{
		"invalid expiry":  bytes.Replace(claimed, []byte(`2026-09-15T03:00:31.900000Z`), []byte(`not-a-timestamp`), 1),
		"null acquiredAt": bytes.Replace(claimed, []byte(`"acquiredAt":"2026-09-15T03:00:01.900000Z"`), []byte(`"acquiredAt":null`), 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := ValidateControlRequest(tampered); !errors.Is(err, ErrControlValidation) {
				t.Fatalf("invalid claim lease timestamp accepted: %v", err)
			}
		})
	}
}

func TestControlKnownFieldPresenceAndResultShapes(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(map[string]any)
	}{
		{name: "claim-fencing-token-null", mutate: func(raw map[string]any) { raw["fencingToken"] = nil }},
		{name: "commit-requested-lease-null", mutate: func(raw map[string]any) {
			raw["operation"] = "commit"
			raw["fencingToken"] = "fence"
			raw["lease"] = map[string]any{"expiresAt": "2026-09-15T03:00:31.900000Z"}
			raw["requestedLease"] = nil
		}},
		{name: "result-bodySha256-invalid", mutate: func(raw map[string]any) {
			raw["kind"] = "result"
			raw["bodySha256"] = "invalid"
			raw["result"] = map[string]any{"disposition": "claimed", "state": "reply_dispatch_claimed", "fencingToken": "fence", "lease": map[string]any{"expiresAt": "2026-09-15T03:00:31.900000Z"}}
		}},
		{name: "result-replyStatus-invalid", mutate: func(raw map[string]any) {
			raw["kind"] = "result"
			raw["replyStatus"] = "invalid"
			raw["result"] = map[string]any{"disposition": "claimed", "state": "reply_dispatch_claimed", "fencingToken": "fence", "lease": map[string]any{"expiresAt": "2026-09-15T03:00:31.900000Z"}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			rawBytes, err := json.Marshal(validControlRequest())
			if err != nil {
				t.Fatal(err)
			}
			var raw map[string]any
			if err := json.Unmarshal(rawBytes, &raw); err != nil {
				t.Fatal(err)
			}
			test.mutate(raw)
			rawBytes, err = json.Marshal(raw)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ValidateControlRequest(rawBytes); !errors.Is(err, ErrControlValidation) {
				t.Fatalf("accepted known invalid shape: %v", err)
			}
		})
	}
}

func TestURIWhitespaceMatchesSchemaWhitespaceClass(t *testing.T) {
	for _, whitespace := range []string{"\f", "\v"} {
		t.Run(whitespace, func(t *testing.T) {
			request := validControlRequest()
			request.ReplyDestination.URI = "codex://local/thread/with" + whitespace + "space"
			data, err := json.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ValidateControlRequest(data); !errors.Is(err, ErrControlValidation) {
				t.Fatalf("URI whitespace %q accepted: %v", whitespace, err)
			}
		})
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
