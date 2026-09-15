package rawrpc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	mektup "github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/appserver"
	"github.com/agensfield/mektup/go/internal/artifact"
	"github.com/agensfield/mektup/go/internal/rpcmeta"
)

type recordingCaller struct {
	calls  atomic.Int64
	method string
	params json.RawMessage
	result *appserver.RPCResult
	err    error
}

func (c *recordingCaller) Call(_ context.Context, request appserver.RPCRequest) (*appserver.RPCResult, error) {
	c.calls.Add(1)
	c.method = request.Method
	c.params = append(json.RawMessage(nil), request.Params...)
	return c.result, c.err
}

func readResult(value string) *appserver.RPCResult {
	return &appserver.RPCResult{ID: "result", Value: json.RawMessage(value), Evidence: appserver.WriteEvidence{Phase: appserver.WriteComplete, Generation: 9}, Generation: 9}
}

func requireRawError(t *testing.T, err error) *Error {
	t.Helper()
	if err == nil {
		t.Fatal("expected raw RPC error")
	}
	var rawErr *Error
	if !errors.As(err, &rawErr) {
		t.Fatalf("error type = %T, want *rawrpc.Error: %v", err, err)
	}
	return rawErr
}

func TestMissingGatesRejectBeforeCallerForEveryGatedEffect(t *testing.T) {
	tests := []struct {
		name   string
		method string
		grant  rpcmeta.EffectClass
	}{
		{"thread-write", "thread/start", rpcmeta.EffectThreadWrite},
		{"host-write", "thread/shellCommand", rpcmeta.EffectHostWrite},
		{"auth", "account/login/start", rpcmeta.EffectAuth},
		{"destructive", "thread/delete", rpcmeta.EffectDestructive},
		{"unknown", "method/added-after-pinning", rpcmeta.EffectUnknown},
		{"independent-destructive", "fs/remove", rpcmeta.EffectHostWrite},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			caller := &recordingCaller{result: readResult(`{"ok":true}`)}
			grants := []rpcmeta.EffectClass(nil)
			if test.name == "independent-destructive" {
				grants = []rpcmeta.EffectClass{test.grant}
			}
			_, err := Execute(context.Background(), caller, Request{Method: test.method, Params: json.RawMessage(`{}`), Grants: grants})
			rawErr := requireRawError(t, err)
			if rawErr.Code != mektup.ErrEffectAcknowledgmentRequired || rawErr.EffectState != string(mektup.StateNotSent) {
				t.Fatalf("error = %+v", rawErr)
			}
			if caller.calls.Load() != 0 {
				t.Fatalf("caller calls = %d, want zero", caller.calls.Load())
			}
		})
	}
}

func TestMethodWorstCaseAndUnknownGrant(t *testing.T) {
	caller := &recordingCaller{result: readResult(`{"ok":true}`)}
	// fs/remove carries both host-write and destructive; acknowledging only
	// one must not downgrade the other.
	_, err := Execute(context.Background(), caller, Request{Method: "fs/remove", Params: json.RawMessage(`{"path":"x"}`), Grants: []rpcmeta.EffectClass{rpcmeta.EffectHostWrite}})
	gate := requireRawError(t, err)
	if len(gate.Details["missing"].([]string)) != 1 || gate.Details["missing"].([]string)[0] != string(rpcmeta.EffectDestructive) {
		t.Fatalf("missing details = %#v", gate.Details["missing"])
	}
	if caller.calls.Load() != 0 {
		t.Fatal("worst-case gate dispatched")
	}

	caller.result = readResult(`{"unknown":true}`)
	response, err := Execute(context.Background(), caller, Request{Method: "method/added-after-pinning", Params: json.RawMessage(`null`), Grants: []rpcmeta.EffectClass{rpcmeta.EffectUnknown}})
	if err != nil || response == nil || caller.calls.Load() != 1 {
		t.Fatalf("unknown method execution response=%+v err=%v calls=%d", response, err, caller.calls.Load())
	}
}

func TestExperimentalMethodAndFieldRequireCapabilityButNotEffectGrant(t *testing.T) {
	methodCaller := &recordingCaller{result: readResult(`{"ok":true}`)}
	plan, err := Prepare(Request{Method: "server/diagnostics", Params: json.RawMessage(`{}`)})
	if err != nil || !plan.ExperimentalAPIRequired {
		t.Fatalf("experimental method plan=%+v err=%v", plan, err)
	}
	if _, err := Execute(context.Background(), methodCaller, plan.Request); err != nil || methodCaller.calls.Load() != 1 {
		t.Fatalf("experimental method execute err=%v calls=%d", err, methodCaller.calls.Load())
	}

	fieldCaller := &recordingCaller{result: readResult(`{"threadId":"child"}`)}
	plan, err = Prepare(Request{Method: "thread/fork", Params: json.RawMessage(`{"threadId":"parent","beforeTurnId":"turn"}`), Grants: []rpcmeta.EffectClass{rpcmeta.EffectThreadWrite}})
	if err != nil || !plan.ExperimentalAPIRequired || len(plan.Decision.ExperimentalFields) != 1 || plan.Decision.ExperimentalFields[0] != "thread/fork.beforeTurnId" {
		t.Fatalf("experimental field plan=%+v err=%v", plan, err)
	}
	if _, err := Execute(context.Background(), fieldCaller, plan.Request); err != nil || fieldCaller.calls.Load() != 1 {
		t.Fatalf("experimental field execute err=%v calls=%d", err, fieldCaller.calls.Load())
	}
}

func TestNetworkReadDispatchesAndRetainsEffectEvidence(t *testing.T) {
	caller := &recordingCaller{result: readResult(`{"models":[]}`)}
	response, err := Execute(context.Background(), caller, Request{Method: "model/list", Params: json.RawMessage(`{"cursor":null}`)})
	if err != nil {
		t.Fatal(err)
	}
	if !response.NetworkRead || !containsEffect(response.Effects, rpcmeta.EffectNetworkRead) {
		t.Fatalf("network evidence = %+v", response)
	}
}

func TestParamsAreForwardedExactlyOnce(t *testing.T) {
	params := json.RawMessage(" {\"a\": [1, 2], \"unicode\": \"\\u00e9\"} ")
	caller := &recordingCaller{result: readResult(`{"ok":true}`)}
	if _, err := Execute(context.Background(), caller, Request{Method: "thread/read", Params: params, ParamsSource: ParamsFile}); err != nil {
		t.Fatal(err)
	}
	if caller.method != "thread/read" || string(caller.params) != string(params) {
		t.Fatalf("forwarded method/params = %q/%q, want %q/%q", caller.method, caller.params, "thread/read", params)
	}
	if plan, err := Prepare(Request{Method: "thread/read", Params: params, ParamsSource: ParamsStdin}); err != nil || string(plan.Request.Params) != string(params) {
		t.Fatalf("prepare params = %q err=%v", plan.Request.Params, err)
	}
}

func TestRawServerErrorIsNestedWithoutChangingStableCode(t *testing.T) {
	server := &appserver.ServerError{ID: "rpc-1", Code: -32603, Message: "server said no", Data: json.RawMessage(`{"detail":"keep"}`), Generation: 4}
	caller := &recordingCaller{err: &appserver.CallError{Server: server, Evidence: appserver.WriteEvidence{Phase: appserver.WriteComplete, Generation: 4}, Generation: 4}}
	_, err := Execute(context.Background(), caller, Request{Method: "thread/read", Params: json.RawMessage(`{}`)})
	rawErr := requireRawError(t, err)
	if rawErr.Code != mektup.ErrDeliveryRejected || rawErr.EffectState != string(mektup.StateRejected) || rawErr.Retryable {
		t.Fatalf("stable error = %+v", rawErr)
	}
	if rawErr.Server == nil || rawErr.Server.Code != server.Code || rawErr.Server.Message != server.Message || string(rawErr.Server.Data) != string(server.Data) || rawErr.Server.Generation != server.Generation {
		t.Fatalf("server evidence = %+v", rawErr.Server)
	}
	if !errors.Is(err, server) && !errors.Is(err, caller.err) {
		t.Fatalf("server error was not retained in unwrap chain: %v", err)
	}
}

func TestWritePhaseClassificationIsConservative(t *testing.T) {
	tests := []struct {
		name  string
		phase appserver.WritePhase
		state mektup.EvidenceState
		code  mektup.ErrorCode
	}{
		{"before-write", appserver.WriteProvenBeforeWrite, mektup.StateNotSent, mektup.ErrEndpointUnavailable},
		{"may-have-written", appserver.WriteMayHaveWritten, mektup.StateOutcomeUnknown, mektup.ErrOutcomeUnknown},
		{"complete-without-reply", appserver.WriteComplete, mektup.StateOutcomeUnknown, mektup.ErrOutcomeUnknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			caller := &recordingCaller{err: &appserver.CallError{Err: errors.New("transport"), Evidence: appserver.WriteEvidence{Phase: test.phase}}}
			_, err := Execute(context.Background(), caller, Request{Method: "thread/start", Params: json.RawMessage(`{}`), Grants: []rpcmeta.EffectClass{rpcmeta.EffectThreadWrite}})
			rawErr := requireRawError(t, err)
			if rawErr.EffectState != string(test.state) || rawErr.Code != test.code || rawErr.Retryable {
				t.Fatalf("error = %+v", rawErr)
			}
		})
	}
}

func TestLargeResponseSpillIncludesDigestAndPrivateArtifact(t *testing.T) {
	payload := json.RawMessage(`{"result":"` + strings.Repeat("x", 128) + `"}`)
	root := filepath.Join(t.TempDir(), "artifacts")
	store, err := artifact.NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	caller := &recordingCaller{result: readResult(string(payload))}
	response, err := Execute(context.Background(), caller, Request{Method: "thread/read", Output: OutputOptions{InlineLimit: 8, Store: store, Name: "response.json"}})
	if err != nil {
		t.Fatal(err)
	}
	if response.Raw != nil || response.Artifact == nil || !response.Artifact.Complete || response.Artifact.Bytes != int64(len(payload)) {
		t.Fatalf("spill response = %+v", response)
	}
	hash := sha256.Sum256(payload)
	if response.Artifact.SHA256 != "sha256:"+hex.EncodeToString(hash[:]) {
		t.Fatalf("digest = %q", response.Artifact.SHA256)
	}
	contents, err := os.ReadFile(response.Artifact.Path)
	if err != nil || string(contents) != string(payload) {
		t.Fatalf("artifact contents = %q err=%v", contents, err)
	}
	info, err := os.Stat(response.Artifact.Path)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("artifact stat=%+v err=%v", info, err)
	}
}

func TestExplicitOutputNoClobberAndForce(t *testing.T) {
	output := filepath.Join(t.TempDir(), "caller", "response.json")
	call := func(force bool, value string) (*Response, error) {
		caller := &recordingCaller{result: readResult(`{"value":"` + value + `"}`)}
		return Execute(context.Background(), caller, Request{Method: "thread/read", Output: OutputOptions{InlineLimit: 1, Path: output, Force: force}})
	}
	if _, err := call(false, "one"); err != nil {
		t.Fatal(err)
	}
	if _, err := call(false, "two"); err == nil || !errors.Is(err, artifact.ErrExists) {
		t.Fatalf("overwrite error = %v, want artifact.ErrExists", err)
	}
	if _, err := call(true, "three"); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(output)
	if err != nil || string(contents) != `{"value":"three"}` {
		t.Fatalf("forced output = %q err=%v", contents, err)
	}
}

func containsEffect(effects []rpcmeta.EffectClass, wanted rpcmeta.EffectClass) bool {
	for _, effect := range effects {
		if effect == wanted {
			return true
		}
	}
	return false
}
