package rawrpc

import (
	"bytes"
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

type capableCaller struct {
	recordingCaller
	experimental bool
}

func (c *capableCaller) ExperimentalAPIEnabled() bool { return c.experimental }

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
	if _, err := Execute(context.Background(), methodCaller, plan.Request); err == nil || methodCaller.calls.Load() != 0 {
		rawErr := requireRawError(t, err)
		if rawErr.Code != mektup.ErrExperimentalMethodUnavailable || rawErr.EffectState != string(mektup.StateNotSent) {
			t.Fatalf("missing method capability error = %+v", rawErr)
		}
	}
	methodCapable := &capableCaller{recordingCaller: recordingCaller{result: readResult(`{"ok":true}`)}, experimental: true}
	if _, err := Execute(context.Background(), methodCapable, plan.Request); err != nil || methodCapable.calls.Load() != 1 {
		t.Fatalf("experimental method execute err=%v calls=%d", err, methodCapable.calls.Load())
	}

	fieldCaller := &recordingCaller{result: readResult(`{"threadId":"child"}`)}
	plan, err = Prepare(Request{Method: "thread/fork", Params: json.RawMessage(`{"threadId":"parent","beforeTurnId":"turn"}`), Grants: []rpcmeta.EffectClass{rpcmeta.EffectThreadWrite}})
	if err != nil || !plan.ExperimentalAPIRequired || len(plan.Decision.ExperimentalFields) != 1 || plan.Decision.ExperimentalFields[0] != "thread/fork.beforeTurnId" {
		t.Fatalf("experimental field plan=%+v err=%v", plan, err)
	}
	if _, err := Execute(context.Background(), fieldCaller, plan.Request); err == nil || fieldCaller.calls.Load() != 0 {
		rawErr := requireRawError(t, err)
		if rawErr.Code != mektup.ErrExperimentalMethodUnavailable || rawErr.EffectState != string(mektup.StateNotSent) {
			t.Fatalf("missing field capability error = %+v", rawErr)
		}
	}
	fieldCapable := &capableCaller{recordingCaller: recordingCaller{result: readResult(`{"threadId":"child"}`)}, experimental: true}
	if _, err := Execute(context.Background(), fieldCapable, plan.Request); err != nil || fieldCapable.calls.Load() != 1 {
		t.Fatalf("experimental field execute err=%v calls=%d", err, fieldCapable.calls.Load())
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
	nested, ok := rawErr.Details["serverError"].(map[string]any)
	inlineData, dataOK := nested["data"].(json.RawMessage)
	if !ok || nested["message"] != server.Message || !dataOK || string(inlineData) != string(server.Data) {
		t.Fatalf("stable details omitted bounded server evidence: %#v", rawErr.Details["serverError"])
	}
	if !errors.Is(err, server) && !errors.Is(err, caller.err) {
		t.Fatalf("server error was not retained in unwrap chain: %v", err)
	}
}

func TestOversizedServerErrorDataSpillsWithDigestAndNoDuplicateBlob(t *testing.T) {
	data := json.RawMessage(`{"secret":"` + strings.Repeat("s", 128) + `"}`)
	store, err := artifact.NewStore(filepath.Join(t.TempDir(), "artifacts"))
	if err != nil {
		t.Fatal(err)
	}
	server := &appserver.ServerError{ID: "rpc-large", Code: -32603, Message: "failed", Data: data, Generation: 8}
	caller := &recordingCaller{err: &appserver.CallError{Server: server, Evidence: appserver.WriteEvidence{Phase: appserver.WriteComplete, Generation: 8}, Generation: 8}}
	_, err = Execute(context.Background(), caller, Request{Method: "thread/read", Output: OutputOptions{InlineLimit: 8, Store: store}})
	rawErr := requireRawError(t, err)
	if rawErr.Code != mektup.ErrDeliveryRejected || rawErr.Server == nil || rawErr.Server.Data != nil || rawErr.Server.DataArtifact == nil {
		t.Fatalf("oversized server error = %+v", rawErr)
	}
	if rawErr.Server.DataBytes != int64(len(data)) || rawErr.Server.DataSHA256 == "" || !rawErr.Server.DataArtifact.SensitiveOutputPossible || !rawErr.Server.DataArtifact.Complete {
		t.Fatalf("server data retention = %+v", rawErr.Server)
	}
	if nested, ok := rawErr.Details["serverError"].(map[string]any); !ok || nested["data"] != nil || nested["dataArtifact"] == nil {
		t.Fatalf("stable nested server evidence = %#v", rawErr.Details["serverError"])
	}
	contents, err := os.ReadFile(rawErr.Server.DataArtifact.Path)
	if err != nil || string(contents) != string(data) {
		t.Fatalf("spilled server data = %q err=%v", contents, err)
	}
}

func TestOversizedServerErrorMessageSpillsCompleteEvidenceWithoutInlineDuplicate(t *testing.T) {
	message := strings.Repeat("m", 128)
	data := json.RawMessage(`{"detail":"keep"}`)
	store, err := artifact.NewStore(filepath.Join(t.TempDir(), "artifacts"))
	if err != nil {
		t.Fatal(err)
	}
	server := &appserver.ServerError{ID: "rpc-message", Code: -32603, Message: message, Data: data, Generation: 11}
	caller := &recordingCaller{err: &appserver.CallError{Server: server, Evidence: appserver.WriteEvidence{Phase: appserver.WriteComplete, Generation: 11}, Generation: 11}}
	_, err = Execute(context.Background(), caller, Request{Method: "thread/read", Output: OutputOptions{InlineLimit: 16, Store: store}})
	rawErr := requireRawError(t, err)
	if rawErr.Code != mektup.ErrDeliveryRejected || rawErr.Server == nil || rawErr.Server.Message != "" || rawErr.Server.Data != nil || rawErr.Server.EvidenceArtifact == nil {
		t.Fatalf("oversized server message = %+v", rawErr)
	}
	if rawErr.Server.MessageBytes != int64(len(message)) || rawErr.Server.MessageSHA256 == "" || rawErr.Server.EvidenceBytes == 0 || rawErr.Server.EvidenceSHA256 == "" || !rawErr.Server.EvidenceArtifact.Complete {
		t.Fatalf("server evidence retention = %+v", rawErr.Server)
	}
	nested, ok := rawErr.Details["serverError"].(map[string]any)
	if !ok || nested["message"] != nil || nested["evidenceArtifact"] == nil {
		t.Fatalf("stable details duplicated oversized message: %#v", rawErr.Details["serverError"])
	}
	contents, err := os.ReadFile(rawErr.Server.EvidenceArtifact.Path)
	if err != nil || !bytes.Contains(contents, []byte(message)) || !bytes.Contains(contents, data) {
		t.Fatalf("spilled server evidence = %q err=%v", contents, err)
	}
}

func TestStableServerErrorPreservesSmallInlineMessageAndData(t *testing.T) {
	server := &appserver.ServerError{ID: "rpc-inline", Code: -32603, Message: "precise_reason", Data: json.RawMessage(`{"diagnostic":"exact-detail"}`)}
	caller := &recordingCaller{err: &appserver.CallError{Server: server, Evidence: appserver.WriteEvidence{Phase: appserver.WriteComplete}}}
	_, err := Execute(context.Background(), caller, Request{Method: "thread/read"})
	rawErr := requireRawError(t, err)
	encoded, err := json.Marshal(rawErr.Stable())
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encoded, []byte("precise_reason")) || !bytes.Contains(encoded, []byte("exact-detail")) {
		t.Fatalf("canonical error lost bounded server evidence: %s", encoded)
	}
}

func TestFailedServerErrorSpillDoesNotRetainOversizedSerializableMessage(t *testing.T) {
	message := strings.Repeat("x", 1<<20)
	server := &appserver.ServerError{ID: "rpc-failed-spill", Code: -32603, Message: message}
	caller := &recordingCaller{err: &appserver.CallError{Server: server, Evidence: appserver.WriteEvidence{Phase: appserver.WriteComplete}}}
	_, err := Execute(context.Background(), caller, Request{Method: "thread/read", Output: OutputOptions{InlineLimit: 32}})
	rawErr := requireRawError(t, err)
	encoded, err := json.Marshal(rawErr)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > 8192 {
		t.Fatalf("failed retention left %d-byte serialized error", len(encoded))
	}
	if rawErr.Server == nil || rawErr.Server.Message != "" || rawErr.Server.MessageBytes != int64(len(message)) || rawErr.Server.MessageSHA256 == "" {
		t.Fatalf("failed retention evidence = %+v", rawErr.Server)
	}
}

func TestServerErrorDataArtifactFailureIsTruthful(t *testing.T) {
	data := json.RawMessage(`{"secret":"` + strings.Repeat("s", 128) + `"}`)
	server := &appserver.ServerError{ID: "rpc-fail", Code: -32603, Message: "failed", Data: data}
	caller := &recordingCaller{err: &appserver.CallError{Server: server, Evidence: appserver.WriteEvidence{Phase: appserver.WriteComplete}}}
	_, err := Execute(context.Background(), caller, Request{Method: "thread/read", Output: OutputOptions{InlineLimit: 8, MaxBytes: 4, Store: mustArtifactStore(t)}})
	rawErr := requireRawError(t, err)
	if rawErr.Code != mektup.ErrOutputTooLarge || rawErr.EffectState != string(mektup.StateRejected) || rawErr.Server == nil || rawErr.Server.DataArtifact != nil {
		t.Fatalf("artifact failure error = %+v", rawErr)
	}
	if rawErr.Details["serverErrorRetention"] == nil {
		t.Fatalf("artifact failure omitted retention evidence: %+v", rawErr.Details)
	}
}

func TestServerErrorDataUsesExplicitOutputPath(t *testing.T) {
	data := json.RawMessage(`{"secret":"` + strings.Repeat("p", 64) + `"}`)
	path := filepath.Join(t.TempDir(), "error-data.json")
	server := &appserver.ServerError{ID: "rpc-path", Code: -32603, Message: "failed", Data: data}
	caller := &recordingCaller{err: &appserver.CallError{Server: server, Evidence: appserver.WriteEvidence{Phase: appserver.WriteComplete}}}
	_, err := Execute(context.Background(), caller, Request{Method: "thread/read", Output: OutputOptions{InlineLimit: 1 << 20, Path: path}})
	rawErr := requireRawError(t, err)
	if rawErr.Code != mektup.ErrDeliveryRejected || rawErr.Server == nil || rawErr.Server.Data != nil || rawErr.Server.DataArtifact == nil || rawErr.Server.DataArtifact.Path != path {
		t.Fatalf("explicit server data output = %+v", rawErr)
	}
	contents, err := os.ReadFile(path)
	if err != nil || string(contents) != string(data) {
		t.Fatalf("explicit server data = %q err=%v", contents, err)
	}
}

func mustArtifactStore(t *testing.T) *artifact.Store {
	t.Helper()
	store, err := artifact.NewStore(filepath.Join(t.TempDir(), "artifacts"))
	if err != nil {
		t.Fatal(err)
	}
	return store
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
	// A repeated deterministic spill is a successful reuse, not an output
	// overflow caused by the existing path.
	repeated, err := Execute(context.Background(), &recordingCaller{result: readResult(string(payload))}, Request{Method: "thread/read", Output: OutputOptions{InlineLimit: 8, Store: store, Name: "response.json"}})
	if err != nil || repeated.Artifact == nil || repeated.Artifact.Path != response.Artifact.Path || repeated.Artifact.SHA256 != response.Artifact.SHA256 {
		t.Fatalf("repeated spill response=%+v err=%v", repeated, err)
	}
}

func TestDefaultSpillNameIsContentAddressedAcrossStoreReopen(t *testing.T) {
	root := filepath.Join(t.TempDir(), "artifacts")
	large := func(value string) json.RawMessage {
		return json.RawMessage(`{"result":"` + strings.Repeat(value, 64) + `"}`)
	}
	firstStore, err := artifact.NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	firstResponse, err := Execute(context.Background(), &recordingCaller{result: readResult(string(large("a")))}, Request{Method: "thread/read", Output: OutputOptions{InlineLimit: 8, Store: firstStore}})
	if err != nil || firstResponse.Artifact == nil {
		t.Fatalf("first spill response=%+v err=%v", firstResponse, err)
	}
	if err := firstStore.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := artifact.NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := Execute(context.Background(), &recordingCaller{result: readResult(string(large("a")))}, Request{Method: "thread/read", Output: OutputOptions{InlineLimit: 8, Store: reopened}})
	if err != nil || repeated.Artifact == nil || repeated.Artifact.Path != firstResponse.Artifact.Path {
		t.Fatalf("reopened identical spill response=%+v err=%v", repeated, err)
	}
	different, err := Execute(context.Background(), &recordingCaller{result: readResult(string(large("b")))}, Request{Method: "thread/read", Output: OutputOptions{InlineLimit: 8, Store: reopened}})
	if err != nil || different.Artifact == nil || different.Artifact.Path == repeated.Artifact.Path {
		t.Fatalf("different spill response=%+v err=%v", different, err)
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

func TestExplicitOutputPathPublishesSmallResponse(t *testing.T) {
	output := filepath.Join(t.TempDir(), "small.json")
	caller := &recordingCaller{result: readResult(`{"small":true}`)}
	response, err := Execute(context.Background(), caller, Request{Method: "thread/read", Output: OutputOptions{InlineLimit: 1 << 20, Path: output}})
	if err != nil {
		t.Fatal(err)
	}
	if response.Raw != nil || response.Artifact == nil || response.Artifact.Path != output {
		t.Fatalf("explicit small response = %+v", response)
	}
	contents, err := os.ReadFile(output)
	if err != nil || string(contents) != `{"small":true}` {
		t.Fatalf("explicit small output = %q err=%v", contents, err)
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
