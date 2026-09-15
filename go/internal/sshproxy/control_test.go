package sshproxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"
)

func validControlRequest() ControlRequest {
	bytesCount := int64(7)
	return ControlRequest{
		Schema: "mektup/control/v1", Kind: "request", Operation: "claim",
		OperationID: "op_0198f0e0-0000-7000-8000-00000000000c", ReplyMessageID: "msg_0198f0e0-0000-7000-8000-000000000007", OriginalMessageID: "msg_0198f0e0-0000-7000-8000-000000000003",
		Custody:          CustodyRef{EndpointID: "ep_0198f0e0-0000-7000-8000-000000000001", StoreID: "store_0198f0e0-0000-7000-8000-000000000002"},
		ReplyDestination: DestinationRef{EndpointID: "ep_0198f0e0-0000-7000-8000-000000000001", ThreadID: "thread-local-001", URI: "codex://local/thread/thread-local-001"},
		BodyBytes:        &bytesCount, BodySHA256: "sha256:" + strings.Repeat("a", 64), ReplyStatus: "success", AttemptOwner: "owner_01",
	}
}

func TestControlValidationRejectsUntrustedPathAndBodyFields(t *testing.T) {
	req := validControlRequest()
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ValidateControlRequest(data)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(parsed, req) {
		t.Fatalf("request = %#v, want %#v", parsed, req)
	}
	for name, mutate := range map[string]func(map[string]any){
		"missing URI": func(raw map[string]any) { delete(raw["replyDestination"].(map[string]any), "uri") },
		"empty URI":   func(raw map[string]any) { raw["replyDestination"].(map[string]any)["uri"] = "" },
		"null URI":    func(raw map[string]any) { raw["replyDestination"].(map[string]any)["uri"] = nil },
		"wrong URI":   func(raw map[string]any) { raw["replyDestination"].(map[string]any)["uri"] = 42 },
	} {
		t.Run(name, func(t *testing.T) {
			var raw map[string]any
			if err := json.Unmarshal(data, &raw); err != nil {
				t.Fatal(err)
			}
			mutate(raw)
			bad, err := json.Marshal(raw)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := ValidateControlRequest(bad); !errors.Is(err, ErrControlValidation) {
				t.Fatalf("URI shape accepted: %v", err)
			}
		})
	}
	for _, field := range []string{"body", "bodyText", "path", "executable", "shell"} {
		var raw map[string]any
		if err := json.Unmarshal(data, &raw); err != nil {
			t.Fatal(err)
		}
		raw[field] = "/tmp/untrusted"
		bad, err := json.Marshal(raw)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := ValidateControlRequest(bad); !errors.Is(err, ErrControlValidation) {
			t.Fatalf("field %q accepted: %v", field, err)
		}
	}
	missingLease := validControlRequest()
	missingLease.Operation = "commit"
	if err := missingLease.Validate(); !errors.Is(err, ErrControlValidation) {
		t.Fatalf("commit without issued lease accepted: %v", err)
	}
}

func TestInvokeControlUsesFixedArgvAndMetadataStdin(t *testing.T) {
	p := newFakeProcess()
	factory := &fakeFactory{process: p}
	request := validControlRequest()
	go func() {
		reader := bufio.NewReader(p.stdinR)
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return
		}
		if bytes.Contains(line, []byte("/tmp")) {
			return
		}
		result := request
		result.Kind = "result"
		result.Result = json.RawMessage(`{"disposition":"claimed","state":"reply_dispatch_claimed","fencingToken":"fence","lease":{"expiresAt":"2026-09-15T03:00:31.900000Z"}}`)
		resultBytes, _ := json.Marshal(result)
		_, _ = p.stdoutW.Write(resultBytes)
		_ = p.stdoutW.Close()
		_ = p.stderrW.Close()
		p.releaseWait()
	}()
	response, err := InvokeControl(context.Background(), Config{Host: "remote"}, request, factory, nil)
	if err != nil {
		t.Fatal(err)
	}
	var decoded ControlRequest
	if err := json.Unmarshal(response, &decoded); err != nil || decoded.Kind != "result" || decoded.OperationID != request.OperationID {
		t.Fatalf("response = %s err=%v", response, err)
	}
	if !reflect.DeepEqual(factory.argv, []string{"ssh", "--", "remote", "mektup", "control", "receive"}) {
		t.Fatalf("argv = %#v", factory.argv)
	}
}

func TestControlValidatorRunsBeforeSpawn(t *testing.T) {
	called := false
	factoryCalled := false
	_, err := InvokeControl(context.Background(), Config{Host: "remote"}, validControlRequest(), ProcessFactoryFunc(func([]string) (Process, error) {
		factoryCalled = true
		return nil, errors.New("must not spawn")
	}), ControlValidatorFunc(func(context.Context, ControlRequest) error {
		called = true
		return errors.New("relationship not registered")
	}))
	if !called || factoryCalled || !strings.Contains(err.Error(), "relationship not registered") {
		t.Fatalf("called=%v factory=%v err=%v", called, factoryCalled, err)
	}
}

func TestInvokeControlRejectsSwappedResponseIdentity(t *testing.T) {
	p := newFakeProcess()
	factory := &fakeFactory{process: p}
	request := validControlRequest()
	go func() {
		_, _ = bufio.NewReader(p.stdinR).ReadBytes('\n')
		result := request
		result.Kind = "result"
		result.OperationID = "op_0198f0e0-0000-7000-8000-00000000000e"
		result.Result = json.RawMessage(`{"disposition":"claimed","state":"reply_dispatch_claimed","fencingToken":"fence","lease":{"expiresAt":"2026-09-15T03:00:31.900000Z"}}`)
		response, _ := json.Marshal(result)
		_, _ = p.stdoutW.Write(response)
		_ = p.stdoutW.Close()
		_ = p.stderrW.Close()
		p.releaseWait()
	}()
	_, err := InvokeControl(context.Background(), Config{Host: "remote"}, request, factory, nil)
	var failure *Failure
	if !errors.As(err, &failure) || failure.Kind != FailureProxy || failure.Evidence != WriteComplete || !errors.Is(err, ErrControlValidation) {
		t.Fatalf("swapped response error = %T %+v", err, err)
	}
}

func TestInvokeControlEarlyFailuresStillReapChild(t *testing.T) {
	t.Run("short-write", func(t *testing.T) {
		process := &waitCountingProcess{fakeProcess: newFakeProcess()}
		process.stdinOverride = shortWriter{}
		_, err := InvokeControl(context.Background(), Config{Host: "remote", CleanupTimeout: 20 * time.Millisecond}, validControlRequest(), ProcessFactoryFunc(func([]string) (Process, error) { return process, nil }), nil)
		if !errors.Is(err, io.ErrShortWrite) || process.waits.Load() != 1 {
			t.Fatalf("err=%v waits=%d", err, process.waits.Load())
		}
	})
	t.Run("encode-failure", func(t *testing.T) {
		process := &waitCountingProcess{fakeProcess: newFakeProcess()}
		request := validControlRequest()
		request.Operation = "status"
		request.Result = json.RawMessage("{")
		_, err := InvokeControl(context.Background(), Config{Host: "remote", CleanupTimeout: 20 * time.Millisecond}, request, ProcessFactoryFunc(func([]string) (Process, error) { return process, nil }), nil)
		if err == nil || process.waits.Load() != 1 {
			t.Fatalf("err=%v waits=%d", err, process.waits.Load())
		}
	})
}
