package sshproxy

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func validControlRequest() ControlRequest {
	bytesCount := int64(7)
	return ControlRequest{
		Schema: "mektup/control/v1", Kind: "request", Operation: "claim",
		OperationID: "op_01", ReplyMessageID: "msg_01", OriginalMessageID: "msg_02",
		Custody:          CustodyRef{EndpointID: "ep_01", StoreID: "store_01"},
		ReplyDestination: DestinationRef{EndpointID: "ep_02", ThreadID: "thread_01"},
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
	go func() {
		reader := bufio.NewReader(p.stdinR)
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return
		}
		if bytes.Contains(line, []byte("/tmp")) {
			return
		}
		_, _ = p.stdoutW.Write([]byte(`{"schema":"mektup/control/v1","kind":"result","operation":"claim","result":{"fencingToken":"fence"}}`))
		_ = p.stdoutW.Close()
		_ = p.stderrW.Close()
		p.releaseWait()
	}()
	response, err := InvokeControl(context.Background(), Config{Host: "remote"}, validControlRequest(), factory, nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(response) != `{"schema":"mektup/control/v1","kind":"result","operation":"claim","result":{"fencingToken":"fence"}}` {
		t.Fatalf("response = %s", response)
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
