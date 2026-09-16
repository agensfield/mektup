package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/agensfield/mektup/go/internal/cli"
	"github.com/agensfield/mektup/go/internal/controlreceiver"
)

func TestRunPredispatchesOnlyExactControlReceive(t *testing.T) {
	wantErr := errors.New("receiver sentinel")
	called := 0
	receive := func(_ context.Context, input io.Reader, output io.Writer, opts controlreceiver.CommandOptions) error {
		called++
		if !reflect.DeepEqual(opts, controlreceiver.CommandOptions{}) {
			t.Fatalf("control options = %#v", opts)
		}
		body, err := io.ReadAll(input)
		if err != nil {
			t.Fatal(err)
		}
		if string(body) != `{"schema":"mektup/control/v1"}` {
			t.Fatalf("input = %q", body)
		}
		if _, err := output.Write([]byte(`{"kind":"result"}`)); err != nil {
			t.Fatal(err)
		}
		return wantErr
	}

	var stdout, stderr bytes.Buffer
	exit := run(context.Background(), []string{"control", "receive"}, strings.NewReader(`{"schema":"mektup/control/v1"}`), &stdout, &stderr, receive)
	if exit != int(cli.ExitInternal) || called != 1 {
		t.Fatalf("exit=%d called=%d", exit, called)
	}
	if stdout.String() != `{"kind":"result"}` {
		t.Fatalf("stdout = %q", stdout.String())
	}
	if !strings.Contains(stderr.String(), wantErr.Error()) {
		t.Fatalf("stderr = %q", stderr.String())
	}
}

func TestRunDoesNotBroadenControlGrammar(t *testing.T) {
	called := 0
	receive := func(context.Context, io.Reader, io.Writer, controlreceiver.CommandOptions) error {
		called++
		return nil
	}

	for _, args := range [][]string{{"control"}, {"control", "receive", "extra"}, {"control", "status"}} {
		var stdout, stderr bytes.Buffer
		exit := run(context.Background(), args, strings.NewReader("ignored"), &stdout, &stderr, receive)
		if exit != int(cli.ExitUsage) {
			t.Fatalf("args=%v exit=%d stdout=%q stderr=%q", args, exit, stdout.String(), stderr.String())
		}
	}
	if called != 0 {
		t.Fatalf("receiver called %d times", called)
	}
}

func TestRunControlReceiveSuccess(t *testing.T) {
	receive := func(context.Context, io.Reader, io.Writer, controlreceiver.CommandOptions) error { return nil }
	if exit := run(context.Background(), []string{"control", "receive"}, strings.NewReader("{}"), io.Discard, io.Discard, receive); exit != int(cli.ExitSuccess) {
		t.Fatalf("exit = %d", exit)
	}
}
