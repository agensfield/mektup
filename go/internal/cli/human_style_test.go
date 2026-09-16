package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestStyleHumanIsOptInAndReadable(t *testing.T) {
	plain := "doctor: healthy\nSTATE  CHECK\nok     ep_01999999-9999-7999-8999-999999999999"
	if got := StyleHuman(plain, false); got != plain {
		t.Fatalf("disabled styling changed output: %q", got)
	}
	styled := StyleHuman(plain, true)
	for _, want := range []string{"\x1b[", "doctor: ", "healthy", "ep_01999999"} {
		if !strings.Contains(styled, want) {
			t.Fatalf("styled output missing %q: %q", want, styled)
		}
	}
}

func TestColorResolutionHonorsPresentationTTYAndOverrides(t *testing.T) {
	tests := []struct {
		name         string
		explicit     string
		env          map[string]string
		presentation Presentation
		terminal     bool
		want         bool
		wantErr      bool
	}{
		{name: "human tty auto", presentation: PresentationHuman, terminal: true, want: true},
		{name: "human pipe auto", presentation: PresentationHuman, terminal: false},
		{name: "always", explicit: "always", presentation: PresentationHuman, want: true},
		{name: "never", explicit: "never", presentation: PresentationHuman, terminal: true},
		{name: "no color", env: map[string]string{"NO_COLOR": ""}, presentation: PresentationHuman, terminal: true},
		{name: "dumb terminal", env: map[string]string{"TERM": "dumb"}, presentation: PresentationHuman, terminal: true},
		{name: "json refuses ansi", explicit: "always", presentation: PresentationJSON, terminal: true},
		{name: "invalid", explicit: "sparkles", presentation: PresentationHuman, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := resolveColor(test.explicit, test.env, test.presentation, test.terminal)
			if (err != nil) != test.wantErr || got != test.want {
				t.Fatalf("got=%t err=%v want=%t wantErr=%t", got, err, test.want, test.wantErr)
			}
		})
	}
}

func TestHumanOutputStylesCentrallyButJSONNeverDoes(t *testing.T) {
	exec := executorFunc(func(_ context.Context, inv Invocation) (ExecutionResult, error) {
		return ExecutionResult{Events: []OutputEvent{{Machine: map[string]any{
			"schema": EventSchema, "event": "doctor.completed", "terminal": true, "ok": true,
		}, Human: "doctor: healthy"}}}, nil
	})

	var humanOut bytes.Buffer
	human := &App{In: strings.NewReader(""), Out: &humanOut, Err: &bytes.Buffer{}, Env: []string{}, Executor: exec}
	if code := human.Run([]string{"--human", "--color", "always", "doctor"}); code != int(ExitSuccess) {
		t.Fatalf("human exit=%d", code)
	}
	if !strings.Contains(humanOut.String(), "\x1b[") || !strings.Contains(humanOut.String(), "doctor: ") || !strings.Contains(humanOut.String(), "healthy") {
		t.Fatalf("human output=%q", humanOut.String())
	}

	var jsonOut bytes.Buffer
	machine := &App{In: strings.NewReader(""), Out: &jsonOut, Err: &bytes.Buffer{}, Env: []string{}, Executor: exec}
	if code := machine.Run([]string{"--json", "--color", "always", "doctor"}); code != int(ExitSuccess) {
		t.Fatalf("json exit=%d", code)
	}
	if strings.Contains(jsonOut.String(), "\x1b[") || strings.Contains(jsonOut.String(), "doctor: healthy") {
		t.Fatalf("machine output was decorated: %q", jsonOut.String())
	}
	var comparisonOut bytes.Buffer
	comparison := &App{In: strings.NewReader(""), Out: &comparisonOut, Err: &bytes.Buffer{}, Env: []string{}, Executor: exec}
	if code := comparison.Run([]string{"--json", "--color", "never", "doctor"}); code != int(ExitSuccess) {
		t.Fatalf("comparison exit=%d", code)
	}
	var alwaysEvent, neverEvent map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(jsonOut.Bytes()), &alwaysEvent); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(bytes.TrimSpace(comparisonOut.Bytes()), &neverEvent); err != nil {
		t.Fatal(err)
	}
	for _, event := range []map[string]any{alwaysEvent, neverEvent} {
		delete(event, "eventId")
		delete(event, "operationId")
		delete(event, "timestamp")
	}
	if !reflect.DeepEqual(alwaysEvent, neverEvent) {
		t.Fatalf("machine JSON semantics changed with color policy:\nalways: %#v\nnever:  %#v", alwaysEvent, neverEvent)
	}
}

func TestReceiptHumanCarriesOutcomeAndRoutingIdentity(t *testing.T) {
	got := receiptHuman(map[string]any{
		"receiptId": "rcpt_01999999-9999-7999-8999-999999999999", "operation": "send", "state": "accepted",
		"message": map[string]any{"messageId": "msg_01999999-9999-7999-8999-999999999998"},
		"target":  map[string]any{"alias": "devbox", "threadId": "01999999-9999-7999-8999-999999999997"},
	})
	for _, want := range []string{"send accepted", "message=msg_", "target=devbox/", "receipt=rcpt_"} {
		if !strings.Contains(got, want) {
			t.Fatalf("receipt output missing %q: %q", want, got)
		}
	}
}

func TestNestedCommandsAcceptGlobalColorOption(t *testing.T) {
	inv, err := Parse([]string{"endpoint", "list", "--color", "always"})
	if err != nil {
		t.Fatal(err)
	}
	if validation := validateInvocation(&App{}, inv); validation != nil {
		t.Fatalf("nested global color rejected: %v", validation)
	}
}
