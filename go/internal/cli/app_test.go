package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

type executorFunc func(context.Context, Invocation) (ExecutionResult, error)

func (f executorFunc) Execute(ctx context.Context, inv Invocation) (ExecutionResult, error) {
	return f(ctx, inv)
}

func runTest(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var out, err bytes.Buffer
	a := &App{In: strings.NewReader(""), Out: &out, Err: &err, Env: []string{}}
	code := a.Run(args)
	return code, out.String(), err.String()
}

func TestSkillAndDocsAgentsAreByteEquivalent(t *testing.T) {
	_, skill, skillErr := runTest(t, "--skill")
	_, docs, docsErr := runTest(t, "docs", "agents")
	if skillErr != "" || docsErr != "" {
		t.Fatalf("offline docs wrote stderr: skill=%q docs=%q", skillErr, docsErr)
	}
	if strings.TrimRight(skill, "\n") != strings.TrimRight(docs, "\n") {
		t.Fatal("--skill and docs agents diverged")
	}
}

func TestOfflineSurfacesDoNotNeedState(t *testing.T) {
	for _, args := range [][]string{{"--skill"}, {"docs", "agents"}, {"version", "--json"}, {"completion", "bash"}} {
		code, _, stderr := runTest(t, args...)
		if code != int(ExitSuccess) || stderr != "" {
			t.Fatalf("%v: code=%d stderr=%q", args, code, stderr)
		}
	}
}

func TestOfflineSurfacesRejectOperationalOverrides(t *testing.T) {
	for _, args := range [][]string{{"--state-dir", "/tmp/state", "--skill"}, {"--config", "/tmp/config", "docs", "agents"}, {"--endpoint", "remote", "docs", "commands"}, {"--audit", "docs", "receipts"}, {"--debug", "version"}} {
		code, _, _ := runTest(t, args...)
		if code != int(ExitUsage) {
			t.Fatalf("%v: got exit %d", args, code)
		}
	}
}

func TestPresentationPrecedenceAndConservativeDetection(t *testing.T) {
	if p, _ := DetectPresentation(false, true, map[string]string{"MEKTUP_AGENT": "1", "CODEX_THREAD_ID": "thread"}); p != PresentationHuman {
		t.Fatalf("explicit human did not win: %q", p)
	}
	if p, _ := DetectPresentation(false, false, map[string]string{"MEKTUP_AGENT": "1", "CODEX_THREAD_ID": "thread"}); p != PresentationJSON {
		t.Fatalf("agent marker did not select JSON: %q", p)
	}
	if p, _ := DetectPresentation(false, false, map[string]string{"CODEX_THREAD_ID": "thread"}); p != PresentationJSON {
		t.Fatalf("thread marker did not select JSON: %q", p)
	}
	if p, _ := DetectPresentation(false, false, map[string]string{"CI": "1", "HERDR_ENV": "dev"}); p != PresentationHuman {
		t.Fatalf("generic markers selected agent output: %q", p)
	}
}

func TestPayloadSourcesAndRawReplyGates(t *testing.T) {
	cases := [][]string{
		{"send", "target", "message", "--stdin"},
		{"send", "target", "--stdin", "--file", "body"},
		{"send", "target", "message", "--raw", "--wait"},
		{"send", "target", "message", "--raw", "--request-reply"},
		{"rpc", "method", "--params", "{}", "--params-file", "params.json"},
	}
	for _, args := range cases {
		code, stdout, stderr := runTest(t, append([]string{"--json"}, args...)...)
		if code != int(ExitUsage) || stderr != "" {
			t.Fatalf("%v: code=%d stderr=%q", args, code, stderr)
		}
		var event map[string]any
		if err := json.Unmarshal([]byte(stdout), &event); err != nil {
			t.Fatalf("%v produced non-JSON output %q: %v", args, stdout, err)
		}
		if event["schema"] != EventSchema {
			t.Fatalf("%v schema=%v", args, event["schema"])
		}
	}
}

func TestJSONLFailureKeepsStderrQuietAndInternalIsNotSuccess(t *testing.T) {
	code, stdout, stderr := runTest(t, "--json", "send", "target", "message")
	if code != int(ExitInternal) || stderr != "" {
		t.Fatalf("code=%d stderr=%q", code, stderr)
	}
	var event map[string]any
	if err := json.Unmarshal([]byte(stdout), &event); err != nil {
		t.Fatal(err)
	}
	if event["terminal"] != true || event["ok"] != false {
		t.Fatalf("non-terminal/ok failure event: %#v", event)
	}
}

func TestDocsRejectMixedOperationalDispatch(t *testing.T) {
	for _, args := range [][]string{{"docs", "agents", "send"}, {"--skill", "send"}, {"docs", "agents", "--file", "secret"}} {
		code, _, _ := runTest(t, args...)
		if code != int(ExitUsage) {
			t.Fatalf("%v: got exit %d", args, code)
		}
	}
}

func TestCommandContractClassifiesCommands(t *testing.T) {
	contract := commandContract()
	if contract.Schema != CommandSchema || len(contract.Commands) < 10 {
		t.Fatalf("unexpected contract: %#v", contract)
	}
	for _, command := range contract.Commands {
		if command.Name == "" || len(command.Effects) == 0 || command.RetrySafety == "" || command.Availability == "" {
			t.Fatalf("incomplete metadata: %#v", command)
		}
	}
}

func TestDocsCommandsJSONIsOneMachineLine(t *testing.T) {
	code, stdout, stderr := runTest(t, "--json", "docs", "commands")
	if code != int(ExitSuccess) || stderr != "" || strings.Count(stdout, "\n") != 1 {
		t.Fatalf("code=%d stderr=%q lines=%q", code, stderr, stdout)
	}
	var document commandContractDocument
	if err := json.Unmarshal([]byte(stdout), &document); err != nil {
		t.Fatal(err)
	}
	if document.Schema != CommandSchema {
		t.Fatalf("schema=%q", document.Schema)
	}
}

func TestExecutorResultStreamsProvidedOutputAndReceipt(t *testing.T) {
	var out, errOut bytes.Buffer
	var got Invocation
	a := &App{In: strings.NewReader(""), Out: &out, Err: &errOut, Env: []string{"MEKTUP_AGENT=1"}}
	a.Executor = executorFunc(func(_ context.Context, inv Invocation) (ExecutionResult, error) {
		got = inv
		return ExecutionResult{Events: []OutputEvent{{Machine: map[string]any{"schema": EventSchema, "event": "read.completed", "terminal": true, "ok": true}}}, Receipt: map[string]any{"schema": "mektup/receipt/v1", "state": "accepted"}}, nil
	})
	if code := a.Run([]string{"inspect", "target"}); code != int(ExitSuccess) {
		t.Fatalf("code=%d stderr=%q", code, errOut.String())
	}
	if got.Resolved.Endpoint == "" || got.Resolved.Config == "" || got.Resolved.StateDir == "" {
		t.Fatalf("globals were not resolved: %#v", got.Resolved)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected event and receipt, got %q", out.String())
	}
}

func TestEmptyExecutorResultFailsInternal(t *testing.T) {
	var out, errOut bytes.Buffer
	a := &App{In: strings.NewReader(""), Out: &out, Err: &errOut, Env: []string{}, Executor: executorFunc(func(context.Context, Invocation) (ExecutionResult, error) { return ExecutionResult{}, nil })}
	if code := a.Run([]string{"inspect", "target"}); code != int(ExitInternal) || !strings.Contains(errOut.String(), "no human output") {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, out.String(), errOut.String())
	}
}

func TestResolvedEnvironmentAndFlagPrecedence(t *testing.T) {
	var got Invocation
	var out bytes.Buffer
	a := &App{In: strings.NewReader(""), Out: &out, Err: &bytes.Buffer{}, Env: []string{
		"MEKTUP_OUTPUT=json", "MEKTUP_ENDPOINT=env-endpoint", "MEKTUP_CONFIG=env-config", "MEKTUP_STATE_DIR=env-state",
	}, Executor: executorFunc(func(_ context.Context, inv Invocation) (ExecutionResult, error) {
		got = inv
		return ExecutionResult{Events: []OutputEvent{{Machine: map[string]any{"ok": true}, Human: "inspection complete"}}}, nil
	})}
	if code := a.Run([]string{"--human", "--endpoint", "flag-endpoint", "inspect", "target"}); code != int(ExitSuccess) {
		t.Fatalf("code=%d", code)
	}
	if got.Resolved.Output != PresentationHuman || got.Resolved.Endpoint != "flag-endpoint" || got.Resolved.Config != "env-config" || got.Resolved.StateDir != "env-state" {
		t.Fatalf("unexpected precedence: %#v", got.Resolved)
	}
}

func TestUUIDv7GeneratorAndEntropyFailure(t *testing.T) {
	id, err := defaultID("evt_")
	if err != nil || !strings.HasPrefix(id, "evt_") {
		t.Fatalf("id=%q err=%v", id, err)
	}
	uuid := strings.TrimPrefix(id, "evt_")
	parts := strings.Split(uuid, "-")
	if len(parts) != 5 || len(parts[2]) < 1 || parts[2][0] != '7' || len(parts[3]) < 1 || (parts[3][0] != '8' && parts[3][0] != '9' && parts[3][0] != 'a' && parts[3][0] != 'b') {
		t.Fatalf("not UUIDv7: %q", id)
	}

	var out, errOut bytes.Buffer
	a := &App{In: strings.NewReader(""), Out: &out, Err: &errOut, Env: []string{"MEKTUP_AGENT=1"}, ID: func(string) (string, error) { return "", errors.New("entropy unavailable") }}
	if code := a.Run([]string{"send", "target", "message"}); code != int(ExitInternal) {
		t.Fatalf("entropy failure code=%d", code)
	}
	if strings.Contains(out.String(), "00000000-0000-4000-8000") {
		t.Fatalf("fixed UUID fallback leaked: %q", out.String())
	}
}
