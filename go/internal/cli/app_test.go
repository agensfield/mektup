package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"runtime/debug"
	"strings"
	"testing"
)

type executorFunc func(context.Context, Invocation) (ExecutionResult, error)

func (f executorFunc) Execute(ctx context.Context, inv Invocation) (ExecutionResult, error) {
	return f(ctx, inv)
}

type streamingExecutor struct {
	operationID string
}

func (s streamingExecutor) Execute(context.Context, Invocation) (ExecutionResult, error) {
	return ExecutionResult{}, errors.New("streaming executor must use ExecuteStream")
}

func (s streamingExecutor) ExecuteStream(_ context.Context, _ Invocation, emit func(ExecutionResult) error) error {
	receipt := map[string]any{"receiptId": "rcpt_03999999-9999-7999-8999-999999999999", "operationId": s.operationID, "state": "accepted"}
	if err := emit(ExecutionResult{Streaming: true, Events: []OutputEvent{{Machine: map[string]any{
		"schema": EventSchema, "event": "send.accepted", "operationId": s.operationID, "terminal": false, "ok": true, "data": map[string]any{"receipt": receipt},
	}}}}); err != nil {
		return err
	}
	return emit(ExecutionResult{Streaming: true, Events: []OutputEvent{{Machine: map[string]any{
		"schema": EventSchema, "event": "reply.accepted", "operationId": s.operationID, "terminal": true, "ok": true, "data": map[string]any{"receipt": receipt},
	}}}})
}

func TestStreamingExecutorEmitsAcceptanceBeforeWaitTerminal(t *testing.T) {
	var out, errOut bytes.Buffer
	app := &App{In: strings.NewReader(""), Out: &out, Err: &errOut, Env: []string{"MEKTUP_AGENT=1"}, Executor: streamingExecutor{operationID: "op_04999999-9999-7999-8999-999999999999"}}
	if code := app.Run([]string{"send", "target", "hello", "--wait"}); code != int(ExitSuccess) {
		t.Fatalf("exit=%d stderr=%q", code, errOut.String())
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("JSONL lines=%d output=%q", len(lines), out.String())
	}
	var first, second map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &first); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(lines[1]), &second); err != nil {
		t.Fatal(err)
	}
	if first["event"] != "send.accepted" || first["terminal"] != false {
		t.Fatalf("acceptance was not a nonterminal first event: %#v", first)
	}
	if second["event"] != "reply.accepted" || second["terminal"] != true {
		t.Fatalf("terminal wait event missing: %#v", second)
	}
	if first["operationId"] != second["operationId"] {
		t.Fatalf("operation IDs diverged: %v vs %v", first["operationId"], second["operationId"])
	}
	if first["sequence"] != float64(1) || second["sequence"] != float64(2) {
		t.Fatalf("stream sequence reset: first=%v second=%v", first["sequence"], second["sequence"])
	}
}

type failFirstWriter struct{ writes int }

func (w *failFirstWriter) Write([]byte) (int, error) {
	w.writes++
	if w.writes == 1 {
		return 0, errors.New("broken output")
	}
	return 1, nil
}

func TestStreamingOutputFailureIsSticky(t *testing.T) {
	writer := &failFirstWriter{}
	app := &App{Out: writer, Err: &bytes.Buffer{}, Executor: streamingExecutor{operationID: "op_04999999-9999-7999-8999-999999999999"}}
	if code := app.Run([]string{"send", "target", "hello", "--json"}); code == int(ExitSuccess) || writer.writes != 1 {
		t.Fatalf("stream output failure was swallowed: code=%d writes=%d", code, writer.writes)
	}
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

func TestVersionReportsLockedContractRevision(t *testing.T) {
	code, stdout, stderr := runTest(t, "version", "--json")
	if code != int(ExitSuccess) || stderr != "" {
		t.Fatalf("code=%d stderr=%q", code, stderr)
	}
	var version struct {
		ContractVersion string `json:"contract_version"`
	}
	if err := json.Unmarshal([]byte(stdout), &version); err != nil {
		t.Fatal(err)
	}
	if version.ContractVersion != ContractVersion {
		t.Fatalf("contract version=%q constant=%q", version.ContractVersion, ContractVersion)
	}
	if version.ContractVersion != "1.0.6" {
		t.Fatalf("contract version=%q, want 1.0.6", version.ContractVersion)
	}
}

func TestModuleBuildInfoIdentifiesGoInstallWithoutOverridingReleaseMetadata(t *testing.T) {
	info := &debug.BuildInfo{
		Main:     debug.Module{Path: "github.com/agensfield/mektup/go", Version: "v1.0.0"},
		Settings: []debug.BuildSetting{{Key: "vcs.revision", Value: "0123456789abcdef"}},
	}
	version, commit, kind := resolveModuleBuildInfo("dev", "unknown", "source", info)
	if version != "1.0.0" || commit != "0123456789abcdef" || kind != "go-install" {
		t.Fatalf("go-install metadata = version %q commit %q kind %q", version, commit, kind)
	}
	version, commit, kind = resolveModuleBuildInfo("1.0.0", "release-commit", "github-release", info)
	if version != "1.0.0" || commit != "release-commit" || kind != "github-release" {
		t.Fatalf("release metadata was overridden = version %q commit %q kind %q", version, commit, kind)
	}
	version, _, kind = resolveModuleBuildInfo("dev", "unknown", "source", &debug.BuildInfo{Main: debug.Module{Path: "github.com/agensfield/mektup/go", Version: "(devel)"}})
	if version != "dev" || kind != "source" {
		t.Fatalf("local source build misidentified = version %q kind %q", version, kind)
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

func TestPresentationTerminatorMatchesParseBoundary(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		env        []string
		wantJSON   bool
		wantTarget string
		wantBody   string
	}{
		{
			name:       "agent trailing human remains body",
			args:       []string{"send", "target", "--", "--human"},
			env:        []string{"MEKTUP_AGENT=1"},
			wantJSON:   true,
			wantTarget: "target",
			wantBody:   "--human",
		},
		{
			name:       "trailing json remains target",
			args:       []string{"send", "--", "--json"},
			env:        []string{},
			wantTarget: "--json",
		},
		{
			name:       "explicit json before terminator wins",
			args:       []string{"--json", "send", "target", "--", "--human"},
			env:        []string{"MEKTUP_AGENT=1"},
			wantJSON:   true,
			wantTarget: "target",
			wantBody:   "--human",
		},
		{
			name:       "explicit human before terminator wins",
			args:       []string{"--human", "send", "target", "--", "--json"},
			env:        []string{"MEKTUP_AGENT=1"},
			wantTarget: "target",
			wantBody:   "--json",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var seen Invocation
			executor := executorFunc(func(_ context.Context, inv Invocation) (ExecutionResult, error) {
				seen = inv
				return ExecutionResult{
					Events:  []OutputEvent{{Machine: map[string]any{"event": "send.completed", "ok": true}, Human: "human result"}},
					Receipt: map[string]any{"receiptId": "rcpt_03999999-9999-7999-8999-999999999999", "state": "accepted"},
				}, nil
			})
			var out, errOut bytes.Buffer
			app := &App{In: strings.NewReader(""), Out: &out, Err: &errOut, Env: test.env, Executor: executor}
			if code := app.Run(test.args); code != int(ExitSuccess) {
				t.Fatalf("exit=%d stdout=%q stderr=%q", code, out.String(), errOut.String())
			}
			if len(seen.Position) < 1 || seen.Position[0] != test.wantTarget {
				t.Fatalf("position=%#v, want target %q", seen.Position, test.wantTarget)
			}
			if test.wantBody != "" && (len(seen.Position) < 2 || seen.Position[1] != test.wantBody) {
				t.Fatalf("position=%#v, want body %q", seen.Position, test.wantBody)
			}
			if test.wantJSON {
				var event map[string]any
				if err := json.Unmarshal([]byte(strings.TrimSpace(out.String())), &event); err != nil {
					t.Fatalf("JSON presentation lost after terminator: %q: %v", out.String(), err)
				}
				data, ok := event["data"].(map[string]any)
				if !ok || data["receipt"] == nil {
					t.Fatalf("JSONL receipt missing from post-write result: %#v", event)
				}
			} else if !strings.Contains(out.String(), "human result") {
				t.Fatalf("human presentation was not retained: %q", out.String())
			}
		})
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

func TestHumanCommandDocsAndInspectHelpAreNavigable(t *testing.T) {
	code, stdout, stderr := runTest(t, "--human", "docs", "commands")
	if code != int(ExitSuccess) || stderr != "" || !strings.Contains(stdout, "Mektup commands (contract "+ContractVersion+")") || !strings.Contains(stdout, "usage: mektup inspect <target>") || strings.Contains(stdout, `"commands"`) {
		t.Fatalf("human command docs code=%d stdout=%q stderr=%q", code, stdout, stderr)
	}
	code, stdout, stderr = runTest(t, "help", "inspect")
	for _, want := range []string{"codex://local/thread/<thread-uuid>", "--receipts 0", "live Herdr agent name"} {
		if code != int(ExitSuccess) || stderr != "" || !strings.Contains(stdout, want) {
			t.Fatalf("inspect help missing %q: code=%d stdout=%q stderr=%q", want, code, stdout, stderr)
		}
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
	if len(lines) != 1 {
		t.Fatalf("expected one terminal event containing receipt, got %q", out.String())
	}
	var event map[string]any
	if err := json.Unmarshal([]byte(lines[0]), &event); err != nil {
		t.Fatal(err)
	}
	if event["schema"] != EventSchema || event["terminal"] != true {
		t.Fatalf("invalid lifecycle event: %#v", event)
	}
	data, _ := event["data"].(map[string]any)
	if data["receipt"] == nil {
		t.Fatalf("receipt was not embedded: %#v", event)
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
	if out.Len() != 0 || !strings.Contains(errOut.String(), "unable to allocate operation identity") {
		t.Fatalf("entropy failure emitted malformed machine output: stdout=%q stderr=%q", out.String(), errOut.String())
	}
}

func TestOperationalJSONLHasContiguousTerminalLifecycleAndEmbeddedReceipt(t *testing.T) {
	var out, errOut bytes.Buffer
	a := &App{In: strings.NewReader(""), Out: &out, Err: &errOut, Env: []string{}, Executor: executorFunc(func(context.Context, Invocation) (ExecutionResult, error) {
		return ExecutionResult{
			Events: []OutputEvent{
				{Machine: map[string]any{"event": "send.accepted", "terminal": false, "ok": true, "data": map[string]any{}}},
				{Machine: map[string]any{"event": "reply.accepted", "terminal": true, "ok": true, "data": map[string]any{}}},
			},
			Receipt: map[string]any{"schema": "mektup/receipt/v1", "receiptId": "rcpt_test", "state": "accepted"},
		}, nil
	})}
	if code := a.Run([]string{"--json", "send", "target", "message"}); code != int(ExitSuccess) {
		t.Fatalf("code=%d stderr=%q", code, errOut.String())
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected two lifecycle lines, got %q", out.String())
	}
	for index, line := range lines {
		var event map[string]any
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatalf("line %d: %v", index, err)
		}
		for _, field := range []string{"schema", "event", "eventId", "sequence", "operationId", "timestamp", "terminal", "ok", "warnings", "data"} {
			if _, ok := event[field]; !ok {
				t.Fatalf("line %d missing %s: %#v", index, field, event)
			}
		}
		if event["schema"] != EventSchema || int(event["sequence"].(float64)) != index+1 {
			t.Fatalf("line %d has invalid schema/sequence: %#v", index, event)
		}
		if index == 0 && event["terminal"] != false {
			t.Fatalf("first event unexpectedly terminal: %#v", event)
		}
		if index == 1 {
			if event["terminal"] != true {
				t.Fatalf("last event not terminal: %#v", event)
			}
			data := event["data"].(map[string]any)
			if data["receipt"] == nil {
				t.Fatalf("receipt not in terminal data: %#v", event)
			}
		}
	}
	var first, second map[string]any
	_ = json.Unmarshal([]byte(lines[0]), &first)
	_ = json.Unmarshal([]byte(lines[1]), &second)
	if first["operationId"] != second["operationId"] {
		t.Fatalf("operation ID changed across events: %v vs %v", first["operationId"], second["operationId"])
	}
	if second["event"] != "reply.accepted" {
		t.Fatalf("receipt embedding erased terminal event: %#v", second)
	}
}

func TestOperationalJSONLRejectsLinesAfterTerminal(t *testing.T) {
	var out, errOut bytes.Buffer
	a := &App{In: strings.NewReader(""), Out: &out, Err: &errOut, Env: []string{}, Executor: executorFunc(func(context.Context, Invocation) (ExecutionResult, error) {
		return ExecutionResult{Events: []OutputEvent{
			{Machine: map[string]any{"event": "done", "terminal": true, "ok": true}},
			{Machine: map[string]any{"event": "late", "terminal": true, "ok": true}},
		}}, nil
	})}
	if code := a.Run([]string{"--json", "inspect", "target"}); code != int(ExitInternal) || out.Len() != 0 || !strings.Contains(errOut.String(), "after a terminal event") {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, out.String(), errOut.String())
	}
}

func TestHelpJSONUsesOfflineSchema(t *testing.T) {
	code, stdout, _ := runTest(t, "--json", "help")
	if code != int(ExitSuccess) {
		t.Fatalf("code=%d", code)
	}
	var document map[string]any
	if err := json.Unmarshal([]byte(stdout), &document); err != nil {
		t.Fatal(err)
	}
	if document["schema"] != "mektup/help/v1" {
		t.Fatalf("help used lifecycle schema: %#v", document)
	}
}

func TestOperationalJSONLRejectsConflictingOperationIDs(t *testing.T) {
	op1, _ := defaultID("op_")
	op2, _ := defaultID("op_")
	var out, errOut bytes.Buffer
	a := &App{In: strings.NewReader(""), Out: &out, Err: &errOut, Env: []string{}, Executor: executorFunc(func(context.Context, Invocation) (ExecutionResult, error) {
		return ExecutionResult{Events: []OutputEvent{
			{Machine: map[string]any{"event": "one", "operationId": op1}},
			{Machine: map[string]any{"event": "two", "operationId": op2}},
		}}, nil
	})}
	if code := a.Run([]string{"--json", "inspect", "target"}); code != int(ExitInternal) || out.Len() != 0 || !strings.Contains(errOut.String(), "conflicting operation IDs") {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, out.String(), errOut.String())
	}
}

func TestStructuredErrorsAlwaysCarryEffectState(t *testing.T) {
	code, stdout, _ := runTest(t, "--json", "send", "target", "message", "--raw", "--wait")
	if code != int(ExitUsage) {
		t.Fatalf("code=%d", code)
	}
	var event map[string]any
	if err := json.Unmarshal([]byte(stdout), &event); err != nil {
		t.Fatal(err)
	}
	errorData := event["data"].(map[string]any)["error"].(map[string]any)
	if errorData["effectState"] != "not_sent" {
		t.Fatalf("usage error effect state=%#v", errorData["effectState"])
	}
}

func TestRunContextReachesOperationalExecutor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	seenCanceled := false
	var out, errOut bytes.Buffer
	a := &App{In: strings.NewReader(""), Out: &out, Err: &errOut, Env: []string{}, Executor: executorFunc(func(callCtx context.Context, _ Invocation) (ExecutionResult, error) {
		seenCanceled = errors.Is(callCtx.Err(), context.Canceled)
		return ExecutionResult{}, &Error{Code: "wait_interrupted", Message: "wait canceled", Effect: "accepted", Exit: ExitIncomplete}
	})}
	if code := a.RunContext(ctx, []string{"--human", "wait", "msg_test"}); code != int(ExitIncomplete) {
		t.Fatalf("code=%d stderr=%q", code, errOut.String())
	}
	if !seenCanceled {
		t.Fatal("executor did not receive caller cancellation")
	}
}
