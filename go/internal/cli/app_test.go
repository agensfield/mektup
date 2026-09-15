package cli

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

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
	for _, args := range [][]string{{"--state-dir", "/definitely/not/created", "--skill"}, {"--config", "/definitely/not/created", "docs", "agents"}, {"version", "--json"}, {"completion", "bash"}} {
		code, _, stderr := runTest(t, args...)
		if code != int(ExitSuccess) || stderr != "" {
			t.Fatalf("%v: code=%d stderr=%q", args, code, stderr)
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
