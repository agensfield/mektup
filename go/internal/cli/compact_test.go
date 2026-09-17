package cli

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/agensfield/mektup/go/internal/journal"
)

type compactTestExecutor struct {
	result    ExecutionResult
	err       error
	retained  []byte
	retainErr error
	seen      Invocation
	artifact  *CompactArtifact
}

type compactStreamExecutor struct {
	compactTestExecutor
	chunks  []ExecutionResult
	emitted int
}

func (e *compactStreamExecutor) ExecuteStream(_ context.Context, _ Invocation, emit func(ExecutionResult) error) error {
	for _, chunk := range e.chunks {
		e.emitted++
		if err := emit(chunk); err != nil {
			return err
		}
	}
	return nil
}

func (e *compactTestExecutor) Execute(_ context.Context, inv Invocation) (ExecutionResult, error) {
	e.seen = inv
	return e.result, e.err
}

func (e *compactTestExecutor) RetainCompact(_ context.Context, _ Invocation, document []byte) (CompactArtifact, error) {
	e.retained = append([]byte(nil), document...)
	if e.retainErr != nil {
		return CompactArtifact{}, e.retainErr
	}
	if e.artifact != nil {
		return *e.artifact, nil
	}
	digest := sha256.Sum256(document)
	return CompactArtifact{Path: "compact-output-test.jsonl", Bytes: int64(len(document)), SHA256: "sha256:" + hex.EncodeToString(digest[:])}, nil
}

func TestImplicitAgentCompactUsesBoundedActionablePage(t *testing.T) {
	executor := &compactTestExecutor{result: ExecutionResult{Events: []OutputEvent{{Machine: map[string]any{
		"event": "thread.list.completed", "terminal": true, "ok": true,
		"data": map[string]any{"resultKind": "thread", "endpointId": "ep_test", "nextCursor": "opaque-next", "data": []any{
			map[string]any{"id": "thread-1", "name": strings.Repeat("界", 700), "turns": []any{map[string]any{"id": "turn-1"}}},
		}},
	}}}}}
	var out, errOut bytes.Buffer
	app := &App{Out: &out, Err: &errOut, Env: []string{"CODEX_THREAD_ID=source"}, Executor: executor}
	if code := app.Run([]string{"thread", "list"}); code != int(ExitSuccess) {
		t.Fatalf("code=%d stderr=%q", code, errOut.String())
	}
	var event map[string]any
	if err := json.Unmarshal(out.Bytes(), &event); err != nil {
		t.Fatal(err)
	}
	if event["presentation"] != "compact" {
		t.Fatalf("presentation=%#v", event["presentation"])
	}
	data := event["data"].(map[string]any)
	if data["count"] != float64(1) || data["requestedLimit"] != nil || data["effectiveLimit"] != float64(10) || data["hasMore"] != true || data["nextCursor"] != "opaque-next" {
		t.Fatalf("page metadata=%#v", data)
	}
	row := data["data"].([]any)[0].(map[string]any)
	if row["endpointId"] != "ep_test" || row["uri"] != "codex://ep_test/thread/thread-1" || row["turnCount"] != float64(1) {
		t.Fatalf("row locator=%#v", row)
	}
	preview := row["name"].(map[string]any)
	if preview["chars"] != float64(512) || preview["sourceChars"] != float64(700) || preview["truncated"] != true {
		t.Fatalf("preview=%#v", preview)
	}
}

func TestCompactOverflowRetainsCompleteRecordAndEmitsBoundedTruth(t *testing.T) {
	huge := strings.Repeat("x", CompactMaxOutputBytes+4096)
	executor := &compactTestExecutor{result: ExecutionResult{Events: []OutputEvent{{Machine: map[string]any{
		"event": "endpoint.list.completed", "terminal": true, "ok": true,
		"data": map[string]any{"resultKind": "endpoint", "data": []any{map[string]any{"id": "ep_test", "unbounded": huge}}},
	}}}}}
	var out, errOut bytes.Buffer
	app := &App{Out: &out, Err: &errOut, Env: []string{"MEKTUP_AGENT=1"}, Executor: executor}
	if code := app.Run([]string{"endpoint", "list"}); code != int(ExitIncomplete) {
		t.Fatalf("code=%d stderr=%q", code, errOut.String())
	}
	if out.Len() > CompactMaxOutputBytes {
		t.Fatalf("compact record bytes=%d", out.Len())
	}
	if !bytes.Contains(executor.retained, []byte(huge)) {
		t.Fatal("complete record was not retained")
	}
	var event map[string]any
	if err := json.Unmarshal(out.Bytes(), &event); err != nil {
		t.Fatal(err)
	}
	if event["event"] != "compact_output_too_large" || event["presentation"] != "compact" || event["terminal"] != true {
		t.Fatalf("fallback=%#v", event)
	}
	errorData := event["data"].(map[string]any)["error"].(map[string]any)
	if errorData["effectState"] != "accepted" || errorData["retryable"] != false {
		t.Fatalf("fallback error=%#v", errorData)
	}
	artifact := errorData["details"].(map[string]any)["artifact"].(map[string]any)
	if artifact["path"] != "compact-output-test.jsonl" {
		t.Fatalf("artifact=%#v", artifact)
	}
}

func TestCompactOverflowRejectsFalseArtifactReceipt(t *testing.T) {
	huge := strings.Repeat("x", CompactMaxOutputBytes+4096)
	invalid := CompactArtifact{Path: "claimed.jsonl", Bytes: 1, SHA256: "sha256:" + strings.Repeat("0", 64)}
	executor := &compactTestExecutor{artifact: &invalid, result: ExecutionResult{Events: []OutputEvent{{Machine: map[string]any{
		"event": "endpoint.list.completed", "terminal": true, "ok": true,
		"data": map[string]any{"resultKind": "endpoint", "data": []any{map[string]any{"id": "ep_test", "unbounded": huge}}},
	}}}}}
	var out bytes.Buffer
	app := &App{Out: &out, Err: &bytes.Buffer{}, Env: []string{"MEKTUP_AGENT=1"}, Executor: executor}
	if code := app.Run([]string{"endpoint", "list"}); code != int(ExitIncomplete) {
		t.Fatalf("code=%d", code)
	}
	if bytes.Contains(out.Bytes(), []byte("claimed.jsonl")) {
		t.Fatalf("false artifact was advertised: %s", out.String())
	}
	if !bytes.Contains(out.Bytes(), []byte("retentionFailure")) {
		t.Fatalf("retention failure missing: %s", out.String())
	}
}

func TestExplicitJSONPreservesFullPageAndNoCompactMarker(t *testing.T) {
	executor := &compactTestExecutor{result: ExecutionResult{Events: []OutputEvent{{Machine: map[string]any{
		"event": "thread.list.completed", "terminal": true, "ok": true,
		"data": map[string]any{"resultKind": "thread", "data": []any{map[string]any{"id": "thread-1", "name": "full"}}},
	}}}}}
	var out bytes.Buffer
	app := &App{Out: &out, Err: &bytes.Buffer{}, Env: []string{"CODEX_THREAD_ID=source"}, Executor: executor}
	if code := app.Run([]string{"--json", "thread", "list"}); code != int(ExitSuccess) {
		t.Fatalf("code=%d", code)
	}
	var event map[string]any
	if err := json.Unmarshal(out.Bytes(), &event); err != nil {
		t.Fatal(err)
	}
	if _, ok := event["presentation"]; ok {
		t.Fatalf("explicit JSON was compacted: %#v", event)
	}
	row := event["data"].(map[string]any)["data"].([]any)[0].(map[string]any)
	if row["name"] != "full" {
		t.Fatalf("full row=%#v", row)
	}
}

func TestExplicitJSONMayExceedCompactBoundWithoutProjection(t *testing.T) {
	huge := strings.Repeat("x", CompactMaxOutputBytes+4096)
	executor := &compactTestExecutor{result: ExecutionResult{Events: []OutputEvent{{Machine: map[string]any{
		"event": "rpc.completed", "terminal": true, "ok": true, "data": map[string]any{"result": huge},
	}}}}}
	var out bytes.Buffer
	app := &App{Out: &out, Err: &bytes.Buffer{}, Env: []string{"CODEX_THREAD_ID=source"}, Executor: executor}
	if code := app.Run([]string{"--json", "rpc", "thread/read"}); code != int(ExitSuccess) {
		t.Fatalf("code=%d", code)
	}
	if out.Len() <= CompactMaxOutputBytes || !bytes.Contains(out.Bytes(), []byte(huge)) || len(executor.retained) != 0 {
		t.Fatalf("explicit full output was projected or retained: bytes=%d retained=%d", out.Len(), len(executor.retained))
	}
}

func TestCompactSelectionPrecedenceDefaultsAndExactSurface(t *testing.T) {
	result := ExecutionResult{Events: []OutputEvent{{Machine: map[string]any{"event": "thread.list.completed", "terminal": true, "ok": true, "data": map[string]any{"data": []any{}}}}}}
	cases := []struct {
		name      string
		env       []string
		args      []string
		compact   bool
		limit     string
		wantUsage bool
	}{
		{name: "implicit agent", env: []string{"CODEX_THREAD_ID=source"}, args: []string{"thread", "list"}, compact: true, limit: "10"},
		{name: "explicit full", env: []string{"CODEX_THREAD_ID=source"}, args: []string{"--json", "thread", "list"}, compact: false},
		{name: "environment full", env: []string{"CODEX_THREAD_ID=source", "MEKTUP_OUTPUT=json"}, args: []string{"thread", "list"}, compact: false},
		{name: "flag overrides environment", env: []string{"MEKTUP_OUTPUT=json"}, args: []string{"--compact", "thread", "list", "--limit", "3"}, compact: true, limit: "3"},
		{name: "page zero uses compact default", args: []string{"--compact", "thread", "list", "--limit", "0"}, compact: true, limit: "10"},
		{name: "exact full view bypass", env: []string{"CODEX_THREAD_ID=source"}, args: []string{"thread", "turns", "thread-1", "--view", "full"}, compact: false},
		{name: "portable bypass", env: []string{"CODEX_THREAD_ID=source"}, args: []string{"receipt", "show", "rcpt_test", "--portable"}, compact: false},
		{name: "content bypass", env: []string{"CODEX_THREAD_ID=source"}, args: []string{"receipt", "show", "rcpt_test", "--content"}, compact: false},
		{name: "explicit compact exact conflict", args: []string{"--compact", "thread", "turns", "thread-1", "--view", "full"}, wantUsage: true},
		{name: "explicit compact portable conflict", args: []string{"--compact", "receipt", "show", "rcpt_test", "--portable"}, wantUsage: true},
		{name: "compact max", args: []string{"--compact", "thread", "list", "--limit", "26"}, wantUsage: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			executor := &compactTestExecutor{result: result}
			var out bytes.Buffer
			app := &App{Out: &out, Err: &bytes.Buffer{}, Env: tc.env, Executor: executor}
			code := app.Run(tc.args)
			if tc.wantUsage {
				if code != int(ExitUsage) || executor.seen.Command != "" {
					t.Fatalf("code=%d seen=%#v", code, executor.seen)
				}
				return
			}
			if code != int(ExitSuccess) || executor.seen.Resolved.Compact != tc.compact || executor.seen.Option("limit") != tc.limit {
				t.Fatalf("code=%d compact=%v limit=%q", code, executor.seen.Resolved.Compact, executor.seen.Option("limit"))
			}
		})
	}
}

func TestCompactInvalidUTF8IsTypedAndNeverReplaced(t *testing.T) {
	bad := string([]byte{'o', 'k', 0xff})
	executor := &compactTestExecutor{result: ExecutionResult{Events: []OutputEvent{{Machine: map[string]any{
		"event": "thread.list.completed", "terminal": true, "ok": true,
		"data": map[string]any{"data": []any{map[string]any{"id": "thread-1", "name": bad}}},
	}}}}}
	var out bytes.Buffer
	app := &App{Out: &out, Err: &bytes.Buffer{}, Env: []string{"MEKTUP_AGENT=1"}, Executor: executor}
	if code := app.Run([]string{"thread", "list"}); code != int(ExitIncomplete) {
		t.Fatalf("code=%d", code)
	}
	if bytes.Contains(out.Bytes(), []byte{0xff}) || bytes.Contains(out.Bytes(), []byte("\\ufffd")) {
		t.Fatalf("invalid source leaked/replaced: %q", out.String())
	}
	var event map[string]any
	if err := json.Unmarshal(out.Bytes(), &event); err != nil {
		t.Fatal(err)
	}
	errorData := event["data"].(map[string]any)["error"].(map[string]any)
	if errorData["code"] != "invalid_utf8" || errorData["effectState"] != "accepted" {
		t.Fatalf("error=%#v", errorData)
	}
}

func TestOfflineCompactUsesStructuredDocsButRejectsExactTextSurfaces(t *testing.T) {
	for _, args := range [][]string{{"--compact", "help"}, {"--compact", "version"}, {"--compact", "docs", "agents"}} {
		code, stdout, _ := runTest(t, args...)
		if code != int(ExitSuccess) || !json.Valid([]byte(stdout)) {
			t.Fatalf("args=%v code=%d stdout=%q", args, code, stdout)
		}
		var document map[string]any
		_ = json.Unmarshal([]byte(stdout), &document)
		if _, ok := document["presentation"]; ok {
			t.Fatalf("offline compact added operational marker: %#v", document)
		}
	}
	for _, args := range [][]string{{"--compact", "--skill"}, {"--compact", "completion", "zsh"}} {
		code, _, _ := runTest(t, args...)
		if code != int(ExitUsage) {
			t.Fatalf("exact text args=%v code=%d", args, code)
		}
	}
}

func TestCompactPreviewAndScopedSearchLocatorPreserveOriginalBasis(t *testing.T) {
	text := strings.Repeat("🙂", 700)
	preview, sourceChars, sourceBytes, chars, bytesCount, truncated := boundedPreview(text)
	if sourceChars != 700 || sourceBytes != 2800 || chars != 512 || bytesCount != 2048 || !truncated || !json.Valid([]byte(`"`+preview+`"`)) {
		t.Fatalf("preview chars/bytes=%d/%d source=%d/%d truncated=%v", chars, bytesCount, sourceChars, sourceBytes, truncated)
	}
	event := compactLifecycleEvent(Invocation{Resolved: ResolvedGlobals{Compact: true}, Options: map[string][]string{"limit": {"10"}}}, map[string]any{
		"event": "search.completed", "data": map[string]any{
			"resultKind": "message", "endpointId": "ep_stable", "threadId": "thread-1", "data": []any{map[string]any{
				"turnId": "turn-1", "itemId": "item-1", "snippet": "hello", "snippetMatchRange": map[string]any{"start": 1, "end": 3},
			}},
		},
	})
	row := event["data"].(map[string]any)["data"].([]any)[0].(map[string]any)
	locator := row["historyLocator"].(map[string]any)
	if locator["endpointId"] != "ep_stable" || locator["threadId"] != "thread-1" || locator["turnId"] != "turn-1" || locator["itemId"] != "item-1" || row["rangeBasis"] != "original-utf16" || row["snippetMatchRange"] == nil {
		t.Fatalf("scoped row=%#v", row)
	}
}

func TestCompactErrorRetainsTruncatedDetailsWithoutChangingUsageExit(t *testing.T) {
	huge := strings.Repeat("detail", 500)
	executor := &compactTestExecutor{err: &Error{Code: "invalid_arguments", Message: "bad request", Effect: "not_sent", Exit: ExitUsage, Details: map[string]any{"cause": huge}}}
	var out bytes.Buffer
	app := &App{Out: &out, Err: &bytes.Buffer{}, Env: []string{"MEKTUP_AGENT=1"}, Executor: executor}
	if code := app.Run([]string{"thread", "list"}); code != int(ExitUsage) {
		t.Fatalf("code=%d output=%s", code, out.String())
	}
	if !bytes.Contains(executor.retained, []byte(huge)) {
		t.Fatal("complete error detail was not retained")
	}
	var event map[string]any
	if err := json.Unmarshal(out.Bytes(), &event); err != nil {
		t.Fatal(err)
	}
	data := event["data"].(map[string]any)
	if data["retainedDetails"] == nil {
		t.Fatalf("retention locator missing: %#v", data)
	}
	details := data["error"].(map[string]any)["details"].(map[string]any)
	preview := details["cause"].(map[string]any)
	if preview["truncated"] != true || preview["sourceChars"].(float64) <= preview["chars"].(float64) {
		t.Fatalf("detail preview=%#v", preview)
	}
}

func TestCompactLongPrimaryErrorIsRetained(t *testing.T) {
	huge := strings.Repeat("x", 3000) + "IMPORTANT END"
	executor := &compactTestExecutor{err: &Error{Code: "endpoint_unavailable", Message: huge, Effect: "not_sent", Exit: ExitRejected}}
	var out bytes.Buffer
	app := &App{Out: &out, Err: &bytes.Buffer{}, Env: []string{"MEKTUP_AGENT=1"}, Executor: executor}
	if code := app.Run([]string{"thread", "list"}); code != int(ExitRejected) {
		t.Fatalf("code=%d", code)
	}
	if !bytes.Contains(out.Bytes(), []byte("IMPORTANT END")) && !bytes.Contains(executor.retained, []byte("IMPORTANT END")) {
		t.Fatalf("primary error evidence lost: %s", out.String())
	}
}

func TestCompactRetainedRecordKeepsExactJSONNumbers(t *testing.T) {
	executor := &compactTestExecutor{result: ExecutionResult{Events: []OutputEvent{{Machine: map[string]any{
		"event": "rpc.completed", "ok": true, "terminal": true,
		"data": map[string]any{"result": map[string]any{"n": json.Number("9007199254740993"), "text": strings.Repeat("x", CompactMaxOutputBytes)}},
	}}}}}
	app := &App{Out: &bytes.Buffer{}, Err: &bytes.Buffer{}, Env: []string{"MEKTUP_AGENT=1"}, Executor: executor}
	_ = app.Run([]string{"rpc", "thread/read"})
	if !bytes.Contains(executor.retained, []byte("9007199254740993")) || bytes.Contains(executor.retained, []byte("9007199254740992")) {
		t.Fatalf("retained exact JSON number changed: %s", executor.retained)
	}
}

func TestCompactDecodeFailureKeepsAcceptedReceiptLocator(t *testing.T) {
	executor := &compactTestExecutor{result: ExecutionResult{
		Receipt: map[string]any{"receiptId": "rcpt_01999999-9999-7999-8999-999999999999", "operationId": "op_01999999-9999-7999-8999-999999999999", "state": "accepted"},
		Events:  []OutputEvent{{Machine: map[string]any{"event": "send.accepted", "ok": true, "terminal": true, "data": map[string]any{"bad": "\xff"}}}},
	}}
	var out bytes.Buffer
	app := &App{Out: &out, Err: &bytes.Buffer{}, Env: []string{"MEKTUP_AGENT=1"}, Executor: executor}
	if code := app.Run([]string{"send", "target", "hello"}); code != int(ExitIncomplete) {
		t.Fatalf("code=%d", code)
	}
	if !bytes.Contains(out.Bytes(), []byte("rcpt_01999999")) {
		t.Fatalf("receipt locator missing: %s", out.String())
	}
}

func TestCompactBatchDecodeFailureKeepsKnownReceipt(t *testing.T) {
	executor := &compactTestExecutor{result: ExecutionResult{
		Receipt: map[string]any{"receiptId": "rcpt_01999999-9999-7999-8999-999999999999", "operationId": "op_01999999-9999-7999-8999-999999999999", "state": "accepted"},
		Events: []OutputEvent{
			{Machine: map[string]any{"event": "operation.progress", "ok": true, "terminal": false, "data": map[string]any{"bad": "\xff"}}},
			{Machine: map[string]any{"event": "send.accepted", "ok": true, "terminal": true, "data": map[string]any{}}},
		},
	}}
	var out bytes.Buffer
	app := &App{Out: &out, Err: &bytes.Buffer{}, Env: []string{"MEKTUP_AGENT=1"}, Executor: executor}
	if code := app.Run([]string{"send", "target", "hello"}); code != int(ExitIncomplete) {
		t.Fatalf("code=%d output=%s", code, out.String())
	}
	if !bytes.Contains(out.Bytes(), []byte("rcpt_01999999-9999-7999-8999-999999999999")) || !bytes.Contains(out.Bytes(), []byte(`"effectState":"accepted"`)) {
		t.Fatalf("known result receipt missing: %s", out.String())
	}
}

func TestCompactEventOnlyWarningRetainedEvenWithReceipt(t *testing.T) {
	message := strings.Repeat("warning", 500) + "ONLY IN EVENT"
	executor := &compactTestExecutor{result: ExecutionResult{
		Receipt: map[string]any{"receiptId": "rcpt_01999999-9999-7999-8999-999999999999", "operationId": "op_01999999-9999-7999-8999-999999999999", "state": "accepted", "warnings": []any{}},
		Events: []OutputEvent{{Machine: map[string]any{
			"event": "send.accepted", "ok": true, "terminal": true,
			"warnings": []any{map[string]any{"code": "cleanup_incomplete", "message": message}}, "data": map[string]any{},
		}}},
	}}
	var out bytes.Buffer
	app := &App{Out: &out, Err: &bytes.Buffer{}, Env: []string{"MEKTUP_AGENT=1"}, Executor: executor}
	if code := app.Run([]string{"send", "target", "hello"}); code != int(ExitSuccess) {
		t.Fatalf("code=%d output=%s", code, out.String())
	}
	if !bytes.Contains(executor.retained, []byte("ONLY IN EVENT")) {
		t.Fatalf("event-only warning was not retained: retained=%d output=%s", len(executor.retained), out.String())
	}
}

func TestCompactRawInvalidUTF8IsRejectedBeforeJSONReplacement(t *testing.T) {
	executor := &compactTestExecutor{result: ExecutionResult{Events: []OutputEvent{{Machine: map[string]any{
		"event": "thread.list.completed", "ok": true, "terminal": true,
		"data": map[string]any{"data": json.RawMessage("[{\"id\":\"thr\",\"name\":\"\xff\"}]")},
	}}}}}
	var out bytes.Buffer
	app := &App{Out: &out, Err: &bytes.Buffer{}, Env: []string{"MEKTUP_AGENT=1"}, Executor: executor}
	if code := app.Run([]string{"thread", "list"}); code != int(ExitIncomplete) || !bytes.Contains(out.Bytes(), []byte("invalid_utf8")) || bytes.Contains(out.Bytes(), []byte("\\ufffd")) {
		t.Fatalf("code=%d output=%s", code, out.String())
	}
}

func TestCompactTypedInvalidUTF8IsRejectedBeforeJSONReplacement(t *testing.T) {
	type typedRow struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	executor := &compactTestExecutor{result: ExecutionResult{Events: []OutputEvent{{Machine: map[string]any{
		"event": "thread.list.completed", "ok": true, "terminal": true,
		"data": map[string]any{"data": []typedRow{{ID: "thr", Name: string([]byte{0xff})}}},
	}}}}}
	var out bytes.Buffer
	app := &App{Out: &out, Err: &bytes.Buffer{}, Env: []string{"MEKTUP_AGENT=1"}, Executor: executor}
	if code := app.Run([]string{"thread", "list"}); code != int(ExitIncomplete) || !bytes.Contains(out.Bytes(), []byte("invalid_utf8")) || bytes.Contains(out.Bytes(), []byte("\\ufffd")) {
		t.Fatalf("code=%d output=%s", code, out.String())
	}
}

func TestCompactLoadedIDAndTypedBlockerRemainActionable(t *testing.T) {
	thread := objectMap(compactThreadAt("01999999-9999-7999-8999-999999999999", "ep_known"))
	if thread["id"] != "01999999-9999-7999-8999-999999999999" || thread["uri"] != "codex://ep_known/thread/01999999-9999-7999-8999-999999999999" {
		t.Fatalf("loaded thread=%#v", thread)
	}
	blocker := objectMap(compactBlocker(journal.Blocker{Method: "item/tool/requestUserInput", CorrelationID: "42", ThreadID: "thr", Generation: "gen"}))
	if blocker["method"] != "item/tool/requestUserInput" || blocker["correlationId"] != "42" || blocker["threadId"] != "thr" || blocker["generation"] != "gen" {
		t.Fatalf("blocker=%#v", blocker)
	}
}

func TestCompactStreamingOverflowEmitsOneTerminalAfterPriorAcceptance(t *testing.T) {
	stream := &compactStreamExecutor{chunks: []ExecutionResult{
		{Events: []OutputEvent{{Machine: map[string]any{"event": "send.accepted", "terminal": false, "ok": true, "data": map[string]any{}}}}, Streaming: true},
		{Events: []OutputEvent{{Machine: map[string]any{"event": "reply.accepted", "terminal": true, "ok": true, "data": map[string]any{"unbounded": strings.Repeat("x", CompactMaxOutputBytes+4096)}}}}, Streaming: true},
		{Events: []OutputEvent{{Machine: map[string]any{"event": "late", "terminal": true, "ok": true}}}, Streaming: true},
	}}
	var out bytes.Buffer
	app := &App{Out: &out, Err: &bytes.Buffer{}, Env: []string{"MEKTUP_AGENT=1"}, Executor: stream}
	if code := app.Run([]string{"send", "target", "hello", "--wait"}); code != int(ExitIncomplete) {
		t.Fatalf("code=%d output=%s", code, out.String())
	}
	if stream.emitted != 2 {
		t.Fatalf("stream continued after fallback terminal: emitted=%d", stream.emitted)
	}
	lines := bytes.Split(bytes.TrimSpace(out.Bytes()), []byte{'\n'})
	if len(lines) != 2 {
		t.Fatalf("lines=%d output=%s", len(lines), out.String())
	}
	var first, second map[string]any
	if json.Unmarshal(lines[0], &first) != nil || json.Unmarshal(lines[1], &second) != nil {
		t.Fatal("invalid JSONL")
	}
	if first["terminal"] != false || second["terminal"] != true || second["event"] != "compact_output_too_large" || first["operationId"] != second["operationId"] || first["sequence"] != float64(1) || second["sequence"] != float64(2) {
		t.Fatalf("stream events=%#v %#v", first, second)
	}
}

func TestCompactStreamingDecodeFailureKeepsPriorReceipt(t *testing.T) {
	stream := &compactStreamExecutor{chunks: []ExecutionResult{
		{
			Receipt:   map[string]any{"receiptId": "rcpt_01999999-9999-7999-8999-999999999999", "operationId": "op_01999999-9999-7999-8999-999999999999", "state": "accepted"},
			Events:    []OutputEvent{{Machine: map[string]any{"event": "send.accepted", "terminal": false, "ok": true, "data": map[string]any{}}}},
			Streaming: true,
		},
		{Events: []OutputEvent{{Machine: map[string]any{"event": "wait.progress", "terminal": false, "ok": true, "data": map[string]any{"bad": "\xff"}}}}, Streaming: true},
	}}
	var out bytes.Buffer
	app := &App{Out: &out, Err: &bytes.Buffer{}, Env: []string{"MEKTUP_AGENT=1"}, Executor: stream}
	if code := app.Run([]string{"send", "target", "hello", "--wait"}); code != int(ExitIncomplete) {
		t.Fatalf("code=%d output=%s", code, out.String())
	}
	if !bytes.Contains(out.Bytes(), []byte("rcpt_01999999-9999-7999-8999-999999999999")) {
		t.Fatalf("prior receipt missing from streaming failure: %s", out.String())
	}
}
