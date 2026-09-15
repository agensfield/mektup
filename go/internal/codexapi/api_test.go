package codexapi

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
)

type recordingCaller struct {
	method string
	params json.RawMessage
	result json.RawMessage
	err    *ServerError
}

type sequencedCaller struct {
	methods []string
	params  []json.RawMessage
	results []json.RawMessage
}

func (r *sequencedCaller) Call(_ context.Context, method string, params json.RawMessage) (json.RawMessage, *ServerError, error) {
	r.methods = append(r.methods, method)
	r.params = append(r.params, append(json.RawMessage(nil), params...))
	if len(r.results) == 0 {
		return nil, nil, nil
	}
	result := r.results[0]
	r.results = r.results[1:]
	return result, nil, nil
}

func (r *recordingCaller) Call(_ context.Context, method string, params json.RawMessage) (json.RawMessage, *ServerError, error) {
	r.method, r.params = method, append(json.RawMessage(nil), params...)
	return r.result, r.err, nil
}

func TestStartOrSteerTurnUsesOnlyPinnedMinimalParams(t *testing.T) {
	r := &recordingCaller{result: json.RawMessage(`{"turn":{"id":"turn-1","items":[],"status":"inProgress"}}`)}
	c := New(r, Options{})
	if _, err := c.StartOrSteerTurn(context.Background(), TurnStartRequest{ThreadID: "thread-1", Text: "hello", ClientUserMessageID: "msg-1"}); err != nil {
		t.Fatal(err)
	}
	if r.method != "turn/start" {
		t.Fatalf("method = %q", r.method)
	}
	var got map[string]any
	if err := json.Unmarshal(r.params, &got); err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"threadId": "thread-1", "clientUserMessageId": "msg-1", "input": []any{map[string]any{"type": "text", "text": "hello"}}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("params = %#v, want %#v", got, want)
	}
}

func TestSearchRequiresExperimentalBeforeCalling(t *testing.T) {
	r := &recordingCaller{result: json.RawMessage(`{"data":[],"nextCursor":null,"backwardsCursor":null}`)}
	_, err := New(r, Options{}).Search(context.Background(), SearchOptions{SearchTerm: "needle"})
	if err != ErrExperimentalAPIRequired {
		t.Fatalf("error = %v, want %v", err, ErrExperimentalAPIRequired)
	}
	if r.method != "" {
		t.Fatalf("caller invoked method %q", r.method)
	}
}

func TestListDefaultsAllSourceKindsIncludingSubagents(t *testing.T) {
	r := &recordingCaller{result: json.RawMessage(`{"data":[],"nextCursor":null,"backwardsCursor":null}`)}
	if _, err := New(r, Options{}).ThreadList(context.Background(), ThreadListOptions{}); err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(r.params, &got); err != nil {
		t.Fatal(err)
	}
	sources, ok := got["sourceKinds"].([]any)
	if !ok || len(sources) < 9 {
		t.Fatalf("sourceKinds = %#v", got["sourceKinds"])
	}
	found := false
	for _, source := range sources {
		if source == "subAgent" || source == "subAgentReview" || source == "subAgentCompact" || source == "subAgentThreadSpawn" || source == "subAgentOther" {
			found = true
		}
	}
	if !found {
		t.Fatalf("subagent source kinds missing: %#v", sources)
	}
}

func TestLoadedListUsesDedicatedBoundedBackendAndIDs(t *testing.T) {
	r := &recordingCaller{result: json.RawMessage(`{"data":["thread-a","thread-b"],"nextCursor":"next"}`)}
	out, err := New(r, Options{}).ThreadList(context.Background(), ThreadListOptions{Loaded: true, Cursor: "start", Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if r.method != "thread/loaded/list" || out.Loaded != true || !reflect.DeepEqual(out.LoadedIDs, []string{"thread-a", "thread-b"}) || len(out.Data) != 0 || out.NextCursor != "next" {
		t.Fatalf("loaded result = %+v, method=%q", out, r.method)
	}
	var params map[string]any
	if err := json.Unmarshal(r.params, &params); err != nil {
		t.Fatal(err)
	}
	if params["cursor"] != "start" || params["limit"] != float64(2) {
		t.Fatalf("loaded params = %#v", params)
	}
}

func TestLoadedListRejectsUnsupportedFiltersButAllowsCursorOnly(t *testing.T) {
	r := &recordingCaller{result: json.RawMessage(`{"data":[],"nextCursor":null}`)}
	if _, err := New(r, Options{}).ThreadList(context.Background(), ThreadListOptions{Loaded: true, Archived: new(bool)}); !errors.Is(err, ErrUnsupportedOption) {
		t.Fatalf("archived loaded filter error = %v", err)
	}
	if r.method != "" {
		t.Fatalf("unsupported loaded filter called %q", r.method)
	}
}

func TestReconcileHistoryPaginatesFullTurns(t *testing.T) {
	page1 := json.RawMessage(`{"data":[{"id":"turn-1","items":[],"status":"completed","itemsView":"full"}],"nextCursor":"c2","backwardsCursor":null}`)
	page2 := json.RawMessage(`{"data":[{"id":"turn-2","items":[],"status":"completed","itemsView":"full"}],"nextCursor":null,"backwardsCursor":null}`)
	r := &sequencedCaller{results: []json.RawMessage{page1, page2}}
	out, err := New(r, Options{}).ReconcileHistory(context.Background(), "thread")
	if err != nil || len(out.Data) != 2 || len(r.methods) != 2 {
		t.Fatalf("reconcile = %+v, err=%v, calls=%d", out, err, len(r.methods))
	}
	for i, raw := range r.params {
		var params map[string]any
		if err := json.Unmarshal(raw, &params); err != nil {
			t.Fatal(err)
		}
		if params["itemsView"] != "full" || params["limit"] != float64(MaxTurnsPageLimit) {
			t.Fatalf("page %d params = %#v", i, params)
		}
	}
}

func TestReconcileHistoryRejectsRepeatedCursor(t *testing.T) {
	r := &sequencedCaller{results: []json.RawMessage{json.RawMessage(`{"data":[],"nextCursor":"c"}`), json.RawMessage(`{"data":[],"nextCursor":"c"}`)}}
	_, err := New(r, Options{}).ReconcileHistory(context.Background(), "thread")
	if !errors.Is(err, ErrPaginationStalled) {
		t.Fatalf("repeated cursor error = %v", err)
	}
}

func TestTurnsRequireExplicitItemsViewAndBoundResponse(t *testing.T) {
	r := &recordingCaller{result: json.RawMessage(`{"data":[],"nextCursor":null,"backwardsCursor":null}`)}
	if _, err := New(r, Options{}).ThreadTurns(context.Background(), TurnsOptions{ThreadID: "thread"}); err == nil {
		t.Fatal("missing itemsView accepted")
	}
	if _, err := New(r, Options{}).ThreadTurns(context.Background(), TurnsOptions{ThreadID: "thread", ItemsView: "full", Limit: MaxTurnsPageLimit + 1}); err == nil {
		t.Fatal("oversized limit accepted")
	}
}

func TestUnknownFieldsAreRetainedAndNullableProjectIDIsAccepted(t *testing.T) {
	r := &recordingCaller{result: json.RawMessage(`{"data":[{"id":"t","cliVersion":"x","createdAt":1,"cwd":"/tmp","ephemeral":false,"modelProvider":"openai","preview":"p","projectId":null,"sessionId":"s","source":"cli","status":{"type":"idle"},"turns":[],"updatedAt":1,"futureField":{"x":1}}]}`)}
	out, err := New(r, Options{}).ThreadList(context.Background(), ThreadListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if string(out.Data[0].Fields["futureField"]) != `{"x":1}` {
		t.Fatalf("unknown field was not retained: %s", out.Data[0].Fields["futureField"])
	}
}

func TestClassifyNotSubmittedOnlyPinnedReviewCompactShapes(t *testing.T) {
	base := json.RawMessage(`{"codexErrorInfo":{"activeTurnNotSteerable":{"turnKind":"review"}}}`)
	err := &ServerError{Code: -32603, Message: "failed to submit turn input: ActiveTurnNotSteerable { turn_kind: Review }", Data: base, Raw: json.RawMessage(`{"code":-32603}`)}
	got := ClassifyNotSubmitted(err)
	if got.Kind != NotSubmittedReview || !got.Retry || string(got.Evidence) != `{"code":-32603}` {
		t.Fatalf("classification = %+v", got)
	}
	err.Message = "internal error"
	if IsRetryableNotSubmitted(err) {
		t.Fatal("generic internal error was classified as retryable")
	}
}

func TestThroughTurnUsesExactReadAndRejectsInProgress(t *testing.T) {
	r := &recordingCaller{result: json.RawMessage(`{"thread":{"id":"t","cliVersion":"x","createdAt":1,"cwd":"/tmp","ephemeral":false,"modelProvider":"openai","preview":"p","projectId":null,"sessionId":"s","source":"cli","status":{"type":"idle"},"turns":[],"updatedAt":1},"model":"m","modelProvider":"openai","cwd":"/tmp","approvalPolicy":"on-request","approvalsReviewer":"user","sandbox":"workspace-write"}`)}
	reader := exactReader{turn: Turn{ID: "turn", Status: "inProgress"}}
	_, err := New(r, Options{ExactRead: reader}).ThreadFork(context.Background(), ForkOptions{ThreadID: "t", ThroughTurnID: "turn"})
	if err != ErrInProgressCutoff {
		t.Fatalf("error = %v, want %v", err, ErrInProgressCutoff)
	}
	if r.method != "" {
		t.Fatalf("fork was called before cutoff check: %q", r.method)
	}
}

func TestThroughTurnSerializesInclusiveLastTurnAndConflictsRejectBeforeRead(t *testing.T) {
	r := &recordingCaller{result: lifecycleFixture()}
	reader := exactReader{turn: Turn{ID: "cut", Status: "completed"}}
	if _, err := New(r, Options{ExactRead: reader}).ThreadFork(context.Background(), ForkOptions{ThreadID: "t", ThroughTurnID: "cut"}); err != nil {
		t.Fatal(err)
	}
	var params map[string]any
	if err := json.Unmarshal(r.params, &params); err != nil {
		t.Fatal(err)
	}
	if params["lastTurnId"] != "cut" || params["beforeTurnId"] != nil {
		t.Fatalf("fork cutoff params = %#v", params)
	}
	r = &recordingCaller{result: lifecycleFixture()}
	if _, err := New(r, Options{ExactRead: reader}).ThreadFork(context.Background(), ForkOptions{ThreadID: "t", ThroughTurnID: "cut", BeforeTurnID: "before"}); err == nil || r.method != "" {
		t.Fatalf("conflicting cutoff dispatched: method=%q error=%v", r.method, err)
	}
}

func TestThreadListDefaultSortAndExperimentalNegotiation(t *testing.T) {
	r := &recordingCaller{result: json.RawMessage(`{"data":[]}`)}
	if _, err := New(r, Options{}).ThreadList(context.Background(), ThreadListOptions{}); err != nil {
		t.Fatal(err)
	}
	var params map[string]any
	if err := json.Unmarshal(r.params, &params); err != nil {
		t.Fatal(err)
	}
	if params["sortKey"] != "updated_at" || params["sortDirection"] != "desc" {
		t.Fatalf("default sort = %#v", params)
	}
	project := "project"
	r = &recordingCaller{result: json.RawMessage(`{"data":[]}`)}
	if _, err := New(r, Options{}).ThreadList(context.Background(), ThreadListOptions{ProjectID: &project}); !errors.Is(err, ErrExperimentalAPIRequired) || r.method != "" {
		t.Fatalf("project field was dispatched without negotiation: method=%q error=%v", r.method, err)
	}
	r = &recordingCaller{result: json.RawMessage(`{"data":[]}`)}
	if _, err := New(r, Options{Capabilities: Capabilities{Methods: map[string]bool{"thread/search": true}}}).Search(context.Background(), SearchOptions{SearchTerm: "x"}); !errors.Is(err, ErrExperimentalAPIRequired) || r.method != "" {
		t.Fatalf("method inventory bypassed negotiated capability: method=%q error=%v", r.method, err)
	}
}

func TestKnownMalformedFieldsRejectAndOmittedItemsViewDefaultsFull(t *testing.T) {
	for _, row := range []string{
		`{"id":"x","items":null,"status":"completed","itemsView":"full"}`,
		`{"id":"x","items":[],"status":"completed","itemsView":42}`,
	} {
		if _, err := decodeTurns(json.RawMessage(`{"data":[`+row+`]}`), 1, "full"); err == nil {
			t.Fatalf("malformed turn accepted: %s", row)
		}
	}
	out, err := decodeTurns(json.RawMessage(`{"data":[{"id":"x","items":[],"status":"completed"}]}`), 1, "full")
	if err != nil || out.Data[0].ItemsView != "full" {
		t.Fatalf("omitted itemsView did not default full: %+v, %v", out, err)
	}
	rawThread := strings.Replace(threadFixture(), `"createdAt":1`, `"createdAt":1.5`, 1)
	if _, err := thread(json.RawMessage(rawThread)); err == nil {
		t.Fatal("fractional createdAt accepted")
	}
	rawThread = strings.Replace(threadFixture(), `"status":{"type":"idle"}`, `"status":{}`, 1)
	if _, err := thread(json.RawMessage(rawThread)); err == nil {
		t.Fatal("empty status discriminator accepted")
	}
}

func TestInvalidCWDAndOutboundCursorRejectBeforeDispatch(t *testing.T) {
	r := &recordingCaller{result: json.RawMessage(`{"data":[]}`)}
	if _, err := New(r, Options{}).ThreadList(context.Background(), ThreadListOptions{CWD: 42}); !errors.Is(err, ErrInvalidCWD) || r.method != "" {
		t.Fatalf("invalid CWD dispatched: method=%q error=%v", r.method, err)
	}
	r = &recordingCaller{result: json.RawMessage(`{"data":[]}`)}
	if _, err := New(r, Options{}).ThreadList(context.Background(), ThreadListOptions{CWD: "/tmp", Cwd: "/tmp"}); !errors.Is(err, ErrConflictingCWD) || r.method != "" {
		t.Fatalf("conflicting CWD aliases dispatched: method=%q error=%v", r.method, err)
	}
	r = &recordingCaller{result: json.RawMessage(`{"data":[]}`)}
	if _, err := New(r, Options{}).ThreadLoadedList(context.Background(), strings.Repeat("x", MaxCursorBytes+1), 1); err == nil || r.method != "" {
		t.Fatalf("oversized cursor dispatched: method=%q error=%v", r.method, err)
	}
}

func lifecycleFixture() json.RawMessage {
	return json.RawMessage(`{"thread":{"id":"t","cliVersion":"x","createdAt":1,"cwd":"/tmp","ephemeral":false,"modelProvider":"openai","preview":"p","projectId":null,"sessionId":"s","source":"cli","status":{"type":"idle"},"turns":[],"updatedAt":1},"model":"m","modelProvider":"openai","cwd":"/tmp","approvalPolicy":"on-request","approvalsReviewer":"user","sandbox":"workspace-write"}`)
}

func threadFixture() string {
	return `{"id":"t","cliVersion":"x","createdAt":1,"cwd":"/tmp","ephemeral":false,"modelProvider":"openai","preview":"p","projectId":null,"sessionId":"s","source":"cli","status":{"type":"idle"},"turns":[],"updatedAt":1}`
}

func TestLifecycleDefaultsDoNotSerializeNullOverrides(t *testing.T) {
	result := json.RawMessage(`{"thread":{"id":"t","cliVersion":"x","createdAt":1,"cwd":"/tmp","ephemeral":false,"modelProvider":"openai","preview":"p","projectId":null,"sessionId":"s","source":"cli","status":{"type":"idle"},"turns":[],"updatedAt":1},"model":"m","modelProvider":"openai","cwd":"/tmp","approvalPolicy":"on-request","approvalsReviewer":"user","sandbox":"workspace-write"}`)
	for _, call := range []func(*Client) error{
		func(c *Client) error { _, err := c.ThreadStart(context.Background(), StartOptions{}); return err },
		func(c *Client) error {
			_, err := c.ThreadResume(context.Background(), ResumeOptions{ThreadID: "t"})
			return err
		},
		func(c *Client) error {
			_, err := c.ThreadFork(context.Background(), ForkOptions{ThreadID: "t"})
			return err
		},
	} {
		r := &recordingCaller{result: result}
		if err := call(New(r, Options{})); err != nil {
			t.Fatal(err)
		}
		if bytes := string(r.params); strings.Contains(bytes, "null") {
			t.Fatalf("default params contain null override: %s", bytes)
		}
	}
}

type exactReader struct {
	turn Turn
	err  error
}

func (r exactReader) ReadTurn(context.Context, string, string) (Turn, error) { return r.turn, r.err }
