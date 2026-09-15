package codexapi

import (
	"context"
	"encoding/json"
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
	reader := exactReader{turn: Turn{Status: "inProgress"}}
	_, err := New(r, Options{ExactRead: reader}).ThreadFork(context.Background(), ForkOptions{ThreadID: "t", ThroughTurnID: "turn"})
	if err != ErrInProgressCutoff {
		t.Fatalf("error = %v, want %v", err, ErrInProgressCutoff)
	}
	if r.method != "" {
		t.Fatalf("fork was called before cutoff check: %q", r.method)
	}
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
