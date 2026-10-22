package codexapi

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestFullTurnHistoryCustomLimitPreservesFullViewAndPagination(t *testing.T) {
	caller := &sequencedCaller{results: []json.RawMessage{
		json.RawMessage(`{"data":[{"id":"turn-1","items":[],"status":"completed","itemsView":"full"}],"nextCursor":"exact-cursor"}`),
		json.RawMessage(`{"data":[{"id":"turn-2","items":[],"status":"completed","itemsView":"full"}],"nextCursor":null}`),
	}}
	result, err := New(caller, Options{}).ReadFullTurnsWithLimit(context.Background(), "thread", 3)
	if err != nil || len(result.Data) != 2 {
		t.Fatalf("history=%+v err=%v", result, err)
	}
	for i, raw := range caller.params {
		var params map[string]any
		if err := json.Unmarshal(raw, &params); err != nil {
			t.Fatal(err)
		}
		if params["limit"] != float64(3) || params["itemsView"] != "full" || params["threadId"] != "thread" {
			t.Fatalf("request %d=%v", i, params)
		}
		if i == 1 && params["cursor"] != "exact-cursor" {
			t.Fatalf("cursor=%v", params["cursor"])
		}
	}
}

func TestFullTurnHistoryCustomLimitRejectsUnboundedAndPartialResults(t *testing.T) {
	for _, limit := range []int{-1, 0, MaxTurnsPageLimit + 1} {
		caller := &recordingCaller{}
		_, err := New(caller, Options{}).ReadFullTurnsWithLimit(context.Background(), "thread", limit)
		if !errors.Is(err, ErrUnboundedPage) || caller.method != "" {
			t.Fatalf("limit=%d err=%v method=%s", limit, err, caller.method)
		}
	}
	calls := 0
	caller := FuncCaller(func(context.Context, string, json.RawMessage) (json.RawMessage, *ServerError, error) {
		calls++
		if calls == 1 {
			return json.RawMessage(`{"data":[{"id":"turn-1","items":[],"status":"completed","itemsView":"full"}],"nextCursor":"page-2"}`), nil, nil
		}
		return nil, nil, ErrResponseTooLarge
	})
	result, err := New(caller, Options{}).ReadFullTurnsWithLimit(context.Background(), "thread", 1)
	if !errors.Is(err, ErrResponseTooLarge) || len(result.Data) != 0 {
		t.Fatalf("partial result=%+v err=%v", result, err)
	}
}
