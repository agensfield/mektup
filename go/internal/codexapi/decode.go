package codexapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
)

func object(raw json.RawMessage) (map[string]json.RawMessage, error) {
	var m map[string]json.RawMessage
	if len(bytes.TrimSpace(raw)) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, fmt.Errorf("required object is missing")
	}
	if err := json.Unmarshal(raw, &m); err != nil || m == nil {
		return nil, fmt.Errorf("expected object")
	}
	return m, nil
}
func req(m map[string]json.RawMessage, k string) (json.RawMessage, error) {
	v, ok := m[k]
	if !ok || bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
		return nil, fmt.Errorf("missing required field %q", k)
	}
	return v, nil
}

func present(m map[string]json.RawMessage, k string) (json.RawMessage, error) {
	v, ok := m[k]
	if !ok {
		return nil, fmt.Errorf("missing required field %q", k)
	}
	return v, nil
}
func stringField(m map[string]json.RawMessage, k string, required bool) (string, error) {
	v, ok := m[k]
	if !ok || bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
		if required {
			return "", fmt.Errorf("missing required field %q", k)
		}
		return "", nil
	}
	var s string
	if err := json.Unmarshal(v, &s); err != nil {
		return "", fmt.Errorf("field %q must be string", k)
	}
	return s, nil
}
func nullableStringField(m map[string]json.RawMessage, k string) (string, error) {
	v, ok := m[k]
	if !ok {
		return "", fmt.Errorf("missing required field %q", k)
	}
	if bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
		return "", nil
	}
	var s string
	if err := json.Unmarshal(v, &s); err != nil {
		return "", fmt.Errorf("field %q must be string or null", k)
	}
	return s, nil
}
func boolField(m map[string]json.RawMessage, k string, required bool) (bool, error) {
	v, ok := m[k]
	if !ok || bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
		if required {
			return false, fmt.Errorf("missing required field %q", k)
		}
		return false, nil
	}
	var b bool
	if err := json.Unmarshal(v, &b); err != nil {
		return false, fmt.Errorf("field %q must be boolean", k)
	}
	return b, nil
}
func rawObject(raw json.RawMessage) (RawObject, map[string]json.RawMessage, error) {
	m, err := object(raw)
	if err != nil {
		return RawObject{}, nil, err
	}
	copy := append(json.RawMessage(nil), raw...)
	return RawObject{Raw: copy, Fields: m}, m, nil
}
func thread(raw json.RawMessage) (Thread, error) {
	ro, m, err := rawObject(raw)
	if err != nil {
		return Thread{}, err
	}
	id, err := stringField(m, "id", true)
	if err != nil {
		return Thread{}, err
	}
	status := ""
	if v, ok := m["status"]; ok {
		var statusObject map[string]json.RawMessage
		if json.Unmarshal(v, &statusObject) == nil {
			if typ, ok := statusObject["type"]; ok {
				_ = json.Unmarshal(typ, &status)
			}
		} else if s, err := stringField(m, "status", false); err == nil {
			status = s
		}
	} // Validate the generated schema's required envelope fields.
	for _, k := range []string{"cliVersion", "createdAt", "cwd", "ephemeral", "modelProvider", "preview", "sessionId", "source", "status", "turns", "updatedAt"} {
		if _, err := req(m, k); err != nil {
			return Thread{}, err
		}
	}
	// projectId is required by the wire schema but nullable for unassigned
	// threads.  Presence, rather than non-nullness, is the contract here.
	if _, err := present(m, "projectId"); err != nil {
		return Thread{}, err
	}
	if _, err := nullableStringField(m, "projectId"); err != nil {
		return Thread{}, err
	}
	if _, err := stringField(m, "cliVersion", true); err != nil {
		return Thread{}, err
	}
	if _, err := stringField(m, "cwd", true); err != nil {
		return Thread{}, err
	}
	if _, err := stringField(m, "modelProvider", true); err != nil {
		return Thread{}, err
	}
	if _, err := stringField(m, "preview", true); err != nil {
		return Thread{}, err
	}
	if _, err := stringField(m, "sessionId", true); err != nil {
		return Thread{}, err
	}
	if _, err := req(m, "source"); err != nil {
		return Thread{}, err
	}
	if source := bytes.TrimSpace(m["source"]); len(source) == 0 || bytes.Equal(source, []byte("null")) {
		return Thread{}, fmt.Errorf("field %q must not be null", "source")
	} else if source[0] != '"' {
		if _, err := object(source); err != nil {
			return Thread{}, fmt.Errorf("field %q must be string or object", "source")
		}
	}
	if statusRaw := bytes.TrimSpace(m["status"]); len(statusRaw) == 0 || statusRaw[0] != '{' {
		return Thread{}, fmt.Errorf("field %q must be object", "status")
	}
	statusObject, err := object(m["status"])
	if err != nil {
		return Thread{}, fmt.Errorf("field %q must be object", "status")
	}
	discriminator, err := stringField(statusObject, "type", true)
	if err != nil {
		return Thread{}, fmt.Errorf("status discriminator: %w", err)
	}
	switch discriminator {
	case "notLoaded", "idle", "systemError":
	case "active":
		if _, err := req(statusObject, "activeFlags"); err != nil {
			return Thread{}, err
		}
		if _, err := jsonArray(statusObject["activeFlags"]); err != nil {
			return Thread{}, fmt.Errorf("field %q must be array", "activeFlags")
		}
	default:
		return Thread{}, fmt.Errorf("unknown thread status discriminator %q", discriminator)
	}
	if _, err := boolField(m, "ephemeral", true); err != nil {
		return Thread{}, err
	}
	if _, err := jsonNumber(m["createdAt"]); err != nil {
		return Thread{}, fmt.Errorf("field %q must be integer", "createdAt")
	}
	if _, err := jsonNumber(m["updatedAt"]); err != nil {
		return Thread{}, fmt.Errorf("field %q must be integer", "updatedAt")
	}
	if _, err := jsonArray(m["turns"]); err != nil {
		return Thread{}, fmt.Errorf("field %q must be array", "turns")
	}
	return Thread{RawObject: ro, ID: id, Status: status}, nil
}
func jsonNumber(v json.RawMessage) (json.Number, error) {
	raw := bytes.TrimSpace(v)
	if len(raw) == 0 || raw[0] == '"' || raw[0] == '{' || raw[0] == '[' || bytes.Equal(raw, []byte("null")) {
		return "", fmt.Errorf("not integer")
	}
	if _, err := strconv.ParseInt(string(raw), 10, 64); err != nil {
		return "", fmt.Errorf("not integer: %w", err)
	}
	return json.Number(string(raw)), nil
}
func jsonArray(v json.RawMessage) ([]json.RawMessage, error) {
	if raw := bytes.TrimSpace(v); len(raw) == 0 || raw[0] != '[' {
		return nil, fmt.Errorf("expected array")
	}
	var a []json.RawMessage
	if err := json.Unmarshal(v, &a); err != nil {
		return nil, err
	}
	return a, nil
}
func turn(raw json.RawMessage) (Turn, error) {
	ro, m, err := rawObject(raw)
	if err != nil {
		return Turn{}, err
	}
	id, err := stringField(m, "id", true)
	if err != nil {
		return Turn{}, err
	}
	status, err := stringField(m, "status", true)
	if err != nil {
		return Turn{}, err
	}
	if status != "completed" && status != "interrupted" && status != "failed" && status != "inProgress" {
		return Turn{}, fmt.Errorf("unknown TurnStatus %q", status)
	}
	if _, err := jsonArray(m["items"]); err != nil {
		return Turn{}, fmt.Errorf("field %q must be array", "items")
	}
	view := "full"
	if _, present := m["itemsView"]; present {
		var err error
		view, err = stringField(m, "itemsView", true)
		if err != nil {
			return Turn{}, err
		}
	}
	if view != "notLoaded" && view != "summary" && view != "full" {
		return Turn{}, fmt.Errorf("unknown itemsView %q", view)
	}
	return Turn{RawObject: ro, ID: id, Status: status, ItemsView: view}, nil
}
func item(raw json.RawMessage) (Item, error) {
	ro, m, err := rawObject(raw)
	if err != nil {
		return Item{}, err
	}
	id, err := stringField(m, "id", true)
	if err != nil {
		return Item{}, err
	}
	typ, err := stringField(m, "type", true)
	if err != nil {
		return Item{}, err
	}
	return Item{RawObject: ro, ID: id, Type: typ}, nil
}
func cursor(m map[string]json.RawMessage, k string) (string, error) {
	v, ok := m[k]
	if !ok || bytes.Equal(bytes.TrimSpace(v), []byte("null")) {
		return "", nil
	}
	s, err := stringField(m, k, true)
	if err != nil {
		return "", err
	}
	if len(s) > MaxCursorBytes {
		return "", fmt.Errorf("cursor %q exceeds %d bytes", k, MaxCursorBytes)
	}
	return s, nil
}
func page(raw json.RawMessage, limit, max int) (map[string]json.RawMessage, []json.RawMessage, string, string, error) {
	m, err := object(raw)
	if err != nil {
		return nil, nil, "", "", err
	}
	v, err := req(m, "data")
	if err != nil {
		return nil, nil, "", "", err
	}
	a, err := jsonArray(v)
	if err != nil {
		return nil, nil, "", "", fmt.Errorf("data must be array")
	}
	bound := max
	if limit > 0 && limit < bound {
		bound = limit
	}
	if len(a) > bound {
		return nil, nil, "", "", ErrUnboundedPage
	}
	next, err := cursor(m, "nextCursor")
	if err != nil {
		return nil, nil, "", "", err
	}
	back, err := cursor(m, "backwardsCursor")
	if err != nil {
		return nil, nil, "", "", err
	}
	return m, a, next, back, nil
}
func decodeTurnStart(raw json.RawMessage) (TurnStartResponse, error) {
	m, err := object(raw)
	if err != nil {
		return TurnStartResponse{}, err
	}
	v, err := req(m, "turn")
	if err != nil {
		return TurnStartResponse{}, err
	}
	t, err := turn(v)
	if err != nil {
		return TurnStartResponse{}, err
	}
	return TurnStartResponse{Turn: t, Raw: append(json.RawMessage(nil), raw...)}, nil
}
func decodeLifecycle(raw json.RawMessage) (LifecycleResponse, error) {
	m, err := object(raw)
	if err != nil {
		return LifecycleResponse{}, err
	}
	v, err := req(m, "thread")
	if err != nil {
		return LifecycleResponse{}, err
	}
	t, err := thread(v)
	if err != nil {
		return LifecycleResponse{}, err
	}
	model, err := stringField(m, "model", true)
	if err != nil {
		return LifecycleResponse{}, err
	}
	provider, err := stringField(m, "modelProvider", true)
	if err != nil {
		return LifecycleResponse{}, err
	}
	cwd, err := stringField(m, "cwd", true)
	if err != nil {
		return LifecycleResponse{}, err
	}
	if _, err := req(m, "approvalPolicy"); err != nil {
		return LifecycleResponse{}, err
	}
	if err := validateApprovalPolicy(m["approvalPolicy"]); err != nil {
		return LifecycleResponse{}, err
	}
	if _, err := req(m, "approvalsReviewer"); err != nil {
		return LifecycleResponse{}, err
	}
	if err := validateApprovalsReviewer(m["approvalsReviewer"]); err != nil {
		return LifecycleResponse{}, err
	}
	if _, err := req(m, "sandbox"); err != nil {
		return LifecycleResponse{}, err
	}
	if err := validateSandboxPolicy(m["sandbox"]); err != nil {
		return LifecycleResponse{}, err
	}
	return LifecycleResponse{Thread: t, Model: model, ModelProvider: provider, CWD: cwd, Raw: append(json.RawMessage(nil), raw...)}, nil
}

func validateApprovalsReviewer(raw json.RawMessage) error {
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return fmt.Errorf("approvalsReviewer must be string")
	}
	switch value {
	case "user", "auto_review", "guardian_subagent":
		return nil
	default:
		return fmt.Errorf("unknown approvalsReviewer %q", value)
	}
}

func validateApprovalPolicy(raw json.RawMessage) error {
	var value string
	if json.Unmarshal(raw, &value) == nil {
		switch value {
		case "untrusted", "on-request", "never":
			return nil
		default:
			return fmt.Errorf("unknown approvalPolicy %q", value)
		}
	}
	m, err := object(raw)
	if err != nil {
		return fmt.Errorf("approvalPolicy must be enum string or granular object")
	}
	granularRaw, err := req(m, "granular")
	if err != nil {
		return err
	}
	granular, err := object(granularRaw)
	if err != nil {
		return fmt.Errorf("approvalPolicy.granular must be object")
	}
	for _, key := range []string{"mcp_elicitations", "rules", "sandbox_approval"} {
		if _, err := boolField(granular, key, true); err != nil {
			return err
		}
	}
	for _, key := range []string{"skill_approval", "request_permissions"} {
		if _, present := granular[key]; present {
			if _, err := boolField(granular, key, true); err != nil {
				return err
			}
		}
	}
	return nil
}

func validateSandboxPolicy(raw json.RawMessage) error {
	m, err := object(raw)
	if err != nil {
		return fmt.Errorf("sandbox must be policy object")
	}
	typeValue, err := stringField(m, "type", true)
	if err != nil {
		return err
	}
	switch typeValue {
	case "dangerFullAccess":
		return nil
	case "readOnly", "workspaceWrite":
		if _, present := m["networkAccess"]; present {
			if _, err := boolField(m, "networkAccess", true); err != nil {
				return err
			}
		}
	case "externalSandbox":
		if rawNetwork, present := m["networkAccess"]; present {
			var network string
			if err := json.Unmarshal(rawNetwork, &network); err != nil || (network != "restricted" && network != "enabled") {
				return fmt.Errorf("sandbox.networkAccess must be restricted or enabled")
			}
		}
	default:
		return fmt.Errorf("unknown sandbox type %q", typeValue)
	}
	if rawRoots, present := m["writableRoots"]; present {
		if _, err := jsonArray(rawRoots); err != nil {
			return fmt.Errorf("sandbox.writableRoots must be array")
		}
	}
	for _, key := range []string{"excludeSlashTmp", "excludeTmpdirEnvVar"} {
		if _, present := m[key]; present {
			if _, err := boolField(m, key, true); err != nil {
				return err
			}
		}
	}
	return nil
}
func decodeThreadList(raw json.RawMessage, limit int) (ThreadListResponse, error) {
	_, a, next, back, err := page(raw, limit, MaxThreadPageLimit)
	if err != nil {
		return ThreadListResponse{}, err
	}
	data := make([]Thread, 0, len(a))
	for _, v := range a {
		t, e := thread(v)
		if e != nil {
			return ThreadListResponse{}, e
		}
		data = append(data, t)
	}
	return ThreadListResponse{Data: data, NextCursor: next, BackwardsCursor: back, Raw: append(json.RawMessage(nil), raw...)}, nil
}

func decodeLoadedList(raw json.RawMessage, limit int) (ThreadLoadedListResponse, error) {
	m, err := object(raw)
	if err != nil {
		return ThreadLoadedListResponse{}, err
	}
	v, err := req(m, "data")
	if err != nil {
		return ThreadLoadedListResponse{}, err
	}
	items, err := jsonArray(v)
	if err != nil {
		return ThreadLoadedListResponse{}, fmt.Errorf("data must be array")
	}
	bound := MaxThreadPageLimit
	if limit > 0 && limit < bound {
		bound = limit
	}
	if len(items) > bound {
		return ThreadLoadedListResponse{}, ErrUnboundedPage
	}
	data := make([]string, 0, len(items))
	for _, item := range items {
		var id string
		if err := json.Unmarshal(item, &id); err != nil || id == "" {
			return ThreadLoadedListResponse{}, fmt.Errorf("loaded thread data must contain non-empty string IDs")
		}
		data = append(data, id)
	}
	next, err := cursor(m, "nextCursor")
	if err != nil {
		return ThreadLoadedListResponse{}, err
	}
	return ThreadLoadedListResponse{Data: data, NextCursor: next, Raw: append(json.RawMessage(nil), raw...)}, nil
}
func decodeThreadRead(raw json.RawMessage, includeTurns bool) (ThreadReadResponse, error) {
	m, err := object(raw)
	if err != nil {
		return ThreadReadResponse{}, err
	}
	v, err := req(m, "thread")
	if err != nil {
		return ThreadReadResponse{}, err
	}
	t, err := thread(v)
	if err != nil {
		return ThreadReadResponse{}, err
	}
	return ThreadReadResponse{Thread: t, Raw: append(json.RawMessage(nil), raw...)}, nil
}
func decodeTurns(raw json.RawMessage, limit int, view string) (ThreadTurnsResponse, error) {
	_, a, next, back, err := page(raw, limit, MaxTurnsPageLimit)
	if err != nil {
		return ThreadTurnsResponse{}, err
	}
	data := make([]Turn, 0, len(a))
	for _, v := range a {
		t, e := turn(v)
		if e != nil {
			return ThreadTurnsResponse{}, e
		}
		if t.ItemsView != "" && t.ItemsView != view {
			return ThreadTurnsResponse{}, fmt.Errorf("itemsView mismatch: requested %q, received %q", view, t.ItemsView)
		}
		data = append(data, t)
	}
	return ThreadTurnsResponse{Data: data, NextCursor: next, BackwardsCursor: back, Raw: append(json.RawMessage(nil), raw...)}, nil
}
func decodeItems(raw json.RawMessage, limit int) (ThreadItemsResponse, error) {
	_, a, next, back, err := page(raw, limit, MaxItemsPageLimit)
	if err != nil {
		return ThreadItemsResponse{}, err
	}
	data := make([]ItemEntry, 0, len(a))
	for _, v := range a {
		m, e := object(v)
		if e != nil {
			return ThreadItemsResponse{}, e
		}
		tid, e := stringField(m, "turnId", true)
		if e != nil {
			return ThreadItemsResponse{}, e
		}
		ir, e := req(m, "item")
		if e != nil {
			return ThreadItemsResponse{}, e
		}
		it, e := item(ir)
		if e != nil {
			return ThreadItemsResponse{}, e
		}
		data = append(data, ItemEntry{TurnID: tid, Item: it, Raw: append(json.RawMessage(nil), v...)})
	}
	return ThreadItemsResponse{Data: data, NextCursor: next, BackwardsCursor: back, Raw: append(json.RawMessage(nil), raw...)}, nil
}
func decodeUnsubscribe(raw json.RawMessage) (UnsubscribeResponse, error) {
	m, err := object(raw)
	if err != nil {
		return UnsubscribeResponse{}, err
	}
	s, err := stringField(m, "status", true)
	if err != nil {
		return UnsubscribeResponse{}, err
	}
	switch s {
	case "notLoaded", "notSubscribed", "unsubscribed":
	default:
		return UnsubscribeResponse{}, fmt.Errorf("unknown unsubscribe status %q", s)
	}
	return UnsubscribeResponse{Status: s, Raw: append(json.RawMessage(nil), raw...)}, nil
}
func decodeSearch(raw json.RawMessage, limit int) (SearchResponse, error) {
	_, a, next, back, err := page(raw, limit, MaxThreadPageLimit)
	if err != nil {
		return SearchResponse{}, err
	}
	data := make([]SearchResult, 0, len(a))
	for _, v := range a {
		m, e := object(v)
		if e != nil {
			return SearchResponse{}, e
		}
		tr, e := req(m, "thread")
		if e != nil {
			return SearchResponse{}, e
		}
		t, e := thread(tr)
		if e != nil {
			return SearchResponse{}, e
		}
		s, e := stringField(m, "snippet", true)
		if e != nil {
			return SearchResponse{}, e
		}
		data = append(data, SearchResult{Thread: t, Snippet: s, Raw: append(json.RawMessage(nil), v...)})
	}
	return SearchResponse{Data: data, NextCursor: next, BackwardsCursor: back, Raw: append(json.RawMessage(nil), raw...)}, nil
}
func decodeOccurrences(raw json.RawMessage, limit int) (SearchOccurrencesResponse, error) {
	_, a, next, _, err := page(raw, limit, MaxOccurrencesPageLimit)
	if err != nil {
		return SearchOccurrencesResponse{}, err
	}
	data := make([]SearchOccurrence, 0, len(a))
	for _, v := range a {
		m, e := object(v)
		if e != nil {
			return SearchOccurrencesResponse{}, e
		}
		tid, e := stringField(m, "turnId", true)
		if e != nil {
			return SearchOccurrencesResponse{}, e
		}
		iid, e := stringField(m, "itemId", true)
		if e != nil {
			return SearchOccurrencesResponse{}, e
		}
		snip, e := stringField(m, "snippet", true)
		if e != nil {
			return SearchOccurrencesResponse{}, e
		}
		tc, e := stringField(m, "turnCursor", true)
		if e != nil {
			return SearchOccurrencesResponse{}, e
		}
		rr, e := req(m, "snippetMatchRange")
		if e != nil {
			return SearchOccurrencesResponse{}, e
		}
		rm, e := object(rr)
		if e != nil {
			return SearchOccurrencesResponse{}, e
		}
		start, e := uintField(rm, "start")
		if e != nil {
			return SearchOccurrencesResponse{}, e
		}
		end, e := uintField(rm, "end")
		if e != nil {
			return SearchOccurrencesResponse{}, e
		}
		data = append(data, SearchOccurrence{TurnID: tid, ItemID: iid, Snippet: snip, SnippetMatchRange: TextRange{Start: start, End: end}, TurnCursor: tc, Raw: append(json.RawMessage(nil), v...)})
	}
	return SearchOccurrencesResponse{Data: data, NextCursor: next, Raw: append(json.RawMessage(nil), raw...)}, nil
}
func uintField(m map[string]json.RawMessage, k string) (uint32, error) {
	v, err := req(m, k)
	if err != nil {
		return 0, err
	}
	var n uint32
	if err := json.Unmarshal(v, &n); err != nil {
		return 0, fmt.Errorf("field %q must be uint32", k)
	}
	return n, nil
}
