package liveacceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agensfield/mektup/go/appserver"
	"github.com/agensfield/mektup/go/internal/connection"
	"github.com/agensfield/mektup/go/internal/endpoint"
	"github.com/agensfield/mektup/go/internal/journal"
	mekruntime "github.com/agensfield/mektup/go/internal/runtime"
	"github.com/agensfield/mektup/go/internal/service"
	"github.com/gorilla/websocket"
)

const callbackPayloadSentinel = "mektup-secret-callback-payload"

// TestIsolatedMultiClientServerRequest exercises the pinned Codex binary with
// a scratch CODEX_HOME, daemon, and Responses backend. It never connects to the
// user's central daemon. The test is opt-in because release hosts need a Codex
// 0.154.0 binary in addition to the Go toolchain.
func TestIsolatedMultiClientServerRequest(t *testing.T) {
	if os.Getenv("MEKTUP_ACCEPT_CALLBACKS") != "1" {
		t.Skip("set MEKTUP_ACCEPT_CALLBACKS=1 to run isolated real app-server callback acceptance")
	}
	codexBinary := os.Getenv("MEKTUP_ACCEPT_CODEX_BINARY")
	if codexBinary == "" {
		var err error
		codexBinary, err = exec.LookPath("codex")
		if err != nil {
			t.Fatal("codex binary is required for callback acceptance")
		}
	}

	responses := newCallbackResponsesServer(t)
	defer responses.Close()

	root, err := os.MkdirTemp("/tmp", "mektup-callback-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	codexHome := filepath.Join(root, "codex-home")
	if err := os.Mkdir(codexHome, 0700); err != nil {
		t.Fatal(err)
	}
	config := fmt.Sprintf(`model = "mock-model"
approval_policy = "on-request"
sandbox_mode = "read-only"
model_provider = "mock_provider"

[model_providers.mock_provider]
name = "Mektup callback acceptance"
base_url = %q
wire_api = "responses"
request_max_retries = 0
stream_max_retries = 0
`, responses.URL+"/v1")
	if err := os.WriteFile(filepath.Join(codexHome, "config.toml"), []byte(config), 0600); err != nil {
		t.Fatal(err)
	}

	socket := filepath.Join(root, "app-server.sock")
	daemonCtx, stopDaemon := context.WithCancel(context.Background())
	defer stopDaemon()
	command := exec.CommandContext(daemonCtx, codexBinary, "app-server", "--listen", "unix://"+socket)
	command.Env = append(os.Environ(), "CODEX_HOME="+codexHome)
	var daemonLog lockedBuffer
	command.Stdout = &daemonLog
	command.Stderr = &daemonLog
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		stopDaemon()
		wait := make(chan error, 1)
		go func() { wait <- command.Wait() }()
		select {
		case <-wait:
		case <-time.After(3 * time.Second):
			_ = command.Process.Kill()
			<-wait
		}
	})
	waitForUnixSocket(t, command, socket, &daemonLog)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	tui := dialRawTUI(t, ctx, socket)
	defer tui.Close()
	tui.request(t, ctx, "init", "initialize", map[string]any{
		"clientInfo":   map[string]any{"name": "mektup-callback-tui", "version": "1.0.0"},
		"capabilities": map[string]any{"experimentalApi": true},
	})
	tui.notify(t, "initialized", nil)
	threadResult := tui.request(t, ctx, "thread", "thread/start", map[string]any{
		"model": "mock-model",
		"cwd":   root,
	})
	threadID := nestedString(t, threadResult, "thread", "id")
	rolloutPath := nestedString(t, threadResult, "thread", "path")
	tui.request(t, ctx, "warmup", "turn/start", map[string]any{
		"threadId": threadID,
		"input":    []map[string]any{{"type": "text", "text": "persist this thread"}},
		"model":    "mock-model",
	})
	tui.waitNotification(t, ctx, "turn/completed")
	waitForRegularFile(t, rolloutPath)

	stateDir := filepath.Join(root, "state")
	blockers, err := journal.Open(ctx, journal.Options{StateDir: stateDir})
	if err != nil {
		t.Fatal(err)
	}
	defer blockers.Close()

	route, err := endpoint.UnixRoute(socket)
	if err != nil {
		t.Fatal(err)
	}
	ep := endpoint.Endpoint{ID: "ep_01a0a000-0000-7000-8000-000000000001", Alias: "callback", Route: route, Herdr: endpoint.HerdrDisabled}
	var outboundMu sync.Mutex
	var outbound [][]byte
	observer := func(direction appserver.FrameDirection, frame appserver.Frame) error {
		if direction == appserver.FrameOutbound {
			outboundMu.Lock()
			outbound = append(outbound, append([]byte(nil), frame.Payload...))
			outboundMu.Unlock()
		}
		return nil
	}
	factory := mekruntime.ConnectionFactory{Options: connection.Options{
		ClientName:       "mektup-callback-observer",
		ClientVersion:    "1.0.0",
		ExperimentalAPI:  true,
		HandshakeTimeout: 5 * time.Second,
		FrameObserver:    observer,
	}}
	lookup := func(id string) (endpoint.Endpoint, error) {
		if id != ep.ID {
			return endpoint.Endpoint{}, endpoint.ErrEndpointNotFound
		}
		return ep, nil
	}
	pool := mekruntime.NewConnectionPool(factory, lookup)
	defer pool.Close(context.Background())
	adapter := &mekruntime.ObservationAdapter{Pool: pool, Blockers: blockers}
	target := service.ResolvedTarget{EndpointID: ep.ID, URI: "codex://" + ep.ID + "/thread/" + threadID, ThreadID: threadID, Loaded: true, Persistent: true}

	first, err := adapter.Subscribe(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	firstDone := drainObservedStream(first)

	tui.request(t, ctx, "turn", "turn/start", map[string]any{
		"threadId": threadID,
		"input":    []map[string]any{{"type": "text", "text": "ask something"}},
		"model":    "mock-model",
		"effort":   "medium",
		"collaborationMode": map[string]any{
			"mode":     "plan",
			"settings": map[string]any{"model": "mock-model", "reasoningEffort": "medium"},
		},
	})
	request := tui.waitRequest(t, ctx, "item/tool/requestUserInput")
	waitForBlockers(t, blockers, ep.ID, threadID, 1, 0)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-firstDone:
	case <-time.After(3 * time.Second):
		t.Fatal("first Mektup observer did not detach")
	}

	second, err := adapter.Subscribe(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	secondDone := drainObservedStream(second)
	// Resume replays the still-pending request. Upsert must remain one bounded
	// metadata row, and the second Mektup client must remain passive too.
	waitForBlockers(t, blockers, ep.ID, threadID, 2, 0)

	tui.respond(t, request.ID, map[string]any{
		"answers": map[string]any{"confirm_path": map[string]any{"answers": []string{"yes"}}},
	})
	tui.waitNotification(t, ctx, "serverRequest/resolved")
	tui.waitNotification(t, ctx, "turn/completed")
	waitForBlockers(t, blockers, ep.ID, threadID, 2, 1)
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-secondDone:
	case <-time.After(3 * time.Second):
		t.Fatal("second Mektup observer did not detach")
	}

	rows, err := blockers.ListBlockers(ctx, journal.BlockerQuery{EndpointID: ep.ID, ThreadID: threadID, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("blocker metadata = %#v", rows)
	}
	resolved := 0
	for _, row := range rows {
		if row.Method != "item/tool/requestUserInput" {
			t.Fatalf("blocker metadata = %#v", rows)
		}
		if row.ResolvedAt != nil {
			resolved++
		}
	}
	if resolved != 1 {
		t.Fatalf("resolved blocker generations = %d, want 1: %#v", resolved, rows)
	}
	outboundMu.Lock()
	frames := append([][]byte(nil), outbound...)
	outboundMu.Unlock()
	for _, frame := range frames {
		var object map[string]json.RawMessage
		if json.Unmarshal(frame, &object) == nil && object["id"] != nil && object["method"] == nil {
			t.Fatalf("Mektup answered a shared server request: %s", frame)
		}
	}
	assertSentinelAbsent(t, stateDir)
}

func newCallbackResponsesServer(t *testing.T) *httptest.Server {
	t.Helper()
	var call atomic.Int32
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		defer request.Body.Close()
		if !strings.HasSuffix(request.URL.Path, "/responses") {
			http.NotFound(w, request)
			return
		}
		_, _ = io.Copy(io.Discard, request.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		switch call.Add(1) {
		case 1:
			writeSSE(w,
				map[string]any{"type": "response.output_item.done", "item": map[string]any{"type": "message", "role": "assistant", "id": "warmup-message", "content": []map[string]any{{"type": "output_text", "text": "ready"}}}},
				completedEvent("warmup-response"),
			)
			return
		case 2:
			arguments, _ := json.Marshal(map[string]any{"questions": []map[string]any{{
				"id": "confirm_path", "header": "Confirm", "question": callbackPayloadSentinel,
				"options": []map[string]any{{"label": "Yes (Recommended)", "description": "Continue."}, {"label": "No", "description": "Stop."}},
			}}})
			writeSSE(w,
				map[string]any{"type": "response.created", "response": map[string]any{"id": "resp-1"}},
				map[string]any{"type": "response.output_item.done", "item": map[string]any{"type": "function_call", "call_id": "call1", "name": "request_user_input", "arguments": string(arguments)}},
				completedEvent("resp-1"),
			)
			return
		}
		writeSSE(w,
			map[string]any{
				"type": "response.output_item.done",
				"item": map[string]any{
					"type": "message", "role": "assistant", "id": "msg-1",
					"content": []map[string]any{{"type": "output_text", "text": "thanks"}},
				},
			},
			completedEvent("resp-2"),
		)
	}))
}

func writeSSE(w io.Writer, events ...map[string]any) {
	for _, event := range events {
		encoded, _ := json.Marshal(event)
		fmt.Fprintf(w, "event: %s\ndata: %s\n\n", event["type"], encoded)
	}
}

func completedEvent(id string) map[string]any {
	return map[string]any{"type": "response.completed", "response": map[string]any{
		"id":    id,
		"usage": map[string]any{"input_tokens": 0, "input_tokens_details": nil, "output_tokens": 0, "output_tokens_details": nil, "total_tokens": 0},
	}}
}

type rawMessage struct {
	ID     json.RawMessage `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
	Result json.RawMessage `json:"result"`
	Error  json.RawMessage `json:"error"`
}

type rawTUI struct {
	conn     *websocket.Conn
	messages chan rawMessage
	err      chan error
	writes   sync.Mutex
}

func dialRawTUI(t *testing.T, ctx context.Context, socket string) *rawTUI {
	t.Helper()
	dialer := websocket.Dialer{NetDialContext: func(dialCtx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(dialCtx, "unix", socket)
	}}
	conn, _, err := dialer.DialContext(ctx, "ws://localhost/rpc", nil)
	if err != nil {
		t.Fatal(err)
	}
	tui := &rawTUI{conn: conn, messages: make(chan rawMessage, 128), err: make(chan error, 1)}
	go func() {
		defer close(tui.messages)
		for {
			_, payload, readErr := conn.ReadMessage()
			if readErr != nil {
				tui.err <- readErr
				return
			}
			var message rawMessage
			if json.Unmarshal(payload, &message) == nil {
				tui.messages <- message
			}
		}
	}()
	return tui
}

func (tui *rawTUI) Close() { _ = tui.conn.Close() }

func (tui *rawTUI) write(t *testing.T, value any) {
	t.Helper()
	tui.writes.Lock()
	defer tui.writes.Unlock()
	if err := tui.conn.WriteJSON(value); err != nil {
		t.Fatal(err)
	}
}

func (tui *rawTUI) request(t *testing.T, ctx context.Context, id, method string, params any) json.RawMessage {
	t.Helper()
	tui.write(t, map[string]any{"id": id, "method": method, "params": params})
	for {
		message := tui.next(t, ctx)
		if string(message.ID) != mustJSON(t, id) {
			continue
		}
		if len(message.Error) != 0 && string(message.Error) != "null" {
			t.Fatalf("%s failed: %s", method, message.Error)
		}
		return message.Result
	}
}

func (tui *rawTUI) notify(t *testing.T, method string, params any) {
	t.Helper()
	message := map[string]any{"method": method}
	if params != nil {
		message["params"] = params
	}
	tui.write(t, message)
}

func (tui *rawTUI) respond(t *testing.T, id json.RawMessage, result any) {
	t.Helper()
	var decoded any
	decoder := json.NewDecoder(bytes.NewReader(id))
	decoder.UseNumber()
	if err := decoder.Decode(&decoded); err != nil {
		t.Fatal(err)
	}
	tui.write(t, map[string]any{"id": decoded, "result": result})
}

func (tui *rawTUI) waitRequest(t *testing.T, ctx context.Context, method string) rawMessage {
	t.Helper()
	for {
		message := tui.next(t, ctx)
		if message.Method == method && len(message.ID) != 0 {
			return message
		}
	}
}

func (tui *rawTUI) waitNotification(t *testing.T, ctx context.Context, method string) rawMessage {
	t.Helper()
	for {
		message := tui.next(t, ctx)
		if message.Method == method && len(message.ID) == 0 {
			return message
		}
	}
}

func (tui *rawTUI) next(t *testing.T, ctx context.Context) rawMessage {
	t.Helper()
	select {
	case message, ok := <-tui.messages:
		if !ok {
			t.Fatal("raw TUI stream closed")
		}
		return message
	case err := <-tui.err:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	return rawMessage{}
}

type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(payload []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(payload)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func waitForUnixSocket(t *testing.T, command *exec.Cmd, socket string, output *lockedBuffer) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if info, err := os.Lstat(socket); err == nil && info.Mode()&os.ModeSocket != 0 {
			return
		}
		if command.ProcessState != nil && command.ProcessState.Exited() {
			t.Fatalf("app-server exited: %s", output.String())
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("app-server socket not ready: %s", output.String())
}

func waitForRegularFile(t *testing.T, name string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if info, err := os.Stat(name); err == nil && info.Mode().IsRegular() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("regular file not ready: %s", name)
}

func nestedString(t *testing.T, raw json.RawMessage, object, field string) string {
	t.Helper()
	var root map[string]json.RawMessage
	if err := json.Unmarshal(raw, &root); err != nil {
		t.Fatalf("decode %s.%s from %s: %v", object, field, raw, err)
	}
	var decoded map[string]json.RawMessage
	if err := json.Unmarshal(root[object], &decoded); err != nil {
		t.Fatalf("decode %s from %s: %v", object, raw, err)
	}
	var value string
	if err := json.Unmarshal(decoded[field], &value); err != nil || value == "" {
		t.Fatalf("%s.%s missing in %s", object, field, raw)
	}
	return value
}

func mustJSON(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func drainObservedStream(stream service.EventStream) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			_, err := stream.Next(context.Background())
			if err != nil {
				return
			}
		}
	}()
	return done
}

func waitForBlockers(t *testing.T, store *journal.Journal, endpointID, threadID string, count, resolved int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		rows, err := store.ListBlockers(context.Background(), journal.BlockerQuery{EndpointID: endpointID, ThreadID: threadID, Limit: 10})
		if err == nil && len(rows) == count {
			resolvedCount := 0
			for _, row := range rows {
				if row.ResolvedAt != nil {
					resolvedCount++
				}
			}
			if resolvedCount == resolved {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("blockers count=%d resolved=%d were not journaled", count, resolved)
}

func assertSentinelAbsent(t *testing.T, stateDir string) {
	t.Helper()
	entries, err := os.ReadDir(stateDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		content, readErr := os.ReadFile(filepath.Join(stateDir, entry.Name()))
		if readErr != nil && !errors.Is(readErr, os.ErrNotExist) {
			t.Fatal(readErr)
		}
		if bytes.Contains(content, []byte(callbackPayloadSentinel)) {
			t.Fatalf("callback payload persisted in %s", entry.Name())
		}
	}
}
