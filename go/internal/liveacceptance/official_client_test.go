package liveacceptance

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
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

// TestOfficialInteractiveClientSharedCallback is the locked spec-1.0.8 TUI
// acceptance. Codex's pinned official app-server test client owns the callback
// and answers through a real PTY; Mektup remains an observer on the same daemon
// and thread across detach and pending replay.
func TestOfficialInteractiveClientSharedCallback(t *testing.T) {
	if os.Getenv("MEKTUP_ACCEPT_OFFICIAL_CLIENT") != "1" {
		t.Skip("set MEKTUP_ACCEPT_OFFICIAL_CLIENT=1 to run pinned official-client callback acceptance")
	}
	codexBinary := requiredExecutable(t, "MEKTUP_ACCEPT_CODEX_BINARY", "codex")
	officialClient := requiredExecutable(t, "MEKTUP_ACCEPT_OFFICIAL_CLIENT_BINARY", "codex-app-server-test-client")
	root, err := os.MkdirTemp("/tmp", "mektup-official-client-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })

	responses := newCallbackResponsesServer(t)
	defer responses.Close()
	codexHome := filepath.Join(root, "codex-home")
	if err := os.Mkdir(codexHome, 0700); err != nil {
		t.Fatal(err)
	}
	config := fmt.Sprintf(`model = "mock-model"
approval_policy = "on-request"
sandbox_mode = "read-only"
model_provider = "mock_provider"

[model_providers.mock_provider]
name = "Mektup official-client acceptance"
base_url = %q
wire_api = "responses"
request_max_retries = 0
stream_max_retries = 0
`, responses.URL+"/v1")
	if err := os.WriteFile(filepath.Join(codexHome, "config.toml"), []byte(config), 0600); err != nil {
		t.Fatal(err)
	}

	address := reserveTCPAddress(t)
	url := "ws://" + address
	daemonCtx, stopDaemon := context.WithCancel(context.Background())
	defer stopDaemon()
	daemon := exec.CommandContext(daemonCtx, codexBinary, "app-server", "--listen", url)
	daemon.Env = replaceEnvironment(os.Environ(), "CODEX_HOME", codexHome)
	var daemonLog lockedBuffer
	daemon.Stdout, daemon.Stderr = &daemonLog, &daemonLog
	if err := daemon.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopProcess(daemon, stopDaemon) })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin := dialRawTUITCP(t, ctx, address)
	defer admin.Close()
	initialize := admin.request(t, ctx, "init", "initialize", map[string]any{
		"clientInfo":   map[string]any{"name": "mektup-official-acceptance-admin", "version": "1.0.0"},
		"capabilities": map[string]any{"experimentalApi": true},
	})
	expectedVersion := os.Getenv("MEKTUP_ACCEPT_CODEX_VERSION")
	if expectedVersion != "" && !strings.Contains(string(initialize), `/`+expectedVersion+` `) {
		t.Fatalf("unexpected daemon initialize result: %s", initialize)
	}
	admin.notify(t, "initialized", nil)
	threadResult := admin.request(t, ctx, "thread", "thread/start", map[string]any{"model": "mock-model", "cwd": root})
	threadID := nestedString(t, threadResult, "thread", "id")
	rolloutPath := nestedString(t, threadResult, "thread", "path")
	admin.request(t, ctx, "warmup", "turn/start", map[string]any{
		"threadId": threadID, "input": []map[string]any{{"type": "text", "text": "persist this thread"}}, "model": "mock-model",
	})
	admin.waitNotification(t, ctx, "turn/completed")
	waitForRegularFile(t, rolloutPath)

	stateDir := filepath.Join(root, "state")
	blockers, err := journal.Open(ctx, journal.Options{StateDir: stateDir})
	if err != nil {
		t.Fatal(err)
	}
	defer blockers.Close()
	route, err := endpoint.UnixRoute(filepath.Join(root, "unused.sock"))
	if err != nil {
		t.Fatal(err)
	}
	ep := endpoint.Endpoint{ID: "ep_01a0a000-0000-7000-8000-000000000002", Alias: "official", Route: route, Herdr: endpoint.HerdrDisabled}
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
	dialer := connection.ClientDialFunc(func(dialCtx context.Context, _ endpoint.Route, options appserver.Options) (*appserver.Client, error) {
		return appserver.DialWebSocket(dialCtx, func(netCtx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(netCtx, "tcp", address)
		}, options)
	})
	factory := mekruntime.ConnectionFactory{Options: connection.Options{
		ClientName: "mektup-official-client-observer", ClientVersion: "1.0.0",
		ExperimentalAPI: true, HandshakeTimeout: 5 * time.Second,
		FrameObserver: observer, ClientDialer: dialer,
	}}
	pool := mekruntime.NewConnectionPool(factory, func(id string) (endpoint.Endpoint, error) {
		if id != ep.ID {
			return endpoint.Endpoint{}, endpoint.ErrEndpointNotFound
		}
		return ep, nil
	})
	defer pool.Close(context.Background())
	adapter := &mekruntime.ObservationAdapter{Pool: pool, Blockers: blockers}
	target := service.ResolvedTarget{EndpointID: ep.ID, URI: "codex://" + ep.ID + "/thread/" + threadID, ThreadID: threadID, Loaded: true, Persistent: true}
	first, err := adapter.Subscribe(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	firstDone := drainObservedStream(first)

	client := exec.Command("/usr/bin/script", "-q", "/dev/null", officialClient, "--url", url, "thread-resume", threadID)
	client.Env = replaceEnvironment(os.Environ(), "CODEX_HOME", codexHome)
	stdin, err := client.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	var clientLog lockedBuffer
	client.Stdout, client.Stderr = &clientLog, &clientLog
	if err := client.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopProcess(client, func() {}) })
	waitForOutput(t, &clientLog, "streaming notifications until process is terminated")

	admin.request(t, ctx, "turn", "turn/start", map[string]any{
		"threadId": threadID,
		"input":    []map[string]any{{"type": "text", "text": "ask something"}},
		"model":    "mock-model", "effort": "medium",
		"collaborationMode": map[string]any{"mode": "plan", "settings": map[string]any{"model": "mock-model", "reasoningEffort": "medium"}},
	})
	waitForOutput(t, &clientLog, "[request_user_input for thread "+threadID)
	waitForBlockers(t, blockers, ep.ID, threadID, 1, 0)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	waitDone(t, firstDone, "first observer detach")

	second, err := adapter.Subscribe(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	secondDone := drainObservedStream(second)
	waitForBlockers(t, blockers, ep.ID, threadID, 2, 0)
	if _, err := io.WriteString(stdin, "1\n"); err != nil {
		t.Fatal(err)
	}
	admin.waitNotification(t, ctx, "serverRequest/resolved")
	admin.waitNotification(t, ctx, "turn/completed")
	waitForBlockers(t, blockers, ep.ID, threadID, 2, 1)
	if err := second.Close(); err != nil {
		t.Fatal(err)
	}
	waitDone(t, secondDone, "second observer detach")

	approval, err := adapter.Subscribe(ctx, target)
	if err != nil {
		t.Fatal(err)
	}
	approvalDone := drainObservedStream(approval)
	admin.request(t, ctx, "approval-turn", "turn/start", map[string]any{
		"threadId": threadID,
		"input":    []map[string]any{{"type": "text", "text": "run the requested command"}},
		"model":    "mock-model",
	})
	waitForOutput(t, &clientLog, "commandExecution approval requested for thread "+threadID)
	admin.waitNotification(t, ctx, "serverRequest/resolved")
	admin.waitNotification(t, ctx, "turn/completed")
	waitForBlockers(t, blockers, ep.ID, threadID, 3, 2)
	if err := approval.Close(); err != nil {
		t.Fatal(err)
	}
	waitDone(t, approvalDone, "approval observer detach")
	_ = stdin.Close()

	text := clientLog.String()
	if !strings.Contains(text, `"method": "item/tool/requestUserInput"`) || !strings.Contains(text, `"answers"`) {
		t.Fatalf("official client did not receive and answer callback: %s", text)
	}
	for _, capability := range []string{
		`"name": "codex-toy-app-server"`, `"experimentalApi": true`,
		`"requestAttestation": false`,
	} {
		if !strings.Contains(text, capability) {
			t.Fatalf("official initialize omitted %s: %s", capability, text)
		}
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
	assertTextAbsent(t, stateDir, "mektup-approval-payload")
	t.Logf("daemon=%s version=%s sha256=%s initialize=%s", codexBinary, executableVersion(t, codexBinary), fileSHA256(t, codexBinary), initialize)
	t.Logf("official_client=%s version=%s sha256=%s", officialClient, executableVersion(t, officialClient), fileSHA256(t, officialClient))
}

func requiredExecutable(t *testing.T, environment, fallback string) string {
	t.Helper()
	name := os.Getenv(environment)
	if name == "" {
		name = fallback
	}
	resolved, err := exec.LookPath(name)
	if err != nil {
		t.Fatalf("%s: %v", environment, err)
	}
	return resolved
}

func reserveTCPAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func dialRawTUITCP(t *testing.T, ctx context.Context, address string) *rawTUI {
	t.Helper()
	dialer := websocket.Dialer{}
	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, _, err := dialer.DialContext(ctx, "ws://"+address, nil)
		if err == nil {
			tui := &rawTUI{conn: conn, messages: make(chan rawMessage, 128), err: make(chan error, 1)}
			go tui.readLoop()
			return tui
		}
		if time.Now().After(deadline) {
			t.Fatalf("dial app-server %s: %v", address, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (tui *rawTUI) readLoop() {
	defer close(tui.messages)
	for {
		_, payload, err := tui.conn.ReadMessage()
		if err != nil {
			tui.err <- err
			return
		}
		var message rawMessage
		if json.Unmarshal(payload, &message) == nil {
			tui.messages <- message
		}
	}
}

func replaceEnvironment(environment []string, name, value string) []string {
	prefix := name + "="
	out := make([]string, 0, len(environment)+1)
	for _, entry := range environment {
		if !strings.HasPrefix(entry, prefix) {
			out = append(out, entry)
		}
	}
	return append(out, prefix+value)
}

func stopProcess(command *exec.Cmd, cancel func()) {
	cancel()
	if command == nil || command.Process == nil {
		return
	}
	_ = command.Process.Kill()
	done := make(chan struct{})
	go func() {
		_ = command.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
	}
}

func waitForOutput(t *testing.T, output *lockedBuffer, expected string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(output.String(), expected) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("official client output omitted %q: %s", expected, output.String())
}

func waitDone(t *testing.T, done <-chan struct{}, label string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("%s timed out", label)
	}
}

func fileSHA256(t *testing.T, name string) string {
	t.Helper()
	file, err := os.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func assertTextAbsent(t *testing.T, directory, forbidden string) {
	t.Helper()
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		content, readErr := os.ReadFile(filepath.Join(directory, entry.Name()))
		if readErr == nil && strings.Contains(string(content), forbidden) {
			t.Fatalf("forbidden callback payload persisted in %s", entry.Name())
		}
	}
}

func executableVersion(t *testing.T, name string) string {
	t.Helper()
	output, err := exec.Command(name, "--version").CombinedOutput()
	if err != nil {
		t.Fatalf("%s --version: %v: %s", name, err, output)
	}
	return strings.TrimSpace(string(output))
}
