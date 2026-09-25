package liveacceptance

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mektup "github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/appserver"
	"github.com/agensfield/mektup/go/internal/codexapi"
	"github.com/agensfield/mektup/go/internal/connection"
	"github.com/agensfield/mektup/go/internal/endpoint"
	"github.com/agensfield/mektup/go/internal/journal"
	mekruntime "github.com/agensfield/mektup/go/internal/runtime"
	"github.com/agensfield/mektup/go/internal/service"
	"github.com/agensfield/mektup/go/internal/sshproxy"
)

// This test runs only inside the disposable release-qualification container.
// The app-server, provider, threads, journal, and socket all live there.
func TestIsolatedMessagingLifecycle(t *testing.T) {
	if os.Getenv("MEKTUP_COMPAT_CONTAINER") != "1" {
		t.Skip("run through scripts/qualify-codex-release.sh")
	}
	codexBinary := requiredExecutable(t, "MEKTUP_ACCEPT_CODEX_BINARY", "codex")
	var calls atomic.Int64
	responses := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		defer request.Body.Close()
		if !strings.HasSuffix(request.URL.Path, "/responses") {
			http.NotFound(w, request)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		id := fmt.Sprintf("qualifier-response-%d", calls.Add(1))
		writeSSE(w,
			map[string]any{"type": "response.output_item.done", "item": map[string]any{"type": "message", "role": "assistant", "id": id, "content": []map[string]any{{"type": "output_text", "text": "ready"}}}},
			completedEvent(id),
		)
	}))
	defer responses.Close()

	root := t.TempDir()
	codexHome := filepath.Join(root, "codex-home")
	if err := os.Mkdir(codexHome, 0700); err != nil {
		t.Fatal(err)
	}
	config := fmt.Sprintf("model = \"mock-model\"\napproval_policy = \"never\"\nsandbox_mode = \"read-only\"\nmodel_provider = \"mock_provider\"\n[model_providers.mock_provider]\nname = \"Mektup qualification\"\nbase_url = %q\nwire_api = \"responses\"\nrequest_max_retries = 0\nstream_max_retries = 0\n", responses.URL+"/v1")
	if err := os.WriteFile(filepath.Join(codexHome, "config.toml"), []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(root, "app-server.sock")
	daemonCtx, stopDaemon := context.WithCancel(context.Background())
	defer stopDaemon()
	command := exec.CommandContext(daemonCtx, codexBinary, "app-server", "--listen", "unix://"+socket)
	command.Env = replaceEnvironment(os.Environ(), "CODEX_HOME", codexHome)
	var daemonLog lockedBuffer
	command.Stdout, command.Stderr = &daemonLog, &daemonLog
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopProcess(command, stopDaemon) })
	waitForUnixSocket(t, command, socket, &daemonLog)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	route, err := endpoint.UnixRoute(socket)
	if err != nil {
		t.Fatal(err)
	}
	var methodsMu sync.Mutex
	methodsSeen := map[string]bool{}
	methodObserver := func(direction appserver.FrameDirection, frame appserver.Frame) error {
		if direction != appserver.FrameOutbound {
			return nil
		}
		var request struct {
			Method string `json:"method"`
		}
		if json.Unmarshal(frame.Payload, &request) == nil && request.Method != "" {
			methodsMu.Lock()
			methodsSeen[request.Method] = true
			methodsMu.Unlock()
		}
		return nil
	}
	admin, err := connection.Connect(ctx, route, connection.Options{ClientName: "mektup-qualifier", ClientVersion: "1.0.0", ExperimentalAPI: true, HandshakeTimeout: 5 * time.Second, FrameObserver: methodObserver})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer closeCancel()
		_ = admin.Close(closeCtx)
	})
	info := admin.Info()
	if expected := os.Getenv("MEKTUP_ACCEPT_CODEX_VERSION"); expected == "" || info.Compatibility.Version != expected {
		t.Fatalf("initialize version = %q, expected %q", info.Compatibility.Version, expected)
	}
	api := codexapi.New(admin, codexapi.Options{Capabilities: admin.Capabilities()})
	source, err := api.ThreadStart(ctx, codexapi.StartOptions{Model: "mock-model", CWD: root})
	if err != nil {
		t.Fatal(err)
	}
	target, err := api.ThreadStart(ctx, codexapi.StartOptions{Model: "mock-model", CWD: root})
	if err != nil {
		t.Fatal(err)
	}
	if source.Thread.ID == "" || target.Thread.ID == "" || source.Thread.ID == target.Thread.ID {
		t.Fatalf("invalid native thread identities: %q %q", source.Thread.ID, target.Thread.ID)
	}
	if _, err := api.ThreadList(ctx, codexapi.ThreadListOptions{Limit: 2}); err != nil {
		t.Fatalf("thread/list: %v", err)
	}
	if _, err := api.ThreadLoadedList(ctx, "", 2); err != nil {
		t.Fatalf("thread/loaded/list: %v", err)
	}
	if read, err := api.ThreadRead(ctx, codexapi.ThreadReadOptions{ThreadID: target.Thread.ID}); err != nil || read.Thread.ID != target.Thread.ID {
		t.Fatalf("thread/read: %q %v", read.Thread.ID, err)
	}
	proxyRoute, err := endpoint.SSHRoute("qualification-host")
	if err != nil {
		t.Fatal(err)
	}
	proxyClient, err := connection.Connect(ctx, proxyRoute, connection.Options{
		ClientName: "mektup-qualifier-proxy", ClientVersion: "1.0.0", HandshakeTimeout: 5 * time.Second,
		ClientDialer: connection.NewSSHClientDialer(sshproxy.Config{Host: "qualification-host"}, qualifierProxyFactory{binary: codexBinary, socket: socket, codexHome: codexHome}),
	})
	if err != nil {
		t.Fatalf("codex app-server proxy: %v", err)
	}
	proxyAPI := codexapi.New(proxyClient, codexapi.Options{Capabilities: proxyClient.Capabilities()})
	if read, err := proxyAPI.ThreadRead(ctx, codexapi.ThreadReadOptions{ThreadID: target.Thread.ID}); err != nil || read.Thread.ID != target.Thread.ID {
		t.Fatalf("proxied thread/read: %q %v", read.Thread.ID, err)
	}
	if err := proxyClient.Close(ctx); err != nil {
		t.Fatalf("proxy cleanup: %v", err)
	}

	endpointID := "ep_01999999-9999-7999-8999-999999999995"
	ep := endpoint.Endpoint{ID: endpointID, Alias: "qualification", Route: route, Herdr: endpoint.HerdrDisabled}
	pool := mekruntime.NewConnectionPool(mekruntime.ConnectionFactory{Options: connection.Options{ClientName: "mektup-qualifier-body", ClientVersion: "1.0.0", HandshakeTimeout: 5 * time.Second, FrameObserver: methodObserver}}, func(id string) (endpoint.Endpoint, error) {
		if id != endpointID {
			return endpoint.Endpoint{}, endpoint.ErrEndpointNotFound
		}
		return ep, nil
	})
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer closeCancel()
		_ = pool.Close(closeCtx)
	})
	stateDir := filepath.Join(root, "state")
	store, err := journal.Open(ctx, journal.Options{StateDir: stateDir})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	journalAdapter, err := mekruntime.NewJournalAdapter(store, service.NewMemoryIdentityRegistry())
	if err != nil {
		t.Fatal(err)
	}
	sourceURI := "codex://" + endpointID + "/thread/" + source.Thread.ID
	targetURI := "codex://" + endpointID + "/thread/" + target.Thread.ID
	targetResolved := service.ResolvedTarget{EndpointID: endpointID, URI: targetURI, ThreadID: target.Thread.ID, Loaded: true, Persistent: true}
	observation := &mekruntime.ObservationAdapter{Pool: pool}
	sender := &service.Service{Resolver: qualifierResolver{source: service.SourceIdentity{EndpointID: endpointID, URI: sourceURI, CustodyEndpointID: endpointID, CustodyStoreID: store.StoreID()}, target: targetResolved}, Delivery: &mekruntime.DeliveryAdapter{Pool: pool}, Journal: journalAdapter, Observe: observation}
	receiver := &service.Service{Resolver: qualifierResolver{source: service.SourceIdentity{EndpointID: endpointID, URI: targetURI, CustodyEndpointID: endpointID, CustodyStoreID: store.StoreID()}, target: targetResolved}, Delivery: &mekruntime.DeliveryAdapter{Pool: pool}, Journal: journalAdapter, Observe: observation}
	sent, err := sender.Send(ctx, service.SendRequest{Target: targetURI, Source: sourceURI, Body: "Mektup qualification body", RequestReply: true})
	if err != nil {
		t.Fatalf("real body send: %v", err)
	}
	if sent.Receipt.Message.MessageID == "" || sent.Receipt.Message.PayloadBytes == 0 {
		t.Fatalf("accepted send lacks durable identity: %+v", sent.Receipt)
	}
	originalResolver := mekruntime.OriginalResolver{Observe: observation, Target: targetResolved}
	var original service.OriginalMessage
	for original.Envelope.MessageID == "" && ctx.Err() == nil {
		original, err = originalResolver.ResolveOriginal(ctx, sent.Receipt.Message.MessageID)
		if err != nil {
			time.Sleep(20 * time.Millisecond)
		}
	}
	if err != nil {
		t.Fatalf("native original discovery: %v", err)
	}
	replied, err := receiver.Reply(ctx, originalResolver, service.ReplyRequest{Reference: original.Envelope.MessageID, Source: targetURI, Body: "Mektup qualification reply"})
	if err != nil {
		t.Fatalf("real correlated reply: %v", err)
	}
	waited, err := sender.Wait(ctx, service.WaitRequest{Reference: sent.Receipt.Message.MessageID, Timeout: 15 * time.Second})
	if err != nil || waited.ReplyID != replied.Receipt.Message.MessageID || waited.Incomplete {
		t.Fatalf("custody wait = %+v, %v", waited, err)
	}
	items, err := observation.FullHistory(ctx, service.ResolvedTarget{EndpointID: endpointID, URI: sourceURI, ThreadID: source.Thread.ID, Loaded: true, Persistent: true})
	if err != nil {
		t.Fatal(err)
	}
	matches := 0
	for _, item := range items {
		envelope, parseErr := mektup.ParseEnvelopeString(item.Text)
		if parseErr == nil && item.NativeType == "userMessage" && item.ClientMessageID == replied.Receipt.Message.MessageID && envelope.InReplyTo == sent.Receipt.Message.MessageID && envelope.Body == "Mektup qualification reply" {
			matches++
		}
	}
	if matches != 1 {
		t.Fatalf("native reply history has %d exact matches, want one", matches)
	}
	if _, err := api.ThreadTurns(ctx, codexapi.TurnsOptions{ThreadID: target.Thread.ID, Limit: 5, ItemsView: "full"}); err != nil {
		t.Fatalf("thread/turns/list full: %v", err)
	}
	if _, err := api.ThreadTurns(ctx, codexapi.TurnsOptions{ThreadID: target.Thread.ID, Limit: 5, ItemsView: "summary"}); err != nil {
		t.Fatalf("thread/turns/list summary: %v", err)
	}
	if _, err := api.ThreadItems(ctx, codexapi.ItemsOptions{ThreadID: target.Thread.ID, Limit: 5}); err != nil {
		t.Fatalf("thread/items/list: %v", err)
	}
	if _, err := api.ThreadRead(ctx, codexapi.ThreadReadOptions{ThreadID: target.Thread.ID, IncludeTurns: true}); err != nil {
		t.Fatalf("thread/read full: %v", err)
	}
	if _, err := api.Search(ctx, codexapi.SearchOptions{SearchTerm: "qualification", Limit: 5}); err != nil {
		t.Fatalf("thread/search: %v", err)
	}
	if _, err := api.SearchOccurrences(ctx, codexapi.SearchOccurrencesOptions{ThreadID: target.Thread.ID, SearchTerm: "qualification", Limit: 5}); err != nil {
		t.Fatalf("thread/searchOccurrences: %v", err)
	}
	excludeTurns := true
	if resumed, err := api.ThreadResume(ctx, codexapi.ResumeOptions{ThreadID: target.Thread.ID, ExcludeTurns: &excludeTurns}); err != nil || resumed.Thread.ID != target.Thread.ID {
		t.Fatalf("thread/resume: %q %v", resumed.Thread.ID, err)
	}
	forked, err := api.ThreadFork(ctx, codexapi.ForkOptions{ThreadID: target.Thread.ID, ExcludeTurns: &excludeTurns})
	if err != nil || forked.Thread.ID == "" || forked.Thread.ID == target.Thread.ID {
		t.Fatalf("thread/fork: %q %v", forked.Thread.ID, err)
	}
	if _, err := api.ThreadSetName(ctx, forked.Thread.ID, "qualification-fork"); err != nil {
		t.Fatalf("thread/name/set: %v", err)
	}
	if _, err := api.ThreadUnsubscribe(ctx, forked.Thread.ID); err != nil {
		t.Fatalf("thread/unsubscribe: %v", err)
	}
	if _, err := api.ThreadRead(ctx, codexapi.ThreadReadOptions{ThreadID: "not-a-thread-id"}); err == nil {
		t.Fatal("invalid native thread/read unexpectedly succeeded")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := journal.OpenExisting(ctx, journal.Options{StateDir: stateDir})
	if err != nil {
		t.Fatalf("restart journal: %v", err)
	}
	defer reopened.Close()
	recovered, err := mekruntime.NewJournalAdapter(reopened, nil)
	if err != nil {
		t.Fatal(err)
	}
	status, err := recovered.Lookup(ctx, sent.Receipt.Message.MessageID)
	if err != nil || status.ReplyID != replied.Receipt.Message.MessageID {
		t.Fatalf("durable reply after restart: %+v, %v", status, err)
	}
	methodsMu.Lock()
	for _, method := range requiredNativeMethods(t) {
		if !methodsSeen[method] {
			methodsMu.Unlock()
			t.Fatalf("Mektup-owned native method %s was not exercised", method)
		}
	}
	methodsMu.Unlock()
	t.Logf("codex=%s sha256=%s initialize=%s source=%s target=%s original=%s reply=%s", codexBinary, fileSHA256(t, codexBinary), info.ServerUserAgent, source.Thread.ID, target.Thread.ID, sent.Receipt.Message.MessageID, waited.ReplyID)
}

func requiredNativeMethods(t *testing.T) []string {
	t.Helper()
	pattern := regexp.MustCompile(`"(?:thread|turn)/[A-Za-z/]+"`)
	seen := map[string]bool{}
	for _, source := range []string{"../codexapi/api.go", "../runtime/runtime.go"} {
		body, err := os.ReadFile(source)
		if err != nil {
			t.Fatal(err)
		}
		for _, match := range pattern.FindAllString(string(body), -1) {
			seen[strings.Trim(match, `"`)] = true
		}
	}
	methods := make([]string, 0, len(seen))
	for method := range seen {
		methods = append(methods, method)
	}
	return methods
}

type qualifierResolver struct {
	source service.SourceIdentity
	target service.ResolvedTarget
}

type qualifierProxyFactory struct {
	binary, socket, codexHome string
}

func (f qualifierProxyFactory) New(argv []string) (sshproxy.Process, error) {
	if !reflect.DeepEqual(argv, []string{"ssh", "--", "qualification-host", "codex", "app-server", "proxy"}) {
		return nil, fmt.Errorf("unexpected proxy argv: %q", argv)
	}
	command := exec.Command(f.binary, "app-server", "proxy", "--sock", f.socket)
	command.Env = replaceEnvironment(os.Environ(), "CODEX_HOME", f.codexHome)
	return qualifierProxyProcess{command}, nil
}

type qualifierProxyProcess struct{ command *exec.Cmd }

func (p qualifierProxyProcess) StdinPipe() (io.WriteCloser, error) { return p.command.StdinPipe() }
func (p qualifierProxyProcess) StdoutPipe() (io.ReadCloser, error) { return p.command.StdoutPipe() }
func (p qualifierProxyProcess) StderrPipe() (io.ReadCloser, error) { return p.command.StderrPipe() }
func (p qualifierProxyProcess) Start() error                       { return p.command.Start() }
func (p qualifierProxyProcess) Wait() error                        { return p.command.Wait() }
func (p qualifierProxyProcess) Kill() error                        { return p.command.Process.Kill() }

func (r qualifierResolver) Resolve(context.Context, string) (service.ResolvedTarget, error) {
	return r.target, nil
}
func (r qualifierResolver) ResolveSource(context.Context, string) (service.SourceIdentity, error) {
	return r.source, nil
}
func (r qualifierResolver) ResolvePinned(_ context.Context, endpointID, uri string) (service.ResolvedTarget, error) {
	if endpointID != r.source.EndpointID || !strings.HasPrefix(uri, "codex://"+endpointID+"/thread/") {
		return service.ResolvedTarget{}, endpoint.ErrEndpointNotFound
	}
	return service.ResolvedTarget{EndpointID: endpointID, URI: uri, ThreadID: strings.TrimPrefix(uri, "codex://"+endpointID+"/thread/"), Loaded: true, Persistent: true}, nil
}
