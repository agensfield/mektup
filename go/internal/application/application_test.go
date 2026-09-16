package application

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	mektup "github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/appserver"
	"github.com/agensfield/mektup/go/internal/artifact"
	"github.com/agensfield/mektup/go/internal/cli"
	"github.com/agensfield/mektup/go/internal/codexapi"
	"github.com/agensfield/mektup/go/internal/connection"
	"github.com/agensfield/mektup/go/internal/controlreceiver"
	"github.com/agensfield/mektup/go/internal/endpoint"
	"github.com/agensfield/mektup/go/internal/executor"
	"github.com/agensfield/mektup/go/internal/journal"
	"github.com/agensfield/mektup/go/internal/logging"
	"github.com/agensfield/mektup/go/internal/rawrpc"
	"github.com/agensfield/mektup/go/internal/runtime"
	"github.com/agensfield/mektup/go/internal/sshproxy"
	_ "modernc.org/sqlite"
)

type fakeTransport struct {
	reads   chan appserver.Frame
	done    chan struct{}
	once    sync.Once
	onWrite func([]byte)
}

func TestReceiptHistoryRejectsWrongEnvelopeDestination(t *testing.T) {
	root := t.TempDir()
	codexHome := filepath.Join(root, "codex")
	if err := os.MkdirAll(filepath.Join(codexHome, "app-server-control"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(codexHome, "app-server-control", "app-server-control.sock"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	configured := endpoint.NewStore(filepath.Join(root, "endpoints.json"), filepath.Join(root, "state"))
	configured.IdentityHome = filepath.Join(root, "identity")
	local, err := configured.EnsureBuiltinLocal(codexHome)
	if err != nil {
		t.Fatal(err)
	}
	identityJournal, err := journal.Open(context.Background(), journal.Options{StateDir: filepath.Join(root, "state")})
	if err != nil {
		t.Fatal(err)
	}
	storeID := identityJournal.StoreID()
	if _, err := identityJournal.Prepare(context.Background(), journal.Operation{OperationID: "op_0198f0e0-0000-7000-8000-000000000120", MessageID: "msg_0198f0e0-0000-7000-8000-000000000121", SourceRoute: "codex://" + local.ID + "/thread/source", TargetRoute: "codex://" + local.ID + "/thread/source", Semantics: "message", SourceEndpointID: local.ID, TargetEndpointID: local.ID, ReplyRoute: "codex://" + local.ID + "/thread/source", ReplyEndpointID: local.ID, CustodyRoute: local.ID, CustodyStoreID: storeID, Digest: "sha256:" + strings.Repeat("a", 64), BodySize: 8}); err != nil {
		t.Fatal(err)
	}
	_ = identityJournal.Close()
	original := mektup.Envelope{MessageID: "msg_0198f0e0-0000-7000-8000-000000000121", Kind: mektup.KindMessage, FromEndpointID: local.ID, From: "codex://" + local.ID + "/thread/source", FromKind: "agent", ToEndpointID: mektup.NewEndpointID(), To: "codex://wrong/thread/wrong", RequestedTarget: "source", ReplyRequested: true, ReplyEndpointID: local.ID, ReplyTo: "codex://" + local.ID + "/thread/source", ReplyCustodyEndpointID: local.ID, ReplyCustodyStoreID: storeID, Body: "question", Provenance: "observed", SentAt: time.Now().UTC().Format(time.RFC3339Nano)}
	original.PayloadBytes = uint64(len(original.Body))
	digest := sha256.Sum256([]byte(original.Body))
	original.PayloadSHA256 = "sha256:" + hex.EncodeToString(digest[:])
	original.Kind = mektup.KindReply
	original.InReplyTo = mektup.NewMessageID()
	original.ReplyStatus = mektup.ReplySuccess
	envelopeText, err := mektup.RenderEnvelope(original)
	if err != nil {
		t.Fatal(err)
	}
	content, _ := json.Marshal([]map[string]string{{"type": "text", "text": string(envelopeText)}})
	item, _ := json.Marshal(map[string]any{"type": "userMessage", "id": "item_0198f0e0-0000-7000-8000-000000000122", "clientId": original.MessageID, "content": json.RawMessage(content)})
	items := json.RawMessage("[" + string(item) + "]")
	session := originalMessagingSession{messagingFakeSession: messagingFakeSession{endpointID: local.ID}, history: []codexapi.Turn{{ID: "turn_0198f0e0-0000-7000-8000-000000000123", RawObject: codexapi.RawObject{Fields: map[string]json.RawMessage{"items": items}}}}}
	pool := runtime.NewConnectionPool(runtime.SessionFactoryFunc(func(context.Context, endpoint.Endpoint) (runtime.Session, error) { return session, nil }), func(string) (endpoint.Endpoint, error) { return local, nil })
	defer pool.Close(context.Background())
	history := runtimeHistory{observe: &runtime.ObservationAdapter{Pool: pool}}
	reviewItems, err := history.FullHistory(context.Background(), local.ID, "source")
	if err != nil {
		t.Fatal(err)
	}
	if len(reviewItems) != 0 {
		t.Fatalf("wrong envelope destination was relabeled as pinned endpoint/thread: %+v", reviewItems)
	}
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{reads: make(chan appserver.Frame, 16), done: make(chan struct{})}
}

func TestAuditCaptureFinalizesExactFramedTrafficAndOverlimitIsAbsent(t *testing.T) {
	logger, err := logging.New(logging.DefaultConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	capture := &auditCapture{logger: logger, invocation: "audit-test", max: logging.DefaultAuditMaxBytes}
	if err := capture.Observe(appserver.FrameOutbound, appserver.Frame{Type: appserver.FrameText, Payload: []byte(`{"method":"x","params":{"secret":"keep-in-audit"}}`)}); err != nil {
		t.Fatal(err)
	}
	if err := capture.Observe(appserver.FrameInbound, appserver.Frame{Type: appserver.FrameText, Payload: []byte(`{"result":{"ok":true}}`)}); err != nil {
		t.Fatal(err)
	}
	receipt, err := capture.Finalize(context.Background())
	if err != nil || !receipt.Complete || receipt.Bytes == 0 || !receipt.Mode.Sensitive || receipt.Mode.NetworkTelemetry {
		t.Fatalf("audit receipt=%+v err=%v", receipt, err)
	}
	data, err := os.ReadFile(receipt.Path)
	if err != nil || !strings.Contains(string(data), "outbound text") || !strings.Contains(string(data), "inbound text") || !strings.Contains(string(data), "keep-in-audit") {
		t.Fatalf("audit body=%s err=%v", data, err)
	}
	tinyLogger, err := logging.New(logging.Config{Dir: t.TempDir(), AuditMaxBytes: 4})
	if err != nil {
		t.Fatal(err)
	}
	tiny := &auditCapture{logger: tinyLogger, invocation: "audit-too-large", max: 4}
	if err := tiny.Observe(appserver.FrameOutbound, appserver.Frame{Type: appserver.FrameText, Payload: []byte("too-large")}); err == nil {
		t.Fatal("overlimit frame was accepted")
	}
	if _, err := os.Stat(filepath.Join(tinyLogger.Config().Dir, "audit", "audit-too-large.audit")); !os.IsNotExist(err) {
		t.Fatalf("overlimit audit artifact exists: %v", err)
	}
}

func TestMessagingAuditReceiptProgressionStrengthensAtTerminal(t *testing.T) {
	logger, err := logging.New(logging.DefaultConfig(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	capture := &auditCapture{logger: logger, invocation: "audit-stream", max: logging.DefaultAuditMaxBytes}
	_ = capture.Observe(appserver.FrameOutbound, appserver.Frame{Type: appserver.FrameText, Payload: []byte(`{"id":"accept"}`)})
	accepted := enrichAuditReceipt(mektup.Receipt{State: mektup.StateAccepted}, capture, false)
	if len(accepted.Evidence) != 1 || accepted.Evidence[0].Details["complete"] != false {
		t.Fatalf("acceptance audit marker = %+v", accepted.Evidence)
	}
	_ = capture.Observe(appserver.FrameInbound, appserver.Frame{Type: appserver.FrameText, Payload: []byte(`{"id":"terminal"}`)})
	terminal := enrichAuditReceipt(accepted, capture, true)
	last := terminal.Evidence[len(terminal.Evidence)-1].Details
	if last["complete"] != true || last["path"] == "" || last["sha256"] == "" {
		t.Fatalf("terminal audit marker = %+v", last)
	}
}

func (f *fakeTransport) Read(ctx context.Context) (appserver.Frame, error) {
	select {
	case frame := <-f.reads:
		return frame, nil
	case <-f.done:
		return appserver.Frame{}, io.EOF
	case <-ctx.Done():
		return appserver.Frame{}, ctx.Err()
	}
}

func (f *fakeTransport) Write(_ context.Context, payload []byte) error {
	if f.onWrite != nil {
		f.onWrite(payload)
	}
	return nil
}

func (f *fakeTransport) Close() error {
	f.once.Do(func() { close(f.done) })
	return nil
}

type recordingDialer struct {
	mu        sync.Mutex
	routes    []endpoint.Route
	options   []appserver.Options
	transport *fakeTransport
}

type failingCloseConnection struct{ closeErr error }

func (c failingCloseConnection) Codex() executor.Codex { return nil }
func (c failingCloseConnection) RPC() rawrpc.Caller    { return nil }
func (c failingCloseConnection) Warnings() []string    { return nil }
func (c failingCloseConnection) Close() error          { return c.closeErr }

func TestArtifactSpillWriterReusesRepeatedContentAddressedReceipt(t *testing.T) {
	store, err := artifact.NewStore(filepath.Join(t.TempDir(), "artifacts"))
	if err != nil {
		t.Fatal(err)
	}
	writer := artifactSpillWriter{store: store}
	body := []byte("repeatable application artifact")
	const digest = "sha256:repeatable"
	first, err := writer.WriteSpill(context.Background(), body, digest)
	if err != nil {
		t.Fatal(err)
	}
	second, err := writer.WriteSpill(context.Background(), body, digest)
	if err != nil || second != first {
		t.Fatalf("repeated application spill first=%q second=%q err=%v", first, second, err)
	}
	info, err := os.Stat(first)
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("artifact info=%+v err=%v", info, err)
	}
}

func (d *recordingDialer) DialClient(_ context.Context, route endpoint.Route, options appserver.Options) (*appserver.Client, error) {
	d.mu.Lock()
	d.routes = append(d.routes, route)
	d.options = append(d.options, options)
	d.mu.Unlock()
	f := d.transport
	f.onWrite = func(payload []byte) {
		var request struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := json.Unmarshal(payload, &request); err != nil {
			return
		}
		if request.Method == "initialize" {
			f.reads <- appserver.Frame{Type: appserver.FrameText, Payload: []byte(fmt.Sprintf(`{"id":%s,"result":{"userAgent":"codex/0.154.0","codexHome":"/tmp/codex"}}`, request.ID))}
		}
	}
	return appserver.New(f, options), nil
}

func endpointID() string { return "ep_0198f0e0-0000-7000-8000-000000000001" }

func TestEnvironmentUsesInvocationResolvedFlagAndEnvPaths(t *testing.T) {
	root := t.TempDir()
	codexHome := filepath.Join(root, "codex")
	stateFromFlags := filepath.Join(root, "flag-state")
	configFromFlags := filepath.Join(root, "flag-config.json")
	stateFromEnv := filepath.Join(root, "env-state")
	configFromEnv := filepath.Join(root, "env-config.json")
	env := New(Options{CodexHome: codexHome, IdentityHome: filepath.Join(root, "identity"), StateDir: filepath.Join(root, "ignored-state"), ConfigPath: filepath.Join(root, "ignored-config.json")})
	for _, paths := range []struct{ state, config string }{{stateFromFlags, configFromFlags}, {stateFromEnv, configFromEnv}} {
		_, err := env.Execute(context.Background(), cli.Invocation{Command: "endpoint", Position: []string{"list"}, Resolved: cli.ResolvedGlobals{Endpoint: "local", StateDir: paths.state, Config: paths.config}})
		if err != nil {
			t.Fatalf("endpoint list for %s: %v", paths.state, err)
		}
		opened, err := journal.Open(context.Background(), journal.Options{StateDir: paths.state})
		if err != nil {
			t.Fatalf("state path %s was not opened: %v", paths.state, err)
		}
		_ = opened.Close()
		if _, err := os.Stat(filepath.Join(paths.state, "journal.sqlite3")); err != nil {
			t.Fatalf("journal path %s: %v", paths.state, err)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "identity", "endpoint-identities.json")); err != nil {
		t.Fatalf("shared endpoint identity path: %v", err)
	}
	_ = env.Close()
}

func TestBuiltinIdentityIsSharedAcrossOperationStateDirs(t *testing.T) {
	root := t.TempDir()
	t.Setenv("XDG_DATA_HOME", filepath.Join(root, "shared-data"))
	env := New(Options{CodexHome: filepath.Join(root, "codex")})
	var ids []string
	for _, name := range []string{"one", "two"} {
		resources, err := env.openResources(context.Background(), cli.Invocation{Command: "endpoint", Position: []string{"list"}, Resolved: cli.ResolvedGlobals{Config: filepath.Join(root, "config.json"), StateDir: filepath.Join(root, name)}})
		if err != nil {
			t.Fatal(err)
		}
		store := resources.ports.Connections.(*connectionFactory).store
		ep, err := store.EnsureBuiltinLocal(env.options.CodexHome)
		_ = resources.Close()
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, ep.ID)
	}
	if len(ids) != 2 || ids[0] != ids[1] {
		t.Fatalf("operation state dirs forked builtin identity: %v", ids)
	}
}

func TestDoctorDoesNotCreateStateAndCanReadCorruptJournal(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, "missing-state")
	config := filepath.Join(root, "config.json")
	var out, errOut bytes.Buffer
	env := New(Options{CodexHome: filepath.Join(root, "codex"), IdentityHome: filepath.Join(root, "identity")})
	app := &cli.App{Out: &out, Err: &errOut, Executor: env, Env: []string{"MEKTUP_OUTPUT=json", "MEKTUP_STATE_DIR=" + state, "MEKTUP_CONFIG=" + config}}
	if code := app.Run([]string{"doctor"}); code != int(cli.ExitSuccess) {
		t.Fatalf("doctor exit=%d stderr=%s", code, errOut.String())
	}
	if _, err := os.Stat(state); !os.IsNotExist(err) {
		t.Fatalf("doctor created state: %v", err)
	}
	if err := os.Mkdir(state, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(state, "journal.sqlite3"), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if code := app.Run([]string{"doctor"}); code != int(cli.ExitSuccess) || !strings.Contains(out.String(), "doctor.completed") {
		t.Fatalf("corrupt doctor exit=%d output=%s", code, out.String())
	}
	_ = env.Close()
}

func TestStorageCorruptOpenUsesStableCode(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, "state")
	if err := os.Mkdir(state, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(state, "journal.sqlite3"), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	env := New(Options{CodexHome: filepath.Join(root, "codex"), IdentityHome: filepath.Join(root, "identity")})
	app := &cli.App{Out: &out, Err: &errOut, Executor: env, Env: []string{"MEKTUP_OUTPUT=json", "MEKTUP_STATE_DIR=" + state, "MEKTUP_CONFIG=" + filepath.Join(root, "config.json")}}
	if code := app.Run([]string{"storage", "status"}); code != int(cli.ExitRejected) || !strings.Contains(out.String(), `"code":"storage_corrupt"`) {
		t.Fatalf("storage exit=%d output=%s stderr=%s", code, out.String(), errOut.String())
	}
	_ = env.Close()
}

func TestStorageCheckUsesReadOnlyPathWithoutJournalOpen(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, "state")
	if err := os.Mkdir(state, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(state, "journal.sqlite3"), []byte("corrupt"), 0600); err != nil {
		t.Fatal(err)
	}
	var out, errOut bytes.Buffer
	env := New(Options{CodexHome: filepath.Join(root, "codex")})
	app := &cli.App{Out: &out, Err: &errOut, Executor: env, Env: []string{"MEKTUP_OUTPUT=json", "MEKTUP_STATE_DIR=" + state, "MEKTUP_CONFIG=" + filepath.Join(root, "config.json")}}
	if code := app.Run([]string{"storage", "check"}); code != int(cli.ExitRejected) || !strings.Contains(out.String(), `"code":"storage_corrupt"`) {
		t.Fatalf("storage check exit=%d output=%s stderr=%s", code, out.String(), errOut.String())
	}
	if _, err := os.Stat(filepath.Join(state, "journal.sqlite3-wal")); !os.IsNotExist(err) {
		t.Fatalf("read-only storage check created WAL: %v", err)
	}
	_ = env.Close()
}

func TestRealSQLiteBusyAtJournalOpenMapsToStorageBusyExit4(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, "state")
	j, err := journal.Open(context.Background(), journal.Options{StateDir: state})
	if err != nil {
		t.Fatal(err)
	}
	_ = j.Close()
	db, err := sql.Open("sqlite", filepath.Join(state, "journal.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec("BEGIN IMMEDIATE"); err != nil {
		t.Fatal(err)
	}
	defer db.Exec("ROLLBACK")
	var out, errOut bytes.Buffer
	env := New(Options{CodexHome: filepath.Join(root, "codex")})
	app := &cli.App{Out: &out, Err: &errOut, Executor: env, Env: []string{"MEKTUP_OUTPUT=json", "MEKTUP_STATE_DIR=" + state, "MEKTUP_CONFIG=" + filepath.Join(root, "config.json")}}
	if code := app.Run([]string{"storage", "status"}); code != int(cli.ExitUnknown) || !strings.Contains(out.String(), `"code":"storage_busy"`) {
		t.Fatalf("busy journal exit=%d output=%s stderr=%s", code, out.String(), errOut.String())
	}
	_ = env.Close()
}

func TestCanceledFIFOIsRejectedBeforeBlockingOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "params.fifo")
	if err := syscall.Mkfifo(path, 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan error, 1)
	go func() { _, err := readFileBounded(ctx, path, 10); done <- err }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("FIFO was accepted")
		}
	case <-time.After(100 * time.Millisecond):
		fd, err := syscall.Open(path, syscall.O_RDWR|syscall.O_NONBLOCK, 0)
		if err != nil {
			t.Fatal(err)
		}
		<-done
		syscall.Close(fd)
		t.Fatal("canceled FIFO read blocked before validation")
	}
}

func TestInjectedAppEnvironmentWinsOverConflictingHostEnvironment(t *testing.T) {
	root := t.TempDir()
	desiredState := filepath.Join(root, "desired-state")
	desiredConfig := filepath.Join(root, "desired-config.json")
	conflictingState := filepath.Join(root, "conflicting-state")
	conflictingConfig := filepath.Join(root, "conflicting-config.json")
	t.Setenv("MEKTUP_CONFIG", conflictingConfig)
	t.Setenv("MEKTUP_STATE_DIR", conflictingState)
	var out, errOut bytes.Buffer
	env := New(Options{CodexHome: filepath.Join(root, "codex"), IdentityHome: filepath.Join(root, "identity")})
	app := &cli.App{In: strings.NewReader(""), Out: &out, Err: &errOut, Env: []string{
		"MEKTUP_OUTPUT=json", "MEKTUP_CONFIG=" + desiredConfig, "MEKTUP_STATE_DIR=" + desiredState,
	}, Executor: env}
	if code := app.Run([]string{"endpoint", "list"}); code != int(cli.ExitSuccess) {
		t.Fatalf("exit=%d stderr=%s", code, errOut.String())
	}
	_ = env.Close()
	if _, err := os.Stat(filepath.Join(root, "identity", "endpoint-identities.json")); err != nil {
		t.Fatalf("shared identity state was not used: %v", err)
	}
	if _, err := os.Stat(conflictingState); !os.IsNotExist(err) {
		t.Fatalf("conflicting host state was touched: %v", err)
	}
}

func TestEnvironmentCloseAndExecuteAdmissionIsRaceSafe(t *testing.T) {
	root := t.TempDir()
	env := New(Options{CodexHome: filepath.Join(root, "codex"), StateDir: filepath.Join(root, "state"), ConfigPath: filepath.Join(root, "config.json")})
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = env.Execute(context.Background(), cli.Invocation{Command: "endpoint", Position: []string{"list"}, Resolved: cli.ResolvedGlobals{StateDir: filepath.Join(root, "state"), Config: filepath.Join(root, "config.json")}})
		}()
		go func() {
			defer wg.Done()
			_ = env.Close()
		}()
	}
	wg.Wait()
	_, err := env.Execute(context.Background(), cli.Invocation{Command: "endpoint", Position: []string{"list"}, Resolved: cli.ResolvedGlobals{StateDir: filepath.Join(root, "state"), Config: filepath.Join(root, "config.json")}})
	if err == nil {
		t.Fatal("closed environment admitted a new execution")
	}
}

func TestConnectionFactoryPassesExperimentalOptionAndSelectsSSHRoute(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, "state")
	store := endpoint.NewStoreWithIdentityHome(filepath.Join(root, "endpoints.json"), state, filepath.Join(root, "identity"))
	unix, err := endpoint.UnixRoute(filepath.Join(root, "codex.sock"))
	if err != nil {
		t.Fatal(err)
	}
	ssh, err := endpoint.SSHRoute("example.invalid")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Add(endpoint.Endpoint{ID: endpointID(), Alias: "remote", Route: ssh, Herdr: endpoint.HerdrDisabled}); err != nil {
		t.Fatal(err)
	}
	dialer := &recordingDialer{transport: newFakeTransport()}
	var selected []endpoint.Route
	factory := &connectionFactory{store: store, dialerForRoute: func(route endpoint.Route, experimental bool) connection.ClientDialer {
		if !experimental {
			t.Error("request-specific experimental option was not forwarded")
		}
		selected = append(selected, route)
		return dialer
	}}
	opened, err := factory.OpenWithOptions(context.Background(), "remote", executor.OpenOptions{ExperimentalAPI: true})
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	if len(selected) != 1 || selected[0].Kind != endpoint.RouteSSH || selected[0].SSHHost != "example.invalid" {
		t.Fatalf("selected routes = %+v", selected)
	}
	if len(dialer.options) != 1 || !dialer.options[0].ExperimentalAPI {
		t.Fatalf("dialer options = %+v", dialer.options)
	}
	_ = unix
}

func TestConnectionCheckSurfacesDetachFailure(t *testing.T) {
	want := errors.New("detach failed")
	factory := &connectionFactory{openOverride: func(context.Context, string, executor.OpenOptions) (executor.Connection, error) {
		return failingCloseConnection{closeErr: want}, nil
	}}
	result, err := factory.Check(context.Background(), "remote")
	var cleanupErr *CleanupError
	if !errors.As(err, &cleanupErr) || !errors.Is(err, want) {
		t.Fatalf("check error=%T %v", err, err)
	}
	if result == nil {
		t.Fatal("check discarded its result while surfacing cleanup failure")
	}
}

func TestConnectionFactoryAcceptsPinnedBuiltinAndConfiguredSelectors(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, "state")
	store := endpoint.NewStoreWithIdentityHome(filepath.Join(root, "endpoints.json"), state, filepath.Join(root, "identity"))
	local, err := store.EnsureBuiltinLocal(filepath.Join(root, "codex"))
	if err != nil {
		t.Fatal(err)
	}
	route, err := endpoint.UnixRoute(filepath.Join(root, "remote.sock"))
	if err != nil {
		t.Fatal(err)
	}
	remoteID := endpointID()
	if err := store.Add(endpoint.Endpoint{ID: remoteID, Alias: "remote", Route: route, Herdr: endpoint.HerdrDisabled}); err != nil {
		t.Fatal(err)
	}
	var dialers []*recordingDialer
	factory := &connectionFactory{store: store, codexHome: filepath.Join(root, "codex"), dialerForRoute: func(endpoint.Route, bool) connection.ClientDialer {
		dialer := &recordingDialer{transport: newFakeTransport()}
		dialers = append(dialers, dialer)
		return dialer
	}}
	for _, selector := range []string{"local", "remote", remoteID} {
		opened, err := factory.OpenWithOptions(context.Background(), selector, executor.OpenOptions{})
		if err != nil {
			t.Fatalf("selector %s: %v", selector, err)
		}
		_ = opened.Close()
	}
	if len(dialers) != 3 || dialers[0].routes[0].UnixSocket != local.Route.UnixSocket || dialers[1].routes[0].UnixSocket != route.UnixSocket || dialers[2].routes[0].UnixSocket != route.UnixSocket {
		t.Fatalf("dialers=%+v local=%+v remote=%+v", dialers, local, route)
	}
}

func TestResolvedEndpointIDCannotRetargetAfterAliasReplacement(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, "state")
	store := endpoint.NewStoreWithIdentityHome(filepath.Join(root, "endpoints.json"), state, filepath.Join(root, "identity"))
	routeA, _ := endpoint.UnixRoute(filepath.Join(root, "a.sock"))
	routeB, _ := endpoint.UnixRoute(filepath.Join(root, "b.sock"))
	idA := "ep_0198f0e0-0000-7000-8000-000000000071"
	idB := "ep_0198f0e0-0000-7000-8000-000000000072"
	if err := store.Add(endpoint.Endpoint{ID: idA, Alias: "remote", Route: routeA, Herdr: endpoint.HerdrDisabled}); err != nil {
		t.Fatal(err)
	}
	old, err := store.ResolveEndpoint("remote", filepath.Join(root, "codex"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Remove("remote"); err != nil {
		t.Fatal(err)
	}
	if err := store.Add(endpoint.Endpoint{ID: idB, Alias: "remote", Route: routeB, Herdr: endpoint.HerdrDisabled}); err != nil {
		t.Fatal(err)
	}
	dialer := &recordingDialer{transport: newFakeTransport()}
	factory := &connectionFactory{store: store, codexHome: filepath.Join(root, "codex"), dialerForRoute: func(endpoint.Route, bool) connection.ClientDialer { return dialer }}
	opened, err := factory.OpenPinned(context.Background(), old, executor.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	if got := opened.(*appConnection).endpoint.ID; got != old.ID || dialer.routes[0].UnixSocket != routeA.UnixSocket {
		t.Fatalf("opened endpoint=%s route=%+v want id=%s route=%+v", got, dialer.routes[0], old.ID, routeA)
	}
}

func TestEndpointRemovePinsIdentityBeforeEffect(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, "state")
	config := filepath.Join(root, "config.json")
	store := endpoint.NewStoreWithIdentityHome(config, state, filepath.Join(root, "identity"))
	route, err := endpoint.SSHRoute("example.invalid")
	if err != nil {
		t.Fatal(err)
	}
	ep := endpoint.Endpoint{ID: endpointID(), Alias: "remote", Route: route, Herdr: endpoint.HerdrDisabled}
	if err := store.Add(ep); err != nil {
		t.Fatal(err)
	}
	j, err := journal.Open(context.Background(), journal.Options{StateDir: state})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	receipts := &receiptStore{journal: j, endpoints: store, pins: &receiptPins{values: make(map[string]endpoint.Endpoint)}}
	ports := endpointPort{store: store, receipts: receipts}
	if err := ports.Remove("remote"); err != nil {
		t.Fatal(err)
	}
	value, err := receipts.Mutation(context.Background(), "endpoint.remove", "remote", map[string]any{"removed": true})
	if err != nil {
		t.Fatalf("pinned receipt failed after remove: %v", err)
	}
	receipt := value.(mektup.Receipt)
	if receipt.Target.EndpointID != ep.ID {
		t.Fatalf("removed endpoint identity = %+v", receipt.Target)
	}
}

func TestReceiptPinsConnectedEndpointAcrossAliasReplacement(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, "state")
	config := filepath.Join(root, "config.json")
	store := endpoint.NewStoreWithIdentityHome(config, state, filepath.Join(root, "identity"))
	route, _ := endpoint.SSHRoute("example.invalid")
	first := endpoint.Endpoint{ID: endpointID(), Alias: "remote", Route: route, Herdr: endpoint.HerdrDisabled}
	if err := store.Add(first); err != nil {
		t.Fatal(err)
	}
	dialer := &recordingDialer{transport: newFakeTransport()}
	env := New(Options{DialerForRoute: func(endpoint.Route, bool) connection.ClientDialer { return dialer }})
	resources, err := env.openResources(context.Background(), cli.Invocation{Command: "thread", Position: []string{"start"}, Resolved: cli.ResolvedGlobals{Endpoint: "remote", Config: config, StateDir: state}})
	if err != nil {
		t.Fatal(err)
	}
	defer resources.Close()
	conn, err := resources.ports.Connections.Open(context.Background(), "remote")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if err := store.Remove("remote"); err != nil {
		t.Fatal(err)
	}
	second := first
	second.ID = "ep_0198f0e0-0000-7000-8000-000000000002"
	if err := store.Add(second); err != nil {
		t.Fatal(err)
	}
	value, err := resources.ports.Receipts.Mutation(context.Background(), "thread.start", "remote", map[string]any{"threadId": "real-thread"})
	if err != nil {
		t.Fatal(err)
	}
	receipt := value.(mektup.Receipt)
	if receipt.Target.EndpointID != first.ID || receipt.Target.ServerVersion == "" {
		t.Fatalf("receipt lost connected identity/facts: %+v", receipt.Target)
	}
}

func TestReceiptStorePersistsStableEndpointIdentity(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, "state")
	codexHome := filepath.Join(root, "codex")
	store := endpoint.NewStoreWithIdentityHome(filepath.Join(root, "endpoints.json"), state, filepath.Join(root, "identity"))
	ep, err := store.EnsureBuiltinLocal(codexHome)
	if err != nil {
		t.Fatal(err)
	}
	j, err := journal.Open(context.Background(), journal.Options{StateDir: state})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	receipts := receiptStore{journal: j, endpoints: store, codexHome: codexHome}
	value, err := receipts.Mutation(context.Background(), "thread.start", "local", map[string]any{"threadId": "t1"})
	if err != nil {
		t.Fatal(err)
	}
	receipt, ok := value.(mektup.Receipt)
	if !ok {
		t.Fatalf("receipt type = %T", value)
	}
	if err := receipt.Validate(); err != nil {
		t.Fatal(err)
	}
	if receipt.Source.EndpointID != ep.ID || receipt.Target.EndpointID != ep.ID || receipt.Target.ThreadID != "t1" || receipt.Source.Alias != "local" || receipt.Source.Transport != "unix" {
		t.Fatalf("receipt identity = %+v", receipt)
	}
	for _, operation := range []string{"search", "rpc", "storage.maintain"} {
		var generic any
		if operation == "search" {
			generic, err = receipts.Read(context.Background(), operation, "local", map[string]any{"result": true})
		} else {
			generic, err = receipts.Mutation(context.Background(), operation, "local", map[string]any{"result": true})
		}
		if err != nil {
			t.Fatalf("%s receipt: %v", operation, err)
		}
		stored, ok := generic.(mektup.Receipt)
		if !ok || stored.Operation != operation {
			t.Fatalf("%s generic receipt = %#v", operation, generic)
		}
		if err := stored.Validate(); err != nil {
			t.Fatalf("%s generic receipt validation: %v", operation, err)
		}
	}
	loaded, err := j.Receipt(context.Background(), receipt.ReceiptID)
	if err != nil || loaded.ReceiptID != receipt.ReceiptID {
		t.Fatalf("loaded receipt = %+v err=%v", loaded, err)
	}
}

func TestCleanupFailurePreservesAcceptedResultAndAddsWireWarning(t *testing.T) {
	result := cli.ExecutionResult{
		Events:  []cli.OutputEvent{{Machine: map[string]any{"event": "thread.completed", "warnings": []map[string]any{}}}},
		Receipt: map[string]any{"receiptId": "rcpt"}, Exit: cli.ExitSuccess,
	}
	got := preserveCleanupResult(result, errors.New("close failed"))
	if got.Exit != cli.ExitInternal || got.Receipt == nil {
		t.Fatalf("cleanup result lost acceptance: %+v", got)
	}
	machine := got.Events[0].Machine.(map[string]any)
	warnings, ok := machine["warnings"].([]any)
	if !ok || len(warnings) != 1 || warnings[0].(map[string]any)["code"] != "cleanup_incomplete" {
		t.Fatalf("cleanup warning = %#v", machine["warnings"])
	}
}

func TestCleanupFailureAddsDurableReceiptWarningProjection(t *testing.T) {
	result := cli.ExecutionResult{Receipt: mektup.Receipt{Warnings: []mektup.Warning{}}, Exit: cli.ExitSuccess}
	got := preserveCleanupResult(result, errors.New("close failed"))
	receipt, ok := got.Receipt.(mektup.Receipt)
	if !ok || len(receipt.Warnings) != 1 || receipt.Warnings[0].Code != mektup.WarningCleanupIncomplete {
		t.Fatalf("receipt cleanup warnings = %#v", got.Receipt)
	}
}

func TestProductionCompositionDoesNotRequireDaemonOrSSHProcessForInjectedRoute(t *testing.T) {
	root := t.TempDir()
	store := endpoint.NewStoreWithIdentityHome(filepath.Join(root, "endpoints.json"), filepath.Join(root, "state"), filepath.Join(root, "identity"))
	route, err := endpoint.UnixRoute(filepath.Join(root, "managed.sock"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Add(endpoint.Endpoint{ID: endpointID(), Alias: "unix", Route: route, Herdr: endpoint.HerdrDisabled}); err != nil {
		t.Fatal(err)
	}
	dialer := &recordingDialer{transport: newFakeTransport()}
	factory := &connectionFactory{store: store, dialerForRoute: func(got endpoint.Route, _ bool) connection.ClientDialer {
		if got.Kind != endpoint.RouteUnix {
			t.Fatalf("route kind = %q", got.Kind)
		}
		return dialer
	}}
	if _, err := factory.Check(context.Background(), "unix"); err == nil {
		// The injected Unix transport proves composition does not need a daemon
		// or a process factory. The check itself should succeed via the fake.
	} else {
		t.Fatal(err)
	}
	if len(dialer.routes) != 1 || dialer.routes[0] != route {
		t.Fatalf("dialed routes = %+v want %+v", dialer.routes, route)
	}
}

func TestApplicationSessionFactorySelectsSSHRouteDialer(t *testing.T) {
	route, err := endpoint.SSHRoute("example.invalid")
	if err != nil {
		t.Fatal(err)
	}
	dialer := &recordingDialer{transport: newFakeTransport()}
	var selected endpoint.Route
	factory := applicationSessionFactory{dialerForRoute: func(got endpoint.Route, _ bool) connection.ClientDialer {
		selected = got
		return dialer
	}}
	session, err := factory.Open(context.Background(), endpoint.Endpoint{ID: endpointID(), Alias: "remote", Route: route, Herdr: endpoint.HerdrDisabled})
	if err != nil {
		t.Fatal(err)
	}
	if session == nil || selected != route || len(dialer.routes) != 1 || dialer.routes[0] != route {
		t.Fatalf("SSH route selection selected=%+v routes=%+v", selected, dialer.routes)
	}
	_ = session.Detach(context.Background())
}

func TestReadFileBoundedSupportsExplicitAbsoluteParamsPath(t *testing.T) {
	path := filepath.Join(t.TempDir(), "params.json")
	if err := os.WriteFile(path, []byte(`{"ok":true}`), 0600); err != nil {
		t.Fatal(err)
	}
	data, err := readFileBounded(context.Background(), path, 1024)
	if err != nil || string(data) != `{"ok":true}` {
		t.Fatalf("data=%s err=%v", data, err)
	}
}

type messagingFakeSession struct {
	endpointID string
	detachErr  error
}

type originalMessagingSession struct {
	messagingFakeSession
	history []codexapi.Turn
}

func (s originalMessagingSession) History(context.Context, string) ([]codexapi.Turn, error) {
	return s.history, nil
}

func (s messagingFakeSession) EndpointID() string { return s.endpointID }
func (messagingFakeSession) StartOrSteer(context.Context, string, string, string) (runtime.TurnResult, error) {
	return runtime.TurnResult{TurnID: "turn_01999999-9999-7999-8999-999999999999", Evidence: "fake accepted"}, nil
}
func (messagingFakeSession) Resume(context.Context, string) error      { return nil }
func (messagingFakeSession) Unsubscribe(context.Context, string) error { return nil }
func (messagingFakeSession) History(context.Context, string) ([]codexapi.Turn, error) {
	return nil, nil
}
func (messagingFakeSession) NextEvent(context.Context) (appserver.Event, error) {
	return appserver.Event{}, io.EOF
}
func (s messagingFakeSession) Detach(context.Context) error                     { return s.detachErr }
func (messagingFakeSession) ThreadExists(context.Context, string) (bool, error) { return true, nil }

func TestMessagingCompositionRegistersOnlyReplyCustody(t *testing.T) {
	root := t.TempDir()
	codexHome := filepath.Join(root, "codex")
	if err := os.MkdirAll(filepath.Join(codexHome, "app-server-control"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(codexHome, "app-server-control", "app-server-control.sock"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	registryPath := filepath.Join(root, "control-registry.json")
	configured := endpoint.NewStore(filepath.Join(root, "endpoints.json"), filepath.Join(root, "state"))
	configured.IdentityHome = filepath.Join(root, "identity")
	local, err := configured.EnsureBuiltinLocal(codexHome)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateEndpointURISelector(configured, codexHome, local.ID, "codex://local/thread/source"); err != nil {
		t.Fatalf("legacy alias selector validation: %v", err)
	}
	env := New(Options{CodexHome: codexHome, ConfigPath: filepath.Join(root, "endpoints.json"), StateDir: filepath.Join(root, "state"), IdentityHome: filepath.Join(root, "identity"), Registry: controlreceiver.FileRegistry{Path: registryPath}, CurrentThreadID: "source-thread", ThreadStateProbe: func(context.Context, string, string) (bool, bool, error) { return false, true, nil }, SessionFactory: runtime.SessionFactoryFunc(func(_ context.Context, ep endpoint.Endpoint) (runtime.Session, error) {
		return messagingFakeSession{endpointID: ep.ID}, nil
	})})
	app := &cli.App{Out: &bytes.Buffer{}, Err: &bytes.Buffer{}, Executor: env, Env: []string{"MEKTUP_AGENT=1", "CODEX_HOME=" + codexHome, "CODEX_THREAD_ID=source-thread", "MEKTUP_CONFIG=" + filepath.Join(root, "endpoints.json"), "MEKTUP_STATE_DIR=" + filepath.Join(root, "state")}}
	target := "codex://" + local.ID + "/thread/target-thread"
	if code := app.Run([]string{"send", target, "one-way"}); code != int(cli.ExitSuccess) {
		t.Fatalf("one-way send exit=%d", code)
	}
	if _, err := os.Stat(registryPath); !os.IsNotExist(err) {
		t.Fatalf("one-way send registered custody: %v", err)
	}
	if code := app.Run([]string{"send", target, "replyable", "--request-reply"}); code != int(cli.ExitSuccess) {
		t.Fatalf("reply-requesting send exit=%d", code)
	}
	if _, err := os.Stat(registryPath); err != nil {
		t.Fatalf("reply-requesting send did not register custody: %v", err)
	}
}

func TestApplicationReplyResolvesStableOriginalEnvelopeAfterCanonicalWireSend(t *testing.T) {
	root := t.TempDir()
	codexHome := filepath.Join(root, "codex")
	if err := os.MkdirAll(filepath.Join(codexHome, "app-server-control"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(codexHome, "app-server-control", "app-server-control.sock"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	configured := endpoint.NewStore(filepath.Join(root, "endpoints.json"), filepath.Join(root, "state"))
	configured.IdentityHome = filepath.Join(root, "identity")
	local, err := configured.EnsureBuiltinLocal(codexHome)
	if err != nil {
		t.Fatal(err)
	}
	identityJournal, err := journal.Open(context.Background(), journal.Options{StateDir: filepath.Join(root, "state")})
	if err != nil {
		t.Fatal(err)
	}
	storeID := identityJournal.StoreID()
	if _, err := identityJournal.Prepare(context.Background(), journal.Operation{OperationID: "op_0198f0e0-0000-7000-8000-000000000120", MessageID: "msg_0198f0e0-0000-7000-8000-000000000121", SourceRoute: "codex://local/thread/source", TargetRoute: "codex://local/thread/source", Semantics: "message", SourceEndpointID: local.ID, TargetEndpointID: local.ID, ReplyRoute: "codex://local/thread/source", ReplyEndpointID: local.ID, CustodyRoute: local.ID, CustodyStoreID: storeID, Digest: "sha256:" + strings.Repeat("a", 64), BodySize: 8}); err != nil {
		t.Fatal(err)
	}
	_ = identityJournal.Close()
	original := mektup.Envelope{MessageID: "msg_0198f0e0-0000-7000-8000-000000000121", Kind: mektup.KindMessage, FromEndpointID: local.ID, From: "codex://local/thread/source", FromKind: "agent", ToEndpointID: local.ID, To: "codex://local/thread/source", RequestedTarget: "source", ReplyRequested: true, ReplyEndpointID: local.ID, ReplyTo: "codex://local/thread/source", ReplyCustodyEndpointID: local.ID, Body: "question", Provenance: "observed", SentAt: time.Now().UTC().Format(time.RFC3339Nano)}
	original.ReplyCustodyStoreID = storeID
	original.PayloadBytes = uint64(len(original.Body))
	digest := sha256.Sum256([]byte(original.Body))
	original.PayloadSHA256 = "sha256:" + hex.EncodeToString(digest[:])
	envelopeText, err := mektup.RenderEnvelope(original)
	if err != nil {
		t.Fatal(err)
	}
	content, _ := json.Marshal([]map[string]string{{"type": "text", "text": string(envelopeText)}})
	item, _ := json.Marshal(map[string]any{"type": "userMessage", "id": "item_0198f0e0-0000-7000-8000-000000000122", "clientId": original.MessageID, "content": json.RawMessage(content)})
	items := json.RawMessage("[" + string(item) + "]")
	session := originalMessagingSession{messagingFakeSession: messagingFakeSession{endpointID: local.ID}, history: []codexapi.Turn{{ID: "turn_0198f0e0-0000-7000-8000-000000000123", RawObject: codexapi.RawObject{Fields: map[string]json.RawMessage{"items": items}}}}}
	env := New(Options{CodexHome: codexHome, ConfigPath: filepath.Join(root, "endpoints.json"), StateDir: filepath.Join(root, "state"), IdentityHome: filepath.Join(root, "identity"), CurrentThreadID: "source", ThreadStateProbe: func(context.Context, string, string) (bool, bool, error) { return true, true, nil }, SessionFactory: runtime.SessionFactoryFunc(func(_ context.Context, ep endpoint.Endpoint) (runtime.Session, error) { return session, nil })})
	out := &bytes.Buffer{}
	app := &cli.App{Out: out, Err: &bytes.Buffer{}, Executor: env, Env: []string{"MEKTUP_AGENT=1", "CODEX_HOME=" + codexHome, "CODEX_THREAD_ID=source", "MEKTUP_CONFIG=" + filepath.Join(root, "endpoints.json"), "MEKTUP_STATE_DIR=" + filepath.Join(root, "state")}}
	if code := app.Run([]string{"reply", original.MessageID, "answer"}); code != int(cli.ExitSuccess) {
		t.Fatalf("reply with stable original route exit=%d output=%s", code, out.String())
	}
}

func TestMessagingReceiptPersistsBeforeOutputAndWaitResolvesReceiptID(t *testing.T) {
	root := t.TempDir()
	home := filepath.Join(root, "codex")
	if err := os.MkdirAll(filepath.Join(home, "app-server-control"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, "app-server-control", "app-server-control.sock"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(root, "state")
	out := &bytes.Buffer{}
	env := New(Options{CodexHome: home, ConfigPath: filepath.Join(root, "endpoints.json"), StateDir: state, IdentityHome: filepath.Join(root, "identity"), CurrentThreadID: "source", ThreadStateProbe: func(context.Context, string, string) (bool, bool, error) { return true, true, nil }, SessionFactory: runtime.SessionFactoryFunc(func(_ context.Context, ep endpoint.Endpoint) (runtime.Session, error) {
		return messagingFakeSession{endpointID: ep.ID}, nil
	})})
	app := &cli.App{Out: out, Err: &bytes.Buffer{}, Executor: env, Env: []string{"MEKTUP_AGENT=1", "CODEX_HOME=" + home, "CODEX_THREAD_ID=source", "MEKTUP_CONFIG=" + filepath.Join(root, "endpoints.json"), "MEKTUP_STATE_DIR=" + state}}
	if code := app.Run([]string{"send", "codex://local/thread/target", "question", "--request-reply"}); code != int(cli.ExitSuccess) {
		t.Fatalf("send exit=%d output=%s", code, out.String())
	}
	var event struct {
		Data struct {
			Receipt mektup.Receipt `json:"receipt"`
		} `json:"data"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &event); err != nil {
		t.Fatal(err)
	}
	if event.Data.Receipt.ReceiptID == "" {
		t.Fatal("send emitted no receipt")
	}
	j, err := journal.Open(context.Background(), journal.Options{StateDir: state})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.Receipt(context.Background(), event.Data.Receipt.ReceiptID); err != nil {
		t.Fatalf("emitted receipt was not durable: %v", err)
	}
	_ = j.Close()
	portablePath := filepath.Join(root, "receipt.json")
	out.Reset()
	if code := app.Run([]string{"receipt", "show", event.Data.Receipt.ReceiptID, "--portable"}); code != int(cli.ExitSuccess) {
		t.Fatalf("portable receipt show exit=%d output=%s", code, out.String())
	}
	if err := os.WriteFile(portablePath, bytes.TrimSpace(out.Bytes()), 0600); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if code := app.Run([]string{"wait", "portable-reference", "--receipt-file", portablePath, "--timeout", "1ms"}); code != int(cli.ExitIncomplete) {
		t.Fatalf("wait receipt reference exit=%d output=%s", code, out.String())
	}
	j, err = journal.Open(context.Background(), journal.Options{StateDir: state})
	if err != nil {
		t.Fatal(err)
	}
	stored, err := j.Receipt(context.Background(), event.Data.Receipt.ReceiptID)
	if err != nil {
		_ = j.Close()
		t.Fatal(err)
	}
	stored.Warnings = append(stored.Warnings, mektup.Warning{Code: mektup.WarningCleanupIncomplete, Message: "later cleanup warning"})
	if err := j.PutReceipt(context.Background(), stored); err != nil {
		_ = j.Close()
		t.Fatal(err)
	}
	_ = j.Close()
	out.Reset()
	if code := app.Run([]string{"wait", "portable-reference", "--receipt-file", portablePath, "--timeout", "1ms"}); code != int(cli.ExitIncomplete) {
		t.Fatalf("advisory update invalidated portable identity: exit=%d output=%s", code, out.String())
	}
	var imported mektup.Receipt
	portableData, err := os.ReadFile(portablePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(bytes.TrimSpace(portableData), &imported); err != nil {
		t.Fatal(err)
	}
	imported.Message.PayloadSHA256 = "sha256:" + strings.Repeat("b", 64)
	mutated, err := json.Marshal(imported)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(portablePath, mutated, 0600); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if code := app.Run([]string{"wait", "portable-reference", "--receipt-file", portablePath, "--timeout", "1ms"}); code != int(cli.ExitRejected) {
		t.Fatalf("mutated portable receipt exit=%d output=%s", code, out.String())
	}
}

func TestPortableAuthorityComparatorUsesImmutableClaims(t *testing.T) {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	base := mektup.Receipt{
		Schema: mektup.ReceiptSchema, ReceiptID: mektup.NewReceiptID(), OperationID: mektup.NewOperationID(), Operation: "send",
		State:      mektup.StateAccepted,
		Source:     mektup.ReceiptIdentity{EndpointID: endpointID(), ThreadID: "source", Resolved: "codex://local/thread/source"},
		Target:     mektup.ReceiptIdentity{EndpointID: "ep_0198f0e0-0000-7000-8000-000000000002", ThreadID: "target", Resolved: "codex://local/thread/target"},
		Message:    mektup.ReceiptMessage{MessageID: mektup.NewMessageID(), ClientMessageID: mektup.NewMessageID(), Kind: string(mektup.KindMessage), ReplyRequested: true, PayloadBytes: 4, PayloadSHA256: "sha256:" + strings.Repeat("a", 64), TurnID: "turn_0198f0e0-0000-7000-8000-000000000003"},
		ContentRef: &mektup.ContentRef{EndpointID: endpointID(), ThreadID: "source", TurnID: "turn_0198f0e0-0000-7000-8000-000000000003", ItemID: "item-1", ClientMessageID: "client-1", PayloadBytes: 4, PayloadSHA256: "sha256:" + strings.Repeat("a", 64)},
		CreatedAt:  now, UpdatedAt: now,
	}
	if !portableAuthorityEqual(base, func() mektup.Receipt {
		updated := base
		updated.State = mektup.StateReplyAccepted
		updated.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
		updated.Warnings = []mektup.Warning{{Code: mektup.WarningCleanupIncomplete, Message: "advisory"}}
		return updated
	}()) {
		t.Fatal("advisory receipt changes invalidated immutable authority")
	}
	turnChanged := base
	turnChanged.Message.TurnID = "turn_0198f0e0-0000-7000-8000-000000000004"
	if portableAuthorityEqual(base, turnChanged) {
		t.Fatal("changed native turn identity was accepted")
	}
	contentChanged := base
	content := *base.ContentRef
	content.ItemID = "item-2"
	contentChanged.ContentRef = &content
	if portableAuthorityEqual(base, contentChanged) {
		t.Fatal("changed content identity was accepted")
	}
	withCustody := base
	withCustody.Evidence = []mektup.EvidenceRecord{{Details: map[string]any{"custodyRoute": "custody-route", "custodyStoreId": "store_0198f0e0-0000-7000-8000-000000000005"}}}
	if portableAuthorityEqual(base, withCustody) == false {
		// A stronger current durable custody claim is allowed when the old
		// portable export did not represent it.
		t.Fatal("durable custody strengthening rejected an older export")
	}
	importedCustody := withCustody
	if portableAuthorityEqual(importedCustody, base) {
		t.Fatal("portable custody claim was accepted without durable authority")
	}
	conflicting := withCustody
	conflicting.Evidence = append(conflicting.Evidence, mektup.EvidenceRecord{Details: map[string]any{"custodyRoute": "other-route"}})
	if portableAuthorityEqual(conflicting, withCustody) {
		t.Fatal("conflicting custody claims were accepted")
	}
	malformed := withCustody
	malformed.Evidence = append([]mektup.EvidenceRecord(nil), withCustody.Evidence...)
	malformed.Evidence[0].Details = map[string]any{"custodyRoute": 42, "custodyStoreId": "store_0198f0e0-0000-7000-8000-000000000005"}
	if portableAuthorityEqual(malformed, withCustody) {
		t.Fatal("malformed custody claim was accepted")
	}
}

func TestPortableApplicationRoutesUseStableIDsAcrossAliases(t *testing.T) {
	root := t.TempDir()
	store := endpoint.NewStore(filepath.Join(root, "endpoints.json"), filepath.Join(root, "state"))
	ep := endpoint.Endpoint{ID: endpointID(), Alias: "receiver-local", Route: endpoint.Route{Kind: endpoint.RouteUnix, UnixSocket: filepath.Join(root, "receiver.sock")}, Herdr: endpoint.HerdrDisabled}
	if err := store.Add(ep); err != nil {
		t.Fatal(err)
	}
	receipt := mektup.Receipt{
		Source:   mektup.ReceiptIdentity{EndpointID: ep.ID, ThreadID: "source", Resolved: "codex://sender-local/thread/source"},
		Target:   mektup.ReceiptIdentity{EndpointID: ep.ID, ThreadID: "target", Resolved: "codex://sender-devbox/thread/target"},
		Evidence: []mektup.EvidenceRecord{{Details: map[string]any{"custodyRoute": ep.ID, "custodyStoreId": "store_0198f0e0-0000-7000-8000-000000000099"}}},
	}
	r := applicationImportResolver{store: store}
	routes, err := r.verifyPortableRoutes(receipt)
	if err != nil {
		t.Fatal(err)
	}
	if routes.source.ID != ep.ID || routes.target.ID != ep.ID || routes.custody.ID != ep.ID {
		t.Fatalf("stable route resolution=%+v", routes)
	}
}

func TestMessagingPoolCleanupWarningIsDurable(t *testing.T) {
	root := t.TempDir()
	home := filepath.Join(root, "codex")
	if err := os.MkdirAll(filepath.Join(home, "app-server-control"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, "app-server-control", "app-server-control.sock"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(root, "state")
	out := &bytes.Buffer{}
	env := New(Options{CodexHome: home, ConfigPath: filepath.Join(root, "endpoints.json"), StateDir: state, IdentityHome: filepath.Join(root, "identity"), CurrentThreadID: "source", ThreadStateProbe: func(context.Context, string, string) (bool, bool, error) { return true, true, nil }, SessionFactory: runtime.SessionFactoryFunc(func(_ context.Context, ep endpoint.Endpoint) (runtime.Session, error) {
		return messagingFakeSession{endpointID: ep.ID, detachErr: errors.New("detach failed")}, nil
	})})
	app := &cli.App{Out: out, Err: &bytes.Buffer{}, Executor: env, Env: []string{"MEKTUP_AGENT=1", "CODEX_HOME=" + home, "CODEX_THREAD_ID=source", "MEKTUP_CONFIG=" + filepath.Join(root, "endpoints.json"), "MEKTUP_STATE_DIR=" + state}}
	if code := app.Run([]string{"send", "codex://local/thread/target", "question"}); code != int(cli.ExitInternal) {
		t.Fatalf("cleanup failure exit=%d output=%s", code, out.String())
	}
	var event struct {
		Data struct {
			Receipt mektup.Receipt `json:"receipt"`
		} `json:"data"`
	}
	if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &event); err != nil {
		t.Fatal(err)
	}
	j, err := journal.Open(context.Background(), journal.Options{StateDir: state})
	if err != nil {
		t.Fatal(err)
	}
	stored, err := j.Receipt(context.Background(), event.Data.Receipt.ReceiptID)
	_ = j.Close()
	if err != nil || len(stored.Warnings) == 0 {
		t.Fatalf("cleanup warning was not durable: err=%v receipt=%+v", err, stored)
	}
}

func TestMessagingUsesBoundedRemoteHerdrRunnerByDefault(t *testing.T) {
	factory := sshproxy.ProcessFactoryFunc(func([]string) (sshproxy.Process, error) {
		return nil, errors.New("not invoked")
	})
	environment := New(Options{
		SSHConfig:  sshproxy.Config{SSHBinary: "/usr/bin/ssh"},
		SSHFactory: factory,
	})
	resolver := environment.herdrResolver()
	runner, ok := resolver.EndpointRunner.(endpoint.RemoteHerdrRunner)
	if !ok {
		t.Fatalf("endpoint runner = %T", resolver.EndpointRunner)
	}
	if runner.Config.SSH.SSHBinary != "/usr/bin/ssh" || runner.Factory == nil {
		t.Fatalf("remote runner was not composed from application SSH options: %#v", runner.Config.SSH)
	}
}

func TestMessagingPreservesInjectedHerdrEndpointRunner(t *testing.T) {
	injected := endpoint.EndpointCommandRunnerFunc(func(context.Context, endpoint.Endpoint, []string) ([]byte, error) {
		return []byte("{\"agents\":[]}"), nil
	})
	environment := New(Options{HerdrEndpointRunner: injected})
	if _, ok := environment.herdrResolver().EndpointRunner.(endpoint.EndpointCommandRunnerFunc); !ok {
		t.Fatalf("injected endpoint runner was replaced")
	}
}

func TestAppThreadListMapsUnavailableUnixSocketWithoutFallback(t *testing.T) {
	tests := []struct {
		name      string
		dialErr   error
		wantCause string
	}{
		{name: "missing", wantCause: "no such file or directory"},
		{name: "refused", dialErr: syscall.ECONNREFUSED, wantCause: "connection refused"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			state := filepath.Join(root, "state")
			config := filepath.Join(root, "endpoints.json")
			socketDir, err := os.MkdirTemp("/tmp", "mu-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(socketDir) })
			socketPath := filepath.Join(socketDir, "isolated.sock")
			route, err := endpoint.UnixRoute(socketPath)
			if err != nil {
				t.Fatal(err)
			}
			store := endpoint.NewStoreWithIdentityHome(config, state, filepath.Join(root, "identity"))
			if err := store.Add(endpoint.Endpoint{ID: endpointID(), Alias: "isolated", Route: route, Herdr: endpoint.HerdrDisabled}); err != nil {
				t.Fatal(err)
			}

			var out, errOut bytes.Buffer
			envOptions := Options{CodexHome: filepath.Join(root, "codex"), IdentityHome: filepath.Join(root, "identity")}
			if test.dialErr != nil {
				envOptions.DialerForRoute = func(endpoint.Route, bool) connection.ClientDialer {
					return connection.ClientDialFunc(func(context.Context, endpoint.Route, appserver.Options) (*appserver.Client, error) {
						return nil, test.dialErr
					})
				}
			}
			env := New(envOptions)
			app := &cli.App{
				In:       strings.NewReader(""),
				Out:      &out,
				Err:      &errOut,
				Executor: env,
				Env:      []string{"MEKTUP_OUTPUT=json", "MEKTUP_CONFIG=" + config, "MEKTUP_STATE_DIR=" + state},
			}
			if code := app.Run([]string{"--endpoint", "isolated", "thread", "list", "--limit", "1"}); code != int(cli.ExitRejected) {
				t.Fatalf("exit=%d stdout=%s stderr=%s", code, out.String(), errOut.String())
			}
			var event map[string]any
			if err := json.Unmarshal([]byte(strings.TrimSpace(out.String())), &event); err != nil {
				t.Fatalf("decode JSONL stdout %q: %v", out.String(), err)
			}
			data, ok := event["data"].(map[string]any)
			if !ok {
				t.Fatalf("event data = %#v", event["data"])
			}
			errData, ok := data["error"].(map[string]any)
			if !ok || errData["code"] != "endpoint_unavailable" || errData["effectState"] != "not_sent" {
				t.Fatalf("error data = %#v", data["error"])
			}
			if !strings.Contains(errData["message"].(string), test.wantCause) {
				t.Fatalf("error message = %q, want %q", errData["message"], test.wantCause)
			}
			if _, err := os.Stat(filepath.Join(root, "codex")); !os.IsNotExist(err) {
				t.Fatalf("missing/refused endpoint unexpectedly created codex home: %v", err)
			}
		})
	}
}
