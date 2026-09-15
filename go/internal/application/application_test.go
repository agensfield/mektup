package application

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	mektup "github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/appserver"
	"github.com/agensfield/mektup/go/internal/cli"
	"github.com/agensfield/mektup/go/internal/connection"
	"github.com/agensfield/mektup/go/internal/endpoint"
	"github.com/agensfield/mektup/go/internal/executor"
	"github.com/agensfield/mektup/go/internal/journal"
)

type fakeTransport struct {
	reads   chan appserver.Frame
	done    chan struct{}
	once    sync.Once
	onWrite func([]byte)
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{reads: make(chan appserver.Frame, 16), done: make(chan struct{})}
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
	env := New(Options{CodexHome: codexHome, StateDir: filepath.Join(root, "ignored-state"), ConfigPath: filepath.Join(root, "ignored-config.json")})
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
	if _, err := os.Stat(filepath.Join(stateFromFlags, "endpoint-identities.json")); err != nil {
		t.Fatalf("flag endpoint identity path: %v", err)
	}
	if _, err := os.Stat(filepath.Join(stateFromEnv, "endpoint-identities.json")); err != nil {
		t.Fatalf("env endpoint identity path: %v", err)
	}
	_ = env.Close()
}

func TestConnectionFactoryPassesExperimentalOptionAndSelectsSSHRoute(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, "state")
	store := endpoint.NewStore(filepath.Join(root, "endpoints.json"), state)
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

func TestReceiptStorePersistsStableEndpointIdentity(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, "state")
	codexHome := filepath.Join(root, "codex")
	store := endpoint.NewStore(filepath.Join(root, "endpoints.json"), state)
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
	if receipt.Source.EndpointID != ep.ID || receipt.Target.EndpointID != ep.ID || receipt.Source.Alias != "local" || receipt.Source.Transport != "unix" {
		t.Fatalf("receipt identity = %+v", receipt)
	}
	loaded, err := j.Receipt(context.Background(), receipt.ReceiptID)
	if err != nil || loaded.ReceiptID != receipt.ReceiptID {
		t.Fatalf("loaded receipt = %+v err=%v", loaded, err)
	}
}

func TestProductionCompositionDoesNotRequireDaemonOrSSHProcessForInjectedRoute(t *testing.T) {
	root := t.TempDir()
	store := endpoint.NewStore(filepath.Join(root, "endpoints.json"), filepath.Join(root, "state"))
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
