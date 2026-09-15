package connection

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agensfield/mektup/go/appserver"
	"github.com/agensfield/mektup/go/internal/codexapi"
	"github.com/agensfield/mektup/go/internal/compat"
	"github.com/agensfield/mektup/go/internal/endpoint"
)

type fakeTransport struct {
	reads    chan appserver.Frame
	writes   chan []byte
	done     chan struct{}
	onWrite  func([]byte)
	writeErr func([]byte) error
	once     sync.Once
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{
		reads:  make(chan appserver.Frame, 32),
		writes: make(chan []byte, 32),
		done:   make(chan struct{}),
	}
}

func (f *fakeTransport) Read(ctx context.Context) (appserver.Frame, error) {
	select {
	case frame := <-f.reads:
		return frame, nil
	case <-f.done:
		return appserver.Frame{}, errors.New("fake transport closed")
	case <-ctx.Done():
		return appserver.Frame{}, ctx.Err()
	}
}

func (f *fakeTransport) Write(ctx context.Context, payload []byte) error {
	select {
	case f.writes <- append([]byte(nil), payload...):
	case <-f.done:
		return &appserver.WriteFailure{Err: errors.New("fake transport closed"), Phase: appserver.WriteMayHaveWritten}
	case <-ctx.Done():
		return ctx.Err()
	}
	if f.onWrite != nil {
		f.onWrite(payload)
	}
	if f.writeErr != nil {
		return f.writeErr(payload)
	}
	return nil
}

func (f *fakeTransport) Close() error {
	f.once.Do(func() { close(f.done) })
	return nil
}

func testRoute(t *testing.T) endpoint.Route {
	t.Helper()
	route, err := endpoint.UnixRoute("/tmp/mektup-test.sock")
	if err != nil {
		t.Fatal(err)
	}
	return route
}

func initializeResponse(id json.RawMessage, userAgent string) []byte {
	return []byte(fmt.Sprintf(`{"id":%s,"result":{"userAgent":%q,"codexHome":"/tmp/codex","platformFamily":"unix","platformOs":"macos"}}`, id, userAgent))
}

func connectFixture(t *testing.T, userAgent string, options Options) (*Connection, *fakeTransport) {
	t.Helper()
	f := newFakeTransport()
	f.onWrite = func(payload []byte) {
		var msg struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := json.Unmarshal(payload, &msg); err != nil {
			t.Errorf("decode request: %v", err)
			return
		}
		switch msg.Method {
		case "initialize":
			f.reads <- appserver.Frame{Type: appserver.FrameText, Payload: initializeResponse(msg.ID, userAgent)}
		}
	}
	options.Dialer = DialFunc(func(context.Context, endpoint.Route) (appserver.Transport, error) { return f, nil })
	conn, err := Connect(context.Background(), testRoute(t), options)
	if err != nil {
		t.Fatal(err)
	}
	return conn, f
}

func TestConnectCapturesAuthorityExperimentalGenerationAndProxySeparately(t *testing.T) {
	var nilConnection *Connection
	if nilConnection.ExperimentalAPIEnabled() {
		t.Fatal("nil connection reported experimental API capability")
	}
	conn, f := connectFixture(t, "codex/0.154.0 (daemon)", Options{
		ClientName:       "mektup-test",
		ClientVersion:    "1.2.3",
		ExperimentalAPI:  true,
		HandshakeTimeout: time.Second,
		Proxy:            ProxyEvidence{Executable: "/usr/local/bin/codex", Version: "0.999.0"},
	})
	defer conn.Close(context.Background())

	info := conn.Info()
	if info.Generation == 0 || info.DaemonVersion != "0.154.0" || info.ServerUserAgent != "codex/0.154.0 (daemon)" {
		t.Fatalf("connection evidence = %+v", info)
	}
	if info.Compatibility.Class != compat.Tested || info.Proxy.Version != "0.999.0" {
		t.Fatalf("authority/proxy evidence = %+v", info)
	}
	if !conn.ExperimentalAPIEnabled() || !conn.Capabilities().ExperimentalAPI {
		t.Fatal("experimental API capability was not retained")
	}
	var initialize map[string]any
	select {
	case payload := <-f.writes:
		if err := json.Unmarshal(payload, &initialize); err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("initialize write was not observed")
	}
	capabilities := initialize["params"].(map[string]any)["capabilities"].(map[string]any)
	if capabilities["experimentalApi"] != true {
		t.Fatalf("experimentalApi bit = %#v", capabilities["experimentalApi"])
	}
}

func TestUntestedAndUnknownVersionsProceedWithStableWarnings(t *testing.T) {
	tests := []struct {
		name    string
		uagent  string
		class   compat.Class
		warning string
	}{
		{name: "untested", uagent: "codex/0.155.0", class: compat.Untested, warning: compat.WarningUntested},
		{name: "unknown", uagent: "codex/nightly-build", class: compat.Unknown, warning: compat.WarningUnknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			conn, _ := connectFixture(t, test.uagent, Options{})
			defer conn.Close(context.Background())
			if got := conn.Compatibility().Class; got != test.class {
				t.Fatalf("class = %q, want %q", got, test.class)
			}
			if got := conn.Warnings(); len(got) != 1 || got[0] != test.warning {
				t.Fatalf("warnings = %#v, want %q", got, test.warning)
			}
		})
	}
}

func TestSSHRouteIsOnlyOpenedThroughInjectedDialer(t *testing.T) {
	f := newFakeTransport()
	var observed endpoint.Route
	f.onWrite = func(payload []byte) {
		var msg struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.Unmarshal(payload, &msg)
		if msg.Method == "initialize" {
			f.reads <- appserver.Frame{Type: appserver.FrameText, Payload: initializeResponse(msg.ID, "codex/0.154.0")}
		}
	}
	route, err := endpoint.SSHRoute("codex.example")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := Connect(context.Background(), route, Options{Dialer: DialFunc(func(_ context.Context, got endpoint.Route) (appserver.Transport, error) {
		observed = got
		return f, nil
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	if observed != route {
		t.Fatalf("injected route = %+v, want %+v", observed, route)
	}
}

func TestDefaultSSHDialDoesNotStartAProxy(t *testing.T) {
	route, err := endpoint.SSHRoute("codex.example")
	if err != nil {
		t.Fatal(err)
	}
	_, err = Connect(context.Background(), route, Options{})
	if !errors.Is(err, ErrSSHUninjected) {
		t.Fatalf("default SSH error = %v", err)
	}
}

func TestUnsupportedServerIsRejectedBeforeOperationalWrite(t *testing.T) {
	f := newFakeTransport()
	var methods []string
	f.onWrite = func(payload []byte) {
		var msg struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		_ = json.Unmarshal(payload, &msg)
		methods = append(methods, msg.Method)
		if msg.Method == "initialize" {
			f.reads <- appserver.Frame{Type: appserver.FrameText, Payload: initializeResponse(msg.ID, "codex/0.142.0")}
		}
	}
	route := testRoute(t)
	_, err := Connect(context.Background(), route, Options{Dialer: DialFunc(func(context.Context, endpoint.Route) (appserver.Transport, error) { return f, nil })})
	var compatibilityErr *CompatibilityError
	if !errors.As(err, &compatibilityErr) || compatibilityErr.Result.Class != compat.Unsupported {
		t.Fatalf("error = %T %v", err, err)
	}
	if len(methods) != 2 || methods[0] != "initialize" || methods[1] != "initialized" {
		t.Fatalf("operational methods were written: %v", methods)
	}
}

func TestCallPreservesRawResultAndServerErrorWriteEvidence(t *testing.T) {
	conn, f := connectFixture(t, "codex/0.154.0", Options{})
	defer conn.Close(context.Background())
	f.onWrite = func(payload []byte) {
		var msg struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := json.Unmarshal(payload, &msg); err != nil {
			t.Errorf("decode call: %v", err)
			return
		}
		if msg.Method == "thread/read" {
			f.reads <- appserver.Frame{Type: appserver.FrameText, Payload: []byte(fmt.Sprintf(`{"id":%s,"result":{"new":true,"nested":{"x":1}}}`, msg.ID))}
		}
		if msg.Method == "thread/bad" {
			f.reads <- appserver.Frame{Type: appserver.FrameText, Payload: []byte(fmt.Sprintf(`{"id":%s,"error":{"code":-32001,"message":"bad","data":{"raw":true}}}`, msg.ID))}
		}
	}
	detailed, err := conn.CallDetailed(context.Background(), "thread/read", json.RawMessage(`{"threadId":"t1"}`))
	if err != nil || detailed.ServerError != nil || string(detailed.Result) != `{"new":true,"nested":{"x":1}}` || detailed.Evidence.Phase != appserver.WriteComplete || detailed.Generation != conn.Generation() {
		t.Fatalf("detailed result = %+v, err = %v", detailed, err)
	}
	raw, serverErr, err := conn.Call(context.Background(), "thread/read", json.RawMessage(`{"threadId":"t1"}`))
	if err != nil || serverErr != nil || string(raw) != `{"new":true,"nested":{"x":1}}` {
		t.Fatalf("raw result = %s, server = %+v, err = %v", raw, serverErr, err)
	}
	detailed, err = conn.RawCall(context.Background(), "thread/bad", nil)
	if err != nil || detailed.ServerError == nil || detailed.Evidence.Phase != appserver.WriteComplete || detailed.Generation != conn.Generation() {
		t.Fatalf("detailed server error = %+v, err = %v", detailed, err)
	}
	serverErr = detailed.ServerError
	if serverErr == nil || string(serverErr.Data) != `{"raw":true}` || !strings.Contains(string(serverErr.Raw), `"code":-32001`) {
		t.Fatalf("server error = %+v", serverErr)
	}
	_, serverErr, err = conn.Call(context.Background(), "thread/bad", nil)
	if err != nil || serverErr == nil {
		t.Fatalf("caller server error contract = server=%+v err=%v", serverErr, err)
	}
}

func TestSemanticCallerReceivesClassifiableServerErrors(t *testing.T) {
	conn, f := connectFixture(t, "codex/0.154.0", Options{})
	defer conn.Close(context.Background())
	f.onWrite = func(payload []byte) {
		var msg struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
			Params json.RawMessage `json:"params"`
		}
		_ = json.Unmarshal(payload, &msg)
		if msg.Method != "turn/start" {
			return
		}
		kind := "Review"
		turnKind := "review"
		if strings.Contains(string(msg.Params), "compact") {
			kind, turnKind = "Compact", "compact"
		}
		if strings.Contains(string(msg.Params), "generic") {
			f.reads <- appserver.Frame{Type: appserver.FrameText, Payload: []byte(fmt.Sprintf(`{"id":%s,"error":{"code":-32603,"message":"generic internal error"}}`, msg.ID))}
			return
		}
		f.reads <- appserver.Frame{Type: appserver.FrameText, Payload: []byte(fmt.Sprintf(`{"id":%s,"error":{"code":-32603,"message":"failed to submit turn input: ActiveTurnNotSteerable { turn_kind: %s }","data":{"codexErrorInfo":{"activeTurnNotSteerable":{"turnKind":%q}}}}}`, msg.ID, kind, turnKind))}
	}
	api := codexapi.New(conn, codexapi.Options{})
	_, err := api.StartOrSteerTurn(context.Background(), codexapi.TurnStartRequest{ThreadID: "t1", Text: "review", ClientUserMessageID: "m1"})
	var reviewErr *codexapi.ServerError
	if !errors.As(err, &reviewErr) || codexapi.ClassifyNotSubmitted(reviewErr).Kind != codexapi.NotSubmittedReview {
		t.Fatalf("review server error = %T %v", err, err)
	}
	_, err = api.StartOrSteerTurn(context.Background(), codexapi.TurnStartRequest{ThreadID: "t1", Text: "compact", ClientUserMessageID: "m2"})
	var compactErr *codexapi.ServerError
	if !errors.As(err, &compactErr) || codexapi.ClassifyNotSubmitted(compactErr).Kind != codexapi.NotSubmittedCompact {
		t.Fatalf("compact server error = %T %v", err, err)
	}
	_, err = api.StartOrSteerTurn(context.Background(), codexapi.TurnStartRequest{ThreadID: "t1", Text: "generic", ClientUserMessageID: "m2"})
	var genericErr *codexapi.ServerError
	if !errors.As(err, &genericErr) || codexapi.ClassifyNotSubmitted(genericErr).Recognized() {
		t.Fatalf("generic server error = %T %v classification=%+v", err, err, codexapi.ClassifyNotSubmitted(genericErr))
	}
}

func TestCallDetailedRetainsPreAndPostWriteLocalEvidence(t *testing.T) {
	pre, _ := connectFixture(t, "codex/0.154.0", Options{})
	result, err := pre.CallDetailed(context.Background(), "", nil)
	var adapterErr *CallError
	if !errors.As(err, &adapterErr) || result.Evidence.Phase != appserver.WriteProvenBeforeWrite || adapterErr.Evidence.Phase != appserver.WriteProvenBeforeWrite {
		t.Fatalf("pre-write result=%+v err=%T %+v", result, err, err)
	}
	pre.Close(context.Background())

	post, f := connectFixture(t, "codex/0.154.0", Options{})
	f.writeErr = func([]byte) error {
		return &appserver.WriteFailure{Err: errors.New("write lost"), Phase: appserver.WriteMayHaveWritten}
	}
	result, err = post.CallDetailed(context.Background(), "thread/write", nil)
	if !errors.As(err, &adapterErr) || result.Evidence.Phase != appserver.WriteMayHaveWritten || adapterErr.Evidence.Phase != appserver.WriteMayHaveWritten {
		t.Fatalf("post-write result=%+v err=%T %+v", result, err, err)
	}
	post.Close(context.Background())
}

func TestCallCannotBypassClosedGateAndEventsRemainObservable(t *testing.T) {
	conn, f := connectFixture(t, "codex/0.154.0", Options{})
	f.reads <- appserver.Frame{Type: appserver.FrameText, Payload: []byte(`{"method":"thread/started","params":{"threadId":"t1"}}`)}
	event, err := conn.NextEvent(context.Background())
	if err != nil || event.Kind != appserver.EventNotification || event.Notification.Method != "thread/started" {
		t.Fatalf("event = %+v, err = %v", event, err)
	}
	if err := conn.Detach(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, _, err = conn.Call(context.Background(), "thread/read", nil)
	var callErr *CallError
	if !errors.As(err, &callErr) || callErr.Evidence.Phase != appserver.WriteProvenBeforeWrite {
		t.Fatalf("closed gate = %T %+v", err, err)
	}
	select {
	case payload := <-f.writes:
		if string(payload) != `{"method":"initialized"}` && !strings.Contains(string(payload), `"method":"initialize"`) {
			t.Fatalf("unexpected post-close write: %s", payload)
		}
	default:
	}
}

func TestRPCAdapterPreservesCallerIDAndAppserverErrorEvidence(t *testing.T) {
	conn, f := connectFixture(t, "codex/0.154.0", Options{ExperimentalAPI: true})
	defer conn.Close(context.Background())
	adapter := NewRPCAdapter(conn)
	if !adapter.ExperimentalAPIEnabled() {
		t.Fatal("adapter lost experimental API proof")
	}
	f.onWrite = func(payload []byte) {
		var msg struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := json.Unmarshal(payload, &msg); err != nil {
			t.Errorf("decode raw request: %v", err)
			return
		}
		switch msg.Method {
		case "raw/read":
			f.reads <- appserver.Frame{Type: appserver.FrameText, Payload: []byte(fmt.Sprintf(`{"id":%s,"result":{"nested":{"ok":true}}}`, msg.ID))}
		case "raw/error":
			f.reads <- appserver.Frame{Type: appserver.FrameText, Payload: []byte(fmt.Sprintf(`{"id":%s,"error":{"code":-32603,"message":"raw failure","data":{"nested":{"reason":"preserve"}}}}`, msg.ID))}
		}
	}
	result, err := adapter.Call(context.Background(), appserver.RPCRequest{ID: "caller-selected", Method: "raw/read"})
	if err != nil || result == nil || result.ID != "caller-selected" || string(result.Value) != `{"nested":{"ok":true}}` || result.Evidence.Phase != appserver.WriteComplete || result.Generation != conn.Generation() {
		t.Fatalf("raw result = %+v err=%v", result, err)
	}
	_, err = adapter.Call(context.Background(), appserver.RPCRequest{ID: "error-id", Method: "raw/error"})
	var callErr *appserver.CallError
	if !errors.As(err, &callErr) || callErr.Server == nil || callErr.Server.Code != -32603 || string(callErr.Server.Data) != `{"nested":{"reason":"preserve"}}` || callErr.Evidence.Phase != appserver.WriteComplete || callErr.Generation != conn.Generation() {
		t.Fatalf("raw server error = %T %+v", err, err)
	}
}

func TestRPCAdapterClosedGateRejectsBeforeWrite(t *testing.T) {
	conn, f := connectFixture(t, "codex/0.154.0", Options{})
	adapter := NewRPCAdapter(conn)
	if err := conn.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	_, err := adapter.Call(context.Background(), appserver.RPCRequest{ID: "closed", Method: "raw/read"})
	var callErr *appserver.CallError
	if !errors.As(err, &callErr) || callErr.Evidence.Phase != appserver.WriteProvenBeforeWrite || callErr.Generation != conn.Generation() {
		t.Fatalf("closed gate error = %T %+v", err, err)
	}
	select {
	case payload := <-f.writes:
		if strings.Contains(string(payload), `"method":"raw/read"`) {
			t.Fatalf("closed adapter wrote operational request: %s", payload)
		}
	default:
	}
	var nilAdapter *RPCAdapter
	if nilAdapter.ExperimentalAPIEnabled() {
		t.Fatal("nil raw adapter reported experimental API")
	}
}
