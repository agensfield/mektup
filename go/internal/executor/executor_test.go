package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	mektup "github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/appserver"
	"github.com/agensfield/mektup/go/internal/cli"
	"github.com/agensfield/mektup/go/internal/codexapi"
	"github.com/agensfield/mektup/go/internal/connection"
	"github.com/agensfield/mektup/go/internal/doctor"
	"github.com/agensfield/mektup/go/internal/endpoint"
	"github.com/agensfield/mektup/go/internal/journal"
	"github.com/agensfield/mektup/go/internal/rawrpc"
	"github.com/agensfield/mektup/go/internal/rpcmeta"
)

type fakeCodex struct {
	scoped    bool
	allowName bool
	threadIDs *[]string
}

func (f fakeCodex) ThreadList(context.Context, codexapi.ThreadListOptions) (codexapi.ThreadListResponse, error) {
	return codexapi.ThreadListResponse{Raw: json.RawMessage(`{"data":[],"nextCursor":"next"}`), NextCursor: "next"}, nil
}
func (f fakeCodex) ThreadRead(_ context.Context, options codexapi.ThreadReadOptions) (codexapi.ThreadReadResponse, error) {
	if f.threadIDs != nil {
		*f.threadIDs = append(*f.threadIDs, options.ThreadID)
	}
	return codexapi.ThreadReadResponse{Raw: json.RawMessage(`{"thread":{"id":"thr_1"}}`)}, nil
}
func (f fakeCodex) ThreadTurns(context.Context, codexapi.TurnsOptions) (codexapi.ThreadTurnsResponse, error) {
	return codexapi.ThreadTurnsResponse{Raw: json.RawMessage(`{"data":[],"nextCursor":"turns"}`), NextCursor: "turns"}, nil
}
func (f fakeCodex) ThreadItems(context.Context, codexapi.ItemsOptions) (codexapi.ThreadItemsResponse, error) {
	return codexapi.ThreadItemsResponse{Raw: json.RawMessage(`{"data":[],"nextCursor":"items"}`), NextCursor: "items"}, nil
}
func (f fakeCodex) ThreadStart(context.Context, codexapi.StartOptions) (codexapi.ThreadStartResponse, error) {
	return codexapi.ThreadStartResponse{Thread: codexapi.Thread{ID: "thr_new"}, Raw: json.RawMessage(`{"thread":{"id":"thr_new","turns":[{"items":[{"text":"secret body"}]}]}}`)}, nil
}
func (f fakeCodex) ThreadResume(context.Context, codexapi.ResumeOptions) (codexapi.ThreadResumeResponse, error) {
	return codexapi.ThreadResumeResponse{Raw: json.RawMessage(`{"thread":{"id":"thr_resume"}}`)}, nil
}
func (f fakeCodex) ThreadFork(context.Context, codexapi.ForkOptions) (codexapi.ThreadForkResponse, error) {
	return codexapi.ThreadForkResponse{Thread: codexapi.Thread{ID: "thr_fork"}, Raw: json.RawMessage(`{"thread":{"id":"thr_fork"}}`)}, nil
}

func (f fakeCodex) ThreadSetName(context.Context, string, string) (codexapi.ThreadNameResponse, error) {
	if !f.allowName {
		return codexapi.ThreadNameResponse{}, errors.New("thread/name/set failed")
	}
	return codexapi.ThreadNameResponse{Raw: json.RawMessage(`{"ok":true}`)}, nil
}
func (f fakeCodex) Search(context.Context, codexapi.SearchOptions) (codexapi.SearchResponse, error) {
	return codexapi.SearchResponse{Raw: json.RawMessage(`{"data":[],"nextCursor":"search"}`), NextCursor: "search"}, nil
}
func (f fakeCodex) SearchOccurrences(context.Context, codexapi.SearchOccurrencesOptions) (codexapi.SearchOccurrencesResponse, error) {
	if !f.scoped {
		return codexapi.SearchOccurrencesResponse{}, errors.New("scoped unavailable")
	}
	return codexapi.SearchOccurrencesResponse{Raw: json.RawMessage(`{"data":[],"nextCursor":"scoped"}`), NextCursor: "scoped"}, nil
}

type noNameCodex struct{}

func (noNameCodex) ThreadList(c context.Context, o codexapi.ThreadListOptions) (codexapi.ThreadListResponse, error) {
	return fakeCodex{}.ThreadList(c, o)
}
func (noNameCodex) ThreadRead(c context.Context, o codexapi.ThreadReadOptions) (codexapi.ThreadReadResponse, error) {
	return fakeCodex{}.ThreadRead(c, o)
}
func (noNameCodex) ThreadTurns(c context.Context, o codexapi.TurnsOptions) (codexapi.ThreadTurnsResponse, error) {
	return fakeCodex{}.ThreadTurns(c, o)
}
func (noNameCodex) ThreadItems(c context.Context, o codexapi.ItemsOptions) (codexapi.ThreadItemsResponse, error) {
	return fakeCodex{}.ThreadItems(c, o)
}
func (noNameCodex) ThreadStart(c context.Context, o codexapi.StartOptions) (codexapi.ThreadStartResponse, error) {
	return fakeCodex{}.ThreadStart(c, o)
}
func (noNameCodex) ThreadResume(c context.Context, o codexapi.ResumeOptions) (codexapi.ThreadResumeResponse, error) {
	return fakeCodex{}.ThreadResume(c, o)
}
func (noNameCodex) ThreadFork(c context.Context, o codexapi.ForkOptions) (codexapi.ThreadForkResponse, error) {
	return fakeCodex{}.ThreadFork(c, o)
}
func (noNameCodex) Search(c context.Context, o codexapi.SearchOptions) (codexapi.SearchResponse, error) {
	return fakeCodex{}.Search(c, o)
}

type fakeConnection struct{ api Codex }

func (f *fakeConnection) Codex() Codex       { return f.api }
func (f *fakeConnection) RPC() rawrpc.Caller { return nil }
func (f *fakeConnection) Warnings() []string { return []string{"server_version_unknown"} }
func (f *fakeConnection) Close() error       { return nil }

type fakeConnections struct {
	opened  int
	checked int
	conn    *fakeConnection
}

type fakeTargets struct {
	endpoint string
	thread   string
	seen     []string
	err      error
}

func (f *fakeTargets) ResolveThread(_ context.Context, selector, _ string) (ThreadTarget, error) {
	f.seen = append(f.seen, selector)
	if f.err != nil {
		return ThreadTarget{}, f.err
	}
	return ThreadTarget{Endpoint: f.endpoint, EndpointID: f.endpoint, Resolved: endpoint.Endpoint{ID: f.endpoint, Alias: "local"}, ThreadID: f.thread, URI: selector}, nil
}

func (f *fakeTargets) ResolveThreadWithOptions(ctx context.Context, selector, endpointOverride string, _ bool) (ThreadTarget, error) {
	return f.ResolveThread(ctx, selector, endpointOverride)
}

type optionsConnections struct {
	*fakeConnections
	options []OpenOptions
}

func (f *optionsConnections) OpenWithOptions(ctx context.Context, selector string, options OpenOptions) (Connection, error) {
	f.options = append(f.options, options)
	return f.fakeConnections.Open(ctx, selector)
}

func (f *fakeConnections) Open(context.Context, string) (Connection, error) {
	f.opened++
	return f.conn, nil
}
func (f *fakeConnections) Check(context.Context, string) (any, error) {
	f.checked++
	return map[string]any{"initialized": true}, nil
}

type fakeEndpoints struct {
	item           endpoint.Endpoint
	added, removed int
}

func (f *fakeEndpoints) List() ([]endpoint.Endpoint, error)     { return []endpoint.Endpoint{f.item}, nil }
func (f *fakeEndpoints) Show(string) (endpoint.Endpoint, error) { return f.item, nil }
func (f *fakeEndpoints) Add(v endpoint.Endpoint) error {
	f.item = v
	f.added++
	return nil
}
func (f *fakeEndpoints) Remove(string) error { f.removed++; return nil }

type fakeStorage struct{ maintained, vacuumed int }

func (f *fakeStorage) Status(context.Context) (journal.StorageStatus, error) {
	return journal.StorageStatus{Counts: map[string]int64{}}, nil
}
func (f *fakeStorage) Check(context.Context) (journal.StorageCheck, error) {
	return journal.StorageCheck{ReadOnly: true, Integrity: "ok"}, nil
}
func (f *fakeStorage) Maintain(context.Context, journal.MaintenanceOptions) (journal.MaintenanceReceipt, error) {
	f.maintained++
	return journal.MaintenanceReceipt{DryRun: true}, nil
}
func (f *fakeStorage) Vacuum(context.Context) (journal.VacuumReceipt, error) {
	f.vacuumed++
	return journal.VacuumReceipt{Applied: true}, nil
}

type fakeDoctor struct{ fixes []bool }

func (f *fakeDoctor) Run(_ context.Context, _ doctor.Options, fix bool) (doctor.Report, error) {
	f.fixes = append(f.fixes, fix)
	return doctor.Report{Fix: fix, ReadOnly: !fix}, nil
}

type fakeReceipts struct {
	count    int
	payloads []any
}

func (f *fakeReceipts) Mutation(_ context.Context, _ string, _ string, payload any) (any, error) {
	f.count++
	// The fake intentionally retains the input so privacy tests can inspect
	// exactly what would have crossed the journal boundary.
	f.payloads = append(f.payloads, payload)
	returnValue := map[string]any{"schema": "mektup/receipt/v1", "receiptId": "rcpt_test", "operationId": "op_00000000-0000-7000-8000-000000000000", "state": "accepted"}
	return returnValue, nil
}

type fakeInput struct{ file, stdin []byte }

func (f fakeInput) ReadFile(context.Context, string, int64) ([]byte, error) { return f.file, nil }
func (f fakeInput) ReadStdin(context.Context, int64) ([]byte, error)        { return f.stdin, nil }

type fakeRPC struct {
	request rawrpc.Request
	count   int
}

func (f *fakeRPC) Execute(_ context.Context, _ rawrpc.Caller, request rawrpc.Request) (*rawrpc.Response, error) {
	f.count++
	f.request = request
	return &rawrpc.Response{Method: request.Method, Raw: json.RawMessage(`{"ok":true}`), Effects: []rpcmeta.EffectClass{rpcmeta.Read}}, nil
}

func invocation(command string, position ...string) cli.Invocation {
	path := []string{command}
	if command == "thread" || command == "endpoint" || command == "storage" {
		path = append(path, position[0])
	}
	return cli.Invocation{Command: command, Position: position, Path: path, Options: map[string][]string{}, Resolved: cli.ResolvedGlobals{Endpoint: "test"}}
}

func TestOwnedCommandsUseOnlyInjectedPorts(t *testing.T) {
	conn := &fakeConnection{api: fakeCodex{scoped: true}}
	connections := &fakeConnections{conn: conn}
	endpoints := &fakeEndpoints{}
	storage := &fakeStorage{}
	doctorPort := &fakeDoctor{}
	receipts := &fakeReceipts{}
	rpc := &fakeRPC{}
	e := New(Ports{Connections: connections, Endpoints: endpoints, Storage: storage, Doctor: doctorPort, RPC: rpc, Receipts: receipts, Input: fakeInput{stdin: []byte(`{}`)}})
	tests := []struct {
		name   string
		inv    cli.Invocation
		mutate bool
	}{
		{"thread list", invocation("thread", "list"), false},
		{"thread read", invocation("thread", "read", "thr_1"), false},
		{"thread turns", invocation("thread", "turns", "thr_1"), false},
		{"thread items", invocation("thread", "items", "thr_1"), false},
		{"thread start", invocation("thread", "start"), true},
		{"thread resume", invocation("thread", "resume", "thr_1"), true},
		{"thread fork", invocation("thread", "fork", "thr_1"), true},
		{"search cross", invocation("search", "needle"), false},
		{"search scoped", func() cli.Invocation {
			i := invocation("search", "needle")
			i.Options["thread"] = []string{"thr_1"}
			return i
		}(), false},
		{"endpoint list", invocation("endpoint", "list"), false},
		{"endpoint show", invocation("endpoint", "show", "local"), false},
		{"endpoint add", func() cli.Invocation {
			i := invocation("endpoint", "add", "dev")
			i.Options["unix"] = []string{"/tmp/codex.sock"}
			i.Options["id"] = []string{"ep_00000000-0000-7000-8000-000000000000"}
			return i
		}(), true},
		{"endpoint remove", invocation("endpoint", "remove", "dev"), true},
		{"endpoint check", invocation("endpoint", "check"), false},
		{"storage status", invocation("storage", "status"), false},
		{"storage check", invocation("storage", "check"), false},
		{"storage maintain", invocation("storage", "maintain"), true},
		{"storage vacuum", invocation("storage", "vacuum"), true},
		{"doctor", invocation("doctor"), false},
		{"doctor fix", func() cli.Invocation { i := invocation("doctor"); i.Options["fix"] = []string{"true"}; return i }(), true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := e.Execute(context.Background(), tc.inv)
			if err != nil {
				t.Fatalf("execute: %v", err)
			}
			if len(result.Events) != 1 {
				t.Fatalf("events=%d", len(result.Events))
			}
			if tc.mutate && result.Receipt == nil {
				t.Fatal("mutation did not emit a receipt")
			}
		})
	}
	if connections.checked != 1 {
		t.Fatalf("endpoint check used %d initialize checks", connections.checked)
	}
	if connections.opened == 0 {
		t.Fatal("operational commands did not use open")
	}
	if storage.maintained != 1 || storage.vacuumed != 1 || len(doctorPort.fixes) != 2 {
		t.Fatalf("ports not exercised: storage=%+v doctor=%v", storage, doctorPort.fixes)
	}
	if endpoints.added != 1 || endpoints.removed != 1 {
		t.Fatalf("endpoint mutations not exercised: %+v", endpoints)
	}
	if endpoints.item.Alias != "dev" || mektup.ValidateID(endpoints.item.ID, mektup.EndpointIDPrefix) != nil {
		t.Fatalf("endpoint alias/id were not mapped independently: %+v", endpoints.item)
	}
	if receipts.count != 10 {
		t.Fatalf("receipt count=%d, want 10 including two search read receipts", receipts.count)
	}
}

func TestEndpointAddLeavesIDEmptyForStoreGeneration(t *testing.T) {
	endpoints := &fakeEndpoints{}
	e := New(Ports{Endpoints: endpoints, Receipts: &fakeReceipts{}})
	i := invocation("endpoint", "add", "generated")
	i.Options["unix"] = []string{"/tmp/codex.sock"}
	if _, err := e.Execute(context.Background(), i); err != nil {
		t.Fatal(err)
	}
	if endpoints.item.Alias != "generated" || mektup.ValidateID(endpoints.item.ID, mektup.EndpointIDPrefix) != nil {
		t.Fatalf("store did not receive alias and generate ID: %+v", endpoints.item)
	}
}

func TestThreadNameIsForwardedOrRejectedRatherThanIgnored(t *testing.T) {
	e := New(Ports{Connections: &fakeConnections{conn: &fakeConnection{api: fakeCodex{allowName: true}}}, Receipts: &fakeReceipts{}})
	start := invocation("thread", "start")
	start.Options["name"] = []string{"session-name"}
	if _, err := e.Execute(context.Background(), start); err != nil {
		t.Fatalf("thread start name was rejected: %v", err)
	}
	fork := invocation("thread", "fork", "thr_1")
	fork.Options["name"] = []string{"session-name"}
	if _, err := e.Execute(context.Background(), fork); err != nil {
		t.Fatalf("thread fork name was not applied through the semantic adapter: %v", err)
	}
}

func TestThreadNameFailureCarriesPartialEffectEvidence(t *testing.T) {
	receipts := &fakeReceipts{}
	e := New(Ports{Connections: &fakeConnections{conn: &fakeConnection{api: fakeCodex{}}}, Receipts: receipts})
	start := invocation("thread", "start")
	start.Options["name"] = []string{"session-name"}
	_, err := e.Execute(context.Background(), start)
	var ce *cli.Error
	if !errors.As(err, &ce) || ce.Effect != "accepted" || ce.Details["partialEffect"] != true || ce.Details["creationState"] != "accepted" || ce.Details["nameEffect"] != "unknown" || receipts.count != 1 {
		t.Fatalf("err=%v receipts=%d", err, receipts.count)
	}
}

type trackingConnection struct {
	api    Codex
	closed int
}

func (c *trackingConnection) Codex() Codex       { return c.api }
func (c *trackingConnection) RPC() rawrpc.Caller { return nil }
func (c *trackingConnection) Warnings() []string { return nil }
func (c *trackingConnection) Close() error       { c.closed++; return nil }

type trackingFactory struct{ conn *trackingConnection }

func (f trackingFactory) Open(context.Context, string) (Connection, error) { return f.conn, nil }
func (f trackingFactory) Check(context.Context, string) (any, error)       { return nil, nil }

func TestNameAdapterPreflightClosesConnection(t *testing.T) {
	conn := &trackingConnection{api: noNameCodex{}}
	e := New(Ports{Connections: trackingFactory{conn: conn}, Receipts: &fakeReceipts{}})
	start := invocation("thread", "start")
	start.Options["name"] = []string{"session"}
	if _, err := e.Execute(context.Background(), start); err == nil {
		t.Fatal("missing name adapter was accepted")
	}
	if conn.closed != 1 {
		t.Fatalf("connection cleanup count=%d", conn.closed)
	}
}

type noOperationReceipt struct{}

func (noOperationReceipt) Mutation(context.Context, string, string, any) (any, error) {
	return map[string]any{"schema": "mektup/receipt/v1", "receiptId": "rcpt_test", "state": "accepted"}, nil
}

func TestReceiptWithoutOperationIdentityFailsClosed(t *testing.T) {
	e := New(Ports{Connections: &fakeConnections{conn: &fakeConnection{api: fakeCodex{}}}, Receipts: noOperationReceipt{}})
	if _, err := e.Execute(context.Background(), invocation("thread", "start")); err == nil {
		t.Fatal("receipt without operation identity was accepted")
	}
}

func TestForceWithoutOutputIsUsageError(t *testing.T) {
	e := New(Ports{Connections: &fakeConnections{conn: &fakeConnection{api: fakeCodex{}}}, RPC: &fakeRPC{}, Receipts: &fakeReceipts{}})
	i := invocation("rpc", "thread/read")
	i.Options["force"] = []string{"true"}
	var ce *cli.Error
	if _, err := e.Execute(context.Background(), i); !errors.As(err, &ce) || ce.Exit != cli.ExitUsage {
		t.Fatalf("err=%v", err)
	}
}

func TestFullAppResolvesPresentationThreadURIBeforeAdapter(t *testing.T) {
	var seen []string
	targets := &fakeTargets{endpoint: "ep_01999999-9999-7999-8999-999999999999", thread: "native-thread", seen: nil}
	api := fakeCodex{threadIDs: &seen}
	e := New(Ports{Connections: &fakeConnections{conn: &fakeConnection{api: api}}, Targets: targets})
	var out, errOut bytes.Buffer
	a := &cli.App{In: strings.NewReader(""), Out: &out, Err: &errOut, Env: []string{"MEKTUP_AGENT=1"}, Executor: e}
	if code := a.Run([]string{"--json", "thread", "read", "codex://local/thread/presentation-id"}); code != int(cli.ExitSuccess) {
		t.Fatalf("code=%d stderr=%q", code, errOut.String())
	}
	if len(targets.seen) != 1 || targets.seen[0] != "codex://local/thread/presentation-id" || len(seen) != 1 || seen[0] != "native-thread" {
		t.Fatalf("target resolution seen=%v adapter=%v", targets.seen, seen)
	}
}

func TestUnmappedOrMismatchedThreadTargetFailsBeforeOpen(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		code cli.ExitCode
	}{
		{"unmapped", endpoint.ErrEndpointNotFound, cli.ExitRejected},
		{"mismatch", endpoint.ErrEndpointMismatch, cli.ExitRejected},
		{"invalid", endpoint.ErrInvalidTarget, cli.ExitUsage},
	} {
		t.Run(tc.name, func(t *testing.T) {
			connections := &fakeConnections{conn: &fakeConnection{api: fakeCodex{}}}
			e := New(Ports{Connections: connections, Targets: &fakeTargets{err: tc.err}})
			_, err := e.Execute(context.Background(), invocation("thread", "read", "codex://missing/thread/id"))
			var ce *cli.Error
			if !errors.As(err, &ce) || ce.Exit != tc.code || connections.opened != 0 {
				t.Fatalf("err=%v opened=%d", err, connections.opened)
			}
		})
	}
}

func TestThreadMutationProjectionNeverCarriesTranscriptBodies(t *testing.T) {
	receipts := &fakeReceipts{}
	e := New(Ports{Connections: &fakeConnections{conn: &fakeConnection{api: fakeCodex{}}}, Receipts: receipts})
	result, err := e.Execute(context.Background(), invocation("thread", "start"))
	if err != nil {
		t.Fatal(err)
	}
	if len(receipts.payloads) != 1 {
		t.Fatalf("receipt payloads=%d", len(receipts.payloads))
	}
	eventBytes, _ := json.Marshal(result.Events[0].Machine)
	receiptBytes, _ := json.Marshal(receipts.payloads[0])
	if strings.Contains(string(eventBytes), "secret body") || strings.Contains(string(receiptBytes), "secret body") {
		t.Fatalf("thread mutation leaked transcript body: event=%s receipt=%s", eventBytes, receiptBytes)
	}
	if !strings.Contains(string(eventBytes), "thr_new") || !strings.Contains(string(receiptBytes), "thr_new") {
		t.Fatalf("bounded thread metadata missing: event=%s receipt=%s", eventBytes, receiptBytes)
	}
}

func TestLifecycleWarningsRemainAtEnvelopeLevel(t *testing.T) {
	var out, errOut bytes.Buffer
	e := New(Ports{Connections: &fakeConnections{conn: &fakeConnection{api: fakeCodex{}}}, Receipts: &fakeReceipts{}})
	a := &cli.App{In: strings.NewReader(""), Out: &out, Err: &errOut, Env: []string{"MEKTUP_AGENT=1"}, Executor: e}
	if code := a.Run([]string{"--json", "search", "needle"}); code != int(cli.ExitSuccess) {
		t.Fatalf("code=%d stderr=%q", code, errOut.String())
	}
	var event map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &event); err != nil {
		t.Fatal(err)
	}
	warnings, ok := event["warnings"].([]any)
	if !ok || len(warnings) != 1 {
		t.Fatalf("warnings=%#v event=%#v", event["warnings"], event)
	}
	first, _ := warnings[0].(map[string]any)
	if first["code"] != "server_version_unknown" || first["details"] == nil {
		t.Fatalf("warning objects=%#v", warnings)
	}
	data, _ := event["data"].(map[string]any)
	if data["experimental"] != true {
		t.Fatalf("experimental marker missing from result: %#v", data)
	}
	if _, nested := data["warnings"]; nested {
		t.Fatalf("warning was nested under data: %#v", data)
	}
}

func TestAppJSONLUsesLockedReadAndStorageFamilies(t *testing.T) {
	commands := []struct {
		name       string
		args       []string
		event      string
		resultKind string
		wantData   bool
		subcommand string
	}{
		{"thread list", []string{"thread", "list"}, "thread.list.completed", "thread", true, ""},
		{"thread read", []string{"thread", "read", "thr_1"}, "thread.read.completed", "thread", true, ""},
		{"thread turns", []string{"thread", "turns", "thr_1", "--view", "summary"}, "thread.turns.completed", "thread", true, ""},
		{"thread items", []string{"thread", "items", "thr_1"}, "thread.items.completed", "thread", true, ""},
		{"search", []string{"search", "needle"}, "search.completed", "thread", true, ""},
		{"search scoped", []string{"search", "needle", "--thread", "thr_1"}, "search.completed", "message", true, ""},
		{"storage status", []string{"storage", "status"}, "storage.completed", "storage", false, "status"},
	}
	for _, tc := range commands {
		t.Run(tc.name, func(t *testing.T) {
			var out, errOut bytes.Buffer
			e := New(Ports{Connections: &fakeConnections{conn: &fakeConnection{api: fakeCodex{scoped: strings.Contains(tc.name, "scoped")}}}, Storage: &fakeStorage{}, Receipts: &fakeReceipts{}})
			a := &cli.App{In: strings.NewReader(""), Out: &out, Err: &errOut, Env: []string{"MEKTUP_AGENT=1"}, Executor: e}
			args := append([]string{"--json"}, tc.args...)
			if code := a.Run(args); code != int(cli.ExitSuccess) {
				t.Fatalf("code=%d stderr=%q", code, errOut.String())
			}
			var event map[string]any
			if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &event); err != nil {
				t.Fatal(err)
			}
			if event["event"] != tc.event {
				t.Fatalf("event=%v want=%s", event["event"], tc.event)
			}
			data := event["data"].(map[string]any)
			if data["resultKind"] != tc.resultKind {
				t.Fatalf("resultKind=%v want=%s", data["resultKind"], tc.resultKind)
			}
			if tc.wantData {
				if _, ok := data["data"].([]any); !ok {
					t.Fatalf("data is not bounded array: %#v", data["data"])
				}
			}
			if tc.subcommand != "" && data["subcommand"] != tc.subcommand {
				t.Fatalf("subcommand=%v want=%s", data["subcommand"], tc.subcommand)
			}
		})
	}
}

func TestEmptyInlineParamsAreRejectedBeforeRPC(t *testing.T) {
	rpc := &fakeRPC{}
	e := New(Ports{Connections: &fakeConnections{conn: &fakeConnection{api: fakeCodex{}}}, RPC: rpc, Receipts: &fakeReceipts{}})
	i := invocation("rpc", "thread/list")
	i.Options["params"] = []string{""}
	if _, err := e.Execute(context.Background(), i); err == nil {
		t.Fatal("empty inline params were accepted")
	}
	if rpc.count != 0 {
		t.Fatalf("RPC dispatched invalid inline params: %d", rpc.count)
	}
}

func TestExplicitRPCOutputRequiresArtifactPort(t *testing.T) {
	rpc := &fakeRPC{}
	e := New(Ports{Connections: &fakeConnections{conn: &fakeConnection{api: fakeCodex{}}}, RPC: rpc, Receipts: &fakeReceipts{}})
	i := invocation("rpc", "thread/list")
	i.Options["output"] = []string{"/tmp/response.json"}
	if _, err := e.Execute(context.Background(), i); err == nil {
		t.Fatal("explicit RPC output was silently ignored")
	}
	if rpc.count != 0 {
		t.Fatalf("RPC dispatched without artifact port: %d", rpc.count)
	}
}

func TestMutationDoesNotRunWithoutReceiptJournal(t *testing.T) {
	connections := &fakeConnections{conn: &fakeConnection{api: fakeCodex{}}}
	endpoints := &fakeEndpoints{}
	e := New(Ports{Connections: connections, Endpoints: endpoints})
	if _, err := e.Execute(context.Background(), invocation("thread", "start")); err == nil {
		t.Fatal("thread mutation succeeded without a receipt journal")
	}
	if connections.opened != 0 {
		t.Fatalf("connection opened before receipt gate: %d", connections.opened)
	}
	add := invocation("endpoint", "add", "dev")
	add.Options["unix"] = []string{"/tmp/codex.sock"}
	if _, err := e.Execute(context.Background(), add); err == nil {
		t.Fatal("endpoint mutation succeeded without a receipt journal")
	}
	if endpoints.added != 0 {
		t.Fatalf("endpoint changed before receipt gate: %d", endpoints.added)
	}
}

func TestRPCParamsAreBoundedExclusiveAndForwarded(t *testing.T) {
	rpc := &fakeRPC{}
	e := New(Ports{Connections: &fakeConnections{conn: &fakeConnection{api: fakeCodex{}}}, RPC: rpc, Receipts: &fakeReceipts{}, Input: fakeInput{file: []byte(`{"file":true}`), stdin: []byte(`{"stdin":true}`)}})
	i := invocation("rpc", "thread/list")
	i.Options["params-file"] = []string{"params.json"}
	i.Options["allow-effect"] = []string{"read"}
	result, err := e.Execute(context.Background(), i)
	if err != nil || result.Receipt == nil {
		t.Fatalf("rpc err=%v result=%+v", err, result)
	}
	if rpc.request.ParamsSource != rawrpc.ParamsFile || string(rpc.request.Params) != `{"file":true}` {
		t.Fatalf("request=%+v", rpc.request)
	}
	i.Options["stdin"] = []string{"true"}
	if _, err := e.Execute(context.Background(), i); err == nil {
		t.Fatal("multiple params sources were accepted")
	}
}

func TestUnownedCommandsAreExplicitlyNotSent(t *testing.T) {
	e := New(Ports{})
	for _, command := range []string{"send", "reply", "wait", "inspect", "receipt"} {
		_, err := e.Execute(context.Background(), invocation(command, "x"))
		var ce *cli.Error
		if !errors.As(err, &ce) || ce.Effect != "not_sent" || ce.Exit != cli.ExitInternal {
			t.Fatalf("%s err=%v", command, err)
		}
	}
}

func TestDomainErrorsMapToStableCLIErrors(t *testing.T) {
	e := New(Ports{Connections: failingConnections{err: errors.New("dial failed")}, Receipts: &fakeReceipts{}})
	_, err := e.Execute(context.Background(), invocation("search", "needle"))
	var ce *cli.Error
	if !errors.As(err, &ce) || ce.Code != "internal_error" || ce.Effect != "unknown" {
		t.Fatalf("err=%v", err)
	}
	if strings.Contains(err.Error(), "panic") {
		t.Fatal(err)
	}
}

func TestPinnedDomainErrorsKeepStableExitClasses(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code cli.ExitCode
		name string
	}{
		{codexapi.ErrExperimentalAPIRequired, cli.ExitRejected, "experimental"},
		{codexapi.ErrUnsupportedOption, cli.ExitUsage, "unsupported"},
		{codexapi.ErrUnboundedPage, cli.ExitUsage, "page"},
		{codexapi.ErrInProgressCutoff, cli.ExitUsage, "cutoff"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var ce *cli.Error
			if !errors.As(mapError(tc.err, "unknown"), &ce) || ce.Exit != tc.code {
				t.Fatalf("err=%v mapped=%+v", tc.err, ce)
			}
		})
	}
}

func TestPossibleWriteDeadlineMapsToOutcomeUnknown(t *testing.T) {
	err := &connection.CallError{Err: context.DeadlineExceeded, Evidence: appserver.WriteEvidence{Phase: appserver.WriteMayHaveWritten, Generation: 42}, Generation: 42}
	var ce *cli.Error
	if !errors.As(mapError(err, "unknown"), &ce) || ce.Code != "outcome_unknown" || ce.Exit != cli.ExitUnknown || ce.Details["generation"] != uint64(42) {
		t.Fatalf("mapped=%+v", ce)
	}
}

func TestStorageSentinelsMapToStableErrors(t *testing.T) {
	for _, tc := range []struct {
		err  error
		code string
		exit cli.ExitCode
	}{
		{journal.ErrStorageBusy, "storage_busy", cli.ExitUnknown},
		{journal.ErrStorageCorrupt, "storage_corrupt", cli.ExitRejected},
	} {
		var ce *cli.Error
		if !errors.As(mapError(tc.err, "not_sent"), &ce) || ce.Code != tc.code || ce.Exit != tc.exit {
			t.Fatalf("err=%v mapped=%+v", tc.err, ce)
		}
	}
}

func TestExperimentalInitializationIsRequestSpecific(t *testing.T) {
	connections := &optionsConnections{fakeConnections: &fakeConnections{conn: &fakeConnection{api: fakeCodex{scoped: true}}}}
	receipts := &fakeReceipts{}
	e := New(Ports{Connections: connections, Receipts: receipts, RPC: &fakeRPC{}, Input: fakeInput{stdin: []byte(`{}`)}})
	if _, err := e.Execute(context.Background(), invocation("thread", "list")); err != nil {
		t.Fatal(err)
	}
	search := invocation("search", "needle")
	if _, err := e.Execute(context.Background(), search); err != nil {
		t.Fatal(err)
	}
	fork := invocation("thread", "fork", "thr_1")
	fork.Options["before-turn"] = []string{"turn_1"}
	if _, err := e.Execute(context.Background(), fork); err != nil {
		t.Fatal(err)
	}
	rpc := invocation("rpc", "server/diagnostics")
	rpc.Options["params"] = []string{"{}"}
	if _, err := e.Execute(context.Background(), rpc); err != nil {
		t.Fatal(err)
	}
	if len(connections.options) != 4 || connections.options[0].ExperimentalAPI || !connections.options[1].ExperimentalAPI || !connections.options[2].ExperimentalAPI || !connections.options[3].ExperimentalAPI {
		t.Fatalf("request-specific options=%+v", connections.options)
	}
}

func TestExecutorRechecksPageAndCursorBounds(t *testing.T) {
	connections := &fakeConnections{conn: &fakeConnection{api: fakeCodex{scoped: true}}}
	e := New(Ports{Connections: connections, Receipts: &fakeReceipts{}})
	tooMany := invocation("search", "needle")
	tooMany.Options["limit"] = []string{"101"}
	if _, err := e.Execute(context.Background(), tooMany); err == nil {
		t.Fatal("executor accepted an unbounded page")
	}
	tooLong := invocation("thread", "list")
	tooLong.Options["cursor"] = []string{strings.Repeat("x", codexapi.MaxCursorBytes+1)}
	if _, err := e.Execute(context.Background(), tooLong); err == nil {
		t.Fatal("executor accepted an unbounded cursor")
	}
	if connections.opened != 0 {
		t.Fatalf("bounds were checked after opening a connection: %d", connections.opened)
	}
	scoped := invocation("search", "needle")
	scoped.Options["thread"] = []string{"thr_1"}
	scoped.Options["limit"] = []string{"250"}
	if _, err := e.Execute(context.Background(), scoped); err != nil {
		t.Fatalf("scoped search rejected its 250-result bound: %v", err)
	}
}

func TestResolverNotFoundEmitsLockedWireErrorCode(t *testing.T) {
	e := New(Ports{Connections: &fakeConnections{conn: &fakeConnection{api: fakeCodex{}}}, Targets: &fakeTargets{err: endpoint.ErrResolverNotFound}})
	var out, errOut bytes.Buffer
	a := &cli.App{Out: &out, Err: &errOut, Env: []string{"MEKTUP_AGENT=1"}, Executor: e}
	if code := a.Run([]string{"--json", "thread", "read", "herdr://local/agent/missing"}); code != int(cli.ExitRejected) {
		t.Fatalf("code=%d stderr=%q", code, errOut.String())
	}
	var event struct {
		Data struct {
			Error struct {
				Code mektup.ErrorCode `json:"code"`
			} `json:"error"`
		} `json:"data"`
	}
	if err := json.Unmarshal(out.Bytes(), &event); err != nil {
		t.Fatal(err)
	}
	if event.Data.Error.Code != mektup.ErrEndpointUnavailable || !event.Data.Error.Code.Valid() {
		t.Fatalf("code=%q", event.Data.Error.Code)
	}
}

type pinnedFactory struct {
	conn Connection
	err  error
}

func (f pinnedFactory) Open(context.Context, string) (Connection, error) { return f.conn, nil }
func (f pinnedFactory) Check(context.Context, string) (any, error)       { return nil, nil }
func (f pinnedFactory) OpenPinned(context.Context, endpoint.Endpoint, OpenOptions) (Connection, error) {
	return f.conn, f.err
}

func TestPinnedOpenNormalizesUnsupportedAndNilResults(t *testing.T) {
	for _, tc := range []struct {
		name    string
		factory pinnedFactory
	}{
		{"unsupported", pinnedFactory{err: connection.ErrUnsupported}},
		{"nil", pinnedFactory{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := New(Ports{Connections: tc.factory, Targets: &fakeTargets{endpoint: "ep_1", thread: "native"}, Receipts: &fakeReceipts{}})
			_, err := e.Execute(context.Background(), invocation("thread", "read", "codex://local/thread/native"))
			var ce *cli.Error
			if !errors.As(err, &ce) {
				t.Fatalf("err=%v", err)
			}
			if tc.name == "unsupported" && (ce.Code != "unsupported_server_version" || ce.Exit != cli.ExitRejected) {
				t.Fatalf("unsupported mapped=%+v", ce)
			}
			if tc.name == "nil" && ce.Code != "internal_error" {
				t.Fatalf("nil mapped=%+v", ce)
			}
		})
	}
}

type failingConnections struct{ err error }

func (f failingConnections) Open(context.Context, string) (Connection, error) { return nil, f.err }
func (f failingConnections) Check(context.Context, string) (any, error)       { return nil, f.err }
