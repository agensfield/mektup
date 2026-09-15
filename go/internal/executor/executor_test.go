package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	mektup "github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/internal/cli"
	"github.com/agensfield/mektup/go/internal/codexapi"
	"github.com/agensfield/mektup/go/internal/doctor"
	"github.com/agensfield/mektup/go/internal/endpoint"
	"github.com/agensfield/mektup/go/internal/journal"
	"github.com/agensfield/mektup/go/internal/rawrpc"
	"github.com/agensfield/mektup/go/internal/rpcmeta"
)

type fakeCodex struct {
	scoped    bool
	allowName bool
}

func (f fakeCodex) ThreadList(context.Context, codexapi.ThreadListOptions) (codexapi.ThreadListResponse, error) {
	return codexapi.ThreadListResponse{Raw: json.RawMessage(`{"data":[],"nextCursor":"next"}`), NextCursor: "next"}, nil
}
func (f fakeCodex) ThreadRead(context.Context, codexapi.ThreadReadOptions) (codexapi.ThreadReadResponse, error) {
	return codexapi.ThreadReadResponse{Raw: json.RawMessage(`{"thread":{"id":"thr_1"}}`)}, nil
}
func (f fakeCodex) ThreadTurns(context.Context, codexapi.TurnsOptions) (codexapi.ThreadTurnsResponse, error) {
	return codexapi.ThreadTurnsResponse{Raw: json.RawMessage(`{"data":[],"nextCursor":"turns"}`), NextCursor: "turns"}, nil
}
func (f fakeCodex) ThreadItems(context.Context, codexapi.ItemsOptions) (codexapi.ThreadItemsResponse, error) {
	return codexapi.ThreadItemsResponse{Raw: json.RawMessage(`{"data":[],"nextCursor":"items"}`), NextCursor: "items"}, nil
}
func (f fakeCodex) ThreadStart(context.Context, codexapi.StartOptions) (codexapi.ThreadStartResponse, error) {
	return codexapi.ThreadStartResponse{Thread: codexapi.Thread{ID: "thr_new"}, Raw: json.RawMessage(`{"thread":{"id":"thr_new"}}`)}, nil
}
func (f fakeCodex) ThreadResume(context.Context, codexapi.ResumeOptions) (codexapi.ThreadResumeResponse, error) {
	return codexapi.ThreadResumeResponse{Raw: json.RawMessage(`{"thread":{"id":"thr_resume"}}`)}, nil
}
func (f fakeCodex) ThreadFork(context.Context, codexapi.ForkOptions) (codexapi.ThreadForkResponse, error) {
	return codexapi.ThreadForkResponse{Thread: codexapi.Thread{ID: "thr_fork"}, Raw: json.RawMessage(`{"thread":{"id":"thr_fork"}}`)}, nil
}

func (f fakeCodex) ThreadSetName(context.Context, string, string) (any, error) {
	if !f.allowName {
		return nil, errors.New("thread/name/set failed")
	}
	return map[string]any{"ok": true}, nil
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

type fakeConnection struct{ api Codex }

func (f *fakeConnection) Codex() Codex       { return f.api }
func (f *fakeConnection) RPC() rawrpc.Caller { return nil }
func (f *fakeConnection) Warnings() []string { return []string{"compat warning"} }
func (f *fakeConnection) Close() error       { return nil }

type fakeConnections struct {
	opened  int
	checked int
	conn    *fakeConnection
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

type fakeReceipts struct{ count int }

func (f *fakeReceipts) Mutation(context.Context, string, string, any) (any, error) {
	f.count++
	return map[string]any{"schema": "mektup/receipt/v1", "receiptId": "rcpt_test", "state": "accepted"}, nil
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
	if receipts.count != 8 {
		t.Fatalf("mutation receipt count=%d, want 8", receipts.count)
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

func TestLifecycleWarningsRemainAtEnvelopeLevel(t *testing.T) {
	var out, errOut bytes.Buffer
	e := New(Ports{Connections: &fakeConnections{conn: &fakeConnection{api: fakeCodex{}}}})
	a := &cli.App{In: strings.NewReader(""), Out: &out, Err: &errOut, Env: []string{"MEKTUP_AGENT=1"}, Executor: e}
	if code := a.Run([]string{"--json", "search", "needle"}); code != int(cli.ExitSuccess) {
		t.Fatalf("code=%d stderr=%q", code, errOut.String())
	}
	var event map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(out.Bytes()), &event); err != nil {
		t.Fatal(err)
	}
	warnings, ok := event["warnings"].([]any)
	if !ok || len(warnings) != 2 {
		t.Fatalf("warnings=%#v event=%#v", event["warnings"], event)
	}
	first, _ := warnings[0].(map[string]any)
	second, _ := warnings[1].(map[string]any)
	if first["code"] != "evidence_gap" || second["code"] != "untested_server_version" || first["details"] == nil || second["details"] == nil {
		t.Fatalf("warning objects=%#v", warnings)
	}
	data, _ := event["data"].(map[string]any)
	if _, nested := data["warnings"]; nested {
		t.Fatalf("warning was nested under data: %#v", data)
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
	e := New(Ports{Connections: failingConnections{err: errors.New("dial failed")}})
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

func TestExecutorRechecksPageAndCursorBounds(t *testing.T) {
	connections := &fakeConnections{conn: &fakeConnection{api: fakeCodex{}}}
	e := New(Ports{Connections: connections})
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
}

type failingConnections struct{ err error }

func (f failingConnections) Open(context.Context, string) (Connection, error) { return nil, f.err }
func (f failingConnections) Check(context.Context, string) (any, error)       { return nil, f.err }
