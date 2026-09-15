// Package executor adapts the validated CLI invocation to the narrow domain
// ports. It deliberately does not construct a daemon, open files, or select a
// transport: the runtime factory owns those decisions.
package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/agensfield/mektup/go/internal/artifact"
	"github.com/agensfield/mektup/go/internal/cli"
	"github.com/agensfield/mektup/go/internal/codexapi"
	"github.com/agensfield/mektup/go/internal/doctor"
	"github.com/agensfield/mektup/go/internal/endpoint"
	"github.com/agensfield/mektup/go/internal/journal"
	"github.com/agensfield/mektup/go/internal/rawrpc"
	"github.com/agensfield/mektup/go/internal/rpcmeta"
)

const (
	maxInputBytes = 1 << 20
)

// Codex is the semantic app-server port. *codexapi.Client satisfies it, while
// tests can provide a fake without a transport or an ambient app-server.
type Codex interface {
	ThreadList(context.Context, codexapi.ThreadListOptions) (codexapi.ThreadListResponse, error)
	ThreadRead(context.Context, codexapi.ThreadReadOptions) (codexapi.ThreadReadResponse, error)
	ThreadTurns(context.Context, codexapi.TurnsOptions) (codexapi.ThreadTurnsResponse, error)
	ThreadItems(context.Context, codexapi.ItemsOptions) (codexapi.ThreadItemsResponse, error)
	ThreadStart(context.Context, codexapi.StartOptions) (codexapi.ThreadStartResponse, error)
	ThreadResume(context.Context, codexapi.ResumeOptions) (codexapi.ThreadResumeResponse, error)
	ThreadFork(context.Context, codexapi.ForkOptions) (codexapi.ThreadForkResponse, error)
	Search(context.Context, codexapi.SearchOptions) (codexapi.SearchResponse, error)
}

// ScopedSearcher is optional so older adapters can continue to expose only
// the cross-thread search method. A scoped invocation fails closed when the
// adapter has not implemented the pinned occurrences endpoint.
type ScopedSearcher interface {
	SearchOccurrences(context.Context, codexapi.SearchOccurrencesOptions) (codexapi.SearchOccurrencesResponse, error)
}

// ThreadNameSetter is the semantic adapter for the separate
// thread/name/set mutation. It is optional on Codex so older adapters cannot
// accidentally treat --name as a start/fork field.
type ThreadNameSetter interface {
	ThreadSetName(context.Context, string, string) (any, error)
}

// Connection is an already initialized, compatibility-gated endpoint. Check
// is intentionally separate from Open so endpoint check cannot start a daemon.
type Connection interface {
	Codex() Codex
	RPC() rawrpc.Caller
	Warnings() []string
	Close() error
}

type ConnectionFactory interface {
	Open(context.Context, string) (Connection, error)
	Check(context.Context, string) (any, error)
}

type EndpointPort interface {
	List() ([]endpoint.Endpoint, error)
	Show(string) (endpoint.Endpoint, error)
	Add(endpoint.Endpoint) error
	Remove(string) error
}

type EndpointChecker interface {
	Check(context.Context, endpoint.Endpoint) (any, error)
}

type StoragePort interface {
	Status(context.Context) (journal.StorageStatus, error)
	Check(context.Context) (journal.StorageCheck, error)
	Maintain(context.Context, journal.MaintenanceOptions) (journal.MaintenanceReceipt, error)
	Vacuum(context.Context) (journal.VacuumReceipt, error)
}

type DoctorPort interface {
	Run(context.Context, doctor.Options, bool) (doctor.Report, error)
}

// InputPort is the only input boundary used by the executor. Implementations
// must enforce their own path policy; the executor enforces the byte bound.
type InputPort interface {
	ReadFile(context.Context, string, int64) ([]byte, error)
	ReadStdin(context.Context, int64) ([]byte, error)
}

type ArtifactPort interface {
	RPCOutput(context.Context, cli.Invocation) (rawrpc.OutputOptions, error)
}

type RPCPort interface {
	Execute(context.Context, rawrpc.Caller, rawrpc.Request) (*rawrpc.Response, error)
}

// ReceiptPort gives a runtime journal a chance to persist a mutation receipt.
// Returning nil is allowed and causes the executor to emit a bounded local
// receipt containing no bodies or credentials.
type ReceiptPort interface {
	Mutation(context.Context, string, string, any) (any, error)
}

type Ports struct {
	Connections    ConnectionFactory
	Endpoints      EndpointPort
	EndpointHealth EndpointChecker
	Storage        StoragePort
	Doctor         DoctorPort
	Input          InputPort
	Artifacts      ArtifactPort
	RPC            RPCPort
	Receipts       ReceiptPort
	DoctorOptions  doctor.Options
}

// Executor is safe to construct with nil ports. A missing port is reported as
// an internal, not-sent error and never replaced with ambient state.
type Executor struct{ ports Ports }

func New(ports Ports) *Executor { return &Executor{ports: ports} }

var _ cli.Executor = (*Executor)(nil)

func (e *Executor) Execute(ctx context.Context, inv cli.Invocation) (cli.ExecutionResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if inv.Command == "send" || inv.Command == "reply" || inv.Command == "wait" || inv.Command == "inspect" || inv.Command == "receipt" {
		return cli.ExecutionResult{}, notOwned(inv.Command)
	}
	if inv.Command == "thread" {
		return e.thread(ctx, inv)
	}
	if inv.Command == "search" {
		return e.search(ctx, inv)
	}
	if inv.Command == "endpoint" {
		return e.endpoint(ctx, inv)
	}
	if inv.Command == "storage" {
		return e.storage(ctx, inv)
	}
	if inv.Command == "doctor" {
		return e.doctor(ctx, inv)
	}
	if inv.Command == "rpc" {
		return e.rpc(ctx, inv)
	}
	return cli.ExecutionResult{}, &cli.Error{Code: "invalid_arguments", Message: "unsupported operational command: " + inv.Command, Effect: "not_sent", Exit: cli.ExitUsage}
}

func notOwned(command string) error {
	return &cli.Error{Code: "internal_error", Message: command + " is owned by the messaging runtime", Effect: "not_sent", Exit: cli.ExitInternal, Details: map[string]any{"command": command}}
}

func (e *Executor) open(ctx context.Context, selector string) (Connection, Codex, error) {
	if e.ports.Connections == nil {
		return nil, nil, missing("connection")
	}
	c, err := e.ports.Connections.Open(ctx, selector)
	if err != nil {
		return nil, nil, mapError(err, "unknown")
	}
	if c == nil || c.Codex() == nil {
		if c != nil {
			_ = c.Close()
		}
		return nil, nil, missing("codex")
	}
	return c, c.Codex(), nil
}

func (e *Executor) thread(ctx context.Context, inv cli.Invocation) (cli.ExecutionResult, error) {
	if len(inv.Position) == 0 {
		return cli.ExecutionResult{}, usage("missing thread subcommand")
	}
	if err := validatePageOptions(inv); err != nil {
		return cli.ExecutionResult{}, err
	}
	mutating := inv.Position[0] == "start" || inv.Position[0] == "resume" || inv.Position[0] == "fork"
	if mutating && e.ports.Receipts == nil {
		return cli.ExecutionResult{}, missing("receipt journal")
	}
	conn, api, err := e.open(ctx, inv.Resolved.Endpoint)
	if err != nil {
		return cli.ExecutionResult{}, err
	}
	defer conn.Close()
	sub := inv.Position[0]
	var data any
	var kind string
	var cursor string
	var warnings = append([]string(nil), conn.Warnings()...)
	switch sub {
	case "list":
		r, callErr := api.ThreadList(ctx, codexapi.ThreadListOptions{Cursor: inv.Option("cursor"), Limit: optionInt(inv, "limit"), SortKey: inv.Option("sort"), SortDirection: inv.Option("order"), SourceKinds: options(inv, "source"), Loaded: has(inv, "loaded"), CWD: optionOrNil(inv, "cwd"), Archived: boolOption(inv, "archived")})
		if callErr != nil {
			return cli.ExecutionResult{}, mapError(callErr, "unknown")
		}
		data, cursor = pageData(r.Raw, r, r.NextCursor)
		kind = "thread.list"
	case "read":
		if len(inv.Position) < 2 {
			return cli.ExecutionResult{}, usage("thread read requires a thread identifier")
		}
		r, callErr := api.ThreadRead(ctx, codexapi.ThreadReadOptions{ThreadID: inv.Position[1]})
		if callErr != nil {
			return cli.ExecutionResult{}, mapError(callErr, "unknown")
		}
		data, kind = rawOr(r.Raw, r), "thread.read"
	case "turns":
		if len(inv.Position) < 2 {
			return cli.ExecutionResult{}, usage("thread turns requires a thread identifier")
		}
		r, callErr := api.ThreadTurns(ctx, codexapi.TurnsOptions{ThreadID: inv.Position[1], Cursor: inv.Option("cursor"), Limit: optionInt(inv, "limit"), SortDirection: inv.Option("order"), ItemsView: inv.Option("view")})
		if callErr != nil {
			return cli.ExecutionResult{}, mapError(callErr, "unknown")
		}
		data, cursor = pageData(r.Raw, r, r.NextCursor)
		kind = "thread.turns"
	case "items":
		if len(inv.Position) < 2 {
			return cli.ExecutionResult{}, usage("thread items requires a thread identifier")
		}
		r, callErr := api.ThreadItems(ctx, codexapi.ItemsOptions{ThreadID: inv.Position[1], TurnID: inv.Option("turn"), Cursor: inv.Option("cursor"), Limit: optionInt(inv, "limit"), SortDirection: inv.Option("order")})
		if callErr != nil {
			return cli.ExecutionResult{}, mapError(callErr, "unknown")
		}
		data, cursor = pageData(r.Raw, r, r.NextCursor)
		kind = "thread.items"
	case "start":
		r, callErr := api.ThreadStart(ctx, codexapi.StartOptions{Model: inv.Option("model"), CWD: inv.Option("cwd"), ThreadSource: inv.Option("source")})
		if callErr != nil {
			return cli.ExecutionResult{}, mapError(callErr, "unknown")
		}
		if name := inv.Option("name"); name != "" {
			if callErr := e.setThreadName(ctx, api, name, r.Thread.ID, r.Raw, "thread.start", inv.Resolved.Endpoint); callErr != nil {
				return cli.ExecutionResult{}, callErr
			}
		}
		data, kind = rawOr(r.Raw, r), "thread.start"
	case "resume":
		if len(inv.Position) < 2 {
			return cli.ExecutionResult{}, usage("thread resume requires a thread identifier")
		}
		r, callErr := api.ThreadResume(ctx, codexapi.ResumeOptions{ThreadID: inv.Position[1]})
		if callErr != nil {
			return cli.ExecutionResult{}, mapError(callErr, "unknown")
		}
		data, kind = rawOr(r.Raw, r), "thread.resume"
	case "fork":
		if len(inv.Position) < 2 {
			return cli.ExecutionResult{}, usage("thread fork requires a thread identifier")
		}
		r, callErr := api.ThreadFork(ctx, codexapi.ForkOptions{ThreadID: inv.Position[1], ThroughTurnID: inv.Option("through-turn"), BeforeTurnID: inv.Option("before-turn"), ExactRead: nil})
		if callErr != nil {
			return cli.ExecutionResult{}, mapError(callErr, "unknown")
		}
		if name := inv.Option("name"); name != "" {
			if callErr := e.setThreadName(ctx, api, name, r.Thread.ID, r.Raw, "thread.fork", inv.Resolved.Endpoint); callErr != nil {
				return cli.ExecutionResult{}, callErr
			}
		}
		data, kind = rawOr(r.Raw, r), "thread.fork"
	default:
		return cli.ExecutionResult{}, usage("unsupported thread subcommand: " + sub)
	}
	return e.result(ctx, kind, data, cursor, warnings, mutating, inv.Resolved.Endpoint)
}

func (e *Executor) search(ctx context.Context, inv cli.Invocation) (cli.ExecutionResult, error) {
	if len(inv.Position) < 1 || strings.TrimSpace(inv.Position[0]) == "" {
		return cli.ExecutionResult{}, usage("search requires a query")
	}
	if err := validatePageOptions(inv); err != nil {
		return cli.ExecutionResult{}, err
	}
	conn, api, err := e.open(ctx, inv.Resolved.Endpoint)
	if err != nil {
		return cli.ExecutionResult{}, err
	}
	defer conn.Close()
	options := codexapi.SearchOptions{SearchTerm: inv.Position[0], Cursor: inv.Option("cursor"), Limit: optionInt(inv, "limit"), SourceKinds: options(inv, "source"), Archived: boolOption(inv, "archived")}
	if thread := inv.Option("thread"); thread != "" {
		scoped, ok := api.(ScopedSearcher)
		if !ok {
			return cli.ExecutionResult{}, &cli.Error{Code: "experimental_method_unavailable", Message: "scoped search is not available on this connection", Effect: "not_sent", Exit: cli.ExitRejected}
		}
		r, callErr := scoped.SearchOccurrences(ctx, codexapi.SearchOccurrencesOptions{ThreadID: thread, SearchTerm: inv.Position[0], Cursor: inv.Option("cursor"), Limit: optionInt(inv, "limit")})
		if callErr != nil {
			return cli.ExecutionResult{}, mapError(callErr, "unknown")
		}
		data, cursor := pageData(r.Raw, r, r.NextCursor)
		warnings := append([]string{"experimental API capability used", "scoped search"}, conn.Warnings()...)
		return e.result(ctx, "search.scoped", data, cursor, warnings, false, inv.Resolved.Endpoint)
	}
	r, callErr := api.Search(ctx, options)
	if callErr != nil {
		return cli.ExecutionResult{}, mapError(callErr, "unknown")
	}
	data, cursor := pageData(r.Raw, r, r.NextCursor)
	warnings := append([]string{"experimental API capability used"}, conn.Warnings()...)
	return e.result(ctx, "search", data, cursor, warnings, false, inv.Resolved.Endpoint)
}

func (e *Executor) endpoint(ctx context.Context, inv cli.Invocation) (cli.ExecutionResult, error) {
	if len(inv.Position) < 1 {
		return cli.ExecutionResult{}, usage("missing endpoint subcommand")
	}
	if e.ports.Endpoints == nil {
		return cli.ExecutionResult{}, missing("endpoint store")
	}
	sub := inv.Position[0]
	if (sub == "add" || sub == "remove") && e.ports.Receipts == nil {
		return cli.ExecutionResult{}, missing("receipt journal")
	}
	switch sub {
	case "list":
		items, err := e.ports.Endpoints.List()
		if err != nil {
			return cli.ExecutionResult{}, mapError(err, "not_sent")
		}
		return e.result(ctx, "endpoint.list", items, "", nil, false, inv.Resolved.Endpoint)
	case "show":
		if len(inv.Position) < 2 {
			return cli.ExecutionResult{}, usage("endpoint show requires a selector")
		}
		item, err := e.ports.Endpoints.Show(inv.Position[1])
		if err != nil {
			return cli.ExecutionResult{}, mapError(err, "not_sent")
		}
		return e.result(ctx, "endpoint.show", item, "", nil, false, inv.Resolved.Endpoint)
	case "add":
		if len(inv.Position) < 2 || strings.TrimSpace(inv.Position[1]) == "" {
			return cli.ExecutionResult{}, usage("endpoint add requires an alias")
		}
		var route endpoint.Route
		var err error
		if value := inv.Option("ssh"); value != "" {
			route, err = endpoint.SSHRoute(value)
		} else {
			route, err = endpoint.UnixRoute(inv.Option("unix"))
		}
		if err != nil {
			return cli.ExecutionResult{}, mapError(err, "not_sent")
		}
		item := endpoint.Endpoint{ID: inv.Option("id"), Alias: inv.Position[1], Route: route, Herdr: endpoint.HerdrMode(inv.Option("herdr"))}.Normalized()
		if item.ID == "" {
			item.ID, err = endpoint.NewEndpointID()
			if err != nil {
				return cli.ExecutionResult{}, mapError(err, "not_sent")
			}
		}
		if err := e.ports.Endpoints.Add(item); err != nil {
			return cli.ExecutionResult{}, mapError(err, "not_sent")
		}
		return e.result(ctx, "endpoint.add", item, "", nil, true, inv.Resolved.Endpoint)
	case "remove":
		if len(inv.Position) < 2 {
			return cli.ExecutionResult{}, usage("endpoint remove requires a selector")
		}
		if err := e.ports.Endpoints.Remove(inv.Position[1]); err != nil {
			return cli.ExecutionResult{}, mapError(err, "not_sent")
		}
		return e.result(ctx, "endpoint.remove", map[string]any{"selector": inv.Position[1], "removed": true}, "", nil, true, inv.Resolved.Endpoint)
	case "check":
		selector := inv.Resolved.Endpoint
		if len(inv.Position) == 2 {
			selector = inv.Position[1]
		}
		if e.ports.Connections == nil {
			return cli.ExecutionResult{}, missing("connection")
		}
		// Check is a factory-level initialize-only operation. It must not call
		// Open, because Open is allowed to create/start a process in adapters.
		v, err := e.ports.Connections.Check(ctx, selector)
		if err != nil {
			return cli.ExecutionResult{}, mapError(err, "not_sent")
		}
		return e.result(ctx, "endpoint.check", v, "", nil, false, selector)
	default:
		return cli.ExecutionResult{}, usage("unsupported endpoint subcommand: " + sub)
	}
}

func (e *Executor) storage(ctx context.Context, inv cli.Invocation) (cli.ExecutionResult, error) {
	if len(inv.Position) < 1 {
		return cli.ExecutionResult{}, usage("missing storage subcommand")
	}
	if e.ports.Storage == nil {
		return cli.ExecutionResult{}, missing("storage")
	}
	sub := inv.Position[0]
	if (sub == "maintain" || sub == "vacuum") && e.ports.Receipts == nil {
		return cli.ExecutionResult{}, missing("receipt journal")
	}
	switch sub {
	case "status":
		v, err := e.ports.Storage.Status(ctx)
		if err != nil {
			return cli.ExecutionResult{}, mapError(err, "not_sent")
		}
		return e.result(ctx, "storage.status", v, "", nil, false, "local")
	case "check":
		v, err := e.ports.Storage.Check(ctx)
		if err != nil {
			return cli.ExecutionResult{}, mapError(err, "not_sent")
		}
		return e.result(ctx, "storage.check", v, "", nil, false, "local")
	case "maintain":
		before, err := parseTime(inv.Option("before"))
		if err != nil {
			return cli.ExecutionResult{}, usage(err.Error())
		}
		v, err := e.ports.Storage.Maintain(ctx, journal.MaintenanceOptions{Before: before, DryRun: has(inv, "dry-run")})
		if err != nil {
			return cli.ExecutionResult{}, mapError(err, "unknown")
		}
		return e.result(ctx, "storage.maintain", v, "", nil, true, "local")
	case "vacuum":
		v, err := e.ports.Storage.Vacuum(ctx)
		if err != nil {
			return cli.ExecutionResult{}, mapError(err, "unknown")
		}
		return e.result(ctx, "storage.vacuum", v, "", nil, true, "local")
	default:
		return cli.ExecutionResult{}, usage("unsupported storage subcommand: " + sub)
	}
}

func (e *Executor) doctor(ctx context.Context, inv cli.Invocation) (cli.ExecutionResult, error) {
	if e.ports.Doctor == nil {
		return cli.ExecutionResult{}, missing("doctor")
	}
	fix := has(inv, "fix")
	if fix && e.ports.Receipts == nil {
		return cli.ExecutionResult{}, missing("receipt journal")
	}
	r, err := e.ports.Doctor.Run(ctx, e.ports.DoctorOptions, fix)
	if err != nil {
		return cli.ExecutionResult{}, mapError(err, "unknown")
	}
	return e.result(ctx, "doctor", r, "", nil, fix, "local")
}

func (e *Executor) rpc(ctx context.Context, inv cli.Invocation) (cli.ExecutionResult, error) {
	if len(inv.Position) < 1 || strings.TrimSpace(inv.Position[0]) == "" {
		return cli.ExecutionResult{}, usage("rpc requires a method")
	}
	if e.ports.Connections == nil || e.ports.RPC == nil {
		return cli.ExecutionResult{}, missing("rpc runtime")
	}
	if e.ports.Receipts == nil {
		return cli.ExecutionResult{}, missing("receipt journal")
	}
	params, source, err := e.params(ctx, inv)
	if err != nil {
		return cli.ExecutionResult{}, err
	}
	conn, _, err := e.open(ctx, inv.Resolved.Endpoint)
	if err != nil {
		return cli.ExecutionResult{}, err
	}
	defer conn.Close()
	var output rawrpc.OutputOptions
	if e.ports.Artifacts != nil {
		output, err = e.ports.Artifacts.RPCOutput(ctx, inv)
		if err != nil {
			return cli.ExecutionResult{}, mapError(err, "not_sent")
		}
	}
	grants := make([]rpcmeta.EffectClass, 0, len(inv.Options["allow-effect"]))
	for _, value := range inv.Options["allow-effect"] {
		grants = append(grants, rpcmeta.EffectClass(value))
	}
	r, callErr := e.ports.RPC.Execute(ctx, conn.RPC(), rawrpc.Request{Method: inv.Position[0], Params: params, ParamsSource: source, Grants: grants, Output: output})
	if callErr != nil {
		return cli.ExecutionResult{}, mapError(callErr, "not_sent")
	}
	if r == nil {
		return cli.ExecutionResult{}, &cli.Error{Code: "internal_error", Message: "rpc returned no response", Effect: "unknown", Exit: cli.ExitInternal}
	}
	data := map[string]any{"method": r.Method, "id": r.ID, "effects": r.Effects, "experimental": r.Experimental, "networkRead": r.NetworkRead, "retrySafety": r.RetrySafety, "effectState": r.EffectState}
	if len(r.Raw) != 0 {
		data["result"] = json.RawMessage(r.Raw)
	}
	if r.Artifact != nil {
		data["artifact"] = r.Artifact
	}
	return e.result(ctx, "rpc", data, "", nil, true, inv.Resolved.Endpoint)
}

func (e *Executor) params(ctx context.Context, inv cli.Invocation) (json.RawMessage, rawrpc.ParamsSource, error) {
	count := 0
	for _, name := range []string{"params", "params-file", "stdin"} {
		count += len(inv.Options[name])
	}
	if count > 1 {
		return nil, "", usage("rpc accepts exactly one params source")
	}
	if has(inv, "params") {
		value := inv.Option("params")
		if !json.Valid([]byte(value)) {
			return nil, "", usage("--params must contain valid JSON")
		}
		if len(value) > maxInputBytes {
			return nil, "", inputTooLarge()
		}
		return json.RawMessage(value), rawrpc.ParamsInline, nil
	}
	if has(inv, "params-file") {
		if e.ports.Input == nil {
			return nil, "", missing("input")
		}
		b, err := e.ports.Input.ReadFile(ctx, inv.Option("params-file"), maxInputBytes)
		if err != nil {
			return nil, "", mapError(err, "not_sent")
		}
		if len(b) > maxInputBytes {
			return nil, "", inputTooLarge()
		}
		if !json.Valid(b) {
			return nil, "", usage("params file must contain valid JSON")
		}
		return json.RawMessage(b), rawrpc.ParamsFile, nil
	}
	if has(inv, "stdin") {
		if e.ports.Input == nil {
			return nil, "", missing("input")
		}
		b, err := e.ports.Input.ReadStdin(ctx, maxInputBytes)
		if err != nil {
			return nil, "", mapError(err, "not_sent")
		}
		if len(b) > maxInputBytes {
			return nil, "", inputTooLarge()
		}
		if !json.Valid(b) {
			return nil, "", usage("stdin params must contain valid JSON")
		}
		return json.RawMessage(b), rawrpc.ParamsStdin, nil
	}
	return json.RawMessage("{}"), rawrpc.ParamsOmitted, nil
}

func (e *Executor) result(ctx context.Context, kind string, data any, cursor string, warnings []string, mutation bool, endpointID string) (cli.ExecutionResult, error) {
	payload := map[string]any{"resultKind": kind, "result": data}
	if cursor != "" {
		payload["nextCursor"] = cursor
	}
	eventMachine := map[string]any{"event": kind + ".completed", "terminal": true, "ok": true, "data": payload}
	if len(warnings) != 0 {
		// warnings is a lifecycle envelope field. Keeping it out of data is
		// important because App preserves this top-level location verbatim.
		eventMachine["warnings"] = warningObjects(warnings)
	}
	event := cli.OutputEvent{Machine: eventMachine, Human: kind}
	result := cli.ExecutionResult{Events: []cli.OutputEvent{event}, Exit: cli.ExitSuccess}
	if mutation {
		if e.ports.Receipts == nil {
			return cli.ExecutionResult{}, missing("receipt journal")
		}
		receipt, err := e.ports.Receipts.Mutation(ctx, kind, endpointID, data)
		if err != nil {
			return cli.ExecutionResult{}, mapError(err, "unknown")
		}
		if receipt == nil {
			return cli.ExecutionResult{}, &cli.Error{Code: "internal_error", Message: "receipt journal returned no receipt", Effect: "unknown", Exit: cli.ExitInternal}
		}
		result.Receipt = receipt
	}
	return result, nil
}

func warningObjects(warnings []string) []map[string]any {
	objects := make([]map[string]any, 0, len(warnings))
	for _, warning := range warnings {
		code := "compatibility_warning"
		if strings.Contains(strings.ToLower(warning), "experimental") {
			code = "experimental_api"
		}
		objects = append(objects, map[string]any{"code": code, "message": warning})
	}
	return objects
}

func (e *Executor) setThreadName(ctx context.Context, api Codex, name, threadID string, raw json.RawMessage, operation, endpointID string) error {
	if threadID == "" {
		var envelope struct {
			Thread struct {
				ID string `json:"id"`
			} `json:"thread"`
		}
		_ = json.Unmarshal(raw, &envelope)
		threadID = envelope.Thread.ID
	}
	setter, ok := api.(ThreadNameSetter)
	if !ok {
		return e.partialNameFailure(ctx, operation, endpointID, threadID, name, errors.New("thread/name/set adapter is not configured"))
	}
	if _, err := setter.ThreadSetName(ctx, threadID, name); err != nil {
		return e.partialNameFailure(ctx, operation, endpointID, threadID, name, err)
	}
	return nil
}

func (e *Executor) partialNameFailure(ctx context.Context, operation, endpointID, threadID, name string, cause error) error {
	details := map[string]any{"partialEffect": true, "threadId": threadID, "name": name, "cause": cause.Error()}
	details["creationState"] = "accepted"
	details["nameEffect"] = "unknown"
	if e.ports.Receipts != nil {
		receipt, err := e.ports.Receipts.Mutation(ctx, operation, endpointID, details)
		if err != nil {
			details["receiptError"] = err.Error()
		} else if receipt != nil {
			details["receipt"] = receipt
		}
	}
	return &cli.Error{Code: "internal_error", Message: "thread was created but naming failed", Effect: "accepted", Details: details, Exit: cli.ExitInternal}
}

func rawOr(raw json.RawMessage, value any) any {
	if len(bytes.TrimSpace(raw)) != 0 {
		return json.RawMessage(raw)
	}
	return value
}

func pageData(raw json.RawMessage, value any, cursor string) (any, string) {
	return rawOr(raw, value), cursor
}

func optionInt(inv cli.Invocation, name string) int {
	var n int
	_, _ = fmt.Sscan(inv.Option(name), &n)
	return n
}

func validatePageOptions(inv cli.Invocation) error {
	if cursor := inv.Option("cursor"); len(cursor) > codexapi.MaxCursorBytes {
		return usage("--cursor exceeds the bounded cursor size")
	}
	if value := inv.Option("limit"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 0 || n > codexapi.MaxThreadPageLimit {
			return usage("--limit must be between 0 and 100")
		}
	}
	return nil
}
func options(inv cli.Invocation, name string) []string {
	return append([]string(nil), inv.Options[name]...)
}
func boolOption(inv cli.Invocation, name string) *bool {
	if !has(inv, name) {
		return nil
	}
	value := true
	return &value
}
func optionOrNil(inv cli.Invocation, name string) any {
	if v := inv.Option(name); v != "" {
		return v
	}
	return nil
}
func has(inv cli.Invocation, name string) bool { return len(inv.Options[name]) != 0 }

func parseTime(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("--before requires RFC3339 time: %w", err)
	}
	return t, nil
}

func usage(message string) error {
	return &cli.Error{Code: "invalid_arguments", Message: message, Effect: "not_sent", Exit: cli.ExitUsage}
}
func missing(name string) error {
	return &cli.Error{Code: "internal_error", Message: name + " port is not configured", Effect: "not_sent", Exit: cli.ExitInternal}
}
func inputTooLarge() error {
	return &cli.Error{Code: "input_too_large", Message: "input exceeds the 1 MiB bound", Effect: "not_sent", Exit: cli.ExitRejected}
}

func mapError(err error, effect string) error {
	if err == nil {
		return nil
	}
	var ce *cli.Error
	if errors.As(err, &ce) {
		if ce.Effect == "" {
			ce.Effect = effect
		}
		return ce
	}
	var re *rawrpc.Error
	if errors.As(err, &re) {
		stable := re.Stable()
		return &cli.Error{Code: string(stable.Code), Message: stable.Message, Retryable: stable.Retryable, Effect: nonempty(stable.EffectState, effect), Details: stable.Details, Exit: cli.ExitCodeForError(string(stable.Code))}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return &cli.Error{Code: "wait_interrupted", Message: err.Error(), Effect: effect, Exit: cli.ExitIncomplete}
	}
	return &cli.Error{Code: "internal_error", Message: err.Error(), Effect: effect, Details: map[string]any{"cause": err.Error()}, Exit: cli.ExitInternal}
}

func nonempty(a, b string) string {
	if strings.TrimSpace(a) != "" {
		return a
	}
	return b
}

// ReaderInput is a bounded, test-friendly InputPort implementation.
type ReaderInput struct {
	In           io.Reader
	ReadFileFunc func(context.Context, string, int64) ([]byte, error)
}

func (r ReaderInput) ReadFile(ctx context.Context, name string, max int64) ([]byte, error) {
	if r.ReadFileFunc != nil {
		return r.ReadFileFunc(ctx, name, max)
	}
	return nil, errors.New("file input is not configured")
}
func (r ReaderInput) ReadStdin(ctx context.Context, max int64) ([]byte, error) {
	if r.In == nil {
		return nil, errors.New("stdin input is not configured")
	}
	return io.ReadAll(io.LimitReader(r.In, max+1))
}

// ArtifactStoreOutput adapts the concrete private artifact store without
// making the executor construct or locate it.
type ArtifactStoreOutput struct{ Store *artifact.Store }

func (a ArtifactStoreOutput) RPCOutput(context.Context, cli.Invocation) (rawrpc.OutputOptions, error) {
	return rawrpc.OutputOptions{Store: a.Store}, nil
}
