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

	mektup "github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/appserver"
	"github.com/agensfield/mektup/go/internal/artifact"
	"github.com/agensfield/mektup/go/internal/cli"
	"github.com/agensfield/mektup/go/internal/codexapi"
	"github.com/agensfield/mektup/go/internal/connection"
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
	ThreadSetName(context.Context, string, string) (codexapi.ThreadNameResponse, error)
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

// OpenOptions carries request-specific initialize requirements. The legacy
// Open method remains supported for adapters that negotiate a fixed baseline;
// capability-aware runtimes should implement ExperimentalConnectionFactory.
type OpenOptions struct{ ExperimentalAPI bool }

type ExperimentalConnectionFactory interface {
	OpenWithOptions(context.Context, string, OpenOptions) (Connection, error)
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

type ThreadTarget struct {
	Endpoint   string
	EndpointID string
	ThreadID   string
	URI        string
	Resolved   endpoint.Endpoint
}

// ThreadTargetResolver converts presentation selectors to a pinned endpoint
// identity and native thread ID before any Codex adapter call.
type ThreadTargetResolver interface {
	ResolveThread(context.Context, string, string) (ThreadTarget, error)
}

type ExplicitThreadTargetResolver interface {
	ResolveThreadWithOptions(context.Context, string, string, bool) (ThreadTarget, error)
}

type PinnedConnectionFactory interface {
	OpenPinned(context.Context, endpoint.Endpoint, OpenOptions) (Connection, error)
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

type ReadReceiptPort interface {
	Read(context.Context, string, string, any) (any, error)
}

type Ports struct {
	Connections    ConnectionFactory
	Endpoints      EndpointPort
	EndpointHealth EndpointChecker
	Targets        ThreadTargetResolver
	Storage        StoragePort
	Doctor         DoctorPort
	Input          InputPort
	Artifacts      ArtifactPort
	RPC            RPCPort
	Receipts       ReceiptPort
	ReadReceipts   ReadReceiptPort
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
	return e.openWithOptions(ctx, selector, OpenOptions{})
}

func (e *Executor) openWithOptions(ctx context.Context, selector string, options OpenOptions) (Connection, Codex, error) {
	if e.ports.Connections == nil {
		return nil, nil, missing("connection")
	}
	var c Connection
	var err error
	if factory, ok := e.ports.Connections.(ExperimentalConnectionFactory); ok {
		c, err = factory.OpenWithOptions(ctx, selector, options)
	} else {
		c, err = e.ports.Connections.Open(ctx, selector)
	}
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

func (e *Executor) thread(ctx context.Context, inv cli.Invocation) (result cli.ExecutionResult, execErr error) {
	if len(inv.Position) == 0 {
		return cli.ExecutionResult{}, usage("missing thread subcommand")
	}
	if err := validatePageOptions(inv, codexapi.MaxThreadPageLimit); err != nil {
		return cli.ExecutionResult{}, err
	}
	mutating := inv.Position[0] == "start" || inv.Position[0] == "resume" || inv.Position[0] == "fork"
	if has(inv, "name") && strings.TrimSpace(inv.Option("name")) == "" {
		return cli.ExecutionResult{}, usage("--name requires a non-empty value")
	}
	if mutating && e.ports.Receipts == nil {
		return cli.ExecutionResult{}, missing("receipt journal")
	}
	needsExperimental := inv.Position[0] == "fork" && inv.Option("before-turn") != ""
	endpointSelector, endpointID, threadID, resolvedEndpoint, err := e.resolveThreadTarget(ctx, inv)
	if err != nil {
		return cli.ExecutionResult{}, err
	}
	var conn Connection
	var api Codex
	if resolvedEndpoint.ID != "" {
		if pinned, ok := e.ports.Connections.(PinnedConnectionFactory); ok {
			conn, err = pinned.OpenPinned(ctx, resolvedEndpoint, OpenOptions{ExperimentalAPI: needsExperimental})
			if err != nil {
				return cli.ExecutionResult{}, mapError(err, "unknown")
			}
			if conn == nil {
				return cli.ExecutionResult{}, missing("connection")
			}
			api = conn.Codex()
			if api == nil {
				_ = conn.Close()
				return cli.ExecutionResult{}, missing("codex")
			}
		} else {
			conn, api, err = e.openWithOptions(ctx, endpointSelector, OpenOptions{ExperimentalAPI: needsExperimental})
		}
	} else {
		conn, api, err = e.openWithOptions(ctx, endpointSelector, OpenOptions{ExperimentalAPI: needsExperimental})
	}
	if err == nil && (conn == nil || api == nil) {
		if conn != nil {
			_ = conn.Close()
		}
		return cli.ExecutionResult{}, missing("codex")
	}
	if err != nil {
		return cli.ExecutionResult{}, err
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			if execErr == nil {
				appendCleanupWarning(&result, closeErr)
			} else if existing, ok := execErr.(*cli.Error); ok {
				if existing.Details == nil {
					existing.Details = map[string]any{}
				}
				existing.Details["cleanup"] = map[string]any{"code": "cleanup_incomplete", "error": closeErr.Error()}
			}
		}
	}()
	if has(inv, "name") {
		if _, ok := api.(ThreadNameSetter); !ok {
			return cli.ExecutionResult{}, missing("thread/name/set adapter")
		}
	}
	sub := inv.Position[0]
	var data any
	var kind string
	var cursor string
	var collection bool
	var resultKind string
	var warnings = append([]string(nil), conn.Warnings()...)
	switch sub {
	case "list":
		r, callErr := api.ThreadList(ctx, codexapi.ThreadListOptions{Cursor: inv.Option("cursor"), Limit: optionInt(inv, "limit"), SortKey: inv.Option("sort"), SortDirection: inv.Option("order"), SourceKinds: options(inv, "source"), Loaded: has(inv, "loaded"), CWD: optionOrNil(inv, "cwd"), Archived: boolOption(inv, "archived")})
		if callErr != nil {
			return cli.ExecutionResult{}, mapError(callErr, "unknown")
		}
		data, cursor = pageData(r.Raw, r, r.NextCursor)
		kind = "thread.list"
		data = responseDataArray(r.Raw)
		collection, resultKind = true, "thread"
	case "read":
		if len(inv.Position) < 2 {
			return cli.ExecutionResult{}, usage("thread read requires a thread identifier")
		}
		r, callErr := api.ThreadRead(ctx, codexapi.ThreadReadOptions{ThreadID: threadID})
		if callErr != nil {
			return cli.ExecutionResult{}, mapError(callErr, "unknown")
		}
		data, kind = responseThreadArray(r.Raw), "thread.read"
		collection, resultKind = true, "thread"
	case "turns":
		if len(inv.Position) < 2 {
			return cli.ExecutionResult{}, usage("thread turns requires a thread identifier")
		}
		itemsView := inv.Option("view")
		if itemsView == "" {
			itemsView = "summary"
		}
		r, callErr := api.ThreadTurns(ctx, codexapi.TurnsOptions{ThreadID: threadID, Cursor: inv.Option("cursor"), Limit: optionInt(inv, "limit"), SortDirection: inv.Option("order"), ItemsView: itemsView})
		if callErr != nil {
			return cli.ExecutionResult{}, mapError(callErr, "unknown")
		}
		data, cursor = pageData(r.Raw, r, r.NextCursor)
		kind = "thread.turns"
		data = responseDataArray(r.Raw)
		collection, resultKind = true, "thread"
	case "items":
		if len(inv.Position) < 2 {
			return cli.ExecutionResult{}, usage("thread items requires a thread identifier")
		}
		r, callErr := api.ThreadItems(ctx, codexapi.ItemsOptions{ThreadID: threadID, TurnID: inv.Option("turn"), Cursor: inv.Option("cursor"), Limit: optionInt(inv, "limit"), SortDirection: inv.Option("order")})
		if callErr != nil {
			return cli.ExecutionResult{}, mapError(callErr, "unknown")
		}
		data, cursor = pageData(r.Raw, r, r.NextCursor)
		kind = "thread.items"
		data = responseDataArray(r.Raw)
		collection, resultKind = true, "thread"
	case "start":
		r, callErr := api.ThreadStart(ctx, codexapi.StartOptions{Model: inv.Option("model"), CWD: inv.Option("cwd"), ThreadSource: inv.Option("source")})
		if callErr != nil {
			return cli.ExecutionResult{}, mapError(callErr, "unknown")
		}
		if name := inv.Option("name"); name != "" {
			if callErr := e.setThreadName(ctx, api, name, r.Thread.ID, r.Raw, "thread.start", endpointID); callErr != nil {
				return cli.ExecutionResult{}, callErr
			}
		}
		data, kind = lifecycleMetadata(r), "thread.start"
	case "resume":
		if len(inv.Position) < 2 {
			return cli.ExecutionResult{}, usage("thread resume requires a thread identifier")
		}
		r, callErr := api.ThreadResume(ctx, codexapi.ResumeOptions{ThreadID: threadID})
		if callErr != nil {
			return cli.ExecutionResult{}, mapError(callErr, "unknown")
		}
		data, kind = lifecycleMetadata(r), "thread.resume"
	case "fork":
		if len(inv.Position) < 2 {
			return cli.ExecutionResult{}, usage("thread fork requires a thread identifier")
		}
		r, callErr := api.ThreadFork(ctx, codexapi.ForkOptions{ThreadID: threadID, ThroughTurnID: inv.Option("through-turn"), BeforeTurnID: inv.Option("before-turn"), ExactRead: nil})
		if callErr != nil {
			return cli.ExecutionResult{}, mapError(callErr, "unknown")
		}
		if name := inv.Option("name"); name != "" {
			if callErr := e.setThreadName(ctx, api, name, r.Thread.ID, r.Raw, "thread.fork", endpointID); callErr != nil {
				return cli.ExecutionResult{}, callErr
			}
		}
		data, kind = lifecycleMetadata(r), "thread.fork"
	default:
		return cli.ExecutionResult{}, usage("unsupported thread subcommand: " + sub)
	}
	if collection {
		output, outputErr := e.collectionResult(ctx, kind, resultKind, data, cursor, warnings, mutating, endpointID)
		if outputErr == nil && threadID != "" {
			addResultData(&output, "threadId", threadID)
		}
		return output, outputErr
	}
	return e.result(ctx, kind, data, cursor, warnings, mutating, endpointID)
}

func (e *Executor) resolveThreadTarget(ctx context.Context, inv cli.Invocation) (string, string, string, endpoint.Endpoint, error) {
	sub := inv.Position[0]
	if sub == "list" || sub == "start" {
		resolved, err := e.resolveEndpointIdentity(inv.Resolved.Endpoint)
		if err != nil {
			return "", "", "", endpoint.Endpoint{}, err
		}
		return inv.Resolved.Endpoint, resolved.ID, "", resolved, nil
	}
	if len(inv.Position) < 2 {
		return "", "", "", endpoint.Endpoint{}, usage("thread target is required")
	}
	selector := inv.Position[1]
	if e.ports.Targets != nil {
		var resolved ThreadTarget
		var err error
		if explicit, ok := e.ports.Targets.(ExplicitThreadTargetResolver); ok {
			resolved, err = explicit.ResolveThreadWithOptions(ctx, selector, inv.Resolved.Endpoint, inv.Resolved.EndpointSource != cli.PathDefault)
		} else {
			resolved, err = e.ports.Targets.ResolveThread(ctx, selector, inv.Resolved.Endpoint)
		}
		if err != nil {
			return "", "", "", endpoint.Endpoint{}, mapError(err, "not_sent")
		}
		endpointID := resolved.EndpointID
		if endpointID == "" {
			endpointID = resolved.Endpoint
		}
		return resolved.Endpoint, endpointID, resolved.ThreadID, resolved.Resolved, nil
	}
	if strings.Contains(selector, "://") {
		return "", "", "", endpoint.Endpoint{}, &cli.Error{Code: "invalid_target", Message: "thread target resolver is required for URI targets", Effect: "not_sent", Exit: cli.ExitUsage}
	}
	return inv.Resolved.Endpoint, inv.Resolved.Endpoint, selector, endpoint.Endpoint{}, nil
}

func (e *Executor) resolveEndpointIdentity(selector string) (endpoint.Endpoint, error) {
	if e.ports.Endpoints == nil {
		return endpoint.Endpoint{ID: selector, Alias: selector}, nil
	}
	resolved, err := e.ports.Endpoints.Show(selector)
	if err != nil {
		return endpoint.Endpoint{}, mapError(err, "not_sent")
	}
	if resolved.ID == "" {
		return endpoint.Endpoint{}, &cli.Error{Code: "endpoint_unavailable", Message: "resolved endpoint has no stable ID", Effect: "not_sent", Exit: cli.ExitRejected}
	}
	return resolved, nil
}

func (e *Executor) search(ctx context.Context, inv cli.Invocation) (result cli.ExecutionResult, execErr error) {
	if len(inv.Position) < 1 || strings.TrimSpace(inv.Position[0]) == "" {
		return cli.ExecutionResult{}, usage("search requires a query")
	}
	pageMax := codexapi.MaxThreadPageLimit
	if has(inv, "thread") {
		pageMax = codexapi.MaxOccurrencesPageLimit
	}
	if err := validatePageOptions(inv, pageMax); err != nil {
		return cli.ExecutionResult{}, err
	}
	if e.ports.ReadReceipts == nil && e.ports.Receipts == nil {
		return cli.ExecutionResult{}, missing("read receipt journal")
	}
	conn, api, err := e.openWithOptions(ctx, inv.Resolved.Endpoint, OpenOptions{ExperimentalAPI: true})
	if err != nil {
		return cli.ExecutionResult{}, err
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			if execErr == nil {
				appendCleanupWarning(&result, closeErr)
			} else if existing, ok := execErr.(*cli.Error); ok {
				if existing.Details == nil {
					existing.Details = map[string]any{}
				}
				existing.Details["cleanup"] = map[string]any{"code": "cleanup_incomplete", "error": closeErr.Error()}
			}
		}
	}()
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
		data, cursor := responseDataArray(r.Raw), r.NextCursor
		warnings := append([]string(nil), conn.Warnings()...)
		receipt, receiptErr := e.searchReceipt(ctx, inv.Resolved.Endpoint, "message", data, cursor)
		if receiptErr != nil {
			return cli.ExecutionResult{}, receiptErr
		}
		output, outputErr := e.collectionResultWithReceipt(ctx, "search", "message", data, cursor, warnings, inv.Resolved.Endpoint, receipt)
		if outputErr == nil {
			addResultData(&output, "threadId", thread)
		}
		return output, outputErr
	}
	r, callErr := api.Search(ctx, options)
	if callErr != nil {
		return cli.ExecutionResult{}, mapError(callErr, "unknown")
	}
	data, cursor := responseDataArray(r.Raw), r.NextCursor
	warnings := append([]string(nil), conn.Warnings()...)
	receipt, receiptErr := e.searchReceipt(ctx, inv.Resolved.Endpoint, "thread", data, cursor)
	if receiptErr != nil {
		return cli.ExecutionResult{}, receiptErr
	}
	return e.collectionResultWithReceipt(ctx, "search", "thread", data, cursor, warnings, inv.Resolved.Endpoint, receipt)
}

func (e *Executor) searchReceipt(ctx context.Context, endpointID, resultKind string, data any, cursor string) (any, error) {
	metadata := map[string]any{"resultKind": resultKind, "experimental": true, "cursor": cursor}
	if items, ok := data.([]any); ok {
		metadata["count"] = len(items)
	}
	if e.ports.ReadReceipts != nil {
		receipt, err := e.ports.ReadReceipts.Read(ctx, "search", endpointID, metadata)
		if err != nil {
			return nil, mapError(err, "unknown")
		}
		if receipt == nil {
			return nil, &cli.Error{Code: "internal_error", Message: "search read receipt was not persisted", Effect: "unknown", Exit: cli.ExitInternal}
		}
		return receipt, nil
	}
	if e.ports.Receipts != nil {
		receipt, err := e.ports.Receipts.Mutation(ctx, "search", endpointID, metadata)
		if err != nil {
			return nil, mapError(err, "unknown")
		}
		if receipt == nil {
			return nil, &cli.Error{Code: "internal_error", Message: "search read receipt was not persisted", Effect: "unknown", Exit: cli.ExitInternal}
		}
		return receipt, nil
	}
	return nil, missing("read receipt journal")
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
		return e.storageResult(ctx, "status", v, false, "local")
	case "check":
		v, err := e.ports.Storage.Check(ctx)
		if err != nil {
			return cli.ExecutionResult{}, mapError(err, "not_sent")
		}
		return e.storageResult(ctx, "check", v, false, "local")
	case "maintain":
		before, err := parseTime(inv.Option("before"))
		if err != nil {
			return cli.ExecutionResult{}, usage(err.Error())
		}
		v, err := e.ports.Storage.Maintain(ctx, journal.MaintenanceOptions{Before: before, DryRun: has(inv, "dry-run")})
		if err != nil {
			return cli.ExecutionResult{}, e.partialMutationError(ctx, "storage", "local", v, err)
		}
		return e.storageResult(ctx, "maintain", v, true, "local")
	case "vacuum":
		v, err := e.ports.Storage.Vacuum(ctx)
		if err != nil {
			return cli.ExecutionResult{}, mapError(err, "unknown")
		}
		return e.storageResult(ctx, "vacuum", v, true, "local")
	default:
		return cli.ExecutionResult{}, usage("unsupported storage subcommand: " + sub)
	}
}

func (e *Executor) partialMutationError(ctx context.Context, operation, endpointID string, partial any, cause error) error {
	if e.ports.Receipts != nil {
		if receipt, err := e.ports.Receipts.Mutation(ctx, operation, endpointID, partial); err == nil && receipt != nil {
			return &cli.Error{Code: "internal_error", Message: cause.Error(), Effect: "accepted", Details: map[string]any{"partialReceipt": receipt, "partial": partial}, Exit: cli.ExitInternal}
		}
	}
	return mapError(cause, "unknown")
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

func (e *Executor) rpc(ctx context.Context, inv cli.Invocation) (result cli.ExecutionResult, execErr error) {
	if len(inv.Position) < 1 || strings.TrimSpace(inv.Position[0]) == "" {
		return cli.ExecutionResult{}, usage("rpc requires a method")
	}
	if e.ports.Connections == nil || e.ports.RPC == nil {
		return cli.ExecutionResult{}, missing("rpc runtime")
	}
	if has(inv, "force") && !has(inv, "output") {
		return cli.ExecutionResult{}, usage("--force requires --output")
	}
	if e.ports.Receipts == nil {
		return cli.ExecutionResult{}, missing("receipt journal")
	}
	params, source, err := e.params(ctx, inv)
	if err != nil {
		return cli.ExecutionResult{}, err
	}
	conn, _, err := e.openWithOptions(ctx, inv.Resolved.Endpoint, OpenOptions{ExperimentalAPI: rpcmeta.Evaluate(inv.Position[0], params).Experimental})
	if err != nil {
		return cli.ExecutionResult{}, err
	}
	defer func() {
		if closeErr := conn.Close(); closeErr != nil {
			if execErr == nil {
				appendCleanupWarning(&result, closeErr)
			} else if existing, ok := execErr.(*cli.Error); ok {
				if existing.Details == nil {
					existing.Details = map[string]any{}
				}
				existing.Details["cleanup"] = map[string]any{"code": "cleanup_incomplete", "error": closeErr.Error()}
			}
		}
	}()
	var output rawrpc.OutputOptions
	if (has(inv, "output") || has(inv, "force")) && e.ports.Artifacts == nil {
		return cli.ExecutionResult{}, missing("artifact")
	}
	if e.ports.Artifacts != nil {
		output, err = e.ports.Artifacts.RPCOutput(ctx, inv)
		if err != nil {
			return cli.ExecutionResult{}, mapError(err, "not_sent")
		}
	}
	if inv.Resolved.Compact {
		output.InlineLimit = cli.CompactRPCInlineBytes
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
	receiptData := map[string]any{"method": r.Method, "id": r.ID, "effects": r.Effects, "experimental": r.Experimental, "networkRead": r.NetworkRead, "retrySafety": r.RetrySafety, "effectState": r.EffectState}
	if r.Artifact != nil {
		receiptData["artifact"] = r.Artifact
	}
	return e.resultEnvelope(ctx, "rpc", "rpc", "result", data, "", nil, true, inv.Resolved.Endpoint, "", nil, receiptData)
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
		if strings.TrimSpace(inv.Option("params-file")) == "" {
			return nil, "", usage("--params-file requires a path")
		}
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
	return e.resultEnvelope(ctx, kind, kind, "result", data, cursor, warnings, mutation, endpointID, "", nil, nil)
}

func (e *Executor) collectionResult(ctx context.Context, eventKind, resultKind string, data any, cursor string, warnings []string, mutation bool, endpointID string) (cli.ExecutionResult, error) {
	return e.resultEnvelope(ctx, eventKind, resultKind, "data", data, cursor, warnings, mutation, endpointID, "", nil, nil)
}

func (e *Executor) collectionResultWithReceipt(ctx context.Context, eventKind, resultKind string, data any, cursor string, warnings []string, endpointID string, receipt any) (cli.ExecutionResult, error) {
	return e.resultEnvelope(ctx, eventKind, resultKind, "data", data, cursor, warnings, false, endpointID, "", receipt, nil)
}

func (e *Executor) storageResult(ctx context.Context, subcommand string, data any, mutation bool, endpointID string) (cli.ExecutionResult, error) {
	return e.resultEnvelope(ctx, "storage", "storage", "result", data, "", nil, mutation, endpointID, subcommand, nil, nil)
}

func (e *Executor) resultEnvelope(ctx context.Context, eventKind, resultKind, field string, data any, cursor string, warnings []string, mutation bool, endpointID, subcommand string, providedReceipt, receiptInput any) (cli.ExecutionResult, error) {
	payload := map[string]any{"resultKind": resultKind, field: data}
	if stable := receiptEndpointID(providedReceipt); stable != "" {
		payload["endpointId"] = stable
	} else if endpointID != "" {
		payload["endpointId"] = endpointID
	}
	if subcommand != "" {
		payload["subcommand"] = subcommand
	}
	if eventKind == "search" {
		payload["experimental"] = true
	}
	if cursor != "" {
		payload["nextCursor"] = cursor
	}
	var receipt any = providedReceipt
	if mutation {
		if e.ports.Receipts == nil {
			return cli.ExecutionResult{}, missing("receipt journal")
		}
		var err error
		input := data
		if receiptInput != nil {
			input = receiptInput
		}
		receipt, err = e.ports.Receipts.Mutation(ctx, eventKind, endpointID, input)
		if err != nil {
			return cli.ExecutionResult{}, mapError(err, "unknown")
		}
		if receipt == nil {
			return cli.ExecutionResult{}, &cli.Error{Code: "internal_error", Message: "receipt journal returned no receipt", Effect: "unknown", Exit: cli.ExitInternal}
		}
	}
	eventMachine := map[string]any{"event": eventKind + ".completed", "terminal": true, "ok": true, "data": payload}
	if receipt != nil {
		operationID, err := receiptOperationID(receipt)
		if err != nil {
			return cli.ExecutionResult{}, err
		}
		eventMachine["operationId"] = operationID
	}
	if len(warnings) != 0 {
		// warnings is a lifecycle envelope field. Keeping it out of data is
		// important because App preserves this top-level location verbatim.
		eventMachine["warnings"] = warningObjects(warnings)
	}
	event := cli.OutputEvent{Machine: eventMachine, Human: humanResult(eventKind, data, cursor, subcommand)}
	result := cli.ExecutionResult{Events: []cli.OutputEvent{event}, Exit: cli.ExitSuccess}
	if receipt != nil {
		result.Receipt = receipt
	}
	return result, nil
}

func addResultData(result *cli.ExecutionResult, key string, value any) {
	if result == nil || len(result.Events) == 0 || value == nil {
		return
	}
	event, ok := result.Events[0].Machine.(map[string]any)
	if !ok {
		return
	}
	data, ok := event["data"].(map[string]any)
	if ok {
		data[key] = value
	}
}

func receiptEndpointID(receipt any) string {
	if receipt == nil {
		return ""
	}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		return ""
	}
	var object map[string]any
	if json.Unmarshal(encoded, &object) != nil {
		return ""
	}
	for _, side := range []string{"target", "source"} {
		identity, _ := object[side].(map[string]any)
		if value, _ := identity["endpointId"].(string); value != "" {
			return value
		}
	}
	return ""
}

func responseDataArray(raw json.RawMessage) any {
	var envelope struct {
		Data json.RawMessage `json:"data"`
	}
	if json.Unmarshal(raw, &envelope) == nil && len(bytes.TrimSpace(envelope.Data)) != 0 && !bytes.Equal(bytes.TrimSpace(envelope.Data), []byte("null")) {
		var value any
		if json.Unmarshal(envelope.Data, &value) == nil {
			return value
		}
	}
	return []any{}
}

func responseThreadArray(raw json.RawMessage) any {
	var envelope struct {
		Thread json.RawMessage `json:"thread"`
	}
	if json.Unmarshal(raw, &envelope) == nil && len(bytes.TrimSpace(envelope.Thread)) != 0 && !bytes.Equal(bytes.TrimSpace(envelope.Thread), []byte("null")) {
		var value any
		if json.Unmarshal(envelope.Thread, &value) == nil {
			return []any{value}
		}
	}
	return []any{}
}

func receiptOperationID(receipt any) (string, error) {
	if receipt == nil {
		return "", &cli.Error{Code: "internal_error", Message: "receipt is missing operationId", Effect: "unknown", Exit: cli.ExitInternal}
	}
	encoded, err := json.Marshal(receipt)
	if err != nil {
		return "", &cli.Error{Code: "internal_error", Message: "receipt operationId is not serializable", Effect: "unknown", Exit: cli.ExitInternal}
	}
	var object map[string]any
	if json.Unmarshal(encoded, &object) != nil {
		return "", &cli.Error{Code: "internal_error", Message: "receipt operationId is malformed", Effect: "unknown", Exit: cli.ExitInternal}
	}
	value, _ := object["operationId"].(string)
	if err := mektup.ValidateID(value, mektup.OperationIDPrefix); err != nil {
		return "", &cli.Error{Code: "internal_error", Message: "receipt operationId is invalid: " + err.Error(), Effect: "unknown", Exit: cli.ExitInternal}
	}
	return value, nil
}

func warningObjects(warnings []string) []map[string]any {
	objects := make([]map[string]any, 0, len(warnings))
	for _, warning := range warnings {
		code := strings.TrimSpace(warning)
		switch code {
		case "untested_server_version", "server_version_unknown", "evidence_gap", "projection_may_lag", "audit_logging_enabled", "output_spilled", "resolver_degraded", "cleanup_incomplete", "manual_resolution":
		default:
			continue
		}
		objects = append(objects, map[string]any{"code": code, "message": warning, "details": map[string]any{}})
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

// lifecycleMetadata is the privacy boundary for thread mutations. Lifecycle
// responses may contain hydrated turns/items in Raw; mutation receipts and
// CLI output only carry bounded identity/runtime fields.
func lifecycleMetadata(response codexapi.LifecycleResponse) map[string]any {
	data := map[string]any{}
	if response.Thread.ID != "" {
		data["threadId"] = response.Thread.ID
	}
	if response.Thread.Status != "" {
		data["status"] = response.Thread.Status
	}
	if response.Model != "" {
		data["model"] = response.Model
	}
	if response.ModelProvider != "" {
		data["modelProvider"] = response.ModelProvider
	}
	if response.CWD != "" {
		data["cwd"] = response.CWD
	}
	return data
}

func pageData(raw json.RawMessage, value any, cursor string) (any, string) {
	return rawOr(raw, value), cursor
}

func optionInt(inv cli.Invocation, name string) int {
	var n int
	_, _ = fmt.Sscan(inv.Option(name), &n)
	return n
}

func validatePageOptions(inv cli.Invocation, maxLimit int) error {
	if cursor := inv.Option("cursor"); len(cursor) > codexapi.MaxCursorBytes {
		return usage("--cursor exceeds the bounded cursor size")
	}
	if value := inv.Option("limit"); value != "" {
		n, err := strconv.Atoi(value)
		if err != nil || n < 0 || n > maxLimit {
			return usage(fmt.Sprintf("--limit must be between 0 and %d", maxLimit))
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
	if errors.Is(err, connection.ErrUnsupported) {
		return &cli.Error{Code: "unsupported_server_version", Message: err.Error(), Effect: "not_sent", Exit: cli.ExitRejected}
	}
	if errors.Is(err, connection.ErrEndpointUnavailable) {
		return &cli.Error{Code: "endpoint_unavailable", Message: err.Error(), Effect: "not_sent", Exit: cli.ExitRejected}
	}
	if errors.Is(err, endpoint.ErrInvalidTarget) {
		return &cli.Error{Code: "invalid_target", Message: err.Error(), Effect: "not_sent", Exit: cli.ExitUsage}
	}
	if errors.Is(err, endpoint.ErrEndpointRequired) || errors.Is(err, endpoint.ErrEndpointNotFound) {
		return &cli.Error{Code: "endpoint_unavailable", Message: err.Error(), Effect: "not_sent", Exit: cli.ExitRejected}
	}
	if errors.Is(err, endpoint.ErrEndpointMismatch) {
		return &cli.Error{Code: "endpoint_unavailable", Message: err.Error(), Effect: "not_sent", Exit: cli.ExitRejected}
	}
	if errors.Is(err, endpoint.ErrResolverUnavailable) {
		return &cli.Error{Code: "resolver_unavailable", Message: err.Error(), Effect: "not_sent", Exit: cli.ExitRejected}
	}
	if errors.Is(err, endpoint.ErrResolverNotFound) {
		return &cli.Error{Code: "endpoint_unavailable", Message: err.Error(), Effect: "not_sent", Exit: cli.ExitRejected}
	}
	if errors.Is(err, endpoint.ErrResolverAmbiguous) {
		return &cli.Error{Code: "target_ambiguous", Message: err.Error(), Effect: "not_sent", Exit: cli.ExitRejected}
	}
	if errors.Is(err, endpoint.ErrResolverStale) {
		return &cli.Error{Code: "route_unavailable", Message: err.Error(), Effect: "not_sent", Exit: cli.ExitRejected}
	}
	var callErr *connection.CallError
	if errors.As(err, &callErr) && (errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled)) && callErr.Evidence.Phase >= appserver.WriteMayHaveWritten {
		return &cli.Error{Code: "outcome_unknown", Message: "operation outcome is unknown after a possible write", Effect: "outcome_unknown", Details: map[string]any{"writePhase": callErr.Evidence.Phase.String(), "generation": callErr.Evidence.Generation}, Exit: cli.ExitUnknown}
	}
	if errors.Is(err, journal.ErrStorageBusy) {
		return &cli.Error{Code: "storage_busy", Message: err.Error(), Effect: effect, Exit: cli.ExitUnknown}
	}
	if errors.Is(err, journal.ErrStorageCorrupt) {
		return &cli.Error{Code: "storage_corrupt", Message: err.Error(), Effect: effect, Exit: cli.ExitRejected}
	}
	var re *rawrpc.Error
	if errors.As(err, &re) {
		stable := re.Stable()
		return &cli.Error{Code: string(stable.Code), Message: stable.Message, Retryable: stable.Retryable, Effect: nonempty(stable.EffectState, effect), Details: stable.Details, Exit: cli.ExitCodeForError(string(stable.Code))}
	}
	if errors.Is(err, codexapi.ErrExperimentalAPIRequired) {
		return &cli.Error{Code: "experimental_method_unavailable", Message: err.Error(), Effect: effect, Exit: cli.ExitRejected}
	}
	for _, invalid := range []error{codexapi.ErrUnsupportedOption, codexapi.ErrInProgressCutoff, codexapi.ErrUnboundedPage, codexapi.ErrInvalidCWD, codexapi.ErrConflictingCWD} {
		if errors.Is(err, invalid) {
			return &cli.Error{Code: "invalid_arguments", Message: err.Error(), Effect: "not_sent", Exit: cli.ExitUsage}
		}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return &cli.Error{Code: "wait_interrupted", Message: err.Error(), Effect: effect, Exit: cli.ExitIncomplete}
	}
	return &cli.Error{Code: "internal_error", Message: err.Error(), Effect: effect, Details: map[string]any{"cause": err.Error()}, Exit: cli.ExitInternal}
}

func appendCleanupWarning(result *cli.ExecutionResult, err error) {
	result.Exit = cli.ExitInternal
	warning := map[string]any{"code": "cleanup_incomplete", "message": "connection cleanup failed", "details": map[string]any{"error": err.Error()}}
	for index := range result.Events {
		machine, ok := result.Events[index].Machine.(map[string]any)
		if !ok {
			continue
		}
		warnings, _ := machine["warnings"].([]map[string]any)
		machine["warnings"] = append(warnings, warning)
	}
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

func (a ArtifactStoreOutput) RPCOutput(_ context.Context, inv cli.Invocation) (rawrpc.OutputOptions, error) {
	return rawrpc.OutputOptions{Store: a.Store, Path: inv.Option("output"), Force: has(inv, "force")}, nil
}
