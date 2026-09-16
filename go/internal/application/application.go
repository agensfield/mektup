// Package application composes the production ports for the approved,
// non-messaging Mektup commands. It never starts or supervises a daemon.
//
// Resources are invocation-scoped: CLI-resolved config/state paths are applied
// before opening the journal, endpoint store, and artifact root, then all
// resources are closed before Execute returns.
package application

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	mektup "github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/internal/artifact"
	"github.com/agensfield/mektup/go/internal/cli"
	"github.com/agensfield/mektup/go/internal/codexapi"
	"github.com/agensfield/mektup/go/internal/connection"
	"github.com/agensfield/mektup/go/internal/controlreceiver"
	"github.com/agensfield/mektup/go/internal/doctor"
	"github.com/agensfield/mektup/go/internal/endpoint"
	"github.com/agensfield/mektup/go/internal/executor"
	"github.com/agensfield/mektup/go/internal/journal"
	"github.com/agensfield/mektup/go/internal/messageexecutor"
	"github.com/agensfield/mektup/go/internal/rawrpc"
	"github.com/agensfield/mektup/go/internal/receipts"
	"github.com/agensfield/mektup/go/internal/runtime"
	"github.com/agensfield/mektup/go/internal/service"
	"github.com/agensfield/mektup/go/internal/sshproxy"
	"github.com/agensfield/mektup/go/internal/storage"
	"modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// Options controls production composition. DialerForRoute is a deterministic
// seam for tests and embedders; when absent, Unix routes use appserver's
// direct Unix socket and SSH routes use the bounded OpenSSH proxy bridge.
type Options struct {
	Input        io.Reader
	CodexHome    string
	ConfigPath   string
	StateDir     string
	ArtifactDir  string
	IdentityHome string

	Connection          connection.Options
	SSHConfig           sshproxy.Config
	SSHFactory          sshproxy.ProcessFactory
	SessionFactory      runtime.SessionFactory
	HerdrRunner         endpoint.CommandRunner
	HerdrEndpointRunner endpoint.EndpointCommandRunner
	Registry            controlreceiver.FileRegistry
	CustodyStoreID      string
	CurrentThreadID     string
	AgentMode           bool
	ThreadStateProbe    runtime.ThreadStateProbe
	HumanGate           receipts.HumanGate

	DialerForRoute func(endpoint.Route, bool) connection.ClientDialer
}

// Environment implements cli.Executor and is the composition seam for a
// future messaging executor/composite. It intentionally exposes no messaging
// executor itself.
type Environment struct {
	options Options
	mu      sync.RWMutex
	closed  bool
}

func New(options Options) *Environment { return &Environment{options: options} }

// NewEnvironment is the explicit constructor spelling for embedders that
// compose this production environment with another command executor.
func NewEnvironment(options Options) *Environment { return New(options) }

var _ cli.Executor = (*Environment)(nil)
var _ cli.StreamingExecutor = (*Environment)(nil)

// Execute opens only the resources needed by the production executor for this
// invocation. No path is taken that starts a daemon.
func (e *Environment) Execute(ctx context.Context, inv cli.Invocation) (result cli.ExecutionResult, execErr error) {
	if e == nil {
		return cli.ExecutionResult{}, &cli.Error{Code: "internal_error", Message: "application environment is closed", Effect: "not_sent", Exit: cli.ExitInternal}
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return cli.ExecutionResult{}, &cli.Error{Code: "internal_error", Message: "application environment is closed", Effect: "not_sent", Exit: cli.ExitInternal}
	}
	resources, err := e.openResources(ctx, inv)
	if err != nil {
		return cli.ExecutionResult{}, err
	}
	defer func() {
		var closeErr error
		result, closeErr = resources.CloseWithResult(ctx, result)
		if closeErr != nil {
			cleanupErr := &CleanupError{Resource: "application resources", Err: closeErr}
			if execErr == nil {
				// CloseWithResult has already decorated the accepted result and
				// persisted the warning while the journal was still available.
			} else {
				execErr = errors.Join(execErr, cleanupErr)
			}
		}
	}()
	if resources.messaging != nil {
		return resources.messaging.Execute(ctx, inv)
	}
	return executor.New(resources.Ports()).Execute(ctx, inv)
}

// ExecuteStream keeps the application resources alive while a messaging
// acceptance callback emits before the correlated wait. Non-messaging
// commands remain owned by the established executor.
func (e *Environment) ExecuteStream(ctx context.Context, inv cli.Invocation, emit func(cli.ExecutionResult) error) error {
	if !isMessaging(inv) {
		result, err := e.Execute(ctx, inv)
		if err != nil {
			return err
		}
		result.Streaming = true
		return emit(result)
	}
	if e == nil {
		return &cli.Error{Code: "internal_error", Message: "application environment is closed", Effect: "not_sent", Exit: cli.ExitInternal}
	}
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.closed {
		return &cli.Error{Code: "internal_error", Message: "application environment is closed", Effect: "not_sent", Exit: cli.ExitInternal}
	}
	resources, err := e.openResources(ctx, inv)
	if err != nil {
		return err
	}
	if resources.messaging == nil {
		_ = resources.Close()
		return &cli.Error{Code: "internal_error", Message: "messaging executor was not composed", Effect: "not_sent", Exit: cli.ExitInternal}
	}
	closed := false
	last := cli.ExecutionResult{}
	closeResources := func(result cli.ExecutionResult) (cli.ExecutionResult, error) {
		if closed {
			return result, nil
		}
		closed = true
		updated, closeErr := resources.CloseWithResult(ctx, result)
		last = updated
		return updated, closeErr
	}
	streamErr := resources.messaging.ExecuteStream(ctx, inv, func(result cli.ExecutionResult) error {
		last = result
		if executionResultTerminal(result) {
			updated, _ := closeResources(result)
			result = updated
		}
		return emit(result)
	})
	if !closed {
		_, closeErr := closeResources(last)
		if streamErr == nil && closeErr != nil {
			streamErr = closeErr
		}
	}
	return streamErr
}

func executionResultTerminal(result cli.ExecutionResult) bool {
	if len(result.RawJSON) > 0 {
		return true
	}
	for _, event := range result.Events {
		encoded, err := json.Marshal(event.Machine)
		if err != nil {
			continue
		}
		var object map[string]any
		if json.Unmarshal(encoded, &object) == nil {
			if terminal, ok := object["terminal"].(bool); ok && terminal {
				return true
			}
		}
	}
	return false
}

// Close prevents future invocations. Resources are invocation-scoped and are
// already closed by Execute, so this operation is deliberately idempotent.
func (e *Environment) Close() error {
	if e == nil {
		return nil
	}
	e.mu.Lock()
	e.closed = true
	e.mu.Unlock()
	return nil
}

// CleanupError identifies a resource cleanup failure without hiding the
// operation's result or its original error.
type CleanupError struct {
	Resource string
	Err      error
}

func preserveCleanupResult(result cli.ExecutionResult, cleanupErr error) cli.ExecutionResult {
	warning := map[string]any{"code": "cleanup_incomplete", "message": "application resource cleanup failed", "details": map[string]any{"error": cleanupErr.Error()}}
	result.Receipt = addReceiptWarning(result.Receipt, cleanupErr)
	for index := range result.Events {
		machine, ok := result.Events[index].Machine.(map[string]any)
		if !ok {
			continue
		}
		machine["warnings"] = appendWireWarning(machine["warnings"], warning)
		result.Events[index].Machine = machine
	}
	if len(result.Events) == 0 {
		result.Events = append(result.Events, cli.OutputEvent{Machine: map[string]any{
			"event": "operation.cleanup_warning", "terminal": true, "ok": false, "warnings": []any{warning},
		}, Human: "cleanup incomplete"})
	} else if strings.TrimSpace(result.Human) != "" {
		result.Human += " (cleanup incomplete)"
	}
	result.Exit = cli.ExitInternal
	return result
}

func addReceiptWarning(value any, cleanupErr error) any {
	receipt, ok := value.(mektup.Receipt)
	if !ok {
		return value
	}
	receipt.Warnings = append(receipt.Warnings, mektup.Warning{Code: mektup.WarningCleanupIncomplete, Message: "application resource cleanup failed", Details: map[string]any{"error": cleanupErr.Error()}})
	return receipt
}

func appendWireWarning(existing any, warning map[string]any) []any {
	result := make([]any, 0, 1)
	switch values := existing.(type) {
	case []any:
		result = append(result, values...)
	case []map[string]any:
		for _, value := range values {
			result = append(result, value)
		}
	}
	return append(result, warning)
}

func (e *CleanupError) Error() string {
	if e == nil {
		return "application cleanup failed"
	}
	return fmt.Sprintf("cleanup %s: %v", e.Resource, e.Err)
}

func (e *CleanupError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

type resources struct {
	journal   *journal.Journal
	artifact  *artifact.Store
	pool      *runtime.ConnectionPool
	messaging *messageexecutor.Executor
	ports     resourcePorts
}

func (r *resources) Ports() executor.Ports { return r.ports.Ports }

func (r *resources) Close() error {
	_, err := r.CloseWithResult(context.Background(), cli.ExecutionResult{})
	return err
}

func (r *resources) CloseWithResult(ctx context.Context, result cli.ExecutionResult) (cli.ExecutionResult, error) {
	if r == nil {
		return result, nil
	}
	var joined error
	if r.pool != nil {
		if err := r.pool.Close(ctx); err != nil {
			cleanupErr := &CleanupError{Resource: "runtime connection pool", Err: err}
			result = preserveCleanupResult(result, cleanupErr)
			joined = errors.Join(joined, cleanupErr)
		}
	}
	if r.artifact != nil {
		if err := r.artifact.Close(); err != nil {
			cleanupErr := &CleanupError{Resource: "artifact store", Err: err}
			result = preserveCleanupResult(result, cleanupErr)
			if persistErr := persistReceiptWarning(ctx, r.journal, result.Receipt); persistErr != nil {
				joined = errors.Join(joined, cleanupErr, persistErr)
			} else {
				joined = errors.Join(joined, cleanupErr)
			}
		}
	}
	if r.journal != nil {
		if err := r.journal.Close(); err != nil {
			result = preserveCleanupResult(result, &CleanupError{Resource: "journal", Err: err})
			joined = errors.Join(joined, err)
		}
	}
	return result, joined
}

func persistReceiptWarning(ctx context.Context, j *journal.Journal, value any) error {
	if j == nil {
		return nil
	}
	receipt, ok := value.(mektup.Receipt)
	if !ok {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return j.PutReceipt(ctx, receipt)
}

type resourcePorts struct {
	executor.Ports
}

func (e *Environment) openResources(ctx context.Context, inv cli.Invocation) (*resources, error) {
	configPath, stateDir := e.paths(inv)
	if stateDir == "" {
		return nil, errors.New("application state directory is required")
	}
	if configPath == "" {
		return nil, errors.New("application config path is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var j *journal.Journal
	var artifacts *artifact.Store
	var err error
	if needsJournal(inv) {
		j, err = journal.Open(ctx, journal.Options{StateDir: stateDir})
		if err != nil {
			return nil, mapJournalOpenError(err)
		}
	}
	if needsArtifacts(inv) {
		artifactRoot := e.options.ArtifactDir
		if artifactRoot == "" {
			artifactRoot = filepath.Join(stateDir, "artifacts")
		}
		artifacts, err = artifact.NewStore(artifactRoot)
		if err != nil {
			if j != nil {
				_ = j.Close()
			}
			return nil, fmt.Errorf("open application artifact store: %w", err)
		}
	}

	store := endpoint.NewStoreWithIdentityHome(configPath, stateDir, e.options.IdentityHome)
	facts := &connectionFacts{values: make(map[string]connection.Info)}
	pins := &receiptPins{values: make(map[string]endpoint.Endpoint)}
	connections := &connectionFactory{store: store, codexHome: e.options.CodexHome, options: e.options.Connection, sshConfig: e.options.SSHConfig, sshFactory: e.options.SSHFactory, dialerForRoute: e.options.DialerForRoute, facts: facts, pins: pins}
	var receipts *receiptStore
	if j != nil {
		receipts = &receiptStore{journal: j, endpoints: store, codexHome: e.options.CodexHome, facts: facts, pins: pins}
	}
	endpoints := endpointPort{store: store, receipts: receipts}
	input := executor.ReaderInput{In: e.options.Input}
	if input.In == nil {
		input.In = os.Stdin
	}
	input.ReadFileFunc = readFileBounded
	configDir := filepath.Dir(configPath)
	socketPath, socketErr := endpoint.LocalSocket(e.options.CodexHome)
	if socketErr != nil {
		socketPath = ""
	}
	doctorOptions := doctor.DefaultOptions(doctor.Paths{StateDir: stateDir, ConfigDir: configDir, ConfigFile: configPath, SocketPath: socketPath})
	ports := executor.Ports{
		Connections:    connections,
		Endpoints:      endpoints,
		EndpointHealth: endpointHealth{factory: connections},
		Storage:        storagePortFor(inv, j, stateDir),
		Doctor:         doctorPort{},
		Input:          input,
		Artifacts:      executor.ArtifactStoreOutput{Store: artifacts},
		RPC:            rpcPort{},
		DoctorOptions:  doctorOptions,
	}
	if receipts != nil {
		ports.Receipts = receipts
		ports.ReadReceipts = receipts
	}
	var pool *runtime.ConnectionPool
	var messaging *messageexecutor.Executor
	if isMessaging(inv) {
		if j == nil {
			return nil, errors.New("messaging requires a writable local journal")
		}
		pool, messaging, err = e.composeMessaging(ctx, inv, j, store, artifacts, input)
		if err != nil {
			if artifacts != nil {
				_ = artifacts.Close()
			}
			_ = j.Close()
			return nil, err
		}
	}
	return &resources{journal: j, artifact: artifacts, pool: pool, messaging: messaging, ports: resourcePorts{Ports: ports}}, nil
}

func (e *Environment) composeMessaging(ctx context.Context, inv cli.Invocation, j *journal.Journal, store endpoint.EndpointStore, artifacts *artifact.Store, input executor.ReaderInput) (*runtime.ConnectionPool, *messageexecutor.Executor, error) {
	codexHome := firstNonEmpty(inv.Resolved.CodexHome, e.options.CodexHome)
	stateProbe := e.options.ThreadStateProbe
	herdr := endpoint.NewHerdrResolver(e.options.HerdrRunner)
	herdr.EndpointRunner = e.options.HerdrEndpointRunner
	sessionFactory := e.options.SessionFactory
	if sessionFactory == nil {
		sessionFactory = applicationSessionFactory{options: e.options.Connection, sshConfig: e.options.SSHConfig, sshFactory: e.options.SSHFactory, dialerForRoute: e.options.DialerForRoute}
	}
	pool := runtime.NewConnectionPool(sessionFactory, func(id string) (endpoint.Endpoint, error) { return store.ResolveEndpointID(id, codexHome) })
	if stateProbe == nil {
		stateProbe = pool.ProbeThreadState
	}
	observe := &runtime.ObservationAdapter{Pool: pool}
	resolverFor := func(operation cli.Invocation) runtime.ResolverAdapter {
		current := firstNonEmpty(operation.Resolved.CurrentThreadID, e.options.CurrentThreadID)
		custodyStore := firstNonEmpty(e.options.CustodyStoreID, j.StoreID())
		return runtime.ResolverAdapter{Store: store, Herdr: herdr, EndpointOverride: operation.Resolved.Endpoint, CodexHome: codexHome, CurrentThreadID: current, ReplyTo: operation.Option("reply-to"), CustodyStoreID: custodyStore, StateProbe: stateProbe}
	}
	localJournal, err := runtime.NewJournalAdapter(j, nil)
	if err != nil {
		_ = pool.Close(ctx)
		return nil, nil, err
	}
	localEndpoint, localErr := store.ResolveExistingEndpoint("local", codexHome)
	if localErr != nil {
		// ResolveSource will establish the local identity for operations that
		// need it; the remote journal only needs this value to distinguish a
		// local custody route from SSH custody.
		localEndpoint = endpoint.Endpoint{}
	}
	remoteJournal := &service.RemoteJournal{Local: &localJournal, Endpoints: store, LocalEndpointID: localEndpoint.ID}
	serviceFactory := func(serviceCtx context.Context, operation cli.Invocation) (messageexecutor.MessagingService, error) {
		resolver := resolverFor(operation)
		return &service.Service{Resolver: resolver, Delivery: &runtime.DeliveryAdapter{Pool: pool}, Journal: remoteJournal, Observe: observe}, nil
	}
	originalFactory := func(originalCtx context.Context, operation cli.Invocation) (service.OriginalResolver, error) {
		resolver := resolverFor(operation)
		source, err := resolver.ResolveSource(originalCtx, operation.Option("reply-to"))
		if err != nil {
			return nil, err
		}
		return runtime.OriginalResolver{Observe: observe, Target: service.ResolvedTarget{EndpointID: source.EndpointID, URI: source.URI, ThreadID: threadIDFromURI(source.URI), Loaded: true, Persistent: true}}, nil
	}
	prepareCustody := func(prepareCtx context.Context, operation cli.Invocation) error {
		resolver := resolverFor(operation)
		source, err := resolver.ResolveSource(prepareCtx, operation.Option("reply-to"))
		if err != nil {
			return &cli.Error{Code: "reply_route_required", Message: "reply custody source could not be established", Effect: "not_sent", Exit: cli.ExitUsage, Details: map[string]any{"cause": err.Error()}}
		}
		ep, err := store.ResolveEndpointID(source.EndpointID, codexHome)
		if err != nil {
			return &cli.Error{Code: "reply_route_unavailable", Message: "reply custody endpoint is unavailable", Effect: "not_sent", Exit: cli.ExitRejected, Details: map[string]any{"cause": err.Error()}}
		}
		if ep.Route.Kind != endpoint.RouteUnix {
			return nil
		}
		registry := e.options.Registry
		if registry.Path == "" {
			registry, err = controlreceiver.NewDefaultRegistry()
			if err != nil {
				return err
			}
		}
		if err := controlreceiver.RegisterLocalJournal(prepareCtx, registry, store, codexHome, source.EndpointID, j); err != nil {
			return &cli.Error{Code: "reply_route_unavailable", Message: "local reply custody registration failed", Effect: "not_sent", Exit: cli.ExitRejected, Details: map[string]any{"cause": err.Error()}}
		}
		remoteJournal.LocalEndpointID = source.EndpointID
		return nil
	}
	storeReceipts := receipts.Store{Journal: receiptJournal{Journal: j}}
	historyFactory := func(context.Context, cli.Invocation, mektup.Receipt) (receipts.HistoryPort, error) {
		return runtimeHistory{observe: observe}, nil
	}
	inspectorFactory := func(_ context.Context, operation cli.Invocation) (receipts.TargetInspector, error) {
		return runtimeInspector{resolver: resolverFor(operation), observe: observe}, nil
	}
	var gateFactory messageexecutor.HumanGateFactory
	if e.options.HumanGate != nil {
		gateFactory = func(context.Context, cli.Invocation) (receipts.HumanGate, error) { return e.options.HumanGate, nil }
	}
	importResolver := applicationImportResolver{observe: observe, store: store, codexHome: codexHome, resolverFor: resolverFor}
	return pool, messageexecutor.New(messageexecutor.Ports{Service: serviceFactory, Original: originalFactory, Receipts: &storeReceipts, Input: input, Artifacts: func(context.Context, cli.Invocation) (receipts.SpillWriter, error) {
		if artifacts == nil {
			return nil, nil
		}
		return artifactSpillWriter{store: artifacts}, nil
	}, History: historyFactory, Inspector: inspectorFactory, HumanGate: gateFactory, ImportResolver: importResolver, PrepareCustody: prepareCustody}), nil
}

type receiptJournal struct{ *journal.Journal }

type artifactSpillWriter struct{ store *artifact.Store }

func (w artifactSpillWriter) WriteSpill(ctx context.Context, body []byte, digest string) (string, error) {
	if w.store == nil {
		return "", errors.New("application artifact store is unavailable")
	}
	name := "receipt-" + strings.TrimPrefix(digest, "sha256:")
	receipt, err := w.store.Spill(ctx, name, bytes.NewReader(body), artifact.Options{MediaType: "text/plain", SensitiveOutputPossible: true})
	if err != nil {
		return "", err
	}
	return receipt.Path, nil
}

type applicationSessionFactory struct {
	options        connection.Options
	sshConfig      sshproxy.Config
	sshFactory     sshproxy.ProcessFactory
	dialerForRoute func(endpoint.Route, bool) connection.ClientDialer
}

func (f applicationSessionFactory) Open(ctx context.Context, ep endpoint.Endpoint) (runtime.Session, error) {
	options := f.options
	if f.dialerForRoute != nil {
		options.ClientDialer = f.dialerForRoute(ep.Route, options.ExperimentalAPI)
	} else if ep.Route.Kind == endpoint.RouteSSH {
		config := f.sshConfig
		if config.Host == "" {
			config.Host = ep.Route.SSHHost
		}
		options.ClientDialer = connection.NewSSHClientDialer(config, f.sshFactory)
	}
	return (runtime.ConnectionFactory{Options: options}).Open(ctx, ep)
}

type runtimeHistory struct{ observe *runtime.ObservationAdapter }

func (h runtimeHistory) FullHistory(ctx context.Context, endpointID, threadID string) ([]receipts.HistoryItem, error) {
	items, err := h.observe.FullHistory(ctx, service.ResolvedTarget{EndpointID: endpointID, ThreadID: threadID, URI: "codex://pinned/thread/" + threadID, Loaded: true, Persistent: true})
	if err != nil {
		return nil, err
	}
	history := make([]receipts.HistoryItem, 0, len(items))
	for _, item := range items {
		envelope, parseErr := mektup.ParseEnvelopeString(item.Text)
		if parseErr != nil {
			continue
		}
		history = append(history, receipts.HistoryItem{EndpointID: endpointID, ThreadID: threadID, TurnID: item.TurnID, ItemID: item.NativeItemID, MessageID: envelope.MessageID, ClientMessageID: item.ClientMessageID, InReplyTo: envelope.InReplyTo, Body: []byte(envelope.Body), PayloadSHA256: envelope.PayloadSHA256})
	}
	return history, nil
}

type runtimeInspector struct {
	resolver runtime.ResolverAdapter
	observe  *runtime.ObservationAdapter
}

func (i runtimeInspector) Inspect(ctx context.Context, selector string) (receipts.TargetIdentity, error) {
	target, err := i.resolver.Resolve(ctx, selector)
	if err != nil {
		return receipts.TargetIdentity{}, err
	}
	return receipts.TargetIdentity{EndpointID: target.EndpointID, ThreadID: target.ThreadID, Requested: selector, Resolved: target.URI, Loaded: target.Loaded, Status: "resolved"}, nil
}

type applicationImportResolver struct {
	observe     *runtime.ObservationAdapter
	store       endpoint.EndpointStore
	codexHome   string
	resolverFor func(cli.Invocation) runtime.ResolverAdapter
}

func (r applicationImportResolver) ResolveOriginal(ctx context.Context, inv cli.Invocation, imported receipts.Imported) (service.OriginalResolver, error) {
	if !imported.Trusted && imported.Receipt.Message.MessageID == "" {
		return nil, errors.New("portable receipt has no exact message identity")
	}
	identity := imported.Receipt.Target
	if identity.EndpointID == "" || identity.ThreadID == "" || identity.Resolved == "" {
		return nil, errors.New("portable receipt lacks an exact target route")
	}
	ep, err := r.store.ResolveEndpointID(identity.EndpointID, r.codexHome)
	if err != nil {
		return nil, fmt.Errorf("portable receipt endpoint is not locally established: %w", err)
	}
	address, err := mektup.ParseThreadURI(identity.Resolved)
	if err != nil || address.ThreadID != identity.ThreadID {
		return nil, errors.New("portable receipt target thread identity is inconsistent")
	}
	if address.Endpoint != ep.Alias {
		mapped, mapErr := r.store.ResolveEndpoint(address.Endpoint, r.codexHome)
		if mapErr != nil || mapped.ID != ep.ID {
			return nil, errors.New("portable receipt target alias is not pinned to the configured endpoint")
		}
	}
	return runtime.OriginalResolver{Observe: r.observe, Target: service.ResolvedTarget{EndpointID: ep.ID, ThreadID: identity.ThreadID, URI: identity.Resolved, Loaded: true, Persistent: true}}, nil
}

func (r applicationImportResolver) ResolveWaitReference(_ context.Context, _ cli.Invocation, imported receipts.Imported) (string, error) {
	if imported.Receipt.OperationID == "" {
		return "", errors.New("portable receipt lacks an operation identity")
	}
	return imported.Receipt.OperationID, nil
}

func threadIDFromURI(uri string) string {
	parsed, err := mektup.ParseThreadURI(uri)
	if err != nil {
		return ""
	}
	return parsed.ThreadID
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func needsJournal(inv cli.Invocation) bool {
	if isMessaging(inv) {
		return true
	}
	switch inv.Command {
	case "search", "rpc":
		return true
	case "storage":
		return len(inv.Position) == 0 || inv.Position[0] != "check"
	case "doctor":
		return hasOption(inv, "fix")
	case "endpoint":
		return len(inv.Position) > 0 && (inv.Position[0] == "add" || inv.Position[0] == "remove")
	case "thread":
		return len(inv.Position) > 0 && (inv.Position[0] == "start" || inv.Position[0] == "resume" || inv.Position[0] == "fork")
	default:
		return false
	}
}

func isMessaging(inv cli.Invocation) bool {
	switch inv.Command {
	case "send", "reply", "wait", "inspect", "receipt":
		return true
	default:
		return false
	}
}

func needsArtifacts(inv cli.Invocation) bool {
	return inv.Command == "rpc" || isMessaging(inv) && inv.Command == "receipt" && len(inv.Position) > 0 && inv.Position[0] == "show" && hasOption(inv, "content")
}

func hasOption(inv cli.Invocation, name string) bool { return len(inv.Options[name]) != 0 }

func storagePort(j *journal.Journal) executor.StoragePort {
	if j == nil {
		return nil
	}
	return storage.New(j)
}

func storagePortFor(inv cli.Invocation, j *journal.Journal, stateDir string) executor.StoragePort {
	if inv.Command == "storage" && len(inv.Position) > 0 && inv.Position[0] == "check" {
		return readOnlyStorage{databasePath: filepath.Join(stateDir, "journal.sqlite3")}
	}
	return storagePort(j)
}

type readOnlyStorage struct{ databasePath string }

func (s readOnlyStorage) Status(context.Context) (journal.StorageStatus, error) {
	return journal.StorageStatus{}, errors.New("storage status requires a writable journal")
}
func (s readOnlyStorage) Check(ctx context.Context) (journal.StorageCheck, error) {
	return journal.CheckPath(ctx, s.databasePath)
}
func (s readOnlyStorage) Maintain(context.Context, journal.MaintenanceOptions) (journal.MaintenanceReceipt, error) {
	return journal.MaintenanceReceipt{}, errors.New("storage maintenance requires a writable journal")
}
func (s readOnlyStorage) Vacuum(context.Context) (journal.VacuumReceipt, error) {
	return journal.VacuumReceipt{}, errors.New("storage vacuum requires a writable journal")
}

func mapJournalOpenError(err error) error {
	if isSQLiteBusyOrLocked(err) {
		err = fmt.Errorf("%w: %v", journal.ErrStorageBusy, err)
	}
	code := "storage_corrupt"
	if errors.Is(err, journal.ErrStorageBusy) {
		code = "storage_busy"
	}
	exit := cli.ExitRejected
	if code == "storage_busy" {
		exit = cli.ExitUnknown
	}
	return &cli.Error{Code: code, Message: err.Error(), Effect: "not_sent", Details: map[string]any{"cause": err.Error()}, Exit: exit}
}

func isSQLiteBusyOrLocked(err error) bool {
	var sqliteErr *sqlite.Error
	if errors.As(err, &sqliteErr) {
		code := sqliteErr.Code() & 0xff
		if code == sqlite3.SQLITE_BUSY || code == sqlite3.SQLITE_LOCKED {
			return true
		}
	}
	message := strings.ToUpper(err.Error())
	return strings.Contains(message, "SQLITE_BUSY") || strings.Contains(message, "SQLITE_LOCKED") || strings.Contains(message, "DATABASE IS LOCKED")
}

func (e *Environment) paths(inv cli.Invocation) (string, string) {
	configPath, stateDir := inv.Resolved.Config, inv.Resolved.StateDir
	if configPath == "" {
		configPath = e.options.ConfigPath
	}
	if stateDir == "" {
		stateDir = e.options.StateDir
	}
	// cli's built-in config value is an owner-private directory. Explicit
	// --config/MEKTUP_CONFIG values remain exact file paths for compatibility
	// with endpoint.EndpointStore.
	if inv.Resolved.ConfigSource == cli.PathDefault && filepath.Base(configPath) == "mektup" && filepath.Ext(configPath) == "" {
		configPath = filepath.Join(configPath, "endpoints.json")
	}
	return configPath, stateDir
}

type connectionFacts struct {
	mu     sync.RWMutex
	values map[string]connection.Info
}

func (f *connectionFacts) Set(id string, info connection.Info) {
	if f == nil {
		return
	}
	f.mu.Lock()
	f.values[id] = info
	f.mu.Unlock()
}

func (f *connectionFacts) Get(id string) (connection.Info, bool) {
	if f == nil {
		return connection.Info{}, false
	}
	f.mu.RLock()
	info, ok := f.values[id]
	f.mu.RUnlock()
	return info, ok
}

type endpointPort struct {
	store    endpoint.EndpointStore
	receipts *receiptStore
}

func (p endpointPort) List() ([]endpoint.Endpoint, error)              { return p.store.List() }
func (p endpointPort) Show(selector string) (endpoint.Endpoint, error) { return p.store.Show(selector) }
func (p endpointPort) Add(ep endpoint.Endpoint) error                  { return p.store.Add(ep) }

func (p endpointPort) Remove(selector string) error {
	// Capture the exact endpoint record before the destructive config change.
	// Receipt projection can then use the pinned identity even if the alias is
	// absent or reused by the time the executor emits its receipt.
	ep, err := p.store.Show(selector)
	if err != nil {
		return err
	}
	if p.receipts != nil {
		p.receipts.Pin(selector, ep)
	}
	return p.store.Remove(selector)
}

type connectionFactory struct {
	store          endpoint.EndpointStore
	codexHome      string
	options        connection.Options
	sshConfig      sshproxy.Config
	sshFactory     sshproxy.ProcessFactory
	dialerForRoute func(endpoint.Route, bool) connection.ClientDialer
	openOverride   func(context.Context, string, executor.OpenOptions) (executor.Connection, error)
	facts          *connectionFacts
	pins           *receiptPins
}

func (f *connectionFactory) Open(ctx context.Context, selector string) (executor.Connection, error) {
	return f.OpenWithOptions(ctx, selector, executor.OpenOptions{})
}

func (f *connectionFactory) OpenWithOptions(ctx context.Context, selector string, open executor.OpenOptions) (executor.Connection, error) {
	if f.openOverride != nil {
		return f.openOverride(ctx, selector, open)
	}
	ep, err := f.store.ResolveEndpoint(selector, f.codexHome)
	if err != nil {
		return nil, err
	}
	if err := ep.Validate(); err != nil {
		return nil, err
	}
	if f.pins != nil {
		// Pin before dialing. A later config mutation or alias replacement must
		// not alter the identity attached to this operation's receipt.
		f.pins.Set(selector, ep)
	}
	options := f.options
	options.ExperimentalAPI = open.ExperimentalAPI
	if f.dialerForRoute != nil {
		options.ClientDialer = f.dialerForRoute(ep.Route, open.ExperimentalAPI)
	} else if ep.Route.Kind == endpoint.RouteSSH {
		config := f.sshConfig
		if config.Host == "" {
			config.Host = ep.Route.SSHHost
		}
		options.ClientDialer = connection.NewSSHClientDialer(config, f.sshFactory)
	}
	conn, err := connection.Connect(ctx, ep.Route, options)
	if err != nil {
		return nil, err
	}
	if f.facts != nil {
		f.facts.Set(ep.ID, conn.Info())
	}
	return &appConnection{endpoint: ep, conn: conn, api: codexapi.New(conn, codexapi.Options{Capabilities: conn.Capabilities()}), rpc: connection.NewRPCAdapter(conn)}, nil
}

func (f *connectionFactory) Check(ctx context.Context, selector string) (any, error) {
	opened, err := f.Open(ctx, selector)
	if err != nil {
		return nil, err
	}
	result := map[string]any{"endpoint": selector, "warnings": opened.Warnings()}
	if closeErr := opened.Close(); closeErr != nil {
		return result, &CleanupError{Resource: "endpoint connection", Err: closeErr}
	}
	return result, nil
}

type endpointHealth struct{ factory *connectionFactory }

func (h endpointHealth) Check(ctx context.Context, ep endpoint.Endpoint) (any, error) {
	if h.factory == nil {
		return nil, errors.New("endpoint health factory is unavailable")
	}
	return h.factory.Check(ctx, ep.Alias)
}

type appConnection struct {
	endpoint endpoint.Endpoint
	conn     *connection.Connection
	api      *codexapi.Client
	rpc      rawrpc.Caller
}

func (c *appConnection) Codex() executor.Codex { return c.api }
func (c *appConnection) RPC() rawrpc.Caller    { return c.rpc }
func (c *appConnection) Warnings() []string    { return c.conn.Warnings() }

func (c *appConnection) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return c.conn.Detach(ctx)
}

type doctorPort struct{}

func (doctorPort) Run(ctx context.Context, options doctor.Options, fix bool) (doctor.Report, error) {
	return doctor.Run(ctx, options, fix)
}

type rpcPort struct{}

func (rpcPort) Execute(ctx context.Context, caller rawrpc.Caller, request rawrpc.Request) (*rawrpc.Response, error) {
	return rawrpc.Execute(ctx, caller, request)
}

type receiptStore struct {
	journal   *journal.Journal
	endpoints endpoint.EndpointStore
	codexHome string
	facts     *connectionFacts
	pins      *receiptPins
}

type receiptPins struct {
	mu     sync.RWMutex
	values map[string]endpoint.Endpoint
}

func (p *receiptPins) Set(selector string, ep endpoint.Endpoint) {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.values[selector] = ep
	p.mu.Unlock()
}

func (s *receiptStore) Pin(selector string, ep endpoint.Endpoint) {
	if s == nil || s.pins == nil {
		return
	}
	s.pins.Set(selector, ep)
}

func (s receiptStore) pinned(selector string) (endpoint.Endpoint, bool) {
	if s.pins == nil {
		return endpoint.Endpoint{}, false
	}
	s.pins.mu.RLock()
	ep, ok := s.pins.values[selector]
	s.pins.mu.RUnlock()
	return ep, ok
}

func (s receiptStore) Mutation(ctx context.Context, operation, endpointSelector string, payload any) (any, error) {
	return s.save(ctx, operation, endpointSelector, payload, mektup.StateAccepted)
}

func (s receiptStore) Read(ctx context.Context, operation, endpointSelector string, payload any) (any, error) {
	return s.save(ctx, operation, endpointSelector, payload, mektup.StateAccepted)
}

func (s receiptStore) save(ctx context.Context, operation, endpointSelector string, payload any, state mektup.EvidenceState) (mektup.Receipt, error) {
	ep, ok := s.pinned(endpointSelector)
	if !ok {
		var err error
		ep, err = s.endpoints.ResolveEndpoint(endpointSelector, s.codexHome)
		if err != nil {
			return mektup.Receipt{}, err
		}
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return mektup.Receipt{}, err
	}
	digest := sha256.Sum256(encoded)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	identity := mektup.ReceiptIdentity{EndpointID: ep.ID, Alias: ep.Alias, Transport: string(ep.Route.Kind), Requested: endpointSelector, Resolved: ep.Alias}
	warnings := []mektup.Warning{}
	if info, found := s.facts.Get(ep.ID); found {
		identity.ServerVersion = info.DaemonVersion
		identity.Compatibility = string(info.Compatibility.Class)
		warnings = warningValues(info.Warnings)
	}
	threadID := payloadThreadID(payload)
	identity.ThreadID = threadID
	receipt := mektup.Receipt{
		Schema: mektup.ReceiptSchema, ReceiptID: mektup.NewReceiptID(), OperationID: mektup.NewOperationID(), Operation: operation, State: state,
		Source: identity, Target: identity,
		Message:  mektup.ReceiptMessage{MessageID: mektup.NewMessageID(), Kind: string(mektup.KindMessage), PayloadBytes: uint64(len(encoded)), PayloadSHA256: "sha256:" + hex.EncodeToString(digest[:])},
		Evidence: []mektup.EvidenceRecord{{State: state, At: now, Kind: operation}}, Warnings: warnings, CreatedAt: now, UpdatedAt: now,
	}
	if err := s.journal.PutReceipt(ctx, receipt); err != nil {
		return mektup.Receipt{}, err
	}
	return receipt, nil
}

func (s receiptStore) ReadReceipt(ctx context.Context, reference string) (mektup.Receipt, error) {
	return s.journal.Receipt(ctx, reference)
}

func payloadThreadID(payload any) string {
	object, ok := payload.(map[string]any)
	if !ok {
		return ""
	}
	if value, ok := object["threadId"].(string); ok {
		return value
	}
	if nested, ok := object["thread"].(map[string]any); ok {
		if value, ok := nested["id"].(string); ok {
			return value
		}
	}
	return ""
}

func warningValues(values []string) []mektup.Warning {
	result := make([]mektup.Warning, 0, len(values))
	for _, value := range values {
		code := mektup.WarningCode(value)
		switch code {
		case mektup.WarningUntestedServerVersion, mektup.WarningServerVersionUnknown, mektup.WarningEvidenceGap, mektup.WarningProjectionMayLag, mektup.WarningAuditLoggingEnabled, mektup.WarningOutputSpilled, mektup.WarningResolverDegraded, mektup.WarningCleanupIncomplete, mektup.WarningManualResolution:
			result = append(result, mektup.Warning{Code: code, Message: value, Details: map[string]any{}})
		}
	}
	return result
}

func readFileBounded(ctx context.Context, name string, max int64) ([]byte, error) {
	if strings.TrimSpace(name) == "" || strings.ContainsRune(name, 0) {
		return nil, errors.New("input path is empty or contains NUL")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	file, err := openInputFile(name)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("input path is not a regular file")
	}
	data, err := io.ReadAll(io.LimitReader(file, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > max {
		return nil, errors.New("input exceeds its byte limit")
	}
	return data, nil
}

var _ executor.ConnectionFactory = (*connectionFactory)(nil)
var _ executor.EndpointChecker = endpointHealth{}
var _ executor.ReceiptPort = receiptStore{}
var _ executor.ReadReceiptPort = receiptStore{}
var _ executor.RPCPort = rpcPort{}
var _ executor.DoctorPort = doctorPort{}
