// Package application composes the production ports for the approved,
// non-messaging Mektup commands. It never starts or supervises a daemon.
//
// Resources are invocation-scoped: CLI-resolved config/state paths are applied
// before opening the journal, endpoint store, and artifact root, then all
// resources are closed before Execute returns.
package application

import (
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
	"time"

	mektup "github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/internal/artifact"
	"github.com/agensfield/mektup/go/internal/cli"
	"github.com/agensfield/mektup/go/internal/codexapi"
	"github.com/agensfield/mektup/go/internal/connection"
	"github.com/agensfield/mektup/go/internal/doctor"
	"github.com/agensfield/mektup/go/internal/endpoint"
	"github.com/agensfield/mektup/go/internal/executor"
	"github.com/agensfield/mektup/go/internal/journal"
	"github.com/agensfield/mektup/go/internal/rawrpc"
	"github.com/agensfield/mektup/go/internal/sshproxy"
	"github.com/agensfield/mektup/go/internal/storage"
)

// Options controls production composition. DialerForRoute is a deterministic
// seam for tests and embedders; when absent, Unix routes use appserver's
// direct Unix socket and SSH routes use the bounded OpenSSH proxy bridge.
type Options struct {
	Input       io.Reader
	CodexHome   string
	ConfigPath  string
	StateDir    string
	ArtifactDir string

	Connection connection.Options
	SSHConfig  sshproxy.Config
	SSHFactory sshproxy.ProcessFactory

	DialerForRoute func(endpoint.Route, bool) connection.ClientDialer
}

// Environment implements cli.Executor and is the composition seam for a
// future messaging executor/composite. It intentionally exposes no messaging
// executor itself.
type Environment struct {
	options Options
	closed  bool
}

func New(options Options) *Environment { return &Environment{options: options} }

// NewEnvironment is the explicit constructor spelling for embedders that
// compose this production environment with another command executor.
func NewEnvironment(options Options) *Environment { return New(options) }

var _ cli.Executor = (*Environment)(nil)

// Execute opens only the resources needed by the production executor for this
// invocation. No path is taken that starts a daemon.
func (e *Environment) Execute(ctx context.Context, inv cli.Invocation) (result cli.ExecutionResult, execErr error) {
	if e == nil || e.closed {
		return cli.ExecutionResult{}, &cli.Error{Code: "internal_error", Message: "application environment is closed", Effect: "not_sent", Exit: cli.ExitInternal}
	}
	resources, err := e.openResources(ctx, inv)
	if err != nil {
		return cli.ExecutionResult{}, err
	}
	defer func() {
		if closeErr := resources.Close(); closeErr != nil {
			if execErr == nil {
				execErr = &CleanupError{Resource: "application resources", Err: closeErr}
			} else {
				execErr = errors.Join(execErr, &CleanupError{Resource: "application resources", Err: closeErr})
			}
		}
	}()
	return executor.New(resources.Ports()).Execute(ctx, inv)
}

// Close prevents future invocations. Resources are invocation-scoped and are
// already closed by Execute, so this operation is deliberately idempotent.
func (e *Environment) Close() error {
	if e == nil {
		return nil
	}
	e.closed = true
	return nil
}

// CleanupError identifies a resource cleanup failure without hiding the
// operation's result or its original error.
type CleanupError struct {
	Resource string
	Err      error
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
	journal  *journal.Journal
	artifact *artifact.Store
	ports    resourcePorts
}

func (r *resources) Ports() executor.Ports { return r.ports.Ports }

func (r *resources) Close() error {
	if r == nil {
		return nil
	}
	var joined error
	if r.artifact != nil {
		joined = errors.Join(joined, r.artifact.Close())
	}
	if r.journal != nil {
		joined = errors.Join(joined, r.journal.Close())
	}
	return joined
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
	j, err := journal.Open(ctx, journal.Options{StateDir: stateDir})
	if err != nil {
		return nil, fmt.Errorf("open application journal: %w", err)
	}
	artifactRoot := e.options.ArtifactDir
	if artifactRoot == "" {
		artifactRoot = filepath.Join(stateDir, "artifacts")
	}
	artifacts, err := artifact.NewStore(artifactRoot)
	if err != nil {
		_ = j.Close()
		return nil, fmt.Errorf("open application artifact store: %w", err)
	}

	store := endpoint.NewStore(configPath, stateDir)
	connections := &connectionFactory{store: store, codexHome: e.options.CodexHome, options: e.options.Connection, sshConfig: e.options.SSHConfig, sshFactory: e.options.SSHFactory, dialerForRoute: e.options.DialerForRoute}
	receipts := receiptStore{journal: j, endpoints: store, codexHome: e.options.CodexHome}
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
		Endpoints:      store,
		EndpointHealth: endpointHealth{factory: connections},
		Storage:        storage.New(j),
		Doctor:         doctorPort{},
		Input:          input,
		Artifacts:      executor.ArtifactStoreOutput{Store: artifacts},
		RPC:            rpcPort{},
		Receipts:       receipts,
		ReadReceipts:   receipts,
		DoctorOptions:  doctorOptions,
	}
	return &resources{journal: j, artifact: artifacts, ports: resourcePorts{Ports: ports}}, nil
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
	if inv.Global.Config == "" && os.Getenv("MEKTUP_CONFIG") == "" && filepath.Base(configPath) == "mektup" && filepath.Ext(configPath) == "" {
		configPath = filepath.Join(configPath, "endpoints.json")
	}
	return configPath, stateDir
}

type connectionFactory struct {
	store          endpoint.EndpointStore
	codexHome      string
	options        connection.Options
	sshConfig      sshproxy.Config
	sshFactory     sshproxy.ProcessFactory
	dialerForRoute func(endpoint.Route, bool) connection.ClientDialer
}

func (f *connectionFactory) Open(ctx context.Context, selector string) (executor.Connection, error) {
	return f.OpenWithOptions(ctx, selector, executor.OpenOptions{})
}

func (f *connectionFactory) OpenWithOptions(ctx context.Context, selector string, open executor.OpenOptions) (executor.Connection, error) {
	ep, err := f.store.ResolveEndpoint(selector, f.codexHome)
	if err != nil {
		return nil, err
	}
	if err := ep.Validate(); err != nil {
		return nil, err
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
	return &appConnection{endpoint: ep, conn: conn, api: codexapi.New(conn, codexapi.Options{Capabilities: conn.Capabilities()}), rpc: connection.NewRPCAdapter(conn)}, nil
}

func (f *connectionFactory) Check(ctx context.Context, selector string) (any, error) {
	opened, err := f.Open(ctx, selector)
	if err != nil {
		return nil, err
	}
	defer opened.Close()
	return map[string]any{"endpoint": selector, "warnings": opened.Warnings()}, nil
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
}

func (s receiptStore) Mutation(ctx context.Context, operation, endpointSelector string, payload any) (any, error) {
	return s.save(ctx, operation, endpointSelector, payload, mektup.StateAccepted)
}

func (s receiptStore) Read(ctx context.Context, operation, endpointSelector string, payload any) (any, error) {
	return s.save(ctx, operation, endpointSelector, payload, mektup.StateAccepted)
}

func (s receiptStore) save(ctx context.Context, operation, endpointSelector string, payload any, state mektup.EvidenceState) (mektup.Receipt, error) {
	ep, err := s.endpoints.ResolveEndpoint(endpointSelector, s.codexHome)
	if err != nil {
		return mektup.Receipt{}, err
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return mektup.Receipt{}, err
	}
	digest := sha256.Sum256(encoded)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	identity := mektup.ReceiptIdentity{EndpointID: ep.ID, Alias: ep.Alias, Transport: string(ep.Route.Kind), Requested: endpointSelector, Resolved: ep.Alias}
	receipt := mektup.Receipt{
		Schema: mektup.ReceiptSchema, ReceiptID: mektup.NewReceiptID(), OperationID: mektup.NewOperationID(), Operation: operation, State: state,
		Source: identity, Target: identity,
		Message:  mektup.ReceiptMessage{MessageID: mektup.NewMessageID(), Kind: string(mektup.KindMessage), PayloadBytes: uint64(len(encoded)), PayloadSHA256: "sha256:" + hex.EncodeToString(digest[:])},
		Evidence: []mektup.EvidenceRecord{{State: state, At: now, Kind: operation}}, Warnings: []mektup.Warning{}, CreatedAt: now, UpdatedAt: now,
	}
	if err := s.journal.PutReceipt(ctx, receipt); err != nil {
		return mektup.Receipt{}, err
	}
	return receipt, nil
}

func (s receiptStore) ReadReceipt(ctx context.Context, reference string) (mektup.Receipt, error) {
	return s.journal.Receipt(ctx, reference)
}

func readFileBounded(ctx context.Context, name string, max int64) ([]byte, error) {
	if strings.TrimSpace(name) == "" || strings.ContainsRune(name, 0) {
		return nil, errors.New("input path is empty or contains NUL")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	file, err := os.Open(name)
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
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
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
