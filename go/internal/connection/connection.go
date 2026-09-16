// Package connection is the bounded, compatibility-gated edge between an
// endpoint route and the pinned Codex API.
//
// It only connects to an already-running app-server. It never starts,
// restarts, reconnects, or falls back to a daemon or proxy process.
package connection

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/agensfield/mektup/go/appserver"
	"github.com/agensfield/mektup/go/internal/codexapi"
	"github.com/agensfield/mektup/go/internal/compat"
	"github.com/agensfield/mektup/go/internal/endpoint"
	"github.com/agensfield/mektup/go/internal/sshproxy"
	"github.com/agensfield/mektup/go/internal/sshtransport"
)

var (
	ErrNotReady            = errors.New("mektup connection is not ready")
	ErrUnsupported         = errors.New("mektup connection server is unsupported")
	ErrSSHUninjected       = errors.New("mektup SSH dialing requires an injected dialer")
	ErrEndpointUnavailable = errors.New("mektup endpoint is unavailable")
)

// Dialer opens the transport for one already-resolved route. The adapter
// owns appserver.Client construction, so every route shares the same
// initialization and event-stream semantics. An injected implementation is
// the seam for SSH and deterministic tests.
type Dialer interface {
	Dial(context.Context, endpoint.Route) (appserver.Transport, error)
}

type DialFunc func(context.Context, endpoint.Route) (appserver.Transport, error)

func (f DialFunc) Dial(ctx context.Context, route endpoint.Route) (appserver.Transport, error) {
	return f(ctx, route)
}

// ClientDialer opens a complete appserver client. It is useful for the
// production Unix implementation while Dialer remains the injectable SSH
// seam used by tests and future proxy transports.
type ClientDialer interface {
	DialClient(context.Context, endpoint.Route, appserver.Options) (*appserver.Client, error)
}

type ClientDialFunc func(context.Context, endpoint.Route, appserver.Options) (*appserver.Client, error)

func (f ClientDialFunc) DialClient(ctx context.Context, route endpoint.Route, options appserver.Options) (*appserver.Client, error) {
	return f(ctx, route, options)
}

// NewSSHClientDialer returns the production SSH bridge for Connect's
// Options.ClientDialer seam. The remote app-server daemon is not started by
// this constructor; sshproxy only relays its raw HTTP Upgrade/WebSocket bytes.
func NewSSHClientDialer(config sshproxy.Config, factory sshproxy.ProcessFactory) ClientDialer {
	return sshtransport.NewClientDialer(config, factory)
}

type defaultClientDialer struct{}

func (defaultClientDialer) DialClient(ctx context.Context, route endpoint.Route, options appserver.Options) (*appserver.Client, error) {
	if err := route.Validate(); err != nil {
		return nil, err
	}
	if route.Kind == endpoint.RouteSSH {
		return nil, ErrSSHUninjected
	}
	return appserver.DialUnix(ctx, route.UnixSocket, options)
}

// ProxyEvidence is optional evidence about a proxy executable. It is never
// used as the daemon's identity or compatibility authority.
type ProxyEvidence struct {
	Executable string
	Version    string
}

// Options controls client identity, initialization, and bounded event/write
// queues. A ClientDialer takes precedence over Dialer when both are supplied.
type Options struct {
	ClientName                string
	ClientVersion             string
	ExperimentalAPI           bool
	OptOutNotificationMethods []string
	HandshakeTimeout          time.Duration
	EventCapacity             int
	WriterCapacity            int
	Dialer                    Dialer
	ClientDialer              ClientDialer
	Proxy                     ProxyEvidence
}

// Info is the immutable evidence captured during initialization.
type Info struct {
	Route           endpoint.Route
	Generation      uint64
	DaemonVersion   string
	ServerUserAgent string
	Compatibility   compat.Result
	Warnings        []string
	Proxy           ProxyEvidence
}

func (i Info) copy() Info {
	i.Warnings = append([]string(nil), i.Warnings...)
	return i
}

// CompatibilityError preserves the authoritative handshake classification
// when the server is rejected before any operational request is written.
type CompatibilityError struct {
	Result     compat.Result
	Generation uint64
}

func (e *CompatibilityError) Error() string {
	if e == nil {
		return ErrUnsupported.Error()
	}
	return fmt.Sprintf("%s: app-server %q (%s)", ErrUnsupported, e.Result.UserAgent, e.Result.Version)
}

func (e *CompatibilityError) Unwrap() error { return ErrUnsupported }

// CallError is the adapter-specific error returned by Call. It preserves the
// appserver write evidence and, for JSON-RPC failures, the converted raw
// server error. The underlying appserver error remains available through
// Unwrap for diagnostics.
type CallError struct {
	Err        error
	Server     *codexapi.ServerError
	Evidence   appserver.WriteEvidence
	Generation uint64
}

// RawCall is the lossless low-level outcome of one operational request. A
// JSON-RPC server error is evidence, not a local Go failure, and therefore is
// populated in ServerError with a nil returned error. Local transport and
// cancellation failures return an error while retaining Evidence and
// Generation here as well.
type RawCall struct {
	Result      json.RawMessage
	ServerError *codexapi.ServerError
	Evidence    appserver.WriteEvidence
	Generation  uint64
}

// RawCallResult is a descriptive alias for callers that prefer result-shaped
// naming.
type RawCallResult = RawCall

func (e *CallError) Error() string {
	if e == nil {
		return "mektup connection call failed"
	}
	if e.Server != nil {
		return e.Server.Error()
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	return "mektup connection call failed"
}

func (e *CallError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// Connection owns one appserver generation. A successful constructor has
// completed initialize, sent initialized, and passed the compatibility gate.
type Connection struct {
	route           endpoint.Route
	client          *appserver.Client
	info            Info
	experimentalAPI bool

	mu     sync.RWMutex
	closed bool
	nextID atomic.Uint64
}

var _ codexapi.Caller = (*Connection)(nil)

// RPCAdapter exposes the low-level appserver request/result contract without
// exposing Connection's underlying client. It is intended for raw-RPC layers
// that need caller-selected request IDs and appserver's untouched evidence.
type RPCAdapter struct{ connection *Connection }

// NewRPCAdapter constructs a raw-RPC adapter over one compatibility-gated
// connection. A nil connection is retained as a safe, closed adapter.
func NewRPCAdapter(connection *Connection) *RPCAdapter {
	return &RPCAdapter{connection: connection}
}

var _ interface {
	Call(context.Context, appserver.RPCRequest) (*appserver.RPCResult, error)
	ExperimentalAPIEnabled() bool
} = (*RPCAdapter)(nil)

// ExperimentalAPIEnabled reports the negotiated initialize capability.
func (a *RPCAdapter) ExperimentalAPIEnabled() bool {
	if a == nil || a.connection == nil {
		return false
	}
	return a.connection.ExperimentalAPIEnabled()
}

// Call delegates exactly one request after the connection's initialized,
// compatibility, and closed gate. The appserver result and error are returned
// untouched, including caller-selected IDs, nested server data, generation,
// and write evidence.
func (a *RPCAdapter) Call(ctx context.Context, request appserver.RPCRequest) (*appserver.RPCResult, error) {
	if a == nil {
		return nil, &appserver.CallError{Err: ErrNotReady, Evidence: appserver.WriteEvidence{Phase: appserver.WriteProvenBeforeWrite}}
	}
	client, _, gateErr := a.connection.operationalClient()
	if gateErr != nil {
		return nil, &appserver.CallError{Err: gateErr.Err, Evidence: gateErr.Evidence, Generation: gateErr.Generation}
	}
	return client.Call(ctx, request)
}

// Connect resolves and opens exactly the supplied route, completes the full
// appserver handshake, and gates operational use on compatibility. On an
// unsupported server the client is closed and CompatibilityError retains the
// authoritative evidence.
func Connect(ctx context.Context, route endpoint.Route, options Options) (*Connection, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := route.Validate(); err != nil {
		return nil, err
	}
	appOptions := appserver.Options{
		ClientName:                options.ClientName,
		ClientVersion:             options.ClientVersion,
		ExperimentalAPI:           options.ExperimentalAPI,
		OptOutNotificationMethods: append([]string(nil), options.OptOutNotificationMethods...),
		HandshakeTimeout:          options.HandshakeTimeout,
		EventCapacity:             options.EventCapacity,
		WriterCapacity:            options.WriterCapacity,
	}
	var (
		client *appserver.Client
		err    error
	)
	if options.ClientDialer != nil {
		client, err = options.ClientDialer.DialClient(ctx, route, appOptions)
	} else if options.Dialer != nil {
		var transport appserver.Transport
		transport, err = options.Dialer.Dial(ctx, route)
		if err == nil {
			if transport == nil {
				err = errors.New("mektup connection dialer returned a nil transport")
			} else {
				client = appserver.New(transport, appOptions)
			}
		}
	} else {
		client, err = (defaultClientDialer{}).DialClient(ctx, route, appOptions)
	}
	if err != nil {
		// Dial errors happen before an app-server client exists, so no
		// operation can have been written. Preserve the underlying cause for
		// diagnostics and context cancellation while giving callers a stable
		// route-unavailable classification.
		if errors.Is(err, ErrSSHUninjected) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: %w", ErrEndpointUnavailable, err)
	}
	if client == nil {
		return nil, errors.New("mektup connection dialer returned a nil client")
	}
	server, err := client.Initialize(ctx)
	if err != nil {
		return nil, err
	}
	classification, err := compat.Classify(server.UserAgent)
	if err != nil {
		closeClientBounded(client)
		return nil, err
	}
	if err := compat.RequireSupported(classification); err != nil {
		generation := client.Generation()
		closeClientBounded(client)
		return nil, &CompatibilityError{Result: classification, Generation: generation}
	}
	warnings := []string(nil)
	if classification.Warning != "" {
		warnings = []string{classification.Warning}
	}
	return &Connection{
		route:  route,
		client: client,
		info: Info{
			Route:           route,
			Generation:      client.Generation(),
			DaemonVersion:   server.ServerVersion,
			ServerUserAgent: server.UserAgent,
			Compatibility:   classification,
			Warnings:        warnings,
			Proxy:           options.Proxy,
		},
		experimentalAPI: options.ExperimentalAPI,
	}, nil
}

func closeClientBounded(client *appserver.Client) {
	if client == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = client.Close(ctx)
}

func (c *Connection) Info() Info {
	if c == nil {
		return Info{}
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.info.copy()
}

func (c *Connection) Generation() uint64 { return c.Info().Generation }

func (c *Connection) Compatibility() compat.Result { return c.Info().Compatibility }

func (c *Connection) Warnings() []string { return c.Info().Warnings }

func (c *Connection) Capabilities() codexapi.Capabilities {
	if c == nil {
		return codexapi.Capabilities{}
	}
	return codexapi.Capabilities{ExperimentalAPI: c.ExperimentalAPIEnabled()}
}

// ExperimentalAPIEnabled reports the experimentalApi capability negotiated for
// this connection's initialize request. It is intentionally nil-safe so a
// rawrpc adapter can probe an optional capability without dereferencing an
// absent connection.
func (c *Connection) ExperimentalAPIEnabled() bool {
	if c == nil {
		return false
	}
	// The capability is a client-side initialization fact. Keeping it on the
	// adapter avoids making proxy metadata appear to be server authority.
	return c.experimentalAPI
}

func (c *Connection) operationalClient() (*appserver.Client, Info, *CallError) {
	if c == nil {
		return nil, Info{}, &CallError{Err: ErrNotReady, Evidence: appserver.WriteEvidence{Phase: appserver.WriteProvenBeforeWrite}}
	}
	c.mu.RLock()
	client := c.client
	closed := c.closed
	info := c.info
	c.mu.RUnlock()
	if closed || client == nil || info.Compatibility.Class == compat.Unsupported {
		evidence := appserver.WriteEvidence{Phase: appserver.WriteProvenBeforeWrite, Generation: info.Generation}
		return nil, info, &CallError{Err: ErrNotReady, Evidence: evidence, Generation: info.Generation}
	}
	return client, info, nil
}

// Call implements codexapi.Caller. The compatibility and initialization gates
// are checked before reaching appserver.Client.Call, so an operational request
// can never bypass this layer.
func (c *Connection) Call(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, *codexapi.ServerError, error) {
	out, err := c.CallDetailed(ctx, method, params)
	if err != nil {
		return nil, nil, err
	}
	if out.ServerError != nil {
		return nil, out.ServerError, nil
	}
	return out.Result, nil, nil
}

// RawCall invokes one operational request and retains write evidence on both
// success and failure. It is an alias for CallDetailed for raw-RPC callers.
func (c *Connection) RawCall(ctx context.Context, method string, params json.RawMessage) (RawCall, error) {
	return c.CallDetailed(ctx, method, params)
}

// CallDetailed invokes one operational request without exposing appserver's
// request-id machinery. It dispatches exactly once and returns JSON-RPC server
// errors as data, matching codexapi.Caller's second-return contract.
func (c *Connection) CallDetailed(ctx context.Context, method string, params json.RawMessage) (RawCall, error) {
	client, info, gateErr := c.operationalClient()
	if gateErr != nil {
		return RawCall{Evidence: gateErr.Evidence, Generation: gateErr.Generation}, gateErr
	}
	result, err := client.Call(ctx, appserver.RPCRequest{ID: fmt.Sprintf("mektup:%d", c.nextID.Add(1)), Method: method, Params: params})
	if err != nil {
		if serverErr := serverErrorFrom(err); serverErr != nil {
			var low *appserver.CallError
			evidence := appserver.WriteEvidence{Phase: appserver.WriteMayHaveWritten, Generation: info.Generation}
			if errors.As(err, &low) {
				evidence = low.Evidence
			}
			return RawCall{ServerError: serverErr, Evidence: evidence, Generation: evidence.Generation}, nil
		}
		adapterErr := callErrorFrom(err, info.Generation)
		return RawCall{Evidence: adapterErr.Evidence, Generation: adapterErr.Generation}, adapterErr
	}
	if result == nil {
		evidence := appserver.WriteEvidence{Phase: appserver.WriteMayHaveWritten, Generation: info.Generation}
		return RawCall{Evidence: evidence, Generation: info.Generation}, &CallError{
			Err:        errors.New("appserver returned a nil result"),
			Evidence:   evidence,
			Generation: info.Generation,
		}
	}
	evidence := result.Evidence
	if evidence.Generation == 0 {
		evidence.Generation = result.Generation
	}
	if evidence.Generation == 0 {
		evidence.Generation = info.Generation
	}
	return RawCall{Result: append(json.RawMessage(nil), result.Value...), Evidence: evidence, Generation: evidence.Generation}, nil
}

func callErrorFrom(err error, generation uint64) *CallError {
	callErr := &CallError{
		Err:        err,
		Evidence:   appserver.WriteEvidence{Phase: appserver.WriteMayHaveWritten, Generation: generation},
		Generation: generation,
	}
	var low *appserver.CallError
	if errors.As(err, &low) {
		callErr.Evidence = low.Evidence
		callErr.Generation = low.Generation
	}
	if server := serverErrorFrom(err); server != nil {
		callErr.Server = server
	}
	return callErr
}

func serverErrorFrom(err error) *codexapi.ServerError {
	var low *appserver.CallError
	if !errors.As(err, &low) || low.Server == nil {
		return nil
	}
	data := append(json.RawMessage(nil), low.Server.Data...)
	raw, marshalErr := json.Marshal(struct {
		ID      any             `json:"id"`
		Code    int64           `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data,omitempty"`
	}{ID: low.Server.ID, Code: low.Server.Code, Message: low.Server.Message, Data: data})
	if marshalErr != nil {
		raw = nil
	}
	return &codexapi.ServerError{Code: low.Server.Code, Message: low.Server.Message, Data: data, Raw: raw}
}

// Events exposes the appserver's bounded observer stream. Consumers must
// treat gaps and disconnection as evidence, not as implicit reconnection.
func (c *Connection) Events() <-chan appserver.Event {
	if c == nil || c.client == nil {
		return closedEvents()
	}
	return c.client.Events()
}

func closedEvents() <-chan appserver.Event {
	ch := make(chan appserver.Event)
	close(ch)
	return ch
}

func (c *Connection) NextEvent(ctx context.Context) (appserver.Event, error) {
	if c == nil || c.client == nil {
		return appserver.Event{}, io.EOF
	}
	return c.client.NextEvent(ctx)
}

// Close detaches this client from the existing app-server. It is bounded by
// ctx and never attempts to stop or restart the daemon.
func (c *Connection) Close(ctx context.Context) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	client := c.client
	// Mark the public gate closed immediately, but still delegate every Close
	// call to appserver. If a caller's first bounded wait expires, a later
	// Close with a fresh context must be able to wait for the same shutdown.
	c.closed = true
	c.mu.Unlock()
	if client == nil {
		return nil
	}
	return client.Close(ctx)
}

// Detach is the explicit lifecycle spelling used by observers and waiters.
// It has the same bounded, non-daemon-stopping behavior as Close.
func (c *Connection) Detach(ctx context.Context) error { return c.Close(ctx) }
