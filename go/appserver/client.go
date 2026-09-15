package appserver

// This package is deliberately a low-level transport boundary. It preserves
// JSON-RPC values rather than trying to model the whole Codex schema, because
// Mektup's semantic layer needs an escape hatch for methods added by Codex.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultQueueCapacity = 64
	defaultHandshakeWait = 10 * time.Second
)

// FrameType identifies the useful subset of WebSocket frames. Control frames
// are consumed by the concrete WebSocket transport and can be ignored by a
// deterministic test transport.
type FrameType uint8

const (
	FrameText FrameType = iota
	FrameBinary
	FramePing
	FramePong
	FrameClose
)

// Frame is one transport frame. Only text frames contain JSON-RPC messages.
type Frame struct {
	Type    FrameType
	Payload []byte
}

// Transport is intentionally small so tests can inject a deterministic
// transport without opening a socket. Read must continue draining the peer;
// Close must unblock a pending Read.
type Transport interface {
	Read(context.Context) (Frame, error)
	Write(context.Context, []byte) error
	Close() error
}

// WritePhase is evidence about how far a request got before a transport
// failure or cancellation. It is not a guess about server-side acceptance.
type WritePhase uint8

const (
	WriteNotStarted WritePhase = iota
	WriteProvenBeforeWrite
	WriteMayHaveWritten
	WriteComplete
)

func (p WritePhase) String() string {
	switch p {
	case WriteProvenBeforeWrite:
		return "proven-before-write"
	case WriteMayHaveWritten:
		return "may-have-written"
	case WriteComplete:
		return "complete"
	default:
		return "not-started"
	}
}

// WriteFailure lets an injected transport report stronger pre-write evidence.
// A transport error without this type is conservatively may-have-written.
type WriteFailure struct {
	Err   error
	Phase WritePhase
}

func (e *WriteFailure) Error() string {
	if e == nil || e.Err == nil {
		return "websocket write failed"
	}
	return e.Err.Error()
}

func (e *WriteFailure) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// WriteEvidence accompanies every call outcome that reached the writer.
type WriteEvidence struct {
	Phase      WritePhase
	Generation uint64
}

// RequestID keeps string and integer JSON-RPC IDs distinct. Values are
// accepted as string, integer, json.Number, or json.RawMessage.
type RequestID = any

// RPCRequest is a raw JSON-RPC request. ID must be a string or number.
type RPCRequest struct {
	ID     RequestID       `json:"id"`
	Method string          `json:"method"`
	Params json.RawMessage `json:"params,omitempty"`
}

// Short aliases keep the raw layer pleasant to use without changing its
// intentionally schema-agnostic representation.
type Request = RPCRequest

// RPCNotification is a raw JSON-RPC notification.
type RPCNotification struct {
	Method     string          `json:"method"`
	Params     json.RawMessage `json:"params,omitempty"`
	Generation uint64          `json:"-"`
}

type Notification = RPCNotification

// RPCResult is the raw result value returned by the app-server.
type RPCResult struct {
	ID         RequestID
	Value      json.RawMessage
	Evidence   WriteEvidence
	Generation uint64
}

type Result = RPCResult

// ServerError is the unmodified JSON-RPC error object.
type ServerError struct {
	ID         RequestID
	Code       int64
	Message    string
	Data       json.RawMessage
	Generation uint64
}

func (e *ServerError) Error() string {
	if e == nil {
		return "app-server request failed"
	}
	if e.Message == "" {
		return fmt.Sprintf("app-server error %d", e.Code)
	}
	return fmt.Sprintf("app-server error %d: %s", e.Code, e.Message)
}

// CallError separates a local transport/cancellation failure from a server
// error. Server remains populated for a JSON-RPC error response.
type CallError struct {
	Err        error
	Server     *ServerError
	Evidence   WriteEvidence
	Canceled   bool
	Generation uint64
}

func (e *CallError) Error() string {
	if e == nil {
		return "app-server call failed"
	}
	if e.Server != nil {
		return e.Server.Error()
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	if e.Canceled {
		return "app-server call canceled"
	}
	return "app-server call failed"
}

func (e *CallError) Unwrap() error {
	if e == nil {
		return nil
	}
	if e.Err != nil {
		return e.Err
	}
	return e.Server
}

// InitializeParams are the stable fields sent to Codex 0.154.0.
type InitializeParams struct {
	ClientInfo   ClientInfo             `json:"clientInfo"`
	Capabilities InitializeCapabilities `json:"capabilities,omitempty"`
}

type ClientInfo struct {
	Name    string `json:"name"`
	Title   string `json:"title,omitempty"`
	Version string `json:"version"`
}

type InitializeCapabilities struct {
	ExperimentalAPI           bool     `json:"experimentalApi,omitempty"`
	OptOutNotificationMethods []string `json:"optOutNotificationMethods,omitempty"`
}

// ServerInfo is parsed from the initialize response. UserAgent is required;
// other fields are retained when the running server supplies them.
type ServerInfo struct {
	UserAgent      string
	ServerVersion  string
	CodexHome      string
	PlatformFamily string
	PlatformOS     string
	Generation     uint64
}

type EventKind uint8

const (
	EventNotification EventKind = iota
	EventServerRequest
	EventGap
	EventDisconnected
)

// Event is the bounded, observer-only stream exposed to callers.
type Event struct {
	Kind         EventKind
	Notification *RPCNotification
	Request      *ServerRequest
	Gap          *Gap
	Disconnected *Disconnected
}

// ServerRequest is delivered for known and unknown methods alike. The client
// never sends a response or error for any server request.
type ServerRequest struct {
	ID         RequestID
	Method     string
	Params     json.RawMessage
	Generation uint64
}

type Gap struct {
	Dropped    uint64
	Generation uint64
	Reason     string
}

type Disconnected struct {
	Err        error
	Generation uint64
}

// Options controls queue bounds and handshake identity.
type Options struct {
	ClientName                string
	ClientVersion             string
	ExperimentalAPI           bool
	OptOutNotificationMethods []string
	EventCapacity             int
	HandshakeTimeout          time.Duration
	WriterCapacity            int
}

func (o Options) normalized() Options {
	if o.ClientName == "" {
		o.ClientName = "mektup"
	}
	if o.ClientVersion == "" {
		o.ClientVersion = "dev"
	}
	if o.EventCapacity <= 0 {
		o.EventCapacity = defaultQueueCapacity
	}
	if o.WriterCapacity <= 0 {
		o.WriterCapacity = defaultQueueCapacity
	}
	if o.HandshakeTimeout <= 0 {
		o.HandshakeTimeout = defaultHandshakeWait
	}
	return o
}

type commandKind uint8

const (
	commandRequest commandKind = iota
	commandNotify
	commandCancel
	commandClose
)

type command struct {
	kind       commandKind
	ctx        context.Context
	req        RPCRequest
	notify     RPCNotification
	key        string
	result     chan callOutcome
	ack        chan cancelOutcome
	notifyDone chan error
}

type callOutcome struct {
	result *RPCResult
	err    *CallError
}

type cancelOutcome struct {
	phase     WritePhase
	ok        bool
	completed bool
	err       error
}

type pendingCall struct {
	result     chan callOutcome
	generation uint64
	phase      WritePhase
}

// Client owns one connection generation and one continuously draining pump.
// It intentionally has no reconnect or replay behavior.
type Client struct {
	transport  Transport
	options    Options
	generation uint64

	commands     chan command
	commandSpace chan struct{}
	reads        chan readResult
	events       chan Event
	done         chan struct{}
	closed       chan struct{}

	mu            sync.Mutex
	reserved      map[string]struct{}
	withdrawn     map[string]struct{}
	queued        map[string]struct{}
	completed     map[string]WriteEvidence
	retired       map[string]struct{}
	reused        map[string]struct{}
	closeOne      sync.Once
	info          atomic.Pointer[ServerInfo]
	initMu        sync.Mutex
	admitMu       sync.Mutex
	accepting     bool
	activeMu      sync.Mutex
	activeKey     string
	activeRequest bool
	activeCancel  context.CancelFunc
}

type readResult struct {
	frame Frame
	err   error
}

// New constructs and starts a client pump. Call Initialize before operational
// requests. The transport is not implicitly reconnected or replaced.
func New(transport Transport, options Options) *Client {
	options = options.normalized()
	c := &Client{
		transport:    transport,
		options:      options,
		generation:   nextGeneration.Add(1),
		commands:     make(chan command, options.WriterCapacity),
		commandSpace: make(chan struct{}, 1),
		reads:        make(chan readResult, maxInt(options.EventCapacity, defaultQueueCapacity)),
		// Reserve two slots for terminal gap/disconnect evidence.
		events:    make(chan Event, options.EventCapacity+2),
		done:      make(chan struct{}),
		closed:    make(chan struct{}),
		reserved:  make(map[string]struct{}),
		withdrawn: make(map[string]struct{}),
		queued:    make(map[string]struct{}),
		completed: make(map[string]WriteEvidence),
		retired:   make(map[string]struct{}),
		reused:    make(map[string]struct{}),
		accepting: true,
	}
	go c.readLoop()
	go c.pump()
	return c
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

var errClientClosed = io.ErrClosedPipe
var nextGeneration atomic.Uint64

func (c *Client) enqueue(ctx context.Context, cmd command) error {
	for {
		c.admitMu.Lock()
		if !c.accepting {
			c.admitMu.Unlock()
			return errClientClosed
		}
		select {
		case c.commands <- cmd:
			c.admitMu.Unlock()
			if cmd.kind == commandRequest {
				c.mu.Lock()
				c.queued[cmd.key] = struct{}{}
				c.mu.Unlock()
			}
			return nil
		default:
			c.admitMu.Unlock()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.closed:
			return errClientClosed
		case <-c.commandSpace:
		}
	}
}

func (c *Client) signalCommandSpace() {
	select {
	case c.commandSpace <- struct{}{}:
	default:
	}
}

func (c *Client) stopAdmission() {
	c.admitMu.Lock()
	c.accepting = false
	c.admitMu.Unlock()
}

func (c *Client) beginWrite(parent context.Context, key string, request bool) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	c.activeMu.Lock()
	c.activeKey = key
	c.activeRequest = request
	c.activeCancel = cancel
	c.activeMu.Unlock()
	return ctx, cancel
}

func (c *Client) endWrite(key string, cancel context.CancelFunc) {
	c.activeMu.Lock()
	if c.activeKey == key {
		c.activeKey = ""
		c.activeRequest = false
		c.activeCancel = nil
	}
	c.activeMu.Unlock()
	cancel()
}

func (c *Client) cancelActiveWrite() bool {
	c.activeMu.Lock()
	anyActive := c.activeCancel != nil
	cancel := c.activeCancel
	c.activeMu.Unlock()
	if anyActive {
		cancel()
		// A conforming transport must honor the context, but closing here also
		// bounds transports that only unblock their writer from Close.
		_ = c.transport.Close()
	}
	return anyActive
}

func (c *Client) cancelAnyActiveWrite() {
	c.activeMu.Lock()
	cancel := c.activeCancel
	c.activeMu.Unlock()
	if cancel != nil {
		cancel()
		_ = c.transport.Close()
	}
}

func (c *Client) recordCompleted(key string, evidence WriteEvidence) {
	c.mu.Lock()
	c.completed[key] = evidence
	c.mu.Unlock()
}

func (c *Client) completedEvidence(key string) (WriteEvidence, bool) {
	c.mu.Lock()
	evidence, ok := c.completed[key]
	c.mu.Unlock()
	return evidence, ok
}

func (c *Client) forgetCompleted(key string) {
	c.mu.Lock()
	delete(c.completed, key)
	c.mu.Unlock()
}

func (c *Client) retire(key string) {
	c.mu.Lock()
	c.retired[key] = struct{}{}
	c.mu.Unlock()
}

func (c *Client) completeReservation(key string, evidence WriteEvidence) {
	c.mu.Lock()
	c.completed[key] = evidence
	c.retired[key] = struct{}{}
	delete(c.reserved, key)
	delete(c.withdrawn, key)
	delete(c.queued, key)
	delete(c.reused, key)
	c.mu.Unlock()
}

func (c *Client) releaseUnknownReservation(key string, retire bool) {
	c.mu.Lock()
	if retire {
		c.retired[key] = struct{}{}
	}
	delete(c.reserved, key)
	delete(c.withdrawn, key)
	delete(c.queued, key)
	c.mu.Unlock()
}

// NewWithTransport is an explicit spelling useful at injection sites.
func NewWithTransport(transport Transport, options Options) *Client { return New(transport, options) }

func NewClient(transport Transport, options Options) *Client { return New(transport, options) }

func (c *Client) Generation() uint64 { return c.generation }

func (c *Client) Events() <-chan Event { return c.events }

func (c *Client) Done() <-chan struct{} { return c.closed }

func (c *Client) ServerInfo() *ServerInfo {
	i := c.info.Load()
	if i == nil {
		return nil
	}
	copy := *i
	return &copy
}

// Initialize performs the Codex initialize/initialized handshake. Events that
// arrive before the response are delivered on Events in arrival order.
func (c *Client) Initialize(ctx context.Context) (*ServerInfo, error) {
	c.initMu.Lock()
	defer c.initMu.Unlock()
	if c.info.Load() != nil {
		return nil, errors.New("app-server client is already initialized")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	params, err := json.Marshal(InitializeParams{
		ClientInfo: ClientInfo{Name: c.options.ClientName, Version: c.options.ClientVersion},
		Capabilities: InitializeCapabilities{
			ExperimentalAPI:           c.options.ExperimentalAPI,
			OptOutNotificationMethods: c.options.OptOutNotificationMethods,
		},
	})
	if err != nil {
		return nil, err
	}
	request := RPCRequest{ID: "initialize", Method: "initialize", Params: params}
	initCtx, cancel := context.WithTimeout(ctx, c.options.HandshakeTimeout)
	defer cancel()
	result, err := c.call(initCtx, request)
	if err != nil {
		_ = c.Close(context.Background())
		return nil, err
	}
	var raw struct {
		UserAgent      any    `json:"userAgent"`
		CodexHome      string `json:"codexHome"`
		PlatformFamily string `json:"platformFamily"`
		PlatformOS     string `json:"platformOs"`
	}
	if err := json.Unmarshal(result.Value, &raw); err != nil {
		_ = c.Close(context.Background())
		return nil, fmt.Errorf("decode initialize response: %w", err)
	}
	userAgent, ok := raw.UserAgent.(string)
	if !ok || userAgent == "" {
		_ = c.Close(context.Background())
		return nil, errors.New("initialize response missing string userAgent")
	}
	info := &ServerInfo{UserAgent: userAgent, ServerVersion: parseServerVersion(userAgent), CodexHome: raw.CodexHome, PlatformFamily: raw.PlatformFamily, PlatformOS: raw.PlatformOS, Generation: c.generation}
	c.info.Store(info)
	if err := c.Notify(initCtx, RPCNotification{Method: "initialized"}); err != nil {
		_ = c.Close(context.Background())
		return nil, fmt.Errorf("send initialized notification: %w", err)
	}
	return info, nil
}

func parseServerVersion(userAgent string) string {
	if _, rest, ok := strings.Cut(userAgent, "/"); ok {
		fields := strings.Fields(rest)
		if len(fields) > 0 {
			return fields[0]
		}
	}
	return ""
}

// Call sends one raw request and preserves the raw result or server error.
func (c *Client) Call(ctx context.Context, request RPCRequest) (*RPCResult, error) {
	if request.Method != "initialize" && c.info.Load() == nil {
		return nil, &CallError{Err: errors.New("app-server client is not initialized"), Evidence: WriteEvidence{Phase: WriteProvenBeforeWrite, Generation: c.generation}, Generation: c.generation}
	}
	return c.call(ctx, request)
}

func (c *Client) call(ctx context.Context, request RPCRequest) (*RPCResult, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	key, err := requestIDKey(request.ID)
	if err != nil {
		return nil, &CallError{Err: err, Evidence: WriteEvidence{Phase: WriteProvenBeforeWrite, Generation: c.generation}, Generation: c.generation}
	}
	resultCh := make(chan callOutcome, 1)
	c.mu.Lock()
	if _, exists := c.reserved[key]; exists {
		c.mu.Unlock()
		return nil, &CallError{Err: fmt.Errorf("duplicate request id %s", key), Evidence: WriteEvidence{Phase: WriteProvenBeforeWrite, Generation: c.generation}, Generation: c.generation}
	}
	if _, exists := c.retired[key]; exists {
		// A reused ID has a one-response quarantine. This keeps a late duplicate
		// response from satisfying the new request while retaining compatibility
		// with callers that deliberately reuse raw JSON-RPC IDs.
		c.reused[key] = struct{}{}
	}
	c.reserved[key] = struct{}{}
	c.mu.Unlock()
	cmd := command{kind: commandRequest, ctx: ctx, req: request, key: key, result: resultCh}
	if err := c.enqueue(ctx, cmd); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			cancel := c.withdraw(ctx, key)
			phase := cancel.phase
			if phase == WriteNotStarted {
				phase = WriteProvenBeforeWrite
			}
			if cancel.err != nil {
				return nil, &CallError{Err: cancel.err, Canceled: true, Evidence: WriteEvidence{Phase: phase, Generation: c.generation}, Generation: c.generation}
			}
			return nil, &CallError{Err: ctx.Err(), Canceled: true, Evidence: WriteEvidence{Phase: phase, Generation: c.generation}, Generation: c.generation}
		}
		c.release(key)
		return nil, &CallError{Err: err, Evidence: WriteEvidence{Phase: WriteProvenBeforeWrite, Generation: c.generation}, Generation: c.generation}
	}
	select {
	case outcome := <-resultCh:
		c.forgetCompleted(key)
		if outcome.err != nil {
			return nil, outcome.err
		}
		return outcome.result, nil
	case <-ctx.Done():
		// A response may already be buffered at the same instant cancellation
		// becomes ready. Prefer that completed evidence before withdrawing.
		select {
		case outcome := <-resultCh:
			c.forgetCompleted(key)
			if outcome.err != nil {
				return nil, outcome.err
			}
			return outcome.result, nil
		default:
		}
		cancel := c.withdraw(ctx, key)
		select {
		case outcome := <-resultCh:
			c.forgetCompleted(key)
			if outcome.err != nil {
				return nil, outcome.err
			}
			return outcome.result, nil
		default:
		}
		if cancel.err != nil {
			return nil, &CallError{Err: cancel.err, Canceled: true, Evidence: WriteEvidence{Phase: WriteMayHaveWritten, Generation: c.generation}, Generation: c.generation}
		}
		phase := cancel.phase
		if phase == WriteNotStarted {
			phase = WriteProvenBeforeWrite
		}
		return nil, &CallError{Err: ctx.Err(), Canceled: true, Evidence: WriteEvidence{Phase: phase, Generation: c.generation}, Generation: c.generation}
	case <-c.closed:
		select {
		case outcome := <-resultCh:
			c.forgetCompleted(key)
			if outcome.err != nil {
				return nil, outcome.err
			}
			return outcome.result, nil
		default:
		}
		return nil, &CallError{Err: errClientClosed, Evidence: WriteEvidence{Phase: WriteMayHaveWritten, Generation: c.generation}, Generation: c.generation}
	}
}

// Notify sends a notification without registering a response.
func (c *Client) Notify(ctx context.Context, notification RPCNotification) error {
	if ctx == nil {
		ctx = context.Background()
	}
	done := make(chan error, 1)
	if err := c.enqueue(ctx, command{kind: commandNotify, ctx: ctx, notify: notification, notifyDone: done}); err != nil {
		return err
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		c.cancelAnyActiveWrite()
		return ctx.Err()
	case <-c.closed:
		return errClientClosed
	}
}

func (c *Client) withdraw(ctx context.Context, key string) cancelOutcome {
	c.mu.Lock()
	c.withdrawn[key] = struct{}{}
	c.mu.Unlock()
	if c.cancelActiveWrite() {
		// The active writer may belong to another command. There is no way for
		// this queued cancellation to receive an ordered writer acknowledgement
		// while that write owns the sole pump, so terminate the generation and
		// report conservative may-have-written evidence.
		return cancelOutcome{phase: WriteMayHaveWritten}
	}
	ack := make(chan cancelOutcome, 1)
	select {
	case c.commands <- command{kind: commandCancel, key: key, ack: ack}:
	case <-ctx.Done():
		// The acknowledgement is a required part of the cancellation contract.
		// Use a background send so a transient full queue cannot become a false
		// proven-before-write claim.
		go func() {
			select {
			case c.commands <- command{kind: commandCancel, key: key, ack: ack}:
			case <-c.closed:
				ack <- cancelOutcome{phase: WriteMayHaveWritten, err: io.ErrClosedPipe}
			}
		}()
	case <-c.closed:
		return cancelOutcome{phase: WriteMayHaveWritten, err: io.ErrClosedPipe}
	}
	select {
	case out := <-ack:
		return out
	case <-c.closed:
		return cancelOutcome{phase: WriteMayHaveWritten, err: io.ErrClosedPipe}
	}
}

func (c *Client) release(key string) {
	c.mu.Lock()
	delete(c.reserved, key)
	delete(c.withdrawn, key)
	delete(c.queued, key)
	delete(c.reused, key)
	c.mu.Unlock()
}

func (c *Client) releaseReservation(key string) {
	c.mu.Lock()
	delete(c.reserved, key)
	delete(c.queued, key)
	delete(c.withdrawn, key)
	delete(c.reused, key)
	c.mu.Unlock()
}

func (c *Client) wasWithdrawn(key string) bool {
	c.mu.Lock()
	_, ok := c.withdrawn[key]
	c.mu.Unlock()
	return ok
}

func (c *Client) readLoop() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for {
		frame, err := c.transport.Read(ctx)
		if err != nil {
			// Publish the terminal read error even when Close concurrently
			// closes done. Dropping it here would leave the pump waiting forever.
			select {
			case c.reads <- readResult{frame: frame, err: err}:
			case <-time.After(time.Second):
			}
			return
		}
		select {
		case c.reads <- readResult{frame: frame}:
		case <-c.done:
			return
		default:
			// The reader itself is bounded. A full read queue is a proven event
			// gap, and the pump will turn it into explicit gap/disconnect evidence.
			select {
			case c.reads <- readResult{err: errReadOverflow}:
			case <-c.done:
			}
			return
		}
	}
}

var errReadOverflow = errors.New("transport read queue overflow")

func (c *Client) pump() {
	defer close(c.closed)
	defer close(c.events)
	defer c.transport.Close()
	pending := make(map[string]*pendingCall)
	var dropped uint64
	var disconnected bool
	deliver := func(event Event) bool {
		if disconnected {
			return false
		}
		select {
		case c.events <- event:
		default:
			if event.Kind == EventDisconnected {
				// Keep terminal disconnect evidence by evicting one non-terminal
				// event where possible. A gap is then emitted on the next read.
				dropped++
				return false
			}
			dropped++
			return false
		}
		return true
	}
	deliverWithGap := func(event Event) bool {
		if dropped > 0 {
			gap := Event{Kind: EventGap, Gap: &Gap{Dropped: dropped, Generation: c.generation, Reason: "bounded event queue overflow"}}
			select {
			case c.events <- gap:
				dropped = 0
			default:
				// Keep counting until a consumer makes room. The disconnect
				// event remains explicit once the queue drains.
				if event.Kind != EventDisconnected {
					dropped++
					return false
				}
			}
		}
		return deliver(event)
	}
	finish := func(err error) {
		if disconnected {
			return
		}
		c.stopAdmission()
		for key, call := range pending {
			phase := call.phase
			if phase == WriteNotStarted {
				phase = WriteProvenBeforeWrite
			} else if phase == WriteComplete {
				phase = WriteMayHaveWritten
			}
			call.result <- callOutcome{err: &CallError{Err: err, Evidence: WriteEvidence{Phase: phase, Generation: c.generation}, Generation: c.generation}}
			c.releaseUnknownReservation(key, phase == WriteMayHaveWritten || phase == WriteComplete)
			delete(pending, key)
		}
		// Resolve requests that were queued but never handed to the transport.
		// This keeps a close race from turning a proven-before-write request into
		// an ambiguous outcome.
		for {
			select {
			case cmd := <-c.commands:
				c.signalCommandSpace()
				switch cmd.kind {
				case commandRequest:
					c.release(cmd.key)
					cmd.result <- callOutcome{err: &CallError{Err: err, Evidence: WriteEvidence{Phase: WriteProvenBeforeWrite, Generation: c.generation}, Generation: c.generation}}
				case commandNotify:
					if cmd.notifyDone != nil {
						cmd.notifyDone <- err
					}
				case commandCancel:
					if cmd.ack != nil {
						cmd.ack <- cancelOutcome{phase: WriteProvenBeforeWrite, ok: true}
					}
				}
			default:
				goto drained
			}
		}
	drained:
		// Terminal evidence must not disappear behind a full bounded queue. Two
		// reserved slots are maintained by evicting queued events and folding
		// them into an explicit gap count.
		evicted := uint64(0)
		for len(c.events) > cap(c.events)-2 {
			<-c.events
			evicted++
		}
		totalDropped := dropped + evicted
		if totalDropped > 0 {
			c.events <- Event{Kind: EventGap, Gap: &Gap{Dropped: totalDropped, Generation: c.generation, Reason: "bounded event queue overflow or terminal displacement"}}
		}
		c.events <- Event{Kind: EventDisconnected, Disconnected: &Disconnected{Err: err, Generation: c.generation}}
		disconnected = true
	}
	for {
		select {
		case <-c.done:
			finish(errClientClosed)
			return
		case cmd := <-c.commands:
			c.signalCommandSpace()
			switch cmd.kind {
			case commandRequest:
				c.mu.Lock()
				delete(c.queued, cmd.key)
				c.mu.Unlock()
				if c.wasWithdrawn(cmd.key) {
					c.release(cmd.key)
					continue
				}
				if _, exists := pending[cmd.key]; exists {
					cmd.result <- callOutcome{err: &CallError{Err: fmt.Errorf("duplicate request id %s", cmd.key), Evidence: WriteEvidence{Phase: WriteProvenBeforeWrite, Generation: c.generation}, Generation: c.generation}}
					c.release(cmd.key)
					continue
				}
				call := &pendingCall{result: cmd.result, generation: c.generation, phase: WriteNotStarted}
				pending[cmd.key] = call
				payload, err := marshalRequest(cmd.req)
				writeStarted := err == nil
				if writeStarted {
					// Once ownership enters the transport, a concurrent disconnect
					// cannot prove that no bytes crossed the boundary.
					call.phase = WriteMayHaveWritten
					writeCtx, writeCancel := c.beginWrite(cmd.ctx, cmd.key, true)
					err = c.transport.Write(writeCtx, payload)
					c.endWrite(cmd.key, writeCancel)
				}
				if err != nil {
					phase := WriteProvenBeforeWrite
					if writeStarted {
						phase = WriteMayHaveWritten
					}
					if wf, ok := err.(*WriteFailure); ok {
						phase = wf.Phase
						err = wf.Err
					}
					call.phase = phase
					delete(pending, cmd.key)
					c.release(cmd.key)
					cmd.result <- callOutcome{err: &CallError{Err: err, Evidence: WriteEvidence{Phase: phase, Generation: c.generation}, Generation: c.generation}}
					finish(err)
					return
				}
				call.phase = WriteComplete
			case commandNotify:
				payload, err := marshalNotification(cmd.notify)
				if err == nil {
					writeCtx, writeCancel := c.beginWrite(cmd.ctx, "", false)
					err = c.transport.Write(writeCtx, payload)
					c.endWrite("", writeCancel)
				}
				if cmd.notifyDone != nil {
					cmd.notifyDone <- err
				}
				if err != nil {
					finish(err)
					return
				}
			case commandCancel:
				if call, exists := pending[cmd.key]; exists {
					if call.phase == WriteNotStarted {
						delete(pending, cmd.key)
						c.release(cmd.key)
						cmd.ack <- cancelOutcome{phase: WriteProvenBeforeWrite, ok: true}
					} else {
						cmd.ack <- cancelOutcome{phase: WriteMayHaveWritten, ok: false}
					}
				} else if evidence, completed := c.completedEvidence(cmd.key); completed {
					cmd.ack <- cancelOutcome{phase: evidence.Phase, completed: true, ok: false}
					c.forgetCompleted(cmd.key)
				} else {
					c.mu.Lock()
					_, queued := c.queued[cmd.key]
					c.mu.Unlock()
					if !queued {
						c.releaseReservation(cmd.key)
					}
					cmd.ack <- cancelOutcome{phase: WriteProvenBeforeWrite, ok: true}
				}
			case commandClose:
				finish(io.ErrClosedPipe)
				return
			}
		case incoming := <-c.reads:
			if incoming.err != nil {
				finish(incoming.err)
				return
			}
			if incoming.frame.Type == FrameBinary {
				finish(errors.New("unexpected binary WebSocket frame"))
				return
			}
			if incoming.frame.Type == FrameClose {
				finish(errors.New("peer closed WebSocket during JSON-RPC session"))
				return
			}
			if incoming.frame.Type != FrameText {
				continue
			}
			msg, err := decodeMessage(incoming.frame.Payload)
			if err != nil {
				finish(fmt.Errorf("decode JSON-RPC message: %w", err))
				return
			}
			switch msg.kind {
			case messageResponse:
				key, keyErr := requestIDKey(msg.id)
				if keyErr != nil {
					continue
				}
				call, exists := pending[key]
				if !exists {
					continue
				}
				c.mu.Lock()
				_, quarantined := c.reused[key]
				if quarantined {
					delete(c.reused, key)
				}
				c.mu.Unlock()
				if quarantined {
					// The first response after a deliberate ID reuse is ambiguous
					// with a late duplicate from the prior operation.
					continue
				}
				delete(pending, key)
				evidence := WriteEvidence{Phase: call.phase, Generation: c.generation}
				c.completeReservation(key, evidence)
				if msg.serverErr != nil {
					msg.serverErr.Generation = c.generation
					call.result <- callOutcome{err: &CallError{Server: msg.serverErr, Evidence: WriteEvidence{Phase: call.phase, Generation: c.generation}, Generation: c.generation}}
				} else {
					call.result <- callOutcome{result: &RPCResult{ID: msg.id, Value: msg.result, Evidence: evidence, Generation: c.generation}}
				}
			case messageNotification:
				if !deliverWithGap(Event{Kind: EventNotification, Notification: &RPCNotification{Method: msg.method, Params: msg.params, Generation: c.generation}}) {
					finish(errors.New("bounded app-server event queue overflow"))
					return
				}
			case messageRequest:
				if !deliverWithGap(Event{Kind: EventServerRequest, Request: &ServerRequest{ID: msg.id, Method: msg.method, Params: msg.params, Generation: c.generation}}) {
					finish(errors.New("bounded app-server event queue overflow"))
					return
				}
			}
		}
	}
}

func marshalRequest(req RPCRequest) ([]byte, error) {
	if req.Method == "" {
		return nil, errors.New("JSON-RPC method is required")
	}
	id, err := normalizeID(req.ID)
	if err != nil {
		return nil, err
	}
	obj := map[string]any{"id": json.RawMessage(id), "method": req.Method}
	if len(req.Params) != 0 && string(req.Params) != "null" {
		obj["params"] = json.RawMessage(req.Params)
	}
	return json.Marshal(obj)
}

func marshalNotification(n RPCNotification) ([]byte, error) {
	if n.Method == "" {
		return nil, errors.New("JSON-RPC method is required")
	}
	obj := map[string]any{"method": n.Method}
	if len(n.Params) != 0 && string(n.Params) != "null" {
		obj["params"] = json.RawMessage(n.Params)
	}
	return json.Marshal(obj)
}

type messageKind uint8

const (
	messageResponse messageKind = iota
	messageNotification
	messageRequest
)

type decodedMessage struct {
	kind      messageKind
	id        RequestID
	method    string
	params    json.RawMessage
	result    json.RawMessage
	serverErr *ServerError
}

func decodeMessage(payload []byte) (decodedMessage, error) {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(payload, &obj); err != nil {
		return decodedMessage{}, err
	}
	idRaw, hasID := obj["id"]
	methodRaw, hasMethod := obj["method"]
	if hasMethod {
		if _, hasResult := obj["result"]; hasResult {
			return decodedMessage{}, errors.New("JSON-RPC request/notification cannot contain result")
		}
		if _, hasError := obj["error"]; hasError {
			return decodedMessage{}, errors.New("JSON-RPC request/notification cannot contain error")
		}
		if len(methodRaw) == 0 || methodRaw[0] != '"' {
			return decodedMessage{}, errors.New("JSON-RPC method must be a string")
		}
		var method string
		if err := json.Unmarshal(methodRaw, &method); err != nil {
			return decodedMessage{}, fmt.Errorf("method: %w", err)
		}
		var params json.RawMessage
		if raw := obj["params"]; len(raw) != 0 {
			params = append(json.RawMessage(nil), raw...)
		}
		if hasID {
			id, err := decodeID(idRaw)
			if err != nil {
				return decodedMessage{}, err
			}
			return decodedMessage{kind: messageRequest, id: id, method: method, params: params}, nil
		}
		return decodedMessage{kind: messageNotification, method: method, params: params}, nil
	}
	if !hasID {
		return decodedMessage{}, errors.New("JSON-RPC message has neither method nor id")
	}
	_, hasResult := obj["result"]
	_, hasError := obj["error"]
	if hasResult == hasError {
		return decodedMessage{}, errors.New("JSON-RPC response must contain exactly one of result or error")
	}
	id, err := decodeID(idRaw)
	if err != nil {
		return decodedMessage{}, err
	}
	if raw, ok := obj["error"]; ok {
		if len(raw) == 0 || raw[0] != '{' {
			return decodedMessage{}, errors.New("JSON-RPC error must be an object")
		}
		var e struct {
			Code    *int64          `json:"code"`
			Message *string         `json:"message"`
			Data    json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(raw, &e); err != nil {
			return decodedMessage{}, fmt.Errorf("error: %w", err)
		}
		if e.Code == nil || e.Message == nil {
			return decodedMessage{}, errors.New("JSON-RPC error requires code and message")
		}
		return decodedMessage{kind: messageResponse, id: id, serverErr: &ServerError{ID: id, Code: *e.Code, Message: *e.Message, Data: e.Data}}, nil
	}
	result := append(json.RawMessage(nil), obj["result"]...)
	return decodedMessage{kind: messageResponse, id: id, result: result}, nil
}

func decodeID(raw json.RawMessage) (RequestID, error) {
	if len(raw) == 0 {
		return nil, errors.New("JSON-RPC request ID is required")
	}
	var s string
	if raw[0] == '"' && json.Unmarshal(raw, &s) == nil {
		return s, nil
	}
	if len(raw) > 0 && raw[0] != '-' && (raw[0] < '0' || raw[0] > '9') {
		return nil, errors.New("JSON-RPC request ID must be a string or signed integer")
	}
	if isJSONInteger(string(raw)) {
		if _, err := strconv.ParseInt(string(raw), 10, 64); err == nil {
			return json.Number(string(raw)), nil
		}
	}
	return nil, errors.New("JSON-RPC request ID must be a signed int64")
}

func normalizeID(id RequestID) ([]byte, error) {
	switch value := id.(type) {
	case string:
		return json.Marshal(value)
	case json.Number:
		if !isJSONInteger(value.String()) {
			return nil, errors.New("JSON-RPC request ID must be a signed int64")
		}
		number, err := strconv.ParseInt(value.String(), 10, 64)
		if err != nil {
			return nil, errors.New("JSON-RPC request ID must be a signed int64")
		}
		return []byte(strconv.FormatInt(number, 10)), nil
	case int:
		return []byte(fmt.Sprintf("%d", value)), nil
	case int8:
		return []byte(fmt.Sprintf("%d", value)), nil
	case int16:
		return []byte(fmt.Sprintf("%d", value)), nil
	case int32:
		return []byte(fmt.Sprintf("%d", value)), nil
	case int64:
		return []byte(fmt.Sprintf("%d", value)), nil
	case json.RawMessage:
		decoded, err := decodeID(value)
		if err != nil {
			return nil, err
		}
		return normalizeID(decoded)
	default:
		return nil, errors.New("JSON-RPC request ID must be a string or integer")
	}
}

func isJSONInteger(value string) bool {
	if value == "0" || value == "-0" {
		return true
	}
	if value == "" {
		return false
	}
	start := 0
	if value[0] == '-' {
		start = 1
		if len(value) == 1 || value[start] == '0' {
			return false
		}
	} else if value[0] < '1' || value[0] > '9' {
		return false
	}
	for _, char := range value[start:] {
		if char < '0' || char > '9' {
			return false
		}
	}
	return true
}

func requestIDKey(id RequestID) (string, error) {
	raw, err := normalizeID(id)
	if err != nil {
		return "", err
	}
	return string(raw), nil
}

// NextEvent waits for one event and retains the generation on all terminal
// evidence. A closed stream returns io.EOF after the disconnect event.
func (c *Client) NextEvent(ctx context.Context) (Event, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case event, ok := <-c.events:
		if !ok {
			return Event{}, io.EOF
		}
		return event, nil
	case <-ctx.Done():
		return Event{}, ctx.Err()
	}
}

// Close does not reconnect or replay pending requests.
func (c *Client) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	c.closeOne.Do(func() {
		c.stopAdmission()
		close(c.done)
	})
	// Closing the underlying connection is required to unblock a reader; the
	// pump still owns all response/error publication and performs no replay.
	c.cancelAnyActiveWrite()
	_ = c.transport.Close()
	select {
	case <-c.closed:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
