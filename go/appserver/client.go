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
	Method string          `json:"method"`
	Params json.RawMessage `json:"params,omitempty"`
}

type Notification = RPCNotification

// RPCResult is the raw result value returned by the app-server.
type RPCResult struct {
	ID         RequestID
	Value      json.RawMessage
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
	phase WritePhase
	ok    bool
	err   error
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

	commands chan command
	reads    chan readResult
	events   chan Event
	done     chan struct{}
	closed   chan struct{}

	mu        sync.Mutex
	reserved  map[string]struct{}
	withdrawn map[string]struct{}
	closeOne  sync.Once
	info      atomic.Pointer[ServerInfo]
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
		transport:  transport,
		options:    options,
		generation: 1,
		commands:   make(chan command, options.WriterCapacity),
		reads:      make(chan readResult, maxInt(options.EventCapacity, defaultQueueCapacity)),
		// Reserve two slots for terminal gap/disconnect evidence.
		events:    make(chan Event, options.EventCapacity+2),
		done:      make(chan struct{}),
		closed:    make(chan struct{}),
		reserved:  make(map[string]struct{}),
		withdrawn: make(map[string]struct{}),
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
	if err := c.Notify(ctx, RPCNotification{Method: "initialized"}); err != nil {
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
	c.reserved[key] = struct{}{}
	c.mu.Unlock()
	cmd := command{kind: commandRequest, req: request, key: key, result: resultCh}
	select {
	case c.commands <- cmd:
	case <-ctx.Done():
		cancel := c.withdraw(ctx, key)
		phase := cancel.phase
		if phase == WriteNotStarted {
			phase = WriteProvenBeforeWrite
		}
		if cancel.err != nil {
			return nil, &CallError{Err: cancel.err, Canceled: true, Evidence: WriteEvidence{Phase: phase, Generation: c.generation}, Generation: c.generation}
		}
		return nil, &CallError{Err: ctx.Err(), Canceled: true, Evidence: WriteEvidence{Phase: phase, Generation: c.generation}, Generation: c.generation}
	case <-c.closed:
		c.release(key)
		return nil, &CallError{Err: io.ErrClosedPipe, Evidence: WriteEvidence{Phase: WriteProvenBeforeWrite, Generation: c.generation}, Generation: c.generation}
	}
	select {
	case outcome := <-resultCh:
		if outcome.err != nil {
			return nil, outcome.err
		}
		return outcome.result, nil
	case <-ctx.Done():
		cancel := c.withdraw(ctx, key)
		if cancel.err != nil {
			return nil, &CallError{Err: cancel.err, Canceled: true, Evidence: WriteEvidence{Phase: WriteMayHaveWritten, Generation: c.generation}, Generation: c.generation}
		}
		phase := cancel.phase
		if phase == WriteNotStarted {
			phase = WriteProvenBeforeWrite
		}
		return nil, &CallError{Err: ctx.Err(), Canceled: true, Evidence: WriteEvidence{Phase: phase, Generation: c.generation}, Generation: c.generation}
	}
}

// Notify sends a notification without registering a response.
func (c *Client) Notify(ctx context.Context, notification RPCNotification) error {
	if ctx == nil {
		ctx = context.Background()
	}
	done := make(chan error, 1)
	select {
	case c.commands <- command{kind: commandNotify, notify: notification, notifyDone: done}:
		select {
		case err := <-done:
			return err
		case <-ctx.Done():
			return ctx.Err()
		case <-c.closed:
			return io.ErrClosedPipe
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closed:
		return io.ErrClosedPipe
	}
}

func (c *Client) withdraw(ctx context.Context, key string) cancelOutcome {
	c.mu.Lock()
	c.withdrawn[key] = struct{}{}
	c.mu.Unlock()
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
	deliver := func(event Event) {
		if disconnected {
			return
		}
		select {
		case c.events <- event:
		default:
			if event.Kind == EventDisconnected {
				// Keep terminal disconnect evidence by evicting one non-terminal
				// event where possible. A gap is then emitted on the next read.
				dropped++
				return
			}
			dropped++
		}
	}
	deliverWithGap := func(event Event) {
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
					return
				}
			}
		}
		deliver(event)
	}
	finish := func(err error) {
		if disconnected {
			return
		}
		for key, call := range pending {
			phase := call.phase
			if phase == WriteNotStarted {
				phase = WriteProvenBeforeWrite
			} else if phase == WriteComplete {
				phase = WriteMayHaveWritten
			}
			call.result <- callOutcome{err: &CallError{Err: err, Evidence: WriteEvidence{Phase: phase, Generation: c.generation}, Generation: c.generation}}
			delete(pending, key)
			c.release(key)
		}
		// Resolve requests that were queued but never handed to the transport.
		// This keeps a close race from turning a proven-before-write request into
		// an ambiguous outcome.
		for {
			select {
			case cmd := <-c.commands:
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
		case cmd := <-c.commands:
			switch cmd.kind {
			case commandRequest:
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
					err = c.transport.Write(context.Background(), payload)
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
					err = c.transport.Write(context.Background(), payload)
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
				} else {
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
				delete(pending, key)
				c.release(key)
				if msg.serverErr != nil {
					msg.serverErr.Generation = c.generation
					call.result <- callOutcome{err: &CallError{Server: msg.serverErr, Evidence: WriteEvidence{Phase: call.phase, Generation: c.generation}, Generation: c.generation}}
				} else {
					call.result <- callOutcome{result: &RPCResult{ID: msg.id, Value: msg.result, Generation: c.generation}}
				}
			case messageNotification:
				deliverWithGap(Event{Kind: EventNotification, Notification: &RPCNotification{Method: msg.method, Params: msg.params}})
			case messageRequest:
				deliverWithGap(Event{Kind: EventServerRequest, Request: &ServerRequest{ID: msg.id, Method: msg.method, Params: msg.params, Generation: c.generation}})
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
	id, err := decodeID(idRaw)
	if err != nil {
		return decodedMessage{}, err
	}
	if raw, ok := obj["error"]; ok {
		var e struct {
			Code    int64           `json:"code"`
			Message string          `json:"message"`
			Data    json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(raw, &e); err != nil {
			return decodedMessage{}, fmt.Errorf("error: %w", err)
		}
		return decodedMessage{kind: messageResponse, id: id, serverErr: &ServerError{ID: id, Code: e.Code, Message: e.Message, Data: e.Data}}, nil
	}
	result := append(json.RawMessage(nil), obj["result"]...)
	return decodedMessage{kind: messageResponse, id: id, result: result}, nil
}

func decodeID(raw json.RawMessage) (RequestID, error) {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s, nil
	}
	var n json.Number
	if json.Unmarshal(raw, &n) == nil {
		if strings.ContainsAny(string(raw), ".eE") {
			return nil, errors.New("JSON-RPC request ID must be an integer")
		}
		return n, nil
	}
	return nil, errors.New("JSON-RPC request ID must be a string or number")
}

func normalizeID(id RequestID) ([]byte, error) {
	switch value := id.(type) {
	case string:
		return json.Marshal(value)
	case json.Number:
		if strings.ContainsAny(value.String(), ".eE") {
			return nil, errors.New("JSON-RPC request ID must be an integer")
		}
		return []byte(value.String()), nil
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
	case uint:
		return []byte(fmt.Sprintf("%d", value)), nil
	case uint8:
		return []byte(fmt.Sprintf("%d", value)), nil
	case uint16:
		return []byte(fmt.Sprintf("%d", value)), nil
	case uint32:
		return []byte(fmt.Sprintf("%d", value)), nil
	case uint64:
		return []byte(fmt.Sprintf("%d", value)), nil
	case json.RawMessage:
		if _, err := decodeID(value); err != nil {
			return nil, err
		}
		return append([]byte(nil), value...), nil
	default:
		return nil, errors.New("JSON-RPC request ID must be a string or integer")
	}
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
	c.closeOne.Do(func() { close(c.done) })
	// Closing the underlying connection is required to unblock a reader; the
	// pump still owns all response/error publication and performs no replay.
	_ = c.transport.Close()
	select {
	case <-c.closed:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
