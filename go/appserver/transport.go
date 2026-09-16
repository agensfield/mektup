package appserver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const unixHandshakeURL = "ws://localhost/rpc"

const maxWebSocketMessageSize = 16 << 20

// NetDialContext is the narrow socket seam used by the WebSocket handshake.
// The URL and HTTP Upgrade remain owned by this package; implementations only
// provide the already-connected byte stream.
type NetDialContext func(context.Context, string, string) (net.Conn, error)

// UnixTransport connects directly to a Codex app-server Unix socket and uses
// the same HTTP/WebSocket handshake as Codex's pinned Rust client.
func DialUnix(ctx context.Context, socketPath string, options Options) (*Client, error) {
	transport, err := dialUnixTransport(ctx, socketPath)
	if err != nil {
		return nil, err
	}
	return New(transport, options), nil
}

// DialWebSocket opens the pinned app-server WebSocket endpoint over a caller
// supplied raw connection. It is intended for transports such as system SSH
// whose connection is not addressable by a local Unix or TCP socket. The
// handshake always uses ws://localhost/rpc, matching DialUnix exactly.
func DialWebSocket(ctx context.Context, netDial NetDialContext, options Options) (*Client, error) {
	transport, err := dialWebSocketTransportWithTimeout(ctx, netDial, "app-server transport", options.HandshakeTimeout)
	if err != nil {
		return nil, err
	}
	return New(transport, options), nil
}

type websocketTransport struct{ conn *websocket.Conn }

func dialUnixTransport(ctx context.Context, socketPath string) (Transport, error) {
	return dialWebSocketTransport(ctx, func(dialCtx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(dialCtx, "unix", socketPath)
	}, fmt.Sprintf("unix socket %q", socketPath))
}

func dialWebSocketTransport(ctx context.Context, netDial NetDialContext, description string) (Transport, error) {
	return dialWebSocketTransportWithTimeout(ctx, netDial, description, 0)
}

func dialWebSocketTransportWithTimeout(ctx context.Context, netDial NetDialContext, description string, timeout time.Duration) (Transport, error) {
	if netDial == nil {
		return nil, errors.New("app-server WebSocket net dialer is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if timeout <= 0 {
		timeout = defaultHandshakeWait
	}
	setupCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	guard := newDialGuard(setupCtx)
	dialer := websocket.Dialer{NetDialContext: func(dialCtx context.Context, network, address string) (net.Conn, error) {
		conn, err := netDial(dialCtx, network, address)
		if err != nil {
			return nil, err
		}
		if !guard.adopt(conn) {
			_ = conn.Close()
			if err := setupCtx.Err(); err != nil {
				return nil, err
			}
			return nil, errors.New("app-server WebSocket dial canceled")
		}
		return conn, nil
	}}
	conn, _, err := dialer.DialContext(setupCtx, unixHandshakeURL, nil)
	if err != nil {
		guard.stop()
		return nil, fmt.Errorf("websocket handshake on %s: %w", description, err)
	}
	guard.stop()
	conn.SetReadLimit(maxWebSocketMessageSize)
	return &websocketTransport{conn: conn}, nil
}

// dialGuard closes a raw connection if setup cancellation happens while the
// HTTP Upgrade is waiting for its response. Once the Upgrade succeeds, stop
// detaches the guard so the setup context cannot own the established lifetime.
type dialGuard struct {
	mu      sync.Mutex
	conn    net.Conn
	stopped bool
	done    chan struct{}
}

func newDialGuard(ctx context.Context) *dialGuard {
	g := &dialGuard{done: make(chan struct{})}
	go func() {
		select {
		case <-ctx.Done():
			g.expire()
		case <-g.done:
		}
	}()
	return g
}

func (g *dialGuard) adopt(conn net.Conn) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopped {
		return false
	}
	g.conn = conn
	return true
}

func (g *dialGuard) expire() {
	g.mu.Lock()
	if g.stopped {
		g.mu.Unlock()
		return
	}
	g.stopped = true
	conn := g.conn
	g.conn = nil
	close(g.done)
	g.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

func (g *dialGuard) stop() {
	g.mu.Lock()
	if !g.stopped {
		g.stopped = true
		g.conn = nil
		close(g.done)
	}
	g.mu.Unlock()
}

func (t *websocketTransport) Read(ctx context.Context) (Frame, error) {
	if deadline, ok := ctx.Deadline(); ok {
		_ = t.conn.SetReadDeadline(deadline)
	}
	kind, payload, err := t.conn.ReadMessage()
	if err != nil {
		return Frame{}, err
	}
	switch kind {
	case websocket.TextMessage:
		return Frame{Type: FrameText, Payload: payload}, nil
	case websocket.BinaryMessage:
		return Frame{Type: FrameBinary, Payload: payload}, nil
	case websocket.PingMessage:
		return Frame{Type: FramePing, Payload: payload}, nil
	case websocket.PongMessage:
		return Frame{Type: FramePong, Payload: payload}, nil
	default:
		return Frame{Type: FrameClose, Payload: payload}, nil
	}
}

func (t *websocketTransport) Write(ctx context.Context, payload []byte) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return &WriteFailure{Err: err, Phase: WriteProvenBeforeWrite}
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = t.conn.SetWriteDeadline(deadline)
	} else {
		_ = t.conn.SetWriteDeadline(time.Time{})
	}
	if err := t.conn.WriteMessage(websocket.TextMessage, payload); err != nil {
		// WriteMessage may fragment a single JSON-RPC message. A nested
		// WriteFailure only describes the fragment that failed; an earlier
		// fragment may already be on the wire. Preserve the safe no-write
		// boundary above, but classify every in-message failure conservatively.
		return &WriteFailure{Err: err, Phase: WriteMayHaveWritten}
	}
	return nil
}

func (t *websocketTransport) Close() error { return t.conn.Close() }
