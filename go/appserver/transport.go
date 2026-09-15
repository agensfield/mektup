package appserver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/gorilla/websocket"
)

const unixHandshakeURL = "ws://localhost/rpc"

const maxWebSocketMessageSize = 128 << 20

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
	transport, err := dialWebSocketTransport(ctx, netDial, "app-server transport")
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
	if netDial == nil {
		return nil, errors.New("app-server WebSocket net dialer is nil")
	}
	dialer := websocket.Dialer{NetDialContext: netDial}
	conn, _, err := dialer.DialContext(ctx, unixHandshakeURL, nil)
	if err != nil {
		return nil, fmt.Errorf("websocket handshake on %s: %w", description, err)
	}
	conn.SetReadLimit(maxWebSocketMessageSize)
	return &websocketTransport{conn: conn}, nil
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
		var writeErr *WriteFailure
		if errors.As(err, &writeErr) {
			return writeErr
		}
		return &WriteFailure{Err: err, Phase: WriteMayHaveWritten}
	}
	return nil
}

func (t *websocketTransport) Close() error { return t.conn.Close() }
