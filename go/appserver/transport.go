package appserver

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/gorilla/websocket"
)

const unixHandshakeURL = "ws://localhost/rpc"

const maxWebSocketMessageSize = 128 << 20

// UnixTransport connects directly to a Codex app-server Unix socket and uses
// the same HTTP/WebSocket handshake as Codex's pinned Rust client.
func DialUnix(ctx context.Context, socketPath string, options Options) (*Client, error) {
	transport, err := dialUnixTransport(ctx, socketPath)
	if err != nil {
		return nil, err
	}
	return New(transport, options), nil
}

type websocketTransport struct{ conn *websocket.Conn }

func dialUnixTransport(ctx context.Context, socketPath string) (Transport, error) {
	dialer := websocket.Dialer{
		NetDialContext: func(dialCtx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(dialCtx, "unix", socketPath)
		},
	}
	conn, _, err := dialer.DialContext(ctx, unixHandshakeURL, nil)
	if err != nil {
		return nil, fmt.Errorf("websocket handshake on unix socket %q: %w", socketPath, err)
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
	if deadline, ok := ctx.Deadline(); ok {
		_ = t.conn.SetWriteDeadline(deadline)
	} else {
		_ = t.conn.SetWriteDeadline(time.Time{})
	}
	if err := t.conn.WriteMessage(websocket.TextMessage, payload); err != nil {
		return &WriteFailure{Err: err, Phase: WriteMayHaveWritten}
	}
	return nil
}

func (t *websocketTransport) Close() error { return t.conn.Close() }
