package appserver

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

type gateWriteConn struct {
	net.Conn
	armed  atomic.Bool
	writes atomic.Int32
	bytes  atomic.Int64
}

func (c *gateWriteConn) Write(payload []byte) (int, error) {
	if c.armed.Load() && c.writes.Add(1) == 2 {
		return 0, &WriteFailure{Err: io.ErrClosedPipe, Phase: WriteProvenBeforeWrite}
	}
	n, err := c.Conn.Write(payload)
	if c.armed.Load() {
		c.bytes.Add(int64(n))
	}
	return n, err
}

func serveGateHandshake(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	request, err := http.ReadRequest(reader)
	if err != nil {
		return
	}
	key := request.Header.Get("Sec-WebSocket-Key")
	hash := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	_, _ = fmt.Fprintf(conn, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", base64.StdEncoding.EncodeToString(hash[:]))
	_, _ = io.Copy(io.Discard, reader)
}

func TestWebSocketMessageFragmentFailureIsConservative(t *testing.T) {
	clientSide, serverSide := net.Pipe()
	go serveGateHandshake(serverSide)
	raw := &gateWriteConn{Conn: clientSide}
	transport, err := dialWebSocketTransport(context.Background(), func(context.Context, string, string) (net.Conn, error) {
		return raw, nil
	}, "gate")
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()
	raw.armed.Store(true)
	err = transport.Write(context.Background(), bytes.Repeat([]byte("x"), 20_000))
	var writeErr *WriteFailure
	if !errors.As(err, &writeErr) {
		t.Fatal(err)
	}
	if raw.bytes.Load() == 0 {
		t.Fatal("probe did not write an earlier message fragment")
	}
	if writeErr.Phase == WriteProvenBeforeWrite {
		t.Fatalf("%d bytes written then classified before-write", raw.bytes.Load())
	}
}

func TestWebSocketUpgradeHonorsHandshakeTimeout(t *testing.T) {
	clientSide, serverSide := net.Pipe()
	defer serverSide.Close()
	go io.Copy(io.Discard, serverSide)
	done := make(chan error, 1)
	go func() {
		client, err := DialWebSocket(context.Background(), func(context.Context, string, string) (net.Conn, error) {
			return clientSide, nil
		}, Options{HandshakeTimeout: 20 * time.Millisecond})
		if client != nil {
			_ = client.Close(context.Background())
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected handshake timeout")
		}
	case <-time.After(100 * time.Millisecond):
		_ = clientSide.Close()
		<-done
		t.Fatal("HTTP Upgrade exceeded configured handshake timeout")
	}
}

func TestWebSocketUpgradeHonorsCancellation(t *testing.T) {
	clientSide, serverSide := net.Pipe()
	defer serverSide.Close()
	go io.Copy(io.Discard, serverSide)
	started := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := DialWebSocket(ctx, func(context.Context, string, string) (net.Conn, error) {
			close(started)
			return clientSide, nil
		}, Options{})
		done <- err
	}()
	<-started
	cancel()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		_ = clientSide.Close()
		<-done
		t.Fatal("canceled Upgrade remained blocked after net dial")
	}
}
