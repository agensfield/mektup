package sshtransport

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agensfield/mektup/go/appserver"
	"github.com/agensfield/mektup/go/internal/endpoint"
	"github.com/agensfield/mektup/go/internal/sshproxy"
)

type fakeProcess struct {
	stdinR      *io.PipeReader
	stdinW      *io.PipeWriter
	stdoutR     *io.PipeReader
	stdoutW     *io.PipeWriter
	stderrR     *io.PipeReader
	stderrW     *io.PipeWriter
	peer        net.Conn
	started     chan struct{}
	waitRelease chan struct{}
	killOnce    sync.Once
	waitCount   atomic.Int32
	killCount   atomic.Int32
}

func newFakeProcess(peer net.Conn) *fakeProcess {
	stdinR, stdinW := io.Pipe()
	stdoutR, stdoutW := io.Pipe()
	stderrR, stderrW := io.Pipe()
	return &fakeProcess{
		stdinR: stdinR, stdinW: stdinW, stdoutR: stdoutR, stdoutW: stdoutW,
		stderrR: stderrR, stderrW: stderrW, peer: peer,
		started: make(chan struct{}), waitRelease: make(chan struct{}),
	}
}

func (p *fakeProcess) StdinPipe() (io.WriteCloser, error) { return p.stdinW, nil }
func (p *fakeProcess) StdoutPipe() (io.ReadCloser, error) { return p.stdoutR, nil }
func (p *fakeProcess) StderrPipe() (io.ReadCloser, error) { return p.stderrR, nil }
func (p *fakeProcess) Start() error {
	close(p.started)
	go func() {
		_, _ = io.Copy(p.peer, p.stdinR)
	}()
	go func() {
		_, _ = io.Copy(p.stdoutW, p.peer)
		_ = p.stdoutW.Close()
	}()
	return nil
}
func (p *fakeProcess) Wait() error {
	p.waitCount.Add(1)
	<-p.waitRelease
	return nil
}
func (p *fakeProcess) Kill() error {
	p.killOnce.Do(func() {
		p.killCount.Add(1)
		_ = p.peer.Close()
		_ = p.stdinR.Close()
		_ = p.stdoutR.Close()
		_ = p.stderrR.Close()
		close(p.waitRelease)
	})
	return nil
}

type fakeFactory struct {
	process *fakeProcess
	errCh   chan error
	done    chan struct{}
	argv    []string
}

func (f *fakeFactory) New(argv []string) (sshproxy.Process, error) {
	f.argv = append([]string(nil), argv...)
	client, server := net.Pipe()
	f.process = newFakeProcess(client)
	f.done = make(chan struct{})
	go serveWebSocket(server, f.errCh, f.done)
	return f.process, nil
}

func serveWebSocket(conn net.Conn, errCh chan error, done chan struct{}) {
	defer func() {
		_ = conn.Close()
		close(done)
	}()
	reader := bufio.NewReader(conn)
	header, err := readHTTPHeader(reader)
	if err != nil {
		errCh <- err
		return
	}
	if header["path"] != "/rpc" || header["host"] != "localhost" ||
		header["upgrade"] != "websocket" || header["connection"] != "Upgrade" {
		errCh <- fmt.Errorf("unexpected handshake: %#v", header)
		return
	}
	key := header["sec-websocket-key"]
	acceptHash := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	response := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + base64.StdEncoding.EncodeToString(acceptHash[:]) + "\r\n\r\n"
	if _, err := io.WriteString(conn, response); err != nil {
		errCh <- err
		return
	}
	for {
		payload, err := readClientFrame(reader)
		if err != nil {
			if !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrClosedPipe) {
				errCh <- err
			}
			return
		}
		var request struct {
			ID     json.RawMessage `json:"id"`
			Method string          `json:"method"`
		}
		if err := json.Unmarshal(payload, &request); err != nil {
			errCh <- err
			return
		}
		switch request.Method {
		case "initialize":
			response := []byte(fmt.Sprintf(`{"id":%s,"result":{"userAgent":"codex/0.154.0 (ssh-daemon)"}}`, request.ID))
			if err := writeServerFrame(conn, response); err != nil {
				errCh <- err
				return
			}
		case "initialized":
		case "thread/read":
			response := []byte(fmt.Sprintf(`{"id":%s,"result":{"raw":true}}`, request.ID))
			if err := writeServerFrame(conn, response); err != nil {
				errCh <- err
				return
			}
		default:
			errCh <- fmt.Errorf("unexpected RPC method %q", request.Method)
			return
		}
	}
}

func readHTTPHeader(reader *bufio.Reader) (map[string]string, error) {
	result := make(map[string]string)
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	parts := strings.Fields(line)
	if len(parts) != 3 || parts[0] != "GET" {
		return nil, fmt.Errorf("unexpected request line %q", line)
	}
	result["path"] = parts[1]
	for {
		line, err = reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			return result, nil
		}
		name, value, ok := strings.Cut(line, ":")
		if !ok {
			return nil, fmt.Errorf("malformed header %q", line)
		}
		result[strings.ToLower(name)] = strings.TrimSpace(value)
	}
}

func readClientFrame(reader *bufio.Reader) ([]byte, error) {
	first, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	second, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	if first&0x0f != 1 || second&0x80 == 0 {
		return nil, fmt.Errorf("unexpected client frame %#x %#x", first, second)
	}
	length := int(second & 0x7f)
	if length == 126 {
		var extended [2]byte
		if _, err := io.ReadFull(reader, extended[:]); err != nil {
			return nil, err
		}
		length = int(extended[0])<<8 | int(extended[1])
	} else if length == 127 {
		return nil, errors.New("test frame is too large")
	}
	var mask [4]byte
	if _, err := io.ReadFull(reader, mask[:]); err != nil {
		return nil, err
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return nil, err
	}
	for i := range payload {
		payload[i] ^= mask[i%4]
	}
	return payload, nil
}

func writeServerFrame(conn net.Conn, payload []byte) error {
	if len(payload) >= 126 {
		return errors.New("test response is too large")
	}
	frame := append([]byte{0x81, byte(len(payload))}, payload...)
	_, err := conn.Write(frame)
	return err
}

func TestSSHClientDialerCarriesRawWebSocketHandshakeAndFrames(t *testing.T) {
	errs := make(chan error, 4)
	factory := &fakeFactory{errCh: errs}
	route, err := endpoint.SSHRoute("codex.example")
	if err != nil {
		t.Fatal(err)
	}
	dialer := NewClientDialer(sshproxy.Config{}, factory)
	client, err := dialer.DialClient(context.Background(), route, appserver.Options{
		ClientName: "mektup-test", ClientVersion: "test", HandshakeTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	wantArgv := []string{"ssh", "--", "codex.example", "codex", "app-server", "proxy"}
	if strings.Join(factory.argv, "\x00") != strings.Join(wantArgv, "\x00") {
		t.Fatalf("proxy argv=%#v, want %#v", factory.argv, wantArgv)
	}
	result, err := client.Call(context.Background(), appserver.RPCRequest{
		ID: "raw-id", Method: "thread/read", Params: json.RawMessage(`{"threadId":"t1"}`),
	})
	if err != nil || result == nil || string(result.Value) != `{"raw":true}` {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-factory.done:
	case <-time.After(time.Second):
		t.Fatal("fake WebSocket server did not stop")
	}
	select {
	case err := <-errs:
		t.Fatal(err)
	default:
	}
	if got := factory.process.killCount.Load(); got != 1 {
		t.Fatalf("kill count=%d, want one", got)
	}
	if got := factory.process.waitCount.Load(); got != 1 {
		t.Fatalf("wait count=%d, want one", got)
	}
}

type failingConn struct{ net.Conn }

func (f failingConn) Write([]byte) (int, error) {
	return 0, &sshproxy.Failure{Kind: sshproxy.FailureCanceled, Cause: sshproxy.FailureCanceled, Evidence: sshproxy.WriteNotStarted, Err: context.Canceled}
}

func TestSSHWriteFailurePreservesProvenBeforeWrite(t *testing.T) {
	n, err := (&evidenceConn{Conn: failingConn{}}).Write([]byte("request"))
	if n != 0 {
		t.Fatalf("write count=%d", n)
	}
	var writeErr *appserver.WriteFailure
	if !errors.As(err, &writeErr) || writeErr.Phase != appserver.WriteProvenBeforeWrite {
		t.Fatalf("write error=%T %+v", err, err)
	}
}

func TestSSHClientDialerRejectsRouteHostMismatch(t *testing.T) {
	route, err := endpoint.SSHRoute("route-host")
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewClientDialer(sshproxy.Config{Host: "config-host"}, nil).DialClient(context.Background(), route, appserver.Options{})
	if err == nil || !strings.Contains(err.Error(), "does not match route host") {
		t.Fatalf("mismatch error=%v", err)
	}
}
