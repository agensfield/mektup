package appserver

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeTransport struct {
	reads      chan Frame
	readErrors chan error
	writes     chan []byte
	done       chan struct{}
	onWrite    func([]byte) error
	onClose    func()
	one        sync.Once
}

func newFakeTransport() *fakeTransport {
	return &fakeTransport{reads: make(chan Frame, 32), readErrors: make(chan error, 1), writes: make(chan []byte, 32), done: make(chan struct{})}
}

func (f *fakeTransport) Read(ctx context.Context) (Frame, error) {
	select {
	case frame := <-f.reads:
		return frame, nil
	case err := <-f.readErrors:
		return Frame{}, err
	case <-f.done:
		return Frame{}, errors.New("fake closed")
	case <-ctx.Done():
		return Frame{}, ctx.Err()
	}
}

func (f *fakeTransport) Write(_ context.Context, payload []byte) error {
	select {
	case f.writes <- append([]byte(nil), payload...):
	case <-f.done:
		return &WriteFailure{Err: errors.New("fake closed"), Phase: WriteMayHaveWritten}
	}
	if f.onWrite != nil {
		if err := f.onWrite(payload); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeTransport) Close() error {
	f.one.Do(func() {
		close(f.done)
		if f.onClose != nil {
			f.onClose()
		}
	})
	return nil
}

func pushJSON(t *fakeTransport, value string) {
	t.reads <- Frame{Type: FrameText, Payload: []byte(value)}
}

func response(id string, value string) string {
	return `{"id":` + id + `,"result":` + value + `}`
}

func waitWrite(t *testing.T, f *fakeTransport) map[string]any {
	t.Helper()
	select {
	case raw := <-f.writes:
		var msg map[string]any
		if err := json.Unmarshal(raw, &msg); err != nil {
			t.Fatal(err)
		}
		return msg
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for write")
		return nil
	}
}

func TestInitializeRetainsEarlyEventsAndParsesMetadata(t *testing.T) {
	f := newFakeTransport()
	c := New(f, Options{EventCapacity: 2, HandshakeTimeout: time.Second})
	go func() {
		msg := waitWrite(t, f)
		if msg["method"] != "initialize" {
			t.Errorf("first method = %v", msg["method"])
		}
		pushJSON(f, `{"method":"thread/started","params":{"threadId":"t1"}}`)
		pushJSON(f, response(`"initialize"`, `{"userAgent":"codex/0.154.0 (test)","codexHome":"/tmp/codex","platformFamily":"unix","platformOs":"macos"}`))
	}()
	info, err := c.Initialize(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if info.ServerVersion != "0.154.0" || info.CodexHome != "/tmp/codex" || info.PlatformOS != "macos" {
		t.Fatalf("metadata = %+v", info)
	}
	select {
	case event := <-c.Events():
		if event.Kind != EventNotification || event.Notification.Method != "thread/started" {
			t.Fatalf("early event = %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("early notification was not retained")
	}
	initialized := waitWrite(t, f)
	if initialized["method"] != "initialized" {
		t.Fatalf("initialized write = %+v", initialized)
	}
	_ = c.Close(context.Background())
}

func TestRawResultServerErrorAndIDs(t *testing.T) {
	f := newFakeTransport()
	c := New(f, Options{})
	go func() {
		msg := waitWrite(t, f)
		pushJSON(f, response(`1`, `{"ok":true}`))
		if msg["id"] != float64(1) {
			t.Errorf("request id = %v", msg["id"])
		}
	}()
	result, err := c.Call(context.Background(), RPCRequest{ID: int64(1), Method: "thread/read"})
	if err != nil || string(result.Value) != `{"ok":true}` {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	go func() {
		_ = waitWrite(t, f)
		pushJSON(f, `{"id":"err","error":{"code":-32603,"message":"nope","data":{"x":1}}}`)
	}()
	_, err = c.Call(context.Background(), RPCRequest{ID: "err", Method: "bad"})
	var callErr *CallError
	if !errors.As(err, &callErr) || callErr.Server == nil || callErr.Server.Code != -32603 || string(callErr.Server.Data) != `{"x":1}` {
		t.Fatalf("server error = %T %+v", err, err)
	}
	_ = c.Close(context.Background())
}

func TestServerRequestsAreObserverOnlyIncludingUnknownMethods(t *testing.T) {
	f := newFakeTransport()
	c := New(f, Options{})
	pushJSON(f, `{"id":"s1","method":"approval/request","params":{"secret":"x"}}`)
	select {
	case event := <-c.Events():
		if event.Kind != EventServerRequest || event.Request.Method != "approval/request" {
			t.Fatalf("event = %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("server request was not observed")
	}
	select {
	case written := <-f.writes:
		t.Fatalf("observer sent a response: %s", written)
	case <-time.After(50 * time.Millisecond):
	}
	_ = c.Close(context.Background())
}

func TestPostWriteDisconnectReportsMayHaveWritten(t *testing.T) {
	f := newFakeTransport()
	c := New(f, Options{})
	go func() {
		_ = waitWrite(t, f)
		f.Close()
	}()
	_, err := c.Call(context.Background(), RPCRequest{ID: "post", Method: "thread/start"})
	var callErr *CallError
	if !errors.As(err, &callErr) || callErr.Evidence.Phase != WriteMayHaveWritten || callErr.Evidence.Generation != 1 {
		t.Fatalf("error evidence = %T %+v", err, err)
	}
	_ = c.Close(context.Background())
}

func TestCancellationWithdrawsQueuedWriteBeforeClaimingNotSent(t *testing.T) {
	f := newFakeTransport()
	gate := make(chan struct{})
	first := true
	f.onWrite = func([]byte) error {
		if first {
			first = false
			<-gate
		}
		return nil
	}
	c := New(f, Options{WriterCapacity: 8})
	firstDone := make(chan error, 1)
	go func() {
		_, err := c.Call(context.Background(), RPCRequest{ID: "first", Method: "slow"})
		firstDone <- err
	}()
	_ = waitWrite(t, f)
	ctx, cancel := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() {
		_, err := c.Call(ctx, RPCRequest{ID: "second", Method: "never"})
		secondDone <- err
	}()
	cancel()
	close(gate)
	select {
	case err := <-secondDone:
		var callErr *CallError
		if !errors.As(err, &callErr) || callErr.Evidence.Phase != WriteProvenBeforeWrite {
			t.Fatalf("cancel evidence = %T %+v", err, err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not receive withdrawal acknowledgement")
	}
	_ = c.Close(context.Background())
	<-firstDone
}

func TestBoundedOverflowProducesGapAndDisconnect(t *testing.T) {
	f := newFakeTransport()
	c := New(f, Options{EventCapacity: 1})
	for i := 0; i < 20; i++ {
		pushJSON(f, `{"method":"notice","params":{"i":1}}`)
	}
	closeDeadline := time.Now().Add(time.Second)
	for len(f.reads) != 0 && time.Now().Before(closeDeadline) {
		time.Sleep(time.Millisecond)
	}
	queueDeadline := time.Now().Add(time.Second)
	for len(c.events) < cap(c.events) && time.Now().Before(queueDeadline) {
		time.Sleep(time.Millisecond)
	}
	f.readErrors <- errors.New("fake disconnect")
	deadline := time.After(time.Second)
	var gap, disconnected bool
	for !disconnected {
		select {
		case event := <-c.Events():
			if event.Kind == EventGap && event.Gap.Dropped > 0 {
				gap = true
			}
			if event.Kind == EventDisconnected {
				disconnected = true
			}
		case <-deadline:
			t.Fatal("missing bounded overflow evidence")
		}
	}
	if !gap {
		t.Fatal("overflow did not emit a gap")
	}
}
