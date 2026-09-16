package appserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeTransport struct {
	reads          chan Frame
	readErrors     chan error
	writes         chan []byte
	done           chan struct{}
	onWrite        func([]byte) error
	onWriteContext func(context.Context, []byte) error
	onClose        func()
	one            sync.Once
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

func (f *fakeTransport) Write(ctx context.Context, payload []byte) error {
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
	if f.onWriteContext != nil {
		if err := f.onWriteContext(ctx, payload); err != nil {
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

func TestFrameObserverSeesExactInboundAndOutboundTextFrames(t *testing.T) {
	f := newFakeTransport()
	var observed []string
	c := New(f, Options{FrameObserver: func(direction FrameDirection, frame Frame) error {
		observed = append(observed, fmt.Sprintf("%d:%s", direction, frame.Payload))
		return nil
	}})
	f.onWrite = func(payload []byte) error {
		var msg map[string]any
		_ = json.Unmarshal(payload, &msg)
		if msg["method"] == "initialize" {
			pushJSON(f, response(`"initialize"`, `{"userAgent":"codex/0.154.0"}`))
		}
		return nil
	}
	if _, err := c.Initialize(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer c.Close(context.Background())
	if len(observed) < 3 || !strings.Contains(observed[0], `"method":"initialize"`) || !strings.Contains(observed[1], `"userAgent":"codex/0.154.0"`) || !strings.Contains(observed[2], `"method":"initialized"`) {
		t.Fatalf("observed frames = %#v", observed)
	}
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
	result, err := c.call(context.Background(), RPCRequest{ID: int64(1), Method: "thread/read"})
	if err != nil || string(result.Value) != `{"ok":true}` {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	go func() {
		_ = waitWrite(t, f)
		pushJSON(f, `{"id":"err","error":{"code":-32603,"message":"nope","data":{"x":1}}}`)
	}()
	_, err = c.call(context.Background(), RPCRequest{ID: "err", Method: "bad"})
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
	_, err := c.call(context.Background(), RPCRequest{ID: "post", Method: "thread/start"})
	var callErr *CallError
	if !errors.As(err, &callErr) || callErr.Evidence.Phase != WriteMayHaveWritten || callErr.Evidence.Generation != c.Generation() {
		t.Fatalf("error evidence = %T %+v phase=%v gen=%d", err, err, callErr.Evidence.Phase, callErr.Evidence.Generation)
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
		_, err := c.call(context.Background(), RPCRequest{ID: "first", Method: "slow"})
		firstDone <- err
	}()
	_ = waitWrite(t, f)
	ctx, cancel := context.WithCancel(context.Background())
	secondDone := make(chan error, 1)
	go func() {
		_, err := c.call(ctx, RPCRequest{ID: "second", Method: "never"})
		secondDone <- err
	}()
	queuedDeadline := time.Now().Add(time.Second)
	for len(c.commands) == 0 && time.Now().Before(queuedDeadline) {
		time.Sleep(time.Millisecond)
	}
	if len(c.commands) == 0 {
		t.Fatal("second request did not reach the writer queue")
	}
	cancel()
	select {
	case err := <-secondDone:
		var callErr *CallError
		if !errors.As(err, &callErr) || callErr.Evidence.Phase != WriteProvenBeforeWrite {
			t.Fatalf("cancel evidence = %T %+v", err, err)
		}
	case <-time.After(time.Second):
		t.Fatal("cancellation did not receive withdrawal acknowledgement")
	}
	close(gate)
	_ = c.Close(context.Background())
	<-firstDone
}

func TestActiveCancellationNeverClaimsProvenBeforeWrite(t *testing.T) {
	for i := 0; i < 100; i++ {
		f := newFakeTransport()
		started := make(chan struct{})
		release := make(chan struct{})
		f.onWrite = func([]byte) error {
			close(started)
			<-release
			return nil
		}
		c := New(f, Options{})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			_, err := c.call(ctx, RPCRequest{ID: "active", Method: "write"})
			done <- err
		}()
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("writer did not become active")
		}
		cancel()
		close(release)
		select {
		case err := <-done:
			var callErr *CallError
			if !errors.As(err, &callErr) || callErr.Evidence.Phase == WriteProvenBeforeWrite {
				t.Fatalf("iteration %d active cancellation evidence = %T %+v", i, err, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("iteration %d active cancellation hung", i)
		}
		_ = c.Close(context.Background())
	}
}

func TestFailedAdmissionDoesNotCancelActiveWriter(t *testing.T) {
	f := newFakeTransport()
	f.onWrite = func([]byte) error {
		<-f.done
		return errors.New("closed")
	}
	c := New(f, Options{WriterCapacity: 1})
	defer c.Close(context.Background())
	go c.call(context.Background(), RPCRequest{ID: "active", Method: "blocked"})
	_ = waitWrite(t, f)
	go c.call(context.Background(), RPCRequest{ID: "queued", Method: "queued"})
	queueDeadline := time.Now().Add(time.Second)
	for len(c.commands) != 1 && time.Now().Before(queueDeadline) {
		time.Sleep(time.Millisecond)
	}
	if len(c.commands) != 1 {
		t.Fatal("queue not full")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.call(ctx, RPCRequest{ID: "never-admitted", Method: "unused"})
	var callErr *CallError
	if !errors.As(err, &callErr) || callErr.Evidence.Phase != WriteProvenBeforeWrite {
		t.Fatalf("failed admission evidence = %+v", err)
	}
	select {
	case <-f.done:
		t.Fatal("failed admission canceled unrelated active writer")
	default:
	}
	_ = f.Close()
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

func TestStalledWriteIsCanceledAndConnectionCloses(t *testing.T) {
	f := newFakeTransport()
	f.onWriteContext = func(ctx context.Context, _ []byte) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-f.done:
			return errors.New("fake closed")
		}
	}
	c := New(f, Options{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := c.call(ctx, RPCRequest{ID: "stall", Method: "slow"})
		done <- err
	}()
	_ = waitWrite(t, f)
	cancel()
	select {
	case err := <-done:
		var callErr *CallError
		if !errors.As(err, &callErr) || callErr.Evidence.Phase != WriteMayHaveWritten {
			t.Fatalf("stalled write evidence = %T %+v", err, err)
		}
	case <-time.After(time.Second):
		t.Fatal("stalled writer did not acknowledge cancellation")
	}
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("stalled writer did not close connection")
	}
}

func TestCanceledBeforeEnqueueReleasesIDForReuse(t *testing.T) {
	f := newFakeTransport()
	c := New(f, Options{WriterCapacity: 1})
	// Occupy the writer with a context-aware write so the second command cannot
	// enqueue before its canceled context is observed.
	f.onWriteContext = func(ctx context.Context, _ []byte) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-f.done:
			return errors.New("fake closed")
		}
	}
	firstDone := make(chan error, 1)
	go func() {
		_, err := c.call(context.Background(), RPCRequest{ID: "occupy", Method: "slow"})
		firstDone <- err
	}()
	_ = waitWrite(t, f)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	secondDone := make(chan error, 1)
	go func() {
		_, err := c.call(ctx, RPCRequest{ID: "reuse", Method: "never"})
		secondDone <- err
	}()
	// The first writer owns the pump. Closing the transport lets it finish so
	// the queued cancellation command can be acknowledged.
	time.Sleep(10 * time.Millisecond)
	_ = f.Close()
	select {
	case err := <-secondDone:
		if err == nil {
			t.Fatal("canceled pre-enqueue call unexpectedly succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("canceled queued call did not complete")
	}
	closeErr := c.Close(context.Background())
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	<-firstDone
	// A fresh connection may reuse the ID, proving the canceled reservation was
	// not left in a withdrawn/reserved state.
	f2 := newFakeTransport()
	c2 := New(f2, Options{})
	go func() {
		_ = waitWrite(t, f2)
		pushJSON(f2, response(`"reuse"`, `true`))
	}()
	if _, err := c2.call(context.Background(), RPCRequest{ID: "reuse", Method: "ok"}); err != nil {
		t.Fatal(err)
	}
	_ = c2.Close(context.Background())
}

func TestIDsAreCanonicalStrictAndCompletedIDsAreRetired(t *testing.T) {
	f := newFakeTransport()
	c := New(f, Options{})
	go func() {
		_ = waitWrite(t, f)
		pushJSON(f, response(`"ab"`, `true`))
	}()
	if _, err := c.call(context.Background(), RPCRequest{ID: json.RawMessage(`"a\u0062"`), Method: "canonical"}); err != nil {
		t.Fatal(err)
	}
	_, err := c.call(context.Background(), RPCRequest{ID: "ab", Method: "stale"})
	var callErr *CallError
	if !errors.As(err, &callErr) || callErr.Evidence.Phase != WriteProvenBeforeWrite {
		t.Fatalf("retired ID reuse = %v", err)
	}
	select {
	case payload := <-f.writes:
		t.Fatalf("retired ID reuse wrote %s", payload)
	default:
	}
	if _, err := c.call(context.Background(), RPCRequest{ID: uint64(^uint64(0)), Method: "bad"}); err == nil {
		t.Fatal("unsigned overflow ID was accepted")
	}
	_ = c.Close(context.Background())

	f2 := newFakeTransport()
	c2 := New(f2, Options{})
	go func() {
		_ = waitWrite(t, f2)
		pushJSON(f2, `{"id":"shape"}`)
	}()
	if _, err := c2.call(context.Background(), RPCRequest{ID: "shape", Method: "shape"}); err == nil {
		t.Fatal("response without result/error was accepted")
	}
	_ = c2.Close(context.Background())
}

func TestHealthyServerOneResponse(t *testing.T) {
	f := newFakeTransport()
	c := New(f, Options{})
	defer closeReview(c)
	go func() {
		_ = waitWrite(t, f)
		pushJSON(f, `{"id":"healthy","result":{"accepted":true}}`)
	}()
	result, err := c.call(context.Background(), RPCRequest{ID: "healthy", Method: "read"})
	if err != nil || string(result.Value) != `{"accepted":true}` {
		t.Fatalf("healthy one-response call result=%+v err=%v", result, err)
	}
}

func TestGenerationIncreasesAcrossConnections(t *testing.T) {
	f1, f2 := newFakeTransport(), newFakeTransport()
	c1, c2 := New(f1, Options{}), New(f2, Options{})
	if c2.Generation() <= c1.Generation() {
		t.Fatalf("generations did not increase: %d then %d", c1.Generation(), c2.Generation())
	}
	_ = c1.Close(context.Background())
	_ = c2.Close(context.Background())
}

func TestQueuedCancellationTerminatesUnrelatedStalledWriter(t *testing.T) {
	f := newFakeTransport()
	f.onWrite = func([]byte) error {
		<-f.done
		return errors.New("closed")
	}
	c := New(f, Options{})
	defer c.Close(context.Background())
	first := make(chan error, 1)
	go func() {
		_, err := c.call(context.Background(), RPCRequest{ID: "first", Method: "blocked"})
		first <- err
	}()
	_ = waitWrite(t, f)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	second := make(chan error, 1)
	go func() {
		_, err := c.call(ctx, RPCRequest{ID: "second", Method: "queued"})
		second <- err
	}()
	select {
	case err := <-second:
		var callErr *CallError
		if !errors.As(err, &callErr) || (callErr.Evidence.Phase != WriteMayHaveWritten && callErr.Evidence.Phase != WriteProvenBeforeWrite) {
			t.Fatalf("queued cancellation evidence = %T %+v", err, err)
		}
	case <-time.After(time.Second):
		t.Fatal("queued cancellation stuck behind unrelated active write")
	}
	_ = f.Close()
	<-first
}

func TestHandshakeTimeoutCoversInitializedWrite(t *testing.T) {
	f := newFakeTransport()
	f.onWrite = func(payload []byte) error {
		if string(payload) == `{"method":"initialized"}` {
			<-f.done
			return errors.New("closed")
		}
		pushJSON(f, `{"id":"initialize","result":{"userAgent":"codex/0.154.0"}}`)
		return nil
	}
	c := New(f, Options{HandshakeTimeout: 10 * time.Millisecond})
	defer c.Close(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := c.Initialize(context.Background())
		done <- err
	}()
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		f.Close()
		<-done
		t.Fatal("handshake timeout did not cover initialized notification")
	}
}

func TestMalformedRPCShapesAndBinaryFrameDisconnect(t *testing.T) {
	for _, payload := range []string{
		`{"id":"r","error":null}`,
		`{"id":"r","error":{}}`,
		`{"method":null}`,
		`{"id":null,"result":{}}`,
	} {
		if _, err := decodeMessage([]byte(payload)); err == nil {
			t.Errorf("accepted malformed message: %s", payload)
		}
	}
	f := newFakeTransport()
	c := New(f, Options{})
	f.reads <- Frame{Type: FrameBinary, Payload: []byte("not JSON-RPC")}
	select {
	case event := <-c.Events():
		if event.Kind != EventDisconnected {
			t.Fatalf("binary frame event = %+v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("binary frame did not disconnect client")
	}
}

func TestPublicCallRequiresInitialize(t *testing.T) {
	f := newFakeTransport()
	c := New(f, Options{})
	defer c.Close(context.Background())
	if _, err := c.Call(context.Background(), RPCRequest{ID: "before", Method: "thread/read"}); err == nil {
		t.Fatal("operational call before initialize unexpectedly dispatched")
	}
	select {
	case write := <-f.writes:
		t.Fatalf("pre-init call wrote %s", write)
	default:
	}
}

func TestNegativeZeroIDMatchesCanonicalZeroResponse(t *testing.T) {
	f := newFakeTransport()
	c := New(f, Options{})
	defer c.Close(context.Background())
	go func() {
		_ = waitWrite(t, f)
		pushJSON(f, `{"id":0,"result":{"ok":true}}`)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := c.call(ctx, RPCRequest{ID: json.Number("-0"), Method: "read"}); err != nil {
		t.Fatalf("numeric -0/0 response identity lost: %v", err)
	}
}

func TestCloseConcurrentReadAlwaysTerminatesPump(t *testing.T) {
	for i := 0; i < 50; i++ {
		f := newFakeTransport()
		pushJSON(f, `{"method":"notice"}`)
		c := New(f, Options{})
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		err := c.Close(ctx)
		cancel()
		if err != nil {
			t.Fatalf("iteration %d close: %v", i, err)
		}
	}
}

func closeReview(c *Client) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = c.Close(ctx)
}

func TestGateNegativeZeroMatchesCanonicalResponse(t *testing.T) {
	f := newFakeTransport()
	c := New(f, Options{})
	defer closeReview(c)
	go func() {
		<-f.writes
		pushJSON(f, `{"id":0,"result":{"ok":true}}`)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := c.call(ctx, RPCRequest{ID: json.Number("-0"), Method: "read"}); err != nil {
		t.Fatalf("numeric -0/0 response identity lost: %v", err)
	}
}

func TestGateCloseWithConcurrentRead(t *testing.T) {
	for i := 0; i < 200; i++ {
		f := newFakeTransport()
		pushJSON(f, `{"method":"notice"}`)
		c := New(f, Options{})
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		err := c.Close(ctx)
		cancel()
		if err != nil {
			t.Fatalf("iteration %d Close left pump alive with concurrent read: %v", i, err)
		}
	}
}

func TestGatePublicCallRequiresInitialize(t *testing.T) {
	f := newFakeTransport()
	c := New(f, Options{})
	defer closeReview(c)
	_, err := c.Call(context.Background(), RPCRequest{ID: "before", Method: "turn/start"})
	var callErr *CallError
	if !errors.As(err, &callErr) || callErr.Evidence.Phase != WriteProvenBeforeWrite {
		t.Fatalf("preinit gate = %v", err)
	}
	select {
	case payload := <-f.writes:
		t.Fatalf("preinit wrote %s", payload)
	default:
	}
}
