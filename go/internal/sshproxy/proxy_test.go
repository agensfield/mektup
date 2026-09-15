package sshproxy

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeProcess struct {
	stdinR      *io.PipeReader
	stdinW      *io.PipeWriter
	stdoutR     *io.PipeReader
	stdoutW     *io.PipeWriter
	stderrR     *io.PipeReader
	stderrW     *io.PipeWriter
	startErr    error
	waitErr     error
	waitBlock   bool
	started     chan struct{}
	waitRelease chan struct{}
	killed      chan struct{}
	killOnce    sync.Once
	releaseOnce sync.Once
}

func newFakeProcess() *fakeProcess {
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	errR, errW := io.Pipe()
	return &fakeProcess{
		stdinR: inR, stdinW: inW, stdoutR: outR, stdoutW: outW, stderrR: errR, stderrW: errW,
		started: make(chan struct{}), waitRelease: make(chan struct{}), killed: make(chan struct{}),
	}
}

func (p *fakeProcess) StdinPipe() (io.WriteCloser, error) { return p.stdinW, nil }
func (p *fakeProcess) StdoutPipe() (io.ReadCloser, error) { return p.stdoutR, nil }
func (p *fakeProcess) StderrPipe() (io.ReadCloser, error) { return p.stderrR, nil }
func (p *fakeProcess) Start() error                       { close(p.started); return p.startErr }
func (p *fakeProcess) Wait() error {
	<-p.waitRelease
	return p.waitErr
}
func (p *fakeProcess) Kill() error {
	p.killOnce.Do(func() {
		close(p.killed)
		if !p.waitBlock {
			p.releaseWait()
		}
		_ = p.stdinR.Close()
		_ = p.stdoutR.Close()
		_ = p.stderrR.Close()
	})
	return nil
}

func (p *fakeProcess) releaseWait() { p.releaseOnce.Do(func() { close(p.waitRelease) }) }

type fakeFactory struct {
	process *fakeProcess
	argv    []string
}

func (f *fakeFactory) New(argv []string) (Process, error) {
	f.argv = append([]string(nil), argv...)
	return f.process, nil
}

func TestProxyArgvIsFixedAndUsesOpenSSHDefaults(t *testing.T) {
	cfg := Config{Host: "ops@example"}
	got, err := cfg.ProxyArgv()
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"ssh", "--", "ops@example", "codex", "app-server", "proxy"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("argv = %#v, want %#v", got, want)
	}
	for _, host := range []string{"host;touch /tmp/pwned", "-oProxyCommand=bad", "host\nname"} {
		if _, err := (Config{Host: host}).ProxyArgv(); !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("host %q was accepted: %v", host, err)
		}
	}
	control, err := cfg.ControlArgv()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.Join(control, " "), "body") || !reflect.DeepEqual(control, []string{"ssh", "--", "ops@example", "mektup", "control", "receive"}) {
		t.Fatalf("control argv = %#v", control)
	}
}

func TestProxyCarriesRawBytesWithoutJSONL(t *testing.T) {
	p := newFakeProcess()
	factory := &fakeFactory{process: p}
	conn, err := Dial(context.Background(), Config{Host: "host"}, factory)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if !reflect.DeepEqual(factory.argv, []string{"ssh", "--", "host", "codex", "app-server", "proxy"}) {
		t.Fatalf("argv = %#v", factory.argv)
	}
	wantIn := []byte{0, 1, 2, '\n', '{', '"', 255}
	inDone := make(chan []byte, 1)
	go func() {
		data, _ := io.ReadAll(io.LimitReader(p.stdinR, int64(len(wantIn))))
		inDone <- data
	}()
	if n, err := conn.Write(wantIn); err != nil || n != len(wantIn) {
		t.Fatalf("write n=%d err=%v", n, err)
	}
	select {
	case got := <-inDone:
		if !bytes.Equal(got, wantIn) {
			t.Fatalf("stdin bytes = %v, want %v", got, wantIn)
		}
	case <-time.After(time.Second):
		t.Fatal("raw stdin did not arrive")
	}
	wantOut := []byte{0xff, 0x00, '{', '\n', 'x'}
	go func() { _, _ = p.stdoutW.Write(wantOut) }()
	got := make([]byte, len(wantOut))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, wantOut) {
		t.Fatalf("stdout bytes = %v, want %v", got, wantOut)
	}
}

func TestProxyFailureKindsAndPossibleWriteEvidence(t *testing.T) {
	t.Run("spawn", func(t *testing.T) {
		_, err := Dial(context.Background(), Config{Host: "host"}, ProcessFactoryFunc(func([]string) (Process, error) {
			return nil, errors.New("permission denied creating process")
		}))
		var failure *Failure
		if !errors.As(err, &failure) || failure.Kind != FailureSpawn || failure.Evidence != WriteNotStarted {
			t.Fatalf("err = %T %+v", err, err)
		}
	})
	for _, test := range []struct {
		name, stderr string
		kind         FailureKind
	}{
		{name: "authentication", stderr: "Permission denied (publickey).\n", kind: FailureAuthentication},
		{name: "proxy", stderr: "codex: command not found\n", kind: FailureProxy},
	} {
		t.Run(test.name, func(t *testing.T) {
			p := newFakeProcess()
			p.waitErr = errors.New("exit status 255")
			factory := &fakeFactory{process: p}
			conn, err := Dial(context.Background(), Config{Host: "host"}, factory)
			if err != nil {
				t.Fatal(err)
			}
			go func() {
				_, _ = p.stderrW.Write([]byte(test.stderr))
				_ = p.stderrW.Close()
				p.releaseWait()
			}()
			failure := conn.Wait()
			var got *Failure
			if !errors.As(failure, &got) || got.Kind != test.kind || got.Evidence != WriteNotStarted {
				t.Fatalf("failure = %T %+v", failure, failure)
			}
		})
	}
	t.Run("possible-write", func(t *testing.T) {
		p := newFakeProcess()
		p.waitErr = errors.New("proxy exited")
		factory := &fakeFactory{process: p}
		conn, err := Dial(context.Background(), Config{Host: "host"}, factory)
		if err != nil {
			t.Fatal(err)
		}
		go func() { _, _ = io.ReadAll(p.stdinR) }()
		if _, err := conn.Write([]byte("request")); err != nil {
			t.Fatalf("write err = %v", err)
		}
		_ = p.stdinR.Close()
		_ = p.stdoutW.Close()
		_ = p.stderrW.Close()
		p.releaseWait()
		failure := conn.Wait()
		var got *Failure
		if !errors.As(failure, &got) || got.Kind != FailurePossibleWrite || got.Evidence != WriteComplete {
			t.Fatalf("failure = %T %+v", failure, failure)
		}
	})
}

func TestCancellationKillsChildWithinBound(t *testing.T) {
	p := newFakeProcess()
	p.waitBlock = true
	factory := &fakeFactory{process: p}
	ctx, cancel := context.WithCancel(context.Background())
	conn, err := Dial(ctx, Config{Host: "host", CleanupTimeout: 20 * time.Millisecond}, factory)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-p.killed:
	case <-time.After(time.Second):
		t.Fatal("cancellation did not kill child")
	}
	start := time.Now()
	err = conn.Close()
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("close exceeded bounded cleanup")
	}
	if err != nil && !errors.Is(err, ErrCleanupTimeout) {
		t.Fatalf("close err = %v", err)
	}
	p.releaseWait()
}
