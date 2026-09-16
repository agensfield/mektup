package endpoint

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agensfield/mektup/go/internal/sshproxy"
)

const testEndpointID = "ep_00000000-0000-7000-8000-000000000001"

type herdrTestProcess struct {
	stdin   io.WriteCloser
	stdout  io.ReadCloser
	stderr  io.ReadCloser
	start   func() error
	wait    func() error
	kill    func() error
	started chan struct{}
	mu      sync.Mutex
	kills   int
	waits   int
}

type testWriteCloser struct{ io.Writer }

func (testWriteCloser) Close() error { return nil }

type failingReadCloser struct{}

func (failingReadCloser) Read([]byte) (int, error) { return 0, errors.New("synthetic read failure") }
func (failingReadCloser) Close() error             { return nil }

func (p *herdrTestProcess) StdinPipe() (io.WriteCloser, error) { return p.stdin, nil }
func (p *herdrTestProcess) StdoutPipe() (io.ReadCloser, error) { return p.stdout, nil }
func (p *herdrTestProcess) StderrPipe() (io.ReadCloser, error) { return p.stderr, nil }
func (p *herdrTestProcess) Start() error {
	if p.started != nil {
		close(p.started)
	}
	if p.start != nil {
		return p.start()
	}
	return nil
}
func (p *herdrTestProcess) Wait() error {
	p.mu.Lock()
	p.waits++
	p.mu.Unlock()
	if p.wait != nil {
		return p.wait()
	}
	return nil
}
func (p *herdrTestProcess) Kill() error {
	p.mu.Lock()
	p.kills++
	p.mu.Unlock()
	if p.kill != nil {
		return p.kill()
	}
	return nil
}

func bufferedHerdrProcess(output, stderr []byte) *herdrTestProcess {
	return &herdrTestProcess{
		stdin:  testWriteCloser{Writer: io.Discard},
		stdout: io.NopCloser(bytes.NewReader(output)),
		stderr: io.NopCloser(bytes.NewReader(stderr)),
	}
}

type testExecProcess struct{ cmd *exec.Cmd }

func (p *testExecProcess) StdinPipe() (io.WriteCloser, error) { return p.cmd.StdinPipe() }
func (p *testExecProcess) StdoutPipe() (io.ReadCloser, error) { return p.cmd.StdoutPipe() }
func (p *testExecProcess) StderrPipe() (io.ReadCloser, error) { return p.cmd.StderrPipe() }
func (p *testExecProcess) Start() error                       { return p.cmd.Start() }
func (p *testExecProcess) Wait() error                        { return p.cmd.Wait() }
func (p *testExecProcess) Kill() error                        { return p.cmd.Process.Kill() }

func testEndpoint(t *testing.T, host string) Endpoint {
	t.Helper()
	route, err := SSHRoute(host)
	if err != nil {
		t.Fatal(err)
	}
	return Endpoint{ID: testEndpointID, Alias: "remote", Route: route, Herdr: HerdrAuto}
}

func TestRemoteHerdrRunnerUsesFixedArgvAndRouteHost(t *testing.T) {
	p := bufferedHerdrProcess([]byte(`{"agents":[]}`), nil)
	var got []string
	runner := RemoteHerdrRunner{
		Config: HerdrRunnerConfig{SSH: sshproxy.Config{Host: "route.example", SSHBinary: "ssh"}},
		Factory: sshproxy.ProcessFactoryFunc(func(argv []string) (sshproxy.Process, error) {
			got = append([]string(nil), argv...)
			return p, nil
		}),
	}
	out, err := runner.RunEndpoint(context.Background(), testEndpoint(t, "route.example"), []string{"herdr", "agent", "list"})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, []byte(`{"agents":[]}`)) {
		t.Fatalf("output = %q", out)
	}
	want := []string{"ssh", "--", "route.example", "herdr", "agent", "list"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("argv = %#v, want %#v", got, want)
	}
}

func TestRemoteHerdrRunnerRejectsHostMismatchAndArbitraryCommands(t *testing.T) {
	runner := RemoteHerdrRunner{Config: HerdrRunnerConfig{SSH: sshproxy.Config{Host: "other.example"}}}
	_, err := runner.RunEndpoint(context.Background(), testEndpoint(t, "route.example"), []string{"herdr", "agent", "list"})
	if !errors.Is(err, ErrHerdrRunnerHostMismatch) {
		t.Fatalf("host mismatch = %v", err)
	}
	runner.Config.SSH.Host = "route.example"
	for _, argv := range [][]string{
		{"sh", "-c", "touch /tmp/pwned"},
		{"herdr", "agent", "get", "--:p1"},
		{"herdr", "agent", "get", "-x:p1"},
		{"herdr", "agent", "get", "w:p1:extra"},
		{"herdr", "agent", "get", "workspace:pane with spaces"},
		{"herdr", "agent", "get", "workspace:p1/../../secret"},
	} {
		_, err := runner.RunEndpoint(context.Background(), testEndpoint(t, "route.example"), argv)
		if !errors.Is(err, ErrHerdrRunnerInvalidCommand) {
			t.Errorf("argv %#v error = %v", argv, err)
		}
	}
}

func TestRemoteHerdrRunnerAcceptsNormalPaneID(t *testing.T) {
	p := bufferedHerdrProcess([]byte(`{"agent":{}}`), nil)
	var got []string
	runner := RemoteHerdrRunner{
		Config: HerdrRunnerConfig{SSH: sshproxy.Config{Host: "route.example"}},
		Factory: sshproxy.ProcessFactoryFunc(func(argv []string) (sshproxy.Process, error) {
			got = append([]string(nil), argv...)
			return p, nil
		}),
	}
	if _, err := runner.RunEndpoint(context.Background(), testEndpoint(t, "route.example"), []string{"herdr", "agent", "get", "w3:p1"}); err != nil {
		t.Fatal(err)
	}
	want := []string{"ssh", "--", "route.example", "herdr", "agent", "get", "w3:p1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("argv = %#v, want %#v", got, want)
	}
}

func TestRemoteHerdrRunnerBoundsOutputAndReaps(t *testing.T) {
	p := bufferedHerdrProcess(bytes.Repeat([]byte("x"), 17), nil)
	runner := RemoteHerdrRunner{
		Config:  HerdrRunnerConfig{SSH: sshproxy.Config{Host: "route.example"}, OutputLimit: 16, CleanupTimeout: time.Second},
		Factory: sshproxy.ProcessFactoryFunc(func([]string) (sshproxy.Process, error) { return p, nil }),
	}
	_, err := runner.RunEndpoint(context.Background(), testEndpoint(t, "route.example"), []string{"herdr", "agent", "list"})
	var failure *HerdrRunnerFailure
	if !errors.As(err, &failure) || failure.Kind != HerdrRunnerOutputFailure || !failure.OutputTruncated {
		t.Fatalf("overflow error = %#v", err)
	}
	p.mu.Lock()
	kills, waits := p.kills, p.waits
	p.mu.Unlock()
	if kills != 1 || waits != 1 {
		t.Fatalf("lifecycle kills=%d waits=%d", kills, waits)
	}
}

func TestRemoteHerdrRunnerCancellationIsBoundedAndReaps(t *testing.T) {
	stdoutR, stdoutW := io.Pipe()
	stderrR, stderrW := io.Pipe()
	killed := make(chan struct{})
	p := &herdrTestProcess{
		stdin: testWriteCloser{Writer: io.Discard}, stdout: stdoutR, stderr: stderrR,
		wait: func() error { <-killed; return nil },
		kill: func() error {
			select {
			case <-killed:
			default:
				close(killed)
			}
			_ = stdoutW.Close()
			_ = stderrW.Close()
			return nil
		},
	}
	runner := RemoteHerdrRunner{
		Config:  HerdrRunnerConfig{SSH: sshproxy.Config{Host: "route.example"}, CommandTimeout: time.Second, CleanupTimeout: time.Second},
		Factory: sshproxy.ProcessFactoryFunc(func([]string) (sshproxy.Process, error) { return p, nil }),
	}
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)
	defer cancel()
	started := time.Now()
	_, err := runner.RunEndpoint(ctx, testEndpoint(t, "route.example"), []string{"herdr", "agent", "list"})
	if time.Since(started) > time.Second {
		t.Fatal("cancellation exceeded bound")
	}
	var failure *HerdrRunnerFailure
	if !errors.As(err, &failure) || failure.Kind != HerdrRunnerCanceled || !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error = %#v", err)
	}
	p.mu.Lock()
	kills, waits := p.kills, p.waits
	p.mu.Unlock()
	if kills != 1 || waits != 1 {
		t.Fatalf("lifecycle kills=%d waits=%d", kills, waits)
	}
}

func TestRemoteHerdrRunnerReadFailureCannotBecomeSuccess(t *testing.T) {
	p := &herdrTestProcess{
		stdin:  testWriteCloser{Writer: io.Discard},
		stdout: failingReadCloser{},
		stderr: io.NopCloser(bytes.NewReader(nil)),
	}
	runner := RemoteHerdrRunner{
		Config:  HerdrRunnerConfig{SSH: sshproxy.Config{Host: "route.example"}},
		Factory: sshproxy.ProcessFactoryFunc(func([]string) (sshproxy.Process, error) { return p, nil }),
	}
	_, err := runner.RunEndpoint(context.Background(), testEndpoint(t, "route.example"), []string{"herdr", "agent", "list"})
	var failure *HerdrRunnerFailure
	if !errors.As(err, &failure) || failure.Kind != HerdrRunnerCommandFailure || !strings.Contains(failure.Error(), "synthetic read failure") {
		t.Fatalf("read failure = %#v", err)
	}
}

func TestRemoteHerdrRunnerPreservesExitAndStderrEvidence(t *testing.T) {
	runner := RemoteHerdrRunner{
		Config: HerdrRunnerConfig{SSH: sshproxy.Config{Host: "route.example"}},
		Factory: sshproxy.ProcessFactoryFunc(func([]string) (sshproxy.Process, error) {
			return &testExecProcess{cmd: exec.Command("sh", "-c", "printf failure >&2; exit 7")}, nil
		}),
	}
	_, err := runner.RunEndpoint(context.Background(), testEndpoint(t, "route.example"), []string{"herdr", "agent", "list"})
	var failure *HerdrRunnerFailure
	if !errors.As(err, &failure) || failure.Kind != HerdrRunnerCommandFailure || failure.ExitCode != 7 || failure.Stderr != "failure" {
		t.Fatalf("exit evidence = %#v", err)
	}
	if strings.Contains(failure.Error(), "failure") {
		t.Fatal("failure Error leaked stderr")
	}
}

type coordinatedReadCloser struct{ release <-chan struct{} }

func (r coordinatedReadCloser) Read([]byte) (int, error) { <-r.release; return 0, io.EOF }
func (coordinatedReadCloser) Close() error               { return nil }

func TestRemoteHerdrRunnerCleanupDrainIsBounded(t *testing.T) {
	release := make(chan struct{})
	p := &herdrTestProcess{
		stdin:  testWriteCloser{Writer: io.Discard},
		stdout: coordinatedReadCloser{release: release},
		stderr: coordinatedReadCloser{release: release},
	}
	runner := RemoteHerdrRunner{
		Config:  HerdrRunnerConfig{SSH: sshproxy.Config{Host: "route.example"}, CommandTimeout: 10 * time.Millisecond, CleanupTimeout: 20 * time.Millisecond},
		Factory: sshproxy.ProcessFactoryFunc(func([]string) (sshproxy.Process, error) { return p, nil }),
	}
	started := time.Now()
	_, err := runner.RunEndpoint(context.Background(), testEndpoint(t, "route.example"), []string{"herdr", "agent", "list"})
	close(release)
	if time.Since(started) > time.Second {
		t.Fatal("cleanup drain exceeded bound")
	}
	if !errors.Is(err, sshproxy.ErrCleanupTimeout) {
		t.Fatalf("cleanup error = %v", err)
	}
}

func TestRemoteHerdrRunnerBlockedStartCannotEscapeAfterTimeout(t *testing.T) {
	startRelease := make(chan struct{})
	reaped := make(chan struct{})
	p := &herdrTestProcess{
		stdin: testWriteCloser{Writer: io.Discard}, stdout: io.NopCloser(bytes.NewReader(nil)), stderr: io.NopCloser(bytes.NewReader(nil)),
		start: func() error { <-startRelease; return nil },
		wait:  func() error { close(reaped); return nil },
	}
	runner := RemoteHerdrRunner{
		Config:  HerdrRunnerConfig{SSH: sshproxy.Config{Host: "route.example"}, StartupTimeout: 10 * time.Millisecond, CleanupTimeout: 50 * time.Millisecond},
		Factory: sshproxy.ProcessFactoryFunc(func([]string) (sshproxy.Process, error) { return p, nil }),
	}
	_, err := runner.RunEndpoint(context.Background(), testEndpoint(t, "route.example"), []string{"herdr", "agent", "list"})
	var failure *HerdrRunnerFailure
	if !errors.As(err, &failure) || failure.Kind != HerdrRunnerCleanupFailure || !errors.Is(err, sshproxy.ErrCleanupTimeout) {
		t.Fatalf("startup timeout = %#v", err)
	}
	close(startRelease)
	select {
	case <-reaped:
	case <-time.After(time.Second):
		t.Fatal("blocked start was not reaped")
	}
	p.mu.Lock()
	kills, waits := p.kills, p.waits
	p.mu.Unlock()
	if kills != 1 || waits != 1 {
		t.Fatalf("lifecycle kills=%d waits=%d", kills, waits)
	}
}

func TestRemoteHerdrRunnerDrainsLargeLocalBurst(t *testing.T) {
	runner := RemoteHerdrRunner{
		Config: HerdrRunnerConfig{SSH: sshproxy.Config{Host: "route.example"}, OutputLimit: 256 << 10},
		Factory: sshproxy.ProcessFactoryFunc(func([]string) (sshproxy.Process, error) {
			return &testExecProcess{cmd: exec.Command("head", "-c", "131072", "/dev/zero")}, nil
		}),
	}
	out, err := runner.RunEndpoint(context.Background(), testEndpoint(t, "route.example"), []string{"herdr", "agent", "list"})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 131072 {
		t.Fatalf("burst output length = %d", len(out))
	}
}
