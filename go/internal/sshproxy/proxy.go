// Package sshproxy contains the deliberately small SSH boundary used by
// Mektup.  It starts the system OpenSSH client with a fixed argv and exposes
// the child stdio as a byte stream.  The stream is the app-server's raw
// HTTP/WebSocket transport; it is not JSONL and this package does not inspect
// or rewrite its bytes.
//
// The package does not own SSH credentials, SSH configuration, or a Codex
// daemon.  The inherited OpenSSH environment and normal user configuration
// remain authoritative.  In particular, this package never runs a shell and
// never puts a message body, filesystem path, or RPC method in argv.
package sshproxy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	DefaultSSHBinary      = "ssh"
	DefaultCleanupTimeout = 2 * time.Second
	defaultReadQueue      = 64
	defaultReadChunk      = 32 << 10
	defaultStderrLimit    = 64 << 10
	defaultControlLimit   = 4 << 20
)

var (
	ErrInvalidConfig     = errors.New("invalid SSH proxy configuration")
	ErrCleanupTimeout    = errors.New("SSH child cleanup timed out")
	ErrControlTooLarge   = errors.New("SSH control response exceeds limit")
	ErrControlValidation = errors.New("invalid SSH control request")
)

// FailureKind identifies the first useful failure boundary.  It deliberately
// does not collapse a child spawn failure, SSH authentication failure, remote
// proxy failure, and an ordinary stream EOF into one transport error.
type FailureKind string

const (
	FailureSpawn          FailureKind = "spawn"
	FailureAuthentication FailureKind = "authentication"
	FailureProxy          FailureKind = "proxy"
	FailureEOF            FailureKind = "eof"
	FailurePossibleWrite  FailureKind = "possible_write"
	FailureCanceled       FailureKind = "canceled"
)

// WriteEvidence records what a local writer knows, not what the remote
// daemon accepted.  A pipe write cannot prove that the app-server processed a
// WebSocket frame once the process or connection fails.
type WriteEvidence string

const (
	WriteNotStarted     WriteEvidence = "not_started"
	WriteMayHaveWritten WriteEvidence = "may_have_written"
	WriteComplete       WriteEvidence = "complete"
)

// Failure is safe to put in a receipt after redaction. Stderr is retained as
// evidence for a caller that explicitly asks for diagnostics, but Error never
// includes it because SSH implementations may echo sensitive configuration.
type Failure struct {
	Kind        FailureKind
	Cause       FailureKind
	Evidence    WriteEvidence
	Err         error
	ExitCode    int
	Stderr      string
	StderrTrunc bool
}

func (f *Failure) Error() string {
	if f == nil {
		return "SSH proxy failed"
	}
	if f.Err != nil {
		return fmt.Sprintf("SSH proxy %s: %v", f.Kind, f.Err)
	}
	return fmt.Sprintf("SSH proxy %s", f.Kind)
}

func (f *Failure) Unwrap() error {
	if f == nil {
		return nil
	}
	return f.Err
}

func (f *Failure) Timeout() bool {
	return f != nil && errors.Is(f.Err, os.ErrDeadlineExceeded)
}

func (f *Failure) Temporary() bool { return f != nil && f.Timeout() }

// Config is local route metadata. Empty binary fields select the system
// defaults. No field represents a remote shell fragment.
type Config struct {
	Host           string
	SSHBinary      string
	CleanupTimeout time.Duration
	StderrLimit    int64
	ControlLimit   int64
}

func (c Config) normalized() Config {
	if c.SSHBinary == "" {
		c.SSHBinary = DefaultSSHBinary
	}
	if c.CleanupTimeout <= 0 {
		c.CleanupTimeout = DefaultCleanupTimeout
	}
	if c.StderrLimit <= 0 {
		c.StderrLimit = defaultStderrLimit
	}
	if c.ControlLimit <= 0 {
		c.ControlLimit = defaultControlLimit
	}
	return c
}

func (c Config) Validate() error {
	c = c.normalized()
	if c.Host == "" || strings.ContainsAny(c.Host, "\x00\r\n\t ") || strings.HasPrefix(c.Host, "-") {
		return fmt.Errorf("%w: invalid OpenSSH host", ErrInvalidConfig)
	}
	if !validExecutable(c.SSHBinary, true) {
		return fmt.Errorf("%w: invalid SSH executable", ErrInvalidConfig)
	}
	if c.CleanupTimeout <= 0 || c.StderrLimit <= 0 || c.ControlLimit <= 0 {
		return fmt.Errorf("%w: bounds must be positive", ErrInvalidConfig)
	}
	return nil
}

func validExecutable(value string, localPath bool) bool {
	if value == "" || strings.ContainsAny(value, "\x00\r\n\t ") {
		return false
	}
	if localPath {
		return true
	}
	for _, r := range value {
		if !(r == '-' || r == '_' || r == '.' || r == '/' ||
			(r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')) {
			return false
		}
	}
	return !strings.Contains(value, "/")
}

// ProxyArgv returns the only app-server proxy command shape. The `--` is an
// OpenSSH option terminator; normal ~/.ssh/config, known_hosts, agent, and
// ProxyJump behavior is left to OpenSSH.
func (c Config) ProxyArgv() ([]string, error) {
	c = c.normalized()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return []string{c.SSHBinary, "--", c.Host, "codex", "app-server", "proxy"}, nil
}

// ControlArgv returns the fixed one-shot Mektup control receiver command.
// Control documents are sent through stdin, never as a body-bearing argv
// argument. The receiver resolves custody IDs against its own registry.
func (c Config) ControlArgv() ([]string, error) {
	c = c.normalized()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return []string{c.SSHBinary, "--", c.Host, "mektup", "control", "receive"}, nil
}

// Process is the narrow child-process seam used by the real OpenSSH runner
// and deterministic tests. Implementations must make Wait observe Start and
// Kill must be safe to call after a failed Start.
type Process interface {
	StdinPipe() (io.WriteCloser, error)
	StdoutPipe() (io.ReadCloser, error)
	StderrPipe() (io.ReadCloser, error)
	Start() error
	Wait() error
	Kill() error
}

// ProcessFactory lets tests record exact argv and provide deterministic pipes.
// Production callers should use nil, which selects exec.Command directly.
type ProcessFactory interface {
	New(argv []string) (Process, error)
}

type ProcessFactoryFunc func([]string) (Process, error)

func (f ProcessFactoryFunc) New(argv []string) (Process, error) { return f(argv) }

type execFactory struct{}

func (execFactory) New(argv []string) (Process, error) {
	if len(argv) == 0 || argv[0] == "" {
		return nil, fmt.Errorf("%w: empty argv", ErrInvalidConfig)
	}
	return &execProcess{cmd: exec.Command(argv[0], argv[1:]...)}, nil
}

type execProcess struct{ cmd *exec.Cmd }

func (p *execProcess) StdinPipe() (io.WriteCloser, error) { return p.cmd.StdinPipe() }
func (p *execProcess) StdoutPipe() (io.ReadCloser, error) { return p.cmd.StdoutPipe() }
func (p *execProcess) StderrPipe() (io.ReadCloser, error) { return p.cmd.StderrPipe() }
func (p *execProcess) Start() error                       { return p.cmd.Start() }
func (p *execProcess) Wait() error                        { return p.cmd.Wait() }
func (p *execProcess) Kill() error                        { return p.cmd.Process.Kill() }

type child struct {
	process Process
	stdin   io.WriteCloser
	stdout  io.ReadCloser
	stderr  io.ReadCloser
	argv    []string
	config  Config

	waitDone    chan struct{}
	stderrDone  chan struct{}
	stdoutDone  chan struct{}
	stdoutOnce  sync.Once
	childErr    error
	stderrText  string
	stderrTrunc bool
	failure     *Failure
	mu          sync.Mutex
	closeOnce   sync.Once
	closeDone   chan struct{}
	closeErr    error
}

func startChild(argv []string, cfg Config, factory ProcessFactory) (*child, error) {
	if factory == nil {
		factory = execFactory{}
	}
	process, err := factory.New(append([]string(nil), argv...))
	if err != nil {
		return nil, &Failure{Kind: FailureSpawn, Cause: FailureSpawn, Evidence: WriteNotStarted, Err: err}
	}
	stdin, err := process.StdinPipe()
	if err != nil {
		return nil, &Failure{Kind: FailureSpawn, Cause: FailureSpawn, Evidence: WriteNotStarted, Err: err}
	}
	stdout, err := process.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		return nil, &Failure{Kind: FailureSpawn, Cause: FailureSpawn, Evidence: WriteNotStarted, Err: err}
	}
	stderr, err := process.StderrPipe()
	if err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		return nil, &Failure{Kind: FailureSpawn, Cause: FailureSpawn, Evidence: WriteNotStarted, Err: err}
	}
	if err := process.Start(); err != nil {
		_ = stdin.Close()
		_ = stdout.Close()
		_ = stderr.Close()
		return nil, &Failure{Kind: FailureSpawn, Cause: FailureSpawn, Evidence: WriteNotStarted, Err: err}
	}
	c := &child{process: process, stdin: stdin, stdout: stdout, stderr: stderr, argv: append([]string(nil), argv...), config: cfg.normalized(), waitDone: make(chan struct{}), stderrDone: make(chan struct{}), stdoutDone: make(chan struct{}), closeDone: make(chan struct{})}
	go c.collectStderr()
	go func() {
		<-c.stdoutDone
		<-c.stderrDone
		err := process.Wait()
		c.mu.Lock()
		c.childErr = err
		c.mu.Unlock()
		close(c.waitDone)
	}()
	return c, nil
}

func (c *child) markStdoutDone() { c.stdoutOnce.Do(func() { close(c.stdoutDone) }) }

func (c *child) collectStderr() {
	defer close(c.stderrDone)
	data, err := io.ReadAll(io.LimitReader(c.stderr, c.config.StderrLimit+1))
	_ = c.stderr.Close()
	if err != nil {
		return
	}
	truncated := int64(len(data)) > c.config.StderrLimit
	if truncated {
		data = data[:c.config.StderrLimit]
	}
	c.mu.Lock()
	c.stderrText, c.stderrTrunc = string(data), truncated
	c.mu.Unlock()
}

func (c *child) status() (error, string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.childErr, c.stderrText, c.stderrTrunc
}

func (c *child) close() error {
	c.closeOnce.Do(func() {
		var result error
		_ = c.stdin.Close()
		_ = c.stdout.Close()
		_ = c.stderr.Close()
		if err := c.process.Kill(); err != nil && !errors.Is(err, os.ErrProcessDone) {
			result = err
		}
		select {
		case <-c.waitDone:
		case <-time.After(c.config.CleanupTimeout):
			if result == nil {
				result = ErrCleanupTimeout
			}
		}
		c.mu.Lock()
		c.closeErr = result
		c.mu.Unlock()
		close(c.closeDone)
	})
	<-c.closeDone
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeErr
}

// Conn is a net.Conn-shaped raw byte stream over one SSH app-server proxy.
// It is suitable for use as a WebSocket dialer's custom net.Conn. No framing,
// JSONL, or message inspection occurs here.
type Conn struct {
	child         *child
	readCh        chan readChunk
	done          chan struct{}
	closeOnce     sync.Once
	readCallMu    sync.Mutex
	readMu        sync.Mutex
	readBuf       []byte
	terminalErr   error
	terminalSeen  bool
	deadlineMu    sync.Mutex
	readDeadline  time.Time
	writeDeadline time.Time
	readWake      chan struct{}
	writeWake     chan struct{}
	writeMu       sync.Mutex
	writePhase    atomic.Value // WriteEvidence
	host          string
	closeErrMu    sync.Mutex
	closeErr      error
	admissionMu   sync.Mutex
	closed        bool
}

type readChunk struct {
	data []byte
	err  error
}

// Dial starts one SSH proxy child. The remote app-server must already be
// running; this method never starts or falls back to an embedded daemon.
func Dial(ctx context.Context, cfg Config, factory ProcessFactory) (*Conn, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, &Failure{Kind: FailureCanceled, Cause: FailureCanceled, Evidence: WriteNotStarted, Err: err}
	}
	argv, err := cfg.ProxyArgv()
	if err != nil {
		return nil, err
	}
	child, err := startChild(argv, cfg, factory)
	if err != nil {
		return nil, err
	}
	c := newConn(child, cfg.Host)
	go c.pump()
	go func() {
		select {
		case <-ctx.Done():
			c.cancel(ctx.Err())
		case <-c.done:
		case <-child.waitDone:
		}
	}()
	return c, nil
}

func newConn(child *child, host string) *Conn {
	c := &Conn{child: child, readCh: make(chan readChunk, defaultReadQueue), done: make(chan struct{}), host: host, readWake: make(chan struct{}), writeWake: make(chan struct{})}
	c.writePhase.Store(WriteNotStarted)
	return c
}

func (c *Conn) pump() {
	defer c.child.markStdoutDone()
	buf := make([]byte, defaultReadChunk)
	for {
		n, err := c.child.stdout.Read(buf)
		if n > 0 {
			chunk := append([]byte(nil), buf[:n]...)
			select {
			case c.readCh <- readChunk{data: chunk}:
			case <-c.done:
				return
			}
		}
		if err != nil {
			select {
			case c.readCh <- readChunk{err: err}:
			case <-c.done:
			}
			return
		}
	}
}

func (c *Conn) cancel(err error) {
	phase := c.phase()
	if phase == WriteNotStarted {
		c.setFailure(&Failure{Kind: FailureCanceled, Cause: FailureCanceled, Evidence: phase, Err: err})
	} else {
		c.setFailure(&Failure{Kind: FailurePossibleWrite, Cause: FailureCanceled, Evidence: phase, Err: err})
	}
	_ = c.Close()
}

func (c *Conn) setFailure(f *Failure) {
	// A failure is first-evidence only. It is intentionally not overwritten by
	// a later EOF or child cleanup detail.
	c.child.mu.Lock()
	defer c.child.mu.Unlock()
	if c.child.failure == nil {
		c.child.failure = f
	}
}

func (c *Conn) phase() WriteEvidence {
	if v := c.writePhase.Load(); v != nil {
		return v.(WriteEvidence)
	}
	return WriteNotStarted
}

func (c *Conn) readState() (time.Time, <-chan struct{}) {
	c.deadlineMu.Lock()
	defer c.deadlineMu.Unlock()
	return c.readDeadline, c.readWake
}

func (c *Conn) writeState() (time.Time, <-chan struct{}) {
	c.deadlineMu.Lock()
	defer c.deadlineMu.Unlock()
	return c.writeDeadline, c.writeWake
}

func (c *Conn) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	c.readCallMu.Lock()
	defer c.readCallMu.Unlock()
	c.readMu.Lock()
	if len(c.readBuf) > 0 {
		n := copy(p, c.readBuf)
		c.readBuf = c.readBuf[n:]
		c.readMu.Unlock()
		return n, nil
	}
	if c.terminalSeen {
		err := c.terminalErr
		c.readMu.Unlock()
		return 0, err
	}
	c.readMu.Unlock()
	for {
		deadline, wake := c.readState()
		wait, cancel := deadlineTimer(deadline)
		select {
		case chunk := <-c.readCh:
			if cancel != nil {
				cancel()
			}
			if len(chunk.data) > 0 {
				c.readMu.Lock()
				n := copy(p, chunk.data)
				if n < len(chunk.data) {
					c.readBuf = append(c.readBuf, chunk.data[n:]...)
				}
				c.readMu.Unlock()
				return n, nil
			}
			if chunk.err != nil {
				failure := classifyReadError(chunk.err, c.phase())
				c.readMu.Lock()
				c.terminalErr = failure
				c.terminalSeen = true
				c.readMu.Unlock()
				return 0, failure
			}
		case <-c.done:
			if cancel != nil {
				cancel()
			}
			return 0, &Failure{Kind: FailureCanceled, Cause: FailureCanceled, Evidence: c.phase(), Err: context.Canceled}
		case <-wake:
			if cancel != nil {
				cancel()
			}
			continue
		case <-wait:
			return 0, &Failure{Kind: FailureEOF, Cause: FailureEOF, Evidence: c.phase(), Err: osErrDeadlineExceeded}
		}
	}
}

func classifyReadError(err error, phase WriteEvidence) error {
	if errors.Is(err, io.EOF) {
		return &Failure{Kind: FailureEOF, Cause: FailureEOF, Evidence: phase, Err: io.EOF}
	}
	return &Failure{Kind: FailureEOF, Cause: FailureEOF, Evidence: phase, Err: err}
}

func deadlineTimer(deadline time.Time) (<-chan time.Time, func()) {
	if deadline.IsZero() {
		return nil, nil
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return closedTimer(), func() {}
	}
	t := time.NewTimer(remaining)
	return t.C, func() {
		if !t.Stop() {
			select {
			case <-t.C:
			default:
			}
		}
	}
}

func closedTimer() <-chan time.Time { ch := make(chan time.Time); close(ch); return ch }

var osErrDeadlineExceeded = os.ErrDeadlineExceeded

func (c *Conn) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	deadline, wake := c.writeState()
	c.admissionMu.Lock()
	if c.closed {
		c.admissionMu.Unlock()
		return 0, &Failure{Kind: FailureCanceled, Cause: FailureCanceled, Evidence: WriteNotStarted, Err: io.ErrClosedPipe}
	}
	if !deadline.IsZero() && !time.Now().Before(deadline) {
		c.admissionMu.Unlock()
		return 0, &Failure{Kind: FailureCanceled, Cause: FailureCanceled, Evidence: WriteNotStarted, Err: os.ErrDeadlineExceeded}
	}
	c.admissionMu.Unlock()
	if c.phase() != WriteNotStarted && c.phase() != WriteComplete {
		return 0, &Failure{Kind: FailurePossibleWrite, Evidence: c.phase(), Err: io.ErrClosedPipe}
	}
	c.writePhase.Store(WriteMayHaveWritten)
	result := make(chan writeResult, 1)
	data := append([]byte(nil), p...)
	go func() {
		n, err := c.child.stdin.Write(data)
		result <- writeResult{n: n, err: err}
	}()
	for {
		deadline, wake = c.writeState()
		wait, cancel := deadlineTimer(deadline)
		select {
		case outcome := <-result:
			if cancel != nil {
				cancel()
			}
			if outcome.err != nil || outcome.n != len(data) {
				if outcome.err == nil {
					outcome.err = io.ErrShortWrite
				}
				return outcome.n, &Failure{Kind: FailurePossibleWrite, Cause: FailureProxy, Evidence: WriteMayHaveWritten, Err: outcome.err}
			}
			c.writePhase.Store(WriteComplete)
			return outcome.n, nil
		case <-c.done:
			if cancel != nil {
				cancel()
			}
			return 0, &Failure{Kind: FailurePossibleWrite, Cause: FailureCanceled, Evidence: WriteMayHaveWritten, Err: context.Canceled}
		case <-wake:
			if cancel != nil {
				cancel()
			}
			// Deadline updates are control events. Re-arm the timer without
			// growing the stack while the same pipe write remains pending.
			continue
		case <-wait:
			_ = c.abort()
			return 0, &Failure{Kind: FailurePossibleWrite, Cause: FailureCanceled, Evidence: WriteMayHaveWritten, Err: osErrDeadlineExceeded}
		}
	}
}

type writeResult struct {
	n   int
	err error
}

func (c *Conn) abort() error {
	c.closeOnce.Do(func() {
		c.admissionMu.Lock()
		c.closed = true
		c.admissionMu.Unlock()
		close(c.done)
		err := c.child.close()
		c.closeErrMu.Lock()
		c.closeErr = err
		c.closeErrMu.Unlock()
	})
	c.closeErrMu.Lock()
	defer c.closeErrMu.Unlock()
	return c.closeErr
}

func (c *Conn) Close() error { return c.abort() }

// Wait observes the SSH child, preserving authentication/proxy/EOF and
// possible-write distinctions. It is separate from Close: a caller may wait
// for a remote process exit without treating a clean stream EOF as a spawn
// error.
func (c *Conn) Wait() error {
	<-c.child.waitDone
	<-c.child.stderrDone
	err, stderr, trunc := c.child.status()
	c.child.mu.Lock()
	firstFailure := c.child.failure
	c.child.mu.Unlock()
	if firstFailure != nil {
		failure := *firstFailure
		failure.Stderr, failure.StderrTrunc = stderr, trunc
		return &failure
	}
	if err == nil {
		return &Failure{Kind: FailureEOF, Cause: FailureEOF, Evidence: c.phase(), Err: io.EOF, Stderr: stderr, StderrTrunc: trunc}
	}
	kind := classifyChildFailure(stderr)
	phase := c.phase()
	if phase != WriteNotStarted {
		return &Failure{Kind: FailurePossibleWrite, Cause: kind, Evidence: phase, Err: err, ExitCode: processExitCode(err), Stderr: stderr, StderrTrunc: trunc}
	}
	return &Failure{Kind: kind, Cause: kind, Evidence: phase, Err: err, ExitCode: processExitCode(err), Stderr: stderr, StderrTrunc: trunc}
}

func processExitCode(err error) int {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return 0
}

func classifyChildFailure(stderr string) FailureKind {
	text := strings.ToLower(stderr)
	for _, marker := range []string{
		"permission denied (", "authentication failed", "host key verification failed",
		"could not verify the host key", "sign_and_send_pubkey", "too many authentication failures",
		"access denied",
	} {
		if strings.Contains(text, marker) {
			return FailureAuthentication
		}
	}
	return FailureProxy
}

func (c *Conn) LocalAddr() net.Addr  { return processAddr{network: "ssh", address: "local"} }
func (c *Conn) RemoteAddr() net.Addr { return processAddr{network: "ssh", address: c.host} }

func (c *Conn) SetDeadline(deadline time.Time) error {
	c.deadlineMu.Lock()
	c.readDeadline = deadline
	c.writeDeadline = deadline
	close(c.readWake)
	close(c.writeWake)
	c.readWake = make(chan struct{})
	c.writeWake = make(chan struct{})
	c.deadlineMu.Unlock()
	return nil
}
func (c *Conn) SetReadDeadline(deadline time.Time) error {
	c.deadlineMu.Lock()
	c.readDeadline = deadline
	close(c.readWake)
	c.readWake = make(chan struct{})
	c.deadlineMu.Unlock()
	return nil
}
func (c *Conn) SetWriteDeadline(deadline time.Time) error {
	c.deadlineMu.Lock()
	c.writeDeadline = deadline
	close(c.writeWake)
	c.writeWake = make(chan struct{})
	c.deadlineMu.Unlock()
	return nil
}

type processAddr struct{ network, address string }

func (a processAddr) Network() string { return a.network }
func (a processAddr) String() string  { return a.address }

var _ net.Conn = (*Conn)(nil)
