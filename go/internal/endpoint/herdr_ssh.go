package endpoint

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"

	"github.com/agensfield/mektup/go/internal/sshproxy"
)

var (
	ErrHerdrRunnerInvalidCommand = errors.New("invalid remote Herdr command")
	ErrHerdrRunnerHostMismatch   = errors.New("remote Herdr SSH host disagrees with endpoint route")
	ErrHerdrRunnerOutputTooLarge = errors.New("remote Herdr output exceeds limit")
	ErrHerdrRunnerTimeout        = errors.New("remote Herdr command timed out")
)

type HerdrRunnerFailureKind string

const (
	HerdrRunnerSpawnFailure   HerdrRunnerFailureKind = "spawn"
	HerdrRunnerCommandFailure HerdrRunnerFailureKind = "command"
	HerdrRunnerTimeoutFailure HerdrRunnerFailureKind = "timeout"
	HerdrRunnerOutputFailure  HerdrRunnerFailureKind = "output_too_large"
	HerdrRunnerCleanupFailure HerdrRunnerFailureKind = "cleanup"
	HerdrRunnerCanceled       HerdrRunnerFailureKind = "canceled"
)

// HerdrRunnerFailure preserves bounded process evidence without putting SSH
// argv, credentials, or arbitrary remote commands into Error().
type HerdrRunnerFailure struct {
	Kind            HerdrRunnerFailureKind
	Err             error
	ExitCode        int
	Stderr          string
	StderrTruncated bool
	OutputTruncated bool
}

func (f *HerdrRunnerFailure) Error() string {
	if f == nil {
		return "remote Herdr command failed"
	}
	if f.Err != nil {
		return fmt.Sprintf("remote Herdr command %s: %v", f.Kind, f.Err)
	}
	return fmt.Sprintf("remote Herdr command %s", f.Kind)
}

func (f *HerdrRunnerFailure) Unwrap() error {
	if f == nil {
		return nil
	}
	return errors.Join(ErrResolverUnavailable, f.Err)
}

type HerdrRunnerConfig struct {
	SSH            sshproxy.Config
	StartupTimeout time.Duration
	CommandTimeout time.Duration
	CleanupTimeout time.Duration
	OutputLimit    int64
	StderrLimit    int64
}

func (c HerdrRunnerConfig) normalized() HerdrRunnerConfig {
	if c.StartupTimeout <= 0 {
		c.StartupTimeout = 5 * time.Second
	}
	if c.CommandTimeout <= 0 {
		c.CommandTimeout = 15 * time.Second
	}
	if c.CleanupTimeout <= 0 {
		c.CleanupTimeout = 2 * time.Second
	}
	if c.OutputLimit <= 0 {
		c.OutputLimit = 1 << 20
	}
	if c.StderrLimit <= 0 {
		c.StderrLimit = 64 << 10
	}
	return c
}

// RemoteHerdrRunner is the endpoint-specific, read-only Herdr adapter. It
// accepts only the two argv forms emitted by HerdrResolver and runs them via
// system OpenSSH with a fixed remote argv, never a shell.
type RemoteHerdrRunner struct {
	Config  HerdrRunnerConfig
	Factory sshproxy.ProcessFactory
}

func (r RemoteHerdrRunner) RunEndpoint(ctx context.Context, ep Endpoint, argv []string) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, &HerdrRunnerFailure{Kind: HerdrRunnerCanceled, Err: err}
	}
	if ep.Route.Kind != RouteSSH {
		return nil, fmt.Errorf("%w: endpoint route is not SSH", ErrHerdrRunnerInvalidCommand)
	}
	remote, err := validateHerdrCommand(argv)
	if err != nil {
		return nil, err
	}
	cfg := r.Config.normalized()
	if cfg.SSH.Host != "" && cfg.SSH.Host != ep.Route.SSHHost {
		return nil, fmt.Errorf("%w: configured=%q route=%q", ErrHerdrRunnerHostMismatch, cfg.SSH.Host, ep.Route.SSHHost)
	}
	if cfg.SSH.SSHBinary == "" {
		cfg.SSH.SSHBinary = sshproxy.DefaultSSHBinary
	}
	cfg.SSH.Host = ep.Route.SSHHost
	if err := cfg.SSH.Validate(); err != nil {
		return nil, err
	}
	argv = append([]string{cfg.SSH.SSHBinary, "--", ep.Route.SSHHost}, remote...)
	factory := r.Factory
	if factory == nil {
		factory = sshproxy.DefaultProcessFactory()
	}
	process, err := factory.New(append([]string(nil), argv...))
	if err != nil {
		return nil, &HerdrRunnerFailure{Kind: HerdrRunnerSpawnFailure, Err: err}
	}
	if process == nil {
		return nil, &HerdrRunnerFailure{Kind: HerdrRunnerSpawnFailure, Err: errors.New("process factory returned nil process")}
	}
	stdin, err := process.StdinPipe()
	if err != nil {
		return cleanupHerdrProcess(process, nil, nil, nil, cfg.CleanupTimeout, &HerdrRunnerFailure{Kind: HerdrRunnerSpawnFailure, Err: err})
	}
	stdout, err := process.StdoutPipe()
	if err != nil {
		return cleanupHerdrProcess(process, stdin, nil, nil, cfg.CleanupTimeout, &HerdrRunnerFailure{Kind: HerdrRunnerSpawnFailure, Err: err})
	}
	stderr, err := process.StderrPipe()
	if err != nil {
		return cleanupHerdrProcess(process, stdin, stdout, nil, cfg.CleanupTimeout, &HerdrRunnerFailure{Kind: HerdrRunnerSpawnFailure, Err: err})
	}
	return runHerdrProcess(ctx, process, stdin, stdout, stderr, cfg)
}

func validateHerdrCommand(argv []string) ([]string, error) {
	if len(argv) == 3 && argv[0] == "herdr" && argv[1] == "agent" && argv[2] == "list" {
		return append([]string(nil), argv...), nil
	}
	if len(argv) == 4 && argv[0] == "herdr" && argv[1] == "agent" && argv[2] == "get" && validRemotePane(argv[3]) {
		return append([]string(nil), argv...), nil
	}
	return nil, fmt.Errorf("%w: only herdr agent list and herdr agent get <workspace:pane> are allowed", ErrHerdrRunnerInvalidCommand)
}

func validRemotePane(value string) bool {
	if value == "" || len(value) > 1024 || strings.Count(value, ":") != 1 {
		return false
	}
	parts := strings.SplitN(value, ":", 2)
	return validHerdrToken(parts[0]) && validHerdrToken(parts[1]) && strings.HasPrefix(parts[1], "p")
}

func validHerdrToken(value string) bool {
	if value == "" || strings.HasPrefix(value, "-") {
		return false
	}
	for _, char := range value {
		if (char >= 'a' && char <= 'z') || (char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') || char == '_' || char == '-' || char == '.' {
			continue
		}
		return false
	}
	return true
}

type herdrReadResult struct {
	data []byte
	err  error
	too  bool
}

func runHerdrProcess(parent context.Context, process sshproxy.Process, stdin io.WriteCloser, stdout, stderr io.ReadCloser, cfg HerdrRunnerConfig) ([]byte, error) {
	startupCtx, startupCancel := context.WithTimeout(parent, cfg.StartupTimeout)
	defer startupCancel()
	startDone := make(chan error, 1)
	go func() { startDone <- process.Start() }()
	select {
	case err := <-startDone:
		if err != nil {
			return cleanupHerdrProcess(process, stdin, stdout, stderr, cfg.CleanupTimeout, &HerdrRunnerFailure{Kind: HerdrRunnerSpawnFailure, Err: err})
		}
	case <-startupCtx.Done():
		failureKind := HerdrRunnerTimeoutFailure
		failureErr := errors.Join(ErrHerdrRunnerTimeout, startupCtx.Err())
		if errors.Is(startupCtx.Err(), context.Canceled) {
			failureKind = HerdrRunnerCanceled
			failureErr = context.Canceled
		}
		return cleanupStartingHerdrProcess(startDone, process, stdin, stdout, stderr, cfg.CleanupTimeout, &HerdrRunnerFailure{Kind: failureKind, Err: failureErr})
	}
	// The command is read-only and has no stdin payload. Closing stdin also
	// avoids waiting for a remote command that incorrectly expects input.
	_ = stdin.Close()
	stdoutCh := make(chan herdrReadResult, 1)
	stderrCh := make(chan herdrReadResult, 1)
	waitCh := make(chan error, 1)
	stdoutDone := make(chan struct{})
	stderrDone := make(chan struct{})
	go func() {
		defer close(stdoutDone)
		stdoutCh <- readHerdrBounded(stdout, cfg.OutputLimit)
	}()
	go func() {
		defer close(stderrDone)
		stderrCh <- readHerdrBounded(stderr, cfg.StderrLimit)
	}()
	go func() {
		// Wait closes exec.Cmd pipes. Let both bounded readers finish first so
		// a normal process exit cannot turn a successful burst into a local
		// "file already closed" read failure.
		<-stdoutDone
		<-stderrDone
		waitCh <- process.Wait()
	}()
	commandCtx, commandCancel := context.WithTimeout(parent, cfg.CommandTimeout)
	defer commandCancel()
	var out, errout herdrReadResult
	var waitErr error
	gotOut, gotErrout, gotWait := false, false, false
	var terminal error
	commandDone := commandCtx.Done()
	var cleanupTimer *time.Timer
	var cleanupDone <-chan time.Time
	defer func() {
		if cleanupTimer != nil {
			cleanupTimer.Stop()
		}
	}()
	for !(gotOut && gotErrout && gotWait) {
		select {
		case out = <-stdoutCh:
			gotOut = true
			if terminal == nil && out.err != nil {
				terminal = &HerdrRunnerFailure{Kind: HerdrRunnerCommandFailure, Err: out.err}
				cleanupDone = beginHerdrCleanup(process, stdout, stderr, cfg.CleanupTimeout, &cleanupTimer)
			} else if out.too && terminal == nil {
				terminal = &HerdrRunnerFailure{Kind: HerdrRunnerOutputFailure, Err: ErrHerdrRunnerOutputTooLarge, OutputTruncated: true}
				cleanupDone = beginHerdrCleanup(process, stdout, stderr, cfg.CleanupTimeout, &cleanupTimer)
			}
		case errout = <-stderrCh:
			gotErrout = true
			if terminal == nil && errout.err != nil {
				terminal = &HerdrRunnerFailure{Kind: HerdrRunnerCommandFailure, Err: errout.err}
				cleanupDone = beginHerdrCleanup(process, stdout, stderr, cfg.CleanupTimeout, &cleanupTimer)
			} else if errout.too && terminal == nil {
				terminal = &HerdrRunnerFailure{Kind: HerdrRunnerOutputFailure, Err: ErrHerdrRunnerOutputTooLarge, StderrTruncated: true}
				cleanupDone = beginHerdrCleanup(process, stdout, stderr, cfg.CleanupTimeout, &cleanupTimer)
			}
		case waitErr = <-waitCh:
			gotWait = true
		case <-commandDone:
			if terminal == nil {
				kind := HerdrRunnerTimeoutFailure
				if errors.Is(commandCtx.Err(), context.Canceled) {
					kind = HerdrRunnerCanceled
				}
				failureErr := commandCtx.Err()
				if kind == HerdrRunnerTimeoutFailure {
					failureErr = errors.Join(ErrHerdrRunnerTimeout, failureErr)
				}
				terminal = &HerdrRunnerFailure{Kind: kind, Err: failureErr}
				cleanupDone = beginHerdrCleanup(process, stdout, stderr, cfg.CleanupTimeout, &cleanupTimer)
			}
			commandDone = nil // disable this select arm
		case <-cleanupDone:
			if failure, ok := terminal.(*HerdrRunnerFailure); ok {
				failure.Stderr = string(errout.data)
				failure.StderrTruncated = failure.StderrTruncated || errout.too
			}
			return nil, &HerdrRunnerFailure{Kind: HerdrRunnerCleanupFailure, Err: errors.Join(terminal, sshproxy.ErrCleanupTimeout), Stderr: string(errout.data), StderrTruncated: errout.too}
		}
	}
	if terminal != nil {
		if failure, ok := terminal.(*HerdrRunnerFailure); ok {
			failure.Stderr = string(errout.data)
			failure.StderrTruncated = failure.StderrTruncated || errout.too
		}
		return nil, terminal
	}
	if waitErr != nil {
		return nil, &HerdrRunnerFailure{Kind: HerdrRunnerCommandFailure, Err: waitErr, ExitCode: processExitCode(waitErr), Stderr: string(errout.data), StderrTruncated: errout.too}
	}
	if out.too || errout.too {
		return nil, &HerdrRunnerFailure{Kind: HerdrRunnerOutputFailure, Err: ErrHerdrRunnerOutputTooLarge, OutputTruncated: out.too, Stderr: string(errout.data), StderrTruncated: errout.too}
	}
	if out.err != nil || errout.err != nil {
		return nil, &HerdrRunnerFailure{Kind: HerdrRunnerCommandFailure, Err: errors.Join(out.err, errout.err), Stderr: string(errout.data), StderrTruncated: errout.too}
	}
	return out.data, nil
}

func beginHerdrCleanup(process sshproxy.Process, stdout, stderr io.ReadCloser, timeout time.Duration, timer **time.Timer) <-chan time.Time {
	_ = process.Kill()
	if stdout != nil {
		_ = stdout.Close()
	}
	if stderr != nil {
		_ = stderr.Close()
	}
	*timer = time.NewTimer(timeout)
	return (*timer).C
}

func readHerdrBounded(reader io.ReadCloser, limit int64) herdrReadResult {
	data, err := io.ReadAll(io.LimitReader(reader, limit+1))
	_ = reader.Close()
	too := int64(len(data)) > limit
	if too {
		data = data[:limit]
	}
	return herdrReadResult{data: data, err: err, too: too}
}

func cleanupHerdrProcess(process sshproxy.Process, stdin io.WriteCloser, stdout, stderr io.ReadCloser, timeout time.Duration, failure error) ([]byte, error) {
	if stdin != nil {
		_ = stdin.Close()
	}
	if stdout != nil {
		_ = stdout.Close()
	}
	if stderr != nil {
		_ = stderr.Close()
	}
	_ = process.Kill()
	waitDone := make(chan error, 1)
	go func() { waitDone <- process.Wait() }()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-waitDone:
		return nil, failure
	case <-timer.C:
		return nil, &HerdrRunnerFailure{Kind: HerdrRunnerCleanupFailure, Err: errors.Join(failure, sshproxy.ErrCleanupTimeout)}
	}
}

// cleanupStartingHerdrProcess handles the one lifecycle edge the narrow
// Process interface cannot cancel itself: Start may still be blocked when the
// startup bound expires. The reaper owns startDone and will kill and Wait if
// Start eventually succeeds, so a child cannot escape after RunEndpoint
// returns. If Start never returns, no child exists to reap.
func cleanupStartingHerdrProcess(startDone <-chan error, process sshproxy.Process, stdin io.WriteCloser, stdout, stderr io.ReadCloser, timeout time.Duration, failure error) ([]byte, error) {
	if stdin != nil {
		_ = stdin.Close()
	}
	if stdout != nil {
		_ = stdout.Close()
	}
	if stderr != nil {
		_ = stderr.Close()
	}
	reaped := make(chan struct{})
	go func() {
		if startErr := <-startDone; startErr == nil {
			_ = process.Kill()
		}
		_ = process.Wait()
		close(reaped)
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-reaped:
		return nil, failure
	case <-timer.C:
		return nil, &HerdrRunnerFailure{Kind: HerdrRunnerCleanupFailure, Err: errors.Join(failure, sshproxy.ErrCleanupTimeout)}
	}
}

func processExitCode(err error) int {
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		return exitErr.ExitCode()
	}
	return 0
}

var _ EndpointCommandRunner = RemoteHerdrRunner{}
