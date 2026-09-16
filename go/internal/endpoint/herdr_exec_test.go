package endpoint

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/agensfield/mektup/go/internal/sshproxy"
)

func TestExecRunnerUsesOnlyFixedHerdrArgv(t *testing.T) {
	var calls [][]string
	runner := ExecRunner{
		Factory: sshproxy.ProcessFactoryFunc(func(argv []string) (sshproxy.Process, error) {
			calls = append(calls, append([]string(nil), argv...))
			return bufferedHerdrProcess([]byte(`{"ok":true}`), nil), nil
		}),
	}
	for _, argv := range [][]string{
		{"herdr", "agent", "list"},
		{"herdr", "agent", "get", "w3:p1"},
	} {
		if _, err := runner.Run(context.Background(), argv); err != nil {
			t.Fatalf("argv %#v: %v", argv, err)
		}
	}
	want := [][]string{
		{"herdr", "agent", "list"},
		{"herdr", "agent", "get", "w3:p1"},
	}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("argv = %#v, want %#v", calls, want)
	}
	if _, err := runner.Run(context.Background(), []string{"sh", "-c", "touch /tmp/pwned"}); !errors.Is(err, ErrHerdrRunnerInvalidCommand) {
		t.Fatalf("arbitrary argv error = %v", err)
	}
}

func TestExecRunnerSanitizesAmbientHerdrIdentity(t *testing.T) {
	dir := t.TempDir()
	herdr := filepath.Join(dir, "herdr")
	if err := os.WriteFile(herdr, []byte("#!/bin/sh\nenv\n"), 0700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	for _, name := range []string{"HERDR_ENV", "HERDR_WORKSPACE_ID", "HERDR_TAB_ID", "HERDR_PANE_ID", "HERDR_SOCKET_PATH"} {
		t.Setenv(name, "poisoned")
	}
	t.Setenv("MEKTUP_ENV_CONTROL", "preserved")

	output, err := (ExecRunner{}).Run(context.Background(), []string{"herdr", "agent", "list"})
	if err != nil {
		t.Fatal(err)
	}
	text := string(output)
	if !strings.Contains(text, "MEKTUP_ENV_CONTROL=preserved") {
		t.Fatalf("ordinary environment was not preserved: %q", text)
	}
	for _, name := range []string{"HERDR_ENV", "HERDR_WORKSPACE_ID", "HERDR_TAB_ID", "HERDR_PANE_ID", "HERDR_SOCKET_PATH"} {
		if strings.Contains(text, name+"=") {
			t.Fatalf("ambient %s reached Herdr: %q", name, text)
		}
	}
}

func TestExecRunnerBoundsOversizedOutputAndReaps(t *testing.T) {
	p := bufferedHerdrProcess(bytes.Repeat([]byte("x"), 17), nil)
	runner := ExecRunner{
		OutputLimit: 16,
		Factory:     sshproxy.ProcessFactoryFunc(func([]string) (sshproxy.Process, error) { return p, nil }),
	}
	_, err := runner.Run(context.Background(), []string{"herdr", "agent", "list"})
	var failure *HerdrRunnerFailure
	if !errors.Is(err, ErrResolverUnavailable) || !errors.As(err, &failure) || failure.Kind != HerdrRunnerOutputFailure || !failure.OutputTruncated {
		t.Fatalf("overflow = %#v", err)
	}
	p.mu.Lock()
	kills, waits := p.kills, p.waits
	p.mu.Unlock()
	if kills != 1 || waits != 1 {
		t.Fatalf("lifecycle kills=%d waits=%d", kills, waits)
	}
}

func TestExecRunnerBoundsStderr(t *testing.T) {
	p := bufferedHerdrProcess([]byte("ok"), bytes.Repeat([]byte("e"), 17))
	runner := ExecRunner{
		StderrLimit: 16,
		Factory:     sshproxy.ProcessFactoryFunc(func([]string) (sshproxy.Process, error) { return p, nil }),
	}
	_, err := runner.Run(context.Background(), []string{"herdr", "agent", "list"})
	var failure *HerdrRunnerFailure
	if !errors.Is(err, ErrResolverUnavailable) || !errors.As(err, &failure) || failure.Kind != HerdrRunnerOutputFailure || !failure.StderrTruncated || failure.Stderr != "eeeeeeeeeeeeeeee" {
		t.Fatalf("stderr overflow = %#v", err)
	}
}

func TestExecRunnerReadErrorCleansUpStalledOtherPipe(t *testing.T) {
	cause := errors.New("local read fault")
	stderrR, stderrW := io.Pipe()
	killed := make(chan struct{})
	p := &herdrTestProcess{
		stdin:  testWriteCloser{Writer: io.Discard},
		stdout: failingReadCloser{err: cause},
		stderr: stderrR,
		kill: func() error {
			select {
			case <-killed:
			default:
				close(killed)
			}
			return stderrW.Close()
		},
		wait: func() error {
			<-killed
			return nil
		},
	}
	runner := ExecRunner{
		CommandTimeout: 30 * time.Millisecond,
		Factory:        sshproxy.ProcessFactoryFunc(func([]string) (sshproxy.Process, error) { return p, nil }),
	}
	started := time.Now()
	_, err := runner.Run(context.Background(), []string{"herdr", "agent", "list"})
	if time.Since(started) > 500*time.Millisecond {
		t.Fatalf("read error waited for command timeout: %v", time.Since(started))
	}
	var failure *HerdrRunnerFailure
	if !errors.Is(err, cause) || !errors.Is(err, ErrResolverUnavailable) || !errors.As(err, &failure) || failure.Kind != HerdrRunnerCommandFailure {
		t.Fatalf("read error = %#v", err)
	}
}

func TestExecRunnerCancellationKillsAndReaps(t *testing.T) {
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
	runner := ExecRunner{
		CommandTimeout: time.Second,
		Factory:        sshproxy.ProcessFactoryFunc(func([]string) (sshproxy.Process, error) { return p, nil }),
	}
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)
	defer cancel()
	_, err := runner.Run(ctx, []string{"herdr", "agent", "list"})
	var failure *HerdrRunnerFailure
	if !errors.Is(err, context.Canceled) || !errors.Is(err, ErrResolverUnavailable) || !errors.As(err, &failure) || failure.Kind != HerdrRunnerCanceled {
		t.Fatalf("cancel = %#v", err)
	}
	p.mu.Lock()
	kills, waits := p.kills, p.waits
	p.mu.Unlock()
	if kills != 1 || waits != 1 {
		t.Fatalf("lifecycle kills=%d waits=%d", kills, waits)
	}
}
