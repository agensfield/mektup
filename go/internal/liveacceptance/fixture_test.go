package liveacceptance

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func requiredExecutable(t *testing.T, environment, fallback string) string {
	t.Helper()
	name := os.Getenv(environment)
	if name == "" {
		name = fallback
	}
	resolved, err := exec.LookPath(name)
	if err != nil {
		t.Fatalf("%s: %v", environment, err)
	}
	return resolved
}

func replaceEnvironment(environment []string, name, value string) []string {
	prefix := name + "="
	out := make([]string, 0, len(environment)+1)
	for _, entry := range environment {
		if !strings.HasPrefix(entry, prefix) {
			out = append(out, entry)
		}
	}
	return append(out, prefix+value)
}

func stopProcess(command *exec.Cmd, cancel func()) {
	cancel()
	if command == nil || command.Process == nil {
		return
	}
	_ = command.Process.Kill()
	done := make(chan struct{})
	go func() {
		_ = command.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
	}
}

func fileSHA256(t *testing.T, name string) string {
	t.Helper()
	file, err := os.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func executableVersion(t *testing.T, name string) string {
	t.Helper()
	output, err := exec.Command(name, "--version").CombinedOutput()
	if err != nil {
		t.Fatalf("%s --version: %v: %s", name, err, output)
	}
	return strings.TrimSpace(string(output))
}
