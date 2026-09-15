package logging

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestMetadataLogRedactsSecretsAndRejectsBodies(t *testing.T) {
	l, err := New(Config{Dir: t.TempDir(), MaxBytes: 1024, MaxFiles: 2})
	if err != nil {
		t.Fatal(err)
	}
	if err := l.Log("request", map[string]string{"authorization": "Bearer top-secret", "status": "ok"}); err != nil {
		t.Fatal(err)
	}
	if err := l.Log("request", map[string]string{"raw_payload": "do not log"}); !errors.Is(err, ErrBodyField) {
		t.Fatalf("body field error = %v", err)
	}
	b, err := os.ReadFile(filepath.Join(l.config.Dir, "mektup.log"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "top-secret") || !strings.Contains(string(b), "[REDACTED]") {
		t.Fatalf("log did not redact secret: %s", b)
	}
	var entry Entry
	if err := json.Unmarshal(bytesBeforeNewline(b), &entry); err != nil {
		t.Fatal(err)
	}
	if entry.Fields["status"] != "ok" {
		t.Fatalf("metadata entry = %+v", entry)
	}
}

func TestDefaultsAreBoundedAndLogSymlinkCannotEscape(t *testing.T) {
	config := DefaultConfig(t.TempDir())
	if config.MaxBytes != DefaultMaxBytes || config.MaxFiles != DefaultMaxFiles || config.Retention != DefaultRetention || config.AuditMaxBytes != DefaultAuditMaxBytes {
		t.Fatalf("defaults = %+v", config)
	}
	l, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	if err := os.Symlink(filepath.Join(outside, "outside.log"), filepath.Join(l.config.Dir, "mektup.log")); err != nil {
		t.Fatal(err)
	}
	if err := l.Log("event", map[string]string{"status": "ok"}); err == nil {
		t.Fatal("expected symlink destination rejection")
	}
	if _, err := os.Stat(filepath.Join(outside, "outside.log")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("outside log stat = %v", err)
	}
}

func TestRotationStaysWithinConfiguredBounds(t *testing.T) {
	l, err := New(Config{Dir: t.TempDir(), MaxBytes: 180, MaxFiles: 3, Retention: 24 * 60 * 60 * 1e9})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 30; i++ {
		if err := l.Log("event", map[string]string{"i": strings.Repeat("x", 20)}); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(l.config.Dir)
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, entry := range entries {
		if entry.IsDir() || (entry.Name() != "mektup.log" && !strings.HasPrefix(entry.Name(), "mektup.log.")) {
			continue
		}
		count++
		info, err := entry.Info()
		if err != nil {
			t.Fatal(err)
		}
		if info.Size() > l.config.MaxBytes {
			t.Fatalf("%s size %d exceeds bound", entry.Name(), info.Size())
		}
	}
	if count > l.config.MaxFiles {
		t.Fatalf("log file count = %d, want <= %d", count, l.config.MaxFiles)
	}
}

func TestAuditIsPrivateBoundedPerInvocationAndVisible(t *testing.T) {
	l, err := New(Config{Dir: t.TempDir(), AuditMaxBytes: 8})
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := l.Audit(context.Background(), AuditOptions{InvocationID: "invoke-1", Body: strings.NewReader("secret!")})
	if err != nil {
		t.Fatal(err)
	}
	if !receipt.Complete || receipt.Bytes != 7 || !receipt.Mode.Sensitive || receipt.Mode.NetworkTelemetry || receipt.Mode.Mode != "audit" {
		t.Fatalf("audit receipt = %+v", receipt)
	}
	encoded, err := json.Marshal(receipt)
	if err != nil || strings.Contains(string(encoded), "network_telemetry") || !strings.Contains(string(encoded), "networkTelemetry") {
		t.Fatalf("audit JSON = %s, err = %v", encoded, err)
	}
	info, err := os.Stat(receipt.Path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("audit mode = %o", info.Mode().Perm())
	}
	if _, err := l.Audit(context.Background(), AuditOptions{InvocationID: "invoke-1", Body: strings.NewReader("again")}); !errors.Is(err, os.ErrExist) {
		t.Fatalf("audit overwrite error = %v", err)
	}
	if _, err := l.Audit(context.Background(), AuditOptions{InvocationID: "too-large", Body: strings.NewReader("123456789")}); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("audit over-limit error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(l.config.Dir, "audit", "too-large.audit")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("over-limit audit stat = %v", err)
	}
	if _, err := l.Audit(context.Background(), AuditOptions{InvocationID: "../escape", Body: strings.NewReader("x")}); !errors.Is(err, ErrAuditID) {
		t.Fatalf("traversal audit error = %v", err)
	}
}

func TestConcurrentMetadataWritesRemainBounded(t *testing.T) {
	l, err := New(Config{Dir: t.TempDir(), MaxBytes: 1024 * 1024, MaxFiles: 2})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := l.Log("concurrent", map[string]string{"status": "ok"}); err != nil {
				t.Errorf("concurrent log: %v", err)
			}
		}()
	}
	wg.Wait()
	b, err := os.ReadFile(filepath.Join(l.config.Dir, "mektup.log"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes := strings.Count(string(b), "\n"); bytes != 20 {
		t.Fatalf("newline count = %d, want 20", bytes)
	}
}

func bytesBeforeNewline(b []byte) []byte {
	if i := strings.IndexByte(string(b), '\n'); i >= 0 {
		return b[:i]
	}
	return b
}
