package artifact

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

type failingReader struct{}

func (failingReader) Read([]byte) (int, error) { return 0, errors.New("source failed") }

func TestSpillReceiptAndPrivatePermissions(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "managed"))
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := store.Spill(context.Background(), "nested/output.bin", strings.NewReader("hello"), Options{MediaType: "text/plain", RetentionEligible: true, SensitiveOutputPossible: true})
	if err != nil {
		t.Fatal(err)
	}
	if !receipt.Complete || receipt.Bytes != 5 || receipt.SHA256 != "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824" {
		t.Fatalf("unexpected receipt: %+v", receipt)
	}
	if receipt.MediaType != "text/plain" || !receipt.RetentionEligible || !receipt.SensitiveOutputPossible {
		t.Fatalf("metadata was not retained: %+v", receipt)
	}
	encoded, err := json.Marshal(receipt)
	if err != nil || strings.Contains(string(encoded), "retention_eligible") || !strings.Contains(string(encoded), "retentionEligible") {
		t.Fatalf("receipt JSON = %s, err = %v", encoded, err)
	}
	info, err := os.Stat(receipt.Path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("artifact mode = %o, want 600", info.Mode().Perm())
	}
	rootInfo, err := os.Stat(store.Root())
	if err != nil {
		t.Fatal(err)
	}
	if rootInfo.Mode().Perm() != 0o700 {
		t.Fatalf("root mode = %o, want 700", rootInfo.Mode().Perm())
	}
}

func TestSpillReusesIdenticalContentAndRPCForceReplaces(t *testing.T) {
	store, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.Spill(context.Background(), "out", strings.NewReader("one"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.Spill(context.Background(), "out", strings.NewReader("one"), Options{})
	if err != nil || second.Path != first.Path || second.SHA256 != first.SHA256 || !second.Complete {
		t.Fatalf("identical spill receipt=%+v err=%v", second, err)
	}
	if _, err := store.Spill(context.Background(), "out", strings.NewReader("two"), Options{}); !errors.Is(err, ErrConflict) || !errors.Is(err, ErrExists) {
		t.Fatalf("mismatched spill error = %v, want conflict and exists", err)
	}
	if contents, err := os.ReadFile(filepath.Join(store.Root(), "out")); err != nil || string(contents) != "one" {
		t.Fatalf("mismatched spill changed existing content=%q err=%v", contents, err)
	}
	if _, err := store.WriteRPCOutput(context.Background(), "out", strings.NewReader("two"), RPCOutputOptions{Force: true}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(store.Root(), "out"))
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "two" {
		t.Fatalf("forced output = %q", b)
	}
}

func TestSpillFailureNeverPublishesCompleteArtifact(t *testing.T) {
	store, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Spill(context.Background(), "failed", failingReader{}, Options{}); err == nil {
		t.Fatal("expected source error")
	}
	if _, err := os.Stat(filepath.Join(store.Root(), "failed")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed destination stat = %v", err)
	}
	if _, err := store.Spill(context.Background(), "large", strings.NewReader("12345"), Options{MaxBytes: 4}); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("large write error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(store.Root(), "large")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("over-limit destination stat = %v", err)
	}
}

func TestManagedPathTraversalAndSymlinkRejected(t *testing.T) {
	root := t.TempDir()
	store, err := NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"../escape", "a/../../escape", "/tmp/escape", "a\\b"} {
		if _, err := store.Spill(context.Background(), name, strings.NewReader("x"), Options{}); !errors.Is(err, ErrPath) {
			t.Errorf("name %q error = %v, want ErrPath", name, err)
		}
	}
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Spill(context.Background(), "link/file", strings.NewReader("x"), Options{}); !errors.Is(err, ErrSymlink) {
		t.Fatalf("symlink parent error = %v", err)
	}
	if _, err := store.Spill(context.Background(), "target", strings.NewReader("x"), Options{}); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(root, "target")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "target")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.WriteRPCOutput(context.Background(), "target", strings.NewReader("x"), RPCOutputOptions{Force: true}); !errors.Is(err, ErrSymlink) {
		t.Fatalf("symlink destination error = %v", err)
	}
}

func TestConcurrentNoForcePublicationHasOneWinner(t *testing.T) {
	store, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const writers = 12
	var wg sync.WaitGroup
	results := make(chan error, writers)
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := store.Spill(context.Background(), "same", strings.NewReader(strings.Repeat(string(rune('a'+i)), 100)), Options{})
			results <- err
		}(i)
	}
	wg.Wait()
	close(results)
	winners := 0
	for err := range results {
		if err == nil {
			winners++
		} else if !errors.Is(err, ErrExists) && !errors.Is(err, ErrConflict) {
			t.Fatalf("concurrent writer error = %v", err)
		}
	}
	if winners != 1 {
		t.Fatalf("winners = %d, want 1", winners)
	}
	b, err := os.ReadFile(filepath.Join(store.Root(), "same"))
	if err != nil || len(b) != 100 {
		t.Fatalf("published bytes = %d, err = %v", len(b), err)
	}
}

func TestConcurrentSameContentSpillReusesOneAtomicWinner(t *testing.T) {
	store, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	const writers = 12
	const content = "same content across concurrent writers"
	results := make(chan struct {
		receipt Receipt
		err     error
	}, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			receipt, err := store.Spill(context.Background(), "same", strings.NewReader(content), Options{})
			results <- struct {
				receipt Receipt
				err     error
			}{receipt, err}
		}()
	}
	wg.Wait()
	close(results)
	var path, digest string
	for result := range results {
		if result.err != nil {
			t.Fatalf("same-content concurrent spill error = %v", result.err)
		}
		if path == "" {
			path, digest = result.receipt.Path, result.receipt.SHA256
		}
		if result.receipt.Path != path || result.receipt.SHA256 != digest || result.receipt.Bytes != int64(len(content)) {
			t.Fatalf("inconsistent reused receipt = %+v", result.receipt)
		}
	}
}

func TestSpillExistingSymlinkNonRegularAndWrongModeFailClosed(t *testing.T) {
	root := t.TempDir()
	store, err := NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "wrong-mode"), []byte("same"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Spill(context.Background(), "wrong-mode", strings.NewReader("same"), Options{}); !errors.Is(err, ErrWrongMode) {
		t.Fatalf("wrong-mode spill error = %v", err)
	}
	if err := os.Mkdir(filepath.Join(root, "directory"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Spill(context.Background(), "directory", strings.NewReader("same"), Options{}); !errors.Is(err, ErrNonRegular) {
		t.Fatalf("directory spill error = %v", err)
	}
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "symlink")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Spill(context.Background(), "symlink", strings.NewReader("same"), Options{}); !errors.Is(err, ErrSymlink) {
		t.Fatalf("symlink spill error = %v", err)
	}
}

func TestExplicitRPCOutputPathIsOutsideManagedRoot(t *testing.T) {
	store, err := NewStore(filepath.Join(t.TempDir(), "managed"))
	if err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(t.TempDir(), "caller", "rpc.out")
	receipt, err := store.WriteRPCOutputPath(context.Background(), outside, strings.NewReader("rpc"), RPCOutputOptions{MediaType: "text/plain"})
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Path != outside || receipt.SHA256 != "sha256:8e1941340511ea290acb09dde1ef0bdb2d48b10d77b774e9e6bab741022a8e4b" {
		t.Fatalf("explicit receipt = %+v", receipt)
	}
	if filepath.Dir(receipt.Path) == store.Root() {
		t.Fatal("explicit output unexpectedly used managed root")
	}
	if _, err := store.WriteRPCOutputPath(context.Background(), outside, strings.NewReader("again"), RPCOutputOptions{}); !errors.Is(err, ErrExists) {
		t.Fatalf("explicit overwrite error = %v", err)
	}
	if _, err := store.WriteRPCOutputPath(context.Background(), outside, strings.NewReader("replaced"), RPCOutputOptions{Force: true}); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(outside)
	if err != nil || string(b) != "replaced" {
		t.Fatalf("explicit output = %q, err = %v", b, err)
	}
	info, err := os.Stat(outside)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("explicit output mode = %o", info.Mode().Perm())
	}
}

type gatedReader struct {
	ready chan<- struct{}
	goOn  <-chan struct{}
	done  bool
}

func (r *gatedReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	r.done = true
	close(r.ready)
	<-r.goOn
	p[0] = 'x'
	return 1, io.EOF
}

func TestManagedRootSurvivesDirectorySwap(t *testing.T) {
	root := t.TempDir()
	store, err := NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	managedDir := filepath.Join(store.Root(), "nested")
	if err := os.Mkdir(managedDir, 0o700); err != nil {
		t.Fatal(err)
	}
	outside := t.TempDir()
	ready := make(chan struct{})
	goOn := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		_, err := store.Spill(context.Background(), "nested/out", &gatedReader{ready: ready, goOn: goOn}, Options{})
		result <- err
	}()
	<-ready
	moved := filepath.Join(root, "nested.moved")
	if err := os.Rename(managedDir, moved); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, managedDir); err != nil {
		t.Fatal(err)
	}
	close(goOn)
	if err := <-result; err == nil {
		t.Fatal("directory swap unexpectedly published")
	}
	if _, err := os.Stat(filepath.Join(outside, "out")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("outside path was written: %v", err)
	}
}
