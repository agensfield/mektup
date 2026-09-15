package artifact

import (
	"context"
	"errors"
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
	if !receipt.Complete || receipt.Bytes != 5 || receipt.SHA256 != "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824" {
		t.Fatalf("unexpected receipt: %+v", receipt)
	}
	if receipt.MediaType != "text/plain" || !receipt.RetentionEligible || !receipt.SensitiveOutputPossible {
		t.Fatalf("metadata was not retained: %+v", receipt)
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

func TestSpillRefusesOverwriteAndRPCForceReplaces(t *testing.T) {
	store, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Spill(context.Background(), "out", strings.NewReader("one"), Options{}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Spill(context.Background(), "out", strings.NewReader("two"), Options{}); !errors.Is(err, ErrExists) {
		t.Fatalf("second spill error = %v, want ErrExists", err)
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
		} else if !errors.Is(err, ErrExists) {
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
