//go:build darwin || linux

package journal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestSafeOpenRejectsStateDirectorySymlink(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target")
	if err := os.Mkdir(target, 0700); err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(root, "state")
	if err := os.Symlink(target, state); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(context.Background(), Options{StateDir: state}); err == nil {
		t.Fatal("state-directory symlink was accepted")
	}
	if _, err := os.Stat(filepath.Join(target, "journal.sqlite3")); !os.IsNotExist(err) {
		t.Fatalf("symlink target was mutated: %v", err)
	}
}

func TestStatePathCanonicalizesSymlinkedAncestor(t *testing.T) {
	root := t.TempDir()
	realParent := filepath.Join(root, "real")
	if err := os.Mkdir(realParent, 0700); err != nil {
		t.Fatal(err)
	}
	aliasParent := filepath.Join(root, "alias")
	if err := os.Symlink(realParent, aliasParent); err != nil {
		t.Fatal(err)
	}
	requested := filepath.Join(aliasParent, "state")
	j, err := Open(context.Background(), Options{StateDir: requested})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	canonicalParent, err := filepath.EvalSymlinks(realParent)
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(canonicalParent, "state")
	if j.StateDir() != want {
		t.Fatalf("state dir = %q, want canonical ancestor %q", j.StateDir(), want)
	}
	if _, err := os.Stat(filepath.Join(want, "journal.sqlite3")); err != nil {
		t.Fatal(err)
	}
}

func TestSafeOpenRejectsDatabaseSymlink(t *testing.T) {
	root := t.TempDir()
	targetDir := filepath.Join(root, "target")
	if err := os.Mkdir(targetDir, 0700); err != nil {
		t.Fatal(err)
	}
	target, err := Open(context.Background(), Options{StateDir: targetDir})
	if err != nil {
		t.Fatal(err)
	}
	if err := target.Close(); err != nil {
		t.Fatal(err)
	}
	state := filepath.Join(root, "state")
	if err := os.Mkdir(state, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(targetDir, "journal.sqlite3"), filepath.Join(state, "journal.sqlite3")); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(context.Background(), Options{StateDir: state}); err == nil {
		t.Fatal("database symlink was accepted")
	}
}

func TestSafeOpenRejectsDatabaseFIFOWithoutBlocking(t *testing.T) {
	state := t.TempDir()
	dbPath := filepath.Join(state, "journal.sqlite3")
	if err := unix.Mkfifo(dbPath, 0600); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := Open(context.Background(), Options{StateDir: state, BusyTimeout: 20 * time.Millisecond})
		done <- err
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("database FIFO was accepted")
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("database FIFO blocked safe open")
	}
}

func TestSafeOpenNormalizesDirectoryAndDatabaseModes(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, "state")
	if err := os.Mkdir(state, 0755); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(state, "journal.sqlite3")
	if err := os.WriteFile(dbPath, nil, 0644); err != nil {
		t.Fatal(err)
	}
	j, err := Open(context.Background(), Options{StateDir: state})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	stateInfo, err := os.Stat(state)
	if err != nil || stateInfo.Mode().Perm() != 0700 {
		t.Fatalf("state mode=%04o err=%v", stateInfo.Mode().Perm(), err)
	}
	dbInfo, err := os.Stat(dbPath)
	if err != nil || dbInfo.Mode().Perm() != 0600 {
		t.Fatalf("database mode=%04o err=%v", dbInfo.Mode().Perm(), err)
	}
}

func TestOpenExistingDoesNotCreateOrInitialize(t *testing.T) {
	root := t.TempDir()
	missing := filepath.Join(root, "missing")
	if _, err := OpenExisting(context.Background(), Options{StateDir: missing}); err == nil {
		t.Fatal("OpenExisting created a missing state directory")
	}
	state := filepath.Join(root, "uninitialized")
	if err := os.Mkdir(state, 0700); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(state, "journal.sqlite3")
	if err := os.WriteFile(dbPath, nil, 0600); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	_, err = OpenExisting(context.Background(), Options{StateDir: state})
	if err == nil || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("uninitialized OpenExisting error=%v", err)
	}
	after, err := os.ReadFile(dbPath)
	if err != nil || string(before) != string(after) {
		t.Fatalf("OpenExisting changed uninitialized database: %v", err)
	}
	if _, err := os.Stat(filepath.Join(state, "journal.sqlite3-wal")); !os.IsNotExist(err) {
		t.Fatalf("OpenExisting created WAL: %v", err)
	}
}
