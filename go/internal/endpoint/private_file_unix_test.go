//go:build darwin || linux

package endpoint

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestEndpointStoreLoadRejectsUnsafeFilesystemObjects(t *testing.T) {
	root := t.TempDir()
	validPath := filepath.Join(root, "valid.json")
	valid := []byte(`{"version":1,"endpoints":[]}`)
	if err := os.WriteFile(validPath, valid, 0o600); err != nil {
		t.Fatal(err)
	}

	t.Run("symlink", func(t *testing.T) {
		path := filepath.Join(root, "symlink.json")
		if err := os.Symlink(validPath, path); err != nil {
			t.Fatal(err)
		}
		if _, err := NewStore(path, root).Load(); !errors.Is(err, ErrConfigPerm) {
			t.Fatalf("symlink error = %v", err)
		}
	})

	t.Run("fifo", func(t *testing.T) {
		path := filepath.Join(root, "fifo.json")
		if err := unix.Mkfifo(path, 0o600); err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() {
			_, err := NewStore(path, root).Load()
			done <- err
		}()
		select {
		case err := <-done:
			if !errors.Is(err, ErrConfigPerm) {
				t.Fatalf("FIFO error = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("FIFO read blocked")
		}
	})

	t.Run("wrong mode", func(t *testing.T) {
		path := filepath.Join(root, "mode.json")
		if err := os.WriteFile(path, valid, 0o640); err != nil {
			t.Fatal(err)
		}
		if _, err := NewStore(path, root).Load(); !errors.Is(err, ErrConfigPerm) {
			t.Fatalf("mode error = %v", err)
		}
	})

	t.Run("oversize", func(t *testing.T) {
		path := filepath.Join(root, "oversize.json")
		if err := os.WriteFile(path, []byte(strings.Repeat("x", int(maxEndpointDocumentBytes+1))), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := NewStore(path, root).Load(); !errors.Is(err, ErrConfigCorrupt) {
			t.Fatalf("oversize error = %v", err)
		}
	})

	if os.Geteuid() == 0 {
		t.Run("wrong owner", func(t *testing.T) {
			path := filepath.Join(root, "owner.json")
			if err := os.WriteFile(path, valid, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Chown(path, 1, -1); err != nil {
				t.Fatal(err)
			}
			if _, err := NewStore(path, root).Load(); !errors.Is(err, ErrConfigPerm) {
				t.Fatalf("owner error = %v", err)
			}
		})
	}
}

func TestEndpointSafeReadPinsOpenedFileAcrossPathReplacement(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "endpoints.json")
	original := []byte(`{"version":1,"endpoints":[]}`)
	replacement := []byte(`{"version":1,"defaultEndpoint":"replacement","endpoints":[]}`)
	if err := os.WriteFile(path, original, 0o600); err != nil {
		t.Fatal(err)
	}

	opened, err := openOwnerPrivateEndpointFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer opened.Close()
	replacementPath := filepath.Join(root, "replacement.json")
	if err := os.WriteFile(replacementPath, replacement, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(replacementPath, path); err != nil {
		t.Fatal(err)
	}
	got, err := readBoundedEndpointFile(opened)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(original) {
		t.Fatalf("opened descriptor followed replacement: %q", got)
	}
}

func TestBuiltinIdentityReadRejectsSymlink(t *testing.T) {
	root := t.TempDir()
	identityHome := filepath.Join(root, "identity")
	if err := os.Mkdir(identityHome, 0o700); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(root, "identities.json")
	if err := os.WriteFile(target, []byte(`{"version":1,"entries":[]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(identityHome, "endpoint-identities.json")); err != nil {
		t.Fatal(err)
	}
	store := NewStoreWithIdentityHome(filepath.Join(root, "config.json"), filepath.Join(root, "state"), identityHome)
	if _, err := store.loadBuiltinIdentities(); !errors.Is(err, ErrConfigPerm) {
		t.Fatalf("built-in symlink error = %v", err)
	}
}

func TestEndpointSaveRejectsSymlinkParent(t *testing.T) {
	root := t.TempDir()
	realParent := filepath.Join(root, "real")
	if err := os.Mkdir(realParent, 0o700); err != nil {
		t.Fatal(err)
	}
	linkedParent := filepath.Join(root, "linked")
	if err := os.Symlink(realParent, linkedParent); err != nil {
		t.Fatal(err)
	}
	store := NewStore(filepath.Join(linkedParent, "endpoints.json"), filepath.Join(root, "state"))
	if err := store.Save(Config{Version: configVersion}); !errors.Is(err, ErrConfigPerm) {
		t.Fatalf("save through symlink parent error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(realParent, "endpoints.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("save wrote through symlink parent: %v", err)
	}
}

func TestEndpointWritersRefuseUnreadableOversizeDocuments(t *testing.T) {
	root := t.TempDir()
	store := NewStoreWithIdentityHome(filepath.Join(root, "config", "endpoints.json"), filepath.Join(root, "state"), filepath.Join(root, "identity"))
	route, err := SSHRoute("example.org")
	if err != nil {
		t.Fatal(err)
	}
	id, err := NewEndpointID()
	if err != nil {
		t.Fatal(err)
	}
	large := strings.Repeat("a", int(maxEndpointDocumentBytes))
	if err := store.Save(Config{Version: configVersion, Endpoints: []Endpoint{{ID: id, Alias: large, Route: route}}}); !errors.Is(err, ErrConfigCorrupt) {
		t.Fatalf("oversize config save error = %v", err)
	}
	if _, err := os.Stat(store.ConfigPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("oversize config was published: %v", err)
	}

	home := filepath.Join(root, "codex")
	socket := filepath.Join(home, localSocketRelative)
	identities := builtinIdentities{Version: 1, Entries: []builtinIdentity{{Key: home + "\x00" + socket, EndpointID: id, Home: home, Socket: socket + large}}}
	if err := store.saveBuiltinIdentities(identities); !errors.Is(err, ErrConfigCorrupt) {
		t.Fatalf("oversize identity save error = %v", err)
	}
	if _, err := os.Stat(store.builtinPath()); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("oversize identity document was published: %v", err)
	}
}

func TestEndpointLockRejectsSymlink(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target")
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	lockPath := filepath.Join(root, "config.lock")
	if err := os.Symlink(target, lockPath); err != nil {
		t.Fatal(err)
	}
	if err := withExclusiveLock(lockPath, func() error { return nil }); !errors.Is(err, ErrConfigPerm) {
		t.Fatalf("symlink lock error = %v", err)
	}
}
