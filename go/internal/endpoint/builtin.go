package endpoint

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

const localSocketRelative = "app-server-control/app-server-control.sock"

type builtinIdentity struct {
	Key        string `json:"key"`
	EndpointID string `json:"endpointId"`
	Home       string `json:"home"`
	Socket     string `json:"socket"`
}

type builtinIdentities struct {
	Version int               `json:"version"`
	Entries []builtinIdentity `json:"entries"`
}

func defaultCodexHome() (string, error) {
	if home := strings.TrimSpace(os.Getenv("CODEX_HOME")); home != "" {
		return home, nil
	}
	userHome, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("find user home for CODEX_HOME: %w", err)
	}
	return filepath.Join(userHome, ".codex"), nil
}

func canonicalPath(value string) (string, error) {
	if value == "" || strings.ContainsRune(value, '\x00') {
		return "", errors.New("path is empty or contains NUL")
	}
	abs, err := filepath.Abs(value)
	if err != nil {
		return "", err
	}
	abs = filepath.Clean(abs)
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		return filepath.Clean(resolved), nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return "", fmt.Errorf("canonicalize %q: %w", value, err)
	}
	return abs, nil
}

func (s EndpointStore) EnsureBuiltinLocal(codexHome string) (Endpoint, error) {
	if s.StateHome == "" {
		return Endpoint{}, errors.New("state home is required for built-in local identity")
	}
	var result Endpoint
	err := withExclusiveLock(filepath.Join(s.StateHome, ".endpoint-identities.lock"), func() error {
		var err error
		result, err = s.ensureBuiltinLocal(codexHome)
		return err
	})
	return result, err
}

func (s EndpointStore) ensureBuiltinLocal(codexHome string) (Endpoint, error) {
	if s.StateHome == "" {
		return Endpoint{}, errors.New("state home is required for built-in local identity")
	}
	if codexHome == "" {
		var err error
		codexHome, err = defaultCodexHome()
		if err != nil {
			return Endpoint{}, err
		}
	}
	home, err := canonicalPath(codexHome)
	if err != nil {
		return Endpoint{}, fmt.Errorf("canonicalize CODEX_HOME: %w", err)
	}
	socket, err := canonicalPath(filepath.Join(home, localSocketRelative))
	if err != nil {
		return Endpoint{}, fmt.Errorf("derive daemon socket: %w", err)
	}
	key := home + "\x00" + socket
	identities, err := s.loadBuiltinIdentities()
	if err != nil {
		return Endpoint{}, err
	}
	for _, identity := range identities.Entries {
		if identity.Key == key {
			route, routeErr := UnixRoute(identity.Socket)
			if routeErr != nil {
				return Endpoint{}, routeErr
			}
			return Endpoint{ID: identity.EndpointID, Alias: "local", Route: route, Builtin: true, Herdr: HerdrAuto}, nil
		}
	}
	id, err := NewEndpointID()
	if err != nil {
		return Endpoint{}, err
	}
	identities.Entries = append(identities.Entries, builtinIdentity{Key: key, EndpointID: id, Home: home, Socket: socket})
	if err := s.saveBuiltinIdentities(identities); err != nil {
		return Endpoint{}, err
	}
	route, err := UnixRoute(socket)
	if err != nil {
		return Endpoint{}, err
	}
	return Endpoint{ID: id, Alias: "local", Route: route, Builtin: true, Herdr: HerdrAuto}, nil
}

func (s EndpointStore) builtinByID(id string) (Endpoint, bool, error) {
	identities, err := s.loadBuiltinIdentities()
	if err != nil {
		return Endpoint{}, false, err
	}
	for _, identity := range identities.Entries {
		if identity.EndpointID != id {
			continue
		}
		route, routeErr := UnixRoute(identity.Socket)
		if routeErr != nil {
			return Endpoint{}, false, routeErr
		}
		return Endpoint{ID: identity.EndpointID, Alias: "local", Route: route, Builtin: true, Herdr: HerdrAuto}, true, nil
	}
	return Endpoint{}, false, nil
}

func (s EndpointStore) builtinPath() string {
	return filepath.Join(s.StateHome, "endpoint-identities.json")
}

func (s EndpointStore) loadBuiltinIdentities() (builtinIdentities, error) {
	b, err := os.ReadFile(s.builtinPath())
	if errors.Is(err, fs.ErrNotExist) {
		return builtinIdentities{Version: 1}, nil
	}
	if err != nil {
		return builtinIdentities{}, fmt.Errorf("read built-in endpoint identities: %w", err)
	}
	if info, statErr := os.Stat(s.builtinPath()); statErr == nil && info.Mode().Perm()&0077 != 0 {
		return builtinIdentities{}, fmt.Errorf("%w: built-in identity mode %04o", ErrConfigPerm, info.Mode().Perm())
	}
	var identities builtinIdentities
	if err := json.Unmarshal(b, &identities); err != nil || identities.Version != 1 {
		return builtinIdentities{}, fmt.Errorf("%w: built-in identity document", ErrConfigCorrupt)
	}
	seenKey, seenID := map[string]bool{}, map[string]bool{}
	for _, identity := range identities.Entries {
		if identity.Key == "" || identity.EndpointID == "" || seenKey[identity.Key] || seenID[identity.EndpointID] {
			return builtinIdentities{}, fmt.Errorf("%w: duplicate built-in identity", ErrConfigCorrupt)
		}
		seenKey[identity.Key], seenID[identity.EndpointID] = true, true
	}
	return identities, nil
}

func (s EndpointStore) saveBuiltinIdentities(identities builtinIdentities) error {
	if err := os.MkdirAll(s.StateHome, 0700); err != nil {
		return fmt.Errorf("create endpoint state home: %w", err)
	}
	if err := os.Chmod(s.StateHome, 0700); err != nil {
		return fmt.Errorf("protect endpoint state home: %w", err)
	}
	b, err := json.MarshalIndent(identities, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	tmp, err := os.CreateTemp(s.StateHome, ".endpoint-identities-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, s.builtinPath()); err != nil {
		return err
	}
	if err := os.Chmod(s.builtinPath(), 0600); err != nil {
		return err
	}
	return nil
}

// LocalSocket returns the current daemon socket path as a diagnostic value.
// It only derives a path; it never checks, starts, replaces, or falls back from
// the managed daemon.
func LocalSocket(codexHome string) (string, error) {
	if codexHome == "" {
		var err error
		codexHome, err = defaultCodexHome()
		if err != nil {
			return "", err
		}
	}
	home, err := canonicalPath(codexHome)
	if err != nil {
		return "", err
	}
	return filepath.Join(home, localSocketRelative), nil
}
