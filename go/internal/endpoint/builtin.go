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
	return canonicalStoredPath(abs)
}

func canonicalStoredPath(value string) (string, error) {
	resolved, err := filepath.EvalSymlinks(value)
	if err == nil {
		return filepath.Clean(resolved), nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return "", err
	}
	current := value
	missing := make([]string, 0, 4)
	for {
		parent := filepath.Dir(current)
		if parent == current {
			return filepath.Clean(value), nil
		}
		missing = append([]string{filepath.Base(current)}, missing...)
		current = parent
		resolved, err := filepath.EvalSymlinks(current)
		if err == nil {
			parts := append([]string{filepath.Clean(resolved)}, missing...)
			return filepath.Join(parts...), nil
		}
		if !errors.Is(err, fs.ErrNotExist) {
			return "", err
		}
	}
}

// builtinSocketPath binds the local endpoint to Codex's stable control path,
// not to the current managed daemon socket behind that path. Codex may replace
// the final component with a symlink when its daemon rotates. Its parent must
// still be canonical so a redirected control directory cannot define routing
// authority.
func builtinSocketPath(home string) (string, error) {
	parent := filepath.Join(home, filepath.Dir(localSocketRelative))
	canonicalParent, err := canonicalStoredPath(parent)
	if err != nil {
		return "", err
	}
	if canonicalParent != parent {
		return "", fmt.Errorf("%w: noncanonical daemon socket parent", ErrConfigCorrupt)
	}
	return filepath.Join(parent, filepath.Base(localSocketRelative)), nil
}

func (s EndpointStore) EnsureBuiltinLocal(codexHome string) (Endpoint, error) {
	if s.StateHome == "" {
		return Endpoint{}, errors.New("state home is required for built-in local identity")
	}
	identityHome := s.IdentityHome
	if identityHome == "" {
		var err error
		identityHome, err = DefaultStateRoot()
		if err != nil {
			return Endpoint{}, err
		}
	}
	var result Endpoint
	err := withExclusiveLock(filepath.Join(identityHome, ".endpoint-identities.lock"), func() error {
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
	socket, err := builtinSocketPath(home)
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

// ExistingBuiltinLocal resolves a persisted local identity without creating
// one. Receiver validation uses this read-only form so input cannot establish
// endpoint authority.
func (s EndpointStore) ExistingBuiltinLocal(codexHome string) (Endpoint, error) {
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
	socket, err := builtinSocketPath(home)
	if err != nil {
		return Endpoint{}, fmt.Errorf("derive daemon socket: %w", err)
	}
	key := home + "\x00" + socket
	identities, err := s.loadBuiltinIdentities()
	if err != nil {
		return Endpoint{}, err
	}
	for _, identity := range identities.Entries {
		if identity.Key != key {
			continue
		}
		route, routeErr := UnixRoute(identity.Socket)
		if routeErr != nil {
			return Endpoint{}, routeErr
		}
		return Endpoint{ID: identity.EndpointID, Alias: "local", Route: route, Builtin: true, Herdr: HerdrAuto}, nil
	}
	return Endpoint{}, fmt.Errorf("%w: persisted local endpoint is unavailable", ErrEndpointNotFound)
}

// ResolveExistingEndpoint resolves configured or persisted endpoint identity
// without creating built-in state.
func (s EndpointStore) ResolveExistingEndpoint(selector, codexHome string) (Endpoint, error) {
	if selector == "local" {
		return s.ExistingBuiltinLocal(codexHome)
	}
	if builtIn, ok, err := s.builtinByID(selector); err != nil {
		return Endpoint{}, err
	} else if ok {
		return builtIn, nil
	}
	cfg, err := s.Load()
	if err != nil {
		return Endpoint{}, err
	}
	if selector == "" {
		selector = cfg.Default
		if selector == "" {
			return s.ExistingBuiltinLocal(codexHome)
		}
	}
	for _, item := range cfg.Endpoints {
		if item.Alias == selector || item.ID == selector {
			return item, nil
		}
	}
	return Endpoint{}, fmt.Errorf("%w: %s", ErrEndpointNotFound, selector)
}

func (s EndpointStore) builtinPath() string {
	home := s.IdentityHome
	if home == "" {
		home, _ = DefaultStateRoot()
	}
	return filepath.Join(home, "endpoint-identities.json")
}

func (s EndpointStore) loadBuiltinIdentities() (builtinIdentities, error) {
	b, err := readOwnerPrivateEndpointFile(s.builtinPath())
	if errors.Is(err, fs.ErrNotExist) {
		return builtinIdentities{Version: 1}, nil
	}
	if err != nil {
		return builtinIdentities{}, fmt.Errorf("read built-in endpoint identities: %w", err)
	}
	var identities builtinIdentities
	if err := json.Unmarshal(b, &identities); err != nil || identities.Version != 1 {
		return builtinIdentities{}, fmt.Errorf("%w: built-in identity document", ErrConfigCorrupt)
	}
	seenKey, seenID := map[string]bool{}, map[string]bool{}
	validEntries := make([]builtinIdentity, 0, len(identities.Entries))
	for _, identity := range identities.Entries {
		if identity.Key == "" || identity.EndpointID == "" || seenKey[identity.Key] || seenID[identity.EndpointID] {
			return builtinIdentities{}, fmt.Errorf("%w: duplicate built-in identity", ErrConfigCorrupt)
		}
		if !filepath.IsAbs(identity.Home) || !filepath.IsAbs(identity.Socket) || filepath.Clean(identity.Home) != identity.Home || filepath.Clean(identity.Socket) != identity.Socket || filepath.Join(identity.Home, localSocketRelative) != identity.Socket || identity.Key != identity.Home+"\x00"+identity.Socket {
			return builtinIdentities{}, fmt.Errorf("%w: inconsistent built-in identity", ErrConfigCorrupt)
		}
		canonicalHome, homeErr := canonicalStoredPath(identity.Home)
		canonicalSocketParent, socketErr := canonicalStoredPath(filepath.Dir(identity.Socket))
		if homeErr != nil || socketErr != nil || canonicalHome != identity.Home || canonicalSocketParent != filepath.Dir(identity.Socket) {
			if _, statErr := os.Lstat(identity.Home); errors.Is(statErr, fs.ErrNotExist) {
				// Older registries may retain identities for deleted temporary
				// homes. Keep them unavailable rather than letting a now-visible
				// symlinked ancestor become routing authority.
				continue
			}
			return builtinIdentities{}, fmt.Errorf("%w: noncanonical built-in identity", ErrConfigCorrupt)
		}
		seenKey[identity.Key], seenID[identity.EndpointID] = true, true
		validEntries = append(validEntries, identity)
	}
	identities.Entries = validEntries
	return identities, nil
}

func (s EndpointStore) saveBuiltinIdentities(identities builtinIdentities) error {
	home := s.IdentityHome
	if home == "" {
		var err error
		home, err = DefaultStateRoot()
		if err != nil {
			return err
		}
	}
	if err := ensureEndpointPrivateDirectory(home); err != nil {
		return fmt.Errorf("create endpoint state home: %w", err)
	}
	b, err := json.MarshalIndent(identities, "", "  ")
	if err != nil {
		return err
	}
	b = append(b, '\n')
	if err := validateEndpointDocumentSize(len(b)); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(home, ".endpoint-identities-*.tmp")
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
