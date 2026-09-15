package endpoint

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

var (
	ErrConfigCorrupt  = errors.New("endpoint configuration is corrupt")
	ErrConfigPerm     = errors.New("endpoint configuration is not owner-private")
	ErrDefaultMissing = errors.New("configured default endpoint is unavailable")
	ErrInvalidAlias   = errors.New("invalid endpoint alias")
)

// Config is intentionally only endpoint metadata.  The caller's CLI owns
// flag parsing; this package only exposes precedence and persistence hooks.
type Config struct {
	Version   int        `json:"version"`
	Default   string     `json:"defaultEndpoint,omitempty"`
	Endpoints []Endpoint `json:"endpoints"`
}

const configVersion = 1

// EndpointStore owns one owner-private config document and the identity state
// used by the built-in local route.  It has no process or transport side
// effects beyond atomic file persistence.
type EndpointStore struct {
	ConfigPath string
	StateHome  string
}

func NewStore(configPath, stateHome string) EndpointStore {
	return EndpointStore{ConfigPath: configPath, StateHome: stateHome}
}

func (s EndpointStore) Load() (Config, error) {
	if s.ConfigPath == "" {
		return Config{Version: configVersion}, nil
	}
	b, err := os.ReadFile(s.ConfigPath)
	if errors.Is(err, fs.ErrNotExist) {
		return Config{Version: configVersion}, nil
	}
	if err != nil {
		return Config{}, fmt.Errorf("read endpoint config: %w", err)
	}
	if info, statErr := os.Stat(s.ConfigPath); statErr == nil && info.Mode().Perm()&0077 != 0 {
		return Config{}, fmt.Errorf("%w: mode %04o", ErrConfigPerm, info.Mode().Perm())
	}
	var cfg Config
	if err := json.Unmarshal(b, &cfg); err != nil {
		return Config{}, fmt.Errorf("%w: %v", ErrConfigCorrupt, err)
	}
	if err := validateConfig(cfg); err != nil {
		return Config{}, err
	}
	for i := range cfg.Endpoints {
		cfg.Endpoints[i] = cfg.Endpoints[i].Normalized()
	}
	return cfg, nil
}

func validateConfig(cfg Config) error {
	if cfg.Version != 0 && cfg.Version != configVersion {
		return fmt.Errorf("%w: unsupported version %d", ErrConfigCorrupt, cfg.Version)
	}
	seenAlias := make(map[string]struct{}, len(cfg.Endpoints))
	seenID := make(map[string]struct{}, len(cfg.Endpoints))
	for i := range cfg.Endpoints {
		endpoint := cfg.Endpoints[i].Normalized()
		cfg.Endpoints[i] = endpoint
		if endpoint.Builtin {
			return fmt.Errorf("%w: built-in endpoint cannot be configured", ErrConfigCorrupt)
		}
		if !validAlias(endpoint.Alias) {
			return fmt.Errorf("%w: alias %q is reserved or invalid", ErrConfigCorrupt, endpoint.Alias)
		}
		if err := endpoint.Validate(); err != nil {
			return fmt.Errorf("%w: %v", ErrConfigCorrupt, err)
		}
		if _, ok := seenAlias[endpoint.Alias]; ok {
			return fmt.Errorf("%w: %q", ErrConfigCorrupt, endpoint.Alias)
		}
		if _, ok := seenID[endpoint.ID]; ok {
			return fmt.Errorf("%w: %q", ErrConfigCorrupt, endpoint.ID)
		}
		seenAlias[endpoint.Alias], seenID[endpoint.ID] = struct{}{}, struct{}{}
	}
	if cfg.Default != "" {
		if _, ok := seenAlias[cfg.Default]; !ok {
			if _, ok := seenID[cfg.Default]; !ok {
				return fmt.Errorf("%w: default %q", ErrConfigCorrupt, cfg.Default)
			}
		}
	}
	return nil
}

func (s EndpointStore) Save(cfg Config) error {
	if s.ConfigPath == "" {
		return errors.New("endpoint config path is empty")
	}
	if cfg.Version == 0 {
		cfg.Version = configVersion
	}
	for i := range cfg.Endpoints {
		cfg.Endpoints[i] = cfg.Endpoints[i].Normalized()
	}
	if err := validateConfig(cfg); err != nil {
		return err
	}
	dir := filepath.Dir(s.ConfigPath)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("create endpoint config directory: %w", err)
	}
	if err := os.Chmod(dir, 0700); err != nil {
		return fmt.Errorf("protect endpoint config directory: %w", err)
	}
	b, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("encode endpoint config: %w", err)
	}
	b = append(b, '\n')
	tmp, err := os.CreateTemp(dir, ".endpoints-*.tmp")
	if err != nil {
		return fmt.Errorf("create endpoint config temporary file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("write endpoint config: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("sync endpoint config: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close endpoint config: %w", err)
	}
	if err := os.Rename(tmpName, s.ConfigPath); err != nil {
		return fmt.Errorf("replace endpoint config: %w", err)
	}
	if err := os.Chmod(s.ConfigPath, 0600); err != nil {
		return fmt.Errorf("protect endpoint config: %w", err)
	}
	if dirFile, err := os.Open(dir); err == nil {
		_ = dirFile.Sync()
		_ = dirFile.Close()
	}
	return nil
}

func NewEndpointID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	// UUIDv7: the first 48 bits are Unix milliseconds, followed by random
	// entropy. Endpoint IDs are opaque, but the ordered form makes diagnostics
	// and portable receipts easier to inspect without sacrificing uniqueness.
	ms := uint64(time.Now().UnixMilli())
	for i := 5; i >= 0; i-- {
		b[i] = byte(ms)
		ms >>= 8
	}
	b[6] = (b[6] & 0x0f) | 0x70
	b[8] = (b[8] & 0x3f) | 0x80
	return "ep_" + hex.EncodeToString(b[:4]) + "-" + hex.EncodeToString(b[4:6]) + "-" + hex.EncodeToString(b[6:8]) + "-" + hex.EncodeToString(b[8:10]) + "-" + hex.EncodeToString(b[10:]), nil
}

func validAlias(alias string) bool {
	return alias != "" && alias != "local" && strings.TrimSpace(alias) == alias && !strings.ContainsAny(alias, "/?#\r\n\t ")
}

func (s EndpointStore) Add(endpoint Endpoint) error {
	if !validAlias(endpoint.Alias) {
		return fmt.Errorf("%w: %q", ErrInvalidAlias, endpoint.Alias)
	}
	if endpoint.Builtin {
		return ErrBuiltinImmutable
	}
	if endpoint.ID == "" {
		var err error
		endpoint.ID, err = NewEndpointID()
		if err != nil {
			return err
		}
	}
	endpoint = endpoint.Normalized()
	if err := endpoint.Validate(); err != nil {
		return err
	}
	return withExclusiveLock(s.configLockPath(), func() error {
		cfg, err := s.Load()
		if err != nil {
			return err
		}
		for _, item := range cfg.Endpoints {
			if item.Alias == endpoint.Alias {
				return fmt.Errorf("%w: %s", ErrDuplicateAlias, endpoint.Alias)
			}
			if item.ID == endpoint.ID {
				return fmt.Errorf("%w: %s", ErrDuplicateEndpointID, endpoint.ID)
			}
		}
		cfg.Endpoints = append(cfg.Endpoints, endpoint)
		return s.Save(cfg)
	})
}

func (s EndpointStore) configLockPath() string {
	if s.ConfigPath == "" {
		return "endpoint-config.lock"
	}
	return s.ConfigPath + ".lock"
}

func (s EndpointStore) List() ([]Endpoint, error) {
	cfg, err := s.Load()
	if err != nil {
		return nil, err
	}
	items := append([]Endpoint(nil), cfg.Endpoints...)
	// The built-in local alias is an invocation-local convenience, but it is a
	// real endpoint record and therefore appears in endpoint list output.  Its
	// identity remains distinct for each current CODEX_HOME/socket pair.
	if local, localErr := s.EnsureBuiltinLocal(""); localErr == nil {
		items = append(items, local)
	} else {
		return nil, localErr
	}
	sort.Slice(items, func(i, j int) bool { return items[i].Alias < items[j].Alias })
	return items, nil
}

func (s EndpointStore) Show(selector string) (Endpoint, error) {
	if selector == "local" {
		return s.EnsureBuiltinLocal("")
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
	for _, endpoint := range cfg.Endpoints {
		if endpoint.Alias == selector || endpoint.ID == selector {
			return endpoint, nil
		}
	}
	return Endpoint{}, fmt.Errorf("%w: %s", ErrEndpointNotFound, selector)
}

func (s EndpointStore) Remove(selector string) error {
	if selector == "local" {
		return ErrBuiltinImmutable
	}
	if _, ok, err := s.builtinByID(selector); err != nil {
		return err
	} else if ok {
		return ErrBuiltinImmutable
	}
	return withExclusiveLock(s.configLockPath(), func() error {
		cfg, err := s.Load()
		if err != nil {
			return err
		}
		index := -1
		for i, endpoint := range cfg.Endpoints {
			if endpoint.Alias == selector || endpoint.ID == selector {
				if endpoint.Builtin {
					return ErrBuiltinImmutable
				}
				index = i
				break
			}
		}
		if index < 0 {
			return fmt.Errorf("%w: %s", ErrEndpointNotFound, selector)
		}
		cfg.Endpoints = append(cfg.Endpoints[:index], cfg.Endpoints[index+1:]...)
		if cfg.Default == selector {
			cfg.Default = ""
		}
		return s.Save(cfg)
	})
}

// SelectEndpoint implements the CLI-independent precedence hook: an explicit
// flag, then dedicated environment value, then config value, then default.
func SelectEndpoint(flag, environment, configured, fallback string) string {
	for _, candidate := range []string{flag, environment, configured, fallback} {
		if strings.TrimSpace(candidate) != "" {
			return candidate
		}
	}
	return ""
}

type SelectionInputs struct {
	Flag, Environment, Config, Default string
}

func (i SelectionInputs) Endpoint() string {
	return SelectEndpoint(i.Flag, i.Environment, i.Config, i.Default)
}
