package controlreceiver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	mektup "github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/internal/endpoint"
	"github.com/agensfield/mektup/go/internal/journal"
)

var (
	ErrStoreUnavailable = errors.New("control receiver: custody store is unavailable")
	ErrRegistryInvalid  = errors.New("control receiver: invalid owner-private store registry")
	ErrRegistryConflict = errors.New("control receiver: custody store registry conflict")
)

const controlRegistryFilename = "control-registry.json"

type RegistryEntry struct {
	StoreID    string `json:"storeId"`
	EndpointID string `json:"endpointId"`
	StateDir   string `json:"stateDir"`
}
type RegistryDocument struct {
	Version int             `json:"version"`
	Stores  []RegistryEntry `json:"stores"`
}
type Store struct {
	Journal    *journal.Journal
	StoreID    string
	EndpointID string
	CloseFunc  func() error
}

func (s Store) Close() error {
	if s.Journal == nil {
		return nil
	}
	if s.CloseFunc != nil {
		return s.CloseFunc()
	}
	return s.Journal.Close()
}

type Resolver interface {
	Resolve(context.Context, string, string) (Store, error)
}
type FileRegistry struct{ Path string }

func DefaultStateRoot() (string, error) { return endpoint.DefaultStateRoot() }
func DefaultRegistryPath() (string, error) {
	root, err := DefaultStateRoot()
	if err != nil {
		return "", err
	}
	return filepath.Join(root, controlRegistryFilename), nil
}
func NewDefaultRegistry() (FileRegistry, error) {
	p, err := DefaultRegistryPath()
	if err != nil {
		return FileRegistry{}, err
	}
	return FileRegistry{Path: p}, nil
}

func (r FileRegistry) Register(ctx context.Context, endpointID string, j *journal.Journal) error {
	if r.Path == "" || j == nil || !validID(endpointID, mektup.EndpointIDPrefix) {
		return fmt.Errorf("%w: invalid local registration", ErrRegistryInvalid)
	}
	stateDir, err := canonicalStateDirectory(j.StateDir())
	if err != nil {
		return err
	}
	if resolved, resolveErr := j.ResolveStoreID(ctx, j.StoreID()); resolveErr != nil || resolved != j.StoreID() {
		return fmt.Errorf("%w: locally opened journal identity is not stable", ErrStoreUnavailable)
	}
	verified, err := journal.OpenExisting(ctx, journal.Options{StateDir: stateDir})
	if err != nil {
		return fmt.Errorf("%w: verify locally opened journal: %v", ErrStoreUnavailable, err)
	}
	resolved, resolveErr := verified.ResolveStoreID(ctx, j.StoreID())
	closeErr := verified.Close()
	if resolveErr != nil || closeErr != nil || resolved != j.StoreID() {
		return fmt.Errorf("%w: local journal store identity mismatch", ErrStoreUnavailable)
	}
	if err := ensurePrivateDirectory(filepath.Dir(r.Path)); err != nil {
		return err
	}
	return withRegistryLock(r.Path+".lock", func() error {
		document, err := loadRegistryForUpdate(r.Path)
		if err != nil {
			return err
		}
		entry := RegistryEntry{StoreID: j.StoreID(), EndpointID: endpointID, StateDir: stateDir}
		for _, existing := range document.Stores {
			if existing.StoreID != entry.StoreID {
				continue
			}
			if existing == entry {
				return nil
			}
			return fmt.Errorf("%w: store ID %s is already mapped", ErrRegistryConflict, entry.StoreID)
		}
		document.Stores = append(document.Stores, entry)
		return writeRegistryAtomically(r.Path, document)
	})
}

func (r FileRegistry) Resolve(ctx context.Context, endpointID, storeID string) (Store, error) {
	if r.Path == "" {
		return Store{}, ErrStoreUnavailable
	}
	data, err := readPrivateRegistry(r.Path)
	if err != nil {
		return Store{}, err
	}
	var document RegistryDocument
	if err := json.Unmarshal(data, &document); err != nil || document.Version != 1 {
		return Store{}, fmt.Errorf("%w: malformed registry", ErrRegistryInvalid)
	}
	if err := validateRegistryDocument(document); err != nil {
		return Store{}, err
	}
	var matches []RegistryEntry
	for _, entry := range document.Stores {
		if entry.StoreID == storeID {
			matches = append(matches, entry)
		}
	}
	if len(matches) == 0 {
		return Store{}, fmt.Errorf("%w: store ID is not pre-registered", ErrStoreUnavailable)
	}
	if len(matches) != 1 {
		return Store{}, fmt.Errorf("%w: store ID has ambiguous registry entries", ErrStoreUnavailable)
	}
	entry := matches[0]
	if entry.EndpointID != endpointID {
		return Store{}, fmt.Errorf("%w: store route does not match", ErrStoreUnavailable)
	}
	canonical, stateErr := canonicalStateDirectory(entry.StateDir)
	if stateErr != nil || canonical != entry.StateDir {
		return Store{}, fmt.Errorf("%w: registered state directory unavailable", ErrStoreUnavailable)
	}
	dbPath := filepath.Join(entry.StateDir, "journal.sqlite3")
	dbFile, identity, err := openExistingDatabase(dbPath)
	if err != nil {
		return Store{}, err
	}
	j, err := journal.OpenExisting(ctx, journal.Options{StateDir: entry.StateDir})
	if err != nil {
		_ = dbFile.Close()
		return Store{}, fmt.Errorf("%w: open registered journal: %v", ErrStoreUnavailable, err)
	}
	_ = dbFile.Close()
	currentIdentity, identityErr := existingDatabaseIdentity(dbPath)
	if identityErr != nil || currentIdentity != identity {
		_ = j.Close()
		return Store{}, fmt.Errorf("%w: registered journal was replaced during open", ErrStoreUnavailable)
	}
	resolved, err := j.ResolveStoreID(ctx, storeID)
	if err != nil || resolved != j.StoreID() {
		_ = j.Close()
		return Store{}, fmt.Errorf("%w: store ID is not registered by journal", ErrStoreUnavailable)
	}
	return Store{Journal: j, StoreID: resolved, EndpointID: entry.EndpointID, CloseFunc: j.Close}, nil
}

func loadRegistryForUpdate(path string) (RegistryDocument, error) {
	if _, err := os.Lstat(path); errors.Is(err, fs.ErrNotExist) {
		return RegistryDocument{Version: 1}, nil
	}
	data, err := readPrivateRegistry(path)
	if err != nil {
		return RegistryDocument{}, err
	}
	var document RegistryDocument
	if err := json.Unmarshal(data, &document); err != nil || document.Version != 1 {
		return RegistryDocument{}, fmt.Errorf("%w: malformed registry", ErrRegistryInvalid)
	}
	if err := validateRegistryDocument(document); err != nil {
		return RegistryDocument{}, err
	}
	return document, nil
}
func validateRegistryDocument(document RegistryDocument) error {
	if document.Version != 1 {
		return fmt.Errorf("%w: unsupported registry version", ErrRegistryInvalid)
	}
	seen := make(map[string]struct{}, len(document.Stores))
	for _, entry := range document.Stores {
		if !validID(entry.StoreID, mektup.StoreIDPrefix) || !validID(entry.EndpointID, mektup.EndpointIDPrefix) || entry.StateDir == "" || !filepath.IsAbs(entry.StateDir) || filepath.Clean(entry.StateDir) != entry.StateDir {
			return fmt.Errorf("%w: invalid registry entry", ErrRegistryInvalid)
		}
		if _, ok := seen[entry.StoreID]; ok {
			return fmt.Errorf("%w: duplicate store ID", ErrRegistryInvalid)
		}
		seen[entry.StoreID] = struct{}{}
	}
	return nil
}
func canonicalStateDirectory(value string) (string, error) {
	abs, err := filepath.Abs(value)
	if err != nil {
		return "", fmt.Errorf("%w: state directory is not absolute", ErrStoreUnavailable)
	}
	abs = filepath.Clean(abs)
	info, err := os.Lstat(abs)
	if err != nil || !info.IsDir() || !ownerPrivate(info) {
		return "", fmt.Errorf("%w: registered state directory is unavailable", ErrStoreUnavailable)
	}
	resolved, err := filepath.EvalSymlinks(abs)
	if err != nil {
		return "", fmt.Errorf("%w: registered state directory is not canonical", ErrStoreUnavailable)
	}
	return filepath.Clean(resolved), nil
}
func ensurePrivateDirectory(path string) error {
	if info, err := os.Lstat(path); err == nil {
		if !info.IsDir() || !ownerCurrent(info) {
			return fmt.Errorf("%w: registry directory is not owner-private", ErrRegistryInvalid)
		}
	} else if errors.Is(err, fs.ErrNotExist) {
		if err := os.MkdirAll(path, 0700); err != nil {
			return fmt.Errorf("%w: registry directory: %v", ErrRegistryInvalid, err)
		}
	} else {
		return fmt.Errorf("%w: registry directory: %v", ErrRegistryInvalid, err)
	}
	if err := os.Chmod(path, 0700); err != nil {
		return fmt.Errorf("%w: registry directory: %v", ErrRegistryInvalid, err)
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || !ownerPrivate(info) {
		return fmt.Errorf("%w: registry directory is not owner-private", ErrRegistryInvalid)
	}
	return nil
}
func writeRegistryAtomically(path string, document RegistryDocument) error {
	data, err := json.MarshalIndent(document, "", "  ")
	if err != nil {
		return fmt.Errorf("%w: encode registry: %v", ErrRegistryInvalid, err)
	}
	data = append(data, '\n')
	if len(data) > maxRegistryBytes {
		return fmt.Errorf("%w: registry exceeds bound", ErrRegistryInvalid)
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".control-registry-*.tmp")
	if err != nil {
		return fmt.Errorf("%w: create registry temporary file: %v", ErrRegistryInvalid, err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0600); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("%w: write registry: %v", ErrRegistryInvalid, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("%w: replace registry: %v", ErrRegistryInvalid, err)
	}
	if err := os.Chmod(path, 0600); err != nil {
		return err
	}
	if dir, err := os.Open(filepath.Dir(path)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}
func validID(value, prefix string) bool { return mektup.ValidateID(value, prefix) == nil }
