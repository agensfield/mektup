package controlreceiver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	mektup "github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/internal/journal"
)

var (
	ErrStoreUnavailable = errors.New("control receiver: custody store is unavailable")
	ErrRegistryInvalid  = errors.New("control receiver: invalid owner-private store registry")
)

// RegistryEntry is trusted local configuration. It is never constructed from
// a control document. StoreID and EndpointID are the only values a remote
// request may select; StateDir remains local registry data.
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

// FileRegistry reads an owner-private registry for every request. The
// receiver never writes this file and never creates a mapping from remote
// input. A control request can therefore select only a pre-registered store.
type FileRegistry struct {
	Path string
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
	for _, entry := range matches {
		if entry.EndpointID != endpointID || !validID(entry.StoreID, mektup.StoreIDPrefix) || entry.StateDir == "" || !filepath.IsAbs(entry.StateDir) {
			return Store{}, fmt.Errorf("%w: store route does not match", ErrStoreUnavailable)
		}
		info, statErr := os.Stat(entry.StateDir)
		if statErr != nil || !info.IsDir() {
			return Store{}, fmt.Errorf("%w: registered state directory unavailable", ErrStoreUnavailable)
		}
		dbPath := filepath.Join(entry.StateDir, "journal.sqlite3")
		dbInfo, statErr := os.Lstat(dbPath)
		if statErr != nil || !dbInfo.Mode().IsRegular() || dbInfo.Mode().Perm()&0077 != 0 {
			return Store{}, fmt.Errorf("%w: registered journal database does not exist privately", ErrStoreUnavailable)
		}
		j, err := journal.Open(ctx, journal.Options{StateDir: entry.StateDir})
		if err != nil {
			return Store{}, fmt.Errorf("%w: open registered journal: %v", ErrStoreUnavailable, err)
		}
		canonical, err := j.ResolveStoreID(ctx, storeID)
		if err != nil || canonical != j.StoreID() {
			_ = j.Close()
			return Store{}, fmt.Errorf("%w: store ID is not registered by journal", ErrStoreUnavailable)
		}
		return Store{Journal: j, StoreID: canonical, EndpointID: entry.EndpointID, CloseFunc: j.Close}, nil
	}
	return Store{}, fmt.Errorf("%w: store ID is not pre-registered", ErrStoreUnavailable)
}

func validID(value, prefix string) bool {
	return mektup.ValidateID(value, prefix) == nil
}
