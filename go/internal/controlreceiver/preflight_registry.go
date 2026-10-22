package controlreceiver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/agensfield/mektup/go/internal/journal"
)

// RegisteredEntry reads existing registry authority without repairing or
// creating files. The returned path is locally trusted registry data only.
func (r FileRegistry) RegisteredEntry(endpointID, storeID string) (RegistryEntry, error) {
	data, err := readPrivateRegistry(r.Path)
	if err != nil {
		return RegistryEntry{}, err
	}
	var doc RegistryDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		return RegistryEntry{}, ErrRegistryInvalid
	}
	if err := validateRegistryDocument(doc); err != nil {
		return RegistryEntry{}, err
	}
	for _, entry := range doc.Stores {
		if entry.StoreID == storeID {
			if entry.EndpointID != endpointID {
				return RegistryEntry{}, ErrStoreUnavailable
			}
			canonical, err := canonicalStateDirectory(entry.StateDir)
			if err != nil || canonical != entry.StateDir {
				return RegistryEntry{}, ErrStoreUnavailable
			}
			return entry, nil
		}
	}
	return RegistryEntry{}, ErrStoreUnavailable
}

// CheckLocalJournal avoids reopening SQLite in the same process as the active
// writer. It validates the registered tuple against the already-open journal.
func (r FileRegistry) CheckLocalJournal(ctx context.Context, endpointID, storeID string, j *journal.Journal) error {
	entry, err := r.RegisteredEntry(endpointID, storeID)
	if err != nil {
		return err
	}
	if j == nil || j.StoreID() != storeID || j.StateDir() != entry.StateDir {
		return ErrStoreUnavailable
	}
	if err := j.ValidateStoreIdentity(ctx); err != nil {
		return err
	}
	canonical, err := j.ResolveStoreID(ctx, storeID)
	if err != nil || canonical != storeID {
		return ErrStoreUnavailable
	}
	return nil
}

// CheckReadiness validates a registered custody path snapshot without opening
// SQLite. A read-only SQLite connection can still mutate WAL shared memory or
// create sidecars. Actual SQL/store identity is checked by the originating
// process through CheckLocalJournal and again by custody operations.
// This does not attest SQL integrity, current transactions or future commit.
func (r FileRegistry) CheckReadiness(ctx context.Context, endpointID, storeID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	entry, err := r.RegisteredEntry(endpointID, storeID)
	if err != nil {
		return err
	}
	databasePath := filepath.Join(entry.StateDir, "journal.sqlite3")
	if err := checkReadableFile(ctx, databasePath, true); err != nil {
		return err
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if _, err := os.Lstat(databasePath + suffix); os.IsNotExist(err) {
			continue
		} else if err != nil {
			return ErrStoreUnavailable
		}
		if err := checkReadableFile(ctx, databasePath+suffix, false); err != nil {
			return err
		}
	}
	// Revalidate directory authority after all file opens, without fixing modes.
	canonical, err := canonicalStateDirectory(entry.StateDir)
	if err != nil || canonical != entry.StateDir {
		return ErrStoreUnavailable
	}
	return ctx.Err()
}

func checkReadableFile(ctx context.Context, path string, database bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || !ownerPrivate(before) {
		return ErrStoreUnavailable
	}
	file, err := os.Open(path)
	if err != nil {
		return ErrStoreUnavailable
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(before, opened) {
		return ErrStoreUnavailable
	}
	if database {
		header := make([]byte, 100)
		if _, err := io.ReadFull(file, header); err != nil || !bytes.Equal(header[:16], []byte("SQLite format 3\x00")) {
			return fmt.Errorf("%w: unrecognized SQLite header", ErrStoreUnavailable)
		}
	} else {
		var one [1]byte
		if _, err := file.Read(one[:]); err != nil && err != io.EOF {
			return ErrStoreUnavailable
		}
	}
	after, err := os.Lstat(path)
	if err != nil || !os.SameFile(before, after) {
		return ErrStoreUnavailable
	}
	return ctx.Err()
}
