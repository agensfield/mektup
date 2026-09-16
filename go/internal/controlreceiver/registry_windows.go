//go:build windows

package controlreceiver

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const maxRegistryBytes = 1 << 20

func readPrivateRegistry(path string) ([]byte, error) {
	parent := filepath.Dir(path)
	parentInfo, err := os.Lstat(parent)
	if err != nil || !parentInfo.IsDir() || !ownerPrivate(parentInfo) {
		return nil, fmt.Errorf("%w: registry directory must be owner-private", ErrRegistryInvalid)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("%w: open registry: %v", ErrRegistryInvalid, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || !ownerPrivate(info) {
		return nil, fmt.Errorf("%w: registry must be an owner-private regular file", ErrRegistryInvalid)
	}
	data, err := io.ReadAll(io.LimitReader(file, maxRegistryBytes+1))
	if err != nil {
		return nil, fmt.Errorf("%w: read registry: %v", ErrRegistryInvalid, err)
	}
	if len(data) > maxRegistryBytes {
		return nil, fmt.Errorf("%w: registry exceeds bound", ErrRegistryInvalid)
	}
	return data, nil
}

func ownerCurrent(os.FileInfo) bool                    { return true }
func ownerPrivate(info os.FileInfo) bool               { return info.Mode().Perm()&0077 == 0 && ownerCurrent(info) }
func withRegistryLock(_ string, fn func() error) error { return fn() }
