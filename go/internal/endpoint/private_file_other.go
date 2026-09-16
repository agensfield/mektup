//go:build !darwin && !linux

package endpoint

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
)

// Platforms without a portable no-follow open use pre/open/post identity
// checks and fail closed on any observed replacement. Go does not expose a
// portable file-owner identity here; native Windows support is not a target.
func readOwnerPrivateEndpointFile(path string) ([]byte, error) {
	before, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fs.ErrNotExist
	}
	if err != nil {
		return nil, err
	}
	if before.Mode()&os.ModeSymlink != 0 || !before.Mode().IsRegular() || before.Mode().Perm() != 0o600 {
		return nil, fmt.Errorf("%w: endpoint document must be a regular file with mode 0600", ErrConfigPerm)
	}
	if before.Size() > maxEndpointDocumentBytes {
		return nil, fmt.Errorf("%w: endpoint document exceeds %d bytes", ErrConfigCorrupt, maxEndpointDocumentBytes)
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if !opened.Mode().IsRegular() || opened.Mode().Perm() != 0o600 || !os.SameFile(before, opened) {
		return nil, fmt.Errorf("%w: endpoint document changed during open", ErrConfigPerm)
	}
	data, err := readBoundedEndpointFile(file)
	if err != nil {
		return nil, err
	}
	after, err := os.Lstat(path)
	if err != nil || after.Mode()&os.ModeSymlink != 0 || !os.SameFile(opened, after) {
		return nil, fmt.Errorf("%w: endpoint document changed during read", ErrConfigPerm)
	}
	return data, nil
}

func endpointOwnerCurrent(os.FileInfo) bool { return true }

func protectEndpointPrivateDirectory(path string) error {
	before, err := os.Lstat(path)
	if err != nil || before.Mode()&os.ModeSymlink != 0 || !before.IsDir() {
		return fmt.Errorf("%w: endpoint directory must be a directory", ErrConfigPerm)
	}
	if err := os.Chmod(path, 0o700); err != nil {
		return err
	}
	after, err := os.Lstat(path)
	if err != nil || after.Mode()&os.ModeSymlink != 0 || !after.IsDir() || !os.SameFile(before, after) || after.Mode().Perm() != 0o700 {
		return fmt.Errorf("%w: endpoint directory changed while being protected", ErrConfigPerm)
	}
	return nil
}
