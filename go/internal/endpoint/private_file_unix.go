//go:build darwin || linux

package endpoint

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func readOwnerPrivateEndpointFile(path string) ([]byte, error) {
	file, err := openOwnerPrivateEndpointFile(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() > maxEndpointDocumentBytes {
		return nil, fmt.Errorf("%w: endpoint document exceeds %d bytes", ErrConfigCorrupt, maxEndpointDocumentBytes)
	}
	return readBoundedEndpointFile(file)
}

func openOwnerPrivateEndpointFile(path string) (*os.File, error) {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NONBLOCK|unix.O_NOFOLLOW, 0)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, fs.ErrNotExist
		}
		if errors.Is(err, unix.ELOOP) {
			return nil, fmt.Errorf("%w: endpoint document must not be a symlink", ErrConfigPerm)
		}
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, errors.New("endpoint document has an invalid descriptor")
	}

	info, err := file.Stat()
	if err != nil {
		_ = file.Close()
		return nil, err
	}
	if !info.Mode().IsRegular() || !endpointOwnerCurrent(info) || info.Mode().Perm() != 0o600 {
		_ = file.Close()
		return nil, fmt.Errorf("%w: endpoint document must be a current-owner regular file with mode 0600", ErrConfigPerm)
	}
	return file, nil
}

func endpointOwnerCurrent(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && stat.Uid == uint32(os.Geteuid())
}

func protectEndpointPrivateDirectory(path string) error {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_DIRECTORY|unix.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("%w: endpoint directory unavailable: %v", ErrConfigPerm, err)
	}
	dir := os.NewFile(uintptr(fd), path)
	if dir == nil {
		_ = unix.Close(fd)
		return errors.New("endpoint directory has an invalid descriptor")
	}
	defer dir.Close()
	info, err := dir.Stat()
	if err != nil {
		return err
	}
	if !info.IsDir() || !endpointOwnerCurrent(info) {
		return fmt.Errorf("%w: endpoint directory must be a current-owner directory", ErrConfigPerm)
	}
	if err := dir.Chmod(0o700); err != nil {
		return err
	}
	info, err = dir.Stat()
	if err != nil {
		return err
	}
	if !info.IsDir() || !endpointOwnerCurrent(info) || info.Mode().Perm() != 0o700 {
		return fmt.Errorf("%w: endpoint directory must have mode 0700", ErrConfigPerm)
	}
	return nil
}
