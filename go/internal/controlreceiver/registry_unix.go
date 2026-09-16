//go:build !windows

package controlreceiver

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

const maxRegistryBytes = 1 << 20

func readPrivateRegistry(path string) ([]byte, error) {
	parent := filepath.Dir(path)
	parentInfo, err := os.Lstat(parent)
	if err != nil || !parentInfo.IsDir() || !ownerPrivate(parentInfo) {
		return nil, fmt.Errorf("%w: registry directory must be owner-private", ErrRegistryInvalid)
	}
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_NONBLOCK|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: open registry: %v", ErrRegistryInvalid, err)
	}
	file := os.NewFile(uintptr(fd), path)
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

func ownerCurrent(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return !ok || stat.Uid == uint32(os.Getuid())
}

func ownerPrivate(info os.FileInfo) bool { return info.Mode().Perm()&0077 == 0 && ownerCurrent(info) }

func withRegistryLock(path string, fn func() error) error {
	if err := ensurePrivateDirectory(filepath.Dir(path)); err != nil {
		return err
	}
	fd, err := syscall.Open(path, syscall.O_CREAT|syscall.O_RDWR|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return fmt.Errorf("%w: open registry lock: %v", ErrRegistryInvalid, err)
	}
	lock := os.NewFile(uintptr(fd), path)
	defer lock.Close()
	if err := lock.Chmod(0600); err != nil {
		return err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("%w: lock registry: %v", ErrRegistryInvalid, err)
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	return fn()
}
