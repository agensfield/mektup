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

func openExistingDatabase(path string) (*os.File, string, error) {
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, "", fmt.Errorf("%w: registered journal database does not exist privately", ErrStoreUnavailable)
	}
	file := os.NewFile(uintptr(fd), path)
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		_ = file.Close()
		return nil, "", fmt.Errorf("%w: registered journal database is not owner-private", ErrStoreUnavailable)
	}
	return file, databaseIdentity(info), nil
}

func existingDatabaseIdentity(path string) (string, error) {
	file, _, err := openExistingDatabase(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return "", err
	}
	return databaseIdentity(info), nil
}

func databaseIdentity(info os.FileInfo) string {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Sprintf("%d:%d:%d", info.Size(), info.ModTime().UnixNano(), info.Mode())
	}
	return fmt.Sprintf("%d:%d", stat.Dev, stat.Ino)
}

func readPrivateRegistry(path string) ([]byte, error) {
	parent := filepath.Dir(path)
	parentInfo, err := os.Stat(parent)
	if err != nil || !parentInfo.IsDir() || parentInfo.Mode().Perm()&0077 != 0 {
		return nil, fmt.Errorf("%w: registry directory must be owner-private", ErrRegistryInvalid)
	}
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_CLOEXEC|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: open registry: %v", ErrRegistryInvalid, err)
	}
	file := os.NewFile(uintptr(fd), path)
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
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
