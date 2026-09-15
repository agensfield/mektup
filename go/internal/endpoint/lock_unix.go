package endpoint

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// withExclusiveLock serializes read-modify-write operations across Mektup
// processes. The lock file is owner-private and intentionally separate from
// the JSON document so replacing the document remains atomic.
func withExclusiveLock(lockPath string, fn func() error) error {
	if err := os.MkdirAll(filepath.Dir(lockPath), 0700); err != nil {
		return fmt.Errorf("create endpoint lock directory: %w", err)
	}
	if err := os.Chmod(filepath.Dir(lockPath), 0700); err != nil {
		return fmt.Errorf("protect endpoint lock directory: %w", err)
	}
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return fmt.Errorf("open endpoint lock: %w", err)
	}
	defer lock.Close()
	if err := lock.Chmod(0600); err != nil {
		return err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("lock endpoint registry: %w", err)
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	return fn()
}
