package endpoint

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"
)

// withExclusiveLock serializes read-modify-write operations across Mektup
// processes. The lock file is owner-private and intentionally separate from
// the JSON document so replacing the document remains atomic.
func withExclusiveLock(lockPath string, fn func() error) error {
	if err := ensureEndpointPrivateDirectory(filepath.Dir(lockPath)); err != nil {
		return fmt.Errorf("create endpoint lock directory: %w", err)
	}
	fd, err := unix.Open(lockPath, unix.O_CREAT|unix.O_RDWR|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		if errors.Is(err, unix.ELOOP) {
			return fmt.Errorf("%w: endpoint lock must not be a symlink", ErrConfigPerm)
		}
		return fmt.Errorf("open endpoint lock: %w", err)
	}
	lock := os.NewFile(uintptr(fd), lockPath)
	if lock == nil {
		_ = unix.Close(fd)
		return errors.New("endpoint lock has an invalid descriptor")
	}
	defer lock.Close()
	info, err := lock.Stat()
	if err != nil || !info.Mode().IsRegular() || !endpointOwnerCurrent(info) {
		return fmt.Errorf("%w: endpoint lock must be a current-owner regular file", ErrConfigPerm)
	}
	if err := lock.Chmod(0600); err != nil {
		return err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		return fmt.Errorf("lock endpoint registry: %w", err)
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	return fn()
}
