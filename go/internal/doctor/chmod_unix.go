//go:build darwin || linux

package doctor

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

// safeChmod opens the path without following a symlink, then changes the
// already-open inode. This closes the check-then-use swap window for the
// owner-private repairs doctor is allowed to make.
func safeChmod(path string, mode os.FileMode) error {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("open permission target without following symlink: %w", err)
	}
	defer unix.Close(fd)
	if err := unix.Fchmod(fd, uint32(mode.Perm())); err != nil {
		return fmt.Errorf("chmod permission target: %w", err)
	}
	return nil
}
