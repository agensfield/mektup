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
func safeChmod(path string, mode os.FileMode, expected os.FileInfo) error {
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("open permission target without following symlink: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return fmt.Errorf("open permission target descriptor: %s", path)
	}
	defer file.Close()
	actual, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat permission target descriptor: %w", err)
	}
	if expected != nil && (!os.SameFile(expected, actual) || expected.Mode()&os.ModeType != actual.Mode()&os.ModeType) {
		return fmt.Errorf("permission target changed since diagnosis: %s", path)
	}
	if !actual.Mode().IsRegular() && !actual.IsDir() {
		return fmt.Errorf("refusing special permission target: %s", path)
	}
	if err := file.Chmod(mode.Perm()); err != nil {
		return fmt.Errorf("chmod permission target: %w", err)
	}
	return nil
}
