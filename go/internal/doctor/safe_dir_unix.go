//go:build darwin || linux

package doctor

import (
	"fmt"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// safeEnsureDir creates/verifies each path component through an open
// descriptor. O_NOFOLLOW prevents a concurrent symlink insertion from
// redirecting the repair outside the requested absolute path.
func safeEnsureDir(path string, mode uint32) error {
	abs, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return err
	}
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("open directory root: %w", err)
	}
	defer func() { _ = unix.Close(fd) }()
	parts := strings.Split(strings.TrimPrefix(abs, string(filepath.Separator)), string(filepath.Separator))
	for _, part := range parts {
		if part == "" || part == "." {
			continue
		}
		next, openErr := unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
		created := false
		if openErr != nil {
			if openErr != unix.ENOENT {
				return fmt.Errorf("open directory component %q without following symlink: %w", part, openErr)
			}
			if err := unix.Mkdirat(fd, part, mode); err != nil {
				if err != unix.EEXIST {
					return fmt.Errorf("create directory component %q: %w", part, err)
				}
			} else {
				created = true
			}
			next, openErr = unix.Openat(fd, part, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
			if openErr != nil {
				return fmt.Errorf("verify created directory component %q: %w", part, openErr)
			}
		}
		var stat unix.Stat_t
		if err := unix.Fstat(next, &stat); err != nil {
			unix.Close(next)
			return fmt.Errorf("stat directory component %q: %w", part, err)
		}
		if stat.Mode&unix.S_IFMT != unix.S_IFDIR {
			unix.Close(next)
			return fmt.Errorf("directory component %q is not a directory", part)
		}
		if created {
			if err := unix.Fchmod(next, mode); err != nil {
				unix.Close(next)
				return fmt.Errorf("protect directory component %q: %w", part, err)
			}
		}
		unix.Close(fd)
		fd = next
	}
	return nil
}
