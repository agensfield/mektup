//go:build darwin || linux

package doctor

import (
	"fmt"
	"os"
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
	abs, err = canonicalExistingPrefix(abs)
	if err != nil {
		return err
	}
	fd, err := unix.Open("/", unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("open directory root: %w", err)
	}
	defer func() { _ = unix.Close(fd) }()
	parts := strings.Split(strings.TrimPrefix(abs, string(filepath.Separator)), string(filepath.Separator))
	for index, part := range parts {
		if part == "" || part == "." {
			continue
		}
		isFinal := index == len(parts)-1
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
		if !ownerCurrentDescriptor(&stat) && isFinal {
			unix.Close(next)
			return fmt.Errorf("directory component %q is not owned by current euid", part)
		}
		if !isFinal && stat.Mode&0002 != 0 && stat.Mode&01000 == 0 {
			unix.Close(next)
			return fmt.Errorf("directory ancestor %q is world-writable without sticky protection", part)
		}
		if created || isFinal {
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

// canonicalExistingPrefix resolves existing ancestors such as macOS /var,
// but never resolves the requested final component. A final path that appears
// between diagnosis and repair must still be opened with O_NOFOLLOW rather
// than being converted into authority over its symlink target.
func canonicalExistingPrefix(path string) (string, error) {
	for candidate := filepath.Dir(path); ; candidate = filepath.Dir(candidate) {
		if _, err := os.Lstat(candidate); err == nil {
			resolved, err := filepath.EvalSymlinks(candidate)
			if err != nil {
				return "", fmt.Errorf("resolve directory ancestor: %w", err)
			}
			rel, err := filepath.Rel(candidate, path)
			if err != nil {
				return "", err
			}
			return filepath.Clean(filepath.Join(resolved, rel)), nil
		} else if !os.IsNotExist(err) {
			return "", err
		}
		parent := filepath.Dir(candidate)
		if parent == candidate {
			return "", fmt.Errorf("no existing directory ancestor for %s", path)
		}
	}
}
