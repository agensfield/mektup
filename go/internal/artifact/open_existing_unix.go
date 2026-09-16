//go:build darwin || linux

package artifact

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

// openExistingNoFollow protects the final component against a symlink swap
// between Lstat and content verification. The os.Root keeps parent traversal
// inside the managed root; O_NOFOLLOW protects the file being reused.
func openExistingNoFollow(root *os.Root, name string) (*os.File, error) {
	f, err := root.OpenFile(name, os.O_RDONLY|unix.O_NONBLOCK|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if errors.Is(err, unix.ELOOP) {
		return nil, ErrSymlink
	}
	return f, err
}
