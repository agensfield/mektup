//go:build darwin || linux

package application

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func openInputFile(name string) (*os.File, error) {
	fd, err := unix.Open(name, unix.O_RDONLY|unix.O_NONBLOCK|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), name)
	if file == nil {
		_ = unix.Close(fd)
		return nil, fmt.Errorf("open input file: invalid descriptor")
	}
	return file, nil
}
