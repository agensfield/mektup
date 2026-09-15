//go:build !darwin && !linux

package application

import (
	"errors"
	"os"
)

func openInputFile(name string) (*os.File, error) {
	info, err := os.Lstat(name)
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, errors.New("input path is not a regular file")
	}
	return os.Open(name)
}
