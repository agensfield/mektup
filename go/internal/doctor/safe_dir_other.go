//go:build !darwin && !linux

package doctor

import "fmt"

func safeEnsureDir(path string, mode uint32) error {
	return fmt.Errorf("owner-private directory repair is unsupported on this platform: %s mode %04o", path, mode)
}
