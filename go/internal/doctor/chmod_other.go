//go:build !darwin && !linux

package doctor

import (
	"fmt"
	"os"
)

// Platforms without the no-follow descriptor primitive fail safely rather
// than attempting a path-based repair.
func safeChmod(path string, mode os.FileMode, expected os.FileInfo) error {
	return fmt.Errorf("owner-private permission repair is unsupported on this platform: %s mode %04o", path, mode.Perm())
}
