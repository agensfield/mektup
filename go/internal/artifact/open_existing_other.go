//go:build !darwin && !linux

package artifact

import (
	"errors"
	"os"
)

// Platforms without a no-follow primitive fail closed rather than treating a
// racy Lstat followed by Open as proof of artifact identity.
func openExistingNoFollow(_ *os.Root, _ string) (*os.File, error) {
	return nil, errors.New("artifact: safe no-follow reuse is unsupported on this platform")
}
