//go:build !darwin && !linux

package doctor

import "os"

func ownerCurrent(os.FileInfo) bool { return true }
