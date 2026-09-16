//go:build darwin || linux

package doctor

import (
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func ownerCurrent(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return ok && uint64(stat.Uid) == uint64(os.Geteuid())
}

func ownerCurrentDescriptor(stat *unix.Stat_t) bool {
	return uint64(stat.Uid) == uint64(os.Geteuid())
}
