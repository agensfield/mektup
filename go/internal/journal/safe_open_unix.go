//go:build darwin || linux

package journal

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"
)

type fileIdentity struct {
	device uint64
	inode  uint64
}

func identityFromStat(stat *unix.Stat_t) fileIdentity {
	return fileIdentity{device: uint64(stat.Dev), inode: uint64(stat.Ino)}
}

func identityFromInfo(info os.FileInfo) (fileIdentity, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fileIdentity{}, fmt.Errorf("journal: unsupported filesystem identity")
	}
	return fileIdentity{device: uint64(stat.Dev), inode: uint64(stat.Ino)}, nil
}

func sameIdentity(a, b fileIdentity) bool { return a == b }

func ownerIsCurrent(stat *unix.Stat_t) bool { return uint64(stat.Uid) == uint64(os.Geteuid()) }

func acquireStateDirectory(path string, create bool) (*os.File, fileIdentity, error) {
	if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
		if !create {
			return nil, fileIdentity{}, fmt.Errorf("journal: existing state directory unavailable")
		}
		if err := os.Mkdir(path, 0700); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, fileIdentity{}, fmt.Errorf("journal: create state directory: %w", err)
		}
	} else if err != nil {
		return nil, fileIdentity{}, fmt.Errorf("journal: inspect state directory: %w", err)
	}
	fd, err := unix.Open(path, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fileIdentity{}, fmt.Errorf("journal: open state directory without following symlink: %w", err)
	}
	file := os.NewFile(uintptr(fd), filepath.Clean(path))
	if file == nil {
		_ = unix.Close(fd)
		return nil, fileIdentity{}, fmt.Errorf("journal: open state directory descriptor")
	}
	identity, err := verifyDirectoryDescriptor(file, 0700, create)
	if err != nil {
		_ = file.Close()
		return nil, fileIdentity{}, err
	}
	if err := verifyPathIdentity(path, identity, true); err != nil {
		_ = file.Close()
		return nil, fileIdentity{}, fmt.Errorf("journal: state directory identity changed: %w", err)
	}
	return file, identity, nil
}

func verifyDirectoryDescriptor(file *os.File, mode os.FileMode, repair bool) (fileIdentity, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		return fileIdentity{}, fmt.Errorf("journal: stat state directory: %w", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR {
		return fileIdentity{}, fmt.Errorf("journal: state path is not a directory")
	}
	if !ownerIsCurrent(&stat) {
		return fileIdentity{}, fmt.Errorf("journal: state directory is not owned by current euid")
	}
	if uint32(stat.Mode&0777) != uint32(mode.Perm()) {
		if !repair {
			return fileIdentity{}, fmt.Errorf("journal: state directory is not mode %04o", mode.Perm())
		}
		if err := unix.Fchmod(int(file.Fd()), uint32(mode.Perm())); err != nil {
			return fileIdentity{}, fmt.Errorf("journal: protect state directory: %w", err)
		}
		if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
			return fileIdentity{}, fmt.Errorf("journal: restat state directory: %w", err)
		}
		if uint32(stat.Mode&0777) != uint32(mode.Perm()) {
			return fileIdentity{}, fmt.Errorf("journal: state directory is not mode %04o", mode.Perm())
		}
	}
	return identityFromStat(&stat), nil
}

func acquireDatabase(dir *os.File, path string, create bool) (*os.File, fileIdentity, error) {
	flags := unix.O_RDWR | unix.O_NONBLOCK | unix.O_CLOEXEC | unix.O_NOFOLLOW
	fd, err := unix.Openat(int(dir.Fd()), "journal.sqlite3", flags, 0)
	if errors.Is(err, unix.ENOENT) && create {
		fd, err = unix.Openat(int(dir.Fd()), "journal.sqlite3", flags|unix.O_CREAT|unix.O_EXCL, 0600)
	}
	if err != nil {
		return nil, fileIdentity{}, fmt.Errorf("journal: open database without following symlink: %w", err)
	}
	file := os.NewFile(uintptr(fd), path)
	if file == nil {
		_ = unix.Close(fd)
		return nil, fileIdentity{}, fmt.Errorf("journal: open database descriptor")
	}
	identity, err := verifyDatabaseDescriptor(file, create)
	if err != nil {
		_ = file.Close()
		return nil, fileIdentity{}, err
	}
	if err := verifyPathIdentity(path, identity, false); err != nil {
		_ = file.Close()
		return nil, fileIdentity{}, fmt.Errorf("journal: database identity changed: %w", err)
	}
	return file, identity, nil
}

func verifyDatabaseDescriptor(file *os.File, repair bool) (fileIdentity, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
		return fileIdentity{}, fmt.Errorf("journal: stat database: %w", err)
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFREG {
		return fileIdentity{}, fmt.Errorf("journal: database is not a regular file")
	}
	if !ownerIsCurrent(&stat) {
		return fileIdentity{}, fmt.Errorf("journal: database is not owned by current euid")
	}
	if stat.Mode&0777 != 0600 {
		if !repair {
			return fileIdentity{}, fmt.Errorf("journal: database is not mode 0600")
		}
		if err := unix.Fchmod(int(file.Fd()), 0600); err != nil {
			return fileIdentity{}, fmt.Errorf("journal: protect database: %w", err)
		}
		if err := unix.Fstat(int(file.Fd()), &stat); err != nil {
			return fileIdentity{}, fmt.Errorf("journal: restat database: %w", err)
		}
		if stat.Mode&0777 != 0600 {
			return fileIdentity{}, fmt.Errorf("journal: database is not mode 0600")
		}
	}
	return identityFromStat(&stat), nil
}

func verifyPathIdentity(path string, expected fileIdentity, directory bool) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if directory != info.IsDir() || !directory && !info.Mode().IsRegular() {
		return fmt.Errorf("journal: pathname has unexpected type")
	}
	actual, err := identityFromInfo(info)
	if err != nil {
		return err
	}
	if !sameIdentity(expected, actual) {
		return fmt.Errorf("journal: pathname identity mismatch")
	}
	return nil
}

func secureDatabaseFiles(dir, database *os.File) error {
	if _, err := verifyDatabaseDescriptor(database, false); err != nil {
		return err
	}
	for _, name := range []string{"journal.sqlite3-wal", "journal.sqlite3-shm"} {
		var stat unix.Stat_t
		err := unix.Fstatat(int(dir.Fd()), name, &stat, unix.AT_SYMLINK_NOFOLLOW)
		if errors.Is(err, unix.ENOENT) {
			continue
		}
		if err != nil {
			return fmt.Errorf("journal: inspect %s safely: %w", name, err)
		}
		if stat.Mode&unix.S_IFMT != unix.S_IFREG {
			return fmt.Errorf("journal: %s is not a regular file", name)
		}
		if !ownerIsCurrent(&stat) {
			return fmt.Errorf("journal: %s is not owned by current euid", name)
		}
		if stat.Mode&0777 != 0600 {
			return fmt.Errorf("journal: %s is not mode 0600", name)
		}
	}
	return nil
}

func validateOpenFiles(statePath string, stateIdentity fileIdentity, dir *os.File, databasePath string, databaseIdentity fileIdentity, database *os.File) error {
	if _, err := verifyDirectoryDescriptor(dir, 0700, false); err != nil {
		return err
	}
	if _, err := verifyDatabaseDescriptor(database, false); err != nil {
		return err
	}
	if err := verifyPathIdentity(statePath, stateIdentity, true); err != nil {
		return fmt.Errorf("journal: state directory identity changed: %w", err)
	}
	if err := verifyPathIdentity(databasePath, databaseIdentity, false); err != nil {
		return fmt.Errorf("journal: database identity changed: %w", err)
	}
	return nil
}
