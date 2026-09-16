//go:build !darwin && !linux

package journal

import (
	"fmt"
	"os"
	"path/filepath"
)

type fileIdentity struct{ value string }

func identityFromInfo(info os.FileInfo) (fileIdentity, error) {
	return fileIdentity{value: fmt.Sprintf("%d:%d:%d", info.Size(), info.ModTime().UnixNano(), info.Mode())}, nil
}
func sameIdentity(a, b fileIdentity) bool { return a == b }

func acquireStateDirectory(path string, create bool) (*os.File, fileIdentity, error) {
	if create {
		if err := os.MkdirAll(path, 0700); err != nil {
			return nil, fileIdentity{}, err
		}
	}
	file, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		return nil, fileIdentity{}, err
	}
	info, err := file.Stat()
	if err != nil || !info.IsDir() {
		_ = file.Close()
		return nil, fileIdentity{}, fmt.Errorf("journal: state directory unavailable")
	}
	identity, _ := identityFromInfo(info)
	return file, identity, nil
}

func acquireDatabase(_ *os.File, path string, create bool) (*os.File, fileIdentity, error) {
	flags := os.O_RDWR
	if create {
		flags |= os.O_CREATE
	}
	file, err := os.OpenFile(path, flags, 0600)
	if err != nil {
		return nil, fileIdentity{}, err
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() {
		_ = file.Close()
		return nil, fileIdentity{}, fmt.Errorf("journal: database unavailable")
	}
	identity, _ := identityFromInfo(info)
	return file, identity, nil
}

func verifyPathIdentity(path string, expected fileIdentity, directory bool) error {
	info, err := os.Stat(filepath.Clean(path))
	if err != nil {
		return err
	}
	if directory != info.IsDir() || !directory && !info.Mode().IsRegular() {
		return fmt.Errorf("journal: pathname has unexpected type")
	}
	actual, _ := identityFromInfo(info)
	if !sameIdentity(expected, actual) {
		return fmt.Errorf("journal: pathname identity mismatch")
	}
	return nil
}

func secureDatabaseFiles(_ *os.File, database *os.File) error {
	info, err := database.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0600 {
		return fmt.Errorf("journal: database is not an owner-private regular file")
	}
	return nil
}

func validateOpenFiles(statePath string, stateIdentity fileIdentity, dir *os.File, databasePath string, databaseIdentity fileIdentity, database *os.File) error {
	dirInfo, err := dir.Stat()
	if err != nil || !dirInfo.IsDir() || dirInfo.Mode().Perm() != 0700 {
		return fmt.Errorf("journal: state directory is not owner-private")
	}
	databaseInfo, err := database.Stat()
	if err != nil || !databaseInfo.Mode().IsRegular() || databaseInfo.Mode().Perm() != 0600 {
		return fmt.Errorf("journal: database is not owner-private")
	}
	if err := verifyPathIdentity(statePath, stateIdentity, true); err != nil {
		return fmt.Errorf("journal: state directory identity changed: %w", err)
	}
	if err := verifyPathIdentity(databasePath, databaseIdentity, false); err != nil {
		return fmt.Errorf("journal: database identity changed: %w", err)
	}
	return nil
}
