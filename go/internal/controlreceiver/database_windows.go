//go:build windows

package controlreceiver

import (
	"fmt"
	"os"
)

func openExistingDatabase(path string) (*os.File, string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, "", fmt.Errorf("%w: registered journal database does not exist privately", ErrStoreUnavailable)
	}
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		_ = file.Close()
		return nil, "", fmt.Errorf("%w: registered journal database is not owner-private", ErrStoreUnavailable)
	}
	return file, databaseIdentity(info), nil
}

func existingDatabaseIdentity(path string) (string, error) {
	file, _, err := openExistingDatabase(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return "", err
	}
	return databaseIdentity(info), nil
}

func databaseIdentity(info os.FileInfo) string {
	return fmt.Sprintf("%d:%d:%d", info.Size(), info.ModTime().UnixNano(), info.Mode())
}
