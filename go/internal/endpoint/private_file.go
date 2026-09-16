package endpoint

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
)

const maxEndpointDocumentBytes int64 = 1 << 20

func readBoundedEndpointFile(file *os.File) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(file, maxEndpointDocumentBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > maxEndpointDocumentBytes {
		return nil, fmt.Errorf("%w: endpoint document exceeds %d bytes", ErrConfigCorrupt, maxEndpointDocumentBytes)
	}
	return data, nil
}

func validateEndpointDocumentSize(size int) error {
	if int64(size) > maxEndpointDocumentBytes {
		return fmt.Errorf("%w: endpoint document exceeds %d bytes", ErrConfigCorrupt, maxEndpointDocumentBytes)
	}
	return nil
}

// ensureEndpointPrivateDirectory protects the final directory component before
// save and lock operations. Parent-component containment is intentionally out
// of scope here; callers retain their existing exact-path behavior.
func ensureEndpointPrivateDirectory(path string) error {
	_, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return err
		}
	}
	return protectEndpointPrivateDirectory(path)
}
