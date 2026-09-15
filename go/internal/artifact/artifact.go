// Package artifact provides owner-private, local artifact storage.
//
// Artifact writes are complete-or-absent: a failed or over-sized write leaves
// no published destination. Receipts describe the bytes that were published;
// they are not a claim that an interrupted write is usable.
package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	// DefaultMaxBytes bounds one spill. A caller may provide a smaller limit.
	DefaultMaxBytes int64 = 16 << 20
	// DefaultMediaType is used when a caller does not know the media type.
	DefaultMediaType = "application/octet-stream"
)

var (
	ErrExists      = errors.New("artifact already exists")
	ErrPath        = errors.New("artifact path is outside the managed root")
	ErrTooLarge    = errors.New("artifact exceeds its byte limit")
	ErrIncomplete  = errors.New("artifact write is incomplete")
	ErrSymlink     = errors.New("symlinks are not allowed in managed artifact paths")
	ErrInvalidName = errors.New("invalid artifact name")
)

// Options controls a managed artifact write. Force only applies to explicit
// RPC output writes. Spill refuses to replace an existing destination.
type Options struct {
	MediaType               string
	RetentionEligible       bool
	SensitiveOutputPossible bool
	MaxBytes                int64
	Force                   bool
}

// RPCOutputOptions is named separately so call sites make the overwrite
// authority visible. It has the same fields as Options.
type RPCOutputOptions = Options

// Receipt is the durable description returned after a complete publication.
// Complete is always true for a successful Spill or WriteRPCOutput result.
type Receipt struct {
	Path                    string    `json:"path"`
	Bytes                   int64     `json:"bytes"`
	SHA256                  string    `json:"sha256"`
	MediaType               string    `json:"media_type"`
	Complete                bool      `json:"complete"`
	RetentionEligible       bool      `json:"retention_eligible"`
	SensitiveOutputPossible bool      `json:"sensitive_output_possible"`
	CreatedAt               time.Time `json:"created_at"`
}

// Store owns one private managed root. The root and all files created by this
// package use owner-only permissions.
type Store struct{ root string }

// NewStore creates or opens a managed owner-private root.
func NewStore(root string) (*Store, error) {
	if root == "" {
		return nil, ErrPath
	}
	root = filepath.Clean(root)
	if err := ensurePrivateDir(root); err != nil {
		return nil, err
	}
	return &Store{root: root}, nil
}

// Root returns the cleaned managed root.
func (s *Store) Root() string { return s.root }

// Spill atomically publishes a complete artifact. The source is streamed and
// hashed without buffering it in memory. A read error, cancellation, or byte
// limit violation removes the temporary file and publishes nothing.
func (s *Store) Spill(ctx context.Context, name string, src io.Reader, opts Options) (Receipt, error) {
	return s.write(ctx, name, src, opts, false)
}

// WriteRPCOutput publishes explicit caller-provided RPC output. Existing
// output is refused unless opts.Force is true. Replacement uses os.Rename in
// the same directory, which is atomic on the filesystems that support it.
func (s *Store) WriteRPCOutput(ctx context.Context, name string, src io.Reader, opts RPCOutputOptions) (Receipt, error) {
	return s.write(ctx, name, src, opts, true)
}

// RPCOutput is a concise alias for WriteRPCOutput.
func (s *Store) RPCOutput(ctx context.Context, name string, src io.Reader, opts RPCOutputOptions) (Receipt, error) {
	return s.WriteRPCOutput(ctx, name, src, opts)
}

// MarshalReceipt is useful to callers that need to emit the receipt without
// depending on filesystem details.
func MarshalReceipt(r Receipt) ([]byte, error) { return json.Marshal(r) }

func (s *Store) write(ctx context.Context, name string, src io.Reader, opts Options, rpc bool) (Receipt, error) {
	var zero Receipt
	if s == nil || s.root == "" || src == nil {
		return zero, fmt.Errorf("artifact: nil store or source")
	}
	if err := ensurePrivateDir(s.root); err != nil {
		return zero, err
	}
	parts, err := pathParts(name)
	if err != nil {
		return zero, err
	}
	if !rpc {
		opts.Force = false
	}
	max := opts.MaxBytes
	if max == 0 {
		max = DefaultMaxBytes
	}
	if max < 1 {
		return zero, fmt.Errorf("artifact: max bytes must be positive")
	}
	if opts.MediaType == "" {
		opts.MediaType = DefaultMediaType
	}

	parent := s.root
	for _, part := range parts[:len(parts)-1] {
		parent = filepath.Join(parent, part)
		if err := ensurePrivateDir(parent); err != nil {
			return zero, err
		}
	}
	target := filepath.Join(parent, parts[len(parts)-1])
	if err := checkTarget(target, opts.Force); err != nil {
		return zero, err
	}
	tmp, err := os.CreateTemp(parent, ".mektup-artifact-*")
	if err != nil {
		return zero, fmt.Errorf("artifact: create temporary file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return zero, fmt.Errorf("artifact: chmod temporary file: %w", err)
	}

	h := sha256.New()
	n, copyErr := copyBounded(ctx, io.MultiWriter(tmp, h), src, max)
	if copyErr != nil {
		tmp.Close()
		return zero, copyErr
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return zero, fmt.Errorf("artifact: sync temporary file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return zero, fmt.Errorf("artifact: close temporary file: %w", err)
	}
	// A second target check catches a destination appearing after the first
	// check. Link gives non-force writes no-clobber publication semantics.
	if err := checkTarget(target, opts.Force); err != nil {
		return zero, err
	}
	if opts.Force {
		if err := os.Rename(tmpName, target); err != nil {
			return zero, fmt.Errorf("artifact: publish replacement: %w", err)
		}
	} else if err := os.Link(tmpName, target); err != nil {
		if errors.Is(err, os.ErrExist) {
			return zero, ErrExists
		}
		return zero, fmt.Errorf("artifact: publish without replacement: %w", err)
	} else if err := os.Remove(tmpName); err != nil {
		return zero, fmt.Errorf("artifact: remove publication temporary: %w", err)
	}
	return Receipt{
		Path: target, Bytes: n, SHA256: hex.EncodeToString(h.Sum(nil)),
		MediaType: opts.MediaType, Complete: true,
		RetentionEligible:       opts.RetentionEligible,
		SensitiveOutputPossible: opts.SensitiveOutputPossible,
		CreatedAt:               time.Now().UTC(),
	}, nil
}

func copyBounded(ctx context.Context, dst io.Writer, src io.Reader, max int64) (int64, error) {
	buf := make([]byte, 32*1024)
	var total int64
	for {
		select {
		case <-ctx.Done():
			return total, ctx.Err()
		default:
		}
		n, readErr := src.Read(buf)
		if n > 0 {
			if int64(n) > max-total {
				return total, ErrTooLarge
			}
			if _, err := dst.Write(buf[:n]); err != nil {
				return total, fmt.Errorf("artifact: write temporary file: %w", err)
			}
			total += int64(n)
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return total, nil
			}
			return total, fmt.Errorf("artifact: read source: %w", readErr)
		}
	}
}

func pathParts(name string) ([]string, error) {
	if name == "" || filepath.IsAbs(name) || strings.ContainsRune(name, 0) || strings.Contains(name, `\`) {
		return nil, ErrPath
	}
	if filepath.Clean(name) != name {
		return nil, ErrPath
	}
	parts := strings.Split(name, string(filepath.Separator))
	if len(parts) == 0 {
		return nil, ErrInvalidName
	}
	for _, part := range parts {
		if part == ".." {
			return nil, ErrPath
		}
		if part == "" || part == "." || strings.HasPrefix(part, ".mektup-artifact-") {
			return nil, ErrInvalidName
		}
	}
	return parts, nil
}

func ensurePrivateDir(dir string) error {
	if info, err := os.Lstat(dir); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return ErrSymlink
		}
		if !info.IsDir() {
			return fmt.Errorf("artifact: %s is not a directory", dir)
		}
	} else if errors.Is(err, os.ErrNotExist) {
		if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("artifact: create managed directory: %w", err)
		}
		info, err := os.Lstat(dir)
		if err != nil {
			return fmt.Errorf("artifact: inspect managed directory: %w", err)
		}
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return ErrSymlink
		}
	} else {
		return fmt.Errorf("artifact: inspect managed directory: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return fmt.Errorf("artifact: make managed directory private: %w", err)
	}
	return nil
}

func checkTarget(target string, force bool) error {
	info, err := os.Lstat(target)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("artifact: inspect destination: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return ErrSymlink
	}
	if info.IsDir() {
		return fmt.Errorf("artifact: destination is a directory")
	}
	if !force {
		return ErrExists
	}
	return nil
}
