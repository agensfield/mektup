// Package logging provides local-only bounded logs.
//
// The normal Log API accepts only event metadata. It has no body or raw
// payload parameter, and rejects metadata keys that conventionally carry
// bodies. Secret-looking values are redacted before serialization. An Audit
// call is the explicit exception: it creates a visibly sensitive, owner-only
// per-invocation sink for bounded forensic capture.
package logging

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
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	// DefaultMaxBytes is the maximum size of one normal log file.
	DefaultMaxBytes int64 = 1 << 20
	// DefaultMaxFiles includes the current file and all rotated files.
	DefaultMaxFiles = 5
	// DefaultRetention is the age after which old normal logs are removed.
	DefaultRetention = 24 * time.Hour
	// DefaultAuditMaxBytes bounds one explicit audit sink.
	DefaultAuditMaxBytes int64 = 4 << 20
)

var (
	ErrBodyField = errors.New("logging: body and raw payload metadata are not accepted")
	ErrAuditID   = errors.New("logging: invalid invocation id")
	ErrTooLarge  = errors.New("logging: entry exceeds its byte limit")
)

// Config sets local logging bounds. Zero values select the documented safe
// defaults. Dir is required and is made owner-private.
type Config struct {
	Dir           string
	MaxBytes      int64
	MaxFiles      int
	Retention     time.Duration
	AuditMaxBytes int64
}

// Metadata is the only shape accepted by the normal logging API. It is
// intentionally string-valued and cannot carry an io.Reader or raw bytes.
type Metadata = map[string]string

// DefaultConfig returns the default bounded local logger configuration.
func DefaultConfig(dir string) Config {
	return Config{Dir: dir, MaxBytes: DefaultMaxBytes, MaxFiles: DefaultMaxFiles,
		Retention: DefaultRetention, AuditMaxBytes: DefaultAuditMaxBytes}
}

// Entry is the serialized shape of a normal metadata log record.
type Entry struct {
	Time   time.Time         `json:"time"`
	Event  string            `json:"event"`
	Fields map[string]string `json:"fields,omitempty"`
}

// AuditOptions opts into a sensitive, per-invocation raw sink. Body is the
// only normal-path escape hatch for raw bytes, and callers must explicitly
// invoke Audit to obtain it.
type AuditOptions struct {
	InvocationID string
	Body         io.Reader
	MaxBytes     int64
}

// ModeMetadata makes the sensitive nature of an audit receipt visible to
// callers and downstream presentation layers.
type ModeMetadata struct {
	Mode              string `json:"mode"`
	Sensitive         bool   `json:"sensitive"`
	NetworkTelemetry  bool   `json:"networkTelemetry"`
	RetentionEligible bool   `json:"retentionEligible"`
}

// AuditReceipt describes a complete audit sink.
type AuditReceipt struct {
	Path      string       `json:"path"`
	Bytes     int64        `json:"bytes"`
	SHA256    string       `json:"sha256"`
	Complete  bool         `json:"complete"`
	Mode      ModeMetadata `json:"mode"`
	CreatedAt time.Time    `json:"createdAt"`
}

// Logger is safe for concurrent use by local callers.
type Logger struct {
	mu      sync.Mutex
	config  Config
	logPath string
}

// New creates a local logger. It never starts a telemetry client or opens a
// network connection.
func New(config Config) (*Logger, error) {
	if config.Dir == "" {
		return nil, fmt.Errorf("logging: directory is required")
	}
	config.Dir = filepath.Clean(config.Dir)
	if config.MaxBytes == 0 {
		config.MaxBytes = DefaultMaxBytes
	}
	if config.MaxFiles == 0 {
		config.MaxFiles = DefaultMaxFiles
	}
	if config.Retention == 0 {
		config.Retention = DefaultRetention
	}
	if config.AuditMaxBytes == 0 {
		config.AuditMaxBytes = DefaultAuditMaxBytes
	}
	if config.MaxBytes < 1 || config.MaxFiles < 1 || config.Retention < 0 || config.AuditMaxBytes < 1 {
		return nil, fmt.Errorf("logging: bounds must be positive")
	}
	if err := ensurePrivateDir(config.Dir); err != nil {
		return nil, err
	}
	if err := ensurePrivateDir(filepath.Join(config.Dir, "audit")); err != nil {
		return nil, err
	}
	return &Logger{config: config, logPath: filepath.Join(config.Dir, "mektup.log")}, nil
}

// NewLogger is an explicit alias for callers that prefer constructor names.
func NewLogger(config Config) (*Logger, error) { return New(config) }

// Config returns the normalized bounds, useful for presenting mode metadata.
func (l *Logger) Config() Config { return l.config }

// Log writes one metadata-only redacted record. The fields map is copied and
// redacted; callers may safely reuse or mutate their map after return.
func (l *Logger) Log(event string, fields map[string]string) error {
	if l == nil {
		return fmt.Errorf("logging: nil logger")
	}
	if event == "" {
		return fmt.Errorf("logging: event is required")
	}
	clean := make(map[string]string, len(fields))
	for key, value := range fields {
		if bodyField(key) {
			return ErrBodyField
		}
		clean[key] = redact(key, value)
	}
	entry, err := json.Marshal(Entry{Time: time.Now().UTC(), Event: redact("event", event), Fields: clean})
	if err != nil {
		return fmt.Errorf("logging: marshal entry: %w", err)
	}
	entry = append(entry, '\n')
	if int64(len(entry)) > l.config.MaxBytes {
		return ErrTooLarge
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if err := checkLogTarget(l.logPath); err != nil {
		return err
	}
	if err := l.pruneLocked(time.Now()); err != nil {
		return err
	}
	if info, err := os.Stat(l.logPath); err == nil && info.Size()+int64(len(entry)) > l.config.MaxBytes {
		if err := l.rotateLocked(); err != nil {
			return err
		}
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("logging: inspect log: %w", err)
	}
	f, err := os.OpenFile(l.logPath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("logging: open log: %w", err)
	}
	defer f.Close()
	if err := f.Chmod(0o600); err != nil {
		return fmt.Errorf("logging: make log private: %w", err)
	}
	if _, err := f.Write(entry); err != nil {
		return fmt.Errorf("logging: append entry: %w", err)
	}
	return nil
}

// LogMetadata is a named alias that makes the no-body contract explicit.
func (l *Logger) LogMetadata(event string, fields map[string]string) error {
	return l.Log(event, fields)
}

// Audit explicitly writes bounded raw bytes to a private per-invocation sink.
// It refuses to truncate: an over-limit or failed source leaves no published
// audit file and Complete is never returned true.
func (l *Logger) Audit(ctx context.Context, opts AuditOptions) (AuditReceipt, error) {
	var zero AuditReceipt
	if l == nil || opts.Body == nil {
		return zero, fmt.Errorf("logging: audit body is required")
	}
	if !validInvocationID(opts.InvocationID) {
		return zero, ErrAuditID
	}
	max := opts.MaxBytes
	if max == 0 {
		max = l.config.AuditMaxBytes
	}
	if max < 1 {
		return zero, fmt.Errorf("logging: audit max bytes must be positive")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	auditDir := filepath.Join(l.config.Dir, "audit")
	if err := ensurePrivateDir(auditDir); err != nil {
		return zero, err
	}
	target := filepath.Join(auditDir, opts.InvocationID+".audit")
	if info, err := os.Lstat(target); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return zero, fmt.Errorf("logging: audit destination is a symlink")
		}
		return zero, os.ErrExist
	} else if !errors.Is(err, os.ErrNotExist) {
		return zero, fmt.Errorf("logging: inspect audit destination: %w", err)
	}
	tmp, err := os.CreateTemp(auditDir, ".audit-*")
	if err != nil {
		return zero, fmt.Errorf("logging: create audit temporary: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return zero, err
	}
	h := sha256.New()
	n, err := copyBounded(ctx, io.MultiWriter(tmp, h), opts.Body, max)
	if err != nil {
		tmp.Close()
		return zero, err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return zero, fmt.Errorf("logging: sync audit: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return zero, fmt.Errorf("logging: close audit: %w", err)
	}
	if _, err := os.Lstat(target); err == nil {
		return zero, os.ErrExist
	} else if !errors.Is(err, os.ErrNotExist) {
		return zero, fmt.Errorf("logging: inspect audit destination: %w", err)
	}
	if err := os.Link(tmpName, target); err != nil {
		if errors.Is(err, os.ErrExist) {
			return zero, os.ErrExist
		}
		return zero, fmt.Errorf("logging: publish audit: %w", err)
	}
	if err := os.Remove(tmpName); err != nil {
		return zero, fmt.Errorf("logging: remove audit temporary: %w", err)
	}
	return AuditReceipt{Path: target, Bytes: n, SHA256: "sha256:" + hex.EncodeToString(h.Sum(nil)), Complete: true,
		Mode:      ModeMetadata{Mode: "audit", Sensitive: true, NetworkTelemetry: false, RetentionEligible: false},
		CreatedAt: time.Now().UTC()}, nil
}

func (l *Logger) rotateLocked() error {
	if err := checkLogTarget(l.logPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if l.config.MaxFiles <= 1 {
		if err := os.Remove(l.logPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("logging: remove current log: %w", err)
		}
		return nil
	}
	oldest := fmt.Sprintf("%s.%d", l.logPath, l.config.MaxFiles-1)
	if err := os.Remove(oldest); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("logging: remove oldest log: %w", err)
	}
	for i := l.config.MaxFiles - 2; i >= 1; i-- {
		from, to := fmt.Sprintf("%s.%d", l.logPath, i), fmt.Sprintf("%s.%d", l.logPath, i+1)
		if err := os.Rename(from, to); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("logging: rotate log: %w", err)
		}
	}
	if err := os.Rename(l.logPath, l.logPath+".1"); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("logging: rotate current log: %w", err)
	}
	return nil
}

func (l *Logger) pruneLocked(now time.Time) error {
	entries, err := os.ReadDir(l.config.Dir)
	if err != nil {
		return fmt.Errorf("logging: list logs: %w", err)
	}
	for _, entry := range entries {
		if entry.IsDir() || (entry.Name() != "mektup.log" && !strings.HasPrefix(entry.Name(), "mektup.log.")) {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("logging: inspect old log: %w", err)
		}
		if l.config.Retention > 0 && now.Sub(info.ModTime()) > l.config.Retention {
			if err := os.Remove(filepath.Join(l.config.Dir, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("logging: remove expired log: %w", err)
			}
		}
	}
	return nil
}

func validInvocationID(id string) bool {
	if id == "" || len(id) > 128 || id == "." || id == ".." || strings.ContainsAny(id, `/\\`) {
		return false
	}
	for _, r := range id {
		if r == 0 || r < 0x20 {
			return false
		}
	}
	return true
}

var secretPattern = regexp.MustCompile(`(?i)(bearer\s+|token\s*[:=]\s*|password\s*[:=]\s*|secret\s*[:=]\s*|api[_-]?key\s*[:=]\s*)[^\s,;]+`)

func bodyField(key string) bool {
	switch strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(key, "-", "_"), " ", "_")) {
	case "body", "raw", "raw_payload", "payload", "request_body", "response_body", "content", "data":
		return true
	default:
		return false
	}
}

func redact(key, value string) string {
	lower := strings.ToLower(strings.ReplaceAll(strings.ReplaceAll(key, "-", "_"), " ", "_"))
	for _, marker := range []string{"password", "passwd", "token", "secret", "api_key", "apikey", "authorization", "cookie", "private_key", "client_secret"} {
		if strings.Contains(lower, marker) {
			return "[REDACTED]"
		}
	}
	if strings.HasPrefix(strings.TrimSpace(value), "Bearer ") || strings.HasPrefix(strings.TrimSpace(value), "eyJ") {
		return "[REDACTED]"
	}
	return secretPattern.ReplaceAllString(value, "[REDACTED]")
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
				return total, fmt.Errorf("logging: write audit: %w", err)
			}
			total += int64(n)
		}
		if readErr != nil {
			if errors.Is(readErr, io.EOF) {
				return total, nil
			}
			return total, fmt.Errorf("logging: read audit: %w", readErr)
		}
	}
}

func ensurePrivateDir(dir string) error {
	if info, err := os.Lstat(dir); err == nil {
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("logging: managed directory is a symlink")
		}
		if !info.IsDir() {
			return fmt.Errorf("logging: managed path is not a directory")
		}
	} else if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return fmt.Errorf("logging: create directory: %w", err)
		}
	} else {
		return fmt.Errorf("logging: inspect directory: %w", err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return fmt.Errorf("logging: make directory private: %w", err)
	}
	return nil
}

func checkLogTarget(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("logging: inspect log destination: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("logging: log destination is a symlink")
	}
	if info.IsDir() {
		return fmt.Errorf("logging: log destination is a directory")
	}
	return nil
}

// SortedFields returns a stable key order for presentation without exposing
// log internals.
func SortedFields(fields map[string]string) []string {
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
