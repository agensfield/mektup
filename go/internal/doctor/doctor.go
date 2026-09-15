// Package doctor provides bounded local diagnostics and explicitly receipted
// owner-private repairs. It has no daemon lifecycle or vacuum capability.
package doctor

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

type Severity string

const (
	SeverityOK      Severity = "ok"
	SeverityNotice  Severity = "notice"
	SeverityWarning Severity = "warning"
	SeverityError   Severity = "error"
)

// Paths are the local surfaces diagnosed by the default probes.
type Paths struct {
	StateDir   string
	ConfigDir  string
	ConfigFile string
	SocketPath string
}

// Finding is a bounded diagnostic. Fix is intentionally not serialized and
// can only be invoked by Run with Fix=true and SafeFix=true.
type Finding struct {
	ID       string   `json:"id"`
	Category string   `json:"category"`
	Severity Severity `json:"severity"`
	Message  string   `json:"message"`
	Path     string   `json:"path,omitempty"`
	Fixable  bool     `json:"fixable"`
	SafeFix  bool     `json:"safeFix"`
	Fix      FixFunc  `json:"-"`
}

type FixFunc func(context.Context) (string, error)

type RepairPlan struct {
	ID       string `json:"id"`
	Category string `json:"category"`
	Path     string `json:"path,omitempty"`
	Action   string `json:"action"`
}

type RepairReceipt struct {
	ID      string `json:"id"`
	Applied bool   `json:"applied"`
	Action  string `json:"action"`
	Detail  string `json:"detail,omitempty"`
	Error   string `json:"error,omitempty"`
}

type Report struct {
	ReadOnly  bool            `json:"readOnly"`
	Fix       bool            `json:"fix"`
	Findings  []Finding       `json:"findings"`
	Plan      []RepairPlan    `json:"plan"`
	Repairs   []RepairReceipt `json:"repairs"`
	CheckedAt time.Time       `json:"checkedAt"`
}

type Probe interface {
	Probe(context.Context) ([]Finding, error)
}

type ProbeFunc func(context.Context) ([]Finding, error)

func (f ProbeFunc) Probe(ctx context.Context) ([]Finding, error) { return f(ctx) }

// Options is injectable for tests and embedders. Supplying Probes replaces
// the default filesystem probes, so tests can prove no ambient state is read.
type Options struct {
	Paths  Paths
	Probes []Probe
	Now    func() time.Time
}

func DefaultOptions(paths Paths) Options { return Options{Paths: paths} }

// Run performs diagnostics. Without fix it is entirely read-only. With fix,
// only safe repairs returned by probes are applied, one by one, and every
// attempted action gets a receipt, including failures.
func Run(ctx context.Context, opts Options, fix bool) (Report, error) {
	if err := ValidatePaths(opts.Paths); err != nil {
		return Report{ReadOnly: !fix, Fix: fix}, err
	}
	now := time.Now().UTC()
	if opts.Now != nil {
		now = opts.Now().UTC()
	}
	probes := opts.Probes
	if len(probes) == 0 {
		probes = defaultProbes(opts.Paths)
	}
	report := Report{ReadOnly: !fix, Fix: fix, CheckedAt: now, Findings: []Finding{}, Plan: []RepairPlan{}, Repairs: []RepairReceipt{}}
	for _, probe := range probes {
		if probe == nil {
			continue
		}
		findings, err := probe.Probe(ctx)
		if err != nil {
			return report, err
		}
		report.Findings = append(report.Findings, findings...)
	}
	sort.SliceStable(report.Findings, func(i, j int) bool { return report.Findings[i].ID < report.Findings[j].ID })
	for _, finding := range report.Findings {
		if !finding.Fixable || !finding.SafeFix || finding.Fix == nil {
			continue
		}
		report.Plan = append(report.Plan, RepairPlan{ID: finding.ID, Category: finding.Category, Path: finding.Path, Action: finding.Message})
	}
	if !fix {
		return report, nil
	}
	for _, finding := range report.Findings {
		if !finding.Fixable || !finding.SafeFix || finding.Fix == nil {
			continue
		}
		receipt := RepairReceipt{ID: finding.ID, Applied: false, Action: finding.Message}
		detail, err := finding.Fix(ctx)
		if err != nil {
			receipt.Error = err.Error()
		} else {
			receipt.Applied = true
			receipt.Detail = detail
		}
		report.Repairs = append(report.Repairs, receipt)
	}
	return report, nil
}

func Check(ctx context.Context, opts Options) (Report, error) { return Run(ctx, opts, false) }

func defaultProbes(paths Paths) []Probe {
	return []Probe{
		ProbeFunc(func(ctx context.Context) ([]Finding, error) {
			return probeDir(ctx, "state.directory", "state", paths.StateDir)
		}),
		ProbeFunc(func(ctx context.Context) ([]Finding, error) {
			return probeFile(ctx, "state.database", "state", childPath(paths.StateDir, "journal.sqlite3"))
		}),
		ProbeFunc(func(ctx context.Context) ([]Finding, error) {
			return probeFile(ctx, "state.wal", "state", childPath(paths.StateDir, "journal.sqlite3-wal"))
		}),
		ProbeFunc(func(ctx context.Context) ([]Finding, error) {
			return probeFile(ctx, "state.shm", "state", childPath(paths.StateDir, "journal.sqlite3-shm"))
		}),
		ProbeFunc(func(ctx context.Context) ([]Finding, error) {
			return probeDir(ctx, "config.directory", "config", paths.ConfigDir)
		}),
		ProbeFunc(func(ctx context.Context) ([]Finding, error) {
			return probeFile(ctx, "config.file", "config", paths.ConfigFile)
		}),
		ProbeFunc(func(ctx context.Context) ([]Finding, error) { return probeSocket(ctx, paths.SocketPath) }),
		ProbeFunc(func(context.Context) ([]Finding, error) {
			return []Finding{{ID: "runtime.version", Category: "version", Severity: SeverityOK, Message: runtime.Version()}}, nil
		}),
	}
}

func childPath(parent, child string) string {
	if parent == "" {
		return ""
	}
	return filepath.Join(parent, child)
}

func probeDir(_ context.Context, id, category, path string) ([]Finding, error) {
	if path == "" {
		return []Finding{{ID: id, Category: category, Severity: SeverityNotice, Message: "not configured", Fixable: false}}, nil
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return []Finding{{ID: id, Category: category, Severity: SeverityWarning, Message: "directory is missing", Path: path, Fixable: true, SafeFix: true, Fix: func(context.Context) (string, error) {
			if err := os.MkdirAll(path, 0700); err != nil {
				return "", err
			}
			if err := os.Chmod(path, 0700); err != nil {
				return "", err
			}
			return "created owner-private directory", nil
		}}}, nil
	}
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return []Finding{{ID: id, Category: category, Severity: SeverityError, Message: "path is not a directory", Path: path}}, nil
	}
	if info.Mode()&0077 != 0 {
		return []Finding{{ID: id, Category: category, Severity: SeverityWarning, Message: fmt.Sprintf("directory mode %04o is not owner-private", info.Mode().Perm()), Path: path, Fixable: true, SafeFix: true, Fix: chmodFix(path, 0700)}}, nil
	}
	return []Finding{{ID: id, Category: category, Severity: SeverityOK, Message: "owner-private directory", Path: path}}, nil
}

func probeFile(_ context.Context, id, category, path string) ([]Finding, error) {
	if path == "" {
		return []Finding{{ID: id, Category: category, Severity: SeverityNotice, Message: "not configured", Fixable: false}}, nil
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return []Finding{{ID: id, Category: category, Severity: SeverityNotice, Message: "file is absent", Path: path}}, nil
	}
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return []Finding{{ID: id, Category: category, Severity: SeverityError, Message: "path is not a regular file", Path: path}}, nil
	}
	if info.Mode()&0077 != 0 {
		return []Finding{{ID: id, Category: category, Severity: SeverityWarning, Message: fmt.Sprintf("file mode %04o is not owner-private", info.Mode().Perm()), Path: path, Fixable: true, SafeFix: true, Fix: chmodFix(path, 0600)}}, nil
	}
	return []Finding{{ID: id, Category: category, Severity: SeverityOK, Message: "owner-private file", Path: path}}, nil
}

func probeSocket(_ context.Context, path string) ([]Finding, error) {
	if path == "" {
		return []Finding{{ID: "socket.path", Category: "socket", Severity: SeverityNotice, Message: "not configured"}}, nil
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return []Finding{{ID: "socket.path", Category: "socket", Severity: SeverityNotice, Message: "socket is absent", Path: path}}, nil
	}
	if err != nil {
		return nil, err
	}
	if info.Mode()&os.ModeSocket == 0 {
		return []Finding{{ID: "socket.path", Category: "socket", Severity: SeverityError, Message: "path is not a Unix socket", Path: path}}, nil
	}
	if info.Mode().Perm()&0077 != 0 {
		return []Finding{{ID: "socket.path", Category: "socket", Severity: SeverityWarning, Message: fmt.Sprintf("socket mode %04o is not owner-private", info.Mode().Perm()), Path: path, Fixable: true, SafeFix: true, Fix: chmodFix(path, 0600)}}, nil
	}
	// Dialing is deliberately not part of the default probe. A connection can
	// be observable by a daemon, so callers needing liveness inject a probe.
	return []Finding{{ID: "socket.path", Category: "socket", Severity: SeverityOK, Message: "Unix socket is present", Path: path}}, nil
}

func chmodFix(path string, mode os.FileMode) FixFunc {
	return func(context.Context) (string, error) {
		info, err := os.Lstat(path)
		if err != nil {
			return "", err
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("refusing symlink repair: %s", path)
		}
		if info.Mode()&os.ModeSocket != 0 {
			return "", fmt.Errorf("refusing socket permission repair without descriptor support: %s", path)
		}
		if err := safeChmod(path, mode); err != nil {
			return "", err
		}
		return fmt.Sprintf("mode set to %04o", mode.Perm()), nil
	}
}

// SocketProbe returns an injectable liveness probe. It is opt-in because a
// connect can have daemon-visible effects and doctor defaults are read-only.
func SocketProbe(path string, timeout time.Duration) Probe {
	return ProbeFunc(func(ctx context.Context) ([]Finding, error) {
		if path == "" {
			return nil, nil
		}
		if timeout <= 0 {
			timeout = 250 * time.Millisecond
		}
		dialer := net.Dialer{Timeout: timeout}
		conn, err := dialer.DialContext(ctx, "unix", path)
		if err != nil {
			return []Finding{{ID: "socket.liveness", Category: "socket", Severity: SeverityWarning, Message: err.Error(), Path: path}}, nil
		}
		_ = conn.Close()
		return []Finding{{ID: "socket.liveness", Category: "socket", Severity: SeverityOK, Message: "Unix socket accepted a connection", Path: path}}, nil
	})
}

// ValidatePaths provides a small helper for embedders that need a deterministic
// path policy before constructing Options.
func ValidatePaths(paths Paths) error {
	for _, p := range []string{paths.StateDir, paths.ConfigDir, paths.ConfigFile, paths.SocketPath} {
		if strings.ContainsRune(p, 0) {
			return fmt.Errorf("doctor: path contains NUL")
		}
		if p != "" && !filepath.IsAbs(p) {
			return fmt.Errorf("doctor: path must be absolute: %s", p)
		}
	}
	return nil
}
