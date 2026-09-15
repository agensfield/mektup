package endpoint

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

type RouteKind string

const (
	RouteUnix RouteKind = "unix"
	RouteSSH  RouteKind = "ssh"
)

var (
	ErrInvalidRoute = errors.New("invalid endpoint route")
	ErrSSHHost      = errors.New("invalid OpenSSH host")
)

// Route is the local, non-portable connection description.  It is intentionally
// separate from PortableEndpoint because a socket path is installation-local.
type Route struct {
	Kind       RouteKind `json:"kind"`
	UnixSocket string    `json:"unixSocket,omitempty"`
	SSHHost    string    `json:"sshHost,omitempty"`
}

func UnixRoute(socket string) (Route, error) {
	if socket == "" || strings.ContainsRune(socket, '\x00') {
		return Route{}, fmt.Errorf("%w: Unix socket is empty", ErrInvalidRoute)
	}
	return Route{Kind: RouteUnix, UnixSocket: filepath.Clean(socket)}, nil
}

func SSHRoute(host string) (Route, error) {
	if host == "" || strings.ContainsAny(host, "\x00\r\n\t ") || strings.HasPrefix(host, "-") {
		return Route{}, fmt.Errorf("%w: %q", ErrSSHHost, host)
	}
	return Route{Kind: RouteSSH, SSHHost: host}, nil
}

func (r Route) Validate() error {
	switch r.Kind {
	case RouteUnix:
		if r.UnixSocket == "" || strings.ContainsRune(r.UnixSocket, '\x00') {
			return fmt.Errorf("%w: Unix socket is empty", ErrInvalidRoute)
		}
		if r.SSHHost != "" {
			return fmt.Errorf("%w: Unix route has SSH host", ErrInvalidRoute)
		}
	case RouteSSH:
		if _, err := SSHRoute(r.SSHHost); err != nil {
			return err
		}
		if r.UnixSocket != "" {
			return fmt.Errorf("%w: SSH route has Unix socket", ErrInvalidRoute)
		}
	default:
		return fmt.Errorf("%w: unknown route kind %q", ErrInvalidRoute, r.Kind)
	}
	return nil
}

// SSHArgv is the only command shape Mektup may use for the app-server proxy.
// It returns argv for exec.Command and never constructs a shell command.  No
// message body, local path, or user-supplied remote command can enter it.
func (r Route) SSHArgv() ([]string, error) {
	if r.Kind != RouteSSH {
		return nil, fmt.Errorf("%w: not an SSH route", ErrInvalidRoute)
	}
	if err := r.Validate(); err != nil {
		return nil, err
	}
	return []string{"ssh", "--", r.SSHHost, "codex", "app-server", "proxy"}, nil
}

// PortableEndpoint is safe to include in receipts and never exposes local
// socket paths, configuration paths, SSH policy, or executable details.
type PortableEndpoint struct {
	ID    string `json:"id"`
	Alias string `json:"alias,omitempty"`
	Kind  string `json:"kind"`
}

type Endpoint struct {
	ID      string    `json:"id"`
	Alias   string    `json:"alias"`
	Route   Route     `json:"route"`
	Builtin bool      `json:"builtin,omitempty"`
	Herdr   HerdrMode `json:"herdr,omitempty"`
}

type HerdrMode string

const (
	HerdrAuto     HerdrMode = "auto"
	HerdrDisabled HerdrMode = "disabled"
)

func (e Endpoint) Validate() error {
	if err := e.Route.Validate(); err != nil {
		return err
	}
	if strings.TrimSpace(e.ID) == "" || strings.ContainsAny(e.ID, "\r\n\t ") {
		return errors.New("endpoint ID must be a nonempty stable token")
	}
	if strings.TrimSpace(e.Alias) == "" || strings.ContainsAny(e.Alias, "/?#\r\n\t ") {
		return errors.New("endpoint alias must be a nonempty local token")
	}
	if e.Herdr == "" {
		e.Herdr = HerdrDisabled
	}
	if e.Herdr != HerdrAuto && e.Herdr != HerdrDisabled {
		return fmt.Errorf("unknown Herdr mode %q", e.Herdr)
	}
	return nil
}

func (e Endpoint) Portable() PortableEndpoint {
	kind := string(e.Route.Kind)
	return PortableEndpoint{ID: e.ID, Alias: e.Alias, Kind: kind}
}
