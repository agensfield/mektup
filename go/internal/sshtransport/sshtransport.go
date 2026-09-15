// Package sshtransport adapts the bounded system-SSH byte relay to the public
// appserver WebSocket transport. SSH remains a connection-lifetime relay: this
// package never invokes a shell, translates JSONL, or puts an RPC path/body in
// the child argv.
package sshtransport

import (
	"context"
	"errors"
	"fmt"
	"net"

	"github.com/agensfield/mektup/go/appserver"
	"github.com/agensfield/mektup/go/internal/endpoint"
	"github.com/agensfield/mektup/go/internal/sshproxy"
)

// ClientDialer opens one app-server client over a system SSH proxy. Config.Host
// may be left empty, in which case the route's SSH host is used. If both are
// present they must agree, so a route can never silently use another host.
// Proxy executable/version evidence belongs to connection.Options.Proxy and is
// deliberately not used as the app-server identity.
type ClientDialer struct {
	Config  sshproxy.Config
	Factory sshproxy.ProcessFactory
}

// NewClientDialer constructs the injected SSH client dialer used by
// connection.Connect. The factory is nil in production and non-nil in
// deterministic tests.
func NewClientDialer(config sshproxy.Config, factory sshproxy.ProcessFactory) ClientDialer {
	return ClientDialer{Config: config, Factory: factory}
}

// DialClient implements connection.ClientDialer without importing that
// package, keeping this transport reusable by callers that only need the
// appserver client.
func (d ClientDialer) DialClient(ctx context.Context, route endpoint.Route, options appserver.Options) (*appserver.Client, error) {
	if route.Kind != endpoint.RouteSSH {
		return nil, fmt.Errorf("SSH client dialer requires an SSH route")
	}
	if err := route.Validate(); err != nil {
		return nil, err
	}
	cfg := d.Config
	if cfg.Host == "" {
		cfg.Host = route.SSHHost
	} else if cfg.Host != route.SSHHost {
		return nil, fmt.Errorf("SSH proxy host %q does not match route host %q", cfg.Host, route.SSHHost)
	}
	return appserver.DialWebSocket(ctx, func(dialCtx context.Context, _, _ string) (net.Conn, error) {
		// dialCtx owns only HTTP Upgrade setup. The WebSocket dial guard closes
		// this raw connection if setup is canceled; after it succeeds, the SSH
		// child must be owned by the returned connection rather than the caller's
		// setup context.
		conn, err := sshproxy.Dial(context.WithoutCancel(dialCtx), cfg, d.Factory)
		if err != nil {
			return nil, err
		}
		if err := dialCtx.Err(); err != nil {
			_ = conn.Close()
			return nil, err
		}
		return &evidenceConn{Conn: conn}, nil
	}, options)
}

// evidenceConn preserves sshproxy's phase evidence through Gorilla's raw
// WebSocket writer. Once a pipe write has begun, callers must not turn a lost
// response into a safe retry, so only an explicit not-started failure maps to
// proven-before-write.
type evidenceConn struct{ net.Conn }

func (c *evidenceConn) Write(p []byte) (int, error) {
	n, err := c.Conn.Write(p)
	if err == nil {
		return n, nil
	}
	var failure *sshproxy.Failure
	if !errors.As(err, &failure) {
		return n, err
	}
	phase := appserver.WriteMayHaveWritten
	switch failure.Evidence {
	case sshproxy.WriteNotStarted:
		phase = appserver.WriteProvenBeforeWrite
	case sshproxy.WriteComplete:
		phase = appserver.WriteComplete
	}
	return n, &appserver.WriteFailure{Err: err, Phase: phase}
}
