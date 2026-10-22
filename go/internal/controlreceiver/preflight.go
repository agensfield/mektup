package controlreceiver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/agensfield/mektup/go/internal/endpoint"
	"github.com/agensfield/mektup/go/internal/sshproxy"
)

// PreflightOptions uses exclusively local configuration. No remote metadata
// can choose an executable, filesystem path, environment or runtime home.
type PreflightOptions struct {
	Registry      FileRegistry
	EndpointStore endpoint.EndpointStore
	CodexHome     string
	SSHConfig     sshproxy.Config
	SSHFactory    sshproxy.ProcessFactory
	Invoke        func(context.Context, endpoint.Route, sshproxy.PreflightDocument) error
}

type PreflightReceiver struct{ Options PreflightOptions }

func (r PreflightReceiver) defaults() (PreflightOptions, error) {
	opts := r.Options
	if opts.Registry.Path == "" {
		var err error
		opts.Registry, err = NewDefaultRegistry()
		if err != nil {
			return opts, err
		}
	}
	if opts.EndpointStore.StateHome == "" || opts.EndpointStore.ConfigPath == "" {
		defaults, err := DefaultEndpointStore()
		if err != nil {
			return opts, err
		}
		if opts.EndpointStore.StateHome == "" {
			opts.EndpointStore.StateHome = defaults.StateHome
		}
		if opts.EndpointStore.ConfigPath == "" {
			opts.EndpointStore.ConfigPath = defaults.ConfigPath
		}
	}
	if opts.Invoke == nil {
		opts.Invoke = func(ctx context.Context, route endpoint.Route, req sshproxy.PreflightDocument) error {
			cfg := opts.SSHConfig
			cfg.Host = route.SSHHost
			return sshproxy.InvokePreflight(ctx, cfg, req, opts.SSHFactory)
		}
	}
	return opts, nil
}

func RunPreflight(ctx context.Context, input io.Reader, output io.Writer, opts PreflightOptions) error {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	readDone := make(chan struct {
		data []byte
		err  error
	}, 1)
	go func() {
		data, err := io.ReadAll(io.LimitReader(input, sshproxy.PreflightLimit+1))
		readDone <- struct {
			data []byte
			err  error
		}{data, err}
	}()
	var data []byte
	var err error
	select {
	case result := <-readDone:
		data, err = result.data, result.err
	case <-ctx.Done():
		if closer, ok := input.(io.Closer); ok {
			_ = closer.Close()
		}
		return ctx.Err()
	}
	if err != nil {
		return err
	}
	req, err := sshproxy.DecodePreflight(data)
	if err != nil {
		return err
	}
	if req.Kind != "request" {
		return errors.New("preflight: expected request")
	}
	receiver := PreflightReceiver{Options: opts}
	result := req
	result.Kind = "result"
	if err := receiver.Check(ctx, req); err != nil {
		result.ErrorCode = "reply_route_unavailable"
		result.ErrorMessage = err.Error()
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return err
	}
	if int64(len(encoded)+1) > sshproxy.PreflightLimit {
		return errors.New("preflight: response exceeds bound")
	}
	n, err := output.Write(append(encoded, '\n'))
	if err == nil && n != len(encoded)+1 {
		return io.ErrShortWrite
	}
	return err
}

// Check validates a snapshot of return routing and registered custody. It does
// not claim acceptance, thread liveness, or future delivery success.
func (r PreflightReceiver) Check(ctx context.Context, req sshproxy.PreflightDocument) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	encoded, err := json.Marshal(req)
	if err != nil {
		return err
	}
	if _, err := sshproxy.DecodePreflight(encoded); err != nil {
		return err
	}
	opts, err := r.defaults()
	if err != nil {
		return err
	}
	local, err := opts.EndpointStore.ResolveExistingEndpoint("local", opts.CodexHome)
	if err != nil || local.ID != req.TargetEndpointID {
		return errors.New("requested endpoint is not this SSH session's default Codex runtime; custom remote runtime selection is unsupported")
	}
	if _, err := resolvePreflightEndpoint(opts.EndpointStore, local.ID, opts.CodexHome); err != nil {
		return fmt.Errorf("local runtime mapping is ambiguous: %w", err)
	}
	switch req.Mode {
	case "identity":
		return nil
	case "custody":
		return opts.Registry.CheckReadiness(ctx, req.Custody.EndpointID, req.Custody.StoreID)
	case "route":
		dest, err := resolvePreflightEndpoint(opts.EndpointStore, req.ReplyDestination.EndpointID, opts.CodexHome)
		if err != nil {
			return fmt.Errorf("return-body stable endpoint mapping unavailable or ambiguous: %w", err)
		}
		custody, err := resolvePreflightEndpoint(opts.EndpointStore, req.Custody.EndpointID, opts.CodexHome)
		if err != nil {
			return fmt.Errorf("custody stable endpoint mapping unavailable or ambiguous: %w", err)
		}
		// ResolveExistingEndpoint validates the entire config, including duplicate
		// stable IDs. Identity probes also reject a stale mapping to another host.
		identity := sshproxy.PreflightDocument{Schema: sshproxy.PreflightSchema, Kind: "request", RequestID: req.RequestID, Mode: "identity", TargetEndpointID: dest.ID}
		if dest.Route.Kind == endpoint.RouteSSH {
			if err := opts.Invoke(ctx, dest.Route, identity); err != nil {
				return fmt.Errorf("return-body endpoint preflight: %w", err)
			}
		} else if dest.ID != local.ID {
			return errors.New("return-body endpoint requires a custom local runtime; SSH preflight supports the default runtime only")
		}
		check := sshproxy.PreflightDocument{Schema: sshproxy.PreflightSchema, Kind: "request", RequestID: req.RequestID, Mode: "custody", TargetEndpointID: custody.ID, Custody: req.Custody}
		if custody.Route.Kind == endpoint.RouteSSH {
			return opts.Invoke(ctx, custody.Route, check)
		}
		if custody.ID != local.ID {
			return errors.New("custody endpoint is not the default local runtime")
		}
		return opts.Registry.CheckReadiness(ctx, custody.ID, req.Custody.StoreID)
	}
	return errors.New("preflight: invalid mode")
}

// A built-in identity can otherwise mask a configured route with the same ID.
// This readiness check rejects that ambiguity instead of choosing a route.
func resolvePreflightEndpoint(store endpoint.EndpointStore, id, home string) (endpoint.Endpoint, error) {
	cfg, err := store.Load()
	if err != nil {
		return endpoint.Endpoint{}, err
	}
	resolved, err := store.ResolveExistingEndpoint(id, home)
	if err != nil {
		return endpoint.Endpoint{}, err
	}
	if resolved.Builtin {
		for _, item := range cfg.Endpoints {
			if item.ID == id {
				return endpoint.Endpoint{}, errors.New("stable endpoint has both configured and built-in mappings")
			}
		}
	}
	return resolved, nil
}
