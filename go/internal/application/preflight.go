package application

import (
	"context"
	"fmt"
	"time"

	mektup "github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/internal/cli"
	"github.com/agensfield/mektup/go/internal/controlreceiver"
	"github.com/agensfield/mektup/go/internal/endpoint"
	"github.com/agensfield/mektup/go/internal/journal"
	"github.com/agensfield/mektup/go/internal/service"
	"github.com/agensfield/mektup/go/internal/sshproxy"
)

func (e *Environment) replyPreflight(store endpoint.EndpointStore, codexHome string, j *journal.Journal) func(context.Context, service.ResolvedTarget, service.SourceIdentity) error {
	return func(ctx context.Context, target service.ResolvedTarget, source service.SourceIdentity) error {
		ep, err := store.ResolveExistingEndpoint(target.EndpointID, codexHome)
		if err != nil {
			return preflightError(err)
		}
		if ep.Route.Kind != endpoint.RouteSSH {
			return nil
		}
		// Registration happens in PrepareCustody before the service is called.
		// Here it is checked, never created or repaired.
		registry := e.options.Registry
		if registry.Path == "" {
			registry, err = controlreceiver.NewDefaultRegistry()
			if err != nil {
				return preflightError(err)
			}
		}
		custody, err := store.ResolveExistingEndpoint(source.CustodyEndpointID, codexHome)
		if err != nil || custody.Route.Kind != endpoint.RouteUnix {
			return preflightError(fmt.Errorf("local custody endpoint is unavailable"))
		}
		if err := registry.CheckLocalJournal(ctx, source.CustodyEndpointID, source.CustodyStoreID, j); err != nil {
			return preflightError(err)
		}
		req := sshproxy.PreflightDocument{
			Schema: sshproxy.PreflightSchema, Kind: "request", RequestID: mektup.NewPrefixedID("probe_"), Mode: "route", TargetEndpointID: ep.ID,
			ReplyDestination: &sshproxy.DestinationRef{EndpointID: source.EndpointID, URI: source.URI, ThreadID: threadIDFromURI(source.URI)},
			Custody:          &sshproxy.CustodyRef{EndpointID: source.CustodyEndpointID, StoreID: source.CustodyStoreID},
		}
		// The complete forward and reverse preflight shares one bounded deadline.
		ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		if e.options.PreflightInvoker != nil {
			err = e.options.PreflightInvoker(ctx, ep.Route, req)
		} else {
			cfg := e.options.SSHConfig
			cfg.Host = ep.Route.SSHHost
			err = sshproxy.InvokePreflight(ctx, cfg, req, e.options.SSHFactory)
		}
		if err != nil {
			return preflightError(err)
		}
		return nil
	}
}

func preflightError(err error) error {
	return &cli.Error{Code: "reply_route_unavailable", Message: "reply route preflight failed before dispatch", Effect: "not_sent", Exit: cli.ExitRejected, Details: map[string]any{"cause": err.Error(), "hint": "Use matching Mektup peers with mektup/preflight/v1 support, exact reverse endpoint mappings, and default Codex runtimes; verify local custody registration."}}
}
