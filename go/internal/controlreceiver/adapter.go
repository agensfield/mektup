package controlreceiver

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/agensfield/mektup/go/internal/endpoint"
	"github.com/agensfield/mektup/go/internal/journal"
)

// EndpointDestinationResolver validates a control destination against the
// persisted local EndpointStore without creating endpoint authority.
type EndpointDestinationResolver struct {
	Store     endpoint.EndpointStore
	CodexHome string
}

func (r EndpointDestinationResolver) ValidateDestination(_ context.Context, endpointID, uri, threadID string) error {
	target, err := endpoint.ParseTarget(uri)
	if err != nil || target.Kind != endpoint.TargetCodex || target.ThreadID != threadID {
		return ErrRelationshipMismatch
	}
	resolved, err := r.Store.ResolveExistingEndpoint(target.Endpoint, r.CodexHome)
	if err != nil || resolved.ID != endpointID {
		return ErrRelationshipMismatch
	}
	return nil
}

type CommandOptions struct {
	Registry        FileRegistry
	EndpointStore   endpoint.EndpointStore
	CodexHome       string
	LocalEndpointID string
	Destination     DestinationResolver
	MaxInput        int64
	MaxOutput       int64
}

type CommandAdapter struct{ receiver Receiver }

// RegisterLocalJournal is the local reply-request setup seam. It requires a
// persisted endpoint identity and a journal already opened/verified locally.
func RegisterLocalJournal(ctx context.Context, registry FileRegistry, endpoints endpoint.EndpointStore, codexHome, endpointID string, j *journal.Journal) error {
	resolved, err := endpoints.ResolveExistingEndpoint(endpointID, codexHome)
	if err != nil || resolved.ID != endpointID {
		return fmt.Errorf("control receiver: custody endpoint is not locally established: %w", ErrRegistryInvalid)
	}
	if j == nil {
		return fmt.Errorf("control receiver: invalid local journal: %w", ErrRegistryInvalid)
	}
	return registry.Register(ctx, endpointID, j)
}

func NewCommandAdapter(opts CommandOptions) (CommandAdapter, error) {
	registry := opts.Registry
	if registry.Path == "" {
		var err error
		registry, err = NewDefaultRegistry()
		if err != nil {
			return CommandAdapter{}, err
		}
	}
	store := opts.EndpointStore
	if store.ConfigPath == "" || store.StateHome == "" {
		defaults, err := DefaultEndpointStore()
		if err != nil {
			return CommandAdapter{}, err
		}
		if store.ConfigPath == "" {
			store.ConfigPath = defaults.ConfigPath
		}
		if store.StateHome == "" {
			store.StateHome = defaults.StateHome
		}
	}
	localID := opts.LocalEndpointID
	if localID == "" {
		local, err := store.ResolveExistingEndpoint("local", opts.CodexHome)
		if err != nil {
			return CommandAdapter{}, fmt.Errorf("control receiver: local endpoint unavailable: %w", err)
		}
		localID = local.ID
	}
	destination := opts.Destination
	if destination == nil {
		destination = EndpointDestinationResolver{Store: store, CodexHome: opts.CodexHome}
	}
	return CommandAdapter{receiver: Receiver{Registry: registry, LocalEndpointID: localID, Destination: destination, MaxInput: opts.MaxInput, MaxOutput: opts.MaxOutput}}, nil
}

func (a CommandAdapter) Serve(ctx context.Context, input io.Reader, output io.Writer) error {
	if a.receiver.Registry == nil || a.receiver.LocalEndpointID == "" || a.receiver.Destination == nil {
		return errors.New("control receiver: command adapter is not configured")
	}
	return a.receiver.Serve(ctx, input, output)
}

func RunControlReceive(ctx context.Context, input io.Reader, output io.Writer, opts CommandOptions) error {
	adapter, err := NewCommandAdapter(opts)
	if err != nil {
		return err
	}
	return adapter.Serve(ctx, input, output)
}

func DefaultEndpointStore() (endpoint.EndpointStore, error) {
	stateRoot, err := endpoint.DefaultStateRoot()
	if err != nil {
		return endpoint.EndpointStore{}, err
	}
	configRoot := strings.TrimSpace(os.Getenv("XDG_CONFIG_HOME"))
	if configRoot == "" {
		home, homeErr := os.UserHomeDir()
		if homeErr != nil {
			return endpoint.EndpointStore{}, homeErr
		}
		configRoot = filepath.Join(home, ".config")
	}
	if !filepath.IsAbs(configRoot) {
		configRoot, err = filepath.Abs(configRoot)
		if err != nil {
			return endpoint.EndpointStore{}, err
		}
	}
	return endpoint.NewStore(filepath.Join(configRoot, "mektup", "endpoints.json"), stateRoot), nil
}
