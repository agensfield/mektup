package controlreceiver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	mektup "github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/internal/endpoint"
	"github.com/agensfield/mektup/go/internal/journal"
	"github.com/agensfield/mektup/go/internal/sshproxy"
)

func preflightSetup(t *testing.T) (PreflightOptions, endpoint.Endpoint, endpoint.Endpoint, *journal.Journal) {
	t.Helper()
	root := t.TempDir()
	home := filepath.Join(root, "codex")
	if err := os.Mkdir(home, 0700); err != nil {
		t.Fatal(err)
	}
	store := endpoint.NewStoreWithIdentityHome(filepath.Join(root, "endpoints.json"), filepath.Join(root, "state"), filepath.Join(root, "identity"))
	local, err := store.EnsureBuiltinLocal(home)
	if err != nil {
		t.Fatal(err)
	}
	remote := endpoint.Endpoint{ID: mektup.NewEndpointID(), Alias: "sender", Route: endpoint.Route{Kind: endpoint.RouteSSH, SSHHost: "sender-host"}, Herdr: endpoint.HerdrDisabled}
	if err := store.Save(endpoint.Config{Version: 1, Endpoints: []endpoint.Endpoint{remote}}); err != nil {
		t.Fatal(err)
	}
	j, err := journal.Open(context.Background(), journal.Options{StateDir: filepath.Join(root, "journal")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = j.Close() })
	registry := FileRegistry{Path: filepath.Join(root, "registry", "control-registry.json")}
	if err := registry.Register(context.Background(), local.ID, j); err != nil {
		t.Fatal(err)
	}
	return PreflightOptions{Registry: registry, EndpointStore: store, CodexHome: home}, local, remote, j
}
func preflightRequest(target, source, store string) sshproxy.PreflightDocument {
	return sshproxy.PreflightDocument{Schema: sshproxy.PreflightSchema, Kind: "request", RequestID: mektup.NewPrefixedID("probe_"), Mode: "route", TargetEndpointID: target, ReplyDestination: &sshproxy.DestinationRef{EndpointID: source, URI: "codex://" + source + "/thread/thread-1", ThreadID: "thread-1"}, Custody: &sshproxy.CustodyRef{EndpointID: source, StoreID: store}}
}
func TestPreflightChecksReverseIdentityAndCustody(t *testing.T) {
	opts, local, remote, j := preflightSetup(t)
	var modes []string
	opts.Invoke = func(_ context.Context, route endpoint.Route, req sshproxy.PreflightDocument) error {
		if route.SSHHost != "sender-host" || req.TargetEndpointID != remote.ID {
			t.Fatal("wrong reverse route")
		}
		modes = append(modes, req.Mode)
		return nil
	}
	req := preflightRequest(local.ID, remote.ID, j.StoreID())
	before, _ := os.ReadFile(opts.EndpointStore.ConfigPath)
	registryBefore, _ := os.ReadFile(opts.Registry.Path)
	if err := (PreflightReceiver{Options: opts}).Check(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if strings.Join(modes, ",") != "identity,custody" {
		t.Fatalf("modes=%v", modes)
	}
	after, _ := os.ReadFile(opts.EndpointStore.ConfigPath)
	registryAfter, _ := os.ReadFile(opts.Registry.Path)
	if !bytes.Equal(before, after) || !bytes.Equal(registryBefore, registryAfter) {
		t.Fatal("readiness changed configuration")
	}
}
func TestPreflightRejectsMissingAmbiguousAndWrongRuntime(t *testing.T) {
	for _, kind := range []string{"missing", "ambiguous", "wrong runtime", "wrong reverse identity"} {
		t.Run(kind, func(t *testing.T) {
			opts, local, remote, j := preflightSetup(t)
			req := preflightRequest(local.ID, remote.ID, j.StoreID())
			opts.Invoke = func(context.Context, endpoint.Route, sshproxy.PreflightDocument) error { return nil }
			switch kind {
			case "missing":
				req.ReplyDestination.EndpointID = mektup.NewEndpointID()
				req.ReplyDestination.URI = "codex://" + req.ReplyDestination.EndpointID + "/thread/thread-1"
			case "ambiguous":
				cfg, _ := opts.EndpointStore.Load()
				cfg.Endpoints = append(cfg.Endpoints, cfg.Endpoints[0])
				cfg.Endpoints[1].Alias = "duplicate"
				data, _ := json.Marshal(cfg)
				if err := os.WriteFile(opts.EndpointStore.ConfigPath, data, 0600); err != nil {
					t.Fatal(err)
				}
			case "wrong runtime":
				req.TargetEndpointID = mektup.NewEndpointID()
			case "wrong reverse identity":
				opts.Invoke = func(context.Context, endpoint.Route, sshproxy.PreflightDocument) error {
					return errors.New("wrong remote identity")
				}
			}
			if err := (PreflightReceiver{Options: opts}).Check(context.Background(), req); err == nil {
				t.Fatal("invalid readiness accepted")
			}
		})
	}
}
func TestReadOnlyCustodyReadinessChecksExactRegistryIdentity(t *testing.T) {
	opts, local, _, j := preflightSetup(t)
	if err := opts.Registry.CheckLocalJournal(context.Background(), local.ID, j.StoreID(), j); err != nil {
		t.Fatal(err)
	}
	storeID := j.StoreID()
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	databasePath := filepath.Join(j.StateDir(), "journal.sqlite3")
	before, _ := os.ReadFile(databasePath)
	registryBefore, _ := os.ReadFile(opts.Registry.Path)
	for _, test := range []struct {
		name, endpoint, store string
		wantErr               bool
	}{
		{"valid", local.ID, storeID, false},
		{"wrong endpoint", mektup.NewEndpointID(), storeID, true},
		{"wrong store", local.ID, mektup.NewStoreID(), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := opts.Registry.CheckReadiness(context.Background(), test.endpoint, test.store); (err != nil) != test.wantErr {
				t.Fatalf("err=%v", err)
			}
		})
	}
	after, _ := os.ReadFile(databasePath)
	registryAfter, _ := os.ReadFile(opts.Registry.Path)
	if !bytes.Equal(before, after) || !bytes.Equal(registryBefore, registryAfter) {
		t.Fatal("readiness changed durable files")
	}
}
func TestPreflightBoundedInputAndCancellation(t *testing.T) {
	if err := RunPreflight(context.Background(), strings.NewReader(strings.Repeat("x", int(sshproxy.PreflightLimit)+1)), io.Discard, PreflightOptions{}); err == nil {
		t.Fatal("oversized metadata accepted")
	}
	reader, writer := io.Pipe()
	defer writer.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := RunPreflight(ctx, reader, io.Discard, PreflightOptions{}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancel=%v", err)
	}
}

func snapshotPreflightDir(t *testing.T, dir string) map[string]string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		out[entry.Name()] = string(b)
	}
	return out
}
func TestCustodyPreflightDoesNotChangeClosedOrLiveJournal(t *testing.T) {
	for _, live := range []bool{false, true} {
		t.Run(map[bool]string{false: "closed", true: "active"}[live], func(t *testing.T) {
			opts, local, _, j := preflightSetup(t)
			storeID := j.StoreID()
			if !live {
				if err := j.Close(); err != nil {
					t.Fatal(err)
				}
			}
			before := snapshotPreflightDir(t, j.StateDir())
			if err := opts.Registry.CheckReadiness(context.Background(), local.ID, storeID); err != nil {
				t.Fatal(err)
			}
			after := snapshotPreflightDir(t, j.StateDir())
			if !reflect.DeepEqual(before, after) {
				t.Fatal("preflight changed journal files or directory entries")
			}
		})
	}
}

func TestCustodyReadinessRejectsUnsafeOrMissingFilesWithoutRepair(t *testing.T) {
	for _, kind := range []string{"missing database", "wrong header", "unsafe sidecar", "sidecar symlink"} {
		t.Run(kind, func(t *testing.T) {
			opts, local, _, j := preflightSetup(t)
			storeID := j.StoreID()
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			database := filepath.Join(j.StateDir(), "journal.sqlite3")
			switch kind {
			case "missing database":
				if err := os.Remove(database); err != nil {
					t.Fatal(err)
				}
			case "wrong header":
				if err := os.WriteFile(database, []byte(strings.Repeat("x", 100)), 0600); err != nil {
					t.Fatal(err)
				}
			case "unsafe sidecar":
				if err := os.WriteFile(database+"-wal", nil, 0644); err != nil {
					t.Fatal(err)
				}
			case "sidecar symlink":
				if err := os.Symlink(database, database+"-wal"); err != nil {
					t.Fatal(err)
				}
			}
			before := snapshotPreflightDir(t, j.StateDir())
			if err := opts.Registry.CheckReadiness(context.Background(), local.ID, storeID); err == nil {
				t.Fatal("unsafe state accepted")
			}
			after := snapshotPreflightDir(t, j.StateDir())
			if !reflect.DeepEqual(before, after) {
				t.Fatal("readiness repaired or created state")
			}
		})
	}
}
