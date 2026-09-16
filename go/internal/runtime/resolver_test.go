package runtime

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/agensfield/mektup/go/internal/endpoint"
)

func TestResolverPinsEndpointAndUsesExplicitRuntimeStateProbe(t *testing.T) {
	store := endpoint.NewStore(filepath.Join(t.TempDir(), "endpoints.json"), t.TempDir())
	ep := endpoint.Endpoint{ID: testTargetEndpoint, Alias: "target", Route: endpoint.Route{Kind: endpoint.RouteUnix, UnixSocket: "/tmp/target.sock"}, Herdr: endpoint.HerdrDisabled}
	if err := store.Add(ep); err != nil {
		t.Fatal(err)
	}
	probed := false
	resolver := ResolverAdapter{Store: store, StateProbe: func(_ context.Context, endpointID, threadID string) (bool, bool, error) {
		probed = true
		if endpointID != testTargetEndpoint || threadID != "thread-1" {
			t.Fatalf("state probe received unpinned identity: %q %q", endpointID, threadID)
		}
		return false, true, nil
	}}
	got, err := resolver.Resolve(context.Background(), "codex://target/thread/thread-1")
	if err != nil {
		t.Fatal(err)
	}
	if !probed || got.EndpointID != testTargetEndpoint || got.ThreadID != "thread-1" || got.Loaded || !got.Persistent || got.URI != "codex://target/thread/thread-1" {
		t.Fatalf("pinned target = %#v", got)
	}
}

func TestResolverPinnedUsesRuntimeStateProbe(t *testing.T) {
	store := endpoint.NewStore(filepath.Join(t.TempDir(), "endpoints.json"), t.TempDir())
	ep := endpoint.Endpoint{ID: testTargetEndpoint, Alias: "target", Route: endpoint.Route{Kind: endpoint.RouteUnix, UnixSocket: "/tmp/target.sock"}, Herdr: endpoint.HerdrDisabled}
	if err := store.Add(ep); err != nil {
		t.Fatal(err)
	}
	called := false
	resolver := ResolverAdapter{Store: store, StateProbe: func(_ context.Context, endpointID, threadID string) (bool, bool, error) {
		called = true
		if endpointID != testTargetEndpoint || threadID != "thread-1" {
			t.Fatalf("pinned probe identity = %q/%q", endpointID, threadID)
		}
		return false, true, nil
	}}
	got, err := resolver.ResolvePinned(context.Background(), testTargetEndpoint, "codex://target/thread/thread-1")
	if err != nil {
		t.Fatal(err)
	}
	if !called || got.Loaded || !got.Persistent {
		t.Fatalf("pinned runtime state = called:%v target:%#v", called, got)
	}
}

func TestResolverPinnedAcceptsForeignAliasAndCanonicalizesWireURI(t *testing.T) {
	store := endpoint.NewStore(filepath.Join(t.TempDir(), "endpoints.json"), t.TempDir())
	ep := endpoint.Endpoint{ID: testTargetEndpoint, Alias: "receiver-local", Route: endpoint.Route{Kind: endpoint.RouteUnix, UnixSocket: "/tmp/target.sock"}, Herdr: endpoint.HerdrDisabled}
	if err := store.Add(ep); err != nil {
		t.Fatal(err)
	}
	got, err := (ResolverAdapter{Store: store}).ResolvePinned(context.Background(), ep.ID, "codex://sender-local/thread/thread-1")
	if err != nil {
		t.Fatal(err)
	}
	want := "codex://" + ep.ID + "/thread/thread-1"
	if got.EndpointID != ep.ID || got.ThreadID != "thread-1" || got.URI != want {
		t.Fatalf("pinned foreign alias = %#v, want canonical URI %q", got, want)
	}
}

func TestResolverResolvesBuiltinTargetByStableID(t *testing.T) {
	root := t.TempDir()
	home := filepath.Join(root, "codex")
	store := endpoint.NewStore(filepath.Join(root, "endpoints.json"), filepath.Join(root, "state"))
	store.IdentityHome = filepath.Join(root, "identity")
	local, err := store.EnsureBuiltinLocal(home)
	if err != nil {
		t.Fatal(err)
	}
	got, err := (ResolverAdapter{Store: store, CodexHome: home}).Resolve(context.Background(), "codex://"+local.ID+"/thread/thread-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.EndpointID != local.ID || got.ThreadID != "thread-1" {
		t.Fatalf("builtin stable target=%+v", got)
	}
}
