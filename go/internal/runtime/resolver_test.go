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
