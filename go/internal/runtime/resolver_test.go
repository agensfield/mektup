package runtime

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/agensfield/mektup/go/internal/endpoint"
)

type resolverRunner func(context.Context, []string) ([]byte, error)

func (r resolverRunner) Run(ctx context.Context, argv []string) ([]byte, error) { return r(ctx, argv) }

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
	if !probed || got.EndpointID != testTargetEndpoint || got.ThreadID != "thread-1" || got.Loaded || !got.Persistent || got.URI != "codex://"+testTargetEndpoint+"/thread/thread-1" {
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

func TestResolverPinnedRejectsUnmappedOrContradictorySelectors(t *testing.T) {
	store := endpoint.NewStore(filepath.Join(t.TempDir(), "endpoints.json"), t.TempDir())
	ep := endpoint.Endpoint{ID: testTargetEndpoint, Alias: "receiver-local", Route: endpoint.Route{Kind: endpoint.RouteUnix, UnixSocket: "/tmp/target.sock"}, Herdr: endpoint.HerdrDisabled}
	if err := store.Add(ep); err != nil {
		t.Fatal(err)
	}
	for _, host := range []string{"sender-local", "ep_0198f0e0-0000-7000-8000-000000000099"} {
		calls := 0
		resolver := ResolverAdapter{Store: store, StateProbe: func(context.Context, string, string) (bool, bool, error) { calls++; return true, true, nil }}
		if _, err := resolver.ResolvePinned(context.Background(), ep.ID, "codex://"+host+"/thread/thread-1"); err == nil || calls != 0 {
			t.Fatalf("selector %q accepted/reached probe: err=%v calls=%d", host, err, calls)
		}
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

func TestResolverSourceAddsVerifiedHerdrPaneProvenance(t *testing.T) {
	root := t.TempDir()
	home := filepath.Join(root, "codex")
	store := endpoint.NewStore(filepath.Join(root, "endpoints.json"), filepath.Join(root, "state"))
	store.IdentityHome = filepath.Join(root, "identity")
	local, err := store.EnsureBuiltinLocal(home)
	if err != nil {
		t.Fatal(err)
	}
	item := `{"agent":"codex","name":"mektup-lead","pane_id":"w3:p29","workspace_id":"w3","tab_id":"w3:t17","agent_status":"working","agent_session":{"agent":"codex","kind":"id","source":"herdr:codex","value":"thread-source"}}`
	responses := [][]byte{
		[]byte(`{"agents":[` + item + `]}`),
		[]byte(`{"agent":` + item + `}`),
		[]byte(`{"agents":[` + item + `]}`),
	}
	herdr := endpoint.NewHerdrResolver(resolverRunner(func(context.Context, []string) ([]byte, error) {
		if len(responses) == 0 {
			return nil, errors.New("unexpected Herdr request")
		}
		out := responses[0]
		responses = responses[1:]
		return out, nil
	}))
	got, err := (ResolverAdapter{Store: store, CodexHome: home, CurrentThreadID: "thread-source", Herdr: herdr}).ResolveSource(context.Background(), "")
	if err != nil {
		t.Fatal(err)
	}
	want := "herdr://" + local.ID + "/pane/w3:p29"
	if got.EndpointID != local.ID || got.URI != "codex://"+local.ID+"/thread/thread-source" || got.Herdr != want {
		t.Fatalf("source identity = %#v, want Herdr %q", got, want)
	}

	empty := endpoint.NewHerdrResolver(resolverRunner(func(context.Context, []string) ([]byte, error) { return []byte(`{"agents":[]}`), nil }))
	got, err = (ResolverAdapter{Store: store, CodexHome: home, CurrentThreadID: "thread-source", Herdr: empty}).ResolveSource(context.Background(), "")
	if err != nil || got.Herdr != "" {
		t.Fatalf("optional missing provenance = %#v, %v", got, err)
	}
}
