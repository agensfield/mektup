package endpoint

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestParseTargetAndPercentEncoding(t *testing.T) {
	tests := []struct {
		raw    string
		kind   TargetKind
		name   string
		pane   string
		thread string
	}{
		{"herdr://edge/agent/space%20name", TargetAgent, "space name", "", ""},
		{"herdr://edge/agent/a%2Fb%25c", TargetAgent, "a/b%c", "", ""},
		{"herdr://edge/pane/w3%3Ap17", TargetPane, "", "w3:p17", ""},
		{"codex://local/thread/01abc-def", TargetCodex, "", "", "01abc-def"},
		{"my-agent", TargetBare, "my-agent", "", ""},
	}
	for _, test := range tests {
		got, err := ParseTarget(test.raw)
		if err != nil {
			t.Fatalf("ParseTarget(%q): %v", test.raw, err)
		}
		if got.Kind != test.kind || got.Name != test.name || got.PaneID != test.pane || got.ThreadID != test.thread {
			t.Fatalf("ParseTarget(%q) = %#v", test.raw, got)
		}
	}
	for _, raw := range []string{"herdr://edge/pane/p17", "herdr://edge/pane/w3/p17", "codex://edge/agent/name", "herdr://edge/agent/"} {
		if _, err := ParseTarget(raw); !errors.Is(err, ErrInvalidTarget) {
			t.Errorf("ParseTarget(%q) error = %v, want ErrInvalidTarget", raw, err)
		}
	}
}

func TestConfigAtomicPrivateAndDuplicateRules(t *testing.T) {
	root := t.TempDir()
	store := NewStoreWithIdentityHome(filepath.Join(root, "config", "endpoints.json"), filepath.Join(root, "state"), filepath.Join(root, "state"))
	route, err := SSHRoute("ops@example;touch /tmp/pwned")
	if err == nil {
		// Semicolons are safe in argv but whitespace is deliberately rejected.
		t.Fatal("expected invalid host")
	}
	route, err = SSHRoute("ops@example")
	if err != nil {
		t.Fatal(err)
	}
	endpoint := Endpoint{Alias: "remote", Route: route, Herdr: HerdrDisabled}
	if err := store.Add(endpoint); err != nil {
		t.Fatal(err)
	}
	otherID, err := NewEndpointID()
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Add(Endpoint{Alias: "remote", ID: otherID, Route: route}); !errors.Is(err, ErrDuplicateAlias) {
		t.Fatalf("duplicate alias error = %v", err)
	}
	got, err := store.Show("remote")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID == "" || got.Route.SSHHost != "ops@example" {
		t.Fatalf("unexpected endpoint %#v", got)
	}
	if err := store.Remove("local"); !errors.Is(err, ErrBuiltinImmutable) {
		t.Fatalf("remove local error = %v", err)
	}
	info, err := os.Stat(store.ConfigPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("config mode = %04o", info.Mode().Perm())
	}
	dirInfo, err := os.Stat(filepath.Dir(store.ConfigPath))
	if err != nil {
		t.Fatal(err)
	}
	if dirInfo.Mode().Perm() != 0700 {
		t.Fatalf("config directory mode = %04o", dirInfo.Mode().Perm())
	}
	if err := os.WriteFile(store.ConfigPath, []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(); !errors.Is(err, ErrConfigCorrupt) {
		t.Fatalf("corrupt config error = %v", err)
	}
}

func TestBuiltinLocalStablePerCanonicalHome(t *testing.T) {
	root := t.TempDir()
	store := NewStoreWithIdentityHome(filepath.Join(root, "config.json"), filepath.Join(root, "state"), filepath.Join(root, "state"))
	first, err := store.EnsureBuiltinLocal(filepath.Join(root, "codex-a"))
	if err != nil {
		t.Fatal(err)
	}
	repeat, err := store.EnsureBuiltinLocal(filepath.Join(root, "codex-a"))
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.EnsureBuiltinLocal(filepath.Join(root, "codex-b"))
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != repeat.ID || first.ID == second.ID || first.Alias != "local" || second.Alias != "local" {
		t.Fatalf("built-in identity mismatch: first=%#v repeat=%#v second=%#v", first, repeat, second)
	}
	if filepath.Base(first.Route.UnixSocket) != "app-server-control.sock" {
		t.Fatalf("socket = %q", first.Route.UnixSocket)
	}
	if _, err := os.Stat(filepath.Join(root, "state", "endpoint-identities.json")); err != nil {
		t.Fatal(err)
	}
	items, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || !items[0].Builtin {
		t.Fatalf("list = %#v, want current built-in local", items)
	}
	if _, err := store.Show(first.ID); err != nil {
		t.Fatalf("show built-in stable ID: %v", err)
	}
	if err := store.Remove(first.ID); !errors.Is(err, ErrBuiltinImmutable) {
		t.Fatalf("remove built-in stable ID = %v", err)
	}
}

func TestEndpointIDsAreUUIDv7AndValidationIsStrict(t *testing.T) {
	id, err := NewEndpointID()
	if err != nil {
		t.Fatal(err)
	}
	if !validEndpointID(id) || id[17] != '7' {
		t.Fatalf("generated endpoint ID is not UUIDv7: %q", id)
	}
	route, _ := SSHRoute("example.org")
	if err := (Endpoint{ID: "ep_not-a-uuid", Alias: "x", Route: route, Herdr: HerdrDisabled}).Validate(); err == nil {
		t.Fatal("arbitrary endpoint ID accepted")
	}
}

func TestEndpointPrecedenceAndSourceIndependence(t *testing.T) {
	if got := SelectEndpoint("flag", "env", "config", "default"); got != "flag" {
		t.Fatal(got)
	}
	if got := SelectEndpoint("", "env", "config", "default"); got != "env" {
		t.Fatal(got)
	}
	root := t.TempDir()
	store := NewStoreWithIdentityHome(filepath.Join(root, "config.json"), filepath.Join(root, "state"), filepath.Join(root, "state"))
	route, _ := SSHRoute("example.org")
	if err := store.Add(Endpoint{Alias: "remote", Route: route}); err != nil {
		t.Fatal(err)
	}
	cfg, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	cfg.Default = "remote"
	if err := store.Save(cfg); err != nil {
		t.Fatal(err)
	}
	defaultEndpoint, err := store.ResolveEndpoint("", filepath.Join(root, "codex-a"))
	if err != nil || defaultEndpoint.Alias != "remote" {
		t.Fatalf("configured default = %#v, err=%v", defaultEndpoint, err)
	}
	source, err := store.ResolveSource(SourceOptions{CurrentThreadID: "thread-a", CodexHome: filepath.Join(root, "codex-a")})
	if err != nil {
		t.Fatal(err)
	}
	target, _ := ParseTarget("codex://remote/thread/thread-b")
	dest, err := store.ResolveDestination(context.Background(), target, "local", filepath.Join(root, "codex-a"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if source.Endpoint.ID == dest.Endpoint.ID || source.ThreadID == dest.ThreadID {
		t.Fatalf("source and destination were conflated: source=%#v dest=%#v", source, dest)
	}
	conflict, _ := ParseTarget("codex://local/thread/other-thread")
	if _, err := store.ResolveSource(SourceOptions{CurrentThreadID: "thread-a", ReplyTo: conflict.String(), CodexHome: filepath.Join(root, "codex-a")}); !errors.Is(err, ErrSourceConflict) {
		t.Fatalf("conflicting reply-to error = %v", err)
	}
}

func TestConcurrentEndpointAddsDoNotLoseUpdates(t *testing.T) {
	root := t.TempDir()
	store := NewStoreWithIdentityHome(filepath.Join(root, "config.json"), filepath.Join(root, "state"), filepath.Join(root, "state"))
	route, _ := SSHRoute("example.org")
	const total = 12
	var group sync.WaitGroup
	results := make(chan error, total)
	for i := 0; i < total; i++ {
		group.Add(1)
		go func(index int) {
			defer group.Done()
			results <- store.Add(Endpoint{Alias: "endpoint-" + string(rune('a'+index)), Route: route})
		}(i)
	}
	group.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	items, err := store.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != total+1 {
		t.Fatalf("concurrent list has %d endpoints, want %d: %#v", len(items), total+1, items)
	}
}

type fakeRunner struct {
	responses [][]byte
	argv      [][]string
}

func (f *fakeRunner) Run(_ context.Context, argv []string) ([]byte, error) {
	f.argv = append(f.argv, append([]string(nil), argv...))
	if len(f.responses) == 0 {
		return nil, errors.New("unexpected command")
	}
	out := f.responses[0]
	f.responses = f.responses[1:]
	return out, nil
}

func herdrJSON(agentName, pane, workspace, tab, thread, status string) []byte {
	item := map[string]any{
		"agent": "codex", "name": agentName, "pane_id": pane,
		"workspace_id": workspace, "tab_id": tab, "agent_status": status,
		"agent_session": map[string]any{"kind": "id", "source": "herdr:codex", "value": thread},
	}
	return mustJSON(map[string]any{"id": "x", "result": map[string]any{"agent": item, "agents": []any{item}}})
}

func mustJSON(value any) []byte {
	b, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return b
}

func TestHerdrResolverPinsIdentityAndRejectsChangedOccupant(t *testing.T) {
	list := mustJSON(map[string]any{"result": map[string]any{"agents": []any{
		map[string]any{"agent": "codex", "name": "agent name", "pane_id": "w3:p1", "workspace_id": "w3", "tab_id": "w3:t1", "agent_status": "idle", "agent_session": map[string]any{"kind": "id", "source": "herdr:codex", "value": "thread-one"}},
	}}})
	get := herdrJSON("agent name", "w3:p1", "w3", "w3:t1", "thread-one", "idle")
	runner := &fakeRunner{responses: [][]byte{list, get}}
	resolver := NewHerdrResolver(runner)
	target, _ := ParseTarget("herdr://local/agent/agent%20name")
	resolved, err := resolver.Resolve(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.ThreadID != "thread-one" || resolved.Requested.Name != "agent name" || resolved.Pane != "w3:p1" {
		t.Fatalf("resolution = %#v", resolved)
	}
	wantArgv := [][]string{{"herdr", "agent", "list"}, {"herdr", "agent", "get", "w3:p1"}}
	if !reflect.DeepEqual(runner.argv, wantArgv) {
		t.Fatalf("argv = %#v, want %#v", runner.argv, wantArgv)
	}
	changed := herdrJSON("other", "w3:p1", "w3", "w3:t1", "thread-two", "idle")
	runner = &fakeRunner{responses: [][]byte{list, changed}}
	_, err = NewHerdrResolver(runner).Resolve(context.Background(), target)
	if !errors.Is(err, ErrResolverStale) {
		t.Fatalf("changed occupant error = %v", err)
	}
}

func TestHerdrResolverFailsClosedOnZeroMultipleAndStale(t *testing.T) {
	empty := mustJSON(map[string]any{"result": map[string]any{"agents": []any{}}})
	target := Target{Kind: TargetAgent, Name: "x"}
	_, err := NewHerdrResolver(&fakeRunner{responses: [][]byte{empty}}).Resolve(context.Background(), target)
	if !errors.Is(err, ErrResolverNotFound) {
		t.Fatalf("zero error = %v", err)
	}
	duplicate := mustJSON(map[string]any{"result": map[string]any{"agents": []any{
		map[string]any{"agent": "codex", "name": "x", "pane_id": "w3:p1", "workspace_id": "w3", "tab_id": "w3:t1", "agent_status": "idle", "agent_session": map[string]any{"kind": "id", "source": "herdr:codex", "value": "a"}},
		map[string]any{"agent": "codex", "name": "x", "pane_id": "w3:p2", "workspace_id": "w3", "tab_id": "w3:t1", "agent_status": "idle", "agent_session": map[string]any{"kind": "id", "source": "herdr:codex", "value": "b"}},
	}}})
	_, err = NewHerdrResolver(&fakeRunner{responses: [][]byte{duplicate}}).Resolve(context.Background(), target)
	if !errors.Is(err, ErrResolverAmbiguous) {
		t.Fatalf("multiple error = %v", err)
	}
	stale := mustJSON(map[string]any{"result": map[string]any{"agents": []any{
		map[string]any{"agent": "codex", "name": "x", "pane_id": "w3:p1", "workspace_id": "w3", "tab_id": "w3:t1", "agent_status": "stale", "agent_session": map[string]any{"kind": "id", "source": "herdr:codex", "value": "a"}},
	}}})
	_, err = NewHerdrResolver(&fakeRunner{responses: [][]byte{stale}}).Resolve(context.Background(), target)
	if !errors.Is(err, ErrResolverStale) {
		t.Fatalf("stale error = %v", err)
	}
}

func TestHerdrResolverRequiresCodexSession(t *testing.T) {
	invalid := mustJSON(map[string]any{"result": map[string]any{"agents": []any{
		map[string]any{"agent": "codex", "name": "x", "pane_id": "w3:p1", "workspace_id": "w3", "tab_id": "w3:t1", "agent_status": "idle", "agent_session": map[string]any{"kind": "id", "source": "other", "value": "thread"}},
	}}})
	_, err := NewHerdrResolver(&fakeRunner{responses: [][]byte{invalid}}).Resolve(context.Background(), Target{Kind: TargetAgent, Name: "x"})
	if !errors.Is(err, ErrResolverStale) {
		t.Fatalf("non-Codex session error = %v", err)
	}
}

func TestSSHHerdrUsesEndpointSpecificRunnerAndContext(t *testing.T) {
	list := mustJSON(map[string]any{"result": map[string]any{"agents": []any{
		map[string]any{"agent": "codex", "name": "remote", "pane_id": "w1:p1", "workspace_id": "w1", "tab_id": "w1:t1", "agent_status": "idle", "agent_session": map[string]any{"kind": "id", "source": "herdr:codex", "value": "thread"}},
	}}})
	get := herdrJSON("remote", "w1:p1", "w1", "w1:t1", "thread", "idle")
	route, err := SSHRoute("remote.example")
	if err != nil {
		t.Fatal(err)
	}
	id, _ := NewEndpointID()
	endpoint := Endpoint{ID: id, Alias: "remote", Route: route, Herdr: HerdrAuto}
	var calls [][]string
	var gotEndpoint Endpoint
	resolver := NewHerdrResolver(nil)
	resolver.EndpointRunner = EndpointCommandRunnerFunc(func(_ context.Context, got Endpoint, argv []string) ([]byte, error) {
		gotEndpoint = got
		calls = append(calls, argv)
		if len(calls) == 1 {
			return list, nil
		}
		return get, nil
	})
	target := Target{Kind: TargetAgent, Name: "remote"}
	resolved, err := resolver.ResolveEndpoint(context.Background(), endpoint, target)
	if err != nil || resolved.ThreadID != "thread" || gotEndpoint.ID != endpoint.ID {
		t.Fatalf("remote resolution = %#v, err=%v endpoint=%#v", resolved, err, gotEndpoint)
	}
	if !reflect.DeepEqual(calls, [][]string{{"herdr", "agent", "list"}, {"herdr", "agent", "get", "w1:p1"}}) {
		t.Fatalf("remote argv = %#v", calls)
	}
}

func TestSSHArgvCannotCarryShellOrBody(t *testing.T) {
	route, err := SSHRoute("host; touch /tmp/pwned")
	if err == nil {
		t.Fatal("expected whitespace rejection")
	}
	route, err = SSHRoute("host;touch")
	if err != nil {
		t.Fatal(err)
	}
	argv, err := route.SSHArgv()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.Join(argv, " "), "body") || !reflect.DeepEqual(argv, []string{"ssh", "--", "host;touch", "codex", "app-server", "proxy"}) {
		t.Fatalf("unsafe or unexpected argv %#v", argv)
	}
}
