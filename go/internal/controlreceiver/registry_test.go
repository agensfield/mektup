package controlreceiver

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/agensfield/mektup/go/internal/endpoint"
	"github.com/agensfield/mektup/go/internal/journal"
	"github.com/agensfield/mektup/go/internal/sshproxy"
)

func TestDefaultPathsIgnoreOperationState(t *testing.T) {
	root := t.TempDir()
	t.Setenv("XDG_DATA_HOME", filepath.Join(root, "data"))
	t.Setenv("MEKTUP_STATE_DIR", filepath.Join(root, "operation"))
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(root, "config"))
	path, err := DefaultRegistryPath()
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(root, "data", "mektup", controlRegistryFilename); path != want {
		t.Fatalf("registry path=%q want %q", path, want)
	}
	store, err := DefaultEndpointStore()
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(root, "config", "mektup", "endpoints.json"); store.ConfigPath != want {
		t.Fatalf("config path=%q want %q", store.ConfigPath, want)
	}
}

func TestRegistryRegisterResolveConflictAndOversize(t *testing.T) {
	root := t.TempDir()
	j, err := journal.Open(context.Background(), journal.Options{StateDir: filepath.Join(root, "journal")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = j.Close() })
	registry := FileRegistry{Path: filepath.Join(root, "registry", controlRegistryFilename)}
	endpointID := "ep_0198f0e0-0000-7000-8000-000000000001"
	if err := registry.Register(context.Background(), endpointID, j); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(context.Background(), endpointID, j); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(context.Background(), "ep_0198f0e0-0000-7000-8000-000000000002", j); !errors.Is(err, ErrRegistryConflict) {
		t.Fatalf("conflict=%v", err)
	}
	resolved, err := registry.Resolve(context.Background(), endpointID, j.StoreID())
	if err != nil {
		t.Fatal(err)
	}
	if err := resolved.Close(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(registry.Path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > maxRegistryBytes {
		t.Fatalf("registry size=%d", len(data))
	}
	if info, err := os.Stat(registry.Path); err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("registry permissions: %v %#v", err, info)
	}

	var doc RegistryDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatal(err)
	}
	doc.Stores[0].StateDir = "/x" + strings.Repeat("a", maxRegistryBytes)
	prior, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(registry.Path, prior, 0600); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(context.Background(), endpointID, j); err == nil {
		t.Fatal("oversize registry replacement succeeded")
	}
	after, err := os.ReadFile(registry.Path)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(prior) {
		t.Fatal("oversize registration replaced prior registry")
	}
}

func TestRegistryResolveRejectsUnsafeDatabaseModeWithoutRepair(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, "journal")
	j, err := journal.Open(context.Background(), journal.Options{StateDir: state})
	if err != nil {
		t.Fatal(err)
	}
	registry := FileRegistry{Path: filepath.Join(root, "registry", controlRegistryFilename)}
	endpointID := "ep_0198f0e0-0000-7000-8000-000000000001"
	if err := registry.Register(context.Background(), endpointID, j); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	database := filepath.Join(state, "journal.sqlite3")
	if err := os.Chmod(database, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Resolve(context.Background(), endpointID, j.StoreID()); err == nil {
		t.Fatal("unsafe database mode resolved")
	}
	info, err := os.Stat(database)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0644 {
		t.Fatalf("resolve repaired unsafe mode to %04o", info.Mode().Perm())
	}
}

func TestRegistryRegisterRejectsReplacedJournalPath(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, "journal")
	j, err := journal.Open(context.Background(), journal.Options{StateDir: state})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	moved := filepath.Join(root, "moved")
	if err := os.Rename(state, moved); err != nil {
		t.Fatal(err)
	}
	replacement, err := journal.Open(context.Background(), journal.Options{StateDir: state})
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	registry := FileRegistry{Path: filepath.Join(root, "registry", controlRegistryFilename)}
	endpointID := "ep_0198f0e0-0000-7000-8000-000000000001"
	if err := registry.Register(context.Background(), endpointID, j); !errors.Is(err, ErrStoreUnavailable) {
		t.Fatalf("replaced path registration error=%v", err)
	}
	if _, err := os.Stat(registry.Path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("replaced path created registry: %v", err)
	}
}

func TestBuiltinIdentitySharedAcrossOperationState(t *testing.T) {
	root := t.TempDir()
	t.Setenv("XDG_DATA_HOME", filepath.Join(root, "machine"))
	config := filepath.Join(root, "config", "endpoints.json")
	codexHome := filepath.Join(root, "codex")
	a, err := endpoint.NewStore(config, filepath.Join(root, "operation-a")).EnsureBuiltinLocal(codexHome)
	if err != nil {
		t.Fatal(err)
	}
	b, err := endpoint.NewStore(config, filepath.Join(root, "operation-b")).EnsureBuiltinLocal(codexHome)
	if err != nil {
		t.Fatal(err)
	}
	if a.ID != b.ID {
		t.Fatalf("identity changed: %s != %s", a.ID, b.ID)
	}
	if _, err := os.Stat(filepath.Join(root, "operation-a", "endpoint-identities.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("operation state received identity: %v", err)
	}
}

func TestDestinationMembershipAndPinnedAliasReplacement(t *testing.T) {
	root := t.TempDir()
	t.Setenv("XDG_DATA_HOME", filepath.Join(root, "machine"))
	config := filepath.Join(root, "config", "endpoints.json")
	store := endpoint.NewStore(config, filepath.Join(root, "operation"))
	route, err := endpoint.SSHRoute("example.invalid")
	if err != nil {
		t.Fatal(err)
	}
	first := endpoint.Endpoint{ID: "ep_0198f0e0-0000-7000-8000-000000000091", Alias: "remote", Route: route, Herdr: endpoint.HerdrDisabled}
	if err := store.Add(first); err != nil {
		t.Fatal(err)
	}
	j, state := openReceiverJournal(t, time.Minute)
	q := request(j)
	q.OriginalMessageID = "msg_0198f0e0-0000-7000-8000-000000000088"
	q.ReplyDestination = sshproxy.DestinationRef{EndpointID: first.ID, ThreadID: "source", URI: "codex://remote/thread/source"}
	if _, err := j.Prepare(context.Background(), journal.Operation{OperationID: "op_0198f0e0-0000-7000-8000-000000000088", MessageID: q.OriginalMessageID, SourceRoute: "src", TargetRoute: "dst", Semantics: "message", ReplyRoute: q.ReplyDestination.URI, ReplyEndpointID: first.ID, ReplyThreadID: "source", CustodyRoute: receiverEndpoint, CustodyStoreID: j.StoreID(), Digest: q.BodySHA256, BodySize: *q.BodyBytes}); err != nil {
		t.Fatal(err)
	}
	receiver := Receiver{Registry: makeRegistry(t, state, j.StoreID()), LocalEndpointID: receiverEndpoint, Destination: EndpointDestinationResolver{Store: store}}
	if err := store.Remove("remote"); err != nil {
		t.Fatal(err)
	}
	second := first
	second.ID = "ep_0198f0e0-0000-7000-8000-000000000092"
	if err := store.Add(second); err != nil {
		t.Fatal(err)
	}
	q.ReplyDestination.EndpointID = second.ID
	if _, err := receiver.Receive(context.Background(), mustMarshal(t, q)); !errors.Is(err, ErrRelationshipMismatch) {
		t.Fatalf("alias replacement err=%v", err)
	}
}
