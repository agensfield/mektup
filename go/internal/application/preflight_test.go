package application

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	mektup "github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/internal/cli"
	"github.com/agensfield/mektup/go/internal/controlreceiver"
	"github.com/agensfield/mektup/go/internal/endpoint"
	"github.com/agensfield/mektup/go/internal/journal"
	"github.com/agensfield/mektup/go/internal/service"
	"github.com/agensfield/mektup/go/internal/sshproxy"
)

func TestApplicationReplyPreflightValidatesLocalRegistrationBeforeRemote(t *testing.T) {
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
	remote := endpoint.Endpoint{ID: mektup.NewEndpointID(), Alias: "remote", Route: endpoint.Route{Kind: endpoint.RouteSSH, SSHHost: "remote-host"}, Herdr: endpoint.HerdrDisabled}
	if err := store.Save(endpoint.Config{Version: 1, Endpoints: []endpoint.Endpoint{remote}}); err != nil {
		t.Fatal(err)
	}
	j, err := journal.Open(context.Background(), journal.Options{StateDir: filepath.Join(root, "journal")})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	registry := controlreceiver.FileRegistry{Path: filepath.Join(root, "registry", "control-registry.json")}
	calls := 0
	env := New(Options{Registry: registry, PreflightInvoker: func(ctx context.Context, route endpoint.Route, req sshproxy.PreflightDocument) error {
		calls++
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("preflight has no deadline")
		}
		if route.SSHHost != "remote-host" || req.TargetEndpointID != remote.ID || req.ReplyDestination.EndpointID != local.ID || req.Custody.StoreID != j.StoreID() {
			t.Fatalf("wrong metadata: %+v", req)
		}
		return nil
	}})
	check := env.replyPreflight(store, home, j)
	target := service.ResolvedTarget{EndpointID: remote.ID}
	source := service.SourceIdentity{EndpointID: local.ID, URI: "codex://" + local.ID + "/thread/thread-source", CustodyEndpointID: local.ID, CustodyStoreID: j.StoreID()}
	if err := check(context.Background(), target, source); err == nil {
		t.Fatal("unregistered store accepted")
	}
	if calls != 0 {
		t.Fatal("remote contacted before local registration validation")
	}
	if err := controlreceiver.RegisterLocalJournal(context.Background(), registry, store, home, local.ID, j); err != nil {
		t.Fatal(err)
	}
	if err := check(context.Background(), target, source); err != nil {
		t.Fatal(err)
	}
	if calls != 1 {
		t.Fatalf("calls=%d", calls)
	}
	source.CustodyStoreID = mektup.NewStoreID()
	var cliErr *cli.Error
	if err := check(context.Background(), target, source); !errors.As(err, &cliErr) || cliErr.Code != "reply_route_unavailable" || cliErr.Effect != "not_sent" {
		t.Fatalf("wrong failure: %v", err)
	}
	if calls != 1 {
		t.Fatal("wrong store contacted remote")
	}
}
