package service

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	mektup "github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/internal/endpoint"
	"github.com/agensfield/mektup/go/internal/journal"
	"github.com/agensfield/mektup/go/internal/sshproxy"
)

const (
	remoteCustodyEndpoint   = "ep_0198f0e0-0000-7000-8000-000000000071"
	bodyDestinationEndpoint = "ep_0198f0e0-0000-7000-8000-000000000072"
	remoteCustodyStore      = "store_0198f0e0-0000-7000-8000-000000000073"
	remoteOriginal          = "msg_0198f0e0-0000-7000-8000-000000000074"
	remoteReply             = "msg_0198f0e0-0000-7000-8000-000000000075"
	remoteOperation         = "op_0198f0e0-0000-7000-8000-000000000076"
)

func TestRemoteJournalRoutesCustodyAndPreservesSeparateBodyEndpoint(t *testing.T) {
	root := t.TempDir()
	inner, err := journal.Open(context.Background(), journal.Options{StateDir: filepath.Join(root, "journal")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = inner.Close() })
	identity := NewMemoryIdentityRegistry()
	local := &SQLiteJournal{Inner: inner, Registry: identity}
	store := endpoint.NewStore(filepath.Join(root, "config", "endpoints.json"), filepath.Join(root, "state"))
	remoteRoute, _ := endpoint.SSHRoute("custody.example")
	if err := store.Add(endpoint.Endpoint{ID: remoteCustodyEndpoint, Alias: "custody", Route: remoteRoute, Herdr: endpoint.HerdrDisabled}); err != nil {
		t.Fatal(err)
	}
	bodyRoute, _ := endpoint.UnixRoute(filepath.Join(root, "body.sock"))
	if err := store.Add(endpoint.Endpoint{ID: bodyDestinationEndpoint, Alias: "body", Route: bodyRoute, Herdr: endpoint.HerdrDisabled}); err != nil {
		t.Fatal(err)
	}
	var operations []string
	invoker := func(_ context.Context, route endpoint.Route, req sshproxy.ControlRequest) ([]byte, error) {
		if route.Kind != endpoint.RouteSSH || route.SSHHost != "custody.example" {
			t.Fatalf("route=%#v", route)
		}
		operations = append(operations, req.Operation)
		response := req
		response.Kind = "result"
		response.FencingToken = ""
		response.Lease = nil
		response.BodyBytes = nil
		response.BodySHA256 = ""
		response.ReplyStatus = ""
		response.AttemptOwner = ""
		switch req.Operation {
		case "claim":
			response.Result = json.RawMessage(`{"disposition":"claimed","state":"reply_dispatch_claimed","fencingToken":"fence-1","lease":{"acquiredAt":"2026-09-16T00:00:00.000000001Z","expiresAt":"2026-09-16T00:00:30.000000001Z"}}`)
		case "heartbeat":
			response.Result = json.RawMessage(`{"state":"reply_dispatch_claimed","lease":{"expiresAt":"2026-09-16T00:00:31.000000001Z"}}`)
		case "commit":
			response.Result = json.RawMessage(`{"state":"reply_accepted","wakeRecorded":true,"won":true}`)
		case "status", "reconcile":
			response.Result = json.RawMessage(`{"state":"reply_accepted","replyStatus":"success","commitSeq":1}`)
		default:
			t.Fatalf("unexpected operation %s", req.Operation)
		}
		return json.Marshal(response)
	}
	router := &RemoteJournal{Local: local, Endpoints: store, LocalEndpointID: bodyDestinationEndpoint, Invoke: invoker, Now: func() time.Time { return time.Date(2026, 9, 16, 0, 0, 0, 1, time.UTC) }}
	op := Operation{OperationID: remoteOperation, MessageID: remoteOriginal, SourceRoute: "codex://body/thread/source", TargetRoute: "codex://body/thread/target", Semantics: "message", ReplyRoute: "codex://body/thread/reply", ReplyEndpointID: bodyDestinationEndpoint, CustodyRoute: remoteCustodyEndpoint, CustodyStoreID: remoteCustodyStore, AttemptOwner: "sender", Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", BodySize: 4, SourceEndpointID: bodyDestinationEndpoint, TargetEndpointID: bodyDestinationEndpoint}
	if _, err := router.Prepare(context.Background(), op); err != nil {
		t.Fatal(err)
	}
	input := ReplyClaimInput{ReplyID: remoteReply, OriginalID: remoteOriginal, Digest: op.Digest, BodySize: 4, Status: "success", ReplyRoute: op.ReplyRoute, CustodyRoute: remoteCustodyEndpoint, CustodyStoreID: remoteCustodyStore, Owner: "sender"}
	claim, err := router.ClaimReply(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if claim.Token != "fence-1" || claim.Joined || claim.State != mektup.StateReplyDispatchClaimed {
		t.Fatalf("claim=%#v", claim)
	}
	if err := router.Heartbeat(context.Background(), remoteReply, "sender", claim.Token); err != nil {
		t.Fatal(err)
	}
	committed, err := router.CommitReply(context.Background(), remoteReply, "sender", claim.Token)
	if err != nil {
		t.Fatal(err)
	}
	if committed.State != mektup.StateReplyAccepted || !committed.Won {
		t.Fatalf("commit=%#v", committed)
	}
	if _, err := router.WaitReply(context.Background(), remoteReply, time.Second); err == nil {
		t.Fatal("own custody acceptance completed a child wait")
	}
	if len(operations) != 4 || operations[0] != "claim" || operations[1] != "heartbeat" || operations[2] != "commit" || operations[3] != "status" {
		t.Fatalf("operations=%v", operations)
	}
}

func TestRemoteJournalRejectsSwappedResponseAndRequiresMapping(t *testing.T) {
	root := t.TempDir()
	inner, err := journal.Open(context.Background(), journal.Options{StateDir: filepath.Join(root, "journal")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = inner.Close() })
	local := &SQLiteJournal{Inner: inner, Registry: NewMemoryIdentityRegistry()}
	store := endpoint.NewStore(filepath.Join(root, "config", "endpoints.json"), filepath.Join(root, "state"))
	remoteRoute, _ := endpoint.SSHRoute("custody.example")
	if err := store.Add(endpoint.Endpoint{ID: remoteCustodyEndpoint, Alias: "custody", Route: remoteRoute, Herdr: endpoint.HerdrDisabled}); err != nil {
		t.Fatal(err)
	}
	calls := 0
	router := &RemoteJournal{Local: local, Endpoints: store, LocalEndpointID: bodyDestinationEndpoint, Invoke: func(_ context.Context, _ endpoint.Route, req sshproxy.ControlRequest) ([]byte, error) {
		calls++
		response := req
		response.Kind = "result"
		response.OperationID = "op_0198f0e0-0000-7000-8000-000000000099"
		response.Result = json.RawMessage(`{"disposition":"existing","state":"reply_dispatch_claimed"}`)
		return json.Marshal(response)
	}}
	op := Operation{OperationID: remoteOperation, MessageID: remoteOriginal, SourceRoute: "codex://body/thread/source", TargetRoute: "codex://body/thread/target", Semantics: "message", ReplyRoute: "codex://body/thread/reply", ReplyEndpointID: bodyDestinationEndpoint, CustodyRoute: remoteCustodyEndpoint, CustodyStoreID: remoteCustodyStore, Digest: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", BodySize: 1, SourceEndpointID: bodyDestinationEndpoint, TargetEndpointID: bodyDestinationEndpoint}
	if _, err := router.Prepare(context.Background(), op); err != nil {
		t.Fatal(err)
	}
	_, err = router.ClaimReply(context.Background(), ReplyClaimInput{ReplyID: remoteReply, OriginalID: remoteOriginal, Digest: op.Digest, BodySize: 1, Status: "success", ReplyRoute: op.ReplyRoute, CustodyRoute: remoteCustodyEndpoint, CustodyStoreID: remoteCustodyStore, Owner: "sender"})
	if !errors.Is(err, sshproxy.ErrControlValidation) {
		t.Fatalf("swapped response err=%v", err)
	}
	if calls != 1 {
		t.Fatalf("calls=%d", calls)
	}
	missing := &RemoteJournal{Local: router.Local, Endpoints: endpoint.NewStore(filepath.Join(root, "missing", "endpoints.json"), filepath.Join(root, "missing-state")), LocalEndpointID: bodyDestinationEndpoint}
	if _, err := missing.ClaimReply(context.Background(), ReplyClaimInput{ReplyID: remoteReply, OriginalID: remoteOriginal, Digest: op.Digest, BodySize: 1, Status: "success", ReplyRoute: op.ReplyRoute, CustodyRoute: remoteCustodyEndpoint, CustodyStoreID: remoteCustodyStore, Owner: "sender"}); err == nil {
		t.Fatal("unmapped remote custody accepted")
	}
}
