package service

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mektup "github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/internal/controlreceiver"
	"github.com/agensfield/mektup/go/internal/endpoint"
	"github.com/agensfield/mektup/go/internal/journal"
	"github.com/agensfield/mektup/go/internal/sshproxy"
)

type canonicalStatusResolver struct {
	store controlreceiver.Store
}

func (r canonicalStatusResolver) Resolve(context.Context, string, string) (controlreceiver.Store, error) {
	return r.store, nil
}

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
		case "observe":
			response.Result = json.RawMessage(`{"state":"reply_observed","status":"observed","winner":{"replyMessageId":"msg_0198f0e0-0000-7000-8000-000000000075","commitSeq":1,"status":"success","bodyBytes":4,"bodySha256":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","nativeItemId":"native-remote-1"},"provenance":{"endpointId":"ep_0198f0e0-0000-7000-8000-000000000072","controlRoute":"ep_0198f0e0-0000-7000-8000-000000000071"}}`)
		case "status", "reconcile":
			response.Result = json.RawMessage(`{"state":"reply_accepted","replyStatus":"success","commitSeq":1}`)
		default:
			t.Fatalf("unexpected operation %s", req.Operation)
		}
		raw, marshalErr := json.Marshal(response)
		if req.Operation == "observe" {
			if _, validateErr := sshproxy.ValidateControlRequest(raw); validateErr != nil {
				t.Fatalf("fake observe result invalid: %v raw=%s", validateErr, raw)
			}
		}
		return raw, marshalErr
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
	if err := router.ObserveReply(context.Background(), remoteReply, "native-remote-1", op.Digest); err != nil {
		t.Fatal(err)
	}
	if _, err := router.WaitReply(context.Background(), remoteReply, time.Second); err == nil {
		t.Fatal("own custody acceptance completed a child wait")
	}
	if len(operations) != 5 || operations[0] != "claim" || operations[1] != "heartbeat" || operations[2] != "commit" || operations[3] != "observe" || operations[4] != "status" {
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

func TestRemoteJournalOriginalStatusUsesCanonicalReceiverSelections(t *testing.T) {
	root := t.TempDir()
	server, err := journal.Open(context.Background(), journal.Options{StateDir: filepath.Join(root, "server")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	client, err := journal.Open(context.Background(), journal.Options{StateDir: filepath.Join(root, "client")})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	store := endpoint.NewStore(filepath.Join(root, "config", "endpoints.json"), filepath.Join(root, "state"))
	remoteRoute, _ := endpoint.SSHRoute("custody.example")
	if err := store.Add(endpoint.Endpoint{ID: remoteCustodyEndpoint, Alias: "custody", Route: remoteRoute, Herdr: endpoint.HerdrDisabled}); err != nil {
		t.Fatal(err)
	}
	bodyRoute, _ := endpoint.UnixRoute(filepath.Join(root, "body.sock"))
	if err := store.Add(endpoint.Endpoint{ID: bodyDestinationEndpoint, Alias: "body", Route: bodyRoute, Herdr: endpoint.HerdrDisabled}); err != nil {
		t.Fatal(err)
	}
	resolver := canonicalStatusResolver{store: controlreceiver.Store{Journal: server, StoreID: server.StoreID(), EndpointID: remoteCustodyEndpoint, CloseFunc: func() error { return nil }}}
	receiver := controlreceiver.Receiver{
		Registry:        resolver,
		LocalEndpointID: remoteCustodyEndpoint,
		Destination: controlreceiver.DestinationResolverFunc(func(_ context.Context, endpointID, uri, threadID string) error {
			if endpointID != bodyDestinationEndpoint || uri != "codex://body/thread/source" || threadID != "source" {
				return controlreceiver.ErrRelationshipMismatch
			}
			return nil
		}),
	}
	router := &RemoteJournal{
		Local:           &SQLiteJournal{Inner: client},
		Endpoints:       store,
		LocalEndpointID: bodyDestinationEndpoint,
		Invoke: func(ctx context.Context, _ endpoint.Route, request sshproxy.ControlRequest) ([]byte, error) {
			data, err := json.Marshal(request)
			if err != nil {
				return nil, err
			}
			// The canonical receiver contract requires the optional
			// replyMessageId key to be absent on originalStatus requests.
			// The shared wire struct on this base still serializes its empty
			// value; model the approved wire document at this seam.
			var document map[string]json.RawMessage
			if err := json.Unmarshal(data, &document); err != nil {
				return nil, err
			}
			delete(document, "replyMessageId")
			data, err = json.Marshal(document)
			if err != nil {
				return nil, err
			}
			return receiver.Receive(ctx, data)
		},
	}
	newOperation := func(operationID, messageID string) Operation {
		return Operation{OperationID: operationID, MessageID: messageID, SourceRoute: "codex://body/thread/source", TargetRoute: "codex://body/thread/target", Semantics: "message", ReplyRoute: "codex://body/thread/source", ReplyEndpointID: bodyDestinationEndpoint, CustodyRoute: remoteCustodyEndpoint, CustodyStoreID: server.StoreID(), Digest: "sha256:" + strings.Repeat("a", 64), BodySize: 4, SourceEndpointID: bodyDestinationEndpoint, TargetEndpointID: bodyDestinationEndpoint}
	}
	importOriginal := func(op Operation) {
		err := server.ImportOperation(context.Background(), journal.Operation{OperationID: op.OperationID, MessageID: op.MessageID, SourceRoute: op.SourceRoute, TargetRoute: op.TargetRoute, Semantics: op.Semantics, SourceEndpointID: op.SourceEndpointID, TargetEndpointID: op.TargetEndpointID, ReplyRoute: op.ReplyRoute, ReplyEndpointID: op.ReplyEndpointID, ReplyThreadID: "source", CustodyRoute: op.CustodyRoute, CustodyStoreID: op.CustodyStoreID, Digest: op.Digest, BodySize: op.BodySize}, journal.StatePrepared, "")
		if err != nil {
			t.Fatal(err)
		}
	}
	pending := newOperation("op_0198f0e0-0000-7000-8000-000000000081", "msg_0198f0e0-0000-7000-8000-000000000082")
	importOriginal(pending)
	if err := client.ImportOperation(context.Background(), journal.Operation{OperationID: pending.OperationID, MessageID: pending.MessageID, SourceRoute: pending.SourceRoute, TargetRoute: pending.TargetRoute, Semantics: pending.Semantics, SourceEndpointID: pending.SourceEndpointID, TargetEndpointID: pending.TargetEndpointID, ReplyRoute: pending.ReplyRoute, ReplyEndpointID: pending.ReplyEndpointID, ReplyThreadID: "source", CustodyRoute: pending.CustodyRoute, CustodyStoreID: pending.CustodyStoreID, Digest: pending.Digest, BodySize: pending.BodySize}, journal.StatePrepared, "portable_import"); err != nil {
		t.Fatal(err)
	}
	if imported, err := client.Operation(context.Background(), pending.OperationID); err != nil || imported.SourceEndpointID == "" || imported.TargetEndpointID == "" {
		t.Fatalf("client imported operation=%+v err=%v", imported, err)
	}
	result, err := router.OriginalStatus(context.Background(), pending)
	if err != nil || result.Selection != "pending" {
		t.Fatalf("pending result=%+v err=%v", result, err)
	}
	if status, err := router.Lookup(context.Background(), pending.OperationID); err != nil || status.State != mektup.StatePrepared {
		t.Fatalf("cacheless pending lookup=%+v err=%v", status, err)
	}
	pendingClaim, err := server.ClaimReply(context.Background(), journal.ClaimInput{ReplyID: "msg_0198f0e0-0000-7000-8000-000000000089", OriginalID: pending.MessageID, Digest: pending.Digest, BodySize: pending.BodySize, Status: "success", ReplyRoute: pending.ReplyRoute, CustodyRoute: pending.CustodyRoute, CustodyStoreID: pending.CustodyStoreID, Owner: "receiver"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.CommitReply(context.Background(), pendingClaim.ReplyID, pendingClaim.Owner, pendingClaim.Token); err != nil {
		t.Fatal(err)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	client, err = journal.Open(context.Background(), journal.Options{StateDir: filepath.Join(root, "client")})
	if err != nil {
		t.Fatal(err)
	}
	restarted := &RemoteJournal{Local: &SQLiteJournal{Inner: client}, Endpoints: store, LocalEndpointID: bodyDestinationEndpoint, Invoke: func(ctx context.Context, _ endpoint.Route, request sshproxy.ControlRequest) ([]byte, error) {
		data, err := json.Marshal(request)
		if err != nil {
			return nil, err
		}
		var document map[string]json.RawMessage
		if err := json.Unmarshal(data, &document); err != nil {
			return nil, err
		}
		delete(document, "replyMessageId")
		data, err = json.Marshal(document)
		if err != nil {
			return nil, err
		}
		return receiver.Receive(ctx, data)
	}}
	if status, err := restarted.Lookup(context.Background(), pending.OperationID); err != nil || status.State != mektup.StateReplyAccepted || status.ReplyID != pendingClaim.ReplyID {
		t.Fatalf("restart winner lookup=%+v err=%v", status, err)
	}
	winner := newOperation("op_0198f0e0-0000-7000-8000-000000000083", "msg_0198f0e0-0000-7000-8000-000000000084")
	importOriginal(winner)
	winnerClaim, err := server.ClaimReply(context.Background(), journal.ClaimInput{ReplyID: "msg_0198f0e0-0000-7000-8000-000000000085", OriginalID: winner.MessageID, Digest: winner.Digest, BodySize: winner.BodySize, Status: "success", ReplyRoute: winner.ReplyRoute, CustodyRoute: winner.CustodyRoute, CustodyStoreID: winner.CustodyStoreID, Owner: "receiver"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := server.CommitReply(context.Background(), winnerClaim.ReplyID, winnerClaim.Owner, winnerClaim.Token); err != nil {
		t.Fatal(err)
	}
	result, err = router.OriginalStatus(context.Background(), winner)
	if err != nil || result.Selection != "winner" || result.ReplyID != winnerClaim.ReplyID || result.CommitSeq < 1 {
		t.Fatalf("winner result=%+v err=%v", result, err)
	}
	if _, err := client.Operation(context.Background(), winner.OperationID); !errors.Is(err, journal.ErrNotFound) {
		t.Fatalf("originalStatus created a local per-reply projection: err=%v", err)
	}
	unknown := newOperation("op_0198f0e0-0000-7000-8000-000000000086", "msg_0198f0e0-0000-7000-8000-000000000087")
	importOriginal(unknown)
	unknownClaim, err := server.ClaimReply(context.Background(), journal.ClaimInput{ReplyID: "msg_0198f0e0-0000-7000-8000-000000000088", OriginalID: unknown.MessageID, Digest: unknown.Digest, BodySize: unknown.BodySize, Status: "error", ErrorCode: "E_REMOTE", ReplyRoute: unknown.ReplyRoute, CustodyRoute: unknown.CustodyRoute, CustodyStoreID: unknown.CustodyStoreID, Owner: "receiver"})
	if err != nil {
		t.Fatal(err)
	}
	if err := server.AbandonReply(context.Background(), unknownClaim.ReplyID, unknownClaim.Owner, unknownClaim.Token); err != nil {
		t.Fatal(err)
	}
	result, err = router.OriginalStatus(context.Background(), unknown)
	if err != nil || result.Selection != "terminal_unknown" || result.ReplyID != unknownClaim.ReplyID || result.EventSeq < 1 || result.ErrorCode != "E_REMOTE" {
		t.Fatalf("unknown result=%+v err=%v", result, err)
	}
}

func TestDecodeObserveResultAllowsErrorWinnerWithoutCode(t *testing.T) {
	response := sshproxy.ControlRequest{Result: json.RawMessage(`{"state":"reply_observed","status":"observed","winner":{"replyMessageId":"msg_0198f0e0-0000-7000-8000-000000000075","commitSeq":1,"status":"error","bodyBytes":4,"bodySha256":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","nativeItemId":"native-remote-1"},"provenance":{"endpointId":"ep_0198f0e0-0000-7000-8000-000000000072","controlRoute":"ep_0198f0e0-0000-7000-8000-000000000071"}}`)}
	request := sshproxy.ControlRequest{Custody: sshproxy.CustodyRef{EndpointID: remoteCustodyEndpoint}, ReplyDestination: sshproxy.DestinationRef{EndpointID: bodyDestinationEndpoint}}
	if _, err := decodeObserveResult(response, request); err != nil {
		t.Fatalf("error winner without optional code rejected: %v", err)
	}
	for _, value := range []string{"null", `"4"`, "1.5", "true"} {
		bad := response
		bad.Result = bytes.Replace(response.Result, []byte(`"bodyBytes":4`), []byte(`"bodyBytes":`+value), 1)
		if _, err := decodeObserveResult(bad, request); err == nil {
			t.Fatalf("bodyBytes %s accepted", value)
		}
	}
	zero := response
	zero.Result = bytes.Replace(response.Result, []byte(`"bodyBytes":4`), []byte(`"bodyBytes":0`), 1)
	if _, err := decodeObserveResult(zero, request); err != nil {
		t.Fatalf("integer bodyBytes zero rejected: %v", err)
	}
}

func TestDecodeOriginalStatusPendingAllowsAdditiveMetadata(t *testing.T) {
	result, err := decodeCanonicalOriginalStatus(json.RawMessage(`{"selection":"pending","traceId":"future-extension"}`))
	if err != nil || result.Selection != "pending" {
		t.Fatalf("additive pending metadata rejected: result=%+v err=%v", result, err)
	}
	for _, field := range []string{"state", "bodyBytes", "replyMessageId", "fencingToken"} {
		data := []byte(`{"selection":"pending","` + field + `":"forbidden"}`)
		if _, err := decodeCanonicalOriginalStatus(data); err == nil {
			t.Fatalf("pending known field %s accepted", field)
		}
	}
}
