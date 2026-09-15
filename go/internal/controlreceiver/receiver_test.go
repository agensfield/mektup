package controlreceiver

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agensfield/mektup/go/internal/journal"
	"github.com/agensfield/mektup/go/internal/sshproxy"
)

const (
	receiverEndpoint = "ep_0198f0e0-0000-7000-8000-000000000001"
	originalID       = "msg_0198f0e0-0000-7000-8000-000000000003"
	replyID          = "msg_0198f0e0-0000-7000-8000-000000000007"
	operationID      = "op_0198f0e0-0000-7000-8000-00000000000c"
)

func openReceiverJournal(t *testing.T, lease time.Duration) (*journal.Journal, string) {
	t.Helper()
	state := filepath.Join(t.TempDir(), "state")
	j, err := journal.Open(context.Background(), journal.Options{StateDir: state, LeaseDuration: lease})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = j.Close() })
	return j, state
}

func makeRegistry(t *testing.T, state, storeID string) FileRegistry {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "stores.json")
	doc := RegistryDocument{Version: 1, Stores: []RegistryEntry{{StoreID: storeID, EndpointID: receiverEndpoint, StateDir: state}}}
	data, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	return FileRegistry{Path: path}
}

func prepareOriginal(t *testing.T, j *journal.Journal) {
	t.Helper()
	if _, err := j.Prepare(context.Background(), journal.Operation{
		OperationID: operationID, MessageID: originalID, SourceRoute: "codex://local/thread/source", TargetRoute: "codex://remote/thread/target", Semantics: "message",
		ReplyRoute: "codex://local/thread/source", CustodyRoute: receiverEndpoint, CustodyStoreID: j.StoreID(), Digest: "sha256:" + strings.Repeat("b", 64), BodySize: 7,
	}); err != nil {
		t.Fatal(err)
	}
}

func request(j *journal.Journal) sshproxy.ControlRequest {
	bytesCount := int64(7)
	return sshproxy.ControlRequest{
		Schema: "mektup/control/v1", Kind: "request", Operation: "claim", OperationID: operationID, ReplyMessageID: replyID, OriginalMessageID: originalID,
		Custody:          sshproxy.CustodyRef{EndpointID: receiverEndpoint, StoreID: j.StoreID()},
		ReplyDestination: sshproxy.DestinationRef{EndpointID: receiverEndpoint, ThreadID: "thread-source", URI: "codex://local/thread/source"},
		BodyBytes:        &bytesCount, BodySHA256: "sha256:" + strings.Repeat("c", 64), ReplyStatus: "success", AttemptOwner: "receiver-test",
	}
}

func receive(t *testing.T, receiver Receiver, req sshproxy.ControlRequest) sshproxy.ControlRequest {
	t.Helper()
	data, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	response, err := receiver.Receive(context.Background(), data)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := sshproxy.ValidateControlRequest(response)
	if err != nil {
		t.Fatal(err)
	}
	return parsed
}

type staticResolver struct{ store Store }

func (r staticResolver) Resolve(context.Context, string, string) (Store, error) { return r.store, nil }

func TestReceiverClaimHeartbeatCommitStatusAndDuplicate(t *testing.T) {
	j, _ := openReceiverJournal(t, time.Minute)
	prepareOriginal(t, j)
	receiver := Receiver{Registry: staticResolver{store: Store{Journal: j, StoreID: j.StoreID(), EndpointID: receiverEndpoint, CloseFunc: func() error { return nil }}}, LocalEndpointID: receiverEndpoint}
	claimRequest := request(j)
	claimResult := receive(t, receiver, claimRequest)
	var claimPayload struct {
		Disposition  string         `json:"disposition"`
		State        string         `json:"state"`
		FencingToken string         `json:"fencingToken"`
		Lease        sshproxy.Lease `json:"lease"`
	}
	if err := json.Unmarshal(claimResult.Result, &claimPayload); err != nil {
		t.Fatal(err)
	}
	if claimPayload.Disposition != "claimed" || claimPayload.State != string(journal.StateReplyClaimed) || claimPayload.FencingToken == "" || claimPayload.Lease.ExpiresAt == "" {
		t.Fatalf("claim result = %s", claimResult.Result)
	}

	duplicateBytes, err := receiver.Receive(context.Background(), mustMarshal(t, claimRequest))
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := sshproxy.ValidateControlRequest(duplicateBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(duplicate.Result), `"disposition":"existing"`) {
		t.Fatalf("duplicate result = %s", duplicate.Result)
	}

	heartbeat := claimRequest
	heartbeat.Operation = "heartbeat"
	heartbeat.FencingToken = claimPayload.FencingToken
	heartbeat.Lease = &sshproxy.Lease{ExpiresAt: claimPayload.Lease.ExpiresAt}
	heartbeatResult := receive(t, receiver, heartbeat)
	if !strings.Contains(string(heartbeatResult.Result), string(journal.StateReplyClaimed)) {
		t.Fatalf("heartbeat result = %s", heartbeatResult.Result)
	}

	commit := heartbeat
	commit.Operation = "commit"
	commitResult := receive(t, receiver, commit)
	if !strings.Contains(string(commitResult.Result), string(journal.StateReplyAccepted)) {
		t.Fatalf("commit result = %s", commitResult.Result)
	}

	status := claimRequest
	status.Operation = "status"
	status.Result = nil
	statusResult := receive(t, receiver, status)
	if !strings.Contains(string(statusResult.Result), string(journal.StateReplyAccepted)) {
		t.Fatalf("status result = %s", statusResult.Result)
	}
}

func TestReceiverRejectsWrongStoreRouteAndConflict(t *testing.T) {
	j, state := openReceiverJournal(t, time.Minute)
	prepareOriginal(t, j)
	receiver := Receiver{Registry: makeRegistry(t, state, j.StoreID()), LocalEndpointID: receiverEndpoint}
	wrong := request(j)
	wrong.Custody.EndpointID = "ep_0198f0e0-0000-7000-8000-000000000002"
	if _, err := receiver.Receive(context.Background(), mustMarshal(t, wrong)); !errors.Is(err, ErrStoreUnavailable) && !errors.Is(err, ErrRelationshipMismatch) {
		t.Fatalf("wrong route err = %v", err)
	}
	good := request(j)
	if _, err := receiver.Receive(context.Background(), mustMarshal(t, good)); err != nil {
		t.Fatal(err)
	}
	conflict := good
	conflict.BodySHA256 = "sha256:" + strings.Repeat("d", 64)
	if _, err := receiver.Receive(context.Background(), mustMarshal(t, conflict)); !errors.Is(err, journal.ErrIdentityConflict) {
		t.Fatalf("conflict err = %v", err)
	}
}

func TestReceiverRejectsWrongThreadForMatchingDestination(t *testing.T) {
	j, state := openReceiverJournal(t, time.Minute)
	prepareOriginal(t, j)
	receiver := Receiver{Registry: makeRegistry(t, state, j.StoreID()), LocalEndpointID: receiverEndpoint}
	wrong := request(j)
	wrong.ReplyDestination.ThreadID = "thread-other"
	if _, err := receiver.Receive(context.Background(), mustMarshal(t, wrong)); !errors.Is(err, ErrRelationshipMismatch) {
		t.Fatalf("wrong thread err = %v", err)
	}
}

func TestReceiverExpiredTokenAndUnknownStatus(t *testing.T) {
	now := atomic.Int64{}
	now.Store(time.Now().UnixNano())
	state := filepath.Join(t.TempDir(), "state")
	j, err := journal.Open(context.Background(), journal.Options{StateDir: state, LeaseDuration: time.Second, Now: func() time.Time { return time.Unix(0, now.Load()) }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = j.Close() })
	prepareOriginal(t, j)
	receiver := Receiver{Registry: staticResolver{store: Store{Journal: j, StoreID: j.StoreID(), EndpointID: receiverEndpoint, CloseFunc: func() error { return nil }}}, LocalEndpointID: receiverEndpoint}
	claim := request(j)
	result := receive(t, receiver, claim)
	var payload struct {
		FencingToken string         `json:"fencingToken"`
		Lease        sshproxy.Lease `json:"lease"`
	}
	if err := json.Unmarshal(result.Result, &payload); err != nil {
		t.Fatal(err)
	}
	now.Add(2 * int64(time.Second))
	commit := claim
	commit.Operation = "commit"
	commit.FencingToken = payload.FencingToken
	commit.Lease = &sshproxy.Lease{ExpiresAt: payload.Lease.ExpiresAt}
	if _, err := receiver.Receive(context.Background(), mustMarshal(t, commit)); !errors.Is(err, journal.ErrClaimExpired) {
		t.Fatalf("expired token err = %v", err)
	}
	unknown := claim
	unknown.Operation = "status"
	unknown.ReplyMessageID = "msg_0198f0e0-0000-7000-8000-000000000008"
	if _, err := receiver.Receive(context.Background(), mustMarshal(t, unknown)); !errors.Is(err, journal.ErrNotFound) {
		t.Fatalf("unknown status err = %v", err)
	}
}

func TestReceiverServeRejectsMultipleDocumentsAndBody(t *testing.T) {
	j, state := openReceiverJournal(t, time.Minute)
	prepareOriginal(t, j)
	receiver := Receiver{Registry: makeRegistry(t, state, j.StoreID()), LocalEndpointID: receiverEndpoint}
	data := mustMarshal(t, request(j))
	if err := receiver.Serve(context.Background(), strings.NewReader(string(data)+"\n"+string(data)), io.Discard); !errors.Is(err, sshproxy.ErrControlValidation) {
		t.Fatalf("multiple docs err = %v", err)
	}
	withBody := append([]byte{}, data...)
	withBody = append(withBody[:len(withBody)-1], []byte(`,"body":"secret"}`)...)
	if _, err := receiver.Receive(context.Background(), withBody); !errors.Is(err, sshproxy.ErrControlValidation) {
		t.Fatalf("body err = %v", err)
	}
}

func mustMarshal(t *testing.T, value sshproxy.ControlRequest) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
