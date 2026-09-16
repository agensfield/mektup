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
	canonical, err := filepath.EvalSymlinks(j.StateDir())
	if err != nil {
		t.Fatal(err)
	}
	return j, canonical
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
		ReplyRoute: "codex://local/thread/source", ReplyEndpointID: receiverEndpoint, ReplyThreadID: "source", CustodyRoute: receiverEndpoint, CustodyStoreID: j.StoreID(), Digest: "sha256:" + strings.Repeat("b", 64), BodySize: 7,
	}); err != nil {
		t.Fatal(err)
	}
}

func request(j *journal.Journal) sshproxy.ControlRequest {
	bytesCount := int64(7)
	return sshproxy.ControlRequest{
		Schema: "mektup/control/v1", Kind: "request", Operation: "claim", OperationID: operationID, ReplyMessageID: replyID, OriginalMessageID: originalID,
		Custody:          sshproxy.CustodyRef{EndpointID: receiverEndpoint, StoreID: j.StoreID()},
		ReplyDestination: sshproxy.DestinationRef{EndpointID: receiverEndpoint, ThreadID: "source", URI: "codex://local/thread/source"},
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

func localDestination() DestinationResolver {
	return DestinationResolverFunc(func(_ context.Context, endpointID, uri, threadID string) error {
		if endpointID == receiverEndpoint && uri == "codex://local/thread/source" && threadID == "source" {
			return nil
		}
		return ErrRelationshipMismatch
	})
}

func TestReceiverClaimHeartbeatCommitStatusAndDuplicate(t *testing.T) {
	j, _ := openReceiverJournal(t, time.Minute)
	prepareOriginal(t, j)
	receiver := Receiver{Registry: staticResolver{store: Store{Journal: j, StoreID: j.StoreID(), EndpointID: receiverEndpoint, CloseFunc: func() error { return nil }}}, LocalEndpointID: receiverEndpoint, Destination: localDestination()}
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

func TestReceiverOriginalStatusIsTokenlessAndWinnerFirst(t *testing.T) {
	j, _ := openReceiverJournal(t, time.Minute)
	prepareOriginal(t, j)
	claim, err := j.ClaimReply(context.Background(), journal.ClaimInput{ReplyID: replyID, OriginalID: originalID, Digest: "sha256:" + strings.Repeat("c", 64), BodySize: 7, Status: "success", ReplyRoute: "codex://local/thread/source", CustodyRoute: receiverEndpoint, CustodyStoreID: j.StoreID(), Owner: "receiver-test"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.CommitReply(context.Background(), claim.ReplyID, claim.Owner, claim.Token); err != nil {
		t.Fatal(err)
	}
	receiver := Receiver{Registry: staticResolver{store: Store{Journal: j, StoreID: j.StoreID(), EndpointID: receiverEndpoint, CloseFunc: func() error { return nil }}}, LocalEndpointID: receiverEndpoint, Destination: localDestination()}
	q := request(j)
	q.Operation = "originalStatus"
	q.ReplyMessageID = ""
	q.BodyBytes = nil
	q.BodySHA256 = ""
	q.ReplyStatus = ""
	q.AttemptOwner = ""
	q.ReplyErrorCode = ""
	raw := mustMarshal(t, q)
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	delete(document, "replyMessageId")
	raw, err = json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	response, err := receiver.Receive(context.Background(), raw)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := sshproxy.ValidateControlRequest(response)
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err := json.Unmarshal(parsed.Result, &result); err != nil {
		t.Fatal(err)
	}
	if result["selection"] != "winner" || result["replyMessageId"] != replyID || result["replyStatus"] != "success" {
		t.Fatalf("originalStatus result %#v", result)
	}
	if _, ok := result["fencingToken"]; ok {
		t.Fatal("originalStatus leaked token")
	}
}

func TestReceiverOriginalStatusRejectsReplyIDPresenceAndOperationMismatch(t *testing.T) {
	j, _ := openReceiverJournal(t, time.Minute)
	prepareOriginal(t, j)
	receiver := Receiver{Registry: staticResolver{store: Store{Journal: j, StoreID: j.StoreID(), EndpointID: receiverEndpoint, CloseFunc: func() error { return nil }}}, LocalEndpointID: receiverEndpoint, Destination: localDestination()}
	var err error
	base := request(j)
	base.Operation = "originalStatus"
	base.BodyBytes = nil
	base.BodySHA256 = ""
	base.ReplyStatus = ""
	base.AttemptOwner = ""
	base.ReplyErrorCode = ""
	for name, replyValue := range map[string]any{"empty": "", "null": nil, "nonempty": replyID} {
		t.Run(name, func(t *testing.T) {
			raw := mustMarshal(t, base)
			var document map[string]any
			if err := json.Unmarshal(raw, &document); err != nil {
				t.Fatal(err)
			}
			document["replyMessageId"] = replyValue
			raw, err = json.Marshal(document)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := receiver.Receive(context.Background(), raw); !errors.Is(err, sshproxy.ErrControlValidation) {
				t.Fatalf("replyMessageId %v accepted: %v", replyValue, err)
			}
		})
	}
	mismatch := base
	mismatch.OperationID = "op_0198f0e0-0000-7000-8000-000000000099"
	raw := mustMarshal(t, mismatch)
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	delete(document, "replyMessageId")
	raw, err = json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := receiver.Receive(context.Background(), raw); !errors.Is(err, ErrRelationshipMismatch) {
		t.Fatalf("operation mismatch error: %v", err)
	}
}

func TestReceiverObserveAcceptedIsTokenlessAndIdempotent(t *testing.T) {
	j, _ := openReceiverJournal(t, time.Minute)
	prepareOriginal(t, j)
	receiver := Receiver{Registry: staticResolver{store: Store{Journal: j, StoreID: j.StoreID(), EndpointID: receiverEndpoint, CloseFunc: func() error { return nil }}}, LocalEndpointID: receiverEndpoint, Destination: localDestination()}
	claimRequest := request(j)
	claimResult := receive(t, receiver, claimRequest)
	var claimed struct {
		FencingToken string         `json:"fencingToken"`
		Lease        sshproxy.Lease `json:"lease"`
	}
	if err := json.Unmarshal(claimResult.Result, &claimed); err != nil {
		t.Fatal(err)
	}
	commit := claimRequest
	commit.Operation = "commit"
	commit.FencingToken = claimed.FencingToken
	commit.Lease = &claimed.Lease
	if _, err := receiver.Receive(context.Background(), mustMarshal(t, commit)); err != nil {
		t.Fatal(err)
	}
	observe := claimRequest
	observe.Operation = "observe"
	observe.NativeItemID = "native-reply-1"
	observe.AttemptOwner = ""
	observe.FencingToken = ""
	observe.Lease = nil
	first := receive(t, receiver, observe)
	var result struct {
		State      string         `json:"state"`
		Status     string         `json:"status"`
		Winner     map[string]any `json:"winner"`
		Provenance map[string]any `json:"provenance"`
	}
	if err := json.Unmarshal(first.Result, &result); err != nil {
		t.Fatal(err)
	}
	if result.State != string(journal.StateReplyObserved) || result.Status != "observed" || result.Winner["nativeItemId"] != observe.NativeItemID || result.Provenance["controlRoute"] != observe.Custody.EndpointID {
		t.Fatalf("observe result = %s", first.Result)
	}
	_, _, storedEndpoint, storedRoute, err := j.Observation(context.Background(), observe.ReplyMessageID)
	if err != nil || storedEndpoint != observe.ReplyDestination.EndpointID || storedRoute != observe.Custody.EndpointID {
		t.Fatalf("observation provenance not durable: endpoint=%q route=%q err=%v", storedEndpoint, storedRoute, err)
	}
	if _, err := receiver.Receive(context.Background(), mustMarshal(t, observe)); err != nil {
		t.Fatalf("same observation was not idempotent: %v", err)
	}
	conflict := observe
	conflict.NativeItemID = "native-reply-2"
	if _, err := receiver.Receive(context.Background(), mustMarshal(t, conflict)); !errors.Is(err, journal.ErrIdentityConflict) {
		t.Fatalf("conflicting observation error = %v", err)
	}
}

func TestReceiverObserveRejectsSelectedClaimTupleMismatch(t *testing.T) {
	j, _ := openReceiverJournal(t, time.Minute)
	prepareOriginal(t, j)
	receiver := Receiver{Registry: staticResolver{store: Store{Journal: j, StoreID: j.StoreID(), EndpointID: receiverEndpoint, CloseFunc: func() error { return nil }}}, LocalEndpointID: receiverEndpoint, Destination: localDestination()}
	claimRequest := request(j)
	if _, err := receiver.Receive(context.Background(), mustMarshal(t, claimRequest)); err != nil {
		t.Fatal(err)
	}
	observe := claimRequest
	observe.Operation = "observe"
	observe.NativeItemID = "native-reply-1"
	observe.AttemptOwner = ""
	observe.FencingToken = ""
	observe.Lease = nil
	wrong := observe
	wrong.BodyBytes = new(int64)
	*wrong.BodyBytes = 8
	if _, err := receiver.Receive(context.Background(), mustMarshal(t, wrong)); !errors.Is(err, ErrRelationshipMismatch) {
		t.Fatalf("tuple mismatch error = %v", err)
	}
}

func TestReceiverTerminalClaimDuplicatesAreStatusOnly(t *testing.T) {
	for _, terminal := range []string{"accepted", "observed", "unknown"} {
		t.Run(terminal, func(t *testing.T) {
			j, state := openReceiverJournal(t, time.Minute)
			prepareOriginal(t, j)
			receiver := Receiver{Registry: makeRegistry(t, state, j.StoreID()), LocalEndpointID: receiverEndpoint, Destination: localDestination()}
			claimRequest := request(j)
			claimResult := receive(t, receiver, claimRequest)
			var payload struct {
				FencingToken string         `json:"fencingToken"`
				Lease        sshproxy.Lease `json:"lease"`
			}
			if err := json.Unmarshal(claimResult.Result, &payload); err != nil {
				t.Fatal(err)
			}
			switch terminal {
			case "accepted", "observed":
				if _, err := j.CommitReply(context.Background(), claimRequest.ReplyMessageID, claimRequest.AttemptOwner, payload.FencingToken); err != nil {
					t.Fatal(err)
				}
				if terminal == "observed" {
					if err := j.ObserveReply(context.Background(), claimRequest.ReplyMessageID, "native-item", claimRequest.BodySHA256); err != nil {
						t.Fatal(err)
					}
				}
			case "unknown":
				if err := j.AbandonReply(context.Background(), claimRequest.ReplyMessageID, claimRequest.AttemptOwner, payload.FencingToken); err != nil {
					t.Fatal(err)
				}
			}
			response, err := receiver.Receive(context.Background(), mustMarshal(t, claimRequest))
			if err != nil {
				t.Fatal(err)
			}
			parsed, err := sshproxy.ValidateControlRequest(response)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(parsed.Result), `"disposition":"existing"`) || strings.Contains(string(parsed.Result), "fencingToken") || strings.Contains(string(parsed.Result), "lease") || strings.Contains(string(parsed.Result), "won") {
				t.Fatalf("terminal result = %s", parsed.Result)
			}
			_ = j.Close()
		})
	}
}

func TestReceiverClaimJoinsAcrossAttemptOwners(t *testing.T) {
	for _, terminal := range []string{"active", "accepted"} {
		t.Run(terminal, func(t *testing.T) {
			j, _ := openReceiverJournal(t, time.Minute)
			prepareOriginal(t, j)
			receiver := Receiver{Registry: staticResolver{store: Store{Journal: j, StoreID: j.StoreID(), EndpointID: receiverEndpoint, CloseFunc: func() error { return nil }}}, LocalEndpointID: receiverEndpoint, Destination: localDestination()}
			first := request(j)
			claimed := receive(t, receiver, first)
			if terminal == "accepted" {
				var payload struct {
					FencingToken string `json:"fencingToken"`
				}
				if err := json.Unmarshal(claimed.Result, &payload); err != nil {
					t.Fatal(err)
				}
				if _, err := j.CommitReply(context.Background(), first.ReplyMessageID, first.AttemptOwner, payload.FencingToken); err != nil {
					t.Fatal(err)
				}
			}
			join := first
			join.AttemptOwner = "receiver-other"
			response, err := receiver.Receive(context.Background(), mustMarshal(t, join))
			if err != nil {
				t.Fatal(err)
			}
			parsed, err := sshproxy.ValidateControlRequest(response)
			if err != nil {
				t.Fatal(err)
			}
			result := string(parsed.Result)
			if !strings.Contains(result, `"disposition":"existing"`) || strings.Contains(result, "fencingToken") || strings.Contains(result, "lease") || strings.Contains(result, "won") {
				t.Fatalf("%s cross-owner result = %s", terminal, result)
			}
		})
	}
}

func TestReceiverRejectsWrongStoreRouteAndConflict(t *testing.T) {
	j, state := openReceiverJournal(t, time.Minute)
	prepareOriginal(t, j)
	receiver := Receiver{Registry: makeRegistry(t, state, j.StoreID()), LocalEndpointID: receiverEndpoint, Destination: localDestination()}
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
	receiver := Receiver{Registry: makeRegistry(t, state, j.StoreID()), LocalEndpointID: receiverEndpoint, Destination: localDestination()}
	wrong := request(j)
	wrong.ReplyDestination.ThreadID = "thread-other"
	if _, err := receiver.Receive(context.Background(), mustMarshal(t, wrong)); !errors.Is(err, ErrRelationshipMismatch) {
		t.Fatalf("wrong thread err = %v", err)
	}
	presentation := request(j)
	presentation.ReplyDestination.ThreadID = "thread-source"
	if _, err := receiver.Receive(context.Background(), mustMarshal(t, presentation)); !errors.Is(err, ErrRelationshipMismatch) {
		t.Fatalf("presentation thread alias err = %v", err)
	}
}

func TestReceiverAllowsDistinctTrustedReplyEndpointTopology(t *testing.T) {
	j, state := openReceiverJournal(t, time.Minute)
	trustedEndpoint := "ep_0198f0e0-0000-7000-8000-000000000099"
	receiver := Receiver{Registry: makeRegistry(t, state, j.StoreID()), LocalEndpointID: receiverEndpoint, Destination: DestinationResolverFunc(func(_ context.Context, endpointID, uri, threadID string) error {
		if endpointID == trustedEndpoint && uri == "codex://replyhost/thread/source" && threadID == "source" {
			return nil
		}
		return ErrRelationshipMismatch
	})}
	otherOriginal := "msg_0198f0e0-0000-7000-8000-000000000088"
	if _, err := j.Prepare(context.Background(), journal.Operation{OperationID: "op_0198f0e0-0000-7000-8000-000000000088", MessageID: otherOriginal, SourceRoute: "src", TargetRoute: "dst", Semantics: "message", ReplyRoute: "codex://replyhost/thread/source", ReplyEndpointID: trustedEndpoint, ReplyThreadID: "source", CustodyRoute: receiverEndpoint, CustodyStoreID: j.StoreID(), Digest: "digest", BodySize: 1}); err != nil {
		t.Fatal(err)
	}
	q := request(j)
	q.OperationID = "op_0198f0e0-0000-7000-8000-000000000088"
	q.OriginalMessageID = otherOriginal
	q.ReplyDestination.EndpointID = trustedEndpoint
	q.ReplyDestination.URI = "codex://replyhost/thread/source"
	q.BodyBytes = ptrInt64(1)
	q.BodySHA256 = "sha256:" + strings.Repeat("d", 64)
	if _, err := receiver.Receive(context.Background(), mustMarshal(t, q)); err != nil {
		t.Fatalf("distinct endpoint topology rejected: %v", err)
	}
	wrong := q
	wrong.ReplyDestination.EndpointID = "ep_0198f0e0-0000-7000-8000-000000000098"
	if _, err := receiver.Receive(context.Background(), mustMarshal(t, wrong)); !errors.Is(err, ErrRelationshipMismatch) {
		t.Fatalf("unmapped endpoint err = %v", err)
	}
}

func TestRegistryDoesNotInitializeZeroByteDatabase(t *testing.T) {
	state := filepath.Join(t.TempDir(), "state")
	if err := os.MkdirAll(state, 0700); err != nil {
		t.Fatal(err)
	}
	dbPath := filepath.Join(state, "journal.sqlite3")
	if err := os.WriteFile(dbPath, nil, 0600); err != nil {
		t.Fatal(err)
	}
	registryDir := t.TempDir()
	if err := os.Chmod(registryDir, 0700); err != nil {
		t.Fatal(err)
	}
	regPath := filepath.Join(registryDir, "stores.json")
	storeID := "store_0198f0e0-0000-7000-8000-000000000099"
	doc, err := json.Marshal(RegistryDocument{Version: 1, Stores: []RegistryEntry{{StoreID: storeID, EndpointID: receiverEndpoint, StateDir: state}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(regPath, doc, 0600); err != nil {
		t.Fatal(err)
	}
	registry := FileRegistry{Path: regPath}
	if _, err := registry.Resolve(context.Background(), receiverEndpoint, storeID); err == nil {
		t.Fatal("zero-byte database accepted")
	}
	info, err := os.Stat(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Size() != 0 {
		t.Fatalf("zero-byte database changed to %d bytes", info.Size())
	}
}

func ptrInt64(value int64) *int64 { return &value }

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
	receiver := Receiver{Registry: staticResolver{store: Store{Journal: j, StoreID: j.StoreID(), EndpointID: receiverEndpoint, CloseFunc: func() error { return nil }}}, LocalEndpointID: receiverEndpoint, Destination: localDestination()}
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
	receiver := Receiver{Registry: makeRegistry(t, state, j.StoreID()), LocalEndpointID: receiverEndpoint, Destination: localDestination()}
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
