package receipts

import (
	"context"
	"errors"
	"testing"
	"time"

	mektup "github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/internal/journal"
)

type failingReceiptJournal struct{ *journal.Journal }

func (failingReceiptJournal) PutReceipt(context.Context, mektup.Receipt) error {
	return errors.New("injected receipt write failure")
}

func reviewPrepare(t *testing.T, j *journal.Journal, receipt mektup.Receipt, reply bool) {
	t.Helper()
	op := journal.Operation{OperationID: receipt.OperationID, MessageID: receipt.Message.MessageID, SourceRoute: "src", TargetRoute: "dst", Semantics: "send", Digest: receipt.Message.PayloadSHA256, BodySize: int64(receipt.Message.PayloadBytes)}
	if reply {
		op.ReplyRoute, op.CustodyRoute, op.CustodyStoreID = "reply", "custody", "store"
	}
	if _, err := j.Prepare(context.Background(), op); err != nil {
		t.Fatal(err)
	}
	if err := j.MarkDispatchStarted(context.Background(), op.OperationID); err != nil {
		t.Fatal(err)
	}
	if err := j.RecordResult(context.Background(), op.OperationID, journal.EvidenceState(receipt.State), "test"); err != nil {
		t.Fatal(err)
	}
}

func TestManualResolveReceiptWriteFailureRemainsRetryable(t *testing.T) {
	store, j := openStore(t)
	receipt := testReceipt(t, mektup.StateOutcomeUnknown)
	reviewPrepare(t, j, receipt, false)
	if err := store.Save(context.Background(), receipt); err != nil {
		t.Fatal(err)
	}
	store.Journal = failingReceiptJournal{j}
	_, err := store.Resolve(context.Background(), ResolveRequest{Reference: receipt.ReceiptID, Assertion: "not_delivered", Actor: "operator", Reason: "test", EvidenceRef: "test", Presentation: "human", Intent: "receipt.resolve", Gate: &gateStub{}})
	if err == nil {
		t.Fatal("injection did not fire")
	}
	op, _ := j.Operation(context.Background(), receipt.OperationID)
	stored, _ := j.Receipt(context.Background(), receipt.ReceiptID)
	if mektup.EvidenceState(op.State) != stored.State {
		t.Fatalf("partial manual commit: operation=%s receipt=%s", op.State, stored.State)
	}
}

type reviewInspector struct{ identity TargetIdentity }

func (r reviewInspector) Inspect(context.Context, string) (TargetIdentity, error) {
	return r.identity, nil
}

func TestInspectFiltersBeforeLimit(t *testing.T) {
	store, _ := openStore(t)
	related := testReceipt(t, mektup.StateAccepted)
	related.CreatedAt = time.Now().Add(-time.Hour).UTC().Format(time.RFC3339Nano)
	if err := store.Save(context.Background(), related); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(context.Background(), testReceipt(t, mektup.StateAccepted)); err != nil {
		t.Fatal(err)
	}
	got, err := store.Inspect(context.Background(), "target", reviewInspector{identity: TargetIdentity{EndpointID: related.Target.EndpointID, ThreadID: related.Target.ThreadID}}, InspectOptions{ReceiptLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Receipts) != 1 {
		t.Fatal("newer unrelated receipt hides related receipt before limit")
	}
}

func TestReceiptRetentionPreservesUnansweredFamily(t *testing.T) {
	store, j := openStore(t)
	receipt := testReceipt(t, mektup.StateAccepted)
	receipt.Message.ReplyRequested = true
	old := time.Now().Add(-31 * 24 * time.Hour).UTC().Format(time.RFC3339Nano)
	receipt.CreatedAt, receipt.UpdatedAt = old, old
	reviewPrepare(t, j, receipt, true)
	if err := store.Save(context.Background(), receipt); err != nil {
		t.Fatal(err)
	}
	if _, err := j.StorageMaintain(context.Background(), journal.MaintenanceOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Operation(context.Background(), receipt.OperationID); err != nil {
		t.Fatal(err)
	}
	if _, err := j.Receipt(context.Background(), receipt.ReceiptID); err != nil {
		t.Fatal("unanswered operation retained but its receipt deleted")
	}
}

func TestReconcileHashesActualBody(t *testing.T) {
	store, j := openStore(t)
	receipt := testReceipt(t, mektup.StateOutcomeUnknown)
	receipt.Message.PayloadSHA256 = bodyDigest([]byte("good"))
	reviewPrepare(t, j, receipt, false)
	if err := store.Save(context.Background(), receipt); err != nil {
		t.Fatal(err)
	}
	item := HistoryItem{EndpointID: receipt.Target.EndpointID, ThreadID: receipt.Target.ThreadID, ItemID: "native", MessageID: receipt.Message.MessageID, Body: []byte("evil"), PayloadSHA256: receipt.Message.PayloadSHA256}
	_, err := store.Reconcile(context.Background(), receipt.ReceiptID, historyStub{items: []HistoryItem{item}})
	op, _ := j.Operation(context.Background(), receipt.OperationID)
	if err == nil || op.State == journal.StateAccepted {
		t.Fatalf("forged advertised digest accepted actual wrong body: state=%s err=%v", op.State, err)
	}
}

func TestReconcileDoesNotSelectReplyByDigest(t *testing.T) {
	store, j := openStore(t)
	receipt := testReceipt(t, mektup.StateOutcomeUnknown)
	receipt.Message.PayloadSHA256 = bodyDigest([]byte("good"))
	receipt.Message.ReplyRequested = true
	reviewPrepare(t, j, receipt, true)
	if err := store.Save(context.Background(), receipt); err != nil {
		t.Fatal(err)
	}
	claim, err := j.ClaimReply(context.Background(), journal.ClaimInput{ReplyID: "other-reply", OriginalID: receipt.Message.MessageID, Digest: receipt.Message.PayloadSHA256, BodySize: 4, Status: "success", ReplyRoute: "reply", CustodyRoute: "custody", CustodyStoreID: "store", Owner: "owner"})
	if err != nil {
		t.Fatal(err)
	}
	if err := j.AbandonReply(context.Background(), claim.ReplyID, claim.Owner, claim.Token); err != nil {
		t.Fatal(err)
	}
	item := HistoryItem{EndpointID: receipt.Target.EndpointID, ThreadID: receipt.Target.ThreadID, ItemID: "original-item", MessageID: receipt.Message.MessageID, ClientMessageID: receipt.Message.MessageID, Body: []byte("good"), PayloadSHA256: receipt.Message.PayloadSHA256}
	_, _ = store.Reconcile(context.Background(), receipt.ReceiptID, historyStub{items: []HistoryItem{item}})
	after, _ := j.Reply(context.Background(), claim.ReplyID)
	if after.State == journal.StateReplyObserved {
		t.Fatal("original native item promoted different reply solely by digest")
	}
}
