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

type reviewHistoryHook func(context.Context, string, string) ([]HistoryItem, error)

func (f reviewHistoryHook) FullHistory(ctx context.Context, endpointID, threadID string) ([]HistoryItem, error) {
	return f(ctx, endpointID, threadID)
}

func (failingReceiptJournal) PutReceipt(context.Context, mektup.Receipt) error {
	return errors.New("injected receipt write failure")
}

type stagedProjectionJournal struct {
	*journal.Journal
	staged  chan struct{}
	release chan struct{}
	once    bool
}

func (s *stagedProjectionJournal) PutReceipt(ctx context.Context, receipt mektup.Receipt) error {
	if err := s.Journal.PutReceipt(ctx, receipt); err != nil {
		return err
	}
	if !s.once && receipt.State == mektup.StateManuallyResolved {
		s.once = true
		close(s.staged)
		<-s.release
	}
	return nil
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

func TestManualResolveRecoveryCannotBeClobberedByOriginalWriter(t *testing.T) {
	store, j := openStore(t)
	receipt := testReceipt(t, mektup.StateOutcomeUnknown)
	reviewPrepare(t, j, receipt, false)
	if err := store.Save(context.Background(), receipt); err != nil {
		t.Fatal(err)
	}
	staged := &stagedProjectionJournal{Journal: j, staged: make(chan struct{}), release: make(chan struct{})}
	first := Store{Journal: staged, Now: store.Now}
	request := ResolveRequest{Reference: receipt.ReceiptID, Assertion: "not_delivered", Actor: "operator", Reason: "test", EvidenceRef: "test", Presentation: "human", Intent: "receipt.resolve", Gate: &gateStub{}}
	done := make(chan error, 1)
	go func() {
		_, err := first.Resolve(context.Background(), request)
		done <- err
	}()
	<-staged.staged
	resolved, err := store.Resolve(context.Background(), request)
	if err != nil {
		close(staged.release)
		<-done
		t.Fatal(err)
	}
	close(staged.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	op, _ := j.Operation(context.Background(), receipt.OperationID)
	stored, _ := j.Receipt(context.Background(), receipt.ReceiptID)
	if stored.State != resolved.State || stored.State != mektup.EvidenceState(op.State) {
		t.Fatalf("successful recovery overwritten by stale writer: returned=%s operation=%s receipt=%s", resolved.State, op.State, stored.State)
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

func TestReconcileDurableReplyObservationUsesExactPinnedReplyRoute(t *testing.T) {
	store, j := openStore(t)
	epSource, epTarget := mektup.NewEndpointID(), mektup.NewEndpointID()
	originalID, replyID := mektup.NewMessageID(), mektup.NewMessageID()
	replyBody := []byte("durable reply")
	replyDigest := bodyDigest(replyBody)
	receipt := testReceipt(t, mektup.StateReplyAccepted)
	receipt.Source = mektup.ReceiptIdentity{EndpointID: epSource, ThreadID: "source-thread", Resolved: "codex://source/thread/source-thread"}
	receipt.Target = mektup.ReceiptIdentity{EndpointID: epTarget, ThreadID: "target-thread", Resolved: "codex://target/thread/target-thread"}
	receipt.Message.MessageID = originalID
	receipt.Message.ReplyRequested = true
	receipt.OperationID = mektup.NewOperationID()
	if _, err := j.Prepare(context.Background(), journal.Operation{OperationID: receipt.OperationID, MessageID: originalID, SourceRoute: receipt.Source.Resolved, TargetRoute: receipt.Target.Resolved, Semantics: "send", SourceEndpointID: epSource, TargetEndpointID: epTarget, ReplyRoute: receipt.Source.Resolved, ReplyEndpointID: epSource, ReplyThreadID: "source-thread", CustodyRoute: "custody", CustodyStoreID: j.StoreID(), Digest: receipt.Message.PayloadSHA256, BodySize: int64(receipt.Message.PayloadBytes)}); err != nil {
		t.Fatal(err)
	}
	if err := j.MarkDispatchStarted(context.Background(), receipt.OperationID); err != nil {
		t.Fatal(err)
	}
	if err := j.RecordResult(context.Background(), receipt.OperationID, journal.StateAccepted, ""); err != nil {
		t.Fatal(err)
	}
	claim, err := j.ClaimReply(context.Background(), journal.ClaimInput{ReplyID: replyID, OriginalID: originalID, Digest: replyDigest, BodySize: int64(len(replyBody)), Status: "success", ReplyRoute: receipt.Source.Resolved, CustodyRoute: "custody", CustodyStoreID: j.StoreID(), Owner: "reply-owner"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.CommitReply(context.Background(), claim.ReplyID, claim.Owner, claim.Token); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(context.Background(), receipt); err != nil {
		t.Fatal(err)
	}
	historyItem := HistoryItem{EndpointID: epSource, ThreadID: "source-thread", TurnID: "turn-1", ItemID: "native-reply", MessageID: replyID, ClientMessageID: replyID, InReplyTo: originalID, ReplyStatus: "success", EnvelopeToEndpointID: epSource, EnvelopeTo: receipt.Source.Resolved, EnvelopeFromEndpointID: epTarget, EnvelopeFrom: receipt.Target.Resolved, Body: replyBody}
	history := historyStub{items: []HistoryItem{historyItem}}
	historyItem.Body = []byte("wrong digest")
	badHistory := historyStub{items: []HistoryItem{historyItem}}
	historyItem.Body = replyBody
	if _, err := store.Reconcile(context.Background(), receipt.ReceiptID, badHistory); !errors.Is(err, ErrReconcileIncomplete) {
		t.Fatalf("wrong native digest accepted: %v", err)
	}
	updated, err := store.Reconcile(context.Background(), receipt.ReceiptID, history)
	if err != nil {
		t.Fatal(err)
	}
	if updated.State != mektup.StateReplyObserved || updated.ContentRef == nil || updated.ContentRef.EndpointID != epSource || updated.ContentRef.ClientMessageID != replyID {
		t.Fatalf("updated receipt = %+v", updated)
	}
	claimAfter, err := j.Reply(context.Background(), replyID)
	if err != nil || claimAfter.State != journal.StateReplyObserved {
		t.Fatalf("claim after observation = %+v, %v", claimAfter, err)
	}
	if _, err := store.Reconcile(context.Background(), receipt.ReceiptID, history); err != nil {
		t.Fatalf("idempotent reconcile = %v", err)
	}
}

func TestReconcileDurableReplyRejectsWrongNativeIdentity(t *testing.T) {
	store, j := openStore(t)
	epSource := mektup.NewEndpointID()
	originalID, replyID := mektup.NewMessageID(), mektup.NewMessageID()
	replyBody := []byte("reply")
	receipt := testReceipt(t, mektup.StateReplyAccepted)
	receipt.Source = mektup.ReceiptIdentity{EndpointID: epSource, ThreadID: "source-thread"}
	receipt.Target = mektup.ReceiptIdentity{EndpointID: mektup.NewEndpointID(), ThreadID: "target-thread"}
	receipt.Message.MessageID = originalID
	receipt.OperationID = mektup.NewOperationID()
	if _, err := j.Prepare(context.Background(), journal.Operation{OperationID: receipt.OperationID, MessageID: originalID, SourceRoute: "codex://source/thread/source-thread", TargetRoute: "codex://target/thread/target-thread", Semantics: "send", SourceEndpointID: epSource, TargetEndpointID: receipt.Target.EndpointID, ReplyRoute: "codex://source/thread/source-thread", ReplyEndpointID: epSource, ReplyThreadID: "source-thread", CustodyRoute: "custody", CustodyStoreID: j.StoreID(), Digest: receipt.Message.PayloadSHA256, BodySize: int64(receipt.Message.PayloadBytes)}); err != nil {
		t.Fatal(err)
	}
	if err := j.MarkDispatchStarted(context.Background(), receipt.OperationID); err != nil {
		t.Fatal(err)
	}
	if err := j.RecordResult(context.Background(), receipt.OperationID, journal.StateAccepted, ""); err != nil {
		t.Fatal(err)
	}
	claim, err := j.ClaimReply(context.Background(), journal.ClaimInput{ReplyID: replyID, OriginalID: originalID, Digest: bodyDigest(replyBody), BodySize: int64(len(replyBody)), Status: "success", ReplyRoute: "codex://source/thread/source-thread", CustodyRoute: "custody", CustodyStoreID: j.StoreID(), Owner: "reply-owner"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.CommitReply(context.Background(), claim.ReplyID, claim.Owner, claim.Token); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(context.Background(), receipt); err != nil {
		t.Fatal(err)
	}
	wrong := historyStub{items: []HistoryItem{{EndpointID: receipt.Target.EndpointID, ThreadID: "target-thread", ItemID: "wrong", MessageID: replyID, ClientMessageID: replyID, InReplyTo: originalID, ReplyStatus: "success", Body: replyBody}}}
	if _, err := store.Reconcile(context.Background(), receipt.ReceiptID, wrong); !errors.Is(err, ErrReconcileIncomplete) {
		t.Fatalf("wrong endpoint accepted: %v", err)
	}
}

func TestReconcileDurableReplyDoesNotPublishStaleWinner(t *testing.T) {
	store, j := openStore(t)
	epSource, epTarget := mektup.NewEndpointID(), mektup.NewEndpointID()
	originalID, replyA := mektup.NewMessageID(), mektup.NewMessageID()
	replyBody := []byte("durable reply")
	receipt := testReceipt(t, mektup.StateReplyAccepted)
	receipt.Source = mektup.ReceiptIdentity{EndpointID: epSource, ThreadID: "source-thread", Resolved: "codex://source/thread/source-thread"}
	receipt.Target = mektup.ReceiptIdentity{EndpointID: epTarget, ThreadID: "target-thread", Resolved: "codex://target/thread/target-thread"}
	receipt.Message.MessageID, receipt.OperationID = originalID, mektup.NewOperationID()
	if _, err := j.Prepare(context.Background(), journal.Operation{OperationID: receipt.OperationID, MessageID: originalID, SourceRoute: receipt.Source.Resolved, TargetRoute: receipt.Target.Resolved, Semantics: "send", SourceEndpointID: epSource, TargetEndpointID: epTarget, ReplyRoute: receipt.Source.Resolved, ReplyEndpointID: epSource, ReplyThreadID: "source-thread", CustodyRoute: "custody", CustodyStoreID: j.StoreID(), Digest: receipt.Message.PayloadSHA256, BodySize: int64(receipt.Message.PayloadBytes)}); err != nil {
		t.Fatal(err)
	}
	if err := j.MarkDispatchStarted(context.Background(), receipt.OperationID); err != nil {
		t.Fatal(err)
	}
	if err := j.RecordResult(context.Background(), receipt.OperationID, journal.StateAccepted, ""); err != nil {
		t.Fatal(err)
	}
	claim, err := j.ClaimReply(context.Background(), journal.ClaimInput{ReplyID: replyA, OriginalID: originalID, Digest: bodyDigest(replyBody), BodySize: int64(len(replyBody)), Status: "success", ReplyRoute: receipt.Source.Resolved, CustodyRoute: "custody", CustodyStoreID: j.StoreID(), Owner: "reply-a"})
	if err != nil {
		t.Fatal(err)
	}
	if err := j.AbandonReply(context.Background(), claim.ReplyID, claim.Owner, claim.Token); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(context.Background(), receipt); err != nil {
		t.Fatal(err)
	}
	item := HistoryItem{EndpointID: epSource, ThreadID: "source-thread", ItemID: "native-a", MessageID: replyA, ClientMessageID: replyA, InReplyTo: originalID, ReplyStatus: "success", EnvelopeToEndpointID: epSource, EnvelopeTo: receipt.Source.Resolved, EnvelopeFromEndpointID: epTarget, EnvelopeFrom: receipt.Target.Resolved, Body: replyBody}
	history := reviewHistoryHook(func(context.Context, string, string) ([]HistoryItem, error) {
		winner := mektup.NewMessageID()
		later, err := j.ClaimReply(context.Background(), journal.ClaimInput{ReplyID: winner, OriginalID: originalID, Digest: bodyDigest(replyBody), BodySize: int64(len(replyBody)), Status: "success", ReplyRoute: receipt.Source.Resolved, CustodyRoute: "custody", CustodyStoreID: j.StoreID(), Owner: "reply-b"})
		if err != nil {
			return nil, err
		}
		if _, err := j.CommitReply(context.Background(), later.ReplyID, later.Owner, later.Token); err != nil {
			return nil, err
		}
		return []HistoryItem{item}, nil
	})
	got, err := store.Reconcile(context.Background(), receipt.ReceiptID, history)
	if err == nil || got.ContentRef != nil {
		t.Fatalf("stale winner was published: receipt=%+v err=%v", got, err)
	}
	stored, _ := j.Receipt(context.Background(), receipt.ReceiptID)
	if stored.ContentRef != nil || stored.State != mektup.StateReplyAccepted {
		t.Fatalf("stale receipt projection persisted: %+v", stored)
	}
}
