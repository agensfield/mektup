package service

import (
	"context"
	"errors"
	"testing"
	"time"

	mektup "github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/internal/journal"
)

func TestSQLiteJournalAdapterPreservesDispatchFenceAndReceiptIdentity(t *testing.T) {
	ctx := context.Background()
	inner, err := journal.Open(ctx, journal.Options{StateDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	a := SQLiteJournal{Inner: inner, SourceEndpointID: epSource, TargetEndpointID: epTarget}
	op := Operation{OperationID: "op_10999999-9999-7999-8999-999999999999", MessageID: "msg_11999999-9999-7999-8999-999999999999", AttemptOwner: "owner-recovery", SourceRoute: "codex://local/thread/source", TargetRoute: "codex://local/thread/target", Semantics: "message", Digest: digest("x"), BodySize: 1, ReplyRequested: true, ReplyRoute: "codex://local/thread/source", CustodyRoute: epSource, CustodyStoreID: storeID, SourceEndpointID: epSource, TargetEndpointID: epTarget}
	prepared, err := a.Prepare(ctx, op)
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := a.Prepare(ctx, op)
	if err != nil || recovered.Token == "" {
		t.Fatalf("same owned prepared attempt did not recover a real fence token: %#v err=%v", recovered, err)
	}
	if err := a.MarkDispatchStarted(ctx, prepared.OperationID, prepared.Owner, prepared.Token); err != nil {
		t.Fatal(err)
	}
	if err := a.RecordResult(ctx, op.OperationID, mektup.StateOutcomeUnknown, "transport_uncertain"); err != nil {
		t.Fatal(err)
	}
	status, err := a.Lookup(ctx, op.MessageID)
	if err != nil {
		t.Fatal(err)
	}
	if status.State != mektup.StateOutcomeUnknown || status.SourceEndpointID != epSource || status.TargetEndpointID != epTarget {
		t.Fatalf("status lost fence or identity: %#v", status)
	}
}

func TestOperationByMessageRedactsAttemptToken(t *testing.T) {
	ctx := context.Background()
	inner, err := journal.Open(ctx, journal.Options{StateDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	if _, err := inner.Prepare(ctx, journal.Operation{OperationID: "op_23999999-9999-7999-8999-999999999999", MessageID: "msg_24999999-9999-7999-8999-999999999999", SourceRoute: "src", TargetRoute: "dst", Semantics: "message", Digest: digest("x"), BodySize: 1}); err != nil {
		t.Fatal(err)
	}
	record, err := inner.OperationByMessage(ctx, "msg_24999999-9999-7999-8999-999999999999")
	if err != nil {
		t.Fatal(err)
	}
	if record.AttemptToken != "" {
		t.Fatal("message lookup exposed a live fencing token")
	}
}

func TestSQLiteJournalAdapterReplyAcceptanceProjectsClaimMetadata(t *testing.T) {
	ctx := context.Background()
	inner, err := journal.Open(ctx, journal.Options{StateDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	a := SQLiteJournal{Inner: inner, SourceEndpointID: epSource, TargetEndpointID: epTarget}
	op := Operation{OperationID: "op_12999999-9999-7999-8999-999999999999", MessageID: "msg_13999999-9999-7999-8999-999999999999", SourceRoute: "codex://local/thread/source", TargetRoute: "codex://local/thread/target", Semantics: "message", Digest: digest("question"), BodySize: 8, ReplyRequested: true, ReplyRoute: "codex://local/thread/source", CustodyRoute: epSource, CustodyStoreID: storeID, SourceEndpointID: epSource, TargetEndpointID: epTarget}
	if _, err := a.Prepare(ctx, op); err != nil {
		t.Fatal(err)
	}
	replyID := "msg_14999999-9999-7999-8999-999999999999"
	claim, err := a.ClaimReply(ctx, ReplyClaimInput{ReplyID: replyID, OriginalID: op.MessageID, Digest: digest("answer"), BodySize: 6, Status: "success", ReplyRoute: op.ReplyRoute, CustodyRoute: op.CustodyRoute, CustodyStoreID: op.CustodyStoreID, Owner: "receiver"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.CommitReply(ctx, claim.ReplyID, claim.Owner, claim.Token); err != nil {
		t.Fatal(err)
	}
	status, err := a.Lookup(ctx, op.MessageID)
	if err != nil {
		t.Fatal(err)
	}
	if status.State != mektup.StateReplyAccepted || status.ReplyID != replyID || status.ReplyDigest != digest("answer") {
		t.Fatalf("claim metadata not projected: %#v", status)
	}
	result := (&Service{}).waitResult(status, false, "")
	if err := result.Receipt.Validate(); err != nil {
		t.Fatalf("projected wait receipt invalid: %v", err)
	}
}

type adapterFailingDelivery struct{ messageID string }

func (d *adapterFailingDelivery) Send(_ context.Context, _ ResolvedTarget, _ string, id string) (DeliveryResult, error) {
	d.messageID = id
	return DeliveryResult{}, &DeliveryError{Err: context.DeadlineExceeded, Phase: WriteProvenBeforeWrite}
}
func (*adapterFailingDelivery) Resume(context.Context, ResolvedTarget) (string, error) {
	return "", nil
}
func (*adapterFailingDelivery) Detach(context.Context, ResolvedTarget) error { return nil }

func TestServiceWithSQLiteAdapterMapsPostFencePreWriteToUnknown(t *testing.T) {
	ctx := context.Background()
	inner, err := journal.Open(ctx, journal.Options{StateDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	r := baseResolver()
	a := SQLiteJournal{Inner: inner, SourceEndpointID: epSource, TargetEndpointID: epTarget}
	d := &adapterFailingDelivery{}
	s := &Service{Resolver: &r, Delivery: d, Journal: a}
	_, sendErr := s.Send(ctx, SendRequest{Target: "target", Body: "x", DeliveryTimeout: time.Second})
	var se *Error
	if !errors.As(sendErr, &se) || se.Code != mektup.ErrOutcomeUnknown {
		t.Fatalf("send error = %v", sendErr)
	}
	status, err := a.Lookup(ctx, d.messageID)
	if err != nil {
		t.Fatal(err)
	}
	if status.State != mektup.StateOutcomeUnknown {
		t.Fatalf("post-fence pre-write state = %s", status.State)
	}
}

func TestSQLiteReplyWaitDoesNotCountOwnAcceptance(t *testing.T) {
	ctx := context.Background()
	inner, err := journal.Open(ctx, journal.Options{StateDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	r := baseResolver()
	original := mektup.Envelope{MessageID: "msg_27999999-9999-7999-8999-999999999999", Kind: mektup.KindMessage, FromEndpointID: epSource, From: r.source.URI, FromKind: "agent", ToEndpointID: epTarget, To: r.target.URI, RequestedTarget: "target", ReplyRequested: true, ReplyEndpointID: epSource, ReplyTo: r.source.URI, ReplyCustodyEndpointID: epSource, ReplyCustodyStoreID: storeID, Body: "question", Provenance: "observed", SentAt: time.Now().UTC().Format(time.RFC3339Nano)}
	original.PayloadBytes = uint64(len(original.Body))
	original.PayloadSHA256 = digest(original.Body)
	if _, err := inner.Prepare(ctx, journal.Operation{OperationID: "op_28999999-9999-7999-8999-999999999999", MessageID: original.MessageID, SourceRoute: original.From, TargetRoute: original.To, Semantics: "message", Digest: original.PayloadSHA256, BodySize: int64(original.PayloadBytes), ReplyRoute: original.ReplyTo, CustodyRoute: original.ReplyCustodyEndpointID, CustodyStoreID: original.ReplyCustodyStoreID}); err != nil {
		t.Fatal(err)
	}
	a := SQLiteJournal{Inner: inner, SourceEndpointID: epSource, TargetEndpointID: epTarget, ReplyEndpointID: epSource}
	d := &fakeDelivery{}
	s := &Service{Resolver: &r, Delivery: d, Journal: a}
	got, err := s.Reply(ctx, originalResolver{original: OriginalMessage{Envelope: original, CurrentThread: original.To}}, ReplyRequest{Reference: original.MessageID, Body: "answer", Wait: true, WaitTimeout: 30 * time.Millisecond})
	var se *Error
	if !errors.As(err, &se) || se.Code != mektup.ErrWaitIncomplete || got.Wait == nil || got.Wait.ReplyID != "" {
		t.Fatalf("own acceptance counted as child reply: wait=%#v err=%v", got.Wait, err)
	}
}
