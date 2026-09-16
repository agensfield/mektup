package journal

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mektup "github.com/agensfield/mektup/go"
)

func identityJournal(t *testing.T) *Journal {
	t.Helper()
	clock := &atomic.Int64{}
	clock.Store(time.Now().UnixNano())
	return testJournal(t, t.TempDir(), clock)
}

func identityReceipt(t *testing.T) mektup.Receipt {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return mektup.Receipt{Schema: mektup.ReceiptSchema, ReceiptID: mektup.NewReceiptID(), OperationID: mektup.NewOperationID(), Operation: "send", State: mektup.StateAccepted,
		Source:   mektup.ReceiptIdentity{EndpointID: mektup.NewEndpointID(), ThreadID: "source", Resolved: "codex://source/thread/source"},
		Target:   mektup.ReceiptIdentity{EndpointID: mektup.NewEndpointID(), ThreadID: "target", Resolved: "codex://target/thread/target"},
		Message:  mektup.ReceiptMessage{MessageID: mektup.NewMessageID(), Kind: string(mektup.KindMessage), ReplyRequested: true, PayloadBytes: 4, PayloadSHA256: "sha256:" + strings.Repeat("a", 64)},
		Evidence: []mektup.EvidenceRecord{{State: mektup.StateAccepted, At: now, Kind: "accepted"}}, Warnings: []mektup.Warning{}, CreatedAt: now, UpdatedAt: now}
}

func TestPutReceiptRejectsImmutableIdentityRebinding(t *testing.T) {
	j := identityJournal(t)
	base := identityReceipt(t)
	if err := j.PutReceipt(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	mutations := []func(*mektup.Receipt){
		func(r *mektup.Receipt) { r.OperationID = mektup.NewOperationID() },
		func(r *mektup.Receipt) { r.Operation = "reply" },
		func(r *mektup.Receipt) { r.Message.MessageID = mektup.NewMessageID() },
		func(r *mektup.Receipt) { r.Message.PayloadBytes = 5 },
		func(r *mektup.Receipt) { r.Message.PayloadSHA256 = "sha256:" + strings.Repeat("b", 64) },
		func(r *mektup.Receipt) { r.Source.ThreadID = "other" },
		func(r *mektup.Receipt) { r.Target.Resolved = "codex://target/thread/other" },
		func(r *mektup.Receipt) { r.CreatedAt = time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano) },
	}
	for i, mutate := range mutations {
		candidate := base
		mutate(&candidate)
		if err := j.PutReceipt(context.Background(), candidate); !errors.Is(err, ErrIdentityConflict) {
			t.Fatalf("mutation %d error=%v, want identity conflict", i, err)
		}
	}
	stored, err := j.Receipt(context.Background(), base.ReceiptID)
	if err != nil || stored.OperationID != base.OperationID || stored.Message.MessageID != base.Message.MessageID {
		t.Fatalf("stored receipt was rebound: %+v %v", stored, err)
	}
}

func TestPutReceiptRejectsInconsistentContentRefButAllowsStrengthening(t *testing.T) {
	j := identityJournal(t)
	base := identityReceipt(t)
	if err := j.PutReceipt(context.Background(), base); err != nil {
		t.Fatal(err)
	}
	digest := "sha256:" + strings.Repeat("c", 64)
	ref := &mektup.ContentRef{EndpointID: base.Source.EndpointID, ThreadID: base.Source.ThreadID, ItemID: "item-1", PayloadBytes: 4, PayloadSHA256: digest}
	strengthened := base
	strengthened.State = mektup.StateReplyObserved
	strengthened.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	strengthened.Evidence = append(strengthened.Evidence, mektup.EvidenceRecord{State: mektup.StateReplyObserved, Kind: "observed"})
	strengthened.Warnings = append(strengthened.Warnings, mektup.Warning{Code: mektup.WarningEvidenceGap, Message: "advisory"})
	strengthened.ContentRef = ref
	if err := j.PutReceipt(context.Background(), strengthened); err != nil {
		t.Fatal(err)
	}
	conflict := strengthened
	conflict.ContentRef = &mektup.ContentRef{EndpointID: base.Target.EndpointID, ThreadID: base.Target.ThreadID, ItemID: "item-2", PayloadBytes: 4, PayloadSHA256: digest}
	if err := j.PutReceipt(context.Background(), conflict); !errors.Is(err, ErrIdentityConflict) {
		t.Fatalf("content locator conflict error=%v", err)
	}
	stored, err := j.Receipt(context.Background(), base.ReceiptID)
	if err != nil || stored.State != mektup.StateReplyObserved || stored.ContentRef == nil || stored.ContentRef.ItemID != "item-1" || len(stored.Evidence) < 2 || len(stored.Warnings) != 1 {
		t.Fatalf("legitimate strengthening lost: %+v %v", stored, err)
	}
}

func TestPutReceiptConcurrentRebindOnlyOneWriterSucceeds(t *testing.T) {
	j := identityJournal(t)
	base := identityReceipt(t)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	operationIDs := []string{base.OperationID, mektup.NewOperationID()}
	for i := 0; i < 2; i++ {
		candidate := base
		candidate.OperationID = operationIDs[i]
		wg.Add(1)
		go func(r mektup.Receipt) {
			defer wg.Done()
			errs <- j.PutReceipt(context.Background(), r)
		}(candidate)
	}
	wg.Wait()
	close(errs)
	var successes, conflicts int
	for err := range errs {
		if err == nil {
			successes++
		} else if errors.Is(err, ErrIdentityConflict) {
			conflicts++
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("concurrent writes successes=%d conflicts=%d", successes, conflicts)
	}
	stored, err := j.Receipt(context.Background(), base.ReceiptID)
	if err != nil || (stored.OperationID != operationIDs[0] && stored.OperationID != operationIDs[1]) {
		t.Fatalf("concurrent rebind clobbered receipt: %+v %v", stored, err)
	}
}
