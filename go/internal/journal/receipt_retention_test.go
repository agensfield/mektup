package journal

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	mektup "github.com/agensfield/mektup/go"
)

func TestReceiptAndResolvedBlockerRetentionIsBounded(t *testing.T) {
	clock := &atomic.Int64{}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock.Store(now.UnixNano())
	j := testJournal(t, t.TempDir(), clock)
	old := now.Add(-RetentionAge - time.Hour).Format(time.RFC3339Nano)
	receipt := mektup.Receipt{Schema: mektup.ReceiptSchema, ReceiptID: mektup.NewReceiptID(), OperationID: mektup.NewOperationID(), Operation: "send", State: mektup.StateAccepted,
		Source: mektup.ReceiptIdentity{EndpointID: mektup.NewEndpointID(), ThreadID: "source"}, Target: mektup.ReceiptIdentity{EndpointID: mektup.NewEndpointID(), ThreadID: "target"},
		Message:  mektup.ReceiptMessage{MessageID: mektup.NewMessageID(), Kind: string(mektup.KindMessage), PayloadBytes: 0, PayloadSHA256: "sha256:" + strings.Repeat("0", 64)},
		Evidence: []mektup.EvidenceRecord{}, Warnings: []mektup.Warning{}, CreatedAt: old, UpdatedAt: old}
	if err := j.PutReceipt(context.Background(), receipt); err != nil {
		t.Fatal(err)
	}
	resolved := now.Add(-RetentionAge - time.Hour)
	if err := j.UpsertBlocker(context.Background(), BlockerObservation{Method: "item/requestUserInput", CorrelationID: "old", SeenAt: resolved, ResolvedAt: &resolved}); err != nil {
		t.Fatal(err)
	}
	maintenance, err := j.StorageMaintain(context.Background(), MaintenanceOptions{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if maintenance.Actions[1].Eligible != 1 || maintenance.Actions[1].Changed != 1 {
		t.Fatalf("retention receipt = %#v", maintenance)
	}
	if _, err := j.Receipt(context.Background(), receipt.ReceiptID); err != nil {
		t.Fatalf("protected receipt after retention = %v", err)
	}
	blockers, err := j.ListBlockers(context.Background(), BlockerQuery{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(blockers) != 0 {
		t.Fatalf("resolved blocker survived retention: %#v", blockers)
	}
}
