package receipts

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	mektup "github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/internal/journal"
)

type historyStub struct{ items []HistoryItem }

func (h historyStub) FullHistory(context.Context, string, string) ([]HistoryItem, error) {
	return h.items, nil
}

type gateStub struct {
	intent ResolveIntent
}

func (g *gateStub) Authorize(_ context.Context, intent ResolveIntent) (string, error) {
	g.intent = intent
	return "one-time-human-gate", nil
}

func testReceipt(t *testing.T, state mektup.EvidenceState) mektup.Receipt {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	digest := "sha256:" + strings.Repeat("a", 64)
	return mektup.Receipt{
		Schema: mektup.ReceiptSchema, ReceiptID: mektup.NewReceiptID(), OperationID: mektup.NewOperationID(), Operation: "send", State: state,
		Source:   mektup.ReceiptIdentity{EndpointID: mektup.NewEndpointID(), ThreadID: "thread-source"},
		Target:   mektup.ReceiptIdentity{EndpointID: mektup.NewEndpointID(), ThreadID: "thread-target"},
		Message:  mektup.ReceiptMessage{MessageID: mektup.NewMessageID(), Kind: string(mektup.KindMessage), PayloadBytes: 4, PayloadSHA256: digest},
		Evidence: []mektup.EvidenceRecord{{State: state, At: now, Kind: "test"}}, Warnings: []mektup.Warning{}, CreatedAt: now, UpdatedAt: now,
	}
}

func openStore(t *testing.T) (*Store, *journal.Journal) {
	t.Helper()
	j, err := journal.Open(context.Background(), journal.Options{StateDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = j.Close() })
	return &Store{Journal: j, Now: func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }}, j
}

func TestImportRejectsForgedBodyAndDoesNotTrustClaim(t *testing.T) {
	receipt := testReceipt(t, mektup.StateOutcomeUnknown)
	encoded, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	var object map[string]any
	if err := json.Unmarshal(encoded, &object); err != nil {
		t.Fatal(err)
	}
	message := object["message"].(map[string]any)
	message["body"] = "forged payload"
	encoded, _ = json.Marshal(object)
	if _, err := Import(encoded); !errors.Is(err, ErrUntrusted) {
		t.Fatalf("forged import error = %v", err)
	}
	trusted, err := Import(mustJSON(t, receipt))
	if err != nil {
		t.Fatal(err)
	}
	if trusted.Trusted {
		t.Fatal("portable receipt was granted trust")
	}
}

func TestListShowAndContentAreBoundedAndMetadataOnly(t *testing.T) {
	store, _ := openStore(t)
	receipt := testReceipt(t, mektup.StateAccepted)
	if err := store.Save(context.Background(), receipt); err != nil {
		t.Fatal(err)
	}
	got, err := store.List(context.Background(), ListOptions{Limit: 1})
	if err != nil || len(got) != 1 {
		t.Fatalf("list = %v, %v", got, err)
	}
	if _, err := store.List(context.Background(), ListOptions{Limit: MaxLimit + 1}); !errors.Is(err, ErrLimit) {
		t.Fatalf("unbounded list error = %v", err)
	}
	shown, err := store.Show(context.Background(), receipt.ReceiptID, ShowOptions{})
	if err != nil || shown.ReceiptID != receipt.ReceiptID {
		t.Fatalf("show = %v, %v", shown, err)
	}
	if _, err := store.Show(context.Background(), receipt.ReceiptID, ShowOptions{Portable: true, Content: true}); !errors.Is(err, ErrPortableContent) {
		t.Fatalf("portable/content error = %v", err)
	}
}

func TestResolveRequiresGateAndPreservesObservedEvidence(t *testing.T) {
	store, j := openStore(t)
	receipt := testReceipt(t, mektup.StateOutcomeUnknown)
	receipt.Evidence = append(receipt.Evidence, mektup.EvidenceRecord{State: mektup.StateDispatchStarted, Kind: "original"})
	if _, err := j.Prepare(context.Background(), journal.Operation{OperationID: receipt.OperationID, MessageID: receipt.Message.MessageID, SourceRoute: "src", TargetRoute: "dst", Semantics: "send", Digest: receipt.Message.PayloadSHA256, BodySize: 4}); err != nil {
		t.Fatal(err)
	}
	if err := j.MarkDispatchStarted(context.Background(), receipt.OperationID); err != nil {
		t.Fatal(err)
	}
	if err := j.RecordResult(context.Background(), receipt.OperationID, journal.StateOutcomeUnknown, "lost"); err != nil {
		t.Fatal(err)
	}
	if err := store.Save(context.Background(), receipt); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Resolve(context.Background(), ResolveRequest{Reference: receipt.ReceiptID}); !errors.Is(err, ErrHumanGateRequired) {
		t.Fatalf("missing gate error = %v", err)
	}
	gate := &gateStub{}
	resolved, err := store.Resolve(context.Background(), ResolveRequest{Reference: receipt.ReceiptID, Assertion: "not_delivered", Actor: "operator", Reason: "external evidence", EvidenceRef: "incident-1", Presentation: "human", Intent: "receipt.resolve", Gate: gate})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.State != mektup.StateManuallyResolved || resolved.ManualResolution == nil || len(resolved.Evidence) != 2 {
		t.Fatalf("resolved receipt = %+v", resolved)
	}
	if gate.intent.Intent != "receipt.resolve" {
		t.Fatalf("gate intent = %+v", gate.intent)
	}
}

func TestContentExactLocatorAndDigestMiss(t *testing.T) {
	store, _ := openStore(t)
	body := []byte("reply")
	digest := bodyDigest(body)
	receipt := testReceipt(t, mektup.StateReplyAccepted)
	receipt.ContentRef = &mektup.ContentRef{EndpointID: receipt.Source.EndpointID, ThreadID: receipt.Source.ThreadID, TurnID: "turn-1", ItemID: "item-1", ClientMessageID: "reply-client", PayloadBytes: uint64(len(body)), PayloadSHA256: digest}
	if err := store.Save(context.Background(), receipt); err != nil {
		t.Fatal(err)
	}
	history := historyStub{items: []HistoryItem{{EndpointID: receipt.Source.EndpointID, ThreadID: receipt.Source.ThreadID, TurnID: "turn-1", ItemID: "item-1", ClientMessageID: "reply-client", InReplyTo: receipt.Message.MessageID, Body: body}}}
	got, err := store.Content(context.Background(), receipt.ReceiptID, history, ContentOptions{})
	if err != nil || string(got.Body) != string(body) {
		t.Fatalf("content = %+v, %v", got, err)
	}
	history.items[0].Body = []byte("xxxxx")
	if _, err := store.Content(context.Background(), receipt.ReceiptID, history, ContentOptions{}); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("digest miss = %v", err)
	}
}

func mustJSON(t *testing.T, receipt mektup.Receipt) []byte {
	t.Helper()
	data, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
