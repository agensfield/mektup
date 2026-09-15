package journal

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestBlockerUpsertIsOneMetadataRowPerCorrelation(t *testing.T) {
	clock := &atomic.Int64{}
	clock.Store(time.Now().UnixNano())
	j := testJournal(t, t.TempDir(), clock)
	first := time.Unix(1_700_000_000, 0).UTC()
	last := first.Add(time.Minute)
	if err := j.UpsertBlocker(context.Background(), BlockerObservation{Method: "item/requestUserInput", CorrelationID: "req-1", SeenAt: first, EndpointID: "ep-1", ThreadID: "thread-1", OperationID: "op-1"}); err != nil {
		t.Fatal(err)
	}
	if err := j.UpsertBlocker(context.Background(), BlockerObservation{Method: "item/requestUserInput", CorrelationID: "req-1", SeenAt: last, EndpointID: "ep-1", ThreadID: "thread-1", MessageID: "msg-1"}); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := j.db.QueryRow("SELECT COUNT(*) FROM blockers").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("blocker rows = %d", count)
	}
	rows, err := j.ListBlockers(context.Background(), BlockerQuery{ThreadID: "thread-1", Limit: 1})
	if err != nil || len(rows) != 1 {
		t.Fatalf("list = %#v, %v", rows, err)
	}
	if !rows[0].FirstSeen.Equal(first) || !rows[0].LastSeen.Equal(last) || rows[0].OperationID != "op-1" || rows[0].MessageID != "msg-1" {
		t.Fatalf("blocker metadata = %#v", rows[0])
	}
	var payload string
	if err := j.db.QueryRow("SELECT method||correlation_id||thread_id||operation_id||message_id FROM blockers").Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if payload == "" {
		t.Fatal("empty blocker metadata")
	}
}
