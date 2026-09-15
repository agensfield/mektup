package journal

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestCurrentSchemaMissingReceiptsFailsClosed(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	j, err := Open(ctx, Options{StateDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.db.Exec("DROP TABLE receipts"); err != nil {
		t.Fatal(err)
	}
	_ = j.Close()
	if again, err := Open(ctx, Options{StateDir: dir}); err == nil {
		_ = again.Close()
		t.Fatal("current schema missing receipts silently recreated")
	} else if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("missing schema error: %v", err)
	}
}

func TestBlockerIdentityDoesNotCrossEndpointsOrGenerations(t *testing.T) {
	j, err := Open(context.Background(), Options{StateDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	now := time.Now().UTC()
	for _, observation := range []BlockerObservation{{Method: "approval", CorrelationID: "1", Generation: "gen-a", EndpointID: "endpoint-a", SeenAt: now}, {Method: "approval", CorrelationID: "1", Generation: "gen-a", EndpointID: "endpoint-b", SeenAt: now}, {Method: "approval", CorrelationID: "1", Generation: "gen-b", EndpointID: "endpoint-a", SeenAt: now}} {
		if err := j.UpsertBlocker(context.Background(), observation); err != nil {
			t.Fatal(err)
		}
	}
	all, err := j.ListBlockers(context.Background(), BlockerQuery{})
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Fatalf("distinct blocker requests collapsed to %d", len(all))
	}
}
