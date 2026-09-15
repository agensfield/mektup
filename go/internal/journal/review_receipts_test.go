package journal

import (
	"context"
	"errors"
	"path/filepath"
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

func TestV5BlockerStructureFailsClosed(t *testing.T) {
	for _, shape := range []string{"missing-columns", "missing-primary-key"} {
		t.Run(shape, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			j, err := Open(ctx, Options{StateDir: dir})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := j.db.Exec("ALTER TABLE blockers RENAME TO old_blockers"); err != nil {
				t.Fatal(err)
			}
			shapeSQL := "CREATE TABLE blockers(generation TEXT)"
			if shape == "missing-primary-key" {
				shapeSQL = "CREATE TABLE blockers AS SELECT * FROM old_blockers WHERE 0"
			}
			if _, err := j.db.Exec(shapeSQL); err != nil {
				t.Fatal(err)
			}
			if _, err := j.db.Exec("DROP TABLE old_blockers"); err != nil {
				t.Fatal(err)
			}
			_ = j.Close()
			if _, err := CheckPath(ctx, filepath.Join(dir, "journal.sqlite3")); err == nil {
				t.Fatal("read-only check accepted malformed blockers")
			}
			if again, err := Open(ctx, Options{StateDir: dir}); err == nil {
				again.Close()
				t.Fatal("open accepted malformed blockers")
			}
		})
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
