package journal

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

func TestV4ToV5MalformedBlockerMigrationRollsBack(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	j, err := Open(ctx, Options{StateDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.db.Exec("ALTER TABLE blockers RENAME TO old_blockers"); err != nil {
		t.Fatal(err)
	}
	if _, err := j.db.Exec("CREATE TABLE blockers AS SELECT * FROM old_blockers WHERE 0"); err != nil {
		t.Fatal(err)
	}
	if _, err := j.db.Exec("DROP TABLE old_blockers"); err != nil {
		t.Fatal(err)
	}
	if _, err := j.db.Exec("PRAGMA user_version=4"); err != nil {
		t.Fatal(err)
	}
	_ = j.Close()
	if migrated, err := Open(ctx, Options{StateDir: dir}); err == nil {
		migrated.Close()
		t.Fatal("malformed v4 supplemental schema was published as v5")
	}
	db, err := sql.Open("sqlite", "file:"+escapedSQLitePath(filepath.Join(dir, "journal.sqlite3"))+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version int
	if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 4 {
		t.Fatalf("failed migration published version %d", version)
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
