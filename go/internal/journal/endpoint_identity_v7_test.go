package journal

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
)

func TestV6ToV7PreservesPopulatedOperationMetadata(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	j, err := Open(ctx, Options{StateDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.Prepare(ctx, Operation{OperationID: "v6-op", MessageID: "v6-msg", SourceRoute: "src", TargetRoute: "dst", Semantics: "message", Digest: "sha256:v6", BodySize: 3}); err != nil {
		t.Fatal(err)
	}
	if _, err := j.db.Exec("ALTER TABLE operations DROP COLUMN source_endpoint_id"); err != nil {
		t.Fatal(err)
	}
	if _, err := j.db.Exec("ALTER TABLE operations DROP COLUMN target_endpoint_id"); err != nil {
		t.Fatal(err)
	}
	if _, err := j.db.Exec("PRAGMA user_version=6"); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	k, err := Open(ctx, Options{StateDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer k.Close()
	r, err := k.Operation(ctx, "v6-op")
	if err != nil {
		t.Fatal(err)
	}
	if r.MessageID != "v6-msg" || r.Digest != "sha256:v6" || r.BodySize != 3 || r.SourceEndpointID != "" || r.TargetEndpointID != "" {
		t.Fatalf("v6 preservation: %+v", r)
	}
}

func TestV7MalformedEndpointShapeFailsAndRollsBackMigration(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	j, err := Open(ctx, Options{StateDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.db.Exec("ALTER TABLE operations DROP COLUMN source_endpoint_id"); err != nil {
		t.Fatal(err)
	}
	if _, err := j.db.Exec("ALTER TABLE operations ADD COLUMN source_endpoint_id INTEGER NOT NULL DEFAULT 0"); err != nil {
		t.Fatal(err)
	}
	if _, err := j.db.Exec("PRAGMA user_version=6"); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "journal.sqlite3")
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(ctx, Options{StateDir: dir}); err == nil || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("malformed migration error: %v", err)
	}
	db, err := sql.Open("sqlite", "file:"+escapedSQLitePath(path)+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version int
	if err := db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 6 {
		t.Fatalf("failed migration published version %d", version)
	}
}
