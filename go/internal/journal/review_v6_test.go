package journal

import (
	"context"
	"database/sql"
	"path/filepath"
	"strconv"
	"testing"
)

func TestReplyTupleColumnShapeFailsClosedAndMigrationRollsBack(t *testing.T) {
	for _, test := range []struct {
		name, declaration string
		migration         bool
	}{
		{"wrong type", "INTEGER NOT NULL DEFAULT 0", false},
		{"nullable", "TEXT", false},
		{"migration wrong type", "INTEGER NOT NULL DEFAULT 0", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			j, err := Open(ctx, Options{StateDir: dir})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := j.db.Exec("ALTER TABLE operations DROP COLUMN reply_endpoint_id"); err != nil {
				t.Fatal(err)
			}
			if _, err := j.db.Exec("ALTER TABLE operations ADD COLUMN reply_endpoint_id " + test.declaration); err != nil {
				t.Fatal(err)
			}
			version := 6
			if test.migration {
				version = 5
			}
			if _, err := j.db.Exec("PRAGMA user_version=" + strconv.Itoa(version)); err != nil {
				t.Fatal(err)
			}
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			if !test.migration {
				if _, err := CheckPath(ctx, filepath.Join(dir, "journal.sqlite3")); err == nil {
					t.Fatal("CheckPath accepted malformed current schema")
				}
			}
			if opened, err := Open(ctx, Options{StateDir: dir}); err == nil {
				_ = opened.Close()
				t.Fatal("Open accepted malformed tuple column")
			}
			db, err := sql.Open("sqlite", "file:"+escapedSQLitePath(filepath.Join(dir, "journal.sqlite3"))+"?mode=ro")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var got int
			if err := db.QueryRowContext(ctx, "PRAGMA user_version").Scan(&got); err != nil {
				t.Fatal(err)
			}
			if test.migration && got != 5 {
				t.Fatalf("failed migration published version %d", got)
			}
		})
	}
}
