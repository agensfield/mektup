package journal

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
)

func TestCheckPathRejectsMalformedCurrentV8CustodyTables(t *testing.T) {
	for _, table := range []string{
		"operations", "attempts", "reply_claims", "reply_winners", "observations", "events",
		"manual_resolutions", "operation_acceptances", "reply_acceptances", "receipts", "blockers",
	} {
		t.Run(table, func(t *testing.T) {
			dir := t.TempDir()
			j, err := Open(context.Background(), Options{StateDir: dir})
			if err != nil {
				t.Fatal(err)
			}
			if err := j.Close(); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "journal.sqlite3")
			db, err := sql.Open("sqlite", "file:"+escapedSQLitePath(path)+"?mode=rw")
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec("PRAGMA foreign_keys=OFF"); err != nil {
				t.Fatal(err)
			}
			legacy := table + "_manifest_legacy"
			if _, err := db.Exec("ALTER TABLE " + table + " RENAME TO " + legacy); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec("CREATE TABLE " + table + " AS SELECT * FROM " + legacy + " WHERE 0"); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec("DROP TABLE " + legacy); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := CheckPath(context.Background(), path); err == nil || !errors.Is(err, ErrStorageCorrupt) {
				t.Fatalf("malformed %s was accepted: %v", table, err)
			}
		})
	}
}
