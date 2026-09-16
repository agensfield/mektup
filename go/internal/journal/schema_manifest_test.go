package journal

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"strings"
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

func TestCurrentSchemaRejectsWeakenedCheckExpression(t *testing.T) {
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
	var ddl string
	if err := db.QueryRow("SELECT sql FROM sqlite_master WHERE name='operations'").Scan(&ddl); err != nil {
		t.Fatal(err)
	}
	changed := strings.Replace(ddl, "CHECK(body_size >= 0)", "CHECK(body_size >= 0 OR 1)", 1)
	if changed == ddl {
		t.Fatalf("fixture check absent: %s", ddl)
	}
	if _, err := db.Exec("PRAGMA writable_schema=ON"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE sqlite_master SET sql=? WHERE name='operations'", changed); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := CheckPath(context.Background(), path); err == nil || !errors.Is(err, ErrStorageCorrupt) {
		t.Fatalf("weakened check accepted by CheckPath: %v", err)
	}
	if reopened, err := Open(context.Background(), Options{StateDir: dir}); err == nil {
		reopened.Close()
		t.Fatal("weakened check accepted by Open")
	}
}

func TestCurrentSchemaRejectsCheckTextInsideSQLComment(t *testing.T) {
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
	var ddl string
	if err := db.QueryRow("SELECT sql FROM sqlite_master WHERE name='operations'").Scan(&ddl); err != nil {
		t.Fatal(err)
	}
	changed := strings.Replace(ddl, "CHECK(body_size >= 0)", "/* CHECK(body_size >= 0) */", 1)
	if changed == ddl {
		t.Fatalf("fixture check absent: %s", ddl)
	}
	if _, err := db.Exec("PRAGMA writable_schema=ON"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE sqlite_master SET sql=? WHERE name='operations'", changed); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := CheckPath(context.Background(), path); err == nil {
		t.Fatal("CheckPath accepted CHECK text that exists only in a comment")
	}
	if reopened, err := Open(context.Background(), Options{StateDir: dir}); err == nil {
		reopened.Close()
		t.Fatal("Open accepted CHECK text that exists only in a comment")
	}
}

func TestV7MigrationCannotPublishSchemaRejectedByV8Validation(t *testing.T) {
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
	var ddl string
	if err := db.QueryRow("SELECT sql FROM sqlite_master WHERE name='observations'").Scan(&ddl); err != nil {
		t.Fatal(err)
	}
	changed := strings.Replace(ddl, "REFERENCES reply_claims(reply_id) ON DELETE CASCADE", "", 1)
	if changed == ddl {
		t.Fatalf("fixture foreign key absent: %s", ddl)
	}
	if _, err := db.Exec("PRAGMA writable_schema=ON"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE sqlite_master SET sql=? WHERE name='observations'", changed); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("PRAGMA user_version=7"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if migrated, err := Open(context.Background(), Options{StateDir: dir}); err == nil {
		migrated.Close()
		t.Fatal("migration published malformed v8 schema")
	}
	db, err = sql.Open("sqlite", "file:"+escapedSQLitePath(path)+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var version int
	if err := db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 7 {
		t.Fatalf("failed migration published user_version=%d", version)
	}
}
