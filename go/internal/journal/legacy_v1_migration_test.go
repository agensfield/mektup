package journal

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

const authenticSchemaV1 = `
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS operations (
 operation_id TEXT PRIMARY KEY, message_id TEXT NOT NULL UNIQUE,
 source_route TEXT NOT NULL, target_route TEXT NOT NULL, semantics TEXT NOT NULL,
 digest TEXT NOT NULL, body_size INTEGER NOT NULL CHECK(body_size >= 0),
 state TEXT NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
 dispatch_started_at INTEGER, terminal_at INTEGER, error_code TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS attempts (
 operation_id TEXT PRIMARY KEY REFERENCES operations(operation_id) ON DELETE CASCADE,
 state TEXT NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS reply_claims (
 reply_id TEXT PRIMARY KEY, original_id TEXT NOT NULL,
 digest TEXT NOT NULL, body_size INTEGER NOT NULL CHECK(body_size >= 0),
 status TEXT NOT NULL CHECK(status IN ('success','error')),
 reply_route TEXT NOT NULL, custody_route TEXT NOT NULL,
 owner TEXT NOT NULL, token TEXT NOT NULL, lease_until INTEGER NOT NULL,
 state TEXT NOT NULL, created_at INTEGER NOT NULL, updated_at INTEGER NOT NULL,
 accepted_at INTEGER, commit_seq INTEGER, error_code TEXT NOT NULL DEFAULT '',
 UNIQUE(reply_id, original_id)
);
CREATE INDEX IF NOT EXISTS reply_claims_original ON reply_claims(original_id);
CREATE TABLE IF NOT EXISTS reply_winners (
 original_id TEXT PRIMARY KEY, reply_id TEXT NOT NULL REFERENCES reply_claims(reply_id),
 committed_at INTEGER NOT NULL, commit_seq INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS observations (
 reply_id TEXT PRIMARY KEY REFERENCES reply_claims(reply_id) ON DELETE CASCADE,
 native_item_id TEXT NOT NULL, observed_at INTEGER NOT NULL,
 digest TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS events (
 seq INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL,
 operation_id TEXT, reply_id TEXT, state TEXT NOT NULL, at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS events_operation ON events(operation_id, seq);
`

func TestAuthenticV1MigrationPreservesDependentCustodyGraph(t *testing.T) {
	for _, populated := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "populated"}[populated], func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "journal.sqlite3")
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(authenticSchemaV1); err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec("PRAGMA user_version=1"); err != nil {
				t.Fatal(err)
			}
			if populated {
				for _, statement := range []string{
					"INSERT INTO operations VALUES('op-old','msg-old','src','dst','send','digest',4,'accepted',1,1,1,1,'')",
					"INSERT INTO reply_claims VALUES('reply-old','msg-old','reply-digest',4,'success','reply','custody','owner','',0,'reply_observed',1,1,1,1,'')",
					"INSERT INTO reply_winners VALUES('msg-old','reply-old',1,1)",
					"INSERT INTO observations VALUES('reply-old','native-old',1,'reply-digest')",
				} {
					if _, err := db.Exec(statement); err != nil {
						t.Fatal(err)
					}
				}
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(path, 0o600); err != nil {
				t.Fatal(err)
			}
			journal, err := Open(context.Background(), Options{StateDir: dir})
			if err != nil {
				t.Fatalf("authentic v1 migration: %v", err)
			}
			if populated {
				var observations int
				if err := journal.db.QueryRow("SELECT COUNT(*) FROM observations WHERE reply_id='reply-old'").Scan(&observations); err != nil {
					t.Fatal(err)
				}
				if observations != 1 {
					t.Fatalf("lost original observation count=%d", observations)
				}
				winner, _, _, err := journal.Winner(context.Background(), "msg-old")
				if err != nil || winner != "reply-old" {
					t.Fatalf("winner=%q err=%v", winner, err)
				}
			}
			if err := journal.Close(); err != nil {
				t.Fatal(err)
			}
			if _, err := CheckPath(context.Background(), path); err != nil {
				t.Fatalf("migrated v1 failed CheckPath: %v", err)
			}
		})
	}
}
