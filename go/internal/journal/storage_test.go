package journal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestStorageCheckIsReadOnlyAndStatusIsBounded(t *testing.T) {
	dir := t.TempDir()
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	j := testJournal(t, dir, &now)
	path := filepath.Join(dir, "journal.sqlite3")
	beforeInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	check, err := j.StorageCheck(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !check.ReadOnly || check.Integrity != "ok" {
		t.Fatalf("unexpected check %#v", check)
	}
	status, err := j.StorageStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.DatabaseBytes != int64(len(before)) || status.SchemaVersion != 4 || status.JournalMode != "wal" {
		t.Fatalf("unexpected status %#v", status)
	}
	afterInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !beforeInfo.ModTime().Equal(afterInfo.ModTime()) || string(before) != string(after) {
		t.Fatal("storage check/status changed database bytes or mtime")
	}
	pathCheck, err := CheckPath(context.Background(), path)
	if err != nil || !pathCheck.ReadOnly {
		t.Fatalf("read-only path check: %#v %v", pathCheck, err)
	}
}

func TestStorageMaintainHonorsThirtyDayRetentionAndSeparatesActions(t *testing.T) {
	dir := t.TempDir()
	var now atomic.Int64
	now.Store(time.Now().UnixNano() + 100*int64(time.Hour))
	j := testJournal(t, dir, &now)
	old := now.Load() - int64(31*24*time.Hour)
	// An old completed operation is eligible.
	if _, err := j.Prepare(context.Background(), Operation{OperationID: "old-op", MessageID: "old-msg", SourceRoute: "src", TargetRoute: "dst", Semantics: "send", AttemptOwner: "owner", Digest: "old-digest", BodySize: 1}); err != nil {
		t.Fatal(err)
	}
	if err := j.MarkDispatchStarted(context.Background(), "old-op"); err != nil {
		t.Fatal(err)
	}
	if err := j.RecordResult(context.Background(), "old-op", StateAccepted, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := j.db.Exec("UPDATE operations SET created_at=?,updated_at=?,terminal_at=? WHERE operation_id=?", old, old, old, "old-op"); err != nil {
		t.Fatal(err)
	}
	// An old unknown operation must survive.
	if _, err := j.Prepare(context.Background(), Operation{OperationID: "unknown-op", MessageID: "unknown-msg", SourceRoute: "src", TargetRoute: "dst", Semantics: "send", AttemptOwner: "owner", Digest: "unknown-digest", BodySize: 1}); err != nil {
		t.Fatal(err)
	}
	if err := j.MarkDispatchStarted(context.Background(), "unknown-op"); err != nil {
		t.Fatal(err)
	}
	if err := j.RecordResult(context.Background(), "unknown-op", StateOutcomeUnknown, "lost"); err != nil {
		t.Fatal(err)
	}
	if _, err := j.db.Exec("UPDATE operations SET created_at=?,updated_at=?,terminal_at=? WHERE operation_id=?", old, old, old, "unknown-op"); err != nil {
		t.Fatal(err)
	}

	dry, err := j.StorageMaintain(context.Background(), MaintenanceOptions{DryRun: true})
	if err != nil {
		t.Fatal(err)
	}
	if !dry.DryRun || dry.Actions[1].Eligible != 1 || dry.Actions[1].Attempted {
		t.Fatalf("unexpected dry-run %#v", dry)
	}
	if _, err := j.Operation(context.Background(), "old-op"); err != nil {
		t.Fatal("dry-run removed eligible operation")
	}

	receipt, err := j.StorageMaintain(context.Background(), MaintenanceOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if receipt.Actions[1].Changed != 1 || !receipt.Actions[1].Applied || !receipt.Actions[2].Applied || !receipt.Actions[3].Applied {
		t.Fatalf("maintenance actions not separately applied: %#v", receipt)
	}
	if _, err := j.Operation(context.Background(), "old-op"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old operation remains: %v", err)
	}
	unknown, err := j.Operation(context.Background(), "unknown-op")
	if err != nil || unknown.State != StateOutcomeUnknown {
		t.Fatalf("unknown operation was pruned: %#v %v", unknown, err)
	}
}

func TestStorageVacuumReportsSizeAndIsExplicit(t *testing.T) {
	dir := t.TempDir()
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	j := testJournal(t, dir, &now)
	if _, err := j.db.Exec("CREATE TABLE vacuum_probe(value TEXT)"); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 64; i++ {
		if _, err := j.db.Exec("INSERT INTO vacuum_probe(value) VALUES(?)", strings.Repeat("x", 128)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := j.db.Exec("DELETE FROM vacuum_probe"); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(filepath.Join(dir, "journal.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := j.StorageVacuum(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !receipt.Applied || receipt.BeforeBytes != before.Size() || receipt.AfterBytes < 0 {
		t.Fatalf("unexpected vacuum receipt %#v", receipt)
	}
}

func TestStorageCheckCorruptDoesNotRebuildOrDelete(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "journal.sqlite3")
	original := []byte("not a sqlite database")
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	_, err := CheckPath(context.Background(), path)
	if err == nil || !errors.Is(err, ErrStorageCorrupt) {
		t.Fatalf("expected corrupt error, got %v", err)
	}
	after, readErr := os.ReadFile(path)
	if readErr != nil || string(after) != string(original) {
		t.Fatalf("corrupt file changed: %v %q", readErr, after)
	}
}

func TestStorageCheckUnreadablePathFailsWithoutCreation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "missing", "journal.sqlite3")
	_, err := CheckPath(context.Background(), path)
	if err == nil {
		t.Fatal("missing database unexpectedly checked successfully")
	}
	if _, statErr := os.Stat(filepath.Dir(path)); !os.IsNotExist(statErr) {
		t.Fatalf("read-only check created parent: %v", statErr)
	}
}

func TestStorageStatusBusyIsBoundedAndDoesNotRebuild(t *testing.T) {
	dir := t.TempDir()
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	j := testJournal(t, dir, &now)
	j.db.SetMaxOpenConns(1)
	conn, err := j.db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.ExecContext(context.Background(), "BEGIN EXCLUSIVE"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = j.StorageStatus(ctx)
	if err == nil || !errors.Is(err, ErrStorageBusy) {
		t.Fatalf("expected bounded busy error, got %v", err)
	}
	if _, err := conn.ExecContext(context.Background(), "ROLLBACK"); err != nil {
		t.Fatal(err)
	}
}
