package journal

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func portableImportOperation(number string) Operation {
	return Operation{
		OperationID:     "op_0198f0e0-0000-7000-8000-000000000" + number,
		MessageID:       "msg_0198f0e0-0000-7000-8000-000000000" + number,
		SourceRoute:     "codex://source/thread/source",
		TargetRoute:     "codex://target/thread/target",
		Semantics:       "message",
		ReplyRoute:      "codex://source/thread/source",
		ReplyEndpointID: "ep_0198f0e0-0000-7000-8000-000000000001",
		CustodyRoute:    "ep_0198f0e0-0000-7000-8000-000000000002",
		CustodyStoreID:  "store_0198f0e0-0000-7000-8000-000000000003",
		Digest:          "sha256:" + strings.Repeat("b", 64),
		BodySize:        4,
	}
}

func portableImportReply(number string) string {
	return "msg_0198f0e0-0000-7000-8000-000000000" + number
}

func portableImportInput(op Operation, selection, reply string) OriginalStatusImport {
	in := OriginalStatusImport{Operation: op, Selection: selection, ReplyID: reply, Digest: "sha256:" + strings.Repeat("a", 64), BodySize: 4, Status: "success"}
	if selection == OriginalStatusWinner {
		in.CommitSeq = 17
	} else if selection == OriginalStatusTerminalUnknown {
		in.EventSeq = 23
	}
	return in
}

func TestImportOriginalStatusPendingCreatesNoAttempt(t *testing.T) {
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	j := testJournal(t, t.TempDir(), &now)
	op := portableImportOperation("010")
	if err := j.ImportOriginalStatus(context.Background(), OriginalStatusImport{Operation: op, Selection: OriginalStatusPending}); err != nil {
		t.Fatal(err)
	}
	got, err := j.Operation(context.Background(), op.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != StatePrepared || got.AttemptToken != "" {
		t.Fatalf("imported pending operation=%+v", got)
	}
	var attempts int
	if err := j.db.QueryRow("SELECT COUNT(1) FROM attempts WHERE operation_id=?", op.OperationID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 0 {
		t.Fatalf("portable import created %d attempt rows", attempts)
	}
}

func TestImportOriginalStatusPreservesExistingAttemptAuthority(t *testing.T) {
	for _, selection := range []string{OriginalStatusWinner, OriginalStatusTerminalUnknown} {
		t.Run(selection, func(t *testing.T) {
			var now atomic.Int64
			now.Store(time.Now().UnixNano())
			j := testJournal(t, t.TempDir(), &now)
			prepared := prepared(t, j)
			in := portableImportInput(prepared.Operation, selection, portableImportReply("017"))
			if selection == OriginalStatusTerminalUnknown {
				in.EventSeq = 17
			}
			var owner, token string
			var lease int64
			if err := j.db.QueryRow("SELECT owner,token,lease_until FROM attempts WHERE operation_id=?", prepared.OperationID).Scan(&owner, &token, &lease); err != nil {
				t.Fatal(err)
			}
			if err := j.ImportOriginalStatus(context.Background(), in); err != nil {
				t.Fatal(err)
			}
			var afterOwner, afterToken string
			var afterLease int64
			if err := j.db.QueryRow("SELECT owner,token,lease_until FROM attempts WHERE operation_id=?", prepared.OperationID).Scan(&afterOwner, &afterToken, &afterLease); err != nil {
				t.Fatal(err)
			}
			if owner != afterOwner || token != afterToken || lease != afterLease {
				t.Fatalf("portable projection changed dispatch authority: before=%q/%q/%d after=%q/%q/%d", owner, token, lease, afterOwner, afterToken, afterLease)
			}
		})
	}
}

func TestImportOriginalStatusWinnerIsIdempotentAcrossRestart(t *testing.T) {
	dir := t.TempDir()
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	j := testJournal(t, dir, &now)
	op := portableImportOperation("011")
	in := portableImportInput(op, OriginalStatusWinner, portableImportReply("011"))
	if err := j.ImportOriginalStatus(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if err := j.ImportOriginalStatus(context.Background(), in); err != nil {
		t.Fatalf("identical winner import was not idempotent: %v", err)
	}
	var events int
	if err := j.db.QueryRow("SELECT COUNT(1) FROM events WHERE reply_id=?", in.ReplyID).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if events != 1 {
		t.Fatalf("idempotent winner emitted %d events", events)
	}
	_ = j.Close()
	j, err := Open(context.Background(), Options{StateDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	status, err := j.OriginalStatus(context.Background(), op.MessageID)
	if err != nil {
		t.Fatal(err)
	}
	if status.Selection != OriginalStatusWinner || status.Claim.ReplyID != in.ReplyID || status.Claim.CommitSeq != in.CommitSeq {
		t.Fatalf("winner lost across restart: %+v", status)
	}
}

func TestImportOriginalStatusUnknownPreservesRemoteEventOrderingAndIsIdempotent(t *testing.T) {
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	j := testJournal(t, t.TempDir(), &now)
	op := portableImportOperation("012")
	in := portableImportInput(op, OriginalStatusTerminalUnknown, portableImportReply("012"))
	if err := j.ImportOriginalStatus(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	if err := j.ImportOriginalStatus(context.Background(), in); err != nil {
		t.Fatalf("identical unknown import was not idempotent: %v", err)
	}
	var seq int64
	if err := j.db.QueryRow("SELECT seq FROM events WHERE reply_id=?", in.ReplyID).Scan(&seq); err != nil {
		t.Fatal(err)
	}
	if seq < 1 || seq == in.EventSeq {
		t.Fatalf("remote event sequence was used as local key: got %d remote %d", seq, in.EventSeq)
	}
	status, err := j.OriginalStatus(context.Background(), op.MessageID)
	if err != nil {
		t.Fatal(err)
	}
	if status.Selection != OriginalStatusTerminalUnknown || status.TerminalEventSeq != seq {
		t.Fatalf("unknown selection=%+v local_seq=%d", status, seq)
	}
}

func TestImportOriginalStatusUnknownMapsRemoteEventToFreshLocalSequence(t *testing.T) {
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	j := testJournal(t, t.TempDir(), &now)
	prepared(t, j) // unrelated local event occupies sequence one
	op := portableImportOperation("016")
	in := portableImportInput(op, OriginalStatusTerminalUnknown, portableImportReply("016"))
	in.EventSeq = 1 // valid remotely, but not a local SQLite primary key
	if err := j.ImportOriginalStatus(context.Background(), in); err != nil {
		t.Fatalf("remote/local sequence collision rejected import: %v", err)
	}
	var seq int64
	if err := j.db.QueryRow("SELECT seq FROM events WHERE reply_id=?", in.ReplyID).Scan(&seq); err != nil {
		t.Fatal(err)
	}
	if seq <= 1 {
		t.Fatalf("import did not allocate a fresh local event sequence: %d", seq)
	}
	if err := j.ImportOriginalStatus(context.Background(), in); err != nil {
		t.Fatalf("repeated unknown import was not idempotent: %v", err)
	}
	status, err := j.OriginalStatus(context.Background(), op.MessageID)
	if err != nil || status.TerminalEventSeq != seq {
		t.Fatalf("local event selection=%+v err=%v", status, err)
	}
}

func TestImportOriginalStatusRollsBackOnSQLFailure(t *testing.T) {
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	j := testJournal(t, t.TempDir(), &now)
	op := portableImportOperation("013")
	in := portableImportInput(op, OriginalStatusWinner, portableImportReply("013"))
	if _, err := j.db.Exec("DROP TABLE reply_claims"); err != nil {
		t.Fatal(err)
	}
	if err := j.ImportOriginalStatus(context.Background(), in); err == nil {
		t.Fatal("missing reply table did not fail import")
	}
	if _, err := j.Operation(context.Background(), op.OperationID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("operation survived failed import: %v", err)
	}
}

func TestImportOriginalStatusRejectsReplyOwnedByAnotherOriginalWithoutImportingOperation(t *testing.T) {
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	j := testJournal(t, t.TempDir(), &now)
	first := prepared(t, j)
	input := claimInput()
	input.ReplyID = portableImportReply("015")
	claim, err := j.ClaimReply(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if claim.OriginalID != first.MessageID {
		t.Fatalf("unexpected claim original %q", claim.OriginalID)
	}
	second := portableImportOperation("014")
	in := portableImportInput(second, OriginalStatusWinner, claim.ReplyID)
	if err := j.ImportOriginalStatus(context.Background(), in); !errors.Is(err, ErrIdentityConflict) {
		t.Fatalf("reply cross-original conflict: %v", err)
	}
	if _, err := j.Operation(context.Background(), second.OperationID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("conflicted operation was imported: %v", err)
	}
	var count int
	if err := j.db.QueryRow("SELECT COUNT(1) FROM operations WHERE operation_id=?", second.OperationID).Scan(&count); err != nil && !errors.Is(err, sql.ErrNoRows) {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("conflicted operation count=%d", count)
	}
}
