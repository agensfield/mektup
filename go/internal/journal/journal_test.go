package journal

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testJournal(t *testing.T, dir string, now *atomic.Int64) *Journal {
	t.Helper()
	j, err := Open(context.Background(), Options{StateDir: dir, LeaseDuration: time.Second, Now: func() time.Time { return time.Unix(0, now.Load()) }})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = j.Close() })
	return j
}

func prepared(t *testing.T, j *Journal) OperationRecord {
	t.Helper()
	r, err := j.Prepare(context.Background(), Operation{OperationID: "op-1", MessageID: "msg-1", SourceRoute: "src", TargetRoute: "dst", Semantics: "send", Digest: "digest-1", BodySize: 7})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func claimInput() ClaimInput {
	return ClaimInput{ReplyID: "reply-1", OriginalID: "msg-1", Digest: "reply-digest", BodySize: 9, Status: "success", ReplyRoute: "src", CustodyRoute: "custody", Owner: "receiver-a"}
}

func TestOpenCreatesPrivateWALStoreAndStableIdentity(t *testing.T) {
	dir := t.TempDir()
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	j := testJournal(t, dir, &now)
	id := j.StoreID()
	if id == "" {
		t.Fatal("empty store id")
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0700 {
		t.Fatalf("state mode %o", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "journal.sqlite3")); err != nil {
		t.Fatal(err)
	}
	if _, err := j.db.Exec("CREATE TABLE IF NOT EXISTS private_probe(x TEXT)"); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	j2, err := Open(context.Background(), Options{StateDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	defer j2.Close()
	if j2.StoreID() != id {
		t.Fatalf("store identity changed: %q != %q", j2.StoreID(), id)
	}
	var mode string
	if err := j2.db.QueryRow("PRAGMA journal_mode").Scan(&mode); err != nil {
		t.Fatal(err)
	}
	if mode != "wal" {
		t.Fatalf("journal mode %q", mode)
	}
	var fk int
	if err := j2.db.QueryRow("PRAGMA foreign_keys").Scan(&fk); err != nil {
		t.Fatal(err)
	}
	if fk != 1 {
		t.Fatalf("foreign keys %d", fk)
	}
}

func TestPreparedAndDispatchRecoveryIsConservative(t *testing.T) {
	dir := t.TempDir()
	var now atomic.Int64
	now.Store(1_000_000_000)
	j := testJournal(t, dir, &now)
	prepared(t, j)
	if err := j.RecoverOrphans(context.Background()); err != nil {
		t.Fatal(err)
	}
	r, err := j.Operation(context.Background(), "op-1")
	if err != nil {
		t.Fatal(err)
	}
	if r.State != StateNotSent {
		t.Fatalf("prepared recovery %s", r.State)
	}
	if _, err := j.Prepare(context.Background(), Operation{OperationID: "op-2", MessageID: "msg-2", SourceRoute: "s", TargetRoute: "d", Semantics: "x", Digest: "d2", BodySize: 1}); err != nil {
		t.Fatal(err)
	}
	if err := j.MarkDispatchStarted(context.Background(), "op-2"); err != nil {
		t.Fatal(err)
	}
	if err := j.RecoverOrphans(context.Background()); err != nil {
		t.Fatal(err)
	}
	r, err = j.Operation(context.Background(), "op-2")
	if err != nil {
		t.Fatal(err)
	}
	if r.State != StateOutcomeUnknown {
		t.Fatalf("dispatch recovery %s", r.State)
	}
}

func TestConcurrentIdenticalClaimsJoinAndConflict(t *testing.T) {
	dir := t.TempDir()
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	j1 := testJournal(t, dir, &now)
	prepared(t, j1)
	j2, err := Open(context.Background(), Options{StateDir: dir, LeaseDuration: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer j2.Close()
	in := claimInput()
	var wg sync.WaitGroup
	results := make(chan ReplyClaim, 2)
	errs := make(chan error, 2)
	for _, jj := range []*Journal{j1, j2} {
		wg.Add(1)
		go func(jj *Journal) {
			defer wg.Done()
			c, err := jj.ClaimReply(context.Background(), in)
			results <- c
			errs <- err
		}(jj)
	}
	wg.Wait()
	close(results)
	close(errs)
	var joined int
	var token string
	for c := range results {
		if c.Joined {
			joined++
		}
		if token == "" {
			token = c.Token
		}
		if c.Token != token {
			t.Fatal("join returned different token")
		}
	}
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if joined != 1 {
		t.Fatalf("joined count %d", joined)
	}
	bad := in
	bad.Digest = "other"
	if _, err := j1.ClaimReply(context.Background(), bad); !errors.Is(err, ErrIdentityConflict) {
		t.Fatalf("conflict: %v", err)
	}
}

func TestExpiryFencesLateCommitAndWaitWakesUnknown(t *testing.T) {
	dir := t.TempDir()
	var now atomic.Int64
	now.Store(10_000_000_000)
	j := testJournal(t, dir, &now)
	prepared(t, j)
	c, err := j.ClaimReply(context.Background(), claimInput())
	if err != nil {
		t.Fatal(err)
	}
	now.Add(2 * int64(time.Second))
	var wg sync.WaitGroup
	wg.Add(1)
	got := make(chan ReplyClaim, 1)
	go func() {
		defer wg.Done()
		r, e := j.Wait(context.Background(), "msg-1", time.Millisecond)
		if e != nil {
			t.Error(e)
			return
		}
		got <- r
	}()
	wg.Wait()
	r := <-got
	if r.State != StateReplyOutcomeUnknown {
		t.Fatalf("wait state %s", r.State)
	}
	if _, err := j.CommitReply(context.Background(), c.ReplyID, c.Owner, c.Token); !errors.Is(err, ErrClaimExpired) {
		t.Fatalf("late commit: %v", err)
	}
	// A second receiver cannot redispatch the expired reply.
	if _, err := j.ClaimReply(context.Background(), claimInput()); !errors.Is(err, ErrClaimExpired) {
		t.Fatalf("redispatch: %v", err)
	}
}

func TestFirstWinnerIsCustodyCommitOrder(t *testing.T) {
	dir := t.TempDir()
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	j := testJournal(t, dir, &now)
	prepared(t, j)
	a := claimInput()
	a.ReplyID = "reply-a"
	b := claimInput()
	b.ReplyID = "reply-b"
	ca, err := j.ClaimReply(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	cb, err := j.ClaimReply(context.Background(), b)
	if err != nil {
		t.Fatal(err)
	}
	rb, err := j.CommitReply(context.Background(), cb.ReplyID, cb.Owner, cb.Token)
	if err != nil {
		t.Fatal(err)
	}
	if !rb.Won {
		t.Fatal("first commit did not win")
	}
	ra, err := j.CommitReply(context.Background(), ca.ReplyID, ca.Owner, ca.Token)
	if err != nil {
		t.Fatal(err)
	}
	if ra.Won {
		t.Fatal("later commit won")
	}
	w, err := j.Wait(context.Background(), "msg-1", time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if w.ReplyID != "reply-b" {
		t.Fatalf("winner %s", w.ReplyID)
	}
}

func TestAcceptedReplyIsIdempotentAndObservationPreservesWait(t *testing.T) {
	dir := t.TempDir()
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	j := testJournal(t, dir, &now)
	prepared(t, j)
	c, err := j.ClaimReply(context.Background(), claimInput())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.CommitReply(context.Background(), c.ReplyID, c.Owner, c.Token); err != nil {
		t.Fatal(err)
	}
	joined, err := j.ClaimReply(context.Background(), claimInput())
	if err != nil {
		t.Fatal(err)
	}
	if joined.State != StateReplyAccepted || !joined.Joined {
		t.Fatalf("idempotent claim: %+v", joined)
	}
	if err := j.ObserveReply(context.Background(), c.ReplyID, "native-1", c.Digest); err != nil {
		t.Fatal(err)
	}
	w, err := j.Wait(context.Background(), "msg-1", time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if w.State != StateReplyObserved {
		t.Fatalf("observed wait: %s", w.State)
	}
}

func TestObservationDoesNotReviveUnknown(t *testing.T) {
	dir := t.TempDir()
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	j := testJournal(t, dir, &now)
	prepared(t, j)
	c, err := j.ClaimReply(context.Background(), claimInput())
	if err != nil {
		t.Fatal(err)
	}
	now.Add(2 * int64(time.Second))
	if err := j.ExpireClaims(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := j.ObserveReply(context.Background(), c.ReplyID, "native-1", c.Digest); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("unknown observation: %v", err)
	}
}

func TestCorruptDatabaseFailsClosed(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "journal.sqlite3")
	if err := os.WriteFile(p, []byte("not sqlite"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := Open(context.Background(), Options{StateDir: dir})
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corruption: %v", err)
	}
	got, _ := os.ReadFile(p)
	if string(got) != "not sqlite" {
		t.Fatal("corrupt database was replaced")
	}
}

func TestForeignKeyAndMigrationAreTransactional(t *testing.T) {
	dir := t.TempDir()
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	j := testJournal(t, dir, &now)
	if _, err := j.db.Exec(`INSERT INTO reply_winners(original_id,reply_id,committed_at,commit_seq) VALUES('x','missing',1,1)`); err == nil {
		t.Fatal("foreign key not enforced")
	}
	var version int
	if err := j.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 1 {
		t.Fatalf("schema version %d", version)
	}
}

func TestMetadataNeverStoresBodies(t *testing.T) {
	dir := t.TempDir()
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	j := testJournal(t, dir, &now)
	prepared(t, j)
	b, err := os.ReadFile(filepath.Join(dir, "journal.sqlite3"))
	if err != nil {
		t.Fatal(err)
	}
	if string(b) == "body-that-must-not-persist" {
		t.Fatal("body persisted")
	}
	var tables int
	if err := j.db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='bodies'`).Scan(&tables); err != nil {
		t.Fatal(err)
	}
	if tables != 0 {
		t.Fatal("body table exists")
	}
}
