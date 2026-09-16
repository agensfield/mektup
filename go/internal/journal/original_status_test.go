package journal

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func originalStatusClaimInput() ClaimInput {
	in := claimInput()
	in.Digest = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	return in
}

func TestOriginalStatusPendingAndUnknownOrder(t *testing.T) {
	dir := t.TempDir()
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	j := testJournal(t, dir, &now)
	prepared(t, j)
	pending, err := j.OriginalStatus(context.Background(), "msg-1")
	if err != nil {
		t.Fatal(err)
	}
	if pending.Selection != "pending" {
		t.Fatalf("pending selection %q", pending.Selection)
	}
	a := originalStatusClaimInput()
	a.ReplyID = "unknown-a"
	ca, err := j.ClaimReply(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.AbandonReply(context.Background(), ca.ReplyID, ca.Owner, ca.Token); err != nil {
		t.Fatal(err)
	}
	b := originalStatusClaimInput()
	b.ReplyID = "unknown-b"
	cb, err := j.ClaimReply(context.Background(), b)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.AbandonReply(context.Background(), cb.ReplyID, cb.Owner, cb.Token); err != nil {
		t.Fatal(err)
	}
	got, err := j.OriginalStatus(context.Background(), "msg-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Selection != "terminal_unknown" || got.Claim.ReplyID != ca.ReplyID || got.TerminalEventSeq <= 0 {
		t.Fatalf("unknown selection %+v", got)
	}
}

func TestOriginalStatusWinnerPrecedesUnknown(t *testing.T) {
	dir := t.TempDir()
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	j := testJournal(t, dir, &now)
	prepared(t, j)
	a := originalStatusClaimInput()
	a.ReplyID = "winner"
	ca, err := j.ClaimReply(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.CommitReply(context.Background(), ca.ReplyID, ca.Owner, ca.Token); err != nil {
		t.Fatal(err)
	}
	b := originalStatusClaimInput()
	b.ReplyID = "unknown"
	cb, err := j.ClaimReply(context.Background(), b)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.AbandonReply(context.Background(), cb.ReplyID, cb.Owner, cb.Token); err != nil {
		t.Fatal(err)
	}
	got, err := j.OriginalStatus(context.Background(), "msg-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Selection != "winner" || got.Claim.ReplyID != ca.ReplyID || got.Claim.Token != "" || got.EventSeq <= 0 {
		t.Fatalf("winner selection %+v", got)
	}
}

func TestOriginalStatusExpiresDueClaimsAndWakesUnknown(t *testing.T) {
	dir := t.TempDir()
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	j := testJournal(t, dir, &now)
	prepared(t, j)
	c, err := j.ClaimReply(context.Background(), originalStatusClaimInput())
	if err != nil {
		t.Fatal(err)
	}
	now.Add(2 * int64(time.Second))
	got, err := j.OriginalStatus(context.Background(), "msg-1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Selection != "terminal_unknown" || got.Claim.ReplyID != c.ReplyID {
		t.Fatalf("expiry selection %+v", got)
	}
	stored, err := j.Reply(context.Background(), c.ReplyID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.State != StateReplyOutcomeUnknown {
		t.Fatalf("claim not expired: %s", stored.State)
	}
}

func TestOriginalStatusRollbackAndConcurrentLinearization(t *testing.T) {
	dir := t.TempDir()
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	j := testJournal(t, dir, &now)
	prepared(t, j)
	c, err := j.ClaimReply(context.Background(), originalStatusClaimInput())
	if err != nil {
		t.Fatal(err)
	}
	now.Add(2 * int64(time.Second))
	if _, err := j.db.Exec("DROP TABLE events"); err != nil {
		t.Fatal(err)
	}
	if _, err := j.OriginalStatus(context.Background(), "msg-1"); err == nil {
		t.Fatal("missing events table did not roll back")
	}
	if _, err := j.db.Exec("CREATE TABLE events (seq INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL, operation_id TEXT, reply_id TEXT, state TEXT NOT NULL, at INTEGER NOT NULL)"); err != nil {
		t.Fatal(err)
	}
	stored, err := j.Reply(context.Background(), c.ReplyID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.State != StateReplyClaimed {
		t.Fatalf("rollback changed claim: %s", stored.State)
	}
	// A fresh journal with intact events gives one transaction-time snapshot to
	// every concurrent status reader.
	_ = j.Close()
	opts := Options{StateDir: dir, Now: func() time.Time { return time.Unix(0, now.Load()) }}
	j1, err := Open(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer j1.Close()
	j2, err := Open(context.Background(), opts)
	if err != nil {
		t.Fatal(err)
	}
	defer j2.Close()
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, jj := range []*Journal{j1, j2} {
		wg.Add(1)
		go func(x *Journal) {
			defer wg.Done()
			r, e := x.OriginalStatus(context.Background(), "msg-1")
			if e == nil && r.Selection != "terminal_unknown" {
				errs <- errors.New("inconsistent selection")
			} else {
				errs <- e
			}
		}(jj)
	}
	wg.Wait()
	close(errs)
	for e := range errs {
		if e != nil {
			t.Fatal(e)
		}
	}
}

func TestOriginalStatusFailsClosedOnCorruptWinnerMetadata(t *testing.T) {
	dir := t.TempDir()
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	j := testJournal(t, dir, &now)
	prepared(t, j)
	c, err := j.ClaimReply(context.Background(), originalStatusClaimInput())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.CommitReply(context.Background(), c.ReplyID, c.Owner, c.Token); err != nil {
		t.Fatal(err)
	}
	if _, err := j.db.Exec("UPDATE reply_claims SET state=?,commit_seq=0 WHERE reply_id=?", string(StateReplyAccepted), c.ReplyID); err != nil {
		t.Fatal(err)
	}
	if _, err := j.OriginalStatus(context.Background(), "msg-1"); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt winner accepted: %v", err)
	}
	if _, err := j.db.Exec("UPDATE reply_claims SET state=?,commit_seq=1 WHERE reply_id=?", string(StateReplyObserved), c.ReplyID); err != nil {
		t.Fatal(err)
	}
	if _, err := j.db.Exec("DELETE FROM observations WHERE reply_id=?", c.ReplyID); err != nil {
		t.Fatal(err)
	}
	if _, err := j.OriginalStatus(context.Background(), "msg-1"); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("observed winner without native evidence accepted: %v", err)
	}
	if _, err := j.db.Exec("UPDATE reply_claims SET state=?,digest=? WHERE reply_id=?", string(StateReplyAccepted), "sha256:AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", c.ReplyID); err != nil {
		t.Fatal(err)
	}
	if _, err := j.OriginalStatus(context.Background(), "msg-1"); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("uppercase winner digest accepted: %v", err)
	}
}
