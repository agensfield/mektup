package journal

import (
	"context"
	"database/sql"
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
	r, err := j.Prepare(context.Background(), Operation{OperationID: "op-1", MessageID: "msg-1", SourceRoute: "src", TargetRoute: "dst", Semantics: "send", ReplyRoute: "reply-route", CustodyRoute: "custody", CustodyStoreID: "store-test", AttemptOwner: "owner-1", Digest: "digest-1", BodySize: 7})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func claimInput() ClaimInput {
	return ClaimInput{ReplyID: "reply-1", OriginalID: "msg-1", Digest: "reply-digest", BodySize: 9, Status: "success", ReplyRoute: "reply-route", CustodyRoute: "custody", CustodyStoreID: "store-test", Owner: "receiver-a"}
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
	if !validStoreID(id) {
		t.Fatalf("store id is not UUIDv7: %q", id)
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
	var sqliteVersion string
	if err := j2.db.QueryRow("SELECT sqlite_version()").Scan(&sqliteVersion); err != nil {
		t.Fatal(err)
	}
	if !atLeastSQLite(sqliteVersion, 3, 51, 3) {
		t.Fatalf("unsafe SQLite %s", sqliteVersion)
	}
}

func TestPreparedAndDispatchRecoveryIsConservative(t *testing.T) {
	dir := t.TempDir()
	var now atomic.Int64
	now.Store(1_000_000_000)
	j := testJournal(t, dir, &now)
	prepared(t, j)
	now.Add(31 * int64(time.Second))
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
	now.Add(31 * int64(time.Second))
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

func TestRecoveryDoesNotTouchLiveAttempt(t *testing.T) {
	dir := t.TempDir()
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	j := testJournal(t, dir, &now)
	created := prepared(t, j)
	if err := j.RecoverOrphans(context.Background()); err != nil {
		t.Fatal(err)
	}
	r, err := j.Operation(context.Background(), "op-1")
	if err != nil {
		t.Fatal(err)
	}
	if r.State != StatePrepared {
		t.Fatalf("live attempt recovered: %s", r.State)
	}
	if _, err := j.HeartbeatAttempt(context.Background(), "op-1", created.AttemptOwner, created.AttemptToken); err != nil {
		t.Fatal(err)
	}
}

func TestUnknownCannotBeRejectedButMayBeManuallyResolved(t *testing.T) {
	dir := t.TempDir()
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	j := testJournal(t, dir, &now)
	prepared(t, j)
	if err := j.MarkDispatchStarted(context.Background(), "op-1"); err != nil {
		t.Fatal(err)
	}
	if err := j.RecordResult(context.Background(), "op-1", StateOutcomeUnknown, "transport_lost"); err != nil {
		t.Fatal(err)
	}
	if err := j.RecordResult(context.Background(), "op-1", StateRejected, "late_guess"); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("unknown rejected: %v", err)
	}
	if err := j.RecordAccepted(context.Background(), "op-1", "native-acceptance-1"); err != nil {
		t.Fatal(err)
	}
	var errorCode, evidenceRef string
	if err := j.db.QueryRow("SELECT error_code FROM operations WHERE operation_id='op-1'").Scan(&errorCode); err != nil {
		t.Fatal(err)
	}
	if errorCode != "transport_lost" {
		t.Fatalf("acceptance rewrote error authority: %q", errorCode)
	}
	if err := j.db.QueryRow("SELECT evidence_ref FROM operation_acceptances WHERE operation_id='op-1'").Scan(&evidenceRef); err != nil {
		t.Fatal(err)
	}
	if evidenceRef != "native-acceptance-1" {
		t.Fatalf("acceptance evidence %q", evidenceRef)
	}
	if err := j.RecordManualResolution(context.Background(), "op-1", ManualResolution{Assertion: "not_delivered", Actor: "operator", Reason: "verified external logs", EvidenceRef: "incident-1"}); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("resolved accepted operation: %v", err)
	}
	if _, err := j.Prepare(context.Background(), Operation{OperationID: "op-2", MessageID: "msg-2", SourceRoute: "s", TargetRoute: "d", Semantics: "x", Digest: "d2", BodySize: 1}); err != nil {
		t.Fatal(err)
	}
	if err := j.MarkDispatchStarted(context.Background(), "op-2"); err != nil {
		t.Fatal(err)
	}
	if err := j.RecordResult(context.Background(), "op-2", StateOutcomeUnknown, "transport_lost"); err != nil {
		t.Fatal(err)
	}
	if err := j.RecordManualResolution(context.Background(), "op-2", ManualResolution{Assertion: "not_delivered", Actor: "operator", Reason: "verified external logs", EvidenceRef: "incident-1"}); err != nil {
		t.Fatal(err)
	}
	r, err := j.Operation(context.Background(), "op-2")
	if err != nil {
		t.Fatal(err)
	}
	if r.State != StateManuallyResolved {
		t.Fatalf("resolution state %s", r.State)
	}
}

func TestOperationTransitionMatrixRejectsImpossibleTargets(t *testing.T) {
	dir := t.TempDir()
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	j := testJournal(t, dir, &now)
	if _, err := j.Prepare(context.Background(), Operation{OperationID: "matrix-1", MessageID: "matrix-msg-1", SourceRoute: "s", TargetRoute: "d", Semantics: "x", Digest: "m1", BodySize: 1}); err != nil {
		t.Fatal(err)
	}
	if err := j.RecordResult(context.Background(), "matrix-1", StateAccepted, "bad"); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("prepared accepted: %v", err)
	}
	if err := j.RecordResult(context.Background(), "matrix-1", StateOutcomeUnknown, "bad"); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("prepared unknown: %v", err)
	}
	if err := j.RecordResult(context.Background(), "matrix-1", StateNotSent, "proven"); err != nil {
		t.Fatal(err)
	}
	if err := j.RecordResult(context.Background(), "matrix-1", StateAccepted, "late"); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("not-sent accepted: %v", err)
	}
	if _, err := j.Prepare(context.Background(), Operation{OperationID: "matrix-2", MessageID: "matrix-msg-2", SourceRoute: "s", TargetRoute: "d", Semantics: "x", Digest: "m2", BodySize: 1}); err != nil {
		t.Fatal(err)
	}
	if err := j.MarkDispatchStarted(context.Background(), "matrix-2"); err != nil {
		t.Fatal(err)
	}
	if err := j.RecordResult(context.Background(), "matrix-2", StateNotSent, "impossible"); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("dispatch not-sent: %v", err)
	}
	if err := j.RecordResult(context.Background(), "matrix-2", StateOutcomeUnknown, "lost"); err != nil {
		t.Fatal(err)
	}
	if err := j.RecordResult(context.Background(), "matrix-2", StateRejected, "late"); !errors.Is(err, ErrInvalidTransition) {
		t.Fatalf("unknown rejected: %v", err)
	}
}

func TestPrepareJoinRedactsAttemptToken(t *testing.T) {
	dir := t.TempDir()
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	j := testJournal(t, dir, &now)
	op := Operation{OperationID: "join-op", MessageID: "join-msg", SourceRoute: "s", TargetRoute: "d", Semantics: "x", Digest: "join", BodySize: 1, AttemptOwner: "owner-a"}
	first, err := j.Prepare(context.Background(), op)
	if err != nil {
		t.Fatal(err)
	}
	if first.AttemptToken == "" {
		t.Fatal("creator did not receive attempt token")
	}
	joined, err := j.Prepare(context.Background(), op)
	if err != nil {
		t.Fatal(err)
	}
	if joined.AttemptToken != first.AttemptToken {
		t.Fatal("same owner lost token")
	}
	op.AttemptOwner = "owner-b"
	other, err := j.Prepare(context.Background(), op)
	if err != nil {
		t.Fatal(err)
	}
	if other.AttemptToken != "" {
		t.Fatal("joined owner received attempt token")
	}
	status, err := j.Operation(context.Background(), op.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if status.AttemptToken != "" {
		t.Fatal("status exposed attempt token")
	}
}

func TestExpiredReplyCanOnlyBeReconciledWithoutTokenRevival(t *testing.T) {
	dir := t.TempDir()
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	j := testJournal(t, dir, &now)
	prepared(t, j)
	claim, err := j.ClaimReply(context.Background(), claimInput())
	if err != nil {
		t.Fatal(err)
	}
	now.Add(2 * int64(time.Second))
	if _, err := j.ClaimReply(context.Background(), claimInput()); !errors.Is(err, ErrClaimExpired) {
		t.Fatal(err)
	}
	if err := j.ReconcileReplyObservation(context.Background(), claim.ReplyID, "native-late", claim.Digest); err != nil {
		t.Fatal(err)
	}
	r, err := j.Reply(context.Background(), claim.ReplyID)
	if err != nil {
		t.Fatal(err)
	}
	if r.State != StateReplyObserved || r.Token != "" {
		t.Fatalf("reconciled claim: %+v", r)
	}
	if _, err := j.ClaimReply(context.Background(), claimInput()); err != nil {
		t.Fatal(err)
	}
	if _, err := j.CommitReply(context.Background(), claim.ReplyID, claim.Owner, claim.Token); !errors.Is(err, ErrClaimExpired) {
		t.Fatalf("reconciled token revived: %v", err)
	}
}

func TestReplyRouteRelationshipIsFenced(t *testing.T) {
	dir := t.TempDir()
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	j := testJournal(t, dir, &now)
	prepared(t, j)
	in := claimInput()
	in.ReplyRoute = "other-route"
	if _, err := j.ClaimReply(context.Background(), in); !errors.Is(err, ErrIdentityConflict) {
		t.Fatalf("route conflict: %v", err)
	}
}

func TestQuestionMarkStatePathIsEscaped(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state?literal")
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	j := testJournal(t, dir, &now)
	if _, err := j.Prepare(context.Background(), Operation{OperationID: "q-op", MessageID: "q-msg", SourceRoute: "s", TargetRoute: "t", Semantics: "x", Digest: "q", BodySize: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "journal.sqlite3")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(dir), "state")); !os.IsNotExist(err) {
		t.Fatalf("unescaped query path created sibling: %v", err)
	}
}

func TestLegacyStoreIDKeepsAliasProvenance(t *testing.T) {
	dir := t.TempDir()
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	j := testJournal(t, dir, &now)
	canonical := j.StoreID()
	legacy := "store_0123456789abcdef0123456789abcdef"
	if _, err := j.db.Exec("UPDATE meta SET value=? WHERE key='store_id'", legacy); err != nil {
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
	if j2.StoreID() == legacy || !validStoreID(j2.StoreID()) {
		t.Fatalf("legacy canonical id %q", j2.StoreID())
	}
	resolved, err := j2.ResolveStoreID(context.Background(), legacy)
	if err != nil {
		t.Fatal(err)
	}
	if resolved != j2.StoreID() || resolved == canonical {
		t.Fatalf("alias resolution %q canonical %q", resolved, j2.StoreID())
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
			if c.Token != "" {
				t.Fatal("joined claim received fencing token")
			}
			continue
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

func TestDirectExpiryAndLateCommitPersistUnknown(t *testing.T) {
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
	if _, err := j.ClaimReply(context.Background(), claimInput()); !errors.Is(err, ErrClaimExpired) {
		t.Fatalf("claim expiry: %v", err)
	}
	r, err := j.Reply(context.Background(), c.ReplyID)
	if err != nil {
		t.Fatal(err)
	}
	if r.State != StateReplyOutcomeUnknown {
		t.Fatalf("expiry rolled back: %s", r.State)
	}
	if r.Token != "" {
		t.Fatal("status exposed expired token")
	}
	if _, err := j.CommitReply(context.Background(), c.ReplyID, c.Owner, c.Token); !errors.Is(err, ErrClaimExpired) {
		t.Fatalf("late commit: %v", err)
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
	if version != 3 {
		t.Fatalf("schema version %d", version)
	}
}

func TestV1MigrationAddsFencesAndRoutesTransactionally(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "journal.sqlite3")
	db, err := sql.Open("sqlite", "file:"+escapedSQLitePath(dbPath))
	if err != nil {
		t.Fatal(err)
	}
	old := `CREATE TABLE meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
CREATE TABLE operations(operation_id TEXT PRIMARY KEY,message_id TEXT NOT NULL UNIQUE,source_route TEXT NOT NULL,target_route TEXT NOT NULL,semantics TEXT NOT NULL,digest TEXT NOT NULL,body_size INTEGER NOT NULL,state TEXT NOT NULL,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL,dispatch_started_at INTEGER,terminal_at INTEGER,error_code TEXT NOT NULL DEFAULT '');
CREATE TABLE attempts(operation_id TEXT PRIMARY KEY REFERENCES operations(operation_id),state TEXT NOT NULL,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL);
CREATE TABLE reply_claims(reply_id TEXT PRIMARY KEY,original_id TEXT NOT NULL,digest TEXT NOT NULL,body_size INTEGER NOT NULL,status TEXT NOT NULL,reply_route TEXT NOT NULL,custody_route TEXT NOT NULL,owner TEXT NOT NULL,token TEXT NOT NULL,lease_until INTEGER NOT NULL,state TEXT NOT NULL,created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL,accepted_at INTEGER,commit_seq INTEGER,error_code TEXT NOT NULL DEFAULT '');
PRAGMA user_version=1;`
	if _, err := db.Exec(old); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	seed, err := sql.Open("sqlite", "file:"+escapedSQLitePath(dbPath))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UnixNano()
	if _, err := seed.Exec("INSERT INTO operations(operation_id,message_id,source_route,target_route,semantics,digest,body_size,state,created_at,updated_at) VALUES('legacy-op','legacy-msg','s','d','x','digest',1,'prepared',?,?)", now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := seed.Exec("INSERT INTO attempts(operation_id,state,created_at,updated_at) VALUES('legacy-op','prepared',?,?)", now, now); err != nil {
		t.Fatal(err)
	}
	if err := seed.Close(); err != nil {
		t.Fatal(err)
	}
	clock := &atomic.Int64{}
	clock.Store(now)
	j := testJournal(t, dir, clock)
	legacy, err := j.Operation(context.Background(), "legacy-op")
	if err != nil {
		t.Fatal(err)
	}
	if legacy.AttemptOwner == "" || legacy.AttemptLeaseUntil == 0 {
		t.Fatalf("legacy attempt was stranded: %+v", legacy)
	}
	r, err := j.Operation(context.Background(), "missing")
	if !errors.Is(err, ErrNotFound) || r.OperationID != "" {
		t.Fatalf("migration operation lookup: %v", err)
	}
	var version int
	if err := j.db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != 3 {
		t.Fatalf("migrated version %d", version)
	}
	var columns int
	if err := j.db.QueryRow("SELECT COUNT(*) FROM pragma_table_info('attempts') WHERE name IN ('owner','token','lease_until')").Scan(&columns); err != nil {
		t.Fatal(err)
	}
	if columns != 3 {
		t.Fatalf("attempt columns %d", columns)
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
