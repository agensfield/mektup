package journal

import (
	"context"
	cryptorand "crypto/rand"
	"database/sql"
	"fmt"
	"time"
)

type ReplyClaim struct {
	ReplyID      string
	OriginalID   string
	Digest       string
	BodySize     int64
	Status       string
	ReplyRoute   string
	CustodyRoute string
	Owner        string
	Token        string
	LeaseUntil   int64
	State        EvidenceState
	CreatedAt    int64
	UpdatedAt    int64
	AcceptedAt   int64
	CommitSeq    int64
	Joined       bool
	Won          bool
}

// ClaimInput is the complete identity fence. Every field is compared for a
// duplicate reply ID, including routes and terminal status.
type ClaimInput struct {
	ReplyID      string
	OriginalID   string
	Digest       string
	BodySize     int64
	Status       string
	ReplyRoute   string
	CustodyRoute string
	Owner        string
}

func (in ClaimInput) valid() bool {
	return in.ReplyID != "" && in.OriginalID != "" && in.Digest != "" && in.BodySize >= 0 && (in.Status == "success" || in.Status == "error") && in.ReplyRoute != "" && in.CustodyRoute != "" && in.Owner != ""
}

// ClaimReply atomically fences one reply ID before a body write. Identical
// active claims join. Expired claims are terminal unknown and are never
// returned as available for another dispatch.
func (j *Journal) ClaimReply(ctx context.Context, in ClaimInput) (ReplyClaim, error) {
	if !in.valid() {
		return ReplyClaim{}, fmt.Errorf("journal: invalid reply claim")
	}
	now := j.nowUnix()
	var out ReplyClaim
	err := j.withTx(ctx, func(tx *sql.Tx) error {
		var existing ReplyClaim
		err := scanClaim(tx.QueryRow("SELECT reply_id,original_id,digest,body_size,status,reply_route,custody_route,owner,token,lease_until,state,created_at,updated_at,COALESCE(accepted_at,0),COALESCE(commit_seq,0) FROM reply_claims WHERE reply_id=?", in.ReplyID), &existing)
		if err == nil {
			if existing.OriginalID != in.OriginalID || existing.Digest != in.Digest || existing.BodySize != in.BodySize || existing.Status != in.Status || existing.ReplyRoute != in.ReplyRoute || existing.CustodyRoute != in.CustodyRoute {
				return ErrIdentityConflict
			}
			if existing.State == StateReplyAccepted || existing.State == StateReplyObserved {
				existing.Joined = true
				out = existing
				return nil
			}
			if existing.State != StateReplyClaimed {
				out = existing
				return ErrClaimExpired
			}
			if existing.LeaseUntil <= now {
				if err := expireClaimTx(tx, existing.ReplyID, now); err != nil {
					return err
				}
				existing.State = StateReplyOutcomeUnknown
				existing.UpdatedAt = now
				out = existing
				return ErrClaimExpired
			}
			existing.Joined = true
			out = existing
			return nil
		}
		if err != sql.ErrNoRows {
			return err
		}
		// The original request relationship is established by Prepare. A reply
		// receiver cannot introduce an arbitrary relationship through control.
		var one int
		if err := tx.QueryRow("SELECT 1 FROM operations WHERE message_id=? LIMIT 1", in.OriginalID).Scan(&one); err != nil {
			if err == sql.ErrNoRows {
				return ErrNotFound
			}
			return err
		}
		token, err := randomToken()
		if err != nil {
			return err
		}
		lease := now + j.leaseDuration.Nanoseconds()
		_, err = tx.Exec("INSERT INTO reply_claims(reply_id,original_id,digest,body_size,status,reply_route,custody_route,owner,token,lease_until,state,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)", in.ReplyID, in.OriginalID, in.Digest, in.BodySize, in.Status, in.ReplyRoute, in.CustodyRoute, in.Owner, token, lease, string(StateReplyClaimed), now, now)
		if err != nil {
			return err
		}
		if err = emit(tx, "reply.claimed", "", in.ReplyID, StateReplyClaimed, now); err != nil {
			return err
		}
		out = ReplyClaim{ReplyID: in.ReplyID, OriginalID: in.OriginalID, Digest: in.Digest, BodySize: in.BodySize, Status: in.Status, ReplyRoute: in.ReplyRoute, CustodyRoute: in.CustodyRoute, Owner: in.Owner, Token: token, LeaseUntil: lease, State: StateReplyClaimed, CreatedAt: now, UpdatedAt: now}
		return nil
	})
	return out, err
}

func randomToken() (string, error) {
	var b [24]byte
	if _, err := cryptorand.Read(b[:]); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", b[:]), nil
}

func (j *Journal) Heartbeat(ctx context.Context, replyID, owner, token string) (ReplyClaim, error) {
	now := j.nowUnix()
	lease := now + j.leaseDuration.Nanoseconds()
	err := j.withTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.Exec("UPDATE reply_claims SET lease_until=?,updated_at=? WHERE reply_id=? AND owner=? AND token=? AND state=? AND lease_until>?", lease, now, replyID, owner, token, string(StateReplyClaimed), now)
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			return ErrClaimNotOwned
		}
		return nil
	})
	if err != nil {
		return ReplyClaim{}, err
	}
	return j.Reply(ctx, replyID)
}

type CommitResult struct {
	Claim ReplyClaim
	Won   bool
}

// CommitReply records destination acceptance and closes the claim in one
// transaction. Winner selection is by this transaction's custody commit
// order, never by app-server response or item timestamp.
func (j *Journal) CommitReply(ctx context.Context, replyID, owner, token string) (CommitResult, error) {
	now := j.nowUnix()
	var result CommitResult
	err := j.withTx(ctx, func(tx *sql.Tx) error {
		var claim ReplyClaim
		if err := scanClaim(tx.QueryRow("SELECT reply_id,original_id,digest,body_size,status,reply_route,custody_route,owner,token,lease_until,state,created_at,updated_at,COALESCE(accepted_at,0),COALESCE(commit_seq,0) FROM reply_claims WHERE reply_id=?", replyID), &claim); err != nil {
			if err == sql.ErrNoRows {
				return ErrNotFound
			}
			return err
		}
		if claim.Owner != owner || claim.Token != token {
			return ErrClaimNotOwned
		}
		if claim.State != StateReplyClaimed {
			return ErrClaimExpired
		}
		if claim.LeaseUntil <= now {
			if err := expireClaimTx(tx, replyID, now); err != nil {
				return err
			}
			return ErrClaimExpired
		}
		var seq int64
		if err := tx.QueryRow("SELECT COALESCE(MAX(commit_seq),0)+1 FROM reply_claims").Scan(&seq); err != nil {
			return err
		}
		if _, err := tx.Exec("UPDATE reply_claims SET state=?,accepted_at=?,updated_at=?,commit_seq=? WHERE reply_id=? AND owner=? AND token=? AND state=? AND lease_until>?", string(StateReplyAccepted), now, now, seq, replyID, owner, token, string(StateReplyClaimed), now); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT OR IGNORE INTO reply_winners(original_id,reply_id,committed_at,commit_seq) VALUES(?,?,?,?)", claim.OriginalID, replyID, now, seq); err != nil {
			return err
		}
		if err := tx.QueryRow("SELECT EXISTS(SELECT 1 FROM reply_winners WHERE original_id=? AND reply_id=?)", claim.OriginalID, replyID).Scan(&result.Won); err != nil {
			return err
		}
		if err := emit(tx, "reply.accepted", "", replyID, StateReplyAccepted, now); err != nil {
			return err
		}
		claim.State = StateReplyAccepted
		claim.AcceptedAt = now
		claim.UpdatedAt = now
		claim.CommitSeq = seq
		result.Claim = claim
		return nil
	})
	return result, err
}

func expireClaimTx(tx *sql.Tx, replyID string, now int64) error {
	res, err := tx.Exec("UPDATE reply_claims SET state=?,updated_at=?,error_code=? WHERE reply_id=? AND state=? AND lease_until<=?", string(StateReplyOutcomeUnknown), now, "reply_outcome_unknown", replyID, string(StateReplyClaimed), now)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return ErrClaimExpired
	}
	return emit(tx, "reply.expired", "", replyID, StateReplyOutcomeUnknown, now)
}

// ExpireClaims is safe for a waiter/status process to call and is the wake
// path when the delivery process has disappeared.
func (j *Journal) ExpireClaims(ctx context.Context) error {
	now := j.nowUnix()
	return j.withTx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.Query("SELECT reply_id FROM reply_claims WHERE state=? AND lease_until<=?", string(StateReplyClaimed), now)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				return err
			}
			if err := expireClaimTx(tx, id, now); err != nil && err != ErrClaimExpired {
				return err
			}
		}
		return rows.Err()
	})
}

// AbandonReply is an explicit operator/delivery failure transition. It is
// intentionally terminal and never makes the reply claimable again.
func (j *Journal) AbandonReply(ctx context.Context, replyID, owner, token string) error {
	now := j.nowUnix()
	return j.withTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.Exec("UPDATE reply_claims SET state=?,updated_at=?,error_code=? WHERE reply_id=? AND owner=? AND token=? AND state=?", string(StateReplyOutcomeUnknown), now, "reply_outcome_unknown", replyID, owner, token, string(StateReplyClaimed))
		if err != nil {
			return err
		}
		n, _ := res.RowsAffected()
		if n == 0 {
			return ErrClaimNotOwned
		}
		return emit(tx, "reply.abandoned", "", replyID, StateReplyOutcomeUnknown, now)
	})
}

func (j *Journal) Reply(ctx context.Context, replyID string) (ReplyClaim, error) {
	var out ReplyClaim
	err := scanClaim(j.db.QueryRowContext(ctx, "SELECT reply_id,original_id,digest,body_size,status,reply_route,custody_route,owner,token,lease_until,state,created_at,updated_at,COALESCE(accepted_at,0),COALESCE(commit_seq,0) FROM reply_claims WHERE reply_id=?", replyID), &out)
	if err == sql.ErrNoRows {
		return out, ErrNotFound
	}
	return out, err
}

func (j *Journal) RepliesFor(ctx context.Context, originalID string) ([]ReplyClaim, error) {
	rows, err := j.db.QueryContext(ctx, "SELECT reply_id,original_id,digest,body_size,status,reply_route,custody_route,owner,token,lease_until,state,created_at,updated_at,COALESCE(accepted_at,0),COALESCE(commit_seq,0) FROM reply_claims WHERE original_id=? ORDER BY created_at,reply_id", originalID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ReplyClaim
	for rows.Next() {
		var c ReplyClaim
		if err := scanClaim(rows, &c); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func scanClaim(row interface{ Scan(...any) error }, out *ReplyClaim) error {
	return row.Scan(&out.ReplyID, &out.OriginalID, &out.Digest, &out.BodySize, &out.Status, &out.ReplyRoute, &out.CustodyRoute, &out.Owner, &out.Token, &out.LeaseUntil, &out.State, &out.CreatedAt, &out.UpdatedAt, &out.AcceptedAt, &out.CommitSeq)
}

// Wait polls durable metadata and expires abandoned claims. It never returns a
// body. A reply_outcome_unknown record is terminal and wakes the caller.
func (j *Journal) Wait(ctx context.Context, originalID string, poll time.Duration) (ReplyClaim, error) {
	if poll <= 0 {
		poll = 25 * time.Millisecond
	}
	for {
		if err := j.ExpireClaims(ctx); err != nil {
			return ReplyClaim{}, err
		}
		var out ReplyClaim
		err := scanClaim(j.db.QueryRowContext(ctx, `SELECT c.reply_id,c.original_id,c.digest,c.body_size,c.status,c.reply_route,c.custody_route,c.owner,c.token,c.lease_until,c.state,c.created_at,c.updated_at,COALESCE(c.accepted_at,0),COALESCE(c.commit_seq,0)
FROM reply_claims c WHERE c.original_id=? AND c.state IN (?,?,?) ORDER BY CASE WHEN c.state IN (?,?) THEN 0 ELSE 1 END, c.commit_seq LIMIT 1`, originalID, string(StateReplyAccepted), string(StateReplyObserved), string(StateReplyOutcomeUnknown), string(StateReplyAccepted), string(StateReplyObserved)), &out)
		if err == nil {
			return out, nil
		}
		if err != sql.ErrNoRows {
			return ReplyClaim{}, err
		}
		t := time.NewTimer(poll)
		select {
		case <-ctx.Done():
			t.Stop()
			return ReplyClaim{}, ctx.Err()
		case <-t.C:
		}
	}
}

// ObserveReply is a separate strengthening transition. It cannot revive an
// expired token or change first-winner ordering.
func (j *Journal) ObserveReply(ctx context.Context, replyID, nativeItemID, digest string) error {
	if nativeItemID == "" || digest == "" {
		return fmt.Errorf("journal: invalid observation")
	}
	now := j.nowUnix()
	return j.withTx(ctx, func(tx *sql.Tx) error {
		var expected string
		var state EvidenceState
		if err := tx.QueryRow("SELECT digest,state FROM reply_claims WHERE reply_id=?", replyID).Scan(&expected, &state); err != nil {
			if err == sql.ErrNoRows {
				return ErrNotFound
			}
			return err
		}
		if expected != digest {
			return ErrIdentityConflict
		}
		if state != StateReplyAccepted && state != StateReplyObserved {
			return ErrInvalidTransition
		}
		if _, err := tx.Exec("INSERT INTO observations(reply_id,native_item_id,observed_at,digest) VALUES(?,?,?,?) ON CONFLICT(reply_id) DO UPDATE SET native_item_id=excluded.native_item_id,observed_at=excluded.observed_at", replyID, nativeItemID, now, digest); err != nil {
			return err
		}
		if state != StateReplyObserved {
			if _, err := tx.Exec("UPDATE reply_claims SET state=?,updated_at=? WHERE reply_id=?", string(StateReplyObserved), now, replyID); err != nil {
				return err
			}
			return emit(tx, "reply.observed", "", replyID, StateReplyObserved, now)
		}
		return nil
	})
}
