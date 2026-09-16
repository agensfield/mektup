package journal

import (
	"context"
	cryptorand "crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	mektup "github.com/agensfield/mektup/go"
	"strings"
	"time"
)

type ReplyClaim struct {
	ReplyID        string
	OriginalID     string
	Digest         string
	BodySize       int64
	Status         string
	ReplyErrorCode string
	ReplyRoute     string
	CustodyRoute   string
	CustodyStoreID string
	Owner          string
	Token          string
	LeaseUntil     int64
	State          EvidenceState
	CreatedAt      int64
	UpdatedAt      int64
	AcceptedAt     int64
	CommitSeq      int64
	Joined         bool
	Won            bool
}

type OriginalStatusResult struct {
	Selection        string
	Claim            ReplyClaim
	EventSeq         int64
	NativeItemID     string
	TerminalEventSeq int64
}

// OriginalStatusImport is the metadata-only, tokenless projection of a
// remote originalStatus response. Operation is persisted before the selected
// reply evidence in the same SQLite transaction. Bodies are intentionally not
// represented here.
type OriginalStatusImport struct {
	Operation    Operation
	Selection    string
	ReplyID      string
	Digest       string
	BodySize     int64
	Status       string
	ErrorCode    string
	CommitSeq    int64
	EventSeq     int64
	NativeItemID string
}

const (
	OriginalStatusPending         = "pending"
	OriginalStatusWinner          = "winner"
	OriginalStatusTerminalUnknown = "terminal_unknown"
)

// ImportOriginalStatus atomically imports a portable originalStatus result.
// It never creates an attempts row and therefore cannot grant local dispatch
// authority. The transaction is also the identity fence: any conflict in the
// operation, selected reply, winner, native evidence, or remote ordering
// rolls back the operation and every projection made by this call.
func (j *Journal) ImportOriginalStatus(ctx context.Context, in OriginalStatusImport) error {
	if err := validateOriginalStatusImport(in); err != nil {
		return err
	}
	return j.withTx(ctx, func(tx *sql.Tx) error {
		now := j.nowUnix()
		if err := importOperationTxAt(tx, in.Operation, StatePrepared, "portable_import", now, false); err != nil {
			return err
		}
		switch in.Selection {
		case OriginalStatusPending:
			return nil
		case OriginalStatusWinner:
			return importObservedWinnerTx(tx, in, now)
		case OriginalStatusTerminalUnknown:
			return importTerminalUnknownTx(tx, in, now)
		default:
			return fmt.Errorf("journal: invalid original status selection")
		}
	})
}

// ImportPortableOriginalStatus is an explicit alias for callers describing
// the source as a portable receipt rather than an originalStatus response.
func (j *Journal) ImportPortableOriginalStatus(ctx context.Context, in OriginalStatusImport) error {
	return j.ImportOriginalStatus(ctx, in)
}

func validateOriginalStatusImport(in OriginalStatusImport) error {
	if in.Selection != OriginalStatusPending && in.Selection != OriginalStatusWinner && in.Selection != OriginalStatusTerminalUnknown {
		return fmt.Errorf("journal: invalid original status selection")
	}
	if in.Operation.OperationID == "" || in.Operation.MessageID == "" || in.Operation.SourceRoute == "" || in.Operation.TargetRoute == "" || in.Operation.Digest == "" || in.Operation.BodySize < 0 {
		return fmt.Errorf("journal: invalid imported operation metadata")
	}
	if (in.Operation.ReplyRoute == "") != (in.Operation.CustodyRoute == "") || (in.Operation.ReplyRoute != "" && in.Operation.CustodyStoreID == "") {
		return fmt.Errorf("journal: incomplete imported reply custody relationship")
	}
	if in.Selection == OriginalStatusPending {
		if in.ReplyID != "" || in.Digest != "" || in.BodySize != 0 || in.Status != "" || in.ErrorCode != "" || in.CommitSeq != 0 || in.EventSeq != 0 || in.NativeItemID != "" {
			return fmt.Errorf("journal: pending original status contains reply evidence")
		}
		return nil
	}
	if in.ReplyID == "" || mektup.ValidateID(in.ReplyID, mektup.MessageIDPrefix) != nil || in.Digest == "" || in.BodySize < 0 || (in.Status != "success" && in.Status != "error") || (in.Status == "success" && in.ErrorCode != "") {
		return fmt.Errorf("journal: invalid imported reply metadata")
	}
	if !validDigest(in.Digest) {
		return fmt.Errorf("journal: invalid imported reply digest")
	}
	if (in.Selection == OriginalStatusWinner && in.CommitSeq < 1) || (in.Selection == OriginalStatusTerminalUnknown && in.EventSeq < 1) {
		return fmt.Errorf("journal: invalid imported reply ordering")
	}
	if in.Selection == OriginalStatusWinner {
		if in.EventSeq != 0 {
			return fmt.Errorf("journal: winner contains terminal event ordering")
		}
	} else if in.NativeItemID != "" || in.CommitSeq != 0 {
		return fmt.Errorf("journal: terminal unknown contains positive evidence")
	}
	return nil
}

// OriginalStatus expires every due claim for one original and selects the
// authoritative winner, earliest terminal unknown, or pending from one
// linearized custody transaction. It never creates or renews authority.
func (j *Journal) OriginalStatus(ctx context.Context, originalID string) (OriginalStatusResult, error) {
	if originalID == "" {
		return OriginalStatusResult{}, ErrNotFound
	}
	var out OriginalStatusResult
	err := j.withTx(ctx, func(tx *sql.Tx) error {
		now := j.nowUnix()
		var exists int
		if err := tx.QueryRow("SELECT 1 FROM operations WHERE message_id=?", originalID).Scan(&exists); err != nil {
			if err == sql.ErrNoRows {
				return ErrNotFound
			}
			return err
		}
		rows, err := tx.Query("SELECT reply_id FROM reply_claims WHERE original_id=? AND state=? AND lease_until<=?", originalID, string(StateReplyClaimed), now)
		if err != nil {
			return err
		}
		var due []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			due = append(due, id)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		for _, id := range due {
			if err := expireClaimTx(tx, id, now); err != nil {
				return err
			}
		}
		var claim ReplyClaim
		var native sql.NullString
		var seq int64
		err = scanClaim(tx.QueryRow(`SELECT c.reply_id,c.original_id,c.digest,c.body_size,c.status,c.reply_error_code,c.reply_route,c.custody_route,c.custody_store_id,c.owner,c.token,c.lease_until,c.state,c.created_at,c.updated_at,COALESCE(c.accepted_at,0),COALESCE(c.commit_seq,0) FROM reply_winners w JOIN reply_claims c ON c.reply_id=w.reply_id LEFT JOIN observations o ON o.reply_id=c.reply_id WHERE w.original_id=?`, originalID), &claim)
		if err == nil {
			if err := tx.QueryRow("SELECT w.commit_seq,COALESCE(o.native_item_id,'') FROM reply_winners w LEFT JOIN observations o ON o.reply_id=w.reply_id WHERE w.original_id=?", originalID).Scan(&seq, &native); err != nil {
				return err
			}
			if err := validateOriginalSelectedClaim(claim, seq, native.String, true); err != nil {
				return err
			}
			claim.Token = ""
			out = OriginalStatusResult{Selection: "winner", Claim: claim, EventSeq: seq, NativeItemID: native.String}
			return nil
		}
		if err != sql.ErrNoRows {
			return err
		}
		rows, err = tx.Query(`SELECT c.reply_id,COALESCE((SELECT MIN(e.seq) FROM events e WHERE e.reply_id=c.reply_id AND e.state=?),0) FROM reply_claims c WHERE c.original_id=? AND c.state=? ORDER BY 2,c.reply_id`, string(StateReplyOutcomeUnknown), originalID, string(StateReplyOutcomeUnknown))
		if err != nil {
			return err
		}
		type unknown struct {
			id  string
			seq int64
		}
		var unknowns []unknown
		for rows.Next() {
			var u unknown
			if err := rows.Scan(&u.id, &u.seq); err != nil {
				rows.Close()
				return err
			}
			if u.seq == 0 {
				rows.Close()
				return fmt.Errorf("%w: terminal unknown lacks custody event", ErrCorrupt)
			}
			unknowns = append(unknowns, u)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		if len(unknowns) != 0 {
			var u ReplyClaim
			if err := scanClaim(tx.QueryRow("SELECT reply_id,original_id,digest,body_size,status,reply_error_code,reply_route,custody_route,custody_store_id,owner,token,lease_until,state,created_at,updated_at,COALESCE(accepted_at,0),COALESCE(commit_seq,0) FROM reply_claims WHERE reply_id=?", unknowns[0].id), &u); err != nil {
				return err
			}
			if err := validateOriginalSelectedClaim(u, unknowns[0].seq, "", false); err != nil {
				return err
			}
			u.Token = ""
			out = OriginalStatusResult{Selection: "terminal_unknown", Claim: u, TerminalEventSeq: unknowns[0].seq}
			return nil
		}
		out.Selection = "pending"
		return nil
	})
	return out, err
}

// ImportTerminalUnknown projects a tokenless authoritative expired claim into
// a successor journal without creating dispatch authority.
func (j *Journal) ImportTerminalUnknown(ctx context.Context, in ClaimInput) error {
	if in.ReplyID == "" || in.OriginalID == "" || in.Digest == "" || in.BodySize < 0 || (in.Status != "success" && in.Status != "error") || in.ReplyRoute == "" || in.CustodyRoute == "" || in.CustodyStoreID == "" {
		return fmt.Errorf("journal: invalid imported terminal unknown")
	}
	return j.withTx(ctx, func(tx *sql.Tx) error {
		return importTerminalUnknownTx(tx, OriginalStatusImport{Operation: Operation{MessageID: in.OriginalID, ReplyRoute: in.ReplyRoute, CustodyRoute: in.CustodyRoute, CustodyStoreID: in.CustodyStoreID}, ReplyID: in.ReplyID, Digest: in.Digest, BodySize: in.BodySize, Status: in.Status, ErrorCode: in.ErrorCode, EventSeq: 0}, j.nowUnix())
	})
}

func importTerminalUnknownTx(tx *sql.Tx, in OriginalStatusImport, now int64) error {
	var route, custody, storeID string
	if err := tx.QueryRow("SELECT reply_route,custody_route,custody_store_id FROM operations WHERE message_id=?", in.Operation.MessageID).Scan(&route, &custody, &storeID); err != nil {
		if err == sql.ErrNoRows {
			return ErrNotFound
		}
		return err
	}
	if route != in.Operation.ReplyRoute || custody != in.Operation.CustodyRoute || storeID != in.Operation.CustodyStoreID {
		return ErrIdentityConflict
	}
	var existing ReplyClaim
	err := scanClaim(tx.QueryRow("SELECT reply_id,original_id,digest,body_size,status,reply_error_code,reply_route,custody_route,custody_store_id,owner,token,lease_until,state,created_at,updated_at,COALESCE(accepted_at,0),COALESCE(commit_seq,0) FROM reply_claims WHERE reply_id=?", in.ReplyID), &existing)
	if err == nil {
		if existing.OriginalID != in.Operation.MessageID || existing.Digest != in.Digest || existing.BodySize != in.BodySize || existing.Status != in.Status || existing.ReplyErrorCode != in.ErrorCode || existing.ReplyRoute != in.Operation.ReplyRoute || existing.CustodyRoute != in.Operation.CustodyRoute || existing.CustodyStoreID != in.Operation.CustodyStoreID || existing.State != StateReplyOutcomeUnknown {
			return ErrIdentityConflict
		}
		return ensureImportedEventTx(tx, existing.ReplyID, StateReplyOutcomeUnknown, in.EventSeq, now)
	}
	if err != sql.ErrNoRows {
		return err
	}
	if _, err := tx.Exec("INSERT INTO reply_claims(reply_id,original_id,digest,body_size,status,reply_route,custody_route,custody_store_id,owner,token,lease_until,state,created_at,updated_at,reply_error_code) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", in.ReplyID, in.Operation.MessageID, in.Digest, in.BodySize, in.Status, in.Operation.ReplyRoute, in.Operation.CustodyRoute, in.Operation.CustodyStoreID, "", "", int64(0), string(StateReplyOutcomeUnknown), now, now, in.ErrorCode); err != nil {
		return err
	}
	return ensureImportedEventTx(tx, in.ReplyID, StateReplyOutcomeUnknown, in.EventSeq, now)
}

func validateOriginalSelectedClaim(claim ReplyClaim, seq int64, native string, winner bool) error {
	if mektup.ValidateID(claim.ReplyID, mektup.MessageIDPrefix) != nil || !validDigest(claim.Digest) || claim.BodySize < 0 || (claim.Status != "success" && claim.Status != "error") || (claim.Status == "success" && claim.ReplyErrorCode != "") || seq <= 0 {
		return fmt.Errorf("%w: corrupt selected reply metadata", ErrCorrupt)
	}
	if winner {
		if claim.State != StateReplyAccepted && claim.State != StateReplyObserved || claim.CommitSeq != seq {
			return fmt.Errorf("%w: corrupt winner state/order", ErrCorrupt)
		}
		if claim.State == StateReplyObserved && native == "" {
			return fmt.Errorf("%w: observed winner lacks native item", ErrCorrupt)
		}
	} else if claim.State != StateReplyOutcomeUnknown {
		return fmt.Errorf("%w: corrupt terminal unknown state", ErrCorrupt)
	}
	return nil
}

func validDigest(value string) bool {
	if len(value) != len("sha256:")+64 || value[:len("sha256:")] != "sha256:" {
		return false
	}
	if strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value[len("sha256:"):])
	return err == nil
}

// ClaimInput is the complete identity fence. Every field is compared for a
// duplicate reply ID, including routes and terminal status.
type ClaimInput struct {
	ReplyID        string
	OriginalID     string
	Digest         string
	BodySize       int64
	Status         string
	ReplyRoute     string
	CustodyRoute   string
	CustodyStoreID string
	Owner          string
	ErrorCode      string
}

func (in ClaimInput) valid() bool {
	return in.ReplyID != "" && in.OriginalID != "" && in.Digest != "" && in.BodySize >= 0 && (in.Status == "success" || in.Status == "error") && in.ReplyRoute != "" && in.CustodyRoute != "" && in.CustodyStoreID != "" && in.Owner != ""
}

// ClaimReply atomically fences one reply ID before a body write. Identical
// active claims join. Expired claims are terminal unknown and are never
// returned as available for another dispatch.
func (j *Journal) ClaimReply(ctx context.Context, in ClaimInput) (ReplyClaim, error) {
	if !in.valid() {
		return ReplyClaim{}, fmt.Errorf("journal: invalid reply claim")
	}
	var out ReplyClaim
	expired := false
	err := j.withTx(ctx, func(tx *sql.Tx) error {
		now := j.nowUnix()
		var existing ReplyClaim
		err := scanClaim(tx.QueryRow("SELECT reply_id,original_id,digest,body_size,status,reply_error_code,reply_route,custody_route,custody_store_id,owner,token,lease_until,state,created_at,updated_at,COALESCE(accepted_at,0),COALESCE(commit_seq,0) FROM reply_claims WHERE reply_id=?", in.ReplyID), &existing)
		if err == nil {
			var existingErrorCode string
			if err := tx.QueryRow("SELECT reply_error_code FROM reply_claims WHERE reply_id=?", in.ReplyID).Scan(&existingErrorCode); err != nil {
				return err
			}
			if existing.OriginalID != in.OriginalID || existing.Digest != in.Digest || existing.BodySize != in.BodySize || existing.Status != in.Status || existing.ReplyRoute != in.ReplyRoute || existing.CustodyRoute != in.CustodyRoute || existing.CustodyStoreID != in.CustodyStoreID || existingErrorCode != in.ErrorCode {
				return ErrIdentityConflict
			}
			if existing.State == StateReplyAccepted || existing.State == StateReplyObserved {
				existing.Joined = true
				existing.Token = ""
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
				existing.Token = ""
				out = existing
				expired = true
				return nil
			}
			existing.Joined = true
			existing.Token = ""
			out = existing
			return nil
		}
		if err != sql.ErrNoRows {
			return err
		}
		// The original request relationship is established by Prepare. A reply
		// receiver cannot introduce an arbitrary relationship through control.
		var originalReplyRoute, originalCustodyRoute, originalStoreID string
		if err := tx.QueryRow("SELECT reply_route,custody_route,custody_store_id FROM operations WHERE message_id=?", in.OriginalID).Scan(&originalReplyRoute, &originalCustodyRoute, &originalStoreID); err != nil {
			if err == sql.ErrNoRows {
				return ErrNotFound
			}
			return err
		}
		if originalReplyRoute != in.ReplyRoute || originalCustodyRoute != in.CustodyRoute || originalStoreID != in.CustodyStoreID {
			return ErrIdentityConflict
		}
		token, err := randomToken()
		if err != nil {
			return err
		}
		lease := now + j.leaseDuration.Nanoseconds()
		_, err = tx.Exec("INSERT INTO reply_claims(reply_id,original_id,digest,body_size,status,reply_route,custody_route,custody_store_id,owner,token,lease_until,state,created_at,updated_at,reply_error_code) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", in.ReplyID, in.OriginalID, in.Digest, in.BodySize, in.Status, in.ReplyRoute, in.CustodyRoute, in.CustodyStoreID, in.Owner, token, lease, string(StateReplyClaimed), now, now, in.ErrorCode)
		if err != nil {
			return err
		}
		if err = emit(tx, "reply.claimed", "", in.ReplyID, StateReplyClaimed, now); err != nil {
			return err
		}
		out = ReplyClaim{ReplyID: in.ReplyID, OriginalID: in.OriginalID, Digest: in.Digest, BodySize: in.BodySize, Status: in.Status, ReplyErrorCode: in.ErrorCode, ReplyRoute: in.ReplyRoute, CustodyRoute: in.CustodyRoute, CustodyStoreID: in.CustodyStoreID, Owner: in.Owner, Token: token, LeaseUntil: lease, State: StateReplyClaimed, CreatedAt: now, UpdatedAt: now}
		return nil
	})
	if err == nil && expired {
		return out, ErrClaimExpired
	}
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
	err := j.withTx(ctx, func(tx *sql.Tx) error {
		now := j.nowUnix()
		lease := now + j.leaseDuration.Nanoseconds()
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
	var result CommitResult
	expired := false
	err := j.withTx(ctx, func(tx *sql.Tx) error {
		now := j.nowUnix()
		var claim ReplyClaim
		if err := scanClaim(tx.QueryRow("SELECT reply_id,original_id,digest,body_size,status,reply_error_code,reply_route,custody_route,custody_store_id,owner,token,lease_until,state,created_at,updated_at,COALESCE(accepted_at,0),COALESCE(commit_seq,0) FROM reply_claims WHERE reply_id=?", replyID), &claim); err != nil {
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
			expired = true
			return nil
		}
		if _, err := tx.Exec("UPDATE reply_claims SET state=?,updated_at=? WHERE reply_id=? AND owner=? AND token=? AND state=? AND lease_until>?", string(StateReplyAccepted), now, replyID, owner, token, string(StateReplyClaimed), now); err != nil {
			return err
		}
		seq, won, err := assignReplyCommitTx(tx, claim.OriginalID, replyID, now)
		if err != nil {
			return err
		}
		result.Won = won
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
	if err == nil && expired {
		return result, ErrClaimExpired
	}
	return result, err
}

// assignReplyCommitTx gives a reconciled or accepted reply a durable custody
// order and inserts the original's first winner without replacing an earlier
// winner. The caller must already hold the write transaction.
func assignReplyCommitTx(tx *sql.Tx, originalID, replyID string, now int64) (int64, bool, error) {
	var seq int64
	if err := tx.QueryRow("SELECT COALESCE(commit_seq,0) FROM reply_claims WHERE reply_id=?", replyID).Scan(&seq); err != nil {
		return 0, false, err
	}
	if seq == 0 {
		if err := tx.QueryRow("SELECT COALESCE(MAX(commit_seq),0)+1 FROM reply_claims").Scan(&seq); err != nil {
			return 0, false, err
		}
		if _, err := tx.Exec("UPDATE reply_claims SET accepted_at=?,commit_seq=? WHERE reply_id=?", now, seq, replyID); err != nil {
			return 0, false, err
		}
	}
	if _, err := tx.Exec("INSERT OR IGNORE INTO reply_winners(original_id,reply_id,committed_at,commit_seq) VALUES(?,?,?,?)", originalID, replyID, now, seq); err != nil {
		return 0, false, err
	}
	var won bool
	if err := tx.QueryRow("SELECT EXISTS(SELECT 1 FROM reply_winners WHERE original_id=? AND reply_id=?)", originalID, replyID).Scan(&won); err != nil {
		return 0, false, err
	}
	return seq, won, nil
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
	_, err := j.expireClaimsAt(ctx, j.nowUnix())
	return err
}

// expireClaimsAt expires claims against one caller-selected custody clock and
// returns the number of rows whose transition committed. Maintenance uses this
// to avoid reporting a pre-read prediction as an applied change.
func (j *Journal) expireClaimsAt(ctx context.Context, now int64) (int64, error) {
	var changed int64
	err := j.withTx(ctx, func(tx *sql.Tx) error {
		var transactionChanged int64
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
			} else if err == nil {
				transactionChanged++
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		changed = transactionChanged
		return nil
	})
	if err != nil {
		return 0, err
	}
	return changed, err
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
	err := scanClaim(j.db.QueryRowContext(ctx, "SELECT reply_id,original_id,digest,body_size,status,reply_error_code,reply_route,custody_route,custody_store_id,owner,token,lease_until,state,created_at,updated_at,COALESCE(accepted_at,0),COALESCE(commit_seq,0) FROM reply_claims WHERE reply_id=?", replyID), &out)
	if err == sql.ErrNoRows {
		return out, ErrNotFound
	}
	// Status inspection never grants body-dispatch authority. The creator
	// retains the token returned by ClaimReply; joined/status callers do not.
	out.Token = ""
	return out, err
}

// ReplyErrorCode returns the selected claim's declared terminal error code.
// It is kept separate from ReplyClaim's stable metadata projection so older
// callers cannot accidentally treat it as dispatch authority.
func (j *Journal) ReplyErrorCode(ctx context.Context, replyID string) (string, error) {
	var code string
	err := j.db.QueryRowContext(ctx, "SELECT reply_error_code FROM reply_claims WHERE reply_id=?", replyID).Scan(&code)
	if err == sql.ErrNoRows {
		return "", ErrNotFound
	}
	return code, err
}

func (j *Journal) RepliesFor(ctx context.Context, originalID string) ([]ReplyClaim, error) {
	rows, err := j.db.QueryContext(ctx, "SELECT reply_id,original_id,digest,body_size,status,reply_error_code,reply_route,custody_route,custody_store_id,owner,token,lease_until,state,created_at,updated_at,COALESCE(accepted_at,0),COALESCE(commit_seq,0) FROM reply_claims WHERE original_id=? ORDER BY created_at,reply_id", originalID)
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
		c.Token = ""
		out = append(out, c)
	}
	return out, rows.Err()
}

func scanClaim(row interface{ Scan(...any) error }, out *ReplyClaim) error {
	return row.Scan(&out.ReplyID, &out.OriginalID, &out.Digest, &out.BodySize, &out.Status, &out.ReplyErrorCode, &out.ReplyRoute, &out.CustodyRoute, &out.CustodyStoreID, &out.Owner, &out.Token, &out.LeaseUntil, &out.State, &out.CreatedAt, &out.UpdatedAt, &out.AcceptedAt, &out.CommitSeq)
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
		err := scanClaim(j.db.QueryRowContext(ctx, `SELECT c.reply_id,c.original_id,c.digest,c.body_size,c.status,c.reply_error_code,c.reply_route,c.custody_route,c.custody_store_id,c.owner,c.token,c.lease_until,c.state,c.created_at,c.updated_at,COALESCE(c.accepted_at,0),COALESCE(c.commit_seq,0)
FROM reply_claims c LEFT JOIN reply_winners w ON w.reply_id=c.reply_id AND w.original_id=c.original_id
WHERE c.original_id=? AND c.state IN (?,?,?)
ORDER BY CASE WHEN w.reply_id IS NOT NULL THEN 0 WHEN c.state IN (?,?) THEN 1 ELSE 2 END,
COALESCE(w.commit_seq,c.commit_seq,9223372036854775807), c.reply_id LIMIT 1`, originalID, string(StateReplyAccepted), string(StateReplyObserved), string(StateReplyOutcomeUnknown), string(StateReplyAccepted), string(StateReplyObserved)), &out)
		if err == nil {
			out.Token = ""
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
	return j.ObserveReplyWithProvenance(ctx, replyID, nativeItemID, digest, "", "")
}

// RecordObservedReply durably projects verified remote native evidence without
// creating a dispatch claim or any fencing authority. It is used when the
// observer discovers a reply before this process has cached a claim.
func (j *Journal) RecordObservedReply(ctx context.Context, in ClaimInput, nativeItemID, endpointID, controlRoute string) error {
	if in.ReplyID == "" || in.OriginalID == "" || in.Digest == "" || in.BodySize < 0 || (in.Status != "success" && in.Status != "error") || in.ReplyRoute == "" || in.CustodyRoute == "" || in.CustodyStoreID == "" || nativeItemID == "" {
		return fmt.Errorf("journal: invalid observed reply projection")
	}
	now := j.nowUnix()
	return j.withTx(ctx, func(tx *sql.Tx) error {
		var existing ReplyClaim
		err := scanClaim(tx.QueryRow("SELECT reply_id,original_id,digest,body_size,status,reply_error_code,reply_route,custody_route,custody_store_id,owner,token,lease_until,state,created_at,updated_at,COALESCE(accepted_at,0),COALESCE(commit_seq,0) FROM reply_claims WHERE reply_id=?", in.ReplyID), &existing)
		if err == nil {
			if existing.OriginalID != in.OriginalID || existing.Digest != in.Digest || existing.BodySize != in.BodySize || existing.Status != in.Status || existing.ReplyErrorCode != in.ErrorCode || existing.ReplyRoute != in.ReplyRoute || existing.CustodyRoute != in.CustodyRoute || existing.CustodyStoreID != in.CustodyStoreID {
				return ErrIdentityConflict
			}
			if existing.State != StateReplyAccepted && existing.State != StateReplyOutcomeUnknown && existing.State != StateReplyObserved {
				return ErrInvalidTransition
			}
			if existing.State != StateReplyObserved {
				if _, err := tx.Exec("UPDATE reply_claims SET state=?,updated_at=? WHERE reply_id=?", string(StateReplyObserved), now, in.ReplyID); err != nil {
					return err
				}
				if err := emit(tx, "reply.observed", "", in.ReplyID, StateReplyObserved, now); err != nil {
					return err
				}
			}
		} else if err != sql.ErrNoRows {
			return err
		} else {
			var replyRoute, custodyRoute, storeID string
			if err := tx.QueryRow("SELECT reply_route,custody_route,custody_store_id FROM operations WHERE message_id=?", in.OriginalID).Scan(&replyRoute, &custodyRoute, &storeID); err != nil {
				if err == sql.ErrNoRows {
					return ErrNotFound
				}
				return err
			}
			if replyRoute != in.ReplyRoute || custodyRoute != in.CustodyRoute || storeID != in.CustodyStoreID {
				return ErrIdentityConflict
			}
			_, err := tx.Exec("INSERT INTO reply_claims(reply_id,original_id,digest,body_size,status,reply_route,custody_route,custody_store_id,owner,token,lease_until,state,created_at,updated_at,reply_error_code) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", in.ReplyID, in.OriginalID, in.Digest, in.BodySize, in.Status, in.ReplyRoute, in.CustodyRoute, in.CustodyStoreID, "", "", int64(0), string(StateReplyObserved), now, now, in.ErrorCode)
			if err != nil {
				return err
			}
			if err := emit(tx, "reply.observed", "", in.ReplyID, StateReplyObserved, now); err != nil {
				return err
			}
		}
		if err := validateObservationIdentityTx(tx, in.ReplyID, nativeItemID, in.Digest, endpointID, controlRoute); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO observations(reply_id,native_item_id,observed_at,digest,endpoint_id,control_route) VALUES(?,?,?,?,?,?) ON CONFLICT(reply_id) DO UPDATE SET observed_at=excluded.observed_at,endpoint_id=CASE WHEN observations.endpoint_id='' THEN excluded.endpoint_id ELSE observations.endpoint_id END,control_route=CASE WHEN observations.control_route='' THEN excluded.control_route ELSE observations.control_route END", in.ReplyID, nativeItemID, now, in.Digest, endpointID, controlRoute); err != nil {
			return err
		}
		var originalID string
		if err := tx.QueryRow("SELECT original_id FROM reply_claims WHERE reply_id=?", in.ReplyID).Scan(&originalID); err != nil {
			return err
		}
		_, _, err = assignReplyCommitTx(tx, originalID, in.ReplyID, now)
		return err
	})
}

// RecordObservedWinner imports authoritative tokenless winner metadata when
// the winning reply is not present in this process's local claim cache. All
// fields come from the custody result; no body or dispatch authority is made
// up locally.
func (j *Journal) RecordObservedWinner(ctx context.Context, originalID, replyID, digest, status, errorCode, replyRoute, custodyRoute, storeID, nativeID, endpointID, controlRoute string, commitSeq int64, bodySize int64) error {
	if originalID == "" || replyID == "" || digest == "" || bodySize < 0 || (status != "success" && status != "error") || replyRoute == "" || custodyRoute == "" || storeID == "" || commitSeq < 1 {
		return fmt.Errorf("journal: invalid observed winner projection")
	}
	return j.withTx(ctx, func(tx *sql.Tx) error {
		now := j.nowUnix()
		return importObservedWinnerTx(tx, OriginalStatusImport{Operation: Operation{MessageID: originalID, ReplyRoute: replyRoute, CustodyRoute: custodyRoute, CustodyStoreID: storeID}, ReplyID: replyID, Digest: digest, BodySize: bodySize, Status: status, ErrorCode: errorCode, CommitSeq: commitSeq, NativeItemID: nativeID}, now)
	})
}

func importObservedWinnerTx(tx *sql.Tx, in OriginalStatusImport, now int64) error {
	var route, custody, storeID string
	if err := tx.QueryRow("SELECT reply_route,custody_route,custody_store_id FROM operations WHERE message_id=?", in.Operation.MessageID).Scan(&route, &custody, &storeID); err != nil {
		if err == sql.ErrNoRows {
			return ErrNotFound
		}
		return err
	}
	if route != in.Operation.ReplyRoute || custody != in.Operation.CustodyRoute || storeID != in.Operation.CustodyStoreID {
		return ErrIdentityConflict
	}
	var currentWinner string
	errWinner := tx.QueryRow("SELECT reply_id FROM reply_winners WHERE original_id=?", in.Operation.MessageID).Scan(&currentWinner)
	if errWinner != nil && errWinner != sql.ErrNoRows {
		return errWinner
	}
	if errWinner == nil && currentWinner != in.ReplyID {
		return ErrIdentityConflict
	}
	winnerState := StateReplyAccepted
	if in.NativeItemID != "" {
		winnerState = StateReplyObserved
	}
	var existing ReplyClaim
	err := scanClaim(tx.QueryRow("SELECT reply_id,original_id,digest,body_size,status,reply_error_code,reply_route,custody_route,custody_store_id,owner,token,lease_until,state,created_at,updated_at,COALESCE(accepted_at,0),COALESCE(commit_seq,0) FROM reply_claims WHERE reply_id=?", in.ReplyID), &existing)
	if err == sql.ErrNoRows {
		if _, err := tx.Exec("INSERT INTO reply_claims(reply_id,original_id,digest,body_size,status,reply_route,custody_route,custody_store_id,owner,token,lease_until,state,created_at,updated_at,accepted_at,commit_seq,reply_error_code) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", in.ReplyID, in.Operation.MessageID, in.Digest, in.BodySize, in.Status, in.Operation.ReplyRoute, in.Operation.CustodyRoute, in.Operation.CustodyStoreID, "", "", int64(0), string(winnerState), now, now, now, in.CommitSeq, in.ErrorCode); err != nil {
			return err
		}
		eventKind := "reply.accepted"
		if winnerState == StateReplyObserved {
			eventKind = "reply.observed"
		}
		if err := emit(tx, eventKind, "", in.ReplyID, winnerState, now); err != nil {
			return err
		}
	} else if err != nil {
		return err
	} else {
		if existing.OriginalID != in.Operation.MessageID || existing.Digest != in.Digest || existing.BodySize != in.BodySize || existing.Status != in.Status || existing.ReplyErrorCode != in.ErrorCode || existing.ReplyRoute != in.Operation.ReplyRoute || existing.CustodyRoute != in.Operation.CustodyRoute || existing.CustodyStoreID != in.Operation.CustodyStoreID || (existing.CommitSeq != 0 && existing.CommitSeq != in.CommitSeq) {
			return ErrIdentityConflict
		}
		if existing.State != StateReplyOutcomeUnknown && existing.State != StateReplyAccepted && existing.State != StateReplyObserved {
			return ErrIdentityConflict
		}
		state := existing.State
		if winnerState == StateReplyObserved || state == StateReplyOutcomeUnknown {
			state = winnerState
		}
		if _, err := tx.Exec("UPDATE reply_claims SET state=?,token='',lease_until=0,accepted_at=COALESCE(accepted_at,?),commit_seq=?,updated_at=? WHERE reply_id=?", string(state), now, in.CommitSeq, now, in.ReplyID); err != nil {
			return err
		}
	}
	if errWinner == sql.ErrNoRows {
		if _, err := tx.Exec("INSERT INTO reply_winners(original_id,reply_id,committed_at,commit_seq) VALUES(?,?,?,?)", in.Operation.MessageID, in.ReplyID, now, in.CommitSeq); err != nil {
			return err
		}
	} else {
		var seq int64
		if err := tx.QueryRow("SELECT commit_seq FROM reply_winners WHERE original_id=? AND reply_id=?", in.Operation.MessageID, in.ReplyID).Scan(&seq); err != nil {
			return err
		}
		if seq != in.CommitSeq {
			return ErrIdentityConflict
		}
	}
	if in.NativeItemID != "" {
		if err := validateObservationIdentityTx(tx, in.ReplyID, in.NativeItemID, in.Digest, "", ""); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO observations(reply_id,native_item_id,observed_at,digest) VALUES(?,?,?,?) ON CONFLICT(reply_id) DO UPDATE SET observed_at=excluded.observed_at", in.ReplyID, in.NativeItemID, now, in.Digest); err != nil {
			return err
		}
	}
	return nil
}

func ensureImportedEventTx(tx *sql.Tx, replyID string, state EvidenceState, remoteSeq int64, now int64) error {
	if remoteSeq > 0 {
		var replySeq int64
		err := tx.QueryRow("SELECT seq FROM events WHERE reply_id=? AND kind='reply.abandoned' AND state=? ORDER BY seq LIMIT 1", replyID, string(state)).Scan(&replySeq)
		if err == nil && replySeq != remoteSeq {
			return ErrIdentityConflict
		}
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		var kind, existingReply string
		var existingState EvidenceState
		err = tx.QueryRow("SELECT kind,COALESCE(reply_id,''),state FROM events WHERE seq=?", remoteSeq).Scan(&kind, &existingReply, &existingState)
		if err == nil {
			if existingReply != replyID || kind != "reply.abandoned" || existingState != state {
				return ErrIdentityConflict
			}
			return nil
		}
		if err != sql.ErrNoRows {
			return err
		}
		_, err = tx.Exec("INSERT INTO events(seq,kind,operation_id,reply_id,state,at) VALUES(?,?,?,?,?,?)", remoteSeq, "reply.abandoned", "", replyID, string(state), now)
		return err
	}
	var count int
	if err := tx.QueryRow("SELECT COUNT(1) FROM events WHERE reply_id=? AND kind='reply.abandoned' AND state=?", replyID, string(state)).Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return nil
	}
	return emit(tx, "reply.abandoned", "", replyID, state, now)
}

// RecordObservedReplyAndWinner applies an authoritative winner projection and
// the newly observed candidate in one transaction. The caller may safely use
// this for untrusted remote results: any identity conflict rolls back every
// claim, observation, winner, and event mutation.
func (j *Journal) RecordObservedReplyAndWinner(ctx context.Context, candidate ClaimInput, nativeItemID, endpointID, controlRoute string, winnerReplyID, winnerDigest, winnerStatus, winnerErrorCode, winnerNativeID string, winnerCommitSeq int64, winnerBodySize int64) error {
	if candidate.ReplyID == "" || candidate.OriginalID == "" || candidate.Digest == "" || candidate.BodySize < 0 || (candidate.Status != "success" && candidate.Status != "error") || candidate.ReplyRoute == "" || candidate.CustodyRoute == "" || candidate.CustodyStoreID == "" || nativeItemID == "" || winnerReplyID == "" || winnerDigest == "" || (winnerStatus != "success" && winnerStatus != "error") || winnerCommitSeq < 1 || winnerBodySize < 0 {
		return fmt.Errorf("journal: invalid observed projection")
	}
	now := j.nowUnix()
	return j.withTx(ctx, func(tx *sql.Tx) error {
		if candidate.ReplyID == winnerReplyID && (candidate.Digest != winnerDigest || candidate.BodySize != winnerBodySize || candidate.Status != winnerStatus || candidate.ErrorCode != winnerErrorCode || winnerNativeID != nativeItemID) {
			return ErrIdentityConflict
		}
		var route, custody, storeID string
		if err := tx.QueryRow("SELECT reply_route,custody_route,custody_store_id FROM operations WHERE message_id=?", candidate.OriginalID).Scan(&route, &custody, &storeID); err != nil {
			if err == sql.ErrNoRows {
				return ErrNotFound
			}
			return err
		}
		if route != candidate.ReplyRoute || custody != candidate.CustodyRoute || storeID != candidate.CustodyStoreID {
			return ErrIdentityConflict
		}
		var currentWinner string
		errWinner := tx.QueryRow("SELECT reply_id FROM reply_winners WHERE original_id=?", candidate.OriginalID).Scan(&currentWinner)
		if errWinner != nil && errWinner != sql.ErrNoRows {
			return errWinner
		}
		if errWinner == nil && currentWinner != winnerReplyID {
			return ErrIdentityConflict
		}
		var existing ReplyClaim
		err := scanClaim(tx.QueryRow("SELECT reply_id,original_id,digest,body_size,status,reply_error_code,reply_route,custody_route,custody_store_id,owner,token,lease_until,state,created_at,updated_at,COALESCE(accepted_at,0),COALESCE(commit_seq,0) FROM reply_claims WHERE reply_id=?", candidate.ReplyID), &existing)
		if err == nil && (existing.OriginalID != candidate.OriginalID || existing.Digest != candidate.Digest || existing.BodySize != candidate.BodySize || existing.Status != candidate.Status || existing.ReplyErrorCode != candidate.ErrorCode || existing.ReplyRoute != candidate.ReplyRoute || existing.CustodyRoute != candidate.CustodyRoute || existing.CustodyStoreID != candidate.CustodyStoreID) {
			return ErrIdentityConflict
		}
		if err != nil && err != sql.ErrNoRows {
			return err
		}
		// Install or strengthen the authoritative winner first, still inside tx.
		var winner ReplyClaim
		werr := scanClaim(tx.QueryRow("SELECT reply_id,original_id,digest,body_size,status,reply_error_code,reply_route,custody_route,custody_store_id,owner,token,lease_until,state,created_at,updated_at,COALESCE(accepted_at,0),COALESCE(commit_seq,0) FROM reply_claims WHERE reply_id=?", winnerReplyID), &winner)
		winnerState := StateReplyAccepted
		if winnerNativeID != "" {
			winnerState = StateReplyObserved
		}
		if werr == sql.ErrNoRows {
			if _, err := tx.Exec("INSERT INTO reply_claims(reply_id,original_id,digest,body_size,status,reply_route,custody_route,custody_store_id,owner,token,lease_until,state,created_at,updated_at,accepted_at,commit_seq,reply_error_code) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", winnerReplyID, candidate.OriginalID, winnerDigest, winnerBodySize, winnerStatus, candidate.ReplyRoute, candidate.CustodyRoute, candidate.CustodyStoreID, "", "", int64(0), string(winnerState), now, now, now, winnerCommitSeq, winnerErrorCode); err != nil {
				return err
			}
			eventKind := "reply.accepted"
			if winnerState == StateReplyObserved {
				eventKind = "reply.observed"
			}
			if err := emit(tx, eventKind, "", winnerReplyID, winnerState, now); err != nil {
				return err
			}
		} else if werr != nil {
			return werr
		} else if winner.OriginalID != candidate.OriginalID || winner.Digest != winnerDigest || winner.BodySize != winnerBodySize || winner.Status != winnerStatus || winner.ReplyErrorCode != winnerErrorCode || winner.ReplyRoute != candidate.ReplyRoute || winner.CustodyRoute != candidate.CustodyRoute || winner.CustodyStoreID != candidate.CustodyStoreID {
			return ErrIdentityConflict
		} else {
			priorWinnerState := winner.State
			state := winner.State
			if winnerNativeID != "" {
				state = StateReplyObserved
			} else if state != StateReplyObserved {
				state = StateReplyAccepted
			}
			if _, err := tx.Exec("UPDATE reply_claims SET state=?,token='',lease_until=0,accepted_at=?,commit_seq=?,updated_at=? WHERE reply_id=?", string(state), now, winnerCommitSeq, now, winnerReplyID); err != nil {
				return err
			}
			if state != priorWinnerState {
				eventKind := "reply.accepted"
				if state == StateReplyObserved {
					eventKind = "reply.observed"
				}
				if err := emit(tx, eventKind, "", winnerReplyID, state, now); err != nil {
					return err
				}
			}
		}
		if errWinner == sql.ErrNoRows {
			if _, err := tx.Exec("INSERT INTO reply_winners(original_id,reply_id,committed_at,commit_seq) VALUES(?,?,?,?)", candidate.OriginalID, winnerReplyID, now, winnerCommitSeq); err != nil {
				return err
			}
		} else if _, err := tx.Exec("UPDATE reply_winners SET committed_at=?,commit_seq=? WHERE original_id=? AND reply_id=?", now, winnerCommitSeq, candidate.OriginalID, winnerReplyID); err != nil {
			return err
		}
		if winnerNativeID != "" {
			if err := validateObservationIdentityTx(tx, winnerReplyID, winnerNativeID, winnerDigest, endpointID, controlRoute); err != nil {
				return err
			}
			if _, err := tx.Exec("INSERT INTO observations(reply_id,native_item_id,observed_at,digest,endpoint_id,control_route) VALUES(?,?,?,?,?,?) ON CONFLICT(reply_id) DO UPDATE SET observed_at=excluded.observed_at", winnerReplyID, winnerNativeID, now, winnerDigest, endpointID, controlRoute); err != nil {
				return err
			}
		}
		if err == sql.ErrNoRows && winnerReplyID != candidate.ReplyID {
			if _, err := tx.Exec("INSERT INTO reply_claims(reply_id,original_id,digest,body_size,status,reply_route,custody_route,custody_store_id,owner,token,lease_until,state,created_at,updated_at,reply_error_code) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", candidate.ReplyID, candidate.OriginalID, candidate.Digest, candidate.BodySize, candidate.Status, candidate.ReplyRoute, candidate.CustodyRoute, candidate.CustodyStoreID, "", "", int64(0), string(StateReplyObserved), now, now, candidate.ErrorCode); err != nil {
				return err
			}
			if err := emit(tx, "reply.observed", "", candidate.ReplyID, StateReplyObserved, now); err != nil {
				return err
			}
		} else if err == nil {
			priorCandidateState := existing.State
			if existing.State != StateReplyObserved && existing.State != StateReplyAccepted && existing.State != StateReplyOutcomeUnknown {
				return ErrInvalidTransition
			}
			if _, err := tx.Exec("UPDATE reply_claims SET state=?,token='',lease_until=0,accepted_at=?,updated_at=? WHERE reply_id=?", string(StateReplyObserved), now, now, candidate.ReplyID); err != nil {
				return err
			}
			if priorCandidateState != StateReplyObserved && candidate.ReplyID != winnerReplyID {
				if err := emit(tx, "reply.observed", "", candidate.ReplyID, StateReplyObserved, now); err != nil {
					return err
				}
			}
		}
		if _, _, err := assignReplyCommitTx(tx, candidate.OriginalID, candidate.ReplyID, now); err != nil {
			return err
		}
		if err := validateObservationIdentityTx(tx, candidate.ReplyID, nativeItemID, candidate.Digest, endpointID, controlRoute); err != nil {
			return err
		}
		_, err = tx.Exec("INSERT INTO observations(reply_id,native_item_id,observed_at,digest,endpoint_id,control_route) VALUES(?,?,?,?,?,?) ON CONFLICT(reply_id) DO UPDATE SET observed_at=excluded.observed_at", candidate.ReplyID, nativeItemID, now, candidate.Digest, endpointID, controlRoute)
		return err
	})
}

// ObserveReplyWithProvenance records exact configured destination identity
// alongside the native evidence. The route is metadata only and never a
// path/executable authority.
func (j *Journal) ObserveReplyWithProvenance(ctx context.Context, replyID, nativeItemID, digest, endpointID, controlRoute string) error {
	if nativeItemID == "" || digest == "" {
		return fmt.Errorf("journal: invalid observation")
	}
	now := j.nowUnix()
	return j.withTx(ctx, func(tx *sql.Tx) error {
		var expected, originalID string
		var state EvidenceState
		if err := tx.QueryRow("SELECT digest,original_id,state FROM reply_claims WHERE reply_id=?", replyID).Scan(&expected, &originalID, &state); err != nil {
			if err == sql.ErrNoRows {
				return ErrNotFound
			}
			return err
		}
		if expected != digest {
			return ErrIdentityConflict
		}
		if state != StateReplyOutcomeUnknown && state != StateReplyAccepted && state != StateReplyObserved {
			return ErrInvalidTransition
		}
		if err := validateObservationIdentityTx(tx, replyID, nativeItemID, digest, endpointID, controlRoute); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO observations(reply_id,native_item_id,observed_at,digest,endpoint_id,control_route) VALUES(?,?,?,?,?,?) ON CONFLICT(reply_id) DO UPDATE SET observed_at=excluded.observed_at,endpoint_id=CASE WHEN observations.endpoint_id='' THEN excluded.endpoint_id ELSE observations.endpoint_id END,control_route=CASE WHEN observations.control_route='' THEN excluded.control_route ELSE observations.control_route END", replyID, nativeItemID, now, digest, endpointID, controlRoute); err != nil {
			return err
		}
		if _, _, err := assignReplyCommitTx(tx, originalID, replyID, now); err != nil {
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

// ReconcileReplyObservation records native history found after a claim expired.
// It strengthens unknown evidence without reopening the claim or restoring its
// fencing token. The earlier reply_outcome_unknown event remains immutable.
func (j *Journal) ReconcileReplyObservation(ctx context.Context, replyID, nativeItemID, digest string) error {
	if nativeItemID == "" || digest == "" {
		return fmt.Errorf("journal: invalid reconciliation")
	}
	return j.withTx(ctx, func(tx *sql.Tx) error {
		now := j.nowUnix()
		var expected, originalID string
		var state EvidenceState
		if err := tx.QueryRow("SELECT digest,original_id,state FROM reply_claims WHERE reply_id=?", replyID).Scan(&expected, &originalID, &state); err != nil {
			if err == sql.ErrNoRows {
				return ErrNotFound
			}
			return err
		}
		if expected != digest {
			return ErrIdentityConflict
		}
		if state != StateReplyOutcomeUnknown && state != StateReplyAccepted && state != StateReplyObserved {
			return ErrInvalidTransition
		}
		if err := validateObservationIdentityTx(tx, replyID, nativeItemID, digest, "", ""); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO observations(reply_id,native_item_id,observed_at,digest) VALUES(?,?,?,?) ON CONFLICT(reply_id) DO UPDATE SET observed_at=excluded.observed_at", replyID, nativeItemID, now, digest); err != nil {
			return err
		}
		if state == StateReplyObserved {
			return nil
		}
		if _, err := tx.Exec("UPDATE reply_claims SET state=?,updated_at=? WHERE reply_id=? AND state=?", string(StateReplyObserved), now, replyID, string(state)); err != nil {
			return err
		}
		if _, _, err := assignReplyCommitTx(tx, originalID, replyID, now); err != nil {
			return err
		}
		return emit(tx, "reply.reconciled", "", replyID, StateReplyObserved, now)
	})
}

func validateObservationIdentityTx(tx *sql.Tx, replyID, nativeItemID, digest, endpointID, controlRoute string) error {
	var existingItem, existingDigest, existingEndpoint, existingRoute string
	err := tx.QueryRow("SELECT native_item_id,digest,endpoint_id,control_route FROM observations WHERE reply_id=?", replyID).Scan(&existingItem, &existingDigest, &existingEndpoint, &existingRoute)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	if existingItem != nativeItemID || existingDigest != digest || (existingEndpoint != "" && endpointID != "" && existingEndpoint != endpointID) || (existingRoute != "" && controlRoute != "" && existingRoute != controlRoute) {
		return ErrIdentityConflict
	}
	return nil
}

// Observation returns the exact native item identity already recorded for a
// reply. It never returns native body content.
func (j *Journal) Observation(ctx context.Context, replyID string) (string, string, string, string, error) {
	var nativeID, digest, endpointID, controlRoute string
	err := j.db.QueryRowContext(ctx, "SELECT native_item_id,digest,endpoint_id,control_route FROM observations WHERE reply_id=?", replyID).Scan(&nativeID, &digest, &endpointID, &controlRoute)
	if err == sql.ErrNoRows {
		return "", "", "", "", ErrNotFound
	}
	return nativeID, digest, endpointID, controlRoute, err
}

// Winner returns the durable first winner and its observed native item, when
// one exists for an original. Observation never replaces this row.
func (j *Journal) Winner(ctx context.Context, originalID string) (string, string, int64, error) {
	var replyID, nativeID string
	var seq int64
	err := j.db.QueryRowContext(ctx, `SELECT w.reply_id,COALESCE(o.native_item_id,''),w.commit_seq
FROM reply_winners w LEFT JOIN observations o ON o.reply_id=w.reply_id
WHERE w.original_id=?`, originalID).Scan(&replyID, &nativeID, &seq)
	if err == sql.ErrNoRows {
		return "", "", 0, ErrNotFound
	}
	return replyID, nativeID, seq, err
}

// WinnerDetails returns only durable metadata for the first winner and its
// observed native item. It is safe to expose in a tokenless result.
func (j *Journal) WinnerDetails(ctx context.Context, originalID string) (ReplyClaim, string, int64, error) {
	replyID, nativeID, seq, err := j.Winner(ctx, originalID)
	if err != nil {
		return ReplyClaim{}, "", 0, err
	}
	claim, err := j.Reply(ctx, replyID)
	if err != nil {
		return ReplyClaim{}, "", 0, err
	}
	return claim, nativeID, seq, nil
}

// ReconcileReplyAccepted is the acceptance-side counterpart when a durable
// destination receipt is recovered after claim expiry. It never reopens the
// expired dispatch token.
func (j *Journal) ReconcileReplyAccepted(ctx context.Context, replyID, evidenceRef string) error {
	if evidenceRef == "" {
		return fmt.Errorf("journal: acceptance evidence reference required")
	}
	return j.withTx(ctx, func(tx *sql.Tx) error {
		now := j.nowUnix()
		var state EvidenceState
		var originalID string
		if err := tx.QueryRow("SELECT state,original_id FROM reply_claims WHERE reply_id=?", replyID).Scan(&state, &originalID); err != nil {
			if err == sql.ErrNoRows {
				return ErrNotFound
			}
			return err
		}
		if state != StateReplyOutcomeUnknown {
			return ErrInvalidTransition
		}
		if _, err := tx.Exec("UPDATE reply_claims SET state=?,updated_at=? WHERE reply_id=? AND state=?", string(StateReplyAccepted), now, replyID, string(StateReplyOutcomeUnknown)); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT OR REPLACE INTO reply_acceptances(reply_id,evidence_ref,recorded_at) VALUES(?,?,?)", replyID, evidenceRef, now); err != nil {
			return err
		}
		if _, _, err := assignReplyCommitTx(tx, originalID, replyID, now); err != nil {
			return err
		}
		return emit(tx, "reply.reconciled", "", replyID, StateReplyAccepted, now)
	})
}
