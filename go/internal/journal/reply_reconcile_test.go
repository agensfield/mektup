package journal

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestLateNonwinnerObservationStrengthensWithoutReplacingWinner(t *testing.T) {
	var now atomic.Int64
	now.Store(time.Now().UnixNano())
	j := testJournal(t, t.TempDir(), &now)
	prepared(t, j)
	for _, replyID := range []string{"reply-a", "reply-b"} {
		input := claimInput()
		input.ReplyID = replyID
		claim, err := j.ClaimReply(context.Background(), input)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := j.CommitReply(context.Background(), claim.ReplyID, claim.Owner, claim.Token); err != nil {
			t.Fatal(err)
		}
	}
	if err := j.ReconcileReplyObservation(context.Background(), "reply-b", "native-b", claimInput().Digest); err != nil {
		t.Fatalf("late nonwinner observation rejected: %v", err)
	}
	observed, err := j.Reply(context.Background(), "reply-b")
	if err != nil || observed.State != StateReplyObserved {
		t.Fatalf("late reply state=%s err=%v", observed.State, err)
	}
	winner, _, _, err := j.Winner(context.Background(), "msg-1")
	if err != nil || winner != "reply-a" {
		t.Fatalf("winner=%q err=%v", winner, err)
	}
}
