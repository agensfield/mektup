//go:build darwin || linux

package controlreceiver

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/agensfield/mektup/go/internal/journal"
)

const (
	registryWALHelperMode = "MEKTUP_TEST_REGISTRY_WAL_MODE"
	registryWALHelperDir  = "MEKTUP_TEST_REGISTRY_WAL_DIR"
)

func registryWALClaim() journal.ClaimInput {
	return journal.ClaimInput{
		ReplyID:        "reply-1",
		OriginalID:     "msg-1",
		Digest:         "reply-digest",
		BodySize:       9,
		Status:         "success",
		ReplyRoute:     "reply-route",
		CustodyRoute:   "custody",
		CustodyStoreID: "store-test",
		Owner:          "receiver-a",
	}
}

func TestRegistryWALHelper(t *testing.T) {
	mode := os.Getenv(registryWALHelperMode)
	if mode == "" {
		return
	}
	now := time.Unix(1000, 0)
	if mode == "commit" {
		now = now.Add(100 * time.Millisecond)
	}
	j, err := journal.OpenExisting(context.Background(), journal.Options{
		StateDir:      os.Getenv(registryWALHelperDir),
		LeaseDuration: time.Second,
		Now:           func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	switch mode {
	case "claim":
		var claim journal.ReplyClaim
		claim, err = j.ClaimReply(context.Background(), registryWALClaim())
		if err == nil {
			err = os.WriteFile(filepath.Join(os.Getenv(registryWALHelperDir), ".wal-test-token"), []byte(claim.Token), 0600)
		}
	case "commit":
		claim, replyErr := j.Reply(context.Background(), "reply-1")
		if replyErr != nil {
			err = replyErr
			break
		}
		token, readErr := os.ReadFile(filepath.Join(os.Getenv(registryWALHelperDir), ".wal-test-token"))
		if readErr != nil {
			err = readErr
			break
		}
		if _, err = j.Heartbeat(context.Background(), claim.ReplyID, claim.Owner, string(token)); err == nil {
			_, err = j.CommitReply(context.Background(), claim.ReplyID, claim.Owner, string(token))
		}
	default:
		t.Fatalf("unknown helper mode %q", mode)
	}
	if err != nil {
		t.Fatal(err)
	}
}

func TestRegistryOpenClosePreservesCrossProcessCommit(t *testing.T) {
	ctx := context.Background()
	stateDir := filepath.Join(t.TempDir(), "state")
	var clock atomic.Int64
	clock.Store(time.Unix(1000, 0).UnixNano())
	j, err := journal.Open(ctx, journal.Options{
		StateDir:      stateDir,
		LeaseDuration: time.Second,
		Now:           func() time.Time { return time.Unix(0, clock.Load()) },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	op, err := j.Prepare(ctx, journal.Operation{
		OperationID: "op-1", MessageID: "msg-1", SourceRoute: "src", TargetRoute: "dst",
		Semantics: "send", ReplyRoute: "reply-route", CustodyRoute: "custody",
		CustodyStoreID: "store-test", AttemptOwner: "owner-1", Digest: "digest-1", BodySize: 7,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := j.MarkDispatchStarted(ctx, op.OperationID); err != nil {
		t.Fatal(err)
	}
	if err := j.RecordResult(ctx, op.OperationID, journal.StateAccepted, ""); err != nil {
		t.Fatal(err)
	}

	registry := FileRegistry{Path: filepath.Join(t.TempDir(), controlRegistryFilename)}
	endpointID := "ep_0198f0e0-0000-7000-8000-000000000001"
	if err := registry.Register(ctx, endpointID, j); err != nil {
		t.Fatal(err)
	}
	resolved, err := registry.Resolve(ctx, endpointID, j.StoreID())
	if err != nil {
		t.Fatal(err)
	}
	if err := resolved.Close(); err != nil {
		t.Fatal(err)
	}

	runChild := func(mode string) {
		t.Helper()
		cmd := exec.Command(os.Args[0], "-test.run=^TestRegistryWALHelper$", "-test.v")
		cmd.Env = append(os.Environ(), registryWALHelperMode+"="+mode, registryWALHelperDir+"="+stateDir)
		if output, runErr := cmd.CombinedOutput(); runErr != nil {
			t.Fatalf("%s helper: %v\n%s", mode, runErr, output)
		}
	}
	runChild("claim")
	initial, err := j.Reply(ctx, "reply-1")
	if err != nil {
		t.Fatal(err)
	}
	runChild("commit")
	accepted, err := j.Reply(ctx, "reply-1")
	if err != nil {
		t.Fatal(err)
	}
	if accepted.State != journal.StateReplyAccepted || accepted.AcceptedAt == 0 || accepted.CommitSeq == 0 || accepted.LeaseUntil <= initial.LeaseUntil {
		t.Fatalf("cross-process commit not visible: initial=%+v accepted=%+v", initial, accepted)
	}

	clock.Add(int64(2 * time.Second))
	if err := j.ExpireClaims(ctx); err != nil {
		t.Fatal(err)
	}
	afterExpiry, err := j.Reply(ctx, "reply-1")
	if err != nil {
		t.Fatal(err)
	}
	if afterExpiry.State != journal.StateReplyAccepted || afterExpiry.AcceptedAt != accepted.AcceptedAt || afterExpiry.CommitSeq != accepted.CommitSeq {
		t.Fatalf("accepted reply regressed after expiry: accepted=%+v after=%+v", accepted, afterExpiry)
	}
}
