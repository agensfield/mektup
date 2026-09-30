package liveacceptance

import (
	"bytes"
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mektup "github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/appserver"
	"github.com/agensfield/mektup/go/internal/application"
	"github.com/agensfield/mektup/go/internal/cli"
	"github.com/agensfield/mektup/go/internal/codexapi"
	"github.com/agensfield/mektup/go/internal/connection"
	"github.com/agensfield/mektup/go/internal/endpoint"
)

// Exercise production CLI composition against disposable native history.
func qualifyLargeHistory(t *testing.T, ctx context.Context, api *codexapi.Client, route endpoint.Route, codexHome, root, sourceID string) {
	t.Helper()
	large, err := api.ThreadStart(ctx, codexapi.StartOptions{Model: "mock-model", CWD: root})
	if err != nil {
		t.Fatal(err)
	}
	body := strings.Repeat("x", 900_000)
	for i := 0; i < 20; i++ {
		if _, err := api.StartOrSteer(ctx, large.Thread.ID, body, mektup.NewMessageID()); err != nil {
			t.Fatalf("seed turn %d: %v", i, err)
		}
		waitThread(t, ctx, api, large.Thread.ID, true)
	}
	if _, err := api.ThreadUnsubscribe(ctx, large.Thread.ID); err != nil {
		t.Fatal(err)
	}
	waitThread(t, ctx, api, large.Thread.ID, false)
	baseline, err := connection.Connect(ctx, route, connection.Options{ClientName: "mektup-large-baseline", ExperimentalAPI: true})
	if err != nil {
		t.Fatal(err)
	}
	_, resumeErr := codexapi.New(baseline, codexapi.Options{Capabilities: baseline.Capabilities()}).ThreadResume(ctx, codexapi.ResumeOptions{ThreadID: large.Thread.ID})
	_ = baseline.Close(ctx)
	if resumeErr == nil || !strings.Contains(resumeErr.Error(), "read limit exceeded") {
		t.Fatalf("full-history resume should exceed transport limit: %v", resumeErr)
	}
	t.Logf("full-history resume exceeded transport limit: %v", resumeErr)
	waitThread(t, ctx, api, large.Thread.ID, false)
	config := filepath.Join(root, "large-history", "endpoints.json")
	state := filepath.Join(root, "large-history", "state")
	identity := filepath.Join(root, "large-history", "identity")
	run := func(args ...string) int {
		t.Helper()
		resumes := 0
		observer := func(direction appserver.FrameDirection, frame appserver.Frame) error {
			if direction != appserver.FrameOutbound {
				return nil
			}
			var request struct {
				Method string `json:"method"`
				Params struct {
					ExcludeTurns bool `json:"excludeTurns"`
				} `json:"params"`
			}
			if err := json.Unmarshal(frame.Payload, &request); err != nil {
				return err
			}
			if request.Method == "thread/resume" {
				resumes++
				if !request.Params.ExcludeTurns {
					t.Error("production CLI requested full history")
				}
			}
			return nil
		}
		env := application.New(application.Options{CodexHome: codexHome, ConfigPath: config, StateDir: state, IdentityHome: identity, CurrentThreadID: sourceID, Connection: connection.Options{FrameObserver: observer, ExperimentalAPI: true}})
		var out, diagnostics bytes.Buffer
		app := &cli.App{Out: &out, Err: &diagnostics, Executor: env, Env: []string{"MEKTUP_AGENT=1", "CODEX_HOME=" + codexHome, "CODEX_THREAD_ID=" + sourceID, "MEKTUP_CONFIG=" + config, "MEKTUP_STATE_DIR=" + state}}
		if code := app.Run(append([]string{"--json"}, args...)); code != 0 {
			t.Fatalf("CLI %s exit=%d out=%s stderr=%s", args[0], code, out.String(), diagnostics.String())
		}
		if out.Len() > 64<<10 || strings.Contains(out.String(), body[:100]) {
			t.Fatal("CLI output included stored history")
		}
		return resumes
	}
	uri := "codex://local/thread/" + large.Thread.ID
	if count := run("thread", "resume", uri); count != 1 {
		t.Fatalf("explicit resume count=%d", count)
	}
	waitThread(t, ctx, api, large.Thread.ID, false)
	marker := "large-history-cold-send"
	if count := run("send", uri, marker); count != 1 {
		t.Fatalf("cold send resume count=%d", count)
	}
	waitThread(t, ctx, api, large.Thread.ID, false)
	turns, err := api.ThreadTurns(ctx, codexapi.TurnsOptions{ThreadID: large.Thread.ID, Limit: 1, SortDirection: "desc", ItemsView: "full"})
	if err != nil || len(turns.Data) != 1 || strings.Count(string(turns.Data[0].Raw), marker) != 1 {
		t.Fatalf("cold send body missing or duplicated: err=%v turns=%d", err, len(turns.Data))
	}
	exclude := true
	if _, err := api.ThreadResume(ctx, codexapi.ResumeOptions{ThreadID: large.Thread.ID, ExcludeTurns: &exclude}); err != nil {
		t.Fatal(err)
	}
	if count := run("send", uri, "large-history-loaded-send"); count != 0 {
		t.Fatalf("loaded send unnecessarily resumed %d times", count)
	}
	waitThread(t, ctx, api, large.Thread.ID, true)
	if _, err := api.ThreadUnsubscribe(ctx, large.Thread.ID); err != nil {
		t.Fatal(err)
	}
	t.Log("production CLI: metadata resume, cold send with one resume, loaded send without resume, exact paginated native body")
}

func waitThread(t *testing.T, ctx context.Context, api *codexapi.Client, threadID string, loaded bool) {
	t.Helper()
	for ctx.Err() == nil {
		if loaded {
			read, err := api.ThreadRead(ctx, codexapi.ThreadReadOptions{ThreadID: threadID})
			if err != nil {
				t.Fatal(err)
			}
			if read.Thread.Status == "idle" {
				return
			}
		} else {
			list, err := api.ThreadLoadedList(ctx, "", 100)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, id := range list.Data {
				if id == threadID {
					found = true
				}
			}
			if !found {
				return
			}
		}
		select {
		case <-ctx.Done():
		case <-time.After(20 * time.Millisecond):
		}
	}
	t.Fatalf("thread state did not converge: loaded=%v err=%v", loaded, ctx.Err())
}
