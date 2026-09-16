package executor

import (
	"strings"
	"testing"
	"time"

	"github.com/agensfield/mektup/go/internal/doctor"
	"github.com/agensfield/mektup/go/internal/endpoint"
	"github.com/agensfield/mektup/go/internal/journal"
)

func TestHumanResultUsesSemanticEndpointAndDoctorContent(t *testing.T) {
	endpointOutput := humanResult("endpoint.list", []endpoint.Endpoint{{
		ID: "ep_01999999-9999-7999-8999-999999999999", Alias: "local", Builtin: true,
		Route: endpoint.Route{Kind: endpoint.RouteUnix, UnixSocket: "/tmp/codex.sock"}, Herdr: endpoint.HerdrAuto,
	}}, "", "")
	for _, want := range []string{"ALIAS", "local", "ep_01999999", "unix:/tmp/codex.sock", "auto, builtin"} {
		if !strings.Contains(endpointOutput, want) {
			t.Fatalf("endpoint output missing %q: %q", want, endpointOutput)
		}
	}
	if endpointOutput == "endpoint.list" {
		t.Fatal("endpoint output regressed to event label")
	}

	doctorOutput := humanResult("doctor", doctor.Report{Findings: []doctor.Finding{
		{ID: "state.mode", Severity: doctor.SeverityOK, Message: "owner-private"},
		{ID: "socket", Severity: doctor.SeverityWarning, Message: "daemon unavailable", Path: "/tmp/codex.sock"},
	}}, "", "")
	for _, want := range []string{"doctor: attention required", "1 ok", "1 warnings", "state.mode", "daemon unavailable"} {
		if !strings.Contains(doctorOutput, want) {
			t.Fatalf("doctor output missing %q: %q", want, doctorOutput)
		}
	}
}

func TestHumanResultCoversStorageCollectionsAndRPC(t *testing.T) {
	status := journal.StorageStatus{
		DatabasePath: "/tmp/mektup/journal.sqlite3", DatabaseBytes: 2048, WALBytes: 1024,
		SchemaVersion: 8, SQLiteVersion: "3.53.4", JournalMode: "wal",
		Counts: map[string]int64{"receipts": 2}, OperationCounts: map[string]int64{"accepted": 1},
		ReplyCounts: map[string]int64{"reply_observed": 1}, RetentionCutoff: time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), RetentionEligible: 3,
	}
	storageOutput := humanResult("storage", status, "", "status")
	for _, want := range []string{"/tmp/mektup/journal.sqlite3", "schema=8", "2.0 KiB", "receipts=2", "3 eligible"} {
		if !strings.Contains(storageOutput, want) {
			t.Fatalf("storage output missing %q: %q", want, storageOutput)
		}
	}

	threads := []any{map[string]any{
		"id": "01999999-9999-7999-8999-999999999999", "status": map[string]any{"type": "idle"},
		"model": "gpt-5.6", "name": "release lane", "updatedAt": float64(1789516800),
	}}
	threadOutput := humanResult("thread.list", threads, "cursor-2", "")
	for _, want := range []string{"THREAD", "release lane", "gpt-5.6", "next cursor: cursor-2"} {
		if !strings.Contains(threadOutput, want) {
			t.Fatalf("thread output missing %q: %q", want, threadOutput)
		}
	}

	rpcOutput := humanResult("rpc", map[string]any{
		"method": "thread/read", "id": 7, "effectState": "not_sent", "result": map[string]any{"ok": true},
		"artifact": map[string]any{"path": "/tmp/result.json", "bytes": 42, "sha256": "sha256:abc"},
	}, "", "")
	for _, want := range []string{"rpc thread/read id=7", "artifact: /tmp/result.json bytes=42 digest=sha256:abc", `"ok": true`} {
		if !strings.Contains(rpcOutput, want) {
			t.Fatalf("rpc output missing %q: %q", want, rpcOutput)
		}
	}
}

func TestHumanResultHasUsefulEmptyStatesAndDeterministicFallback(t *testing.T) {
	for kind, want := range map[string]string{
		"thread.list": "no threads", "thread.turns": "no turns", "thread.items": "no items", "search": "no matches",
	} {
		if got := humanResult(kind, []any{}, "", ""); got != want {
			t.Fatalf("%s output=%q want=%q", kind, got, want)
		}
	}
	for _, kind := range []string{"thread.list", "thread.turns", "thread.items", "search"} {
		got := humanResult(kind, []any{}, "next-page", "")
		if !strings.Contains(got, "next cursor: next-page") {
			t.Fatalf("%s empty page lost cursor: %q", kind, got)
		}
	}
	loaded := humanResult("thread.list", []any{"01999999-9999-7999-8999-999999999999"}, "", "")
	if !strings.Contains(loaded, "01999999-9999-7999-8999-999999999999") {
		t.Fatalf("ID-only loaded thread disappeared: %q", loaded)
	}
	fallback := humanResult("future.command", map[string]any{"z": 1, "a": "value"}, "next", "")
	if !strings.Contains(fallback, "future.command:") || !strings.Contains(fallback, `"a": "value"`) || !strings.Contains(fallback, "next cursor: next") {
		t.Fatalf("fallback output=%q", fallback)
	}
}

func TestEveryGenericPublicCommandHasSemanticHumanOutput(t *testing.T) {
	ep := endpoint.Endpoint{
		ID: "ep_01999999-9999-7999-8999-999999999999", Alias: "devbox",
		Route: endpoint.Route{Kind: endpoint.RouteSSH, SSHHost: "devbox"}, Herdr: endpoint.HerdrDisabled,
	}
	tests := []struct {
		kind, subcommand string
		data             any
		want             []string
	}{
		{kind: "endpoint.show", data: ep, want: []string{"endpoint devbox", "ssh:devbox"}},
		{kind: "endpoint.add", data: ep, want: []string{"added endpoint devbox", "disabled"}},
		{kind: "endpoint.remove", data: map[string]any{"selector": "devbox"}, want: []string{"removed endpoint devbox"}},
		{kind: "endpoint.check", data: map[string]any{"endpoint": "devbox", "warnings": []string{"untested server"}}, want: []string{"devbox: reachable", "warning: untested server"}},
		{kind: "storage", subcommand: "check", data: journal.StorageCheck{ReadOnly: true, Integrity: "ok"}, want: []string{"storage check: ok", "read-only=true"}},
		{kind: "storage", subcommand: "maintain", data: journal.MaintenanceReceipt{DryRun: true, Actions: []journal.MaintenanceAction{{Kind: "expiry", Attempted: true, Changed: 2}}}, want: []string{"storage maintenance: dry run", "expiry", "changed=2"}},
		{kind: "storage", subcommand: "vacuum", data: journal.VacuumReceipt{DatabasePath: "/tmp/journal.sqlite3", BeforeBytes: 4096, AfterBytes: 2048, Applied: true}, want: []string{"storage vacuum: applied=true", "4.0 KiB", "2.0 KiB"}},
		{kind: "thread.read", data: []any{map[string]any{"id": "thread-1", "status": map[string]any{"type": "idle"}, "turns": []any{map[string]any{"id": "turn-1", "items": []any{map[string]any{"id": "item-1", "type": "userMessage", "text": "hello"}}}}}}, want: []string{"thread thread-1", "turn turn-1", "userMessage item-1", "hello"}},
		{kind: "thread.turns", data: []any{map[string]any{"id": "turn-1", "status": "completed", "items": []any{map[string]any{"id": "item-1"}}, "itemsView": "summary"}}, want: []string{"TURN", "turn-1", "completed", "summary"}},
		{kind: "thread.items", data: []any{map[string]any{"turnId": "turn-1", "item": map[string]any{"id": "item-1", "type": "agentMessage", "text": "answer"}}}, want: []string{"ITEM", "turn-1", "item-1", "answer"}},
		{kind: "thread.start", data: map[string]any{"threadId": "thread-new", "status": "idle", "model": "gpt-5.6"}, want: []string{"thread start thread-new", "status=idle", "model=gpt-5.6"}},
		{kind: "thread.resume", data: map[string]any{"threadId": "thread-1"}, want: []string{"thread resume thread-1"}},
		{kind: "thread.fork", data: map[string]any{"threadId": "thread-fork"}, want: []string{"thread fork thread-fork"}},
		{kind: "search", data: []any{map[string]any{"thread": map[string]any{"id": "thread-1", "status": map[string]any{"type": "idle"}, "name": "Mektup release"}, "snippet": "human output"}}, want: []string{"THREAD", "Mektup release", "human output"}},
	}
	for _, test := range tests {
		t.Run(test.kind+"/"+test.subcommand, func(t *testing.T) {
			got := humanResult(test.kind, test.data, "", test.subcommand)
			if strings.TrimSpace(got) == "" || got == test.kind {
				t.Fatalf("non-semantic output=%q", got)
			}
			for _, want := range test.want {
				if !strings.Contains(got, want) {
					t.Fatalf("output missing %q: %q", want, got)
				}
			}
		})
	}
}
