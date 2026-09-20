package liveacceptance

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/agensfield/mektup/go/appserver"
	"github.com/agensfield/mektup/go/internal/codexapi"
	"github.com/agensfield/mektup/go/internal/compat"
	"github.com/agensfield/mektup/go/internal/connection"
	"github.com/agensfield/mektup/go/internal/endpoint"
	"github.com/agensfield/mektup/go/internal/rawrpc"
)

func TestCentralDaemonReadOnlyTwoClients(t *testing.T) {
	socket := os.Getenv("MEKTUP_ACCEPT_SOCKET")
	if socket == "" {
		t.Skip("set MEKTUP_ACCEPT_SOCKET to run read-only live app-server acceptance")
	}
	threadID := os.Getenv("MEKTUP_ACCEPT_THREAD_ID")
	if threadID == "" {
		t.Skip("set MEKTUP_ACCEPT_THREAD_ID to an existing thread for exact read acceptance")
	}
	route, err := endpoint.UnixRoute(socket)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	clients := make([]*connection.Connection, 0, 2)
	for i := 0; i < 2; i++ {
		client, err := connection.Connect(ctx, route, connection.Options{
			ClientName:       "mektup-live-acceptance",
			ClientVersion:    "1.0.0-dev",
			HandshakeTimeout: 5 * time.Second,
		})
		if err != nil {
			t.Fatalf("connect client %d: %v", i+1, err)
		}
		clients = append(clients, client)
	}
	t.Cleanup(func() {
		for _, client := range clients {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 2*time.Second)
			_ = client.Close(closeCtx)
			closeCancel()
		}
	})

	for i, client := range clients {
		info := client.Info()
		if info.Compatibility.Class != compat.Tested {
			t.Fatalf("client %d compatibility = %+v", i+1, info.Compatibility)
		}
		if expected := os.Getenv("MEKTUP_ACCEPT_CODEX_VERSION"); expected != "" && info.Compatibility.Version != expected {
			t.Fatalf("client %d server version = %q, want %q", i+1, info.Compatibility.Version, expected)
		}
		t.Logf("socket=%s client=%d expected=%s userAgent=%s", socket, i+1, os.Getenv("MEKTUP_ACCEPT_CODEX_VERSION"), info.ServerUserAgent)
		if info.Generation == 0 || info.ServerUserAgent == "" {
			t.Fatalf("client %d missing handshake evidence: %+v", i+1, info)
		}
		api := codexapi.New(client, codexapi.Options{})
		page, err := api.ThreadList(ctx, codexapi.ThreadListOptions{Limit: 2})
		if err != nil {
			t.Fatalf("client %d bounded list: %v", i+1, err)
		}
		if len(page.Data) > 2 {
			t.Fatalf("client %d returned unbounded page of %d", i+1, len(page.Data))
		}
		read, err := api.ThreadRead(ctx, codexapi.ThreadReadOptions{ThreadID: threadID})
		if err != nil {
			t.Fatalf("client %d exact read: %v", i+1, err)
		}
		if read.Thread.ID != threadID {
			t.Fatalf("client %d read thread %q, want %q", i+1, read.Thread.ID, threadID)
		}

		raw, err := rawrpc.Execute(ctx, connection.NewRPCAdapter(client), rawrpc.Request{
			Method:       "thread/read",
			Params:       json.RawMessage(`{"threadId":` + mustJSONString(t, threadID) + `}`),
			ParamsSource: rawrpc.ParamsInline,
		})
		if err != nil {
			t.Fatalf("client %d raw stable read: %v", i+1, err)
		}
		if len(raw.Raw) == 0 || raw.WriteEvidence.Phase != appserver.WriteComplete || raw.WriteEvidence.Generation != client.Generation() {
			t.Fatalf("client %d raw evidence = %+v", i+1, raw)
		}
	}
}

func mustJSONString(t *testing.T, value string) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}
