package runtime

import (
	"context"
	"testing"

	"github.com/agensfield/mektup/go/internal/journal"
	"github.com/agensfield/mektup/go/internal/service"
)

func TestJournalAdapterUsesPerOperationIdentityRegistry(t *testing.T) {
	inner, err := journal.Open(context.Background(), journal.Options{StateDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer inner.Close()
	adapter, err := NewJournalAdapter(inner, service.NewMemoryIdentityRegistry())
	if err != nil {
		t.Fatal(err)
	}
	ops := []service.Operation{
		{OperationID: "op-one", MessageID: "msg-one", SourceRoute: "codex://one/thread/s", TargetRoute: "codex://one/thread/t", Semantics: "message", SourceEndpointID: testSourceEndpoint, TargetEndpointID: testTargetEndpoint, Digest: "sha256:one", BodySize: 1},
		{OperationID: "op-two", MessageID: "msg-two", SourceRoute: "codex://two/thread/s", TargetRoute: "codex://two/thread/t", Semantics: "message", SourceEndpointID: testTargetEndpoint, TargetEndpointID: testSourceEndpoint, Digest: "sha256:two", BodySize: 2},
	}
	for _, op := range ops {
		if _, err := adapter.Prepare(context.Background(), op); err != nil {
			t.Fatal(err)
		}
	}
	first, err := adapter.Lookup(context.Background(), "msg-one")
	if err != nil {
		t.Fatal(err)
	}
	second, err := adapter.Lookup(context.Background(), "msg-two")
	if err != nil {
		t.Fatal(err)
	}
	if first.SourceEndpointID != testSourceEndpoint || first.TargetEndpointID != testTargetEndpoint || second.SourceEndpointID != testTargetEndpoint || second.TargetEndpointID != testSourceEndpoint {
		t.Fatalf("operation identities crossed: first=%#v second=%#v", first, second)
	}
}
