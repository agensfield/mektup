package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/internal/codexapi"
	"github.com/agensfield/mektup/go/internal/service"
)

type originalSearchFake struct {
	hits      codexapi.SearchOccurrencesResponse
	item      codexapi.ExactHistoryItem
	searches  int
	itemReads int
}

func (f *originalSearchFake) SearchOccurrences(_ context.Context, in codexapi.SearchOccurrencesOptions) (codexapi.SearchOccurrencesResponse, error) {
	f.searches++
	if in.ThreadID != "thread-1" || in.SearchTerm == "" || in.Limit != maxOriginalSearchHits {
		return codexapi.SearchOccurrencesResponse{}, fmt.Errorf("wrong search options: %+v", in)
	}
	return f.hits, nil
}

func (f *originalSearchFake) ReadItemAtTurnCursor(_ context.Context, threadID, turnID, itemID, cursor string) (codexapi.ExactHistoryItem, error) {
	f.itemReads++
	if threadID != "thread-1" || turnID != "turn-1" || itemID != "item-1" || cursor != "turn-cursor" {
		return codexapi.ExactHistoryItem{}, fmt.Errorf("wrong exact lookup: %q %q %q %q", threadID, turnID, itemID, cursor)
	}
	return f.item, nil
}

type countingOriginalHistory struct{ reads int }

func (h *countingOriginalHistory) Subscribe(context.Context, service.ResolvedTarget) (service.EventStream, error) {
	return nil, errors.New("not used")
}
func (h *countingOriginalHistory) FullHistory(context.Context, service.ResolvedTarget) ([]service.ObservedItem, error) {
	h.reads++
	return nil, errors.New("whole-history scan must not run")
}

func TestIndexedOriginalLookupVerifiesNativeItemWithoutFullHistory(t *testing.T) {
	envelope := mektup.Envelope{MessageID: "msg_01999999-9999-7999-8999-999999999999", Kind: mektup.KindMessage,
		FromEndpointID: testSourceEndpoint, From: "codex://source/thread/source", FromKind: "agent",
		ToEndpointID: testTargetEndpoint, To: "codex://target/thread/thread-1", RequestedTarget: "target",
		ReplyRequested: true, ReplyEndpointID: testSourceEndpoint, ReplyTo: "codex://source/thread/source",
		ReplyCustodyEndpointID: testSourceEndpoint, ReplyCustodyStoreID: "store_01999999-9999-7999-8999-999999999991",
		Body: "question", Provenance: "observed", SentAt: time.Now().UTC().Format(time.RFC3339Nano)}
	text, err := mektup.RenderEnvelope(envelope)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(map[string]any{"id": "item-1", "type": "userMessage", "clientId": envelope.MessageID, "content": []any{map[string]any{"type": "text", "text": string(text)}}})
	if err != nil {
		t.Fatal(err)
	}
	fake := &originalSearchFake{hits: codexapi.SearchOccurrencesResponse{Data: []codexapi.SearchOccurrence{{TurnID: "turn-1", ItemID: "item-1", TurnCursor: "turn-cursor"}}}, item: codexapi.ExactHistoryItem{TurnID: "turn-1", ItemID: "item-1", Item: raw}}
	history := &countingOriginalHistory{}
	resolver := OriginalResolver{Observe: history, Target: testTarget(), CandidateLookup: func(ctx context.Context, target service.ResolvedTarget, reference string) ([]service.ObservedItem, error) {
		return SearchOriginalCandidates(ctx, fake, target, reference)
	}}
	got, err := resolver.ResolveOriginal(context.Background(), envelope.MessageID)
	if err != nil || got.Envelope.MessageID != envelope.MessageID || fake.searches != 1 || fake.itemReads != 1 || history.reads != 0 {
		t.Fatalf("indexed original = %+v, err=%v, search=%d, exact=%d, full=%d", got, err, fake.searches, fake.itemReads, history.reads)
	}
	fake.hits.Data = nil
	if _, err := resolver.ResolveOriginal(context.Background(), envelope.MessageID); !errors.Is(err, service.ErrOriginalLookupIncomplete) || history.reads != 0 {
		t.Fatalf("search miss must be incomplete without full scan: %v, full=%d", err, history.reads)
	}
	if _, err := SearchOriginalCandidates(context.Background(), &originalSearchFake{hits: codexapi.SearchOccurrencesResponse{NextCursor: "more"}}, testTarget(), envelope.MessageID); err == nil {
		t.Fatal("oversized search result became partial authority")
	}
}
