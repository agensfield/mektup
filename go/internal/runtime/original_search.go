package runtime

import (
	"context"
	"errors"
	"fmt"

	"github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/internal/codexapi"
	"github.com/agensfield/mektup/go/internal/service"
)

// OriginalSearchClient is the read-only native surface used to locate an
// original without materializing a possibly huge thread. Search metadata is
// never returned as evidence; every candidate is read back as a full item.
type OriginalSearchClient interface {
	SearchOccurrences(context.Context, codexapi.SearchOccurrencesOptions) (codexapi.SearchOccurrencesResponse, error)
	ReadItemAtTurnCursor(context.Context, string, string, string, string) (codexapi.ExactHistoryItem, error)
}

const maxOriginalSearchHits = 32
const maxOriginalCandidateReads = 8

func SearchOriginalCandidates(ctx context.Context, client OriginalSearchClient, target service.ResolvedTarget, reference string) ([]service.ObservedItem, error) {
	if client == nil || target.ThreadID == "" || reference == "" {
		return nil, fmt.Errorf("original search requires a client, thread and reference")
	}
	page, err := client.SearchOccurrences(ctx, codexapi.SearchOccurrencesOptions{ThreadID: target.ThreadID, SearchTerm: reference, Limit: maxOriginalSearchHits})
	if err != nil {
		var serverErr *codexapi.ServerError
		if errors.Is(err, codexapi.ErrExperimentalAPIRequired) || errors.As(err, &serverErr) && serverErr.Code == -32601 {
			return nil, ErrTargetedOriginalLookupUnsupported
		}
		return nil, err
	}
	if page.NextCursor != "" {
		return nil, fmt.Errorf("original search exceeded %d hits", maxOriginalSearchHits)
	}
	seen := make(map[string]bool)
	items := make([]service.ObservedItem, 0, len(page.Data))
	for _, hit := range page.Data {
		key := hit.TurnID + "\x00" + hit.ItemID
		if seen[key] {
			continue
		}
		seen[key] = true
		if len(seen) > maxOriginalCandidateReads {
			return nil, fmt.Errorf("original search exceeded %d exact item reads", maxOriginalCandidateReads)
		}
		item, err := client.ReadItemAtTurnCursor(ctx, target.ThreadID, hit.TurnID, hit.ItemID, hit.TurnCursor)
		if err != nil {
			return nil, fmt.Errorf("read indexed original candidate: %w", err)
		}
		observed, ok := visibleItem(item.Item, target.ThreadID, item.TurnID, "")
		if !ok {
			continue
		}
		envelope, parseErr := mektup.ParseEnvelopeString(observed.Text)
		if parseErr == nil && matchesExact(reference, envelope, observed.ClientMessageID) {
			items = append(items, observed)
		}
	}
	return items, nil
}
