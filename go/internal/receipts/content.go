package receipts

import (
	"context"
	"fmt"

	mektup "github.com/agensfield/mektup/go"
)

// HistoryItem is the exact, full-view item required for receipt content. A
// history adapter must obtain it by pinned endpoint/thread IDs; search results
// are intentionally not representable here.
type HistoryItem struct {
	EndpointID      string
	ThreadID        string
	TurnID          string
	ItemID          string
	MessageID       string
	ClientMessageID string
	InReplyTo       string
	ReplyStatus     string
	ReplyErrorCode  string
	Body            []byte
	PayloadSHA256   string
}

// HistoryPort exposes only exact full-history reads. It has no send, search,
// resume, or repair method, making accidental resend impossible in this
// domain layer.
type HistoryPort interface {
	FullHistory(context.Context, string, string) ([]HistoryItem, error)
}

type Spill struct {
	Path   string
	Bytes  uint64
	Digest string
}

type ContentResult struct {
	Body   []byte
	Bytes  uint64
	Digest string
	Spill  *Spill
}

type SpillWriter interface {
	WriteSpill(context.Context, []byte, string) (string, error)
}

type ContentOptions struct {
	MaxInlineBytes uint64
	Spill          SpillWriter
}

func (s Store) Content(ctx context.Context, reference string, history HistoryPort, options ContentOptions) (ContentResult, error) {
	if err := s.valid(); err != nil {
		return ContentResult{}, err
	}
	if reference == "" || history == nil {
		return ContentResult{}, fmt.Errorf("%w: reference and exact history port are required", ErrInvalidArguments)
	}
	receipt, err := s.Show(ctx, reference, ShowOptions{})
	if err != nil {
		return ContentResult{}, err
	}
	if receipt.ContentRef == nil {
		return ContentResult{}, ErrContentUnavailable
	}
	ref := receipt.ContentRef
	endpointID, threadID := ref.EndpointID, ref.ThreadID
	items, err := history.FullHistory(ctx, endpointID, threadID)
	if err != nil {
		return ContentResult{}, fmt.Errorf("%w: %v", ErrContentUnavailable, err)
	}
	var match *HistoryItem
	for i := range items {
		item := &items[i]
		if !contentIdentityMatches(*item, receipt.Message.MessageID, ref) {
			continue
		}
		if match != nil {
			return ContentResult{}, fmt.Errorf("%w: multiple exact items", ErrIdentityMismatch)
		}
		match = item
	}
	if match == nil {
		return ContentResult{}, ErrContentUnavailable
	}
	if uint64(len(match.Body)) != ref.PayloadBytes {
		return ContentResult{}, ErrIdentityMismatch
	}
	digest := bodyDigest(match.Body)
	if digest != ref.PayloadSHA256 {
		return ContentResult{}, ErrDigestMismatch
	}
	if match.PayloadSHA256 != "" && match.PayloadSHA256 != ref.PayloadSHA256 {
		return ContentResult{}, ErrDigestMismatch
	}
	result := ContentResult{Bytes: uint64(len(match.Body)), Digest: digest}
	if options.MaxInlineBytes > 0 && result.Bytes > options.MaxInlineBytes {
		if options.Spill == nil {
			return ContentResult{}, ErrOutputTooLarge
		}
		path, err := options.Spill.WriteSpill(ctx, match.Body, digest)
		if err != nil {
			return ContentResult{}, err
		}
		result.Spill = &Spill{Path: path, Bytes: result.Bytes, Digest: digest}
		return result, nil
	}
	result.Body = append([]byte(nil), match.Body...)
	return result, nil
}

func contentIdentityMatches(item HistoryItem, messageID string, ref *mektup.ContentRef) bool {
	if item.EndpointID != "" && item.EndpointID != ref.EndpointID {
		return false
	}
	if item.ThreadID != "" && item.ThreadID != ref.ThreadID {
		return false
	}
	if ref.TurnID != "" && item.TurnID != ref.TurnID {
		return false
	}
	if ref.ItemID != "" && item.ItemID != ref.ItemID {
		return false
	}
	if ref.ClientMessageID != "" && item.ClientMessageID != ref.ClientMessageID {
		return false
	}
	if item.InReplyTo != messageID && item.MessageID != messageID {
		return false
	}
	// A locator names at least one item/client ID, both of which have already
	// been checked. Do not accept a similarly shaped item by digest alone.
	return ref.ItemID != "" || ref.ClientMessageID != ""
}
