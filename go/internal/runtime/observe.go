package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/agensfield/mektup/go/appserver"
	"github.com/agensfield/mektup/go/internal/service"
)

// ObservationAdapter establishes an event stream before the caller starts a
// history scan. FullHistory deliberately uses full turn pages, because legacy
// local stores reject thread/items/list and summary views omit steered inputs.
type ObservationAdapter struct {
	Pool *ConnectionPool
}

func (a *ObservationAdapter) Subscribe(ctx context.Context, target service.ResolvedTarget) (service.EventStream, error) {
	if a == nil || a.Pool == nil {
		return nil, errors.New("runtime: observation pool is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	streamCtx, streamCancel := context.WithCancel(ctx)
	session, err := a.Pool.openIsolated(streamCtx, target)
	if err != nil {
		streamCancel()
		return nil, err
	}
	if err := session.Resume(streamCtx, target.ThreadID); err != nil {
		streamCancel()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = session.Detach(cleanupCtx)
		cleanupCancel()
		return nil, err
	}
	return &eventStream{session: session, target: target, ctx: streamCtx, cancel: streamCancel}, nil
}

func (a *ObservationAdapter) FullHistory(ctx context.Context, target service.ResolvedTarget) ([]service.ObservedItem, error) {
	if a == nil || a.Pool == nil {
		return nil, errors.New("runtime: observation pool is required")
	}
	session, err := a.Pool.session(ctx, target)
	if err != nil {
		return nil, err
	}
	turns, err := session.History(ctx, target.ThreadID)
	if err != nil {
		fallback, ok := session.(itemHistorySession)
		if !ok {
			return nil, err
		}
		entries, fallbackErr := fallback.ItemsHistory(ctx, target.ThreadID)
		if fallbackErr != nil {
			return nil, errors.Join(err, fallbackErr)
		}
		items := make([]service.ObservedItem, 0, len(entries))
		for _, entry := range entries {
			if item, ok := visibleItem(entry.Item.Raw, target.ThreadID, entry.TurnID, ""); ok {
				items = append(items, item)
			}
		}
		return items, nil
	}
	items := make([]service.ObservedItem, 0)
	for _, turn := range turns {
		rawItems, ok := turn.Fields["items"]
		if !ok {
			continue
		}
		var values []json.RawMessage
		if err := json.Unmarshal(rawItems, &values); err != nil {
			return nil, fmt.Errorf("runtime: full turn %s items: %w", turn.ID, err)
		}
		for _, raw := range values {
			if item, ok := visibleItem(raw, target.ThreadID, turn.ID, ""); ok {
				items = append(items, item)
			}
		}
	}
	return items, nil
}

type eventStream struct {
	session Session
	target  service.ResolvedTarget
	ctx     context.Context
	cancel  context.CancelFunc
	mu      sync.Mutex
	closed  bool
}

func (s *eventStream) Next(ctx context.Context) (service.Event, error) {
	if s == nil || s.isClosed() {
		return service.Event{}, io.EOF
	}
	if ctx == nil {
		ctx = context.Background()
	}
	nextCtx, stop := context.WithCancel(ctx)
	var stopClose func() bool
	if s.ctx != nil {
		stopClose = context.AfterFunc(s.ctx, stop)
	} else {
		stopClose = func() bool { return false }
	}
	defer func() {
		stopClose()
		stop()
	}()
	for {
		event, err := s.session.NextEvent(nextCtx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return service.Event{Gap: true, Reason: "app-server event stream disconnected"}, nil
			}
			return service.Event{}, err
		}
		switch event.Kind {
		case appserver.EventGap:
			reason := "app-server event gap"
			if event.Gap != nil && event.Gap.Reason != "" {
				reason = event.Gap.Reason
			}
			return service.Event{Gap: true, Reason: reason}, nil
		case appserver.EventDisconnected:
			reason := "app-server event stream disconnected"
			if event.Disconnected != nil && event.Disconnected.Err != nil {
				reason += ": " + event.Disconnected.Err.Error()
			}
			return service.Event{Gap: true, Reason: reason}, nil
		case appserver.EventServerRequest:
			// Mektup is a passive observer and MUST NOT answer any server request,
			// including unknown methods. Discard only the payload, retaining the
			// connection's callback for another subscribed client.
			continue
		case appserver.EventNotification:
			if event.Notification == nil || event.Notification.Method != "item/completed" {
				continue
			}
			item, ok := notificationItem(*event.Notification, s.target.ThreadID)
			if !ok {
				continue
			}
			return service.Event{Item: &item}, nil
		default:
			continue
		}
	}
}

func (s *eventStream) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()
	if s.cancel != nil {
		s.cancel()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// This stream owns an isolated observer connection, so it can release its
	// subscription and detach without affecting delivery or other observers.
	err := s.session.Unsubscribe(ctx, s.target.ThreadID)
	return errors.Join(err, s.session.Detach(ctx))
}

func (s *eventStream) isClosed() bool {
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	return closed
}

func notificationItem(notification appserver.RPCNotification, threadID string) (service.ObservedItem, bool) {
	var params map[string]json.RawMessage
	if err := json.Unmarshal(notification.Params, &params); err != nil {
		return service.ObservedItem{}, false
	}
	actualThread, threadOK := requiredStringField(params, "threadId")
	turnID, turnOK := requiredStringField(params, "turnId")
	if !threadOK || !turnOK || actualThread != threadID {
		return service.ObservedItem{}, false
	}
	itemRaw := params["item"]
	if len(itemRaw) == 0 {
		return service.ObservedItem{}, false
	}
	return visibleItem(itemRaw, actualThread, turnID, "")
}

func visibleItem(raw json.RawMessage, threadID, turnID, fallbackClientID string) (service.ObservedItem, bool) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err != nil {
		return service.ObservedItem{}, false
	}
	typ := stringField(object, "type")
	if typ != "userMessage" && typ != "agentMessage" {
		return service.ObservedItem{}, false
	}
	if _, ok := requiredStringField(object, "id"); !ok {
		return service.ObservedItem{}, false
	}
	if typ == "userMessage" && !validUserMessageShape(object) {
		return service.ObservedItem{}, false
	}
	if typ == "agentMessage" {
		if _, ok := requiredStringField(object, "text"); !ok {
			return service.ObservedItem{}, false
		}
	}
	text := itemText(object)
	if text == "" {
		return service.ObservedItem{}, false
	}
	nativeID := stringField(object, "id")
	// The pinned 0.154.0 userMessage schema calls this field clientId. Do not
	// let additive aliases or a notification-level fallback override a present
	// native clientId, including an explicit null.
	clientID := ""
	if typ == "userMessage" {
		clientID = stringField(object, "clientId")
	}
	return service.ObservedItem{ThreadID: threadID, TurnID: turnID, NativeItemID: nativeID, NativeType: typ, ClientMessageID: clientID, Text: text}, true
}

func validUserMessageShape(object map[string]json.RawMessage) bool {
	content, ok := object["content"]
	if !ok {
		return false
	}
	if bytes.Equal(bytes.TrimSpace(content), []byte("null")) {
		return false
	}
	var parts []json.RawMessage
	if json.Unmarshal(content, &parts) != nil {
		return false
	}
	client, present := object["clientId"]
	if !present {
		return false
	}
	if present && string(client) != "null" {
		var value string
		if json.Unmarshal(client, &value) != nil {
			return false
		}
	}
	for _, part := range parts {
		var value map[string]json.RawMessage
		if json.Unmarshal(part, &value) != nil {
			return false
		}
		typ, ok := requiredStringField(value, "type")
		if !ok {
			return false
		}
		switch typ {
		case "text":
			if _, ok := requiredStringField(value, "text"); !ok {
				return false
			}
		case "image", "audio":
			if _, ok := requiredStringField(value, "url"); !ok {
				return false
			}
		case "localImage", "localAudio":
			if _, ok := requiredStringField(value, "path"); !ok {
				return false
			}
		case "skill":
			if _, ok := requiredStringField(value, "name"); !ok {
				return false
			}
			if _, ok := requiredStringField(value, "path"); !ok {
				return false
			}
		case "mention":
			if _, ok := requiredStringField(value, "name"); !ok {
				return false
			}
			if _, ok := requiredStringField(value, "path"); !ok {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func itemText(object map[string]json.RawMessage) string {
	if stringField(object, "type") == "agentMessage" {
		return stringField(object, "text")
	}
	var content []json.RawMessage
	if raw := object["content"]; len(raw) > 0 && json.Unmarshal(raw, &content) == nil {
		var out strings.Builder
		for _, part := range content {
			var value map[string]json.RawMessage
			if json.Unmarshal(part, &value) == nil {
				if text := stringField(value, "text"); text != "" {
					out.WriteString(text)
				}
			} else {
				var text string
				if json.Unmarshal(part, &text) == nil {
					out.WriteString(text)
				}
			}
		}
		return out.String()
	}
	return ""
}

func stringField(object map[string]json.RawMessage, key string) string {
	var value string
	if json.Unmarshal(object[key], &value) == nil {
		return value
	}
	return ""
}

func requiredStringField(object map[string]json.RawMessage, key string) (string, bool) {
	raw, ok := object[key]
	if !ok || string(raw) == "null" {
		return "", false
	}
	var value string
	if json.Unmarshal(raw, &value) != nil || value == "" {
		return "", false
	}
	return value, true
}

var _ service.ObservationPort = (*ObservationAdapter)(nil)
