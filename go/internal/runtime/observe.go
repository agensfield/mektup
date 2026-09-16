package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/appserver"
	"github.com/agensfield/mektup/go/internal/journal"
	"github.com/agensfield/mektup/go/internal/service"
)

// ObservationAdapter establishes an event stream before the caller starts a
// history scan. FullHistory deliberately uses full turn pages, because legacy
// local stores reject thread/items/list and summary views omit steered inputs.
type ObservationAdapter struct {
	Pool     *ConnectionPool
	Blockers BlockerJournal
}

type BlockerJournal interface {
	UpsertBlocker(context.Context, journal.BlockerObservation) error
	ResolveBlocker(context.Context, string, string, string, time.Time) error
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
	return &eventStream{session: session, target: target, blockers: a.Blockers, ctx: streamCtx, cancel: streamCancel}, nil
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
	session  Session
	target   service.ResolvedTarget
	blockers BlockerJournal
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	closed   bool

	blockerOnce sync.Once
	blockerID   string
	blockerErr  error
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
			// including unknown methods. Journal bounded correlation metadata only;
			// discard the payload and retain the shared callback for another client.
			if event.Request != nil && s.blockers != nil {
				correlationID, correlationErr := serverRequestCorrelationID(event.Request.ID)
				if correlationErr != nil {
					return service.Event{}, correlationErr
				}
				generation, generationErr := s.blockerGeneration(event.Request.Generation)
				if generationErr != nil {
					return service.Event{}, generationErr
				}
				threadID, turnID, itemID := serverRequestMetadata(event.Request.Method, event.Request.Params)
				if blockerErr := s.blockers.UpsertBlocker(nextCtx, journal.BlockerObservation{
					Method: event.Request.Method, CorrelationID: correlationID,
					Generation: generation, EndpointID: s.target.EndpointID,
					ThreadID: threadID, TurnID: turnID, ItemID: itemID,
				}); blockerErr != nil {
					return service.Event{}, blockerErr
				}
			}
			continue
		case appserver.EventNotification:
			if event.Notification == nil {
				continue
			}
			if event.Notification.Method == "serverRequest/resolved" {
				if s.blockers != nil {
					_, requestID, resolvedErr := resolvedServerRequest(event.Notification.Params)
					if resolvedErr != nil {
						return service.Event{}, resolvedErr
					}
					generation, generationErr := s.blockerGeneration(event.Notification.Generation)
					if generationErr != nil {
						return service.Event{}, generationErr
					}
					if blockerErr := s.blockers.ResolveBlocker(nextCtx, s.target.EndpointID, generation, requestID, time.Now()); blockerErr != nil {
						return service.Event{}, blockerErr
					}
				}
				continue
			}
			if event.Notification.Method != "item/completed" {
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

func (s *eventStream) blockerGeneration(transportGeneration uint64) (string, error) {
	s.blockerOnce.Do(func() {
		s.blockerID, s.blockerErr = mektup.NewUUIDv7Checked()
	})
	if s.blockerErr != nil {
		return "", fmt.Errorf("runtime: allocate blocker observer identity: %w", s.blockerErr)
	}
	return s.blockerID + "/" + strconv.FormatUint(transportGeneration, 10), nil
}

func serverRequestMetadata(method string, params json.RawMessage) (threadID, turnID, itemID string) {
	var object map[string]json.RawMessage
	switch method {
	case "item/commandExecution/requestApproval", "item/fileChange/requestApproval",
		"item/tool/requestUserInput", "item/permissions/requestApproval":
		if json.Unmarshal(params, &object) != nil {
			return "", "", ""
		}
		threadID = stringField(object, "threadId")
		turnID = stringField(object, "turnId")
		itemID = stringField(object, "itemId")
	case "item/tool/call":
		if json.Unmarshal(params, &object) != nil {
			return "", "", ""
		}
		threadID = stringField(object, "threadId")
		turnID = stringField(object, "turnId")
		itemID = stringField(object, "callId")
	case "mcpServer/elicitation/request":
		if json.Unmarshal(params, &object) != nil {
			return "", "", ""
		}
		threadID = stringField(object, "threadId")
		turnID = stringField(object, "turnId")
	case "currentTime/read":
		if json.Unmarshal(params, &object) != nil {
			return "", "", ""
		}
		threadID = stringField(object, "threadId")
	}
	return threadID, turnID, itemID
}

func serverRequestCorrelationID(id appserver.RequestID) (string, error) {
	encoded, err := json.Marshal(id)
	if err != nil || len(encoded) == 0 || string(encoded) == "null" {
		return "", fmt.Errorf("runtime: invalid server request correlation ID")
	}
	return string(encoded), nil
}

func resolvedServerRequest(params json.RawMessage) (string, string, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(params, &object); err != nil {
		return "", "", fmt.Errorf("runtime: invalid resolved server request: %w", err)
	}
	threadID, ok := requiredStringField(object, "threadId")
	if !ok {
		return "", "", fmt.Errorf("runtime: resolved server request omitted threadId")
	}
	var id any
	decoder := json.NewDecoder(bytes.NewReader(object["requestId"]))
	decoder.UseNumber()
	if err := decoder.Decode(&id); err != nil {
		return "", "", fmt.Errorf("runtime: resolved server request ID: %w", err)
	}
	correlationID, err := serverRequestCorrelationID(id)
	return threadID, correlationID, err
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
	if client, present := object["clientId"]; present && string(client) != "null" {
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
			if !validTextElements(value) {
				return false
			}
		case "image", "audio":
			if _, ok := requiredStringField(value, "url"); !ok {
				return false
			}
			if typ == "image" && !validImageDetail(value) {
				return false
			}
		case "localImage", "localAudio":
			if _, ok := requiredStringField(value, "path"); !ok {
				return false
			}
			if typ == "localImage" && !validImageDetail(value) {
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

func validImageDetail(object map[string]json.RawMessage) bool {
	raw, ok := object["detail"]
	if !ok {
		return true
	}
	if string(raw) == "null" {
		return true
	}
	value := stringField(object, "detail")
	switch value {
	case "auto", "low", "high", "original":
		return true
	default:
		return false
	}
}

func validTextElements(object map[string]json.RawMessage) bool {
	raw, ok := object["text_elements"]
	if !ok {
		return true
	}
	if string(raw) == "null" {
		return false
	}
	var elements []json.RawMessage
	if json.Unmarshal(raw, &elements) != nil {
		return false
	}
	for _, rawElement := range elements {
		var element map[string]json.RawMessage
		if json.Unmarshal(rawElement, &element) != nil {
			return false
		}
		rangeRaw, ok := element["byteRange"]
		if !ok {
			return false
		}
		var byteRange map[string]json.RawMessage
		if json.Unmarshal(rangeRaw, &byteRange) != nil || !validNonNegativeInteger(byteRange["start"]) || !validNonNegativeInteger(byteRange["end"]) {
			return false
		}
		placeholder, present := element["placeholder"]
		if present && string(placeholder) != "null" {
			var value string
			if json.Unmarshal(placeholder, &value) != nil {
				return false
			}
		}
	}
	return true
}

func validNonNegativeInteger(raw json.RawMessage) bool {
	if len(raw) == 0 || string(raw) == "null" {
		return false
	}
	var value uint64
	return json.Unmarshal(raw, &value) == nil
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
				if stringField(value, "type") == "text" {
					if text := stringField(value, "text"); text != "" {
						out.WriteString(text)
					}
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
