package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/agensfield/mektup/go/appserver"
	"github.com/agensfield/mektup/go/internal/codexapi"
	"github.com/agensfield/mektup/go/internal/connection"
	"github.com/agensfield/mektup/go/internal/endpoint"
	"github.com/gorilla/websocket"
)

// Use the production websocket read limit, initialization, semantic parser,
// and pool. Oversized history must not kill the already-open delivery session.
func TestHistoryRecoversOversizedSocketPageWithoutPoisoningDelivery(t *testing.T) {
	for _, large := range []bool{false, true} {
		t.Run(fmt.Sprintf("large=%t", large), func(t *testing.T) {
			body := "small"
			if large {
				body = strings.Repeat("x", 900_000)
			}
			var mu sync.Mutex
			var connections, closes int
			var limits []int
			upgrader := websocket.Upgrader{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ws, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					return
				}
				mu.Lock()
				connections++
				mu.Unlock()
				defer func() {
					_ = ws.Close()
					mu.Lock()
					closes++
					mu.Unlock()
				}()
				for {
					var request struct {
						ID     json.RawMessage `json:"id"`
						Method string          `json:"method"`
						Params struct {
							ThreadID  string `json:"threadId"`
							Limit     int    `json:"limit"`
							Cursor    string `json:"cursor"`
							ItemsView string `json:"itemsView"`
						} `json:"params"`
					}
					if ws.ReadJSON(&request) != nil {
						return
					}
					var result any
					switch request.Method {
					case "initialize":
						result = map[string]any{"userAgent": "codex/0.154.0", "codexHome": "/tmp/codex", "platformFamily": "unix", "platformOs": "macos"}
					case "initialized":
						continue
					case "thread/turns/list":
						if request.Params.ThreadID != "thread-1" || request.Params.ItemsView != "full" {
							t.Errorf("incorrect history target/view: %+v", request.Params)
							return
						}
						mu.Lock()
						limits = append(limits, request.Params.Limit)
						mu.Unlock()
						start, _ := strconv.Atoi(request.Params.Cursor)
						end := min(start+request.Params.Limit, 20)
						turns := make([]any, 0, end-start)
						for i := start; i < end; i++ {
							item := map[string]any{"id": fmt.Sprintf("item-%d", i), "type": "userMessage", "clientId": fmt.Sprintf("client-%d", i), "content": []any{map[string]any{"type": "text", "text": body}}}
							turns = append(turns, map[string]any{"id": fmt.Sprintf("turn-%d", i), "status": "completed", "itemsView": "full", "items": []any{item}})
						}
						var next any
						if end < 20 {
							next = strconv.Itoa(end)
						}
						result = map[string]any{"data": turns, "nextCursor": next}
					case "turn/start":
						result = map[string]any{"turn": map[string]any{"id": "delivery-turn"}}
					default:
						t.Errorf("unexpected request: %s", request.Method)
						return
					}
					if ws.WriteJSON(map[string]any{"id": request.ID, "result": result}) != nil {
						return
					}
				}
			}))
			defer server.Close()
			factory := ConnectionFactory{Options: connection.Options{ClientDialer: connection.ClientDialFunc(func(ctx context.Context, _ endpoint.Route, options appserver.Options) (*appserver.Client, error) {
				return appserver.DialWebSocket(ctx, func(ctx context.Context, _, _ string) (net.Conn, error) {
					return (&net.Dialer{}).DialContext(ctx, "tcp", server.Listener.Addr().String())
				}, options)
			})}}
			pool := NewConnectionPool(factory, func(string) (endpoint.Endpoint, error) {
				return endpoint.Endpoint{ID: testTargetEndpoint, Alias: "target", Herdr: endpoint.HerdrDisabled, Route: endpoint.Route{Kind: endpoint.RouteUnix, UnixSocket: "/unused.sock"}}, nil
			})
			defer pool.Close(context.Background())
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			delivery, err := pool.session(ctx, testTarget())
			if err != nil {
				t.Fatal(err)
			}
			items, err := (&ObservationAdapter{Pool: pool}).FullHistory(ctx, testTarget())
			if err != nil || len(items) != 20 {
				t.Fatalf("history count=%d err=%v", len(items), err)
			}
			for i, item := range items {
				if item.ThreadID != "thread-1" || item.TurnID != fmt.Sprintf("turn-%d", i) || item.NativeItemID != fmt.Sprintf("item-%d", i) || item.ClientMessageID != fmt.Sprintf("client-%d", i) || item.Text != body {
					t.Fatalf("history item %d lost exact identity or content", i)
				}
			}
			if _, err := delivery.StartOrSteer(ctx, "thread-1", "healthy delivery", "client-delivery"); err != nil {
				t.Fatalf("delivery poisoned: %v", err)
			}
			mu.Lock()
			gotConnections, gotLimits := connections, append([]int(nil), limits...)
			mu.Unlock()
			wantConnections, wantLimits := 2, "[25]"
			if large {
				wantConnections, wantLimits = 3, "[25 12 12]"
			}
			if gotConnections != wantConnections || fmt.Sprint(gotLimits) != wantLimits {
				t.Fatalf("connections=%d limits=%v", gotConnections, gotLimits)
			}
			if err := pool.Close(ctx); err != nil {
				t.Fatal(err)
			}
			for ctx.Err() == nil {
				mu.Lock()
				done := closes == connections
				mu.Unlock()
				if done {
					return
				}
				time.Sleep(time.Millisecond)
			}
			t.Fatal("history connection leaked")
		})
	}
}

type sizedHistorySession struct {
	*fakeSession
	limits  *[]int
	itemErr error
}

func (s *sizedHistorySession) HistoryWithLimit(_ context.Context, _ string, limit int) ([]codexapi.Turn, error) {
	*s.limits = append(*s.limits, limit)
	return nil, fmt.Errorf("oversized full turn: %w", websocket.ErrReadLimit)
}

func (s *sizedHistorySession) ItemsHistory(context.Context, string) ([]codexapi.ItemEntry, error) {
	return s.items, s.itemErr
}

func TestSingleOversizedTurnUsesFreshFallbackOrFailsExplicitly(t *testing.T) {
	for _, supported := range []bool{false, true} {
		t.Run(fmt.Sprintf("items-supported=%t", supported), func(t *testing.T) {
			var sessions []*sizedHistorySession
			var limits []int
			unsupported := &codexapi.ServerError{Code: -32600, Message: "thread/items/list is not supported for legacy thread stores"}
			pool := NewConnectionPool(SessionFactoryFunc(func(context.Context, endpoint.Endpoint) (Session, error) {
				s := &sizedHistorySession{fakeSession: &fakeSession{endpoint: testTargetEndpoint}, limits: &limits}
				if !supported {
					s.itemErr = unsupported
				}
				s.items = []codexapi.ItemEntry{{TurnID: "exact-turn", Item: codexapi.Item{RawObject: codexapi.RawObject{Raw: json.RawMessage(`{"id":"exact-item","type":"agentMessage","text":"answer"}`)}}}}
				sessions = append(sessions, s)
				return s, nil
			}), func(string) (endpoint.Endpoint, error) { return endpoint.Endpoint{ID: testTargetEndpoint}, nil })
			items, err := (&ObservationAdapter{Pool: pool}).FullHistory(context.Background(), testTarget())
			if supported {
				if err != nil || len(items) != 1 || items[0].TurnID != "exact-turn" || items[0].NativeItemID != "exact-item" {
					t.Fatalf("fallback items=%v err=%v", items, err)
				}
			} else if len(items) != 0 || !errors.Is(err, websocket.ErrReadLimit) || !errors.Is(err, unsupported) || !strings.Contains(err.Error(), "single history entry") {
				t.Fatalf("must fail without partial history and retain both causes: items=%v err=%v", items, err)
			}
			if fmt.Sprint(limits) != "[25 12 6 3 1]" || len(sessions) != 6 {
				t.Fatalf("limits=%v sessions=%d", limits, len(sessions))
			}
			for _, session := range sessions {
				if session.detach != 1 {
					t.Fatal("history session not detached exactly once")
				}
			}
		})
	}
}
