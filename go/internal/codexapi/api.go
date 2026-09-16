// Package codexapi is the semantic, version-pinned edge of Mektup's Codex
// app-server client.  It deliberately knows nothing about sockets, JSON-RPC
// ids, SQLite, or command-line presentation.
package codexapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const ProtocolVersion = "0.154.0"

const (
	DefaultThreadPageLimit      = 25
	MaxThreadPageLimit          = 100
	DefaultTurnsPageLimit       = 25
	MaxTurnsPageLimit           = 100
	DefaultItemsPageLimit       = 25
	MaxItemsPageLimit           = 100
	DefaultOccurrencesPageLimit = 50
	MaxOccurrencesPageLimit     = 250
	MaxCursorBytes              = 4096
	// MaxSemanticResponseBytes bounds one decoded semantic response before
	// object/page parsing or RawObject copies can amplify its memory cost.
	MaxSemanticResponseBytes = 16 << 20
	MaxReconciliationPages   = 1000
	MaxReconciliationItems   = 100000
	MaxReconciliationBytes   = 64 << 20
)

var (
	ErrExperimentalAPIRequired  = errors.New("codexapi: experimentalApi capability is required")
	ErrUnsupportedOption        = errors.New("codexapi: option is unsupported by the pinned app-server")
	ErrInProgressCutoff         = errors.New("codexapi: throughTurn cannot name an in-progress turn")
	ErrUnboundedPage            = errors.New("codexapi: response page exceeds the bounded limit")
	ErrPaginationStalled        = errors.New("codexapi: pagination cursor repeated")
	ErrPaginationExceeded       = errors.New("codexapi: reconciliation pagination bound exceeded")
	ErrCutoffUnproven           = errors.New("codexapi: exact cutoff read did not prove the requested turn")
	ErrInvalidCWD               = errors.New("codexapi: cwd must be a string or []string")
	ErrConflictingCWD           = errors.New("codexapi: CWD and Cwd aliases conflict")
	ErrResponseTooLarge         = errors.New("codexapi: semantic response exceeds byte bound")
	ErrSemanticResponseTooLarge = ErrResponseTooLarge
)

// ServerError is the exact JSON-RPC error evidence supplied by a Caller.
// Data and Raw are retained so callers can make a pinned classification
// without turning all -32603 errors into retryable errors.
type ServerError struct {
	Code    int64
	Message string
	Data    json.RawMessage
	Raw     json.RawMessage
	// WireBytes is the complete JSON-RPC response size observed before the
	// adapter split error fields into separately retained values.
	WireBytes int64
}

func (e *ServerError) Error() string {
	if e == nil {
		return ""
	}
	return fmt.Sprintf("codex server error %d: %s", e.Code, e.Message)
}

// Caller is intentionally narrower than appserver.Client.  A transport
// adapter only has to return the raw result or the untouched server error.
type Caller interface {
	Call(context.Context, string, json.RawMessage) (json.RawMessage, *ServerError, error)
}

// FuncCaller makes ordinary functions useful in tests and adapters.
type FuncCaller func(context.Context, string, json.RawMessage) (json.RawMessage, *ServerError, error)

func (f FuncCaller) Call(ctx context.Context, method string, params json.RawMessage) (json.RawMessage, *ServerError, error) {
	return f(ctx, method, params)
}

// Capabilities is a connection-level fact, never a request-level grant.
// ItemsList is optional because older local stores reject thread/items/list.
type Capabilities struct {
	ExperimentalAPI bool
	ItemsList       bool
	// Methods is an inventory hint only. It never substitutes for the
	// negotiated ExperimentalAPI bit when gating experimental methods/fields.
	Methods map[string]bool
}

// CapabilityProvider can be implemented by a transport adapter.  Options
// remain useful for adapters whose initialize result is kept elsewhere.
type CapabilityProvider interface{ Capabilities() Capabilities }

type Options struct {
	Capabilities Capabilities
	// ExperimentalAPI is a convenience for callers that already consumed
	// initialize and only need to pass the capability bit onward.
	ExperimentalAPI bool
	ItemsList       bool
	ExactRead       ExactTurnReader
}

type Client struct {
	caller       Caller
	capabilities Capabilities
	read         ExactTurnReader
}

func New(caller Caller, options Options) *Client {
	c := &Client{caller: caller, capabilities: options.Capabilities, read: options.ExactRead}
	if options.ExperimentalAPI {
		c.capabilities.ExperimentalAPI = true
	}
	if options.ItemsList {
		c.capabilities.ItemsList = true
	}
	if provider, ok := caller.(CapabilityProvider); ok {
		provided := provider.Capabilities()
		if provided.ExperimentalAPI {
			c.capabilities.ExperimentalAPI = true
		}
		if provided.ItemsList {
			c.capabilities.ItemsList = true
		}
		if c.capabilities.Methods == nil {
			c.capabilities.Methods = provided.Methods
		}
	}
	return c
}

func NewClient(caller Caller, options Options) *Client { return New(caller, options) }

func (c *Client) Capabilities() Capabilities { return c.capabilities }

// RawObject keeps every field, including additive fields from newer Codex
// servers. Known fields are decoded into adjacent typed fields by each
// response type.
type RawObject struct {
	Raw    json.RawMessage
	Fields map[string]json.RawMessage
}

type Thread struct {
	RawObject
	ID     string
	Status string
}

type Turn struct {
	RawObject
	ID        string
	Status    string
	ItemsView string
}

type Item struct {
	RawObject
	ID   string
	Type string
}

type ItemEntry struct {
	TurnID string
	Item   Item
	Raw    json.RawMessage
}

type TurnStartResponse struct {
	Turn Turn
	Raw  json.RawMessage
}
type ThreadStartResponse = LifecycleResponse
type ThreadResumeResponse = LifecycleResponse
type ThreadForkResponse = LifecycleResponse

type LifecycleResponse struct {
	Thread        Thread
	Model         string
	ModelProvider string
	CWD           string
	Raw           json.RawMessage
}

// ThreadNameResponse is the bounded result of thread/name/set. The pinned
// method does not need to hydrate or return the thread transcript.
type ThreadNameResponse struct{ Raw json.RawMessage }

type ThreadListResponse struct {
	Data []Thread
	// LoadedIDs is populated only for loaded enumeration. Loaded thread IDs
	// are intentionally not represented as hydrated Threads.
	LoadedIDs                   []string
	Loaded                      bool
	NextCursor, BackwardsCursor string
	Raw                         json.RawMessage
}
type ThreadLoadedListResponse struct {
	Data       []string
	NextCursor string
	Raw        json.RawMessage
}
type ThreadReadResponse struct {
	Thread Thread
	Raw    json.RawMessage
}
type ThreadTurnsResponse struct {
	Data                        []Turn
	NextCursor, BackwardsCursor string
	Raw                         json.RawMessage
}
type ThreadTurnsListResponse = ThreadTurnsResponse
type ThreadItemsResponse struct {
	Data                        []ItemEntry
	NextCursor, BackwardsCursor string
	Raw                         json.RawMessage
}
type ThreadItemsListResponse = ThreadItemsResponse
type UnsubscribeResponse struct {
	Status string
	Raw    json.RawMessage
}

type SearchResult struct {
	Thread  Thread
	Snippet string
	Raw     json.RawMessage
}
type SearchResponse struct {
	Data                        []SearchResult
	NextCursor, BackwardsCursor string
	Raw                         json.RawMessage
}
type ThreadSearchResponse = SearchResponse
type SearchOccurrence struct {
	TurnID            string
	ItemID            string
	Snippet           string
	SnippetMatchRange TextRange
	TurnCursor        string
	Raw               json.RawMessage
}
type TextRange struct{ Start, End uint32 }
type SearchOccurrencesResponse struct {
	Data       []SearchOccurrence
	NextCursor string
	Raw        json.RawMessage
}
type ThreadSearchOccurrencesResponse = SearchOccurrencesResponse

type TurnStartRequest struct{ ThreadID, Text, ClientUserMessageID string }

// StartOrSteerTurn is Codex's atomic start-or-steer operation.  Its payload
// is intentionally not extensible: model, cwd, permissions, and schemas are
// never accidentally inherited as JSON nulls or silently overridden.
func (c *Client) StartOrSteerTurn(ctx context.Context, request TurnStartRequest) (TurnStartResponse, error) {
	if request.ThreadID == "" || request.Text == "" || request.ClientUserMessageID == "" {
		return TurnStartResponse{}, errors.New("codexapi: threadId, text, and clientUserMessageId are required")
	}
	params := struct {
		ThreadID            string `json:"threadId"`
		ClientUserMessageID string `json:"clientUserMessageId"`
		Input               []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"input"`
	}{ThreadID: request.ThreadID, ClientUserMessageID: request.ClientUserMessageID,
		Input: []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		}{{Type: "text", Text: request.Text}}}
	var out TurnStartResponse
	if err := c.callDecode(ctx, "turn/start", params, func(raw json.RawMessage) error { var err error; out, err = decodeTurnStart(raw); return err }); err != nil {
		return TurnStartResponse{}, err
	}
	return out, nil
}

func (c *Client) Send(ctx context.Context, threadID, text, clientUserMessageID string) (TurnStartResponse, error) {
	return c.StartOrSteerTurn(ctx, TurnStartRequest{threadID, text, clientUserMessageID})
}

// StartOrSteer is the shorter semantic spelling used by the service layer.
func (c *Client) StartOrSteer(ctx context.Context, threadID, text, clientUserMessageID string) (TurnStartResponse, error) {
	return c.StartOrSteerTurn(ctx, TurnStartRequest{ThreadID: threadID, Text: text, ClientUserMessageID: clientUserMessageID})
}

// ReconcileHistory intentionally requests full turns.  Local app-servers
// backed by legacy thread stores reject thread/items/list; this helper never
// attempts a lossy local projection or silently filters one partial page.
func (c *Client) ReconcileHistory(ctx context.Context, threadID string) (ThreadTurnsResponse, error) {
	if threadID == "" {
		return ThreadTurnsResponse{}, errors.New("codexapi: threadId is required")
	}
	var all []Turn
	cursor := ""
	var lastRaw json.RawMessage
	var totalItems, totalBytes int
	for pageNo := 0; pageNo < MaxReconciliationPages; pageNo++ {
		page, err := c.ThreadTurns(ctx, TurnsOptions{ThreadID: threadID, Cursor: cursor, Limit: MaxTurnsPageLimit, ItemsView: "full"})
		if err != nil {
			return ThreadTurnsResponse{}, fmt.Errorf("codexapi: reconcile turns page %d: %w", pageNo+1, err)
		}
		if len(all)+len(page.Data) > MaxReconciliationItems {
			return ThreadTurnsResponse{}, ErrPaginationExceeded
		}
		for _, turn := range page.Data {
			items, err := jsonArray(turn.Fields["items"])
			if err != nil {
				return ThreadTurnsResponse{}, fmt.Errorf("codexapi: reconcile turn %s items: %w", turn.ID, err)
			}
			totalItems += len(items)
			if totalItems > MaxReconciliationItems {
				return ThreadTurnsResponse{}, ErrPaginationExceeded
			}
		}
		totalBytes += len(page.Raw)
		if totalBytes > MaxReconciliationBytes {
			return ThreadTurnsResponse{}, ErrPaginationExceeded
		}
		all = append(all, page.Data...)
		lastRaw = page.Raw
		if page.NextCursor == "" {
			return ThreadTurnsResponse{Data: all, Raw: lastRaw}, nil
		}
		if page.NextCursor == cursor {
			return ThreadTurnsResponse{}, ErrPaginationStalled
		}
		cursor = page.NextCursor
	}
	return ThreadTurnsResponse{}, ErrPaginationExceeded
}

// ReadFullTurns is an explicit alias for reconciliation callers.
func (c *Client) ReadFullTurns(ctx context.Context, threadID string) (ThreadTurnsResponse, error) {
	return c.ReconcileHistory(ctx, threadID)
}

type ThreadListOptions struct {
	Cursor                                   string
	Limit                                    int
	SortKey, SortDirection                   string
	ModelProviders, SourceKinds, Originators []string
	Archived                                 *bool
	SectionID                                *string
	ProjectID                                *string
	// CWD accepts either one string or []string, matching the generated
	// ThreadListCwdFilter union without making callers manufacture a wrapper.
	CWD                              any
	Cwd                              any // spelling alias for Go callers
	SearchTerm                       string
	ParentThreadID, AncestorThreadID string
	Loaded                           bool
}
type SearchOptions struct {
	SearchTerm, Cursor, SortKey, SortDirection string
	Limit                                      int
	SourceKinds                                []string
	Archived                                   *bool
}
type ThreadReadOptions struct {
	ThreadID     string
	IncludeTurns bool
}
type TurnsOptions struct {
	ThreadID, Cursor, SortDirection, ItemsView string
	Limit                                      int
}
type ItemsOptions struct {
	ThreadID, TurnID, Cursor, SortDirection string
	Limit                                   int
}
type StartOptions struct {
	Model, ModelProvider, CWD, BaseInstructions, DeveloperInstructions, ServiceName string
	Ephemeral                                                                       *bool
	SessionStartSource, ThreadSource                                                string
}
type ResumeOptions struct {
	ThreadID, Model, ModelProvider, CWD, BaseInstructions, DeveloperInstructions string
	ExcludeTurns                                                                 *bool
}
type ForkOptions struct {
	ThreadID, LastTurnID, BeforeTurnID, ThroughTurnID, Model, ModelProvider, CWD, BaseInstructions, DeveloperInstructions, ThreadSource string
	Ephemeral                                                                                                                           *bool
	ExcludeTurns                                                                                                                        *bool
	ExactRead                                                                                                                           ExactTurnReader
}

var allSourceKinds = []string{"cli", "vscode", "exec", "appServer", "subAgent", "subAgentReview", "subAgentCompact", "subAgentThreadSpawn", "subAgentOther", "unknown"}

func (c *Client) ThreadList(ctx context.Context, options ThreadListOptions) (ThreadListResponse, error) {
	if err := validateCursorInput(options.Cursor); err != nil {
		return ThreadListResponse{}, err
	}
	if options.Loaded {
		if err := validateLoadedListOptions(options); err != nil {
			return ThreadListResponse{}, err
		}
		loaded, err := c.ThreadLoadedList(ctx, options.Cursor, options.Limit)
		if err != nil {
			return ThreadListResponse{}, err
		}
		return ThreadListResponse{LoadedIDs: loaded.Data, Loaded: true, NextCursor: loaded.NextCursor, Raw: loaded.Raw}, nil
	}
	// originators are hosted-only in the pinned protocol. There is no local
	// backend capability in this package, so fail closed instead of pretending
	// to filter a page locally or forwarding a request known to be unsupported.
	if len(options.Originators) != 0 {
		return ThreadListResponse{}, fmt.Errorf("%w: originators are not supported by the local app-server", ErrUnsupportedOption)
	}
	if err := validatePage(options.Limit, MaxThreadPageLimit); err != nil {
		return ThreadListResponse{}, err
	}
	if err := validateCWD(options.CWD, options.Cwd); err != nil {
		return ThreadListResponse{}, err
	}
	if (options.ProjectID != nil || options.ParentThreadID != "" || options.AncestorThreadID != "") && !c.capabilities.ExperimentalAPI {
		return ThreadListResponse{}, ErrExperimentalAPIRequired
	}
	p := map[string]any{}
	putString(p, "cursor", options.Cursor)
	putLimit(p, options.Limit)
	sortKey, sortDirection := options.SortKey, options.SortDirection
	if sortKey == "" {
		sortKey = "updated_at"
	}
	if sortDirection == "" {
		sortDirection = "desc"
	}
	putString(p, "sortKey", sortKey)
	putString(p, "sortDirection", sortDirection)
	putStrings(p, "modelProviders", options.ModelProviders)
	sources := options.SourceKinds
	if sources == nil {
		sources = allSourceKinds
	}
	putStrings(p, "sourceKinds", sources)
	putStrings(p, "originators", options.Originators)
	putBool(p, "archived", options.Archived)
	putOptionalString(p, "sectionId", options.SectionID)
	putOptionalString(p, "projectId", options.ProjectID)
	cwd := options.CWD
	if cwd == nil {
		cwd = options.Cwd
	}
	putCWD(p, cwd)
	putString(p, "searchTerm", options.SearchTerm)
	putString(p, "parentThreadId", options.ParentThreadID)
	putString(p, "ancestorThreadId", options.AncestorThreadID)
	var out ThreadListResponse
	err := c.callDecode(ctx, "thread/list", p, func(raw json.RawMessage) error { var e error; out, e = decodeThreadList(raw, options.Limit); return e })
	return out, err
}

func validateLoadedListOptions(options ThreadListOptions) error {
	if options.Archived != nil || options.CWD != nil || options.Cwd != nil || options.SourceKinds != nil || options.SortKey != "" || options.SortDirection != "" || len(options.ModelProviders) != 0 || len(options.Originators) != 0 || options.SectionID != nil || options.ProjectID != nil || options.SearchTerm != "" || options.ParentThreadID != "" || options.AncestorThreadID != "" {
		return fmt.Errorf("%w: loaded enumeration supports only cursor and limit", ErrUnsupportedOption)
	}
	return nil
}

func (c *Client) ThreadLoadedList(ctx context.Context, cursor string, limit int) (ThreadLoadedListResponse, error) {
	if err := validateCursorInput(cursor); err != nil {
		return ThreadLoadedListResponse{}, err
	}
	if err := validatePage(limit, MaxThreadPageLimit); err != nil {
		return ThreadLoadedListResponse{}, err
	}
	p := map[string]any{"limit": DefaultThreadPageLimit}
	if limit > 0 {
		p["limit"] = limit
	}
	putString(p, "cursor", cursor)
	var out ThreadLoadedListResponse
	err := c.callDecode(ctx, "thread/loaded/list", p, func(raw json.RawMessage) error {
		var e error
		out, e = decodeLoadedList(raw, limit)
		return e
	})
	return out, err
}

func (c *Client) LoadedThreads(ctx context.Context, cursor string, limit int) (ThreadLoadedListResponse, error) {
	return c.ThreadLoadedList(ctx, cursor, limit)
}

func (c *Client) ThreadRead(ctx context.Context, options ThreadReadOptions) (ThreadReadResponse, error) {
	if options.ThreadID == "" {
		return ThreadReadResponse{}, errors.New("codexapi: threadId is required")
	}
	p := map[string]any{"threadId": options.ThreadID}
	if options.IncludeTurns {
		p["includeTurns"] = true
	}
	var out ThreadReadResponse
	err := c.callDecode(ctx, "thread/read", p, func(raw json.RawMessage) error {
		var e error
		out, e = decodeThreadRead(raw, options.IncludeTurns)
		return e
	})
	return out, err
}

func (c *Client) ThreadTurns(ctx context.Context, options TurnsOptions) (ThreadTurnsResponse, error) {
	if options.ThreadID == "" {
		return ThreadTurnsResponse{}, errors.New("codexapi: threadId is required")
	}
	if err := validateCursorInput(options.Cursor); err != nil {
		return ThreadTurnsResponse{}, err
	}
	if options.ItemsView != "summary" && options.ItemsView != "full" {
		return ThreadTurnsResponse{}, errors.New("codexapi: itemsView must be explicitly summary or full")
	}
	if err := validatePage(options.Limit, MaxTurnsPageLimit); err != nil {
		return ThreadTurnsResponse{}, err
	}
	p := map[string]any{"threadId": options.ThreadID, "itemsView": options.ItemsView}
	putString(p, "cursor", options.Cursor)
	putLimit(p, options.Limit)
	putString(p, "sortDirection", options.SortDirection)
	var out ThreadTurnsResponse
	err := c.callDecode(ctx, "thread/turns/list", p, func(raw json.RawMessage) error {
		var e error
		out, e = decodeTurns(raw, options.Limit, options.ItemsView)
		return e
	})
	return out, err
}

func (c *Client) ThreadItems(ctx context.Context, options ItemsOptions) (ThreadItemsResponse, error) {
	if options.ThreadID == "" {
		return ThreadItemsResponse{}, errors.New("codexapi: threadId is required")
	}
	if err := validateCursorInput(options.Cursor); err != nil {
		return ThreadItemsResponse{}, err
	}
	if err := validatePage(options.Limit, MaxItemsPageLimit); err != nil {
		return ThreadItemsResponse{}, err
	}
	p := map[string]any{"threadId": options.ThreadID}
	putString(p, "turnId", options.TurnID)
	putString(p, "cursor", options.Cursor)
	putLimit(p, options.Limit)
	putString(p, "sortDirection", options.SortDirection)
	var out ThreadItemsResponse
	err := c.callDecode(ctx, "thread/items/list", p, func(raw json.RawMessage) error { var e error; out, e = decodeItems(raw, options.Limit); return e })
	return out, err
}

func (c *Client) ThreadStart(ctx context.Context, options StartOptions) (ThreadStartResponse, error) {
	p := map[string]any{}
	putString(p, "model", options.Model)
	putString(p, "modelProvider", options.ModelProvider)
	putString(p, "cwd", options.CWD)
	putString(p, "baseInstructions", options.BaseInstructions)
	putString(p, "developerInstructions", options.DeveloperInstructions)
	putString(p, "serviceName", options.ServiceName)
	putBool(p, "ephemeral", options.Ephemeral)
	putString(p, "sessionStartSource", options.SessionStartSource)
	putString(p, "threadSource", options.ThreadSource)
	var out ThreadStartResponse
	err := c.callExperimentalMaybe(ctx, "thread/start", p, false, func(raw json.RawMessage) error { var e error; out, e = decodeLifecycle(raw); return e })
	return out, err
}

func (c *Client) ThreadResume(ctx context.Context, options ResumeOptions) (ThreadResumeResponse, error) {
	if options.ThreadID == "" {
		return ThreadResumeResponse{}, errors.New("codexapi: threadId is required")
	}
	p := map[string]any{"threadId": options.ThreadID}
	putString(p, "model", options.Model)
	putString(p, "modelProvider", options.ModelProvider)
	putString(p, "cwd", options.CWD)
	putString(p, "baseInstructions", options.BaseInstructions)
	putString(p, "developerInstructions", options.DeveloperInstructions)
	putBool(p, "excludeTurns", options.ExcludeTurns)
	var out ThreadResumeResponse
	err := c.callExperimentalMaybe(ctx, "thread/resume", p, false, func(raw json.RawMessage) error { var e error; out, e = decodeLifecycle(raw); return e })
	return out, err
}

func (c *Client) ThreadFork(ctx context.Context, options ForkOptions) (ThreadForkResponse, error) {
	if options.ThreadID == "" {
		return ThreadForkResponse{}, errors.New("codexapi: threadId is required")
	}
	if options.ThroughTurnID != "" && (options.LastTurnID != "" || options.BeforeTurnID != "") {
		return ThreadForkResponse{}, errors.New("codexapi: throughTurn, lastTurnId, and beforeTurnId are mutually exclusive")
	}
	if options.LastTurnID != "" && options.BeforeTurnID != "" {
		return ThreadForkResponse{}, errors.New("codexapi: lastTurnId and beforeTurnId are mutually exclusive")
	}
	reader := options.ExactRead
	if reader == nil {
		reader = c.read
	}
	if options.ThroughTurnID != "" {
		if err := c.checkThroughTurn(ctx, options.ThreadID, options.ThroughTurnID, reader); err != nil {
			return ThreadForkResponse{}, err
		}
	}
	lastTurnID := options.LastTurnID
	if options.ThroughTurnID != "" {
		lastTurnID = options.ThroughTurnID
	}
	p := map[string]any{"threadId": options.ThreadID}
	putString(p, "lastTurnId", lastTurnID)
	putString(p, "beforeTurnId", options.BeforeTurnID)
	putString(p, "model", options.Model)
	putString(p, "modelProvider", options.ModelProvider)
	putString(p, "cwd", options.CWD)
	putString(p, "baseInstructions", options.BaseInstructions)
	putString(p, "developerInstructions", options.DeveloperInstructions)
	putString(p, "threadSource", options.ThreadSource)
	putBool(p, "ephemeral", options.Ephemeral)
	putBool(p, "excludeTurns", options.ExcludeTurns)
	var out ThreadForkResponse
	err := c.callExperimentalMaybe(ctx, "thread/fork", p, options.BeforeTurnID != "", func(raw json.RawMessage) error { var e error; out, e = decodeLifecycle(raw); return e })
	return out, err
}

// ThreadSetName updates a thread's display name as a separate mutation. Name
// is intentionally not part of thread/start or thread/fork options.
func (c *Client) ThreadSetName(ctx context.Context, threadID, name string) (ThreadNameResponse, error) {
	if strings.TrimSpace(threadID) == "" || strings.TrimSpace(name) == "" {
		return ThreadNameResponse{}, errors.New("codexapi: threadId and name are required")
	}
	var out ThreadNameResponse
	err := c.callDecode(ctx, "thread/name/set", map[string]any{"threadId": threadID, "name": name}, func(raw json.RawMessage) error {
		var object map[string]json.RawMessage
		if err := json.Unmarshal(raw, &object); err != nil || object == nil {
			return errors.New("codexapi: thread/name/set response must be a JSON object")
		}
		out.Raw = append(json.RawMessage(nil), raw...)
		return nil
	})
	return out, err
}

func (c *Client) ThreadUnsubscribe(ctx context.Context, threadID string) (UnsubscribeResponse, error) {
	if threadID == "" {
		return UnsubscribeResponse{}, errors.New("codexapi: threadId is required")
	}
	var out UnsubscribeResponse
	err := c.callDecode(ctx, "thread/unsubscribe", map[string]any{"threadId": threadID}, func(raw json.RawMessage) error { var e error; out, e = decodeUnsubscribe(raw); return e })
	return out, err
}

func (c *Client) ListThreads(ctx context.Context, options ThreadListOptions) (ThreadListResponse, error) {
	return c.ThreadList(ctx, options)
}
func (c *Client) ReadThread(ctx context.Context, options ThreadReadOptions) (ThreadReadResponse, error) {
	return c.ThreadRead(ctx, options)
}
func (c *Client) ListTurns(ctx context.Context, options TurnsOptions) (ThreadTurnsResponse, error) {
	return c.ThreadTurns(ctx, options)
}
func (c *Client) ListItems(ctx context.Context, options ItemsOptions) (ThreadItemsResponse, error) {
	return c.ThreadItems(ctx, options)
}
func (c *Client) StartThread(ctx context.Context, options StartOptions) (ThreadStartResponse, error) {
	return c.ThreadStart(ctx, options)
}
func (c *Client) ResumeThread(ctx context.Context, options ResumeOptions) (ThreadResumeResponse, error) {
	return c.ThreadResume(ctx, options)
}
func (c *Client) ForkThread(ctx context.Context, options ForkOptions) (ThreadForkResponse, error) {
	return c.ThreadFork(ctx, options)
}
func (c *Client) UnsubscribeThread(ctx context.Context, threadID string) (UnsubscribeResponse, error) {
	return c.ThreadUnsubscribe(ctx, threadID)
}
func (c *Client) SearchThreads(ctx context.Context, options SearchOptions) (SearchResponse, error) {
	return c.Search(ctx, options)
}

func (c *Client) Search(ctx context.Context, options SearchOptions) (SearchResponse, error) {
	if strings.TrimSpace(options.SearchTerm) == "" {
		return SearchResponse{}, errors.New("codexapi: searchTerm is required")
	}
	if err := validateCursorInput(options.Cursor); err != nil {
		return SearchResponse{}, err
	}
	if err := validatePage(options.Limit, MaxThreadPageLimit); err != nil {
		return SearchResponse{}, err
	}
	p := map[string]any{"searchTerm": options.SearchTerm}
	putString(p, "cursor", options.Cursor)
	putLimit(p, options.Limit)
	putString(p, "sortKey", options.SortKey)
	putString(p, "sortDirection", options.SortDirection)
	sources := options.SourceKinds
	if sources == nil {
		sources = allSourceKinds
	}
	putStrings(p, "sourceKinds", sources)
	putBool(p, "archived", options.Archived)
	var out SearchResponse
	err := c.callExperimentalMaybe(ctx, "thread/search", p, true, func(raw json.RawMessage) error { var e error; out, e = decodeSearch(raw, options.Limit); return e })
	return out, err
}

type SearchOccurrencesOptions struct {
	ThreadID, SearchTerm, Cursor string
	Limit                        int
}

func (c *Client) SearchOccurrences(ctx context.Context, options SearchOccurrencesOptions) (SearchOccurrencesResponse, error) {
	if options.ThreadID == "" || strings.TrimSpace(options.SearchTerm) == "" {
		return SearchOccurrencesResponse{}, errors.New("codexapi: threadId and searchTerm are required")
	}
	if err := validateCursorInput(options.Cursor); err != nil {
		return SearchOccurrencesResponse{}, err
	}
	if err := validatePage(options.Limit, MaxOccurrencesPageLimit); err != nil {
		return SearchOccurrencesResponse{}, err
	}
	p := map[string]any{"threadId": options.ThreadID, "searchTerm": options.SearchTerm}
	putString(p, "cursor", options.Cursor)
	putLimit(p, options.Limit)
	var out SearchOccurrencesResponse
	err := c.callExperimentalMaybe(ctx, "thread/searchOccurrences", p, true, func(raw json.RawMessage) error { var e error; out, e = decodeOccurrences(raw, options.Limit); return e })
	return out, err
}

// ExactTurnReader is injected by reconciliation code to prove a cutoff is
// complete before a fork/read request is emitted.
type ExactTurnReader interface {
	ReadTurn(context.Context, string, string) (Turn, error)
}

func (c *Client) checkThroughTurn(ctx context.Context, threadID, turnID string, reader ExactTurnReader) error {
	if reader == nil {
		return fmt.Errorf("%w: throughTurn requires an exact-read seam", ErrUnsupportedOption)
	}
	turn, err := reader.ReadTurn(ctx, threadID, turnID)
	if err != nil {
		return err
	}
	if turn.ID != turnID {
		return ErrCutoffUnproven
	}
	if turn.Status == "" {
		return ErrCutoffUnproven
	}
	if strings.EqualFold(turn.Status, "inProgress") || strings.EqualFold(turn.Status, "in-progress") {
		return ErrInProgressCutoff
	}
	return nil
}

func (c *Client) callExperimentalMaybe(ctx context.Context, method string, params any, experimental bool, decode func(json.RawMessage) error) error {
	if experimental && !c.capabilities.ExperimentalAPI {
		return ErrExperimentalAPIRequired
	}
	return c.callDecode(ctx, method, params, decode)
}
func (c *Client) callDecode(ctx context.Context, method string, params any, decode func(json.RawMessage) error) error {
	if c == nil || c.caller == nil {
		return errors.New("codexapi: nil caller")
	}
	body, err := json.Marshal(params)
	if err != nil {
		return fmt.Errorf("codexapi: encode %s: %w", method, err)
	}
	raw, serverErr, err := c.caller.Call(ctx, method, body)
	if err != nil {
		return err
	}
	if serverErr != nil {
		responseBytes := serverErr.WireBytes
		if responseBytes == 0 {
			responseBytes = int64(len(serverErr.Raw))
		}
		if responseBytes > MaxSemanticResponseBytes {
			return fmt.Errorf("%w: %d bytes exceeds %d", ErrResponseTooLarge, responseBytes, MaxSemanticResponseBytes)
		}
		return serverErr
	}
	if len(raw) > MaxSemanticResponseBytes {
		return fmt.Errorf("%w: %d bytes exceeds %d", ErrResponseTooLarge, len(raw), MaxSemanticResponseBytes)
	}
	if err := decode(raw); err != nil {
		return fmt.Errorf("codexapi: decode %s: %w", method, err)
	}
	return nil
}

func putString(m map[string]any, k, v string) {
	if v != "" {
		m[k] = v
	}
}
func putStrings(m map[string]any, k string, v []string) {
	if v != nil {
		m[k] = v
	}
}
func putLimit(m map[string]any, v int) {
	if v > 0 {
		m["limit"] = v
	}
}
func putBool(m map[string]any, k string, v *bool) {
	if v != nil {
		m[k] = *v
	}
}
func putOptionalString(m map[string]any, k string, v *string) {
	if v != nil {
		m[k] = *v
	}
}
func putCWD(m map[string]any, value any) {
	switch cwd := value.(type) {
	case string:
		putString(m, "cwd", cwd)
	case []string:
		if len(cwd) == 1 {
			putString(m, "cwd", cwd[0])
		} else if len(cwd) > 1 {
			m["cwd"] = cwd
		}
	case nil:
		return
	default:
		// Public entry points call validateCWD before this helper. Keep this
		// defensive branch inert so an internal future caller cannot panic.
		return
	}
}
func validateCWD(primary, alias any) error {
	if primary != nil && alias != nil {
		return ErrConflictingCWD
	}
	value := primary
	if value == nil {
		value = alias
	}
	switch cwd := value.(type) {
	case nil, string:
		return nil
	case []string:
		_ = cwd
		return nil
	default:
		return ErrInvalidCWD
	}
}
func validateCursorInput(value string) error {
	if len(value) > MaxCursorBytes {
		return fmt.Errorf("codexapi: cursor exceeds %d bytes", MaxCursorBytes)
	}
	return nil
}
func validatePage(limit, max int) error {
	if limit < 0 || limit > max {
		return fmt.Errorf("%w: limit must be between 1 and %d when provided", ErrUnboundedPage, max)
	}
	return nil
}
