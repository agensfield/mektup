// Package rpcmeta contains the offline, version-pinned safety metadata for
// Codex app-server client requests. It deliberately has no transport or CLI
// dependency: callers classify and gate a request before handing it to the
// app-server client.
package rpcmeta

import (
	"bytes"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

const ProtocolVersion = "0.154.0"

type EffectClass string

const (
	EffectRead        EffectClass = "read"
	EffectNetworkRead EffectClass = "network-read"
	EffectThreadWrite EffectClass = "thread-write"
	EffectHostWrite   EffectClass = "host-write"
	EffectAuth        EffectClass = "auth"
	EffectDestructive EffectClass = "destructive"
	EffectUnknown     EffectClass = "unknown"
)

// Short names make the metadata convenient for semantic callers while the
// Effect-prefixed names remain unambiguous in larger packages.
const (
	Read        = EffectRead
	NetworkRead = EffectNetworkRead
	ThreadWrite = EffectThreadWrite
	HostWrite   = EffectHostWrite
	Auth        = EffectAuth
	Destructive = EffectDestructive
	Unknown     = EffectUnknown
)

var effectOrder = []EffectClass{
	EffectRead, EffectNetworkRead, EffectThreadWrite, EffectHostWrite,
	EffectAuth, EffectDestructive, EffectUnknown,
}

type Stability string

const (
	StabilityStable       Stability = "stable"
	StabilityExperimental Stability = "experimental"
	StabilityUnknown      Stability = "unknown"
)

type RetrySafety string

const (
	RetrySafe                RetrySafety = "safe"
	RetryAfterReconciliation RetrySafety = "safe-after-reconciliation"
	RetryIdempotencyKeyed    RetrySafety = "idempotency-keyed"
	RetryUnsafe              RetrySafety = "unsafe"
	RetryUnknown             RetrySafety = "unknown"
)

// FieldTrigger names a field which needs experimentalApi even when its
// enclosing method is stable-exported. Paths use the source's wire spelling
// (for example thread/fork.beforeTurnId). Aliases accommodate callers that
// have already converted JSON names to snake/kebab command names.
type FieldTrigger struct {
	Path         string        `json:"path"`
	Aliases      []string      `json:"aliases,omitempty"`
	Experimental bool          `json:"experimental,omitempty"`
	AddEffects   []EffectClass `json:"add_effects,omitempty"`
	Note         string        `json:"note,omitempty"`
}

type Metadata struct {
	Method        string         `json:"method"`
	Stability     Stability      `json:"stability"`
	Effects       []EffectClass  `json:"effects"`
	RetrySafety   RetrySafety    `json:"retry_safety"`
	Experimental  bool           `json:"experimental"`
	FieldTriggers []FieldTrigger `json:"field_triggers,omitempty"`
	ResponseNote  string         `json:"response_note"`
	TerminalNote  string         `json:"terminal_note"`
	ServerCaveats []string       `json:"server_caveats,omitempty"`
}

type Decision struct {
	Metadata           Metadata      `json:"metadata"`
	Effects            []EffectClass `json:"effects"`
	Experimental       bool          `json:"experimental"`
	ExperimentalFields []string      `json:"experimental_fields,omitempty"`
}

// GateRequest is intentionally a value containing only the invocation. A
// grant is never retained in Registry or package state, so acknowledgments
// cannot become a permanent configuration permission.
type GateRequest struct {
	Method string
	Params json.RawMessage
	Grants []EffectClass
}

type GateError struct {
	Method       string        `json:"method"`
	Missing      []EffectClass `json:"missing"`
	Effects      []EffectClass `json:"effects"`
	Experimental bool          `json:"experimental"`
}

func (e *GateError) Error() string {
	if e == nil {
		return ""
	}
	missing := make([]string, len(e.Missing))
	for i, effect := range e.Missing {
		missing[i] = string(effect)
	}
	return fmt.Sprintf("raw RPC %q requires per-invocation effect acknowledgment: %s", e.Method, strings.Join(missing, ", "))
}

func (e *GateError) IsUnknownMethod() bool {
	return e != nil && containsEffect(e.Effects, EffectUnknown)
}

// Lookup returns a copy so a caller cannot mutate the embedded registry.
func Lookup(method string) (Metadata, bool) {
	metadata, ok := registry[method]
	if !ok {
		return Metadata{}, false
	}
	return cloneMetadata(metadata), true
}

// Evaluate classifies one request. Invalid or absent params are treated as
// having no selected fields; the app-server remains responsible for validating
// parameter shape. Classification itself must never fail open.
func Evaluate(method string, params json.RawMessage) Decision {
	metadata, ok := registry[method]
	if !ok {
		metadata = Metadata{
			Method:        method,
			Stability:     StabilityUnknown,
			Effects:       []EffectClass{EffectUnknown},
			RetrySafety:   RetryUnknown,
			ResponseNote:  "The method is not in the pinned 0.154.0 client inventory.",
			TerminalNote:  "No terminal/completion contract is known; preserve uncertainty after a possible write.",
			ServerCaveats: []string{"Unknown methods may be unsupported or may have effects not represented here."},
		}
	}
	decision := Decision{Metadata: cloneMetadata(metadata), Effects: append([]EffectClass(nil), metadata.Effects...), Experimental: metadata.Experimental}
	if !ok {
		return decision
	}
	for _, trigger := range metadata.FieldTriggers {
		if fieldPresent(params, trigger) {
			if trigger.Experimental {
				decision.Experimental = true
				decision.ExperimentalFields = append(decision.ExperimentalFields, trigger.Path)
			}
			decision.Effects = mergeEffects(decision.Effects, trigger.AddEffects...)
		}
	}
	decision.Metadata.Experimental = decision.Experimental
	decision.Metadata.Effects = append([]EffectClass(nil), decision.Effects...)
	return decision
}

// Classify is the semantic-layer spelling of Evaluate.
func Classify(method string, params json.RawMessage) Decision { return Evaluate(method, params) }

// Gate performs all safety checks synchronously, before a dispatch callback
// can be invoked by a semantic caller. Plain and network-backed observations
// dispatch without a mutation acknowledgment; every mutation, auth,
// destructive, host-write, or unknown class is acknowledged independently.
// Duplicate grants are harmless.
func Gate(request GateRequest) error {
	decision := Evaluate(request.Method, request.Params)
	grants := make(map[EffectClass]bool, len(request.Grants))
	for _, grant := range request.Grants {
		grants[grant] = true
	}
	missing := make([]EffectClass, 0, len(decision.Effects))
	for _, effect := range decision.Effects {
		// Plain observations and network-backed observations are both read
		// effects. Network-read remains in the decision for receipts, but does
		// not require a mutation acknowledgment.
		if effect == EffectRead || effect == EffectNetworkRead || grants[effect] {
			continue
		}
		if !containsEffect(missing, effect) {
			missing = append(missing, effect)
		}
	}
	if len(missing) == 0 {
		return nil
	}
	return &GateError{Method: request.Method, Missing: missing, Effects: decision.Effects, Experimental: decision.Experimental}
}

// Check is the semantic-layer spelling of Gate.
func Check(request GateRequest) error { return Gate(request) }

// Registry returns every unique method in deterministic lexical order. The
// slice and all nested slices are copies.
func Registry() []Metadata {
	methods := make([]Metadata, 0, len(registry))
	for _, metadata := range registry {
		methods = append(methods, cloneMetadata(metadata))
	}
	sort.Slice(methods, func(i, j int) bool { return methods[i].Method < methods[j].Method })
	return methods
}

// Methods is an alias for Registry for docs and command metadata consumers.
func Methods() []Metadata { return Registry() }

func RegistryJSON() ([]byte, error) { return json.Marshal(Registry()) }

func cloneMetadata(metadata Metadata) Metadata {
	metadata.Effects = append([]EffectClass(nil), metadata.Effects...)
	metadata.FieldTriggers = append([]FieldTrigger(nil), metadata.FieldTriggers...)
	for i := range metadata.FieldTriggers {
		metadata.FieldTriggers[i].Aliases = append([]string(nil), metadata.FieldTriggers[i].Aliases...)
		metadata.FieldTriggers[i].AddEffects = append([]EffectClass(nil), metadata.FieldTriggers[i].AddEffects...)
	}
	metadata.ServerCaveats = append([]string(nil), metadata.ServerCaveats...)
	return metadata
}

func mergeEffects(base []EffectClass, additions ...EffectClass) []EffectClass {
	for _, effect := range additions {
		if !containsEffect(base, effect) {
			base = append(base, effect)
		}
	}
	sort.SliceStable(base, func(i, j int) bool { return effectIndex(base[i]) < effectIndex(base[j]) })
	return base
}

func effectIndex(effect EffectClass) int {
	for i, known := range effectOrder {
		if effect == known {
			return i
		}
	}
	return len(effectOrder)
}

func containsEffect(effects []EffectClass, wanted EffectClass) bool {
	for _, effect := range effects {
		if effect == wanted {
			return true
		}
	}
	return false
}

func fieldPresent(params json.RawMessage, trigger FieldTrigger) bool {
	if len(bytes.TrimSpace(params)) == 0 || bytes.Equal(bytes.TrimSpace(params), []byte("null")) {
		return false
	}
	var value any
	if json.Unmarshal(params, &value) != nil {
		return false
	}
	for _, path := range append([]string{trigger.Path}, trigger.Aliases...) {
		if strings.Contains(path, "=") && variantPresent(value, path) {
			return true
		}
		if pathPresent(value, path) {
			return true
		}
	}
	return false
}

func variantPresent(value any, selector string) bool {
	parts := strings.SplitN(selector, "=", 2)
	if len(parts) != 2 {
		return false
	}
	object, ok := value.(map[string]any)
	if !ok {
		return false
	}
	for key, child := range object {
		if normalizeField(key) != normalizeField(parts[0]) {
			continue
		}
		stringValue, ok := child.(string)
		return ok && normalizeField(stringValue) == normalizeField(parts[1])
	}
	return false
}

func pathPresent(value any, path string) bool {
	// Metadata paths are method.field. Only the field portion is used here;
	// method is already selected by the metadata lookup.
	if dot := strings.IndexByte(path, '.'); dot >= 0 {
		path = path[dot+1:]
	}
	path = strings.TrimSpace(path)
	if path == "" {
		return false
	}
	parts := strings.FieldsFunc(path, func(r rune) bool { return r == '.' || r == '/' })
	return walkPath(value, parts)
}

func walkPath(value any, parts []string) bool {
	if len(parts) == 0 {
		return true
	}
	object, ok := value.(map[string]any)
	if !ok {
		return false
	}
	for key, child := range object {
		if normalizeField(key) != normalizeField(parts[0]) {
			continue
		}
		if len(parts) == 1 {
			return true
		}
		return walkPath(child, parts[1:])
	}
	return false
}

func normalizeField(value string) string {
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "--")
	value = strings.ReplaceAll(value, "-", "")
	value = strings.ReplaceAll(value, "_", "")
	return strings.ToLower(value)
}

var registry = buildRegistry()

func buildRegistry() map[string]Metadata {
	result := make(map[string]Metadata, len(stableMethods)+len(experimentalOnlyMethods))
	for _, method := range stableMethods {
		result[method] = baseMetadata(method, StabilityStable, false)
	}
	for _, method := range experimentalOnlyMethods {
		result[method] = baseMetadata(method, StabilityExperimental, true)
	}

	setEffects(result, []EffectClass{EffectNetworkRead},
		"model/list", "account/rateLimits/read", "account/usage/read", "account/workspaceMessages/read",
		"account/read", "account/rateLimitResetCredit/consume", "account/sendAddCreditsNudgeEmail", "gitDiffToRemote",
		"mcpServer/resource/read", "mcpServerStatus/list", "plugin/list", "plugin/installed", "plugin/read",
		"plugin/share/list", "plugin/share/checkout", "plugin/skill/read", "plugin/reconcile", "plugin/share/save", "plugin/share/updateTargets",
		"plugin/share/delete", "plugin/install", "plugin/uninstall", "marketplace/add", "marketplace/upgrade",
		"app/list", "app/read", "app/installed", "feedback/upload",
		"thread/realtime/start", "thread/realtime/appendAudio", "thread/realtime/appendText",
		"thread/realtime/appendSpeech", "mcpServer/tool/call", "mcpServer/oauth/login",
		"environment/info", "environment/status", "remoteControl/pairing/start", "remoteControl/pairing/status",
		"remoteControl/client/list", "remoteControl/client/revoke", "plugin/search",
	)

	setEffects(result, []EffectClass{EffectThreadWrite},
		"thread/start", "thread/resume", "thread/fork", "thread/archive", "thread/unsubscribe", "thread/name/set",
		"thread/goal/set", "thread/goal/clear", "thread/metadata/update", "thread/section/move", "thread/unarchive",
		"thread/compact/start", "thread/rollback", "thread/revert", "threadSection/create", "threadSection/update",
		"threadSection/delete", "thread/inject_items", "turn/start", "turn/steer", "turn/interrupt", "review/start",
		"thread/increment_elicitation", "thread/decrement_elicitation", "thread/queue/add", "thread/queue/update",
		"thread/queue/delete", "thread/queue/reorder", "thread/queue/start", "thread/settings/update",
		"thread/memoryMode/set", "thread/realtime/start", "thread/realtime/appendAudio", "thread/realtime/appendText",
		"thread/realtime/appendSpeech", "thread/realtime/stop", "thread/approveGuardianDeniedAction",
		"thread/backgroundTerminals/clean", "thread/backgroundTerminals/terminate",
	)

	setEffects(result, []EffectClass{EffectHostWrite},
		"thread/shellCommand", "skills/extraRoots/set", "skills/config/write", "marketplace/add", "marketplace/remove",
		"marketplace/upgrade", "plugin/reconcile", "plugin/share/save", "plugin/share/updateTargets", "plugin/share/checkout", "plugin/share/delete",
		"plugin/install", "plugin/uninstall", "fs/writeFile", "fs/createDirectory", "fs/copy", "fs/watch", "fs/unwatch",
		"mcpServer/oauth/login", "config/mcpServer/reload", "windowsSandbox/setupStart", "feedback/upload", "command/exec",
		"command/exec/write", "command/exec/terminate", "command/exec/resize", "config/value/write", "config/batchWrite",
		"externalAgentConfig/import", "externalAgentConfig/import/recordHistory", "environment/add",
		"mcpServer/event/stream/start", "mcpServer/event/stream/stop", "process/spawn", "process/writeStdin",
		"process/kill", "process/resizePty", "fuzzyFileSearch/sessionStart", "fuzzyFileSearch/sessionUpdate", "fuzzyFileSearch/sessionStop",
		"project/create", "project/import", "project/update", "project/move", "project/delete", "externalAgentConfig/detect",
		"experimentalFeature/enablement/set",
	)

	setEffects(result, []EffectClass{EffectAuth},
		"account/login/start", "account/login/cancel", "account/logout", "userVerification/enroll", "userVerification/delete",
		"userVerification/verify", "account/bedrock/setup", "mcpServer/oauth/login",
	)
	setEffects(result, []EffectClass{EffectHostWrite}, "remoteControl/enable", "remoteControl/disable", "remoteControl/pairing/start", "remoteControl/client/revoke", "mcpServer/oauth/login")

	setEffects(result, []EffectClass{EffectDestructive},
		"thread/delete", "thread/rollback", "thread/revert", "marketplace/remove", "plugin/share/delete", "plugin/uninstall",
		"fs/remove", "account/logout", "memory/reset", "project/delete", "thread/queue/delete", "thread/backgroundTerminals/clean",
		"thread/backgroundTerminals/terminate", "process/kill", "remoteControl/client/revoke", "userVerification/delete",
	)
	setEffects(result, []EffectClass{EffectHostWrite, EffectDestructive}, "fs/remove", "memory/reset")
	setEffects(result, []EffectClass{EffectUnknown}, "mock/experimentalMethod", "account/rateLimitResetCredit/consume", "account/sendAddCreditsNudgeEmail", "mcpServer/tool/call")

	// Experimental fields are kept explicit so a stable-exported method cannot
	// accidentally send a v2 field without requesting experimentalApi.
	triggers := map[string][]FieldTrigger{
		"thread/start":  triggersFor("thread/start", "allowProviderModelFallback", "runtimeWorkspaceRoots", "approvalPolicy", "permissions", "multiAgentMode", "historyMode", "projectId", "environments", "dynamicTools", "selectedCapabilityRoots", "mockExperimentalField", "experimentalRawEvents"),
		"thread/resume": triggersFor("thread/resume", "history", "path", "runtimeWorkspaceRoots", "approvalPolicy", "permissions", "initialTurnsPage"),
		"thread/fork": {
			{Path: "thread/fork.beforeTurnId", Aliases: []string{"before-turn", "before_turn_id"}, Experimental: true, Note: "The experimental cutoff excludes the referenced turn and all later turns; it cannot be combined with lastTurnId and an in-progress cutoff is rejected."},
			{Path: "thread/fork.path", Experimental: true, Note: "Path-based fork uses a different backend/history loading path."},
			{Path: "thread/fork.runtimeWorkspaceRoots", Experimental: true}, {Path: "thread/fork.approvalPolicy", Experimental: true}, {Path: "thread/fork.permissions", Experimental: true}, {Path: "thread/fork.deferGoalContinuation", Experimental: true},
		},
		"thread/settings/update": triggersFor("thread/settings/update", "approvalPolicy", "permissions", "collaborationMode", "multiAgentMode"),
		"thread/metadata/update": triggersFor("thread/metadata/update", "projectId", "daybreakEnabled"),
		"thread/list":            triggersFor("thread/list", "projectId", "parentThreadId", "ancestorThreadId"),
		"turn/start":             triggersFor("turn/start", "responsesapiClientMetadata", "additionalContext", "environments", "runtimeWorkspaceRoots", "approvalPolicy", "permissions", "collaborationMode", "multiAgentMode", "cyberAccessProgram"),
		"turn/steer":             triggersFor("turn/steer", "responsesapiClientMetadata", "additionalContext"),
		"command/exec":           triggersFor("command/exec", "permissionProfile"),
		"account/login/start": {
			{Path: "account/login/start.chatgptAuthTokens", Aliases: []string{"type=chatgptAuthTokens"}, Experimental: true},
			{Path: "account/login/start.amazonBedrock", Aliases: []string{"type=amazonBedrock"}, Experimental: true},
			{Path: "account/login/start.amazonBedrockAccessKeys", Aliases: []string{"type=amazonBedrockAccessKeys"}, Experimental: true},
		},
	}
	for method, fields := range triggers {
		if metadata, ok := result[method]; ok {
			metadata.FieldTriggers = fields
			result[method] = metadata
		}
	}

	// Parameter-aware risk raises for operations documented as cache-refreshing
	// or auth-refreshing. The base class remains the method worst case.
	addTriggerEffect(result, "plugin/list", "forceRefetch", EffectNetworkRead)
	addTriggerEffect(result, "app/list", "forceRefetch", EffectNetworkRead)
	addTriggerEffect(result, "app/installed", "forceRefresh", EffectNetworkRead)
	addTriggerEffect(result, "account/read", "forceRefresh", EffectNetworkRead)

	for method, metadata := range result {
		metadata.Effects = normalizeEffects(metadata.Effects)
		if metadata.RetrySafety == "" {
			metadata.RetrySafety = retryFor(metadata.Effects)
		}
		if method == "account/rateLimitResetCredit/consume" || method == "project/create" {
			metadata.RetrySafety = RetryIdempotencyKeyed
		}
		if method == "thread/unsubscribe" || method == "turn/interrupt" || method == "fs/watch" || method == "fs/unwatch" {
			metadata.RetrySafety = RetryAfterReconciliation
		}
		metadata.ResponseNote = responseNote(method, metadata.Effects)
		metadata.TerminalNote = terminalNote(method, metadata.Effects)
		metadata.ServerCaveats = caveats(method, metadata.Stability, metadata.Effects)
		result[method] = metadata
	}
	return result
}

func baseMetadata(method string, stability Stability, experimental bool) Metadata {
	return Metadata{Method: method, Stability: stability, Effects: []EffectClass{EffectRead}, Experimental: experimental}
}

func setEffects(registry map[string]Metadata, effects []EffectClass, methods ...string) {
	for _, method := range methods {
		metadata, ok := registry[method]
		if !ok {
			continue
		}
		if len(metadata.Effects) == 1 && metadata.Effects[0] == EffectRead {
			metadata.Effects = append([]EffectClass(nil), effects...)
		} else {
			metadata.Effects = mergeEffects(metadata.Effects, effects...)
		}
		registry[method] = metadata
	}
}

func addTriggerEffect(registry map[string]Metadata, method, field string, effect EffectClass) {
	metadata := registry[method]
	for i := range metadata.FieldTriggers {
		if strings.HasSuffix(metadata.FieldTriggers[i].Path, "."+field) {
			metadata.FieldTriggers[i].AddEffects = append(metadata.FieldTriggers[i].AddEffects, effect)
			registry[method] = metadata
			return
		}
	}
	metadata.FieldTriggers = append(metadata.FieldTriggers, FieldTrigger{Path: method + "." + field, AddEffects: []EffectClass{effect}})
	registry[method] = metadata
}

func triggersFor(method string, fields ...string) []FieldTrigger {
	triggers := make([]FieldTrigger, 0, len(fields))
	for _, field := range fields {
		triggers = append(triggers, FieldTrigger{Path: method + "." + field, Aliases: []string{strings.ReplaceAll(field, "_", "-")}, Experimental: true})
	}
	return triggers
}

func normalizeEffects(effects []EffectClass) []EffectClass {
	return mergeEffects(nil, effects...)
}

func retryFor(effects []EffectClass) RetrySafety {
	for _, effect := range effects {
		switch effect {
		case EffectUnknown:
			return RetryUnknown
		case EffectThreadWrite, EffectHostWrite, EffectAuth, EffectDestructive:
			return RetryUnsafe
		}
	}
	return RetrySafe
}

func responseNote(method string, effects []EffectClass) string {
	for _, async := range asynchronousMethods {
		if method == async {
			return "The response acknowledges initiation or admission; completion is delivered through notifications, events, or history and must be reconciled."
		}
	}
	if containsEffect(effects, EffectUnknown) {
		return "No pinned response contract is known for this method."
	}
	return "The JSON-RPC response is the method response; unknown fields must be preserved by the raw layer."
}

func terminalNote(method string, effects []EffectClass) string {
	for _, async := range asynchronousMethods {
		if method == async {
			return "The JSON-RPC response is not terminal operation evidence; use the documented notification/history surface."
		}
	}
	if containsEffect(effects, EffectUnknown) {
		return "There is no known terminal guarantee; a lost response remains uncertain."
	}
	return "The response is the terminal RPC result, subject to backend warnings and connection evidence."
}

func caveats(method string, stability Stability, effects []EffectClass) []string {
	result := []string{"Generated contract presence does not prove backend availability, permissions, persistence, or retry safety."}
	if stability == StabilityExperimental {
		result = append(result, "Experimental method requires initialize.capabilities.experimentalApi=true and may be absent or disabled on a running server.")
	}
	if containsEffect(effects, EffectNetworkRead) {
		result = append(result, "A successful response may reflect a remote/backend request, cache refresh, or stale fallback; preserve network effect evidence.")
	}
	if method == "thread/items/list" || method == "thread/turns/list" || method == "thread/timeline/list" {
		result = append(result, "History availability varies by legacy, paginated, archived, and state-database backends.")
	}
	if method == "thread/shellCommand" {
		result = append(result, "This stable method runs on the app-server host outside the thread sandbox and can have full host impact.")
	}
	if method == "fs/remove" {
		result = append(result, "Removal is recursively and forcefully destructive by default; symlink metadata is handled specially.")
	}
	if method == "feedback/upload" {
		result = append(result, "The request may collect and upload sensitive logs, rollouts, caches, and caller-selected paths.")
	}
	return result
}

var asynchronousMethods = []string{
	"account/login/start", "externalAgentConfig/import", "feedback/upload", "command/exec", "thread/compact/start",
	"thread/shellCommand", "turn/start", "turn/steer", "review/start", "plugin/install", "plugin/uninstall",
	"mcpServer/oauth/login", "windowsSandbox/setupStart", "thread/realtime/start", "thread/realtime/stop",
	"thread/queue/start", "fuzzyFileSearch/sessionStart", "fuzzyFileSearch/sessionUpdate", "fuzzyFileSearch/sessionStop",
	"process/spawn", "process/kill", "remoteControl/pairing/start", "account/sendAddCreditsNudgeEmail",
}

var stableMethods = []string{
	"initialize", "thread/start", "thread/resume", "thread/fork", "thread/archive", "thread/delete", "thread/unsubscribe",
	"thread/name/set", "thread/goal/set", "thread/goal/get", "thread/goal/clear", "thread/metadata/update", "thread/section/move",
	"thread/unarchive", "thread/compact/start", "thread/shellCommand", "thread/approveGuardianDeniedAction", "thread/rollback",
	"thread/revert", "thread/list", "threadSection/list", "threadSection/create", "threadSection/update", "threadSection/delete",
	"thread/loaded/list", "thread/read", "thread/turns/list", "thread/items/list", "thread/inject_items", "skills/list",
	"skills/extraRoots/set", "hooks/list", "marketplace/add", "marketplace/remove", "marketplace/upgrade", "plugin/list",
	"plugin/installed", "plugin/reconcile", "plugin/read", "plugin/skill/read", "plugin/share/save", "plugin/share/updateTargets",
	"plugin/share/list", "plugin/share/checkout", "plugin/share/delete", "app/read", "app/list", "app/installed", "fs/readFile",
	"fs/writeFile", "fs/createDirectory", "fs/getMetadata", "fs/readDirectory", "fs/remove", "fs/copy", "fs/watch", "fs/unwatch",
	"skills/config/write", "plugin/install", "plugin/uninstall", "turn/start", "turn/steer", "turn/interrupt", "review/start",
	"model/list", "modelProvider/capabilities/read", "experimentalFeature/list", "permissionProfile/list", "experimentalFeature/enablement/set",
	"mcpServer/oauth/login", "config/mcpServer/reload", "mcpServerStatus/list", "mcpServer/resource/read", "mcpServer/tool/call",
	"windowsSandbox/setupStart", "windowsSandbox/readiness", "account/login/start", "account/login/cancel", "account/logout",
	"account/rateLimits/read", "account/rateLimitResetCredit/consume", "account/usage/read", "account/workspaceMessages/read",
	"account/sendAddCreditsNudgeEmail", "feedback/upload", "command/exec", "command/exec/write", "command/exec/terminate",
	"command/exec/resize", "config/read", "externalAgentConfig/detect", "externalAgentConfig/import", "externalAgentConfig/import/recordHistory",
	"externalAgentConfig/import/readHistories", "config/value/write", "config/batchWrite", "configRequirements/read", "account/read",
	"getConversationSummary", "gitDiffToRemote", "getAuthStatus", "fuzzyFileSearch",
}

var experimentalOnlyMethods = []string{
	"server/diagnostics", "userVerification/status", "userVerification/enroll", "userVerification/delete", "userVerification/verify",
	"thread/increment_elicitation", "thread/decrement_elicitation", "thread/queue/add", "thread/queue/list", "thread/queue/update",
	"thread/queue/delete", "thread/queue/reorder", "thread/queue/start", "thread/settings/update", "thread/memoryMode/set", "memory/reset",
	"thread/backgroundTerminals/clean", "thread/backgroundTerminals/list", "thread/backgroundTerminals/terminate", "project/list", "project/read",
	"project/create", "project/import", "project/update", "project/move", "project/delete", "thread/search", "thread/searchOccurrences",
	"plugin/search", "turn/settings/update", "thread/realtime/start", "thread/realtime/appendAudio", "thread/realtime/appendText",
	"thread/realtime/appendSpeech", "thread/realtime/stop", "thread/timeline/list", "thread/realtime/listVoices", "remoteControl/enable",
	"remoteControl/disable", "remoteControl/status/read", "remoteControl/pairing/start", "remoteControl/pairing/status", "remoteControl/client/list",
	"remoteControl/client/revoke", "collaborationMode/list", "mock/experimentalMethod", "environment/add", "environment/info", "environment/status",
	"mcpServer/event/stream/start", "mcpServer/event/stream/stop", "account/bedrock/discover", "account/bedrock/setup", "process/spawn",
	"process/writeStdin", "process/kill", "process/resizePty", "fuzzyFileSearch/sessionStart", "fuzzyFileSearch/sessionUpdate", "fuzzyFileSearch/sessionStop",
}
