// Package cli implements the Mektup command grammar and presentation
// contract. Operational work is deliberately supplied by an injected
// Executor; the default executor reports that the operation is not yet
// implemented rather than pretending that it succeeded.
package cli

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	ContractVersion = "1.0.1"
	EventSchema     = "mektup/event/v1"
	CommandSchema   = "mektup/command-contract/v1"
)

// ExitCode is the stable process-level exit class from wire-contract-v1.
type ExitCode int

const (
	ExitSuccess    ExitCode = 0
	ExitInternal   ExitCode = 1
	ExitUsage      ExitCode = 2
	ExitRejected   ExitCode = 3
	ExitUnknown    ExitCode = 4
	ExitIncomplete ExitCode = 5
)

type Presentation string

const (
	PresentationHuman Presentation = "human"
	PresentationJSON  Presentation = "json"
)

// BuildInfo is included in human and machine version output.
type BuildInfo struct {
	Version            string
	Commit             string
	InstallKind        string
	ContractVersion    string
	TestedCodexServers []string
}

// These variables are the release ldflag seam. They intentionally live in
// internal/cli, where the release configuration can set them without having
// to know the tiny cmd package's implementation details.
var (
	buildVersion         = "dev"
	buildCommit          = "unknown"
	buildContractVersion = ContractVersion
	buildTestedCodex     = "0.154.0"
	buildInstallKind     = "source"
)

var DefaultBuildInfo = BuildInfo{
	Version:            "dev",
	Commit:             "unknown",
	InstallKind:        "source",
	ContractVersion:    ContractVersion,
	TestedCodexServers: []string{"0.154.0"},
}

// BuildInfoFromBuildVars returns metadata after release ldflags have been
// applied. Tested versions are comma-separated in buildTestedCodex.
func BuildInfoFromBuildVars() BuildInfo {
	servers := make([]string, 0)
	for _, server := range strings.Split(buildTestedCodex, ",") {
		if server = strings.TrimSpace(server); server != "" {
			servers = append(servers, server)
		}
	}
	if len(servers) == 0 {
		servers = append(servers, DefaultBuildInfo.TestedCodexServers...)
	}
	return BuildInfo{Version: buildVersion, Commit: buildCommit, InstallKind: buildInstallKind, ContractVersion: buildContractVersion, TestedCodexServers: servers}
}

// IDGenerator is injectable for deterministic tests and for a future shared
// wire ID implementation. It must return an error when entropy is unavailable.
type IDGenerator func(prefix string) (string, error)

// OutputEvent is one domain result in the executor-to-CLI presentation seam.
// Machine is encoded as one compact JSONL object; Human is used only by human
// presentation.
type OutputEvent struct {
	Machine any
	Human   string
}

// ExecutionResult is a successful or terminal operational result. A result
// with no event, human text, or receipt is not a success: it is treated as an
// internal executor contract violation.
type ExecutionResult struct {
	Events  []OutputEvent
	Human   string
	Receipt any
	Exit    ExitCode
}

// Executor is the seam for the transport/journal implementation. It must
// return a typed Error when work fails or a non-empty ExecutionResult when it
// succeeds. The CLI never turns a nil/empty result into an optimistic success.
type Executor interface {
	Execute(context.Context, Invocation) (ExecutionResult, error)
}

type defaultExecutor struct{}

func (defaultExecutor) Execute(context.Context, Invocation) (ExecutionResult, error) {
	return ExecutionResult{}, &Error{Code: "internal_error", Message: "operational command is not implemented", Effect: "not_sent", Exit: ExitInternal}
}

// App is the embeddable CLI application.
type App struct {
	In       io.Reader
	Out      io.Writer
	Err      io.Writer
	Env      []string
	Build    BuildInfo
	Executor Executor
	ID       IDGenerator
}

func New() *App {
	return &App{In: os.Stdin, Out: os.Stdout, Err: os.Stderr, Build: BuildInfoFromBuildVars(), ID: defaultID}
}

// Error is a stable Mektup error suitable for JSONL terminal output.
type Error struct {
	Code      string
	Message   string
	Retryable bool
	Effect    string
	Details   map[string]any
	Exit      ExitCode
}

func (e *Error) Error() string {
	if e == nil {
		return ""
	}
	return e.Message
}

func usageError(message string) *Error {
	return &Error{Code: "invalid_arguments", Message: message, Exit: ExitUsage}
}

// Invocation is the validated command handed to an Executor.
type Invocation struct {
	Command  string
	Path     []string
	Position []string
	Options  map[string][]string
	Global   Globals
	Resolved ResolvedGlobals
}

type Globals struct {
	JSON     bool
	Human    bool
	Endpoint string
	Config   string
	StateDir string
	Debug    bool
	Audit    bool
	Help     bool
}

// ResolvedGlobals records effective configuration without reading or writing
// config/state files. Flags win over dedicated environment variables, then
// built-in defaults are used.
type ResolvedGlobals struct {
	Output   Presentation
	Endpoint string
	Config   string
	StateDir string
}

func (i Invocation) Option(name string) string {
	v := i.Options[name]
	if len(v) == 0 {
		return ""
	}
	return v[len(v)-1]
}

// DetectPresentation applies the locked precedence. No generic CI, TTY,
// HERDR_ENV, or terminal-program marker is consulted.
func DetectPresentation(explicitJSON, explicitHuman bool, env map[string]string) (Presentation, error) {
	if explicitJSON && explicitHuman {
		return "", usageError("--json and --human are mutually exclusive")
	}
	if explicitJSON {
		return PresentationJSON, nil
	}
	if explicitHuman {
		return PresentationHuman, nil
	}
	if env["MEKTUP_AGENT"] == "1" {
		return PresentationJSON, nil
	}
	if strings.TrimSpace(env["CODEX_THREAD_ID"]) != "" {
		return PresentationJSON, nil
	}
	return PresentationHuman, nil
}

func resolvePresentation(explicitJSON, explicitHuman bool, env map[string]string) (Presentation, error) {
	if explicitJSON || explicitHuman {
		return DetectPresentation(explicitJSON, explicitHuman, env)
	}
	if output := strings.ToLower(strings.TrimSpace(env["MEKTUP_OUTPUT"])); output != "" && output != "auto" {
		switch output {
		case string(PresentationJSON):
			return PresentationJSON, nil
		case string(PresentationHuman):
			return PresentationHuman, nil
		default:
			return "", usageError("MEKTUP_OUTPUT must be json, human, or auto")
		}
	}
	return DetectPresentation(false, false, env)
}

func resolveGlobals(inv Invocation, env map[string]string) (Invocation, *Error) {
	config, state := defaultStatePaths(env)
	endpoint := "local"
	if inv.Global.Endpoint != "" {
		endpoint = inv.Global.Endpoint
	} else if value := strings.TrimSpace(env["MEKTUP_ENDPOINT"]); value != "" {
		endpoint = value
	}
	if inv.Global.Config != "" {
		config = inv.Global.Config
	} else if value := strings.TrimSpace(env["MEKTUP_CONFIG"]); value != "" {
		config = value
	}
	if inv.Global.StateDir != "" {
		state = inv.Global.StateDir
	} else if value := strings.TrimSpace(env["MEKTUP_STATE_DIR"]); value != "" {
		state = value
	}
	output, err := resolvePresentation(inv.Global.JSON, inv.Global.Human, env)
	if err != nil {
		return inv, normalizeError(err)
	}
	inv.Resolved = ResolvedGlobals{Output: output, Endpoint: endpoint, Config: config, StateDir: state}
	return inv, nil
}

func (a *App) env() map[string]string {
	result := make(map[string]string)
	if a.Env == nil {
		for _, kv := range os.Environ() {
			if key, value, ok := strings.Cut(kv, "="); ok {
				result[key] = value
			}
		}
		return result
	}
	for _, kv := range a.Env {
		if key, value, ok := strings.Cut(kv, "="); ok {
			result[key] = value
		}
	}
	return result
}

func (a *App) Run(args []string) int {
	if a.In == nil {
		a.In = os.Stdin
	}
	if a.Out == nil {
		a.Out = os.Stdout
	}
	if a.Err == nil {
		a.Err = os.Stderr
	}
	defaults := DefaultBuildInfo
	if a.Build.Version == "" {
		a.Build.Version = defaults.Version
	}
	if a.Build.Commit == "" {
		a.Build.Commit = defaults.Commit
	}
	if a.Build.InstallKind == "" {
		a.Build.InstallKind = defaults.InstallKind
	}
	if len(a.Build.TestedCodexServers) == 0 {
		a.Build.TestedCodexServers = defaults.TestedCodexServers
	}
	if a.Build.ContractVersion == "" {
		a.Build.ContractVersion = defaults.ContractVersion
	}
	if a.Executor == nil {
		a.Executor = defaultExecutor{}
	}
	if a.ID == nil {
		a.ID = defaultID
	}

	env := a.env()
	preJSON, preHuman := scanPresentation(args)
	parsed, parseErr := Parse(args)
	presentation, presentationErr := resolvePresentation(preJSON, preHuman, env)
	if presentationErr != nil {
		return a.finish(presentation, Invocation{}, presentationErr)
	}
	if parseErr != nil {
		return a.finish(presentation, parsed, parseErr)
	}
	if globalErr := validateGlobals(parsed); globalErr != nil {
		return a.finish(presentation, parsed, globalErr)
	}
	if parsed.Global.JSON || parsed.Global.Human {
		presentation, presentationErr = resolvePresentation(parsed.Global.JSON, parsed.Global.Human, env)
		if presentationErr != nil {
			return a.finish(presentation, parsed, presentationErr)
		}
	}
	resolved, resolveErr := resolveGlobals(parsed, env)
	if resolveErr != nil {
		return a.finish(presentation, parsed, resolveErr)
	}
	parsed = resolved

	if has(parsed, "skill") {
		if parsed.Command != "--skill" || len(parsed.Position) != 0 || !onlyOptions(parsed, "skill", "json", "human") {
			return a.finish(presentation, parsed, usageError("use mektup --skill without operational arguments"))
		}
		return a.writeGuide()
	}
	if parsed.Global.Help || parsed.Command == "" || parsed.Command == "help" {
		if parsed.Command == "help" && len(parsed.Position) > 1 {
			return a.finish(presentation, parsed, usageError("usage: mektup help [command]"))
		}
		topic := parsed.Position
		if parsed.Global.Help && parsed.Command != "" && parsed.Command != "help" {
			topic = []string{parsed.Command}
		}
		return a.help(presentation, topic)
	}
	switch parsed.Command {
	case "version":
		if len(parsed.Position) != 0 || !onlyOptions(parsed, "json", "human") {
			return a.finish(presentation, parsed, usageError("usage: mektup version [--json]"))
		}
		return a.version(presentation)
	case "completion":
		if len(parsed.Position) != 1 || !onlyOptions(parsed, "json", "human") {
			return a.finish(presentation, parsed, usageError("usage: mektup completion <zsh|bash|fish>"))
		}
		return a.completion(presentation, parsed.Position[0])
	case "docs":
		return a.docs(presentation, parsed)
	}

	if err := validateInvocation(a, parsed); err != nil {
		return a.finish(presentation, parsed, err)
	}
	result, err := a.Executor.Execute(context.Background(), parsed)
	if err == nil {
		return a.writeExecutionResult(presentation, parsed, result)
	}
	return a.finish(presentation, parsed, normalizeError(err))
}

func (a *App) help(p Presentation, position []string) int {
	text := usageText
	if len(position) == 1 {
		if detail, ok := helpTopics[position[0]]; ok {
			text = detail
		} else {
			return a.finish(p, Invocation{Command: "help"}, usageError("unknown help topic: "+position[0]))
		}
	}
	if p == PresentationJSON {
		return a.writeJSON(map[string]any{"schema": "mektup/help/v1", "ok": true, "topic": "help", "text": text})
	}
	_, _ = io.WriteString(a.Out, ensureFinalNewline(text))
	return int(ExitSuccess)
}

func (a *App) version(p Presentation) int {
	if p == PresentationJSON {
		return a.writeJSON(map[string]any{"schema": "mektup/version/v1", "ok": true, "version": a.Build.Version, "commit": a.Build.Commit, "install_kind": a.Build.InstallKind, "contract_version": a.Build.ContractVersion, "tested_codex_versions": a.Build.TestedCodexServers})
	}
	_, _ = fmt.Fprintf(a.Out, "mektup %s (commit %s, %s, contract %s; tested Codex %s)\n", a.Build.Version, a.Build.Commit, a.Build.InstallKind, a.Build.ContractVersion, strings.Join(a.Build.TestedCodexServers, ", "))
	return int(ExitSuccess)
}

func (a *App) completion(p Presentation, shell string) int {
	if shell != "zsh" && shell != "bash" && shell != "fish" {
		return a.finish(p, Invocation{Command: "completion"}, usageError("unsupported completion shell: "+shell))
	}
	if p == PresentationJSON {
		return a.writeJSON(map[string]any{"schema": "mektup/completion/v1", "ok": true, "shell": shell, "script": completionScript(shell)})
	}
	_, _ = io.WriteString(a.Out, completionScript(shell))
	return int(ExitSuccess)
}

func (a *App) docs(p Presentation, inv Invocation) int {
	if len(inv.Position) != 1 {
		return a.finish(p, inv, usageError("usage: mektup docs agents|commands|envelopes|receipts [--json]"))
	}
	if !onlyOptions(inv, "json", "human") {
		return a.finish(p, inv, usageError("docs accepts only a documentation topic and presentation options"))
	}
	switch inv.Position[0] {
	case "agents":
		return a.writeGuide()
	case "commands":
		var data []byte
		var err error
		if inv.Global.JSON {
			data, err = json.Marshal(commandContract())
		} else {
			data, err = json.MarshalIndent(commandContract(), "", "  ")
		}
		if err != nil {
			return a.finish(p, inv, &Error{Code: "internal_error", Message: err.Error(), Exit: ExitInternal})
		}
		if inv.Global.JSON {
			_, _ = a.Out.Write(append(data, '\n'))
			return int(ExitSuccess)
		}
		_, _ = a.Out.Write(append(data, '\n'))
		return int(ExitSuccess)
	case "envelopes":
		return a.writeAsset(envelopeDocs)
	case "receipts":
		return a.writeAsset(receiptDocs)
	default:
		return a.finish(p, inv, usageError("unknown docs topic: "+inv.Position[0]))
	}
}

func (a *App) writeGuide() int {
	_, _ = io.WriteString(a.Out, ensureFinalNewline(agentGuide))
	return int(ExitSuccess)
}

func (a *App) writeAsset(asset string) int {
	_, _ = io.WriteString(a.Out, ensureFinalNewline(asset))
	return int(ExitSuccess)
}

func (a *App) finish(p Presentation, inv Invocation, err error) int {
	if err == nil {
		return int(ExitSuccess)
	}
	e := normalizeError(err)
	if p == PresentationJSON {
		return a.writeEvent(inv, e)
	}
	if e.Message != "" {
		_, _ = fmt.Fprintln(a.Err, "mektup: "+e.Message)
	}
	return int(e.Exit)
}

func (a *App) writeExecutionResult(p Presentation, inv Invocation, result ExecutionResult) int {
	if result.Exit == 0 {
		result.Exit = ExitSuccess
	}
	if p == PresentationJSON {
		events, err := a.lifecycleEvents(inv, result)
		if err != nil {
			return a.internalFailure(err.Error())
		}
		for _, event := range events {
			if status := a.writeJSON(event); status != int(ExitSuccess) {
				return status
			}
		}
		return int(result.Exit)
	}

	hasOutput := strings.TrimSpace(result.Human) != ""
	if result.Human != "" {
		if _, err := io.WriteString(a.Out, ensureFinalNewline(result.Human)); err != nil {
			return int(ExitInternal)
		}
	}
	for _, event := range result.Events {
		if strings.TrimSpace(event.Human) != "" {
			hasOutput = true
			if _, err := io.WriteString(a.Out, ensureFinalNewline(event.Human)); err != nil {
				return int(ExitInternal)
			}
		}
	}
	if result.Receipt != nil {
		hasOutput = true
		if _, err := io.WriteString(a.Out, ensureFinalNewline(receiptHuman(result.Receipt))); err != nil {
			return a.internalFailure("unable to write receipt: " + err.Error())
		}
	}
	if !hasOutput {
		return a.finish(p, inv, &Error{Code: "internal_error", Message: "operational command returned no human output", Exit: ExitInternal})
	}
	return int(result.Exit)
}

// lifecycleEvents makes the executor boundary safe even while the domain
// packages are being integrated. A receipt is data on the terminal lifecycle
// event, never a second bare JSON line after that event.
func (a *App) lifecycleEvents(inv Invocation, result ExecutionResult) ([]map[string]any, error) {
	if len(result.Events) == 0 && result.Receipt == nil {
		return nil, errors.New("operational command returned no machine output")
	}
	events := make([]map[string]any, 0, len(result.Events)+1)
	for _, output := range result.Events {
		if output.Machine == nil {
			continue
		}
		encoded, err := json.Marshal(output.Machine)
		if err != nil {
			return nil, fmt.Errorf("invalid executor lifecycle event: %w", err)
		}
		var event map[string]any
		if err := json.Unmarshal(encoded, &event); err != nil {
			return nil, fmt.Errorf("invalid executor lifecycle event: %w", err)
		}
		events = append(events, event)
	}
	syntheticTerminal := len(events) == 0
	if syntheticTerminal && result.Receipt != nil {
		events = append(events, map[string]any{"event": "operation.completed", "data": map[string]any{"receipt": result.Receipt}})
	}
	if len(events) == 0 {
		return nil, errors.New("operational command returned no machine output")
	}
	operationID := ""
	for _, event := range events {
		if supplied, ok := event["operationId"].(string); ok && supplied != "" {
			if !validUUIDv7ID(supplied, "op_") {
				return nil, fmt.Errorf("executor operationId is not a UUIDv7: %q", supplied)
			}
			if operationID != "" && operationID != supplied {
				return nil, errors.New("executor lifecycle events contain conflicting operation IDs")
			}
			operationID = supplied
		}
	}
	if operationID == "" {
		var err error
		operationID, err = a.ID("op_")
		if err != nil {
			return nil, fmt.Errorf("unable to allocate lifecycle operation identity: %w", err)
		}
		if !validUUIDv7ID(operationID, "op_") {
			return nil, fmt.Errorf("generated operationId is not a UUIDv7: %q", operationID)
		}
	}
	for index, event := range events {
		if schema, ok := event["schema"].(string); ok && schema != "" && schema != EventSchema {
			return nil, fmt.Errorf("executor event schema %q is not %s", schema, EventSchema)
		}
		event["schema"] = EventSchema
		if _, ok := event["event"].(string); !ok || event["event"] == "" {
			event["event"] = "operation.progress"
		}
		if _, ok := event["eventId"].(string); !ok || event["eventId"] == "" {
			id, err := a.ID("evt_")
			if err != nil {
				return nil, fmt.Errorf("unable to allocate lifecycle event identity: %w", err)
			}
			if !validUUIDv7ID(id, "evt_") {
				return nil, fmt.Errorf("generated eventId is not a UUIDv7: %q", id)
			}
			event["eventId"] = id
		} else if !validUUIDv7ID(event["eventId"].(string), "evt_") {
			return nil, fmt.Errorf("executor eventId is not a UUIDv7: %q", event["eventId"])
		}
		event["operationId"] = operationID
		if _, ok := event["timestamp"].(string); !ok || event["timestamp"] == "" {
			event["timestamp"] = time.Now().UTC().Format(time.RFC3339Nano)
		}
		event["sequence"] = index + 1
		if _, ok := event["terminal"].(bool); !ok {
			event["terminal"] = false
		}
		if _, ok := event["ok"].(bool); !ok {
			event["ok"] = result.Exit == ExitSuccess
		}
		if _, ok := event["warnings"]; !ok {
			event["warnings"] = []any{}
		}
		if _, ok := event["data"].(map[string]any); !ok {
			event["data"] = map[string]any{}
		}
		if terminal, _ := event["terminal"].(bool); terminal && index != len(events)-1 {
			return nil, errors.New("executor lifecycle event appears after a terminal event")
		}
	}
	last := events[len(events)-1]
	if result.Receipt != nil {
		data := last["data"].(map[string]any)
		data["receipt"] = result.Receipt
		last["terminal"] = true
		if syntheticTerminal {
			last["event"] = "operation.completed"
		}
	}
	if terminal, _ := last["terminal"].(bool); !terminal {
		last["terminal"] = true
	}
	return events, nil
}

func validUUIDv7ID(value, prefix string) bool {
	if !strings.HasPrefix(value, prefix) {
		return false
	}
	uuid := strings.TrimPrefix(value, prefix)
	parts := strings.Split(uuid, "-")
	if len(parts) != 5 || len(parts[0]) != 8 || len(parts[1]) != 4 || len(parts[2]) != 4 || len(parts[3]) != 4 || len(parts[4]) != 12 {
		return false
	}
	if _, err := hex.DecodeString(strings.Join(parts, "")); err != nil {
		return false
	}
	if parts[2][0] != '7' {
		return false
	}
	switch parts[3][0] {
	case '8', '9', 'a', 'b':
		return true
	default:
		return false
	}
}

func receiptHuman(receipt any) string {
	encoded, err := json.Marshal(receipt)
	if err != nil {
		return "receipt emitted"
	}
	var object map[string]any
	if err := json.Unmarshal(encoded, &object); err != nil {
		return "receipt emitted"
	}
	parts := []string{"receipt"}
	if id, ok := object["receiptId"].(string); ok && id != "" {
		parts = append(parts, id)
	}
	if state, ok := object["state"].(string); ok && state != "" {
		parts = append(parts, "state="+state)
	}
	return strings.Join(parts, " ")
}

func (a *App) internalFailure(message string) int {
	_, _ = fmt.Fprintln(a.Err, "mektup: "+message)
	return int(ExitInternal)
}

func (a *App) writeEvent(inv Invocation, e *Error) int {
	operation := "operation.failed"
	if e.Code == "invalid_arguments" {
		operation = "operation.rejected"
	}
	eventID, eventErr := a.ID("evt_")
	operationID, operationErr := a.ID("op_")
	if eventErr != nil || operationErr != nil {
		return a.internalFailure("unable to allocate operation identity")
	}
	event := map[string]any{
		"schema":      EventSchema,
		"event":       operation,
		"eventId":     eventID,
		"sequence":    1,
		"operationId": operationID,
		"timestamp":   time.Now().UTC().Format(time.RFC3339Nano),
		"terminal":    true,
		"ok":          false,
		"warnings":    []any{},
		"data": map[string]any{"error": map[string]any{
			"code": e.Code, "message": e.Message, "retryable": e.Retryable, "effectState": e.Effect, "details": e.Details,
		}},
	}
	if status := a.writeJSON(eventWithCommand(event, inv)); status != int(ExitSuccess) {
		return status
	}
	return int(e.Exit)
}

func eventWithCommand(event map[string]any, inv Invocation) map[string]any {
	if inv.Command != "" {
		if data, ok := event["data"].(map[string]any); ok {
			data["command"] = inv.Command
		}
	}
	return event
}

func (a *App) writeJSON(value any) int {
	encoder := json.NewEncoder(a.Out)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		_, _ = fmt.Fprintln(a.Err, "mektup: unable to write output: "+err.Error())
		return int(ExitInternal)
	}
	return int(ExitSuccess)
}

func normalizeError(err error) *Error {
	var e *Error
	if errors.As(err, &e) {
		if e.Code == "" {
			e.Code = "internal_error"
		}
		if e.Exit == 0 && e.Code != "" {
			e.Exit = exitForError(e.Code)
		}
		if e.Effect == "" {
			if e.Exit == ExitUsage {
				e.Effect = "not_sent"
			} else {
				e.Effect = "unknown"
			}
		}
		if e.Details == nil {
			e.Details = map[string]any{}
		}
		return e
	}
	return &Error{Code: "internal_error", Message: err.Error(), Effect: "unknown", Details: map[string]any{}, Exit: ExitInternal}
}

func exitForError(code string) ExitCode {
	switch code {
	case "invalid_arguments", "invalid_target", "reply_route_required", "invalid_raw_wait", "invalid_raw_reply_request", "effect_acknowledgment_required":
		return ExitUsage
	case "target_ambiguous", "message_not_found", "message_identity_conflict", "message_not_addressed_to_thread", "delivery_rejected", "delivery_temporarily_unavailable", "resolver_unavailable", "route_unavailable", "reply_route_unavailable", "endpoint_unavailable", "unsupported_server_version", "input_too_large", "reply_not_requested", "content_unavailable", "experimental_method_unavailable":
		return ExitRejected
	case "outcome_unknown", "reply_outcome_unknown":
		return ExitUnknown
	case "wait_incomplete", "wait_interrupted":
		return ExitIncomplete
	default:
		return ExitInternal
	}
}

// ExitCodeForError exposes the stable mapping for embedders and conformance
// tests without requiring them to duplicate process-level policy.
func ExitCodeForError(code string) ExitCode { return exitForError(code) }

func defaultID(prefix string) (string, error) {
	var bytes [16]byte
	// UUIDv7 stores Unix milliseconds in the first 48 bits and random data in
	// the remaining bits. Never substitute a fixed ID when entropy fails.
	millis := uint64(time.Now().UnixMilli()) & ((uint64(1) << 48) - 1)
	bytes[0] = byte(millis >> 40)
	bytes[1] = byte(millis >> 32)
	bytes[2] = byte(millis >> 24)
	bytes[3] = byte(millis >> 16)
	bytes[4] = byte(millis >> 8)
	bytes[5] = byte(millis)
	if _, err := rand.Read(bytes[6:]); err != nil {
		return "", fmt.Errorf("uuidv7 entropy unavailable: %w", err)
	}
	bytes[6] = (bytes[6] & 0x0f) | 0x70
	bytes[8] = (bytes[8] & 0x3f) | 0x80
	return prefix + fmt.Sprintf("%s-%s-%s-%s-%s", hex.EncodeToString(bytes[0:4]), hex.EncodeToString(bytes[4:6]), hex.EncodeToString(bytes[6:8]), hex.EncodeToString(bytes[8:10]), hex.EncodeToString(bytes[10:16])), nil
}

func ensureFinalNewline(s string) string {
	return strings.TrimRight(s, "\n") + "\n"
}

func scanPresentation(args []string) (jsonMode, humanMode bool) {
	for _, arg := range args {
		switch arg {
		case "--json":
			jsonMode = true
		case "--human":
			humanMode = true
		}
	}
	return
}

func onlyOptions(inv Invocation, names ...string) bool {
	allowed := make(map[string]bool, len(names))
	for _, name := range names {
		allowed[name] = true
	}
	for name := range inv.Options {
		if !allowed[name] {
			return false
		}
	}
	return true
}

func defaultStatePaths(env map[string]string) (string, string) {
	config := env["MEKTUP_CONFIG"]
	if config == "" {
		if xdg := env["XDG_CONFIG_HOME"]; xdg != "" {
			config = filepath.Join(xdg, "mektup")
		} else if home, err := os.UserHomeDir(); err == nil {
			config = filepath.Join(home, ".config", "mektup")
		}
	}
	state := env["MEKTUP_STATE_DIR"]
	if state == "" {
		if xdg := env["XDG_DATA_HOME"]; xdg != "" {
			state = filepath.Join(xdg, "mektup")
		} else if home, err := os.UserHomeDir(); err == nil {
			state = filepath.Join(home, ".local", "share", "mektup")
		}
	}
	return config, state
}
