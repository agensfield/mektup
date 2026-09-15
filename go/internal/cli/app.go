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

var DefaultBuildInfo = BuildInfo{
	Version:            "dev",
	Commit:             "unknown",
	InstallKind:        "source",
	ContractVersion:    ContractVersion,
	TestedCodexServers: []string{"0.154.0"},
}

// Executor is the seam for the transport/journal implementation. It must
// return a typed Error when work fails. The CLI never turns a nil result into
// an optimistic success on behalf of an executor.
type Executor interface {
	Execute(context.Context, Invocation) (any, error)
}

type defaultExecutor struct{}

func (defaultExecutor) Execute(context.Context, Invocation) (any, error) {
	return nil, &Error{Code: "internal_error", Message: "operational command is not implemented", Exit: ExitInternal}
}

// App is the embeddable CLI application.
type App struct {
	In       io.Reader
	Out      io.Writer
	Err      io.Writer
	Env      []string
	Build    BuildInfo
	Executor Executor
}

func New() *App { return &App{In: os.Stdin, Out: os.Stdout, Err: os.Stderr, Build: DefaultBuildInfo} }

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

	env := a.env()
	preJSON, preHuman := scanPresentation(args)
	presentation, presentationErr := DetectPresentation(preJSON, preHuman, env)
	parsed, parseErr := Parse(args)
	if presentationErr != nil {
		return a.finish(presentation, Invocation{}, presentationErr)
	}
	if parseErr != nil {
		return a.finish(presentation, parsed, parseErr)
	}
	if parsed.Global.JSON || parsed.Global.Human {
		presentation, presentationErr = DetectPresentation(parsed.Global.JSON, parsed.Global.Human, env)
		if presentationErr != nil {
			return a.finish(presentation, parsed, presentationErr)
		}
	}

	if has(parsed, "skill") {
		if parsed.Command != "--skill" || len(parsed.Position) != 0 || !onlyOptions(parsed, "skill", "json", "human", "debug", "audit", "endpoint", "config", "state-dir") {
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
		if len(parsed.Position) != 0 || !onlyOptions(parsed, "json", "human", "debug", "audit", "endpoint", "config", "state-dir") {
			return a.finish(presentation, parsed, usageError("usage: mektup version [--json]"))
		}
		return a.version(presentation)
	case "completion":
		if len(parsed.Position) != 1 || !onlyOptions(parsed, "json", "human", "debug", "audit", "endpoint", "config", "state-dir") {
			return a.finish(presentation, parsed, usageError("usage: mektup completion <zsh|bash|fish>"))
		}
		return a.completion(presentation, parsed.Position[0])
	case "docs":
		return a.docs(presentation, parsed)
	}

	if err := validateInvocation(a, parsed); err != nil {
		return a.finish(presentation, parsed, err)
	}
	_, err := a.Executor.Execute(context.Background(), parsed)
	if err == nil {
		return a.finish(presentation, parsed, &Error{Code: "internal_error", Message: "operational command returned no result", Exit: ExitInternal})
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
		return a.writeJSON(map[string]any{"schema": EventSchema, "event": "help", "terminal": true, "ok": true, "data": map[string]any{"text": text}})
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
	if !onlyOptions(inv, "json", "human", "debug", "audit", "endpoint", "config", "state-dir") {
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

func (a *App) writeEvent(inv Invocation, e *Error) int {
	operation := "operation.failed"
	if e.Code == "invalid_arguments" {
		operation = "operation.rejected"
	}
	event := map[string]any{
		"schema":      EventSchema,
		"event":       operation,
		"eventId":     newID("evt_"),
		"sequence":    1,
		"operationId": newID("op_"),
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
		if e.Exit == 0 && e.Code != "" {
			e.Exit = exitForError(e.Code)
		}
		return e
	}
	return &Error{Code: "internal_error", Message: err.Error(), Exit: ExitInternal}
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

func newID(prefix string) string {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return prefix + "00000000-0000-4000-8000-000000000000"
	}
	// UUID-shaped randomness is sufficient for CLI operation identity here.
	bytes[6] = (bytes[6] & 0x0f) | 0x40
	bytes[8] = (bytes[8] & 0x3f) | 0x80
	return prefix + fmt.Sprintf("%s-%s-%s-%s-%s", hex.EncodeToString(bytes[0:4]), hex.EncodeToString(bytes[4:6]), hex.EncodeToString(bytes[6:8]), hex.EncodeToString(bytes[8:10]), hex.EncodeToString(bytes[10:16]))
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
