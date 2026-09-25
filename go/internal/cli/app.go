// Package cli implements the Mektup command grammar and presentation
// contract. Operational work is deliberately supplied by an injected
// Executor; the default executor reports that the operation is not yet
// implemented rather than pretending that it succeeded.
package cli

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"runtime/debug"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/mattn/go-isatty"
	"golang.org/x/mod/semver"
)

const (
	ContractVersion = "1.0.8"
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
	buildTestedCodex     = "0.154.0,0.155.1,0.156.0,0.156.1,0.157.0"
	buildInstallKind     = "source"
)

var DefaultBuildInfo = BuildInfo{
	Version:            "dev",
	Commit:             "unknown",
	InstallKind:        "source",
	ContractVersion:    ContractVersion,
	TestedCodexServers: []string{"0.154.0", "0.155.1", "0.156.0", "0.156.1", "0.157.0"},
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
	version, commit, installKind := buildVersion, buildCommit, buildInstallKind
	if info, ok := debug.ReadBuildInfo(); ok {
		version, commit, installKind = resolveModuleBuildInfo(version, commit, installKind, info)
	}
	return BuildInfo{Version: version, Commit: commit, InstallKind: installKind, ContractVersion: buildContractVersion, TestedCodexServers: servers}
}

func resolveModuleBuildInfo(version, commit, installKind string, info *debug.BuildInfo) (string, string, string) {
	if info == nil {
		return version, commit, installKind
	}
	// GoReleaser's explicit metadata remains authoritative. Ordinary
	// `go install module/cmd@vX.Y.Z` has no ldflag seam, but Go embeds the main
	// module version in runtime build info. Use it only when the binary still
	// carries the source defaults.
	if version == "dev" && info.Main.Path == "github.com/agensfield/mektup/go" && semver.IsValid(info.Main.Version) {
		version = strings.TrimPrefix(info.Main.Version, "v")
		if installKind == "source" {
			installKind = "go-install"
		}
	}
	if commit == "unknown" {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" && setting.Value != "" {
				commit = setting.Value
				break
			}
		}
	}
	return version, commit, installKind
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
	Events    []OutputEvent
	Human     string
	Receipt   any
	Exit      ExitCode
	Streaming bool
	RawJSON   []byte
}

// Executor is the seam for the transport/journal implementation. It must
// return a typed Error when work fails or a non-empty ExecutionResult when it
// succeeds. The CLI never turns a nil/empty result into an optimistic success.
type Executor interface {
	Execute(context.Context, Invocation) (ExecutionResult, error)
}

// StreamingExecutor is the optional lifecycle seam for operations that must
// expose an accepted milestone before their terminal wait completes. Each
// callback result is rendered immediately; Streaming keeps nonterminal chunks
// from being upgraded into terminal JSONL events.
type StreamingExecutor interface {
	ExecuteStream(context.Context, Invocation, func(ExecutionResult) error) error
}

// CompactArtifact is a truthful owner-private retention receipt for a compact
// record that could not fit the terminal output bound.
type CompactArtifact struct {
	Path   string `json:"path"`
	Bytes  int64  `json:"bytes"`
	SHA256 string `json:"sha256"`
}

// CompactRetainer is optional. Production implements it so a complete record
// remains reachable when the bounded compact terminal cannot carry it.
type CompactRetainer interface {
	RetainCompact(context.Context, Invocation, []byte) (CompactArtifact, error)
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
	Compact  bool
	Endpoint string
	Config   string
	StateDir string
	Debug    bool
	Audit    bool
	Help     bool
	Color    string
}

// ResolvedGlobals records effective configuration without reading or writing
// config/state files. Flags win over dedicated environment variables, then
// built-in defaults are used.
type ResolvedGlobals struct {
	Output                   Presentation
	Endpoint                 string
	EndpointSource           PathSource
	Config                   string
	StateDir                 string
	ConfigSource             PathSource
	StateSource              PathSource
	CodexHome                string
	CurrentThreadID          string
	AgentMode                bool
	Compact                  bool
	CompactRequestedLimit    *int
	CompactRequestedReceipts *int
	Color                    bool
	ErrorColor               bool
}

// PathSource records which input won path precedence. It is intentionally
// internal application metadata, not a wire field, so embedders can preserve
// injected environments without consulting the host process environment.
type PathSource string

const (
	PathDefault PathSource = "default"
	PathFlag    PathSource = "flag"
	PathEnv     PathSource = "environment"
)

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

func resolveGlobals(inv Invocation, env map[string]string, outputTerminal, errorTerminal bool) (Invocation, *Error) {
	config, state := defaultStatePaths(env)
	configSource, stateSource := PathDefault, PathDefault
	endpoint := "local"
	endpointSource := PathDefault
	if inv.Global.Endpoint != "" {
		endpoint = inv.Global.Endpoint
		endpointSource = PathFlag
	} else if value := strings.TrimSpace(env["MEKTUP_ENDPOINT"]); value != "" {
		endpoint = value
		endpointSource = PathEnv
	}
	if inv.Global.Config != "" {
		config = inv.Global.Config
		configSource = PathFlag
	} else if value := strings.TrimSpace(env["MEKTUP_CONFIG"]); value != "" {
		config = value
		configSource = PathEnv
	}
	if inv.Global.StateDir != "" {
		state = inv.Global.StateDir
		stateSource = PathFlag
	} else if value := strings.TrimSpace(env["MEKTUP_STATE_DIR"]); value != "" {
		state = value
		stateSource = PathEnv
	}
	output, err := resolvePresentation(inv.Global.JSON || inv.Global.Compact, inv.Global.Human, env)
	if err != nil {
		return inv, normalizeError(err)
	}
	color, colorErr := resolveColor(inv.Global.Color, env, output, outputTerminal)
	if colorErr != nil {
		return inv, colorErr
	}
	errorColor, colorErr := resolveColor(inv.Global.Color, env, output, errorTerminal)
	if colorErr != nil {
		return inv, colorErr
	}
	outputSetting := strings.ToLower(strings.TrimSpace(env["MEKTUP_OUTPUT"]))
	implicitAgentJSON := !inv.Global.JSON && !inv.Global.Human && !inv.Global.Compact &&
		(outputSetting == "" || outputSetting == "auto") &&
		(env["MEKTUP_AGENT"] == "1" || strings.TrimSpace(env["CODEX_THREAD_ID"]) != "")
	inv.Resolved = ResolvedGlobals{Output: output, Endpoint: endpoint, EndpointSource: endpointSource, Config: config, StateDir: state, ConfigSource: configSource, StateSource: stateSource, CodexHome: strings.TrimSpace(env["CODEX_HOME"]), CurrentThreadID: strings.TrimSpace(env["CODEX_THREAD_ID"]), AgentMode: env["MEKTUP_AGENT"] == "1", Compact: inv.Global.Compact || implicitAgentJSON, Color: color, ErrorColor: errorColor}
	return inv, nil
}

func resolveColor(explicit string, env map[string]string, output Presentation, terminal bool) (bool, *Error) {
	if output != PresentationHuman {
		return false, nil
	}
	choice := strings.ToLower(strings.TrimSpace(explicit))
	if choice == "" {
		choice = strings.ToLower(strings.TrimSpace(env["MEKTUP_COLOR"]))
	}
	if choice == "" {
		choice = "auto"
	}
	switch choice {
	case "always":
		return true, nil
	case "never":
		return false, nil
	case "auto":
		if _, disabled := env["NO_COLOR"]; disabled || strings.EqualFold(strings.TrimSpace(env["TERM"]), "dumb") {
			return false, nil
		}
		return terminal, nil
	default:
		return false, usageError("--color and MEKTUP_COLOR must be auto, always, or never")
	}
}

func writerIsTerminal(writer io.Writer) bool {
	file, ok := writer.(*os.File)
	if !ok {
		return false
	}
	fd := file.Fd()
	return isatty.IsTerminal(fd) || isatty.IsCygwinTerminal(fd)
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
	return a.RunContext(context.Background(), args)
}

// RunContext executes one invocation with caller-owned cancellation. Long
// waits and transport operations must observe this context; cancellation never
// invalidates an already accepted delivery.
func (a *App) RunContext(ctx context.Context, args []string) int {
	if ctx == nil {
		ctx = context.Background()
	}
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
	preJSON, preHuman, preCompact := scanPresentation(args)
	parsed, parseErr := Parse(args)
	presentation, presentationErr := resolvePresentation(preJSON || preCompact, preHuman, env)
	if presentationErr != nil {
		return a.finish(presentation, Invocation{}, presentationErr)
	}
	outputSetting := strings.ToLower(strings.TrimSpace(env["MEKTUP_OUTPUT"]))
	parsed.Resolved.Compact = preCompact && !preJSON && !preHuman ||
		!preJSON && !preHuman && !preCompact && (outputSetting == "" || outputSetting == "auto") &&
			(env["MEKTUP_AGENT"] == "1" || strings.TrimSpace(env["CODEX_THREAD_ID"]) != "")
	if parseErr != nil {
		return a.finish(presentation, parsed, parseErr)
	}
	if globalErr := validateGlobals(parsed); globalErr != nil {
		return a.finish(presentation, parsed, globalErr)
	}
	if parsed.Global.JSON || parsed.Global.Human || parsed.Global.Compact {
		presentation, presentationErr = resolvePresentation(parsed.Global.JSON || parsed.Global.Compact, parsed.Global.Human, env)
		if presentationErr != nil {
			return a.finish(presentation, parsed, presentationErr)
		}
	}
	resolved, resolveErr := resolveGlobals(parsed, env, writerIsTerminal(a.Out), writerIsTerminal(a.Err))
	if resolveErr != nil {
		return a.finish(presentation, parsed, resolveErr)
	}
	parsed = resolved
	if compactErr := resolveCompactInvocation(&parsed); compactErr != nil {
		return a.finish(presentation, parsed, compactErr)
	}
	applyCompactDefaults(&parsed)

	if has(parsed, "skill") {
		if parsed.Command != "--skill" || len(parsed.Position) != 0 || !onlyOptions(parsed, "skill", "json", "human", "color") {
			return a.finish(presentation, parsed, usageError("use mektup --skill without operational arguments"))
		}
		return a.writeGuide(parsed.Resolved.Color)
	}
	if parsed.Global.Help || parsed.Command == "" || parsed.Command == "help" {
		if parsed.Command == "help" && len(parsed.Position) > 1 {
			return a.finish(presentation, parsed, usageError("usage: mektup help [command]"))
		}
		topic := parsed.Position
		if parsed.Global.Help && parsed.Command != "" && parsed.Command != "help" {
			topic = []string{parsed.Command}
		}
		return a.help(presentation, topic, parsed.Resolved.Color, parsed.Resolved.ErrorColor)
	}
	switch parsed.Command {
	case "version":
		if len(parsed.Position) != 0 || !onlyOptions(parsed, "json", "human", "compact", "color") {
			return a.finish(presentation, parsed, usageError("usage: mektup version [--json]"))
		}
		return a.version(presentation, parsed.Resolved.Color)
	case "completion":
		if len(parsed.Position) != 1 || !onlyOptions(parsed, "json", "human", "color") {
			return a.finish(presentation, parsed, usageError("usage: mektup completion <zsh|bash|fish>"))
		}
		return a.completion(presentation, parsed.Position[0])
	case "docs":
		return a.docs(presentation, parsed)
	}

	if err := validateInvocation(a, parsed); err != nil {
		return a.finish(presentation, parsed, err)
	}
	if streaming, ok := a.Executor.(StreamingExecutor); ok {
		status := int(ExitSuccess)
		emitted := false
		state := &streamLifecycleState{}
		streamErr := streaming.ExecuteStream(ctx, parsed, func(result ExecutionResult) error {
			emitted = true
			status = a.writeExecutionResultState(presentation, parsed, result, state)
			if status != int(ExitSuccess) {
				return errors.New("streaming execution output failed")
			}
			return nil
		})
		if streamErr != nil {
			if status != int(ExitSuccess) {
				return status
			}
			if emitted && !state.terminal {
				return a.writeStreamingFailure(presentation, parsed, state, streamErr)
			}
			if !emitted {
				return a.finish(presentation, parsed, normalizeError(streamErr))
			}
			return int(normalizeError(streamErr).Exit)
		}
		if !emitted {
			return a.finish(presentation, parsed, &Error{Code: "internal_error", Message: "streaming executor returned no output", Effect: "unknown", Exit: ExitInternal})
		}
		if !state.terminal {
			return a.writeStreamingFailure(presentation, parsed, state, &Error{Code: "internal_error", Message: "streaming executor ended without a terminal event", Effect: "unknown", Exit: ExitInternal})
		}
		return status
	}
	result, err := a.Executor.Execute(ctx, parsed)
	if err == nil {
		return a.writeExecutionResult(presentation, parsed, result)
	}
	return a.finish(presentation, parsed, normalizeError(err))
}

func (a *App) help(p Presentation, position []string, color, errorColor bool) int {
	text := usageText
	if len(position) == 1 {
		if detail, ok := helpTopics[position[0]]; ok {
			text = detail
		} else {
			return a.finish(p, Invocation{Command: "help", Resolved: ResolvedGlobals{Color: color, ErrorColor: errorColor}}, usageError("unknown help topic: "+position[0]))
		}
	}
	if p == PresentationJSON {
		return a.writeJSON(map[string]any{"schema": "mektup/help/v1", "ok": true, "topic": "help", "text": text})
	}
	_, _ = io.WriteString(a.Out, ensureFinalNewline(StyleHuman(text, color)))
	return int(ExitSuccess)
}

func (a *App) version(p Presentation, color bool) int {
	if p == PresentationJSON {
		return a.writeJSON(map[string]any{"schema": "mektup/version/v1", "ok": true, "version": a.Build.Version, "commit": a.Build.Commit, "install_kind": a.Build.InstallKind, "contract_version": a.Build.ContractVersion, "tested_codex_versions": a.Build.TestedCodexServers})
	}
	line := fmt.Sprintf("mektup %s (commit %s, %s, contract %s; tested Codex %s)", a.Build.Version, a.Build.Commit, a.Build.InstallKind, a.Build.ContractVersion, strings.Join(a.Build.TestedCodexServers, ", "))
	_, _ = fmt.Fprintln(a.Out, StyleHuman(line, color))
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
		return a.finish(p, inv, usageError("usage: mektup docs agents|commands|envelopes|receipts [--json|--compact]"))
	}
	if !onlyOptions(inv, "json", "human", "compact", "color") {
		return a.finish(p, inv, usageError("docs accepts only a documentation topic and presentation options"))
	}
	switch inv.Position[0] {
	case "agents":
		if p == PresentationJSON {
			return a.writeJSON(map[string]any{"schema": "mektup/docs/v1", "ok": true, "topic": "agents", "text": agentGuide})
		}
		return a.writeGuide(inv.Resolved.Color)
	case "commands":
		if p == PresentationJSON {
			data, err := json.Marshal(commandContract())
			if err != nil {
				return a.finish(p, inv, &Error{Code: "internal_error", Message: err.Error(), Exit: ExitInternal})
			}
			_, _ = a.Out.Write(append(data, '\n'))
			return int(ExitSuccess)
		}
		_, _ = io.WriteString(a.Out, ensureFinalNewline(StyleHuman(commandContractHuman(commandContract()), inv.Resolved.Color)))
		return int(ExitSuccess)
	case "envelopes":
		if p == PresentationJSON {
			return a.writeJSON(map[string]any{"schema": "mektup/docs/v1", "ok": true, "topic": "envelopes", "text": envelopeDocs})
		}
		return a.writeAsset(envelopeDocs, inv.Resolved.Color)
	case "receipts":
		if p == PresentationJSON {
			return a.writeJSON(map[string]any{"schema": "mektup/docs/v1", "ok": true, "topic": "receipts", "text": receiptDocs})
		}
		return a.writeAsset(receiptDocs, inv.Resolved.Color)
	default:
		return a.finish(p, inv, usageError("unknown docs topic: "+inv.Position[0]))
	}
}

func (a *App) writeGuide(color bool) int {
	_, _ = io.WriteString(a.Out, ensureFinalNewline(StyleHuman(agentGuide, color)))
	return int(ExitSuccess)
}

func (a *App) writeAsset(asset string, color bool) int {
	_, _ = io.WriteString(a.Out, ensureFinalNewline(StyleHuman(asset, color)))
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
		_, _ = fmt.Fprintln(a.Err, StyleHuman("mektup: "+e.Message, inv.Resolved.ErrorColor))
	}
	return int(e.Exit)
}

func (a *App) writeExecutionResult(p Presentation, inv Invocation, result ExecutionResult) int {
	return a.writeExecutionResultState(p, inv, result, nil)
}

func (a *App) writeExecutionResultState(p Presentation, inv Invocation, result ExecutionResult, state *streamLifecycleState) int {
	if result.Exit == 0 {
		result.Exit = ExitSuccess
	}
	if len(result.RawJSON) > 0 {
		data := append(append([]byte(nil), result.RawJSON...), '\n')
		n, err := a.Out.Write(data)
		if err != nil || n != len(data) {
			return int(ExitInternal)
		}
		if state != nil {
			state.terminal = true
		}
		return int(result.Exit)
	}
	if p == PresentationJSON {
		invalidUTF8 := false
		if inv.Resolved.Compact {
			invalidUTF8 = containsInvalidUTF8(result.Receipt)
			for _, output := range result.Events {
				invalidUTF8 = invalidUTF8 || containsInvalidUTF8(output.Machine)
			}
		}
		events, err := a.lifecycleEventsState(inv, result, state)
		if err != nil {
			return a.internalFailure(err.Error())
		}
		recoveryReceipt := compactResultReceiptLocator(events)
		if len(recoveryReceipt) == 0 && state != nil {
			recoveryReceipt = compactReceiptValueLocator(state.lastReceipt)
		}
		for _, event := range events {
			invalidUTF8 = invalidUTF8 || containsInvalidUTF8(event)
			fullEvent := compactEventWithReceiptLocator(cloneEvent(event), recoveryReceipt)
			event = compactLifecycleEvent(inv, event)
			if inv.Resolved.Compact && invalidUTF8 {
				if status := a.writeJSON(compactDecodeFailureEvent(fullEvent)); status != int(ExitSuccess) {
					return status
				}
				return compactOverflowExit(fullEvent)
			}
			if status := a.writeProjectedEvent(inv, event, fullEvent); status != int(ExitSuccess) {
				return status
			}
		}
		return int(result.Exit)
	}

	hasOutput := strings.TrimSpace(result.Human) != ""
	if result.Human != "" {
		if _, err := io.WriteString(a.Out, ensureFinalNewline(StyleHuman(result.Human, inv.Resolved.Color))); err != nil {
			return int(ExitInternal)
		}
	}
	for _, event := range result.Events {
		if strings.TrimSpace(event.Human) != "" {
			hasOutput = true
			if _, err := io.WriteString(a.Out, ensureFinalNewline(StyleHuman(event.Human, inv.Resolved.Color))); err != nil {
				return int(ExitInternal)
			}
		}
	}
	for _, warning := range humanWarnings(result) {
		hasOutput = true
		if _, err := io.WriteString(a.Out, ensureFinalNewline(StyleHuman("warning: "+warning, inv.Resolved.Color))); err != nil {
			return int(ExitInternal)
		}
	}
	if result.Receipt != nil {
		hasOutput = true
		if _, err := io.WriteString(a.Out, ensureFinalNewline(StyleHuman(receiptHuman(result.Receipt), inv.Resolved.Color))); err != nil {
			return a.internalFailure("unable to write receipt: " + err.Error())
		}
	}
	if !hasOutput {
		return a.finish(p, inv, &Error{Code: "internal_error", Message: "operational command returned no human output", Exit: ExitInternal})
	}
	if state != nil {
		if result.Receipt != nil {
			state.lastReceipt = result.Receipt
		}
		if resultHasTerminal(result) {
			state.terminal = true
		}
	}
	return int(result.Exit)
}

func resultHasTerminal(result ExecutionResult) bool {
	for _, event := range result.Events {
		encoded, err := json.Marshal(event.Machine)
		if err != nil {
			continue
		}
		var object map[string]any
		if json.Unmarshal(encoded, &object) == nil {
			if terminal, ok := object["terminal"].(bool); ok && terminal {
				return true
			}
		}
	}
	return false
}

// lifecycleEvents makes the executor boundary safe even while the domain
// packages are being integrated. A receipt is data on the terminal lifecycle
// event, never a second bare JSON line after that event.
func (a *App) lifecycleEvents(inv Invocation, result ExecutionResult) ([]map[string]any, error) {
	return a.lifecycleEventsState(inv, result, nil)
}

type streamLifecycleState struct {
	operationID  string
	nextSequence int
	terminal     bool
	lastReceipt  any
}

func (a *App) lifecycleEventsState(inv Invocation, result ExecutionResult, state *streamLifecycleState) ([]map[string]any, error) {
	localState := state
	if localState == nil {
		localState = &streamLifecycleState{}
	}
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
		event, err := decodeExactObject(encoded)
		if err != nil {
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
	operationID := localState.operationID
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
	localState.operationID = operationID
	if localState.terminal {
		return nil, errors.New("streaming executor emitted output after a terminal event")
	}
	startSequence := localState.nextSequence
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
		event["sequence"] = startSequence + index + 1
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
		localState.lastReceipt = result.Receipt
		if !result.Streaming {
			last["terminal"] = true
		}
		if syntheticTerminal && !result.Streaming {
			last["event"] = "operation.completed"
		}
	}
	if !result.Streaming {
		if terminal, _ := last["terminal"].(bool); !terminal {
			last["terminal"] = true
		}
	}
	localState.nextSequence += len(events)
	if terminal, _ := last["terminal"].(bool); terminal {
		localState.terminal = true
	}
	return events, nil
}

func (a *App) writeStreamingFailure(p Presentation, inv Invocation, state *streamLifecycleState, err error) int {
	if p != PresentationJSON {
		e := normalizeError(err)
		if e.Message != "" {
			_, _ = fmt.Fprintln(a.Err, StyleHuman("mektup: "+e.Message, inv.Resolved.ErrorColor))
		}
		return int(e.Exit)
	}
	if state == nil || state.terminal {
		return int(normalizeError(err).Exit)
	}
	e := normalizeError(err)
	details := e.Details
	if details == nil {
		details = map[string]any{}
	}
	data := map[string]any{"error": map[string]any{"code": e.Code, "message": e.Message, "retryable": e.Retryable, "effectState": e.Effect, "details": details}}
	if state.lastReceipt != nil {
		data["receipt"] = state.lastReceipt
	}
	result := ExecutionResult{Events: []OutputEvent{{Machine: map[string]any{"schema": EventSchema, "event": "operation.failed", "terminal": true, "ok": false, "operationId": state.operationID, "data": data}}}, Exit: e.Exit, Streaming: true}
	return a.writeExecutionResultState(p, inv, result, state)
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
	parts := make([]string, 0, 6)
	if operation, ok := object["operation"].(string); ok && operation != "" {
		parts = append(parts, operation)
	} else {
		parts = append(parts, "receipt")
	}
	if state, ok := object["state"].(string); ok && state != "" {
		parts = append(parts, string(state))
	}
	if message, ok := object["message"].(map[string]any); ok {
		if id, ok := message["messageId"].(string); ok && id != "" {
			parts = append(parts, "message="+id)
		}
	}
	if target, ok := object["target"].(map[string]any); ok {
		alias, _ := target["alias"].(string)
		thread, _ := target["threadId"].(string)
		if alias != "" || thread != "" {
			parts = append(parts, "target="+strings.Trim(alias+"/"+thread, "/"))
		}
	}
	if id, ok := object["receiptId"].(string); ok && id != "" {
		parts = append(parts, "receipt="+id)
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
	event = eventWithCommand(event, inv)
	invalidUTF8 := containsInvalidUTF8(event)
	fullEvent := cloneEvent(event)
	if inv.Resolved.Compact {
		if invalidUTF8 {
			if status := a.writeJSON(compactDecodeFailureEvent(fullEvent)); status != int(ExitSuccess) {
				return status
			}
			return compactOverflowExit(fullEvent)
		}
		event["presentation"] = "compact"
		data := objectMap(event["data"])
		errorData := objectMap(data["error"])
		messagePreview := previewValue(errorData["message"])
		errorData["messagePreview"] = messagePreview
		errorData["message"] = objectMap(messagePreview)["text"]
		if details := objectMap(errorData["details"]); len(details) != 0 {
			errorData["details"] = compactErrorDetails(details)
		}
		data["error"] = errorData
		event["data"] = data
	}
	if status := a.writeProjectedEvent(inv, event, fullEvent); status != int(ExitSuccess) {
		return status
	}
	return int(e.Exit)
}

func containsInvalidUTF8(value any) bool {
	return containsInvalidUTF8Value(reflect.ValueOf(value), make(map[uintptr]bool), 0)
}

func containsInvalidUTF8Value(value reflect.Value, seen map[uintptr]bool, depth int) bool {
	if !value.IsValid() || depth > 64 {
		return false
	}
	if value.Kind() == reflect.Interface {
		if value.IsNil() {
			return false
		}
		return containsInvalidUTF8Value(value.Elem(), seen, depth+1)
	}
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return false
		}
		ptr := value.Pointer()
		if seen[ptr] {
			return false
		}
		seen[ptr] = true
		return containsInvalidUTF8Value(value.Elem(), seen, depth+1)
	}
	if value.Type() == reflect.TypeOf(json.RawMessage{}) {
		return !utf8.Valid(value.Bytes())
	}
	switch value.Kind() {
	case reflect.String:
		return !utf8.ValidString(value.String())
	case reflect.Map:
		iter := value.MapRange()
		for iter.Next() {
			if containsInvalidUTF8Value(iter.Key(), seen, depth+1) || containsInvalidUTF8Value(iter.Value(), seen, depth+1) {
				return true
			}
		}
	case reflect.Slice, reflect.Array:
		if value.Type().Elem().Kind() == reflect.Uint8 {
			return false // Byte blobs are not textual previews; RawMessage was handled above.
		}
		for i := 0; i < value.Len(); i++ {
			if containsInvalidUTF8Value(value.Index(i), seen, depth+1) {
				return true
			}
		}
	case reflect.Struct:
		for i := 0; i < value.NumField(); i++ {
			field := value.Field(i)
			if field.CanInterface() && containsInvalidUTF8Value(field, seen, depth+1) {
				return true
			}
		}
	}
	return false
}

func decodeExactObject(document []byte) (map[string]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(document))
	decoder.UseNumber()
	var value map[string]any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return value, nil
}

func compactDecodeFailureEvent(original map[string]any) map[string]any {
	event := pick(original, "schema", "eventId", "sequence", "operationId", "timestamp")
	event["event"] = "compact.decode_failed"
	event["terminal"] = true
	event["ok"] = false
	event["presentation"] = "compact"
	event["warnings"] = []any{}
	details := map[string]any{"guidance": []string{"use --json for exact machine diagnostics"}}
	if receipt := compactReceiptLocator(original); len(receipt) != 0 {
		details["receipt"] = receipt
	}
	event["data"] = map[string]any{"error": map[string]any{
		"code": "invalid_utf8", "message": "compact textual input contains invalid UTF-8", "messagePreview": previewValue("compact textual input contains invalid UTF-8"), "retryable": false,
		"effectState": compactEffectState(original), "details": details,
	}}
	return event
}

func compactResultReceiptLocator(events []map[string]any) map[string]any {
	for index := len(events) - 1; index >= 0; index-- {
		if receipt := compactReceiptLocator(events[index]); len(receipt) != 0 {
			return receipt
		}
	}
	return nil
}

func compactReceiptValueLocator(value any) map[string]any {
	return pick(objectMap(value), "receiptId", "operationId", "state")
}

func compactEventWithReceiptLocator(event, receipt map[string]any) map[string]any {
	if len(receipt) == 0 || len(compactReceiptLocator(event)) != 0 {
		return event
	}
	recovered := cloneEvent(event)
	data := objectMap(recovered["data"])
	data["receipt"] = receipt
	recovered["data"] = data
	return recovered
}

func compactErrorDetails(details map[string]any) map[string]any {
	return compactDetailMap(details)
}

func compactDetailMap(details map[string]any) map[string]any {
	keys := make([]string, 0, len(details))
	for key := range details {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	truncated := len(keys) > CompactMaxLimit
	if truncated {
		keys = keys[:CompactMaxLimit]
	}
	out := make(map[string]any, len(keys)+2)
	for _, key := range keys {
		out[key] = compactDetailValue(details[key])
	}
	if truncated {
		out["projection"] = "bounded-details"
		out["sourceCount"] = len(details)
		out["truncated"] = true
	}
	return out
}

func compactDetailValue(value any) any {
	switch typed := value.(type) {
	case string:
		return previewValue(typed)
	case map[string]any:
		return compactDetailMap(typed)
	case []any:
		limit := len(typed)
		if limit > CompactMaxLimit {
			limit = CompactMaxLimit
		}
		items := make([]any, 0, limit)
		for _, child := range typed[:limit] {
			items = append(items, compactDetailValue(child))
		}
		if limit != len(typed) {
			return map[string]any{"items": items, "sourceCount": len(typed), "truncated": true}
		}
		return items
	default:
		return typed
	}
}

func cloneEvent(event map[string]any) map[string]any {
	document, err := json.Marshal(event)
	if err != nil {
		return event
	}
	cloned, decodeErr := decodeExactObject(document)
	if decodeErr != nil {
		return event
	}
	return cloned
}

func (a *App) writeProjectedEvent(inv Invocation, event, fullEvent map[string]any) int {
	if !inv.Resolved.Compact {
		return a.writeJSON(event)
	}
	fullDocument, marshalErr := json.Marshal(fullEvent)
	recoveryEvent := compactEventWithReceiptLocator(event, compactReceiptLocator(fullEvent))
	var retained *CompactArtifact
	var retainErr error
	retain := func() {
		if retained != nil || retainErr != nil || marshalErr != nil {
			return
		}
		if retainer, ok := a.Executor.(CompactRetainer); ok {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			artifact, err := retainer.RetainCompact(ctx, inv, append(fullDocument, '\n'))
			cancel()
			if err == nil && validCompactArtifact(artifact, append(fullDocument, '\n')) {
				retained = &artifact
			} else {
				if err != nil {
					retainErr = err
				} else {
					retainErr = errors.New("compact artifact retention returned an invalid receipt")
				}
			}
		} else {
			retainErr = errors.New("compact artifact retention is unavailable")
		}
	}
	if compactDetailsNeedRetention(event) {
		retain()
		if retained == nil {
			fallback := compactOverflowEvent(recoveryEvent, nil, marshalErr, retainErr)
			if status := a.writeJSON(fallback); status != int(ExitSuccess) {
				return status
			}
			return compactOverflowExit(recoveryEvent)
		}
		data := objectMap(event["data"])
		data["retainedDetails"] = retained
		event["data"] = data
	}
	if compactEncodedSize(event) <= CompactMaxOutputBytes {
		return a.writeJSON(event)
	}
	retain()
	fallback := compactOverflowEvent(recoveryEvent, retained, marshalErr, retainErr)
	if compactEncodedSize(fallback) > CompactMaxOutputBytes {
		fallback = minimalCompactOverflowEvent(recoveryEvent, retained)
	}
	if status := a.writeJSON(fallback); status != int(ExitSuccess) {
		return status
	}
	return compactOverflowExit(recoveryEvent)
}

func validCompactArtifact(artifact CompactArtifact, document []byte) bool {
	if strings.TrimSpace(artifact.Path) == "" || artifact.Bytes != int64(len(document)) {
		return false
	}
	digest := sha256.Sum256(document)
	return artifact.SHA256 == "sha256:"+hex.EncodeToString(digest[:])
}

func compactOverflowExit(event map[string]any) int {
	switch compactEffectState(event) {
	case "not_sent", "rejected":
		return int(ExitRejected)
	case "unknown", "outcome_unknown", "reply_outcome_unknown":
		return int(ExitUnknown)
	default:
		return int(ExitIncomplete)
	}
}

func compactDetailsNeedRetention(event map[string]any) bool {
	if event["warningsTruncated"] == true {
		return true
	}
	for _, warning := range anySlice(event["warnings"]) {
		if preview, ok := objectMap(warning)["messagePreview"]; ok && objectMap(preview)["truncated"] == true {
			return true
		}
	}
	data := objectMap(event["data"])
	errorData := objectMap(data["error"])
	if objectMap(errorData["messagePreview"])["truncated"] == true {
		return true
	}
	return containsTruncatedPreview(errorData["details"])
}

func containsTruncatedPreview(value any) bool {
	switch typed := value.(type) {
	case map[string]any:
		if typed["truncated"] == true {
			return true
		}
		for _, child := range typed {
			if containsTruncatedPreview(child) {
				return true
			}
		}
	case []any:
		for _, child := range typed {
			if containsTruncatedPreview(child) {
				return true
			}
		}
	}
	return false
}

func compactOverflowEvent(original map[string]any, artifact *CompactArtifact, marshalErr, retainErr error) map[string]any {
	fallback := pick(original, "schema", "eventId", "sequence", "operationId", "timestamp")
	fallback["event"] = "compact_output_too_large"
	fallback["terminal"] = true
	fallback["ok"] = false
	fallback["presentation"] = "compact"
	fallback["warnings"] = []any{}
	effect := compactEffectState(original)
	receipt := compactReceiptLocator(original)
	guidance := []string{"rerun this read with --json or a smaller limit", "use the exact content or artifact command"}
	if len(receipt) != 0 {
		guidance = []string{"retrieve the accepted operation with receipt show --json", "use the retained artifact or exact content command; do not rerun the mutation"}
	}
	details := map[string]any{"guidance": guidance}
	if len(receipt) != 0 {
		details["receipt"] = receipt
	}
	if artifact != nil {
		details["artifact"] = artifact
	} else {
		failure := "compact artifact retention is unavailable"
		if marshalErr != nil {
			failure = "complete output could not be encoded for retention"
		} else if retainErr != nil {
			failure = "compact artifact retention failed"
		}
		details["retentionFailure"] = map[string]any{"message": failure}
	}
	fallback["data"] = map[string]any{"error": map[string]any{
		"code": "compact_output_too_large", "message": "compact output exceeded 131072 bytes", "messagePreview": previewValue("compact output exceeded 131072 bytes"), "retryable": false, "effectState": effect, "details": details,
	}}
	return fallback
}

func minimalCompactOverflowEvent(original map[string]any, artifact *CompactArtifact) map[string]any {
	fallback := pick(original, "schema", "eventId", "sequence", "operationId", "timestamp")
	fallback["event"] = "compact_output_too_large"
	fallback["terminal"] = true
	fallback["ok"] = false
	fallback["presentation"] = "compact"
	fallback["warnings"] = []any{}
	details := map[string]any{}
	if artifact != nil {
		details["artifact"] = artifact
	} else {
		details["retentionFailure"] = map[string]any{"message": "compact artifact retention failed"}
	}
	if receipt := compactReceiptLocator(original); len(receipt) != 0 {
		details["receipt"] = receipt
	}
	fallback["data"] = map[string]any{"error": map[string]any{
		"code": "compact_output_too_large", "message": "compact output exceeded 131072 bytes", "messagePreview": previewValue("compact output exceeded 131072 bytes"), "retryable": false,
		"effectState": compactEffectState(original), "details": details,
	}}
	return fallback
}

func compactEffectState(event map[string]any) string {
	if data := objectMap(event["data"]); len(data) != 0 {
		if failure := objectMap(data["error"]); len(failure) != 0 {
			if effect, ok := failure["effectState"].(string); ok && effect != "" {
				return effect
			}
		}
		if receipt := objectMap(data["receipt"]); len(receipt) != 0 {
			if state, ok := receipt["state"].(string); ok && state != "" {
				return state
			}
		}
	}
	return "accepted"
}

func compactReceiptLocator(event map[string]any) map[string]any {
	data := objectMap(event["data"])
	receipt := objectMap(data["receipt"])
	return pick(receipt, "receiptId", "operationId", "state")
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
	case "target_ambiguous", "message_not_found", "message_identity_conflict", "message_not_addressed_to_thread", "delivery_rejected", "delivery_temporarily_unavailable", "resolver_unavailable", "route_unavailable", "reply_route_unavailable", "endpoint_unavailable", "unsupported_server_version", "input_too_large", "invalid_utf8", "reply_not_requested", "content_unavailable", "output_too_large", "experimental_method_unavailable":
		return ExitRejected
	case "outcome_unknown", "reply_outcome_unknown", "storage_busy":
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

func scanPresentation(args []string) (jsonMode, humanMode, compactMode bool) {
	for _, arg := range args {
		if arg == "--" {
			break
		}
		switch arg {
		case "--json":
			jsonMode = true
		case "--human":
			humanMode = true
		case "--compact":
			compactMode = true
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
