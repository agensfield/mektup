package endpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/agensfield/mektup/go/internal/sshproxy"
)

var (
	ErrResolverUnavailable = errors.New("Herdr resolver is unavailable")
	ErrResolverAmbiguous   = errors.New("Herdr selector matched multiple live agents")
	ErrResolverStale       = errors.New("Herdr mapping is stale or changed")
	ErrResolverNotFound    = errors.New("Herdr selector matched no live agent")
)

// CommandRunner is a narrow seam for testing resolver behavior without
// starting Herdr. argv includes the executable and is passed directly to
// exec.Command, never through a shell.
type CommandRunner interface {
	Run(context.Context, []string) ([]byte, error)
}

// EndpointCommandRunner is the transport boundary for endpoint-specific
// Herdr lookups. A caller may provide a remote one-shot implementation for an
// SSH route; this package only supplies the fixed argv and never builds a
// shell command or transports bytes itself.
type EndpointCommandRunner interface {
	RunEndpoint(context.Context, Endpoint, []string) ([]byte, error)
}

type EndpointCommandRunnerFunc func(context.Context, Endpoint, []string) ([]byte, error)

func (f EndpointCommandRunnerFunc) RunEndpoint(ctx context.Context, endpoint Endpoint, argv []string) ([]byte, error) {
	return f(ctx, endpoint, argv)
}

// ExecRunner is the bounded local Herdr command adapter. It accepts only the
// fixed argv forms emitted by HerdrResolver and never invokes a shell. The
// optional factory exists for deterministic lifecycle tests; production uses
// the system executable resolved by LookPath.
type ExecRunner struct {
	Factory        sshproxy.ProcessFactory
	StartupTimeout time.Duration
	CommandTimeout time.Duration
	CleanupTimeout time.Duration
	OutputLimit    int64
	StderrLimit    int64
}

func (r ExecRunner) Run(ctx context.Context, argv []string) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, &HerdrRunnerFailure{Kind: HerdrRunnerCanceled, Err: err}
	}
	remote, err := validateHerdrCommand(argv)
	if err != nil {
		return nil, err
	}
	return runLocalHerdr(ctx, r, remote)
}

func runLocalHerdr(ctx context.Context, runner ExecRunner, argv []string) ([]byte, error) {
	cfg := (HerdrRunnerConfig{
		StartupTimeout: runner.StartupTimeout, CommandTimeout: runner.CommandTimeout,
		CleanupTimeout: runner.CleanupTimeout, OutputLimit: runner.OutputLimit, StderrLimit: runner.StderrLimit,
	}).normalized()
	factory := runner.Factory
	if factory == nil {
		path, err := exec.LookPath(argv[0])
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrResolverUnavailable, err)
		}
		argv = append([]string{path}, argv[1:]...)
		factory = sshproxy.DefaultProcessFactory()
	}
	process, err := factory.New(append([]string(nil), argv...))
	if err != nil {
		return nil, &HerdrRunnerFailure{Kind: HerdrRunnerSpawnFailure, Err: err}
	}
	if process == nil {
		return nil, &HerdrRunnerFailure{Kind: HerdrRunnerSpawnFailure, Err: errors.New("process factory returned nil process")}
	}
	stdin, err := process.StdinPipe()
	if err != nil {
		return cleanupHerdrProcess(process, nil, nil, nil, cfg.CleanupTimeout, &HerdrRunnerFailure{Kind: HerdrRunnerSpawnFailure, Err: err})
	}
	stdout, err := process.StdoutPipe()
	if err != nil {
		return cleanupHerdrProcess(process, stdin, nil, nil, cfg.CleanupTimeout, &HerdrRunnerFailure{Kind: HerdrRunnerSpawnFailure, Err: err})
	}
	stderr, err := process.StderrPipe()
	if err != nil {
		return cleanupHerdrProcess(process, stdin, stdout, nil, cfg.CleanupTimeout, &HerdrRunnerFailure{Kind: HerdrRunnerSpawnFailure, Err: err})
	}
	return runHerdrProcess(ctx, process, stdin, stdout, stderr, cfg)
}

type HerdrResolver struct {
	Runner         CommandRunner
	EndpointRunner EndpointCommandRunner
}

func NewHerdrResolver(runner CommandRunner) *HerdrResolver {
	if runner == nil {
		runner = ExecRunner{}
	}
	return &HerdrResolver{Runner: runner}
}

type HerdrResolution struct {
	Requested Target `json:"requested"`
	Name      string `json:"name"`
	Workspace string `json:"workspaceId"`
	Tab       string `json:"tabId"`
	Pane      string `json:"paneId"`
	ThreadID  string `json:"codexThreadId"`
	Status    string `json:"status"`
}

type herdrAgent struct {
	Name        string `json:"name"`
	PaneID      string `json:"pane_id"`
	WorkspaceID string `json:"workspace_id"`
	TabID       string `json:"tab_id"`
	Status      string `json:"agent_status"`
	Agent       string `json:"agent"`
	Session     struct {
		Agent  string `json:"agent"`
		Kind   string `json:"kind"`
		Source string `json:"source"`
		Value  string `json:"value"`
	} `json:"agent_session"`
}

func (r *HerdrResolver) Resolve(ctx context.Context, target Target) (HerdrResolution, error) {
	return r.resolve(ctx, target, func(runCtx context.Context, argv []string) ([]byte, error) {
		return r.Runner.Run(runCtx, argv)
	})
}

// ResolveEndpoint binds a resolver query to the selected endpoint. SSH routes
// require an explicitly supplied endpoint runner, while direct local routes
// may use the installed herdr executable through Runner.
func (r *HerdrResolver) ResolveEndpoint(ctx context.Context, endpoint Endpoint, target Target) (HerdrResolution, error) {
	run, err := r.endpointRunner(endpoint)
	if err != nil {
		return HerdrResolution{}, err
	}
	return r.resolve(ctx, target, run)
}

func (r *HerdrResolver) endpointRunner(endpoint Endpoint) (func(context.Context, []string) ([]byte, error), error) {
	if r == nil {
		return nil, ErrResolverUnavailable
	}
	if endpoint.Route.Kind == RouteSSH {
		if r.EndpointRunner == nil {
			return nil, fmt.Errorf("%w: SSH endpoint needs an endpoint-specific Herdr runner", ErrResolverUnavailable)
		}
		return func(runCtx context.Context, argv []string) ([]byte, error) {
			return r.EndpointRunner.RunEndpoint(runCtx, endpoint, argv)
		}, nil
	}
	if r.Runner == nil {
		return nil, ErrResolverUnavailable
	}
	return func(runCtx context.Context, argv []string) ([]byte, error) {
		return r.Runner.Run(runCtx, argv)
	}, nil
}

// ResolveThreadEndpoint performs reverse source-provenance discovery after the
// Codex endpoint and thread have already been pinned. Official Herdr Codex
// integrations and herdr-codex-bridge both publish the same native
// agent_session tuple, so no ambient pane variables or bridge-specific files
// participate in this lookup.
func (r *HerdrResolver) ResolveThreadEndpoint(ctx context.Context, endpoint Endpoint, codexThreadID string) (HerdrResolution, error) {
	if strings.TrimSpace(codexThreadID) == "" {
		return HerdrResolution{}, ErrResolverNotFound
	}
	run, err := r.endpointRunner(endpoint)
	if err != nil {
		return HerdrResolution{}, err
	}
	if ctx == nil {
		ctx = context.Background()
	}
	list, err := run(ctx, []string{"herdr", "agent", "list"})
	if err != nil {
		return HerdrResolution{}, err
	}
	candidate, err := uniqueThreadAgent(list, codexThreadID)
	if err != nil {
		return HerdrResolution{}, err
	}
	get, err := run(ctx, []string{"herdr", "agent", "get", candidate.PaneID})
	if err != nil {
		return HerdrResolution{}, err
	}
	confirmed, err := parseGetAgent(get)
	if err != nil || validateThreadAgent(confirmed, codexThreadID) != nil || !sameNativeSession(candidate, confirmed) {
		return HerdrResolution{}, ErrResolverStale
	}
	finalList, err := run(ctx, []string{"herdr", "agent", "list"})
	if err != nil {
		return HerdrResolution{}, err
	}
	final, err := uniqueThreadAgent(finalList, codexThreadID)
	if err != nil || !sameNativeSession(confirmed, final) {
		return HerdrResolution{}, ErrResolverStale
	}
	return HerdrResolution{
		Name: final.Name, Workspace: final.WorkspaceID, Tab: final.TabID,
		Pane: final.PaneID, ThreadID: final.Session.Value, Status: final.Status,
	}, nil
}

func (r *HerdrResolver) resolve(ctx context.Context, target Target, run func(context.Context, []string) ([]byte, error)) (HerdrResolution, error) {
	if r == nil || r.Runner == nil {
		if r == nil || r.EndpointRunner == nil {
			return HerdrResolution{}, ErrResolverUnavailable
		}
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if target.Kind == TargetBare {
		target.Kind = TargetAgent
	}
	if target.Kind != TargetAgent && target.Kind != TargetPane {
		return HerdrResolution{}, fmt.Errorf("%w: target is not a Herdr selector", ErrInvalidTarget)
	}
	list, err := run(ctx, []string{"herdr", "agent", "list"})
	if err != nil {
		return HerdrResolution{}, err
	}
	agents, err := parseAgents(list)
	if err != nil {
		return HerdrResolution{}, fmt.Errorf("%w: invalid herdr agent list: %v", ErrResolverUnavailable, err)
	}
	matches := make([]herdrAgent, 0, 1)
	for _, agent := range agents {
		if (target.Kind == TargetAgent && agent.Name == target.Name) || (target.Kind == TargetPane && agent.PaneID == target.PaneID) {
			matches = append(matches, agent)
		}
	}
	if len(matches) == 0 {
		return HerdrResolution{}, ErrResolverNotFound
	}
	if len(matches) != 1 {
		return HerdrResolution{}, ErrResolverAmbiguous
	}
	candidate := matches[0]
	if err := validateLive(candidate); err != nil {
		return HerdrResolution{}, err
	}
	get, err := run(ctx, []string{"herdr", "agent", "get", candidate.PaneID})
	if err != nil {
		return HerdrResolution{}, err
	}
	confirmed, err := parseGetAgent(get)
	if err != nil {
		return HerdrResolution{}, fmt.Errorf("%w: invalid herdr agent get: %v", ErrResolverUnavailable, err)
	}
	if err := validateLive(confirmed); err != nil {
		return HerdrResolution{}, err
	}
	if confirmed.Name != candidate.Name || confirmed.PaneID != candidate.PaneID || confirmed.Session.Value != candidate.Session.Value {
		return HerdrResolution{}, ErrResolverStale
	}
	return HerdrResolution{
		Requested: target,
		Name:      confirmed.Name, Workspace: confirmed.WorkspaceID, Tab: confirmed.TabID,
		Pane: confirmed.PaneID, ThreadID: confirmed.Session.Value, Status: confirmed.Status,
	}, nil
}

func validateLive(agent herdrAgent) error {
	if agent.Agent != "codex" || agent.Name == "" || agent.PaneID == "" || !fullyQualifiedPane(agent.PaneID) ||
		agent.WorkspaceID == "" || agent.TabID == "" || agent.Session.Kind != "id" ||
		agent.Session.Source != "herdr:codex" || agent.Session.Value == "" {
		return ErrResolverStale
	}
	switch strings.ToLower(agent.Status) {
	case "idle", "working", "blocked", "done", "unknown":
		return nil
	default:
		return ErrResolverStale
	}
}

func validateThreadAgent(agent herdrAgent, threadID string) error {
	if agent.Agent != "codex" || agent.Session.Agent != "codex" || agent.PaneID == "" || !fullyQualifiedPane(agent.PaneID) ||
		agent.WorkspaceID == "" || agent.TabID == "" || agent.Session.Kind != "id" ||
		agent.Session.Source != "herdr:codex" || agent.Session.Value != threadID {
		return ErrResolverStale
	}
	switch strings.ToLower(agent.Status) {
	case "idle", "working", "blocked", "done", "unknown":
		return nil
	default:
		return ErrResolverStale
	}
}

func uniqueThreadAgent(data []byte, threadID string) (herdrAgent, error) {
	agents, err := parseAgents(data)
	if err != nil {
		return herdrAgent{}, fmt.Errorf("%w: invalid herdr agent list: %v", ErrResolverUnavailable, err)
	}
	matches := make([]herdrAgent, 0, 1)
	for _, agent := range agents {
		if agent.Session.Value != threadID {
			continue
		}
		if err := validateThreadAgent(agent, threadID); err != nil {
			return herdrAgent{}, err
		}
		matches = append(matches, agent)
	}
	if len(matches) == 0 {
		return herdrAgent{}, ErrResolverNotFound
	}
	if len(matches) != 1 {
		return herdrAgent{}, ErrResolverAmbiguous
	}
	return matches[0], nil
}

func sameNativeSession(a, b herdrAgent) bool {
	return a.Agent == b.Agent && a.Session.Agent == b.Session.Agent && a.Session.Kind == b.Session.Kind &&
		a.Session.Source == b.Session.Source && a.Session.Value == b.Session.Value &&
		a.PaneID == b.PaneID && a.WorkspaceID == b.WorkspaceID && a.TabID == b.TabID
}

func parseAgents(data []byte) ([]herdrAgent, error) {
	var envelope struct {
		Result struct {
			Agents []herdrAgent `json:"agents"`
		} `json:"result"`
		Agents []herdrAgent `json:"agents"`
	}
	if err := json.Unmarshal(data, &envelope); err == nil {
		if envelope.Result.Agents != nil {
			return envelope.Result.Agents, nil
		}
		if envelope.Agents != nil {
			return envelope.Agents, nil
		}
	}
	var agents []herdrAgent
	if err := json.Unmarshal(data, &agents); err != nil {
		return nil, err
	}
	return agents, nil
}

func parseGetAgent(data []byte) (herdrAgent, error) {
	var envelope struct {
		Result struct {
			Agent herdrAgent `json:"agent"`
		} `json:"result"`
		Agent herdrAgent `json:"agent"`
	}
	if err := json.Unmarshal(data, &envelope); err != nil {
		return herdrAgent{}, err
	}
	if envelope.Result.Agent.PaneID != "" {
		return envelope.Result.Agent, nil
	}
	if envelope.Agent.PaneID != "" {
		return envelope.Agent, nil
	}
	return herdrAgent{}, errors.New("missing agent")
}
