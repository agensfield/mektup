package endpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
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

type ExecRunner struct{}

func (ExecRunner) Run(ctx context.Context, argv []string) ([]byte, error) {
	if len(argv) == 0 || argv[0] == "" {
		return nil, errors.New("empty command argv")
	}
	path, err := exec.LookPath(argv[0])
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrResolverUnavailable, err)
	}
	cmd := exec.CommandContext(ctx, path, argv[1:]...)
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("%w: herdr agent query failed: %v", ErrResolverUnavailable, err)
	}
	return out, nil
}

type HerdrResolver struct {
	Runner CommandRunner
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
		Value string `json:"value"`
	} `json:"agent_session"`
}

func (r *HerdrResolver) Resolve(ctx context.Context, target Target) (HerdrResolution, error) {
	if r == nil || r.Runner == nil {
		return HerdrResolution{}, ErrResolverUnavailable
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
	list, err := r.Runner.Run(ctx, []string{"herdr", "agent", "list"})
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
	get, err := r.Runner.Run(ctx, []string{"herdr", "agent", "get", candidate.PaneID})
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
	if agent.Name == "" || agent.PaneID == "" || !fullyQualifiedPane(agent.PaneID) ||
		agent.WorkspaceID == "" || agent.TabID == "" || agent.Session.Value == "" {
		return ErrResolverStale
	}
	switch strings.ToLower(agent.Status) {
	case "idle", "working", "blocked", "done", "unknown":
		return nil
	default:
		return ErrResolverStale
	}
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
	if envelope.Result.Agent.Name != "" {
		return envelope.Result.Agent, nil
	}
	if envelope.Agent.Name != "" {
		return envelope.Agent, nil
	}
	return herdrAgent{}, errors.New("missing agent")
}
