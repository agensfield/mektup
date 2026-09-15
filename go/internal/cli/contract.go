package cli

type commandContractDocument struct {
	Schema          string            `json:"schema"`
	Contract        string            `json:"contract_version"`
	Commands        []commandMetadata `json:"commands"`
	Presentation    []string          `json:"presentation"`
	ExitClasses     map[string]int    `json:"exit_classes"`
	OfflineSurfaces []string          `json:"offline_surfaces"`
}

type commandMetadata struct {
	Name         string   `json:"name"`
	Usage        string   `json:"usage"`
	Effects      []string `json:"effects"`
	RetrySafety  string   `json:"retry_safety"`
	Availability string   `json:"availability"`
	Result       string   `json:"result"`
	Receipt      string   `json:"receipt"`
	HumanGate    bool     `json:"human_gate"`
}

func commandContract() commandContractDocument {
	return commandContractDocument{
		Schema:   CommandSchema,
		Contract: ContractVersion,
		Presentation: []string{
			"explicit --json/--human",
			"MEKTUP_AGENT=1",
			"nonempty CODEX_THREAD_ID",
			"interactive human default",
		},
		ExitClasses: map[string]int{
			"success": 0, "internal": 1, "usage": 2, "rejected": 3, "unknown": 4, "incomplete": 5,
		},
		OfflineSurfaces: []string{"help", "version", "completion", "--skill", "docs agents", "docs commands", "docs envelopes", "docs receipts"},
		Commands: []commandMetadata{
			{Name: "send", Usage: "mektup send <target> [message|--stdin|--file <path>] [--request-reply] [--wait]", Effects: []string{"network-read", "thread-write"}, RetrySafety: "classified-temporary-rejection-only", Availability: "endpoint and target", Result: "send.accepted then optional reply event", Receipt: "required", HumanGate: false},
			{Name: "reply", Usage: "mektup reply <message-or-receipt-id> [message|--stdin|--file <path>] [--wait]", Effects: []string{"network-read", "thread-write"}, RetrySafety: "same-id idempotent; unknown never resent", Availability: "pinned source endpoint and custody", Result: "reply.accepted then optional reply event", Receipt: "required", HumanGate: false},
			{Name: "wait", Usage: "mektup wait <receipt-or-message-id> [--timeout <duration>]", Effects: []string{"network-read"}, RetrySafety: "safe observation", Availability: "custody or pinned source endpoint", Result: "reply or wait_incomplete", Receipt: "existing receipt", HumanGate: false},
			{Name: "inspect", Usage: "mektup inspect <target> [--receipts <limit>] [--blockers]", Effects: []string{"network-read"}, RetrySafety: "safe observation", Availability: "endpoint and target", Result: "bounded identity/runtime metadata", Receipt: "none required", HumanGate: false},
			{Name: "search", Usage: "mektup search <query> [--thread <target>]", Effects: []string{"network-read"}, RetrySafety: "safe observation", Availability: "endpoint/backend search capability", Result: "bounded thread or message matches", Receipt: "read event", HumanGate: false},
			{Name: "thread", Usage: "mektup thread list|read|turns|items|start|resume|fork", Effects: []string{"network-read", "thread-write for start/resume/fork"}, RetrySafety: "reads safe; mutations pinned and receipted", Availability: "endpoint/app-server", Result: "bounded metadata/transcript or thread URI", Receipt: "mutation required", HumanGate: false},
			{Name: "receipt", Usage: "mektup receipt list|show|reconcile|resolve", Effects: []string{"read; network-read for reconcile; thread-write for resolve"}, RetrySafety: "read safe; resolve explicit assertion", Availability: "journal and optionally endpoint", Result: "bounded receipt/evidence", Receipt: "resolve required", HumanGate: true},
			{Name: "endpoint", Usage: "mektup endpoint list|show|add|remove|check", Effects: []string{"read; host-write for add/remove"}, RetrySafety: "reads safe; config writes explicit", Availability: "local configuration; endpoint for check", Result: "endpoint metadata/health", Receipt: "mutation required", HumanGate: true},
			{Name: "storage", Usage: "mektup storage status|check|maintain|vacuum", Effects: []string{"read; destructive for vacuum"}, RetrySafety: "check safe; maintain/vacuum explicit", Availability: "local journal", Result: "storage status/check receipt", Receipt: "mutation required", HumanGate: true},
			{Name: "rpc", Usage: "mektup rpc <method> [--params|--params-file|--stdin] [--allow-effect <class>]...", Effects: []string{"method-classified: read/network-read/thread-write/host-write/auth/destructive/unknown"}, RetrySafety: "method-specific; unknown mutation never retried", Availability: "endpoint and method capability", Result: "method response or private spill artifact", Receipt: "required", HumanGate: true},
			{Name: "doctor", Usage: "mektup doctor [--fix]", Effects: []string{"read; host-write only with --fix"}, RetrySafety: "classified repair only", Availability: "local state and configured endpoints", Result: "diagnostic/repair plan", Receipt: "fix mutation required", HumanGate: true},
			{Name: "docs", Usage: "mektup docs agents|commands [--json]|envelopes|receipts", Effects: []string{"none"}, RetrySafety: "not applicable", Availability: "offline embedded assets", Result: "embedded documentation", Receipt: "none", HumanGate: false},
			{Name: "version", Usage: "mektup version [--json]", Effects: []string{"none"}, RetrySafety: "not applicable", Availability: "offline build metadata", Result: "version metadata", Receipt: "none", HumanGate: false},
			{Name: "completion", Usage: "mektup completion <zsh|bash|fish>", Effects: []string{"none"}, RetrySafety: "not applicable", Availability: "offline embedded script", Result: "shell completion", Receipt: "none", HumanGate: false},
		},
	}
}
