package cli

import _ "embed"

// These are version-matched, offline assets. Keep --skill and docs agents
// sourced from the same value so drift is impossible.
//
//go:embed docs/agent-guide.md
var agentGuide string

//go:embed docs/envelopes.md
var envelopeDocs string

//go:embed docs/receipts.md
var receiptDocs string

const usageText = `Mektup: reliable Codex thread control

Usage: mektup [global-options] <command> [command-options]

Global options:
  --json | --human             select machine JSONL or concise human output
  --endpoint <alias-or-id>     select destination endpoint
  --config <path>              select configuration location
  --state-dir <path>           select journal location
  --color auto|always|never    colorize human output when appropriate
  --debug                      enable redacted diagnostics
  --audit                      explicitly enable sensitive audit capture

Messaging:
  send <target> [message|--stdin|--file <path>] [--request-reply] [--wait]
  reply <message-or-receipt-id> [message|--stdin|--file <path>] [--wait]
  wait <receipt-or-message-id> [--timeout <duration>]
  inspect <target> [--receipts <limit>] [--blockers]

Targets:
  <unique-live-herdr-name>
  codex://<endpoint-alias-or-id>/thread/<thread-uuid>
  herdr://<endpoint-alias-or-id>/agent/<percent-encoded-name>
  herdr://<endpoint-alias-or-id>/pane/<workspace:pane>

Thread/read:
  thread list|read|turns|items|start|resume|fork
  search <query> [--thread <target>]

Receipts/endpoints/storage:
  receipt list|show|reconcile|resolve
  endpoint list|show|add|remove|check
  storage status|check|maintain|vacuum
  rpc <method> [--params <small-json>|--params-file <path>|--stdin]
  doctor [--fix]

Offline:
  version [--json]
  completion <zsh|bash|fish>
  --skill
  docs agents|commands [--json]|envelopes|receipts

Use mektup help <command> for command-specific guidance.
`

var helpTopics = map[string]string{
	"send": `Usage: mektup send <target> [message|--stdin|--file <path>] [options]

Exactly one body source is accepted. --wait implies --request-reply; raw mode
cannot request or wait for a reply. A successful send means acceptance, not
turn completion or human understanding.
`,
	"reply": `Usage: mektup reply <message-or-receipt-id> [message|--stdin|--file <path>] [options]

Replies resolve the original pinned sender thread. --status is success or
error; --error-code requires --status error.
`,
	"wait": `Usage: mektup wait <receipt-or-message-id> [--timeout <duration>]

Wait never sends or edits a message. Plain/raw sends fail reply_not_requested.
`,
	"inspect": `Usage: mektup inspect <target> [--receipts <limit>] [--blockers]

Inspect resolves a target without sending a message. A bare target is a unique
live Herdr agent name. Address a known Codex thread directly as:

  codex://local/thread/<thread-uuid>

The default includes up to 10 related receipts. Use --receipts 0 for an
identity-only existence check, or --blockers to include callback blockers.
`,
	"docs": `Usage: mektup docs agents|commands [--json]|envelopes|receipts

Documentation is embedded and works without endpoint or journal state.
`,
	"search": "Usage: mektup search <query> [--thread <target>] [--archived] [--source <kind>]...\n",
	"thread": `Usage: mektup thread list|read|turns|items|start|resume|fork [options]

Read and lifecycle commands accept a configured target URI. A known local
thread UUID can be addressed as codex://local/thread/<thread-uuid>.
`,
	"receipt":    "Usage: mektup receipt list|show|reconcile|resolve [options]\n",
	"endpoint":   "Usage: mektup endpoint list|show|add|remove|check [options]\n",
	"storage":    "Usage: mektup storage status|check|maintain|vacuum [options]\n",
	"rpc":        "Usage: mektup rpc <method> [--params|--params-file|--stdin] [--allow-effect <class>]...\n",
	"doctor":     "Usage: mektup doctor [--fix]\n",
	"completion": "Usage: mektup completion <zsh|bash|fish>\n",
	"version":    "Usage: mektup version [--json]\n",
}

func completionScript(shell string) string {
	commands := "send reply wait inspect search thread receipt endpoint storage rpc doctor docs version completion"
	switch shell {
	case "zsh":
		return "#compdef mektup\n_arguments '1:command:((send reply wait inspect search thread receipt endpoint storage rpc doctor docs version completion))'\n"
	case "fish":
		return "complete -c mektup -f -n '__fish_use_subcommand' -a '" + commands + "'\n"
	default:
		return "_mektup_completions() { COMPREPLY=( $(compgen -W '" + commands + "' -- \"${COMP_WORDS[COMP_CWORD]}\") ); }\ncomplete -F _mektup_completions mektup\n"
	}
}
