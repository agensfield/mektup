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
  --json | --human | --compact select full JSONL, concise human, or bounded JSONL
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
  inspect <target> [--receipts <limit>] [--receipts-cursor <token>] [--blockers]

Targets:
  <unique-live-herdr-name>
  codex://<endpoint-alias-or-id>/thread/<thread-uuid>
  herdr://<endpoint-alias-or-id>/agent/<percent-encoded-name>
  herdr://<endpoint-alias-or-id>/pane/<workspace:pane>

Thread/read:
  thread list|read|turns|items|item|start|resume|fork
  search <query> [--thread <target>]

Receipts/endpoints/storage:
  receipt list|show|reconcile|resolve
  endpoint list|show|add|remove|check
  storage status|check|maintain|vacuum
  rpc <method> [--params <small-json>|--params-file <path>|--stdin]
  doctor [--fix]

Offline:
  version [--json|--compact]
  completion <zsh|bash|fish>
  --skill
  docs agents|commands|envelopes|receipts [--json|--compact]

Use mektup help <command> for command-specific guidance.
`

var helpTopics = map[string]string{
	"send": `Usage: mektup send <target> [message|--stdin|--file <path>] [options]

Exactly one body source is accepted. --wait implies --request-reply; raw mode
cannot request or wait for a reply. A successful send means acceptance, not
turn completion or human understanding.
`,
	"reply": `Usage: mektup reply <message-or-receipt-id> [message|--stdin|--file <path>] [options]

Replies resolve the exact wrapped original in your current thread. Use its
message-id, not a search snippet or a native item ID. An incomplete history
lookup reports resolver_unavailable rather than claiming message_not_found.
--status is success or error; --error-code requires --status error.
`,
	"wait": `Usage: mektup wait <receipt-or-message-id> [--timeout <duration>]

Wait never sends or edits a message. Plain/raw sends fail reply_not_requested.
`,
	"inspect": `Usage: mektup inspect <target> [--receipts <limit>] [--receipts-cursor <token>] [--blockers]

Inspect resolves a target without sending a message. A bare target is a unique
live Herdr agent name. Address a known Codex thread directly as:

  codex://local/thread/<thread-uuid>

The default includes up to 10 related receipts. Use --receipts 0 for an
identity-only existence check, or --blockers to include callback blockers.
Compact receipt pages include an opaque nextCursor consumed by
--receipts-cursor with the same target and filters.
`,
	"docs": `Usage: mektup docs agents|commands|envelopes|receipts [--json|--compact]

Documentation is embedded and works without endpoint or journal state.
`,
	"search": `Usage: mektup search <query> [--thread <thread-UUID-or-URI>] [--archived] [--source <kind>]... [--limit <n>] [--cursor <token>]

Scoped --thread accepts a bare native UUID or a Codex thread URI. The result
includes turnId, itemId, and turnCursor for an exact full-item read:

  mektup thread item <thread-URI> <turnId> <itemId> --cursor '<turnCursor>' --json
`,
	"thread": `Usage: mektup thread list|read|turns|items|item|start|resume|fork [options]

Read and lifecycle commands accept a configured target URI. A known local
thread UUID can be addressed as codex://local/thread/<thread-uuid>. For a
search hit, use thread item with its turnId, itemId, and turnCursor to read
only that native item instead of paging the whole thread.
`,
	"thread item": `Usage: mektup thread item <thread-target> <turn-id> <item-id> --cursor <turnCursor> [--json]

Use the exact turnId, itemId, and turnCursor from a scoped search hit. Mektup
reads the native full turn at that cursor, checks both IDs, and returns one
row in the existing thread.items.completed event. Implicit agent output is a
bounded preview; --json deliberately returns the complete native item.
Search/cursor metadata is never trusted as content or reply authority.
`,
	"thread items": `Usage: mektup thread items <thread-target> [--turn <turn-id>] [--limit <n>] [--cursor <token>] [--json]

This is a paginated collection. --turn restricts it to one turn; --json keeps
full native items, while implicit agent mode previews them. For a known search
hit, thread item with turnCursor avoids paging even a long turn.
`,
	"thread turns": `Usage: mektup thread turns <thread-target> [--view summary|full] [--limit <n>] [--cursor <token>]

--view full is an explicit exact turn read. Search-hit turnCursor can locate
one turn with --cursor and --limit 1; thread item extracts one item directly.
`,
	"receipt":    "Usage: mektup receipt list [--limit <n>] [--cursor <token>]|show|reconcile|resolve [options]\n",
	"endpoint":   "Usage: mektup endpoint list|show|add|remove|check [options]\n",
	"storage":    "Usage: mektup storage status|check|maintain|vacuum [options]\n",
	"rpc":        "Usage: mektup rpc <method> [--params|--params-file|--stdin] [--allow-effect <class>]...\n",
	"doctor":     "Usage: mektup doctor [--fix]\n",
	"completion": "Usage: mektup completion <zsh|bash|fish>\n",
	"version":    "Usage: mektup version [--json|--compact]\n",
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
