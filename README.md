# Mektup

Mektup is an agent-first, reliable Codex thread-control library and CLI.
Messaging is its first product-critical workflow; thread control is its reusable
foundation.

Mektup v1.0.3 implements specification revision 1.0.8 and is tested against
Codex app-server 0.154.0.

Wrapped messages may relay a verified Herdr registered name for recipient
display. The name remains optional observational metadata; stable endpoint,
thread, pane, and custody identities remain authoritative.

## Install

With Homebrew:

```sh
brew install agensfield/tap/mektup
```

Or with Go 1.25.13 or newer:

```sh
go install github.com/agensfield/mektup/go/cmd/mektup@v1.0.3
```

Run `mektup --skill` for agent-facing operating instructions or
`mektup --help` for the command surface.

Interactive terminals receive concise tables, summaries, and automatic color.
Use `--color always|never|auto`, `MEKTUP_COLOR`, or the standard `NO_COLOR`
environment variable to control decoration. Machine JSONL and redirected human
output never contain ANSI escapes unless color is explicitly forced.

Implicit agent mode emits context-budgeted compact JSONL pages. Compact
collections default to 10 rows and never exceed 25; opaque cursors continue a
page without losing exact endpoint, thread, turn, item, receipt, or evidence
locators. Use explicit `--json` or `MEKTUP_OUTPUT=json` for the compatible full
machine page, and `--compact` to request compact output deliberately.

## Repository layout

```text
contracts/       Language-neutral schemas and golden fixtures
conformance/     Observable cross-implementation scenarios
go/              Go library and CLI
```

Mektup does not start or supervise Codex or Herdr processes. It connects to an
existing Codex app-server and treats Herdr as an optional target resolver.

## License

MIT
