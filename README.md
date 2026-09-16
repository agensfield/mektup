# Mektup

Mektup is an agent-first, reliable Codex thread-control library and CLI.
Messaging is its first product-critical workflow; thread control is its reusable
foundation.

Mektup v1.0.0 implements specification revision 1.0.6 and is tested against
Codex app-server 0.154.0.

## Install

With Homebrew:

```sh
brew install agensfield/tap/mektup
```

Or with Go 1.25.13 or newer:

```sh
go install github.com/agensfield/mektup/go/cmd/mektup@v1.0.0
```

Run `mektup --skill` for agent-facing operating instructions or
`mektup --help` for the command surface.

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
