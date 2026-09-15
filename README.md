# Mektup

Mektup is an agent-first, reliable Codex thread-control library and CLI.
Messaging is its first product-critical workflow; thread control is its reusable
foundation.

The repository is being implemented against Mektup specification revision
1.0.3 and Codex app-server 0.154.0. No stable release exists yet.

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
