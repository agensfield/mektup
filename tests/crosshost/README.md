# Isolated two-host acceptance

Run from the repository with locally built candidates for both platforms:

```sh
MEKTUP_ACCEPT_REPORT_DIR=/tmp/mektup-acceptance \
  scripts/test-cross-host.sh /tmp/mektup-darwin-arm64 /tmp/mektup-linux-amd64
```

The default SSH aliases are `devbox` and, on that host, `seele`. Override them
with `MEKTUP_ACCEPT_REMOTE_HOST` and `MEKTUP_ACCEPT_RETURN_HOST`. Both hosts need
Python 3, installed Codex, and working noninteractive SSH to each other. The
fixture never installs the candidates or uses the user's Codex daemon. It owns
temporary Codex homes, fake Responses providers, independent daemons, endpoint
registries, journals, and threads, then terminates only its own processes.

The encrypted SSH hops are real. Test-only fixed-command wrappers select the
other fixture's temporary environment. This proves the protocol across two
hosts; it does not qualify ambient SSH command lookup or production daemon
configuration. The receipt records that distinction and candidate SHA-256s.

Both request origins exercise ordinary send, reply, native exact UTF-8 body and
hash, single native item, original-custody wait, interrupted waiter followed by
journal reopening, and repeated observation without duplicate delivery. One return
control receiver commits then deliberately discards its response, to check
recovery without resubmitting the body. A missing reverse endpoint must produce
`reply_route_unavailable` / `not_sent`, with no native body observed.

JSON receipts and per-host logs are saved only when a report directory is set.
The fake provider always returns a fixed response and never invokes a model or
asks Codex to run a tool. This suite is deliberately opt-in; normal Go tests do
not access SSH or an installed Codex.
