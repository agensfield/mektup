# Mektup agent guide (contract 1.0.3)

Mektup delivers messages to Codex threads and preserves evidence. Use ordinary
`mektup send <target> <message>` for one-way delivery. Use
`mektup send <target> <message> --request-reply` when a later response is
wanted, and use `mektup reply <message-or-receipt-id> <message>` to answer a
request. `--wait` and standalone `mektup wait <receipt-or-message-id>` are for
the cases where your next action strictly depends on the correlated reply.

Do not infer that acceptance means the recipient read, understood, or finished
the message. A final receipt is always emitted for successful mutations. In
machine mode, stdout is JSONL lifecycle events and stderr is reserved for
explicit diagnostics. Use `--json` for machine output or `--human` for concise
interactive output. Agent mode is selected conservatively by explicit flags,
`MEKTUP_AGENT=1`, or a nonempty `CODEX_THREAD_ID`.

Exactly one message body source is allowed: one positional string, `--stdin`,
or `--file <path>`. `--raw` is unwrapped one-way delivery and cannot be used
with `--wait` or `--request-reply`. Mektup never silently queues or retries an
outcome whose effect is unknown. Search is discovery, not delivery evidence.

Reply custody claim results are an authority union: `claimed` includes the
current fencing token and lease for the one caller allowed to submit the body;
`existing` is tokenless and may include status or winner metadata only. An
existing result never transfers authority and must not trigger body resubmission.

The control registry is owner-private and remains under the default machine-user
state root. `--state-dir` and `MEKTUP_STATE_DIR` select an operation journal but
do not relocate the registry or fork built-in endpoint identity. Receiver
membership requires the stable endpoint, URI alias, decoded thread, and durable
journal relationship to agree exactly.

Run `mektup docs commands --json` for version-matched command metadata and
`mektup version --json` for build and tested Codex metadata. Embedded docs work
offline and do not require endpoint or journal state.
