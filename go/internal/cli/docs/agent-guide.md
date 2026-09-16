# Mektup agent guide (contract 1.0.8)

Mektup delivers messages to Codex threads and preserves evidence. Use ordinary
`mektup send <target> <message>` for one-way delivery. Use
`mektup send <target> <message> --request-reply` when a later response is
wanted, and use `mektup reply <message-or-receipt-id> <message>` to answer a
request. `--wait` and standalone `mektup wait <receipt-or-message-id>` are for
the cases where your next action strictly depends on the correlated reply.

Do not infer that acceptance means the recipient read, understood, or finished
the message. A final receipt is always emitted for successful mutations. In
machine mode, stdout is JSONL lifecycle events and stderr is reserved for
explicit diagnostics. Implicit agent mode (`MEKTUP_AGENT=1` or a nonempty
`CODEX_THREAD_ID`) uses bounded compact JSONL. Use explicit `--json` or
`MEKTUP_OUTPUT=json` for the compatible full machine page, `--compact` for a
deliberate compact page, or `--human` for concise interactive output.

Compact collections default to 10 rows and accept at most 25. Continue with
the returned opaque `nextCursor` and the command's `--cursor` option; inspect
uses `receiptsPage.nextCursor` with `--receipts-cursor`. Cursors are bound to
the store and exact query. Compact previews do not erase information: every
row retains exact endpoint/thread/history or receipt locators, and deliberate
`--view full`, `--portable`, and `--content` requests keep their exact output.
If compact output still cannot fit the hard record bound, Mektup reports a
typed terminal and an owner-private artifact or durable receipt locator.

Exactly one message body source is allowed: one positional string, `--stdin`,
or `--file <path>`. `--raw` is unwrapped one-way delivery and cannot be used
with `--wait` or `--request-reply`. Mektup never silently queues or retries an
outcome whose effect is unknown. Search is discovery, not delivery evidence.

Targets may be a unique live Herdr agent name, an explicit Herdr URI, or a
direct Codex thread URI. A bare UUID is not a messaging target. To preflight a
named live agent without returning receipt history, use:

```text
mektup inspect <agent-name> --receipts 0
```

To address a known local thread UUID directly, use the complete form:

```text
codex://local/thread/<thread-uuid>
```

Zero or multiple live Herdr-name matches fail closed. Resolution success proves
the target identity was found; it is not message delivery evidence.

When the source has one verified Herdr pane association and a safe registered
agent name, wrapped envelopes may include `from-herdr-name` beside the stable
`from-herdr` URI. Treat the name as convenient sender presentation only. It is
not routing, custody, authentication, or proof that the sender understood a
reply.

Reply custody claim results are an authority union: `claimed` includes the
current fencing token and lease for the one caller allowed to submit the body;
`existing` is tokenless and may include status or winner metadata only. An
existing result never transfers authority and must not trigger body resubmission.

The control registry is owner-private and remains under the default machine-user
state root. `--state-dir` and `MEKTUP_STATE_DIR` select an operation journal but
do not relocate the registry or fork built-in endpoint identity. Receiver
membership requires the stable endpoint, URI alias, decoded thread, and durable
journal relationship to agree exactly.

Remote exact-history strengthening uses an internal tokenless `observe` control
operation. It carries the native item identity and verified tuple/digest, never
a fencing token, lease, body, or dispatch owner. Repeated identical evidence is
idempotent; conflicting native identity or digest is rejected.

Portable reply recovery uses the tokenless `originalStatus` control operation.
It requires the exact original custody and reply-destination tuple, never reads
body content or revives authority, and returns only the strict `winner`,
`terminal_unknown`, or `pending` selection union.

Run `mektup docs commands --json` for version-matched command metadata and
`mektup version --json` for build and tested Codex metadata. Embedded docs work
offline and do not require endpoint or journal state.
