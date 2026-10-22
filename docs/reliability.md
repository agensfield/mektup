# Delivery and recovery boundaries

Mektup 1.0.14 implements specification revision 1.0.9. Existing envelopes,
receipt schemas and custody claim fencing are unchanged. This revision makes
admission deadlines, observation gaps and SSH return-route readiness explicit.

## Admission

The delivery deadline covers semantic target/source resolution, return-route
preflight, preparation, custody claims, resume and submission. Reply waiting
and bounded cleanup use separate contexts; acceptance is not undone when a
caller cancels after submission.

Before SSH request-reply sends, the recipient checks its configured mappings
for the exact reply-body endpoint and custody endpoint/store. Fixed one-shot
`mektup control preflight` calls carry `mektup/preflight/v1` metadata only.
They never carry bodies, filesystem paths, executable names or credentials.
The checks observe configured identities and registered private store paths;
they do not reserve a runtime or guarantee a later write will succeed.

Both SSH peers need Mektup 1.0.14 or newer. An unsupported peer or unavailable
return path rejects before body dispatch. One-way delivery does not require
reverse reachability. Upgrade the peer rather than bypassing the check.
Current SSH commands select the remote user's default Codex runtime; a
nondefault runtime ID is rejected explicitly. No remote request may select a
Codex home or inject a command into the SSH invocation.

## Waiting through observation gaps

Custody's terminal reply evidence takes precedence over a failed history or
event observation. Successful, error and unknown replies retain their distinct
results. A gap remains diagnostic evidence and is not presented as native
observation.

When a caller supplied a finite wait deadline, custody polling can continue
after an observation failure until that deadline. Without a finite deadline,
an unresolved known gap returns incomplete after one bounded final read.

History reads are isolated from delivery connections. Oversized full-turn
pages retry at smaller page sizes on fresh connections. Item fallback also
uses a fresh connection. Existing message and aggregate reconciliation limits
remain in effect; a single oversized native item still fails explicitly when
the backend cannot provide a bounded representation.

## Same-operation recovery

Failures after local preparation expose the real receipt, operation and
message IDs, including failures before body submission. The body and remote
claim have separate evidence: a lost claim response can leave an unsent body
and an unknown remote claim. It does not authorize dispatch with another ID.

Use `receipt show` to inspect the recorded operation and `receipt reconcile`
to strengthen it from exact native history. A fresh `send` or `reply` command
allocates a new operation. Neither inspection nor reconciliation resends the
body. Duplicate reply claim owners cannot acquire dispatch authority.

These properties are conservative delivery evidence, not exactly-once
execution of arbitrary agent work. Offline eventual delivery would require a
separate retained-body outbox/inbox protocol and is not implemented.

## Validation

Unit regressions cover terminal-custody races, delayed custody, cancellation,
oversized socket responses, healthy fallback connections, stalled admission,
reply claim errors and retained failure receipts. `scripts/test-cross-host.sh`
adds opt-in black-box checks using two real isolated Codex daemons, separate
journals and actual SSH. Its wrappers select disposable runtime environments;
they do not alter the user's running daemons or default configuration.
