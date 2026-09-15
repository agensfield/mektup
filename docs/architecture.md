# Architecture

Mektup has two public Go packages and deliberately narrow internal adapters.
Observable behavior is governed by the language-neutral contracts at the
repository root, not by storage rows or CLI presentation types.

## Layers

```text
CLI presentation and embedded docs
                 |
public mektup intent service
       /         |          \
appserver    journal ports    resolver/custody ports
 transport       |          /
       \      internal adapters
        local UDS / system SSH
```

### `appserver`

The public low-level package owns WebSocket framing, initialization, request
correlation, passive event and server-request observation, and connection
failure evidence. It has no dependency on SQLite, Herdr, semantic receipts, or
CLI output.

One pump continuously drains the connection. Request registration precedes any
possible write. Cancellation can report that a request was not written only
after the writer queue has acknowledged withdrawal. Results distinguish a
proven pre-write failure, a request that may have been written, a server error,
and a received response. A disconnect or bounded-event-queue overflow closes
the connection and conservatively completes pending mutations; it never drops
events while continuing to claim a gap-free stream.

Server requests are observations. Mektup never answers or rejects them, even
when their method is unknown. This differs intentionally from the pinned Codex
Rust remote client because app-server callbacks are shared and first response
wins.

### `mektup`

The public intent package owns target pinning, compatibility policy, envelopes,
message/reply semantics, method-specific safe retry, reconciliation, and domain
results. Public request and result types do not expose SQL rows, transport
queues, local paths, or CLI rendering.

Ordinary send always calls app-server `turn/start` with minimal parameters and a
Mektup message ID as `clientUserMessageId`. Codex 0.154.0 atomically starts or
steers and returns the same response shape for both; Mektup therefore receipts
only acceptance into the returned turn. It never performs a read-then-start or
read-then-steer race.

Retry sits above the transport and is permitted only for a recognized response
that proves non-admission. Generic `-32603`, timeouts, EOF, and response loss are
not retryable. Review/compact temporary rejection is classified by pinned exact
fixtures rather than error code alone.

### Journal and custody

The SQLite implementation remains internal. It exposes explicit compare-and-
swap transitions through narrow service ports; callers cannot arbitrarily set a
receipt state. No transaction remains open across app-server or SSH work.

For ordinary delivery:

1. Persist the operation, message, pinned routes, semantics, digest, and a
   prepared attempt.
2. Commit `dispatch_started` before handing the request to code able to write.
3. Commit conclusive acceptance or rejection before emitting stdout.
4. Recover an orphaned prepared attempt as `not_sent`; recover an orphaned
   dispatch-started attempt as `outcome_unknown`.

For a reply:

1. Establish the original request and custody relationship before the outbound
   request is sent.
2. Claim the reply ID, original ID, route, status, digest, size, attempt owner,
   fencing token, and finite lease before body submission.
3. Keep the claim alive independently while the body operation blocks.
4. After the pinned destination accepts the body, atomically commit
   `reply_accepted`, close the claim, and select the first reply when none has
   already won.
5. Expiry fences a late token, records `reply_outcome_unknown`, and wakes the
   waiter. It never permits redispatch. A later exact native item may strengthen
   evidence through reconciliation without reviving the token.

The journal stores metadata and digests, never message or reply bodies.

### Endpoint and custody registries

A stable endpoint ID identifies one Codex app-server route. A custody store ID
selects one locally registered authoritative journal. Display aliases are local
convenience and grant no portable authority. Source, destination, reply-body
destination, and custody endpoint are resolved independently.

The built-in local route uses the existing daemon Unix socket and never starts,
restarts, replaces, or falls back to an embedded app-server. SSH invokes system
OpenSSH and a fixed remote Mektup/app-server command. Bodies, paths, credentials,
and shell fragments never enter generated argv. The proxy carries the raw HTTP
Upgrade and WebSocket byte stream over stdio.

### Reconciliation

Wait reads custody first, then establishes live subscription and buffering
before scanning full history. It checks custody during long scans, revisits
mutable/current turns, and deduplicates overlap by native item identity plus
Mektup message identity. Full turn pages are the legacy fallback when the item
backend is unavailable. Summary views and fuzzy search are never delivery
evidence.

An overflow, disconnect, or unprovable handoff is an explicit evidence gap. It
causes a bounded retry of reconciliation or a degraded/incomplete result; it
never becomes an indefinite gap-free wait.

## Evidence boundaries

Mektup keeps these claims separate:

- source implemented;
- automated unit/integration tested;
- isolated real app-server conformance tested;
- local live-daemon accepted;
- SSH accepted;
- cross-platform artifact built;
- published release;
- installed release accepted.

No earlier layer implies a later one.
