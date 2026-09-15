# Mektup v1 language-neutral contracts (spec revision 1.0.3)

The locked product specification revision is 1.0.3. The wire schema identifiers
remain versioned as `.../v1`; the revision is contract metadata, not a new wire
schema family.

The schemas in `schemas/` describe the stable JSON representations used by
Mektup implementations. They use JSON Schema 2020-12 and deliberately allow
additive unknown object fields because v1 readers must ignore fields they do
not understand. Stable identifiers, evidence states, warning/error codes, and
the exact target-visible envelope metadata remain constrained.

The target-visible `Mektup/1` body is not JSON. Its metadata schema preserves
the canonical hyphenated header names; the paired `.txt` fixture is the exact
LF-delimited rendering and includes the body whose byte count and SHA-256 are
declared by the header.

Portable receipts contain metadata and content locators only. The receipt
schema rejects the conventional body-bearing property names (`body`,
`bodyText`, and `replyBody`) while allowing unrelated future fields. Control
documents are metadata-only stdin messages for the fenced custody receiver.
Claim requests carry operation/message identity, owner, digest, status, and
routes, with an optional requested lease duration. The authoritative receiver
returns an explicit claim result union: `disposition: claimed` includes the
current fencing token and lease, while `disposition: existing` includes state
and optional non-authority status/winner metadata and forbids both token and
lease. Heartbeat, commit, and abandon must present both values. `receiptId` is optional on
control documents, including status/reconcile, because custody authority is the
operation plus original/reply message identity rather than a local receipt
handle.

Fixtures are intended to be consumed by Go, future Rust, and other
implementations without changing their native APIs.
