# Mektup v1 language-neutral contracts

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
returns the fencing token and current lease in the claim result; heartbeat,
commit, and abandon must present both values. `receiptId` is optional on
control documents, including status/reconcile, because custody authority is the
operation plus original/reply message identity rather than a local receipt
handle.

Fixtures are intended to be consumed by Go, future Rust, and other
implementations without changing their native APIs.
