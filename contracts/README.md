# Mektup v1 language-neutral contracts (spec revision 1.0.8)

The locked product specification revision is 1.0.8. The wire schema identifiers
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
declared by the header. The optional `from-herdr-name` field is observed,
non-authoritative sender presentation metadata. Canonical emitters include it
only beside `from-herdr` and only for Herdr's registered-name grammar; receivers
ignore absent or malformed values without weakening stable identity checks.

Portable receipts contain metadata and content locators only. The receipt
schema rejects the conventional body-bearing property names (`body`,
`bodyText`, and `replyBody`) while allowing unrelated future fields. Control
documents are metadata-only stdin messages for the fenced custody receiver.
Claim requests carry operation/message identity, owner, digest, status, and
routes, with an optional requested lease duration. The authoritative receiver
returns an explicit claim result union: `disposition: claimed` includes the
current fencing token and lease, while `disposition: existing` includes state
and optional non-authority status/winner metadata and forbids both token and
lease. Heartbeat, commit, and abandon must present both values. The `observe`
operation carries exact native item evidence without dispatch authority and
returns tokenless state/status/winner metadata. `receiptId` is optional on
control documents, including status/reconcile, because custody authority is the
operation plus original/reply message identity rather than a local receipt
handle.

Compact JSONL is a presentation contract, not a second authority format.
`compact-v1.schema.json` defines bounded text-preview and page metadata shapes;
`receipt-summary-v1.schema.json` deliberately identifies compact summaries as
non-canonical. A summary cannot be imported or used as dispatch authority, but
retains exact local locators for retrieving the canonical receipt.

Portable reply recovery uses the tokenless `originalStatus` control operation.
It requires the exact original custody and reply-destination tuple, never reads
body content or revives authority, and returns only the strict `winner`,
`terminal_unknown`, or `pending` selection union.

Fixtures are intended to be consumed by Go, future Rust, and other
implementations without changing their native APIs.
