# Mektup receipts

Portable receipts use schema `mektup/receipt/v1`. They preserve operation,
endpoint, thread, turn, message, evidence, warning, timestamp, size, and digest
metadata without bodies or credentials. Evidence distinguishes `not_sent`,
`rejected`, `accepted`, `reply_accepted`, `reply_observed`, `outcome_unknown`,
and `manually_resolved`. Imported receipts are untrusted claims and never grant
endpoint or custody authority.

Compact receipt pages use the distinct `mektup/receipt-summary/v1` projection.
A summary is explicitly non-canonical and cannot be imported or used as
dispatch authority. It preserves exact receipt/operation/message and local
content/evidence locators so `mektup receipt show <receipt-id> --json` can
retrieve the complete metadata. Continue compact lists with `--cursor`; inspect
receipt pages use `--receipts-cursor`. Tokens are opaque and bound to the same
store, filters, ordering, and initial traversal anchor.
