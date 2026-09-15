# Mektup receipts

Portable receipts use schema `mektup/receipt/v1`. They preserve operation,
endpoint, thread, turn, message, evidence, warning, timestamp, size, and digest
metadata without bodies or credentials. Evidence distinguishes `not_sent`,
`rejected`, `accepted`, `reply_accepted`, `reply_observed`, `outcome_unknown`,
and `manually_resolved`. Imported receipts are untrusted claims and never grant
endpoint or custody authority.
