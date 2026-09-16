# Observability

Mektup observability is local and bounded.

Normal operational metadata logs live under `<state-dir>/logs`, use owner-only
permissions, rotate at 1 MiB, retain at most five files, and expire after 24
hours. The normal logger accepts metadata only, rejects body/raw/payload fields,
and redacts secret-looking values. It never records RPC bodies or raw params.

`--debug` writes redacted setup diagnostics to stderr only when explicitly
requested. Normal machine-mode stderr remains quiet.

`--audit` opts into a sensitive per-invocation capture under
`<state-dir>/logs/audit`. It records exact framed inbound/outbound text frames,
is bounded to 4 MiB, and refuses truncation. Publication is complete-or-absent
with a SHA-256 digest and owner-only permissions. Audit metadata declares
`sensitive=true` and `networkTelemetry=false`; it is not telemetry and is not
retention-eligible. Audit setup or capture over-limit failures never silently
produce a partial artifact.

Audit warnings and the complete artifact marker are included in non-messaging
operation output and receipts. Server requests remain observer-only: capture
does not answer or synthesize responses.
