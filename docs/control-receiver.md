# Control receiver policy

`go/internal/controlreceiver` is a callable one-shot custody receiver. It is
not wired into the production command in this lane.

The receiver accepts one `mektup/control/v1` metadata document on stdin. The
registry path, state directory, executable, journal path, and reply body are
never selected by that document. A pre-registered owner-private registry maps
opaque store IDs to trusted local state directories and endpoint IDs. Registry
lookups require an existing private `journal.sqlite3`; they never initialize a
missing database.

Claim, heartbeat, commit, abandon, status, and reconcile operate only on the
validated original operation and selected claim tuple. No app-server request or
reply body is dispatched here. `reconcile` expires custody claims using the
journal's clock; native app-server observation remains outside this receiver.

`requestedLease.durationMs` is accepted as advisory metadata for compatibility,
but the journal's configured lease policy is authoritative. Results report the
actual journal expiry issued by claim/heartbeat, not the requested duration.
Supplied lease timestamps are shape-validated; owner/token and the journal's
current clock decide whether a mutation is authorized.

Claim results use the locked union: `disposition: "claimed"` carries the
current fencing token and lease and authorizes the first body attempt;
`disposition: "existing"` carries status/winner metadata only and never
authorizes body dispatch or fencing operations.
