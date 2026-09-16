# Mektup envelopes

Target-visible envelopes use schema `Mektup/1`. They carry an immutable
`message-id`, pinned source and destination identities, reply routing and
custody references when requested, UTF-8 payload size, and a SHA-256 payload
digest. `--raw` sends the exact selected body and has no target-visible
correlation ID. Envelope provenance is observed evidence, not authenticated
authorship.

When the source thread has one verified Herdr pane association with a registered
agent name, `from-herdr-name` relays that name beside the stable `from-herdr`
pane URI. The name is optional, forgeable presentation metadata only. It never
participates in routing, authorization, custody, correlation, or provenance
strength, and receivers do not re-resolve it on the sender's host.
