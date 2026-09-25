# Qualifying a Codex app-server release

Run `scripts/qualify-codex-release.sh 0.157.0` from any checkout with Docker,
`curl`, and `jq`. The argument may be any published Codex version with Linux
release assets, including prereleases. The optional GitHub Actions workflow
accepts the same version through `workflow_dispatch`; its weekly run selects
GitHub's latest stable Codex release. No central daemon or installed Codex is
changed.

The runner resolves the official release asset and its published SHA-256,
installs that exact binary in a disposable Linux image, and records its
initialize version. The container has no host Codex home, daemon socket, or
network access at test time. Its local fake Responses server makes real turns
deterministic. `CODEX_HOME`, the Unix socket, threads, and SQLite state are
created inside the container.

`PASS` means all of these checks succeeded against the named binary and the
clean Mektup source commit:

- the complete Go race suite and declarative wire conformance;
- real native thread start, resume, fork, name, unsubscribe, list, loaded
  list, read, turns, items, search, occurrences, and turn delivery;
- an automatic census of Mektup-owned native RPC method literals, all of
  which must appear in the real request trace;
- exact native body and reply identity, local custody wait, and journal
  recovery after close/reopen;
- built-in `local` endpoint identity and stable-ID resolution through the
  daemon's control path, including a symlinked managed socket, plus a healthy
  read-only `doctor` socket finding;
- Codex `app-server proxy` carrying an actual Mektup SSH transport client;
- passive Mektup observation while another client handles both user-input
  and command-approval requests, including detach, replay, and blocker state.

This qualification concerns Mektup's existing app-server dependency contract.
Codex features that Mektup does not consume need no qualification. Arbitrary
user-supplied raw RPC methods are outside that contract. The receipt names the
Linux architecture tested; a host-specific installed check is still a
separate deployment observation.

The runner outputs one `mektup/codex-qualification/v1` JSON receipt with
`status`, `qualified`, and `checksPassed` at the front. A dirty Mektup checkout
can produce `DEVELOPMENT_PASS` with `checksPassed:true` for iteration, but its
`qualified` value is false and it never reports `PASS`.
Set `MEKTUP_COMPAT_REPORT_DIR` to retain the full test log and receipt in an
explicit private directory. Otherwise those temporary logs are removed. The
runtime container uses `--rm`; the runner also removes its uniquely tagged
image and temporary directory on success or failure. Docker's reusable build
cache is left alone so later versions can reuse the Go dependency layers.

The `tested_codex_versions` baked into an already published Mektup binary is
historical release metadata. A qualification receipt does not silently change
that binary or suppress its honest `untested_server_version` warning. A
Mektup release is needed only when product behavior or embedded metadata is
intentionally changed, not to perform the qualification itself.
