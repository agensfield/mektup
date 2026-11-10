# Automatic compatibility publication

The weekly Codex qualifier remains the single real-binary compatibility test.
`Publish qualified Codex compatibility` consumes its successful main-branch run:

1. Validate the workflow origin, clean-source PASS receipt, exact source SHA,
   official stable Codex release and Linux asset digest. Only the latest stable
   Codex release is eligible. An already-tested version is a no-op.
2. Generate one deterministic metadata-only patch from that qualified source.
   Patch the next Mektup patch version, classifier/build lists, classifier/version
   coverage and docs. Create a PR; verify its complete diff before merging it.
3. Run the existing CI workflow on the exact PR head. Merge only after success,
   without bypassing the main ruleset. Run existing CI on the exact merged main.
4. Atomically create paired repository/Go-module tags. Existing tags must name
   the same commit; they are never moved, deleted or overwritten.
5. Invoke the existing packaging workflow. Verify all four archives and SBOMs,
   native Linux binary metadata, and a fresh public Go installation. Publish
   linked release notes with the source and qualification/CI receipts.

`GITHUB_TOKEN` cannot rely on its own pushes/PRs to start subsequent workflows.
The coordinator explicitly dispatches the existing CI and release workflows and
correlates their exact SHA/ref/run identities. Existing matching runs are reused.
There is one serialized publication coordinator; it does not introduce another
qualification, source test suite, or packaging pipeline.

The Homebrew tap's own `Publish Mektup formula` workflow checks public releases
hourly. It requires successful product packaging/verification, matches the
manifest against GitHub's archive/SBOM asset digests, publishes the four formula
URLs/hashes, then dispatches the existing macOS 26/Linux formula matrix. It uses
only the tap's own token. Product CI has no tap credential, host SSH key or
personal access token.

**The automation ends at publication.** It installs no launchd jobs, host timers,
or host updaters, and never connects to Seele/devbox or restarts Codex/Herdr.
Host installation remains a separate deliberate operation.

## Permissions and recovery

The publication job alone requests contents, pull-request and Actions write
permissions. Other workflows retain read-by-default permissions. Repository
settings must allow Actions to create PRs. New release tags can be created by contents-write actors, while tag
update/deletion protections remain in force. GitHub does not permit its built-in
Actions integration as a creation-rule bypass actor for this organization; the
previous owner-only creation rule was removed, without granting a tag mutation
bypass or changing the existing owner bypass.
The main PR/check requirements remain unchanged; no approval or CI bypass is
used. Enabling PR creation is GitHub's combined create/approve setting; the
workflow never submits approving reviews.

A stale qualification, altered patch, source movement, failed CI, conflicting tag,
missing artifact or checksum mismatch stops publication. No failure is silently
converted into a fresh version, changed tag or weaker check. New runtime changes
require a new clean-source qualification. For a transient failed child workflow,
rerun that exact child first, then resume the coordinator using the same receipt:

```sh
gh workflow run auto-codex-release.yml --ref main -f qualification_run_id=<run-id>
```

A published release is preserved during recovery; packaging skips creating it
again and reruns verification. A partial or inconsistent release fails validation
and requires deliberate repair or a new version. A closed unmerged automation PR
is not reopened automatically. The coordinator can reuse an already-pushed exact
metadata branch or merged PR after interruption.

For manual packaging recovery, dispatch against the existing tag itself:

```sh
gh workflow run release.yml --ref v1.0.19 -f tag=v1.0.19
```

Local verification is `python3 -m unittest discover -s tests/automation -v` plus
`actionlint .github/workflows/*.yml`. Disposable Git fixtures exercise successful
publication, failed PR/main gates, merged-PR recovery, completed-run idempotency,
stale source, changed candidates, conflicting immutable tags, receipt trust,
workflow correlation and manifest corruption. They do not claim a future public
Codex version was qualified or published.
