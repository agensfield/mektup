# Releasing Mektup

This is the planned release procedure. A green source build, a green test
suite, or a locally built archive is not a published or installed release.
Keep those evidence classes separate in the release record.

## Preconditions

Before creating any tag, the acceptance-v1 distribution, performance, and
security gates must be green on the intended merge commit. At minimum, record
the results of:

- `make fmt-check`, `make test`, `make test-race`, `make vet`, and the contract
  JSON validation (Draft 2020-12 positive and negative fixtures through the
  pinned `ajv-cli@5.0.0` validator);
- `make notices` and the pinned `make govulncheck` result, with the dependency
  inventory and license policy in [dependencies.md](dependencies.md);
- the CI Darwin/Linux amd64/arm64 build matrix;
- the supported app-server compatibility tests, including tested Codex
  `0.154.0` evidence and the explicit untested/unsupported paths;
- the scoped security review and any accepted residual risk.

Do not describe source or CI evidence as physical app-server, SSH, published,
or installed acceptance.

## Version and tags

Mektup has one repository with a Go module rooted at `go/`. For a future
`1.0.0` release, create both tags on the same release commit:

```text
v1.0.0       repository release tag
go/v1.0.0    Go module tag for github.com/agensfield/mektup/go
```

The `go/v1.0.0` tag is required for Go's subdirectory module versioning. It is
not a replacement for the repository tag, and it must not be created on a
different commit. A release is not ready if either tag is missing or points
elsewhere.

The module can then be installed from its public module path with:

```sh
go install github.com/agensfield/mektup/go/cmd/mektup@v1.0.0
```

Run that command in a clean temporary environment and retain the exact command,
Go version, resolved module version, and `mektup version --json` output.

## Build and publish

After the tags are approved and created by the release owner, the `v*.*.*` tag
workflow runs GoReleaser. It is configured to produce deterministic,
`-trimpath` Darwin/Linux amd64/arm64 `tar.gz` archives, a SHA-256 checksum
manifest, and one SBOM document per archive. Release metadata is embedded with
ldflags for version, commit, contract `1.0.6`, and tested Codex `0.154.0`.
The workflow pins Syft `v1.51.1` for SBOM generation.
The commit timestamp is used for archive metadata so rebuilding the same source
does not acquire a wall-clock timestamp.

The workflow creates GitHub release metadata only. It does not update the
Homebrew tap. After the GitHub artifacts exist, prepare a separate reviewed
Homebrew formula change that points at the exact archive URLs and SHA-256
entries. The tap update is a separate publication action with its own review,
not evidence that the GitHub release or a fresh install has passed.

## Artifact checks

For every archive, a reviewer should record:

1. `sha256sum --check checksums.txt` (or the platform equivalent);
2. archive member names and executable mode;
3. `mektup version --json`, including version, commit, contract version, and
   tested Codex versions;
4. the SBOM file and its association with the archive.

Check at least one Darwin and one Linux artifact on their native supported
platforms when possible. Cross-compilation proves a build, not runtime
acceptance.

## Fresh-install checks

Use a clean temporary `GOBIN` and an empty module cache for the Go install
check. Separately install the Homebrew formula after its tap change. For both
paths, capture:

- the exact source URL/tag or formula revision;
- the resolved binary path and `mektup version --json` output;
- offline commands (`--help`, `version`, completion/docs surfaces);
- a bounded read-only endpoint/health check if the acceptance environment
  provides one;
- exit status and stderr, including any expected unsupported-environment
  result.

No send, reply, endpoint mutation, storage mutation, or other operational
write is implied by a fresh-install check. Those are separate explicitly
authorized acceptance actions.

## Rollback and records

Do not replace immutable tags or silently overwrite release bytes. If an error
is found, stop publication, record the tag, commit, artifact names, checksums,
and observed failure, then prepare a new corrective version. Keep the CI run,
security review, acceptance receipts, GoReleaser output, SBOMs, and install
checks linked from the release record.
