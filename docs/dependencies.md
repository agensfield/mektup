# Runtime dependencies and notices

This inventory describes the current v1 integration shape: the app-server
transport, journal, CLI, and compatibility packages are built from the Go
module at `go/`. It is an implementation/release record, not a promise that a
future dependency graph will remain unchanged.

## Runtime-linked inventory

The release notice generator uses `GOPROXY=off GOSUMDB=off go list -deps
-json ./...` and follows each linked package's `Module` record. It separately
uses `go list -m -json all` to identify direct module requirements. Standard
library packages and the main module are excluded.

The current integration versions are:

| Module | Version | Edge | License source |
| --- | --- | --- | --- |
| `github.com/gorilla/websocket` | `v1.5.3` | direct | `LICENSE` |
| `golang.org/x/mod` | `v0.40.0` | direct | `LICENSE` |
| `modernc.org/sqlite` | `v1.59.0` | direct | `LICENSE`, `LICENSE-SQLITE`, `LICENSE-SQLITE_VEC` |
| `github.com/dustin/go-humanize` | `v1.0.1` | transitive | `LICENSE` |
| `github.com/google/uuid` | `v1.6.0` | transitive | `LICENSE` |
| `github.com/mattn/go-isatty` | `v0.0.24` | transitive | `LICENSE` |
| `github.com/ncruces/go-strftime` | `v1.0.0` | transitive | `LICENSE` |
| `github.com/remyoudompheng/bigfft` | `20230129092748-24d4a6f8daec` | transitive | `LICENSE` |
| `golang.org/x/sys` | `v0.47.0` | transitive | `LICENSE` |
| `modernc.org/libc` | `v1.75.7` | transitive | `LICENSE`, `LICENSE-3RD-PARTY.md` |
| `modernc.org/mathutil` | `v1.7.1` | transitive | `LICENSE` |
| `modernc.org/memory` | `v1.12.1` | transitive | `LICENSE`, `LICENSE-GO`, `LICENSE-LOGO`, `LICENSE-MMAP-GO` |

The module graph also contains build or optional dependencies that are not
runtime-linked by `go list -deps` for the current packages, such as
`github.com/google/pprof`, `github.com/hashicorp/golang-lru/v2`, modernc
compiler packages, `golang.org/x/tools`, and `golang.org/x/sync`. They remain
subject to dependency review when the linked package set changes, but are not
listed as shipped runtime notices until the binary links them.

## License policy

The generator reads license and notice files from the exact module source
directories selected by Go. It currently allows MIT, BSD-2-Clause, BSD-3-Clause,
Apache-2.0, ISC, public-domain text, and named attribution notices. It fails
closed when a linked module has no recognizable license/notice file or its
license text does not match an allowed class. This is a release gate, not a
substitute for legal review.

Composite files such as `LICENSE-3RD-PARTY.md` are classified as bundled
upstream notices and retained verbatim rather than forced into one SPDX label.

`modernc.org/libc`, `modernc.org/memory`, and `modernc.org/sqlite` ship more
than one notice file. The generator retains every matching `LICENSE*`,
`COPYING*`, and `NOTICE*` file, including SQLite's public-domain notice and
the `LICENSE-3RD-PARTY.md` / logo / mmap attributions. It does not flatten or
rewrite those texts.

## Deterministic generation and archive policy

Run:

```sh
make notices
```

The command writes `THIRD_PARTY_NOTICES.md` at the repository root. It sorts
modules and notice filenames, uses no timestamps, and refuses to access the
network. A clean machine must prefetch the exact `go.sum` inputs first:

```sh
(cd go && go mod download)
make notices
```

The GoReleaser `before` hook runs the same generator, after the release job's
explicit module-prefetch step. Archives include `LICENSE`, the generated
`THIRD_PARTY_NOTICES.md`, and the release/architecture documentation. The
repository's MIT `LICENSE` is therefore present alongside, but not merged into,
the dependency notices.

## Vulnerability scanning

Vulnerability scanning is intentionally separate from archive generation. CI
and the local Make target use the exact tool version `golang.org/x/vuln`
`v1.8.0`:

```sh
make govulncheck
```

The command may consult the Go vulnerability database and therefore is not a
reproducibility input for `THIRD_PARTY_NOTICES.md` or GoReleaser archives. A
release record should retain the scan version, database observation date, exit
status, and any accepted findings. A green scan does not prove runtime or
physical app-server acceptance.
