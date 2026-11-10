"""Deterministic, metadata-only patch for one qualified stable Codex version."""

import json
import re
from pathlib import Path

FILES = ("go/internal/compat/compat.go", "go/internal/compat/compat_test.go",
         "go/internal/cli/app.go", "go/internal/cli/app_test.go", ".goreleaser.yaml",
         "README.md", "docs/releasing.md", "docs/codex-qualification.md")


def stable(value):
    if not re.fullmatch(r"(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)\.(?:0|[1-9][0-9]*)", value):
        raise ValueError(f"expected a complete stable version: {value!r}")
    return tuple(map(int, value.split(".")))


def tested(root):
    text = (root / "go/internal/compat/compat.go").read_text()
    match = re.search(r"var testedVersions = \[\.\.\.\]string\{([^}]+)\}", text)
    if not match:
        raise ValueError("tested version declaration changed")
    values = json.loads("[" + match[1] + "]")
    return [value.removeprefix("v") for value in values]


def replace_once(text, old, new):
    if text.count(old) != 1:
        raise ValueError(f"expected exactly one metadata marker: {old!r}")
    return text.replace(old, new)


def render(root: Path, version: str, run_id: int, receipt: dict):
    versions = tested(root)
    stable(version)
    if version in versions:
        return None
    if stable(version) <= max(map(stable, versions)):
        raise ValueError("automatic qualification releases must advance stable Codex")
    readme = (root / "README.md").read_text()
    previous = re.search(r"Mektup v([0-9]+\.[0-9]+\.[0-9]+) implements", readme)[1]
    major, minor, patch = stable(previous)
    release = f"{major}.{minor}.{patch + 1}"
    files = {}
    old_csv, new_csv = ",".join(versions), ",".join([*versions, version])
    for name in ["go/internal/cli/app.go", "go/internal/cli/app_test.go",
                 ".goreleaser.yaml", "docs/releasing.md"]:
        files[name] = replace_once((root / name).read_text(), old_csv, new_csv)
    files["go/internal/cli/app.go"] = replace_once(
        files["go/internal/cli/app.go"], '"' + versions[-1] + '"}',
        '"' + versions[-1] + '", "' + version + '"}')
    name = "go/internal/compat/compat.go"
    files[name] = replace_once((root / name).read_text(),
                              f'"v{versions[-1]}"}}', f'"v{versions[-1]}", "v{version}"}}')
    name = "go/internal/compat/compat_test.go"
    text = (root / name).read_text()
    marker = '\t\t{"latest tested build metadata",'
    text = replace_once(text, marker,
                        f'\t\t{{"qualified {version}", "codex_cli_rs/{version} (Linux; amd64)", '
                        f'"{version}", Tested, ""}},\n' + marker)
    lines = text.splitlines(keepends=True)
    for index, line in enumerate(lines):
        if '"latest tested build metadata"' in line or '"prerelease"' in line:
            lines[index] = line.replace(versions[-1], version)
        elif '"future patch"' in line:
            old = ".".join(map(str, (*stable(versions[-1])[:2], stable(versions[-1])[2] + 1)))
            new = ".".join(map(str, (*stable(version)[:2], stable(version)[2] + 1)))
            lines[index] = line.replace(old, new)
    files[name] = "".join(lines)
    files["README.md"] = readme.replace(f"v{previous}", f"v{release}").replace(
        f" and {versions[-1]}.", f", {versions[-1]} and {version}.")
    if files["README.md"] == readme or f"and {version}." not in files["README.md"]:
        raise ValueError("README version markers changed")
    name = "docs/codex-qualification.md"
    text = (root / name).read_text()
    text = re.sub(r"qualify-codex-release.sh [0-9]+\.[0-9]+\.[0-9]+", f"qualify-codex-release.sh {version}", text)
    text = re.split(r"\n## (?:Codex [0-9].* receipt|Latest receipt)\n", text)[0].rstrip()
    files[name] = text + f"""

## Latest receipt

[Qualification run {run_id}](https://github.com/agensfield/mektup/actions/runs/{run_id})
returned `PASS`, `qualified=true`, `checksPassed=true`, `sourceDirty=false`
for exact Codex {version} on clean Mektup source `{receipt['mektupSourceCommit']}`.
Platform: `{receipt['platform']}`. Official asset SHA-256:
`{receipt['codexAssetSha256']}`.
Mektup v{release} records this version as tested. Contract and messaging
behavior are unchanged.
"""
    return previous, release, files
