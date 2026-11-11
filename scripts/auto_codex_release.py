#!/usr/bin/env python3
"""Consume one trusted qualification; ship its exact metadata patch through CI."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time

from release_metadata import FILES, render, stable, tested

REPO = "agensfield/mektup"
QUALIFIER = ".github/workflows/codex-qualification.yml"


def command(*args, **kwargs):
    return subprocess.check_output(args, text=True, **kwargs).strip()


def gh(*args):
    return command("gh", *args)


def api(route):
    return json.loads(gh("api", f"repos/{REPO}/{route}"))


def validate_run(run):
    if (run.get("conclusion") != "success" or run.get("path") != QUALIFIER
            or run.get("head_branch") != "main"
            or run.get("head_repository", {}).get("full_name") != REPO
            or run.get("event") not in ("schedule", "workflow_dispatch")):
        raise ValueError("qualification must be a successful trusted main workflow")


def validate_receipt(run, receipt, official):
    validate_run(run)
    if (receipt.get("schema") != "mektup/codex-qualification/v1"
            or receipt.get("status") != "PASS" or receipt.get("qualified") is not True
            or receipt.get("checksPassed") is not True or receipt.get("sourceDirty") is not False
            or receipt.get("mektupSourceCommit") != run["head_sha"]):
        raise ValueError("qualification receipt is not a clean exact-source PASS")
    version = receipt["codexVersion"]
    stable(version)
    if (official.get("tag_name") != f"rust-v{version}" or official.get("draft")
            or official.get("prerelease")):
        raise ValueError("qualification must name an official stable release")
    names = {"linux/x86_64": "codex-x86_64-unknown-linux-musl.tar.gz",
             "linux/aarch64": "codex-aarch64-unknown-linux-musl.tar.gz"}
    asset_name = names.get(receipt.get("platform"))
    assets = [a for a in official["assets"] if a["name"] == asset_name]
    if len(assets) != 1 or assets[0].get("digest") != receipt.get("codexAssetSha256"):
        raise ValueError("qualification asset identity differs from the official release")
    return version


def matching_run(runs, sha, label, branch, ordinary_event):
    exact_source = [r for r in runs if r["head_sha"] == sha and r["head_branch"] == branch]
    correlated = [r for r in exact_source if r["event"] == "workflow_dispatch"
                  and r["display_title"] == label]
    # A PR opened by GITHUB_TOKEN can create a newer approval-required run.
    # It is not the CI dispatch we own and must neither shadow it nor suppress
    # dispatch. A correlated failure still wins and remains a real failed gate.
    candidates = correlated or [r for r in exact_source if r["event"] == ordinary_event
                                and r.get("conclusion") != "action_required"]
    return max(candidates, key=lambda r: r["id"]) if candidates else None


def workflow(name, ref, sha, correlation, ordinary_event, **inputs):
    label = f"{'CI' if name == 'ci.yml' else 'Release'} {correlation}"

    def find():
        runs = api(f"actions/workflows/{name}/runs?head_sha={sha}&per_page=100")["workflow_runs"]
        return matching_run(runs, sha, label, ref, ordinary_event)

    run = find()
    if run is None:
        args = ["workflow", "run", name, "--repo", REPO, "--ref", ref,
                "-f", f"release_run_id={correlation}"]
        for key, value in inputs.items():
            args += ["-f", f"{key}={value}"]
        gh(*args)
        # Never repeat a dispatch whose API outcome or listing visibility is uncertain.
        for _ in range(18):
            time.sleep(10)
            run = find()
            if run:
                break
        if run is None:
            raise RuntimeError(f"dispatch not visible: {label}; inspect before resuming")
    if run["status"] != "completed":
        gh("run", "watch", str(run["id"]), "--repo", REPO, "--exit-status", "--interval", "15")
    run = api(f"actions/runs/{run['id']}")
    if run["conclusion"] != "success" or run["head_sha"] != sha:
        raise RuntimeError(f"required workflow failed: {run['html_url']}")
    return run["html_url"]


def verify_patch(source, head, expected):
    if command("git", "rev-parse", f"{head}^") != source:
        raise ValueError("automation branch must contain exactly one patch on qualified source")
    changed = command("git", "diff", "--name-only", source, head).splitlines()
    if set(changed) != set(expected):
        raise ValueError("automation PR contains unexpected files")
    for name, content in expected.items():
        # check_output.strip would erase significant final whitespace here.
        actual = subprocess.check_output(["git", "show", f"{head}:{name}"], text=True)
        if actual != content:
            raise ValueError(f"automation PR differs from deterministic metadata patch: {name}")


def ensure_tags(version, sha):
    missing = []
    for tag in (f"go/v{version}", f"v{version}"):
        refs = command("git", "ls-remote", "--tags", "origin", f"refs/tags/{tag}", f"refs/tags/{tag}^{{}}")
        if refs:
            target = refs.splitlines()[-1].split()[0]
            if target != sha:
                raise ValueError(f"immutable tag {tag} points elsewhere")
        else:
            missing.append(tag)
    if missing:
        for tag in missing:
            command("git", "tag", "-a", tag, sha, "-m", f"Mektup {tag}")
        command("git", "push", "--atomic", "origin", *(f"refs/tags/{t}" for t in missing))


def main(run_id):
    run = api(f"actions/runs/{run_id}")
    validate_run(run)
    with tempfile.TemporaryDirectory() as scratch:
        scratch = Path(scratch)
        gh("run", "download", str(run_id), "--repo", REPO,
           "--name", f"codex-qualification-{run_id}", "--dir", str(scratch / "receipt"))
        receipt_path = scratch / "receipt/receipt.json"
        if receipt_path.stat().st_size > 16384:
            raise ValueError("oversized qualification receipt")
        receipt = json.loads(receipt_path.read_text())
        stable(receipt["codexVersion"])
        # The upstream release belongs to OpenAI, not this repository.
        official = json.loads(gh("api", f"repos/openai/codex/releases/tags/rust-v{receipt['codexVersion']}"))
        version = validate_receipt(run, receipt, official)
        latest = json.loads(gh("api", "repos/openai/codex/releases/latest"))
        if latest["tag_name"] != f"rust-v{version}":
            print("No publication: qualification is not the latest stable Codex.")
            return
        command("git", "fetch", "origin", "main")
        source = receipt["mektupSourceCommit"]
        branch = f"automation/codex-{version}-{source[:12]}"
        prs = json.loads(gh("pr", "list", "--repo", REPO, "--head", branch,
                            "--base", "main", "--state", "all", "--json",
                            "number,state,headRefOid,mergeCommit,baseRefOid,headRepositoryOwner"))
        if len(prs) > 1:
            raise ValueError("ambiguous automation PR history")
        if not prs and version in tested(Path.cwd()):
            print(f"No publication: Codex {version} is already recorded as tested.")
            return
        base = scratch / "source"
        for name in FILES:
            target = base / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(subprocess.check_output(["git", "show", f"{source}:{name}"], text=True))
        plan = render(base, version, run_id, receipt)
        if plan is None:
            print("No publication: qualification source already records this version.")
            return
        previous, release, expected = plan
        if not prs:
            if command("git", "rev-parse", "origin/main") != source:
                raise ValueError("main advanced since qualification; qualify the new source first")
            published = api("releases/latest")
            if published["tag_name"] != f"v{previous}":
                raise ValueError("source version does not match the latest published Mektup")
            existing = command("git", "ls-remote", "origin", f"refs/heads/{branch}")
            if existing:
                head = existing.split()[0]
                command("git", "fetch", "origin", head)
                verify_patch(source, head, expected)
            else:
                command("git", "checkout", "-b", branch, source)
                for name, content in expected.items():
                    Path(name).write_text(content)
                command("git", "diff", "--check")
                command("git", "add", "--", *expected)
                command("git", "commit", "-m", f"chore: qualify Codex {version} for Mektup {release}")
                command("git", "push", "origin", f"HEAD:refs/heads/{branch}")
            body = scratch / "pr.md"
            body.write_text(f"Exact Codex {version} passed [clean-source qualification]({run['html_url']}). "
                            f"Record it as tested for Mektup {release}.\n\n"
                            "Generated metadata-only patch; runtime, dependencies and contract are unchanged. "
                            "The automation verifies this exact diff, runs required PR and main checks, "
                            "then creates immutable paired tags and invokes the existing release workflow.\n")
            gh("pr", "create", "--repo", REPO, "--head", branch, "--base", "main",
               "--title", f"chore: qualify Codex {version} for Mektup {release}", "--body-file", str(body))
            prs = json.loads(gh("pr", "list", "--repo", REPO, "--head", branch, "--state", "open",
                                "--json", "number,state,headRefOid,mergeCommit,baseRefOid,headRepositoryOwner"))
        pr = prs[0]
        if pr["headRepositoryOwner"]["login"] != "agensfield":
            raise ValueError("automation PR origin changed")
        command("git", "fetch", "origin", pr["headRefOid"])
        verify_patch(source, pr["headRefOid"], expected)
        if pr["state"] == "CLOSED":
            raise ValueError("automation PR was closed without merging; do not reopen automatically")
        if pr["state"] != "MERGED":
            workflow("ci.yml", branch, pr["headRefOid"], f"qualification-{run_id}-pr", "pull_request")
            if command("git", "ls-remote", "origin", "refs/heads/main").split()[0] != source:
                raise ValueError("main changed while validating the PR")
            gh("pr", "merge", str(pr["number"]), "--repo", REPO, "--merge", "--delete-branch",
               "--match-head-commit", pr["headRefOid"])
        merged = json.loads(gh("pr", "view", str(pr["number"]), "--repo", REPO, "--json", "mergeCommit"))
        sha = merged["mergeCommit"]["oid"]
        workflow("ci.yml", "main", sha, f"qualification-{run_id}-main", "push")
        command("git", "fetch", "origin", "main")
        if command("git", "rev-parse", "origin/main") != sha:
            raise ValueError("main advanced before tagging; stop instead of releasing another commit")
        ensure_tags(release, sha)
        url = workflow("release.yml", f"v{release}", sha, f"qualification-{run_id}-release", "push", tag=f"v{release}")
        notes = scratch / "release.md"
        notes.write_text(f"Mektup {release} records exact Codex {version} as tested. "
                         "The qualification PR is metadata-only. Other changes since the previous release are linked below.\n\n"
                         f"- [Clean-source qualification]({run['html_url']})\n"
                         f"- [Metadata PR](https://github.com/{REPO}/pull/{pr['number']})\n"
                         f"- [All changes since v{previous}](https://github.com/{REPO}/compare/v{previous}...v{release})\n"
                         f"- [Packaging, archive/SBOM verification and fresh Go install]({url})\n\n"
                         f"Release source: `{sha}`. Includes four platform archives, SHA-256 checksums "
                         "and one SBOM per archive. Host installation is outside this automation.\n")
        gh("release", "edit", f"v{release}", "--repo", REPO, "--notes-file", str(notes))
        print(f"Published Mektup {release}: {url}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--qualification-run", type=int, required=True)
    args = parser.parse_args()
    if os.environ.get("GITHUB_ACTIONS") != "true" or os.environ.get("GITHUB_REPOSITORY") != REPO:
        parser.error("publication runs only in this repository's trusted GitHub Actions workflow")
    main(args.qualification_run)
