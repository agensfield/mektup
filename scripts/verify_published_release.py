#!/usr/bin/env python3
"""Verify published archives/SBOMs, native metadata and a fresh public Go install."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile
import tempfile


def validate_manifest(root, version):
    expected = {f"mektup_{version}_{system}_{arch}.tar.gz" for system in ("darwin", "linux")
                for arch in ("amd64", "arm64")}
    expected |= {name + ".sbom.json" for name in tuple(expected)}
    entries = {}
    for line in (root / "checksums.txt").read_text().splitlines():
        digest, name = line.split()
        if name not in expected or name in entries or not re.fullmatch("[0-9a-f]{64}", digest):
            raise ValueError("unexpected or duplicate checksum entry")
        if hashlib.sha256((root / name).read_bytes()).hexdigest() != digest:
            raise ValueError(f"published checksum mismatch: {name}")
        entries[name] = digest
    if set(entries) != expected:
        raise ValueError("release must contain all four archives and their SBOMs")
    return entries


def validate_version(value, version, sha, expected_tested, install_kind):
    if (value["version"] != version or value["contract_version"] != "1.0.9"
            or value["install_kind"] != install_kind
            or value["tested_codex_versions"] != expected_tested
            or (install_kind == "github-release" and value["commit"] != sha)):
        raise ValueError("published binary metadata differs from the release source")


def verify(tag):
    if not re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+", tag):
        raise ValueError("expected a stable release tag")
    version = tag[1:]
    sha = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    from release_metadata import tested
    expected_tested = tested(Path.cwd())
    with tempfile.TemporaryDirectory() as temporary:
        root = Path(temporary)
        subprocess.run(["gh", "release", "download", tag, "--repo", "agensfield/mektup",
                        "--dir", str(root)], check=True)
        digests = validate_manifest(root, version)
        native = root / "native-mektup"
        for name in digests:
            if name.endswith(".json"):
                json.loads((root / name).read_text())
                continue
            with tarfile.open(root / name) as archive:
                member = archive.getmember("mektup")
                if not member.isfile() or not member.mode & 0o111:
                    raise ValueError("archive binary is not an executable regular file")
                binary = root / (name + ".binary")
                binary.write_bytes(archive.extractfile(member).read())
                info = subprocess.check_output(["go", "version", "-m", str(binary)], text=True)
                if f"vcs.revision={sha}" not in info or "vcs.modified=false" not in info:
                    raise ValueError("archive does not identify the exact clean release commit")
                if name == f"mektup_{version}_linux_amd64.tar.gz":
                    binary.chmod(0o755)
                    native = binary
        value = json.loads(subprocess.check_output([str(native), "version", "--json"], text=True))
        validate_version(value, version, sha, expected_tested, "github-release")
        env = dict(os.environ, GOBIN=str(root / "bin"), GOMODCACHE=str(root / "mod"),
                   GOCACHE=str(root / "build"), GOPROXY="https://proxy.golang.org", GOSUMDB="sum.golang.org")
        subprocess.run(["go", "install", f"github.com/agensfield/mektup/go/cmd/mektup@{tag}"],
                       env=env, check=True)
        installed = root / "bin/mektup"
        value = json.loads(subprocess.check_output([str(installed), "version", "--json"], text=True))
        validate_version(value, version, sha, expected_tested, "go-install")
        subprocess.run([str(installed), "--help"], check=True, stdout=subprocess.DEVNULL)
        print(json.dumps({"status": "PASS", "tag": tag, "source": sha, "archives": 4,
                          "sboms": 4, "nativeLinuxMetadata": "PASS", "freshPublicGoInstall": "PASS"}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tag")
    verify(parser.parse_args().tag)
