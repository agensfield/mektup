#!/usr/bin/env bash
set -euo pipefail
# Explicit opt-in: the arguments are locally-built candidate binaries, not installs.
if [[ $# != 2 ]]; then
  printf 'usage: %s <local-mektup-binary> <remote-mektup-binary>\n' "$0" >&2
  exit 2
fi
repo_root="$(cd "$(dirname "$0")/.." && pwd -P)"
arguments=( \
  --local-binary "$1" --remote-binary "$2" \
  --remote-host "${MEKTUP_ACCEPT_REMOTE_HOST:-devbox}" \
  --return-host "${MEKTUP_ACCEPT_RETURN_HOST:-seele}" )
if [[ -n "${MEKTUP_ACCEPT_REPORT_DIR:-}" ]]; then
  arguments+=(--report-dir "$MEKTUP_ACCEPT_REPORT_DIR")
fi
exec python3 "$repo_root/tests/crosshost/fixture.py" "${arguments[@]}"
