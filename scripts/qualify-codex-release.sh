#!/usr/bin/env bash
set -euo pipefail

version="${1:-}"
if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.]+)?$ ]]; then
  printf 'usage: %s <published Codex version, e.g. 0.157.0>\n' "$0" >&2
  exit 2
fi
for tool in curl docker jq; do
  command -v "$tool" >/dev/null || { printf 'required tool missing: %s\n' "$tool" >&2; exit 2; }
done

repo_root="$(cd "$(dirname "$0")/.." && pwd -P)"
case "$(docker info --format '{{.Architecture}}')" in
  aarch64|arm64) codex_arch=aarch64 ;;
  x86_64|amd64) codex_arch=x86_64 ;;
  *) printf 'Docker host architecture is unsupported\n' >&2; exit 2 ;;
esac

release_headers=()
if [[ -n "${GH_TOKEN:-}" ]]; then
  release_headers=(-H "Authorization: Bearer $GH_TOKEN")
fi
release_json="$(curl --fail --location --silent --show-error --retry 3 "${release_headers[@]}" \
  "https://api.github.com/repos/openai/codex/releases/tags/rust-v${version}")"
jq -e --arg tag "rust-v$version" '.tag_name == $tag and .draft == false' <<<"$release_json" >/dev/null
asset_name="codex-${codex_arch}-unknown-linux-musl.tar.gz"
asset="$(jq -cer --arg name "$asset_name" '.assets[] | select(.name == $name) | {url: .browser_download_url, digest: .digest}' <<<"$release_json")"
asset_url="$(jq -er '.url' <<<"$asset")"
asset_digest="$(jq -er '.digest | select(startswith("sha256:")) | ltrimstr("sha256:") | select(test("^[0-9a-f]{64}$"))' <<<"$asset")"

image="mektup-compat:${version}-${codex_arch}-$$"
scratch="$(mktemp -d "${TMPDIR:-/tmp}/mektup-compat.XXXXXXXX")"
container="mektup-compat-$$"
# Invoked by the EXIT trap on successful and failed runs.
# shellcheck disable=SC2329
cleanup() {
  docker rm --force "$container" >/dev/null 2>&1 || true
  docker image rm "$image" >/dev/null 2>&1 || true
  rm -r -- "$scratch"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

docker build --quiet --file "$repo_root/tests/compat/Dockerfile" \
  --build-arg "CODEX_VERSION=$version" \
  --build-arg "CODEX_ARCH=$codex_arch" \
  --build-arg "CODEX_ASSET_URL=$asset_url" \
  --build-arg "CODEX_ASSET_SHA256=$asset_digest" \
  --tag "$image" "$repo_root" >/dev/null

set +e
docker run --name "$container" --rm --init --network none \
  --cap-drop ALL --security-opt no-new-privileges \
  --env "MEKTUP_ACCEPT_CODEX_VERSION=$version" \
  --env MEKTUP_ACCEPT_CODEX_BINARY=/usr/local/bin/codex \
  "$image" bash -euc '
    go test -count=1 -race ./...
    go run ./cmd/mektup-conformance
    MEKTUP_ACCEPT_CALLBACKS=1 MEKTUP_COMPAT_CONTAINER=1 \
      go test -count=1 -race -run "^TestIsolated(MultiClientServerRequest|MessagingLifecycle)$" -v ./internal/liveacceptance
  ' >"$scratch/test.log" 2>&1
result=$?
set -e

if [[ "$result" -ne 0 ]]; then
  tail -n 80 "$scratch/test.log" >&2
fi
source_dirty=false
if [[ -n "$(git -C "$repo_root" status --porcelain)" ]]; then
  source_dirty=true
fi
status=FAIL
if [[ "$result" -eq 0 && "$source_dirty" == false ]]; then
  status=PASS
elif [[ "$result" -eq 0 ]]; then
  status=DEVELOPMENT_PASS
fi
receipt="$(jq -nc \
  --arg version "$version" --arg digest "sha256:$asset_digest" \
  --arg platform "linux/$codex_arch" --arg status "$status" \
  --arg source "$(git -C "$repo_root" rev-parse HEAD)" \
  --argjson dirty "$source_dirty" \
  --argjson checksPassed "$(if [[ "$result" -eq 0 ]]; then printf true; else printf false; fi)" \
  '{status:$status, qualified:($status == "PASS"), checksPassed:$checksPassed, schema:"mektup/codex-qualification/v1", codexVersion:$version, codexAssetSha256:$digest, platform:$platform, mektupSourceCommit:$source, sourceDirty:$dirty, checks:["all Go race tests", "wire conformance", "real app-server lifecycle and all owned RPC methods", "built-in local identity and doctor on the managed socket", "real body, reply, custody wait and restart", "real app-server proxy", "user-input and approval callback passivity"]}')"
printf '%s\n' "$receipt"
if [[ -n "${MEKTUP_COMPAT_REPORT_DIR:-}" ]]; then
  install -d -m 0700 "$MEKTUP_COMPAT_REPORT_DIR"
  cp "$scratch/test.log" "$MEKTUP_COMPAT_REPORT_DIR/test.log"
  printf '%s\n' "$receipt" >"$MEKTUP_COMPAT_REPORT_DIR/receipt.json"
fi
exit "$result"
