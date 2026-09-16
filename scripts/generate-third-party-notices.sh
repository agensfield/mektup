#!/bin/sh

# Generate the notices shipped with release archives. The inventory is based
# on packages that are actually linked by the Go module, not every module in
# the module graph. GOPROXY=off makes a missing local module cache a hard
# failure, so archive-time generation cannot silently fetch moving inputs.
set -eu

export LC_ALL=C
root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
module_dir="$root/go"
output=${1:-"$root/THIRD_PARTY_NOTICES.md"}

command -v go >/dev/null 2>&1 || { printf '%s\n' 'go is required' >&2; exit 1; }
command -v jq >/dev/null 2>&1 || { printf '%s\n' 'jq is required' >&2; exit 1; }
[ -f "$module_dir/go.mod" ] || { printf '%s\n' 'go/go.mod is required' >&2; exit 1; }

tmp=$(mktemp "${output}.tmp.XXXXXX")
deps_json=$(mktemp)
modules_json=$(mktemp)
runtime_modules=$(mktemp)
direct_modules=$(mktemp)
cleanup() {
	rm -f "$tmp" "$deps_json" "$modules_json" "$runtime_modules" "$direct_modules"
}
trap cleanup EXIT HUP INT TERM

(
	cd "$module_dir"
	GOPROXY=off GOSUMDB=off go list -deps -json ./... >"$deps_json"
	GOPROXY=off GOSUMDB=off go list -m -json all >"$modules_json"
)

jq -s -r '
	map(select(.Module != null and (.Module.Main != true)))
	| map(.Module)
	| unique_by(.Path + "@" + (.Version // ""))
	| sort_by(.Path, .Version)
	| .[] | [.Path, (.Version // ""), (.Dir // "")] | @tsv
' "$deps_json" >"$runtime_modules"

jq -s -r '
	.[] | select(.Path != "github.com/agensfield/mektup/go" and (.Indirect != true)) | .Path
' "$modules_json" | sort -u >"$direct_modules"

classify_license() {
	file=$1
	base=$(basename "$file")
	case "$base" in
		# This is an attribution URL, not a license text. It is retained in
		# full because modernc.org/memory ships it alongside its licenses.
		LICENSE-LOGO) printf '%s\n' 'Attribution notice'; return 0 ;;
		# This file intentionally contains several upstream notices. Its
		# complete text is copied below rather than reduced to one SPDX ID.
		LICENSE-3RD-PARTY.md) printf '%s\n' 'Bundled upstream notices'; return 0 ;;
	esac
	if grep -Eiq 'MIT License|MIT license|opensource.org/licenses/mit' "$file"; then
		printf '%s\n' 'MIT'
	elif grep -Eiq 'Apache License' "$file" && grep -Eiq 'Version 2\.0' "$file"; then
		printf '%s\n' 'Apache-2.0'
	elif grep -Eiq 'Redistribution and use in source and binary forms' "$file" && grep -Eiq 'Neither the name' "$file"; then
		printf '%s\n' 'BSD-3-Clause'
	elif grep -Eiq 'Redistribution and use in source and binary forms' "$file"; then
		printf '%s\n' 'BSD-2-Clause'
	elif grep -Eiq 'ISC License|Permission to use, copy, modify, and/or distribute' "$file"; then
		printf '%s\n' 'ISC'
	elif grep -Eiq 'public domain' "$file"; then
		printf '%s\n' 'Public domain'
	else
		printf 'unknown or disallowed license text: %s\n' "$file" >&2
		return 1
	fi
}

{
	printf '%s\n\n' '# Third-party notices'
	printf '%s\n\n' 'This file is generated from the runtime-linked Go module inventory and is bundled with Mektup release archives. It preserves the license and attribution files shipped by each dependency.'
	printf '%s\n\n' 'The repository license is distributed separately as LICENSE.'

	while IFS="	" read -r module version dir; do
		[ -n "$module" ] || continue
		[ -d "$dir" ] || { printf 'module source directory is unavailable: %s\n' "$dir" >&2; exit 1; }
		if grep -Fqx "$module" "$direct_modules"; then
			edge='direct'
		else
			edge='transitive'
		fi
		printf '## %s %s\n\n' "$module" "$version"
		printf '%s\n\n' "- Dependency edge: $edge"
		printf '%s\n\n' "- Source module: $module"
		license_files=$(find "$dir" -maxdepth 1 -type f \( -iname 'license*' -o -iname 'copying*' -o -iname 'notice*' \) -print | sort)
		[ -n "$license_files" ] || { printf 'no license or notice files found for %s\n' "$module" >&2; exit 1; }
		printf '%s\n' "$license_files" | while IFS= read -r license_file; do
			[ -n "$license_file" ] || continue
			kind=$(classify_license "$license_file")
			printf '### %s (%s)\n\n' "$(basename "$license_file")" "$kind"
			cat "$license_file"
			printf '\n\n'
		done
	done <"$runtime_modules"
} >"$tmp"

mv "$tmp" "$output"
printf 'generated %s\n' "$output"
