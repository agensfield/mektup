#!/bin/sh

# Validate the language-neutral contract documents without requiring a
# third-party schema runner. JSON syntax is the first portable gate; the
# implementation/conformance jobs remain responsible for behavioral checks.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
contracts="$root/contracts"

if [ ! -d "$contracts" ]; then
	printf '%s\n' "contracts/: directory is required for contract validation" >&2
	exit 1
fi

count=0
while IFS= read -r file; do
	count=$((count + 1))
	if ! jq -e . "$file" >/dev/null; then
		printf 'invalid JSON: %s\n' "$file" >&2
		exit 1
	fi
done <<EOF
$(find "$contracts" -type f -name '*.json' -print | sort)
EOF

if [ "$count" -eq 0 ]; then
	printf '%s\n' "contracts/: no JSON documents found" >&2
	exit 1
fi

printf 'validated %s contract JSON documents\n' "$count"
