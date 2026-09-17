#!/bin/sh

# Validate the language-neutral contract documents with a pinned Draft
# 2020-12 validator. Positive fixtures are checked against their schema and
# every negative fixture must be rejected. The implementation/conformance
# jobs remain responsible for behavioral checks.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
contracts="$root/contracts"
schema_dir="$contracts/schemas"
fixture_dir="$contracts/fixtures/v1"
validator="$root/node_modules/.bin/ajv"

if [ ! -d "$schema_dir" ] || [ ! -d "$fixture_dir" ]; then
	printf '%s\n' "contracts/schemas and contracts/fixtures/v1 are required" >&2
	exit 1
fi
command -v jq >/dev/null 2>&1 || {
	printf '%s\n' "jq is required for contract JSON syntax validation" >&2
	exit 1
}

if [ ! -x "$validator" ]; then
	printf '%s\n' "contract validator is missing; run npm ci --ignore-scripts --no-audit --no-fund" >&2
	exit 1
fi

json_count=$(find "$contracts" -type f -name '*.json' -print | wc -l | tr -d ' ')
if [ "$json_count" -eq 0 ]; then
	printf '%s\n' "contracts/: no JSON documents found" >&2
	exit 1
fi

json_files=$(find "$contracts" -type f -name '*.json' -print | sort)
printf '%s\n' "$json_files" | while IFS= read -r file; do
	if ! jq -e . "$file" >/dev/null; then
		printf 'invalid JSON: %s\n' "$file" >&2
		exit 1
	fi
done
printf 'validated JSON syntax for %s contract documents\n' "$json_count"

validate_fixture() {
	schema=$1
	data=$2
	# Ajv needs every sibling schema available for relative $ref values, but
	# the selected root schema must not be loaded twice under its $id.
	shift 2
	set --
	for candidate in "$schema_dir"/*.schema.json; do
		[ "$candidate" = "$schema" ] || set -- "$@" -r "$candidate"
	done
	"$validator" validate \
		--spec=draft2020 --strict=false \
		-s "$schema" "$@" -d "$data" >/dev/null
}

for data in "$fixture_dir"/*.json; do
	[ -f "$data" ] || continue
	case "$(basename "$data")" in
		*.invalid.json|manifest.json) continue ;;
		control-*.json) schema="$schema_dir/control-v1.schema.json" ;;
		envelope-*.json) schema="$schema_dir/mektup-envelope-v1.schema.json" ;;
		error-*.json) schema="$schema_dir/error-v1.schema.json" ;;
		event-*.json) schema="$schema_dir/event-v1.schema.json" ;;
		receipt-summary-*.json) schema="$schema_dir/receipt-summary-v1.schema.json" ;;
		receipt-*.json) schema="$schema_dir/receipt-v1.schema.json" ;;
		warning-*.json) schema="$schema_dir/warning-v1.schema.json" ;;
		*)
			printf 'no schema mapping for positive fixture: %s\n' "$data" >&2
			exit 1
			;;
	esac
	validate_fixture "$schema" "$data"
	printf 'valid fixture: %s\n' "$data"
done

negative_count=0
for data in "$fixture_dir"/*.invalid.json; do
	[ -f "$data" ] || continue
	negative_count=$((negative_count + 1))
	case "$(basename "$data")" in
		control-*.invalid.json) schema="$schema_dir/control-v1.schema.json" ;;
		envelope-*.invalid.json) schema="$schema_dir/mektup-envelope-v1.schema.json" ;;
		event-*.invalid.json) schema="$schema_dir/event-v1.schema.json" ;;
		receipt-summary-*.invalid.json) schema="$schema_dir/receipt-summary-v1.schema.json" ;;
		receipt-*.json) schema="$schema_dir/receipt-v1.schema.json" ;;
		*)
			printf 'no schema mapping for negative fixture: %s\n' "$data" >&2
			exit 1
			;;
	esac
	if validate_fixture "$schema" "$data" >/dev/null 2>&1; then
		printf 'negative fixture unexpectedly accepted: %s\n' "$data" >&2
		exit 1
	fi
	printf 'rejected negative fixture: %s\n' "$data"
done

if [ "$negative_count" -eq 0 ]; then
	printf '%s\n' "contracts/fixtures/v1 contains no *.invalid.json documents" >&2
	exit 1
fi
