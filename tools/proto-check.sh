#!/usr/bin/env bash
# Generate the protobuf output into a fresh tree and compare the full generated
# file set with the checked-in files. Detects changed files, missing files and
# orphaned generated files. Hand-written files in generated directories (tests,
# for example) are preserved and ignored.
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ ! -x web/node_modules/.bin/buf ]]; then
  (cd web && pnpm install --frozen-lockfile)
fi
export PATH="$PWD/web/node_modules/.bin:$PATH"

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

web/node_modules/.bin/buf generate -o "$tmp" >/dev/null

status=0

compare_tree() {
  local generated_root="$1"
  local repo_root="$2"
  local glob="$3"

  # Every freshly generated file must exist and be byte-identical.
  while IFS= read -r -d '' file; do
    file="${file#"$tmp"/}"
    if [[ ! -f "$file" ]]; then
      echo "proto-check: missing generated file: $file" >&2
      status=1
    elif ! cmp -s "$tmp/$file" "$file"; then
      echo "proto-check: stale generated file: $file" >&2
      status=1
    fi
  done < <(cd "$tmp" && find "$generated_root" -type f -print0)

  # Every checked-in file that looks generated must still be produced.
  while IFS= read -r -d '' file; do
    if [[ ! -f "$tmp/$file" ]]; then
      echo "proto-check: orphaned generated file: $file" >&2
      status=1
    fi
  done < <(find "$repo_root" -type f -name "$glob" -print0)
}

compare_tree "internal/pb" "internal/pb" "*.pb.go"
compare_tree "web/src/gen" "web/src/gen" "*"

if [[ "$status" -eq 0 ]]; then
  echo "proto-check: generated Go and TypeScript are up to date (full file set)"
fi
exit "$status"
