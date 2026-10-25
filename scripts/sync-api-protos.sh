#!/usr/bin/env bash
# Copies the Temporal API protos that the crate's own protos import, and the protos those import,
# from the sdk-rust release of the `temporalio-common` version in Cargo.lock. The copy is for
# imports only. The crate maps `.temporal.api` to `temporalio-common`, so it generates no API types
# of its own.
#
#   scripts/sync-api-protos.sh [rev]
#
# With no rev, it uses the tag `v<version>` of that version. CI runs it and fails on a diff, so the
# copy always matches the API types the crate builds on. With `SDK_RUST_DIR` set, it copies from
# that sdk-rust checkout instead, for a version that is not released yet.
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
if [ -n "${SDK_RUST_DIR:-}" ]; then
  rev=$(git -C "$SDK_RUST_DIR" rev-parse HEAD)
  local_src="$SDK_RUST_DIR/crates/protos/protos/api_upstream"
else
  version=$(awk '/^name = "temporalio-common"$/ { getline; gsub(/version = |"/, ""); print }' \
    "$root/Cargo.lock")
  [ -n "${1:-}$version" ] || { echo "no temporalio-common in Cargo.lock" >&2; exit 1; }
  rev=${1:-v$version}
  src="https://raw.githubusercontent.com/temporalio/sdk-rust/$rev/crates/protos/protos/api_upstream"
fi
protos="$root/crates/streams/protos"
out="$protos/api_include"

imports() {
  sed -n 's/^import \(public \)\{0,1\}"\(temporal\/api\/[^"]*\)";.*/\2/p' "$1"
}

rm -rf "$out"
mkdir -p "$out"
todo=$(find "$protos" -path "$out" -prune -o -name '*.proto' -print | while read -r f; do
  imports "$f"
done | sort -u)
seen=""
while [ -n "$todo" ]; do
  next=""
  for path in $todo; do
    case " $seen " in *" $path "*) continue ;; esac
    # The crate's own API protos, such as the stream service, are not in sdk-rust.
    [ -f "$protos/$path" ] && continue
    seen="$seen $path"
    mkdir -p "$out/$(dirname "$path")"
    if [ -n "${local_src:-}" ]; then
      cp "$local_src/$path" "$out/$path"
    else
      curl -fsSL "$src/$path" -o "$out/$path"
    fi
    next="$next $(imports "$out/$path")"
  done
  todo=$(echo "$next" | tr ' ' '\n' | sort -u | tr '\n' ' ' | sed 's/^ *//')
done
echo "copied$(echo "$seen" | tr ' ' '\n' | sed '/^$/d' | sed 's/^/ /' | tr -d '\n') at $rev"
