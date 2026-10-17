#!/usr/bin/env bash
# Copies the stream API that the crate calls the server with, from the moetemp/api commit that
# `crates/streams/protos/api_stream/API_COMMIT` names. sdk-rust vendors none of it, so the crate
# generates these messages itself and calls the notifier RPCs by method path. Each file is copied
# as it is. CI runs it and fails on a diff.
#
#   scripts/sync-stream-api.sh [commit]
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
out="$root/crates/streams/protos/api_stream"
commit=${1:-$(cat "$out/API_COMMIT")}
src="https://raw.githubusercontent.com/moetemp/api/$commit"

rm -rf "$out/temporal"
for path in \
  temporal/api/stream/v1/message.proto \
  temporal/api/enums/v1/stream.proto \
  temporal/api/workflowservice/v1/stream_request_response.proto; do
  mkdir -p "$out/$(dirname "$path")"
  curl -fsSL "$src/$path" -o "$out/$path"
done
echo "$commit" >"$out/API_COMMIT"
echo "copied the stream API at $commit"
