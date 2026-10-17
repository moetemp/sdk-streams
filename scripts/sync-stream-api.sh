#!/usr/bin/env bash
# Copies the stream API that the crate calls the server with, from the moetemp/api commit that
# `crates/streams/protos/api_stream/API_COMMIT` names. sdk-rust vendors none of it, so the crate
# generates these messages itself and calls the notifier RPCs by method path. The copy holds
# `stream/v1/message.proto` and `enums/v1/stream.proto` as they are, and the request and response
# messages of the notifier RPCs, cut from `workflowservice/v1/request_response.proto` into
# `workflowservice/v1/stream_notifier.proto`. CI runs it and fails on a diff.
#
#   scripts/sync-stream-api.sh [commit]
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
out="$root/crates/streams/protos/api_stream"
commit=${1:-$(cat "$out/API_COMMIT")}
src="https://raw.githubusercontent.com/moetemp/api/$commit"
messages="AttachStreamCallback WaitStreamCallback DetachStreamCallback NotifyStream
  DescribeStreamNotifier PollStreamNotifier NotifyStreams"

rm -rf "$out/temporal"
for path in temporal/api/stream/v1/message.proto temporal/api/enums/v1/stream.proto; do
  mkdir -p "$out/$(dirname "$path")"
  curl -fsSL "$src/$path" -o "$out/$path"
done

rr=$(curl -fsSL "$src/temporal/api/workflowservice/v1/request_response.proto")
notifier="$out/temporal/api/workflowservice/v1/stream_notifier.proto"
mkdir -p "$(dirname "$notifier")"
{
  cat <<'HEAD'
// The request and response messages of the stream notifier RPCs, cut from
// temporal/api/workflowservice/v1/request_response.proto by scripts/sync-stream-api.sh.
syntax = "proto3";

package temporal.api.workflowservice.v1;

import "google/protobuf/timestamp.proto";
import "temporal/api/common/v1/message.proto";
import "temporal/api/failure/v1/message.proto";
import "temporal/api/stream/v1/message.proto";
HEAD
  for rpc in $messages; do
    for kind in Request Response; do
      echo
      echo "$rr" | awk -v m="message $rpc$kind {" '$0 == m {p = 1} p {print} p && /^}$/ {exit}'
    done
  done
} >"$notifier"
echo "$commit" >"$out/API_COMMIT"
echo "copied the stream API at $commit"
