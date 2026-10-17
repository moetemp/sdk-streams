#!/usr/bin/env bash
# Copies the Nexus progress API and the Nexus codegen inputs from the moetemp/api commit that
# `crates/streams/protos/api_nexus/API_COMMIT` names. sdk-rust vendors only the callback's
# `progress_operation` field, so the crate generates these messages itself and calls
# `ExecuteSyncNexusOperation` by method path. The WIT and YAML go to `crates/streams/nexus`, where
# a lang's Nexus codegen reads them. Each file is copied as it is. CI runs it and fails on a diff.
#
#   scripts/sync-nexus-api.sh [commit]
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
out="$root/crates/streams/protos/api_nexus"
nexus="$root/crates/streams/nexus"
commit=${1:-$(cat "$out/API_COMMIT")}
src="https://raw.githubusercontent.com/moetemp/api/$commit"

rm -rf "$out/temporal"
for path in \
  temporal/api/nexus/v1/progress.proto \
  temporal/api/notificationservice/v1/progress.proto \
  temporal/api/workflowservice/v1/nexus_request_response.proto; do
  mkdir -p "$out/$(dirname "$path")"
  curl -fsSL "$src/$path" -o "$out/$path"
done
for name in \
  notification-service.wit \
  workflow-service.wit \
  temporal-proto-models-nexusrpc.yaml; do
  curl -fsSL "$src/nexus/$name" -o "$nexus/$name"
done
echo "$commit" >"$out/API_COMMIT"
echo "copied the Nexus API at $commit"
