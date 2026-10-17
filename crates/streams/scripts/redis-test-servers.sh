#!/usr/bin/env bash
# Starts the Redis servers the stream store tests run against: one standalone server, and a
# cluster of three primaries. Uses a local redis-server when there is one, and docker on a Linux
# host otherwise, since a cluster's nodes must reach each other on the addresses they announce.
#
#   STREAMS_REDIS_URL=redis://localhost:6384/0
#   STREAMS_REDIS_CLUSTER_URL=redis://localhost:7101/0
set -euo pipefail

STANDALONE=${STREAMS_STANDALONE_PORT:-6384}
CLUSTER=(7101 7102 7103)
DATA=${STREAMS_REDIS_DATA:-${TMPDIR:-/tmp}/streams-redis}
mkdir -p "$DATA"

start() {
  local port=$1
  shift
  if redis-cli -p "$port" ping >/dev/null 2>&1; then
    return
  fi
  if command -v redis-server >/dev/null; then
    mkdir -p "$DATA/$port"
    redis-server --port "$port" --save '' --appendonly no --daemonize yes \
      --dir "$DATA/$port" --logfile "$DATA/$port.log" "$@"
  else
    docker run -d --rm --name "streams-redis-$port" --network host redis:7 \
      redis-server --port "$port" --save '' --appendonly no "$@" >/dev/null
  fi
}

start "$STANDALONE"
for port in "${CLUSTER[@]}"; do
  start "$port" --cluster-enabled yes --cluster-config-file nodes.conf
done
for _ in $(seq 50); do
  redis-cli -p "${CLUSTER[2]}" ping >/dev/null 2>&1 && break
  sleep 0.1
done
if ! redis-cli -p "${CLUSTER[0]}" cluster info | grep -q cluster_state:ok; then
  nodes=()
  for port in "${CLUSTER[@]}"; do
    nodes+=("127.0.0.1:$port")
  done
  redis-cli --cluster create "${nodes[@]}" --cluster-replicas 0 --cluster-yes >/dev/null
  for _ in $(seq 50); do
    redis-cli -p "${CLUSTER[0]}" cluster info | grep -q cluster_state:ok && break
    sleep 0.1
  done
fi
echo "STREAMS_REDIS_URL=redis://localhost:$STANDALONE/0"
echo "STREAMS_REDIS_CLUSTER_URL=redis://localhost:${CLUSTER[0]}/0"
