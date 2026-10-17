#!/usr/bin/env bash
# Starts the Redis servers the stream store tests run against: one standalone server, and a
# cluster of three primaries with one replica each, so the driver tests can fail a primary over.
# Uses a local redis-server when there is one, and docker on a Linux host otherwise, since a
# cluster's nodes must reach each other on the addresses they announce. STREAMS_CLUSTER_BASE moves
# the six cluster ports.
#
# It also starts Apache Kvrocks, a Redis protocol server on disk, for a test-only conformance lane,
# from a local kvrocks binary or the apache/kvrocks image. It skips Kvrocks when it has neither.
# STREAMS_KVROCKS_PORT moves it.
#
#   STREAMS_REDIS_URL=redis://localhost:6384/0
#   STREAMS_REDIS_CLUSTER_URL=redis://localhost:7101/0
#   STREAMS_KVROCKS_URL=redis://localhost:6666/0
set -euo pipefail

STANDALONE=${STREAMS_STANDALONE_PORT:-6384}
BASE=${STREAMS_CLUSTER_BASE:-7101}
KVROCKS=${STREAMS_KVROCKS_PORT:-6666}
CLUSTER=($BASE $((BASE + 1)) $((BASE + 2)) $((BASE + 3)) $((BASE + 4)) $((BASE + 5)))
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
  redis-cli -p "${CLUSTER[5]}" ping >/dev/null 2>&1 && break
  sleep 0.1
done
if ! redis-cli -p "${CLUSTER[0]}" cluster info | grep -q cluster_state:ok; then
  nodes=()
  for port in "${CLUSTER[@]}"; do
    nodes+=("127.0.0.1:$port")
  done
  redis-cli --cluster create "${nodes[@]}" --cluster-replicas 1 --cluster-yes >/dev/null
  for _ in $(seq 50); do
    redis-cli -p "${CLUSTER[0]}" cluster info | grep -q cluster_state:ok && break
    sleep 0.1
  done
fi

start_kvrocks() {
  if redis-cli -p "$KVROCKS" ping >/dev/null 2>&1; then
    return
  fi
  if command -v kvrocks >/dev/null; then
    mkdir -p "$DATA/kvrocks-$KVROCKS"
    kvrocks --port "$KVROCKS" --dir "$DATA/kvrocks-$KVROCKS" --daemonize yes \
      --log-dir "$DATA/kvrocks-$KVROCKS" >/dev/null
  elif command -v docker >/dev/null; then
    docker run -d --rm --name "streams-kvrocks-$KVROCKS" -p "$KVROCKS:6666" apache/kvrocks \
      >/dev/null
  else
    return 1
  fi
  for _ in $(seq 100); do
    redis-cli -p "$KVROCKS" ping >/dev/null 2>&1 && return
    sleep 0.1
  done
  return 1
}

echo "STREAMS_REDIS_URL=redis://localhost:$STANDALONE/0"
echo "STREAMS_REDIS_CLUSTER_URL=redis://localhost:${CLUSTER[0]}/0"
if start_kvrocks; then
  echo "STREAMS_KVROCKS_URL=redis://localhost:$KVROCKS/0"
else
  echo "Kvrocks did not start, so its lane does not run." >&2
fi
