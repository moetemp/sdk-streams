#!/usr/bin/env bash
# Starts the PostgreSQL servers the stream store tests run against, 14 and 17, in Docker on free
# ports, with TLS on from the image's self-signed certificate, and PgBouncer in transaction mode
# in front of 17 when its image pulls. Prints the variables to export. `stop` stops them.
# STREAMS_POSTGRES_NAME moves the container names, so two runs can share a host.
#
#   eval "$(crates/streams/scripts/postgres-test-server.sh)"
#   STREAMS_POSTGRES_URL=postgres://postgres:streams@localhost:<port>/postgres       (17)
#   STREAMS_POSTGRES_14_URL=postgres://postgres:streams@localhost:<port>/postgres    (14)
#   STREAMS_POSTGRES_TLS_URL=$STREAMS_POSTGRES_URL
#   STREAMS_POSTGRES_PGBOUNCER_URL=postgres://postgres:streams@localhost:<port>/postgres
set -euo pipefail

NAME=${STREAMS_POSTGRES_NAME:-streams-postgres}
VERSIONS=(14 17)

if [ "${1:-}" = stop ]; then
  for version in "${VERSIONS[@]}" pgbouncer; do
    docker rm -f "$NAME-$version" >/dev/null 2>&1 || true
  done
  exit 0
fi

free_port() {
  python3 -c 'import socket; s = socket.socket(); s.bind(("", 0)); print(s.getsockname()[1])'
}

for version in "${VERSIONS[@]}"; do
  container="$NAME-$version"
  if ! docker inspect "$container" >/dev/null 2>&1; then
    docker run -d --rm --name "$container" -e POSTGRES_PASSWORD=streams \
      -p "127.0.0.1:$(free_port):5432" "postgres:$version" \
      -c ssl=on \
      -c ssl_cert_file=/etc/ssl/certs/ssl-cert-snakeoil.pem \
      -c ssl_key_file=/etc/ssl/private/ssl-cert-snakeoil.key \
      -c max_connections=400 >/dev/null
  fi
  port=$(docker port "$container" 5432/tcp | head -1 | sed 's/.*://')
  for _ in $(seq 100); do
    docker exec "$container" pg_isready -U postgres -h 127.0.0.1 >/dev/null 2>&1 && break
    sleep 0.2
  done
  eval "URL_$version=postgres://postgres:streams@localhost:$port/postgres"
done

# Prepared statements in transaction mode need PgBouncer 1.21 or later.
# PgBouncer resolves names itself, so it reaches 17 by its address on the Docker bridge.
bouncer="$NAME-pgbouncer"
address_17=$(docker inspect -f '{{.NetworkSettings.Networks.bridge.IPAddress}}' "$NAME-17")
if ! docker inspect "$bouncer" >/dev/null 2>&1; then
  docker run -d --rm --name "$bouncer" -p "127.0.0.1:$(free_port):5432" \
    -e DATABASE_URL="postgres://postgres:streams@$address_17:5432/postgres" \
    -e POOL_MODE=transaction -e MAX_PREPARED_STATEMENTS=200 -e AUTH_TYPE=scram-sha-256 \
    -e MAX_CLIENT_CONN=1000 -e DEFAULT_POOL_SIZE=40 -e LISTEN_PORT=5432 \
    edoburu/pgbouncer:latest >/dev/null 2>&1 || echo "PgBouncer did not start" >&2
fi
if docker inspect "$bouncer" >/dev/null 2>&1; then
  port=$(docker port "$bouncer" 5432/tcp | head -1 | sed 's/.*://')
  echo "export STREAMS_POSTGRES_PGBOUNCER_URL=postgres://postgres:streams@localhost:$port/postgres"
fi
echo "export STREAMS_POSTGRES_URL=$URL_17"
echo "export STREAMS_POSTGRES_14_URL=$URL_14"
echo "export STREAMS_POSTGRES_TLS_URL=$URL_17"
