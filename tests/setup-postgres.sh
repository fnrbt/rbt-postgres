#!/usr/bin/env bash
# Provision the PostgreSQL container used by the Fspg integration tests.
# Idempotent: re-running recreates the container from scratch.
#
# Configures everything later phases need:
#   - TCP (port 55432) + Unix-domain socket (bind-mounted ./run)
#   - TLS (ssl=on) with a generated CA + server cert (SAN localhost/127.0.0.1)
#   - wal_level=logical (+ senders/slots) for streaming replication
#   - pg_hba rules for replication, md5 and cleartext ('password') auth
#   - test roles: md5user/md5pass (md5), clearuser/clearpass (scram-stored)
set -euo pipefail

NAME="${FSPG_CONTAINER:-fspg-test}"
IMAGE="${FSPG_IMAGE:-docker.io/library/postgres:18}"
HOST_PORT="${FSPG_PORT:-55432}"
PGUSER="${FSPG_USER:-tester}"
PGPASS="${FSPG_PASSWORD:-secret}"
PGDB="${FSPG_DB:-testdb}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUNDIR="$ROOT/run"
CERTDIR="$ROOT/tests/certs"
mkdir -p "$RUNDIR" "$CERTDIR"

# --- TLS certs: CA + server cert with SAN localhost / 127.0.0.1 --------------
if [[ ! -f "$CERTDIR/server.crt" ]]; then
  echo "generating TLS certificates in $CERTDIR"
  openssl req -new -x509 -days 3650 -nodes -newkey rsa:2048 \
    -keyout "$CERTDIR/root.key" -out "$CERTDIR/root.crt" \
    -subj "/CN=fspg-test-ca" >/dev/null 2>&1
  openssl req -new -nodes -newkey rsa:2048 \
    -keyout "$CERTDIR/server.key" -out "$CERTDIR/server.csr" \
    -subj "/CN=localhost" >/dev/null 2>&1
  openssl x509 -req -in "$CERTDIR/server.csr" -days 3650 \
    -CA "$CERTDIR/root.crt" -CAkey "$CERTDIR/root.key" -CAcreateserial \
    -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1") \
    -out "$CERTDIR/server.crt" >/dev/null 2>&1
  chmod 600 "$CERTDIR/server.key"
fi

# --- (re)create the container ------------------------------------------------
podman rm -f "$NAME" >/dev/null 2>&1 || true
podman run -d --name "$NAME" \
  -e POSTGRES_USER="$PGUSER" -e POSTGRES_PASSWORD="$PGPASS" -e POSTGRES_DB="$PGDB" \
  -p "${HOST_PORT}:5432" \
  -v "$RUNDIR:/var/run/postgresql:U" \
  "$IMAGE" \
  -c wal_level=logical -c max_wal_senders=10 -c max_replication_slots=10 \
  >/dev/null
echo "started container $NAME ($IMAGE)"

# --- wait for readiness ------------------------------------------------------
for _ in $(seq 1 60); do
  if podman exec "$NAME" pg_isready -U "$PGUSER" -d "$PGDB" >/dev/null 2>&1; then
    ready=1; break
  fi
  sleep 1
done
[[ "${ready:-}" == 1 ]] || { echo "server did not become ready" >&2; exit 1; }
echo "server ready"

# --- install TLS certs inside the container (owned by postgres, key 0600) ----
podman cp "$CERTDIR/server.crt" "$NAME:/var/lib/postgresql/server.crt"
podman cp "$CERTDIR/server.key" "$NAME:/var/lib/postgresql/server.key"
podman exec --user root "$NAME" bash -c '
  chown postgres:postgres /var/lib/postgresql/server.crt /var/lib/postgresql/server.key
  chmod 600 /var/lib/postgresql/server.key'
podman exec "$NAME" psql -U "$PGUSER" -d "$PGDB" -v ON_ERROR_STOP=1 \
  -c "ALTER SYSTEM SET ssl = on;" \
  -c "ALTER SYSTEM SET ssl_cert_file = '/var/lib/postgresql/server.crt';" \
  -c "ALTER SYSTEM SET ssl_key_file = '/var/lib/postgresql/server.key';" >/dev/null

# --- pg_hba: replication + md5 + cleartext rules ahead of the scram catch-all -
HBA="$(podman exec "$NAME" psql -U "$PGUSER" -d "$PGDB" -tAc 'SHOW hba_file;')"
podman exec --user root "$NAME" bash -c "
  { echo 'host all md5user all md5';
    echo 'host all clearuser all password';
    echo 'host replication all all scram-sha-256';
  } | cat - '$HBA' > /tmp/hba && cp /tmp/hba '$HBA'"

# reload to apply ssl + pg_hba changes
podman exec "$NAME" psql -U "$PGUSER" -d "$PGDB" -tAc "SELECT pg_reload_conf();" >/dev/null

# --- test roles --------------------------------------------------------------
podman exec -i "$NAME" psql -U "$PGUSER" -d "$PGDB" -v ON_ERROR_STOP=1 >/dev/null <<SQL
SET password_encryption = 'md5';
DROP ROLE IF EXISTS md5user;   CREATE ROLE md5user   LOGIN PASSWORD 'md5pass';
SET password_encryption = 'scram-sha-256';
DROP ROLE IF EXISTS clearuser; DROP ROLE IF EXISTS repluser;
CREATE ROLE clearuser LOGIN PASSWORD 'clearpass';
CREATE ROLE repluser  LOGIN REPLICATION PASSWORD 'replpass';
SQL

echo "done: $NAME on 127.0.0.1:${HOST_PORT} (user=$PGUSER db=$PGDB)"
echo "  UDS : $RUNDIR/.s.PGSQL.5432"
echo "  TLS : ssl=on; client CA at $CERTDIR/root.crt"
