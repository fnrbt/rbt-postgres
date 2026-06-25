#!/usr/bin/env bash
# Stand up a self-contained Kerberos KDC + GSSAPI-enabled PostgreSQL inside a
# single Podman container, then verify the fspg client authenticates via GSSAPI.
#
# Everything (KDC, Postgres, the .NET client) runs in one container so there is
# no host Kerberos config to manage and no cross-container DNS to get right.
# Teardown:  podman rm -f fspg-krb
set -euo pipefail

NAME="${FSPG_KRB_CONTAINER:-fspg-krb}"
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

podman rm -f "$NAME" >/dev/null 2>&1 || true
podman run -d --name "$NAME" --hostname pghost -e DEBIAN_FRONTEND=noninteractive \
  -v "$REPO:/work:ro" mcr.microsoft.com/dotnet/sdk:10.0 sleep infinity >/dev/null

echo "[1/4] installing postgresql + krb5 ..."
podman exec "$NAME" bash -lc '
  apt-get update -qq >/dev/null
  apt-get install -y -qq postgresql krb5-kdc krb5-admin-server krb5-user iproute2 >/dev/null'

echo "[2/4] configuring Kerberos realm EXAMPLE.COM + starting KDC ..."
podman exec "$NAME" bash -lc '
set -e
cat > /etc/krb5.conf <<EOF
[libdefaults]
    default_realm = EXAMPLE.COM
    dns_lookup_realm = false
    dns_lookup_kdc = false
    rdns = false
    forwardable = true
[realms]
    EXAMPLE.COM = {
        kdc = localhost
        admin_server = localhost
    }
[domain_realm]
    pghost = EXAMPLE.COM
    .pghost = EXAMPLE.COM
EOF
mkdir -p /etc/krb5kdc /var/lib/krb5kdc
cat > /etc/krb5kdc/kdc.conf <<EOF
[kdcdefaults]
    kdc_ports = 88,750
[realms]
    EXAMPLE.COM = {
        database_name = /var/lib/krb5kdc/principal
        admin_keytab = /etc/krb5kdc/kadm5.keytab
        acl_file = /etc/krb5kdc/kadm5.acl
        key_stash_file = /etc/krb5kdc/stash
    }
EOF
echo "*/admin *" > /etc/krb5kdc/kadm5.acl
kdb5_util create -s -r EXAMPLE.COM -P masterkey
kadmin.local -q "addprinc -randkey postgres/pghost@EXAMPLE.COM" >/dev/null
kadmin.local -q "ktadd -k /etc/krb5.keytab postgres/pghost@EXAMPLE.COM" >/dev/null
kadmin.local -q "addprinc -pw testerpw tester@EXAMPLE.COM" >/dev/null
chmod 644 /etc/krb5.keytab
/usr/sbin/krb5kdc'

echo "[3/4] configuring + starting GSS PostgreSQL ..."
podman exec "$NAME" bash -lc '
set -e
PGCONF=/etc/postgresql/16/main
echo "krb_server_keyfile = '\''/etc/krb5.keytab'\''" >> $PGCONF/postgresql.conf
echo "listen_addresses = '\''*'\''" >> $PGCONF/postgresql.conf
sed -i "1i host all all 0.0.0.0/0 gss include_realm=0 krb_realm=EXAMPLE.COM" $PGCONF/pg_hba.conf
sed -i "1i local all all trust" $PGCONF/pg_hba.conf
pg_ctlcluster 16 main start
sleep 2
su postgres -c "psql -tAc \"CREATE ROLE tester LOGIN SUPERUSER;\"" >/dev/null
su postgres -c "psql -tAc \"CREATE DATABASE tester OWNER tester;\"" >/dev/null'

echo "[4/4] verifying the fspg client authenticates via GSSAPI ..."
podman exec "$NAME" bash -lc '
set -e
rm -rf /tmp/fspg && mkdir -p /tmp/fspg
cp -r /work/src /work/samples /work/fspg.slnx /tmp/fspg/
cd /tmp/fspg
dotnet build samples/Fspg.Sample/Fspg.Sample.fsproj -c Release -v quiet --nologo >/dev/null
echo testerpw | kinit tester@EXAMPLE.COM
dotnet run --project samples/Fspg.Sample -c Release --no-build -- \
  --host pghost --port 5432 --user tester --password ignored --db tester'

echo
echo "GSSAPI verification complete. Teardown: podman rm -f $NAME"
