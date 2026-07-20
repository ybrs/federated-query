#!/usr/bin/env bash
# End-to-end SCRAM-SHA-256 interop for the ACL feature: start fedq-server with a
# bootstrap-superuser VERIFIER (no plaintext, no SaltedPassword in the config),
# connect with the real postgres-17 psql using the correct password and run a
# query, then connect with a WRONG password and confirm rejection. Finally grep
# the persisted store and the config for any plaintext password or SaltedPassword
# (must be absent).
set -u

ROOT=/workspace/federated-query
PSQL=$ROOT/postgres-17/bin/psql
SERVER=$ROOT/target/release/fedq-server
PY=/workspace/venv-fedq/bin/python
WORK=$(mktemp -d /tmp/acl-interop.XXXXXX)
PORT=54329
PASSWORD='s3cr3t-Pa55!interop'

cleanup() { [ -n "${SRV_PID:-}" ] && kill "$SRV_PID" 2>/dev/null; }
trap cleanup EXIT

echo "== work dir: $WORK =="

# Seed a DuckDB source with one table.
"$PY" - "$WORK/shop.duckdb" <<'PYEOF'
import sys, duckdb
con = duckdb.connect(sys.argv[1])
con.execute("CREATE TABLE orders(id INTEGER, total INTEGER)")
con.execute("INSERT INTO orders VALUES (1,10),(2,20),(3,30)")
con.close()
PYEOF

# Derive the bootstrap-superuser VERIFIER block (the plaintext never enters the config).
VERIFIER_BLOCK=$("$SERVER" hash-password --superuser admin "$PASSWORD")
echo "== hash-password emitted:"; echo "$VERIFIER_BLOCK"

CONFIG=$WORK/config.yaml
{
  echo "datasources:"
  echo "  duck:"
  echo "    type: duckdb"
  echo "    path: $WORK/shop.duckdb"
  echo "server:"
  echo "  users:"
  echo "$VERIFIER_BLOCK"
} > "$CONFIG"
echo "== config:"; cat "$CONFIG"

# Start the server.
"$SERVER" --config "$CONFIG" --listen "127.0.0.1:$PORT" &
SRV_PID=$!
sleep 2

RESULT=0

echo "== CORRECT password (must succeed and return rows) =="
PGPASSWORD="$PASSWORD" "$PSQL" "host=127.0.0.1 port=$PORT user=admin dbname=fedq" \
  -c "SELECT count(*) AS n FROM duck.main.orders" -t
if [ $? -ne 0 ]; then echo "FAIL: correct password could not query"; RESULT=1; fi

echo "== WRONG password (must be rejected) =="
PGPASSWORD="wrong-$PASSWORD" "$PSQL" "host=127.0.0.1 port=$PORT user=admin dbname=fedq" \
  -c "SELECT 1" -t >"$WORK/wrong.out" 2>&1
WRONG_STATUS=$?
cat "$WORK/wrong.out"
if [ $WRONG_STATUS -eq 0 ]; then
  echo "FAIL: wrong password was accepted"; RESULT=1
else
  echo "OK: wrong password rejected"
fi
grep -qi "password authentication failed\|SASL" "$WORK/wrong.out" || echo "(note: rejection message above)"

echo "== GREP PROOF: no plaintext password or SaltedPassword in config or store =="
STORE=$WORK/config.stats.sqlite
LEAK=0
if grep -aiq -- "$PASSWORD" "$CONFIG"; then echo "LEAK: plaintext in config"; LEAK=1; fi
if [ -f "$STORE" ] && grep -aiq -- "$PASSWORD" "$STORE"; then echo "LEAK: plaintext in store"; LEAK=1; fi
if grep -aiq "salted_password\|saltedpassword" "$CONFIG"; then echo "LEAK: SaltedPassword field in config"; LEAK=1; fi
if [ -f "$STORE" ] && grep -aiq "salted_password\|saltedpassword" "$STORE"; then echo "LEAK: SaltedPassword field in store"; LEAK=1; fi
if [ "$LEAK" -eq 0 ]; then
  echo "OK: no plaintext password and no SaltedPassword in the config or the persisted store"
  echo "    (only the pg_authid verifier is present:)"
  grep -ao "SCRAM-SHA-256\$[0-9]*:[A-Za-z0-9+/=]*" "$STORE" 2>/dev/null | head -1
else
  RESULT=1
fi

exit $RESULT
