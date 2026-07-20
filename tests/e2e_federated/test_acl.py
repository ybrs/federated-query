"""End-to-end access control against a running fedq-server over SCRAM-SHA-256.

A bootstrap superuser creates a user and grants it one table through SQL; a
restricted principal reads the granted table but is blocked from a sibling
(a non-leaking not-found); SHOW USERS / SHOW GRANTS redact the verifier. The
server is the enforcement perimeter, so these run over the wire (psycopg2),
not against the embedded engine (which is an implicit superuser).
"""

import os
import subprocess
import tempfile
import time

import duckdb
import psycopg2
import pytest

_ROOT = "/workspace/federated-query"
_SERVER = os.path.join(_ROOT, "target", "release", "fedq-server")
_PASSWORD = "adminpw-Z7"
_BOB_PASSWORD = "bobpw-Q2"
_PORT = 54417


def _require_server():
    """Skip the module when the release server binary is not built."""
    if not os.path.exists(_SERVER):
        pytest.skip("fedq-server release binary is not built")


def _seed_duck(directory):
    """Seed a DuckDB file with two tables and return its path."""
    path = os.path.join(directory, "shop.duckdb")
    con = duckdb.connect(path)
    con.execute("CREATE TABLE orders(id INTEGER, total INTEGER)")
    con.execute("INSERT INTO orders VALUES (1, 10), (2, 20)")
    con.execute("CREATE TABLE lineitem(id INTEGER, qty INTEGER)")
    con.close()
    return path


def _admin_verifier():
    """Derive the bootstrap-superuser verifier YAML block via hash-password."""
    result = subprocess.run(
        [_SERVER, "hash-password", "--superuser", "admin", _PASSWORD],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


def _write_config(directory, duck_path, verifier_block):
    """Write a config with the DuckDB source and the bootstrap superuser."""
    path = os.path.join(directory, "config.yaml")
    handle = open(path, "w")
    handle.write("datasources:\n  duck:\n    type: duckdb\n")
    handle.write("    path: " + duck_path + "\n")
    handle.write("server:\n  users:\n")
    handle.write(verifier_block)
    handle.close()
    return path


def _wait_until_listening(process, port):
    """Poll the port until the server accepts a connection or time runs out."""
    deadline = time.time() + 15
    while time.time() < deadline:
        if process.poll() is not None:
            raise RuntimeError("server exited during startup")
        if _can_connect(port):
            return
        time.sleep(0.3)
    raise RuntimeError("server did not start listening")


def _can_connect(port):
    """Whether a TCP connect to the server port succeeds."""
    import socket

    probe = socket.socket()
    probe.settimeout(0.5)
    try:
        probe.connect(("127.0.0.1", port))
        return True
    except OSError:
        return False
    finally:
        probe.close()


@pytest.fixture(scope="module")
def server():
    """Start a fedq-server with a bootstrap superuser; yield its port."""
    _require_server()
    directory = tempfile.mkdtemp(prefix="fedq_acl_")
    config = _write_config(directory, _seed_duck(directory), _admin_verifier())
    process = subprocess.Popen(
        [_SERVER, "--config", config, "--listen", "127.0.0.1:" + str(_PORT)]
    )
    try:
        _wait_until_listening(process, _PORT)
        yield _PORT
    finally:
        process.terminate()
        process.wait()


def _connect(port, user, password):
    """Open an autocommit psycopg2 connection as the given user."""
    connection = psycopg2.connect(
        host="127.0.0.1",
        port=port,
        user=user,
        password=password,
        dbname="fedq",
    )
    connection.autocommit = True
    return connection


def _query_one(connection, sql):
    """Run one query and return its single scalar result."""
    cursor = connection.cursor()
    cursor.execute(sql)
    value = cursor.fetchone()
    cursor.close()
    return value


def test_superuser_provisions_and_a_restricted_principal_is_enforced(server):
    """Admin provisions bob; bob reads the grant and is blocked from a sibling."""
    admin = _connect(server, "admin", _PASSWORD)
    admin.cursor().execute("CREATE USER bob WITH PASSWORD '" + _BOB_PASSWORD + "'")
    admin.cursor().execute("GRANT SELECT ON TABLE duck.main.orders TO bob")
    admin.close()
    time.sleep(1)
    bob = _connect(server, "bob", _BOB_PASSWORD)
    granted = _query_one(bob, "SELECT count(*) FROM duck.main.orders")
    assert granted[0] == 2
    with pytest.raises(psycopg2.Error) as denied:
        _query_one(bob, "SELECT count(*) FROM duck.main.lineitem")
    bob.close()
    assert "not found" in str(denied.value).lower()


def test_show_users_and_grants_redact_the_verifier(server):
    """SHOW USERS / SHOW GRANTS expose no verifier column or value."""
    admin = _connect(server, "admin", _PASSWORD)
    users = _fetch_all(admin, "SHOW USERS")
    grants = _fetch_all(admin, "SHOW GRANTS")
    admin.close()
    joined = str(users) + str(grants)
    assert "admin" in str(users)
    assert "SCRAM-SHA-256" not in joined
    assert "verifier" not in joined.lower()


def _fetch_all(connection, sql):
    """Run a statement and return its column names plus every row."""
    cursor = connection.cursor()
    cursor.execute(sql)
    columns = []
    for description in cursor.description:
        columns.append(description[0])
    rows = cursor.fetchall()
    cursor.close()
    return (columns, rows)
