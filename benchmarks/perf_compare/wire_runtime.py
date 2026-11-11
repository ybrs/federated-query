"""Measure release-server statements over PostgreSQL wire with fresh runtimes."""

from contextlib import contextmanager
from decimal import Decimal
import json
import os
from pathlib import Path
import socket
import subprocess
import time

import psycopg2

ROOT = Path(__file__).resolve().parents[2]
COUNTS = {"statements": 0}


def execute(connection, sql):
    """Execute and fully decode one statement, counting every harness request."""
    COUNTS["statements"] += 1
    with connection.cursor() as cursor:
        cursor.execute(sql)
        names = []
        for column in cursor.description:
            names.append(column.name)
        rows = []
        for values in cursor.fetchall():
            rows.append(dict(zip(names, values, strict=True)))
        return rows


def encode_decimal(value):
    """Serialize exact SQL decimals while rejecting unrecognized result types."""
    if isinstance(value, Decimal):
        return str(value)
    raise TypeError(type(value).__name__)


def free_port():
    """Choose a loopback port for one isolated server instance."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def try_connect(dsn):
    """Retry only connection refusal while the server opens its listening socket."""
    try:
        return psycopg2.connect(dsn, connect_timeout=2)
    except psycopg2.OperationalError as error:
        if "Connection refused" not in str(error):
            raise
        return None


def wait_ready(process, port):
    """Return the first ready connection or fail with the server's exit status."""
    dsn = f"host=127.0.0.1 port={port} dbname=main user=analyst"
    for _ in range(200):
        if process.poll() is not None:
            raise subprocess.CalledProcessError(process.returncode, process.args)
        connection = try_connect(dsn)
        if connection is not None:
            return connection
        time.sleep(0.05)
    raise TimeoutError(dsn)


def prepare(connection, substitution):
    """Fix statistics policy and diagnostics without changing the planning budget."""
    connection.autocommit = True
    execute(connection, "SET optimizer.enable_learned_statistics = false")
    execute(connection, "SET diagnostics.record_pushdown_fragments = true")
    enabled = str(substitution).lower()
    execute(connection, f"SET accelerator.enable_substitution = {enabled}")


def start_server(config, port, log):
    """Launch only the workspace release server and retain its complete output."""
    binary = ROOT / "target" / "release" / "fedq-server"
    command = [
        str(binary.resolve(strict=True)),
        "--config",
        str(config),
        "--listen",
        f"127.0.0.1:{port}",
    ]
    environment = dict(os.environ)
    environment["FEDQRS_TRACE_SQL"] = "1"
    return subprocess.Popen(
        command, stdout=log, stderr=subprocess.STDOUT, env=environment
    )


def stop_server(process):
    """Terminate the harness-owned server and wait for its process to exit."""
    if process.poll() is None:
        process.terminate()
    process.wait(timeout=10)


@contextmanager
def server(config, log_path, substitution=True):
    """Own one fresh server process and one runtime-bearing client connection."""
    with Path(log_path).open("a", encoding="utf8") as log:
        port = free_port()
        process = start_server(config, port, log)
        connection = None
        try:
            connection = wait_ready(process, port)
            prepare(connection, substitution)
            yield connection
        finally:
            if connection is not None:
                connection.close()
            stop_server(process)


def measure(connection, sql):
    """Time statement issue through complete result decoding, including planning."""
    started = time.perf_counter()
    try:
        rows = execute(connection, sql)
    except psycopg2.Error as error:
        status = "ERROR"
        if "planning budget exceeded" in str(error).lower():
            status = "KILLED"
        return {"status": status, "message": str(error)}
    return {"status": "OK", "ms": (time.perf_counter() - started) * 1000, "rows": rows}


def cold_worker(payload):
    """Measure one target on a fresh process/runtime and emit one JSON record."""
    expected = {"config", "sql", "substitution", "before", "log"}
    if set(payload) != expected:
        raise ValueError(f"Unknown or missing worker fields: {set(payload) ^ expected}")
    with server(payload["config"], payload["log"], payload["substitution"]) as client:
        for sql in payload["before"]:
            execute(client, sql)
        record = measure(client, payload["sql"])
    record["statements"] = COUNTS["statements"]
    print(json.dumps(record, default=encode_decimal), flush=True)
