"""Load fixtures into the isolated Compose databases and write the FedQ config."""

from datetime import datetime
import os
from pathlib import Path
import shlex
import subprocess

import adbc_driver_postgresql
import pyarrow.parquet as pq
import yaml

from schema import OUTPUT, ROOT, SCHEMAS, fixture_path

CLIENTS = {
    "postgres": ["psql", "-U", "tutorial", "-d", "game", "-v", "ON_ERROR_STOP=1"],
    "mysql": ["mysql", "-u", "tutorial", "--password=tutorial", "support"],
    "clickhouse": [
        "clickhouse-client",
        "--user",
        "tutorial",
        "--password",
        "tutorial",
        "--database",
        "gameplay",
        "--multiquery",
    ],
}
SOURCES = {
    "players": "postgres",
    "transactions": "postgres",
    "tickets": "mysql",
    "events": "clickhouse",
}


def execute(source, sql):
    """Send SQL through the source's client and propagate every nonzero exit."""
    command = shlex.split(os.environ.get("GAME_STUDIO_DOCKER", "docker"))
    command.extend(["compose", "-f", str(ROOT / "compose.yaml"), "exec", "-T", source])
    subprocess.run(command + CLIENTS[source], input=sql, text=True, check=True)


def literal(value):
    """Render generated scalar values as SQL literals without losing NULLs."""
    if value is None:
        return "NULL"
    if isinstance(value, datetime):
        return "'" + value.isoformat(sep=" ") + "'"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return str(value)


def column_type(field, source):
    """Map the fixture's three scalar types to the source's native SQL types."""
    types = {"int64": "BIGINT", "string": "VARCHAR(40)", "timestamp[us]": "TIMESTAMP"}
    if source == "mysql":
        types["timestamp[us]"] = "DATETIME(6)"
    if source == "clickhouse":
        types = {
            "int64": "Int64",
            "string": "String",
            "timestamp[us]": "DateTime64(6, 'UTC')",
        }
    return types[str(field.type)]


def create_table(name, source):
    """Create a fresh fixture table; an existing table raises instead of duplicating."""
    columns = []
    for field in SCHEMAS[name]:
        columns.append(column_definition(field, source))
    suffix = ""
    if source == "clickhouse":
        suffix = " ENGINE = MergeTree ORDER BY (player_id, occurred_at, event_id)"
    else:
        columns.append(f"PRIMARY KEY ({SCHEMAS[name].names[0]})")
    execute(source, f"CREATE TABLE {name} ({', '.join(columns)}){suffix};")


def column_definition(field, source):
    """Require values on operational rows except for unresolved ticket closure."""
    nullability = " NOT NULL"
    if field.name == "closed_at":
        nullability = ""
    if source == "clickhouse":
        nullability = ""
    return f"{field.name} {column_type(field, source)}{nullability}"


def insert_rows(name, source):
    """Insert bounded batches with explicit column names into the source table."""
    table = pq.read_table(fixture_path(name))
    columns = ", ".join(table.column_names)
    for batch in table.to_batches(max_chunksize=500):
        records = []
        for row in batch.to_pylist():
            values = map(literal, row.values())
            records.append("(" + ", ".join(values) + ")")
        execute(
            source,
            f"INSERT INTO {name} ({columns}) VALUES " + ",\n".join(records) + ";",
        )


def connection(kind, port, database):
    """Describe a loopback-only tutorial database with explicit credentials."""
    return {
        "type": kind,
        "host": "127.0.0.1",
        "port": port,
        "database": database,
        "user": "tutorial",
        "password": "tutorial",
    }


def billing_connection():
    """Resolve the PostgreSQL Arrow driver from the active Python environment."""
    config = connection("postgresql", 55432, "game")
    driver = (
        Path(adbc_driver_postgresql.__file__).parent / "libadbc_driver_postgresql.so"
    )
    config["adbc_driver"] = str(driver.resolve(strict=True))
    return config


def build_config():
    """Resolve all driver, Parquet, and engine-state paths before loading data."""
    config = {
        "storage": {"root": str(OUTPUT / "fedq-state")},
        "datasources": {
            "billing": billing_connection(),
            "crm": connection("mysql", 53306, "support"),
            "gameplay": connection("clickhouse", 58123, "gameplay"),
            "acquisition": {"type": "parquet", "dir": str(OUTPUT / "acquisition")},
        },
    }
    return config


def main():
    """Load all fixtures and publish the config only after every insert succeeds."""
    config = build_config()
    for name in SCHEMAS:
        if not fixture_path(name).is_file():
            raise FileNotFoundError(fixture_path(name))
    for name, source in SOURCES.items():
        create_table(name, source)
        insert_rows(name, source)
    (OUTPUT / "fedq.yaml").write_text(yaml.safe_dump(config), encoding="ascii")
    print(f"Seed complete: {OUTPUT / 'fedq.yaml'}")


if __name__ == "__main__":
    main()
