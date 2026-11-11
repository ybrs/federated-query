"""Prepare the game-studio accelerator's isolated state and correctness oracle."""

from pathlib import Path
import os
import shlex
import subprocess

import duckdb
import yaml

from wire_runtime import ROOT, execute

STUDIO = ROOT / "tutorials" / "game-studio"
VIEW = "studio_accelerator_support"
DROP = f"DROP MATERIALIZED VIEW {VIEW}"
IDS = "900000001, 900000002"


def source_sql(filename):
    """Read one checked-in source mutation or defining SELECT verbatim."""
    return (STUDIO / filename).read_text(encoding="ascii")


def upstream(filename):
    """Run the actual MySQL mutation through the tutorial's Docker Compose client."""
    command = shlex.split(os.environ.get("GAME_STUDIO_DOCKER", "docker"))
    command.extend(
        [
            "compose",
            "-f",
            str(STUDIO / "compose.yaml"),
            "exec",
            "-T",
            "mysql",
            "mysql",
            "-u",
            "tutorial",
            "--password=tutorial",
            "support",
        ]
    )
    subprocess.run(command, input=source_sql(filename), text=True, check=True)


def scenario_config(path, output):
    """Reuse the real sources with a private accelerator catalog for this report."""
    config = yaml.safe_load(Path(path).read_text())
    config["storage"] = {"root": str(output / "state")}
    destination = output / "fedq.yaml"
    destination.write_text(yaml.safe_dump(config), encoding="ascii")
    return destination


def oracle_sql(query):
    """Map the three qualified source tables onto their original fixture tables."""
    relations = {
        "billing.public.players": "players",
        "crm.support.tickets": "tickets",
        "gameplay.gameplay.events": "events",
    }
    for relation, table in relations.items():
        query = query.replace(relation, table)
    return query


def oracle_rows(connection, query):
    """Return named oracle values once for each source-data state."""
    result = connection.execute(oracle_sql(query))
    names = []
    for column in result.description:
        names.append(column[0])
    rows = []
    for values in result.fetchall():
        rows.append(dict(zip(names, values, strict=True)))
    return sorted(rows, key=lambda row: row["subscription_plan"])


def expected_results(query):
    """Compute the original and post-insert oracle answers before measurements."""
    with duckdb.connect() as oracle:
        for table in ("players", "tickets", "events"):
            path = STUDIO / "generated" / f"{table}.parquet"
            oracle.execute(
                f"CREATE TABLE {table} AS SELECT * FROM read_parquet(?)",
                [str(path.resolve(strict=True))],
            )
        before = oracle_rows(oracle, query)
        oracle.execute(source_sql("accelerator-insert.sql"))
        after = oracle_rows(oracle, query)
    if before == after:
        raise ValueError("The inserted tickets do not affect this fixture's query")
    return {"before": before, "after": after}


def require_clean(client):
    """Refuse existing views or reserved IDs before any destructive preparation."""
    if execute(client, "SHOW MATERIALIZED VIEWS"):
        raise ValueError("The experiment's private catalog already contains views")
    sql = (
        "SELECT COUNT(*) AS rows FROM crm.support.tickets AS t "
        f"WHERE t.ticket_id IN ({IDS})"
    )
    if execute(client, sql)[0]["rows"] != 0:
        raise ValueError("The experiment's reserved ticket IDs already exist")


def drop_owned_view(client):
    """Remove only the experiment's view when it was successfully created."""
    for row in execute(client, "SHOW MATERIALIZED VIEWS"):
        if row["name"] == VIEW:
            execute(client, DROP)


def require_rows(actual, expected):
    """Compare every result value after normalizing SQL's unordered output."""
    actual = sorted(actual, key=lambda row: row["subscription_plan"])
    if actual != expected:
        raise ValueError(f"Result differs: {actual!r} != {expected!r}")


def verify_restoration(client, query, expected):
    """Check both the live answer and removal of the two demonstration tickets."""
    require_rows(execute(client, query), expected)
    sql = (
        "SELECT COUNT(*) AS rows FROM crm.support.tickets AS t "
        f"WHERE t.ticket_id IN ({IDS})"
    )
    if execute(client, sql)[0]["rows"] != 0:
        raise ValueError("Demonstration tickets remain after cleanup")
