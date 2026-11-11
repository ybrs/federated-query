"""Verify automatic substitution through the server without measuring latency."""

import argparse
import json

import duckdb
import psycopg2

from schema import ROOT
from verify import (
    compare_results,
    encode_decimal,
    execute,
    oracle_rows,
    register_fixtures,
    reject_invalid_query,
)

VIEW = "studio_substitution_check"
QUERY = ROOT / "queries" / "06_accelerator_support.sql"


def command(connection, sql):
    """Execute DDL or session settings and let server failures propagate."""
    with connection.cursor() as cursor:
        cursor.execute(sql)


def view_row(connection):
    """Require exactly one metadata record for this verification's stored result."""
    matches = []
    for row in execute(connection, "SHOW MATERIALIZED VIEWS"):
        if row["name"] == VIEW:
            matches.append(row)
    if len(matches) != 1:
        raise ValueError(f"Expected one {VIEW} record, got {matches!r}")
    return matches[0]


def require_unused_name(connection):
    """Refuse to replace any existing view with the verification's reserved name."""
    for row in execute(connection, "SHOW MATERIALIZED VIEWS"):
        if row["name"] == VIEW:
            raise ValueError(f"Reserved view already exists: {VIEW}")


def plan_uses_view(plan):
    """Identify a cached read of this view in the actual server EXPLAIN output."""
    for row in plan:
        for value in row.values():
            if f"MaterializedScan [{VIEW}]" in str(value):
                return True
    return False


def inspect_plan(connection, sql, expected):
    """Require the intended cache decision and zero counter changes from EXPLAIN."""
    before = view_row(connection)["use_count"]
    plan = execute(connection, "EXPLAIN " + sql)
    if view_row(connection)["use_count"] != before:
        raise ValueError("EXPLAIN changed the substitution use counter")
    if plan_uses_view(plan) != expected:
        raise ValueError(f"Unexpected substitution decision: {plan!r}")
    return plan, before


def require_cached_source(sources):
    """Reject mixed upstream reads or a missing accelerator source."""
    if len(sources) != 1 or not next(iter(sources)).startswith("accel:"):
        raise ValueError(f"Expected one local accelerator source: {sources!r}")


def require_fragments(fragments, cached):
    """Require actual execution to read only local chunks or all three sources."""
    sources = set()
    for fragment in fragments:
        sources.add(fragment["datasource"])
    if cached:
        require_cached_source(sources)
        return
    if sources != {"billing", "crm", "gameplay"}:
        raise ValueError(f"Expected three live source fragments: {fragments!r}")


def verify_case(connection, oracle, case):
    """Check the answer, actual fragments, and served-reuse counter for one query."""
    label, sql, expected = case
    plan, before = inspect_plan(connection, sql, expected)
    actual = execute(connection, sql)
    compare_results(actual, oracle_rows(oracle, sql), sql)
    fragments = execute(connection, "SHOW PUSHDOWN FRAGMENTS")
    require_fragments(fragments, expected)
    after = view_row(connection)["use_count"]
    if after - before != int(expected):
        raise ValueError(f"{label}: reuse counter changed by {after - before}")
    print(f"Verified {label}: substitution={expected}", flush=True)
    return dict(
        case=label,
        sql=sql,
        substituted=expected,
        plan=plan,
        rows=actual,
        fragments=fragments,
        use_count_before=before,
        use_count_after=after,
    )


def mismatch_cases(sql):
    """Keep changed predicates and output names out of the original view match."""
    predicate = sql.replace("t.category = 'gameplay'", "t.category = 'billing'")
    output = sql.replace("AS gameplay_tickets", "AS ticket_count")
    return [
        ("different predicate", predicate, False),
        ("different output alias", output, False),
    ]


def query_cases(sql):
    """Exercise equivalent SQL, structural mismatches, and an outer ordered query."""
    formatting = sql.replace("SELECT", "select\n ")
    ordered = (
        "SELECT review.* FROM (" + sql + ") AS review ORDER BY review.subscription_plan"
    )
    cases = [
        ("original SELECT", sql, True),
        ("keyword case and whitespace", formatting, True),
    ]
    cases.extend(mismatch_cases(sql))
    cases.append(("outer ordering", ordered, True))
    return cases


def check_switch(connection, oracle, sql):
    """Check live bypass and restoration without reconnecting or changing SQL."""
    command(connection, "SET accelerator.enable_substitution = false")
    disabled = verify_case(connection, oracle, ("session disabled", sql, False))
    command(connection, "RESET accelerator.enable_substitution")
    restored = verify_case(connection, oracle, ("session reset", sql, True))
    return [disabled, restored]


def check_invalid(connection):
    """Require invalid SQL to raise without recording a served substitution."""
    before = view_row(connection)["use_count"]
    reject_invalid_query(connection)
    if view_row(connection)["use_count"] != before:
        raise ValueError("An invalid query changed the substitution use counter")
    return {"invalid_column_rejected": True, "use_count_unchanged": True}


def check_created_view(connection, oracle, sql):
    """Verify matching, mismatches, counters, and session controls against fixtures."""
    checks = []
    for case in query_cases(sql):
        checks.append(verify_case(connection, oracle, case))
    checks.extend(check_switch(connection, oracle, sql))
    return {"checks": checks, "invalid_query": check_invalid(connection)}


def verify(connection):
    """Create only our own view and remove it even if any assertion fails."""
    sql = QUERY.read_text(encoding="ascii").strip().rstrip(";")
    require_unused_name(connection)
    command(connection, "SET diagnostics.record_pushdown_fragments = true")
    with duckdb.connect() as oracle:
        register_fixtures(oracle, connection)
        command(connection, f"CREATE MATERIALIZED VIEW {VIEW} AS {sql}")
        try:
            report = check_created_view(connection, oracle, sql)
        finally:
            command(connection, f"DROP MATERIALIZED VIEW {VIEW}")
    require_unused_name(connection)
    return report


def main():
    """Check real server behavior and optionally save the successful evidence."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dsn", default="host=127.0.0.1 port=56432 dbname=main user=analyst"
    )
    parser.add_argument("--output", type=str)
    args = parser.parse_args()
    connection = psycopg2.connect(args.dsn)
    connection.autocommit = True
    try:
        report = verify(connection)
    finally:
        connection.close()
    report["view_removed"] = True
    if args.output:
        with open(args.output, "w", encoding="ascii") as output:
            json.dump(report, output, indent=2, default=encode_decimal)
            output.write("\n")


if __name__ == "__main__":
    main()
