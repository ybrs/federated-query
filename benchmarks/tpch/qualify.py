"""Rewrite bare TPC-H table names into fully qualified engine references.

The DuckDB TPC-H extension emits queries that reference tables by their bare
name (``FROM lineitem``). The federated query engine requires three-part names
(``source.schema.table``). This module qualifies only the eight TPC-H base
tables and leaves every other name (CTE names such as ``revenue`` in query 15,
column names, aliases) untouched, then renders the result in a target dialect.
"""

import sqlglot
from sqlglot import exp


# The eight TPC-H base tables produced by dbgen. Any name outside this set
# (CTEs, derived tables) is deliberately left unqualified.
TPCH_TABLES = frozenset(
    {
        "customer",
        "lineitem",
        "nation",
        "orders",
        "part",
        "partsupp",
        "region",
        "supplier",
    }
)


def _qualify_table(table, source_name, schema_name):
    """Attach source and schema to a base TPC-H table node in place."""
    if table.name.lower() not in TPCH_TABLES:
        return
    if table.args.get("db"):
        return
    table.set("db", exp.to_identifier(schema_name))
    table.set("catalog", exp.to_identifier(source_name))


def qualify_query(sql, source_name, schema_name, target_dialect):
    """Qualify TPC-H tables in one query and render it in the target dialect.

    The query is parsed as DuckDB SQL (its source dialect), every base TPC-H
    table is rewritten to ``source_name.schema_name.table``, and the tree is
    rendered in ``target_dialect`` so the engine's parser accepts it.
    """
    tree = sqlglot.parse_one(sql, dialect="duckdb")
    for table in tree.find_all(exp.Table):
        _qualify_table(table, source_name, schema_name)
    return tree.sql(dialect=target_dialect)
