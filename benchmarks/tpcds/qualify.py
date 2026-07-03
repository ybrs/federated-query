"""Rewrite bare TPC-DS table names into fully qualified engine references.

The DuckDB TPC-DS extension emits queries that reference tables by their bare
name (``FROM store_sales``). The federated query engine requires three-part
names (``source.schema.table``). This module qualifies only the 24 TPC-DS base
tables and leaves every other name (CTE names, column names, aliases) untouched,
then renders the result in a target dialect.
"""

import sqlglot
from sqlglot import exp


# The 24 TPC-DS base tables produced by dsdgen. Any name outside this set
# (CTEs, derived tables) is deliberately left unqualified.
TPCDS_TABLES = frozenset(
    {
        "call_center",
        "catalog_page",
        "catalog_returns",
        "catalog_sales",
        "customer",
        "customer_address",
        "customer_demographics",
        "date_dim",
        "household_demographics",
        "income_band",
        "inventory",
        "item",
        "promotion",
        "reason",
        "ship_mode",
        "store",
        "store_returns",
        "store_sales",
        "time_dim",
        "warehouse",
        "web_page",
        "web_returns",
        "web_sales",
        "web_site",
    }
)


def _qualify_table(table, source_name, schema_name):
    """Attach source and schema to a base TPC-DS table node in place."""
    if table.name.lower() not in TPCDS_TABLES:
        return
    if table.args.get("db"):
        return
    table.set("db", exp.to_identifier(schema_name))
    table.set("catalog", exp.to_identifier(source_name))


def qualify_query(sql, source_name, schema_name, target_dialect):
    """Qualify TPC-DS tables in one query and render it in the target dialect.

    The query is parsed as DuckDB SQL (its source dialect), every base TPC-DS
    table is rewritten to ``source_name.schema_name.table``, and the tree is
    rendered in ``target_dialect`` so the engine's parser accepts it.
    """
    tree = sqlglot.parse_one(sql, dialect="duckdb")
    for table in tree.find_all(exp.Table):
        _qualify_table(table, source_name, schema_name)
    return tree.sql(dialect=target_dialect)
