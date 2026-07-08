"""A seeded schema is returned WITHOUT probing the source.

A shipped-dimension temp table lives only on the engine's pinned connection,
so the python-side probe (get_query_schema, a LIMIT 0 query) cannot see it.
The shipping rule seeds the island's known schema instead. These tests pin
that a seeded node never probes, and that an unseeded node still does.
"""

import pyarrow as pa
import pytest

from federated_query.plan.physical import PhysicalRemoteQuery, PhysicalScan


class _RaisingConnection:
    """A source connection whose probe raises, proving it is never called."""

    render_dialect = "duckdb"

    def get_query_schema(self, query: str) -> pa.Schema:
        raise AssertionError("probe must not run when a schema is seeded")

    def parse_query(self, sql: str):
        raise AssertionError("SQL must not render when a schema is seeded")


_SEEDED = pa.schema([("d_date_sk", pa.int64()), ("d_moy", pa.int64())])


def test_seeded_scan_skips_probe():
    """A PhysicalScan with a seeded schema returns it without probing."""
    scan = PhysicalScan.create(
        datasource="duck",
        schema_name="temp",
        table_name="__fedq_ship_0",
        columns=["d_date_sk", "d_moy"],
        datasource_connection=_RaisingConnection(),
        seeded_schema=_SEEDED,
    )
    assert scan.schema() is _SEEDED


def test_unseeded_scan_probes():
    """Without a seeded schema, a scan still probes its source."""
    probed = pa.schema([("id", pa.int64())])

    class _ProbeConnection:
        render_dialect = "duckdb"

        def parse_query(self, sql: str):
            return self

        def sql(self, dialect: str) -> str:
            return "SELECT id FROM t"

        def get_query_schema(self, query: str) -> pa.Schema:
            return probed

    scan = PhysicalScan.create(
        datasource="duck",
        schema_name="public",
        table_name="t",
        columns=["id"],
        datasource_connection=_ProbeConnection(),
    )
    assert scan.schema() is probed


def test_seeded_remote_query_skips_probe():
    """A PhysicalRemoteQuery island with a seeded schema returns it directly."""
    island = PhysicalRemoteQuery.create(
        datasource="duck",
        datasource_connection=_RaisingConnection(),
        query_ast=None,
        output_names=["d_moy"],
        column_alias_map={},
        seeded_schema=_SEEDED,
    )
    assert island.schema() is _SEEDED


def test_seeded_schema_survives_model_copy():
    """A copy keeps the seeded schema field so a rewritten node still skips
    the probe (the derived cache is dropped, but the field is preserved)."""
    scan = PhysicalScan.create(
        datasource="duck",
        schema_name="temp",
        table_name="__fedq_ship_0",
        columns=["d_date_sk", "d_moy"],
        datasource_connection=_RaisingConnection(),
        seeded_schema=_SEEDED,
    )
    copied = scan.model_copy(update={"alias": "d"})
    assert copied.schema() is _SEEDED
