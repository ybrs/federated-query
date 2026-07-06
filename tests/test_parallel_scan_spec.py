"""Unit tests for the parallel source-scan spec choice (rust_ir).

A big plain PostgreSQL scan is emitted as a structured spec marked
``parallel`` so the engine reads it with ctid-partitioned connections; every
other scan keeps the pre-rendered raw-SQL single-stream path. The dangerous
mistakes - parallelizing a DISTINCT / TABLESAMPLE / ordered / limited scan
(per-partition results compose wrongly), or a non-Postgres source - must be
refused.
"""

import pyarrow as pa

from federated_query.datasources.postgresql import PostgreSQLDataSource
from federated_query.executor.rust_ir import (
    PARALLEL_SCAN_MIN_ROWS,
    _source_scan_spec,
)
from federated_query.plan.expressions import (
    BinaryOp,
    BinaryOpType,
    ColumnRef,
    DataType,
    Literal,
)
from federated_query.plan.physical import PhysicalScan


def _pg_connection():
    """An unconnected PostgreSQL datasource (spec choice never connects)."""
    return PostgreSQLDataSource("pg", {"host": "x", "database": "d", "user": "u"})


def _scan(columns, connection, **kw):
    """A qualified physical scan with its output schema seeded, so neither
    column_aliases() nor SQL rendering needs a live connection."""
    scan = PhysicalScan(
        datasource="pg", schema_name="public", table_name="customer",
        columns=list(columns), alias="customer",
        datasource_connection=connection, **kw,
    )
    fields = []
    for name in columns:
        fields.append((name, pa.int32()))
    object.__setattr__(scan, "_schema", pa.schema(fields))
    return scan


def _big_rows():
    """An estimate comfortably above the parallel threshold."""
    return PARALLEL_SCAN_MIN_ROWS * 3


def test_big_plain_pg_scan_goes_structured_parallel():
    """The qualifying case: structured spec, parallel flag, filter included."""
    filters = BinaryOp(
        op=BinaryOpType.EQ,
        left=ColumnRef(table="customer", column="c_custkey",
                       data_type=DataType.INTEGER),
        right=Literal(value=7, data_type=DataType.INTEGER),
    )
    scan = _scan(["c_custkey", "c_name"], _pg_connection(),
                 estimated_rows=_big_rows(), filters=filters)
    spec = _source_scan_spec(scan)
    assert spec.get("parallel") is True
    assert spec["table"] == "customer" and "raw_sql" not in spec
    assert spec["filter"]["node"] == "binary"


def test_small_pg_scan_stays_raw():
    """Below the threshold the single stream is cheaper: raw SQL, no flag."""
    scan = _scan(["c_custkey"], _pg_connection(), estimated_rows=100)
    assert "raw_sql" in _source_scan_spec(scan)


def test_unestimated_pg_scan_stays_raw():
    """No cost estimate = no size evidence: stay on the single-stream path."""
    scan = _scan(["c_custkey"], _pg_connection(), estimated_rows=None)
    assert "raw_sql" in _source_scan_spec(scan)


def test_non_postgres_scan_stays_raw():
    """Only Postgres has the ctid-partitioned machinery."""
    from federated_query.datasources.duckdb import DuckDBDataSource

    duck = DuckDBDataSource("duck", {"path": ":memory:"})
    scan = _scan(["c_custkey"], duck, estimated_rows=_big_rows())
    assert "raw_sql" in _source_scan_spec(scan)


def test_partition_unsafe_scans_stay_raw():
    """DISTINCT / TABLESAMPLE / ORDER BY / LIMIT compose wrongly per-partition
    and must never parallelize, however big the scan is."""
    key = ColumnRef(table="customer", column="c_custkey",
                    data_type=DataType.INTEGER)
    unsafe_variants = [
        {"distinct": True},
        {"sample": "BERNOULLI (10)"},
        {"order_by_keys": [key], "order_by_ascending": [True]},
        {"limit": 10},
    ]
    for fields in unsafe_variants:
        scan = _scan(["c_custkey"], _pg_connection(),
                     estimated_rows=_big_rows(), **fields)
        assert "raw_sql" in _source_scan_spec(scan), f"must refuse {fields}"
