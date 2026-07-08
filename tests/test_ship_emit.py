"""The ship step is emitted in the load-bearing order.

A PhysicalShipment must emit: the body binding, then a `ship` step, then the
island that reads the shipped temp table. The ship step MUST precede the
island's source_scan so the temp table exists when the engine reads it.
"""

import sqlglot
import pyarrow as pa

from federated_query.executor.rust_ir import build_ir
from federated_query.plan.physical import PhysicalScan, PhysicalShipment


class _FakeConnection:
    """A source connection that renders SQL but is never probed (schemas are
    seeded / not needed here)."""

    render_dialect = "duckdb"

    def parse_query(self, sql: str):
        return sqlglot.parse_one(sql, dialect="postgres")


def _scan(table: str, columns, seeded=None) -> PhysicalScan:
    return PhysicalScan.create(
        datasource="duck",
        schema_name="temp" if seeded is not None else "public",
        table_name=table,
        columns=columns,
        datasource_connection=_FakeConnection(),
        seeded_schema=seeded,
    )


def test_ship_step_between_body_and_island():
    """build_ir emits body scan, then ship, then the island scan, in order."""
    body = _scan("dim_src", ["d_date_sk", "d_moy"])
    island = _scan(
        "__fedq_ship_0",
        ["d_moy"],
        seeded=pa.schema([("d_moy", pa.int64())]),
    )
    shipment = PhysicalShipment.create(
        table="__fedq_ship_0", datasource="duck", body=body, child=island
    )

    ir = build_ir(shipment)
    ops = []
    for step in ir["steps"]:
        ops.append(step["op"])

    ship_index = ops.index("ship")
    scan_indices = []
    for index, step in enumerate(ir["steps"]):
        if step["op"] == "source_scan":
            scan_indices.append(index)
    # The body scan runs before the ship; the island scan runs after it.
    assert scan_indices[0] < ship_index < scan_indices[1]

    ship_step = ir["steps"][ship_index]
    assert ship_step["datasource"] == "duck"
    assert ship_step["table"] == "__fedq_ship_0"
    # The ship consumes the body binding produced by the first scan.
    assert ship_step["input"] == ir["steps"][scan_indices[0]]["binding"]
    # The plan's output columns come from the seeded island schema.
    assert ir["outputs"] == ["d_moy"]
