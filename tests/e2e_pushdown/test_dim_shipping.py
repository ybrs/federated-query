"""Dim shipping end to end: a small foreign dimension is shipped INTO the fact's
source so the whole join+aggregate collapses into one island there.

The fixture data is tiny, so the size gates are lowered for these tests; the
mechanism (ship step, pinned connection, seeded island) is what is exercised,
against the REAL Rust engine over three distinct DuckDB files.
"""

from federated_query.optimizer import dim_shipping
from federated_query.plan.physical import PhysicalRemoteQuery, PhysicalShipment

from .helpers import build_runtime


def _lower_gates(monkeypatch):
    """Make the size gates fire on the tiny fixture data."""
    monkeypatch.setattr(dim_shipping, "SHIP_LOCAL_FLOOR", 0)
    monkeypatch.setattr(dim_shipping, "SHIP_MIN_RATIO", 1)
    monkeypatch.setattr(dim_shipping, "SHIP_ROW_BUDGET", 1_000_000)


def _physical_plan(runtime, sql):
    """Build the physical plan through the executor's own pipeline stages."""
    qe = runtime.query_executor
    rewritten = qe._run_before_processors(sql)
    logical = qe._parse_query(rewritten)
    logical = qe._bind_plan(logical)
    logical = qe._decorrelate_plan(logical)
    logical = qe._optimize_plan(logical)
    return qe._build_physical_plan(logical)


def _find(node, node_type):
    """Return the first node of a type in the plan, or None."""
    if isinstance(node, node_type):
        return node
    for child in node.children():
        found = _find(child, node_type)
        if found is not None:
            return found
    return None


_GROUP_BY_DIM = (
    "SELECT p.category AS category, count(*) AS cnt "
    "FROM duckdb_orders.main.orders o, duckdb_products.main.products p "
    "WHERE o.product_id = p.id "
    "GROUP BY p.category"
)


def test_group_by_dim_ships_and_collapses(multi_source_env, monkeypatch):
    """Grouping by a foreign-dim column ships the dim and collapses to one
    seeded island; the shipment materializes the dim into the fact's source."""
    _lower_gates(monkeypatch)
    runtime = build_runtime(multi_source_env)

    plan = _physical_plan(runtime, _GROUP_BY_DIM)
    shipment = _find(plan, PhysicalShipment)
    assert shipment is not None
    # The dim (products) ships INTO the fact's source (orders).
    assert shipment.datasource == "duckdb_orders"
    island = _find(shipment.child, PhysicalRemoteQuery)
    assert island is not None and island.datasource == "duckdb_orders"
    # The island reads a shipped temp table, so its schema is seeded, not probed.
    assert island.seeded_schema is not None


def test_shipped_result_is_correct(multi_source_env, monkeypatch):
    """The shipped plan returns exactly the cross-source aggregation."""
    _lower_gates(monkeypatch)
    runtime = build_runtime(multi_source_env)

    table = runtime.execute(_GROUP_BY_DIM)
    counts = {}
    for row in table.to_pylist():
        counts[row["category"]] = row["cnt"]
    # orders.product_id joined to products.id, counted per category:
    # clothing (101,102 x2) = 4, electronics (103,104 x2) = 4, home (105,106) = 2.
    assert counts == {"clothing": 4, "electronics": 4, "home": 2}


def test_shipping_disabled_matches_shipped_result(multi_source_env, monkeypatch):
    """With shipping gated off, the same query returns the same rows: shipping
    is a pure optimization, never a change in results."""
    monkeypatch.setattr(dim_shipping, "SHIP_LOCAL_FLOOR", 10**12)
    runtime = build_runtime(multi_source_env)

    table = runtime.execute(_GROUP_BY_DIM)
    plan = _physical_plan(runtime, _GROUP_BY_DIM)
    assert _find(plan, PhysicalShipment) is None
    counts = {}
    for row in table.to_pylist():
        counts[row["category"]] = row["cnt"]
    assert counts == {"clothing": 4, "electronics": 4, "home": 2}
