"""Dim shipping end to end against the Rust engine.

A small foreign dimension is shipped INTO the fact's source so the whole
join+aggregate collapses into one island there. The fixture data is tiny, so the
dim-shipping size gates are lowered THROUGH CONFIG (the YAML ``optimizer:``
section) for these tests; the mechanism (the ship step and the collapsed island)
is what is exercised, against the REAL Rust engine over three DuckDB files.

Shipping is a pure optimization: with the floor raised so shipping declines, the
same query returns the same rows.
"""

from tests.rust_runtime import RustRuntime

# Lowering these three gates makes shipping fire on the tiny fixture: the fact
# floor drops to zero, the fact/dim ratio to one, and the row budget above the
# fixture size.
_LOWERED_GATES = {
    "ship_local_floor": 0,
    "ship_min_ratio": 1,
    "ship_row_budget": 1000000,
}

# A floor no fixture can clear, so shipping declines and the cross-source plan
# stands.
_RAISED_FLOOR = {"ship_local_floor": 1000000000000}

_GROUP_BY_DIM = (
    "SELECT p.category AS category, count(*) AS cnt "
    "FROM duckdb_orders.main.orders o, duckdb_products.main.products p "
    "WHERE o.product_id = p.id "
    "GROUP BY p.category"
)


def _runtime(env, optimizer_config):
    """A Rust runtime over the environment's sources with a given optimizer config."""
    return RustRuntime(env.source_pairs(), optimizer_config)


def _shipment_lines(runtime, sql):
    """The EXPLAIN plan lines that are dim-shipment nodes, stripped."""
    lines = []
    for line in runtime.explain_text(sql).splitlines():
        stripped = line.strip()
        if stripped.startswith("Shipment "):
            lines.append(stripped)
    return lines


def _counts(table):
    """Map the category->cnt result rows into a dict."""
    counts = {}
    for row in table.to_pylist():
        counts[row["category"]] = row["cnt"]
    return counts


def test_group_by_dim_ships_and_collapses(multi_source_env):
    """With the gates lowered by config, grouping by a foreign-dim column ships
    the dim INTO the fact's source and collapses the subtree to one island."""
    runtime = _runtime(multi_source_env, _LOWERED_GATES)
    plan_text = runtime.explain_text(_GROUP_BY_DIM)
    ships = _shipment_lines(runtime, _GROUP_BY_DIM)
    # Exactly one dim (products) ships INTO the fact's source (duckdb_orders).
    assert len(ships) == 1, plan_text
    assert "[duckdb_orders]" in ships[0], plan_text
    # The island the shipment wraps is one remote query on the fact's source.
    assert "RemoteQuery [duckdb_orders]" in plan_text, plan_text


def test_shipped_result_is_correct(multi_source_env):
    """The shipped plan returns exactly the cross-source aggregation."""
    runtime = _runtime(multi_source_env, _LOWERED_GATES)
    # The shipment must have fired for this to exercise the shipped path.
    assert _shipment_lines(runtime, _GROUP_BY_DIM)
    counts = _counts(runtime.execute(_GROUP_BY_DIM))
    # orders.product_id joined to products.id, counted per category:
    # clothing (101,102 x2) = 4, electronics (103,104 x2) = 4, home (105,106) = 2.
    assert counts == {"clothing": 4, "electronics": 4, "home": 2}


def test_shipping_disabled_matches_shipped_result(multi_source_env):
    """With the floor raised so shipping declines, the same query returns the
    same rows: shipping is a pure optimization, never a change in results."""
    runtime = _runtime(multi_source_env, _RAISED_FLOOR)
    assert _shipment_lines(runtime, _GROUP_BY_DIM) == []
    counts = _counts(runtime.execute(_GROUP_BY_DIM))
    assert counts == {"clothing": 4, "electronics": 4, "home": 2}
