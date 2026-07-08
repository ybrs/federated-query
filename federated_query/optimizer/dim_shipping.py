"""Dim shipping: collapse a cross-source fact-dimension subtree into one island.

When a large fact (on one source) joins small dimensions (on another) and the
query groups by dimension columns, the fact cannot collapse into its own source
and every fact row must move to the coordinator. Instead we SHIP the small
dimensions INTO the fact's source as temp tables, so the whole join+aggregate
runs as one island there and only the aggregate OUTPUT crosses the boundary.

Correctness: only INNER equi joins over deterministic foreign subtrees ship. A
foreign relation is evaluated once and joined locally, exactly as the
coordinator join would evaluate and join it - the result is identical. The
shipped temp table lives only on the engine's pinned connection, so the island
that reads it carries a SEEDED schema (it cannot be probed python-side).

The gates are conservative: ship only when the local side is genuinely large,
the foreign side is small and known, and the local side dwarfs it. An unknown
foreign estimate declines (a mis-shipped big dimension would flood the source).
"""

import os

from ..datasources.base import DataSourceCapability
from ..datasources.postgresql import PostgreSQLDataSource
from ..plan.logical import (
    Aggregate,
    Filter,
    Join,
    JoinType,
    Limit,
    Projection,
    Scan,
    Sort,
    SubqueryScan,
)
from ..plan.physical import (
    PhysicalHashAggregate,
    PhysicalShipment,
    PhysicalWindow,
)

# The schema a shipped temp table is referenced under, per target: DuckDB puts
# CREATE TEMP TABLE in ``temp``; PostgreSQL puts a session temp table in
# ``pg_temp``. Both resolve as a two-part ``<schema>."name"`` reference.
_DUCK_SHIP_SCHEMA = "temp"
_PG_SHIP_SCHEMA = "pg_temp"

# Only ship across these deterministic, row-preserving-or-reducing relational
# nodes. A Join is additionally required to be INNER (checked separately): a
# LEFT/SEMI/ANTI join's result depends on which side is preserved, so relocating
# a dimension across it could change the answer.
_SHIPPABLE_NODES = (Scan, Filter, Projection, Aggregate, Sort, Limit, SubqueryScan)

# Only bother when the local (fact) side clears this many rows: below it the
# fact transfer is cheap and shipping's temp-table build is not worth it.
SHIP_LOCAL_FLOOR = 100_000
# Never ship more than this many foreign rows into a source. A large shipped
# dimension (e.g. customer) both costs a big temp-table build and tends to mean
# the fact is not the dominant transfer, so shipping does not pay (measured
# regressions on q38/q87, which ship a ~500k-row customer dimension).
SHIP_ROW_BUDGET = 200_000
# The local side must exceed the shipped foreign side by at least this factor.
SHIP_MIN_RATIO = 20


class DimShipping:
    """Builds a PhysicalShipment for a shippable cross-source subtree, or None."""

    def __init__(self, planner) -> None:
        """Hold the planner to reuse its cost model, pushdown, and sub-planning."""
        self._planner = planner

    def try_ship(self, node):
        """Return a PhysicalShipment collapsing ``node`` into one island, or None
        when the subtree's shape or size does not qualify."""
        analysis = self._analyze(node)
        if analysis is None:
            return None
        local, foreign, rows = analysis
        return self._build(node, local, foreign, rows)

    def _analyze(self, node):
        """Validate the shape and cost gates; return (local_source, foreign_scans,
        rows) when the subtree qualifies to ship, else None."""
        if os.environ.get("FEDQ_DIM_SHIPPING") == "0":
            return None
        if self._planner.cost_model is None:
            return None
        if not self._is_shippable_shape(node):
            return None
        if not self._collapses_via_aggregate(node):
            return None
        scans = self._planner._collect_base_scans(node)
        rows = self._scan_rows_map(scans)
        local = self._local_source(scans, rows)
        if local is None or not self._local_is_ship_target(local):
            return None
        foreign = self._foreign_scans(scans, local)
        if not foreign or not self._cost_gate_passes(scans, rows, local, foreign):
            return None
        return local, foreign, rows

    def _scan_rows_map(self, scans):
        """Map each scan to its row estimate (None when unknown). Uses the scan's
        own annotation, else a statistics-backed cost estimate; a size resting on
        any default is left None so the rule declines rather than ship on a
        guess."""
        rows = {}
        for scan in scans:
            rows[id(scan)] = self._scan_rows(scan)
        return rows

    def _scan_rows(self, scan):
        """One scan's row estimate, or None when the source has no statistics."""
        if scan.estimated_rows is not None:
            return scan.estimated_rows
        estimate = self._planner.cost_model.estimate(scan)
        if estimate.defaults_used:
            return None
        return estimate.rows

    def _is_shippable_shape(self, node) -> bool:
        """Every node in the subtree is a deterministic INNER-only relational op
        we can safely ship a dimension across."""
        if not self._node_is_shippable(node):
            return False
        for child in node.children():
            if not self._is_shippable_shape(child):
                return False
        return True

    def _node_is_shippable(self, node) -> bool:
        """A single node's shippability: an INNER join, or a listed safe node."""
        if isinstance(node, Join):
            return node.join_type == JoinType.INNER
        return isinstance(node, _SHIPPABLE_NODES)

    def _collapses_via_aggregate(self, node) -> bool:
        """The subtree must reduce its output through a plain GROUP BY at its
        root (under any row-preserving wrappers). Shipping wins only when little
        crosses the boundary: a bare join or scan ships the whole joined fact for
        no gain (q22 below its rollup, q38/q87's DISTINCT branches), and a
        ROLLUP/CUBE aggregate collapses poorly evaluated in one island
        (q14/q22/q67). Only a plain aggregate reliably shrinks the transfer."""
        current = node
        while isinstance(current, (Projection, Filter, Sort, Limit, SubqueryScan)):
            current = current.children()[0]
        return isinstance(current, Aggregate) and not current.grouping_sets

    def _local_source(self, scans, rows):
        """The datasource holding the largest scan (the fact side), or None when
        no scan carries a cost estimate to compare by."""
        largest = None
        for scan in scans:
            if rows[id(scan)] is None:
                continue
            if largest is None or rows[id(scan)] > rows[id(largest)]:
                largest = scan
        if largest is None:
            return None
        return largest.datasource

    def _local_is_ship_target(self, local) -> bool:
        """The ship target (the fact source) must accept a shipped temp table.
        DuckDB and PostgreSQL do; a read-only source (Parquet) does not. This is
        a real physical capability, not a cost choice - the cost gates decide
        WHETHER to ship among capable targets."""
        source = self._planner.catalog.get_datasource(local)
        return source is not None and source.supports_capability(
            DataSourceCapability.SHIP_TARGET
        )

    def _ship_schema(self, local) -> str:
        """The temp-table schema qualifier for the target: ``pg_temp`` for a
        PostgreSQL fact source, ``temp`` for DuckDB."""
        source = self._planner.catalog.get_datasource(local)
        if isinstance(source, PostgreSQLDataSource):
            return _PG_SHIP_SCHEMA
        return _DUCK_SHIP_SCHEMA

    def _foreign_scans(self, scans, local):
        """Every base scan NOT on the local source (the dimensions to ship)."""
        foreign = []
        for scan in scans:
            if scan.datasource != local:
                foreign.append(scan)
        return foreign

    def _cost_gate_passes(self, scans, rows, local, foreign) -> bool:
        """The local side is large, every foreign size is known and small, and
        the local side dwarfs the foreign total by the required factor."""
        if not self._all_estimated(rows, foreign):
            return False
        local_rows = self._max_rows(scans, rows, local)
        foreign_rows = self._sum_rows(rows, foreign)
        return self._ratio_ok(local_rows, foreign_rows)

    def _all_estimated(self, rows, foreign) -> bool:
        """True when every foreign scan carries a row estimate."""
        for scan in foreign:
            if rows[id(scan)] is None:
                return False
        return True

    def _ratio_ok(self, local_rows, foreign_rows) -> bool:
        """The floor / budget / ratio gates on the two side sizes."""
        if local_rows < SHIP_LOCAL_FLOOR:
            return False
        if foreign_rows > SHIP_ROW_BUDGET:
            return False
        return local_rows >= foreign_rows * SHIP_MIN_RATIO

    def _max_rows(self, scans, rows, local) -> int:
        """The largest estimate among the local source's scans (the fact size)."""
        largest = 0
        for scan in scans:
            if scan.datasource == local and rows[id(scan)] is not None:
                largest = max(largest, rows[id(scan)])
        return largest

    def _sum_rows(self, rows, foreign) -> int:
        """The total estimated rows shipped across all foreign scans."""
        total = 0
        for scan in foreign:
            total += rows[id(scan)]
        return total

    def _build(self, node, local, foreign, rows):
        """Swap the foreign scans for temp scans, collapse to one island, and
        wrap it in the shipments. Returns None if the swap does not collapse
        into an island that exposes exactly the pure plan's output columns."""
        mapping, shipments = self._synthetic_scans(foreign, local, rows)
        swapped = self._replace(node, mapping)
        island = self._planner.single_source.try_build(swapped)
        if island is None:
            return None
        fallback = self._planner.plan_without_shipping(node)
        if self._has_window(fallback) or not self._outputs_match(island, fallback):
            return None
        island = island.model_copy(update={"seeded_schema": fallback.schema()})
        return self._wrap_shipments(shipments, island, local)

    def _has_window(self, node) -> bool:
        """Whether the plan contains a window function. Collapsing a window over
        a grouping/rollup into one remote SELECT bypasses the engine's two-stage
        window split and renders SQL the source cannot bind (q86); such a subtree
        keeps its coordinator plan instead of shipping."""
        if isinstance(node, PhysicalWindow):
            return True
        if isinstance(node, PhysicalHashAggregate) and node.has_window_output():
            return True
        for child in node.children():
            if self._has_window(child):
                return True
        return False

    def _outputs_match(self, island, fallback) -> bool:
        """The collapsed island must expose EXACTLY the columns (in order) that
        the pure cross-source plan does. A shape single-source pushdown cannot
        cleanly collapse - an unprojected derived table yields an island with no
        output columns - is declined here rather than emitted as a broken plan."""
        return list(island.output_names) == list(fallback.schema().names)

    def _synthetic_scans(self, foreign, local, rows):
        """Build a temp scan for each foreign scan; return the id->temp map and
        the (temp_table_name, foreign_scan) shipment list."""
        schema = self._ship_schema(local)
        mapping = {}
        shipments = []
        for scan in foreign:
            name = self._planner.next_ship_name()
            temp = self._temp_scan(scan, name, local, schema, rows[id(scan)])
            mapping[id(scan)] = temp
            shipments.append((name, scan))
        return mapping, shipments

    def _temp_scan(self, foreign_scan, table, local, schema, estimated_rows):
        """A local temp-table scan replacing a foreign scan, under the SAME alias
        and columns so every reference above resolves unchanged. The local
        filters moved into the shipped body, so this scan carries none."""
        # create, NOT foreign_scan.model_copy: this is a fresh BARE read of the
        # shipped table, and model_copy would carry over the foreign scan's
        # filters/aggregates/ordering (which moved into the shipped body and must
        # be ABSENT here). Every field the temp scan needs is named below; all
        # others correctly default to none/empty, giving exactly a plain read.
        return Scan.create(
            datasource=local,
            schema_name=schema,
            table_name=table,
            columns=list(foreign_scan.columns),
            alias=foreign_scan.alias or foreign_scan.table_name,
            estimated_rows=estimated_rows,
        )

    def _replace(self, node, mapping):
        """Rebuild the subtree with each mapped scan replaced by its temp scan."""
        replacement = mapping.get(id(node))
        if replacement is not None:
            return replacement
        children = node.children()
        if not children:
            return node
        new_children = []
        for child in children:
            new_children.append(self._replace(child, mapping))
        return node.with_children(new_children)

    def _wrap_shipments(self, shipments, island, local):
        """Wrap the island in one PhysicalShipment per shipped foreign scan."""
        child = island
        for name, foreign_scan in reversed(shipments):
            body = self._planner.plan_without_shipping(foreign_scan)
            # create: a genuinely fresh node - there is no existing
            # PhysicalShipment to derive from - and all four of its fields are
            # named here. It materializes the body into the local source as temp
            # table `name`, then the child reads it.
            child = PhysicalShipment.create(
                table=name, datasource=local, body=body, child=child
            )
        return child
