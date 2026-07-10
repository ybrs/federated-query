"""Cost model for query optimization."""

import datetime
import math
from typing import Any, Dict, List, Optional, Tuple

from ..plan.logical import (
    CTE,
    CTERef,
    LogicalPlanNode,
    Scan,
    Projection,
    Filter,
    GroupedLimit,
    Join,
    Aggregate,
    LateralJoin,
    Limit,
    SetOperation,
    SetOpKind,
    SingleRowGuard,
    Sort,
    SubqueryScan,
    Union,
    Values,
    JoinType,
)
from ..plan.expressions import (
    Expression,
    BetweenExpression,
    BinaryOp,
    Cast,
    InList,
    UnaryOp,
    ColumnRef,
    DataType,
    Literal,
    BinaryOpType,
    UnaryOpType,
    combine_and,
    split_conjuncts,
)
from ..config.config import CostConfig
from ..optimizer.estimate_defaults import (
    CardinalityEstimate,
    apply_conjunct_term,
    cap_composite_denom,
    combine_defaults,
    max_known_ndv,
)
from .subplan_signature import subplan_signature, group_column_names
from ..optimizer.pushdown import bare_names
from ..optimizer.statistics import StatisticsCollector
from ..datasources.base import TableStatistics


# Sentinel distinguishing "operand is not a literal" from a literal None.
_NOT_A_LITERAL = object()

# CAST target types whose string literals carry calendar values the range
# interpolation can place on the day scale (DATE '1993-10-01' parses as a
# Cast around a string Literal).
_TEMPORAL_TYPES = (DataType.DATE, DataType.TIMESTAMP)


def _comparable_literal(node):
    """The comparable value of a range operand: a plain Literal's value, a
    temporal CAST of one (DATE 'x') parsed to its date, or _NOT_A_LITERAL."""
    if isinstance(node, Literal):
        return node.value
    if isinstance(node, Cast) and isinstance(node.expr, Literal):
        if node.data_type in _TEMPORAL_TYPES:
            return _parse_temporal(node.expr.value)
        return node.expr.value
    return _NOT_A_LITERAL


def _parse_temporal(value):
    """A date/timestamp literal payload as a datetime.date/.datetime; the
    payload unchanged when it is not an ISO string (already parsed, or a
    shape interpolation will reject downstream)."""
    if not isinstance(value, str):
        return value
    try:
        return datetime.datetime.fromisoformat(value)
    except ValueError:
        return value


def _range_ordinal(value) -> Optional[float]:
    """A range bound's position on one shared ordinal scale: numbers as-is,
    dates and timestamps as (fractional) days since day zero, ISO date or
    timestamp strings parsed first (PostgreSQL histogram bounds arrive as
    text). None for anything else - the caller then falls back to the named
    range default instead of guessing."""
    if isinstance(value, str):
        value = _parse_temporal(value)
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return _temporal_days(value)


def _temporal_days(value) -> Optional[float]:
    """Days since day zero for a date or datetime (fractional for the time of
    day), so date bounds and timestamp literals share one scale."""
    if isinstance(value, datetime.datetime):
        seconds = value.hour * 3600 + value.minute * 60 + value.second
        return float(value.toordinal()) + seconds / 86400.0
    if isinstance(value, datetime.date):
        return float(value.toordinal())
    return None


def _interpolate_fraction(low, high, point, below) -> Optional[float]:
    """Linear position of `point` in [low, high] oriented by direction; None
    when any bound is missing or the span is degenerate."""
    if low is None or high is None or point is None:
        return None
    span = high - low
    if span <= 0:
        return None
    base = min(1.0, max(0.0, (point - low) / span))
    return base if below else 1.0 - base


# The four one-sided range comparison operators.
_RANGE_OPS = (BinaryOpType.LT, BinaryOpType.LTE, BinaryOpType.GT, BinaryOpType.GTE)


def _ordinal_stats(col_stats) -> bool:
    """Whether the column's min/max sit on a usable ordinal scale."""
    low = _range_ordinal(col_stats.min_value)
    high = _range_ordinal(col_stats.max_value)
    return low is not None and high is not None


def _paired_interval_fraction(bounds):
    """One combined fraction over every column with BOTH a lower and an upper
    bound; single-direction bounds are handed back for per-conjunct pricing."""
    fraction = 1.0
    unpaired = []
    for entry in bounds.values():
        if entry["below"] and entry["above"]:
            fraction *= _interval_fraction(entry)
        else:
            for _point, conjunct in entry["below"] + entry["above"]:
                unpaired.append(conjunct)
    return fraction, unpaired


def _interval_fraction(entry) -> float:
    """F(tightest upper) - F(tightest lower) on the column's ordinal scale,
    clamped to [0, 1] (inverted bounds select nothing)."""
    low = _range_ordinal(entry["stats"].min_value)
    high = _range_ordinal(entry["stats"].max_value)
    span = high - low
    if span <= 0:
        return 1.0
    upper = _tightest(entry["below"], min)
    lower = _tightest(entry["above"], max)
    upper_fraction = min(1.0, max(0.0, (upper - low) / span))
    lower_fraction = min(1.0, max(0.0, (lower - low) / span))
    return max(0.0, upper_fraction - lower_fraction)


def _tightest(items, chooser):
    """The tightest bound point among (point, conjunct) pairs."""
    points = []
    for point, _conjunct in items:
        points.append(point)
    return chooser(points)


def _add_rows(left, right) -> Optional[int]:
    """A sum of row counts where UNKNOWN (None) propagates: a sum with an
    unbounded term has no bound."""
    if left is None or right is None:
        return None
    return left + right


def _min_rows(left, right) -> Optional[int]:
    """The smaller of two row counts, ignoring UNKNOWN sides: a minimum is
    still bounded by whichever side is known; both unknown is unknown."""
    if left is None:
        return right
    if right is None:
        return left
    return min(left, right)


def _mul_groups(groups, ndv) -> Optional[int]:
    """A group-count product where UNKNOWN (None) propagates: a product with
    an unknown factor has no value."""
    if groups is None or ndv is None:
        return None
    return groups * ndv




class CostModel:
    """Cost model for estimating query execution cost."""

    def __init__(
        self, config: CostConfig, stats_collector: Optional[StatisticsCollector] = None
    ):
        """Initialize cost model.

        Args:
            config: Cost model configuration
            stats_collector: Statistics collector for cardinality info
        """
        self.config = config
        self.stats_collector = stats_collector
        # CTE bodies registered during the current plan walk, so a CTERef atom
        # estimates its body instead of guessing. reset_cte_registry() scopes
        # the registry to one plan (names could collide across queries).
        self.cte_bodies: Dict[str, LogicalPlanNode] = {}

    def reset_cte_registry(self) -> None:
        """Drop every registered CTE body - called at the start of a plan walk
        so a previous query's same-named CTE can never feed this one's
        estimates."""
        self.cte_bodies.clear()

    def register_cte(self, cte: CTE) -> None:
        """Make a CTE's body available to CTERef estimation by name. A
        RECURSIVE body references its own name and would loop the estimator,
        so it is not registered - its references estimate as unknown."""
        if not cte.recursive:
            self.cte_bodies[cte.name] = cte.cte_plan

    # ---- provenance-carrying estimation (cost-based join ordering) ----

    def estimate(self, plan: LogicalPlanNode) -> CardinalityEstimate:
        """Cardinality estimate of a subtree, carrying the provenance of every
        default that fed it. Covers every logical node type explicitly; a
        genuinely unknown type raises - a silently-guessed cardinality for an
        unmodeled operator would corrupt join ordering."""
        if isinstance(plan, Scan):
            return self._estimate_scan_tracked(plan)
        if isinstance(plan, Filter):
            return self._estimate_filter_tracked(plan)
        if isinstance(plan, (Projection, Sort, SubqueryScan)):
            return self.estimate(plan.input)
        if isinstance(plan, Join):
            return self._estimate_join_tracked(plan)
        if isinstance(plan, Aggregate):
            return self._estimate_aggregate_tracked(plan)
        if isinstance(plan, Limit):
            return self._estimate_limit_tracked(plan)
        return self._estimate_rare_tracked(plan)

    def _estimate_rare_tracked(self, plan: LogicalPlanNode) -> CardinalityEstimate:
        """The remaining node types, each modeled explicitly (never a silent
        catch-all): set operations, CTEs, VALUES, decorrelation wrappers."""
        if isinstance(plan, Union):
            return self._estimate_union_tracked(plan)
        if isinstance(plan, SetOperation):
            return self._estimate_setop_tracked(plan)
        if isinstance(plan, CTE):
            self.register_cte(plan)
            return self.estimate(plan.child)
        if isinstance(plan, CTERef):
            return self._estimate_cte_ref_tracked(plan)
        if isinstance(plan, Values):
            # A VALUES relation's cardinality is literally its row count;
            # nothing is estimated, so the provenance stays empty.
            return CardinalityEstimate.create(
                rows=len(plan.rows), defaults_used=[]
            )
        return self._estimate_guard_tracked(plan)

    def _estimate_cte_ref_tracked(self, ref: CTERef) -> CardinalityEstimate:
        """A CTE reference estimates its registered BODY - the body plan is a
        real subtree with real statistics, not a guess. An unregistered name
        (a recursive CTE, or an estimate outside a registering plan walk) is
        honestly unknown."""
        body = self.cte_bodies.get(ref.name)
        if body is None:
            # create, not model_copy: a fresh UNKNOWN leaf estimate with no
            # prior node to copy from. Both fields named: rows + provenance.
            return CardinalityEstimate.create(
                rows=None, defaults_used=[f"row_count(cte {ref.name})"]
            )
        return self.estimate(body)

    def _estimate_guard_tracked(self, plan: LogicalPlanNode) -> CardinalityEstimate:
        """Decorrelation wrappers: guards and per-key limits bound their
        input's cardinality, laterals multiply the outer side."""
        if isinstance(plan, (SingleRowGuard, GroupedLimit)):
            return self.estimate(plan.input)
        if isinstance(plan, LateralJoin):
            left = self.estimate(plan.left)
            # The lateral body's per-outer-row multiplicity is not a statistic
            # any catalog holds; without it the product is unknowable, and an
            # unknown must propagate rather than let the outer side's rows
            # masquerade as the join's size.
            return CardinalityEstimate.create(
                rows=None,
                defaults_used=combine_defaults([left], ["lateral_multiplicity"]),
            )
        raise ValueError(f"estimate has no rule for plan node {type(plan).__name__}")

    def _estimate_union_tracked(self, union: Union) -> CardinalityEstimate:
        """A UNION's upper bound: the sum of its inputs. Any unknown branch
        makes the sum unknown (there is no bound on a sum with an unbounded
        term)."""
        total = 0
        parents: List[CardinalityEstimate] = []
        for child in union.inputs:
            child_estimate = self.estimate(child)
            parents.append(child_estimate)
            total = _add_rows(total, child_estimate.rows)
        # Upper bound: DISTINCT can only shrink the concatenation, so the sum
        # of the inputs is the honest (bounding) estimate either way.
        return CardinalityEstimate.create(
            rows=total, defaults_used=combine_defaults(parents, [])
        )

    def _estimate_setop_tracked(self, setop: SetOperation) -> CardinalityEstimate:
        """Set-operation bounds: UNION sums, INTERSECT takes the smaller side,
        EXCEPT keeps at most the left side."""
        left = self.estimate(setop.left)
        right = self.estimate(setop.right)
        rows = self._setop_rows(setop.kind, left.rows, right.rows)
        # Structural bounds only - no statistics are involved, so the merged
        # provenance is exactly the two sides' provenance.
        return CardinalityEstimate.create(
            rows=rows, defaults_used=combine_defaults([left, right], [])
        )

    def _setop_rows(self, kind, left_rows, right_rows) -> Optional[int]:
        """The bounding row count of one set-operation kind. An unknown side
        keeps whatever bound still holds without it: an INTERSECT is no larger
        than either side alone, an EXCEPT no larger than its left side; a
        UNION with an unknown branch has no bound."""
        if kind == SetOpKind.UNION:
            return _add_rows(left_rows, right_rows)
        if kind == SetOpKind.INTERSECT:
            return _min_rows(left_rows, right_rows)
        if kind == SetOpKind.EXCEPT:
            return left_rows
        raise ValueError(f"no cardinality bound for set operation {kind}")

    def join_tree_cost(self, plan: LogicalPlanNode) -> float:
        """C_out: the sum of every join's estimated output rows in a subtree.

        THE SEAM for richer costing: a locality/network term (penalizing
        cross-source joins, rewarding same-source adjacency that pushes down
        as one remote query) is added here, without touching the enumerator.
        """
        total = 0.0
        if isinstance(plan, Join):
            estimate = self.estimate(plan)
            # An UNKNOWN join has no output estimate to add; callers that
            # compare join_tree_cost values must check estimate() provenance
            # before trusting a tree containing unknowns.
            if estimate.rows is not None:
                total += estimate.rows
        for child in plan.children():
            total += self.join_tree_cost(child)
        return total

    def column_ndv(self, subtree: LogicalPlanNode, ref: ColumnRef) -> Optional[int]:
        """The base NDV of a qualified column resolved inside a subtree, from
        source statistics; None when the owner is opaque or has no NDV.

        Public entry for the join-ordering estimator, which resolves join-key
        NDVs against individual region atoms."""
        owner = self._find_relation(subtree, ref.table)
        if owner is None:
            raise ValueError(
                f"column {ref.table}.{ref.column} resolves to no relation"
            )
        return self._owner_column_ndv(owner, ref.column)

    def conjunct_selectivity(
        self, predicate: Expression, target: str
    ) -> Tuple[Optional[float], List[str]]:
        """Tracked selectivity of one predicate with no table statistics in
        scope (cross-relation residual conjuncts). None is UNKNOWN with the
        gap recorded; the caller prices it at its no-reduction bound.

        Public entry for the join-ordering estimator."""
        return self._tracked_selectivity(predicate, None, target)

    def group_key_dimension(
        self, input_node: LogicalPlanNode, key: Expression
    ) -> Tuple[Optional[LogicalPlanNode], Optional[int]]:
        """The (owner relation, NDV) a GROUP BY key resolves to over its input.

        Public entry for the dim-shipping gate, which counts how many
        independent high-cardinality dimensions a GROUP BY spans. The owner is
        None when the key is not a qualified column (an expression or a key that
        resolves to no relation); the NDV is None when the owner has no
        resolvable distinct count. The NDV here is bounded by the owner's row
        count when a column histogram is absent (a small dimension classifies
        low-card even without per-column stats), a widening confined to this
        gate so the join-ordering NDV path is unchanged."""
        if not (isinstance(key, ColumnRef) and key.table is not None):
            return None, None
        owner = self._find_relation(input_node, key.table)
        if owner is None:
            return None, None
        return owner, self._bounded_owner_ndv(owner, key.column)

    def _estimate_scan_tracked(self, scan: Scan) -> CardinalityEstimate:
        """Scan estimate: stats row count x filter selectivity, then pushed-
        down grouping when the scan carries a remote aggregate. An unknown
        base row count stays unknown through the filter (a fraction of an
        unknown is unknown), but a pushed GROUP BY may still bound the output
        by its keys' NDV product."""
        stats = self._scan_stats(scan)
        rows, defaults = self._tracked_base_rows(scan, stats)
        if scan.filters is not None and rows is not None:
            rows, filter_defaults = self._scan_filter_rows(scan, stats, rows)
            defaults = defaults + filter_defaults
        if scan.group_by:
            return self._tracked_scan_groups(scan, stats, rows, defaults)
        # A leaf estimate straight from source statistics (or the recorded
        # gaps); every non-leaf estimate above is derived from these.
        return CardinalityEstimate.create(rows=rows, defaults_used=defaults)

    def _scan_filter_rows(self, scan, stats, rows) -> Tuple[int, List[str]]:
        """(filtered rows, gap notes) for a scan's pushed predicate: the
        engine's own pricing when it is complete; else the SOURCE PLANNER's
        estimate for the rendered scan (the source prices a LIKE or a
        column-vs-column range from ITS statistics - informed, with
        provenance); else the no-reduction bound with the gaps recorded."""
        selectivity, gaps = self._tracked_selectivity(
            scan.filters, stats, self._scan_target(scan)
        )
        if not gaps:
            return self._filtered_rows(rows, selectivity), []
        planner_rows = self._source_scan_estimate(scan)
        if planner_rows is not None:
            # A filtered scan cannot outgrow its base table; clamp defensively.
            return max(1, min(planner_rows, rows)), []
        return self._filtered_rows(rows, selectivity), gaps

    def _source_scan_estimate(self, scan) -> Optional[int]:
        """The source planner's row estimate for this filtered scan via the
        collector's cached EXPLAIN round trip; None when unavailable. Only a
        PLAIN scan asks: a pushed-aggregate scan's filter is a HAVING over
        aggregate OUTPUT names, which do not exist as stored columns - the
        rendered probe would be invalid SQL at the source."""
        if self.stats_collector is None:
            return None
        if scan.group_by or scan.aggregates or scan.grouping_sets:
            return None
        return self.stats_collector.scan_planner_estimate(scan)

    def _filtered_rows(self, rows: int, selectivity: Optional[float]) -> int:
        """Rows after a filter whose selectivity may be UNKNOWN: an unknown
        predicate reduces nothing (its honest ceiling is 1.0), so the input
        rows stand as the bound."""
        if selectivity is None:
            return rows
        return max(1, int(rows * selectivity))

    def _scan_stats(self, scan: Scan) -> Optional[TableStatistics]:
        """The scan's table statistics covering its filter and group columns."""
        if not self.stats_collector:
            return None
        return self.stats_collector.get_table_statistics(
            scan.datasource, scan.schema_name, scan.table_name,
            self._scan_needed_columns(scan),
        )

    def _scan_needed_columns(self, scan: Scan) -> List[str]:
        """The columns a scan's estimate reads stats for: filter + group keys,
        restricted to REAL base columns. A pushed-aggregate scan's filter is a
        HAVING over aggregate outputs (e.g. sum-alias > 300) whose names are
        not stored columns; fetching their stats from the source would fail."""
        needed = set()
        if scan.filters is not None:
            needed.update(bare_names(scan.filters))
        for key in scan.group_by or []:
            needed.update(bare_names(key))
        return sorted(needed & set(scan.columns))

    def _scan_target(self, scan: Scan) -> str:
        """The scan's identity used in provenance entries."""
        return f"{scan.datasource}.{scan.schema_name}.{scan.table_name}"

    def _tracked_base_rows(self, scan, stats) -> Tuple[Optional[int], List[str]]:
        """The raw table row count: a measurement or honestly UNKNOWN (None),
        never a fabricated constant - a made-up count next to a measured one
        inverts every larger-side decision downstream."""
        if stats is not None and stats.row_count is not None:
            return stats.row_count, []
        return None, [f"row_count({self._scan_target(scan)})"]

    def _tracked_scan_groups(self, scan, stats, rows, defaults) -> CardinalityEstimate:
        """A scan with a pushed-down GROUP BY outputs its group count: the
        product of its keys' NDVs bounded by the (filtered) input rows. A key
        with no NDV makes the product unknown; the input rows remain a valid
        bound, and both unknown is unknown."""
        groups = 1
        for key in scan.group_by:
            ndv, key_defaults = self._scan_key_ndv(key, stats, rows, scan)
            groups = _mul_groups(groups, ndv)
            defaults = defaults + key_defaults
        # create, not model_copy: a fresh leaf estimate derived from source
        # statistics, no prior node to copy. Both fields named.
        return CardinalityEstimate.create(
            rows=_min_rows(rows, groups), defaults_used=defaults
        )

    def _scan_key_ndv(self, key, stats, rows, scan) -> Tuple[Optional[int], List[str]]:
        """One pushed group key's NDV from the scan's own statistics, or
        UNKNOWN with its gap recorded - never a fabricated fraction of the
        row count."""
        col_stats = None
        if isinstance(key, ColumnRef) and stats is not None:
            col_stats = stats.column_stats.get(key.column)
        if col_stats is None or col_stats.num_distinct is None:
            gap = f"group_ndv({self._scan_target(scan)}.{self._key_name(key)})"
            return None, [gap]
        return max(1, _min_rows(col_stats.num_distinct, rows)), []

    def _key_name(self, key: Expression) -> str:
        """A group key's display name for provenance entries."""
        if isinstance(key, ColumnRef):
            return key.column
        return type(key).__name__

    def _estimate_filter_tracked(self, node: Filter) -> CardinalityEstimate:
        """A standalone Filter: input estimate x tracked selectivity. An
        unknown input stays unknown; an unknown selectivity reduces nothing
        (its ceiling is 1.0), leaving the input rows as the bound."""
        input_estimate = self.estimate(node.input)
        selectivity, defaults = self._tracked_selectivity(
            node.predicate, None, "filter"
        )
        rows = input_estimate.rows
        if rows is not None:
            rows = self._filtered_rows(rows, selectivity)
        # create, not model_copy: this derives a NEW estimate (filtered rows,
        # merged provenance) from the input's, not a field tweak on it.
        return CardinalityEstimate.create(
            rows=rows, defaults_used=combine_defaults([input_estimate], defaults)
        )

    def _estimate_limit_tracked(self, node: Limit) -> CardinalityEstimate:
        """LIMIT caps the input estimate at offset + limit. The cap is a
        literal value, so it bounds the output even over an UNKNOWN input."""
        input_estimate = self.estimate(node.input)
        rows = _min_rows(input_estimate.rows, node.offset + node.limit)
        # create, not model_copy: a NEW capped estimate carrying the input's
        # provenance forward; both fields are named here.
        return CardinalityEstimate.create(
            rows=rows, defaults_used=input_estimate.defaults_used
        )

    def _estimate_aggregate_tracked(self, agg: Aggregate) -> CardinalityEstimate:
        """Group count = product of the group keys' NDVs, clamped by input."""
        input_estimate = self.estimate(agg.input)
        if not agg.group_by:
            # A global aggregate always produces exactly one row; only the
            # input's provenance carries through (rows=1 needs no default).
            return CardinalityEstimate.create(
                rows=1, defaults_used=input_estimate.defaults_used
            )
        learned = self._learned_group_count(agg)
        if learned is not None:
            # A MEASURED group count (exact, from a pushed single-table GROUP BY
            # the engine ran) replaces the NDV-independence product, which
            # over-counts a correlated key set. It stands even over an unknown
            # input: the group count IS the output, not derived from the input.
            return CardinalityEstimate.create(
                rows=_min_rows(input_estimate.rows, learned),
                defaults_used=input_estimate.defaults_used,
            )
        groups, defaults = self._tracked_group_count(
            agg.group_by, agg.input, input_estimate.rows
        )
        # More groups than input rows is impossible; either side of the clamp
        # may be UNKNOWN and the other still bounds the output (a known key-NDV
        # product bounds an unknown input's group count, and vice versa).
        return CardinalityEstimate.create(
            rows=_min_rows(input_estimate.rows, groups),
            defaults_used=combine_defaults([input_estimate], defaults),
        )

    def _learned_group_count(self, agg: Aggregate) -> Optional[int]:
        """The catalog's MEASURED group count for a plain GROUP BY, or None. The
        subject matches the write side: a single UNFILTERED base table is keyed
        by its name (the folded pushed-aggregate write); anything else - a
        joined/filtered, cross-source aggregate - is keyed by its input's SUBPLAN
        SIGNATURE (the coordinator-aggregate write)."""
        catalog = getattr(self.stats_collector, "stats_catalog", None)
        columns = group_column_names(agg.group_by)
        if catalog is None or columns is None:
            return None
        subject = self._group_subject(agg.input)
        ttl = self.stats_collector.learned_ttl_seconds
        return catalog.group_count(subject, columns, ttl)

    def _group_subject(self, input_node) -> str:
        """The catalog subject for an aggregate's input: a single unfiltered base
        table's name, else the input's subplan signature."""
        if isinstance(input_node, Scan) and input_node.filters is None:
            return f"{input_node.datasource}.{input_node.schema_name}.{input_node.table_name}"
        return subplan_signature(input_node)

    def _tracked_group_count(self, keys, input_node, input_rows):
        """The product of the group keys' NDVs over the input subtree; a key
        with no NDV makes the product UNKNOWN (the caller still bounds the
        output by the input rows when those are known)."""
        groups = 1
        defaults: List[str] = []
        for key in keys:
            ndv, key_defaults = self._group_key_ndv(key, input_node, input_rows)
            groups = _mul_groups(groups, ndv)
            defaults.extend(key_defaults)
        return groups, defaults

    def _group_key_ndv(self, key, input_node, input_rows):
        """One group key's NDV, or UNKNOWN with its gap recorded - never a
        fabricated fraction of the input rows."""
        if isinstance(key, ColumnRef) and key.table is not None:
            owner = self._find_relation(input_node, key.table)
            ndv = self._owner_column_ndv(owner, key.column)
            if ndv is not None:
                return max(1, _min_rows(ndv, input_rows)), []
        return None, [f"group_ndv({self._key_name(key)})"]

    def _estimate_join_tracked(self, join: Join) -> CardinalityEstimate:
        """Join estimate: the NDV formula over equi keys, clamped by type. A
        join over an UNKNOWN side is unknown - a product with an unbounded
        factor has no bound, and substituting anything would let a guess meet
        a measurement in the same arithmetic."""
        left = self.estimate(join.left)
        right = self.estimate(join.right)
        if left.rows is None or right.rows is None:
            # create, not model_copy: a fresh UNKNOWN estimate merging both
            # sides' provenance; there is no single node to copy from.
            return CardinalityEstimate.create(
                rows=None, defaults_used=combine_defaults([left, right], [])
            )
        if join.condition is None or join.join_type == JoinType.CROSS:
            # No condition constrains anything: the honest estimate is the
            # full cross product (this is what join ordering must avoid).
            return CardinalityEstimate.create(
                rows=left.rows * right.rows,
                defaults_used=combine_defaults([left, right], []),
            )
        inner, defaults = self._tracked_inner_rows(join, left, right)
        rows = self._clamp_join_rows(join.join_type, inner, left.rows, right.rows)
        # The type-clamped estimate inherits both sides' provenance plus the
        # gaps its own condition's selectivities hit.
        return CardinalityEstimate.create(
            rows=rows, defaults_used=combine_defaults([left, right], defaults)
        )

    def _tracked_inner_rows(self, join, left, right) -> Tuple[int, List[str]]:
        """Inner-join rows: cross product / capped key NDV x non-equi
        selectivity. The equi-key denominator is the product of per-column
        NDVs; that product assumes the columns are INDEPENDENT and wildly
        over-counts a composite (multi-column FK) key, so it is capped at the
        smaller side's rows - the distinct key COMBINATIONS can never exceed
        that - or a foreign-key join is grossly under-estimated."""
        denom, selectivity, equi_count, defaults = self._conjunct_terms(
            join, left, right)
        denom = cap_composite_denom(denom, equi_count, left.rows, right.rows)
        rows = float(left.rows) * float(right.rows) * selectivity / denom
        return max(1, int(rows)), defaults

    def _conjunct_terms(self, join, left, right):
        """Accumulate the equi-key NDV denominator (product), the non-equi
        selectivity, and the equi-conjunct COUNT across the join's conjuncts
        (the count decides whether the composite cap applies)."""
        denom = 1.0
        selectivity = 1.0
        equi_count = 0
        defaults: List[str] = []
        for conjunct in split_conjuncts(join.condition):
            is_equi, value, conjunct_defaults = self._conjunct_term(
                join, conjunct, left, right
            )
            denom, selectivity, equi_count = apply_conjunct_term(
                is_equi, value, denom, selectivity, equi_count)
            defaults.extend(conjunct_defaults)
        return denom, selectivity, equi_count, defaults

    def _conjunct_term(self, join, conjunct, left, right):
        """(is_equi, value, defaults): for a cross-side equi key the value is
        its NDV (a denominator term); for anything else it is the tracked
        selectivity (a multiplier). Either value may be None (UNKNOWN) - the
        term then contributes its no-reduction bound downstream."""
        pair = self._equi_key_pair(join, conjunct)
        if pair is None:
            factor, defaults = self._tracked_selectivity(conjunct, None, "join")
            return False, factor, defaults
        left_ndv, left_defaults = self._side_key_ndv(join.left, pair[0], left.rows)
        right_ndv, right_defaults = self._side_key_ndv(join.right, pair[1], right.rows)
        return True, max_known_ndv(left_ndv, right_ndv), left_defaults + right_defaults

    def _equi_key_pair(self, join, conjunct):
        """(left_ref, right_ref) for a column=column equality across the two
        sides of the join; None for anything else."""
        if not (isinstance(conjunct, BinaryOp) and conjunct.op == BinaryOpType.EQ):
            return None
        if not isinstance(conjunct.left, ColumnRef):
            return None
        if not isinstance(conjunct.right, ColumnRef):
            return None
        return self._orient_pair(join, conjunct.left, conjunct.right)

    def _orient_pair(self, join, first, second):
        """Assign two column refs to the join's sides by their qualifiers."""
        self._require_qualified(first)
        self._require_qualified(second)
        if self._resolves_in(join.left, first) and self._resolves_in(join.right, second):
            return first, second
        if self._resolves_in(join.left, second) and self._resolves_in(join.right, first):
            return second, first
        return None

    def _require_qualified(self, ref: ColumnRef) -> None:
        """An unqualified column in a join condition is an upstream bug: every
        post-binder reference must carry its relation qualifier."""
        if not ref.table:
            raise ValueError(
                f"unqualified column {ref.column!r} in a join condition; "
                "every post-binder reference must be qualified"
            )

    def _resolves_in(self, node, ref: ColumnRef) -> bool:
        """Whether the ref's qualifier names a relation inside this subtree."""
        return self._find_relation(node, ref.table) is not None

    def _find_relation(self, node, qualifier):
        """The relation node owning a qualifier in a subtree, or None.

        Raises on a duplicate qualifier: resolving a join key against an
        ambiguous alias would silently pick a side and mis-estimate.
        """
        matches: List[LogicalPlanNode] = []
        self._collect_relations(node, qualifier, matches)
        if len(matches) > 1:
            raise ValueError(
                f"qualifier {qualifier!r} is ambiguous in the join subtree"
            )
        return matches[0] if matches else None

    def _collect_relations(self, node, qualifier, matches) -> None:
        """Append every relation in the subtree whose name is the qualifier.

        Mirrors the join-graph qualifier collection: scans by alias or table
        name, derived tables and CTE references by their exposed alias."""
        if isinstance(node, Scan):
            name = node.alias if node.alias else node.table_name
            if name == qualifier:
                matches.append(node)
            return
        if isinstance(node, SubqueryScan):
            if node.alias == qualifier:
                matches.append(node)
            return
        if isinstance(node, CTERef):
            name = node.alias if node.alias else node.name
            if name == qualifier:
                matches.append(node)
            return
        for child in node.children():
            self._collect_relations(child, qualifier, matches)

    def _side_key_ndv(self, side, ref, side_rows) -> Tuple[Optional[int], List[str]]:
        """A join key's NDV on its side, clamped to the side's row estimate
        (a filtered side cannot hold more distinct keys than rows). An unknown
        NDV stays UNKNOWN with its gap recorded - never a fabricated fraction
        of the rows, which pairs a guess with a measurement in the join
        denominator (the verified q23 skew)."""
        owner = self._find_relation(side, ref.table)
        if owner is None:
            raise ValueError(
                f"join key {ref.table}.{ref.column} resolves to no relation"
            )
        ndv = self._owner_column_ndv(owner, ref.column)
        if ndv is None:
            return None, [f"ndv({self._owner_target(owner, ref)})"]
        return max(1, _min_rows(ndv, side_rows)), []

    def _owner_column_ndv(self, owner, column: str) -> Optional[int]:
        """The base NDV of a scan column from source statistics, else None."""
        if not isinstance(owner, Scan) or not self.stats_collector:
            return None
        stats = self.stats_collector.get_table_statistics(
            owner.datasource, owner.schema_name, owner.table_name, [column]
        )
        if stats is None:
            return None
        col_stats = stats.column_stats.get(column)
        if col_stats is None:
            return None
        return col_stats.num_distinct

    def _bounded_owner_ndv(self, owner, column: str) -> Optional[int]:
        """A column's NDV, falling back to the owner's table row count as an
        upper bound when the column has no histogram (a column cannot hold more
        distinct values than the table has rows). Gate-facing only."""
        ndv = self._owner_column_ndv(owner, column)
        if ndv is not None:
            return ndv
        return self._owner_row_count(owner)

    def _owner_row_count(self, owner) -> Optional[int]:
        """The owning scan's table row count from source statistics, or None."""
        if not isinstance(owner, Scan) or not self.stats_collector:
            return None
        stats = self.stats_collector.get_table_statistics(
            owner.datasource, owner.schema_name, owner.table_name, []
        )
        if stats is None:
            return None
        return stats.row_count

    def _owner_target(self, owner, ref: ColumnRef) -> str:
        """The provenance name of a join key's owning relation and column."""
        if isinstance(owner, Scan):
            return f"{self._scan_target(owner)}.{ref.column}"
        return f"{ref.table}.{ref.column}"

    def _clamp_join_rows(self, join_type, inner, left_rows, right_rows) -> int:
        """Join-type bounds over the inner-join estimate."""
        if join_type == JoinType.INNER:
            return inner
        if join_type == JoinType.LEFT:
            return max(left_rows, inner)
        if join_type == JoinType.RIGHT:
            return max(right_rows, inner)
        return self._clamp_other_join_rows(join_type, inner, left_rows, right_rows)

    def _clamp_other_join_rows(self, join_type, inner, left_rows, right_rows) -> int:
        """FULL preserves both sides; SEMI/ANTI partition the left side."""
        if join_type == JoinType.FULL:
            return max(left_rows + right_rows - inner, left_rows, right_rows)
        matched = self._semi_matched_rows(inner, left_rows)
        if join_type == JoinType.SEMI:
            return max(1, matched)
        if join_type == JoinType.ANTI:
            return max(1, left_rows - matched)
        raise ValueError(f"no join-cardinality clamp for join type {join_type}")

    def _semi_matched_rows(self, inner: int, left_rows: int) -> int:
        """Expected DISTINCT left rows with at least one match, for a semi/anti
        join. min(left, inner) would count match MULTIPLICITY and saturate to
        left_rows for a many-to-many inner (inner >= left), spuriously emptying
        an ANTI join. The occupancy estimate left * (1 - e^-fanout), with
        fanout = inner/left the average matches per left row, never saturates:
        it stays ~inner when inner << left and approaches (not equals) left as
        the fanout grows."""
        if left_rows <= 0:
            return 0
        fanout = inner / left_rows
        return int(left_rows * (1.0 - math.exp(-fanout)))

    def _tracked_selectivity(
        self, predicate: Expression, stats: Optional[TableStatistics], target: str
    ) -> Tuple[Optional[float], List[str]]:
        """Selectivity from measured statistics, or UNKNOWN (None) with the
        gap recorded - never a fabricated prior. Callers price an unknown at
        its no-reduction bound (a selectivity's ceiling is 1.0)."""
        if isinstance(predicate, BinaryOp):
            return self._tracked_binary_selectivity(predicate, stats, target)
        if isinstance(predicate, UnaryOp):
            return self._tracked_unary_selectivity(predicate, stats, target)
        if isinstance(predicate, BetweenExpression):
            return self._tracked_between_selectivity(predicate, stats, target)
        if isinstance(predicate, InList):
            return self._tracked_in_list_selectivity(predicate, stats, target)
        return None, [f"selectivity({target}:{type(predicate).__name__})"]

    def _tracked_in_list_selectivity(self, in_list, stats, target):
        """IN over a column with a known NDV keeps len(options)/ndv of the
        rows (each option matches ~1/ndv, options are distinct); an unknown
        NDV or a non-column value is UNKNOWN."""
        col_ref = in_list.value if isinstance(in_list.value, ColumnRef) else None
        col_stats = self._column_stats_or_none(stats, col_ref)
        if col_stats is None or not col_stats.num_distinct:
            name = col_ref.column if col_ref else type(in_list.value).__name__
            return None, [f"in_selectivity({target}.{name})"]
        return min(1.0, len(in_list.options) / col_stats.num_distinct), []

    def _tracked_between_selectivity(self, between, stats, target):
        """BETWEEN as the equivalent both-bounded conjunction, so the interval
        pairing (not the product of two marginals) prices it."""
        # The lower bound of the interval, value >= lower, built for
        # estimation only (never emitted into a plan).
        lower = BinaryOp.create(
            op=BinaryOpType.GTE, left=between.value, right=between.lower
        )
        # The matching upper bound, value <= upper, completing the pair the
        # interval pricing combines into F(upper) - F(lower).
        upper = BinaryOp.create(
            op=BinaryOpType.LTE, left=between.value, right=between.upper
        )
        return self._tracked_conjunction(combine_and([lower, upper]), stats, target)

    def _tracked_binary_selectivity(self, binop, stats, target) -> Tuple[float, List[str]]:
        """Dispatch a binary predicate to its tracked selectivity rule."""
        if binop.op == BinaryOpType.AND:
            return self._tracked_conjunction(binop, stats, target)
        if binop.op == BinaryOpType.OR:
            return self._tracked_or_selectivity(binop, stats, target)
        if binop.op in (BinaryOpType.EQ, BinaryOpType.NEQ):
            return self._tracked_equality_selectivity(binop, stats, target)
        if binop.op in _RANGE_OPS:
            return self._tracked_range_selectivity(binop, stats, target)
        if binop.op == BinaryOpType.LIKE:
            # Pattern selectivity is not derivable from catalog statistics.
            return None, [f"like_selectivity({target})"]
        return None, [f"selectivity({target}:{binop.op.value})"]

    def _tracked_or_selectivity(self, binop, stats, target):
        """OR as the inclusion-exclusion complement of its two sides; either
        side UNKNOWN makes the disjunction unknown (the complement of an
        unknown is unknown)."""
        left_sel, left_defaults = self._tracked_selectivity(binop.left, stats, target)
        right_sel, right_defaults = self._tracked_selectivity(binop.right, stats, target)
        defaults = left_defaults + right_defaults
        if left_sel is None or right_sel is None:
            return None, defaults
        return 1.0 - ((1.0 - left_sel) * (1.0 - right_sel)), defaults

    def _tracked_conjunction(self, predicate, stats, target) -> Tuple[float, List[str]]:
        """An AND tree: both-bounded range pairs on one column combine into a
        single interval fraction FIRST (the product of the two one-sided
        marginals badly over-estimates a narrow interval: a 3-month
        o_orderdate window is 3.7% of the column, the product says ~21%);
        every other conjunct multiplies in independently."""
        remaining, fraction = self._interval_terms(split_conjuncts(predicate), stats)
        selectivity = fraction
        defaults: List[str] = []
        for conjunct in remaining:
            one, one_defaults = self._tracked_selectivity(conjunct, stats, target)
            # An UNKNOWN conjunct reduces nothing (its ceiling is 1.0); the
            # product of the known conjuncts remains a valid upper bound.
            if one is not None:
                selectivity *= one
            defaults.extend(one_defaults)
        return selectivity, defaults

    def _interval_terms(self, conjuncts, stats):
        """Partition conjuncts into per-column both-bounded range pairs
        (returned as one combined fraction) and everything else."""
        bounds: Dict[str, Dict[str, Any]] = {}
        remaining = []
        for conjunct in conjuncts:
            if not self._bucket_range_bound(conjunct, stats, bounds):
                remaining.append(conjunct)
        fraction, unpaired = _paired_interval_fraction(bounds)
        remaining.extend(unpaired)
        return remaining, fraction

    def _bucket_range_bound(self, conjunct, stats, bounds) -> bool:
        """File one range conjunct under its column when the column's bounds
        and the literal all sit on one ordinal scale; False to leave the
        conjunct for the ordinary per-conjunct path."""
        if not isinstance(conjunct, BinaryOp) or conjunct.op not in _RANGE_OPS:
            return False
        col_ref, value, below = self._range_parts(conjunct)
        col_stats = self._column_stats_or_none(stats, col_ref)
        point = _range_ordinal(value) if col_stats is not None else None
        if point is None or not _ordinal_stats(col_stats):
            return False
        entry = bounds.setdefault(
            col_ref.column, {"below": [], "above": [], "stats": col_stats}
        )
        entry["below" if below else "above"].append((point, conjunct))
        return True

    def _tracked_equality_selectivity(self, binop, stats, target):
        """EQ is 1/ndv when the column's NDV is known; NEQ its complement.
        The complement of an UNKNOWN is unknown."""
        selectivity, defaults = self._tracked_eq_base(binop, stats, target)
        if binop.op == BinaryOpType.NEQ:
            if selectivity is None:
                return None, defaults
            return 1.0 - selectivity, defaults
        return selectivity, defaults

    def _extract_column_ref(self, binop: BinaryOp) -> Optional[ColumnRef]:
        """The column side of a column-vs-value comparison, or None when
        neither operand is a plain column reference."""
        if isinstance(binop.left, ColumnRef):
            return binop.left
        if isinstance(binop.right, ColumnRef):
            return binop.right
        return None

    def _tracked_eq_base(self, binop, stats, target):
        """The equality selectivity: 1/ndv, or UNKNOWN with the gap recorded."""
        col_ref = self._extract_column_ref(binop)
        col_stats = self._column_stats_or_none(stats, col_ref)
        if col_stats is None or col_stats.num_distinct is None:
            name = col_ref.column if col_ref else type(binop).__name__
            return None, [f"eq_selectivity({target}.{name})"]
        if col_stats.num_distinct == 0:
            return 0.0, []
        return min(1.0, 1.0 / col_stats.num_distinct), []

    def _column_stats_or_none(self, stats, col_ref):
        """The column's statistics when both the ref and the stats exist."""
        if stats is None or col_ref is None:
            return None
        return stats.column_stats.get(col_ref.column)

    def _tracked_range_selectivity(self, binop, stats, target):
        """min/max interpolation when possible, else UNKNOWN with the gap
        recorded."""
        fraction = self._range_fraction(binop, stats)
        if fraction is None:
            return None, [f"range_selectivity({target})"]
        return fraction, []

    def _range_fraction(self, binop, stats) -> Optional[float]:
        """(literal - min) / (max - min) oriented by the comparison direction;
        None when the column, bounds, or literal share no ordinal scale."""
        col_ref, value, below = self._range_parts(binop)
        col_stats = self._column_stats_or_none(stats, col_ref)
        if col_stats is None:
            return None
        low = _range_ordinal(col_stats.min_value)
        high = _range_ordinal(col_stats.max_value)
        point = _range_ordinal(value)
        return _interpolate_fraction(low, high, point, below)

    def _range_parts(self, binop):
        """Normalize a range comparison to (column, literal, below) where
        below means the predicate keeps column values BELOW the literal."""
        below_ops = (BinaryOpType.LT, BinaryOpType.LTE)
        left_value = _comparable_literal(binop.left)
        right_value = _comparable_literal(binop.right)
        if isinstance(binop.left, ColumnRef) and right_value is not _NOT_A_LITERAL:
            return binop.left, right_value, binop.op in below_ops
        if left_value is not _NOT_A_LITERAL and isinstance(binop.right, ColumnRef):
            # literal < column reads as column > literal: direction flips.
            return binop.right, left_value, binop.op not in below_ops
        return None, None, False

    def _tracked_unary_selectivity(self, unop, stats, target):
        """NOT complements (unknown stays unknown); IS NULL reads the null
        fraction when known."""
        if unop.op == UnaryOpType.NOT:
            inner, defaults = self._tracked_selectivity(unop.operand, stats, target)
            if inner is None:
                return None, defaults
            return 1.0 - inner, defaults
        if unop.op in (UnaryOpType.IS_NULL, UnaryOpType.IS_NOT_NULL):
            return self._tracked_null_selectivity(unop, stats, target)
        return None, [f"selectivity({target}:{unop.op.value})"]

    def _tracked_null_selectivity(self, unop, stats, target):
        """IS NULL from the column's null fraction; IS NOT NULL complements.
        The complement of an UNKNOWN is unknown."""
        fraction, defaults = self._tracked_null_fraction(unop, stats, target)
        if unop.op == UnaryOpType.IS_NOT_NULL:
            if fraction is None:
                return None, defaults
            return 1.0 - fraction, defaults
        return fraction, defaults

    def _tracked_null_fraction(self, unop, stats, target):
        """The operand column's null fraction, or UNKNOWN with the gap
        recorded."""
        col_ref = unop.operand if isinstance(unop.operand, ColumnRef) else None
        col_stats = self._column_stats_or_none(stats, col_ref)
        if col_stats is None:
            name = col_ref.column if col_ref else type(unop.operand).__name__
            return None, [f"null_fraction({target}.{name})"]
        return col_stats.null_fraction, []

    def __repr__(self) -> str:
        return "CostModel()"
