"""Named estimation defaults and the provenance-carrying cardinality estimate.

The cost-based optimizer estimates cardinalities from statistics fetched from
the data sources' catalogs. When a statistic is missing (a table never
ANALYZEd, a connector without NDV support), the estimator falls back to one of
the NAMED defaults below and records WHICH default fed WHICH object in the
estimate's ``defaults_used`` provenance list (e.g.
``"ndv(duck.main.lineitem.l_suppkey)"``). EXPLAIN surfaces that list, so a
plan built on guesses is never silently indistinguishable from a plan built on
real statistics.
"""

from typing import List

from pydantic import Field

from federated_query.model import StateModel


# Row count assumed for a table whose source reports no row count (e.g. a
# PostgreSQL table before its first ANALYZE reports reltuples = -1).
DEFAULT_ROW_COUNT = 1000

# Row count assumed for a join atom that is not a plain table scan (an opaque
# subtree such as a CTE reference) and therefore has no catalog statistics.
DEFAULT_ATOM_ROWS = 1000

# NDV assumed as a fraction of row count when a column's distinct count is
# unknown: ndv = max(1, rows * DEFAULT_NDV_FRACTION).
DEFAULT_NDV_FRACTION = 0.1

# Selectivity of an equality predicate whose column NDV is unknown.
DEFAULT_EQ_SELECTIVITY = 0.1

# Selectivity of a range predicate (<, <=, >, >=) when min/max interpolation
# is not possible (missing bounds or a non-numeric column).
DEFAULT_RANGE_SELECTIVITY = 0.33

# Selectivity of a LIKE predicate; pattern selectivity is not estimable from
# catalog statistics.
DEFAULT_LIKE_SELECTIVITY = 0.1

# Null fraction assumed for IS NULL when the column's null statistics are
# unknown.
DEFAULT_NULL_FRACTION = 0.05

# Relative cost of one row CROSSING A SOURCE BOUNDARY versus one row of
# coordinator join output (C_out). The join-order enumerator adds
# TRANSFER_WEIGHT x (rows shipped from sources to the coordinator) to a
# candidate's cost, which is what makes same-source islands stay adjacent
# (they collapse into one remote query and only their RESULT crosses).
# Calibrated against the federated TPC-H benchmark gate.
TRANSFER_WEIGHT = 1.0


def apply_conjunct_term(is_equi, value, denom, selectivity, equi_count):
    """Fold one join conjunct into the running estimate terms: an equi key
    multiplies the NDV denominator and counts toward the composite cap; a
    non-equi conjunct multiplies the selectivity."""
    if is_equi:
        return denom * value, selectivity, equi_count + 1
    return denom, selectivity * value, equi_count


def cap_composite_denom(denom, equi_count, left_rows, right_rows):
    """Cap a COMPOSITE key's NDV denominator at the smaller side's rows.

    The denominator is the product of per-column NDVs, which assumes the
    columns are independent. For a single key that product is just the key's
    NDV and is left alone (it can legitimately exceed min(rows) - a small
    table referencing a big unique key). For a MULTI-column key the product
    over-counts the distinct combinations, which can never exceed the smaller
    side's rows, so it is capped there (else a composite FK join is grossly
    under-estimated - TPC-H q09's fact island)."""
    if equi_count < 2:
        return denom
    return min(denom, max(1.0, float(min(left_rows, right_rows))))


def larger_estimated_side(left, right):
    """The side with the greater cost-estimated row count, or None when either
    estimate is missing or the two tie. Shared by the reduction orientation
    (reduce the larger side) and the hash-build choice (build the smaller), so
    the two decisions can never disagree about which side is bigger."""
    left_rows = getattr(left, "estimated_rows", None)
    right_rows = getattr(right, "estimated_rows", None)
    if left_rows is None or right_rows is None or left_rows == right_rows:
        return None
    return right if right_rows > left_rows else left


# A dynamic filter is refused when the build side's distinct keys are expected
# to cover at least this fraction of the probe column's value domain: such a
# filter keeps (nearly) every probe row, so collecting, shipping, and applying
# it is pure overhead. Measured: TPC-H q07 injects ALL 10k supplier keys into
# lineitem (l_suppkey NDV = 10k) - the filter passes 100% of rows and the
# temp-table semi-join alone costs +75ms on the 6M-row scan.
USELESS_KEYS_NDV_FRACTION = 0.8


def useless_key_reduction(build_keys_ndv, build_rows, probe_column_ndv):
    """True when a planned key reduction provably filters (almost) nothing:
    the build side's expected distinct keys cover >= USELESS_KEYS_NDV_FRACTION
    of the probe column's value domain. Deciding needs BOTH the build key's
    base NDV and the probe column's base NDV from source statistics; when
    either is unknown this returns False (keep today's reduce-by-default)."""
    if build_keys_ndv is None or probe_column_ndv is None:
        return False
    expected_keys = build_keys_ndv
    if build_rows is not None:
        # A filtered build cannot donate more distinct keys than it has rows.
        expected_keys = min(expected_keys, build_rows)
    return expected_keys >= probe_column_ndv * USELESS_KEYS_NDV_FRACTION


class CardinalityEstimate(StateModel):
    """A row-count estimate plus the provenance of every default that fed it.

    ``defaults_used`` entries name both the default and the object it was
    applied to, so EXPLAIN can show exactly which parts of a plan were costed
    from real statistics and which from documented guesses.
    """

    rows: int = Field(ge=0)
    defaults_used: List[str]

    @classmethod
    def create(
        cls,
        *,
        rows: int,
        defaults_used: List[str],
    ) -> "CardinalityEstimate":
        """Sanctioned fresh-construction path for CardinalityEstimate.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            rows=rows,
            defaults_used=defaults_used,
        )


def combine_defaults(
    parents: List[CardinalityEstimate], extra_defaults: List[str]
) -> List[str]:
    """Union the provenance of parent estimates plus new defaults, in first-
    occurrence order and without duplicates, for a derived estimate."""
    merged = []
    seen = set()
    for parent in parents:
        _extend_unique(merged, seen, parent.defaults_used)
    _extend_unique(merged, seen, extra_defaults)
    return merged


def _extend_unique(merged: List[str], seen: set, entries: List[str]) -> None:
    """Append entries not seen yet, tracking them in the seen set."""
    for entry in entries:
        if entry not in seen:
            seen.add(entry)
            merged.append(entry)
