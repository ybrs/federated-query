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
