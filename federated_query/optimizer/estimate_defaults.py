"""The provenance-carrying cardinality estimate and its combination helpers.

A statistic feeding an estimate is either a MEASUREMENT with provenance (the
source catalog, the learned catalog, a probe) or it is UNKNOWN - never a
fabricated constant. A missing base row count makes the estimate's ``rows``
None and unknown PROPAGATES through derived estimates; a missing NDV or an
inestimable predicate contributes its honest BOUND (no reduction) instead of a
mid-scale guess. Either way the gap is recorded in ``defaults_used`` (e.g.
``"ndv(duck.main.lineitem.l_suppkey)"``), so EXPLAIN can show exactly which
parts of a plan were costed from real statistics and which from bounds - and
decision points can refuse to trust a gap-fed estimate.

The fabricated constants that used to live here (DEFAULT_ROW_COUNT = 1000,
DEFAULT_NDV_FRACTION and the selectivity priors) are deliberately GONE: a
fabricated row count next to a measured one inverts every larger-side decision
(the verified q39 cold-source regression), and a real row count paired with a
fabricated NDV skews exactly the ratios that pick join order (q23).
"""

from typing import List, Optional

from pydantic import Field

from federated_query.model import StateModel


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
    non-equi conjunct multiplies the selectivity. A None value is an UNKNOWN
    statistic: the term contributes its honest bound (no reduction - the
    denominator's floor is 1, a selectivity's ceiling is 1.0) instead of a
    fabricated factor; an unknown equi key still counts toward the composite
    cap (it IS a key, and the cap can only widen the bound, never shrink it)."""
    if is_equi:
        if value is None:
            return denom, selectivity, equi_count + 1
        return denom * value, selectivity, equi_count + 1
    if value is None:
        return denom, selectivity, equi_count
    return denom, selectivity * value, equi_count


def max_known_ndv(left, right) -> Optional[int]:
    """The larger of two join-key NDVs, ignoring UNKNOWN (None) sides: the
    known side's domain is a valid denominator on its own - the same
    containment assumption `useless_key_reduction` applies. Both unknown is
    unknown, and the equi term then prices at its no-reduction bound."""
    if left is None:
        return right
    if right is None:
        return left
    return max(left, right)


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


def orientation_rows(node):
    """A node's size for orientation decisions: a remote query's real OUTPUT
    estimate when the optimizer computed one (its estimated_rows is only the
    deliberate max-base floor for remotes whose root carries no estimate),
    any other node's threaded estimate."""
    output_rows = getattr(node, "output_estimated_rows", None)
    if output_rows is not None:
        return output_rows
    return getattr(node, "estimated_rows", None)


def larger_estimated_side(left, right):
    """The side with the greater cost-estimated row count, or None when either
    estimate is missing or the two tie. Shared by the reduction orientation
    (reduce the larger side) and the hash-build choice (build the smaller), so
    the two decisions can never disagree about which side is bigger."""
    left_rows = orientation_rows(left)
    right_rows = orientation_rows(right)
    if left_rows is None or right_rows is None or left_rows == right_rows:
        return None
    return right if right_rows > left_rows else left


# A dynamic filter is refused when its expected semi-join selectivity is at
# least this fraction: such a filter keeps (nearly) every probe row, so
# collecting, shipping, and applying it is pure overhead. Measured: TPC-H q07
# injects ALL 10k supplier keys into lineitem (l_suppkey NDV = 10k) - the
# filter passes 100% of rows and the temp-table semi-join alone costs +75ms
# on the 6M-row scan.
USELESS_KEYS_NDV_FRACTION = 0.8


def useless_key_reduction(build_keys_ndv, build_rows, probe_column_ndv):
    """True when a planned key reduction provably filters (almost) nothing.

    The expected fraction of probe rows the injected keys keep is
    expected_keys / max(build key NDV, probe column NDV) - the same
    max-NDV denominator the join estimate uses: keys drawn uniformly from
    the build domain hit that share of either side's value domain, whichever
    is wider. expected_keys is the build key's base NDV clamped by the
    build's estimated rows (a filtered build donates fewer distinct keys).
    An unknown build NDV abstains (False, reduce-by-default); an unknown
    probe NDV falls back to the build domain alone (FK containment)."""
    if build_keys_ndv is None:
        return False
    expected_keys = build_keys_ndv
    if build_rows is not None:
        # A filtered build cannot donate more distinct keys than it has rows.
        expected_keys = min(expected_keys, build_rows)
    domain = max(build_keys_ndv, probe_column_ndv or 0)
    return expected_keys >= domain * USELESS_KEYS_NDV_FRACTION


class CardinalityEstimate(StateModel):
    """A row-count estimate plus the provenance of every statistics gap in it.

    ``rows`` is None when the estimate is UNKNOWN - a base row count the
    source, the learned catalog and a probe all lack; unknown propagates to
    every estimate derived from this one. A non-None ``rows`` may still rest
    on gaps (an unknown NDV or an inestimable predicate priced at its
    no-reduction bound); ``defaults_used`` names each gap and the object it
    applied to, so EXPLAIN surfaces them and decision points can refuse to
    treat a gap-fed estimate as a known size.
    """

    rows: Optional[int] = Field(ge=0)
    defaults_used: List[str]

    @classmethod
    def create(
        cls,
        *,
        rows: Optional[int],
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
