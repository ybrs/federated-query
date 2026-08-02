"""The plan variants that answer a query differently from the oracle, and why.

An entry names one (variant, case) whose plan returns something the reference
does not. The variant is still RUN and still COMPARED - it is recorded, not
excused - so the defect stays exercised and stays visible in the report.

Every entry is STRICT, as in ``known_failures`` and
``known_fragment_divergences``: a registered pair that starts agreeing FAILS the
run, which forces the entry to be deleted in the change that fixes the defect.

An entry here is heavier than one in the other two registries. It says the engine
computes a different answer depending on which plan it chose for one query over
one dataset, which is the failure this suite exists to detect. It is legitimate
only while the defect is recorded and being worked, never as a way to keep a run
green.
"""

# A RIGHT JOIN null-extends the rows of its right side that find no match, and a
# null-rejecting WHERE on the left side's columns must then eliminate exactly
# those rows. The COORDINATOR does not: its Filter over a RIGHT JOIN keeps a row
# whose predicate is false for it, so the query returns one row where the answer
# is none. A real SQL source gets the same query right, which is why only the
# distributions the coordinator joins are listed - `duck1-duck1` and `pg1-pg1`
# push the whole join into one server and are correct.
_RIGHT_JOIN_NULL_REJECTION = (
    "the coordinator's Filter over a RIGHT JOIN does not reject the "
    "null-extended rows its null-rejecting predicate is false for"
)

_NULL_REJECTION_CASES = (
    "fuzz_right_join_null_rejecting_where_extra_rows",
    "fuzz_right_join_null_rejecting_where_extra_rows_v2",
)

# Every distribution of those cases EXCEPT the two that put both tables on one
# real server, where the join is pushed whole and answered correctly. Co-located
# Parquet is here because a Parquet source declares no join capability, so that
# distribution is joined by the coordinator like the cross-source ones.
_COORDINATOR_JOINED = (
    "pq1-pq1",
    "duck1-duck2",
    "duck1-pg1",
    "duck1-pq1",
    "pg1-duck1",
    "pg1-pg2",
    "pg1-pq1",
    "pq1-duck1",
    "pq1-pg1",
    "pq1-pq2",
)


def _entries():
    """Return (variant, case, distribution) -> reason for every known one."""
    known = {}
    for case in _NULL_REJECTION_CASES:
        for distribution in _COORDINATOR_JOINED:
            known[("predicate_pushdown_off", case, distribution)] = (
                _RIGHT_JOIN_NULL_REJECTION
            )
    return known


KNOWN_DISAGREEMENTS = _entries()


def lookup(variant_name, case_name, distribution_name):
    """Return the reason this variant is known to disagree here, or None."""
    return KNOWN_DISAGREEMENTS.get((variant_name, case_name, distribution_name))
