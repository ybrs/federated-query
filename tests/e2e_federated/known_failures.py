"""The distributions the engine currently gets wrong, and why.

Each entry names one (case, distribution) that fails today. The test still RUNS -
it is marked xfail, not skipped - so the defect stays exercised and stays visible
in the report instead of disappearing from the run.

Every entry is STRICT: if the engine starts returning the right answer the run
FAILS, which forces the entry to be deleted in the same change that fixes the
defect. That is what stops this file becoming a list of breakage nobody
remembers.

Strictness is only possible because the corpus plans without learned statistics
(see the engine fixture). With them on, six of these distributions pass, because
a learned column NDV changes the join order and the broken plan stops being
chosen; their verdict would then depend on what ran before them and could be
neither asserted as failing nor as passing.
"""

# INTERSECT ALL and EXCEPT ALL pushed whole into a single Parquet source return
# the wrong number of rows: INTERSECT ALL one too many, EXCEPT ALL one too few,
# which is what happens when duplicate multiplicity is not counted per side. The
# same SQL over the same Parquet files is correct when DuckDB runs it directly,
# and both operations are correct on DuckDB and PostgreSQL sources.
_PARQUET_SET_OP = (
    "INTERSECT ALL / EXCEPT ALL pushed into one Parquet source return the wrong "
    "multiset row counts"
)

# A derived table that renames a column (SELECT item_key AS ik ...) loses that
# alias when it is pushed down together with the table it joins to, so the outer
# join on the alias cannot resolve it and the query raises.
_ALIAS_LOST = (
    "a derived table's column alias is dropped when it is pushed down "
    "co-located with the table it joins to"
)

_SET_OP_CASES = (
    "set_intersect_all_shared_ids",
    "set_except_all_shared_ids",
    "gap_intersect_all_three_branches",
    "gap_intersect_all_with_nulls",
    "gap_except_all_with_nulls",
)

# Variation ids name the datasource each table is read from, in table-name order:
# dim_day, dim_dept, dim_item, fact_sales. Every one of these co-locates dim_dept
# with dim_item, which is the pair whose pushdown drops the alias.
_ALIAS_LOST_DISTRIBUTIONS = (
    "duck1-duck2-duck2-duck3",
    "duck1-duck2-duck2-pq1",
    "duck1-pg1-pg1-duck2",
    "duck1-pg1-pg1-pg2",
    "duck1-pg1-pg1-pq1",
    "pg1-duck1-duck1-duck2",
    "pg1-duck1-duck1-pq1",
    "pg1-pg2-pg2-duck1",
    "pg1-pg2-pg2-pg3",
    "pg1-pg2-pg2-pq1",
    "pq1-duck1-duck1-duck2",
    "pq1-duck1-duck1-pq2",
    "pq1-pg1-pg1-duck1",
    "pq1-pg1-pg1-pg2",
    "pq1-pg1-pg1-pq2",
)


def _entries():
    """Return (case name, distribution name) -> reason for every known failure."""
    known = {}
    for case in _SET_OP_CASES:
        known[(case, "pq1-pq1")] = _PARQUET_SET_OP
    for distribution in _ALIAS_LOST_DISTRIBUTIONS:
        known[("multi_key_alias_flow", distribution)] = _ALIAS_LOST
    return known


KNOWN_FAILURES = _entries()


def lookup(case_name, distribution_name):
    """Return the reason this distribution is a known failure, or None."""
    return KNOWN_FAILURES.get((case_name, distribution_name))
