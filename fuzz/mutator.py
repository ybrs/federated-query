"""Corpus-seeded mutation for the federated fuzzer.

Loads the e2e corpus cases (skipping the ``expect_error`` ones), then derives new
queries by applying small mutations to a known-good query. Each mutation is
labeled semantics-PRESERVING or semantics-CHANGING:

- A PRESERVING mutation (wrap in a pass-through derived table, add a tautology
  filter) must not change the result. It feeds a metamorphic check - the engine's
  result for the mutated query must equal its result for the original - so a bug
  that only shows under the mutated plan is caught even where both still match the
  oracle.
- A CHANGING mutation (DISTINCT wrap, COUNT wrap, INNER->LEFT) yields a different
  but valid query that is checked the normal way against the single-DuckDB oracle.

Every mutation wraps or edits the query as text while preserving its
``{table_name}`` placeholders, so the harness's per-placement qualification keeps
working. A mutation that produces SQL the oracle rejects is discarded downstream.
"""

from fuzz.generator import GenQuery
from tests.e2e_federated import cases as cases_mod


def seed_cases():
    """Return the corpus cases eligible for mutation (no expect_error cases)."""
    eligible = []
    for module in cases_mod.CORPUS_MODULES.values():
        _collect_eligible(module, eligible)
    return eligible


def _collect_eligible(module, eligible):
    """Append a module's mutable cases (no expect_error, no inline extra tables).

    A case with ``extra_tables`` carries inline table specs the fuzzer's spec
    builder does not thread through a mutated query, so its mutations would
    reference an unseeded table; such cases are skipped rather than always
    discarded.
    """
    for case in module.CASES:
        if "expect_error" not in case and "extra_tables" not in case:
            eligible.append(case)


class Mutation:
    """One mutation of a corpus case: the original and mutated queries and kind.

    ``kind`` is ``preserving`` (feeds the metamorphic check) or ``changing``
    (checked against the oracle like any generated query).
    """

    def __init__(self, name, original, mutated, kind):
        """Store the mutation's name, original/mutated GenQuery, and kind."""
        self.name = name
        self.original = original
        self.mutated = mutated
        self.kind = kind


def _original_query(case):
    """Build a GenQuery mirroring a corpus case (its declared order-sensitivity)."""
    order_sensitive = case.get("order_sensitive", False)
    return GenQuery(
        case["query"], case["tables"], "corpus", order_sensitive=order_sensitive
    )


def mutations(rng, case):
    """Return the applicable mutations for one corpus case, in random order."""
    original = _original_query(case)
    built = []
    _add_wrap_mutations(case, original, built)
    _add_join_mutation(case, original, built)
    rng.shuffle(built)
    return built


def _add_wrap_mutations(case, original, built):
    """Add the derived-table wrap mutations that always yield valid SQL."""
    query = case["query"]
    tables = case["tables"]
    built.append(
        _wrap(
            case,
            original,
            tables,
            "SELECT * FROM (" + query + ") mm",
            "wrap_passthru",
            "preserving",
        )
    )
    built.append(
        _wrap(
            case,
            original,
            tables,
            "SELECT * FROM (" + query + ") mm WHERE 1 = 1",
            "wrap_true_filter",
            "preserving",
        )
    )
    built.append(
        _wrap(
            case,
            original,
            tables,
            "SELECT DISTINCT * FROM (" + query + ") mm",
            "wrap_distinct",
            "changing",
        )
    )
    built.append(
        _wrap(
            case,
            original,
            tables,
            "SELECT COUNT(*) AS c0 FROM (" + query + ") mm",
            "wrap_count",
            "changing",
        )
    )


def _wrap(case, original, tables, sql, name, kind):
    """Build a Mutation whose mutated query is multiset-compared (order dropped)."""
    mutated = GenQuery(sql, tables, "mut:" + name, order_sensitive=False)
    return Mutation(name, original, mutated, kind)


def _add_join_mutation(case, original, built):
    """Add an INNER->LEFT mutation when the query is a pure inner-join query."""
    query = case["query"]
    if not _is_pure_inner_join(query):
        return
    mutated_sql = query.replace(" JOIN ", " LEFT JOIN ", 1)
    mutated = GenQuery(
        mutated_sql, case["tables"], "mut:inner_to_left", order_sensitive=False
    )
    built.append(Mutation("inner_to_left", original, mutated, "changing"))


def _is_pure_inner_join(query):
    """Whether a query has a plain JOIN and no outer/cross join to confuse text."""
    if " JOIN " not in query:
        return False
    for marker in (" LEFT JOIN ", " RIGHT JOIN ", " FULL JOIN ", " CROSS JOIN "):
        if marker in query:
            return False
    return True
