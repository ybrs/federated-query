"""The case model for the federated e2e corpus.

A case is a plain dict describing one query and how to check it. Required keys:

- ``name``: unique identifier, also the pytest id stem.
- ``tables``: list of library table names (see ``tables.py``) the query uses.
- ``query``: SQL that references each table as a ``{table_name}`` placeholder.
  The harness fills each placeholder with the placement's fully qualified name
  (``str.format``), so a placeholder for an undeclared table raises loudly and a
  bare substring like ``order_id`` is never mistaken for the ``orders`` table.

Optional keys:

- ``order_sensitive`` (default False): compare rows in exact order. Set True only
  when the query ends in a deterministic top-level ORDER BY; otherwise rows are
  compared as a multiset.
- ``expect_error`` (default None): a substring the engine's error must contain.
  The case must RAISE in every placement; no oracle is computed for it.
- ``min_sources`` (default 1): skip placements whose assignment spreads the
  case's tables over fewer than this many distinct sources (cross-source-only
  cases).
- ``extra_tables`` (default None): a dict of inline table specs
  (name -> {"ddl", "inserts"}) seeded alongside the library tables for this case.

``validate_case`` raises on any unknown key, missing required key, or malformed
value, so a typo in a case fails at collection rather than as a wrong result.
"""

import os
import string

from tests.e2e_federated import corpus_aggregates
from tests.e2e_federated import corpus_edges
from tests.e2e_federated import corpus_gaps
from tests.e2e_federated import corpus_joins
from tests.e2e_federated import corpus_setops_ctes
from tests.e2e_federated import corpus_subqueries
from tests.e2e_federated import corpus_windows_expressions
from tests.e2e_federated import tables as table_library
from tests.e2e_federated.sanity import corpus as sanity_corpus

CORPUS_MODULES = {
    "sanity": sanity_corpus,
    "corpus_joins": corpus_joins,
    "corpus_edges": corpus_edges,
    "corpus_subqueries": corpus_subqueries,
    "corpus_aggregates": corpus_aggregates,
    "corpus_setops_ctes": corpus_setops_ctes,
    "corpus_windows_expressions": corpus_windows_expressions,
    "corpus_gaps": corpus_gaps,
}

REQUIRED_KEYS = ("name", "tables", "query")
OPTIONAL_KEYS = (
    "order_sensitive",
    "expect_error",
    "min_sources",
    "extra_tables",
)
_ALL_KEYS = REQUIRED_KEYS + OPTIONAL_KEYS


def validate_case(case):
    """Validate one case dict, raising on any structural or reference error."""
    _reject_unknown_keys(case)
    _require_present(case)
    _validate_tables(case)
    extra = _validate_extra_tables(case)
    _validate_options(case)
    _validate_placeholders(case, extra)


def _reject_unknown_keys(case):
    """Raise if the case carries a key outside the known required/optional set."""
    for key in case:
        if key not in _ALL_KEYS:
            raise ValueError("case has unknown key '" + str(key) + "'")


def _require_present(case):
    """Raise if any required key is missing from the case."""
    for key in REQUIRED_KEYS:
        if key not in case:
            raise ValueError("case missing required key '" + key + "'")


def _validate_tables(case):
    """Raise unless ``tables`` is a list of known library table names."""
    names = case["tables"]
    if not isinstance(names, list):
        raise ValueError("case '" + case["name"] + "' tables must be a list")
    for name in names:
        table_library.get_spec(name)


def _validate_extra_tables(case):
    """Return the case's inline extra-table names, validating their specs."""
    extra = case.get("extra_tables")
    if extra is None:
        return set()
    if not isinstance(extra, dict):
        raise ValueError("case '" + case["name"] + "' extra_tables must be a dict")
    names = set()
    for name, spec in extra.items():
        _validate_inline_spec(case, name, spec)
        names.add(name)
    return names


def _validate_inline_spec(case, name, spec):
    """Raise unless an inline extra-table spec has a DDL and an insert list."""
    if "ddl" not in spec or "inserts" not in spec:
        raise ValueError(
            "case '" + case["name"] + "' extra table '" + name + "' needs ddl+inserts"
        )
    if not isinstance(spec["inserts"], list):
        raise ValueError(
            "case '" + case["name"] + "' extra table '" + name + "' inserts must be a list"
        )


def _validate_options(case):
    """Validate the optional flags' types (order_sensitive, min_sources, ...)."""
    _require_bool(case, "order_sensitive")
    _require_min_sources(case)
    _require_error_substring(case)


def _require_bool(case, key):
    """Raise if an optional key is present but not a bool."""
    if key in case and not isinstance(case[key], bool):
        raise ValueError("case '" + case["name"] + "' " + key + " must be a bool")


def _require_min_sources(case):
    """Raise unless min_sources, when present, is an int of at least one."""
    if "min_sources" not in case:
        return
    value = case["min_sources"]
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValueError("case '" + case["name"] + "' min_sources must be int >= 1")


def _require_error_substring(case):
    """Raise if expect_error is present but not a non-empty string."""
    if "expect_error" not in case:
        return
    value = case["expect_error"]
    if not isinstance(value, str) or value == "":
        raise ValueError("case '" + case["name"] + "' expect_error must be a string")


def _validate_placeholders(case, extra_names):
    """Raise if a query placeholder names a table the case did not declare."""
    declared = set(case["tables"])
    declared.update(extra_names)
    for field in _query_placeholders(case["query"]):
        if field not in declared:
            raise ValueError(
                "case '" + case["name"] + "' query uses undeclared table '" + field + "'"
            )


def _query_placeholders(query):
    """Return the set of ``{field}`` placeholder names used in a query string."""
    fields = set()
    for _literal, field, _spec, _conv in string.Formatter().parse(query):
        if field is not None and field != "":
            fields.add(field)
    return fields


def case_table_specs(case):
    """Return an ordered name -> TableSpec map for every table a case seeds."""
    specs = {}
    for name in case["tables"]:
        specs[name] = table_library.get_spec(name)
    _add_extra_specs(case, specs)
    return specs


def _add_extra_specs(case, specs):
    """Insert the case's inline extra-table specs into the spec map."""
    extra = case.get("extra_tables")
    if extra is None:
        return
    for name, spec in extra.items():
        specs[name] = table_library.TableSpec(
            name=name, ddl=spec["ddl"], inserts=spec["inserts"]
        )


def _selected_modules():
    """Return the corpus modules named by FEDQ_E2E_CORPUS, or all of them.

    FEDQ_E2E_CORPUS is a comma-separated list of CORPUS_MODULES keys; an
    unknown name raises. Unset means every registered module is collected.
    """
    selection = os.environ.get("FEDQ_E2E_CORPUS")
    if selection is None:
        return list(CORPUS_MODULES.values())
    modules = []
    for name in selection.split(","):
        if name not in CORPUS_MODULES:
            raise ValueError("unknown corpus module '" + name + "'")
        modules.append(CORPUS_MODULES[name])
    return modules


def all_cases():
    """Return every validated corpus case, raising on a duplicate name."""
    cases = []
    for module in _selected_modules():
        for case in module.CASES:
            cases.append(case)
    seen = set()
    for case in cases:
        validate_case(case)
        if case["name"] in seen:
            raise ValueError("duplicate case name '" + case["name"] + "'")
        seen.add(case["name"])
    return cases
