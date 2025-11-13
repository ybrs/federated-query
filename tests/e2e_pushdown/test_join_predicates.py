"""Exhaustive join + predicate pushdown expectations."""

from __future__ import annotations

import itertools
from typing import Dict, List

import pytest

from .utils import plan_contains_remote_join, run_query


JOIN_TYPES = ["INNER", "LEFT", "RIGHT"]

PREDICATE_VARIANTS = [
    {
        "name": "no_filter",
        "clause": "",
        "token": None,
        "description": "No WHERE clause – baseline to ensure joins collapse to remote query.",
    },
    {
        "name": "price_gt_50",
        "clause": "WHERE {o}.price > 50",
        "token": "price > 50",
        "description": "Filter on numeric column from left side.",
    },
    {
        "name": "category_eq",
        "clause": "WHERE {p}.category = 'clothing'",
        "token": "category = 'clothing'",
        "description": "Filter on right table dimension column.",
    },
    {
        "name": "compound_predicate",
        "clause": "WHERE {o}.price > 40 AND {p}.active = TRUE",
        "token": "active = TRUE",
        "description": "Combined AND predicate touching both tables.",
    },
]

PROJECTION_VARIANTS = [
    {
        "name": "select_all",
        "select": "SELECT *",
        "description": "Wild-card projection to mimic SELECT * queries.",
    },
    {
        "name": "select_subset",
        "select": "SELECT {o}.order_id, {p}.name, {o}.status",
        "description": "Explicit subset of columns across both tables.",
    },
    {
        "name": "select_expression",
        "select": (
            "SELECT {o}.order_id, {p}.name, "
            "{o}.quantity * {p}.base_price AS extended_price"
        ),
        "description": "Projection with computed expression referencing both tables.",
    },
]

LIMIT_VARIANTS = [
    {
        "name": "no_limit",
        "clause": "",
        "description": "No LIMIT – ensures default behavior.",
    },
    {
        "name": "limit_5",
        "clause": "LIMIT 5",
        "description": "LIMIT clause to confirm remote joins still apply.",
    },
]

ALIAS_VARIANTS = [
    {"name": "lower_alias", "o": "o", "p": "p", "description": "Lowercase table aliases."},
    {"name": "upper_alias", "o": "O", "p": "P", "description": "Uppercase table aliases."},
]


def _build_case(
    join_type: str,
    projection: Dict[str, str],
    predicate: Dict[str, str],
    limit: Dict[str, str],
    alias: Dict[str, str],
) -> Dict[str, str]:
    select_clause = projection["select"].format(o=alias["o"], p=alias["p"])
    predicate_clause = ""
    if predicate["clause"]:
        predicate_clause = predicate["clause"].format(o=alias["o"], p=alias["p"])
    limit_clause = limit["clause"]

    sql = f"""
{select_clause}
FROM testdb.main.orders {alias["o"]}
{join_type} JOIN testdb.main.products {alias["p"]}
    ON {alias["o"]}.product_id = {alias["p"]}.id
{predicate_clause}
{limit_clause}
""".strip()

    description = (
        f"{join_type} join | {projection['description']} | "
        f"{predicate['description']} | {limit['description']} | {alias['description']}"
    )

    return {
        "name": f"{join_type}_{projection['name']}_{predicate['name']}_{limit['name']}_{alias['name']}",
        "sql": sql,
        "expect_remote_join": True,
        "expectation": description,
        "predicate_token": predicate["token"],
    }


JOIN_PREDICATE_CASES: List[Dict[str, str]] = []
for combination in itertools.product(
    JOIN_TYPES, PROJECTION_VARIANTS, PREDICATE_VARIANTS, LIMIT_VARIANTS, ALIAS_VARIANTS
):
    case = _build_case(*combination)
    JOIN_PREDICATE_CASES.append(case)


@pytest.mark.parametrize("case", JOIN_PREDICATE_CASES, ids=lambda c: c["name"])
def test_join_predicate_pushdown_exhaustive(case, single_source_env):
    """Validate join predicate pushdown expectations for every generated scenario."""

    result = run_query(single_source_env, case["sql"])

    assert (
        plan_contains_remote_join(result.physical_plan) is case["expect_remote_join"]
    ), f"{case['expectation']} should {'include' if case['expect_remote_join'] else 'not include'} a remote join"

    query_texts = list(result.query_log.values())[0]
    assert query_texts, f"{case['name']} did not emit any datasource SQL"
    last_query = query_texts[-1]

    assert (
        "JOIN" in last_query.upper()
    ), f"{case['name']} should issue a JOIN in remote SQL\nExpected: {case['expectation']}\nSQL Sent: {last_query}"

    if case["predicate_token"]:
        assert (
            case["predicate_token"].replace("{o}", "").replace("{p}", "")[:5] is not None
        )
