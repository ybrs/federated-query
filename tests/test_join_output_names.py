"""Right-side join output naming stays collision-free in chained self-joins.

A left-deep join that references the same relation three or more times exposes
the same column name from several relations. `_right_output_name` renames a
right column that collides with a left name to `right_<name>`; the bug was that
it never checked whether `right_<name>` was ITSELF already produced by an
earlier join in the chain, so two relations' columns collapsed onto one physical
name and column resolution picked the wrong one - the query then dropped every
row. (Concretely, TPC-DS q31 self-joins one aggregate CTE as ss1/ss2/ss3, so its
`store_sales` column arrives from three relations.) These pin that each colliding
column gets a distinct output name.
"""

from federated_query.plan.physical import _right_output_name


def test_no_collision_keeps_the_name():
    """A right column whose name is free on the left keeps its own name."""
    assert _right_output_name("web_sales", {"store_sales", "ca_county"}) == "web_sales"


def test_single_collision_prefixes_right():
    """A right column colliding with a left name is renamed right_<name>."""
    assert _right_output_name("web_sales", {"web_sales"}) == "right_web_sales"


def test_chained_collision_suffixes_until_unique():
    """When right_<name> was already produced by an earlier join, suffix it.

    In a three-way self-join, the first collision (e.g. the ws1-vs-ws2 join)
    already emitted right_web_sales, so the third relation's web_sales must
    become right_web_sales_1, not reuse right_web_sales.
    """
    left = {"web_sales", "right_web_sales"}
    assert _right_output_name("web_sales", left) == "right_web_sales_1"

    left_two = {"web_sales", "right_web_sales", "right_web_sales_1"}
    assert _right_output_name("web_sales", left_two) == "right_web_sales_2"


def test_two_distinct_right_columns_never_collide():
    """Distinct right columns map to distinct outputs (different bases)."""
    left = {"web_sales", "store_sales", "right_web_sales", "right_store_sales"}
    first = _right_output_name("web_sales", left)
    second = _right_output_name("store_sales", left)
    assert first == "right_web_sales_1"
    assert second == "right_store_sales_1"
    assert first != second
