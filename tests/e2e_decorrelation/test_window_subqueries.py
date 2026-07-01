"""E2E tests for window functions inside correlated subqueries (Phase 9, 9.4).

A window inside a correlated scalar subquery decorrelates by lifting the
correlation columns into the window's PARTITION BY, then routing the scalar
LIMIT through the existing per-key pick-one machinery. Orders fixture:
    user 1 -> amounts [100, 200]   (order ids 1, 2)
    user 2 -> amounts [150]        (order id 3)
    user 3 -> amounts [300, 50]    (order ids 4, 5)
    user 4 -> no orders
    user 5 -> amounts [500]        (order id 6)
"""

import pytest

from federated_query.parser.parser import Parser
from federated_query.parser.binder import Binder
from federated_query.optimizer.decorrelation import Decorrelator, DecorrelationError
from federated_query.executor.executor import Executor
from .test_utils import execute_and_fetch_all


def _run(catalog, sql):
    """Parse, bind, decorrelate, and execute a query, returning rows by id."""
    plan = Parser().parse(sql)
    bound = Binder(catalog).bind(plan)
    decorrelated = Decorrelator().decorrelate(bound)
    rows = execute_and_fetch_all(Executor(catalog), decorrelated)
    return {row["id"]: row for row in rows}


def test_correlated_window_deterministic(catalog, setup_test_data):
    """Window ranks by amount; outer ORDER BY id DESC picks one row per user.

    The window's implicit per-user partition (correlation user_id = u.id) must
    become PARTITION BY user_id, so each user's ROW_NUMBER is computed over only
    its own orders. Outer ORDER BY id DESC then picks the highest-id order:
        u1: highest id order 2 (amount 200) -> rank 2 of [100, 200]
        u3: highest id order 5 (amount 50)  -> rank 1 of [50, 300]
    """
    sql = """
        SELECT u.id,
               (SELECT ROW_NUMBER() OVER (ORDER BY amount ASC)
                FROM pg.orders WHERE user_id = u.id ORDER BY id DESC LIMIT 1) AS rnk
        FROM pg.users u
    """
    rows = _run(catalog, sql)

    assert len(rows) == 5
    assert rows[1]["rnk"] == 2
    assert rows[2]["rnk"] == 1
    assert rows[3]["rnk"] == 1
    assert rows[4]["rnk"] is None  # no orders -> scalar subquery yields NULL
    assert rows[5]["rnk"] == 1


def test_correlated_window_single_order_users(catalog, setup_test_data):
    """Without an outer ORDER BY the picked row is arbitrary, but bounded.

    A user with one order always ranks 1; a user with two orders ranks 1 or 2;
    a user with no orders yields NULL. This mirrors the original SQL's
    arbitrary-single-row semantics while proving the partition is per-user.
    """
    sql = """
        SELECT u.id,
               (SELECT ROW_NUMBER() OVER (ORDER BY amount)
                FROM pg.orders WHERE user_id = u.id LIMIT 1) AS rnk
        FROM pg.users u
    """
    rows = _run(catalog, sql)

    assert len(rows) == 5
    assert rows[2]["rnk"] == 1  # one order
    assert rows[5]["rnk"] == 1  # one order
    assert rows[4]["rnk"] is None  # no orders
    assert rows[1]["rnk"] in (1, 2)  # two orders
    assert rows[3]["rnk"] in (1, 2)  # two orders


def test_correlated_window_non_equi_correlation_fails(catalog, setup_test_data):
    """A non-equality correlation cannot lift to PARTITION BY; fail fast.

    `amount < u.id` matches many key groups, so a per-key partition would not be
    a per-outer-row window. This needs a general dependent join (Phase 10).
    """
    sql = """
        SELECT u.id,
               (SELECT ROW_NUMBER() OVER (ORDER BY amount)
                FROM pg.orders WHERE amount < u.id LIMIT 1) AS rnk
        FROM pg.users u
    """
    plan = Parser().parse(sql)
    bound = Binder(catalog).bind(plan)
    with pytest.raises(DecorrelationError):
        Decorrelator().decorrelate(bound)
