"""Tests for SELECT * rewrite expansion."""

from typing import List
import pytest

from federated_query.parser import Parser, Binder
from federated_query.preprocessor import StarExpansionError
from federated_query.plan.logical import LogicalPlanNode, Scan
from tests.test_e2e_joins import setup_test_db, setup_catalog


def _collect_scans(node: LogicalPlanNode, scans: List[Scan]) -> None:
    if isinstance(node, Scan):
        scans.append(node)
        return
    for child in node.children():
        _collect_scans(child, scans)


def _scan_by_table(scans: List[Scan], name: str) -> Scan:
    for scan in scans:
        if scan.table_name == name:
            return scan
    raise AssertionError(f"Scan for table {name} not found")


def test_select_star_expands_all_join_tables():
    conn = setup_test_db()
    catalog = setup_catalog(conn)
    parser = Parser()
    binder = Binder(catalog)

    sql = """
        SELECT *
        FROM testdb.main.customers c
        JOIN testdb.main.orders o ON c.id = o.customer_id
    """

    logical_plan = parser.parse_to_logical_plan(sql, catalog)
    bound_plan = binder.bind(logical_plan)

    scans: List[Scan] = []
    _collect_scans(bound_plan, scans)

    customers_scan = _scan_by_table(scans, "customers")
    orders_scan = _scan_by_table(scans, "orders")

    customer_columns = set()
    for column in customers_scan.columns:
        customer_columns.add(column)
    assert customer_columns == {"id", "name", "city"}

    order_columns = set()
    for column in orders_scan.columns:
        order_columns.add(column)
    assert order_columns == {"order_id", "customer_id", "amount", "product"}

    conn.close()


def test_table_star_only_expands_matching_scan():
    conn = setup_test_db()
    catalog = setup_catalog(conn)
    parser = Parser()
    binder = Binder(catalog)

    sql = """
        SELECT c.*, o.order_id
        FROM testdb.main.customers c
        JOIN testdb.main.orders o ON c.id = o.customer_id
    """

    logical_plan = parser.parse_to_logical_plan(sql, catalog)
    bound_plan = binder.bind(logical_plan)

    scans: List[Scan] = []
    _collect_scans(bound_plan, scans)

    customers_scan = _scan_by_table(scans, "customers")
    orders_scan = _scan_by_table(scans, "orders")

    customer_columns = set()
    for column in customers_scan.columns:
        customer_columns.add(column)
    assert customer_columns == {"id", "name", "city"}

    order_columns = set()
    for column in orders_scan.columns:
        order_columns.add(column)
    assert order_columns == {"order_id", "customer_id"}

    conn.close()


def test_star_expansion_raises_for_subquery_sources():
    conn = setup_test_db()
    catalog = setup_catalog(conn)
    parser = Parser()

    sql = """
        SELECT *
        FROM (
            SELECT id, name
            FROM testdb.main.customers
        ) sub
        JOIN testdb.main.orders o ON sub.id = o.customer_id
    """

    with pytest.raises(StarExpansionError):
        parser.parse_to_logical_plan(sql, catalog)

    conn.close()
