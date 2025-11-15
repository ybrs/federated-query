"""Unit tests for _normalize_join_kind helper function.

This test ensures the function correctly extracts join types from sqlglot AST,
handling both enum.value extraction for 'kind' and 'side' attributes.
"""

from sqlglot import parse_one

from tests.e2e_pushdown.test_advanced_join_types import _normalize_join_kind


def test_normalize_join_kind_left_join():
    """Verify LEFT JOIN is correctly extracted from side enum."""
    sql = "SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id"
    ast = parse_one(sql)
    joins = ast.args.get("joins") or []
    assert len(joins) == 1
    result = _normalize_join_kind(joins[0])
    assert result == "LEFT", f"Expected 'LEFT', got '{result}'"


def test_normalize_join_kind_right_join():
    """Verify RIGHT JOIN is correctly extracted from side enum."""
    sql = "SELECT * FROM t1 RIGHT JOIN t2 ON t1.id = t2.id"
    ast = parse_one(sql)
    joins = ast.args.get("joins") or []
    assert len(joins) == 1
    result = _normalize_join_kind(joins[0])
    assert result == "RIGHT", f"Expected 'RIGHT', got '{result}'"


def test_normalize_join_kind_full_join():
    """Verify FULL OUTER JOIN is correctly extracted from kind enum."""
    sql = "SELECT * FROM t1 FULL OUTER JOIN t2 ON t1.id = t2.id"
    ast = parse_one(sql)
    joins = ast.args.get("joins") or []
    assert len(joins) == 1
    result = _normalize_join_kind(joins[0])
    assert result == "FULL", f"Expected 'FULL', got '{result}'"


def test_normalize_join_kind_inner_join():
    """Verify INNER JOIN is correctly extracted from kind enum."""
    sql = "SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id"
    ast = parse_one(sql)
    joins = ast.args.get("joins") or []
    assert len(joins) == 1
    result = _normalize_join_kind(joins[0])
    assert result == "INNER", f"Expected 'INNER', got '{result}'"


def test_normalize_join_kind_default_join():
    """Verify default JOIN (no keyword) returns INNER."""
    sql = "SELECT * FROM t1 JOIN t2 ON t1.id = t2.id"
    ast = parse_one(sql)
    joins = ast.args.get("joins") or []
    assert len(joins) == 1
    result = _normalize_join_kind(joins[0])
    assert result == "INNER", f"Expected 'INNER', got '{result}'"


def test_normalize_join_kind_no_enum_string_pollution():
    """Verify the function doesn't return enum string representation like 'JOINSIDE.LEFT'."""
    sql = "SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id"
    ast = parse_one(sql)
    joins = ast.args.get("joins") or []
    assert len(joins) == 1
    result = _normalize_join_kind(joins[0])

    assert "JOINSIDE" not in result, f"Result should not contain 'JOINSIDE', got '{result}'"
    assert "JOINKIND" not in result, f"Result should not contain 'JOINKIND', got '{result}'"
    assert result in ["LEFT", "RIGHT", "FULL", "INNER"], f"Result must be clean join type, got '{result}'"
