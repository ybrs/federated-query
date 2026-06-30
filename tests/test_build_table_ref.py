"""build_table_ref is the one scan-to-exp.Table builder for FROM references.

Shared by the remote scan (PhysicalScan._table_ref) and the single-source
pushdown (_scan_ref). Each caller supplies its own alias policy; the node shape
- quoted name, optional quoted schema, optional quoted alias, optional sample -
is built here so it exists once.
"""

from federated_query.plan.physical import build_table_ref


def test_name_and_schema_quoted():
    """A bare scan renders ``"schema"."name"`` with both identifiers quoted."""
    table = build_table_ref("orders", "public")
    assert table.sql(dialect="postgres") == '"public"."orders"'


def test_alias_is_applied_when_given():
    """An alias renders as a quoted AS clause."""
    table = build_table_ref("orders", "public", alias="o")
    assert table.sql(dialect="postgres") == '"public"."orders" AS "o"'


def test_no_alias_when_falsy():
    """A falsy alias leaves the reference unaliased (the remote-scan policy)."""
    table = build_table_ref("orders", "public", alias=None)
    assert " AS " not in table.sql(dialect="postgres")


def test_registered_relation_has_no_schema():
    """A registered Arrow relation reference omits the schema qualifier."""
    table = build_table_ref("in_scan_0", alias="o")
    assert table.sql(dialect="postgres") == '"in_scan_0" AS "o"'


def test_sample_clause_is_attached():
    """A stored TABLESAMPLE fragment is recovered onto the table reference."""
    table = build_table_ref("orders", "public", sample="TABLESAMPLE BERNOULLI (10)")
    rendered = table.sql(dialect="postgres").upper()
    assert "TABLESAMPLE" in rendered
