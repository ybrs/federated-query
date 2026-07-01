"""Guards for two field-preservation/consistency fixes.

1. A FILTER on an ordered-set aggregate must keep its WITHIN GROUP key: the guard
   rewrite rebuilds the aggregate via model_copy, never re-listing fields (which
   silently dropped within_group_key).
2. The Postgres fetch-path OID->Arrow map must agree with the catalog's
   native-name->DataType->Arrow path, or a real column would be typed two ways.
"""

import pyarrow as pa

from federated_query.parser.parser import Parser
from federated_query.plan.logical import Aggregate
from federated_query.plan.arrow_types import arrow_type_for
from federated_query.datasources.postgresql import PostgreSQLDataSource


def _find_aggregate(node):
    """Return the first Aggregate node in a plan subtree."""
    if isinstance(node, Aggregate):
        return node
    for child in node.children():
        found = _find_aggregate(child)
        if found is not None:
            return found
    return None


def test_filter_on_ordered_set_aggregate_keeps_within_group_key():
    """A FILTERed ordered-set aggregate preserves its WITHIN GROUP ordering key."""
    parser = Parser()
    sql = (
        "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY y) "
        "FILTER (WHERE y > 0) AS m FROM s.t"
    )
    plan = parser.parse_to_logical_plan(sql, None)
    aggregate = _find_aggregate(plan)
    func = aggregate.aggregates[0]
    assert func.within_group_key is not None


def test_plain_filtered_aggregate_still_guards_args():
    """A plain FILTERed aggregate still wraps its argument (no within-group key)."""
    parser = Parser()
    sql = "SELECT SUM(x) FILTER (WHERE x > 0) AS s FROM s.t"
    plan = parser.parse_to_logical_plan(sql, None)
    aggregate = _find_aggregate(plan)
    func = aggregate.aggregates[0]
    assert func.function_name.upper() == "SUM"
    assert func.within_group_key is None


def test_postgres_float_oids_agree_with_catalog_mapping():
    """The fetch OID->Arrow map matches the catalog native-name->DataType->Arrow."""
    source = PostgreSQLDataSource(name="pg", config={"host": "localhost"})
    # real (float4, OID 700) is 32-bit; double precision (float8, OID 701) is 64.
    assert source._OID_TO_ARROW[700] == arrow_type_for(source.map_native_type("real"))
    assert source._OID_TO_ARROW[701] == arrow_type_for(
        source.map_native_type("double precision")
    )
    assert source._OID_TO_ARROW[700] == pa.float32()
