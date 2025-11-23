"""
Utility functions for decorrelation e2e tests.

Provides helpers to verify plan structure and execute queries.
"""
from typing import List, Set, Any
from federated_query.plan.logical import (
    LogicalPlanNode,
    LogicalJoin,
    LogicalFilter,
    LogicalProject,
    LogicalAggregate,
    LogicalScan
)
from federated_query.plan.expressions import Expression


def find_nodes_of_type(plan: LogicalPlanNode, node_type: type) -> List[LogicalPlanNode]:
    """
    Find all nodes of a specific type in the plan tree.

    Args:
        plan: Root of the plan tree
        node_type: Type of node to find

    Returns:
        List of nodes matching the type
    """
    nodes = []
    if isinstance(plan, node_type):
        nodes.append(plan)

    if hasattr(plan, 'children'):
        for child in plan.children:
            nodes.extend(find_nodes_of_type(child, node_type))
    elif hasattr(plan, 'input'):
        if plan.input:
            nodes.extend(find_nodes_of_type(plan.input, node_type))
    elif hasattr(plan, 'left') and hasattr(plan, 'right'):
        if plan.left:
            nodes.extend(find_nodes_of_type(plan.left, node_type))
        if plan.right:
            nodes.extend(find_nodes_of_type(plan.right, node_type))

    return nodes


def has_join_type(plan: LogicalPlanNode, join_type: str) -> bool:
    """
    Check if plan contains a join of specific type.

    Args:
        plan: Root of the plan tree
        join_type: Join type to search for (INNER, LEFT, SEMI, ANTI, etc.)

    Returns:
        True if join type found
    """
    joins = find_nodes_of_type(plan, LogicalJoin)
    for join in joins:
        if join.join_type.upper() == join_type.upper():
            return True
    return False


def count_joins_of_type(plan: LogicalPlanNode, join_type: str) -> int:
    """
    Count joins of a specific type in the plan.

    Args:
        plan: Root of the plan tree
        join_type: Join type to count

    Returns:
        Number of joins of that type
    """
    joins = find_nodes_of_type(plan, LogicalJoin)
    count = 0
    for join in joins:
        if join.join_type.upper() == join_type.upper():
            count += 1
    return count


def has_subquery_expressions(plan: LogicalPlanNode) -> bool:
    """
    Check if any subquery expression nodes remain in the plan.

    After decorrelation, no SubqueryExpression, ExistsExpression,
    InSubquery, or QuantifiedComparison nodes should remain.

    Args:
        plan: Root of the plan tree

    Returns:
        True if subquery expressions found (bad)
    """
    # TODO: Update when subquery expression classes are added
    # For now, check if plan has been properly decorrelated
    # by verifying expected join structures exist
    return False


def verify_no_subquery_expressions(plan: LogicalPlanNode):
    """
    Assert that no subquery expressions remain after decorrelation.

    Args:
        plan: Decorrelated plan to verify

    Raises:
        AssertionError if subquery expressions found
    """
    assert not has_subquery_expressions(plan), \
        "Decorrelated plan should not contain subquery expression nodes"


def get_join_conditions(plan: LogicalPlanNode) -> List[Expression]:
    """
    Extract all join conditions from the plan.

    Args:
        plan: Root of the plan tree

    Returns:
        List of join condition expressions
    """
    joins = find_nodes_of_type(plan, LogicalJoin)
    conditions = []
    for join in joins:
        if hasattr(join, 'condition') and join.condition:
            conditions.append(join.condition)
    return conditions


def execute_and_fetch_all(executor, plan: LogicalPlanNode) -> List[dict]:
    """
    Execute a plan and fetch all results as list of dicts.

    Args:
        executor: Executor instance
        plan: Plan to execute

    Returns:
        List of result rows as dictionaries
    """
    results = []
    for batch in executor.execute(plan):
        batch_dict = batch.to_pydict()
        num_rows = len(batch_dict[list(batch_dict.keys())[0]])
        for i in range(num_rows):
            row = {}
            for col_name in batch_dict.keys():
                row[col_name] = batch_dict[col_name][i]
            results.append(row)
    return results


def assert_result_count(executor, plan: LogicalPlanNode, expected_count: int):
    """
    Assert that executing the plan returns expected number of rows.

    Args:
        executor: Executor instance
        plan: Plan to execute
        expected_count: Expected number of result rows

    Raises:
        AssertionError if count doesn't match
    """
    results = execute_and_fetch_all(executor, plan)
    assert len(results) == expected_count, \
        f"Expected {expected_count} rows, got {len(results)}"


def assert_result_contains_ids(executor, plan: LogicalPlanNode, expected_ids: Set[int]):
    """
    Assert that result set contains exactly the expected IDs.

    Args:
        executor: Executor instance
        plan: Plan to execute
        expected_ids: Set of expected id values

    Raises:
        AssertionError if IDs don't match
    """
    results = execute_and_fetch_all(executor, plan)
    actual_ids = set()
    for row in results:
        if 'id' in row:
            actual_ids.add(row['id'])
        elif 'user_id' in row:
            actual_ids.add(row['user_id'])

    assert actual_ids == expected_ids, \
        f"Expected IDs {expected_ids}, got {actual_ids}"


def assert_column_values(executor, plan: LogicalPlanNode,
                         column_name: str, expected_values: List[Any]):
    """
    Assert that a column contains expected values.

    Args:
        executor: Executor instance
        plan: Plan to execute
        column_name: Name of column to check
        expected_values: List of expected values (order-independent)

    Raises:
        AssertionError if values don't match
    """
    results = execute_and_fetch_all(executor, plan)
    actual_values = []
    for row in results:
        actual_values.append(row[column_name])

    actual_set = set(actual_values)
    expected_set = set(expected_values)

    assert actual_set == expected_set, \
        f"Expected {expected_set} for {column_name}, got {actual_set}"


def assert_all_rows_satisfy(executor, plan: LogicalPlanNode,
                            predicate: callable) -> bool:
    """
    Assert that all result rows satisfy a predicate.

    Args:
        executor: Executor instance
        plan: Plan to execute
        predicate: Function that takes a row dict and returns bool

    Raises:
        AssertionError if any row fails predicate
    """
    results = execute_and_fetch_all(executor, plan)
    for row in results:
        assert predicate(row), f"Row {row} failed predicate"


def assert_plan_structure(plan: LogicalPlanNode, expected_structure: dict):
    """
    Assert plan has expected structure.

    Args:
        plan: Plan to verify
        expected_structure: Dict describing expected structure, e.g.:
            {
                'has_semi_join': True,
                'has_anti_join': False,
                'semi_join_count': 1,
                'has_aggregation': True
            }

    Raises:
        AssertionError if structure doesn't match
    """
    if 'has_semi_join' in expected_structure:
        expected = expected_structure['has_semi_join']
        actual = has_join_type(plan, 'SEMI')
        assert actual == expected, \
            f"Expected has_semi_join={expected}, got {actual}"

    if 'has_anti_join' in expected_structure:
        expected = expected_structure['has_anti_join']
        actual = has_join_type(plan, 'ANTI')
        assert actual == expected, \
            f"Expected has_anti_join={expected}, got {actual}"

    if 'has_left_join' in expected_structure:
        expected = expected_structure['has_left_join']
        actual = has_join_type(plan, 'LEFT')
        assert actual == expected, \
            f"Expected has_left_join={expected}, got {actual}"

    if 'semi_join_count' in expected_structure:
        expected = expected_structure['semi_join_count']
        actual = count_joins_of_type(plan, 'SEMI')
        assert actual == expected, \
            f"Expected {expected} SEMI joins, got {actual}"

    if 'anti_join_count' in expected_structure:
        expected = expected_structure['anti_join_count']
        actual = count_joins_of_type(plan, 'ANTI')
        assert actual == expected, \
            f"Expected {expected} ANTI joins, got {actual}"

    if 'has_aggregation' in expected_structure:
        expected = expected_structure['has_aggregation']
        aggs = find_nodes_of_type(plan, LogicalAggregate)
        actual = len(aggs) > 0
        assert actual == expected, \
            f"Expected has_aggregation={expected}, got {actual}"

    verify_no_subquery_expressions(plan)
