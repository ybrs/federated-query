"""Shared helpers for pushdown E2E tests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List

import pyarrow as pa

from federated_query.executor import Executor
from federated_query.optimizer import (
    AggregatePushdownRule,
    ExpressionSimplificationRule,
    LimitPushdownRule,
    PhysicalPlanner,
    PredicatePushdownRule,
    ProjectionPushdownRule,
    RuleBasedOptimizer,
)
from federated_query.parser import Binder, Parser
from federated_query.plan.physical import PhysicalPlanNode, PhysicalRemoteJoin

from .conftest import QueryEnvironment


@dataclass
class QueryResult:
    """Container holding execution artifacts for assertions."""

    sql: str
    physical_plan: PhysicalPlanNode
    batches: List[pa.RecordBatch]
    query_log: Dict[str, List[str]]


def run_query(env: QueryEnvironment, sql: str) -> QueryResult:
    """Execute SQL through the full stack and capture plan + datasource SQL."""

    env.clear_queries()

    parser = Parser()
    logical_plan = parser.parse_to_logical_plan(sql)

    binder = Binder(env.catalog)
    bound_plan = binder.bind(logical_plan)

    optimizer = RuleBasedOptimizer(env.catalog)
    optimizer.add_rule(ExpressionSimplificationRule())
    optimizer.add_rule(PredicatePushdownRule())
    optimizer.add_rule(ProjectionPushdownRule())
    optimizer.add_rule(AggregatePushdownRule())
    optimizer.add_rule(LimitPushdownRule())
    optimized_plan = optimizer.optimize(bound_plan)

    planner = PhysicalPlanner(env.catalog)
    physical_plan = planner.plan(optimized_plan)

    executor = Executor()
    batches = list(executor.execute(physical_plan))

    return QueryResult(
        sql=sql,
        physical_plan=physical_plan,
        batches=batches,
        query_log=env.query_log(),
    )


def plan_contains_remote_join(node: PhysicalPlanNode) -> bool:
    """Check if a physical plan subtree already consolidated a remote join."""

    if isinstance(node, PhysicalRemoteJoin):
        return True

    for child in node.children():
        if plan_contains_remote_join(child):
            return True
    return False


def count_physical_nodes(node: PhysicalPlanNode, typ) -> int:
    """Count the number of nodes of a particular type within a plan."""

    count = 1 if isinstance(node, typ) else 0

    for child in node.children():
        count += count_physical_nodes(child, typ)

    return count
