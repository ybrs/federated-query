"""Query plan representations (logical and physical)."""

from .logical import (
    LogicalPlanNode,
    Scan,
    Project,
    Filter,
    Join,
    Aggregate,
    Sort,
    Limit,
    Union,
)
from .physical import (
    PhysicalPlanNode,
    PhysicalScan,
    PhysicalProject,
    PhysicalFilter,
    PhysicalHashJoin,
    PhysicalNestedLoopJoin,
    PhysicalHashAggregate,
    PhysicalSort,
    PhysicalLimit,
    Gather,
)
from .expressions import (
    Expression,
    ColumnRef,
    Literal,
    BinaryOp,
    UnaryOp,
    FunctionCall,
    CaseExpr,
)

__all__ = [
    # Logical nodes
    "LogicalPlanNode",
    "Scan",
    "Project",
    "Filter",
    "Join",
    "Aggregate",
    "Sort",
    "Limit",
    "Union",
    # Physical nodes
    "PhysicalPlanNode",
    "PhysicalScan",
    "PhysicalProject",
    "PhysicalFilter",
    "PhysicalHashJoin",
    "PhysicalNestedLoopJoin",
    "PhysicalHashAggregate",
    "PhysicalSort",
    "PhysicalLimit",
    "Gather",
    # Expressions
    "Expression",
    "ColumnRef",
    "Literal",
    "BinaryOp",
    "UnaryOp",
    "FunctionCall",
    "CaseExpr",
]
