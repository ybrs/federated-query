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
    Explain,
    ExplainFormat,
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
    PhysicalExplain,
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
    "Explain",
    "ExplainFormat",
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
    "PhysicalExplain",
    # Expressions
    "Expression",
    "ColumnRef",
    "Literal",
    "BinaryOp",
    "UnaryOp",
    "FunctionCall",
    "CaseExpr",
]
