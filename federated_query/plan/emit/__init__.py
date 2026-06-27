"""Single SQL-emission path: lower engine expressions/plans to a sqlglot AST.

Every place that needs SQL text builds a sqlglot ``exp.Expression`` here and
renders it once with ``ast.sql(dialect=...)``. There is exactly one expression
converter; the only thing that differs between the remote-pushdown path and the
local DuckDB merge path is the :class:`ColumnResolver` and the dialect string.
"""

from .resolver import (
    ColumnResolver,
    SourceResolver,
    MergeResolver,
    CANONICAL_SOURCE_RESOLVER,
)
from .expressions import SqlglotEmitter, expression_to_ast

__all__ = [
    "ColumnResolver",
    "SourceResolver",
    "MergeResolver",
    "CANONICAL_SOURCE_RESOLVER",
    "SqlglotEmitter",
    "expression_to_ast",
]
