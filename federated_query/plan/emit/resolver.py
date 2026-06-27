"""Column-reference resolution strategies for SQL emission.

The one expression-to-AST converter serves two paths that differ only in how a
``ColumnRef`` becomes a sqlglot node: the remote path emits quoted,
table-qualified source columns; the merge path resolves to the physical Arrow
column name a node exposes. A resolver encapsulates exactly that difference.
"""

from abc import ABC, abstractmethod
from typing import Optional

from sqlglot import exp


class ColumnResolver(ABC):
    """Turns an engine ``ColumnRef`` (table, column) into a sqlglot node."""

    @abstractmethod
    def resolve(self, table: Optional[str], column: str) -> exp.Expression:
        """Build the sqlglot column/star node for one reference."""


class SourceResolver(ColumnResolver):
    """Remote path: emit a quoted, table-qualified column for the source DB.

    Identifiers are always quoted so the source sees exactly the intended name
    with no dialect case-folding; ``*`` (bare or ``alias.*``) maps to a star.
    """

    def resolve(self, table: Optional[str], column: str) -> exp.Expression:
        """Quote the identifier(s); route ``*`` to a (possibly qualified) star."""
        if column == "*":
            return self._star(table)
        return exp.column(column, table=table or None, quoted=True)

    def _star(self, table: Optional[str]) -> exp.Expression:
        """Build ``*`` or ``"alias".*`` depending on whether a table is given."""
        if table:
            return exp.Column(this=exp.Star(), table=exp.to_identifier(table, quoted=True))
        return exp.Star()


# The canonical resolver for the engine's internal Postgres-form SQL. Used by
# Expression.to_sql(), which is the one place every diagnostic/string caller
# shares.
CANONICAL_SOURCE_RESOLVER = SourceResolver()
