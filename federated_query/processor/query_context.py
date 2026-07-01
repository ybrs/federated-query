"""Query context objects shared across middleware and execution."""

from typing import Any, Dict, List, Optional

from ..model import StateModel


class ColumnMapping(StateModel):
    """Represents a single output column's naming data."""

    internal_name: str
    visible_name: str
    alias: Optional[str] = None

    @classmethod
    def create(
        cls,
        *,
        internal_name: str,
        visible_name: str,
        alias: Optional[str] = None,
    ) -> "ColumnMapping":
        """Sanctioned fresh-construction path for ColumnMapping.
        Names every field so none is dropped; derive from an existing node
        with model_copy(update=...) instead of re-listing fields here."""
        return cls(
            internal_name=internal_name,
            visible_name=visible_name,
            alias=alias,
        )


class QueryContext:
    """Tracks metadata every processor can read or update.

    Intentionally a plain mutable class, not a StateModel: this is per-request
    scratch state that processors build up incrementally (rewritten_sql and
    columns are set after construction), not a plan/expression value node. The
    StateModel contract (extra="forbid", validated model_copy) guards the
    field preservation of copied plan nodes, which does not apply to a context
    object that is mutated in place and never copied.
    """

    def __init__(self, original_sql: str):
        """Initialize context with the original user SQL."""
        self.original_sql = original_sql
        self.rewritten_sql = original_sql
        self.metadata: Dict[str, Any] = {}
        self.columns: List[ColumnMapping] = []

    def add_column(self, mapping: ColumnMapping) -> None:
        """Record a new column mapping in projection order."""
        self.columns.append(mapping)

    def set_metadata(self, key: str, value: Any) -> None:
        """Store arbitrary processor metadata."""
        self.metadata[key] = value

    def get_metadata(self, key: str) -> Any:
        """Read metadata produced by earlier processors."""
        return self.metadata.get(key)
