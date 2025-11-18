"""Query context objects shared across middleware and execution."""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class ColumnMapping:
    """Represents a single output column's naming data."""

    internal_name: str
    visible_name: str
    alias: Optional[str] = None


class QueryContext:
    """Tracks metadata every processor can read or update."""

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
