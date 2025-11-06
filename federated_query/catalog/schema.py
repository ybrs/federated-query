"""Schema metadata classes."""

from dataclasses import dataclass
from typing import List, Optional, Dict
from ..plan.expressions import DataType


@dataclass
class Column:
    """Column metadata."""

    name: str
    data_type: DataType
    nullable: bool
    table: Optional["Table"] = None

    def fully_qualified_name(self) -> str:
        """Get fully qualified column name."""
        if self.table:
            return f"{self.table.fully_qualified_name()}.{self.name}"
        return self.name

    def __repr__(self) -> str:
        return f"Column({self.name}, {self.data_type.value})"


@dataclass
class Table:
    """Table metadata."""

    name: str
    schema: Optional["Schema"] = None
    columns: List[Column] = None

    def __post_init__(self):
        if self.columns is None:
            self.columns = []
        # Set back-reference to table
        for col in self.columns:
            col.table = self

    def get_column(self, name: str) -> Optional[Column]:
        """Get column by name."""
        for col in self.columns:
            if col.name.lower() == name.lower():
                return col
        return None

    def fully_qualified_name(self) -> str:
        """Get fully qualified table name."""
        if self.schema:
            return f"{self.schema.datasource}.{self.schema.name}.{self.name}"
        return self.name

    def __repr__(self) -> str:
        return f"Table({self.name}, cols={len(self.columns)})"


@dataclass
class Schema:
    """Schema metadata."""

    name: str
    datasource: str
    tables: Dict[str, Table] = None

    def __post_init__(self):
        if self.tables is None:
            self.tables = {}
        # Set back-reference to schema
        for table in self.tables.values():
            table.schema = self

    def get_table(self, name: str) -> Optional[Table]:
        """Get table by name."""
        return self.tables.get(name.lower())

    def add_table(self, table: Table) -> None:
        """Add a table to this schema."""
        table.schema = self
        self.tables[table.name.lower()] = table

    def __repr__(self) -> str:
        return f"Schema({self.datasource}.{self.name}, tables={len(self.tables)})"
