"""Schema metadata classes."""

from typing import List, Optional, Dict

from pydantic import Field, model_validator

from ..model import StateModel
from ..plan.expressions import DataType


class Column(StateModel):
    """Column metadata."""

    name: str
    data_type: DataType
    nullable: bool
    # Parent back-reference, set by the owning Table. A private attr (not a
    # field) so the column/table/schema graph stays acyclic for equality and
    # serialization; it is navigational, not part of the column's identity.
    _table: Optional["Table"] = None

    @property
    def table(self) -> Optional["Table"]:
        """The owning table (a read-only view of the back-reference)."""
        return self._table

    def fully_qualified_name(self) -> str:
        """Get fully qualified column name."""
        if self._table:
            return f"{self._table.fully_qualified_name()}.{self.name}"
        return self.name

    def __repr__(self) -> str:
        return f"Column({self.name}, {self.data_type.value})"


class Table(StateModel):
    """Table metadata."""

    name: str
    columns: List[Column] = Field(default_factory=list)
    # Parent back-reference, set by the owning Schema (private; see Column._table).
    _schema: Optional["Schema"] = None

    @property
    def schema(self) -> Optional["Schema"]:
        """The owning schema (a read-only view of the back-reference)."""
        return self._schema

    @model_validator(mode="after")
    def _link_columns(self) -> "Table":
        """Point each column back at this table.

        Runs on construction only; ``model_copy`` does not re-run validators, so
        a copied Table's columns would keep the original's back-ref. Catalog
        objects are built once and never copied, so this is fine - revisit if
        that changes.
        """
        for col in self.columns:
            col._table = self
        return self

    def get_column(self, name: str) -> Optional[Column]:
        """Get column by name."""
        for col in self.columns:
            if col.name.lower() == name.lower():
                return col
        return None

    def fully_qualified_name(self) -> str:
        """Get fully qualified table name."""
        if self._schema:
            return f"{self._schema.datasource}.{self._schema.name}.{self.name}"
        return self.name

    def __repr__(self) -> str:
        return f"Table({self.name}, cols={len(self.columns)})"


class Schema(StateModel):
    """Schema metadata."""

    name: str
    datasource: str
    tables: Dict[str, Table] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _link_tables(self) -> "Schema":
        """Lowercase table keys and point each table back at this schema.

        get_table/add_table key by lowercase name; normalizing here keeps a
        directly-constructed ``tables`` dict consistent, so a mixed-case key is
        not silently unreachable via get_table.
        """
        normalized = {}
        for key, table in self.tables.items():
            table._schema = self
            normalized[key.lower()] = table
        self.tables = normalized
        return self

    def get_table(self, name: str) -> Optional[Table]:
        """Get table by name."""
        return self.tables.get(name.lower())

    def add_table(self, table: Table) -> None:
        """Add a table to this schema."""
        table._schema = self
        self.tables[table.name.lower()] = table

    def __repr__(self) -> str:
        return f"Schema({self.datasource}.{self.name}, tables={len(self.tables)})"
