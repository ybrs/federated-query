"""Catalog for managing metadata across all data sources."""

from typing import Dict, Optional, List, Tuple
from ..datasources.base import DataSource
from .schema import Schema, Table, Column
from ..plan.expressions import DataType


class Catalog:
    """Central catalog managing metadata from all data sources."""

    def __init__(self):
        """Initialize catalog."""
        self.datasources: Dict[str, DataSource] = {}
        self.schemas: Dict[Tuple[str, str], Schema] = (
            {}
        )  # (datasource, schema_name) -> Schema
        self._metadata_loaded = False

    def register_datasource(self, datasource: DataSource) -> None:
        """Register a data source with the catalog.

        Args:
            datasource: Data source to register
        """
        self.datasources[datasource.name] = datasource

    def load_metadata(self) -> None:
        """Load metadata from all registered data sources.

        This discovers all schemas, tables, and columns from each data source.
        """
        for ds_name, datasource in self.datasources.items():
            # Ensure the source is connected (pooled sources have no single
            # `connection` attribute, so ask the source itself).
            if not datasource.is_connected():
                datasource.connect()

            # Load schemas
            schema_names = datasource.list_schemas()
            for schema_name in schema_names:
                schema = Schema(name=schema_name, datasource=ds_name)

                # Load tables
                table_names = datasource.list_tables(schema_name)
                for table_name in table_names:
                    metadata = datasource.get_table_metadata(schema_name, table_name)

                    # Convert to our Column format
                    columns = []
                    for col_meta in metadata.columns:
                        # TODO: Proper type mapping
                        data_type = self._map_type(col_meta.data_type)
                        columns.append(
                            Column(
                                name=col_meta.name,
                                data_type=data_type,
                                nullable=col_meta.nullable,
                            )
                        )

                    table = Table(name=table_name, columns=columns)
                    schema.add_table(table)

                # Register schema
                self.schemas[(ds_name, schema_name)] = schema

        self._metadata_loaded = True

    def get_datasource(self, name: str) -> Optional[DataSource]:
        """Get data source by name.

        Args:
            name: Data source name

        Returns:
            Data source if found, None otherwise
        """
        return self.datasources.get(name)

    def get_schema(self, datasource: str, schema_name: str) -> Optional[Schema]:
        """Get schema by data source and name.

        Args:
            datasource: Data source name
            schema_name: Schema name

        Returns:
            Schema if found, None otherwise
        """
        return self.schemas.get((datasource, schema_name))

    def get_table(
        self, datasource: str, schema_name: str, table_name: str
    ) -> Optional[Table]:
        """Get table by fully qualified name.

        Args:
            datasource: Data source name
            schema_name: Schema name
            table_name: Table name

        Returns:
            Table if found, None otherwise
        """
        schema = self.get_schema(datasource, schema_name)
        if schema:
            return schema.get_table(table_name)
        return None

    def _map_type(self, type_str: str) -> DataType:
        """Map a database type string to a DataType, most specific first.

        Ordering matters: TIMESTAMP/DATETIME must be matched before DATE (else
        ``DATETIME`` mis-maps to DATE), and integer matching must be word-aware
        so ``POINT`` (which contains ``INT``) is not read as an integer.
        """
        normalized = type_str.upper().split("(")[0].strip()
        temporal = self._map_temporal_type(normalized)
        if temporal is not None:
            return temporal
        numeric = self._map_numeric_type(normalized)
        if numeric is not None:
            return numeric
        return self._map_textual_type(normalized)

    def _map_temporal_type(self, type_str: str) -> Optional[DataType]:
        """Map date/time types, checking TIMESTAMP/DATETIME before DATE."""
        if "TIMESTAMP" in type_str or "DATETIME" in type_str:
            return DataType.TIMESTAMP
        if "DATE" in type_str:
            return DataType.DATE
        if "TIME" in type_str:
            return DataType.TIMESTAMP
        return None

    def _map_numeric_type(self, type_str: str) -> Optional[DataType]:
        """Map numeric types; integer match avoids the POINT/INT trap."""
        if "DOUBLE" in type_str or "NUMERIC" in type_str or "DECIMAL" in type_str:
            return DataType.DOUBLE
        if "FLOAT" in type_str or "REAL" in type_str:
            return DataType.FLOAT
        if "BIGINT" in type_str or "INT8" in type_str or "BIGSERIAL" in type_str:
            return DataType.BIGINT
        if self._is_integer_type(type_str):
            return DataType.INTEGER
        return None

    def _is_integer_type(self, type_str: str) -> bool:
        """Whether a type name denotes an integer (word-aware, not POINT)."""
        return type_str.startswith("INT") or type_str in (
            "SMALLINT",
            "SERIAL",
            "INTEGER",
        )

    def _map_textual_type(self, type_str: str) -> DataType:
        """Map boolean and string types; raise on a type the engine does not model.

        An unknown type is NOT silently coerced to VARCHAR: a mis-typed column is
        a wrong answer with no error. An unmodeled type must be added explicitly.
        """
        if "BOOL" in type_str:
            return DataType.BOOLEAN
        if "CHAR" in type_str or "TEXT" in type_str or "STRING" in type_str:
            return DataType.VARCHAR if "VAR" in type_str else DataType.TEXT
        raise ValueError(f"Unsupported column type for catalog mapping: {type_str}")

    def __repr__(self) -> str:
        return (
            f"Catalog(datasources={len(self.datasources)}, schemas={len(self.schemas)})"
        )
