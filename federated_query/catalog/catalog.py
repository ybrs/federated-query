"""Catalog for managing metadata across all data sources."""

from typing import Dict, Optional, List, Tuple
from ..datasources.base import DataSource
from .schema import Schema, Table, Column
from ..plan.arrow_types import is_renderable


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

                    # Convert to our Column format. The source maps its own
                    # native type, so the catalog and the execution path never
                    # disagree on what a column is; the engine guarantees that
                    # DataType renders to Arrow, so a source that ever produces a
                    # non-renderable type fails loudly here, not mid-query.
                    columns = []
                    for col_meta in metadata.columns:
                        data_type = datasource.map_native_type(col_meta.data_type)
                        self._require_renderable(col_meta.name, data_type)
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

    def _require_renderable(self, column_name: str, data_type) -> None:
        """Raise if a column's mapped DataType has no Arrow rendering.

        The connector type contract: map_native_type must yield a DataType the
        engine can render to Arrow. A gap is a connector bug, surfaced here with
        the offending column rather than as a later execution crash.
        """
        if not is_renderable(data_type):
            raise ValueError(
                f"Column {column_name!r} maps to DataType {data_type.value}, "
                f"which has no Arrow rendering"
            )

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

    def __repr__(self) -> str:
        return (
            f"Catalog(datasources={len(self.datasources)}, schemas={len(self.schemas)})"
        )
