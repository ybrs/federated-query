"""Simple interactive CLI for the federated query engine."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import click
import duckdb
import pyarrow as pa
from prompt_toolkit import PromptSession
from prompt_toolkit.history import FileHistory
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from sqlglot import errors as sqlglot_errors

from ..catalog import Catalog
from ..config import (
    Config,
    DataSourceConfig,
    OptimizerConfig,
    ExecutorConfig,
    CostConfig,
    load_config,
)
from ..datasources.duckdb import DuckDBDataSource
from ..datasources.postgresql import PostgreSQLDataSource
from ..datasources.clickhouse import ClickHouseDataSource
from ..executor import Executor
from ..parser import Binder, Parser, BindingError
from ..optimizer import PhysicalPlanner, build_optimizer
from ..optimizer.factory import build_cost_model
from ..optimizer.decorrelation import Decorrelator
from ..processor import QueryExecutor, StarExpansionError, StarExpansionProcessor


def build_json_explain_table(document: Dict[str, Any]) -> pa.Table:
    """Convert explain document to single-column Arrow table with JSON text."""
    serializable = _serialize_explain_document(document)
    json_text = json.dumps(serializable, indent=2)
    array = pa.array([json_text])
    table = pa.Table.from_arrays([array], names=["plan"])
    return table


def _serialize_explain_document(document: Dict[str, Any]) -> Dict[str, Any]:
    """Produce JSON-safe copy without mutating the original document."""
    output: Dict[str, Any] = {}
    for key in document:
        if key == "queries":
            entries = document[key]
            output["queries"] = _serialize_queries(entries)
        else:
            output[key] = document[key]
    return output


def _serialize_queries(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Convert each remote-query entry to a datasource name plus SQL string."""
    serialized: List[Dict[str, Any]] = []
    for entry in entries:
        datasource = entry.get("datasource_name")
        query_value = entry.get("query")
        query_text = _stringify_query_value(query_value)
        converted = {
            "datasource_name": datasource,
            "query": query_text,
        }
        serialized.append(converted)
    return serialized


def _stringify_query_value(query_value: Any) -> str:
    """Render a query value as Postgres SQL text, or "NULL" when missing."""
    if hasattr(query_value, "sql"):
        return query_value.sql(dialect="postgres")
    if query_value is None:
        return "NULL"
    return str(query_value)


class FedQRuntime:

    def __init__(self, catalog: Catalog, config: Config):
        """Assemble and warm up the full query-execution pipeline.

        Takes the whole Config: the optimizer stack is built from its
        optimizer and cost sections (via the shared factory, so every
        OptimizerConfig flag is honored) and the executor from its executor
        section.
        """
        self.catalog = catalog
        self.parser = Parser()
        self.binder = Binder(catalog)
        # ONE learned-stats catalog for the session, shared by the read path
        # (cost model overlays measured values) and the write path (executor
        # persists measurements). None disables learning.
        self.stats_catalog = self._open_stats_catalog()
        # ONE cost model (and statistics cache) for the session: join ordering
        # and the physical planner's scan annotation read the same numbers.
        cost_model = build_cost_model(
            catalog, config.cost, stats_catalog=self.stats_catalog
        )
        self.optimizer = build_optimizer(
            catalog, config.optimizer, config.cost, cost_model=cost_model
        )
        self.planner = PhysicalPlanner(catalog, cost_model=cost_model)
        self.decorrelator = Decorrelator()
        physical_executor = Executor(
            config=config.executor, stats_catalog=self.stats_catalog
        )
        processors = [StarExpansionProcessor(catalog, dialect=self.parser.dialect)]
        self.query_executor = QueryExecutor(
            catalog=catalog,
            parser=self.parser,
            binder=self.binder,
            optimizer=self.optimizer,
            planner=self.planner,
            physical_executor=physical_executor,
            processors=processors,
            decorrelator=self.decorrelator,
        )

    def _open_stats_catalog(self):
        """Open the learned-stats catalog when FEDQ_STATS_CATALOG names a path,
        else None (learning off). One catalog per configuration, shared by the
        read and write paths."""
        path = os.environ.get("FEDQ_STATS_CATALOG")
        if not path:
            return None
        from ..catalog.stats_catalog import StatsCatalog

        return StatsCatalog(path)

    def execute(self, sql: str) -> Union[pa.Table, Dict[str, Any]]:
        """Run a SQL statement and return results."""
        return self.query_executor.execute(sql)

    def explain(self, sql: str) -> Dict[str, Any]:
        """Return the JSON EXPLAIN document (plan + remote queries) for a query."""
        document = self.execute(f"EXPLAIN (FORMAT JSON) {sql}")
        if not isinstance(document, dict):
            raise RuntimeError("EXPLAIN did not produce a JSON document")
        return document


class ResultPrinter:
    """Formats Arrow tables for CLI display."""

    def __init__(self, emit):
        """Store the callable used to emit each output line."""
        self.emit = emit

    def display(self, table: pa.Table, elapsed_ms: float) -> None:
        """Render the table as an ASCII grid and emit a row count and timing."""
        rows = self._build_rows(table)
        headers = list(table.schema.names)
        lines = self._format_table(headers, rows)
        for line in lines:
            self.emit(line)
        summary = f"{table.num_rows} rows in {elapsed_ms:.2f} ms"
        self.emit(summary)

    def _build_rows(self, table: pa.Table) -> List[List[object]]:
        """Transpose the table's columns into a list of row value lists."""
        columns = self._collect_columns(table)
        rows: List[List[object]] = []
        row_index = 0
        while row_index < table.num_rows:
            row = []
            col_index = 0
            while col_index < len(columns):
                row.append(columns[col_index][row_index])
                col_index += 1
            rows.append(row)
            row_index += 1
        return rows

    def _collect_columns(self, table: pa.Table) -> List[List[object]]:
        """Materialize each table column as a Python list of values."""
        columns: List[List[object]] = []
        column_index = 0
        while column_index < table.num_columns:
            column = table.column(column_index).to_pylist()
            columns.append(column)
            column_index += 1
        return columns

    def _format_table(self, headers: List[str], rows: List[List[object]]) -> List[str]:
        """Build the bordered header-and-rows lines for the ASCII grid."""
        widths = self._compute_widths(headers, rows)
        border = self._build_border(widths)
        lines: List[str] = []
        lines.append(border)
        lines.append(self._format_row(headers, widths))
        lines.append(border)
        for row in rows:
            string_values = self._stringify_row(row)
            lines.append(self._format_row(string_values, widths))
        lines.append(border)
        return lines

    def _compute_widths(
        self,
        headers: List[str],
        rows: List[List[object]],
    ) -> List[int]:
        """Compute each column's width as the longest header or cell text."""
        widths: List[int] = []
        index = 0
        while index < len(headers):
            widths.append(len(headers[index]))
            index += 1
        for row in rows:
            col_index = 0
            while col_index < len(row):
                text = self._stringify_cell(row[col_index])
                current = widths[col_index]
                if len(text) > current:
                    widths[col_index] = len(text)
                col_index += 1
        return widths

    def _build_border(self, widths: List[int]) -> str:
        """Render the horizontal border row for the table at the given column widths."""
        parts: List[str] = []
        parts.append("+")
        index = 0
        while index < len(widths):
            parts.append("-" * (widths[index] + 2))
            parts.append("+")
            index += 1
        return "".join(parts)

    def _format_row(self, values: List[str], widths: List[int]) -> str:
        """Render one pipe-delimited row with each value left-padded to its width."""
        parts: List[str] = []
        parts.append("|")
        index = 0
        while index < len(values):
            value = values[index]
            padded = value.ljust(widths[index])
            parts.append(f" {padded} ")
            parts.append("|")
            index += 1
        return "".join(parts)

    def _stringify_row(self, row: List[object]) -> List[str]:
        """Convert every cell in a row to its display string."""
        string_values: List[str] = []
        for value in row:
            string_values.append(self._stringify_cell(value))
        return string_values

    def _stringify_cell(self, value: object) -> str:
        """Render a single cell value as text, showing None as "NULL"."""
        if value is None:
            return "NULL"
        return str(value)


class CatalogPrinter:
    """Prints catalog metadata in a readable format."""

    def __init__(self, emit):
        """Store the callable used to emit each output line."""
        self.emit = emit

    def display_catalog(self, catalog: Catalog) -> None:
        """Print all catalog schemas, or a notice when the catalog is empty."""
        if not catalog.schemas:
            self.emit("Catalog is empty.")
            return
        self.emit("\nCatalog Contents:")
        self.emit("=" * 80)
        self._print_schemas(catalog)

    def _print_schemas(self, catalog: Catalog) -> None:
        """Print each schema's header and tables in sorted datasource/schema order."""
        for key in sorted(catalog.schemas.keys()):
            datasource, schema_name = key
            schema = catalog.schemas[key]
            self._print_schema_header(datasource, schema_name)
            self._print_tables(datasource, schema_name, schema)
            self.emit("")

    def _print_schema_header(self, datasource: str, schema_name: str) -> None:
        """Print the data source and schema name with an underline rule."""
        header = f"\nData Source: {datasource}, Schema: {schema_name}"
        self.emit(header)
        self.emit("-" * len(header))

    def _print_tables(self, datasource: str, schema_name: str, schema) -> None:
        """Print each table's fully qualified name and columns in sorted order."""
        for table_name in sorted(schema.tables.keys()):
            table = schema.tables[table_name]
            full_name = f"{datasource}.{schema_name}.{table_name}"
            self.emit(f"\nTable: {full_name}")
            self._print_columns(table)

    def _print_columns(self, table) -> None:
        """Print each column's name, data type, and nullability for the table."""
        self.emit("  Columns:")
        for column in table.columns:
            nullable = "NULL" if column.nullable else "NOT NULL"
            self.emit(f"    - {column.name}: {column.data_type.name} {nullable}")


class FedQRepl:
    """Interactive loop with full terminal support."""

    def __init__(
        self,
        runtime: FedQRuntime,
        printer: ResultPrinter,
        catalog_printer: CatalogPrinter,
        catalog: Catalog,
    ):
        """Store the runtime, printers, and catalog, and open a prompt session."""
        self.runtime = runtime
        self.printer = printer
        self.catalog_printer = catalog_printer
        self.catalog = catalog
        self.session = self._create_session()

    def _create_session(self) -> PromptSession:
        """Create prompt session backed by persistent history."""
        history_file = self._history_path()
        history = FileHistory(str(history_file))
        auto_suggest = AutoSuggestFromHistory()
        session = PromptSession(history=history, auto_suggest=auto_suggest)
        return session

    def _history_path(self) -> Path:
        """Return history file path, creating the file when necessary."""
        history_path = Path(".history")
        if not history_path.exists():
            history_path.touch()
        return history_path

    def run(self) -> None:
        """Run the read-eval-print loop, dispatching exits, shortcuts, and SQL."""
        buffer: List[str] = []
        while True:
            line, should_continue = self._read_line(buffer)
            if not should_continue:
                break
            if line is None:
                continue
            if self._is_exit_command(line):
                break
            if self._is_shortcut_command(line):
                self._execute_shortcut(line)
                continue
            buffer.append(line)
            if self._is_complete_statement(line):
                statement = self._build_statement(buffer)
                buffer.clear()
                self._execute_query(statement)

    def _read_line(self, buffer: List[str]) -> Tuple[Optional[str], bool]:
        """Read one prompt line, returning the line and whether to keep looping.

        EOF stops the loop; an interrupt clears the buffer and continues.
        """
        prompt = self._get_prompt(buffer)
        try:
            line = self.session.prompt(prompt)
            return line, True
        except EOFError:
            click.echo("")
            return None, False
        except KeyboardInterrupt:
            click.echo("")
            buffer.clear()
            return None, True

    def _get_prompt(self, buffer: List[str]) -> str:
        """Return the continuation prompt mid-statement, else the primary prompt."""
        if buffer:
            return "...> "
        return "fedq> "

    def _is_exit_command(self, line: str) -> bool:
        """Return whether the line is one of the recognized exit commands."""
        trimmed = line.strip().lower()
        exit_commands = ["\\q", "quit", "exit"]
        for command in exit_commands:
            if trimmed == command:
                return True
        return False

    def _is_shortcut_command(self, line: str) -> bool:
        """Return whether the line is a dot-prefixed shortcut command."""
        trimmed = line.strip()
        return trimmed.startswith(".")

    def _execute_shortcut(self, line: str) -> None:
        """Run the .catalog shortcut, or report an unknown shortcut."""
        trimmed = line.strip().lower()
        if trimmed == ".catalog":
            self.catalog_printer.display_catalog(self.catalog)
        else:
            click.echo(f"Unknown shortcut: {line.strip()}")
            click.echo("Available shortcuts: .catalog")

    def _is_complete_statement(self, line: str) -> bool:
        """Return whether the line ends with a semicolon terminating a statement."""
        return line.strip().endswith(";")

    def _build_statement(self, buffer: List[str]) -> str:
        """Join the buffered input lines into a single newline-separated statement."""
        parts: List[str] = []
        for chunk in buffer:
            parts.append(chunk)
        statement = "\n".join(parts)
        return statement

    def _execute_query(self, statement: str) -> None:
        """Run a statement, display its result or EXPLAIN output, and report errors."""
        clean = self._clean_statement(statement)
        if not clean:
            return
        try:
            start = time.time()
            result = self.runtime.execute(clean)
            elapsed = (time.time() - start) * 1000
            if isinstance(result, dict):
                json_table = build_json_explain_table(result)
                self.printer.display(json_table, elapsed)
                click.echo(f"EXPLAIN completed in {elapsed:.2f} ms")
            else:
                self.printer.display(result, elapsed)
            self._emit_profile()
        except (
            ValueError,
            RuntimeError,
            BindingError,
            duckdb.Error,
            sqlglot_errors.ParseError,
            StarExpansionError,
        ) as exc:
            click.echo(f"error: {exc}")
        except Exception as exc:
            click.echo(f"unexpected error: {exc}")

    def _emit_profile(self) -> None:
        """Print the per-query profiling breakdown when FEDQ_PROFILE is set."""
        report = self.runtime.query_executor.last_profile_report
        if report:
            click.echo(report)

    def _clean_statement(self, statement: str) -> str:
        """Strip surrounding whitespace and the trailing semicolon from a statement."""
        clean = statement.strip()
        if clean.endswith(";"):
            clean = clean[:-1].rstrip()
        return clean


def _prepare_runtime(
    config_path: Optional[str],
) -> Tuple[FedQRuntime, Catalog, str]:
    """Load config, build the catalog and runtime, and return them with any note."""
    config, message = _load_config_bundle(config_path)
    catalog = _build_catalog(config, message is not None)
    runtime = FedQRuntime(catalog, config)
    note = ""
    if message:
        note = message
    return runtime, catalog, note


def _load_config_bundle(config_path: Optional[str]) -> Tuple[Config, Optional[str]]:
    """Load config from the given path, or fall back to the in-memory demo config."""
    if config_path:
        config = load_config(config_path)
        return config, None
    config = _build_default_config()
    note = "Using in-memory DuckDB data source with demo tables."
    return config, note


def _build_default_config() -> Config:
    """Build a config with a single in-memory DuckDB data source for the demo."""
    # Empty root config for the demo; sub-configs use their defaults and the
    # single datasource below is attached after construction.
    config = Config.create(
        datasources={},
        optimizer=OptimizerConfig.create(),
        executor=ExecutorConfig.create(),
        cost=CostConfig.create(),
    )
    # The lone in-memory DuckDB source that backs the demo tables, declaring
    # only the capabilities the demo exercises.
    ds_config = DataSourceConfig.create(
        name="duckdb_mem",
        type="duckdb",
        config={"path": ":memory:", "read_only": False},
        capabilities=["aggregations", "joins"],
    )
    config.datasources[ds_config.name] = ds_config
    return config


def _build_catalog(config: Config, seed_demo: bool) -> Catalog:
    """Connect each configured data source, optionally seed demo data, and load metadata."""
    catalog = Catalog()
    for ds_config in config.datasources.values():
        datasource = _create_datasource(ds_config)
        datasource.connect()
        if seed_demo:
            _seed_demo_data(datasource)
        catalog.register_datasource(datasource)
    catalog.load_metadata()
    return catalog


def _create_datasource(ds_config: DataSourceConfig):
    """Instantiate the data source connector matching the config type."""
    if ds_config.type == "duckdb":
        return DuckDBDataSource(ds_config.name, ds_config.config)
    if ds_config.type == "postgresql":
        return PostgreSQLDataSource(ds_config.name, ds_config.config)
    if ds_config.type == "clickhouse":
        return ClickHouseDataSource(ds_config.name, ds_config.config)
    raise ValueError(f"Unsupported data source type: {ds_config.type}")


def _seed_demo_data(datasource: DuckDBDataSource) -> None:
    """Create and populate the demo_users table on the data source's connection."""
    connection = datasource.connection
    if connection is None:
        return
    _create_demo_users(connection)
    _insert_demo_users(connection)


def _create_demo_users(connection) -> None:
    """Create the demo_users table if it does not already exist."""
    sql = """
        CREATE TABLE IF NOT EXISTS demo_users (
            id INTEGER,
            name VARCHAR,
            age INTEGER,
            city VARCHAR
        )
    """
    connection.execute(sql)


def _insert_demo_users(connection) -> None:
    """Replace the demo_users rows with the fixed set of demo records."""
    sql = """
        INSERT INTO demo_users VALUES
        (1, 'Alice', 30, 'New York'),
        (2, 'Bob', 34, 'Boston'),
        (3, 'Carlos', 28, 'Austin'),
        (4, 'Diana', 41, 'Chicago'),
        (5, 'Eve', 25, 'Seattle')
    """
    connection.execute("DELETE FROM demo_users")
    connection.execute(sql)


@click.command()
@click.option(
    "-c",
    "--config",
    "config_path",
    type=click.Path(exists=True, dir_okay=False, readable=True),
    help="Path to YAML config file. Defaults to an in-memory DuckDB demo.",
)
def cli(config_path: Optional[str]) -> None:
    """Entry point for the fedq CLI."""
    runtime, catalog, note = _prepare_runtime(config_path)
    printer = ResultPrinter(click.echo)
    catalog_printer = CatalogPrinter(click.echo)
    if note:
        click.echo(note)
    click.echo("Type SQL statements terminated by ';'. Use \\q to exit.")
    click.echo("Use .catalog to view available tables.")
    repl = FedQRepl(runtime, printer, catalog_printer, catalog)
    repl.run()
