"""Simple interactive CLI for the federated query engine."""

from __future__ import annotations

import json
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
from ..config import Config, DataSourceConfig, ExecutorConfig, load_config
from ..datasources.duckdb import DuckDBDataSource
from ..datasources.postgresql import PostgreSQLDataSource
from ..executor import Executor
from ..parser import Binder, Parser, BindingError
from ..optimizer import (
    PhysicalPlanner,
    RuleBasedOptimizer,
    PredicatePushdownRule,
    ProjectionPushdownRule,
    AggregatePushdownRule,
    OrderByPushdownRule,
    LimitPushdownRule,
    ExpressionSimplificationRule,
)
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
    if hasattr(query_value, "sql"):
        return query_value.sql(dialect="postgres")
    if query_value is None:
        return "NULL"
    return str(query_value)


class FedQRuntime:
    """Wraps the parse → bind → optimize → plan → execute pipeline."""

    def __init__(self, catalog: Catalog, executor_config: ExecutorConfig):
        self.catalog = catalog
        self.parser = Parser()
        self.binder = Binder(catalog)
        self.optimizer = RuleBasedOptimizer(catalog)
        self._register_optimization_rules()
        self.planner = PhysicalPlanner(catalog)
        physical_executor = Executor(executor_config)
        processors = [
            StarExpansionProcessor(catalog, dialect=self.parser.dialect)
        ]
        self.query_executor = QueryExecutor(
            catalog=catalog,
            parser=self.parser,
            binder=self.binder,
            optimizer=self.optimizer,
            planner=self.planner,
            physical_executor=physical_executor,
            processors=processors,
        )

    def _register_optimization_rules(self) -> None:
        """Register optimization rules in the correct order."""
        self.optimizer.add_rule(ExpressionSimplificationRule())
        self.optimizer.add_rule(PredicatePushdownRule())
        self.optimizer.add_rule(ProjectionPushdownRule())
        self.optimizer.add_rule(AggregatePushdownRule())
        self.optimizer.add_rule(OrderByPushdownRule())
        self.optimizer.add_rule(LimitPushdownRule())

    def execute(self, sql: str) -> Union[pa.Table, Dict[str, Any]]:
        """Run a SQL statement and return results."""
        return self.query_executor.execute(sql)


class ResultPrinter:
    """Formats Arrow tables for CLI display."""

    def __init__(self, emit):
        self.emit = emit

    def display(self, table: pa.Table, elapsed_ms: float) -> None:
        rows = self._build_rows(table)
        headers = list(table.schema.names)
        lines = self._format_table(headers, rows)
        for line in lines:
            self.emit(line)
        summary = f"{table.num_rows} rows in {elapsed_ms:.2f} ms"
        self.emit(summary)

    def _build_rows(self, table: pa.Table) -> List[List[object]]:
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
        columns: List[List[object]] = []
        column_index = 0
        while column_index < table.num_columns:
            column = table.column(column_index).to_pylist()
            columns.append(column)
            column_index += 1
        return columns

    def _format_table(self, headers: List[str], rows: List[List[object]]) -> List[str]:
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

    def _compute_widths(self, headers: List[str], rows: List[List[object]]) -> List[int]:
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
        parts: List[str] = []
        parts.append("+")
        index = 0
        while index < len(widths):
            parts.append("-" * (widths[index] + 2))
            parts.append("+")
            index += 1
        return "".join(parts)

    def _format_row(self, values: List[str], widths: List[int]) -> str:
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
        string_values: List[str] = []
        for value in row:
            string_values.append(self._stringify_cell(value))
        return string_values

    def _stringify_cell(self, value: object) -> str:
        if value is None:
            return "NULL"
        return str(value)


class CatalogPrinter:
    """Prints catalog metadata in a readable format."""

    def __init__(self, emit):
        self.emit = emit

    def display_catalog(self, catalog: Catalog) -> None:
        if not catalog.schemas:
            self.emit("Catalog is empty.")
            return
        self.emit("\nCatalog Contents:")
        self.emit("=" * 80)
        self._print_schemas(catalog)

    def _print_schemas(self, catalog: Catalog) -> None:
        for key in sorted(catalog.schemas.keys()):
            datasource, schema_name = key
            schema = catalog.schemas[key]
            self._print_schema_header(datasource, schema_name)
            self._print_tables(datasource, schema_name, schema)
            self.emit("")

    def _print_schema_header(self, datasource: str, schema_name: str) -> None:
        header = f"\nData Source: {datasource}, Schema: {schema_name}"
        self.emit(header)
        self.emit("-" * len(header))

    def _print_tables(self, datasource: str, schema_name: str, schema) -> None:
        for table_name in sorted(schema.tables.keys()):
            table = schema.tables[table_name]
            full_name = f"{datasource}.{schema_name}.{table_name}"
            self.emit(f"\nTable: {full_name}")
            self._print_columns(table)

    def _print_columns(self, table) -> None:
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
        if buffer:
            return "...> "
        return "fedq> "

    def _is_exit_command(self, line: str) -> bool:
        trimmed = line.strip().lower()
        exit_commands = ["\\q", "quit", "exit"]
        for command in exit_commands:
            if trimmed == command:
                return True
        return False

    def _is_shortcut_command(self, line: str) -> bool:
        trimmed = line.strip()
        return trimmed.startswith(".")

    def _execute_shortcut(self, line: str) -> None:
        trimmed = line.strip().lower()
        if trimmed == ".catalog":
            self.catalog_printer.display_catalog(self.catalog)
        else:
            click.echo(f"Unknown shortcut: {line.strip()}")
            click.echo("Available shortcuts: .catalog")

    def _is_complete_statement(self, line: str) -> bool:
        return line.strip().endswith(";")

    def _build_statement(self, buffer: List[str]) -> str:
        parts: List[str] = []
        for chunk in buffer:
            parts.append(chunk)
        statement = "\n".join(parts)
        return statement

    def _execute_query(self, statement: str) -> None:
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

    def _clean_statement(self, statement: str) -> str:
        clean = statement.strip()
        if clean.endswith(";"):
            clean = clean[:-1].rstrip()
        return clean


def _prepare_runtime(
    config_path: Optional[str],
) -> Tuple[FedQRuntime, Catalog, str]:
    config, message = _load_config_bundle(config_path)
    catalog = _build_catalog(config, message is not None)
    runtime = FedQRuntime(catalog, config.executor)
    note = ""
    if message:
        note = message
    return runtime, catalog, note


def _load_config_bundle(config_path: Optional[str]) -> Tuple[Config, Optional[str]]:
    if config_path:
        config = load_config(config_path)
        return config, None
    config = _build_default_config()
    note = "Using in-memory DuckDB data source with demo tables."
    return config, note


def _build_default_config() -> Config:
    config = Config()
    ds_config = DataSourceConfig(
        name="duckdb_mem",
        type="duckdb",
        config={"path": ":memory:", "read_only": False},
        capabilities=["aggregations", "joins"],
    )
    config.datasources[ds_config.name] = ds_config
    return config


def _build_catalog(config: Config, seed_demo: bool) -> Catalog:
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
    if ds_config.type == "duckdb":
        return DuckDBDataSource(ds_config.name, ds_config.config)
    if ds_config.type == "postgresql":
        return PostgreSQLDataSource(ds_config.name, ds_config.config)
    raise ValueError(f"Unsupported data source type: {ds_config.type}")


def _seed_demo_data(datasource: DuckDBDataSource) -> None:
    connection = datasource.connection
    if connection is None:
        return
    _create_demo_users(connection)
    _insert_demo_users(connection)


def _create_demo_users(connection) -> None:
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
