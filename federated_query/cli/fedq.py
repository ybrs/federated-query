"""Simple interactive CLI for the federated query engine."""

from __future__ import annotations

import time
from typing import List, Optional, Tuple

import click
import duckdb
import pyarrow as pa
from sqlglot import errors as sqlglot_errors

from ..catalog import Catalog
from ..config import Config, DataSourceConfig, ExecutorConfig, load_config
from ..datasources.duckdb import DuckDBDataSource
from ..datasources.postgresql import PostgreSQLDataSource
from ..executor import Executor
from ..parser import Binder, Parser, BindingError
from ..optimizer import PhysicalPlanner


class FedQRuntime:
    """Wraps the parse → bind → plan → execute pipeline."""

    def __init__(self, catalog: Catalog, executor_config: ExecutorConfig):
        self.parser = Parser()
        self.binder = Binder(catalog)
        self.planner = PhysicalPlanner(catalog)
        self.executor = Executor(executor_config)

    def execute(self, sql: str) -> pa.Table:
        """Run a SQL statement and return results as a table."""
        ast = self.parser.parse(sql)
        logical_plan = self.parser.ast_to_logical_plan(ast)
        bound_plan = self.binder.bind(logical_plan)
        physical_plan = self.planner.plan(bound_plan)
        return self.executor.execute_to_table(physical_plan)


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


class FedQRepl:
    """Interactive loop similar to psql."""

    def __init__(self, runtime: FedQRuntime, printer: ResultPrinter):
        self.runtime = runtime
        self.printer = printer

    def run(self) -> None:
        buffer: List[str] = []
        while True:
            line, status = self._read_line(buffer)
            if status == "exit":
                break
            if status == "skip":
                continue
            if line is None:
                continue
            trimmed = line.strip()
            if not trimmed:
                continue
            if self._should_exit(trimmed):
                break
            buffer.append(line)
            if self._statement_complete(trimmed):
                statement = self._merge_buffer(buffer)
                buffer.clear()
                self._execute_statement(statement)

    def _execute_statement(self, statement: str) -> None:
        clean = statement.strip()
        if clean.endswith(";"):
            clean = clean[:-1].rstrip()
        if not clean:
            return
        try:
            start = time.time()
            table = self.runtime.execute(clean)
            elapsed = (time.time() - start) * 1000
            self.printer.display(table, elapsed)
        except (
            ValueError,
            RuntimeError,
            BindingError,
            duckdb.Error,
            sqlglot_errors.ParseError,
        ) as exc:
            click.echo(f"error: {exc}")

    def _read_line(self, buffer: List[str]) -> Tuple[Optional[str], str]:
        prompt = self._prompt_for_buffer(buffer)
        try:
            line = input(prompt)
            return line, "ok"
        except EOFError:
            click.echo("")
            return None, "exit"
        except KeyboardInterrupt:
            click.echo("")
            buffer.clear()
            return None, "skip"

    def _prompt_for_buffer(self, buffer: List[str]) -> str:
        if buffer:
            return "...> "
        return "fedq> "

    def _should_exit(self, line: str) -> bool:
        commands = ["\\q", "quit", "exit"]
        for command in commands:
            if line.lower() == command or line == command:
                return True
        return False

    def _statement_complete(self, line: str) -> bool:
        return line.endswith(";")

    def _merge_buffer(self, buffer: List[str]) -> str:
        parts: List[str] = []
        for chunk in buffer:
            parts.append(chunk)
        statement = "\n".join(parts)
        return statement


def _prepare_runtime(config_path: Optional[str]) -> Tuple[FedQRuntime, str]:
    config, message = _load_config_bundle(config_path)
    catalog = _build_catalog(config, message is not None)
    runtime = FedQRuntime(catalog, config.executor)
    note = ""
    if message:
        note = message
    return runtime, note


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
    runtime, note = _prepare_runtime(config_path)
    printer = ResultPrinter(click.echo)
    if note:
        click.echo(note)
    click.echo("Type SQL statements terminated by ';'. Use \\q to exit.")
    repl = FedQRepl(runtime, printer)
    repl.run()
