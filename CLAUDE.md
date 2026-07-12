# Federated Query Engine - Claude Developer Guide

This document provides an overview of the codebase structure, key classes, compilation instructions, and coding standards for AI-assisted development.

## TOP PRIORITY : no silent crashes

# A crash never ships a lie.

Never fail silently. Never fail silently. This is the most important thing we care in this project. Do whatever the fuck you can do to stop silent fails. 

The code should be readable, self explained.Most of all the code should be defensive. We appreciate crashes more than silent fails.

## TOP PRIORITY : an invalid query MUST raise. Answering one is an EPIC FAIL.

Returning rows for an invalid query is an EPIC FAIL. It is WORSE than a silent
fail: a silent fail hides a problem, but answering an invalid query manufactures
a wrong answer and presents it as correct. A crash never ships a lie; answering
an invalid query ships a lie. There is no "the result happened to be right" -
the query was invalid, so any result is wrong. Never describe this as
acceptable, normal, or a minor issue. It is the most severe failure mode in the
engine.

# No ascii

Only ascii edits are allowed.

# RUST REWRITE GUARDRAILS (process rules - each one exists because it was violated once)

1. PLANNING IS O(METADATA). Planning may read catalogs, cached/learned statistics,
   and source planner estimates (EXPLAIN) - NEVER data. This is enforced at
   runtime: planning has a hard wall-clock budget (`optimizer.planning_budget_ms`,
   default 100ms). A blown budget KILLS the query with a per-stage report
   (`fq-runtime` StageLog) or, when a statistics fetch broke it, names the exact
   fetch (`EstimateError::PlanBudget`). There is NO off switch; a justified edge
   case raises the configured budget explicitly and says why. Never "fix" a
   budget kill by raising the budget - fix the O(data) work it caught.

2. NEVER COMPILE DUCKDB IN CARGO. The `duckdb` crate's `bundled` feature compiles
   the C++ amalgamation at the active cargo profile's opt level: a dev build is an
   -O0 DuckDB (~10x slower execution) and a release build costs ~10min of C++ per
   clean build. The workspace links the OFFICIAL prebuilt library:
   `scripts/setup-duckdb-lib.sh` (or `make duckdb-lib`) fetches the version pinned
   by Cargo.lock into `.duckdb-lib/current`, `.cargo/config.toml` wires
   DUCKDB_LIB_DIR + rpath. fq-lint rule FQ-BUNDLED fails on any reintroduction.

3. PERF NUMBERS ONLY FROM benchmarks/perf_compare. No ad-hoc timing runs: every
   engine perf claim comes from `benchmarks/perf_compare/compare.py` (cold = fresh
   process + fresh runtime, warm = medians on a live runtime, both ALWAYS
   reported, release builds only, budget kills shown as KILLED, unsupported
   sources shown as UNSUPPORTED). If a number was not produced by the harness it
   does not go in a report, commit message, or decision.

4. NO STALE DEFERRALS. Deferring work is fine ONLY as: a comment/doc note that
   names WHAT is deferred and the BLOCKER it waits on (a crate, a feature, a
   measurement). When the blocker lands, the deferral MUST be swept in that same
   milestone - re-read every deferral note whose blocker was just built and
   either do the work or re-justify against the new state. A deferral whose
   stated blocker already exists is a lie in the codebase (this happened:
   `scan_planner_estimate` kept "fq-emit does not exist yet" for four milestones
   after fq-emit landed, hiding an O(data) planning path). At every milestone
   close, grep the workspace for `deferred`/`TODO(` and re-adjudicate.

5. ONE RUNNER PER BENCHMARK SUITE. Each benchmark directory has exactly ONE
   runner script (tpcds: run_federated_rust.py; tpch: run_federated_rust.py;
   cross-engine: perf_compare/compare.py). NEVER create a new benchmark/timing
   script - extend the existing runner. Oracle/reference/baseline work is
   measured ONCE per dataset by the suite's save-refs path and READ from the
   references file at run time (oracle_timings table) - a runner that
   re-measures a function of the data on every invocation is a bug (this
   happened: a runner re-ran the DuckDB baseline 1+warm_runs times per query
   per invocation; at SF10 that was minutes of re-measuring a constant).
   Before running, a runner prints how many engine executions the invocation
   will perform; expected wall times live in the suite README.

Never fail silently. If something breaks it should throw an error. We don't want silent fails. You can only catch exceptions when we show it to the user in the cli. Otherwise all exceptions should be thrown. 

ALL COLUMNS MUST BE QUALIFIED AFTER THE BINDER. Non-negotiable rule: once binding is done, every column reference (`ColumnRef`) flowing through the logical and physical plan MUST carry its relation qualifier (`table` set to the owning relation/alias). An unqualified column ref (`table` is None or empty) must NEVER pass through the plan after binding. This is not only the binder's job: any pass that MANUFACTURES columns (decorrelation's synthetic subquery outputs, optimizer-introduced projections, etc.) must qualify them to a real relation, exposing that relation under an alias if one does not already exist. Enforce it with a loud guard that walks the plan after binding/decorrelation and raises on any unqualified ref. Rationale: side-assignment, join-key orientation, and scope resolution all rely on the qualifier; an unqualified column silently defeats them and manufactures wrong or empty results.

NO SHORTCUT COLUMN LISTS. Relational operators must carry explicit, real column names - never a star placeholder standing in for "all columns". In particular `Scan.columns` MUST be a concrete list of the table's actual column names; it must never be `["*"]` or contain `"*"`. The binder expands any star read-set into the real column list from catalog metadata. Guard it: a `*` in a bound scan's columns must raise. A star is a shortcut that hides the real schema from every pass that reasons about columns.

NO DATACLASSES; state types are Pydantic models. Final, non-negotiable design decision: no Python dataclasses (`@dataclass`) anywhere. Dataclasses silently drop fields on reconstruction (a forgotten field takes its default = a wrong answer with no error). Every state-carrying type (plan nodes, expression nodes, catalog/config/state objects) derives from `federated_query.model.StateModel` (a `pydantic.BaseModel` with `arbitrary_types_allowed`; construction is keyword-only). `StateModel` is built to keep mistakes LOUD: `extra="forbid"` (an unknown/renamed construction kwarg raises, never silently dropped) and a `model_copy` override that rejects unknown `update` keys (plain Pydantic `model_copy` would set a junk attribute silently). The migration is COMPLETE; `tests/test_no_dataclasses.py` enforces it; `tests/test_state_model.py` pins the loudness. Rules:
- Never write a `@dataclass` or import `dataclasses`. A `@dataclass` is a bug to migrate, not preserve. New state types subclass `StateModel`.
- Fields are declared as Pydantic annotations; a field with no default is REQUIRED and Pydantic raises on omission. Construct with kwargs: `Scan(datasource=..., schema_name=..., ...)`.
- To copy-with-change (any transformation), use `node.model_copy(update={"filters": ...})`, which copies EVERY field and overrides only what you name, so a field can never be dropped. NEVER reconstruct a node by re-listing its fields into a raw constructor.
- Equality/copy/introspection come from Pydantic: structural `==` over all fields (this is the change-detection contract - do not hand-write field-by-field comparison), `model_copy`, `model_fields`. Hold non-Pydantic values (sqlglot, etc.) via `model_config = ConfigDict(arbitrary_types_allowed=True)` on a base.
- Nodes keep their `children()`/`with_children()`/`accept()`/`schema()` methods; `with_children` is `self.model_copy(update={...})`.
The test `tests/test_node_field_preservation.py` pins that `with_children` preserves every field for each node type.

Mutable by default; do not reach for immutability. Use plain mutable classes. Do NOT make a type immutable - no `frozen=True`, no `__setattr__` guards, no read-only/value-object wrappers - unless there is a STRONG, concrete reason. Immutability is not a goal in itself and does not by itself guarantee correctness; never introduce it speculatively, and never change a design to be immutable without a real, stated reason. If a specific type genuinely needs to be immutable, the reason MUST be written down here as a listed exception (which type, and why). Immutability exceptions (currently none): _none_.

No legacy / compatibility cruft. This is a system we are building from scratch - there is no legacy code, no old callers, no external API to stay compatible with. Never add "store for compatibility" fields, shim attributes, dead aliases, or back-compat branches. If something is no longer needed, delete it; if an abstraction doesn't fit (e.g. a base attribute that a subclass can't honor), fix the abstraction and its callers, don't paper over it. A comment like "kept for compatibility" is a red flag to remove, not preserve.

Follow Python PEP8

use meaningful names

every function/method needs a comment with explaining what it does.

COMMENTS DESCRIBE THE CODE, NEVER THE PROJECT. Code is not a todo list and not
a changelog. A comment states what the code does, its invariants, and why - as
it is TODAY. Forbidden in code comments: TODO/FIXME/XXX/HACK markers,
milestone/phase/stage labels, commit ids, dates, task/ticket references, punch
lists, review notes, "deferred to X" scheduling, "landed/implemented in"
history, references to spec/plan documents. Work tracking lives in HANDOFF.md
and the plan docs; history lives in git. If behavior is intentionally missing,
state the current behavior as a fact ("X is not supported; this raises Y"),
never as a plan. Enforced twice: fq-lint FQ-PMCOMMENT catches the mechanical
markers instantly (Rust), and the SEMANTIC pre-commit gate (scripts/
comment-gate: a haiku judge reviews every staged .rs/.py comment block and
Python docstring against RUBRIC.md) catches what keyword lists cannot. The
hook installs itself on session start (SessionStart hook -> scripts/
install-hooks.sh; manual: `make hook-install`); a gate failure blocks the
commit and prints each offending comment.

This project is not a toy/homework project. Do real production quality software development. We need real-query-engine level behaviour.

Eg: the following task is correct.
```
# SELECT * Expansion Contract

- All star projections (`*` or `alias.*`) MUST be expanded into explicit column references **before** the logical plan reaches the parser/binder. No star survives into the engine.
- Expansion is performed by the preprocessor using catalog metadata and generates an **internal column name** (`internal_name`) alongside the user-visible name (`visible_name`). Example mapping:
  - SQL: `SELECT * FROM pg.users`
  - Internal projection: `pg.users.id` (`internal_name`) with `id` (`visible_name`)
  - Engine uses `internal_name` for planning/execution; results presented to the user show `visible_name`.
- Queries with user aliases (`SELECT foo AS alias_1 FROM pg.users`) map as:
  - `internal_name = pg.users.foo`
  - `visible_name = alias_1`
  - Execution works exclusively on the internal name; final result column is `alias_1`.
- Expansion fails fast (raises StarExpansionError) when a star cannot be resolved (missing table metadata, unsupported source, etc.).
```

This is wrong """All star projections (`*` or `alias.*`) MUST be expanded. If the table is not aliased we bail out and pass * to underlying engine. """ or """all star projections will only work for aliased tables and we raise an exception, we don't support subqueries""" these are unacceptable. When planning a feature we want real engine-level planning.

We also don't want wrapping exceptions with beatiful messages or whatever you think is better. So the next example is wrong

```python
try:
   something()
except Exception:
   raise Exception('some error happened')

```

What we want is this

```
   something()
```

Simply fail if there is an exception unless otherwise requested. 

We also don't want pointless fuck as follows
```py
   class Foo:
      def __init__(self, processors):
         # WRONG BECAUSE ITS POINTLESS FUCK
         self.processors: List[QueryProcessor] = []
         if processors:
            index = 0
            while index < len(processors):
               self.processors.append(processors[index])
               index += 1
```
This is pointless fuck because we can simply do this

```py
   class Foo:
      def __init__(self, processors:List[QueryProcessor]):
         # RIGHT
         self.processors: List[QueryProcessor] = processors
```

We also don't want `while index <.... ` type loops. Use for loops. Iterate over lists.

```py
      if isinstance(value, list):
         # WRONG BECAUSE ITS POINTLESS FUCK
         index = 0
         while index < len(value):
            entry = value[index]
            if isinstance(entry, exp.Expression):
               self._rewrite_expression(entry, context, False)
            index += 1
```

This is right 

```py
      if isinstance(value, list):
         # right, because value is a list, we can loop over it.
         for entry in value:
            if isinstance(entry, exp.Expression):
               self._rewrite_expression(entry, context, False)
```

Seeing any pointless fuck in the code will result in immediate fail in task and ending your contract, you'll be fired on the spot. So think carefully before you write any code. We don't tolerate bullshit.


## Repository Overview

This is a production-grade federated query engine that executes SQL queries across multiple heterogeneous data sources (PostgreSQL, DuckDB) with intelligent optimization.

## Archtiecture doc
read this file first: README-architecture.md


## Project Structure

```
federated-query/
  federated_query/          # Main package
    catalog/                # Metadata catalog
      catalog.py            # Catalog class - manages metadata across data sources
      schema.py             # Schema, Table, Column classes
    config/                 # Configuration management
      config.py             # Config classes and YAML loader
    datasources/            # Data source connectors
      base.py               # DataSource abstract base class
      postgresql.py         # PostgreSQLDataSource implementation
      duckdb.py             # DuckDBDataSource implementation
    executor/               # Query execution engine
      executor.py           # Executor class - executes physical plans
      operators.py          # Physical operators (joins, aggregations, etc.)
    optimizer/              # Query optimization
      rules.py              # RuleBasedOptimizer and optimization rules
      cost.py               # CostModel for query cost estimation
      decorrelation.py      # Subquery decorrelation transformer
      statistics.py         # Statistics collection and estimation
    parser/                 # SQL parsing and binding
      parser.py             # Parser class - SQL to logical plan
      binder.py             # Binder class - resolves references
    plan/                   # Plan representations
      logical.py            # Logical plan nodes (LogicalScan, LogicalJoin, etc.)
      physical.py           # Physical plan nodes (PhysicalScan, PhysicalHashJoin, etc.)
      expressions.py        # Expression nodes (Column, Literal, BinaryOp, etc.)
    utils/                  # Utilities
      logging.py            # Logging configuration
  config/                   # Configuration files
    example_config.yaml     # Example configuration
  tests/                    # Test suite
    test_parser.py          # Parser tests
    test_catalog.py         # Catalog tests
    test_config.py          # Configuration tests
    test_datasources.py     # Data source tests
  scripts/                  # Utility scripts
    init_duckdb.py          # Initialize test DuckDB database
  docker/                   # Docker configuration for test databases
```

## Key Classes and Their Locations

### Core Query Pipeline
- **Parser** (`federated_query/parser/parser.py`): Converts SQL to logical plan using sqlglot
- **Binder** (`federated_query/parser/binder.py`): Resolves table/column references against catalog
- **RuleBasedOptimizer** (`federated_query/optimizer/rules.py`): Applies optimization rules to logical plan
- **Executor** (`federated_query/executor/executor.py`): Executes physical plan and returns results

### Metadata Management
- **Catalog** (`federated_query/catalog/catalog.py`): Central metadata registry
- **Schema** (`federated_query/catalog/schema.py`): Schema metadata container
- **Table** (`federated_query/catalog/schema.py`): Table metadata with columns
- **Column** (`federated_query/catalog/schema.py`): Column metadata with type info

### Data Source Connectors
- **DataSource** (`federated_query/datasources/base.py`): Abstract base class for all connectors
- **PostgreSQLDataSource** (`federated_query/datasources/postgresql.py`): PostgreSQL connector with pooling
- **DuckDBDataSource** (`federated_query/datasources/duckdb.py`): DuckDB connector

### Plan Nodes
- **Logical Plans** (`federated_query/plan/logical.py`): LogicalScan, LogicalJoin, LogicalFilter, LogicalProject, LogicalAggregate
- **Physical Plans** (`federated_query/plan/physical.py`): PhysicalScan, PhysicalHashJoin, PhysicalNestedLoopJoin, PhysicalBroadcastJoin
- **Expressions** (`federated_query/plan/expressions.py`): Column, Literal, BinaryOp, UnaryOp, AggregateCall

### Optimization
- **CostModel** (`federated_query/optimizer/cost.py`): Estimates query execution cost
- **Decorrelator** (`federated_query/optimizer/decorrelation.py`): Transforms correlated subqueries
- **Statistics** (`federated_query/optimizer/statistics.py`): Collects and uses table statistics

## Installation and Setup

### Install Dependencies
```bash
pip install -r requirements.txt
pip install -e .
```

### Install Development Tools
```bash
pip install pytest pytest-cov black mypy
```

## Running Tests

### Run All Tests
```bash
pytest
```

### Run Tests with Coverage
```bash
pytest --cov=federated_query --cov-report=term-missing
```

### Run Specific Test File
```bash
pytest tests/test_parser.py -v
```

### Run Tests with PostgreSQL
The tests automatically use the PostgreSQL service when available:
```bash
# Using docker-compose
docker-compose up -d
pytest tests/test_datasources.py
```

## Code Formatting and Type Checking

### Format Code with Black
```bash
black federated_query tests
```

### Type Check with mypy
```bash
mypy federated_query
```

## Architecture Flow

```
SQL Query String
   |
   v
Parser.parse() -> sqlglot AST
   |
   v
Parser.ast_to_logical_plan() -> LogicalPlanNode
   |
   v
Binder.bind() -> Bound LogicalPlanNode (resolved references)
   |
   v
RuleBasedOptimizer.optimize() -> Optimized LogicalPlanNode
   |
   v
PhysicalPlanner.plan() -> PhysicalPlanNode (with cost estimation)
   |
   v
Executor.execute() -> Iterator[pa.RecordBatch] (Arrow format)
```

## Coding Standards and Rules

When working on this codebase, **strictly adhere** to the following rules:

### 1. Exception Handling

**NEVER catch bare exceptions.** We want to see exceptions at higher levels. Don't catch exceptions unnecessarily.

**WRONG:**
```python
def parse(self, sql: str) -> exp.Expression:
    try:
        parsed = sqlglot.parse_one(sql, dialect=self.dialect)
        return parsed
    except Exception as e:
        raise ValueError(f"Failed to parse SQL: {e}")
```

This is wrong because it adds complexity and hides the correct cause of the parse error.

**CORRECT:**
```python
def parse(self, sql: str) -> exp.Expression:
    parsed = sqlglot.parse_one(sql, dialect=self.dialect)
    return parsed
```

**Only catch specific exceptions when you have a legitimate reason to handle them differently.** Never catch bare exceptions unless explicitly told or there is no other way.

### 2. List Comprehensions

**NEVER use list comprehensions.** Always use explicit loops for clarity and debuggability.

**WRONG:**
```python
columns = [desc[0] for desc in cursor.description]
```

**CORRECT:**
```python
columns = []
for desc in cursor.description:
    columns.append(desc[0])
```

### 3. Code Readability

**This code is written for humans.** Use proper naming and self-explanatory lines.

- Use descriptive variable names
- Avoid single-letter variables except for common conventions (i, j for loops)
- Write code that reads like prose
- Prefer clarity over cleverness

### 4. Cyclomatic Complexity

**No function/method can be over cyclomatic complexity of 4.**

Cyclomatic complexity measures the number of independent paths through code:
- Each if/elif adds 1
- Each loop (for/while) adds 1
- Each logical operator (and/or) adds 1
- Each except clause adds 1

If complexity exceeds 4, break the function into smaller helper functions.

### 5. Function Length

**No function/method can be longer than 20 lines.**

This does not include:
- Docstrings
- Blank lines
- Comments

If a function exceeds 20 lines of actual code, refactor it into smaller functions.

### 6. PEP-8 Compliance

**Follow PEP-8 rules as much as possible.**

- Use 4 spaces for indentation
- Maximum line length: 88 characters (Black formatter default)
- Two blank lines between top-level functions/classes
- One blank line between methods
- Import order: standard library, third-party, local
- Use snake_case for functions and variables
- Use PascalCase for classes

## Development Workflow

1. **Before making changes:** Run tests to ensure baseline works
2. **Make changes:** Follow the coding standards above
3. **Format code:** Run `black` to format
4. **Run tests:** Ensure all tests pass
5. **Check types:** Run `mypy` if touching type-heavy code
6. **Commit:** Write clear, descriptive commit messages

## Current Development Status

This project is in **Phase 0** (completed) with basic infrastructure:
- Project structure and skeleton
- Core abstractions (plans, expressions, data sources)
- Configuration system
- Basic catalog structure
- Test infrastructure
- Data source connectors (PostgreSQL, DuckDB)

See `tasks.md` for the full roadmap.

## Testing Strategy

- **Unit tests:** Test individual classes in isolation
- **Integration tests:** Test interactions between components
- **Database tests:** Use real PostgreSQL and DuckDB instances
- **Coverage target:** Aim for >80% code coverage

## Resources

- **Architecture details:** See `plan.md`
- **Implementation roadmap:** See `tasks.md`
- **Example config:** See `config/example_config.yaml`
- **README:** See `README.md` for user-facing documentation

---

**Remember:** Code quality and maintainability are paramount. Always prioritize readable, debuggable, and testable code over clever or compact solutions.
