# HANDOFF - Python to Rust rewrite

This document is the working state of the Python->Rust rewrite of the engine.
The previous handoff (the Python engine's development history) is preserved in
`oldhandoff.md`.

## What this is

We are rewriting the entire engine (30,183 lines of Python under
`federated_query/`) into Rust, as a single Cargo workspace under
`federated-query/`. The governing technical plan is
`rewrite-python-to-rust-plan.md`.

Strategy (decided with the user):

- BIG-BANG, not incremental. The Rust engine is built as a parallel
  implementation. The Python engine stays the working product and the
  behavioral reference, untouched, until the Rust engine passes the full gate
  (all translated tests + the SQL-level corpus + the TPC-DS/TPC-H tallies vs
  pure-DuckDB truth), after which the Python package is deleted.
- TEST-FIRST per crate: each crate's Python test corpus is translated into Rust
  tests before/with the implementation. Every retirement (a Python-mechanics
  test the type system now owns - pydantic field preservation, `.create`
  discipline, no-dataclass sweeps) is stated explicitly in the crate's port
  notes, never silent.
- CLEAN RUST over Python parity. We do NOT need byte-compatibility with the
  Python engine, nor to read its on-disk artifacts (the learned-stats SQLite can
  be rebuilt). When faithful-to-Python and clean-Rust conflict, pick clean Rust;
  port BEHAVIOR, express it the natural Rust way.
- Python is FROZEN during the rewrite (stable reference; no new features).

## Branch, build, gates

- Branch: `rewrite-python-to-rust` (in this repo; `fedqrs` has a matching
  branch but has not been merged in yet).
- Toolchain: cargo 1.96.1 at `$HOME/.cargo/bin` (HOME=/tmp). clippy + rustfmt
  installed.
- Build env: `export CARGO_TARGET_DIR=/workspace/federated-query/target`. Deps
  come from crates.io.
- Before every commit run: `cargo fmt --all && cargo clippy --workspace
  --all-targets && cargo test --workspace`. A `cargo fmt` pre-commit hook is
  configured in `.claude/settings.json` (PreToolUse/Bash, gated on the command
  containing "git commit").
- Lint policy: `warnings = "deny"` ESCALATES clippy pedantic to errors. A
  curated allow-list lives in `Cargo.toml [workspace.lints.clippy]` (each entry
  justified): struct_excessive_bools, missing_errors_doc, doc_markdown,
  must_use_candidate, similar_names, missing_panics_doc, cast_precision_loss,
  cast_possible_truncation, implicit_hasher (per-fn), too_many_lines (per-fn).
  `clippy.toml` sets too-many-arguments-threshold=10.
- Test env: Postgres 17.4 is running on :5432 (binaries at
  `federated-query/postgres-17`, trust auth; started via
  `scripts/run-postgres.sh`). DuckDB fixtures live under `benchmarks/*/data/`.

## Crate DAG (target)

```
fq-parse -> fq-plan -> fq-common
fq-bind -> fq-plan, fq-catalog
fq-decorrelate -> fq-plan
fq-optimize -> fq-plan, fq-catalog, fq-connectors
fq-physical -> fq-plan, fq-optimize, fq-emit
fq-emit -> fq-plan
fq-exec -> fq-plan, fq-connectors
fq-runtime -> everything above
fedq, fedq-py -> fq-runtime
```

Key layering decision made during the port: the `DataSource` trait is the
CATALOG/STATISTICS tier only (metadata, statistics, capabilities,
map_native_type, render_dialect - no Arrow, no driver deps) and it lives in
fq-catalog; the concrete connectors implement it in fq-connectors. This keeps
fq-catalog (and thus the binder) free of the duckdb/postgres driver deps. The
DATA-PLANE fetch tier (Arrow streaming, ship, ctid-parallel = today's
pyo3-coupled `fedqrs` connectors.rs) is a separate concern that moves in with
fq-exec, de-pyo3-ified.

## Status: 108 tests green, clippy pedantic (deny) + rustfmt clean

Done crates (in build order):

### fq-common (12 tests) - COMPLETE
`config.rs` (load_config + Config/DataSourceConfig/OptimizerConfig/
ExecutorConfig/CostConfig; serde `deny_unknown_fields` + `default` replaces the
pydantic StateModel loudness), `error.rs` (ConfigError + UnsupportedSqlError),
`logging.rs` (tracing), `types.rs` (`DataType` enum + `is_renderable` - put here
so fq-catalog needn't depend on fq-plan). The `DataType -> Arrow` object mapping
(`arrow_type_for`) is deferred to fq-exec where the arrow crate lives.

### fq-catalog (25 tests) - COMPLETE (except stats-catalog cross-crate consumers)
- `schema.rs`: owned Schema/Table/Column tree (NO Arc/Weak - the Python parent
  back-references were engine-dead, only test-used, so retired; the FQN
  qualifier is denormalized onto children).
- `catalog.rs`: registry + `load_metadata` + `require_renderable` +
  `resolve_table_columns(ds?, schema?, table)` (used by star expansion).
- `datasource.rs`: the `DataSource` trait + value types (ColumnMetadata/
  TableMetadata/ColumnStatistics/TableStatistics), DataSourceCapability,
  RenderDialect, StatValue, the default `map_native_type` + shared builders.
- `stats_catalog.rs`: `StatsCatalog` on rusqlite (bundled). Direct surface
  ported; its cross-crate consumer tests (`_persist_observations`, the
  CostModel/StatisticsCollector overlay) translate with fq-physical/fq-optimize.
- `error.rs`: `CatalogError`.

### fq-plan (20 tests) - COMPLETE (except EXPLAIN doc + arrow_type_for, deferred)
- `expr.rs`: `Expr` enum + the shared walkers (`children`, `map_children`,
  `column_refs`, `split_conjuncts/disjuncts`, `combine_and/or`,
  `contains_aggregate`, `split_where_having`, `aggregate_output_map`, `get_type`)
  - every walker an exhaustive `match` with no `_` arm (the compiler replaces the
  Python walker-exhaustiveness tests). `LiteralValue` enum replaces `Any`;
  `NullsOrder` replaces the NULLS FIRST/LAST strings. The visitor ABC retires.
- `logical.rs`: `LogicalPlan` enum, struct-per-node, `children()`/`schema()`
  (Projection `*`-expansion, SEMI/ANTI left-only). `Scan` boxed inside its
  variant.
- `physical.rs`: `PhysicalPlan` enum over 25 node structs + `children()`.
  Runtime/emit artifacts made clean (datasource_connection dropped, query_ast/
  lateral_sql held as String, seeded_schema as name+DataType pairs, private
  caches + group_observation + dynamic_filter_values deferred).
- Deferred (tasks, not written blind): `arrow_type_for` -> fq-exec; the EXPLAIN
  document builder (build_document/_PlanFormatter) -> fq-runtime/fq-emit (needs
  estimated_cost, expression rendering, a physical-plan producer, e2e tests).

### fq-connectors (15 tests) - DuckDB + Postgres done
- `duckdb_source.rs`: DuckDbSource against a real embedded DuckDB (metadata via
  information_schema, row count via duckdb_tables(), one approx aggregate scan
  for per-column NDV/nulls/min/max). Connection behind a Mutex (Connection is
  Send not Sync; the trait is Send+Sync; reads are optimize-time).
- `postgres_source.rs`: PostgresSource via the sync `postgres` crate.
  information_schema metadata + the uuid->VARCHAR map_native_type override;
  reltuples row count + pg_stats decode (signed n_distinct, histogram min/max);
  the statless PROBE (exact aggregate scan <=256MB, else TABLESAMPLE);
  estimate_scan_rows via EXPLAIN (FORMAT JSON), ANALYZEd-only. Tests use a
  schema-per-test (dropped on Drop) and early-return if pg is unreachable.
- Remaining (deferred, lower priority): parquet + clickhouse metadata surfaces;
  the data-plane Arrow fetch (moves in with fq-exec).

### fq-parse (31 tests) - SUBSTANTIALLY COMPLETE
On polyglot-sql 0.5.15 (its `Expression` is a ~1000-variant enum with a
dedicated variant per known function, so conversion is an ALLOWLIST: supported
constructs convert, every other variant raises `ParseError::Unsupported`).
Structured as a `Converter` struct (`convert.rs`) holding the catalog + an
in-scope CTE registry (RefCell); `expr.rs` and `functions.rs` are `impl
Converter` methods. Recursive descent: query -> plan, expr -> Expr, a
subquery-bearing expr recurses back into query.

Handled: single/multi-table SELECT with the full clause pipeline (FROM -> WHERE
-> GROUP BY -> HAVING -> SELECT -> ORDER BY -> LIMIT/OFFSET), left-deep JOINs
(JoinKind -> JoinType + natural flag), catalog-driven star expansion (`*` /
`alias.*`, no star survives), derived tables (-> SubqueryScan), CTE references
(-> CteRef), non-recursive WITH (-> Cte), DISTINCT / DISTINCT ON, binary set
operations (UNION/INTERSECT/EXCEPT), VALUES; expression nodes: columns,
literals, comparison/logical/arithmetic/concat binary ops, NOT/negate, IS
NULL/IS NOT NULL, parens, Cast, CASE (simple form lowered to searched),
BETWEEN/NOT BETWEEN, IN list/subquery, EXISTS, scalar subquery, op ANY/ALL, the
five aggregates, and scalar functions (a typed set + the generic
`Expression::Function{name,args}` node, so any function round-trips by name).

Still raises (minor tail, always loud): WITH RECURSIVE, comma joins, positioned
TRIM, window functions (Select.windows / QUALIFY), APPLY/ASOF joins, non-numeric
non-string literals (Date/Time/etc), BETWEEN SYMMETRIC, some IS shapes.

Entry points: `fq_parse::parse_with_catalog(sql, &Catalog)` (real, expands
stars) and `fq_parse::parse(sql)` (empty catalog convenience; a star raises).

## NEXT: fq-bind = MILESTONE A (SQL -> bound plan)

Port `parser/binder.py` (1633 lines). The scope chain (a stack of alias->Table
scopes plus in-scope CTEs); ONE column resolver (`resolve_in_scopes`: qualified
-> nearest scope defining the table; bare -> nearest defining the column;
intra-scope ambiguity = error; no-scope resolution = BindError). This is where
the "an invalid query MUST raise" rule is enforced (a bogus qualifier or typo'd
column raises before any source is touched). The `RawColumn -> BoundColumn` type
flip (post-bind every ColumnRef carries its relation qualifier and DataType -
enforce it). Scan column pruning to real catalog columns; star read-set
expansion; UNION arity checks; HAVING/ORDER-BY-over-aggregate resolution; the
aggregate-output hoists.

Binder unit tests can use HAND-BUILT catalogs (`Catalog::insert_schema` with
Schema/Table/Column) - no live connector needed. Test corpus: test_binder.py
(615), test_binder_subqueries.py (176), plus the binding-error e2e guard cases.

MILESTONE A gate: every corpus query binds; every invalid-query fixture raises
the right error.

After fq-bind: fq-decorrelate, fq-optimize, fq-emit, fq-physical, fq-exec (IR
deletion + move the `fedqrs` engine in), fq-runtime + fedq + fedq-py, then the
DELETE of `federated_query/` at the final gate.

## Commit log (rewrite so far)

```
0a589fd fq-parse scalar functions (typed + generic Function node)
9a8cbf4 fq-parse CTEs (WITH -> Cte + CteRef)
00f613a fq-parse expression nodes, subqueries, set ops, derived tables
7272031 fq-parse star expansion (catalog-driven)
5cbefe0 fq-parse joins (left-deep fold, per-table column partition)
529e4f5 fq-parse structural core (single-table SELECT pipeline)
d38f862 Postgres connector (metadata, pg_stats, probe, EXPLAIN estimate)
9b4a98e fq-connectors + DuckDB connector (catalog/statistics tier)
b9b5bec fq-catalog DataSource trait + load_metadata (connector abstraction)
30a66c1 fq-plan stages B+C (physical enum, is_renderable, where/having split)
77e6847 scaffold workspace + fq-common, fq-catalog, fq-plan (stage A)
```
