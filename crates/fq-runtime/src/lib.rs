//! fq-runtime: the config-driven `execute(sql)` orchestration.
//!
//! `Runtime::from_config` wires every configured datasource into TWO tiers from
//! one config: the shared `fq_catalog::Catalog` (metadata + live statistics, via
//! the fq-connectors `DataSource`s) and the fq-exec DATA PLANE (the native
//! read/ship registry). `Runtime::execute` then drives the whole pipeline -
//! parse -> bind -> decorrelate -> optimize -> physical plan -> execute - and
//! renames the engine's internal result schema onto the query's user-visible
//! output column names.
//!
//! This crate is pyo3-FREE. The `fedq-py` crate wraps it behind the Python FFI.

pub mod error;
mod explain;

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef};
use serde_yaml::Value;

use fq_catalog::{Catalog, StatsCatalog};
use fq_common::{Config, DataSourceConfig, PlanBudget};
use fq_connectors::{DuckDbSource, PostgresSource};
use fq_exec::{connectors, execute_plan};
use fq_optimize::{build_optimizer, CostModel, StatisticsCollector};
use fq_parse::parse_with_catalog;
use fq_physical::steps::Observation;
use fq_physical::PhysicalPlanner;
use fq_plan::logical::LogicalPlan;
use fq_plan::output_column_names;
use fq_plan::physical::PhysicalPlan;

pub use error::RuntimeError;

/// One assembled engine session: a shared catalog (metadata + statistics) plus
/// the config that governs optimization and cost. The fq-exec data plane is a
/// process-wide registry the constructor populates as a side effect, so it is
/// not held here.
pub struct Runtime {
    catalog: Arc<Catalog>,
    config: Config,
    /// ONE statistics collector for the whole session: its caches (source
    /// stats, planner estimates) persist across queries, and its learned
    /// overlay reads the stats catalog this runtime also WRITES after every
    /// execution. A per-query collector would re-fetch every statistic on
    /// every plan.
    stats: Arc<StatisticsCollector>,
}

impl Runtime {
    /// Assemble a runtime from a config: build each datasource's catalog handle
    /// (loading its metadata) AND register it in the fq-exec data plane, then
    /// hold the shared catalog and the config for query time.
    ///
    /// DuckDB handles open READ-ONLY on both tiers so the catalog's stats handle
    /// and the exec read handle coexist on the same file for the whole session
    /// (DuckDB is single-writer per file per process).
    pub fn from_config(config: &Config) -> Result<Self, RuntimeError> {
        let mut catalog = Catalog::new();
        for datasource in config.datasources.values() {
            register_datasource(&mut catalog, datasource)?;
        }
        catalog.load_metadata()?;
        let catalog = Arc::new(catalog);
        let learned = open_stats_catalog(config)?;
        let stats = Arc::new(StatisticsCollector::new(
            Arc::clone(&catalog),
            learned,
            None,
        ));
        Ok(Self {
            catalog,
            config: config.clone(),
            stats,
        })
    }

    /// Run one SQL statement and return its Arrow result under the query's
    /// user-visible column names. A leading EXPLAIN short-circuits to a textual
    /// plan WITHOUT executing.
    pub fn execute(&self, sql: &str) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        if let Some(inner) = explain::strip_explain(sql) {
            return self.explain(inner);
        }
        let physical = self.plan(sql)?;
        let visible_names = output_column_names(&physical);
        let execution = execute_plan(&physical)?;
        // The learned-stats WRITE path: each measured (binding, rows) whose
        // binding carries provenance lands in the stats catalog, AFTER the
        // result is materialized (off the query's critical path). The next
        // query's plans read these instead of asking the source.
        self.persist_observations(&execution.measurements, &execution.observations)?;
        rename_result(&execution.schema, execution.batches, &visible_names)
    }

    /// Parse -> bind -> decorrelate -> optimize into an optimized logical plan,
    /// recording each stage's wall clock in `stages` (which kills the plan the
    /// moment the shared budget is blown).
    fn optimize(&self, sql: &str, stages: &mut StageLog) -> Result<LogicalPlan, RuntimeError> {
        let parsed = parse_with_catalog(sql, self.catalog.as_ref())?;
        stages.finish("parse")?;
        let bound = fq_bind::bind(self.catalog.as_ref(), parsed)?;
        stages.finish("bind")?;
        let decorrelated = fq_decorrelate::decorrelate(bound)?;
        stages.finish("decorrelate")?;
        let optimizer = build_optimizer(&self.config.optimizer, self.cost_model());
        let optimized = optimizer.optimize(decorrelated)?;
        stages.finish("optimize")?;
        Ok(optimized)
    }

    /// Lower SQL into an executable physical plan under the planning budget.
    ///
    /// Planning is O(metadata) BY DESIGN and budget-enforced: ONE clock covers
    /// every stage AND the statistics fetches inside them (the cost models carry
    /// the same `PlanBudget`), so a plan-time data scan or a slow remote probe is
    /// KILLED and reported, never silently paid per query.
    fn plan(&self, sql: &str) -> Result<PhysicalPlan, RuntimeError> {
        let mut stages = StageLog::start(self.config.optimizer.planning_budget_ms);
        // The SHARED collector carries this plan's budget so a statistics fetch
        // that overruns it is killed at the fetch and named in the error.
        self.stats.set_plan_budget(stages.budget);
        let optimized = self.optimize(sql, &mut stages)?;
        let cost_model = Rc::new(RefCell::new(self.cost_model()));
        let mut planner = PhysicalPlanner::new(Arc::clone(&self.catalog), Some(cost_model));
        let physical = planner.plan(&optimized)?;
        stages.finish("physical")?;
        Ok(physical)
    }

    /// Write each measured (binding, rows) whose binding carries provenance into
    /// the learned-stats catalog; a binding with no provenance (a merge fragment,
    /// an unmapped step) is skipped. A no-op without a stats catalog.
    fn persist_observations(
        &self,
        measurements: &[(String, usize)],
        observations: &std::collections::BTreeMap<String, Observation>,
    ) -> Result<(), RuntimeError> {
        let Some(catalog) = self.stats.stats_catalog() else {
            return Ok(());
        };
        for (binding, rows) in measurements {
            if let Some(observation) = observations.get(binding) {
                // A row count cannot exceed i64::MAX in practice; refuse to
                // wrap rather than record a negative measurement.
                let rows = i64::try_from(*rows).map_err(|_| {
                    RuntimeError::ResultShape(format!("row count {rows} overflows"))
                })?;
                persist_one(catalog, observation, rows)?;
            }
        }
        Ok(())
    }

    /// Plan the inner statement of an EXPLAIN and return its physical plan tree
    /// as a single `plan` text column, one row per line. Nothing executes.
    fn explain(&self, inner: &str) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let physical = self.plan(inner)?;
        let lines = explain::describe(&physical);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "plan",
            ArrowDataType::Utf8,
            false,
        )]));
        let column = Arc::new(StringArray::from(lines));
        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![column])
            .map_err(|error| RuntimeError::ResultShape(error.to_string()))?;
        Ok((schema, vec![batch]))
    }

    /// A cost model over the SESSION's shared statistics collector: cardinality
    /// and join-order decisions read cached/learned/EXPLAIN statistics
    /// (honest-unknown defaults fill any gap; a suboptimal plan still returns
    /// correct rows). The collector carries the active plan's budget, so a
    /// statistics fetch that overruns it is killed at the fetch.
    fn cost_model(&self) -> CostModel {
        CostModel::with_shared_stats(self.config.cost.clone(), Some(Arc::clone(&self.stats)))
    }
}

/// Open the learned-stats catalog that lives NEXT TO the config file
/// (`<config-stem>.stats.sqlite`, one catalog per configuration), or `None` for
/// a programmatically built config with no source path.
fn open_stats_catalog(config: &Config) -> Result<Option<Arc<StatsCatalog>>, RuntimeError> {
    let Some(source_path) = config.source_path.as_deref() else {
        return Ok(None);
    };
    let path = std::path::Path::new(source_path);
    let stem = path
        .file_stem()
        .map_or_else(|| "fedq".to_string(), |s| s.to_string_lossy().to_string());
    let stats_path = path
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join(stem + ".stats.sqlite");
    let catalog = StatsCatalog::open(&stats_path.to_string_lossy())?;
    Ok(Some(Arc::new(catalog)))
}

/// Dispatch one observation to its catalog table by provenance kind. The
/// predicate arm reads the catalog's freshest base count so the stored
/// selectivity is measurement-over-measurement; an unknown base stores the
/// output alone.
fn persist_one(
    catalog: &StatsCatalog,
    observation: &Observation,
    rows: i64,
) -> Result<(), RuntimeError> {
    match observation {
        Observation::TableRows {
            datasource,
            schema,
            table,
        } => catalog.record_table_rows(datasource, schema, table, rows)?,
        Observation::ColumnNdv {
            datasource,
            schema,
            table,
            column,
        } => catalog.record_column_ndv(datasource, schema, table, column, rows)?,
        Observation::Group { subject, columns } => {
            catalog.record_group(subject, columns, rows, None)?;
        }
        Observation::Predicate {
            datasource,
            schema,
            table,
            template,
        } => {
            let input_rows = catalog.table_rows(datasource, schema, table, None)?;
            catalog.record_predicate(datasource, schema, table, template, input_rows, rows, "")?;
        }
    }
    Ok(())
}

/// The per-stage planning wall-clock log backing the budget kill report. One
/// `PlanBudget` clock spans all stages; `finish` records the stage that just
/// completed and kills the plan with the full report once the clock runs out.
struct StageLog {
    budget: PlanBudget,
    stage_start: Instant,
    entries: Vec<(&'static str, f64)>,
}

impl StageLog {
    /// Start the shared planning clock and the first stage's timer.
    fn start(budget_ms: u64) -> Self {
        Self {
            budget: PlanBudget::start(budget_ms),
            stage_start: Instant::now(),
            entries: Vec::new(),
        }
    }

    /// Record the just-completed stage's timing; raise `PlanningBudget` with
    /// the per-stage report when the shared budget is now exceeded.
    fn finish(&mut self, stage: &'static str) -> Result<(), RuntimeError> {
        let elapsed_ms = self.stage_start.elapsed().as_secs_f64() * 1000.0;
        self.entries.push((stage, elapsed_ms));
        self.stage_start = Instant::now();
        if !self.budget.exceeded() {
            return Ok(());
        }
        Err(RuntimeError::PlanningBudget(self.report(stage)))
    }

    /// The kill message: total vs budget, the stage it died after, and every
    /// completed stage's unrounded timing.
    fn report(&self, killed_after: &str) -> String {
        let mut parts = Vec::new();
        for (stage, ms) in &self.entries {
            parts.push(format!("{stage} {ms:.1}ms"));
        }
        format!(
            "planning budget exceeded: {:.1}ms > {}ms budget; killed after {killed_after} ({}) \
             - planning must be O(metadata); raise optimizer.planning_budget_ms only for a \
             justified edge case",
            self.budget.elapsed_ms(),
            self.budget.limit_ms(),
            parts.join(", ")
        )
    }
}

/// Build one datasource's catalog handle and register it in the fq-exec data
/// plane, dispatching on the declared `type`.
fn register_datasource(
    catalog: &mut Catalog,
    datasource: &DataSourceConfig,
) -> Result<(), RuntimeError> {
    match datasource.ty.as_str() {
        "duckdb" => register_duckdb(catalog, datasource),
        "postgres" | "postgresql" => register_postgres(catalog, datasource),
        other => Err(RuntimeError::Config(format!(
            "datasource '{}' has unsupported type '{other}'",
            datasource.name
        ))),
    }
}

/// Register a DuckDB source. The whole process shares ONE read-write DuckDB
/// instance per file: the catalog opens it here, and the exec data plane is
/// seeded with a clone of that same connection (`seed_duck_connection`) so it
/// never opens the file a second time. Read-write is required because the engine
/// ships dim/semi-join temp tables via the DuckDB Appender.
fn register_duckdb(
    catalog: &mut Catalog,
    datasource: &DataSourceConfig,
) -> Result<(), RuntimeError> {
    let path = require_str(datasource, "path")?;
    let source = DuckDbSource::open(datasource.name.clone(), &path)?;
    connectors::seed_duck_connection(path.clone(), source.clone_connection()?);
    catalog.register_datasource(Arc::new(source));
    let spec = connectors::spec_from_kind("duckdb", Some(path), None)?;
    connectors::register(datasource.name.clone(), spec);
    Ok(())
}

/// Register a Postgres source: a libpq catalog handle (metadata/stats) plus the
/// exec-plane ADBC spec built from the same connection parameters.
fn register_postgres(
    catalog: &mut Catalog,
    datasource: &DataSourceConfig,
) -> Result<(), RuntimeError> {
    let source = PostgresSource::connect(
        datasource.name.clone(),
        &libpq_conn_str(datasource)?,
        schemas(datasource),
    )?;
    catalog.register_datasource(Arc::new(source));
    let adbc_driver = optional_str(datasource, "adbc_driver");
    let spec =
        connectors::spec_from_kind("postgres", Some(postgres_uri(datasource)?), adbc_driver)?;
    connectors::register(datasource.name.clone(), spec);
    Ok(())
}

/// The libpq connection string the catalog connector uses: the space-delimited
/// `key=value` form, with `password` appended only when the config sets one.
fn libpq_conn_str(datasource: &DataSourceConfig) -> Result<String, RuntimeError> {
    let host = string_param(datasource, "host").unwrap_or_else(|| "localhost".to_string());
    let port = string_param(datasource, "port").unwrap_or_else(|| "5432".to_string());
    let user = require_str(datasource, "user")?;
    let database = database_name(datasource)?;
    let conn = format!("host={host} port={port} user={user} dbname={database}");
    match string_param(datasource, "password") {
        Some(password) => Ok(format!("{conn} password={password}")),
        None => Ok(conn),
    }
}

/// The `postgresql://` URI the ADBC exec connector uses, built from the same
/// parameters (password embedded only when set).
fn postgres_uri(datasource: &DataSourceConfig) -> Result<String, RuntimeError> {
    let host = string_param(datasource, "host").unwrap_or_else(|| "localhost".to_string());
    let port = string_param(datasource, "port").unwrap_or_else(|| "5432".to_string());
    let user = require_str(datasource, "user")?;
    let database = database_name(datasource)?;
    let credentials = match string_param(datasource, "password") {
        Some(password) => format!("{user}:{password}"),
        None => user,
    };
    Ok(format!(
        "postgresql://{credentials}@{host}:{port}/{database}"
    ))
}

/// The Postgres database name, accepting either the `database` or the `dbname`
/// config key (the Python config used `database`; libpq uses `dbname`).
fn database_name(datasource: &DataSourceConfig) -> Result<String, RuntimeError> {
    if let Some(name) = optional_str(datasource, "database") {
        return Ok(name);
    }
    require_str(datasource, "dbname")
}

/// The Postgres schema list, from the `schemas` config sequence; defaults to
/// `["public"]` when absent.
fn schemas(datasource: &DataSourceConfig) -> Vec<String> {
    match datasource
        .config
        .get("schemas")
        .and_then(Value::as_sequence)
    {
        Some(sequence) => collect_schema_names(sequence),
        None => vec!["public".to_string()],
    }
}

/// Collect the string elements of a `schemas` sequence, skipping any non-string
/// element (a malformed entry contributes no schema rather than a bogus name).
fn collect_schema_names(sequence: &[Value]) -> Vec<String> {
    let mut names = Vec::new();
    for element in sequence {
        if let Some(name) = element.as_str() {
            names.push(name.to_string());
        }
    }
    names
}

/// A required string connection parameter; raises a config error if it is absent
/// or not a string (a mistyped or missing param must fail loud, not default).
fn require_str(datasource: &DataSourceConfig, key: &str) -> Result<String, RuntimeError> {
    optional_str(datasource, key).ok_or_else(|| {
        RuntimeError::Config(format!(
            "datasource '{}' is missing required string parameter '{key}'",
            datasource.name
        ))
    })
}

/// An optional string connection parameter (present and a string, else `None`).
fn optional_str(datasource: &DataSourceConfig, key: &str) -> Option<String> {
    datasource
        .config
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// A scalar connection parameter rendered as a string: a YAML string as-is, a
/// YAML integer stringified (e.g. `port: 5432`). Other shapes yield `None`.
fn string_param(datasource: &DataSourceConfig, key: &str) -> Option<String> {
    match datasource.config.get(key) {
        Some(Value::String(text)) => Some(text.clone()),
        Some(Value::Number(number)) => Some(number.to_string()),
        _ => None,
    }
}

/// Rename the engine's internal result schema onto the plan's user-visible output
/// column names, positionally. The engine derives its schema from the final
/// binding (physical names); this restores the names the user asked for. RAISES
/// on a column-count mismatch rather than ship a mislabeled result.
fn rename_result(
    schema: &SchemaRef,
    batches: Vec<RecordBatch>,
    names: &[String],
) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
    if schema.fields().len() != names.len() {
        return Err(RuntimeError::ResultShape(format!(
            "engine returned {} columns but the plan names {}",
            schema.fields().len(),
            names.len()
        )));
    }
    let renamed = renamed_schema(schema, names);
    let mut out = Vec::with_capacity(batches.len());
    for batch in batches {
        let rebuilt = RecordBatch::try_new(Arc::clone(&renamed), batch.columns().to_vec())
            .map_err(|error| RuntimeError::ResultShape(error.to_string()))?;
        out.push(rebuilt);
    }
    Ok((renamed, out))
}

/// Build a schema identical to `schema` but with each field renamed to the
/// matching user-visible name (type, nullability, and metadata preserved).
fn renamed_schema(schema: &SchemaRef, names: &[String]) -> SchemaRef {
    let mut fields = Vec::with_capacity(names.len());
    for (field, name) in schema.fields().iter().zip(names) {
        fields.push(field.as_ref().clone().with_name(name.clone()));
    }
    Arc::new(Schema::new(fields))
}
