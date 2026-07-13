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

mod delta;
pub mod error;
mod events;
mod explain;
mod materialized;
mod settings;

use std::cell::RefCell;
use std::collections::BTreeSet;
use std::rc::Rc;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::Instant;

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef};
use serde_yaml::Value;

use fq_accel::Accelerator;
use fq_catalog::{Catalog, StatsCatalog};
use fq_common::{Config, DataSourceConfig, DataType, PlanBudget};
use fq_connectors::{ClickHouseSource, DuckDbSource, MySqlSource, ParquetSource, PostgresSource};
use fq_exec::{connectors, execute_plan};
use fq_optimize::{build_optimizer, CostModel, StatisticsCollector};
use fq_parse::{classify_statement, parse_with_catalog, Statement};
use fq_physical::steps::Observation;
use fq_physical::{PhysicalPlanner, ShipThresholds};
use fq_plan::logical::LogicalPlan;
use fq_plan::output_column_names;
use fq_plan::physical::PhysicalPlan;

pub use error::RuntimeError;

/// One assembled engine session: a shared catalog (metadata + statistics) plus
/// the config that governs optimization and cost. The fq-exec data plane is a
/// process-wide registry the constructor populates as a side effect, so it is
/// not held here.
pub struct Runtime {
    /// The metadata catalog behind a copy-and-swap: a materialized-view DDL
    /// clones the catalog, applies the change, and swaps the `Arc`, so every
    /// query plans against one consistent snapshot.
    catalog: RwLock<Arc<Catalog>>,
    /// The live engine config behind a lock so `SET`/`RESET` can swap a
    /// session-mutable setting between queries. Each plan reads ONE snapshot
    /// (`config_snapshot`), so a concurrent `SET` cannot change it mid-pipeline.
    config: RwLock<Config>,
    /// The setting names changed by `SET` on this runtime (cleared per name by
    /// `RESET`). Drives the `source = set` column and `RESET ALL`.
    settings_overrides: RwLock<BTreeSet<String>>,
    /// ONE statistics collector for the whole session: its caches (source
    /// stats, planner estimates) persist across queries, and its learned
    /// overlay reads the stats catalog this runtime also WRITES after every
    /// execution. A per-query collector would re-fetch every statistic on
    /// every plan. Rebuilt (over the same learned catalog) when a DDL swaps
    /// the metadata catalog it reads.
    stats: RwLock<Arc<StatisticsCollector>>,
    /// The learned-stats sqlite next to the config, kept to rebuild the
    /// collector on a catalog swap and to persist observations.
    learned: Option<Arc<StatsCatalog>>,
    /// The materialized-view store, present when the config has a file path
    /// (the store lives next to it). DDL against a path-less config raises.
    accelerator: Option<Accelerator>,
    /// The event-view role registry, opened together with the accelerator
    /// (same stats SQLite): an event view is a materialized view plus roles.
    events: Option<fq_events::EventViewRegistry>,
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
        // The materialized-view store registers as one more datasource (its
        // views load through the same metadata path) and its chunk tables go
        // to the exec data plane, so a NEW runtime over the same config sees
        // every view a previous session created.
        let accelerator = materialized::open_accelerator(config)?;
        if let Some(accel) = &accelerator {
            materialized::register_store(&mut catalog, accel)?;
        }
        catalog.load_metadata()?;
        // A change-key declaration naming an unknown table/column, or a
        // monotonic column whose type cannot carry a watermark, fails HERE at
        // load - not silently at a refresh months later.
        delta::validate_change_keys(&catalog, config)?;
        let events = events::open_event_registry(config)?;
        let catalog = Arc::new(catalog);
        let learned = open_stats_catalog(config)?;
        let stats = Arc::new(StatisticsCollector::new(
            Arc::clone(&catalog),
            learned.clone(),
            None,
        ));
        Ok(Self {
            catalog: RwLock::new(catalog),
            config: RwLock::new(config.clone()),
            settings_overrides: RwLock::new(BTreeSet::new()),
            stats: RwLock::new(stats),
            learned,
            accelerator,
            events,
        })
    }

    /// The current catalog snapshot (consistent for the caller's whole plan).
    fn catalog_snapshot(&self) -> Arc<Catalog> {
        Arc::clone(&self.catalog.read().expect("catalog lock poisoned"))
    }

    /// The current config snapshot: ONE consistent copy for a whole plan, so a
    /// concurrent `SET` cannot change the optimizer/cost/ship values mid-pipeline.
    fn config_snapshot(&self) -> Config {
        self.config.read().expect("config lock poisoned").clone()
    }

    /// The current set of SET-overridden setting names (for SHOW's source column).
    fn settings_overrides(&self) -> RwLockReadGuard<'_, BTreeSet<String>> {
        self.settings_overrides
            .read()
            .expect("settings overrides lock poisoned")
    }

    /// Mutable access to the SET-override set (for `SET`/`RESET`).
    fn settings_overrides_mut(&self) -> RwLockWriteGuard<'_, BTreeSet<String>> {
        self.settings_overrides
            .write()
            .expect("settings overrides lock poisoned")
    }

    /// The current statistics-collector snapshot.
    fn stats_snapshot(&self) -> Arc<StatisticsCollector> {
        Arc::clone(&self.stats.read().expect("stats lock poisoned"))
    }

    /// Run one SQL statement and return its Arrow result under the query's
    /// user-visible column names. A leading EXPLAIN short-circuits to a textual
    /// plan WITHOUT executing. Materialized-view DDL dispatches to the store
    /// and returns a one-row `status` table (the pg command-tag convention).
    pub fn execute(&self, sql: &str) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        if let Some(inner) = explain::strip_explain(sql) {
            return self.explain(inner);
        }
        match classify_statement(sql)? {
            Statement::Query(text) => self.execute_query(text),
            Statement::CreateMaterializedView { name, select_sql } => {
                self.create_materialized_view(&name, select_sql)
            }
            Statement::RefreshMaterializedView { name } => self.refresh_materialized_view(&name),
            Statement::DropMaterializedView { name } => self.drop_materialized_view(&name),
            Statement::ShowSettings => self.show_settings(),
            Statement::ShowSetting { name } => self.show_setting(&name),
            Statement::SetSetting { name, value } => self.set_setting(&name, &value),
            Statement::ResetSetting { name } => self.reset_setting(name.as_deref()),
            Statement::CreateEventView {
                name,
                roles,
                select_sql,
            } => self.create_event_view(&name, &roles, select_sql),
            Statement::RefreshEventView { name } => self.refresh_event_view(&name),
            Statement::DropEventView { name } => self.drop_event_view(&name),
            Statement::Funnel(spec) => self.run_funnel_statement(&spec),
            Statement::Segment(spec) => self.run_segment_statement(&spec),
        }
    }

    /// Plan and run one SELECT/VALUES statement through the full pipeline.
    fn execute_query(&self, sql: &str) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
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

    /// Resolve the result schema of `sql` WITHOUT executing it: the ordered
    /// (visible column name, engine type) pairs the query would produce. It
    /// runs the same parse -> ... -> physical plan pipeline `execute` runs and
    /// reads the plan root's output types, which planning derives from catalog
    /// metadata alone (O(metadata), under the same planning budget) - no source
    /// data is scanned. A leading EXPLAIN resolves to the single textual `plan`
    /// column, matching what `execute` returns for it.
    ///
    /// The types are the engine's static types. A DECIMAL's runtime scale is
    /// data-derived and may differ from the static DECIMAL here, but the Postgres
    /// type the wire layer maps both to (NUMERIC) is the same, so the row
    /// description stays consistent with the executed rows.
    pub fn describe(&self, sql: &str) -> Result<Vec<(String, DataType)>, RuntimeError> {
        if explain::strip_explain(sql).is_some() {
            return Ok(vec![("plan".to_owned(), DataType::Text)]);
        }
        // A non-query statement resolves to the columns its execution returns,
        // without running it: SHOW resolves to the settings result columns, the
        // analysis forms to their fixed result relations, and every DDL / SET /
        // RESET to the single `status` column (the pg command-tag convention).
        match classify_statement(sql)? {
            Statement::Query(_) => {}
            Statement::ShowSettings | Statement::ShowSetting { .. } => {
                return Ok(settings::describe_columns());
            }
            Statement::Funnel(_) => return Ok(events::funnel_describe_columns()),
            Statement::Segment(_) => return Ok(events::segment_describe_columns()),
            _ => return Ok(vec![("status".to_owned(), DataType::Text)]),
        }
        let physical = self.plan(sql)?;
        let names = output_column_names(&physical);
        let types = physical.schema();
        if names.len() != types.len() {
            return Err(RuntimeError::ResultShape(format!(
                "plan names {} columns but its schema has {}",
                names.len(),
                types.len()
            )));
        }
        let mut columns = Vec::with_capacity(names.len());
        for (name, (_internal, data_type)) in names.into_iter().zip(types) {
            columns.push((name, data_type));
        }
        Ok(columns)
    }

    /// Parse -> bind -> decorrelate -> optimize into an optimized logical plan,
    /// recording each stage's wall clock in `stages` (which kills the plan the
    /// moment the shared budget is blown).
    fn optimize(
        &self,
        catalog: &Catalog,
        sql: &str,
        stages: &mut StageLog,
        config: &Config,
    ) -> Result<LogicalPlan, RuntimeError> {
        let parsed = parse_with_catalog(sql, catalog)?;
        stages.finish("parse")?;
        let bound = fq_bind::bind(catalog, parsed)?;
        stages.finish("bind")?;
        let decorrelated = fq_decorrelate::decorrelate(bound)?;
        stages.finish("decorrelate")?;
        let optimizer = build_optimizer(&config.optimizer, self.cost_model(config));
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
        // One catalog snapshot for the whole plan, so a concurrent DDL swap
        // cannot change the metadata mid-pipeline.
        let catalog = self.catalog_snapshot();
        // One config snapshot for the whole plan, so a concurrent SET cannot
        // change the optimizer/cost/ship values mid-pipeline.
        let config = self.config_snapshot();
        let mut stages = StageLog::start(config.optimizer.planning_budget_ms);
        // The SHARED collector carries this plan's budget so a statistics fetch
        // that overruns it is killed at the fetch and named in the error.
        self.stats_snapshot().set_plan_budget(stages.budget);
        let optimized = self.optimize(&catalog, sql, &mut stages, &config)?;
        let cost_model = Rc::new(RefCell::new(self.cost_model(&config)));
        // The dim-shipping gates come from THIS runtime's optimizer config, so a
        // config that lowers them (small fixtures) or raises them (kill switch)
        // governs whether a dimension ships.
        let ship_thresholds = ShipThresholds::from_config(&config.optimizer);
        let mut planner = PhysicalPlanner::new(Arc::clone(&catalog), Some(cost_model))
            .with_ship_thresholds(ship_thresholds);
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
        let Some(catalog) = self.learned.as_deref() else {
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
    /// Only a query has a plan: EXPLAIN of a DDL, settings, or analysis
    /// statement raises naming the form instead of feeding it to the SQL parser.
    fn explain(&self, inner: &str) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        if !matches!(classify_statement(inner)?, Statement::Query(_)) {
            return Err(RuntimeError::Parse(fq_parse::ParseError::Unsupported(
                "EXPLAIN takes a query; DDL, settings, and event-analysis \
                 statements have no plan to explain"
                    .to_string(),
            )));
        }
        let physical = self.plan(inner)?;
        let lines = explain::describe(&physical)?;
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
    fn cost_model(&self, config: &Config) -> CostModel {
        CostModel::with_shared_stats(config.cost.clone(), Some(self.stats_snapshot()))
    }
}

/// The path of the stats SQLite that lives NEXT TO the config file
/// (`<config-stem>.stats.sqlite`, one per configuration; it also carries the
/// materialized-view and event-view registries), or `None` for a
/// programmatically built config with no source path.
pub(crate) fn stats_sqlite_path(config: &Config) -> Option<std::path::PathBuf> {
    let source_path = config.source_path.as_deref()?;
    let path = std::path::Path::new(source_path);
    let stem = path
        .file_stem()
        .map_or_else(|| "fedq".to_string(), |s| s.to_string_lossy().to_string());
    Some(
        path.parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .join(stem + ".stats.sqlite"),
    )
}

/// Open the learned-stats catalog at the config's stats SQLite path, or
/// `None` for a config with no source path.
fn open_stats_catalog(config: &Config) -> Result<Option<Arc<StatsCatalog>>, RuntimeError> {
    let Some(stats_path) = stats_sqlite_path(config) else {
        return Ok(None);
    };
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
        "clickhouse" => register_clickhouse(catalog, datasource),
        "mysql" => register_mysql(catalog, datasource),
        "parquet" => register_parquet(catalog, datasource),
        other => Err(RuntimeError::Config(format!(
            "datasource '{}' has unsupported type '{other}'",
            datasource.name
        ))),
    }
}

/// Register a Parquet directory source: a footer-only catalog handle
/// (schema/row-count/statistics) plus the exec-plane spec that points DataFusion
/// at the same directory. Read-only: the catalog handle does not open the file
/// data, and the exec plane reads the file bytes through DataFusion at fetch time.
fn register_parquet(
    catalog: &mut Catalog,
    datasource: &DataSourceConfig,
) -> Result<(), RuntimeError> {
    let dir = require_str(datasource, "dir")?;
    let source = ParquetSource::open(datasource.name.clone(), &dir)?;
    catalog.register_datasource(Arc::new(source));
    let spec = connectors::spec_from_kind("parquet", Some(dir), None)?;
    connectors::register(datasource.name.clone(), spec);
    Ok(())
}

/// Reject any connection-param key not in `allowed` for a datasource block, so a
/// typo (e.g. `hostt`) fails loud at load rather than being silently ignored. An
/// allowlist, never a denylist: an unrecognized key is always an error.
fn reject_unknown_keys(
    datasource: &DataSourceConfig,
    allowed: &[&str],
) -> Result<(), RuntimeError> {
    for key in datasource.config.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(RuntimeError::Config(format!(
                "datasource '{}' has unknown parameter '{key}'",
                datasource.name
            )));
        }
    }
    Ok(())
}

/// Register a ClickHouse source: an HTTP catalog handle (system tables, EXPLAIN
/// ESTIMATE) plus the exec-plane spec, both from the same endpoint. The endpoint
/// URI carries `?user=/&password=` auth so the data-plane POST needs no headers.
fn register_clickhouse(
    catalog: &mut Catalog,
    datasource: &DataSourceConfig,
) -> Result<(), RuntimeError> {
    reject_unknown_keys(
        datasource,
        &[
            "host", "port", "user", "username", "password", "database", "schemas",
        ],
    )?;
    let base_url = clickhouse_base_url(datasource);
    let user = optional_str(datasource, "user").or_else(|| optional_str(datasource, "username"));
    let password = optional_str(datasource, "password");
    let source = ClickHouseSource::connect(
        datasource.name.clone(),
        &base_url,
        user.clone(),
        password.clone(),
        clickhouse_schemas(datasource)?,
    )?;
    catalog.register_datasource(Arc::new(source));
    let spec = connectors::spec_from_kind(
        "clickhouse",
        Some(clickhouse_endpoint(&base_url, user, password)),
        None,
    )?;
    connectors::register(datasource.name.clone(), spec);
    Ok(())
}

/// Register a MySQL source: a sync catalog handle (information_schema, EXPLAIN)
/// plus the exec-plane spec, both from the same `mysql://` URL built from the
/// connection params.
fn register_mysql(
    catalog: &mut Catalog,
    datasource: &DataSourceConfig,
) -> Result<(), RuntimeError> {
    reject_unknown_keys(
        datasource,
        &[
            "host", "port", "user", "username", "password", "database", "schemas",
        ],
    )?;
    let url = mysql_url(datasource)?;
    let source = MySqlSource::connect(datasource.name.clone(), &url, mysql_schemas(datasource)?)?;
    catalog.register_datasource(Arc::new(source));
    let spec = connectors::spec_from_kind("mysql", Some(url), None)?;
    connectors::register(datasource.name.clone(), spec);
    Ok(())
}

/// The `mysql://` URL for a source from its `host`/`port`/`user`/`password`/
/// `database` params (default `localhost:3306`). The default database is the
/// first configured schema, so unqualified reads resolve.
fn mysql_url(datasource: &DataSourceConfig) -> Result<String, RuntimeError> {
    let host = string_param(datasource, "host").unwrap_or_else(|| "localhost".to_string());
    let port = string_param(datasource, "port").unwrap_or_else(|| "3306".to_string());
    let user = optional_str(datasource, "user")
        .or_else(|| optional_str(datasource, "username"))
        .unwrap_or_else(|| "root".to_string());
    let database = mysql_schemas(datasource)?.remove(0);
    let credentials = match string_param(datasource, "password") {
        Some(password) => format!("{}:{}", url_encode(&user), url_encode(&password)),
        None => url_encode(&user),
    };
    Ok(format!("mysql://{credentials}@{host}:{port}/{database}"))
}

/// The MySQL database list from the `schemas` sequence, or the single `database`
/// param. An absent or empty list is a configuration error (MySQL has no default
/// database name here).
fn mysql_schemas(datasource: &DataSourceConfig) -> Result<Vec<String>, RuntimeError> {
    let names = match datasource
        .config
        .get("schemas")
        .and_then(Value::as_sequence)
    {
        Some(sequence) => collect_schema_names(sequence),
        None => match optional_str(datasource, "database") {
            Some(database) => vec![database],
            None => Vec::new(),
        },
    };
    if names.is_empty() {
        return Err(RuntimeError::Config(format!(
            "mysql datasource '{}' needs a 'database' or non-empty 'schemas'",
            datasource.name
        )));
    }
    Ok(names)
}

/// The base HTTP URL of a ClickHouse source from its `host`/`port` (defaults
/// `localhost:8123`), used by the catalog handle (which sends auth as headers).
fn clickhouse_base_url(datasource: &DataSourceConfig) -> String {
    let host = string_param(datasource, "host").unwrap_or_else(|| "localhost".to_string());
    let port = string_param(datasource, "port").unwrap_or_else(|| "8123".to_string());
    format!("http://{host}:{port}/")
}

/// The data-plane endpoint URI: the base URL with any `user`/`password` embedded
/// as query parameters (ClickHouse HTTP authenticates from the query string), so
/// the streaming POST is a pure body request.
fn clickhouse_endpoint(base_url: &str, user: Option<String>, password: Option<String>) -> String {
    match user {
        None => base_url.to_string(),
        Some(user) => format!(
            "{base_url}?user={}&password={}",
            url_encode(&user),
            url_encode(&password.unwrap_or_default())
        ),
    }
}

/// Percent-encode the characters that would break a URL query value. A narrow
/// encoder for credential strings; the reserved set covers the shapes a password
/// can contain that a query parser would otherwise misread.
fn url_encode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(byte as char);
            }
            other => {
                const HEX: &[u8; 16] = b"0123456789ABCDEF";
                out.push('%');
                out.push(HEX[(other >> 4) as usize] as char);
                out.push(HEX[(other & 0x0F) as usize] as char);
            }
        }
    }
    out
}

/// The ClickHouse database list from the `schemas` config sequence. Unlike
/// Postgres, a ClickHouse source has no default database name, so an absent or
/// empty `schemas` is a configuration error rather than a silent default.
fn clickhouse_schemas(datasource: &DataSourceConfig) -> Result<Vec<String>, RuntimeError> {
    let names = match datasource
        .config
        .get("schemas")
        .and_then(Value::as_sequence)
    {
        Some(sequence) => collect_schema_names(sequence),
        None => match optional_str(datasource, "database") {
            Some(database) => vec![database],
            None => Vec::new(),
        },
    };
    if names.is_empty() {
        return Err(RuntimeError::Config(format!(
            "clickhouse datasource '{}' needs a 'database' or non-empty 'schemas'",
            datasource.name
        )));
    }
    Ok(names)
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
