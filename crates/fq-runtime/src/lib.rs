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

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef};
use serde_yaml::Value;

use fq_catalog::Catalog;
use fq_common::{Config, DataSourceConfig};
use fq_connectors::{DuckDbSource, PostgresSource};
use fq_exec::{connectors, execute_plan};
use fq_optimize::{build_optimizer, CostModel, StatisticsCollector};
use fq_parse::parse_with_catalog;
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
        Ok(Self {
            catalog: Arc::new(catalog),
            config: config.clone(),
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
        let (schema, batches, _observations) = execute_plan(&physical)?;
        rename_result(&schema, batches, &visible_names)
    }

    /// Parse -> bind -> decorrelate -> optimize into an optimized logical plan.
    fn optimize(&self, sql: &str) -> Result<LogicalPlan, RuntimeError> {
        let parsed = parse_with_catalog(sql, self.catalog.as_ref())?;
        let bound = fq_bind::bind(self.catalog.as_ref(), parsed)?;
        let decorrelated = fq_decorrelate::decorrelate(bound)?;
        let optimizer = build_optimizer(&self.config.optimizer, self.cost_model());
        Ok(optimizer.optimize(decorrelated)?)
    }

    /// Lower an optimized logical plan into an executable physical plan.
    fn plan(&self, sql: &str) -> Result<PhysicalPlan, RuntimeError> {
        let optimized = self.optimize(sql)?;
        let cost_model = Rc::new(RefCell::new(self.cost_model()));
        let mut planner = PhysicalPlanner::new(Arc::clone(&self.catalog), Some(cost_model));
        Ok(planner.plan(&optimized)?)
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

    /// A fresh cost model wired to this session's catalog so cardinality and
    /// join-order decisions read the sources' LIVE statistics (honest-unknown
    /// defaults fill any gap; a suboptimal plan still returns correct rows).
    fn cost_model(&self) -> CostModel {
        let stats = StatisticsCollector::new(Arc::clone(&self.catalog), None, None);
        CostModel::new(self.config.cost.clone(), Some(stats))
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

/// Register a DuckDB source: a read-only catalog handle plus the exec-plane spec
/// under the datasource's name (the name the physical scans carry).
fn register_duckdb(
    catalog: &mut Catalog,
    datasource: &DataSourceConfig,
) -> Result<(), RuntimeError> {
    let path = require_str(datasource, "path")?;
    let source = DuckDbSource::open_read_only(datasource.name.clone(), &path)?;
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
