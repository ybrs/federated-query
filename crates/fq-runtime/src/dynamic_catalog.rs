//! Runtime datasource DDL: `CREATE DATASOURCE`, `DROP DATASOURCE`, and
//! `SHOW DATASOURCES`.
//!
//! A datasource becomes manageable at runtime: CREATE validates the source
//! (connect + metadata load, under `catalog.create_connect_timeout_ms`) BEFORE
//! it persists a row in the stats SQLite and installs it into this session;
//! DROP removes the row, purges the name's learned stats, and deregisters it;
//! SHOW lists every bootstrap (YAML) and dynamic (persisted) source with its
//! connection params fail-closed redacted. New sessions pick up persisted
//! sources at `from_config`, so a source survives process restart.

use std::collections::BTreeMap;
use std::sync::{mpsc, Arc};
use std::thread;
use std::time::Duration;

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef};
use serde_yaml::Value;

use fq_accel::DynamicDatasource;
use fq_catalog::Catalog;
use fq_common::{Config, DataSourceConfig, DataType};
use fq_exec::connectors;
use fq_optimize::StatisticsCollector;

use crate::error::RuntimeError;
use crate::materialized::status_result;
use crate::{register_datasource, Runtime};

/// The connection-param keys `SHOW DATASOURCES` may render: an ALLOWLIST, so a
/// credential (or any key not listed) is redacted by omission. A password, or
/// any param added later, never appears unless it is added here deliberately.
const SAFE_DISPLAY_KEYS: [&str; 7] =
    ["host", "port", "database", "schemas", "path", "dir", "file"];

/// The connection-param keys each connector kind accepts. An unknown key raises
/// at CREATE naming it (a typo must fail loud, not connect and be ignored).
fn allowed_keys(kind: &str) -> &'static [&'static str] {
    match kind {
        "postgres" | "postgresql" => &[
            "host",
            "port",
            "user",
            "username",
            "password",
            "database",
            "dbname",
            "schemas",
            "adbc_driver",
        ],
        "clickhouse" | "mysql" => &[
            "host", "port", "user", "username", "password", "database", "schemas",
        ],
        "duckdb" => &["path"],
        "parquet" => &["dir", "file"],
        // The parser admits only the five kinds; a value here is a wiring bug.
        other => unreachable!("unsupported datasource kind '{other}' reached validation"),
    }
}

/// Reject any WITH parameter not on the kind's allowlist, naming it.
fn validate_params(
    name: &str,
    kind: &str,
    params: &[(String, String)],
) -> Result<(), RuntimeError> {
    let allowed = allowed_keys(kind);
    for (key, _) in params {
        if !allowed.contains(&key.as_str()) {
            return Err(RuntimeError::Config(format!(
                "CREATE DATASOURCE '{name}' (type {kind}): unknown parameter '{key}'"
            )));
        }
    }
    Ok(())
}

/// The ordered WITH params as a `BTreeMap` for persistence (JSON) and rendering.
fn params_map(params: &[(String, String)]) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    for (key, value) in params {
        map.insert(key.clone(), value.clone());
    }
    map
}

/// Build the in-memory `DataSourceConfig` a connector registration reads from
/// the stored string params: every value stays a string except `schemas`, which
/// splits on comma into the sequence the pg/clickhouse/mysql registrars expect.
fn datasource_config(
    name: &str,
    kind: &str,
    params: &BTreeMap<String, String>,
) -> DataSourceConfig {
    let mut config = BTreeMap::new();
    for (key, value) in params {
        if key == "schemas" {
            config.insert(key.clone(), split_schemas(value));
        } else {
            config.insert(key.clone(), Value::String(value.clone()));
        }
    }
    // A fresh config assembled from DDL params: the DDL surface carries no
    // capabilities or change-key declarations, so both are empty here (a YAML
    // block would fill them; a dynamic source declares neither).
    DataSourceConfig {
        name: name.to_string(),
        ty: kind.to_string(),
        config,
        capabilities: Vec::new(),
        change_keys: BTreeMap::new(),
    }
}

/// Split a comma-separated `schemas` value into the YAML sequence of names the
/// registrars read, trimming surrounding whitespace and dropping empty entries.
fn split_schemas(value: &str) -> Value {
    let mut names = Vec::new();
    for part in value.split(',') {
        let trimmed = part.trim();
        if !trimmed.is_empty() {
            names.push(Value::String(trimmed.to_string()));
        }
    }
    Value::Sequence(names)
}

/// Validate a datasource in ISOLATION: register it into a throwaway catalog
/// (connecting) and load its metadata, on a worker thread bounded by
/// `timeout_ms`. Success returns the probe catalog (its handle + schemas are
/// absorbed into the live catalog by the caller); the exec-plane registration
/// the probe performed under `session` is kept, so the source is ready to
/// serve. A failure or timeout returns the real error naming the source, kind,
/// and phase. On timeout the worker is left to finish or fail on its own TCP
/// deadline (there is no driver cancellation); it can only leave a harmless
/// stale exec-plane entry that a later create overwrites or session drop prunes.
fn probe_validate(
    session: connectors::SessionId,
    ds: &DataSourceConfig,
    timeout_ms: u64,
) -> Result<Catalog, RuntimeError> {
    let (tx, rx) = mpsc::channel();
    let worker_ds = ds.clone();
    thread::spawn(move || {
        let _ = tx.send(probe_connect_and_load(session, &worker_ds));
    });
    match rx.recv_timeout(Duration::from_millis(timeout_ms)) {
        Ok(result) => result,
        Err(mpsc::RecvTimeoutError::Timeout | mpsc::RecvTimeoutError::Disconnected) => {
            Err(RuntimeError::Config(format!(
                "CREATE DATASOURCE '{}' (type {}): connect + metadata load exceeded \
                 {timeout_ms}ms (catalog.create_connect_timeout_ms)",
                ds.name, ds.ty
            )))
        }
    }
}

/// Connect the source (build its catalog handle + register it in the exec plane)
/// and introspect its metadata, naming the phase that fails in the error.
fn probe_connect_and_load(
    session: connectors::SessionId,
    ds: &DataSourceConfig,
) -> Result<Catalog, RuntimeError> {
    let mut catalog = Catalog::new();
    register_datasource(session, &mut catalog, ds).map_err(|error| {
        RuntimeError::Config(format!(
            "datasource '{}' (type {}) failed to connect: {error}",
            ds.name, ds.ty
        ))
    })?;
    catalog.load_metadata().map_err(|error| {
        RuntimeError::Config(format!(
            "datasource '{}' (type {}) failed to load metadata: {error}",
            ds.name, ds.ty
        ))
    })?;
    Ok(catalog)
}

/// Merge the persisted dynamic sources into a freshly built catalog: each is
/// validated in isolation, its handle+schemas absorbed on success, and marked
/// UNAVAILABLE (with the real error) on failure so an unreferenced dead source
/// blocks nothing while a reference to it raises the stored error. A YAML/
/// persisted name collision is caught earlier by `reject_bootstrap_collision`.
pub(crate) fn install_persisted_datasources(
    session: connectors::SessionId,
    catalog: &mut Catalog,
    persisted: &[DynamicDatasource],
    timeout_ms: u64,
) {
    for row in persisted {
        let ds = datasource_config(&row.name, &row.kind, &row.params);
        match probe_validate(session, &ds, timeout_ms) {
            Ok(probe) => catalog.take_datasource_from(&probe, &row.name),
            Err(error) => catalog.mark_unavailable(row.name.clone(), error.to_string()),
        }
    }
}

/// Raise when a name is defined in BOTH the YAML config and the persisted
/// dynamic catalog: the two rows may carry different params, so there is no
/// correct source to pick and construction refuses rather than guess.
pub(crate) fn reject_bootstrap_collision(
    config: &Config,
    persisted: &[DynamicDatasource],
) -> Result<(), RuntimeError> {
    for row in persisted {
        let collides = config
            .datasources
            .keys()
            .any(|name| name.eq_ignore_ascii_case(&row.name));
        if collides {
            return Err(RuntimeError::Config(format!(
                "datasource '{}' is defined in BOTH the config file and the persisted \
                 dynamic catalog; remove one (drop the dynamic source or delete the \
                 YAML block)",
                row.name
            )));
        }
    }
    Ok(())
}

impl Runtime {
    /// `CREATE DATASOURCE name TYPE kind WITH (...)`: validate the params, then
    /// connect + metadata-load the source; only on success persist the row
    /// (`INSERT OR IGNORE` - a concurrent create of the same name makes the
    /// loser raise `already exists`) and install it into THIS session. Nothing
    /// persists or installs on a validation failure, which raises the real
    /// connector error.
    pub(crate) fn create_datasource(
        &self,
        name: &str,
        kind: &str,
        params: &[(String, String)],
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let accel = self.accelerator()?;
        validate_params(name, kind, params)?;
        if self.catalog_snapshot().get_datasource(name).is_some() {
            return Err(RuntimeError::Config(format!(
                "CREATE DATASOURCE: datasource '{name}' already exists"
            )));
        }
        let stored = params_map(params);
        let ds = datasource_config(name, kind, &stored);
        let timeout = self.config_snapshot().catalog.create_connect_timeout_ms;
        let probe = probe_validate(self.session, &ds, timeout)?;
        self.persist_and_install(accel, name, kind, &stored, &probe)?;
        status_result("CREATE DATASOURCE")
    }

    /// Persist the validated source and install it into this session, or - when
    /// a concurrent create won the `INSERT OR IGNORE` race - discard the
    /// validated connection and raise `already exists`.
    fn persist_and_install(
        &self,
        accel: &fq_accel::Accelerator,
        name: &str,
        kind: &str,
        stored: &BTreeMap<String, String>,
        probe: &Catalog,
    ) -> Result<(), RuntimeError> {
        // A row assembled to persist the validated source; `created_at` is
        // stamped by the registry on insert, so the placeholder is never read.
        let row = DynamicDatasource {
            name: name.to_string(),
            kind: kind.to_string(),
            params: stored.clone(),
            created_at: String::new(),
        };
        if !accel.insert_datasource(&row)? {
            connectors::deregister(self.session, name);
            return Err(RuntimeError::Config(format!(
                "CREATE DATASOURCE: '{name}' already exists"
            )));
        }
        self.install_datasource(probe, name);
        Ok(())
    }

    /// Swap in a catalog clone carrying the new source's handle and schemas, and
    /// rebuild the statistics collector over it (the exec-plane registration
    /// happened during validation). Mirrors `install_views`.
    fn install_datasource(&self, probe: &Catalog, name: &str) {
        let mut next = (*self.catalog_snapshot()).clone();
        next.take_datasource_from(probe, name);
        let next = Arc::new(next);
        *self.catalog.write().expect("catalog lock poisoned") = Arc::clone(&next);
        *self.stats.write().expect("stats lock poisoned") =
            Arc::new(StatisticsCollector::new(next, self.learned.clone(), None));
    }

    /// `DROP DATASOURCE name`: remove a DYNAMIC source's persisted row, purge
    /// its learned stats, and deregister it from this session. A bootstrap
    /// (YAML) source raises (removing it here would resurrect it on reconnect);
    /// a source a materialized view depends on raises naming the view(s);
    /// an unknown name raises.
    pub(crate) fn drop_datasource(
        &self,
        name: &str,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let accel = self.accelerator()?;
        if accel.get_datasource(name)?.is_none() {
            return Err(self.drop_missing_error(name));
        }
        let dependents = self.views_referencing(name)?;
        if !dependents.is_empty() {
            return Err(RuntimeError::Config(format!(
                "DROP DATASOURCE: '{name}' is referenced by materialized view(s) \
                 {}; drop the view(s) first",
                dependents.join(", ")
            )));
        }
        if let Some(learned) = &self.learned {
            learned.purge_datasource(name)?;
        }
        accel.delete_datasource(name)?;
        connectors::deregister(self.session, name);
        self.uninstall_datasource(name);
        status_result("DROP DATASOURCE")
    }

    /// The raise for a DROP of a name that is not a dynamic source: a bootstrap
    /// (YAML) source gets the remove-from-YAML message; anything else does not
    /// exist.
    fn drop_missing_error(&self, name: &str) -> RuntimeError {
        let is_bootstrap = self
            .config_snapshot()
            .datasources
            .keys()
            .any(|configured| configured.eq_ignore_ascii_case(name));
        if is_bootstrap {
            return RuntimeError::Config(format!(
                "'{name}' is a bootstrap datasource defined in the config file; remove \
                 it from the YAML, not via DROP"
            ));
        }
        RuntimeError::Config(format!(
            "DROP DATASOURCE: datasource '{name}' does not exist"
        ))
    }

    /// The names of the materialized views whose definition reads a base
    /// table in datasource `name`. Each view's stored SELECT is bound against
    /// the live catalog (the source is still registered at this point); if a
    /// definition no longer binds, a conservative textual check on the
    /// qualifier keeps the drop from orphaning a dependent view.
    fn views_referencing(&self, name: &str) -> Result<Vec<String>, RuntimeError> {
        let accel = self.accelerator()?;
        let catalog = self.catalog_snapshot();
        let config = self.config_snapshot();
        let mut dependents = Vec::new();
        for view in accel.views()? {
            if view_reads_datasource(&catalog, &config, &view.definition_sql, name) {
                dependents.push(view.name.clone());
            }
        }
        Ok(dependents)
    }

    /// Remove the dropped source from this session: swap in a catalog clone
    /// without it (and without any unavailable mark), rebuild the stats
    /// collector. A peer session keeps its own snapshot until it reconnects.
    fn uninstall_datasource(&self, name: &str) {
        let mut next = (*self.catalog_snapshot()).clone();
        next.remove_datasource(name);
        let next = Arc::new(next);
        *self.catalog.write().expect("catalog lock poisoned") = Arc::clone(&next);
        *self.stats.write().expect("stats lock poisoned") =
            Arc::new(StatisticsCollector::new(next, self.learned.clone(), None));
    }

    /// `SHOW DATASOURCES`: one row per bootstrap and dynamic source - name,
    /// type, origin, a fail-closed redacted param summary, and creation time
    /// (empty for a bootstrap source). Passwords and any non-allowlisted param
    /// never appear in the summary.
    pub(crate) fn show_datasources(&self) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let mut rows = DatasourceRows::default();
        for datasource in self.config_snapshot().datasources.values() {
            rows.push_bootstrap(datasource);
        }
        for row in self.accelerator()?.list_datasources()? {
            rows.push_dynamic(&row);
        }
        rows.sort_by_name();
        let schema = datasources_schema();
        let batch = rows.into_batch(Arc::clone(&schema))?;
        Ok((schema, vec![batch]))
    }
}

/// Whether a view's stored SELECT reads a base table in datasource `name`.
fn view_reads_datasource(
    catalog: &Catalog,
    config: &Config,
    definition_sql: &str,
    name: &str,
) -> bool {
    match crate::delta::classify(catalog, config, definition_sql) {
        Ok(plan) => plan
            .base_tables
            .iter()
            .any(|base| base.datasource.eq_ignore_ascii_case(name)),
        // A definition that no longer binds cannot be introspected; fall back to
        // a conservative qualifier scan so a drop never orphans a dependent view.
        Err(_) => sql_mentions_qualifier(definition_sql, name),
    }
}

/// Whether `sql` contains the datasource name used as a qualifier (`name.`),
/// case-insensitively. A conservative over-match: refusing a drop is safer than
/// orphaning a view whose next refresh would fail to bind.
fn sql_mentions_qualifier(sql: &str, name: &str) -> bool {
    let needle = format!("{}.", name.to_ascii_lowercase());
    sql.to_ascii_lowercase().contains(&needle)
}

/// The fail-closed redacted summary of connection params from the in-memory YAML
/// config: only allowlisted keys, each rendered as `key=value`.
fn summary_from_config(config: &BTreeMap<String, Value>) -> String {
    let mut parts = Vec::new();
    for key in SAFE_DISPLAY_KEYS {
        if let Some(value) = config.get(key) {
            if let Some(rendered) = render_config_value(value) {
                parts.push(format!("{key}={rendered}"));
            }
        }
    }
    parts.join(" ")
}

/// The fail-closed redacted summary of the persisted string params: only
/// allowlisted keys, each rendered as `key=value`.
fn summary_from_params(params: &BTreeMap<String, String>) -> String {
    let mut parts = Vec::new();
    for key in SAFE_DISPLAY_KEYS {
        if let Some(value) = params.get(key) {
            parts.push(format!("{key}={value}"));
        }
    }
    parts.join(" ")
}

/// Render one allowlisted YAML param value: a string as-is, a number
/// stringified, a sequence (schemas) comma-joined. Other shapes render nothing.
fn render_config_value(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        Value::Number(number) => Some(number.to_string()),
        Value::Sequence(items) => Some(join_sequence(items)),
        _ => None,
    }
}

/// Comma-join the string elements of a `schemas` sequence for the summary.
fn join_sequence(items: &[Value]) -> String {
    let mut names = Vec::new();
    for item in items {
        if let Some(text) = item.as_str() {
            names.push(text.to_string());
        }
    }
    names.join(",")
}

/// The ordered result columns of `SHOW DATASOURCES`.
const DATASOURCE_COLUMNS: [&str; 5] = ["name", "type", "origin", "summary", "created"];

/// The (column name, engine type) pairs `describe` reports for SHOW DATASOURCES.
pub(crate) fn datasources_describe_columns() -> Vec<(String, DataType)> {
    let mut columns = Vec::with_capacity(DATASOURCE_COLUMNS.len());
    for name in DATASOURCE_COLUMNS {
        columns.push((name.to_string(), DataType::Text));
    }
    columns
}

/// The Arrow result schema of `SHOW DATASOURCES`: five text columns.
fn datasources_schema() -> SchemaRef {
    let mut fields = Vec::with_capacity(DATASOURCE_COLUMNS.len());
    for name in DATASOURCE_COLUMNS {
        fields.push(Field::new(name, ArrowDataType::Utf8, false));
    }
    Arc::new(Schema::new(fields))
}

/// The five string columns of a SHOW DATASOURCES result, filled row by row.
#[derive(Default)]
struct DatasourceRows {
    names: Vec<String>,
    types: Vec<String>,
    origins: Vec<String>,
    summaries: Vec<String>,
    created: Vec<String>,
}

impl DatasourceRows {
    /// Append one bootstrap (YAML) source: origin `bootstrap`, no creation time.
    fn push_bootstrap(&mut self, datasource: &DataSourceConfig) {
        self.names.push(datasource.name.clone());
        self.types.push(datasource.ty.clone());
        self.origins.push("bootstrap".to_string());
        self.summaries.push(summary_from_config(&datasource.config));
        self.created.push(String::new());
    }

    /// Append one dynamic (persisted) source: origin `dynamic`, its stored
    /// creation time.
    fn push_dynamic(&mut self, row: &DynamicDatasource) {
        self.names.push(row.name.clone());
        self.types.push(row.kind.clone());
        self.origins.push("dynamic".to_string());
        self.summaries.push(summary_from_params(&row.params));
        self.created.push(row.created_at.clone());
    }

    /// Order the rows by name so the output is deterministic across origins.
    fn sort_by_name(&mut self) {
        let mut order: Vec<usize> = (0..self.names.len()).collect();
        order.sort_by(|&a, &b| self.names[a].cmp(&self.names[b]));
        self.names = reorder(&self.names, &order);
        self.types = reorder(&self.types, &order);
        self.origins = reorder(&self.origins, &order);
        self.summaries = reorder(&self.summaries, &order);
        self.created = reorder(&self.created, &order);
    }

    /// Assemble the five columns into one record batch.
    fn into_batch(self, schema: SchemaRef) -> Result<RecordBatch, RuntimeError> {
        let columns: Vec<Arc<dyn arrow::array::Array>> = vec![
            Arc::new(StringArray::from(self.names)),
            Arc::new(StringArray::from(self.types)),
            Arc::new(StringArray::from(self.origins)),
            Arc::new(StringArray::from(self.summaries)),
            Arc::new(StringArray::from(self.created)),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|error| RuntimeError::ResultShape(error.to_string()))
    }
}

/// Return a new vector with `values` permuted into `order`.
fn reorder(values: &[String], order: &[usize]) -> Vec<String> {
    let mut out = Vec::with_capacity(order.len());
    for &index in order {
        out.push(values[index].clone());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A persisted param summary emits only allowlisted keys; a password is
    /// dropped even though it is present.
    #[test]
    fn summary_is_fail_closed() {
        let mut params = BTreeMap::new();
        params.insert("host".to_string(), "h1".to_string());
        params.insert("port".to_string(), "5432".to_string());
        params.insert("password".to_string(), "secret".to_string());
        params.insert("user".to_string(), "sam".to_string());
        let summary = summary_from_params(&params);
        assert!(summary.contains("host=h1"));
        assert!(summary.contains("port=5432"));
        assert!(!summary.contains("secret"));
        assert!(!summary.contains("sam"));
    }

    /// The YAML summary renders a schemas sequence comma-joined and never a
    /// non-allowlisted key.
    #[test]
    fn config_summary_renders_schemas_and_redacts() {
        let mut config = BTreeMap::new();
        config.insert("host".to_string(), Value::String("h".to_string()));
        config.insert(
            "schemas".to_string(),
            Value::Sequence(vec![
                Value::String("public".to_string()),
                Value::String("staging".to_string()),
            ]),
        );
        config.insert("password".to_string(), Value::String("p".to_string()));
        let summary = summary_from_config(&config);
        assert!(summary.contains("schemas=public,staging"));
        assert!(!summary.contains("password"));
    }

    /// `schemas` splits on comma into a sequence; other params stay strings.
    #[test]
    fn schemas_split_into_a_sequence() {
        let mut params = BTreeMap::new();
        params.insert("schemas".to_string(), "public, inventory".to_string());
        params.insert("host".to_string(), "h".to_string());
        let ds = datasource_config("s", "postgres", &params);
        let schemas = ds.config.get("schemas").and_then(Value::as_sequence);
        let schemas = schemas.expect("schemas is a sequence");
        assert_eq!(schemas.len(), 2);
        assert_eq!(schemas[0].as_str(), Some("public"));
        assert_eq!(schemas[1].as_str(), Some("inventory"));
        assert!(matches!(ds.config.get("host"), Some(Value::String(_))));
    }

    /// An unknown parameter for a kind raises naming it.
    #[test]
    fn unknown_param_raises() {
        let params = vec![("hostt".to_string(), "h".to_string())];
        let error = validate_params("s", "postgres", &params).unwrap_err();
        assert!(matches!(error, RuntimeError::Config(ref m) if m.contains("hostt")));
    }

    /// A duckdb source accepts only `path`.
    #[test]
    fn duckdb_rejects_host() {
        let params = vec![("host".to_string(), "h".to_string())];
        assert!(validate_params("s", "duckdb", &params).is_err());
        let ok = vec![("path".to_string(), "/a.db".to_string())];
        assert!(validate_params("s", "duckdb", &ok).is_ok());
    }
}
