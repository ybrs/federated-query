//! Engine configuration: the YAML-backed `Config` tree and its loader.
//!
//! Ports `federated_query/config/config.py`. The Python side used pydantic
//! `StateModel`s whose `extra="forbid"` made an unknown key loud; here the
//! typed structs plus `#[serde(deny_unknown_fields)]` carry the same guarantee,
//! so the same YAML files parse unchanged and a typo still raises. Field-level
//! defaults come from each struct's `Default` (via `#[serde(default)]`), so a
//! missing key falls back exactly as the Python defaults did.

use std::collections::BTreeMap;
use std::path::Path;

use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_yaml::{Mapping, Value};

use crate::error::ConfigError;

/// The top-level sections `load_config` recognizes. Anything else is a typo and
/// raises (`config.py`'s `known_sections`).
const KNOWN_SECTIONS: [&str; 5] = ["datasources", "optimizer", "executor", "cost", "server"];

/// Configuration for a single data source.
///
/// `ty` is the Python `type` field (renamed - `type` is a Rust keyword). `config`
/// holds the connector-specific connection params (every block key that is not
/// `type` or `capabilities`), preserved as raw YAML values.
#[derive(Debug, Clone, PartialEq)]
pub struct DataSourceConfig {
    pub name: String,
    pub ty: String,
    pub config: BTreeMap<String, Value>,
    pub capabilities: Vec<String>,
}

/// Configuration for the query optimizer: the rule enable flags, the
/// join-reorder bound, and the dim-shipping size/cardinality gates.
///
/// The gates are configuration, not compile-time constants: a small-fixture test
/// lowers them through the YAML `optimizer:` section to make shipping fire on
/// tiny data, and a production deployment can retune them without a rebuild.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct OptimizerConfig {
    pub enable_predicate_pushdown: bool,
    pub enable_projection_pushdown: bool,
    pub enable_join_reordering: bool,
    pub enable_decorrelation: bool,
    /// Use the Selinger DP for at most this many tables; GOO greedy above it.
    pub max_join_reorder_size: u32,
    /// Hard wall-clock budget for PLANNING one query, in milliseconds. Planning
    /// is O(metadata) by design; blowing this budget means something O(data)
    /// ran at plan time, and the pipeline KILLS the plan with a stage report
    /// (`fq_common::PlanBudget`). Raise it explicitly for a genuine edge case;
    /// there is no off switch.
    pub planning_budget_ms: u64,
    /// Dim shipping: the fact (local) side must clear this many rows before a
    /// dimension ships into it; below it the fact transfer is cheap and the
    /// temp-table build is not worth it.
    pub ship_local_floor: u64,
    /// Dim shipping: never ship more than this many foreign rows into a source; a
    /// big shipped dim costs a big build AND means the fact is not the dominant
    /// transfer.
    pub ship_row_budget: u64,
    /// Dim shipping: the local (fact) side must exceed the shipped foreign side by
    /// at least this factor.
    pub ship_min_ratio: u64,
    /// Dim shipping: a group-key dimension with NDV at or above this is
    /// high-cardinality (two independent high-card dimensions do not collapse).
    pub ship_high_card_ndv: i64,
    /// Dim shipping: a ship-target aggregate whose measured group count keeps more
    /// than this fraction of its estimated input rows does not collapse.
    pub ship_collapse_max_fraction: f64,
}

impl Default for OptimizerConfig {
    /// Mirrors the Python defaults: every rule on, DP up to 10 tables. The
    /// planning budget is Rust-new: 100ms, far above a healthy metadata-only
    /// plan and far below any plan-time data scan. The ship gates carry the
    /// values dim shipping was tuned against on TPC-DS.
    fn default() -> Self {
        Self {
            enable_predicate_pushdown: true,
            enable_projection_pushdown: true,
            enable_join_reordering: true,
            enable_decorrelation: true,
            max_join_reorder_size: 10,
            planning_budget_ms: 100,
            ship_local_floor: 100_000,
            ship_row_budget: 200_000,
            ship_min_ratio: 20,
            ship_high_card_ndv: 10_000,
            ship_collapse_max_fraction: 0.1,
        }
    }
}

/// Configuration for the query executor.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ExecutorConfig {
    pub max_memory_mb: u64,
    pub batch_size: u64,
    pub max_threads: u32,
    pub enable_parallel_fetch: bool,
}

impl Default for ExecutorConfig {
    /// Mirrors the Python defaults.
    fn default() -> Self {
        Self {
            max_memory_mb: 1024,
            batch_size: 10000,
            max_threads: 4,
            enable_parallel_fetch: true,
        }
    }
}

/// Configuration for the cost model.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct CostConfig {
    pub cpu_tuple_cost: f64,
    pub io_page_cost: f64,
    pub network_byte_cost: f64,
    pub network_rtt_ms: f64,
}

impl Default for CostConfig {
    /// Mirrors the Python defaults.
    fn default() -> Self {
        Self {
            cpu_tuple_cost: 0.01,
            io_page_cost: 1.0,
            network_byte_cost: 0.0001,
            network_rtt_ms: 10.0,
        }
    }
}

/// Authentication for the wire-protocol server (`fedq-server`).
///
/// An empty `users` list - the default, and the state when the `server:` section
/// is absent - means trust authentication: every connection is accepted with no
/// password, like a `trust` line in `pg_hba.conf`. A non-empty list turns on
/// SCRAM-SHA-256: a connection must authenticate as one of these users. This
/// section is inert for the engine library and the Python FFI; only the wire
/// server reads it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
#[serde(deny_unknown_fields, default)]
pub struct ServerConfig {
    pub users: Vec<UserCredential>,
}

/// One user's stored SCRAM-SHA-256 credential.
///
/// The password itself is NEVER stored. `salted_password` is the base64 of
/// `PBKDF2-HMAC-SHA256(password, salt, 4096)` - the same one-way salted hash the
/// SCRAM handshake verifies a login against - and `salt` is the base64 of the
/// random salt it was derived with. The iteration count is fixed at the SCRAM
/// default (4096) for every user, because one handshake advertises one iteration
/// count to the client. `fedq-server hash-password` produces these fields from a
/// plaintext password so the plaintext never enters the config file.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UserCredential {
    pub name: String,
    pub salt: String,
    pub salted_password: String,
}

/// The SCRAM-SHA-256 iteration count every `UserCredential` is hashed with. Fixed
/// because a single handshake advertises one iteration count to the client, so
/// all users on one server must share it.
pub const SCRAM_ITERATIONS: u32 = 4096;

/// The fully assembled engine configuration.
///
/// `source_path` is the YAML file this config was loaded from; the learned-stats
/// catalog defaults next to it (one catalog per configuration). It is `None` for
/// a programmatically built config.
#[derive(Debug, Clone, PartialEq)]
pub struct Config {
    pub datasources: BTreeMap<String, DataSourceConfig>,
    pub optimizer: OptimizerConfig,
    pub executor: ExecutorConfig,
    pub cost: CostConfig,
    pub server: ServerConfig,
    pub source_path: Option<String>,
}

/// Load configuration from a YAML file.
///
/// Rejects a missing file, an unknown top-level section, and a malformed
/// data-source block loudly (see `ConfigError`); optimizer/executor/cost
/// sections default when absent and reject unknown keys when present.
pub fn load_config(config_path: &str) -> Result<Config, ConfigError> {
    let path = Path::new(config_path);
    if !path.exists() {
        return Err(ConfigError::FileNotFound(config_path.to_string()));
    }
    let text = std::fs::read_to_string(path)?;
    let root: Value = serde_yaml::from_str(&text)?;
    let mapping = root.as_mapping().ok_or(ConfigError::RootNotMapping)?;

    reject_unknown_sections(mapping)?;

    Ok(Config {
        datasources: parse_datasources(mapping)?,
        optimizer: parse_section(mapping, "optimizer")?,
        executor: parse_section(mapping, "executor")?,
        cost: parse_section(mapping, "cost")?,
        server: parse_section(mapping, "server")?,
        source_path: Some(path.display().to_string()),
    })
}

/// Raise if any top-level key is not a known section, so a typo (e.g. `optimizr`)
/// fails loud instead of being dropped. The message lists the offending names
/// sorted, matching `config.py`.
fn reject_unknown_sections(mapping: &Mapping) -> Result<(), ConfigError> {
    let mut unknown = Vec::new();
    for (key, _) in mapping {
        let name = key.as_str().ok_or(ConfigError::NonStringSection)?;
        if !KNOWN_SECTIONS.contains(&name) {
            unknown.push(name.to_string());
        }
    }
    if unknown.is_empty() {
        return Ok(());
    }
    unknown.sort();
    Err(ConfigError::UnknownSection(unknown.join(", ")))
}

/// Deserialize an optimizer/executor/cost section, defaulting when it is absent
/// or explicitly null so the estimator/executor always has a concrete config.
fn parse_section<T>(mapping: &Mapping, key: &str) -> Result<T, ConfigError>
where
    T: Default + DeserializeOwned,
{
    match mapping.get(key) {
        None => Ok(T::default()),
        Some(value) if value.is_null() => Ok(T::default()),
        Some(value) => Ok(serde_yaml::from_value(value.clone())?),
    }
}

/// Parse the `datasources` section into a name -> config map. An absent or null
/// section yields an empty map.
fn parse_datasources(mapping: &Mapping) -> Result<BTreeMap<String, DataSourceConfig>, ConfigError> {
    let mut datasources = BTreeMap::new();
    let section = match mapping.get("datasources") {
        None => return Ok(datasources),
        Some(value) if value.is_null() => return Ok(datasources),
        Some(value) => value,
    };
    let block = section
        .as_mapping()
        .ok_or(ConfigError::DatasourcesNotMapping)?;
    for (name_value, ds_value) in block {
        let name = name_value.as_str().ok_or(ConfigError::NonStringSection)?;
        datasources.insert(name.to_string(), parse_one_datasource(name, ds_value)?);
    }
    Ok(datasources)
}

/// Build one `DataSourceConfig` from its YAML block: pull out `type` and
/// `capabilities`, and keep every other key as a connection param in `config`.
fn parse_one_datasource(name: &str, block: &Value) -> Result<DataSourceConfig, ConfigError> {
    let map = block
        .as_mapping()
        .ok_or_else(|| ConfigError::DatasourceNotMapping(name.to_string()))?;
    let mut ty = None;
    let mut capabilities = Vec::new();
    let mut config = BTreeMap::new();
    for (key_value, value) in map {
        let key = key_value
            .as_str()
            .ok_or_else(|| ConfigError::NonStringDatasourceKey(name.to_string()))?;
        assign_datasource_field(name, key, value, &mut ty, &mut capabilities, &mut config)?;
    }
    let ty = ty.ok_or_else(|| ConfigError::MissingDatasourceType(name.to_string()))?;
    Ok(DataSourceConfig {
        name: name.to_string(),
        ty,
        config,
        capabilities,
    })
}

/// Route a single data-source block key into `type`, `capabilities`, or the
/// leftover connection-param `config` map.
fn assign_datasource_field(
    name: &str,
    key: &str,
    value: &Value,
    ty: &mut Option<String>,
    capabilities: &mut Vec<String>,
    config: &mut BTreeMap<String, Value>,
) -> Result<(), ConfigError> {
    match key {
        "type" => {
            let text = value
                .as_str()
                .ok_or_else(|| ConfigError::NonStringType(name.to_string()))?;
            *ty = Some(text.to_string());
        }
        "capabilities" => {
            *capabilities = parse_capabilities(name, value)?;
        }
        other => {
            config.insert(other.to_string(), value.clone());
        }
    }
    Ok(())
}

/// Parse a `capabilities` value into a list of strings, raising on any non-string
/// element or a non-sequence value.
fn parse_capabilities(name: &str, value: &Value) -> Result<Vec<String>, ConfigError> {
    let sequence = value
        .as_sequence()
        .ok_or_else(|| ConfigError::BadCapabilities(name.to_string()))?;
    let mut capabilities = Vec::new();
    for element in sequence {
        let text = element
            .as_str()
            .ok_or_else(|| ConfigError::BadCapabilities(name.to_string()))?;
        capabilities.push(text.to_string());
    }
    Ok(capabilities)
}
