//! The engine's settings surface: one registry describing every tunable, and the
//! `SHOW SETTINGS` / `SHOW SETTING` / `SET` / `RESET` execution behind
//! `Runtime::execute`.
//!
//! One `SettingDef` per tunable carries its dotted name, a description, its
//! default, how to read its current value off a `Config`, and - for the
//! session-mutable ones - how to apply a new value. A setting is one of:
//!
//! - SESSION-MUTABLE: it lives in this runtime's `Config` and is read FRESH on
//!   every plan (the optimizer/cost/ship gates re-read the config snapshot per
//!   query), so changing it on a live runtime takes effect on the next query.
//!   `SET`/`RESET` swap it under the config lock.
//! - STATIC: it is fixed for the life of the process - a compile-time exec
//!   constant, a process-global environment flag read at its consumption site, or
//!   the server credential list baked in at startup. `SET` on a static RAISES and
//!   names why; there is no per-runtime session state to change.
//!
//! The registry is the single inventory of the engine's SCALAR tunables: the
//! completeness test serializes each scalar config struct's default
//! (`OptimizerConfig`, `CostConfig`, `ExecutorConfig`) and asserts every field
//! appears here, so a new scalar field cannot be added without registering it.
//! A datasource's `change_keys` is deliberately outside the registry: it is a
//! per-datasource, per-`schema.table` map of change-key declarations, not a
//! single scalar value with one name, and it is validated at catalog load and
//! never `SET` on a live runtime, so it has no `SHOW`/`SET`/`RESET` shape.

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef};

use fq_common::{
    AcceleratorConfig, CatalogConfig, Config, CostConfig, DataType, EventsConfig, ExecutorConfig,
    OptimizerConfig,
};

use crate::error::RuntimeError;
use crate::Runtime;

/// Whether a setting can be changed on a live runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mutability {
    /// Fixed for the life of the process (exec constant, env flag, or a value
    /// baked in at startup). `SET` raises.
    Static,
    /// Lives in the runtime's config and is read per query; `SET`/`RESET` apply.
    SessionMutable,
}

impl Mutability {
    /// The `mutable` column text.
    fn label(self) -> &'static str {
        match self {
            Mutability::Static => "static",
            Mutability::SessionMutable => "session",
        }
    }
}

/// Where a setting's current value came from, reported in the `source` column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source {
    /// The compile-time default (no config file value, no env, no SET).
    Default,
    /// The loaded config file set it (its current value differs from default).
    ConfigFile,
    /// An environment variable is setting it.
    Env,
    /// A `SET` on this runtime changed it.
    Set,
}

impl Source {
    /// The `source` column text.
    fn label(self) -> &'static str {
        match self {
            Source::Default => "default",
            Source::ConfigFile => "config",
            Source::Env => "env",
            Source::Set => "set",
        }
    }
}

/// A typed setting value: the shape a setting reads, defaults to, and parses a
/// `SET` string into. The variant a setting uses is fixed by its `default`.
#[derive(Debug, Clone, PartialEq)]
pub enum SettingValue {
    Bool(bool),
    U64(u64),
    I64(i64),
    F64(f64),
    Usize(usize),
    Text(String),
}

impl SettingValue {
    /// Render the value for the `value` / `default` result columns.
    fn render(&self) -> String {
        match self {
            SettingValue::Bool(value) => value.to_string(),
            SettingValue::U64(value) => value.to_string(),
            SettingValue::I64(value) => value.to_string(),
            SettingValue::F64(value) => value.to_string(),
            SettingValue::Usize(value) => value.to_string(),
            SettingValue::Text(value) => value.clone(),
        }
    }

    /// The type name for a mismatch message.
    fn type_name(&self) -> &'static str {
        match self {
            SettingValue::Bool(_) => "boolean",
            SettingValue::U64(_) | SettingValue::Usize(_) => "unsigned integer",
            SettingValue::I64(_) => "integer",
            SettingValue::F64(_) => "number",
            SettingValue::Text(_) => "text",
        }
    }

    /// Parse `raw` into the SAME variant as `self` (the setting's declared type),
    /// so `SET` is type-checked against the setting. A malformed value raises.
    fn parse_like(&self, raw: &str) -> Result<SettingValue, SettingsError> {
        match self {
            SettingValue::Bool(_) => parse_bool(raw).map(SettingValue::Bool),
            SettingValue::U64(_) => parse_number(raw, "unsigned integer").map(SettingValue::U64),
            SettingValue::I64(_) => parse_number(raw, "integer").map(SettingValue::I64),
            SettingValue::F64(_) => parse_number(raw, "number").map(SettingValue::F64),
            SettingValue::Usize(_) => {
                parse_number(raw, "unsigned integer").map(SettingValue::Usize)
            }
            SettingValue::Text(_) => Ok(SettingValue::Text(raw.to_string())),
        }
    }

    /// Extract a `u64`, or raise if this is not an unsigned-integer value.
    fn as_u64(&self) -> Result<u64, SettingsError> {
        match self {
            SettingValue::U64(value) => Ok(*value),
            other => Err(SettingsError::internal_type(other, "unsigned integer")),
        }
    }

    /// Extract an `i64`, or raise if this is not an integer value.
    fn as_i64(&self) -> Result<i64, SettingsError> {
        match self {
            SettingValue::I64(value) => Ok(*value),
            other => Err(SettingsError::internal_type(other, "integer")),
        }
    }

    /// Extract an `f64`, or raise if this is not a number value.
    fn as_f64(&self) -> Result<f64, SettingsError> {
        match self {
            SettingValue::F64(value) => Ok(*value),
            other => Err(SettingsError::internal_type(other, "number")),
        }
    }

    /// Extract a `usize`, or raise if this is not an unsigned value.
    fn as_usize(&self) -> Result<usize, SettingsError> {
        match self {
            SettingValue::Usize(value) => Ok(*value),
            other => Err(SettingsError::internal_type(other, "unsigned integer")),
        }
    }

    /// Extract a `bool`, or raise if this is not a boolean value.
    fn as_bool(&self) -> Result<bool, SettingsError> {
        match self {
            SettingValue::Bool(value) => Ok(*value),
            other => Err(SettingsError::internal_type(other, "boolean")),
        }
    }
}

/// Parse a boolean SET value: true/false/on/off/yes/no/1/0 (case-insensitive).
fn parse_bool(raw: &str) -> Result<bool, SettingsError> {
    match raw.to_ascii_lowercase().as_str() {
        "true" | "on" | "yes" | "1" => Ok(true),
        "false" | "off" | "no" | "0" => Ok(false),
        _ => Err(SettingsError::BadValue {
            expected: "boolean",
            got: raw.to_string(),
        }),
    }
}

/// Parse a numeric SET value into any `FromStr` number, raising on a malformed
/// or out-of-range value (e.g. a negative for an unsigned setting).
fn parse_number<T: std::str::FromStr>(
    raw: &str,
    expected: &'static str,
) -> Result<T, SettingsError> {
    raw.parse::<T>().map_err(|_| SettingsError::BadValue {
        expected,
        got: raw.to_string(),
    })
}

/// Cast a `u64` SET value down to the config field's `u32` width, raising if it
/// does not fit rather than wrapping to a wrong value.
fn to_u32(value: u64) -> Result<u32, SettingsError> {
    u32::try_from(value).map_err(|_| SettingsError::BadValue {
        expected: "unsigned integer below 2^32",
        got: value.to_string(),
    })
}

/// Apply a type-checked value to the config (the write half of a session-mutable
/// setting). `None` on a static setting.
type ApplyFn = fn(&mut Config, &SettingValue) -> Result<(), SettingsError>;

/// One tunable in the registry.
struct SettingDef {
    /// Dotted name, e.g. `optimizer.planning_budget_ms`.
    name: &'static str,
    /// What the setting controls, as it behaves today.
    description: &'static str,
    /// Whether `SET`/`RESET` apply (session-mutable) or raise (static).
    mutability: Mutability,
    /// The environment variable that sets a static env flag, if any; used to
    /// report `source = env` and, for statics, to read the current value.
    env_var: Option<&'static str>,
    /// The compile-time default value.
    default: fn() -> SettingValue,
    /// Read the current value off a config snapshot (statics ignore it and read
    /// the env/const at their consumption site).
    read: fn(&Config) -> SettingValue,
    /// Apply a (type-checked) new value to the config; `None` for a static.
    apply: Option<ApplyFn>,
}

/// The single settings inventory, grouped by area. Every `OptimizerConfig`,
/// `CostConfig`, and `ExecutorConfig` field appears here (asserted by the
/// completeness test); the env flags and exec constants complete the sweep.
static SETTINGS: &[SettingDef] = &[
    // --- optimizer: rule gates, join-reorder bound, planning budget ---------
    SettingDef {
        name: "optimizer.enable_predicate_pushdown",
        description: "Push WHERE predicates toward the scans / into single-source subtrees.",
        mutability: Mutability::SessionMutable,
        env_var: None,
        default: || SettingValue::Bool(OptimizerConfig::default().enable_predicate_pushdown),
        read: |c| SettingValue::Bool(c.optimizer.enable_predicate_pushdown),
        apply: Some(|c, v| {
            c.optimizer.enable_predicate_pushdown = v.as_bool()?;
            Ok(())
        }),
    },
    SettingDef {
        name: "optimizer.enable_projection_pushdown",
        description: "Prune unreferenced columns down to the scans.",
        mutability: Mutability::SessionMutable,
        env_var: None,
        default: || SettingValue::Bool(OptimizerConfig::default().enable_projection_pushdown),
        read: |c| SettingValue::Bool(c.optimizer.enable_projection_pushdown),
        apply: Some(|c, v| {
            c.optimizer.enable_projection_pushdown = v.as_bool()?;
            Ok(())
        }),
    },
    SettingDef {
        name: "optimizer.enable_join_reordering",
        description: "Run the cost-based join reorderer (Selinger DP / GOO greedy).",
        mutability: Mutability::SessionMutable,
        env_var: None,
        default: || SettingValue::Bool(OptimizerConfig::default().enable_join_reordering),
        read: |c| SettingValue::Bool(c.optimizer.enable_join_reordering),
        apply: Some(|c, v| {
            c.optimizer.enable_join_reordering = v.as_bool()?;
            Ok(())
        }),
    },
    SettingDef {
        name: "optimizer.enable_decorrelation",
        description: "Configured decorrelation gate. The decorrelation pass runs \
                      unconditionally, so this flag currently has no effect.",
        mutability: Mutability::SessionMutable,
        env_var: None,
        default: || SettingValue::Bool(OptimizerConfig::default().enable_decorrelation),
        read: |c| SettingValue::Bool(c.optimizer.enable_decorrelation),
        apply: Some(|c, v| {
            c.optimizer.enable_decorrelation = v.as_bool()?;
            Ok(())
        }),
    },
    SettingDef {
        name: "optimizer.max_join_reorder_size",
        description: "Use the Selinger DP for at most this many tables; GOO greedy above it.",
        mutability: Mutability::SessionMutable,
        env_var: None,
        default: || SettingValue::U64(u64::from(OptimizerConfig::default().max_join_reorder_size)),
        read: |c| SettingValue::U64(u64::from(c.optimizer.max_join_reorder_size)),
        apply: Some(|c, v| {
            c.optimizer.max_join_reorder_size = to_u32(v.as_u64()?)?;
            Ok(())
        }),
    },
    SettingDef {
        name: "optimizer.planning_budget_ms",
        description: "Hard wall-clock budget for planning one query; a blown budget \
                      kills the plan (planning is O(metadata) by design).",
        mutability: Mutability::SessionMutable,
        env_var: None,
        default: || SettingValue::U64(OptimizerConfig::default().planning_budget_ms),
        read: |c| SettingValue::U64(c.optimizer.planning_budget_ms),
        apply: Some(|c, v| {
            c.optimizer.planning_budget_ms = v.as_u64()?;
            Ok(())
        }),
    },
    SettingDef {
        name: "optimizer.ship_local_floor",
        description: "Dim shipping: the fact (local) side must clear this many rows \
                      before a dimension ships into it.",
        mutability: Mutability::SessionMutable,
        env_var: None,
        default: || SettingValue::U64(OptimizerConfig::default().ship_local_floor),
        read: |c| SettingValue::U64(c.optimizer.ship_local_floor),
        apply: Some(|c, v| {
            c.optimizer.ship_local_floor = v.as_u64()?;
            Ok(())
        }),
    },
    SettingDef {
        name: "optimizer.ship_row_budget",
        description: "Dim shipping: never ship more than this many foreign rows into a source.",
        mutability: Mutability::SessionMutable,
        env_var: None,
        default: || SettingValue::U64(OptimizerConfig::default().ship_row_budget),
        read: |c| SettingValue::U64(c.optimizer.ship_row_budget),
        apply: Some(|c, v| {
            c.optimizer.ship_row_budget = v.as_u64()?;
            Ok(())
        }),
    },
    SettingDef {
        name: "optimizer.ship_min_ratio",
        description: "Dim shipping: the local (fact) side must exceed the shipped \
                      foreign side by at least this factor.",
        mutability: Mutability::SessionMutable,
        env_var: None,
        default: || SettingValue::U64(OptimizerConfig::default().ship_min_ratio),
        read: |c| SettingValue::U64(c.optimizer.ship_min_ratio),
        apply: Some(|c, v| {
            c.optimizer.ship_min_ratio = v.as_u64()?;
            Ok(())
        }),
    },
    SettingDef {
        name: "optimizer.ship_high_card_ndv",
        description: "Dim shipping: a group-key dimension with NDV at or above this \
                      is high-cardinality.",
        mutability: Mutability::SessionMutable,
        env_var: None,
        default: || SettingValue::I64(OptimizerConfig::default().ship_high_card_ndv),
        read: |c| SettingValue::I64(c.optimizer.ship_high_card_ndv),
        apply: Some(|c, v| {
            c.optimizer.ship_high_card_ndv = v.as_i64()?;
            Ok(())
        }),
    },
    SettingDef {
        name: "optimizer.ship_collapse_max_fraction",
        description: "Dim shipping: a ship-target aggregate keeping more than this \
                      fraction of its estimated input rows does not collapse.",
        mutability: Mutability::SessionMutable,
        env_var: None,
        default: || SettingValue::F64(OptimizerConfig::default().ship_collapse_max_fraction),
        read: |c| SettingValue::F64(c.optimizer.ship_collapse_max_fraction),
        apply: Some(|c, v| {
            c.optimizer.ship_collapse_max_fraction = v.as_f64()?;
            Ok(())
        }),
    },
    // --- cost model coefficients --------------------------------------------
    SettingDef {
        name: "cost.cpu_tuple_cost",
        description: "Cost model: per-tuple CPU cost.",
        mutability: Mutability::SessionMutable,
        env_var: None,
        default: || SettingValue::F64(CostConfig::default().cpu_tuple_cost),
        read: |c| SettingValue::F64(c.cost.cpu_tuple_cost),
        apply: Some(|c, v| {
            c.cost.cpu_tuple_cost = v.as_f64()?;
            Ok(())
        }),
    },
    SettingDef {
        name: "cost.io_page_cost",
        description: "Cost model: per-page I/O cost.",
        mutability: Mutability::SessionMutable,
        env_var: None,
        default: || SettingValue::F64(CostConfig::default().io_page_cost),
        read: |c| SettingValue::F64(c.cost.io_page_cost),
        apply: Some(|c, v| {
            c.cost.io_page_cost = v.as_f64()?;
            Ok(())
        }),
    },
    SettingDef {
        name: "cost.network_byte_cost",
        description: "Cost model: per-byte network transfer cost.",
        mutability: Mutability::SessionMutable,
        env_var: None,
        default: || SettingValue::F64(CostConfig::default().network_byte_cost),
        read: |c| SettingValue::F64(c.cost.network_byte_cost),
        apply: Some(|c, v| {
            c.cost.network_byte_cost = v.as_f64()?;
            Ok(())
        }),
    },
    SettingDef {
        name: "cost.network_rtt_ms",
        description: "Cost model: per-request network round-trip latency in milliseconds.",
        mutability: Mutability::SessionMutable,
        env_var: None,
        default: || SettingValue::F64(CostConfig::default().network_rtt_ms),
        read: |c| SettingValue::F64(c.cost.network_rtt_ms),
        apply: Some(|c, v| {
            c.cost.network_rtt_ms = v.as_f64()?;
            Ok(())
        }),
    },
    // --- executor config (parsed from YAML; the Rust executor does not read
    //     these today, so changing one is safe but affects no behavior) -------
    SettingDef {
        name: "executor.max_memory_mb",
        description: "Executor memory budget in MB. Parsed from config; the Rust \
                      executor does not read it today.",
        mutability: Mutability::SessionMutable,
        env_var: None,
        default: || SettingValue::U64(ExecutorConfig::default().max_memory_mb),
        read: |c| SettingValue::U64(c.executor.max_memory_mb),
        apply: Some(|c, v| {
            c.executor.max_memory_mb = v.as_u64()?;
            Ok(())
        }),
    },
    SettingDef {
        name: "executor.batch_size",
        description: "Executor batch row count. Parsed from config; the Rust \
                      executor does not read it today.",
        mutability: Mutability::SessionMutable,
        env_var: None,
        default: || SettingValue::U64(ExecutorConfig::default().batch_size),
        read: |c| SettingValue::U64(c.executor.batch_size),
        apply: Some(|c, v| {
            c.executor.batch_size = v.as_u64()?;
            Ok(())
        }),
    },
    SettingDef {
        name: "executor.max_threads",
        description: "Executor worker-thread count. Parsed from config; the Rust \
                      executor does not read it today.",
        mutability: Mutability::SessionMutable,
        env_var: None,
        default: || SettingValue::U64(u64::from(ExecutorConfig::default().max_threads)),
        read: |c| SettingValue::U64(u64::from(c.executor.max_threads)),
        apply: Some(|c, v| {
            c.executor.max_threads = to_u32(v.as_u64()?)?;
            Ok(())
        }),
    },
    SettingDef {
        name: "executor.enable_parallel_fetch",
        description: "Executor parallel-fetch flag. Parsed from config; the Rust \
                      executor does not read it today.",
        mutability: Mutability::SessionMutable,
        env_var: None,
        default: || SettingValue::Bool(ExecutorConfig::default().enable_parallel_fetch),
        read: |c| SettingValue::Bool(c.executor.enable_parallel_fetch),
        apply: Some(|c, v| {
            c.executor.enable_parallel_fetch = v.as_bool()?;
            Ok(())
        }),
    },
    // --- accelerator: materialized-view substitution ------------------------
    SettingDef {
        name: "accelerator.enable_substitution",
        description: "Automatically read a materialized view's chunks in place of \
                      recomputing a query subtree that matches the view's definition \
                      (cost-gated). Off restores the exact non-accelerated plan.",
        mutability: Mutability::SessionMutable,
        env_var: None,
        default: || SettingValue::Bool(AcceleratorConfig::default().enable_substitution),
        read: |c| SettingValue::Bool(c.accelerator.enable_substitution),
        apply: Some(|c, v| {
            c.accelerator.enable_substitution = v.as_bool()?;
            Ok(())
        }),
    },
    // --- catalog: runtime datasource DDL ------------------------------------
    SettingDef {
        name: "catalog.create_connect_timeout_ms",
        description: "CREATE DATASOURCE: cap on the connect + metadata load before it \
                      raises, so an unreachable host fails promptly (DDL I/O is exempt \
                      from the O(metadata) planning budget).",
        mutability: Mutability::SessionMutable,
        env_var: None,
        default: || SettingValue::U64(CatalogConfig::default().create_connect_timeout_ms),
        read: |c| SettingValue::U64(c.catalog.create_connect_timeout_ms),
        apply: Some(|c, v| {
            c.catalog.create_connect_timeout_ms = v.as_u64()?;
            Ok(())
        }),
    },
    // --- server: read at wire-server startup, so static here ----------------
    SettingDef {
        name: "server.users",
        description: "Wire-server auth: the SCRAM user list (empty = trust auth). \
                      Read at server startup; not changeable on a live runtime.",
        mutability: Mutability::Static,
        env_var: None,
        default: || SettingValue::Text(server_users_summary(&[])),
        read: |c| SettingValue::Text(server_users_summary(&c.server.users)),
        apply: None,
    },
    SettingDef {
        name: "server.scram_iterations",
        description: "Wire-server auth: the PBKDF2 work factor NEW SCRAM verifiers are \
                      derived with (CREATE/ALTER USER, hash-password). Existing verifiers \
                      carry their own count. Read at startup; the RFC 7677 floor is 4096.",
        mutability: Mutability::Static,
        env_var: None,
        default: || SettingValue::U64(u64::from(fq_common::SCRAM_ITERATIONS)),
        read: |c| SettingValue::U64(u64::from(c.server.scram_iterations)),
        apply: None,
    },
    // --- environment kill switches / trace flags: process-global, static ----
    SettingDef {
        name: "env.dim_shipping",
        description: "Dim-shipping master switch (env FEDQ_DIM_SHIPPING). Set to 0 to \
                      disable shipping process-wide; on otherwise.",
        mutability: Mutability::Static,
        env_var: Some("FEDQ_DIM_SHIPPING"),
        default: || SettingValue::Bool(true),
        read: |_| SettingValue::Bool(env_on_default_true("FEDQ_DIM_SHIPPING")),
        apply: None,
    },
    SettingDef {
        name: "env.stats_probe",
        description: "Statless-table stats probing (env FEDQ_STATS_PROBE). Set to 0 to \
                      disable probing; on otherwise.",
        mutability: Mutability::Static,
        env_var: Some("FEDQ_STATS_PROBE"),
        default: || SettingValue::Bool(true),
        read: |_| SettingValue::Bool(env_on_default_true("FEDQ_STATS_PROBE")),
        apply: None,
    },
    SettingDef {
        name: "env.eager_aggregation",
        description: "Eager-aggregation rewrite (env FEDQ_EAGER_AGG). Set to 0 to \
                      disable the rewrite; on otherwise.",
        mutability: Mutability::Static,
        env_var: Some("FEDQ_EAGER_AGG"),
        default: || SettingValue::Bool(true),
        read: |_| SettingValue::Bool(env_on_default_true("FEDQ_EAGER_AGG")),
        apply: None,
    },
    SettingDef {
        name: "env.parallel_steps",
        description: "Cross-step parallel reads (env FEDQRS_PARALLEL_STEPS). Set to 0 to \
                      restore the fully sequential step loop; on otherwise.",
        mutability: Mutability::Static,
        env_var: Some("FEDQRS_PARALLEL_STEPS"),
        default: || SettingValue::Bool(true),
        read: |_| SettingValue::Bool(env_on_default_true("FEDQRS_PARALLEL_STEPS")),
        apply: None,
    },
    SettingDef {
        name: "env.profile",
        description: "Per-step timing to stderr (env FEDQRS_PROFILE). Read once at first \
                      execution; off unless set.",
        mutability: Mutability::Static,
        env_var: Some("FEDQRS_PROFILE"),
        default: || SettingValue::Bool(false),
        read: |_| SettingValue::Bool(env_on_default_false("FEDQRS_PROFILE")),
        apply: None,
    },
    SettingDef {
        name: "env.trace_sql",
        description: "Trace every SQL string pushed to a source to stderr (env \
                      FEDQRS_TRACE_SQL). Read once at first push; off unless set.",
        mutability: Mutability::Static,
        env_var: Some("FEDQRS_TRACE_SQL"),
        default: || SettingValue::Bool(false),
        read: |_| SettingValue::Bool(env_on_default_false("FEDQRS_TRACE_SQL")),
        apply: None,
    },
    // --- execution-engine capacity constants: compile-time, static ----------
    SettingDef {
        name: "exec.parallel_partitions",
        description: "Partition count for ctid-parallel / partitioned reads (compile-time).",
        mutability: Mutability::Static,
        env_var: None,
        default: || SettingValue::Usize(fq_exec::engine::PARALLEL_PARTITIONS),
        read: |_| SettingValue::Usize(fq_exec::engine::PARALLEL_PARTITIONS),
        apply: None,
    },
    SettingDef {
        name: "exec.operator_memory_limit_bytes",
        description: "Shared DataFusion operator memory pool cap in bytes (compile-time).",
        mutability: Mutability::Static,
        env_var: None,
        default: || SettingValue::Usize(fq_exec::engine::MEMORY_LIMIT_BYTES),
        read: |_| SettingValue::Usize(fq_exec::engine::MEMORY_LIMIT_BYTES),
        apply: None,
    },
    SettingDef {
        name: "exec.region_spill_join_input_bytes",
        description: "Region input-byte threshold above which joins run spillable \
                      sort-merge from the start (compile-time).",
        mutability: Mutability::Static,
        env_var: None,
        default: || SettingValue::Usize(fq_exec::engine::REGION_SPILL_JOIN_INPUT_BYTES),
        read: |_| SettingValue::Usize(fq_exec::engine::REGION_SPILL_JOIN_INPUT_BYTES),
        apply: None,
    },
    SettingDef {
        name: "exec.resident_bindings_budget_bytes",
        description: "Total resident-binding bytes before the largest bindings spill \
                      to disk (compile-time).",
        mutability: Mutability::Static,
        env_var: None,
        default: || SettingValue::Usize(fq_exec::engine::RESIDENT_BINDINGS_BUDGET),
        read: |_| SettingValue::Usize(fq_exec::engine::RESIDENT_BINDINGS_BUDGET),
        apply: None,
    },
    SettingDef {
        name: "exec.in_list_inline_cap",
        description: "Distinct-key ceiling below which a dynamic filter inlines an IN \
                      list instead of a temp table (compile-time).",
        mutability: Mutability::Static,
        env_var: None,
        default: || SettingValue::Usize(fq_exec::engine::IN_CAP),
        read: |_| SettingValue::Usize(fq_exec::engine::IN_CAP),
        apply: None,
    },
    SettingDef {
        name: "exec.duck_temp_ingest_cap",
        description: "DuckDB key-ingest ceiling past which reading the probe whole \
                      beats appending keys to a temp table (compile-time).",
        mutability: Mutability::Static,
        env_var: None,
        default: || SettingValue::Usize(fq_exec::engine::DUCK_TEMP_CAP),
        read: |_| SettingValue::Usize(fq_exec::engine::DUCK_TEMP_CAP),
        apply: None,
    },
    // --- events: the event-analytics store's build and query budgets --------
    SettingDef {
        name: "events.build_memory_bytes",
        description: "Peak memory budget of an event dataset build; shard sizing and \
                      spill buffers derive from it, so peak RSS is independent of \
                      total row count.",
        mutability: Mutability::SessionMutable,
        env_var: None,
        default: || SettingValue::U64(EventsConfig::default().build_memory_bytes),
        read: |c| SettingValue::U64(c.events.build_memory_bytes),
        apply: Some(|c, v| {
            c.events.build_memory_bytes = v.as_u64()?;
            Ok(())
        }),
    },
    SettingDef {
        name: "events.build_threads",
        description: "Event build worker threads; 0 uses every core.",
        mutability: Mutability::SessionMutable,
        env_var: None,
        default: || SettingValue::Usize(EventsConfig::default().build_threads),
        read: |c| SettingValue::Usize(c.events.build_threads),
        apply: Some(|c, v| {
            c.events.build_threads = v.as_usize()?;
            Ok(())
        }),
    },
    SettingDef {
        name: "events.target_shard_bytes",
        description: "Raw columnar bytes targeted per shard when deriving an event \
                      dataset's shard count (fixed at CREATE for the dataset's life).",
        mutability: Mutability::Static,
        env_var: None,
        default: || SettingValue::U64(EventsConfig::default().target_shard_bytes),
        read: |c| SettingValue::U64(c.events.target_shard_bytes),
        apply: None,
    },
    SettingDef {
        name: "events.dict_max_bytes",
        description: "Per-property build-dictionary budget; a property whose value map \
                      exceeds it is promoted to raw-string encoding.",
        mutability: Mutability::SessionMutable,
        env_var: None,
        default: || SettingValue::U64(EventsConfig::default().dict_max_bytes),
        read: |c| SettingValue::U64(c.events.dict_max_bytes),
        apply: Some(|c, v| {
            c.events.dict_max_bytes = v.as_u64()?;
            Ok(())
        }),
    },
    SettingDef {
        name: "events.cache_bytes",
        description: "Decoded-block cache budget for warm event analyses.",
        mutability: Mutability::SessionMutable,
        env_var: None,
        default: || SettingValue::U64(EventsConfig::default().cache_bytes),
        read: |c| SettingValue::U64(c.events.cache_bytes),
        apply: Some(|c, v| {
            c.events.cache_bytes = v.as_u64()?;
            Ok(())
        }),
    },
    SettingDef {
        name: "events.query_memory_bytes",
        description: "Per-query group/partial memory budget of the event analyses.",
        mutability: Mutability::SessionMutable,
        env_var: None,
        default: || SettingValue::U64(EventsConfig::default().query_memory_bytes),
        read: |c| SettingValue::U64(c.events.query_memory_bytes),
        apply: Some(|c, v| {
            c.events.query_memory_bytes = v.as_u64()?;
            Ok(())
        }),
    },
    SettingDef {
        name: "events.max_generations",
        description: "Generation count per shard past which the next refresh compacts \
                      it.",
        mutability: Mutability::SessionMutable,
        env_var: None,
        default: || SettingValue::U64(EventsConfig::default().max_generations),
        read: |c| SettingValue::U64(c.events.max_generations),
        apply: Some(|c, v| {
            c.events.max_generations = v.as_u64()?;
            Ok(())
        }),
    },
    SettingDef {
        name: "events.allow_unknown_names",
        description: "Whether an unknown event NAME in an analysis statement matches \
                      nothing instead of raising the unknown-name error.",
        mutability: Mutability::SessionMutable,
        env_var: None,
        default: || SettingValue::Bool(EventsConfig::default().allow_unknown_names),
        read: |c| SettingValue::Bool(c.events.allow_unknown_names),
        apply: Some(|c, v| {
            c.events.allow_unknown_names = v.as_bool()?;
            Ok(())
        }),
    },
];

/// Whether an env kill switch is on: only the exact string `"0"` turns it off;
/// unset or anything else is on. Mirrors the consumption-site reads.
fn env_on_default_true(var: &str) -> bool {
    std::env::var(var).map_or(true, |value| value != "0")
}

/// Whether an env trace flag is on: on for any non-empty value other than `"0"`;
/// unset is off. Mirrors the consumption-site reads.
fn env_on_default_false(var: &str) -> bool {
    std::env::var(var).is_ok_and(|value| value != "0" && !value.is_empty())
}

/// A one-line summary of the server credential list for the `server.users`
/// value: the count and auth mode, never the credentials themselves.
fn server_users_summary(users: &[fq_common::UserCredential]) -> String {
    if users.is_empty() {
        return "trust (no users)".to_string();
    }
    format!("{} user(s) (SCRAM-SHA-256)", users.len())
}

/// Every failure the settings surface raises.
#[derive(Debug)]
pub enum SettingsError {
    /// No setting has this name; carries the nearest registered name.
    Unknown { name: String, suggestion: String },
    /// A `SET`/`RESET` targeted a static setting.
    Static { name: String },
    /// A `SET` value did not parse into the setting's type.
    BadValue { expected: &'static str, got: String },
    /// A value already parsed to the wrong variant reached an `apply` - a
    /// registry wiring bug, surfaced loudly rather than silently coerced.
    InternalType {
        have: &'static str,
        want: &'static str,
    },
}

impl SettingsError {
    /// Build an internal-type error from a value that reached the wrong `apply`.
    fn internal_type(have: &SettingValue, want: &'static str) -> Self {
        SettingsError::InternalType {
            have: have.type_name(),
            want,
        }
    }

    /// Render the error for the runtime's message.
    fn message(&self) -> String {
        match self {
            SettingsError::Unknown { name, suggestion } => format!(
                "unknown setting '{name}'; did you mean '{suggestion}'? \
                 run SHOW SETTINGS to list every setting"
            ),
            SettingsError::Static { name } => format!(
                "setting '{name}' is static: it is fixed for the life of the process \
                 (an exec constant, an environment flag, or a value baked in at \
                 startup), so it cannot be changed on a live runtime"
            ),
            SettingsError::BadValue { expected, got } => {
                format!("expected {expected} for this setting, got '{got}'")
            }
            SettingsError::InternalType { have, want } => {
                format!("internal settings type mismatch: have {have}, want {want}")
            }
        }
    }
}

impl From<SettingsError> for RuntimeError {
    /// Surface a settings failure as a runtime error (an invalid SET/RESET/SHOW
    /// must raise, never silently no-op).
    fn from(error: SettingsError) -> Self {
        RuntimeError::Settings(error.message())
    }
}

/// Find a setting by exact name.
fn find(name: &str) -> Option<&'static SettingDef> {
    SETTINGS.iter().find(|def| def.name == name)
}

/// The registered name closest to `name` by edit distance, for an unknown-name
/// suggestion.
fn nearest(name: &str) -> String {
    let mut best = SETTINGS[0].name;
    let mut best_distance = usize::MAX;
    for def in SETTINGS {
        let distance = edit_distance(name, def.name);
        if distance < best_distance {
            best_distance = distance;
            best = def.name;
        }
    }
    best.to_string()
}

/// Levenshtein edit distance between two strings, for the nearest-name hint.
fn edit_distance(a: &str, b: &str) -> usize {
    let b_chars: Vec<char> = b.chars().collect();
    let mut previous: Vec<usize> = (0..=b_chars.len()).collect();
    for (i, a_char) in a.chars().enumerate() {
        let mut current = vec![i + 1];
        for (j, b_char) in b_chars.iter().enumerate() {
            let cost = usize::from(a_char != *b_char);
            let deletion = previous[j + 1] + 1;
            let insertion = current[j] + 1;
            let substitution = previous[j] + cost;
            current.push(deletion.min(insertion).min(substitution));
        }
        previous = current;
    }
    previous[b_chars.len()]
}

/// The ordered result column names every `SHOW SETTINGS` / `SHOW SETTING`
/// returns. The single source both `show_schema` and `describe_columns` build on,
/// so the executed result and its pre-execution description never diverge.
const SHOW_COLUMNS: [&str; 6] = [
    "name",
    "value",
    "default",
    "source",
    "mutable",
    "description",
];

/// The result schema every `SHOW SETTINGS` / `SHOW SETTING` returns: six text
/// columns (name, value, default, source, mutable, description).
pub fn show_schema() -> SchemaRef {
    let mut fields = Vec::with_capacity(SHOW_COLUMNS.len());
    for name in SHOW_COLUMNS {
        fields.push(Field::new(name, ArrowDataType::Utf8, false));
    }
    Arc::new(Schema::new(fields))
}

/// The (column name, engine type) pairs `describe` reports for a SHOW statement,
/// matching `show_schema` positionally (every column is text).
pub fn describe_columns() -> Vec<(String, DataType)> {
    let mut columns = Vec::with_capacity(SHOW_COLUMNS.len());
    for name in SHOW_COLUMNS {
        columns.push((name.to_string(), DataType::Text));
    }
    columns
}

impl Runtime {
    /// `SHOW SETTINGS`: one row per registered setting, in registry order.
    pub(crate) fn show_settings(&self) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let config = self.config_snapshot();
        let overrides = self.settings_overrides();
        build_show(SETTINGS.iter(), &config, &overrides)
    }

    /// `SHOW SETTING <name>`: the one named setting, or an unknown-name raise.
    pub(crate) fn show_setting(
        &self,
        name: &str,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let def = find(name).ok_or_else(|| SettingsError::Unknown {
            name: name.to_string(),
            suggestion: nearest(name),
        })?;
        let config = self.config_snapshot();
        let overrides = self.settings_overrides();
        build_show(std::iter::once(def), &config, &overrides)
    }

    /// `SET <name> = <value>`: type-check and apply one session-mutable setting to
    /// this runtime's live config, then record it as a SET override. Raises on an
    /// unknown name, a static setting, or a value that does not parse.
    pub(crate) fn set_setting(
        &self,
        name: &str,
        raw_value: &str,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let def = find(name).ok_or_else(|| SettingsError::Unknown {
            name: name.to_string(),
            suggestion: nearest(name),
        })?;
        let apply = def.apply.ok_or_else(|| SettingsError::Static {
            name: name.to_string(),
        })?;
        let value = (def.default)()
            .parse_like(raw_value)
            .map_err(RuntimeError::from)?;
        {
            let mut config = self.config.write().expect("config lock poisoned");
            apply(&mut config, &value)?;
        }
        self.settings_overrides_mut().insert(name.to_string());
        // A store-backed runtime persists the override, so the setting
        // survives restart (a new runtime re-applies it at construction); a
        // path-less runtime has no store and the SET stays session-scoped.
        if let Some(accel) = &self.accelerator {
            accel.upsert_setting(name, raw_value)?;
        }
        status_result("SET")
    }

    /// Apply one PERSISTED setting override onto a config under assembly,
    /// with the same validation `SET` runs: an unknown name, a static
    /// setting, or an unparsable value raises - a broken persisted row fails
    /// construction loudly, never gets skipped.
    pub(crate) fn apply_persisted_setting(
        config: &mut Config,
        name: &str,
        raw_value: &str,
    ) -> Result<(), RuntimeError> {
        let def = find(name).ok_or_else(|| SettingsError::Unknown {
            name: name.to_string(),
            suggestion: nearest(name),
        })?;
        let apply = def.apply.ok_or_else(|| SettingsError::Static {
            name: name.to_string(),
        })?;
        let value = (def.default)()
            .parse_like(raw_value)
            .map_err(RuntimeError::from)?;
        apply(config, &value)?;
        Ok(())
    }

    /// `RESET <name>` restores one setting to its default; `RESET ALL` restores
    /// every session override. Raises on an unknown name or a static setting.
    pub(crate) fn reset_setting(
        &self,
        name: Option<&str>,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        match name {
            None => self.reset_all(),
            Some(name) => self.reset_one(name),
        }
    }

    /// Restore one named setting to its compile-time default.
    fn reset_one(&self, name: &str) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let def = find(name).ok_or_else(|| SettingsError::Unknown {
            name: name.to_string(),
            suggestion: nearest(name),
        })?;
        let apply = def.apply.ok_or_else(|| SettingsError::Static {
            name: name.to_string(),
        })?;
        {
            let mut config = self.config.write().expect("config lock poisoned");
            apply(&mut config, &(def.default)())?;
        }
        self.settings_overrides_mut().remove(name);
        if let Some(accel) = &self.accelerator {
            accel.delete_setting(name)?;
        }
        status_result("RESET")
    }

    /// Restore every session-mutable setting that was SET back to its default.
    fn reset_all(&self) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let names: Vec<String> = self.settings_overrides().iter().cloned().collect();
        for name in &names {
            self.reset_one(name)?;
        }
        if let Some(accel) = &self.accelerator {
            accel.clear_settings()?;
        }
        status_result("RESET")
    }
}

/// Build a SHOW result table over the given settings, computing each row's
/// current value and source against one config snapshot and override set.
fn build_show<'a>(
    defs: impl Iterator<Item = &'a SettingDef>,
    config: &Config,
    overrides: &BTreeSet<String>,
) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
    let mut rows = SettingRows::default();
    for def in defs {
        rows.push(def, config, overrides);
    }
    let schema = show_schema();
    let batch = rows.into_batch(Arc::clone(&schema))?;
    Ok((schema, vec![batch]))
}

/// The six string columns of a SHOW result, filled row by row.
#[derive(Default)]
struct SettingRows {
    names: Vec<String>,
    values: Vec<String>,
    defaults: Vec<String>,
    sources: Vec<String>,
    mutables: Vec<String>,
    descriptions: Vec<String>,
}

impl SettingRows {
    /// Append one setting's row (name, current value, default, source, mutable,
    /// description) computed against the config snapshot and override set.
    fn push(&mut self, def: &SettingDef, config: &Config, overrides: &BTreeSet<String>) {
        self.names.push(def.name.to_string());
        self.values.push((def.read)(config).render());
        self.defaults.push((def.default)().render());
        self.sources
            .push(source_of(def, config, overrides).label().to_string());
        self.mutables.push(def.mutability.label().to_string());
        self.descriptions.push(def.description.to_string());
    }

    /// Assemble the six columns into one record batch.
    fn into_batch(self, schema: SchemaRef) -> Result<RecordBatch, RuntimeError> {
        let columns: Vec<Arc<dyn arrow::array::Array>> = vec![
            Arc::new(StringArray::from(self.names)),
            Arc::new(StringArray::from(self.values)),
            Arc::new(StringArray::from(self.defaults)),
            Arc::new(StringArray::from(self.sources)),
            Arc::new(StringArray::from(self.mutables)),
            Arc::new(StringArray::from(self.descriptions)),
        ];
        RecordBatch::try_new(schema, columns)
            .map_err(|error| RuntimeError::ResultShape(error.to_string()))
    }
}

/// Where a setting's current value comes from: a SET override wins, then an env
/// flag, then a non-default config-file value, else the compile default.
fn source_of(def: &SettingDef, config: &Config, overrides: &BTreeSet<String>) -> Source {
    if overrides.contains(def.name) {
        return Source::Set;
    }
    if def.env_var.is_some_and(|var| std::env::var(var).is_ok()) {
        return Source::Env;
    }
    if (def.read)(config) != (def.default)() {
        return Source::ConfigFile;
    }
    Source::Default
}

/// The one-row, one-column `status` table a SET/RESET returns (the pg
/// command-tag convention rendered as a result set).
fn status_result(tag: &str) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "status",
        ArrowDataType::Utf8,
        false,
    )]));
    let column = Arc::new(StringArray::from(vec![tag.to_string()]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![column])
        .map_err(|error| RuntimeError::ResultShape(error.to_string()))?;
    Ok((schema, vec![batch]))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every `OptimizerConfig` field is registered under `optimizer.` - a new
    /// field breaks this the moment it is added without a SettingDef.
    #[test]
    fn every_optimizer_field_is_registered() {
        let mapping =
            serde_yaml::to_value(OptimizerConfig::default()).expect("serialize optimizer");
        assert_fields_registered(&mapping, "optimizer");
    }

    /// Every `CostConfig` field is registered under `cost.`.
    #[test]
    fn every_cost_field_is_registered() {
        let mapping = serde_yaml::to_value(CostConfig::default()).expect("serialize cost");
        assert_fields_registered(&mapping, "cost");
    }

    /// Every `ExecutorConfig` field is registered under `executor.`.
    #[test]
    fn every_executor_field_is_registered() {
        let mapping = serde_yaml::to_value(ExecutorConfig::default()).expect("serialize executor");
        assert_fields_registered(&mapping, "executor");
    }

    /// Every `AcceleratorConfig` field is registered under `accelerator.`.
    #[test]
    fn every_accelerator_field_is_registered() {
        let mapping =
            serde_yaml::to_value(AcceleratorConfig::default()).expect("serialize accelerator");
        assert_fields_registered(&mapping, "accelerator");
    }

    /// Every `CatalogConfig` field is registered under `catalog.`.
    #[test]
    fn every_catalog_field_is_registered() {
        let mapping = serde_yaml::to_value(CatalogConfig::default()).expect("serialize catalog");
        assert_fields_registered(&mapping, "catalog");
    }

    /// Every `EventsConfig` field is registered under `events.`.
    #[test]
    fn every_events_field_is_registered() {
        let mapping = serde_yaml::to_value(EventsConfig::default()).expect("serialize events");
        assert_fields_registered(&mapping, "events");
    }

    /// Assert every key of a serialized config section appears in the registry
    /// under `<prefix>.<key>`.
    fn assert_fields_registered(mapping: &serde_yaml::Value, prefix: &str) {
        let mapping = mapping.as_mapping().expect("config section is a mapping");
        for (key, _) in mapping {
            let key = key.as_str().expect("config key is a string");
            let name = format!("{prefix}.{key}");
            assert!(
                find(&name).is_some(),
                "config field '{name}' is not registered in the settings registry"
            );
        }
    }

    /// The nearest-name hint points at the obvious typo target.
    #[test]
    fn nearest_name_suggests_the_close_match() {
        assert_eq!(
            nearest("optimizer.ship_local_flor"),
            "optimizer.ship_local_floor"
        );
    }

    /// A boolean SET value accepts the common spellings and rejects junk.
    #[test]
    fn bool_parsing_accepts_spellings() {
        assert!(parse_bool("on").unwrap());
        assert!(!parse_bool("FALSE").unwrap());
        assert!(parse_bool("maybe").is_err());
    }

    /// A negative value for an unsigned setting raises rather than wrapping.
    #[test]
    fn unsigned_rejects_negative() {
        let template = SettingValue::U64(0);
        assert!(template.parse_like("-1").is_err());
    }
}
