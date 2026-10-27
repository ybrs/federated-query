//! fq-common: the leaf crate of the engine - shared configuration, errors, and
//! logging. Ports `federated_query/model.py`, `config/config.py`,
//! `utils/logging.py`, and `parser/errors.py`.
//!
//! PORT NOTE - what retires here (never silent,):
//! the Python `StateModel` base (`model.py`) and its loudness pins
//! (`test_state_model.py`: `extra="forbid"`, `model_copy` key validation, the
//! no-dataclass sweep) do NOT translate. Rust's typed structs plus serde
//! `#[serde(deny_unknown_fields)]` carry the same guarantee at compile/parse
//! time: an unknown or renamed field cannot be silently dropped. The loudness
//! is now owned by the type system and by serde, and is pinned by the
//! deny-unknown-field test in `tests/config.rs`.

pub mod budget;
pub mod config;
pub mod error;
pub mod events;
pub mod logging;
mod nested_types;
pub mod types;
#[macro_use]
mod update;

/// The stack size for any thread that PLANS or EXECUTES queries. The plan
/// passes (bind, decorrelate, optimize, physical, steps) recurse once per plan
/// level, so a join over n tables costs ~n frames per pass; on the 2MiB
/// default of a spawned thread, a 64-table star overflows in a debug build -
/// a SIGSEGV, not an error. Sixteen MiB carries several hundred join levels
/// in debug with margin. Depth beyond what this stack carries is refused at
/// bind time by [`MAX_PLAN_DEPTH`], so a query too deep to plan raises instead
/// of dying by stack overflow.
pub const QUERY_THREAD_STACK_BYTES: usize = 16 * 1024 * 1024;

/// The deepest chain of plan NODES the engine will plan. The binder measures
/// its input and its output and refuses anything deeper, because past this the
/// recursive passes are the ones that decide the outcome, and what they do is
/// overflow the stack - a SIGSEGV carrying no message.
///
/// Derived from [`QUERY_THREAD_STACK_BYTES`] by measurement, not arithmetic:
/// on a thread of that size, in a DEBUG build, a chain-join plan 402 levels
/// deep plans through the whole pipeline and one 427 levels deep overflows.
/// The limit is set at roughly half the measured-safe depth. The margin is
/// wide on purpose: frames-per-level differ by shape and by pass (a join level
/// costs far more than a projection level), a future pass can add frames, and
/// the failure this prevents is unrecoverable, while the cost of refusing a
/// query that would have planned is one loud, explicit error.
pub const MAX_PLAN_DEPTH: usize = 200;

pub use budget::PlanBudget;
pub use config::{
    load_config, AcceleratorConfig, CatalogConfig, ChangeKey, Config, CostConfig, DataSourceConfig,
    DiagnosticsConfig, ExecutorConfig, OptimizerConfig, Principal, ServerConfig, UserCredential,
    SCRAM_ITERATIONS,
};
pub use error::{ConfigError, UnsupportedSqlError};
pub use events::EventsConfig;
pub use logging::setup_logging;
pub use types::DataType;

/// JSON lookup paths shared by binding and native expression evaluation.
pub mod json_path;
