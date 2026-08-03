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
pub mod types;
#[macro_use]
mod update;

/// The stack size for any thread that PLANS or EXECUTES queries. The plan
/// passes (bind, decorrelate, optimize, physical, steps) recurse once per plan
/// level, so a join over n tables costs ~n frames per pass; on the 2MiB
/// default of a spawned thread, a 64-table star overflows in a debug build -
/// a SIGSEGV, not an error. Sixteen MiB carries several hundred join levels
/// in debug with margin. Query depth beyond what this stack carries dies by
/// stack overflow: there is no plan-depth guard, so no loud error precedes it.
pub const QUERY_THREAD_STACK_BYTES: usize = 16 * 1024 * 1024;

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
