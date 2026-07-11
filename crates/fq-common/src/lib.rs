//! fq-common: the leaf crate of the engine - shared configuration, errors, and
//! logging. Ports `federated_query/model.py`, `config/config.py`,
//! `utils/logging.py`, and `parser/errors.py`.
//!
//! PORT NOTE - what retires here (never silent, per the plan's test strategy):
//! the Python `StateModel` base (`model.py`) and its loudness pins
//! (`test_state_model.py`: `extra="forbid"`, `model_copy` key validation, the
//! no-dataclass sweep) do NOT translate. Rust's typed structs plus serde
//! `#[serde(deny_unknown_fields)]` carry the same guarantee at compile/parse
//! time: an unknown or renamed field cannot be silently dropped. The loudness
//! is now owned by the type system and by serde, and is pinned by the
//! deny-unknown-field test in `tests/config.rs`.

pub mod config;
pub mod error;
pub mod logging;
pub mod types;
#[macro_use]
mod update;

pub use config::{
    load_config, Config, CostConfig, DataSourceConfig, ExecutorConfig, OptimizerConfig,
};
pub use error::{ConfigError, UnsupportedSqlError};
pub use logging::setup_logging;
pub use types::DataType;
