//! Error types shared across the engine's crate boundaries.
//!
//! Ports `parser/errors.py` (`UnsupportedSqlError`) and adds the loader error
//! for `config.rs`. Following the plan's error posture: each layer boundary has
//! its own `thiserror`-derived error, message text preserved where a test
//! asserts on it, and `?` propagates rather than catch-and-rewrap.

use thiserror::Error;

/// Failure loading and validating an engine YAML configuration.
///
/// Ports the loud checks in `config.py::load_config`: a missing file, an
/// unknown top-level section, and a malformed data-source block all raise here
/// rather than being silently ignored (which would drop the user's settings and
/// fall back to defaults).
#[derive(Debug, Error)]
pub enum ConfigError {
    /// The config path does not exist. Mirrors Python's `FileNotFoundError`.
    #[error("Config file not found: {0}")]
    FileNotFound(String),

    /// The YAML document root is not a mapping (e.g. an empty or scalar file).
    #[error("Config root must be a YAML mapping")]
    RootNotMapping,

    /// One or more top-level sections are not recognized. The message lists the
    /// offending names sorted, matching `config.py` byte-for-byte so the
    /// "optimizr" typo test keeps asserting on it.
    #[error("Unknown config section(s): {0}")]
    UnknownSection(String),

    /// A top-level section key was a non-string YAML value.
    #[error("Config section name must be a string")]
    NonStringSection,

    /// The `datasources` section is present but not a mapping.
    #[error("datasources section must be a mapping")]
    DatasourcesNotMapping,

    /// A single data-source block is not a mapping.
    #[error("datasource '{0}' must be a mapping")]
    DatasourceNotMapping(String),

    /// A data-source block is missing the required `type`. In Python this was a
    /// `KeyError` from `ds_config.pop("type")`; here it is a loud, named error.
    #[error("datasource '{0}' is missing required 'type'")]
    MissingDatasourceType(String),

    /// A data-source block has a non-string key.
    #[error("datasource '{0}' has a non-string key")]
    NonStringDatasourceKey(String),

    /// A data-source `type` value was not a string.
    #[error("datasource '{0}' 'type' must be a string")]
    NonStringType(String),

    /// A data-source `capabilities` value was not a list of strings.
    #[error("datasource '{0}' 'capabilities' must be a list of strings")]
    BadCapabilities(String),

    /// YAML parse failure (also covers `deny_unknown_fields` rejections in the
    /// optimizer/executor/cost sections - the analogue of `extra="forbid"`).
    #[error(transparent)]
    Yaml(#[from] serde_yaml::Error),

    /// Filesystem read failure.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Raised when a SQL clause parses but the engine cannot plan it.
///
/// Ports `parser/errors.py::UnsupportedSQLError`. Failing fast keeps the engine
/// from silently dropping a clause and returning a wrong answer (a named WINDOW
/// clause, UNPIVOT, FETCH FIRST ... WITH TIES / PERCENT, TRY_CAST, BETWEEN
/// SYMMETRIC, ...). It is defined here (the crate that owns `parser/errors.py`)
/// and consumed by fq-parse.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
#[error("{0}")]
pub struct UnsupportedSqlError(pub String);
