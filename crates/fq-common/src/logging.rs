//! Logging setup.
//!
//! Ports the intent of `utils/logging.py`: a level plus an optional structured
//! (JSON) formatter, installed on the global `tracing` subscriber.
//!
//! PORT NOTE: the exact JSON field shape (`timestamp`/`level`/`logger`/... keys),
//! the file sink, and the contextual `LoggerAdapter` are NOT ported - no test
//! pins them, and `tracing` supplies its own structured fields. They will be
//! ported (with tests) if and when a test asserts on the log output shape.

use tracing_subscriber::EnvFilter;

/// Install the global tracing subscriber at `level` (e.g. "info", "debug"),
/// choosing the JSON formatter when `structured` is set. Idempotent: a second
/// call is a no-op rather than a panic, so calling it more than once per process
/// is safe (Python used `force=True` for the same reason).
pub fn setup_logging(level: &str, structured: bool) {
    let filter =
        EnvFilter::try_new(level.to_lowercase()).unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    if structured {
        let _ = builder.json().try_init();
    } else {
        let _ = builder.try_init();
    }
}
