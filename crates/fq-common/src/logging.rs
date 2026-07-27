//! Logging setup.
//!
//! Ports the intent of `utils/logging.py`: a level plus an optional structured
//! (JSON) formatter, installed on the global `tracing` subscriber.
//!
//! The structured form is `tracing`'s own JSON shape, not the Python field
//! layout (`timestamp`/`level`/`logger`/...). There is no file sink and no
//! contextual adapter: a subscriber writes to stderr and the host redirects it.

use tracing_subscriber::EnvFilter;

/// Install the global tracing subscriber at `level` (e.g. "info", "debug"),
/// choosing the JSON formatter when `structured` is set. Idempotent: a second
/// call is a no-op rather than a panic, so calling it more than once per process
/// is safe (Python used `force=True` for the same reason).
///
/// Logs go to STDERR. stdout carries query results, and the CLI renders them as
/// CSV and JSON for piping; a log line landing in that stream would corrupt the
/// document a caller is parsing. The formatter's own default is stdout, so this
/// has to be said explicitly.
pub fn setup_logging(level: &str, structured: bool) {
    let filter =
        EnvFilter::try_new(level.to_lowercase()).unwrap_or_else(|_| EnvFilter::new("info"));
    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr);
    if structured {
        let _ = builder.json().try_init();
    } else {
        let _ = builder.try_init();
    }
}
