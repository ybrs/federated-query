//! The per-connection execution bridge.
//!
//! A `Session` owns one OS thread that builds a `Runtime` from the config and
//! runs that connection's queries on it in order. The async handler talks to the
//! thread over channels: it sends the SQL and awaits the Arrow result. Keeping
//! every `execute` for a connection pinned to one thread is what makes the
//! fq-exec thread-local data-plane caches coherent (see the crate module doc).

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use fq_common::Config;
use fq_runtime::Runtime;
use tokio::sync::{mpsc, oneshot};

/// A materialized engine result: the output schema (user-visible column names)
/// and every batch of rows. Both halves are `Send`, so they cross back from the
/// worker thread to the async handler cleanly.
pub struct QueryResult {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

/// One unit of work handed to the worker thread: a SQL string and the reply
/// channel the result (or error message) is sent back on.
struct Job {
    sql: String,
    reply: oneshot::Sender<Result<QueryResult, String>>,
}

/// A handle to one connection's execution thread. Dropping it closes the job
/// channel, which stops the worker and drops its `Runtime` (releasing the
/// read-only source handles). Cloneable senders are not exposed: exactly one
/// connection owns exactly one session.
pub struct Session {
    jobs: mpsc::UnboundedSender<Job>,
}

impl Session {
    /// Spawn the worker thread for a connection. The thread builds its own
    /// `Runtime` from `config`; construction happens on the worker (not the
    /// caller) so a build failure surfaces as an error on the first query rather
    /// than blocking `accept`.
    pub fn spawn(config: Config) -> Session {
        let (jobs, receiver) = mpsc::unbounded_channel::<Job>();
        std::thread::spawn(move || run_worker(&config, receiver));
        Session { jobs }
    }

    /// Run one SQL statement on this connection's runtime and await the Arrow
    /// result. Errors come back as their engine message string; a dead worker
    /// (channel closed) is itself reported as an error, never silently ignored.
    pub async fn execute(&self, sql: String) -> Result<QueryResult, String> {
        let (reply, answer) = oneshot::channel();
        self.jobs
            .send(Job { sql, reply })
            .map_err(|_| "session worker thread is not running".to_string())?;
        match answer.await {
            Ok(result) => result,
            Err(_) => Err("session worker dropped the query without replying".to_string()),
        }
    }
}

/// The worker thread body: build the runtime once, then service jobs in arrival
/// order until the channel closes. If the runtime fails to build, every job for
/// this connection is answered with that failure (loud on first query), so the
/// connection reports the real cause instead of a silent drop.
fn run_worker(config: &Config, mut receiver: mpsc::UnboundedReceiver<Job>) {
    let runtime = Runtime::from_config(config).map_err(|error| error.to_string());
    while let Some(job) = receiver.blocking_recv() {
        let result = match &runtime {
            Ok(runtime) => runtime
                .execute(&job.sql)
                .map(|(schema, batches)| QueryResult { schema, batches })
                .map_err(|error| error.to_string()),
            Err(build_error) => Err(build_error.clone()),
        };
        // The receiver is gone only if the client already disconnected; that is
        // an expected race, not an error to surface.
        let _ = job.reply.send(result);
    }
}
