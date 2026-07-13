//! The per-connection execution bridge.
//!
//! A `Session` owns one OS thread that builds a `Runtime` from the config and
//! runs that connection's queries on it in order. The async handler talks to the
//! thread over channels: it sends the SQL and awaits the Arrow result. Keeping
//! every `execute` for a connection pinned to one thread is what makes the
//! fq-exec thread-local data-plane caches coherent (see the crate module doc).

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use fq_common::{Config, DataType};
use fq_runtime::Runtime;
use tokio::sync::{mpsc, oneshot};

/// A materialized engine result: the output schema (user-visible column names)
/// and every batch of rows. Both halves are `Send`, so they cross back from the
/// worker thread to the async handler cleanly.
pub struct QueryResult {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

/// One result column's static description: its user-visible name and the
/// engine type the plan resolved for it, produced by `describe` without
/// executing.
pub type ColumnDescription = (String, DataType);

/// One unit of work handed to the worker thread. `Execute` runs the SQL and
/// returns its rows; `Describe` resolves the SQL's result schema by planning
/// only. Each carries the reply channel its answer (or error message) is sent
/// back on.
enum Job {
    Execute {
        sql: String,
        reply: oneshot::Sender<Result<QueryResult, String>>,
    },
    Describe {
        sql: String,
        reply: oneshot::Sender<Result<Vec<ColumnDescription>, String>>,
    },
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
        let job = Job::Execute { sql, reply };
        self.dispatch(job, answer).await
    }

    /// Resolve one SQL statement's result columns on this connection's runtime
    /// WITHOUT executing it (planning only), for the extended protocol's Describe
    /// step. Errors and a dead worker surface exactly as `execute`'s do.
    pub async fn describe(&self, sql: String) -> Result<Vec<ColumnDescription>, String> {
        let (reply, answer) = oneshot::channel();
        let job = Job::Describe { sql, reply };
        self.dispatch(job, answer).await
    }

    /// Send a job to the worker and await its reply on `answer`. A closed job
    /// channel (worker gone) or a dropped reply is reported as an error, never
    /// silently ignored.
    async fn dispatch<T>(
        &self,
        job: Job,
        answer: oneshot::Receiver<Result<T, String>>,
    ) -> Result<T, String> {
        self.jobs
            .send(job)
            .map_err(|_| "session worker thread is not running".to_string())?;
        match answer.await {
            Ok(result) => result,
            Err(_) => Err("session worker dropped the request without replying".to_string()),
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
        serve_job(&runtime, job);
    }
}

/// Answer one job on the worker thread. Both job kinds report a runtime that
/// failed to build with that build error (loud on every request), and a dropped
/// reply channel (client already disconnected) is an expected race, not an error.
fn serve_job(runtime: &Result<Runtime, String>, job: Job) {
    match job {
        Job::Execute { sql, reply } => {
            let result = match runtime {
                Ok(runtime) => runtime
                    .execute(&sql)
                    .map(|(schema, batches)| QueryResult { schema, batches })
                    .map_err(|error| error.to_string()),
                Err(build_error) => Err(build_error.clone()),
            };
            let _ = reply.send(result);
        }
        Job::Describe { sql, reply } => {
            let result = match runtime {
                Ok(runtime) => runtime.describe(&sql).map_err(|error| error.to_string()),
                Err(build_error) => Err(build_error.clone()),
            };
            let _ = reply.send(result);
        }
    }
}
