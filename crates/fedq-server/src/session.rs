//! One connection's fixed database and identity, executed on its own worker thread.

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use fq_common::database::PrincipalId;
use fq_common::{Config, DataType};
use fq_runtime::{Runtime, RuntimeError};
use tokio::sync::{mpsc, oneshot};

/// Materialized rows crossing from the engine thread to the wire handler.
pub struct QueryResult {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

/// A result column resolved by planning without execution.
pub type ColumnDescription = (String, DataType);

/// Identity established by the startup handler, independent of later query jobs.
pub enum SessionIdentity {
    Trusted(String),
    Authenticated(PrincipalId),
}

/// Native engine errors and explicit failures of the execution bridge.
#[derive(Debug, thiserror::Error)]
pub enum SessionError {
    #[error(transparent)]
    Runtime(#[from] RuntimeError),
    #[error("session worker thread is not running")]
    WorkerUnavailable,
    #[error("session worker dropped the request without replying")]
    ReplyDropped,
    #[error("session has not completed startup")]
    NotInitialized,
    #[error("session database and identity are already bound")]
    AlreadyInitialized,
}

/// Requests are ordered on one worker; query jobs cannot change session authority.
enum Job {
    Initialize {
        database: String,
        identity: SessionIdentity,
        reply: oneshot::Sender<Result<(), SessionError>>,
    },
    Execute {
        sql: String,
        reply: oneshot::Sender<Result<QueryResult, SessionError>>,
    },
    Describe {
        sql: String,
        reply: oneshot::Sender<Result<Vec<ColumnDescription>, SessionError>>,
    },
}

/// Shared by startup and query handlers belonging to exactly one connection.
pub struct Session {
    jobs: mpsc::UnboundedSender<Job>,
}

impl Session {
    /// Start an idle worker; no database or connector opens until startup is authorized.
    pub fn spawn(config: Config) -> Self {
        let (jobs, receiver) = mpsc::unbounded_channel();
        std::thread::Builder::new()
            .stack_size(fq_common::QUERY_THREAD_STACK_BYTES)
            .spawn(move || run_worker(&config, receiver))
            .expect("session worker thread spawn failed");
        Self { jobs }
    }

    /// Bind the selected database and proven identity before ReadyForQuery is sent.
    pub async fn initialize(
        &self,
        database: String,
        identity: SessionIdentity,
    ) -> Result<(), SessionError> {
        let (reply, answer) = oneshot::channel();
        self.dispatch(
            Job::Initialize {
                database,
                identity,
                reply,
            },
            answer,
        )
        .await
    }

    /// Execute a statement under the connection's established authority.
    pub async fn execute(&self, sql: String) -> Result<QueryResult, SessionError> {
        let (reply, answer) = oneshot::channel();
        self.dispatch(Job::Execute { sql, reply }, answer).await
    }

    /// Resolve a statement's columns under the same authority as execution.
    pub async fn describe(&self, sql: String) -> Result<Vec<ColumnDescription>, SessionError> {
        let (reply, answer) = oneshot::channel();
        self.dispatch(Job::Describe { sql, reply }, answer).await
    }

    /// Report channel failures explicitly instead of losing a request silently.
    async fn dispatch<T>(
        &self,
        job: Job,
        answer: oneshot::Receiver<Result<T, SessionError>>,
    ) -> Result<T, SessionError> {
        self.jobs
            .send(job)
            .map_err(|_| SessionError::WorkerUnavailable)?;
        answer.await.map_err(|_| SessionError::ReplyDropped)?
    }
}

/// Service requests without creating an embedded-authority runtime before authentication.
fn run_worker(config: &Config, mut receiver: mpsc::UnboundedReceiver<Job>) {
    let mut runtime = None;
    while let Some(job) = receiver.blocking_recv() {
        match job {
            Job::Initialize {
                database,
                identity,
                reply,
            } => {
                let result = initialize(config, &mut runtime, &database, identity);
                let _ = reply.send(result);
            }
            job => serve_job(runtime.as_ref(), job),
        }
    }
}

/// Publish a runtime only after database admission and complete initialization succeed.
fn initialize(
    config: &Config,
    runtime: &mut Option<Runtime>,
    database: &str,
    identity: SessionIdentity,
) -> Result<(), SessionError> {
    if runtime.is_some() {
        return Err(SessionError::AlreadyInitialized);
    }
    let initialized = match identity {
        SessionIdentity::Trusted(name) => {
            let initialized = Runtime::from_config_in_database(config, database)?;
            initialized.set_principal(&name)?;
            initialized
        }
        SessionIdentity::Authenticated(id) => {
            Runtime::from_config_authenticated(config, database, id)?
        }
    };
    *runtime = Some(initialized);
    Ok(())
}

/// Answer query jobs; a disconnected client's dropped reply needs no response.
fn serve_job(runtime: Option<&Runtime>, job: Job) {
    match job {
        Job::Execute { sql, reply } => {
            let result = execute(runtime, &sql);
            let _ = reply.send(result);
        }
        Job::Describe { sql, reply } => {
            let result = runtime
                .ok_or(SessionError::NotInitialized)
                .and_then(|runtime| runtime.describe(&sql).map_err(SessionError::from));
            let _ = reply.send(result);
        }
        Job::Initialize { .. } => {
            unreachable!("startup requests are handled before query dispatch")
        }
    }
}

/// Preserve the native engine error when returning query rows to the async handler.
fn execute(runtime: Option<&Runtime>, sql: &str) -> Result<QueryResult, SessionError> {
    let runtime = runtime.ok_or(SessionError::NotInitialized)?;
    let (schema, batches) = runtime.execute(sql)?;
    Ok(QueryResult { schema, batches })
}
