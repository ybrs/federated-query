//! The pgwire protocol handlers.
//!
//! `FedqBackend` serves the simple query protocol by running SQL on the
//! connection's `Session` and encoding the Arrow result; it also implements the
//! extended query protocol as a loud "not supported" error so parameterized
//! clients get a clean `ErrorResponse` instead of a dropped connection.
//! `TrustStartup` performs the no-auth handshake, and `FedqHandlers` bundles the
//! three for pgwire's `process_socket`.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Sink;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::auth::StartupHandler;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DescribePortalResponse, DescribeStatementResponse, Response};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, PgWireServerHandlers};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;

use crate::encode;
use crate::session::Session;

/// One connection's query handler. Holds the `Session` that owns the connection's
/// runtime thread and a shared no-op parser used only to satisfy the extended
/// query handler's associated type.
pub struct FedqBackend {
    session: Session,
    query_parser: Arc<NoopQueryParser>,
}

impl FedqBackend {
    /// Wrap a session as a query backend.
    fn new(session: Session) -> Self {
        Self {
            session,
            query_parser: Arc::new(NoopQueryParser),
        }
    }
}

#[async_trait]
impl SimpleQueryHandler for FedqBackend {
    /// Run one simple-protocol query: an empty statement returns the empty-query
    /// response, otherwise the SQL runs on the session and its Arrow result is
    /// encoded to Postgres rows. An engine error becomes an `ErrorResponse`
    /// carrying the engine's own message.
    async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if query.trim().is_empty() {
            return Ok(vec![Response::EmptyQuery]);
        }
        let result = self
            .session
            .execute(query.to_owned())
            .await
            .map_err(engine_error)?;
        let response = encode::to_response(&result.schema, &result.batches)?;
        Ok(vec![response])
    }
}

#[async_trait]
impl ExtendedQueryHandler for FedqBackend {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    /// The parser used by the default parse/bind machinery; never produces a
    /// usable statement here because execution is refused below.
    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::clone(&self.query_parser)
    }

    /// Refuse extended-protocol execution loudly (feature_not_supported), keeping
    /// the connection open for the next statement.
    async fn do_query<C>(
        &self,
        _client: &mut C,
        _portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Err(extended_unsupported())
    }

    /// Refuse the Describe-statement step of the extended protocol loudly.
    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        _target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Err(extended_unsupported())
    }

    /// Refuse the Describe-portal step of the extended protocol loudly.
    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        _target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        Err(extended_unsupported())
    }
}

/// The no-authentication startup handler: any user name is accepted with no
/// password, and the rest of the handshake runs normally. A distinct type so a
/// real auth source can replace it without touching the query path.
pub struct TrustStartup;

impl NoopStartupHandler for TrustStartup {}

/// The handler bundle pgwire's `process_socket` consumes for one connection. Both
/// query handlers share the single backend; copy/error/cancel use pgwire's
/// defaults.
pub struct FedqHandlers {
    backend: Arc<FedqBackend>,
}

impl FedqHandlers {
    /// Build the handler bundle for a connection from its session.
    pub fn new(session: Session) -> Self {
        Self {
            backend: Arc::new(FedqBackend::new(session)),
        }
    }
}

impl PgWireServerHandlers for FedqHandlers {
    /// The simple query handler is the backend itself.
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        Arc::clone(&self.backend)
    }

    /// The extended query handler is the same backend, which refuses execution.
    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        Arc::clone(&self.backend)
    }

    /// Startup uses trust auth.
    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::new(TrustStartup)
    }
}

/// Wrap an engine error message as a Postgres `ErrorResponse`. The engine's
/// message is passed through unmodified, so an invalid query surfaces its real
/// bind/parse cause instead of a laundered one.
fn engine_error(message: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "XX000".to_owned(),
        message,
    )))
}

/// The error returned for any extended query protocol request.
fn extended_unsupported() -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_owned(),
        "0A000".to_owned(),
        "the extended query protocol (prepared statements) is not supported; use the simple query protocol".to_owned(),
    )))
}
