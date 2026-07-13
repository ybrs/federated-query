//! The pgwire protocol handlers.
//!
//! `FedqBackend` serves both wire query protocols by running SQL on the
//! connection's `Session` and encoding the Arrow result. The simple protocol runs
//! the query string directly; the extended protocol (Parse/Bind/Describe/Execute)
//! resolves a statement's result schema by planning (without executing) for
//! Describe, and splices bound parameter values into the SQL at Execute.
//! `TrustStartup` performs the no-auth handshake, and `FedqHandlers` bundles them
//! for pgwire's `process_socket`.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Sink;
use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::auth::StartupHandler;
use pgwire::api::portal::{Format, Portal};
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DescribePortalResponse, DescribeStatementResponse, Response};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::store::PortalStore;
use pgwire::api::{ClientInfo, ClientPortalStore, PgWireServerHandlers, Type};
use pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;

use crate::encode;
use crate::params;
use crate::session::Session;

/// One connection's query handler. Holds the `Session` that owns the connection's
/// runtime thread and the parser that stores each extended-protocol statement as
/// its verbatim SQL string.
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
        // The simple protocol always returns rows in text format.
        let response = encode::to_response(&result.schema, &result.batches, &Format::UnifiedText)?;
        Ok(vec![response])
    }
}

#[async_trait]
impl ExtendedQueryHandler for FedqBackend {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    /// The parser used by the default Parse machinery: it stores the SQL string
    /// verbatim as the statement (the engine parses it lazily at Execute), so no
    /// parsing happens here.
    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::clone(&self.query_parser)
    }

    /// Execute a bound portal: splice its parameter values into the statement's
    /// SQL, run it on the session, and encode the rows in the format the client
    /// requested for the portal's result columns. pgwire applies the Execute
    /// message's row limit to the returned response.
    async fn do_query<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = params::substitute(portal)?;
        let result = self.session.execute(sql).await.map_err(engine_error)?;
        encode::to_response(
            &result.schema,
            &result.batches,
            &portal.result_column_format,
        )
    }

    /// Describe a prepared statement before any value is bound: resolve its result
    /// columns by planning a placeholder-free form of the SQL (parameter values do
    /// not change the output types), and report the declared parameter types back
    /// to the client. No rows are read.
    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = params::substitute_nulls(&target.statement, &target.parameter_types)?;
        let columns = self.session.describe(sql).await.map_err(engine_error)?;
        let fields = encode::describe_fields(&columns)?;
        Ok(DescribeStatementResponse::new(
            declared_param_types(&target.parameter_types),
            fields,
        ))
    }

    /// Describe a bound portal: resolve its result columns by planning the SQL
    /// with the bound parameter values spliced in. No rows are read.
    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        portal: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let sql = params::substitute(portal)?;
        let columns = self.session.describe(sql).await.map_err(engine_error)?;
        let fields = encode::describe_fields(&columns)?;
        Ok(DescribePortalResponse::new(fields))
    }
}

/// The parameter types to report in a statement's parameter description: the
/// client's declared type for each `$n`, or the unknown type where it declared
/// none.
fn declared_param_types(types: &[Option<Type>]) -> Vec<Type> {
    let mut declared = Vec::with_capacity(types.len());
    for pg_type in types {
        declared.push(pg_type.clone().unwrap_or(Type::UNKNOWN));
    }
    declared
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
