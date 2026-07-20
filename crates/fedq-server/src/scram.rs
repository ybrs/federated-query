//! The vendored SCRAM-SHA-256 startup handler.
//!
//! pgwire 0.40's stock `ScramAuth` verifies a client proof by recomputing from
//! `SaltedPassword`, which this engine does NOT store. This handler drives the
//! same SASL wire exchange pgwire's `SASLAuthStartupHandler` does, but delegates
//! the proof math to `fq_scram::ScramServer`, which verifies from the stored
//! `StoredKey`/`ServerKey` (the standard server-side SCRAM verification real
//! PostgreSQL performs). It reuses pgwire's public startup helpers
//! (`protocol_negotiation`, `save_startup_parameters_to_metadata`,
//! `finish_authentication`) and its connection-manager registration so
//! cancellation works exactly as under the stock handler. v1 advertises
//! `SCRAM-SHA-256` only (no channel binding): the transport is plaintext TCP.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{Sink, SinkExt};
use tokio::sync::Mutex;

use pgwire::api::auth::{
    finish_authentication, protocol_negotiation, save_startup_parameters_to_metadata,
    DefaultServerParameterProvider, LoginInfo, StartupHandler,
};
use pgwire::api::{
    ClientInfo, ConnectionGuard, ConnectionHandle, ConnectionManager, PgWireConnectionState,
    PidSecretKeyGenerator, RandomPidSecretKeyGenerator,
};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::startup::{Authentication, PasswordMessageFamily};
use pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};

use fq_scram::{ScramServer, MECHANISM};

use crate::auth::StoreAuthSource;

/// The per-connection SASL state machine for the vendored handshake.
enum ScramFlow {
    /// Server advertised SCRAM; awaiting the client-first message.
    Initial,
    /// Server-first sent; awaiting the client-final proof. Carries the verifier
    /// state and the authenticated username (for the error/audit path).
    AwaitingFinal {
        server: Box<ScramServer>,
        username: String,
    },
    /// Authentication completed.
    Done,
}

/// The vendored SCRAM-SHA-256 startup handler for one connection.
pub struct ScramStartupHandler {
    auth: Arc<StoreAuthSource>,
    manager: Arc<ConnectionManager>,
    provider: DefaultServerParameterProvider,
    pid_generator: RandomPidSecretKeyGenerator,
    state: Mutex<ScramFlow>,
}

impl ScramStartupHandler {
    /// Build the handler over the shared verifier source and connection manager.
    pub fn new(auth: Arc<StoreAuthSource>, manager: Arc<ConnectionManager>) -> Self {
        Self {
            auth,
            manager,
            provider: DefaultServerParameterProvider::default(),
            pid_generator: RandomPidSecretKeyGenerator::default(),
            state: Mutex::new(ScramFlow::Initial),
        }
    }

    /// Handle one client SASL password-family message, driving the state machine.
    async fn handle_password<C>(
        &self,
        client: &mut C,
        message: PasswordMessageFamily,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        // Move the state out (leaving Done); the Initial arm restores AwaitingFinal.
        let current = std::mem::replace(&mut *self.state.lock().await, ScramFlow::Done);
        match current {
            ScramFlow::Initial => self.on_client_first(client, message).await,
            ScramFlow::AwaitingFinal { server, username } => {
                self.on_client_final(client, message, &server, &username)
                    .await
            }
            ScramFlow::Done => Err(PgWireError::InvalidSASLState),
        }
    }

    /// Process the client-first message: look up the (real or mock) verifier for
    /// the startup username and reply with the server-first message.
    async fn on_client_first<C>(
        &self,
        client: &mut C,
        message: PasswordMessageFamily,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let initial = message.into_sasl_initial_response()?;
        if initial.auth_method != MECHANISM {
            return Err(PgWireError::UnsupportedSASLAuthMethod(initial.auth_method));
        }
        let client_first = initial
            .data
            .ok_or_else(|| PgWireError::InvalidScramMessage("empty client-first".to_owned()))?;
        // The authenticated identity is the STARTUP username (saved to metadata),
        // never trusted from the SCRAM n= field; an unknown user gets a mock.
        let username = LoginInfo::from_client_info(client)
            .user()
            .unwrap_or_default()
            .to_owned();
        let verifier = self.auth.verifier_for(&username);
        let (server_first, server) = ScramServer::start(verifier, &client_first)
            .map_err(|error| PgWireError::InvalidScramMessage(error.to_string()))?;
        client
            .send(PgWireBackendMessage::Authentication(
                Authentication::SASLContinue(server_first.into_bytes().into()),
            ))
            .await?;
        *self.state.lock().await = ScramFlow::AwaitingFinal {
            server: Box::new(server),
            username,
        };
        Ok(())
    }

    /// Process the client-final message: verify the proof from the stored
    /// StoredKey/ServerKey. A failure (wrong password, or a mock for an unknown
    /// user) is audited and surfaced as a clean authentication failure; success
    /// completes the handshake and registers the connection for cancellation.
    async fn on_client_final<C>(
        &self,
        client: &mut C,
        message: PasswordMessageFamily,
        server: &ScramServer,
        username: &str,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let response = message.into_sasl_response()?;
        let authenticated = match server.finish(&response.data, username) {
            Ok(authenticated) => authenticated,
            Err(error) => {
                // The real cause is server-logged; the client sees a clean auth
                // failure that names the user, never the credential.
                tracing::warn!(target: "fedq::audit", %username, cause = %error, "login failed");
                return Err(PgWireError::InvalidPassword(username.to_owned()));
            }
        };
        client
            .send(PgWireBackendMessage::Authentication(
                Authentication::SASLFinal(authenticated.server_final.into_bytes().into()),
            ))
            .await?;
        let (pid, secret_key) = self.pid_generator.generate(client);
        client.set_pid_and_secret_key(pid, secret_key);
        self.register_connection(client);
        finish_authentication(client, &self.provider).await?;
        tracing::info!(target: "fedq::audit", %username, "login succeeded");
        Ok(())
    }

    /// Register this connection with the shared manager so a later CancelRequest
    /// resolves to it, mirroring pgwire's own (private) registration.
    fn register_connection<C: ClientInfo>(&self, client: &C) {
        let (pid, secret_key) = client.pid_and_secret_key();
        let (handle, guard) = self.manager.register(pid, secret_key);
        client
            .session_extensions()
            .insert::<Arc<ConnectionHandle>>(handle);
        client.session_extensions().insert::<ConnectionGuard>(guard);
    }
}

#[async_trait]
impl StartupHandler for ScramStartupHandler {
    /// Drive the SASL handshake: on Startup, advertise SCRAM-SHA-256; on each
    /// password-family message, advance the state machine.
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match message {
            PgWireFrontendMessage::Startup(ref startup) => {
                protocol_negotiation(client, startup).await?;
                save_startup_parameters_to_metadata(client, startup);
                client.set_state(PgWireConnectionState::AuthenticationInProgress);
                client
                    .send(PgWireBackendMessage::Authentication(Authentication::SASL(
                        vec![MECHANISM.to_owned()],
                    )))
                    .await?;
            }
            PgWireFrontendMessage::PasswordMessageFamily(message) => {
                self.handle_password(client, message).await?;
            }
            _ => {}
        }
        Ok(())
    }
}
