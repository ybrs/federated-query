//! Errors from verifier handling and the SCRAM handshake.

/// A failure deriving, parsing, or verifying a SCRAM-SHA-256 credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScramError {
    /// A password with no characters: an empty credential would let a bare
    /// handshake authenticate, so derivation refuses it.
    EmptyPassword,
    /// A `pg_authid` verifier string that does not parse (bad scheme, count,
    /// base64, or key length). The message names the malformed part, never the
    /// credential bytes.
    InvalidVerifier(String),
    /// A client SCRAM message that does not parse to the expected shape.
    InvalidMessage(String),
    /// The client's channel-binding value did not match; v1 advertises
    /// SCRAM-SHA-256 only, so the client must send the no-binding `biws`.
    ChannelBindingMismatch,
    /// The client's proof did not verify against the stored verifier - a wrong
    /// password, or an unknown user checked against a mock verifier. Carries the
    /// username for the server audit line, NEVER the credential.
    AuthenticationFailed(String),
}

impl std::fmt::Display for ScramError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScramError::EmptyPassword => write!(f, "password is empty"),
            ScramError::InvalidVerifier(reason) => write!(f, "invalid SCRAM verifier: {reason}"),
            ScramError::InvalidMessage(reason) => write!(f, "invalid SCRAM message: {reason}"),
            ScramError::ChannelBindingMismatch => write!(f, "SCRAM channel binding mismatch"),
            ScramError::AuthenticationFailed(user) => {
                write!(f, "SCRAM authentication failed for user '{user}'")
            }
        }
    }
}

impl std::error::Error for ScramError {}
