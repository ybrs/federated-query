//! RFC-5802 server-side SCRAM-SHA-256 verification from a stored verifier.
//!
//! pgwire 0.40's stock `ScramAuth` recomputes the client proof from
//! `SaltedPassword`, which the engine deliberately does NOT store. This module
//! is the standard server-side verification real PostgreSQL performs from the
//! stored `StoredKey`/`ServerKey`: recover `ClientKey` from the client's proof,
//! check `SHA-256(ClientKey) == StoredKey`, and sign the reply with `ServerKey`.
//! The ~5 SCRAM wire-message shapes (private in pgwire) are reparsed here so the
//! crate stays pgwire-free; fedq-server wraps this behind a pgwire
//! `StartupHandler`. v1 advertises `SCRAM-SHA-256` only (no channel binding),
//! because the transport is plaintext TCP with no server certificate.

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use subtle::ConstantTimeEq;

use crate::error::ScramError;
use crate::verifier::{hmac_sha256, random_bytes, sha256, Verifier};

/// The base64 of the gs2 header `n,,` (no channel binding, no authzid) - the
/// `c=` value a client that requested SCRAM-SHA-256 (not -PLUS) must send in its
/// final message. v1 accepts only this.
const NO_CHANNEL_BINDING: &str = "biws";

/// The SCRAM mechanism name this server advertises.
pub const MECHANISM: &str = "SCRAM-SHA-256";

/// The state between server-first and the final proof check: the pieces the
/// `AuthMessage` is rebuilt from, plus the verifier the proof is checked against.
#[derive(Debug)]
pub struct ScramServer {
    verifier: Verifier,
    client_first_bare: String,
    server_first: String,
}

/// The outcome of a successful proof check: the base64 `ServerSignature` to send
/// as the SASL final message, and the authenticated username.
#[derive(Debug)]
pub struct Authenticated {
    /// The `v=<base64 ServerSignature>` payload of the SASL final message.
    pub server_final: String,
}

impl ScramServer {
    /// Handle the client-first message: parse it, generate the server nonce, and
    /// return the server-first message plus the state carrying it forward. The
    /// caller supplies the verifier to check against (a real user's, or a mock
    /// for an unknown user - the same path either way).
    pub fn start(
        verifier: Verifier,
        client_first: &[u8],
    ) -> Result<(String, ScramServer), ScramError> {
        let parsed = ClientFirst::parse(decode_utf8(client_first)?)?;
        let mut full_nonce = parsed.nonce.clone();
        full_nonce.push_str(&base64_nonce());
        let server_first = format!(
            "r={full_nonce},s={},i={}",
            STANDARD.encode(&verifier.salt),
            verifier.iterations
        );
        let client_first_bare = format!("n={},r={}", parsed.username, parsed.nonce);
        Ok((
            server_first.clone(),
            ScramServer {
                verifier,
                client_first_bare,
                server_first,
            },
        ))
    }

    /// Verify the client-final message against the stored verifier. Recovers
    /// `ClientKey` from the proof, checks `SHA-256(ClientKey) == StoredKey`, and
    /// on success returns the `ServerSignature` reply. A wrong proof (wrong
    /// password, or a mock verifier for an unknown user) fails identically.
    pub fn finish(&self, client_final: &[u8], username: &str) -> Result<Authenticated, ScramError> {
        let parsed = ClientFinal::parse(decode_utf8(client_final)?)?;
        if parsed.channel_binding != NO_CHANNEL_BINDING {
            return Err(ScramError::ChannelBindingMismatch);
        }
        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, self.server_first, parsed.without_proof
        );
        let client_signature = hmac_sha256(&self.verifier.stored_key, auth_message.as_bytes());
        let proof = STANDARD
            .decode(&parsed.proof)
            .map_err(|error| ScramError::InvalidMessage(format!("bad base64 proof: {error}")))?;
        let recovered_client_key = xor(&proof, &client_signature)?;
        // Constant-time compare: the recovered key is derived from client-supplied
        // proof bytes, so a byte-by-byte early exit would leak StoredKey through
        // timing. `ct_eq` runs in time independent of where the first byte differs.
        let computed = sha256(&recovered_client_key);
        if !bool::from(computed[..].ct_eq(&self.verifier.stored_key[..])) {
            return Err(ScramError::AuthenticationFailed(username.to_string()));
        }
        let server_signature = hmac_sha256(&self.verifier.server_key, auth_message.as_bytes());
        Ok(Authenticated {
            server_final: format!("v={}", STANDARD.encode(server_signature)),
        })
    }
}

/// The parsed client-first message: the username and the client nonce. The gs2
/// header is required to be no-channel-binding (`n` or `y`); v1 rejects a `p=`
/// channel-binding request since it advertises no -PLUS mechanism.
struct ClientFirst {
    username: String,
    nonce: String,
}

impl ClientFirst {
    /// Parse `n,,n=<user>,r=<nonce>` (with the optional reserved-mext `m=` and
    /// authzid tolerated), raising on any other shape.
    fn parse(message: &str) -> Result<Self, ScramError> {
        let mut parts = message.split(',');
        let cbind = next_part(&mut parts, message)?;
        if cbind.starts_with("p=") {
            // A p= flag requests channel binding, which needs SCRAM-SHA-256-PLUS;
            // v1 advertises only SCRAM-SHA-256, so this is a protocol mismatch.
            return Err(ScramError::ChannelBindingMismatch);
        }
        if cbind != "n" && cbind != "y" {
            return Err(ScramError::InvalidMessage(message.to_string()));
        }
        // The authzid field (may be empty); its content is not used here.
        let _authzid = next_part(&mut parts, message)?;
        let mut field = next_part(&mut parts, message)?;
        if field.starts_with("m=") {
            // A reserved mandatory extension the server does not understand.
            return Err(ScramError::InvalidMessage(
                "unsupported SCRAM mandatory extension".to_string(),
            ));
        }
        let username = field
            .strip_prefix("n=")
            .ok_or_else(|| ScramError::InvalidMessage(message.to_string()))?
            .to_string();
        field = next_part(&mut parts, message)?;
        let nonce = field
            .strip_prefix("r=")
            .ok_or_else(|| ScramError::InvalidMessage(message.to_string()))?
            .to_string();
        Ok(Self {
            username: decode_saslname(&username),
            nonce,
        })
    }
}

/// The parsed client-final message: the without-proof prefix (channel binding +
/// full nonce, re-rendered for the `AuthMessage`) and the base64 client proof.
struct ClientFinal {
    channel_binding: String,
    without_proof: String,
    proof: String,
}

impl ClientFinal {
    /// Parse `c=<b64 cbind>,r=<full nonce>,p=<b64 proof>`.
    fn parse(message: &str) -> Result<Self, ScramError> {
        let mut parts = message.split(',');
        let channel_binding = next_part(&mut parts, message)?
            .strip_prefix("c=")
            .ok_or_else(|| ScramError::InvalidMessage(message.to_string()))?
            .to_string();
        let nonce = next_part(&mut parts, message)?
            .strip_prefix("r=")
            .ok_or_else(|| ScramError::InvalidMessage(message.to_string()))?
            .to_string();
        // The proof is the LAST field; a client may (rarely) send extensions
        // between the nonce and the proof, so take the final p= field.
        let proof = last_part(&mut parts, message)?
            .strip_prefix("p=")
            .ok_or_else(|| ScramError::InvalidMessage(message.to_string()))?
            .to_string();
        let without_proof = format!("c={channel_binding},r={nonce}");
        Ok(Self {
            channel_binding,
            without_proof,
            proof,
        })
    }
}

/// The next comma-separated field, or a loud parse error naming the message.
fn next_part<'a>(
    parts: &mut std::str::Split<'a, char>,
    message: &str,
) -> Result<&'a str, ScramError> {
    parts
        .next()
        .ok_or_else(|| ScramError::InvalidMessage(message.to_string()))
}

/// The last comma-separated field (the proof), consuming any extensions before
/// it.
fn last_part<'a>(
    parts: &mut std::str::Split<'a, char>,
    message: &str,
) -> Result<&'a str, ScramError> {
    parts
        .last()
        .ok_or_else(|| ScramError::InvalidMessage(message.to_string()))
}

/// Decode a SCRAM `saslname`, unescaping `=2C` -> `,` and `=3D` -> `=` (the only
/// two escapes the grammar defines).
fn decode_saslname(name: &str) -> String {
    name.replace("=2C", ",").replace("=3D", "=")
}

/// XOR two equal-length byte slices; a length mismatch is a malformed proof.
fn xor(lhs: &[u8], rhs: &[u8]) -> Result<Vec<u8>, ScramError> {
    if lhs.len() != rhs.len() {
        return Err(ScramError::InvalidMessage(
            "proof length mismatch".to_string(),
        ));
    }
    let mut out = Vec::with_capacity(lhs.len());
    for (l, r) in lhs.iter().zip(rhs.iter()) {
        out.push(l ^ r);
    }
    Ok(out)
}

/// A fresh base64 nonce (18 random bytes), the server's contribution to the
/// combined nonce.
fn base64_nonce() -> String {
    STANDARD.encode(random_bytes(18))
}

/// Decode SCRAM message bytes as UTF-8, raising on invalid input.
fn decode_utf8(data: &[u8]) -> Result<&str, ScramError> {
    std::str::from_utf8(data).map_err(|error| ScramError::InvalidMessage(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::verifier::derive_verifier;

    /// A minimal RFC-5802 client that drives the server side to completion, so
    /// the vendored verification is exercised end-to-end without pgwire.
    struct TestClient {
        username: String,
        password: String,
        client_nonce: String,
        client_first_bare: String,
    }

    impl TestClient {
        fn new(username: &str, password: &str) -> Self {
            Self {
                username: username.to_string(),
                password: password.to_string(),
                client_nonce: "clientnonce123".to_string(),
                client_first_bare: String::new(),
            }
        }

        fn client_first(&mut self) -> String {
            self.client_first_bare = format!("n={},r={}", self.username, self.client_nonce);
            format!("n,,{}", self.client_first_bare)
        }

        /// Build the client-final message and its expected server signature,
        /// computing the proof from the plaintext the real client would use.
        fn client_final(&self, server_first: &str, salt: &[u8], iterations: u32) -> String {
            let full_nonce = parse_field(server_first, "r=");
            let without_proof = format!("c={NO_CHANNEL_BINDING},r={full_nonce}");
            let salted =
                crate::verifier::derive_verifier(&self.password, salt, iterations).expect("derive");
            // Recompute ClientKey from the plaintext (the client has it) to form
            // the proof; the server never does this - it only has StoredKey.
            let salted_password = client_salted_password(&self.password, salt, iterations);
            let client_key = hmac_sha256(&salted_password, b"Client Key");
            let auth_message = format!(
                "{},{},{}",
                self.client_first_bare, server_first, without_proof
            );
            let client_signature = hmac_sha256(&salted.stored_key, auth_message.as_bytes());
            let proof = xor(&client_key, &client_signature).expect("xor");
            format!("{without_proof},p={}", STANDARD.encode(proof))
        }
    }

    /// The client's SaltedPassword (the master secret only the client holds).
    fn client_salted_password(password: &str, salt: &[u8], iterations: u32) -> [u8; 32] {
        use ring::pbkdf2;
        use std::num::NonZeroU32;
        let mut out = [0u8; 32];
        let normalized =
            stringprep::saslprep(password).unwrap_or(std::borrow::Cow::Borrowed(password));
        pbkdf2::derive(
            pbkdf2::PBKDF2_HMAC_SHA256,
            NonZeroU32::new(iterations).unwrap(),
            salt,
            normalized.as_bytes(),
            &mut out,
        );
        out
    }

    fn parse_field(message: &str, prefix: &str) -> String {
        for part in message.split(',') {
            if let Some(value) = part.strip_prefix(prefix) {
                return value.to_string();
            }
        }
        panic!("field {prefix} not in {message}");
    }

    #[test]
    fn a_correct_password_authenticates() {
        let salt = b"0123456789abcdef";
        let verifier = derive_verifier("hunter2", salt, 4096).expect("derive");
        let mut client = TestClient::new("alice", "hunter2");
        let client_first = client.client_first();
        let (server_first, server) =
            ScramServer::start(verifier, client_first.as_bytes()).expect("start");
        let client_final = client.client_final(&server_first, salt, 4096);
        let authenticated = server
            .finish(client_final.as_bytes(), "alice")
            .expect("authenticated");
        assert!(authenticated.server_final.starts_with("v="));
    }

    #[test]
    fn a_wrong_password_fails() {
        let salt = b"0123456789abcdef";
        let verifier = derive_verifier("hunter2", salt, 4096).expect("derive");
        let mut client = TestClient::new("alice", "wrong-password");
        let client_first = client.client_first();
        let (server_first, server) =
            ScramServer::start(verifier, client_first.as_bytes()).expect("start");
        let client_final = client.client_final(&server_first, salt, 4096);
        assert!(matches!(
            server.finish(client_final.as_bytes(), "alice").unwrap_err(),
            ScramError::AuthenticationFailed(user) if user == "alice"
        ));
    }

    #[test]
    fn an_unknown_user_against_a_mock_fails_on_the_same_path() {
        // The unknown-user path checks a real handshake against a mock verifier;
        // it must fail with the same AuthenticationFailed error, not a fast reject.
        let mock = crate::verifier::mock_verifier("ghost", b"server-secret", 4096);
        let mut client = TestClient::new("ghost", "any-password");
        let client_first = client.client_first();
        let (server_first, server) =
            ScramServer::start(mock, client_first.as_bytes()).expect("start");
        let client_final = client.client_final(&server_first, b"0123456789abcdef", 4096);
        assert!(matches!(
            server.finish(client_final.as_bytes(), "ghost").unwrap_err(),
            ScramError::AuthenticationFailed(_)
        ));
    }

    #[test]
    fn a_channel_binding_request_is_rejected() {
        let verifier = derive_verifier("pw", b"0123456789abcdef", 4096).expect("derive");
        // A gs2 p= flag requests SCRAM-SHA-256-PLUS, which v1 does not advertise.
        let client_first = "p=tls-server-end-point,,n=alice,r=nonce";
        assert!(matches!(
            ScramServer::start(verifier, client_first.as_bytes()).unwrap_err(),
            ScramError::ChannelBindingMismatch
        ));
    }
}
