//! Config-driven SCRAM-SHA-256 authentication.
//!
//! The `server:` config section lists users, each with a stored SCRAM credential
//! (a salt and a salted password, both base64; see `fq_common::UserCredential`).
//! `ConfigAuthSource` is the pgwire `AuthSource` the SCRAM handshake queries: for
//! a named user it returns that user's salt and salted password so pgwire can
//! verify the client's proof, and for an unknown user it refuses. The password
//! itself is never held - only the PBKDF2 salted hash the client's proof is
//! checked against.
//!
//! `hash_password` is the inverse used by the `hash-password` CLI: it turns a
//! plaintext password into a `UserCredential` so the plaintext never enters the
//! config file.

use std::collections::HashMap;

use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use pgwire::api::auth::sasl::scram::gen_salted_password;
use pgwire::api::auth::{AuthSource, LoginInfo, Password};
use pgwire::error::{PgWireError, PgWireResult};
use rand::RngCore;

use fq_common::{ServerConfig, UserCredential, SCRAM_ITERATIONS};

/// The random salt length, in bytes, for a generated SCRAM credential.
const SALT_LEN: usize = 16;

/// One decoded user credential: the raw salt and the raw PBKDF2 salted password
/// the SCRAM handshake verifies a login against.
#[derive(Debug, Clone)]
struct StoredCredential {
    salt: Vec<u8>,
    salted_password: Vec<u8>,
}

/// The pgwire `AuthSource` backed by the configured user list. It holds each
/// user's decoded salt and salted password, keyed by user name.
#[derive(Debug)]
pub struct ConfigAuthSource {
    users: HashMap<String, StoredCredential>,
}

impl ConfigAuthSource {
    /// Decode every configured user's base64 salt and salted password once, at
    /// server startup. A malformed base64 field or a duplicate user name fails
    /// loudly here rather than at a client's first login attempt.
    pub fn from_config(server: &ServerConfig) -> Result<Self, String> {
        let mut users = HashMap::with_capacity(server.users.len());
        for user in &server.users {
            let credential = decode_credential(user)?;
            if users.insert(user.name.clone(), credential).is_some() {
                return Err(format!("duplicate server user '{}'", user.name));
            }
        }
        Ok(Self { users })
    }

    /// Whether any user is configured. An empty source means trust auth.
    pub fn is_empty(&self) -> bool {
        self.users.is_empty()
    }
}

/// Decode one config user's base64 salt and salted password into raw bytes,
/// naming the user on a malformed field.
fn decode_credential(user: &UserCredential) -> Result<StoredCredential, String> {
    let salt = STANDARD
        .decode(&user.salt)
        .map_err(|error| format!("user '{}' has an invalid base64 salt: {error}", user.name))?;
    let salted_password = STANDARD.decode(&user.salted_password).map_err(|error| {
        format!(
            "user '{}' has an invalid base64 salted_password: {error}",
            user.name
        )
    })?;
    Ok(StoredCredential {
        salt,
        salted_password,
    })
}

#[async_trait]
impl AuthSource for ConfigAuthSource {
    /// Return the named user's salt and salted password for the SCRAM handshake.
    /// An unknown user, or a startup that named no user, is refused with an
    /// invalid-password error so the client sees a clean authentication failure.
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password> {
        let user = login
            .user()
            .ok_or_else(|| PgWireError::InvalidPassword(String::new()))?;
        let credential = self
            .users
            .get(user)
            .ok_or_else(|| PgWireError::InvalidPassword(user.to_owned()))?;
        Ok(Password::new(
            Some(credential.salt.clone()),
            credential.salted_password.clone(),
        ))
    }
}

/// Turn a plaintext password into a stored SCRAM credential for `user`: a fresh
/// random salt and the base64 of `PBKDF2-HMAC-SHA256(password, salt, 4096)`. The
/// plaintext is used only to derive the one-way hash and is never returned.
pub fn hash_password(user: &str, password: &str) -> UserCredential {
    let mut salt = [0u8; SALT_LEN];
    rand::rng().fill_bytes(&mut salt);
    let salted = gen_salted_password(password, &salt, SCRAM_ITERATIONS as usize);
    UserCredential {
        name: user.to_owned(),
        salt: STANDARD.encode(salt),
        salted_password: STANDARD.encode(salted),
    }
}

/// Render a `UserCredential` as the YAML block that goes under `server.users`,
/// ready to paste into a config file.
pub fn credential_yaml(credential: &UserCredential) -> String {
    format!(
        "    - name: {}\n      salt: {}\n      salted_password: {}\n",
        credential.name, credential.salt, credential.salted_password
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hashed_password_round_trips_through_the_auth_source() {
        // A credential generated by `hash_password` decodes back into a stored
        // credential whose salted password matches re-deriving it from the same
        // plaintext and salt - the invariant the SCRAM handshake relies on.
        let credential = hash_password("alice", "hunter2");
        let server = ServerConfig {
            users: vec![credential.clone()],
        };
        let source = ConfigAuthSource::from_config(&server).expect("build source");
        assert!(!source.is_empty());

        let salt = STANDARD.decode(&credential.salt).expect("decode salt");
        let expected = gen_salted_password("hunter2", &salt, SCRAM_ITERATIONS as usize);
        let stored = source.users.get("alice").expect("user present");
        assert_eq!(stored.salted_password, expected);
    }

    #[test]
    fn a_duplicate_user_name_is_refused() {
        let server = ServerConfig {
            users: vec![hash_password("alice", "a"), hash_password("alice", "b")],
        };
        assert!(ConfigAuthSource::from_config(&server).is_err());
    }

    #[test]
    fn invalid_base64_is_refused() {
        let server = ServerConfig {
            users: vec![UserCredential {
                name: "bob".to_owned(),
                salt: "not base64 !!!".to_owned(),
                salted_password: "aGFzaA==".to_owned(),
            }],
        };
        assert!(ConfigAuthSource::from_config(&server).is_err());
    }
}
