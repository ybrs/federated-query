//! The verifier-backed authentication source and credential tooling.
//!
//! A user's password is stored as a SCRAM-SHA-256 VERIFIER (the PostgreSQL
//! `pg_authid` shape: salt, iteration count, StoredKey, ServerKey) - never a
//! plaintext and never the SaltedPassword master secret. `StoreAuthSource` looks
//! a verifier up by username from the persisted `users` table for the vendored
//! SCRAM handshake; for an unknown user it synthesizes a DETERMINISTIC mock
//! verifier so the handshake fails on the same code path with the same timing
//! (no enumeration / timing oracle). `hash_password` / `upgrade_credential` are
//! the `fedq-server` subcommands that derive a verifier so the plaintext never
//! enters the config.

use std::sync::Arc;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;

use fq_catalog::store::ServerCatalog;
use fq_common::UserCredential;
use fq_scram::Verifier;
use pgwire::error::{PgWireError, PgWireResult};

/// A credential carries the stable identity captured alongside its verifier.
pub struct AuthCredential {
    pub verifier: Verifier,
    pub principal: Option<fq_common::database::PrincipalId>,
}

/// Server-owned credential lookup with deterministic unknown-user mocks.
pub struct StoreAuthSource {
    store: Arc<ServerCatalog>,
    iterations: u32,
    mock_secret: Vec<u8>,
}

impl StoreAuthSource {
    /// Build the source over the shared store, with a fresh per-server mock
    /// secret generated at startup.
    pub fn new(store: Arc<ServerCatalog>, iterations: u32) -> Self {
        Self {
            store,
            iterations,
            mock_secret: fq_scram::random_bytes(32),
        }
    }

    /// Look up a real verifier or an unknown user's deterministic mock.
    /// Metadata failures and malformed stored verifiers propagate unchanged.
    pub fn credential_for(&self, username: &str) -> PgWireResult<AuthCredential> {
        let user = self
            .store
            .get_user(username)
            .map_err(|error| PgWireError::ApiError(Box::new(error)))?;
        match user {
            Some(user) => Ok(AuthCredential {
                verifier: Verifier::parse(&user.verifier)
                    .map_err(|error| PgWireError::ApiError(Box::new(error)))?,
                principal: Some(user.id),
            }),
            None => Ok(AuthCredential {
                verifier: fq_scram::mock_verifier(username, &self.mock_secret, self.iterations),
                principal: None,
            }),
        }
    }
}

/// Derive a stored verifier for `user`/`password` at `iterations` with a fresh
/// random salt. The plaintext is used only to derive the one-way verifier and is
/// never returned or stored.
pub fn hash_password(
    user: &str,
    password: &str,
    superuser: bool,
    iterations: u32,
) -> Result<UserCredential, String> {
    let verifier = fq_scram::derive_verifier_random_salt(password, iterations)
        .map_err(|error| error.to_string())?;
    Ok(UserCredential {
        name: user.to_owned(),
        verifier: verifier.to_authid_string(),
        superuser,
    })
}

/// Reshape a pre-verifier credential (a base64 salt + base64 SaltedPassword) into
/// a verifier WITHOUT the plaintext: StoredKey/ServerKey are derivable from the
/// SaltedPassword. For an operator upgrading an old `server.users` block.
pub fn upgrade_credential(
    user: &str,
    salt_b64: &str,
    salted_password_b64: &str,
    superuser: bool,
    iterations: u32,
) -> Result<UserCredential, String> {
    let salt = STANDARD
        .decode(salt_b64)
        .map_err(|error| format!("invalid base64 salt: {error}"))?;
    let salted = STANDARD
        .decode(salted_password_b64)
        .map_err(|error| format!("invalid base64 salted_password: {error}"))?;
    let verifier = fq_scram::upgrade_from_salted_password(&salted, salt, iterations)
        .map_err(|error| error.to_string())?;
    Ok(UserCredential {
        name: user.to_owned(),
        verifier: verifier.to_authid_string(),
        superuser,
    })
}

/// Render a `UserCredential` as the YAML block that goes under `server.users`,
/// ready to paste into a config file. The verifier is a one-way credential; the
/// plaintext is never present.
pub fn credential_yaml(credential: &UserCredential) -> String {
    let mut block = format!(
        "    - name: {}\n      verifier: {}\n",
        credential.name, credential.verifier
    );
    if credential.superuser {
        block.push_str("      superuser: true\n");
    }
    block
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_hashed_password_yields_a_parseable_verifier() {
        let credential = hash_password("alice", "hunter2", true, 4096).expect("hash");
        assert_eq!(credential.name, "alice");
        assert!(credential.superuser);
        // The stored field is a verifier, never the plaintext or SaltedPassword.
        assert!(credential.verifier.starts_with("SCRAM-SHA-256$"));
        assert!(!credential.verifier.contains("hunter2"));
        assert!(Verifier::parse(&credential.verifier).is_ok());
    }

    #[test]
    fn the_yaml_block_carries_the_verifier_not_a_plaintext() {
        let credential = hash_password("bob", "secret-pw", false, 4096).expect("hash");
        let yaml = credential_yaml(&credential);
        assert!(yaml.contains("verifier: SCRAM-SHA-256$"));
        assert!(!yaml.contains("secret-pw"));
        assert!(!yaml.contains("superuser"), "non-superuser omits the flag");
    }

    #[test]
    fn upgrade_reshapes_a_salted_password_into_a_verifier() {
        // Derive an old-form SaltedPassword, then upgrade it and confirm the
        // resulting verifier matches a fresh derive of the same password.
        let salt = b"0123456789abcdef";
        let fresh = fq_scram::derive_verifier(salt_password_password(), salt, 4096).expect("fresh");
        let salted = old_salted_password(salt_password_password(), salt, 4096);
        let upgraded = upgrade_credential(
            "carol",
            &STANDARD.encode(salt),
            &STANDARD.encode(salted),
            false,
            4096,
        )
        .expect("upgrade");
        assert_eq!(
            upgraded.verifier,
            fresh.to_authid_string(),
            "upgrade reproduces the verifier a fresh derive makes"
        );
    }

    /// The plaintext the upgrade round-trip test derives from.
    fn salt_password_password() -> &'static str {
        "hunter2"
    }

    /// The old-form SaltedPassword: PBKDF2 of the password (what the pre-verifier
    /// store held).
    fn old_salted_password(password: &str, salt: &[u8], iterations: u32) -> Vec<u8> {
        use ring::pbkdf2;
        use std::num::NonZeroU32;
        let mut out = [0u8; 32];
        pbkdf2::derive(
            pbkdf2::PBKDF2_HMAC_SHA256,
            NonZeroU32::new(iterations).unwrap(),
            salt,
            password.as_bytes(),
            &mut out,
        );
        out.to_vec()
    }
    /// Allocate a standalone identity store for a credential-lookup regression.
    fn identity_store(tag: &str) -> Arc<ServerCatalog> {
        let dir = std::env::temp_dir().join(format!("fq_auth_{tag}_{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("create identity directory");
        let path = dir.join("identities.sqlite");
        Arc::new(ServerCatalog::open(&path.to_string_lossy()).expect("open identities"))
    }

    /// A missing identity uses the mock proof path without opening data stores.
    #[test]
    fn unknown_user_gets_a_mock_verifier() {
        let source = StoreAuthSource::new(identity_store("unknown"), 4096);
        assert!(source
            .credential_for("unknown")
            .expect("mock")
            .principal
            .is_none());
    }

    /// A verified credential keeps the identity that existed at the first SCRAM lookup.
    #[test]
    fn credential_capture_does_not_follow_username_recreation() {
        let store = identity_store("captured");
        let verifier = hash_password("alice", "pw", false, 4096)
            .expect("hash")
            .verifier;
        store
            .create_user("alice", &verifier, false, "test")
            .expect("create");
        let source = StoreAuthSource::new(Arc::clone(&store), 4096);
        let captured = source.credential_for("alice").expect("lookup");
        store.drop_user("alice").expect("drop");
        store
            .create_user("alice", &verifier, false, "test")
            .expect("recreate");
        assert_ne!(
            captured.principal,
            source
                .credential_for("alice")
                .expect("new credential")
                .principal
        );
        assert!(store
            .get_user_by_id(captured.principal.expect("real identity"))
            .expect("old identity")
            .is_none());
    }

    /// Invalid stored credentials surface their parse failure rather than a mock.
    #[test]
    fn malformed_stored_verifier_raises() {
        let store = identity_store("malformed");
        store
            .create_user("broken", "invalid-verifier", false, "test")
            .expect("seed");
        let source = StoreAuthSource::new(store, 4096);
        assert!(matches!(
            source.credential_for("broken"),
            Err(PgWireError::ApiError(_))
        ));
    }
}
