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

use fq_accel::Accelerator;
use fq_common::UserCredential;
use fq_scram::Verifier;

/// The verifier source the vendored SCRAM handshake queries: the persisted
/// `users` table, with a mock verifier for unknown users. It holds the store, the
/// iteration count NEW mock salts advertise, and a per-server random secret the
/// mock verifier is derived from (never persisted to a client-visible place).
pub struct StoreAuthSource {
    store: Arc<Accelerator>,
    iterations: u32,
    mock_secret: Vec<u8>,
}

impl StoreAuthSource {
    /// Build the source over the shared store, with a fresh per-server mock
    /// secret generated at startup.
    pub fn new(store: Arc<Accelerator>, iterations: u32) -> Self {
        Self {
            store,
            iterations,
            mock_secret: fq_scram::random_bytes(32),
        }
    }

    /// The verifier to check `username`'s login against: the stored verifier, or a
    /// deterministic mock for an unknown user (or an unreadable/malformed stored
    /// verifier). The handshake runs its full proof check either way, failing at
    /// the end exactly as a wrong password does, so "no such user" is
    /// indistinguishable from "wrong password".
    pub fn verifier_for(&self, username: &str) -> Verifier {
        if let Ok(Some(user)) = self.store.get_user(username) {
            if let Ok(verifier) = Verifier::parse(&user.verifier) {
                return verifier;
            }
        }
        fq_scram::mock_verifier(username, &self.mock_secret, self.iterations)
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
}
