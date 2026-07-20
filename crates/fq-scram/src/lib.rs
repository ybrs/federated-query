//! fq-scram: SCRAM-SHA-256 verifier derivation and RFC-5802 server-side proof
//! verification, stored in the PostgreSQL `pg_authid` shape.
//!
//! The engine NEVER stores a plaintext password or the `SaltedPassword` master
//! secret. It stores a VERIFIER - `salt`, iteration count, `StoredKey`,
//! `ServerKey` - from which client authentication cannot be forged (recovering
//! `ClientKey` needs a `SHA-256` preimage of `StoredKey`); a stolen verifier
//! yields only server impersonation and an offline dictionary attack. This crate
//! provides:
//!
//! - [`derive_verifier`] / [`derive_verifier_random_salt`]: hash a plaintext
//!   into a verifier (CREATE/ALTER USER, `hash-password`).
//! - [`Verifier`]: the verifier value, its `pg_authid` string
//!   ([`Verifier::to_authid_string`] / [`Verifier::parse`]), and a redacting
//!   `Debug`.
//! - [`upgrade_from_salted_password`]: reshape a pre-verifier `SaltedPassword`
//!   credential into a verifier without the plaintext.
//! - [`mock_verifier`]: a deterministic verifier for an unknown user, so the
//!   handshake fails on the same path with the same timing (no enumeration).
//! - [`ScramServer`]: the server side of the handshake, verifying a client proof
//!   from `StoredKey`/`ServerKey` (what pgwire's stock `ScramAuth` cannot do).
//!
//! It is pgwire-free; fedq-server wraps [`ScramServer`] behind a pgwire
//! `StartupHandler`.

pub mod error;
pub mod handshake;
pub mod verifier;

pub use error::ScramError;
pub use handshake::{Authenticated, ScramServer, MECHANISM};
pub use verifier::{
    derive_verifier, derive_verifier_random_salt, mock_verifier, random_bytes,
    upgrade_from_salted_password, Verifier, MIN_ITERATIONS,
};
