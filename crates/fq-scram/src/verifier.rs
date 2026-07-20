//! The SCRAM-SHA-256 verifier: derivation, the `pg_authid` string, and the
//! primitives the handshake verification shares.
//!
//! Given a plaintext password, a random salt, and an iteration count `i`:
//!
//! ```text
//! SaltedPassword = PBKDF2-HMAC-SHA-256(SASLprep(password), salt, i)
//! ClientKey      = HMAC-SHA-256(SaltedPassword, "Client Key")
//! StoredKey      = SHA-256(ClientKey)
//! ServerKey      = HMAC-SHA-256(SaltedPassword, "Server Key")
//! ```
//!
//! STORED (the verifier): `salt`, `i`, `StoredKey`, `ServerKey`, serialized as
//! the PostgreSQL `pg_authid.rolpassword` literal
//! `SCRAM-SHA-256$<i>:<base64(salt)>$<base64(StoredKey)>:<base64(ServerKey)>`.
//! The plaintext, `SaltedPassword`, and `ClientKey` are never persisted: a
//! stolen verifier does not yield client-auth capability (recovering `ClientKey`
//! needs a `SHA-256` preimage of `StoredKey`), only server impersonation and an
//! offline dictionary attack whose work factor is `i` PBKDF2 iterations/guess.

use std::borrow::Cow;
use std::num::NonZeroU32;

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use rand::RngCore;
use ring::{digest, hmac, pbkdf2};

use crate::error::ScramError;

/// The RFC 7677 floor for the SCRAM-SHA-256 iteration count. A verifier derived
/// below this is refused; the server default and every `CREATE USER` clamp up.
pub const MIN_ITERATIONS: u32 = 4096;

/// The length of a SCRAM SHA-256 key (StoredKey / ServerKey), in bytes.
pub const KEY_LEN: usize = 32;

/// The random salt length, in bytes, for a freshly derived verifier.
pub const SALT_LEN: usize = 16;

/// The `pg_authid` scheme prefix every serialized verifier carries.
const SCHEME: &str = "SCRAM-SHA-256";

/// A stored SCRAM-SHA-256 verifier: everything the server keeps for one user's
/// password and everything the handshake verifies a login against. It holds NO
/// secret from which client authentication can be forged.
#[derive(Clone, PartialEq, Eq)]
pub struct Verifier {
    /// The PBKDF2 iteration count this verifier was derived with, advertised to
    /// the client per handshake (so users may carry different counts).
    pub iterations: u32,
    /// The random salt the plaintext was PBKDF2-stretched with.
    pub salt: Vec<u8>,
    /// `SHA-256(ClientKey)`: verifies a client proof (preimage-resistant, so it
    /// does not yield `ClientKey`).
    pub stored_key: [u8; KEY_LEN],
    /// `HMAC-SHA-256(SaltedPassword, "Server Key")`: signs the server's proof.
    pub server_key: [u8; KEY_LEN],
}

impl Verifier {
    /// Serialize as the `pg_authid.rolpassword` literal
    /// `SCRAM-SHA-256$<i>:<b64 salt>$<b64 StoredKey>:<b64 ServerKey>`.
    pub fn to_authid_string(&self) -> String {
        format!(
            "{SCHEME}${}:{}${}:{}",
            self.iterations,
            STANDARD.encode(&self.salt),
            STANDARD.encode(self.stored_key),
            STANDARD.encode(self.server_key),
        )
    }

    /// Parse a `pg_authid` verifier string, raising on any malformed part rather
    /// than accepting a credential the handshake would then misverify.
    pub fn parse(text: &str) -> Result<Self, ScramError> {
        let rest = text
            .strip_prefix(SCHEME)
            .and_then(|rest| rest.strip_prefix('$'))
            .ok_or_else(|| ScramError::InvalidVerifier("expected SCRAM-SHA-256$ prefix".into()))?;
        let (iter_salt, keys) = rest
            .split_once('$')
            .ok_or_else(|| ScramError::InvalidVerifier("missing '$' before the keys".into()))?;
        let (iterations, salt) = parse_iterations_and_salt(iter_salt)?;
        let (stored_key, server_key) = parse_key_pair(keys)?;
        Ok(Self {
            iterations,
            salt,
            stored_key,
            server_key,
        })
    }
}

/// Redact the verifier under `Debug`: it is not a plaintext password but is
/// sensitive (enables the offline attack and server impersonation), so it is
/// never rendered verbatim into a log or panic message.
impl std::fmt::Debug for Verifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Verifier")
            .field("iterations", &self.iterations)
            .field("salt", &"<redacted>")
            .field("stored_key", &"<redacted>")
            .field("server_key", &"<redacted>")
            .finish()
    }
}

/// Derive a verifier from a plaintext password, a salt, and an iteration count.
/// The plaintext is SASLprep-normalized (matching PostgreSQL and the pgwire
/// client) and used only to compute the one-way keys; it is not retained.
pub fn derive_verifier(
    password: &str,
    salt: &[u8],
    iterations: u32,
) -> Result<Verifier, ScramError> {
    let iterations = clamp_iterations(iterations)?;
    let salted = salted_password(password, salt, iterations)?;
    Ok(from_salted_password(&salted, salt.to_vec(), iterations))
}

/// Derive a verifier with a fresh random 16-byte salt.
pub fn derive_verifier_random_salt(
    password: &str,
    iterations: u32,
) -> Result<Verifier, ScramError> {
    derive_verifier(password, &random_bytes(SALT_LEN), iterations)
}

/// Reconstruct a verifier from a stored `SaltedPassword` (the weaker
/// pre-verifier credential): `StoredKey`/`ServerKey` ARE derivable from it, so an
/// operator holding the old form upgrades to a verifier without the plaintext.
pub fn upgrade_from_salted_password(
    salted_password: &[u8],
    salt: Vec<u8>,
    iterations: u32,
) -> Result<Verifier, ScramError> {
    let iterations = clamp_iterations(iterations)?;
    Ok(from_salted_password(salted_password, salt, iterations))
}

/// A DETERMINISTIC mock verifier for an unknown user, derived from the username
/// and a server-wide secret. The handshake runs its FULL proof check against
/// this and fails at the end exactly as a wrong password does - same code path,
/// same timing - so an attacker cannot distinguish "no such user" from "wrong
/// password" (mirrors PostgreSQL's `scram_mock_salt`).
pub fn mock_verifier(username: &str, server_secret: &[u8], iterations: u32) -> Verifier {
    // Clamp is infallible here: a mock never derives below the floor because the
    // keys are HMAC outputs, not PBKDF2 - but the advertised count must still be
    // a valid one, so the same floor applies.
    let iterations = iterations.max(MIN_ITERATIONS);
    let key = hmac::Key::new(hmac::HMAC_SHA256, server_secret);
    let salt = mock_field(&key, username, b"salt")[..SALT_LEN].to_vec();
    Verifier {
        iterations,
        salt,
        stored_key: mock_field(&key, username, b"stored"),
        server_key: mock_field(&key, username, b"server"),
    }
}

/// Fill `n` bytes from the OS CSPRNG (fresh salts, the server mock secret).
pub fn random_bytes(n: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; n];
    rand::rng().fill_bytes(&mut bytes);
    bytes
}

/// One deterministic 32-byte mock field: `HMAC(secret, username || "|" || tag)`.
fn mock_field(key: &hmac::Key, username: &str, tag: &[u8]) -> [u8; KEY_LEN] {
    let mut message = username.as_bytes().to_vec();
    message.push(b'|');
    message.extend_from_slice(tag);
    let tag = hmac::sign(key, &message);
    let mut out = [0u8; KEY_LEN];
    out.copy_from_slice(tag.as_ref());
    out
}

/// Build the `{StoredKey, ServerKey}` half of a verifier from a `SaltedPassword`.
fn from_salted_password(salted_password: &[u8], salt: Vec<u8>, iterations: u32) -> Verifier {
    let client_key = hmac_sha256(salted_password, b"Client Key");
    let stored_key = sha256(&client_key);
    let server_key = hmac_sha256(salted_password, b"Server Key");
    Verifier {
        iterations,
        salt,
        stored_key,
        server_key,
    }
}

/// `PBKDF2-HMAC-SHA-256(SASLprep(password), salt, iterations)` - the 32-byte
/// SaltedPassword. An empty password is refused: an empty credential would let a
/// bare handshake authenticate.
fn salted_password(
    password: &str,
    salt: &[u8],
    iterations: u32,
) -> Result<[u8; KEY_LEN], ScramError> {
    if password.is_empty() {
        return Err(ScramError::EmptyPassword);
    }
    // PostgreSQL falls back to the raw password when SASLprep cannot normalize it
    // (e.g. a byte string that is not valid stringprep input), rather than failing.
    let normalized = stringprep::saslprep(password).unwrap_or(Cow::Borrowed(password));
    let count = NonZeroU32::new(iterations).ok_or(ScramError::EmptyPassword)?;
    let mut out = [0u8; KEY_LEN];
    pbkdf2::derive(
        pbkdf2::PBKDF2_HMAC_SHA256,
        count,
        salt,
        normalized.as_bytes(),
        &mut out,
    );
    Ok(out)
}

/// `HMAC-SHA-256(key, message)` as a fixed 32-byte array.
pub(crate) fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; KEY_LEN] {
    let mac = hmac::Key::new(hmac::HMAC_SHA256, key);
    let tag = hmac::sign(&mac, message);
    let mut out = [0u8; KEY_LEN];
    out.copy_from_slice(tag.as_ref());
    out
}

/// `SHA-256(message)` as a fixed 32-byte array.
pub(crate) fn sha256(message: &[u8]) -> [u8; KEY_LEN] {
    let hash = digest::digest(&digest::SHA256, message);
    let mut out = [0u8; KEY_LEN];
    out.copy_from_slice(hash.as_ref());
    out
}

/// Clamp an iteration count to the RFC 7677 floor, refusing a zero count that
/// PBKDF2 cannot use.
fn clamp_iterations(iterations: u32) -> Result<u32, ScramError> {
    if iterations == 0 {
        return Err(ScramError::InvalidVerifier(
            "iteration count is zero".into(),
        ));
    }
    Ok(iterations.max(MIN_ITERATIONS))
}

/// Parse the `<i>:<base64 salt>` half of a verifier string.
fn parse_iterations_and_salt(text: &str) -> Result<(u32, Vec<u8>), ScramError> {
    let (iterations, salt) = text.split_once(':').ok_or_else(|| {
        ScramError::InvalidVerifier("missing ':' after the iteration count".into())
    })?;
    let iterations = iterations
        .parse::<u32>()
        .map_err(|_| ScramError::InvalidVerifier(format!("bad iteration count '{iterations}'")))?;
    let salt = STANDARD
        .decode(salt)
        .map_err(|error| ScramError::InvalidVerifier(format!("bad base64 salt: {error}")))?;
    Ok((iterations, salt))
}

/// Parse the `<base64 StoredKey>:<base64 ServerKey>` half of a verifier string.
fn parse_key_pair(text: &str) -> Result<([u8; KEY_LEN], [u8; KEY_LEN]), ScramError> {
    let (stored, server) = text
        .split_once(':')
        .ok_or_else(|| ScramError::InvalidVerifier("missing ':' between the keys".into()))?;
    Ok((
        decode_key(stored, "StoredKey")?,
        decode_key(server, "ServerKey")?,
    ))
}

/// Decode one base64 key into a fixed 32-byte array, naming which key on error.
fn decode_key(text: &str, which: &str) -> Result<[u8; KEY_LEN], ScramError> {
    let bytes = STANDARD
        .decode(text)
        .map_err(|error| ScramError::InvalidVerifier(format!("bad base64 {which}: {error}")))?;
    let array: [u8; KEY_LEN] = bytes
        .try_into()
        .map_err(|_| ScramError::InvalidVerifier(format!("{which} is not {KEY_LEN} bytes")))?;
    Ok(array)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rfc7677_published_vector_pins_derivation_and_verification() {
        // The official RFC 7677 section 3 SCRAM-SHA-256 exchange (username
        // `user`, password `pencil`). Deriving the verifier from the password
        // and then verifying the RFC's published ClientProof and reproducing its
        // published ServerSignature pins BOTH the PBKDF2/key derivation AND the
        // proof-verification math against the standard, not just an internal
        // self-consistent loop.
        let salt = STANDARD.decode("W22ZaJ0SNY7soEsUEjb6gQ==").expect("salt");
        let verifier = derive_verifier("pencil", &salt, 4096).expect("derive");

        let auth_message = "n=user,r=rOprNGfwEbeRWgbNEkqO,\
             r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,\
             s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096,\
             c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";

        // Recover ClientKey = ClientProof XOR HMAC(StoredKey, AuthMessage) and
        // check SHA-256(ClientKey) == StoredKey, exactly as the server does.
        let proof = STANDARD
            .decode("dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=")
            .expect("proof");
        let client_signature = hmac_sha256(&verifier.stored_key, auth_message.as_bytes());
        let mut client_key = [0u8; KEY_LEN];
        for index in 0..KEY_LEN {
            client_key[index] = proof[index] ^ client_signature[index];
        }
        assert_eq!(
            sha256(&client_key),
            verifier.stored_key,
            "the RFC 7677 client proof must verify against the derived StoredKey"
        );

        // The derived ServerKey reproduces the RFC's ServerSignature.
        let server_signature = hmac_sha256(&verifier.server_key, auth_message.as_bytes());
        assert_eq!(
            STANDARD.encode(server_signature),
            "6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=",
            "the server signature must match the RFC 7677 published value"
        );
    }

    #[test]
    fn a_verifier_round_trips_through_its_authid_string() {
        let verifier = derive_verifier("hunter2", b"0123456789abcdef", 4096).expect("derive");
        let text = verifier.to_authid_string();
        assert!(text.starts_with("SCRAM-SHA-256$4096:"));
        let parsed = Verifier::parse(&text).expect("parse");
        assert_eq!(parsed, verifier);
    }

    #[test]
    fn upgrade_from_salted_password_matches_a_fresh_derive() {
        // The salted password is the pre-verifier credential; upgrading it must
        // reproduce exactly the verifier a fresh derive of the same inputs makes.
        let salt = b"0123456789abcdef".to_vec();
        let salted = salted_password("hunter2", &salt, 4096).expect("salt");
        let upgraded = upgrade_from_salted_password(&salted, salt.clone(), 4096).expect("upgrade");
        let fresh = derive_verifier("hunter2", &salt, 4096).expect("derive");
        assert_eq!(upgraded, fresh);
    }

    #[test]
    fn iterations_below_the_floor_clamp_up() {
        let verifier = derive_verifier("pw", b"saltsaltsaltsalt", 100).expect("derive");
        assert_eq!(verifier.iterations, MIN_ITERATIONS);
    }

    #[test]
    fn an_empty_password_is_refused() {
        assert!(matches!(
            derive_verifier("", b"saltsaltsaltsalt", 4096),
            Err(ScramError::EmptyPassword)
        ));
    }

    #[test]
    fn a_malformed_verifier_string_raises() {
        for bad in [
            "not-a-verifier",
            "SCRAM-SHA-256$abc:c2FsdA==$AAAA:BBBB",
            "SCRAM-SHA-256$4096:@@@$AAAA:BBBB",
            "SCRAM-SHA-256$4096:c2FsdA==$onlyone",
        ] {
            assert!(Verifier::parse(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn a_mock_verifier_is_deterministic_and_well_formed() {
        let secret = b"server-wide-secret";
        let a = mock_verifier("ghost", secret, 4096);
        let b = mock_verifier("ghost", secret, 4096);
        assert_eq!(a, b);
        assert_eq!(a.salt.len(), SALT_LEN);
        // A different username yields a different verifier (no fixed mock).
        assert_ne!(mock_verifier("other", secret, 4096), a);
        // It round-trips through the pg_authid string like a real verifier.
        assert_eq!(Verifier::parse(&a.to_authid_string()).expect("parse"), a);
    }
}
