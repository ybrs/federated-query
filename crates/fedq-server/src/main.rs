//! The fedq-server binary: parse flags, load a config, and serve the Postgres
//! wire protocol over the federated engine.
//!
//! Flags:
//!   --config <path>   YAML engine config (required; same format the engine CLI
//!                     and tests use).
//!   --listen <addr>   host:port to bind (default 127.0.0.1:5432).
//!
//! Subcommands:
//!   hash-password [--superuser] <user> <password>
//!                     Print the `server.users` YAML block for a user, storing a
//!                     SCRAM VERIFIER (never the plaintext or SaltedPassword).
//!   upgrade-credential [--superuser] <user> <salt> <salted_password>
//!                     Reshape a pre-verifier credential (base64 salt + base64
//!                     SaltedPassword) into a verifier block, without the plaintext.

use std::process::exit;

use fedq_server::{credential_yaml, hash_password, serve, upgrade_credential};
use fq_common::{load_config, SCRAM_ITERATIONS};
use tokio::net::TcpListener;

/// Parsed command-line arguments.
struct Args {
    config: String,
    listen: String,
}

/// The usage text, shown on `--help` and on any argument error.
fn usage() -> String {
    "usage: fedq-server --config <path> [--listen <host:port>]\n       \
     fedq-server hash-password [--superuser] <user> <password>\n       \
     fedq-server upgrade-credential [--superuser] <user> <salt> <salted_password>"
        .to_owned()
}

/// Consume a leading `--superuser` flag from the argument list, returning whether
/// it was present.
fn take_superuser_flag(args: &mut std::iter::Peekable<impl Iterator<Item = String>>) -> bool {
    if args.peek().is_some_and(|arg| arg == "--superuser") {
        args.next();
        return true;
    }
    false
}

/// Print the `server.users` YAML block for `user`/`password` and exit. The
/// plaintext is used only to derive the SCRAM verifier; only the verifier is
/// printed, so the config file never holds the plaintext or the SaltedPassword.
fn run_hash_password(mut args: impl Iterator<Item = String>) -> ! {
    let mut args = args.by_ref().peekable();
    let superuser = take_superuser_flag(&mut args);
    let (Some(user), Some(password)) = (args.next(), args.next()) else {
        eprintln!("hash-password requires <user> <password>\n{}", usage());
        exit(2);
    };
    match hash_password(&user, &password, superuser, SCRAM_ITERATIONS) {
        Ok(credential) => {
            print!("{}", credential_yaml(&credential));
            exit(0);
        }
        Err(error) => {
            eprintln!("hash-password failed: {error}");
            exit(2);
        }
    }
}

/// Reshape a pre-verifier credential (base64 salt + base64 SaltedPassword) into a
/// verifier YAML block and exit. No plaintext is involved.
fn run_upgrade_credential(mut args: impl Iterator<Item = String>) -> ! {
    let mut args = args.by_ref().peekable();
    let superuser = take_superuser_flag(&mut args);
    let (Some(user), Some(salt), Some(salted)) = (args.next(), args.next(), args.next()) else {
        eprintln!(
            "upgrade-credential requires <user> <salt> <salted_password>\n{}",
            usage()
        );
        exit(2);
    };
    match upgrade_credential(&user, &salt, &salted, superuser, SCRAM_ITERATIONS) {
        Ok(credential) => {
            print!("{}", credential_yaml(&credential));
            exit(0);
        }
        Err(error) => {
            eprintln!("upgrade-credential failed: {error}");
            exit(2);
        }
    }
}

/// Read the value that must follow a flag, or report the flag as missing it.
fn next_value(args: &mut impl Iterator<Item = String>, flag: &str) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("{flag} requires a value\n{}", usage()))
}

/// Parse the argument list into `Args`. Unknown flags and a missing required
/// `--config` fail loudly; a missing `--listen` defaults to 127.0.0.1:5432.
fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Args, String> {
    let mut config = None;
    let mut listen = "127.0.0.1:5432".to_owned();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--config" => config = Some(next_value(&mut args, "--config")?),
            "--listen" => listen = next_value(&mut args, "--listen")?,
            other => return Err(format!("unknown argument: {other}\n{}", usage())),
        }
    }
    let config = config.ok_or_else(|| format!("--config <path> is required\n{}", usage()))?;
    Ok(Args { config, listen })
}

#[tokio::main]
async fn main() {
    let mut raw = std::env::args().skip(1).peekable();
    if raw.peek().is_some_and(|arg| arg == "--help" || arg == "-h") {
        println!("{}", usage());
        return;
    }
    if raw.peek().is_some_and(|arg| arg == "hash-password") {
        raw.next();
        run_hash_password(raw);
    }
    if raw.peek().is_some_and(|arg| arg == "upgrade-credential") {
        raw.next();
        run_upgrade_credential(raw);
    }
    // Audit lines (login success/failure, DDL, denied access) go to the server
    // log; install the subscriber before serving so they are emitted.
    fq_common::setup_logging("info", false);

    let args = match parse_args(raw) {
        Ok(args) => args,
        Err(message) => {
            eprintln!("{message}");
            exit(2);
        }
    };

    let config = match load_config(&args.config) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("failed to load config {}: {error}", args.config);
            exit(1);
        }
    };

    let listener = match TcpListener::bind(&args.listen).await {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!("failed to bind {}: {error}", args.listen);
            exit(1);
        }
    };
    println!(
        "fedq-server listening on {} (config {})",
        args.listen, args.config
    );

    if let Err(error) = serve(config, listener).await {
        eprintln!("server stopped: {error}");
        exit(1);
    }
}
