//! The fedq-server binary: parse flags, load a config, and serve the Postgres
//! wire protocol over the federated engine.
//!
//! Flags:
//!   --config <path>   YAML engine config (required; same format the engine CLI
//!                     and tests use).
//!   --listen <addr>   host:port to bind (default 127.0.0.1:5432).
//!
//! Subcommand:
//!   hash-password <user> <password>   Print the `server.users` YAML block for a
//!                     user, storing a SCRAM salted hash instead of the plaintext.

use std::process::exit;

use fedq_server::{credential_yaml, hash_password, serve};
use fq_common::load_config;
use tokio::net::TcpListener;

/// Parsed command-line arguments.
struct Args {
    config: String,
    listen: String,
}

/// The usage text, shown on `--help` and on any argument error.
fn usage() -> String {
    "usage: fedq-server --config <path> [--listen <host:port>]\n       \
     fedq-server hash-password <user> <password>"
        .to_owned()
}

/// Print the `server.users` YAML block for `user`/`password` and exit. The
/// plaintext password is used only to derive the SCRAM salted hash; only the salt
/// and salted hash are printed, so the config file never holds the plaintext.
fn run_hash_password(mut args: impl Iterator<Item = String>) -> ! {
    let user = args.next();
    let password = args.next();
    let (Some(user), Some(password)) = (user, password) else {
        eprintln!("hash-password requires <user> <password>\n{}", usage());
        exit(2);
    };
    let credential = hash_password(&user, &password);
    print!("{}", credential_yaml(&credential));
    exit(0);
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
