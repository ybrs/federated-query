//! The fedq command-line front end over the federated query engine.
//!
//! One-shot: `fedq --config <yaml> --command "<sql>"` (or `--file <path.sql>`)
//! runs a single statement, prints the result in the requested `--format`
//! (table, csv, or json), and exits nonzero with the error on stderr if it
//! fails. With neither `--command` nor `--file`, the same config opens an
//! interactive REPL (see the `repl` module). Argument parsing is clap-derived so
//! the flag surface and `--help` stay defined in one place.

mod render;
mod repl;

use std::path::PathBuf;
use std::process::exit;

use clap::Parser;
use fq_common::load_config;
use fq_runtime::Runtime;

use render::{render, OutputFormat};

/// The fedq command line. A `--config` is always required (it builds the engine
/// session); `--command` and `--file` are the two mutually exclusive one-shot
/// sources, and their absence selects the interactive shell.
#[derive(Parser)]
#[command(name = "fedq", about = "Run federated SQL from the command line.")]
struct Cli {
    /// Path to the YAML engine config (datasources, optimizer, cost, ...).
    #[arg(short, long, value_name = "YAML")]
    config: PathBuf,

    /// A single SQL statement to run, then exit (one-shot mode).
    #[arg(long, value_name = "SQL", conflicts_with = "file")]
    command: Option<String>,

    /// A file whose contents are one SQL statement to run, then exit.
    #[arg(long, value_name = "PATH")]
    file: Option<PathBuf>,

    /// The output format for results.
    #[arg(long, value_enum, default_value = "table")]
    format: OutputFormat,
}

/// Parse arguments, then run one-shot or interactive. Any failure prints to
/// stderr and exits with status 1; clap already exits 2 on an argument error.
fn main() {
    let cli = Cli::parse();
    if let Err(message) = run(&cli) {
        eprintln!("{message}");
        exit(1);
    }
}

/// Build the runtime from the config and dispatch to one-shot or interactive
/// execution. Every failure is returned as a message for `main` to print.
fn run(cli: &Cli) -> Result<(), String> {
    let config_path = cli
        .config
        .to_str()
        .ok_or_else(|| format!("config path is not valid UTF-8: {}", cli.config.display()))?;
    let config = load_config(config_path)
        .map_err(|error| format!("failed to load config {config_path}: {error}"))?;
    let runtime = Runtime::from_config(&config).map_err(|error| error.to_string())?;
    match one_shot_sql(cli)? {
        Some(sql) => run_once(&runtime, &sql, cli.format),
        None => repl::run(&runtime, cli.format).map_err(|error| error.to_string()),
    }
}

/// The one-shot SQL to run: the `--command` text, or the contents of `--file`,
/// or `None` when neither is given (interactive mode). clap has already rejected
/// giving both.
fn one_shot_sql(cli: &Cli) -> Result<Option<String>, String> {
    if let Some(command) = &cli.command {
        return Ok(Some(command.clone()));
    }
    if let Some(path) = &cli.file {
        let text = std::fs::read_to_string(path)
            .map_err(|error| format!("failed to read {}: {error}", path.display()))?;
        return Ok(Some(text));
    }
    Ok(None)
}

/// Execute one statement and print its rendered result to stdout. A blank input
/// (an empty command or a file of only whitespace/`;`) is a usage error rather
/// than a silent no-op. A trailing `;` is stripped; an interior one is left for
/// the parser to reject loudly.
fn run_once(runtime: &Runtime, sql: &str, format: OutputFormat) -> Result<(), String> {
    let statement = sql.trim().trim_end_matches(';').trim();
    if statement.is_empty() {
        return Err("no SQL statement to execute".to_owned());
    }
    let (schema, batches) = runtime
        .execute(statement)
        .map_err(|error| error.to_string())?;
    let text = render(format, &schema, &batches).map_err(|error| error.to_string())?;
    println!("{text}");
    Ok(())
}
