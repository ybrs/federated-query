//! `fq-index` - a call-graph index of this workspace and everything it links.
//!
//! Build it once, then ask it questions from a shell:
//!
//! ```text
//! fq-index build --root . --db fq-index.sqlite --workspace
//! fq-index build --root . --db fq-index.sqlite --deps
//! fq-index defs Runtime::execute
//! fq-index callers StatsCatalog::open
//! fq-index calls fq_runtime::Runtime::from_config
//! fq-index reach --from fq_runtime::Runtime::execute --to std::fs::remove_file
//! fq-index coverage
//! ```
//!
//! The index is built by rust-analyzer, so a call is resolved with real name
//! resolution and type inference rather than by matching text. What that still
//! cannot decide is listed by `fq-index coverage` and repeated under every
//! `reach` answer: calls through function pointers and closures, calls on a
//! `dyn Trait` receiver (reported as a separate, wider bound), macro bodies that
//! did not expand, and anything behind an FFI boundary - a C library's behaviour
//! is invisible here, including SQL that a database engine executes.
//!
//! A build writes each crate to the index as it finishes it and reports progress
//! to a log file, never to a terminal. A build that is killed leaves every crate
//! it completed; running it again continues from there.

mod index;
mod log;
mod query;
mod store;

use std::collections::HashSet;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

/// The `ra_ap_*` release the index was resolved with. It is pinned in the
/// manifest because the API and the resolution behaviour both change between
/// releases; recording it makes an index self-describing.
const RA_AP_VERSION: &str = "0.0.344";

/// How many unread crates a reachability answer names one by one before it
/// counts the rest. The whole list runs to hundreds of dependency crates; the
/// ones holding the most reached functions are the ones worth reading.
const UNREAD_CRATES_SHOWN: usize = 20;

/// The command line.
#[derive(Parser)]
#[command(
    name = "fq-index",
    about = "Call-graph index of the workspace and its dependencies"
)]
struct Cli {
    /// The index database. The same file is written by `build` and read by every
    /// other command.
    #[arg(long, default_value = "fq-index.sqlite", global = true)]
    db: String,
    #[command(subcommand)]
    command: Command,
}

/// What the invocation asks for.
#[derive(Subcommand)]
enum Command {
    /// Load the project into rust-analyzer and write the index, one crate at a
    /// time. Crates already in the index are skipped unless `--rebuild` is given.
    Build {
        /// The directory holding the workspace manifest.
        #[arg(long, default_value = ".")]
        root: PathBuf,
        /// Read the bodies of this repository's own crates.
        #[arg(long)]
        workspace: bool,
        /// Read the bodies of the dependency graph, `std` included. This is
        /// millions of lines and takes hours; it is resumable.
        #[arg(long)]
        deps: bool,
        /// Delete an existing index and start from nothing.
        #[arg(long)]
        rebuild: bool,
        /// Where the per-crate progress lines are appended. Defaults to the
        /// index file's path with a `.log` suffix.
        #[arg(long)]
        log: Option<PathBuf>,
    },
    /// Where a function is defined.
    Defs { pattern: String },
    /// Which call sites reach a function.
    Callers { pattern: String },
    /// Which calls a function makes, resolved or not.
    Calls { pattern: String },
    /// Every function that can transitively reach the matching ones.
    Ancestors { pattern: String },
    /// Whether one function can reach another, and by what path.
    Reach {
        /// Entry points; every item whose path matches is used.
        #[arg(long, required = true)]
        from: Vec<String>,
        /// Targets to look for among the reachable items.
        #[arg(long, required = true)]
        to: Vec<String>,
    },
    /// What the index contains and what it could not see.
    Coverage,
}

/// Dispatch the command, printing the failure and exiting non-zero on any error.
fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(&cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("fq-index: {error:#}");
            ExitCode::FAILURE
        }
    }
}

/// Run one command against the index named on the command line.
fn run(cli: &Cli) -> anyhow::Result<()> {
    match &cli.command {
        Command::Build {
            root,
            workspace,
            deps,
            rebuild,
            log,
        } => build(&BuildRequest {
            root: root.clone(),
            db_path: cli.db.clone(),
            scope: index::Scope {
                workspace: *workspace,
                deps: *deps,
            },
            rebuild: *rebuild,
            log_override: log.clone(),
        }),
        Command::Defs { pattern } => defs(&cli.db, pattern),
        Command::Callers { pattern } => callers(&cli.db, pattern),
        Command::Calls { pattern } => calls(&cli.db, pattern),
        Command::Ancestors { pattern } => ancestors(&cli.db, pattern),
        Command::Reach { from, to } => reach(&cli.db, from, to),
        Command::Coverage => coverage(&cli.db),
    }
}

/// Everything one build invocation was asked for.
struct BuildRequest {
    root: PathBuf,
    db_path: String,
    scope: index::Scope,
    rebuild: bool,
    /// The log file named on the command line, when one was.
    log_override: Option<PathBuf>,
}

impl BuildRequest {
    /// Where the progress lines go: what was asked for, or the index's own path
    /// with a `.log` suffix so the log always sits beside the index it describes.
    fn log_path(&self) -> PathBuf {
        self.log_override
            .clone()
            .unwrap_or_else(|| PathBuf::from(format!("{}.log", self.db_path)))
    }
}

/// Build or continue the index. Nothing is printed: progress goes to the log,
/// and the exit status says whether the run finished.
fn build(request: &BuildRequest) -> anyhow::Result<()> {
    if !request.scope.workspace && !request.scope.deps {
        anyhow::bail!("name what to read: --workspace, --deps, or both");
    }
    if request.rebuild {
        remove_existing_index(&request.db_path)?;
    }
    let log = log::FileLog::appending_to(&request.log_path())?;
    log.line(&format!(
        "build starting: root={} db={} scope={} rebuild={}",
        request.root.display(),
        request.db_path,
        request.scope.describe(),
        request.rebuild
    ));
    let outcome = build_logged(request, &log);
    if let Err(error) = &outcome {
        log.line(&format!("build FAILED: {error:#}"));
    }
    outcome
}

/// Load the project, scan the in-scope crates and record the build facts.
fn build_logged(request: &BuildRequest, log: &log::FileLog) -> anyhow::Result<()> {
    let started = std::time::Instant::now();
    log.line("loading the project into rust-analyzer");
    let (database, vfs) = index::load(&request.root, &|message| log.line(&message))?;
    log.line("project loaded");
    let mut connection = store::open_for_write(&request.db_path)?;
    let blind = index::build(&database, &vfs, &mut connection, &request.scope, log)?;
    record_build_facts(
        &connection,
        request,
        started.elapsed().as_secs_f64(),
        &blind,
    )?;
    log.line(&format!(
        "build finished in {:.1}s",
        started.elapsed().as_secs_f64()
    ));
    log_coverage(&connection, log)?;
    Ok(())
}

/// Delete an index and the files `SQLite` keeps beside it, so `--rebuild` starts
/// from nothing rather than from a mixture of two runs.
fn remove_existing_index(db_path: &str) -> anyhow::Result<()> {
    for suffix in ["", "-wal", "-shm"] {
        let path = PathBuf::from(format!("{db_path}{suffix}"));
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
    }
    Ok(())
}

/// Store how the index was built and what it could not resolve, so a later query
/// can print the limits without re-deriving them.
///
/// The blind-spot counters are ADDED to what is there: an index built over
/// several runs reports the totals of the index, not of its last run.
fn record_build_facts(
    connection: &rusqlite::Connection,
    request: &BuildRequest,
    seconds: f64,
    blind: &index::BlindSpots,
) -> anyhow::Result<()> {
    store::set_meta(connection, "root", &request.root.display().to_string())?;
    store::set_meta(connection, "last_run_seconds", &format!("{seconds:.1}"))?;
    store::add_meta(connection, "build_seconds_total", seconds as u64)?;
    store::set_meta(connection, "fq_index_version", env!("CARGO_PKG_VERSION"))?;
    store::set_meta(connection, "rust_analyzer_crates", RA_AP_VERSION)?;
    store::set_meta(connection, "scope", &request.scope.describe())?;
    store::set_meta(
        connection,
        "built_unix_seconds",
        &std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)?
            .as_secs()
            .to_string(),
    )?;
    for (key, value) in blind.rows() {
        store::add_meta(connection, key, value)?;
    }
    Ok(())
}

/// Print where the matching functions are defined.
fn defs(db_path: &str, pattern: &str) -> anyhow::Result<()> {
    let connection = store::open(db_path)?;
    let items = query::find_items(&connection, pattern)?;
    for item in &items {
        println!("{} [{}] {}:{}", item.path, item.kind, item.file, item.line);
    }
    println!("-- {} definitions match {pattern:?}", items.len());
    Ok(())
}

/// Print the call sites that reach the matching functions.
fn callers(db_path: &str, pattern: &str) -> anyhow::Result<()> {
    let connection = store::open(db_path)?;
    let ids = matching_ids(&connection, pattern)?;
    let sites = query::callers_of(&connection, &ids)?;
    for site in &sites {
        println!(
            "{} {}:{} [{}] {}",
            site.caller_path, site.file, site.line, site.resolution, site.text
        );
    }
    println!(
        "-- {} call sites reach {} definitions of {pattern:?}",
        sites.len(),
        ids.len()
    );
    println!("-- callers through a trait method appear only if the call resolved to that trait; run `reach` for dispatch");
    Ok(())
}

/// Print the calls the matching functions make.
fn calls(db_path: &str, pattern: &str) -> anyhow::Result<()> {
    let connection = store::open(db_path)?;
    let ids = matching_ids(&connection, pattern)?;
    let sites = query::calls_from(&connection, &ids)?;
    let mut unresolved = 0;
    for site in &sites {
        if site.resolution != "function" {
            unresolved += 1;
        }
        println!(
            "{}:{} [{}] {}",
            site.file, site.line, site.resolution, site.text
        );
    }
    println!(
        "-- {} calls, {unresolved} of them not resolved to a single function",
        sites.len()
    );
    Ok(())
}

/// Print every function that can transitively reach the matching ones, listing
/// the ones in this repository's own crates in full and counting the rest.
fn ancestors(db_path: &str, pattern: &str) -> anyhow::Result<()> {
    let connection = store::open(db_path)?;
    let ids = matching_ids(&connection, pattern)?;
    let graph = query::load_graph(&connection)?;
    let reaching = query::ancestors(&graph, &ids);
    let workspace = query::items_from_origin(&connection, &reaching, "workspace")?;
    for item in &workspace {
        println!("{} {}:{}", item.path, item.file, item.line);
    }
    println!(
        "-- {} functions can reach {} definitions of {pattern:?}; {} of them are in this workspace",
        reaching.len(),
        ids.len(),
        workspace.len()
    );
    println!("-- the walk crosses trait dispatch without checking the receiver's real type, so this is an upper bound");
    Ok(())
}

/// Walk the graph from the entry points and report which targets are reachable.
fn reach(db_path: &str, from: &[String], to: &[String]) -> anyhow::Result<()> {
    let connection = store::open(db_path)?;
    let entries = entry_ids(&connection, from)?;
    let graph = query::load_graph(&connection)?;
    let result = query::reachable(&graph, &entries);
    println!(
        "-- {} items reachable, {} of them through resolved calls only",
        result.parents.len(),
        result.static_only.len()
    );
    for pattern in to {
        report_target(&connection, &result, pattern)?;
    }
    report_reach_limits(&connection, &result)?;
    Ok(())
}

/// Print, for one target pattern, which of its definitions were reached and how.
fn report_target(
    connection: &rusqlite::Connection,
    result: &query::Reachability,
    pattern: &str,
) -> anyhow::Result<()> {
    let mut reached = 0;
    for item in query::find_items(connection, pattern)? {
        if !result.parents.contains_key(&item.id) {
            continue;
        }
        reached += 1;
        let certainty = if result.static_only.contains(&item.id) {
            "resolved calls only"
        } else {
            "only via trait dispatch"
        };
        println!(
            "REACHABLE {} ({certainty}) {}:{}",
            item.path, item.file, item.line
        );
        print_path(connection, result, item.id)?;
    }
    if reached == 0 {
        println!("NOT REACHABLE {pattern}");
    }
    Ok(())
}

/// Print the chain of calls that reached one item.
fn print_path(
    connection: &rusqlite::Connection,
    result: &query::Reachability,
    target: i64,
) -> anyhow::Result<()> {
    for (id, step) in query::path_to(result, target) {
        let marker = match step {
            query::Step::Static => "->",
            query::Step::Dyn => "~>",
        };
        println!("    {marker} {}", query::item_path(connection, id)?);
    }
    Ok(())
}

/// Print how many calls inside the reachable region the index could not resolve.
/// A "not reachable" answer is bounded by these numbers and by nothing else.
fn report_reach_limits(
    connection: &rusqlite::Connection,
    result: &query::Reachability,
) -> anyhow::Result<()> {
    let region: HashSet<i64> = result.parents.keys().copied().collect();
    println!("-- calls inside the reachable region, by resolution:");
    for (resolution, count) in query::unresolved_within(connection, &region)? {
        println!("     {resolution}: {count}");
    }
    println!("-- `~>` marks a step through a trait method, where the receiver's real type picks the implementation");
    println!("-- an unresolved, closure or fn_pointer call is an edge the index does NOT have");
    let mut lines = Vec::new();
    push_partial_crates(
        &mut lines,
        &query::partial_crates_of(connection, &region)?,
        "in the reachable region",
    );
    for line in lines {
        println!("{line}");
    }
    report_unread_crates(connection, &region)
}

/// Print the crates the walk reached but never read.
///
/// The walk stops at the doorstep of such a crate: its functions are named
/// because something called them, but what THEY call was never resolved. A
/// "not reachable" answer is bounded by exactly this list.
fn report_unread_crates(
    connection: &rusqlite::Connection,
    region: &HashSet<i64>,
) -> anyhow::Result<()> {
    let crates = query::unscanned_crates_of(connection, region)?;
    if crates.is_empty() {
        println!("-- every crate the walk reached had its bodies read");
        return Ok(());
    }
    let mut total_items = 0;
    for (_, reached) in &crates {
        total_items += reached;
    }
    println!(
        "-- {} crates were reached but NEVER read ({total_items} of the reached functions are \
         theirs); what those functions call is not in the index:",
        crates.len()
    );
    for (name, reached) in crates.iter().take(UNREAD_CRATES_SHOWN) {
        println!("     {name}: {reached} functions reached");
    }
    if crates.len() > UNREAD_CRATES_SHOWN {
        println!("     ... and {} more", crates.len() - UNREAD_CRATES_SHOWN);
    }
    Ok(())
}

/// The crates whose body walk stopped at the per-crate deadline. Any "not
/// reachable" answer is bounded by these: a path could run through a body the
/// index started a crate for and never finished.
fn push_partial_crates(lines: &mut Vec<String>, crates: &[(String, i64)], where_: &str) {
    if crates.is_empty() {
        lines.push(format!(
            "-- every crate {where_} that was read was read to the end"
        ));
        return;
    }
    lines.push(format!(
        "-- crates {where_} whose bodies were NOT all read:"
    ));
    for (name, unread) in crates {
        lines.push(format!("     {name}: {unread} bodies unread"));
    }
}

/// Print what the index contains and what it could not see.
fn coverage(db_path: &str) -> anyhow::Result<()> {
    let connection = store::open(db_path)?;
    for line in coverage_lines(&connection)? {
        println!("{line}");
    }
    Ok(())
}

/// Append the same coverage report to a build's log.
fn log_coverage(connection: &rusqlite::Connection, log: &log::FileLog) -> anyhow::Result<()> {
    for line in coverage_lines(connection)? {
        log.line(&line);
    }
    Ok(())
}

/// The table sizes, the recorded build facts, and which crates were read.
fn coverage_lines(connection: &rusqlite::Connection) -> anyhow::Result<Vec<String>> {
    let mut lines = vec!["-- index contents:".to_string()];
    for (table, count) in query::table_counts(connection)? {
        lines.push(format!("     {table}: {count}"));
    }
    let (scanned, total) = query::scanned_crates(connection)?;
    lines.push(format!(
        "-- {scanned} of {total} crates in the index had their bodies read; the other \
         {} are there because something read called into them",
        total - scanned
    ));
    lines.push("-- build facts and blind spots:".to_string());
    for (key, value) in query::coverage(connection)? {
        lines.push(format!("     {key}: {value}"));
    }
    push_partial_crates(
        &mut lines,
        &query::partial_crates(connection)?,
        "in the index",
    );
    Ok(lines)
}

/// The ids of the items a pattern matches, failing loudly when nothing matches:
/// an empty answer to a misspelled name is the failure mode this tool exists to
/// remove.
fn matching_ids(connection: &rusqlite::Connection, pattern: &str) -> anyhow::Result<Vec<i64>> {
    let items = query::find_items(connection, pattern)?;
    if items.is_empty() {
        anyhow::bail!("no definition matches {pattern:?}");
    }
    let mut ids = Vec::new();
    for item in items {
        ids.push(item.id);
    }
    Ok(ids)
}

/// The ids of every entry point named on the command line, printed so the walk's
/// starting set is never a guess.
fn entry_ids(connection: &rusqlite::Connection, patterns: &[String]) -> anyhow::Result<Vec<i64>> {
    let mut ids = Vec::new();
    for pattern in patterns {
        for item in query::find_items(connection, pattern)? {
            println!("-- entry: {} ({}:{})", item.path, item.file, item.line);
            ids.push(item.id);
        }
    }
    if ids.is_empty() {
        anyhow::bail!("no definition matches any of {patterns:?}");
    }
    Ok(ids)
}
