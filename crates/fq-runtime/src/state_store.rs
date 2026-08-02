//! The guard over the SQLite file the engine keeps its state in.
//!
//! One file next to the config (`<config-stem>.stats.sqlite`) holds the learned
//! statistics, the persisted SET overrides, the dynamic-datasource registry, the
//! materialized-view catalog, the user/grant store and the event-dataset
//! registry. Every one of those steers plan choice or access, so a runtime whose
//! state file vanished, was replaced, or stopped being a database is not
//! degraded - it is in an UNKNOWN state.
//!
//! That is fatal, and it is fatal ONCE: the loss is recorded on the runtime and
//! every later call returns the same `RuntimeError::StateStoreLost` instead of
//! attempting work. Without this, a lost store shows up as one SQLite error per
//! query, forever, on queries that never touched it - an unbounded stream of
//! misleading per-query failures hiding a single unrecoverable fault.
//!
//! The check runs at the runtime's call boundary rather than at each SQLite
//! call: the file is shared by three subsystems whose errors carry different
//! detail (fq-events flattens its rusqlite errors to a string), it runs BEFORE
//! any work so the fatal loss is what the caller sees rather than whatever error
//! the missing state happened to produce, and a deleted or replaced file is
//! invisible to SQLite entirely - an open connection keeps reading the unlinked
//! inode and would serve state no longer at that path.

use std::collections::BTreeSet;
use std::io::Read;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use fq_common::Config;

use crate::error::RuntimeError;

/// The bytes every SQLite database file begins with. A state file that no
/// longer starts with them has been truncated or overwritten by other content.
const SQLITE_HEADER: &[u8; 16] = b"SQLite format 3\0";

/// Which file a path pointed at when the runtime opened it. A different device
/// or inode means a DIFFERENT file stands at that path now: the runtime's open
/// connections still address the old one, so what it reads and writes no longer
/// reaches the state the path names.
#[derive(PartialEq, Eq)]
struct FileIdentity {
    device: u64,
    inode: u64,
}

/// One state file under guard: where it is, and which file it was at open.
struct StoreFile {
    path: PathBuf,
    identity: FileIdentity,
}

/// The runtime's hold on its state files, plus the fatal message once one of
/// them is found gone. The message is set once and never cleared: a store that
/// vanished is not made trustworthy by reappearing, because everything written
/// while it was gone is lost.
pub(crate) struct StateStore {
    files: Vec<StoreFile>,
    lost: OnceLock<String>,
}

impl StateStore {
    /// Record what this runtime's state files are, immediately after the
    /// subsystems opened them. Their identity now is the identity every later
    /// check compares against. A config with no file path behind it guards
    /// nothing (there is no store to lose), so the file list is empty and the
    /// check is a no-op.
    pub(crate) fn record(config: &Config) -> Result<Self, RuntimeError> {
        let mut files = Vec::new();
        for path in state_store_paths(config) {
            files.push(StoreFile {
                identity: identify(&path)?,
                path,
            });
        }
        Ok(Self {
            files,
            lost: OnceLock::new(),
        })
    }

    /// Fail when the state store is gone. Returns the recorded fatal message
    /// once a loss has been seen, so a poisoned runtime never re-reads a store
    /// it already knows it cannot trust; otherwise re-inspects every guarded
    /// file and poisons the runtime on the first one that is no longer the
    /// database it opened.
    pub(crate) fn check(&self) -> Result<(), RuntimeError> {
        if let Some(message) = self.lost.get() {
            return Err(RuntimeError::StateStoreLost(message.clone()));
        }
        for file in &self.files {
            if let Err(detail) = inspect(&file.path, &file.identity) {
                return Err(self.poison(&file.path, &detail));
            }
        }
        Ok(())
    }

    /// Record the loss (first one wins) and return it as the fatal error.
    fn poison(&self, path: &Path, detail: &str) -> RuntimeError {
        let message = self.lost.get_or_init(|| fatal_message(path, detail));
        RuntimeError::StateStoreLost(message.clone())
    }
}

/// The SQLite files this runtime keeps engine state in: the store beside the
/// config, plus the learned-statistics file when `FEDQ_STATS_CATALOG` points it
/// somewhere else. A config with neither has no state store at all.
fn state_store_paths(config: &Config) -> BTreeSet<PathBuf> {
    let mut paths = BTreeSet::new();
    paths.extend(crate::stats_sqlite_path(config));
    paths.extend(crate::learned_catalog_path(config));
    paths
}

/// The identity of a state file the runtime has just opened. It exists by now -
/// opening created it - so a file that cannot be read here is already the fatal
/// case, not the legitimate "no store yet" one.
fn identify(path: &Path) -> Result<FileIdentity, RuntimeError> {
    read_identity(path).map_err(|error| {
        RuntimeError::StateStoreLost(fatal_message(path, &format!("cannot be read: {error}")))
    })
}

/// The (device, inode) pair naming the file at `path`.
fn read_identity(path: &Path) -> std::io::Result<FileIdentity> {
    let metadata = std::fs::metadata(path)?;
    Ok(FileIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

/// Whether the file at `path` is still the SQLite database that was opened: it
/// must be readable, be the same file, and still carry the SQLite header. The
/// error describes what happened to it, for the fatal message.
fn inspect(path: &Path, opened_as: &FileIdentity) -> Result<(), String> {
    let identity = read_identity(path).map_err(|error| format!("cannot be read: {error}"))?;
    if identity != *opened_as {
        return Err("was replaced by a different file".to_string());
    }
    if !starts_with_sqlite_header(path)? {
        return Err("is no longer a SQLite database (truncated or overwritten)".to_string());
    }
    Ok(())
}

/// Whether the file still begins with the SQLite header. A file too short to
/// hold the header has been truncated, which reads as no header.
fn starts_with_sqlite_header(path: &Path) -> Result<bool, String> {
    let mut file =
        std::fs::File::open(path).map_err(|error| format!("cannot be opened: {error}"))?;
    let mut header = [0u8; SQLITE_HEADER.len()];
    match file.read_exact(&mut header) {
        Ok(()) => Ok(&header == SQLITE_HEADER),
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
        Err(error) => Err(format!("cannot be read: {error}")),
    }
}

/// The fatal message: what happened, to which file, and that the runtime is
/// finished rather than this one statement.
fn fatal_message(path: &Path, detail: &str) -> String {
    format!(
        "FATAL: the engine state store {} {detail}. It holds the learned statistics, \
         persisted settings, registered datasources, materialized views, users and \
         event datasets this engine plans and authorizes with, so its state is now \
         unknown and this runtime will run nothing further. This is not a query \
         failure: restart the engine over an intact store.",
        path.display()
    )
}
