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
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::ptr::NonNull;
use std::sync::{Mutex, OnceLock};

use fq_common::Config;
use rusqlite::{ffi, Connection, OpenFlags};

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
    header_reader: Mutex<Connection>,
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
                header_reader: open_header_reader(&path)?,
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
            if let Err(detail) = inspect(file) {
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
fn inspect(file: &StoreFile) -> Result<(), String> {
    let identity = read_identity(&file.path).map_err(|error| format!("cannot be read: {error}"))?;
    if identity != file.identity {
        return Err("was replaced by a different file".to_string());
    }
    let reader = file
        .header_reader
        .lock()
        .expect("state header reader lock poisoned");
    if !starts_with_sqlite_header(&reader)? {
        return Err("is no longer a SQLite database (truncated or overwritten)".to_string());
    }
    Ok(())
}

/// Keep the header descriptor under SQLite's inode-lock bookkeeping. Closing an
/// ordinary file descriptor would release the process's SQLite POSIX locks too.
fn open_header_reader(path: &Path) -> Result<Mutex<Connection>, RuntimeError> {
    let reader = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)
        .map_err(|error| RuntimeError::StateStoreLost(fatal_message(path, &error.to_string())))?;
    Ok(Mutex::new(reader))
}

/// Read the actual file header through SQLite's VFS, bypassing its page cache.
/// A file too short to hold the header has been truncated, which reads as false.
fn starts_with_sqlite_header(reader: &Connection) -> Result<bool, String> {
    let mut header = [0u8; SQLITE_HEADER.len()];
    match read_header(reader, &mut header)? {
        ffi::SQLITE_OK => Ok(&header == SQLITE_HEADER),
        ffi::SQLITE_IOERR_SHORT_READ => Ok(false),
        code => Err(sqlite_read_error(code)),
    }
}

/// Borrow the open main-database VFS file while its owning connection stays live.
fn sqlite_file(reader: &Connection) -> Result<NonNull<ffi::sqlite3_file>, String> {
    let mut file: *mut ffi::sqlite3_file = std::ptr::null_mut();
    // SAFETY: the connection is live and the output points to a writable file slot.
    let code = unsafe {
        ffi::sqlite3_file_control(
            reader.handle(),
            c"main".as_ptr(),
            ffi::SQLITE_FCNTL_FILE_POINTER,
            (&raw mut file).cast(),
        )
    };
    if code != ffi::SQLITE_OK {
        return Err(sqlite_read_error(code));
    }
    NonNull::new(file).ok_or_else(|| "SQLite state-file handle is null".to_string())
}

/// Read only the header bytes with the connection-owned file's VFS callback.
fn read_header(reader: &Connection, header: &mut [u8; 16]) -> Result<i32, String> {
    let file = sqlite_file(reader)?;
    // SAFETY: SQLite owns this file and its method table for the live connection.
    let methods = unsafe { file.as_ref().pMethods.as_ref() }
        .ok_or_else(|| "SQLite state-file methods are null".to_string())?;
    let read = methods
        .xRead
        .ok_or_else(|| "SQLite state-file reader is absent".to_string())?;
    // SAFETY: the connection is locked and the callback receives a writable 16-byte buffer.
    Ok(unsafe { read(file.as_ptr(), header.as_mut_ptr().cast(), 16, 0) })
}

/// Preserve SQLite's native result code in a failed header read.
fn sqlite_read_error(code: i32) -> String {
    format!(
        "cannot read SQLite file header: {}",
        rusqlite::Error::SqliteFailure(ffi::Error::new(code), None)
    )
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

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::time::{SystemTime, UNIX_EPOCH};

    use fq_common::Config;
    use rusqlite::Connection;

    use super::StateStore;

    const PEER_QUERY: &str = concat!(
        "import sqlite3,sys; db=sqlite3.connect(sys.argv[1]); ",
        "print(db.execute('SELECT count(*) FROM guard_rows').fetchone()[0]); ",
        "db.close()"
    );
    const WAL_SETUP: &str = concat!(
        "PRAGMA journal_mode=WAL; CREATE TABLE guard_rows(value); ",
        "INSERT INTO guard_rows VALUES(1);"
    );

    /// Create an isolated WAL writer and the runtime's state-file guard.
    fn guarded_writer() -> (PathBuf, PathBuf, Connection, StateStore) {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the clock follows the Unix epoch")
            .as_nanos();
        let directory = std::env::temp_dir().join(format!("fq_header_locks_{nonce}"));
        std::fs::create_dir(&directory).expect("create state-file sandbox");
        let path = directory.join("config.stats.sqlite");
        let writer = Connection::open(&path).expect("open WAL writer");
        writer
            .execute_batch(WAL_SETUP)
            .expect("initialize committed WAL state");
        let config = guard_config(&directory);
        let guard = StateStore::record(&config).expect("record state-file identity");
        (directory, path, writer, guard)
    }

    /// Load a minimal real config that names this sandbox's state store.
    fn guard_config(directory: &Path) -> Config {
        let path = directory.join("config.yaml");
        std::fs::write(&path, "datasources: {}\n").expect("write guard config");
        fq_common::load_config(path.to_str().expect("config path is UTF-8"))
            .expect("load guard config")
    }

    /// Read and close SQLite in another process, exercising its last-close checkpoint.
    fn peer_count(path: &Path) -> i64 {
        let output = peer_reader(path);
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8(output.stdout)
            .expect("reader output is UTF-8")
            .trim()
            .parse()
            .expect("reader returns a row count")
    }

    /// Execute the independent reader with the active Python virtualenv.
    fn peer_reader(path: &Path) -> std::process::Output {
        let environment = std::env::var_os("VIRTUAL_ENV").expect("activate the Python virtualenv");
        let python = PathBuf::from(environment).join("bin/python3");
        Command::new(python)
            .args(["-c", PEER_QUERY])
            .arg(path)
            .output()
            .expect("run independent SQLite reader")
    }

    /// Keep a peer from deleting the WAL that the live writer still needs.
    #[test]
    fn header_checks_preserve_committed_wal_state_for_other_processes() {
        let (directory, path, writer, guard) = guarded_writer();
        guard.check().expect("the intact header is accepted");
        assert_eq!(peer_count(&path), 1);
        writer
            .execute("INSERT INTO guard_rows VALUES(2)", [])
            .expect("commit another row");
        assert_eq!(
            peer_count(&path),
            2,
            "a peer must see the committed WAL write"
        );
        drop(guard);
        drop(writer);
        std::fs::remove_dir_all(directory).expect("remove state-file sandbox");
    }
}
