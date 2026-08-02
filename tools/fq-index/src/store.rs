//! The `SQLite` index: its schema, and the writes that fill it.
//!
//! Everything the indexer learns lands in tables a shell can query directly, and
//! everything it could NOT learn lands there too - an unresolved call is a row
//! in `call` with `resolution = 'unresolved'`, never an absent row. A query that
//! reports "no path" is only worth reading next to the count of edges the index
//! never saw, so those counts are stored, not discarded.
//!
//! The index is written crate by crate and is meant to be re-opened: a run that
//! dies leaves every crate it finished in the file, and the next run continues
//! from there. That is why identity here is CONTENT, not insertion order - a
//! crate is identified by its name and root file, a file by its path, an item by
//! its signature. A second process resolves the same source to the same row.

use rusqlite::{Connection, OptionalExtension};

/// The tables and indices the index is made of.
///
/// - `krate` is one crate of the resolved graph, with where its sources came
///   from, whether its bodies were walked, and how many were left unread.
/// - `file` is one source file, keyed by its path.
/// - `item` is one function, method or trait signature, with its canonical path
///   and the signature that identifies it across processes.
/// - `call` is one call expression in a body, with how it was resolved.
/// - `edge` is one caller to callee link, the callee being the function
///   rust-analyzer resolved the call to.
/// - `dyn_target` links a trait signature to one implementation of it, which is
///   what a call on a `dyn Trait` receiver may dispatch to.
/// - `meta` is how this index was built: root, versions, timings, blind spots.
const SCHEMA: &[&str] = &[
    "CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)",
    "CREATE TABLE krate(
        id INTEGER PRIMARY KEY,
        key TEXT NOT NULL UNIQUE,
        name TEXT NOT NULL,
        origin TEXT NOT NULL,
        scanned INTEGER NOT NULL,
        unread_bodies INTEGER NOT NULL)",
    "CREATE TABLE file(
        id INTEGER PRIMARY KEY,
        path TEXT NOT NULL UNIQUE)",
    "CREATE TABLE item(
        id INTEGER PRIMARY KEY,
        sig TEXT NOT NULL UNIQUE,
        krate_id INTEGER NOT NULL,
        file_id INTEGER,
        kind TEXT NOT NULL,
        name TEXT NOT NULL,
        path TEXT NOT NULL,
        owner TEXT,
        in_trait TEXT,
        is_unsafe INTEGER NOT NULL,
        has_body INTEGER NOT NULL,
        line INTEGER)",
    "CREATE TABLE call(
        id INTEGER PRIMARY KEY,
        caller_id INTEGER NOT NULL,
        file_id INTEGER,
        line INTEGER,
        syntax TEXT NOT NULL,
        resolution TEXT NOT NULL,
        text TEXT NOT NULL)",
    "CREATE TABLE edge(
        caller_id INTEGER NOT NULL,
        callee_id INTEGER NOT NULL,
        call_id INTEGER NOT NULL,
        kind TEXT NOT NULL)",
    "CREATE TABLE dyn_target(
        trait_item_id INTEGER NOT NULL,
        impl_item_id INTEGER NOT NULL,
        UNIQUE(trait_item_id, impl_item_id))",
    "CREATE INDEX item_name ON item(name)",
    "CREATE INDEX item_path ON item(path)",
    "CREATE INDEX item_krate ON item(krate_id)",
    "CREATE INDEX call_caller ON call(caller_id)",
    "CREATE INDEX edge_caller ON edge(caller_id)",
    "CREATE INDEX edge_callee ON edge(callee_id)",
];

/// Open the index for writing, creating the schema when the file is new.
///
/// The indices exist from the start rather than being built at the end: a build
/// looks items up by signature as it goes, and a build that dies must leave a
/// file that queries at once.
pub fn open_for_write(path: &str) -> Result<Connection, rusqlite::Error> {
    let connection = Connection::open(path)?;
    // A crash must lose at most the crate in flight, so the write-ahead log and
    // its checkpoints stay on; only the fsync per commit is relaxed.
    connection.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL;")?;
    let built: i64 = connection.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'meta'",
        [],
        |row| row.get(0),
    )?;
    if built == 0 {
        for statement in SCHEMA {
            connection.execute(statement, [])?;
        }
    }
    Ok(connection)
}

/// Open an existing index for querying.
pub fn open(path: &str) -> Result<Connection, rusqlite::Error> {
    Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
}

/// The next free primary key of a table, so a resumed run continues the
/// numbering of the one that wrote the file.
pub fn next_id(connection: &Connection, table: &str) -> Result<i64, rusqlite::Error> {
    connection.query_row(
        &format!("SELECT COALESCE(MAX(id), 0) + 1 FROM {table}"),
        [],
        |row| row.get(0),
    )
}

/// One crate row.
pub struct CrateRow {
    pub id: i64,
    pub key: String,
    pub name: String,
    pub origin: &'static str,
}

/// One function-like definition.
pub struct ItemRow {
    pub id: i64,
    pub sig: String,
    pub krate_id: i64,
    pub file_id: Option<i64>,
    pub kind: &'static str,
    pub name: String,
    pub path: String,
    pub owner: Option<String>,
    pub in_trait: Option<String>,
    pub is_unsafe: bool,
    pub has_body: bool,
    pub line: Option<i64>,
}

/// One call expression, with what rust-analyzer made of it.
pub struct CallRow {
    pub id: i64,
    pub caller_id: i64,
    pub file_id: Option<i64>,
    pub line: Option<i64>,
    pub syntax: &'static str,
    pub resolution: &'static str,
    pub text: String,
}

/// One caller -> callee link.
pub struct EdgeRow {
    pub caller_id: i64,
    pub callee_id: i64,
    pub call_id: i64,
    pub kind: &'static str,
}

/// Insert one crate, not yet scanned.
pub fn insert_crate(connection: &Connection, row: &CrateRow) -> Result<(), rusqlite::Error> {
    connection.execute(
        "INSERT INTO krate(id, key, name, origin, scanned, unread_bodies)
         VALUES (?1, ?2, ?3, ?4, 0, 0)",
        rusqlite::params![row.id, row.key, row.name, row.origin],
    )?;
    Ok(())
}

/// The id of the crate stored under `key`, if this or an earlier run wrote it.
pub fn crate_id_by_key(connection: &Connection, key: &str) -> Result<Option<i64>, rusqlite::Error> {
    connection
        .query_row(
            "SELECT id FROM krate WHERE key = ?1",
            rusqlite::params![key],
            |row| row.get(0),
        )
        .optional()
}

/// Whether a crate's bodies were already walked, which is what a resumed run
/// checks before spending time on it again.
pub fn crate_is_scanned(connection: &Connection, krate_id: i64) -> Result<bool, rusqlite::Error> {
    let scanned: i64 = connection.query_row(
        "SELECT scanned FROM krate WHERE id = ?1",
        rusqlite::params![krate_id],
        |row| row.get(0),
    )?;
    Ok(scanned == 1)
}

/// Record that a crate's bodies were walked, and how many were left unread
/// because the crate ran past its deadline.
pub fn mark_crate_scanned(
    connection: &Connection,
    krate_id: i64,
    unread_bodies: u64,
) -> Result<(), rusqlite::Error> {
    connection.execute(
        "UPDATE krate SET scanned = 1, unread_bodies = ?1 WHERE id = ?2",
        rusqlite::params![
            i64::try_from(unread_bodies).expect("body count fits in i64"),
            krate_id
        ],
    )?;
    Ok(())
}

/// Insert one source file.
pub fn insert_file(connection: &Connection, id: i64, path: &str) -> Result<(), rusqlite::Error> {
    connection.execute(
        "INSERT INTO file(id, path) VALUES (?1, ?2)",
        rusqlite::params![id, path],
    )?;
    Ok(())
}

/// The id of the file stored under `path`, if it is already recorded.
pub fn file_id_by_path(
    connection: &Connection,
    path: &str,
) -> Result<Option<i64>, rusqlite::Error> {
    connection
        .query_row(
            "SELECT id FROM file WHERE path = ?1",
            rusqlite::params![path],
            |row| row.get(0),
        )
        .optional()
}

/// Insert one item.
pub fn insert_item(connection: &Connection, row: &ItemRow) -> Result<(), rusqlite::Error> {
    connection.execute(
        "INSERT INTO item(id, sig, krate_id, file_id, kind, name, path, owner, in_trait,
                          is_unsafe, has_body, line)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        rusqlite::params![
            row.id,
            row.sig,
            row.krate_id,
            row.file_id,
            row.kind,
            row.name,
            row.path,
            row.owner,
            row.in_trait,
            row.is_unsafe,
            row.has_body,
            row.line,
        ],
    )?;
    Ok(())
}

/// The id of the item stored under `sig`, if this or an earlier run wrote it.
pub fn item_id_by_sig(connection: &Connection, sig: &str) -> Result<Option<i64>, rusqlite::Error> {
    connection
        .query_row(
            "SELECT id FROM item WHERE sig = ?1",
            rusqlite::params![sig],
            |row| row.get(0),
        )
        .optional()
}

/// Insert one call site.
pub fn insert_call(connection: &Connection, row: &CallRow) -> Result<(), rusqlite::Error> {
    connection.execute(
        "INSERT INTO call(id, caller_id, file_id, line, syntax, resolution, text)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
        rusqlite::params![
            row.id,
            row.caller_id,
            row.file_id,
            row.line,
            row.syntax,
            row.resolution,
            row.text,
        ],
    )?;
    Ok(())
}

/// Insert one edge.
pub fn insert_edge(connection: &Connection, row: &EdgeRow) -> Result<(), rusqlite::Error> {
    connection.execute(
        "INSERT INTO edge(caller_id, callee_id, call_id, kind) VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![row.caller_id, row.callee_id, row.call_id, row.kind],
    )?;
    Ok(())
}

/// Link a trait signature to one implementation of it. The same pair is written
/// once however many times the two ends are met.
pub fn insert_dyn_target(
    connection: &Connection,
    trait_item_id: i64,
    impl_item_id: i64,
) -> Result<(), rusqlite::Error> {
    connection.execute(
        "INSERT OR IGNORE INTO dyn_target(trait_item_id, impl_item_id) VALUES (?1, ?2)",
        rusqlite::params![trait_item_id, impl_item_id],
    )?;
    Ok(())
}

/// Record one fact about how this index was built.
pub fn set_meta(connection: &Connection, key: &str, value: &str) -> Result<(), rusqlite::Error> {
    connection.execute(
        "INSERT OR REPLACE INTO meta(key, value) VALUES (?1, ?2)",
        rusqlite::params![key, value],
    )?;
    Ok(())
}

/// Add to a counted fact, so the totals of an index built over several runs are
/// the totals of the whole index rather than of its last run.
pub fn add_meta(connection: &Connection, key: &str, amount: u64) -> Result<(), rusqlite::Error> {
    let previous = get_meta(connection, key)?.map_or(0, |value| {
        value
            .parse::<u64>()
            .expect("a counted meta value is a number")
    });
    set_meta(connection, key, &(previous + amount).to_string())
}

/// Read one fact about how the open index was built.
pub fn get_meta(connection: &Connection, key: &str) -> Result<Option<String>, rusqlite::Error> {
    connection
        .query_row(
            "SELECT value FROM meta WHERE key = ?1",
            rusqlite::params![key],
            |row| row.get(0),
        )
        .optional()
}
