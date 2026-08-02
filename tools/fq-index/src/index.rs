//! Building the index: load the project into rust-analyzer, then take the
//! resolved crate graph one crate at a time - enumerate its functions, resolve
//! every call in every body, and COMMIT that crate before moving to the next.
//!
//! One crate is the unit of work and the unit of durability. A run that is
//! killed at crate 40 of 200 leaves 39 crates in the index, and the next run
//! skips them; nothing is buffered to be written "at the end", because a build
//! that writes at the end writes nothing when it is killed.
//!
//! Resolution is rust-analyzer's, so `receiver.method()` reaches the method that
//! actually runs and a call into a dependency or into `std` is followed. What it
//! cannot decide - a call through a function pointer, a call on a `dyn Trait`
//! receiver, a body inside a macro that failed to expand - is written down as
//! such instead of being dropped.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::path::Path;

use hir::{AsAssocItem, AssocItem, AssocItemContainer, HirDisplay, ModuleDef, Semantics};
use ide_db::base_db::SourceDatabase;
use ide_db::RootDatabase;
use load_cargo::{LoadCargoConfig, ProcMacroServerChoice};
use project_model::{CargoConfig, RustLibSource};
use ra_ap_hir as hir;
use ra_ap_ide_db as ide_db;
use ra_ap_load_cargo as load_cargo;
use ra_ap_project_model as project_model;
use ra_ap_syntax as syntax;
use ra_ap_vfs as vfs;
use syntax::{ast, AstNode, SyntaxNode};

use crate::store;

/// How deep a macro expansion is followed before the indexer stops descending.
/// Recursive macros would otherwise expand without end; a body deeper than this
/// is counted as unread rather than silently truncated.
const MAX_MACRO_DEPTH: u32 = 32;

/// The most syntax nodes one body's walk visits, macro expansions included.
///
/// A token-muncher macro expands into another call of itself, so a body can
/// produce a tree that grows at every level; without a ceiling a single body
/// consumes the whole run. A body that reaches this ceiling is counted in
/// `bodies_over_budget`, so the calls it did not reach are a reported number
/// rather than an absence nobody notices.
const MAX_BODY_NODES: u32 = 250_000;

/// The longest the body resolution of ONE crate may run.
///
/// Type inference is not linear in body size, and a few dependency crates are
/// generated code with thousands of very large bodies. The deadline is checked
/// between bodies, so a crate made of many slow bodies stops; the crate is then
/// recorded with the number of bodies left unread, and every query prints that,
/// so an answer over a partially read crate is never mistaken for a whole one.
const MAX_CRATE_RESOLVE: std::time::Duration = std::time::Duration::from_mins(10);

/// The largest function body, in bytes of source, whose calls are resolved.
///
/// Type inference is superlinear in body size. The dependency tree contains
/// generated parsers with single functions of 100 KB, and one of those costs
/// more than every workspace crate together - so much that a per-crate deadline
/// cannot even fire, because it is checked between bodies and one body never
/// returns. The ceiling is checked on the body's source length BEFORE any
/// inference is asked for, which is the only check that can precede the cost.
///
/// The largest function in this repository is 27 KB, so no workspace body is
/// affected. A body over the ceiling is counted in `bodies_too_large` and its
/// calls are absent from the index.
const MAX_BODY_BYTES: u32 = 65_536;

/// Which crates of the resolved graph have their bodies walked.
///
/// Items are enumerated for whatever a walked body calls, whichever crate the
/// callee lives in - so a workspace-only build still names `std::fs::remove_file`
/// as the callee of a workspace call. The scope decides whose BODIES are read,
/// which is the expensive part and the part a query's boundedness depends on.
pub struct Scope {
    /// Crates of this repository.
    pub workspace: bool,
    /// Everything else in the resolved graph: dependencies, `std`, the sysroot.
    pub deps: bool,
}

impl Scope {
    /// Whether a crate of the given origin has its bodies walked.
    fn includes(&self, origin: &str) -> bool {
        match origin {
            "workspace" => self.workspace,
            _ => self.deps,
        }
    }

    /// How the scope is recorded in the index, so a query can say what was read.
    pub fn describe(&self) -> String {
        format!("workspace={} deps={}", self.workspace, self.deps)
    }
}

/// What one crate's scan produced, as the log records it.
pub struct CrateReport {
    pub name: String,
    /// Position of this crate in the scan, 1-indexed.
    pub position: usize,
    pub total: usize,
    pub items: u64,
    pub calls: u64,
    pub edges: u64,
    pub bodies_unread: u64,
    pub seconds: f64,
    /// Set when the crate was already in the index and was not read again.
    pub skipped: bool,
}

/// Where a build writes its progress. The build itself never prints: a long run
/// is watched by reading the log, and stdout belongs to query results.
pub trait BuildLog {
    /// Record one line about the run as a whole.
    fn note(&self, message: &str);
    /// Record one finished crate.
    fn crate_done(&self, report: &CrateReport);
}

/// The counters that say what the index could NOT see. Every one of these is
/// printed at the end of a build and stored in `meta`, because a call graph is
/// only as trustworthy as its list of missing edges.
#[derive(Default)]
pub struct BlindSpots {
    /// Calls of a closure value: the target is chosen at run time.
    pub closure_calls: u64,
    /// Calls through a function pointer: likewise.
    pub fn_pointer_calls: u64,
    /// Calls rust-analyzer could not type at all (unresolved names, code behind
    /// a cfg it did not select, inference failures).
    pub unresolved_calls: u64,
    /// Calls that resolved to a trait method, whose running implementation
    /// depends on the receiver's real type. The `dyn_target` links say which
    /// implementations exist among the crates that were scanned.
    pub trait_method_calls: u64,
    /// Macro calls that did not expand, so any call inside them is invisible.
    pub unexpanded_macros: u64,
    /// Macro expansions abandoned at the depth limit.
    pub macro_depth_hits: u64,
    /// Bodies whose walk hit the node ceiling, so the rest of that body's calls
    /// were not read.
    pub bodies_over_budget: u64,
    /// Crates whose body resolution hit the time ceiling.
    pub crates_over_budget: u64,
    /// Bodies left unread because their crate hit the time ceiling.
    pub bodies_left_unread: u64,
    /// Items whose source location could not be found, so they are identified by
    /// path alone.
    pub items_without_location: u64,
    /// Bodies skipped for being longer than the source-size ceiling.
    pub bodies_too_large: u64,
}

impl BlindSpots {
    /// The counters as the `meta` rows a build adds to.
    pub fn rows(&self) -> Vec<(&'static str, u64)> {
        vec![
            ("blind.closure_calls", self.closure_calls),
            ("blind.fn_pointer_calls", self.fn_pointer_calls),
            ("blind.unresolved_calls", self.unresolved_calls),
            ("blind.trait_method_calls", self.trait_method_calls),
            ("blind.unexpanded_macros", self.unexpanded_macros),
            ("blind.macro_depth_hits", self.macro_depth_hits),
            ("blind.bodies_over_budget", self.bodies_over_budget),
            ("blind.crates_over_budget", self.crates_over_budget),
            ("blind.bodies_left_unread", self.bodies_left_unread),
            ("blind.items_without_location", self.items_without_location),
            ("blind.bodies_too_large", self.bodies_too_large),
        ]
    }
}

/// Load the project at `root` into rust-analyzer with its full dependency graph,
/// its build-script output, and the sysroot sources.
///
/// Build scripts are run and the sysroot proc-macro server is used, so generated
/// code and `#[derive]` / attribute-macro expansions are part of what is indexed
/// rather than a blind spot.
pub fn load(
    root: &Path,
    progress: &(dyn Fn(String) + Sync),
) -> anyhow::Result<(RootDatabase, vfs::Vfs)> {
    let cargo_config = CargoConfig {
        sysroot: Some(RustLibSource::Discover),
        all_targets: true,
        ..CargoConfig::default()
    };
    let load_config = LoadCargoConfig {
        load_out_dirs_from_check: true,
        with_proc_macro_server: ProcMacroServerChoice::Sysroot,
        prefill_caches: false,
        num_worker_threads: std::thread::available_parallelism()?.get(),
        proc_macro_processes: 1,
    };
    let (db, vfs, _proc_macro) =
        load_cargo::load_workspace_at(root, &cargo_config, &load_config, progress)?;
    Ok((db, vfs))
}

/// Scan every in-scope crate, committing each one as it is finished.
pub fn build(
    db: &RootDatabase,
    vfs: &vfs::Vfs,
    connection: &mut rusqlite::Connection,
    scope: &Scope,
    log: &dyn BuildLog,
) -> anyhow::Result<BlindSpots> {
    // Type rendering and trait solving reach for the database through a
    // thread-local rather than through an argument; without this the first such
    // query aborts the whole build.
    hir::attach_db(db, || build_attached(db, vfs, connection, scope, log))
}

/// Do the build with the salsa database attached to this thread.
fn build_attached(
    db: &RootDatabase,
    vfs: &vfs::Vfs,
    connection: &mut rusqlite::Connection,
    scope: &Scope,
    log: &dyn BuildLog,
) -> anyhow::Result<BlindSpots> {
    let mut catalog = Catalog::resuming(connection)?;
    let in_scope = in_scope_crates(db, scope);
    log.note(&format!(
        "{} crates in the resolved graph, {} in scope ({})",
        hir::Crate::all(db).len(),
        in_scope.len(),
        scope.describe()
    ));
    let mut blind = BlindSpots::default();
    for (position, krate) in in_scope.iter().enumerate() {
        let scan = CrateScan {
            db,
            vfs,
            krate: *krate,
            position: position + 1,
            total: in_scope.len(),
        };
        scan.run(connection, &mut catalog, &mut blind, log)?;
    }
    Ok(blind)
}

/// The crates whose bodies this build reads, in a fixed order: the workspace's
/// own crates first, then everything else by name, so two runs of the same scope
/// visit the same crates in the same order.
fn in_scope_crates(db: &RootDatabase, scope: &Scope) -> Vec<hir::Crate> {
    let mut selected = Vec::new();
    for krate in hir::Crate::all(db) {
        if scope.includes(crate_origin(db, krate)) {
            selected.push(krate);
        }
    }
    selected.sort_by_key(|krate| {
        let local = crate_origin(db, *krate) != "workspace";
        (local, crate_name(db, *krate), krate.root_file(db).index())
    });
    selected
}

/// One crate being scanned, and where it sits in the run.
struct CrateScan<'a> {
    db: &'a RootDatabase,
    vfs: &'a vfs::Vfs,
    krate: hir::Crate,
    position: usize,
    total: usize,
}

impl CrateScan<'_> {
    /// Enumerate the crate's items, resolve its bodies and commit, or skip it
    /// when a previous run already wrote it.
    fn run(
        &self,
        connection: &mut rusqlite::Connection,
        catalog: &mut Catalog,
        blind: &mut BlindSpots,
        log: &dyn BuildLog,
    ) -> anyhow::Result<()> {
        let started = std::time::Instant::now();
        let krate_id = ensure_crate(connection, self.db, self.vfs, self.krate, catalog)?;
        if store::crate_is_scanned(connection, krate_id)? {
            log.crate_done(&self.report(&Tally::default(), 0, started, true));
            return Ok(());
        }
        let transaction = connection.transaction()?;
        let writer = Writer::new(self.db, self.vfs, &transaction);
        let unread = self.walk(&writer, krate_id, catalog, blind)?;
        store::mark_crate_scanned(&transaction, krate_id, unread)?;
        let tally = writer.tally();
        drop(writer);
        // The crate's items, calls, edges and its "scanned" flag land together:
        // the index never holds a crate whose bodies were read but not recorded.
        transaction.commit()?;
        record_unread(blind, unread);
        log.crate_done(&self.report(&tally, unread, started, false));
        Ok(())
    }

    /// Enumerate the crate's functions, then resolve each of their bodies until
    /// the crate's deadline. Returns how many bodies were left unread.
    fn walk(
        &self,
        writer: &Writer,
        krate_id: i64,
        catalog: &mut Catalog,
        blind: &mut BlindSpots,
    ) -> anyhow::Result<u64> {
        let functions = enumerate_crate(writer, self.krate, krate_id, catalog, blind)?;
        let deadline = std::time::Instant::now() + MAX_CRATE_RESOLVE;
        let mut read = 0usize;
        for (function, item_id) in &functions {
            if std::time::Instant::now() >= deadline {
                break;
            }
            resolve_body(writer, *function, *item_id, catalog, blind)?;
            read += 1;
        }
        Ok((functions.len() - read) as u64)
    }

    /// The log line for this crate.
    fn report(
        &self,
        tally: &Tally,
        bodies_unread: u64,
        started: std::time::Instant,
        skipped: bool,
    ) -> CrateReport {
        CrateReport {
            name: crate_name(self.db, self.krate),
            position: self.position,
            total: self.total,
            items: tally.items,
            calls: tally.calls,
            edges: tally.edges,
            bodies_unread,
            seconds: started.elapsed().as_secs_f64(),
            skipped,
        }
    }
}

/// Count a crate that ran past its deadline.
fn record_unread(blind: &mut BlindSpots, unread: u64) {
    if unread > 0 {
        blind.crates_over_budget += 1;
        blind.bodies_left_unread += unread;
    }
}

/// What one crate's scan wrote, for the log.
#[derive(Default)]
struct Tally {
    items: u64,
    calls: u64,
    edges: u64,
}

/// The ids handed out so far, and the lookups that keep a resumed run from
/// writing a second row for something an earlier run already wrote.
///
/// Every map here is a cache of what is in the database: a miss is answered by
/// the database, never by inventing a new row. That is what makes `--rebuild`
/// unnecessary for a run that was killed.
struct Catalog {
    items: HashMap<hir::Function, i64>,
    crates: HashMap<hir::Crate, i64>,
    files: HashMap<vfs::FileId, i64>,
    next_item_id: i64,
    next_call_id: i64,
    next_crate_id: i64,
    next_file_id: i64,
}

impl Catalog {
    /// Start from what the index already holds, so ids continue rather than
    /// collide.
    fn resuming(connection: &rusqlite::Connection) -> Result<Self, rusqlite::Error> {
        Ok(Catalog {
            items: HashMap::new(),
            crates: HashMap::new(),
            files: HashMap::new(),
            next_item_id: store::next_id(connection, "item")?,
            next_call_id: store::next_id(connection, "call")?,
            next_crate_id: store::next_id(connection, "krate")?,
            next_file_id: store::next_id(connection, "file")?,
        })
    }
}

/// What every write needs: the analysis database and the semantic view over it,
/// the file map that turns a file id into a path, and the connection inside the
/// open transaction.
///
/// One `Writer` lives for one crate's scan, which is also the lifetime of the
/// caches inside it: the parsed syntax trees held by `Semantics` and the line
/// tables are released when the crate is committed.
struct Writer<'a> {
    db: &'a RootDatabase,
    sema: Semantics<'a, RootDatabase>,
    vfs: &'a vfs::Vfs,
    connection: &'a rusqlite::Connection,
    lines: RefCell<HashMap<vfs::FileId, LineTable>>,
    items: Cell<u64>,
    calls: Cell<u64>,
    edges: Cell<u64>,
}

/// One file's text and the byte offset each of its lines starts at.
///
/// Built once per file: a lookup that instead scanned to the offset would be
/// linear in FILE size at every call site, and the dependency tree contains
/// single generated source files of several megabytes.
struct LineTable {
    text: String,
    starts: Vec<u32>,
}

impl LineTable {
    /// Index one file's text.
    fn of(text: String) -> Self {
        let mut starts = vec![0u32];
        for (offset, byte) in text.bytes().enumerate() {
            if byte == b'\n' {
                starts.push(u32::try_from(offset).expect("file offset fits in u32") + 1);
            }
        }
        LineTable { text, starts }
    }

    /// The 1-indexed line a byte offset falls on.
    ///
    /// The offset must be a character boundary in THIS file; one that is not
    /// means it was taken in a different coordinate space (a macro expansion
    /// rather than the original file), which would put every line number in the
    /// index off, so it panics rather than round to something plausible.
    fn line_of(&self, offset: u32) -> i64 {
        assert!(
            self.text.is_char_boundary(offset as usize),
            "offset {offset} is not a character boundary in this file"
        );
        i64::try_from(self.starts.partition_point(|start| *start <= offset))
            .expect("line count fits in i64")
    }
}

impl<'a> Writer<'a> {
    /// A writer over one open transaction, with empty caches.
    fn new(db: &'a RootDatabase, vfs: &'a vfs::Vfs, connection: &'a rusqlite::Connection) -> Self {
        Writer {
            db,
            sema: Semantics::new(db),
            vfs,
            connection,
            lines: RefCell::new(HashMap::new()),
            items: Cell::new(0),
            calls: Cell::new(0),
            edges: Cell::new(0),
        }
    }

    /// The 1-indexed line a byte offset falls on, indexing the file on first use.
    fn line_of(&self, file_id: vfs::FileId, offset: u32) -> i64 {
        let mut lines = self.lines.borrow_mut();
        let table = lines
            .entry(file_id)
            .or_insert_with(|| LineTable::of(self.db.file_text(file_id).text(self.db).to_string()));
        table.line_of(offset)
    }

    /// What this crate's scan has written so far.
    fn tally(&self) -> Tally {
        Tally {
            items: self.items.get(),
            calls: self.calls.get(),
            edges: self.edges.get(),
        }
    }
}

/// How one function is named in the index: which sort of definition it is, the
/// type or trait it hangs off, and the trait it implements.
struct Naming {
    kind: &'static str,
    owner: Option<String>,
    in_trait: Option<String>,
}

/// The id of a crate's row, writing the row the first time the crate is met.
///
/// A crate is identified by its name and its root file, not by the order it was
/// enumerated in: two processes indexing the same sources give it the same row,
/// which is what lets a killed run be continued.
fn ensure_crate(
    connection: &rusqlite::Connection,
    db: &RootDatabase,
    vfs: &vfs::Vfs,
    krate: hir::Crate,
    catalog: &mut Catalog,
) -> anyhow::Result<i64> {
    if let Some(id) = catalog.crates.get(&krate) {
        return Ok(*id);
    }
    let key = format!(
        "{}@{}",
        crate_name(db, krate),
        vfs.file_path(krate.root_file(db))
    );
    let id = match store::crate_id_by_key(connection, &key)? {
        Some(existing) => existing,
        None => insert_crate_row(connection, db, krate, key, catalog)?,
    };
    catalog.crates.insert(krate, id);
    Ok(id)
}

/// Write one crate row under the next free id.
fn insert_crate_row(
    connection: &rusqlite::Connection,
    db: &RootDatabase,
    krate: hir::Crate,
    key: String,
    catalog: &mut Catalog,
) -> anyhow::Result<i64> {
    let id = catalog.next_crate_id;
    catalog.next_crate_id += 1;
    store::insert_crate(
        connection,
        &store::CrateRow {
            id,
            key,
            name: crate_name(db, krate),
            origin: crate_origin(db, krate),
        },
    )?;
    Ok(id)
}

/// The id of a file's row, writing the row the first time the file is met.
///
/// Files are keyed by path because rust-analyzer's file ids are assigned per
/// process: a resumed run would otherwise attach one run's ids to another run's
/// paths and print the wrong file for every item.
fn ensure_file(
    writer: &Writer,
    file_id: vfs::FileId,
    catalog: &mut Catalog,
) -> anyhow::Result<i64> {
    if let Some(id) = catalog.files.get(&file_id) {
        return Ok(*id);
    }
    let path = writer.vfs.file_path(file_id).to_string();
    let id = match store::file_id_by_path(writer.connection, &path)? {
        Some(existing) => existing,
        None => insert_file_row(writer, &path, catalog)?,
    };
    catalog.files.insert(file_id, id);
    Ok(id)
}

/// Write one file row under the next free id.
fn insert_file_row(writer: &Writer, path: &str, catalog: &mut Catalog) -> anyhow::Result<i64> {
    let id = catalog.next_file_id;
    catalog.next_file_id += 1;
    store::insert_file(writer.connection, id, path)?;
    Ok(id)
}

/// Enumerate every function, method and trait signature of one crate, and link
/// each implementation to the trait signature it satisfies.
///
/// Returns the crate's functions with their item ids, in the order their bodies
/// are then resolved.
fn enumerate_crate(
    writer: &Writer,
    krate: hir::Crate,
    krate_id: i64,
    catalog: &mut Catalog,
    blind: &mut BlindSpots,
) -> anyhow::Result<Vec<(hir::Function, i64)>> {
    let mut functions = Vec::new();
    for module in krate.modules(writer.db) {
        for declaration in module.declarations(writer.db) {
            collect_declaration(writer, declaration, catalog, blind, &mut functions)?;
        }
        for impl_def in module.impl_defs(writer.db) {
            collect_impl(writer, impl_def, catalog, blind, &mut functions)?;
        }
    }
    functions.retain(|(_, item_id)| item_belongs_to(writer, *item_id, krate_id));
    Ok(functions)
}

/// Whether an enumerated item is really this crate's own.
///
/// One source file can belong to two crates - `tests/common/mod.rs` is compiled
/// into every integration-test target that declares it - so both crates
/// enumerate the same definition, and both resolve to the one item row that
/// identifies it. Walking that body once per crate would write every call in it
/// twice. The row belongs to the crate that defines it; the others skip it.
fn item_belongs_to(writer: &Writer, item_id: i64, krate_id: i64) -> bool {
    let owner: i64 = writer
        .connection
        .query_row(
            "SELECT krate_id FROM item WHERE id = ?1",
            rusqlite::params![item_id],
            |row| row.get(0),
        )
        .expect("an enumerated item has a row");
    owner == krate_id
}

/// Enumerate a module-level declaration: a free function directly, a trait
/// through the signatures it declares.
fn collect_declaration(
    writer: &Writer,
    declaration: ModuleDef,
    catalog: &mut Catalog,
    blind: &mut BlindSpots,
    functions: &mut Vec<(hir::Function, i64)>,
) -> anyhow::Result<()> {
    match declaration {
        ModuleDef::Function(function) => {
            let id = ensure_item(writer, function, catalog, blind)?;
            functions.push((function, id));
            Ok(())
        }
        ModuleDef::Trait(trait_def) => {
            let items = trait_def.items(writer.db);
            collect_assoc_items(writer, &items, catalog, blind, functions)
        }
        _ => Ok(()),
    }
}

/// Enumerate the functions among a set of associated items.
fn collect_assoc_items(
    writer: &Writer,
    items: &[AssocItem],
    catalog: &mut Catalog,
    blind: &mut BlindSpots,
    functions: &mut Vec<(hir::Function, i64)>,
) -> anyhow::Result<()> {
    for item in items {
        if let AssocItem::Function(function) = item {
            let id = ensure_item(writer, *function, catalog, blind)?;
            functions.push((*function, id));
        }
    }
    Ok(())
}

/// Enumerate the functions of one `impl` block, and, when it implements a trait,
/// record which trait signature each one implements so a call that resolved to
/// the trait can be expanded to the implementations it may run.
fn collect_impl(
    writer: &Writer,
    impl_def: hir::Impl,
    catalog: &mut Catalog,
    blind: &mut BlindSpots,
    functions: &mut Vec<(hir::Function, i64)>,
) -> anyhow::Result<()> {
    let items = impl_def.items(writer.db);
    collect_assoc_items(writer, &items, catalog, blind, functions)?;
    let Some(trait_def) = impl_def.trait_(writer.db) else {
        return Ok(());
    };
    for item in items {
        if let AssocItem::Function(function) = item {
            link_to_trait(writer, trait_def, function, catalog, blind)?;
        }
    }
    Ok(())
}

/// Link one implementing function to the trait signature of the same name.
fn link_to_trait(
    writer: &Writer,
    trait_def: hir::Trait,
    function: hir::Function,
    catalog: &mut Catalog,
    blind: &mut BlindSpots,
) -> anyhow::Result<()> {
    let name = function.name(writer.db);
    for item in trait_def.items(writer.db) {
        let AssocItem::Function(declared) = item else {
            continue;
        };
        if declared.name(writer.db) == name {
            let trait_id = ensure_item(writer, declared, catalog, blind)?;
            let impl_id = ensure_item(writer, function, catalog, blind)?;
            store::insert_dyn_target(writer.connection, trait_id, impl_id)?;
        }
    }
    Ok(())
}

/// The item id of a function, writing its row the first time it is met.
///
/// This is the only place an item row is created, so a function met as a callee
/// of a scanned crate is indexed exactly like one enumerated from its own crate.
/// That is what makes a workspace-only build able to name `std::fs::remove_file`
/// without reading a single body of `std`.
fn ensure_item(
    writer: &Writer,
    function: hir::Function,
    catalog: &mut Catalog,
    blind: &mut BlindSpots,
) -> anyhow::Result<i64> {
    if let Some(id) = catalog.items.get(&function) {
        return Ok(*id);
    }
    let facts = item_facts(writer, function, catalog, blind)?;
    let id = match store::item_id_by_sig(writer.connection, &facts.sig)? {
        Some(existing) => existing,
        None => insert_item_row(writer, function, facts, catalog)?,
    };
    catalog.items.insert(function, id);
    Ok(id)
}

/// What identifies one item, computed once and then written.
struct ItemFacts {
    naming: Naming,
    path: String,
    sig: String,
    file_id: Option<i64>,
    line: Option<i64>,
}

/// Name and locate one function, and derive the signature it is stored under.
fn item_facts(
    writer: &Writer,
    function: hir::Function,
    catalog: &mut Catalog,
    blind: &mut BlindSpots,
) -> anyhow::Result<ItemFacts> {
    let spot = function_location(writer, function);
    let naming = naming_of(writer.db, function);
    let path = item_path(writer.db, function, naming.owner.as_deref());
    let sig = signature(writer, &naming, &path, spot);
    let Some((file, line)) = spot else {
        blind.items_without_location += 1;
        return Ok(ItemFacts {
            naming,
            path,
            sig,
            file_id: None,
            line: None,
        });
    };
    Ok(ItemFacts {
        naming,
        path,
        sig,
        file_id: Some(ensure_file(writer, file, catalog)?),
        line: Some(line),
    })
}

/// The string that identifies an item across processes: its kind, its canonical
/// path, the trait it implements, and where it is written.
///
/// The location is part of it because a path is not unique - `impl From<A> for T`
/// and `impl From<B> for T` both declare `t::T::from` - and two definitions
/// cannot start at the same line of the same file. An item whose source could
/// not be located is identified by the rest alone and counted as such.
fn signature(
    writer: &Writer,
    naming: &Naming,
    path: &str,
    spot: Option<(vfs::FileId, i64)>,
) -> String {
    let location = spot.map_or_else(
        || "?".to_string(),
        |(file, line)| format!("{}:{line}", writer.vfs.file_path(file)),
    );
    let in_trait = naming.in_trait.clone().unwrap_or_else(|| "-".to_string());
    format!("{}|{path}|{in_trait}|{location}", naming.kind)
}

/// Write one item row under the next free id, under the crate that defines it.
fn insert_item_row(
    writer: &Writer,
    function: hir::Function,
    facts: ItemFacts,
    catalog: &mut Catalog,
) -> anyhow::Result<i64> {
    let db = writer.db;
    let krate = function.module(db).krate(db);
    let krate_id = ensure_crate(writer.connection, db, writer.vfs, krate, catalog)?;
    let id = catalog.next_item_id;
    catalog.next_item_id += 1;
    store::insert_item(
        writer.connection,
        &store::ItemRow {
            id,
            sig: facts.sig,
            krate_id,
            file_id: facts.file_id,
            kind: facts.naming.kind,
            path: facts.path,
            name: function.name(db).as_str().to_string(),
            owner: facts.naming.owner,
            in_trait: facts.naming.in_trait,
            is_unsafe: function.is_unsafe(db),
            has_body: function.has_body(db),
            line: facts.line,
        },
    )?;
    writer.items.set(writer.items.get() + 1);
    Ok(id)
}

/// Which sort of definition a function is, and what it hangs off.
fn naming_of(db: &RootDatabase, function: hir::Function) -> Naming {
    match function.as_assoc_item(db).map(|item| item.container(db)) {
        Some(AssocItemContainer::Trait(trait_def)) => {
            let name = trait_def.name(db).as_str().to_string();
            Naming {
                kind: "trait_fn",
                owner: Some(name.clone()),
                in_trait: Some(name),
            }
        }
        Some(AssocItemContainer::Impl(impl_def)) => Naming {
            kind: "assoc_fn",
            owner: Some(self_type_name(db, impl_def)),
            in_trait: impl_def
                .trait_(db)
                .map(|trait_def| trait_def.name(db).as_str().to_string()),
        },
        None => Naming {
            kind: "fn",
            owner: None,
            in_trait: None,
        },
    }
}

/// The file and 1-indexed line a function is defined at. A function whose source
/// cannot be located (one produced by a macro expansion with no original range)
/// has none.
fn function_location(writer: &Writer, function: hir::Function) -> Option<(vfs::FileId, i64)> {
    let source = writer.sema.source(function)?;
    let range = writer.sema.original_range(source.value.syntax());
    let file_id = range.file_id.file_id(writer.db);
    Some((
        file_id,
        writer.line_of(file_id, u32::from(range.range.start())),
    ))
}

/// The canonical path a function is indexed under:
/// `crate::module::Owner::name` for an associated or trait function,
/// `crate::module::name` for a free one.
fn item_path(db: &RootDatabase, function: hir::Function, owner: Option<&str>) -> String {
    let module = function.module(db);
    let mut segments = vec![crate_name(db, module.krate(db))];
    for segment in module.path_segments(db) {
        segments.push(segment.as_str().to_string());
    }
    if let Some(owner) = owner {
        segments.push(owner.to_string());
    }
    segments.push(function.name(db).as_str().to_string());
    segments.join("::")
}

/// The type an `impl` block hangs off, rendered without its generic arguments so
/// `StatsCatalog<T>` and `StatsCatalog` are the same owner.
fn self_type_name(db: &RootDatabase, impl_def: hir::Impl) -> String {
    let target = hir::DisplayTarget::from_crate(db, impl_def.module(db).krate(db).into());
    let rendered = impl_def.self_ty(db).display(db, target).to_string();
    let head = rendered.split('<').next().unwrap_or(&rendered);
    head.trim_start_matches('&').trim().to_string()
}

/// The crate's name as it is written in a path.
fn crate_name(db: &RootDatabase, krate: hir::Crate) -> String {
    krate
        .display_name(db)
        .map_or_else(|| "?".to_string(), |name| name.to_string())
}

/// Where a crate's sources came from, which bounds what the index can claim.
fn crate_origin(db: &RootDatabase, krate: hir::Crate) -> &'static str {
    match krate.origin(db) {
        ide_db::base_db::CrateOrigin::Local { .. } => "workspace",
        ide_db::base_db::CrateOrigin::Library { .. } => "dependency",
        ide_db::base_db::CrateOrigin::Lang(_) => "lang",
        ide_db::base_db::CrateOrigin::Rustc { .. } => "rustc",
    }
}

/// One call site the walker found, before it is written.
struct FoundCall {
    syntax: &'static str,
    resolution: &'static str,
    callee: Option<hir::Function>,
    at: Located,
}

/// Walk one function's body, resolving each call and writing it with its edges.
fn resolve_body(
    writer: &Writer,
    function: hir::Function,
    caller_id: i64,
    catalog: &mut Catalog,
    blind: &mut BlindSpots,
) -> anyhow::Result<()> {
    if !function.has_body(writer.db) {
        return Ok(());
    }
    let sema = &writer.sema;
    let Some(source) = sema.source(function) else {
        return Ok(());
    };
    let Some(body) = source.value.body() else {
        return Ok(());
    };
    if u32::from(body.syntax().text_range().len()) > MAX_BODY_BYTES {
        blind.bodies_too_large += 1;
        return Ok(());
    }
    let mut walk = BodyWalk {
        found: Vec::new(),
        nodes_left: MAX_BODY_NODES,
    };
    walk_calls(sema, body.syntax(), 0, &mut walk, blind);
    if walk.nodes_left == 0 {
        blind.bodies_over_budget += 1;
    }
    for call in &walk.found {
        write_call(writer, caller_id, call, catalog, blind)?;
    }
    Ok(())
}

/// What one body's walk has found so far, and how much of its node ceiling is
/// left. A zero ceiling stops the walk: it is the one place the indexer stops
/// short, and it is counted where it happens.
struct BodyWalk {
    found: Vec<FoundCall>,
    nodes_left: u32,
}

/// Collect the calls in one syntax tree, descending into macro expansions so a
/// call written inside `format!` or produced by an attribute macro is seen.
fn walk_calls(
    sema: &Semantics<'_, RootDatabase>,
    node: &SyntaxNode,
    depth: u32,
    walk: &mut BodyWalk,
    blind: &mut BlindSpots,
) {
    for descendant in node.descendants() {
        if walk.nodes_left == 0 {
            return;
        }
        walk.nodes_left -= 1;
        if let Some(macro_call) = ast::MacroCall::cast(descendant.clone()) {
            expand_into(sema, &macro_call, depth, walk, blind);
        } else if let Some(callable) = ast::CallableExpr::cast(descendant) {
            let call = classify_call(sema, &callable, blind);
            walk.found.push(call);
        }
    }
}

/// Expand one macro call and walk what it produced, counting the expansions that
/// did not happen instead of passing over them.
fn expand_into(
    sema: &Semantics<'_, RootDatabase>,
    macro_call: &ast::MacroCall,
    depth: u32,
    walk: &mut BodyWalk,
    blind: &mut BlindSpots,
) {
    if depth >= MAX_MACRO_DEPTH {
        blind.macro_depth_hits += 1;
        return;
    }
    let Some(expanded) = sema.expand_macro_call(macro_call) else {
        blind.unexpanded_macros += 1;
        return;
    };
    walk_calls(sema, &expanded.value, depth + 1, walk, blind);
}

/// Decide what one call expression resolves to.
fn classify_call(
    sema: &Semantics<'_, RootDatabase>,
    callable: &ast::CallableExpr,
    blind: &mut BlindSpots,
) -> FoundCall {
    match callable {
        ast::CallableExpr::Call(call) => classify_plain_call(sema, call, blind),
        ast::CallableExpr::MethodCall(call) => classify_method_call(sema, call, blind),
    }
}

/// Resolve `f(..)`: a named function, a tuple constructor, a closure value, or a
/// function pointer. Only the first is an edge; the rest are recorded as the
/// run-time decisions they are.
fn classify_plain_call(
    sema: &Semantics<'_, RootDatabase>,
    call: &ast::CallExpr,
    blind: &mut BlindSpots,
) -> FoundCall {
    let at = locate(sema, call.syntax());
    let callable = call
        .expr()
        .and_then(|expr| sema.type_of_expr(&expr))
        .and_then(|info| info.original.as_callable(sema.db));
    let Some(callable) = callable else {
        blind.unresolved_calls += 1;
        return unresolved("call", "unresolved", at);
    };
    from_callable_kind(sema, &callable.kind(), at, blind)
}

/// Turn what rust-analyzer decided a call expression is into a call record.
fn from_callable_kind(
    sema: &Semantics<'_, RootDatabase>,
    kind: &hir::CallableKind<'_>,
    at: Located,
    blind: &mut BlindSpots,
) -> FoundCall {
    match kind {
        hir::CallableKind::Function(function) => FoundCall {
            syntax: "call",
            resolution: resolution_of(sema, *function, blind),
            callee: Some(*function),
            at,
        },
        hir::CallableKind::Closure(_) | hir::CallableKind::FnImpl(_) => {
            blind.closure_calls += 1;
            unresolved("call", "closure", at)
        }
        hir::CallableKind::FnPtr => {
            blind.fn_pointer_calls += 1;
            unresolved("call", "fn_pointer", at)
        }
        _ => unresolved("call", "constructor", at),
    }
}

/// Resolve `receiver.m(..)` through rust-analyzer's method resolution.
fn classify_method_call(
    sema: &Semantics<'_, RootDatabase>,
    call: &ast::MethodCallExpr,
    blind: &mut BlindSpots,
) -> FoundCall {
    let at = locate(sema, call.syntax());
    let Some(function) = sema.resolve_method_call(call) else {
        blind.unresolved_calls += 1;
        return unresolved("method_call", "unresolved", at);
    };
    FoundCall {
        syntax: "method_call",
        resolution: resolution_of(sema, function, blind),
        callee: Some(function),
        at,
    }
}

/// Whether a resolved callee is the function that runs, or a trait signature
/// standing in for whichever implementation the receiver's real type selects.
fn resolution_of(
    sema: &Semantics<'_, RootDatabase>,
    function: hir::Function,
    blind: &mut BlindSpots,
) -> &'static str {
    let container = function
        .as_assoc_item(sema.db)
        .map(|item| item.container(sema.db));
    if let Some(AssocItemContainer::Trait(_)) = container {
        blind.trait_method_calls += 1;
        return "trait_method";
    }
    "function"
}

/// A call with no callee item, carrying why.
fn unresolved(syntax: &'static str, resolution: &'static str, at: Located) -> FoundCall {
    FoundCall {
        syntax,
        resolution,
        callee: None,
        at,
    }
}

/// Where a call site sits and how it reads, taken once per call expression.
struct Located {
    /// The call expression as written, for reading a result without opening the file.
    text: String,
    /// Byte offset of the expression, turned into a line number when it is written.
    offset: u32,
    /// The file the expression belongs to after mapping out of any macro expansion.
    file: Option<vfs::FileId>,
}

/// Locate one call expression in its original file.
///
/// Both the file and the offset come from the SAME mapped range. A node inside a
/// macro expansion has one offset in the expansion and another in the file the
/// macro was written in; mixing the two names a byte in the wrong text.
fn locate(sema: &Semantics<'_, RootDatabase>, node: &SyntaxNode) -> Located {
    let original = sema.original_range_opt(node);
    let file = original.map(|range| range.file_id.file_id(sema.db));
    let offset = original.map_or(0, |range| u32::from(range.range.start()));
    Located {
        text: node_text(node),
        offset,
        file,
    }
}

/// The call expression as written, on one line and clipped, so a query result
/// reads without opening the file.
fn node_text(node: &SyntaxNode) -> String {
    let mut text = node.text().to_string().replace('\n', " ");
    while text.contains("  ") {
        text = text.replace("  ", " ");
    }
    text.chars().take(160).collect()
}

/// Write one call row and, when it resolved to a function, its edge.
fn write_call(
    writer: &Writer,
    caller_id: i64,
    call: &FoundCall,
    catalog: &mut Catalog,
    blind: &mut BlindSpots,
) -> anyhow::Result<()> {
    let call_id = catalog.next_call_id;
    catalog.next_call_id += 1;
    let (file_id, line) = call_location(writer, call, catalog)?;
    store::insert_call(
        writer.connection,
        &store::CallRow {
            id: call_id,
            caller_id,
            file_id,
            line,
            syntax: call.syntax,
            resolution: call.resolution,
            text: call.at.text.clone(),
        },
    )?;
    writer.calls.set(writer.calls.get() + 1);
    write_call_edge(writer, caller_id, call_id, call, catalog, blind)
}

/// Write the edge a resolved call produces, indexing the callee if this is the
/// first time it has been seen.
fn write_call_edge(
    writer: &Writer,
    caller_id: i64,
    call_id: i64,
    call: &FoundCall,
    catalog: &mut Catalog,
    blind: &mut BlindSpots,
) -> anyhow::Result<()> {
    let Some(callee) = call.callee else {
        return Ok(());
    };
    let target = ensure_item(writer, callee, catalog, blind)?;
    store::insert_edge(
        writer.connection,
        &store::EdgeRow {
            caller_id,
            callee_id: target,
            call_id,
            kind: "static",
        },
    )?;
    writer.edges.set(writer.edges.get() + 1);
    Ok(())
}

/// The file and line of a call site, writing the file row on first sight.
fn call_location(
    writer: &Writer,
    call: &FoundCall,
    catalog: &mut Catalog,
) -> anyhow::Result<(Option<i64>, Option<i64>)> {
    let Some(file_id) = call.at.file else {
        return Ok((None, None));
    };
    let stored = ensure_file(writer, file_id, catalog)?;
    Ok((Some(stored), Some(writer.line_of(file_id, call.at.offset))))
}
