//! The shell-facing queries over a built index.
//!
//! Every answer is printed next to what the index could not see. A reachability
//! result that says "no path" is only meaningful beside the number of calls in
//! the searched region that never resolved, so those counts are part of the
//! output rather than a footnote somewhere else.

use std::collections::{HashMap, HashSet, VecDeque};

use rusqlite::Connection;

/// One item as the queries print it.
pub struct Item {
    pub id: i64,
    pub path: String,
    pub kind: String,
    pub file: String,
    pub line: i64,
}

/// Items whose canonical path contains `pattern`, or whose bare name equals it.
///
/// Matching on the path substring is what makes the tool usable from a shell:
/// `Runtime::execute` finds `fq_runtime::Runtime::execute` without the caller
/// having to know the module chain.
pub fn find_items(connection: &Connection, pattern: &str) -> Result<Vec<Item>, rusqlite::Error> {
    let mut statement = connection.prepare(
        "SELECT item.id, item.path, item.kind, COALESCE(file.path, '?'), COALESCE(item.line, 0)
         FROM item LEFT JOIN file ON file.id = item.file_id
         WHERE item.path LIKE '%' || ?1 || '%' OR item.name = ?1
         ORDER BY item.path",
    )?;
    let rows = statement.query_map(rusqlite::params![pattern], |row| {
        Ok(Item {
            id: row.get(0)?,
            path: row.get(1)?,
            kind: row.get(2)?,
            file: row.get(3)?,
            line: row.get(4)?,
        })
    })?;
    let mut items = Vec::new();
    for row in rows {
        items.push(row?);
    }
    Ok(items)
}

/// One call site as the queries print it.
pub struct CallSite {
    pub caller_path: String,
    pub file: String,
    pub line: i64,
    pub resolution: String,
    pub text: String,
}

/// The call sites that reach the given items, i.e. who calls X.
pub fn callers_of(
    connection: &Connection,
    item_ids: &[i64],
) -> Result<Vec<CallSite>, rusqlite::Error> {
    let list = id_list(item_ids);
    let sql = format!(
        "SELECT item.path, COALESCE(file.path, '?'), COALESCE(call.line, 0),
                call.resolution, call.text
         FROM edge
         JOIN call ON call.id = edge.call_id
         JOIN item ON item.id = edge.caller_id
         LEFT JOIN file ON file.id = call.file_id
         WHERE edge.callee_id IN ({list})
         ORDER BY item.path"
    );
    collect_call_sites(connection, &sql)
}

/// The calls made by the given items, resolved or not, i.e. what X calls.
pub fn calls_from(
    connection: &Connection,
    item_ids: &[i64],
) -> Result<Vec<CallSite>, rusqlite::Error> {
    let list = id_list(item_ids);
    let sql = format!(
        "SELECT item.path, COALESCE(file.path, '?'), COALESCE(call.line, 0),
                call.resolution, call.text
         FROM call
         JOIN item ON item.id = call.caller_id
         LEFT JOIN file ON file.id = call.file_id
         WHERE call.caller_id IN ({list})
         ORDER BY call.id"
    );
    collect_call_sites(connection, &sql)
}

/// Run a call-site query that already selects the five printed columns.
fn collect_call_sites(
    connection: &Connection,
    sql: &str,
) -> Result<Vec<CallSite>, rusqlite::Error> {
    let mut statement = connection.prepare(sql)?;
    let rows = statement.query_map([], |row| {
        Ok(CallSite {
            caller_path: row.get(0)?,
            file: row.get(1)?,
            line: row.get(2)?,
            resolution: row.get(3)?,
            text: row.get(4)?,
        })
    })?;
    let mut sites = Vec::new();
    for row in rows {
        sites.push(row?);
    }
    Ok(sites)
}

/// Render an id list for an `IN (...)` clause. The ids come from the index's own
/// primary keys, never from user text.
fn id_list(ids: &[i64]) -> String {
    let mut rendered = Vec::new();
    for id in ids {
        rendered.push(id.to_string());
    }
    if rendered.is_empty() {
        return "NULL".to_string();
    }
    rendered.join(",")
}

/// Put a set of item ids into a temporary table and return its name.
///
/// A reachable set here runs to six figures, and an `IN (...)` list of that many
/// literals is a megabyte of SQL text; a temp table joins instead. The temp
/// database is writable even when the index itself is opened read-only.
fn stage_ids(connection: &Connection, ids: &HashSet<i64>) -> Result<&'static str, rusqlite::Error> {
    connection.execute("DROP TABLE IF EXISTS temp.subject", [])?;
    connection.execute("CREATE TEMP TABLE subject(id INTEGER PRIMARY KEY)", [])?;
    let mut insert = connection.prepare("INSERT OR IGNORE INTO temp.subject(id) VALUES (?1)")?;
    for id in ids {
        insert.execute(rusqlite::params![id])?;
    }
    Ok("temp.subject")
}

/// The whole call graph held in memory for a walk, in both directions.
pub struct Graph {
    /// caller -> callee for the statically resolved edges.
    forward: HashMap<i64, Vec<i64>>,
    /// callee -> caller, the same edges read the other way.
    backward: HashMap<i64, Vec<i64>>,
    /// trait signature -> the implementations a dyn call may dispatch to.
    dyn_targets: HashMap<i64, Vec<i64>>,
    /// implementation -> the trait signature a dyn call would name it by.
    dyn_sources: HashMap<i64, Vec<i64>>,
}

/// Load every edge and every trait dispatch link, in both directions.
pub fn load_graph(connection: &Connection) -> Result<Graph, rusqlite::Error> {
    let mut graph = Graph {
        forward: HashMap::new(),
        backward: HashMap::new(),
        dyn_targets: HashMap::new(),
        dyn_sources: HashMap::new(),
    };
    let mut statement = connection.prepare("SELECT caller_id, callee_id FROM edge")?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let (from_item, to_item): (i64, i64) = (row.get(0)?, row.get(1)?);
        graph.forward.entry(from_item).or_default().push(to_item);
        graph.backward.entry(to_item).or_default().push(from_item);
    }
    let mut statement = connection.prepare("SELECT trait_item_id, impl_item_id FROM dyn_target")?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let (declared, implemented): (i64, i64) = (row.get(0)?, row.get(1)?);
        graph
            .dyn_targets
            .entry(declared)
            .or_default()
            .push(implemented);
        graph
            .dyn_sources
            .entry(implemented)
            .or_default()
            .push(declared);
    }
    Ok(graph)
}

/// Every item that can transitively reach the given items, walking the edges
/// backwards.
///
/// A trait implementation is reached from the trait signature, so an ancestor
/// that only ever calls through `dyn Trait` is included; the walk does not check
/// whether that receiver could really hold this implementation, so the set is an
/// upper bound, deliberately.
pub fn ancestors(graph: &Graph, targets: &[i64]) -> HashSet<i64> {
    let mut seen: HashSet<i64> = targets.iter().copied().collect();
    let mut queue: VecDeque<i64> = targets.iter().copied().collect();
    while let Some(item) = queue.pop_front() {
        push_predecessors(graph.backward.get(&item), &mut seen, &mut queue);
        push_predecessors(graph.dyn_sources.get(&item), &mut seen, &mut queue);
    }
    seen
}

/// Queue the predecessors not yet seen.
fn push_predecessors(
    predecessors: Option<&Vec<i64>>,
    seen: &mut HashSet<i64>,
    queue: &mut VecDeque<i64>,
) {
    for predecessor in predecessors.into_iter().flatten() {
        if seen.insert(*predecessor) {
            queue.push_back(*predecessor);
        }
    }
}

/// The items of the given set that belong to crates of the named origin,
/// with their paths and locations.
pub fn items_from_origin(
    connection: &Connection,
    ids: &HashSet<i64>,
    origin: &str,
) -> Result<Vec<Item>, rusqlite::Error> {
    let subject = stage_ids(connection, ids)?;
    let sql = format!(
        "SELECT item.id, item.path, item.kind, COALESCE(file.path, '?'), COALESCE(item.line, 0)
         FROM item
         JOIN {subject} ON {subject}.id = item.id
         JOIN krate ON krate.id = item.krate_id
         LEFT JOIN file ON file.id = item.file_id
         WHERE krate.origin = ?1
         ORDER BY item.path"
    );
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map(rusqlite::params![origin], |row| {
        Ok(Item {
            id: row.get(0)?,
            path: row.get(1)?,
            kind: row.get(2)?,
            file: row.get(3)?,
            line: row.get(4)?,
        })
    })?;
    let mut items = Vec::new();
    for row in rows {
        items.push(row?);
    }
    Ok(items)
}

/// How an item was reached, so a printed path shows where the walk stopped being
/// certain.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// The callee rust-analyzer resolved for the call.
    Static,
    /// One implementation of a trait method the call resolved to. The receiver's
    /// real type decides which implementation runs; the walk assumes any of them.
    Dyn,
}

/// The result of a reachability walk.
pub struct Reachability {
    /// Every item reached, with the item it was reached from and how.
    pub parents: HashMap<i64, (i64, Step)>,
    /// Items reached without ever crossing a `Dyn` step.
    pub static_only: HashSet<i64>,
    /// Items whose successors are still to be expanded, paired with whether the
    /// route that reached them stayed inside resolved calls.
    pending: VecDeque<(i64, bool)>,
}

impl Reachability {
    /// Start a walk at the entry items, all of them reached statically by
    /// definition.
    fn starting_at(entries: &[i64]) -> Self {
        let mut walk = Reachability {
            parents: HashMap::new(),
            static_only: HashSet::new(),
            pending: VecDeque::new(),
        };
        for entry in entries {
            walk.parents.insert(*entry, (*entry, Step::Static));
            walk.static_only.insert(*entry);
            walk.pending.push_back((*entry, true));
        }
        walk
    }

    /// Push the successors of one item: the callees of its resolved calls, and
    /// the implementations of any trait method it is.
    fn expand(&mut self, graph: &Graph, item: i64, purely_static: bool) {
        for callee in graph.forward.get(&item).into_iter().flatten() {
            self.reach(*callee, item, Step::Static, purely_static);
        }
        for implementation in graph.dyn_targets.get(&item).into_iter().flatten() {
            self.reach(*implementation, item, Step::Dyn, false);
        }
    }

    /// Record a newly reached item, and re-visit one already reached only when
    /// this route upgrades it from "via dispatch" to "resolved throughout".
    fn reach(&mut self, callee: i64, from: i64, step: Step, purely_static: bool) {
        let first_visit = !self.parents.contains_key(&callee);
        let upgrades = purely_static && !self.static_only.contains(&callee);
        if !first_visit && !upgrades {
            return;
        }
        if first_visit {
            self.parents.insert(callee, (from, step));
        }
        if purely_static {
            self.static_only.insert(callee);
        }
        self.pending.push_back((callee, purely_static));
    }
}

/// Walk forward from the entry items over resolved edges and trait dispatch.
///
/// Two answers come out of one walk: the set reachable through resolved calls
/// alone, and the set reachable when a call to a trait method is allowed to
/// dispatch to any implementation. The first is what the index is certain of;
/// the second is the bound it cannot rule out. Reporting one without the other
/// would either overstate or understate the answer.
pub fn reachable(graph: &Graph, entries: &[i64]) -> Reachability {
    let mut walk = Reachability::starting_at(entries);
    while let Some((item, purely_static)) = walk.pending.pop_front() {
        walk.expand(graph, item, purely_static);
    }
    walk
}

/// The chain of items from an entry to `target`, as ids paired with the step
/// that reached each one.
pub fn path_to(result: &Reachability, target: i64) -> Vec<(i64, Step)> {
    let mut chain = vec![(target, Step::Static)];
    let mut current = target;
    while let Some((parent, step)) = result.parents.get(&current) {
        if *parent == current {
            break;
        }
        chain.push((*parent, *step));
        current = *parent;
    }
    chain.reverse();
    chain
}

/// The path of an item, for printing a walk.
pub fn item_path(connection: &Connection, id: i64) -> Result<String, rusqlite::Error> {
    connection.query_row(
        "SELECT path FROM item WHERE id = ?1",
        rusqlite::params![id],
        |row| row.get(0),
    )
}

/// How many calls in a set of items never resolved, broken down by why.
pub fn unresolved_within(
    connection: &Connection,
    item_ids: &HashSet<i64>,
) -> Result<Vec<(String, i64)>, rusqlite::Error> {
    let subject = stage_ids(connection, item_ids)?;
    let sql = format!(
        "SELECT call.resolution, COUNT(*) FROM call
         JOIN {subject} ON {subject}.id = call.caller_id
         GROUP BY call.resolution ORDER BY call.resolution"
    );
    let mut statement = connection.prepare(&sql)?;
    let rows = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
    let mut counts = Vec::new();
    for row in rows {
        counts.push(row?);
    }
    Ok(counts)
}

/// Everything the index recorded about its own coverage.
pub fn coverage(connection: &Connection) -> Result<Vec<(String, String)>, rusqlite::Error> {
    let mut statement = connection.prepare("SELECT key, value FROM meta ORDER BY key")?;
    let rows = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
    let mut facts = Vec::new();
    for row in rows {
        facts.push(row?);
    }
    Ok(facts)
}

/// The row counts of every table, so a query result can be read against the size
/// of what was searched.
pub fn table_counts(connection: &Connection) -> Result<Vec<(String, i64)>, rusqlite::Error> {
    let mut counts = Vec::new();
    for table in ["krate", "file", "item", "call", "edge", "dyn_target"] {
        let count: i64 =
            connection.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })?;
        counts.push((table.to_string(), count));
    }
    Ok(counts)
}

/// How many of the crates in the index had their bodies read, and how many
/// crates there are. A crate is in the index the moment something calls into it,
/// so the difference is the part of the graph that is named but not walked.
pub fn scanned_crates(connection: &Connection) -> Result<(i64, i64), rusqlite::Error> {
    connection.query_row(
        "SELECT COALESCE(SUM(scanned), 0), COUNT(*) FROM krate",
        [],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
}

/// The crates whose body walk was started and stopped at the deadline, with how
/// many bodies were left, so a query result is read next to the part of the
/// graph the index began and never finished.
pub fn partial_crates(connection: &Connection) -> Result<Vec<(String, i64)>, rusqlite::Error> {
    let mut statement = connection.prepare(
        "SELECT name, unread_bodies FROM krate
         WHERE scanned = 1 AND unread_bodies > 0
         ORDER BY unread_bodies DESC",
    )?;
    let rows = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
    let mut crates = Vec::new();
    for row in rows {
        crates.push(row?);
    }
    Ok(crates)
}

/// The partially read crates that the given items belong to.
pub fn partial_crates_of(
    connection: &Connection,
    ids: &HashSet<i64>,
) -> Result<Vec<(String, i64)>, rusqlite::Error> {
    let subject = stage_ids(connection, ids)?;
    let sql = format!(
        "SELECT krate.name, krate.unread_bodies
         FROM krate
         WHERE krate.scanned = 1 AND krate.unread_bodies > 0
           AND EXISTS (SELECT 1 FROM item JOIN {subject} ON {subject}.id = item.id
                       WHERE item.krate_id = krate.id)
         ORDER BY krate.unread_bodies DESC"
    );
    collect_crate_counts(connection, &sql)
}

/// The crates the given items reach into whose bodies were never read, with how
/// many of the items are theirs. These are where the walk stopped.
pub fn unscanned_crates_of(
    connection: &Connection,
    ids: &HashSet<i64>,
) -> Result<Vec<(String, i64)>, rusqlite::Error> {
    let subject = stage_ids(connection, ids)?;
    let sql = format!(
        "SELECT krate.name, COUNT(*)
         FROM item
         JOIN {subject} ON {subject}.id = item.id
         JOIN krate ON krate.id = item.krate_id
         WHERE krate.scanned = 0
         GROUP BY krate.id
         ORDER BY COUNT(*) DESC"
    );
    collect_crate_counts(connection, &sql)
}

/// Run a query that selects a crate name and one count.
fn collect_crate_counts(
    connection: &Connection,
    sql: &str,
) -> Result<Vec<(String, i64)>, rusqlite::Error> {
    let mut statement = connection.prepare(sql)?;
    let rows = statement.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
    let mut crates = Vec::new();
    for row in rows {
        crates.push(row?);
    }
    Ok(crates)
}
