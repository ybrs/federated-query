//! Dim shipping: collapse a shippable cross-source fact-dimension subtree into ONE
//! island by shipping the foreign (dimension) relations INTO the local (fact)
//! source as temp tables. Ports `federated_query/optimizer/dim_shipping.py`
//! (SPEC-dim-shipping.md).
//!
//! When a large fact (on one source) INNER-joins small dimensions (on another) and
//! the query GROUP BYs dimension columns, the fact cannot collapse into its own
//! source and every fact row must cross to the coordinator. Dim shipping ships the
//! small dimensions INTO the fact's source, so the whole join+aggregate runs as one
//! island there and only the aggregate OUTPUT crosses the boundary.
//!
//! DECLINE vs RAISE (CLAUDE.md "a crash never ships a lie"): every GATE returns
//! `Ok(None)` on a miss - the sanctioned decline, distinct from a raise. Declining
//! keeps the proven no-ship plan, which never changes a RESULT, only a runtime.
//! Only a genuine collaborator failure (a cost-model `estimate`, a stats-catalog
//! read, a sub-plan) RAISES; a decline is never manufactured from an error.
//!
//! The rule is STATELESS: every collaborator (cost model, catalog, single-source
//! pushdown, ship-name counter, sub-planner, learned-stats catalog) is reached
//! through the `&mut PhysicalPlanner` handed to `try_ship`, so no aliasing
//! back-reference is held (Python's `DimShipping(self)` cycle is inverted here).

use std::collections::{HashMap, HashSet};
use std::rc::Rc;

use fq_catalog::datasource::{DataSourceCapability, RenderDialect};
use fq_catalog::StatsCatalog;
use fq_optimize::cost::group_subject;
use fq_optimize::{group_column_names, CostModel};
use fq_plan::expr::Expr;
use fq_plan::logical::{Aggregate, Join, JoinType, LogicalPlan, Scan};
use fq_plan::physical::{GroupObservation, PhysicalPlan, PhysicalRemoteQuery, PhysicalShipment};

use crate::error::PhysicalError;
use crate::planner::PhysicalPlanner;

type Result<T> = std::result::Result<T, PhysicalError>;

// --- constants (SPEC section 4; values are dim_shipping.py lines 46-81) --------

/// DuckDB `CREATE TEMP TABLE` schema qualifier (`_DUCK_SHIP_SCHEMA`).
const DUCK_SHIP_SCHEMA: &str = "temp";
/// Postgres session temp-table schema qualifier (`_PG_SHIP_SCHEMA`).
const PG_SHIP_SCHEMA: &str = "pg_temp";
/// The fact side must clear this many rows; below it the fact transfer is cheap
/// and the temp-table build is not worth it (`SHIP_LOCAL_FLOOR`).
const SHIP_LOCAL_FLOOR: u64 = 100_000;
/// Never ship more than this many foreign rows into a source: a big shipped dim
/// costs a big build AND means the fact is not the dominant transfer (excludes the
/// q38/q87 ~500k customer regressions) (`SHIP_ROW_BUDGET`).
const SHIP_ROW_BUDGET: u64 = 200_000;
/// The local side must exceed the shipped foreign side by at least this factor
/// (`SHIP_MIN_RATIO`).
const SHIP_MIN_RATIO: u64 = 20;
/// A group-key dimension with NDV >= this is high-cardinality; 10k sits with wide
/// margin between every TPC-DS dim that ships (`HIGH_CARD_NDV`).
const HIGH_CARD_NDV: i64 = 10_000;
/// A ship-target aggregate whose MEASURED group count keeps more than this fraction
/// of its estimated input rows does not collapse (`SHIP_COLLAPSE_MAX_FRACTION`).
const SHIP_COLLAPSE_MAX_FRACTION: f64 = 0.1;

/// A base scan's identity within the analyzed tree: its ADDRESS. The tree is
/// borrowed (never cloned) from `collect_base_scans` through `replace`, so a scan's
/// address is stable across the analysis, and a self-join's two structurally-equal
/// scans key INDEPENDENTLY (a value-equality map would collide them). SPEC 6.4(A).
type ScanId = usize;

/// The address of a borrowed scan node (its `ScanId`).
fn scan_id(scan: &Scan) -> ScanId {
    std::ptr::from_ref(scan) as ScanId
}

/// The gate verdict: the fact source name and the per-scan row estimates, threaded
/// from `analyze` into `build`. Keyed by `ScanId` (address), valid while the
/// analyzed `node` lives (it does, through `build`).
struct ShipAnalysis {
    local: String,
    rows: HashMap<ScanId, Option<u64>>,
}

/// Dim-shipping rule. A unit value; all state lives on the planner it is handed.
pub struct DimShipping;

impl DimShipping {
    /// Return a `PhysicalShipment` collapsing `node` into one island, or `None` when
    /// the subtree's shape or size does not qualify (the sanctioned decline). Err
    /// only on a genuine collaborator failure, never as a decline.
    pub fn try_ship(
        &self,
        planner: &mut PhysicalPlanner,
        node: &LogicalPlan,
    ) -> Result<Option<PhysicalPlan>> {
        let Some(analysis) = analyze(planner, node)? else {
            return Ok(None);
        };
        build(planner, node, &analysis)
    }
}

// --- analyze: the gate sequence (SPEC 3.1) ------------------------------------

/// Run the gates IN ORDER; the FIRST miss returns `Ok(None)`. The ordering is
/// load-bearing: `dimension_explosion` (5) reads the aggregate that
/// `collapses_via_aggregate` (4) confirmed, so it must run after it.
fn analyze(planner: &mut PhysicalPlanner, node: &LogicalPlan) -> Result<Option<ShipAnalysis>> {
    // 1. KILL SWITCH: only the exact string "0" disables shipping (a proven
    // win, off only deliberately - not empty, not "false").
    if std::env::var("FEDQ_DIM_SHIPPING").ok().as_deref() == Some("0") {
        return Ok(None);
    }
    // 2. COST MODEL PRESENT: every downstream gate needs it.
    let Some(cost_rc) = planner.cost_model().map(Rc::clone) else {
        return Ok(None);
    };
    // 3. SHIPPABLE SHAPE. 4. PLAIN-AGGREGATE-COLLAPSE ROOT.
    if !is_shippable_shape(node) || !collapses_via_aggregate(node) {
        return Ok(None);
    }
    // 5. DIMENSION EXPLOSION (measured override, else the width heuristic).
    let stats_catalog = planner.stats_catalog();
    let ttl = planner.learned_ttl_seconds();
    {
        let mut cost_model = cost_rc.borrow_mut();
        if dimension_explosion(&mut cost_model, stats_catalog.as_deref(), ttl, node)? {
            return Ok(None);
        }
    }
    // 6. COLLECT SCANS + ROWS.
    let mut scans = Vec::new();
    PhysicalPlanner::collect_base_scans(node, &mut scans);
    let rows = {
        let mut cost_model = cost_rc.borrow_mut();
        scan_rows_map(&mut cost_model, &scans)?
    };
    // 7. LOCAL SOURCE + ship-target capability.
    let Some(local) = local_source(&scans, &rows) else {
        return Ok(None);
    };
    if !local_is_ship_target(planner, &local) {
        return Ok(None);
    }
    // 8. FOREIGN SCANS.
    let foreign: Vec<&Scan> = scans
        .iter()
        .copied()
        .filter(|scan| scan.datasource != local)
        .collect();
    if foreign.is_empty() {
        return Ok(None);
    }
    // 9. COST GATE.
    if !cost_gate_passes(&scans, &rows, &local, &foreign) {
        return Ok(None);
    }
    Ok(Some(ShipAnalysis { local, rows }))
}

/// Map each collected scan (by identity) to its row estimate, `None` when
/// unknown. SPEC 3.2.
fn scan_rows_map(
    cost_model: &mut CostModel,
    scans: &[&Scan],
) -> Result<HashMap<ScanId, Option<u64>>> {
    let mut rows = HashMap::with_capacity(scans.len());
    for scan in scans {
        rows.insert(scan_id(scan), scan_rows(cost_model, scan)?);
    }
    Ok(rows)
}

/// One scan's row estimate, or `None` when unknown OR resting on ANY default (a
/// gap-fed size is left None: the rule declines rather than ship on a guess).
/// An `estimate` FAILURE propagates (a real cost-model fault, not a decline).
fn scan_rows(cost_model: &mut CostModel, scan: &Scan) -> Result<Option<u64>> {
    if let Some(rows) = scan.estimated_rows {
        return Ok(Some(rows));
    }
    let estimate = cost_model.estimate(&LogicalPlan::Scan(Box::new(scan.clone())))?;
    if estimate.rows.is_none() || !estimate.defaults_used.is_empty() {
        return Ok(None);
    }
    Ok(estimate.rows)
}

// --- gate 5: dimension explosion (SPEC 3.5, 3.11, 3.12) -------------------

/// The ship-target aggregate's GROUP BY spans two or more independent high-card
/// dimensions, so it will not collapse. A MEASURED group count OVERRIDES the
/// width heuristic in both directions. Runs only after `collapses_via_aggregate`
/// so `ship_aggregate` is guaranteed a plain Aggregate.
fn dimension_explosion(
    cost_model: &mut CostModel,
    stats: Option<&StatsCatalog>,
    ttl: Option<i64>,
    node: &LogicalPlan,
) -> Result<bool> {
    if let Some(measured) = measured_explosion(cost_model, stats, ttl, node)? {
        return Ok(measured);
    }
    let agg = expect_ship_aggregate(node);
    Ok(count_high_dimensions(cost_model, agg)? >= 2)
}

/// The MEASURED collapse verdict, or `None` when nothing was learned / the input
/// is unjudgeable (then the width heuristic decides). Explosion = the measured
/// output keeps more than `SHIP_COLLAPSE_MAX_FRACTION` of the estimated input.
fn measured_explosion(
    cost_model: &mut CostModel,
    stats: Option<&StatsCatalog>,
    ttl: Option<i64>,
    node: &LogicalPlan,
) -> Result<Option<bool>> {
    let Some(observation) = island_group_observation(node) else {
        return Ok(None);
    };
    let Some(groups) = learned_group_count(stats, ttl, &observation)? else {
        return Ok(None);
    };
    let agg = expect_ship_aggregate(node);
    let input_rows = cost_model.estimate(&agg.input)?.rows;
    // Python `if not input_rows`: None OR 0 both abstain.
    let Some(input_rows) = input_rows.filter(|&rows| rows != 0) else {
        return Ok(None);
    };
    Ok(Some(measured_collapse_exceeds(groups, input_rows)))
}

/// The catalog's MEASURED group count for a stamped subject/key set, or `None`
/// when no learned catalog is wired or nothing was recorded. A catalog FAULT
/// propagates (SPEC 3.12 / section 12.7): mapping it to `None` would silently
/// hide the fault; a genuinely-empty result is `Ok(None)`.
fn learned_group_count(
    stats: Option<&StatsCatalog>,
    ttl: Option<i64>,
    observation: &GroupObservation,
) -> Result<Option<i64>> {
    let Some(catalog) = stats else {
        return Ok(None);
    };
    Ok(catalog.group_count(&observation.subject, &observation.columns, ttl)?)
}

/// The number of distinct source dimensions contributing a high-card group key.
/// Correlated keys from ONE relation share an owner (pointer identity) and count
/// once; an ownerless high-card key counts as its own dimension. SPEC section 10.
fn count_high_dimensions(cost_model: &CostModel, agg: &Aggregate) -> Result<usize> {
    let mut owners: HashSet<usize> = HashSet::new();
    let mut ownerless = 0usize;
    for key in &agg.group_by {
        let (contributes, owner) = high_card_owner(cost_model, agg, key)?;
        if !contributes {
            continue;
        }
        match owner {
            Some(owner_id) => {
                owners.insert(owner_id);
            }
            None => ownerless += 1,
        }
    }
    Ok(owners.len() + ownerless)
}

/// `(is_high_card, owner_identity)` for one group key. A resolved NDV below the
/// threshold is low-card and does not contribute; an unknown NDV is treated as
/// high-card (conservative). The owner identity is the relation node's ADDRESS,
/// so two keys from the same relation collapse to one dimension.
fn high_card_owner(
    cost_model: &CostModel,
    agg: &Aggregate,
    key: &Expr,
) -> Result<(bool, Option<usize>)> {
    let (owner, ndv) = cost_model.group_key_dimension(&agg.input, key)?;
    if let Some(ndv) = ndv {
        if ndv < HIGH_CARD_NDV {
            return Ok((false, None));
        }
    }
    Ok((true, owner.map(|node| std::ptr::from_ref(node) as usize)))
}

// --- build: swap, collapse, wrap (SPEC 3.10, 3.13-3.17) -------------------

/// Swap the foreign scans for temp scans, collapse to one island, and wrap it in
/// the shipments. `None` when the swap does not collapse into an island that
/// exposes EXACTLY the pure plan's output columns.
fn build(
    planner: &mut PhysicalPlanner,
    node: &LogicalPlan,
    analysis: &ShipAnalysis,
) -> Result<Option<PhysicalPlan>> {
    let (mapping, shipments) = synthetic_scans(planner, node, analysis);
    let swapped = replace(node, &mapping);
    let Some(mut island) = planner.single_source().try_build(&swapped)? else {
        return Ok(None);
    };
    let fallback = planner.plan_without_shipping(node)?;
    if has_window(&fallback) || !outputs_match(&island, &fallback) {
        return Ok(None);
    }
    island.seeded_schema = Some(fallback.schema());
    island.group_observation = island_group_observation(node);
    let island = PhysicalPlan::RemoteQuery(Box::new(island));
    Ok(Some(wrap_shipments(
        planner,
        &shipments,
        island,
        &analysis.local,
    )?))
}

/// Build a temp scan for each foreign scan (in `collect_base_scans` DFS order);
/// return the identity->temp map and the `(temp_table_name, foreign_scan)`
/// shipment list. SPEC 3.13. The `next_ship_name` DFS order is what makes the
/// N-th foreign scan get `__fedq_ship_N` deterministically (SPEC 6.4 invariant).
fn synthetic_scans(
    planner: &mut PhysicalPlanner,
    node: &LogicalPlan,
    analysis: &ShipAnalysis,
) -> (HashMap<ScanId, Scan>, Vec<(String, Scan)>) {
    let mut scans = Vec::new();
    PhysicalPlanner::collect_base_scans(node, &mut scans);
    let schema = ship_schema(planner, &analysis.local);
    let mut mapping = HashMap::new();
    let mut shipments = Vec::new();
    for scan in scans {
        if scan.datasource == analysis.local {
            continue;
        }
        let name = planner.next_ship_name();
        let estimated_rows = analysis.rows.get(&scan_id(scan)).copied().flatten();
        let temp = temp_scan(scan, &name, &analysis.local, &schema, estimated_rows);
        mapping.insert(scan_id(scan), temp);
        shipments.push((name, scan.clone()));
    }
    (mapping, shipments)
}

/// Rebuild the subtree with each mapped scan replaced by its temp scan, walking
/// the ORIGINAL borrowed tree so scan addresses match the mapping keys. The
/// subtree is guaranteed shippable (`is_shippable_shape`), so only the shippable
/// node kinds appear; any other is a violated invariant surfaced loudly. SPEC 3.14.
fn replace(node: &LogicalPlan, mapping: &HashMap<ScanId, Scan>) -> LogicalPlan {
    match node {
        LogicalPlan::Scan(scan) => match mapping.get(&scan_id(scan)) {
            Some(temp) => LogicalPlan::Scan(Box::new(temp.clone())),
            None => node.clone(),
        },
        LogicalPlan::Filter(n) => LogicalPlan::Filter(fq_plan::logical::Filter {
            input: Box::new(replace(&n.input, mapping)),
            ..n.clone()
        }),
        LogicalPlan::Projection(n) => LogicalPlan::Projection(fq_plan::logical::Projection {
            input: Box::new(replace(&n.input, mapping)),
            ..n.clone()
        }),
        LogicalPlan::Aggregate(n) => LogicalPlan::Aggregate(Aggregate {
            input: Box::new(replace(&n.input, mapping)),
            ..n.clone()
        }),
        LogicalPlan::Sort(n) => LogicalPlan::Sort(fq_plan::logical::Sort {
            input: Box::new(replace(&n.input, mapping)),
            ..n.clone()
        }),
        LogicalPlan::Limit(n) => LogicalPlan::Limit(fq_plan::logical::Limit {
            input: Box::new(replace(&n.input, mapping)),
            ..n.clone()
        }),
        LogicalPlan::SubqueryScan(n) => LogicalPlan::SubqueryScan(fq_plan::logical::SubqueryScan {
            input: Box::new(replace(&n.input, mapping)),
            ..n.clone()
        }),
        LogicalPlan::Join(n) => LogicalPlan::Join(Join {
            left: Box::new(replace(&n.left, mapping)),
            right: Box::new(replace(&n.right, mapping)),
            ..n.clone()
        }),
        _ => {
            unreachable!("replace over a non-shippable node; is_shippable_shape gates the subtree")
        }
    }
}

/// Whether the plan contains a window function: collapsing a window over a
/// grouping into one remote SELECT bypasses the two-stage window split and
/// renders SQL the source cannot bind, so such a subtree keeps its coordinator
/// plan. SPEC 3.15.
fn has_window(node: &PhysicalPlan) -> bool {
    if matches!(node, PhysicalPlan::Window(_)) {
        return true;
    }
    if let PhysicalPlan::HashAggregate(agg) = node {
        if agg.has_window_output() {
            return true;
        }
    }
    node.children().iter().any(|child| has_window(child))
}

/// Wrap the island in one `PhysicalShipment` per shipped foreign scan (innermost
/// last-shipped, so the emit order materializes each body before the island
/// reads it). Ships in REVERSE of the collection order. SPEC 3.17.
fn wrap_shipments(
    planner: &mut PhysicalPlanner,
    shipments: &[(String, Scan)],
    island: PhysicalPlan,
    local: &str,
) -> Result<PhysicalPlan> {
    let mut child = island;
    for (name, foreign_scan) in shipments.iter().rev() {
        // The FOREIGN scan planned as its own (cross-source) subtree = the
        // relation to materialize, WITH its own filters intact (not the bare
        // temp scan).
        let body =
            planner.plan_without_shipping(&LogicalPlan::Scan(Box::new(foreign_scan.clone())))?;
        child = PhysicalPlan::Shipment(PhysicalShipment {
            table: name.clone(),
            datasource: local.to_string(),
            body: Box::new(body),
            child: Box::new(child),
        });
    }
    Ok(child)
}

// --- free helpers (no collaborator state) --------------------------------------

/// Every node in the subtree is a deterministic INNER-only relational op a
/// dimension can be safely shipped across. SPEC 3.3.
fn is_shippable_shape(node: &LogicalPlan) -> bool {
    node_is_shippable(node)
        && node
            .children()
            .iter()
            .all(|child| is_shippable_shape(child))
}

/// A single node's shippability: an INNER join, or a listed row-preserving-or-
/// reducing node. A LEFT/SEMI/ANTI join's result depends on which side is
/// preserved, so relocating a dim across it could change the answer.
fn node_is_shippable(node: &LogicalPlan) -> bool {
    match node {
        LogicalPlan::Join(join) => join.join_type == JoinType::Inner,
        LogicalPlan::Scan(_)
        | LogicalPlan::Filter(_)
        | LogicalPlan::Projection(_)
        | LogicalPlan::Aggregate(_)
        | LogicalPlan::Sort(_)
        | LogicalPlan::Limit(_)
        | LogicalPlan::SubqueryScan(_) => true,
        LogicalPlan::Union(_)
        | LogicalPlan::SetOperation(_)
        | LogicalPlan::Explain(_)
        | LogicalPlan::Cte(_)
        | LogicalPlan::CteRef(_)
        | LogicalPlan::Values(_)
        | LogicalPlan::SingleRowGuard(_)
        | LogicalPlan::GroupedLimit(_)
        | LogicalPlan::LateralJoin(_) => false,
    }
}

/// The subtree must reduce its output through a PLAIN GROUP BY at its root (under
/// any row-preserving wrappers). A ROLLUP/CUBE aggregate collapses poorly in one
/// island; a bare join/scan ships the whole joined fact for no gain. SPEC 3.4.
fn collapses_via_aggregate(node: &LogicalPlan) -> bool {
    matches!(ship_aggregate(node), LogicalPlan::Aggregate(agg) if is_plain_group(agg))
}

/// A plain GROUP BY (no ROLLUP/CUBE/GROUPING SETS): `grouping_sets` absent or empty
/// (Python `not root.grouping_sets`).
fn is_plain_group(agg: &Aggregate) -> bool {
    agg.grouping_sets.as_ref().is_none_or(Vec::is_empty)
}

/// The node under the row-preserving wrappers {Projection, Filter, Sort, Limit,
/// SubqueryScan}. NOTE this wrapper set INCLUDES Filter and Limit, unlike
/// `island_group_observation`'s walk - the difference is intentional. SPEC 3.4.
fn ship_aggregate(node: &LogicalPlan) -> &LogicalPlan {
    let mut current = node;
    loop {
        current = match current {
            LogicalPlan::Projection(n) => &n.input,
            LogicalPlan::Filter(n) => &n.input,
            LogicalPlan::Sort(n) => &n.input,
            LogicalPlan::Limit(n) => &n.input,
            LogicalPlan::SubqueryScan(n) => &n.input,
            other => return other,
        };
    }
}

/// The ship-target aggregate, which `collapses_via_aggregate` proved is a plain
/// Aggregate before either explosion path runs. A non-Aggregate here is a violated
/// invariant, surfaced loudly rather than defaulted.
fn expect_ship_aggregate(node: &LogicalPlan) -> &Aggregate {
    match ship_aggregate(node) {
        LogicalPlan::Aggregate(agg) => agg,
        _ => unreachable!(
            "ship aggregate is not a plain Aggregate; collapses_via_aggregate gates this"
        ),
    }
}

/// Whether a MEASURED group count keeps more than `SHIP_COLLAPSE_MAX_FRACTION` of
/// the estimated input rows (the explosion verdict). Both sizes are large heuristic
/// counts; the f64 widening is a threshold judgement, not an exact arithmetic.
#[allow(clippy::cast_precision_loss)]
fn measured_collapse_exceeds(groups: i64, input_rows: u64) -> bool {
    groups as f64 > input_rows as f64 * SHIP_COLLAPSE_MAX_FRACTION
}

/// The learned-stats group provenance to stamp on the shipped island: the PRE-SHIP
/// aggregate's subject (the SAME key the cost model reads) plus its group column
/// names. `None` when a Filter or Limit sits between the ship root and the
/// aggregate (the island row count is then NOT the group count) or a key is not a
/// plain column. SPEC 3.11.
fn island_group_observation(node: &LogicalPlan) -> Option<GroupObservation> {
    let mut current = node;
    // Walk DOWN through {Projection, Sort, SubqueryScan} ONLY - NOT Filter, NOT
    // Limit (the distinguishing point vs ship_aggregate).
    let landing = loop {
        match current {
            LogicalPlan::Projection(n) => current = &n.input,
            LogicalPlan::Sort(n) => current = &n.input,
            LogicalPlan::SubqueryScan(n) => current = &n.input,
            other => break other,
        }
    };
    let LogicalPlan::Aggregate(agg) = landing else {
        return None;
    };
    if !is_plain_group(agg) {
        return None;
    }
    let columns = group_column_names(&agg.group_by)?;
    Some(GroupObservation {
        subject: group_subject(&agg.input),
        columns,
    })
}

/// The datasource NAME holding the largest scan (the fact side), or `None` when no
/// scan carries a cost estimate to compare by. SPEC 3.6.
fn local_source(scans: &[&Scan], rows: &HashMap<ScanId, Option<u64>>) -> Option<String> {
    let mut largest: Option<(&Scan, u64)> = None;
    for scan in scans {
        let Some(row_estimate) = rows.get(&scan_id(scan)).copied().flatten() else {
            continue;
        };
        if largest.is_none_or(|(_, best)| row_estimate > best) {
            largest = Some((scan, row_estimate));
        }
    }
    largest.map(|(scan, _)| scan.datasource.clone())
}

/// The ship target (the fact source) must ACCEPT a shipped temp table - a physical
/// capability, not a cost choice. SPEC 3.7.
fn local_is_ship_target(planner: &PhysicalPlanner, local: &str) -> bool {
    match planner.catalog().get_datasource(local) {
        Some(source) => source.supports_capability(DataSourceCapability::ShipTarget),
        None => false,
    }
}

/// The temp-table schema qualifier for the target: `pg_temp` for a Postgres fact
/// source, `temp` for DuckDB (a missing source falls to the DuckDB default,
/// matching Python's else branch). SPEC 3.18.
fn ship_schema(planner: &PhysicalPlanner, local: &str) -> String {
    match planner
        .catalog()
        .get_datasource(local)
        .map(|source| source.render_dialect())
    {
        Some(RenderDialect::Postgres) => PG_SHIP_SCHEMA.to_string(),
        _ => DUCK_SHIP_SCHEMA.to_string(),
    }
}

/// A LOCAL temp-table scan replacing a foreign scan, under the SAME alias and
/// columns so every reference above resolves unchanged. It is a FRESH bare read:
/// the foreign scan's filters/aggregates/ordering moved INTO the shipped body and
/// must be ABSENT here (Scan::new zero-defaults every optional clause). SPEC 3.13.
fn temp_scan(
    foreign: &Scan,
    table: &str,
    local: &str,
    schema: &str,
    estimated_rows: Option<u64>,
) -> Scan {
    let mut scan = Scan::new(local, schema, table, foreign.columns.clone());
    scan.alias = Some(
        foreign
            .alias
            .clone()
            .unwrap_or_else(|| foreign.table_name.clone()),
    );
    scan.estimated_rows = estimated_rows;
    scan
}

/// The local side is large, every foreign size is known and small, and the local
/// side dwarfs the foreign total by the required factor. SPEC 3.9.
fn cost_gate_passes(
    scans: &[&Scan],
    rows: &HashMap<ScanId, Option<u64>>,
    local: &str,
    foreign: &[&Scan],
) -> bool {
    if !all_estimated(rows, foreign) {
        return false;
    }
    let local_rows = max_rows(scans, rows, local);
    let foreign_rows = sum_rows(rows, foreign);
    ratio_ok(local_rows, foreign_rows)
}

/// True when every foreign scan carries a row estimate: a single unknown foreign
/// size DECLINES (a mis-shipped big dim would flood the source) - the correctness-
/// critical gate.
fn all_estimated(rows: &HashMap<ScanId, Option<u64>>, foreign: &[&Scan]) -> bool {
    foreign
        .iter()
        .all(|scan| rows.get(&scan_id(scan)).copied().flatten().is_some())
}

/// The largest estimate among the local source's scans (the fact size); 0 when none.
fn max_rows(scans: &[&Scan], rows: &HashMap<ScanId, Option<u64>>, local: &str) -> u64 {
    let mut largest = 0u64;
    for scan in scans {
        if scan.datasource == local {
            if let Some(row_estimate) = rows.get(&scan_id(scan)).copied().flatten() {
                largest = largest.max(row_estimate);
            }
        }
    }
    largest
}

/// The total estimated rows shipped across all foreign scans (all known by the
/// `all_estimated` gate above).
fn sum_rows(rows: &HashMap<ScanId, Option<u64>>, foreign: &[&Scan]) -> u64 {
    let mut total = 0u64;
    for scan in foreign {
        total += rows.get(&scan_id(scan)).copied().flatten().unwrap_or(0);
    }
    total
}

/// The floor / budget / ratio gates on the two side sizes. The multiply is widened
/// to u128 to avoid a u64 overflow on `foreign_rows * SHIP_MIN_RATIO`.
fn ratio_ok(local_rows: u64, foreign_rows: u64) -> bool {
    if local_rows < SHIP_LOCAL_FLOOR {
        return false;
    }
    if foreign_rows > SHIP_ROW_BUDGET {
        return false;
    }
    u128::from(local_rows) >= u128::from(foreign_rows) * u128::from(SHIP_MIN_RATIO)
}

/// The collapsed island must expose EXACTLY the columns (in order) that the pure
/// cross-source plan does. A shape that cannot cleanly collapse is DECLINED here
/// rather than emitted as a broken plan. SPEC 3.16.
fn outputs_match(island: &PhysicalRemoteQuery, fallback: &PhysicalPlan) -> bool {
    let fallback_names: Vec<String> = fallback
        .schema()
        .into_iter()
        .map(|(name, _)| name)
        .collect();
    island.output_names == fallback_names
}

#[cfg(test)]
mod tests {
    //! Gate unit tests: they exercise `dimension_explosion` and
    //! `island_group_observation` directly against a stats-backed cost model and a
    //! StatsCatalog test double - no live source or full planner. Ports
    //! `tests/test_dim_shipping_gate.py` (SPEC 11.1). Each case builds
    //! `Scan -> Join(INNER) -> Aggregate(plain)` and asserts on the gate.

    use std::collections::BTreeMap;
    use std::sync::Arc;

    use fq_catalog::datasource::{
        ColumnStatistics, DataSource, DataSourceCapability, TableMetadata, TableStatistics,
    };
    use fq_catalog::{Catalog, CatalogError, StatsCatalog};
    use fq_common::{CostConfig, DataType};
    use fq_optimize::{subplan_signature, CostModel, StatisticsCollector};
    use fq_plan::expr::{ColumnRef, Expr};
    use fq_plan::logical::{Aggregate, Join, JoinType, LogicalPlan, Scan};

    use super::{dimension_explosion, island_group_observation, GroupObservation};

    /// A table's row count and per-column NDV statistics.
    type TableStats = (Option<i64>, BTreeMap<String, ColumnStatistics>);

    /// A stats-serving fact/dimension source. Serves the SPEC 11.1 fixtures by
    /// table name; an unknown table returns `None` (no stats at all).
    struct FixtureSource {
        tables: BTreeMap<String, TableStats>,
    }

    impl DataSource for FixtureSource {
        // The trait returns `&str` tied to `&self`; this fixture answers a literal
        // (matching fq-optimize's end_to_end.rs test source convention).
        #[allow(clippy::unnecessary_literal_bound)]
        fn name(&self) -> &str {
            "src"
        }
        fn capabilities(&self) -> Vec<DataSourceCapability> {
            vec![DataSourceCapability::ShipTarget]
        }
        fn list_schemas(&self) -> Result<Vec<String>, CatalogError> {
            Ok(vec!["main".to_string()])
        }
        fn list_tables(&self, _schema: &str) -> Result<Vec<String>, CatalogError> {
            Ok(self.tables.keys().cloned().collect())
        }
        fn get_table_metadata(
            &self,
            schema: &str,
            table: &str,
        ) -> Result<TableMetadata, CatalogError> {
            Ok(TableMetadata {
                schema_name: schema.to_string(),
                table_name: table.to_string(),
                columns: vec![],
                row_count: None,
                size_bytes: None,
            })
        }
        fn get_table_statistics(
            &self,
            _schema: &str,
            table: &str,
            columns: &[String],
        ) -> Result<Option<TableStatistics>, CatalogError> {
            let Some((row_count, all_stats)) = self.tables.get(table) else {
                return Ok(None);
            };
            let mut column_stats = BTreeMap::new();
            for column in columns {
                if let Some(stats) = all_stats.get(column) {
                    column_stats.insert(column.clone(), stats.clone());
                }
            }
            Ok(Some(TableStatistics {
                row_count: *row_count,
                total_size_bytes: 0,
                column_stats,
            }))
        }
        fn map_native_type(&self, type_str: &str) -> Result<DataType, CatalogError> {
            fq_catalog::datasource::map_native_type_default(type_str)
        }
    }

    /// A column-NDV statistic.
    fn ndv(num_distinct: i64) -> ColumnStatistics {
        ColumnStatistics {
            num_distinct: Some(num_distinct),
            null_fraction: 0.0,
            avg_width: 8,
            min_value: None,
            max_value: None,
        }
    }

    /// The SPEC 11.1 fixtures: item 100000 rows (i_item_sk 100000, i_item_desc
    /// 60000), date_dim 70000 (d_date 70000), store 100 (s_state 30), warehouse 10
    /// rows with NO column stats. `mystery` is intentionally absent (no stats).
    fn fixtures() -> BTreeMap<String, TableStats> {
        let mut item = BTreeMap::new();
        item.insert("i_item_sk".to_string(), ndv(100_000));
        item.insert("i_item_desc".to_string(), ndv(60_000));
        let mut date_dim = BTreeMap::new();
        date_dim.insert("d_date".to_string(), ndv(70_000));
        let mut store = BTreeMap::new();
        store.insert("s_state".to_string(), ndv(30));
        let mut tables = BTreeMap::new();
        tables.insert("item".to_string(), (Some(100_000), item));
        tables.insert("date_dim".to_string(), (Some(70_000), date_dim));
        tables.insert("store".to_string(), (Some(100), store));
        tables.insert("warehouse".to_string(), (Some(10), BTreeMap::new()));
        tables
    }

    /// A catalog registering the stats-serving source (the gate tests hand-build
    /// logical plans, so no schema tree is needed for binding).
    fn catalog() -> Arc<Catalog> {
        let mut catalog = Catalog::new();
        catalog.register_datasource(Arc::new(FixtureSource { tables: fixtures() }));
        Arc::new(catalog)
    }

    /// A cost model over the fixture source, optionally overlaying a learned catalog.
    fn cost_model(learned: Option<Arc<StatsCatalog>>) -> CostModel {
        let collector = StatisticsCollector::new(catalog(), learned, None);
        CostModel::new(CostConfig::default(), Some(collector))
    }

    /// A base scan on `src.main`, aliased to its table name so a group key
    /// qualified by the table name resolves to it.
    fn scan(table: &str, columns: &[&str]) -> LogicalPlan {
        let mut scan = Scan::new(
            "src",
            "main",
            table,
            columns.iter().map(|c| (*c).to_string()).collect(),
        );
        scan.alias = Some(table.to_string());
        LogicalPlan::Scan(Box::new(scan))
    }

    /// A qualified integer column reference.
    fn col(table: &str, name: &str) -> Expr {
        Expr::Column(ColumnRef::new(
            Some(table.to_string()),
            name,
            Some(DataType::Integer),
        ))
    }

    /// An INNER join of two base scans (an arbitrary equi-condition; the gate reads
    /// only the scans' stats, not the condition).
    fn inner_join(left: LogicalPlan, right: LogicalPlan, condition: Expr) -> LogicalPlan {
        LogicalPlan::Join(Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinType::Inner,
            condition: Some(condition),
            natural: false,
            using: None,
            estimated_rows: None,
            estimate_defaults: None,
        })
    }

    /// A plain `GROUP BY group_keys` over `input`.
    fn aggregate(input: LogicalPlan, group_keys: Vec<Expr>) -> LogicalPlan {
        let output_names = group_keys
            .iter()
            .map(|_| "g".to_string())
            .collect::<Vec<_>>();
        LogicalPlan::Aggregate(Aggregate {
            input: Box::new(input),
            group_by: group_keys,
            aggregates: Vec::new(),
            output_names,
            grouping_sets: None,
        })
    }

    /// The q23 shape: `item INNER JOIN <dim>` grouped by the two named keys.
    fn q23_shape(dim: &str, dim_col: &str, group_keys: Vec<Expr>) -> LogicalPlan {
        let join = inner_join(
            scan("item", &["i_item_sk", "i_item_desc"]),
            scan(dim, &[dim_col]),
            eq(col("item", "i_item_sk"), col(dim, dim_col)),
        );
        aggregate(join, group_keys)
    }

    /// An equality predicate.
    fn eq(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            op: fq_plan::expr::BinaryOpType::Eq,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    #[test]
    fn two_independent_high_card_dimensions_explode() {
        // item.i_item_sk (NDV 100000) x date_dim.d_date (NDV 70000): two dimensions.
        let node = q23_shape(
            "date_dim",
            "d_date",
            vec![col("item", "i_item_sk"), col("date_dim", "d_date")],
        );
        let mut cost = cost_model(None);
        assert!(dimension_explosion(&mut cost, None, None, &node).unwrap());
    }

    #[test]
    fn correlated_keys_from_one_dimension_do_not_explode() {
        // Both keys from item -> one owner, counts once.
        let join = inner_join(
            scan("item", &["i_item_sk", "i_item_desc"]),
            scan("date_dim", &["d_date"]),
            eq(col("item", "i_item_sk"), col("date_dim", "d_date")),
        );
        let node = aggregate(
            join,
            vec![col("item", "i_item_sk"), col("item", "i_item_desc")],
        );
        let mut cost = cost_model(None);
        assert!(!dimension_explosion(&mut cost, None, None, &node).unwrap());
    }

    #[test]
    fn one_high_and_one_low_dimension_do_not_explode() {
        // store.s_state has NDV 30 (low) -> only one high-card dimension.
        let node = q23_shape(
            "store",
            "s_state",
            vec![col("item", "i_item_sk"), col("store", "s_state")],
        );
        let mut cost = cost_model(None);
        assert!(!dimension_explosion(&mut cost, None, None, &node).unwrap());
    }

    #[test]
    fn row_count_bounds_ndv_when_column_stats_absent() {
        // warehouse has 10 rows and no histogram -> its key's NDV is bounded by 10
        // (low), so only item contributes a high-card dimension.
        let node = q23_shape(
            "warehouse",
            "w_warehouse_name",
            vec![
                col("item", "i_item_sk"),
                col("warehouse", "w_warehouse_name"),
            ],
        );
        let mut cost = cost_model(None);
        assert!(!dimension_explosion(&mut cost, None, None, &node).unwrap());
    }

    #[test]
    fn unknown_ndv_without_row_count_counts_as_high_card() {
        // `mystery` has no stats at all -> its key is conservative high-card, giving
        // two independent high-card dimensions.
        let node = q23_shape(
            "mystery",
            "m_key",
            vec![col("item", "i_item_sk"), col("mystery", "m_key")],
        );
        let mut cost = cost_model(None);
        assert!(dimension_explosion(&mut cost, None, None, &node).unwrap());
    }

    #[test]
    fn measured_collapse_overrides_width_heuristic() {
        // q23 shape (width says explode), but a measured group count of 1 is far
        // below 10% of the estimated input -> the override says COLLAPSE (no
        // explosion), so the shape SHIPS.
        // StatsCatalog wraps a non-Sync sqlite connection; this test-only Arc never
        // crosses threads (matches fq-optimize's statistics.rs test convention).
        #[allow(clippy::arc_with_non_send_sync)]
        let learned = Arc::new(StatsCatalog::open(":memory:").expect("open catalog"));
        let node = q23_shape(
            "date_dim",
            "d_date",
            vec![col("item", "i_item_sk"), col("date_dim", "d_date")],
        );
        let LogicalPlan::Aggregate(agg) = &node else {
            unreachable!()
        };
        let subject = subplan_signature(&agg.input);
        learned
            .record_group(
                &subject,
                &["i_item_sk".to_string(), "d_date".to_string()],
                1,
                None,
            )
            .expect("record group");
        let mut cost = cost_model(Some(Arc::clone(&learned)));
        assert!(!dimension_explosion(&mut cost, Some(&learned), None, &node).unwrap());
    }

    #[test]
    fn measured_non_collapse_declines() {
        // Same q23 shape, but a measured group count far ABOVE 10% of the input ->
        // the override confirms explosion, so the shape DECLINES.
        // StatsCatalog wraps a non-Sync sqlite connection; this test-only Arc never
        // crosses threads (matches fq-optimize's statistics.rs test convention).
        #[allow(clippy::arc_with_non_send_sync)]
        let learned = Arc::new(StatsCatalog::open(":memory:").expect("open catalog"));
        let node = q23_shape(
            "date_dim",
            "d_date",
            vec![col("item", "i_item_sk"), col("date_dim", "d_date")],
        );
        let LogicalPlan::Aggregate(agg) = &node else {
            unreachable!()
        };
        let subject = subplan_signature(&agg.input);
        learned
            .record_group(
                &subject,
                &["i_item_sk".to_string(), "d_date".to_string()],
                1_000_000_000_000,
                None,
            )
            .expect("record group");
        let mut cost = cost_model(Some(Arc::clone(&learned)));
        assert!(dimension_explosion(&mut cost, Some(&learned), None, &node).unwrap());
    }

    #[test]
    fn island_stamp_matches_gate_key() {
        // The stamp's subject/columns must equal the key a later run reads back.
        let node = q23_shape(
            "date_dim",
            "d_date",
            vec![col("item", "i_item_sk"), col("date_dim", "d_date")],
        );
        let LogicalPlan::Aggregate(agg) = &node else {
            unreachable!()
        };
        let expected = GroupObservation {
            subject: subplan_signature(&agg.input),
            columns: vec!["i_item_sk".to_string(), "d_date".to_string()],
        };
        assert_eq!(island_group_observation(&node), Some(expected));
    }
}
