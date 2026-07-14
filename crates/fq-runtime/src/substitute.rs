//! Automatic materialized-view substitution: reading a view's chunks in place
//! of recomputing a query subtree that matches the view's definition.
//!
//! WHERE IN THE PIPELINE. Substitution runs on the OPTIMIZED LOGICAL plan,
//! between `optimize` and physical planning (`Runtime::plan`). Two reasons it
//! sits here rather than as a physical-tree rewrite:
//!
//! - A materialized view is ALREADY a queryable relation (`MaterializedViewSource`
//!   over the store), so the replacement is a plain `Scan` of that relation.
//!   Physical planning then lowers it to the exact `MaterializedScan` an explicit
//!   `FROM <view>` produces - zero new execution machinery, one code path with
//!   the read side the store already ships.
//! - The optimizer is where alias and formatting noise is gone (predicate
//!   pushdown, projection pruning, join reordering, constant folding all applied)
//!   but the plan is still a comparable structural value. Matching a view's
//!   definition normalized through the SAME optimizer against subtrees of the
//!   incoming optimized plan is therefore a structural `==` (fq-plan nodes derive
//!   `PartialEq`), constants and all.
//!
//! MATCHING. For each live view, the definition SQL is run through the SAME
//! parse -> bind -> decorrelate -> optimize pipeline the incoming query used
//! (`optimize_definition`), against the SAME catalog and config snapshot, giving
//! a canonical plan. The rewrite walks the incoming optimized plan top-down and,
//! at each node that is structurally EQUAL to a view's canonical plan, replaces
//! it with a scan of that view - the LARGEST matching subtree wins. Match is
//! EXACT only: a different constant, a different column set, or an extra filter
//! yields a different optimized shape and simply does not match (a conservative
//! decline, never a wrong answer). Alias differences that the optimizer does not
//! fold likewise decline rather than mismatch. Whole-query and derived-table
//! (SubqueryScan input) positions are the shapes that match in practice, because
//! a full-SELECT shape appears nested in SQL only as a derived table.
//!
//! IDENTITY. The store holds exactly the rows the definition produced at the
//! last pull, and serving trusts the last pull (no query-path freshness check),
//! so a substituted scan returns the same ROW SET the recompute would over
//! unchanged sources - byte-identical when the query orders its output.
//!
//! GATES. Substitution fires only when all hold:
//! - the kill switch is off (`accelerator.enable_substitution`, read fresh per
//!   plan, so `SET ... = false` disables it live);
//! - the matched subtree imposes no ordering or row selection ANYWHERE within
//!   it (no Sort, LIMIT, or scan with a folded ORDER BY/LIMIT at any depth): a
//!   chunk read is an unordered set, so a subtree that fixes order would lose it
//!   when served. An ordering the query applies ABOVE the match is unaffected;

//! - the output column NAMES of the matched subtree equal the view's stored
//!   columns in order (a belt-and-suspenders correctness guard: the replacement
//!   must report the exact names the parent resolves against);
//! - the cost gate clears: the estimated recompute cost strictly exceeds the
//!   cached-read cost (below), declining on any unknown.
//!
//! COST GATE. Recompute cost is the total BASE-TABLE input rows the subtree
//! scans - the cost model's estimate of each `Scan` leaf's UNFILTERED table,
//! summed (an opaque leaf such as a `CteRef` makes the sum unknown). Read cost is
//! the view's `measured_rows` (the rows a chunk scan reads back). Comparing input
//! work against output rows is what distinguishes a worthwhile view (a large
//! scan/join/aggregate collapsed to few rows: input >> read, substitute) from a
//! pointless one (a `SELECT * FROM t` whose input equals its output: not strictly
//! greater, decline). Output cardinality alone cannot gate this - the recompute's
//! output always equals `measured_rows`. Any unknown declines (the safe
//! direction: never substitute a fragment we cannot prove is cheaper to read).
//! The saving (`recompute - read`) accrues to the view's `cost_saved` counter.

use std::cell::RefCell;
use std::rc::Rc;

use fq_accel::{MaterializedView, VIEW_SCHEMA_NAME};
use fq_catalog::Catalog;
use fq_common::Config;
use fq_optimize::{build_optimizer, CostModel, OptimizeError};
use fq_parse::parse_with_catalog;
use fq_plan::logical::{LogicalPlan, Scan};

use crate::error::RuntimeError;
use crate::Runtime;

/// One substitution the planner applied, recorded AFTER the query executes so
/// the view's benefit counters advance only for substitutions actually served
/// (an EXPLAIN or a describe plans without executing and records nothing).
pub(crate) struct AppliedSubstitution {
    /// The substituted view.
    pub view_name: String,
    /// The cost model's estimated saving of this substitution (recompute minus
    /// cached read), added to the view's `cost_saved`.
    pub cost_saved: f64,
}

/// A view eligible for matching: its record plus its canonical optimized plan.
struct Candidate {
    view: MaterializedView,
    canonical: LogicalPlan,
}

impl Runtime {
    /// Rewrite `plan` to read a matching view's chunks in place of recomputing a
    /// subtree, returning the rewritten plan and the substitutions applied. A
    /// no-op (returns `plan` unchanged) when the kill switch is off, there is no
    /// store, or no view is registered.
    pub(crate) fn substitute_views(
        &self,
        plan: LogicalPlan,
        catalog: &Catalog,
        config: &Config,
        cost_model: &Rc<RefCell<CostModel>>,
    ) -> Result<(LogicalPlan, Vec<AppliedSubstitution>), RuntimeError> {
        if !config.accelerator.enable_substitution {
            return Ok((plan, Vec::new()));
        }
        let Some(accel) = self.accelerator.as_ref() else {
            return Ok((plan, Vec::new()));
        };
        let views = accel.views()?;
        if views.is_empty() {
            return Ok((plan, Vec::new()));
        }
        let candidates = self.build_candidates(views, catalog, config);
        if candidates.is_empty() {
            return Ok((plan, Vec::new()));
        }
        let accel_name = accel.datasource_name().to_string();
        let mut applied = Vec::new();
        let rewritten = rewrite(plan, &candidates, &accel_name, cost_model, &mut applied)?;
        Ok((rewritten, applied))
    }

    /// Build the match candidates: each live view paired with its definition
    /// normalized through the same pipeline the incoming query used. A view
    /// whose definition no longer plans (a base table was dropped since it was
    /// created) is not a candidate - the incoming query does not depend on it,
    /// so declining to match it is correct, not a swallowed query error.
    fn build_candidates(
        &self,
        views: Vec<MaterializedView>,
        catalog: &Catalog,
        config: &Config,
    ) -> Vec<Candidate> {
        let mut candidates = Vec::with_capacity(views.len());
        for view in views {
            if let Ok(canonical) = self.optimize_definition(catalog, &view.definition_sql, config) {
                candidates.push(Candidate { view, canonical });
            }
        }
        candidates
    }

    /// Normalize a view's defining SELECT into its canonical optimized logical
    /// plan through the SAME stages the incoming query runs (parse, bind,
    /// decorrelate, optimize), so a match is a structural equality of two plans
    /// produced identically. No stage log: the caller's `substitute` stage times
    /// the whole substitution under the shared planning budget.
    fn optimize_definition(
        &self,
        catalog: &Catalog,
        sql: &str,
        config: &Config,
    ) -> Result<LogicalPlan, RuntimeError> {
        let parsed = parse_with_catalog(sql, catalog)?;
        let bound = fq_bind::bind(catalog, parsed)?;
        let decorrelated = fq_decorrelate::decorrelate(bound)?;
        let optimizer = build_optimizer(&config.optimizer, self.cost_model(config));
        Ok(optimizer.optimize(decorrelated)?)
    }
}

/// Walk `node` top-down; the first node structurally equal to a view's canonical
/// plan that also clears the name guard and the cost gate is replaced with a
/// scan of that view (the largest matching subtree wins, so recursion stops at
/// the replacement). A node that does not substitute recurses into its children.
fn rewrite(
    node: LogicalPlan,
    candidates: &[Candidate],
    accel_name: &str,
    cost_model: &Rc<RefCell<CostModel>>,
    applied: &mut Vec<AppliedSubstitution>,
) -> Result<LogicalPlan, RuntimeError> {
    // A substituted scan reads the view's chunks as a SET (chunk read order is
    // not the definition's row order across chunks), so a subtree that fixes
    // output order or row selection ANYWHERE within it - a Sort, a LIMIT, or a
    // scan carrying a folded ORDER BY/LIMIT, at the root or nested under a
    // Projection/Limit - is never substituted here; its ordering would be lost.
    // An ordering the query applies OUTSIDE the matched subtree still holds (it
    // re-sorts the substituted scan). This keeps substitution set-equivalent, so
    // the identity contract holds for every query, not just single-row ones.
    if !order_sensitive(&node) {
        if let Some(result) = try_match(&node, candidates, accel_name, cost_model, applied)? {
            return Ok(result);
        }
    }
    node.try_map_children(|child| rewrite(child, candidates, accel_name, cost_model, applied))
}

/// Try every candidate against `node`; return the replacement scan of the first
/// that matches structurally, clears the name guard, and clears the cost gate.
fn try_match(
    node: &LogicalPlan,
    candidates: &[Candidate],
    accel_name: &str,
    cost_model: &Rc<RefCell<CostModel>>,
    applied: &mut Vec<AppliedSubstitution>,
) -> Result<Option<LogicalPlan>, RuntimeError> {
    for candidate in candidates {
        if *node != candidate.canonical {
            continue;
        }
        // The replacement scan reports the view's stored column names; the
        // parent resolves the subtree's outputs by those names, so they must
        // match the subtree's own output names in order. A structural match
        // implies this, but the query only ever ships answers when it holds.
        if node.schema() != view_column_names(&candidate.view) {
            continue;
        }
        if let Some(saved) = cost_gate(node, &candidate.view, cost_model)? {
            applied.push(AppliedSubstitution {
                view_name: candidate.view.name.clone(),
                cost_saved: saved,
            });
            return Ok(Some(view_scan(&candidate.view, accel_name)));
        }
    }
    Ok(None)
}

/// Whether a subtree fixes output order or row selection ANYWHERE within it -
/// order/limit semantics a chunk read (an unordered set scan) would not
/// reproduce: a Sort or LIMIT node, or a scan with a folded ORDER BY / LIMIT,
/// at the subtree root OR nested below it (optimization roots an
/// `ORDER BY ... LIMIT n` query in a Projection over Limit over Scan, so the
/// root alone is not enough). Such a subtree is not substituted - its ordering
/// lives in the definition and is not recoverable from the chunks; an ordering
/// the query applies ABOVE the matched subtree is unaffected. The per-node test
/// is exhaustive: a new order-imposing node forces a decision here.
fn order_sensitive(node: &LogicalPlan) -> bool {
    if node_fixes_order(node) {
        return true;
    }
    for child in node.children() {
        if order_sensitive(child) {
            return true;
        }
    }
    false
}

/// Whether a single node imposes output order or row selection: a Sort, a LIMIT
/// (plain or grouped), or a scan carrying a folded ORDER BY / LIMIT. Exhaustive:
/// a new order-imposing node forces a decision here.
fn node_fixes_order(node: &LogicalPlan) -> bool {
    match node {
        LogicalPlan::Sort(_) | LogicalPlan::Limit(_) | LogicalPlan::GroupedLimit(_) => true,
        LogicalPlan::Scan(scan) => scan.order_by_keys.is_some() || scan.limit.is_some(),
        _ => false,
    }
}

/// The view's stored output column names, in order.
fn view_column_names(view: &MaterializedView) -> Vec<String> {
    let mut names = Vec::with_capacity(view.columns.len());
    for column in &view.columns {
        names.push(column.name.clone());
    }
    names
}

/// A scan of `view`'s chunks reading every stored column in order - exactly the
/// relation an explicit `FROM <view>` binds to, so physical planning lowers it
/// to the store's `MaterializedScan` with no substitution-specific machinery.
fn view_scan(view: &MaterializedView, accel_name: &str) -> LogicalPlan {
    LogicalPlan::Scan(Box::new(Scan::new(
        accel_name.to_string(),
        VIEW_SCHEMA_NAME.to_string(),
        view.name.clone(),
        view_column_names(view),
    )))
}

/// The cost gate: `Some(saving)` when the estimated recompute cost strictly
/// exceeds the cached-read cost, else `None` (decline). Read cost is the view's
/// measured row count; recompute cost is the total base-table input rows the
/// subtree scans. Any unknown recompute estimate declines.
fn cost_gate(
    node: &LogicalPlan,
    view: &MaterializedView,
    cost_model: &Rc<RefCell<CostModel>>,
) -> Result<Option<f64>, RuntimeError> {
    let read_cost = view.measured_rows as f64;
    let Some(recompute_rows) = recompute_input_rows(node, cost_model)? else {
        return Ok(None);
    };
    let recompute_cost = recompute_rows as f64;
    if recompute_cost > read_cost {
        Ok(Some(recompute_cost - read_cost))
    } else {
        Ok(None)
    }
}

/// The total base-table input rows a subtree scans - the recompute-work proxy. A
/// `Scan` leaf contributes its UNFILTERED table's estimated rows (the work a
/// recompute reads before folding filters/aggregates); a `Values` leaf its
/// constant row count; an opaque `CteRef` leaf makes the total unknown. Internal
/// nodes sum their children. Any unknown child makes the whole total unknown.
fn recompute_input_rows(
    node: &LogicalPlan,
    cost_model: &Rc<RefCell<CostModel>>,
) -> Result<Option<u64>, RuntimeError> {
    match node {
        LogicalPlan::Scan(scan) => base_scan_rows(scan, cost_model),
        LogicalPlan::Values(values) => Ok(Some(values.rows.len() as u64)),
        LogicalPlan::CteRef(_) => Ok(None),
        other => {
            let mut total = Some(0u64);
            for child in other.children() {
                let child_rows = recompute_input_rows(child, cost_model)?;
                total = add_rows(total, child_rows);
            }
            Ok(total)
        }
    }
}

/// The estimated UNFILTERED row count of a scan's base table. Estimating a
/// filterless copy of the scan gives the source-side work a recompute pays
/// (reading the table before any pushed filter/aggregate reduces it); `None`
/// when the source has no row-count statistic.
fn base_scan_rows(
    scan: &Scan,
    cost_model: &Rc<RefCell<CostModel>>,
) -> Result<Option<u64>, RuntimeError> {
    let base = Scan::new(
        scan.datasource.clone(),
        scan.schema_name.clone(),
        scan.table_name.clone(),
        scan.columns.clone(),
    );
    let estimate = cost_model
        .borrow_mut()
        .estimate(&LogicalPlan::Scan(Box::new(base)))
        .map_err(|error| RuntimeError::Optimize(OptimizeError::from(error)))?;
    Ok(estimate.rows)
}

/// Sum two optional row counts, saturating; unknown (`None`) on either side
/// makes the sum unknown - the conservative direction for the cost gate.
fn add_rows(left: Option<u64>, right: Option<u64>) -> Option<u64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.saturating_add(right)),
        _ => None,
    }
}
