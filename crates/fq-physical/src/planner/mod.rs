//! The physical planner: converts an optimized `LogicalPlan` into a
//! `PhysicalPlan` tree. Ports `optimizer/physical_planner.py`.
//!
//! Dispatch order is load-bearing: same-source pushdown FIRST, dim shipping
//! SECOND (gated), then the per-node lowerings. The match over `LogicalPlan` is
//! EXHAUSTIVE - a new variant forces a new arm, so there is no "unknown node"
//! path to guess on (the `raise ValueError("Unsupported logical plan node")`
//! fallthrough is retired by the type system).

mod cte;
mod join;
mod lateral;
pub mod orient;
mod set_op;

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use fq_catalog::Catalog;
use fq_optimize::{group_column_names, subplan_signature, CostModel};
use fq_plan::expr::Expr;
use fq_plan::logical::{
    Aggregate, Explain, Filter, GroupedLimit, LogicalPlan, Projection, Scan, SingleRowGuard,
    SubqueryScan, Union, Values,
};
use fq_plan::physical::{
    GroupObservation, PhysicalAliasedRelation, PhysicalCte, PhysicalExplain, PhysicalFilter,
    PhysicalGroupedLimit, PhysicalHashAggregate, PhysicalLimit, PhysicalPlan, PhysicalProjection,
    PhysicalScan, PhysicalSingleRowGuard, PhysicalSort, PhysicalUnion, PhysicalValues,
    PhysicalWindow,
};

use crate::dim_shipping::DimShipping;
use crate::error::PhysicalError;
use crate::single_source::SingleSourcePushdown;

/// Converts an optimized logical plan into a physical plan tree.
pub struct PhysicalPlanner {
    /// Read-only catalog; resolves a datasource by name for capability checks.
    catalog: Arc<Catalog>,
    /// The optimizer's shared cost model (with its session statistics cache).
    /// `estimate` needs `&mut`, so the shared handle is `Rc<RefCell<..>>`. `None`
    /// means "no cost model": estimate/NDV annotation abstain, as in Python.
    cost_model: Option<Rc<RefCell<CostModel>>>,
    /// Same-source pushdown, tried FIRST for every node.
    single_source: SingleSourcePushdown,
    /// Ship attempts are suppressed while planning a shipping fallback (the pure
    /// cross-source seed plan), so a subtree never ships itself recursively.
    shipping_enabled: bool,
    /// Per-query counter giving each shipped temp table a unique name.
    ship_counter: u64,
    /// CTE name -> its materializing producer, registered while a cross-source
    /// CTE's child is planned so each CteRef resolves to a shared scan.
    cte_producers: HashMap<String, PhysicalCte>,
}

impl PhysicalPlanner {
    /// Build a planner over a catalog, optionally sharing the optimizer's cost
    /// model for scan-estimate and join-key NDV annotation of sides join ordering
    /// never visited.
    pub fn new(catalog: Arc<Catalog>, cost_model: Option<Rc<RefCell<CostModel>>>) -> Self {
        let single_source = SingleSourcePushdown::new(Arc::clone(&catalog));
        Self {
            catalog,
            cost_model,
            single_source,
            shipping_enabled: true,
            ship_counter: 0,
            cte_producers: HashMap::new(),
        }
    }

    /// Convert a logical plan to a physical plan. Resets the per-query ship
    /// counter, then dispatches from the root.
    pub fn plan(&mut self, logical_plan: &LogicalPlan) -> Result<PhysicalPlan, PhysicalError> {
        self.ship_counter = 0;
        self.plan_node(logical_plan)
    }

    /// Plan a single node: try same-source pushdown, then dim shipping, then lower
    /// by node type. Exhaustive over `LogicalPlan`.
    pub(crate) fn plan_node(&mut self, node: &LogicalPlan) -> Result<PhysicalPlan, PhysicalError> {
        if let Some(remote) = self.single_source.try_build(node)? {
            return Ok(PhysicalPlan::RemoteQuery(Box::new(remote)));
        }
        if self.shipping_enabled {
            if let Some(shipped) = DimShipping.try_ship(self, node)? {
                return Ok(shipped);
            }
        }
        match node {
            LogicalPlan::Scan(scan) => self.plan_scan(scan),
            LogicalPlan::Filter(f) => self.plan_filter(f),
            LogicalPlan::Projection(p) => self.plan_projection(p),
            LogicalPlan::Limit(l) => self.plan_limit(l),
            LogicalPlan::Sort(s) => self.plan_sort(s),
            LogicalPlan::Join(j) => self.plan_join(j),
            LogicalPlan::Aggregate(a) => self.plan_aggregate(a),
            LogicalPlan::Explain(e) => self.plan_explain(e),
            LogicalPlan::Cte(c) => self.plan_cte(c),
            LogicalPlan::CteRef(r) => self.plan_cte_ref(r),
            LogicalPlan::SetOperation(so) => self.plan_set_operation(so),
            LogicalPlan::Union(u) => self.plan_union(u),
            LogicalPlan::Values(v) => Ok(Self::plan_values(v)),
            LogicalPlan::SubqueryScan(sq) => self.plan_subquery_scan(sq),
            LogicalPlan::LateralJoin(lj) => self.plan_lateral_join(lj),
            LogicalPlan::SingleRowGuard(g) => self.plan_single_row_guard(g),
            LogicalPlan::GroupedLimit(gl) => self.plan_grouped_limit(gl),
        }
    }

    // ---- helpers exposed to the sibling rules (all pub(crate)) ---------------

    /// A unique temp-table name for a shipped dimension within this query.
    // M3 seam (SPEC-dim-shipping.md section 8): dim shipping is the only caller;
    // held now so the stub's replacement matches the signature without churn.
    #[allow(dead_code)]
    pub(crate) fn next_ship_name(&mut self) -> String {
        let name = format!("__fedq_ship_{}", self.ship_counter);
        self.ship_counter += 1;
        name
    }

    /// Plan a subtree with dim shipping suppressed (the pure cross-source seed
    /// plan, also the decline path). Saves/restores the flag around the call.
    // M3 seam (SPEC-dim-shipping.md section 8): dim shipping is the only caller.
    #[allow(dead_code)]
    pub(crate) fn plan_without_shipping(
        &mut self,
        node: &LogicalPlan,
    ) -> Result<PhysicalPlan, PhysicalError> {
        let saved = self.shipping_enabled;
        self.shipping_enabled = false;
        let result = self.plan_node(node);
        self.shipping_enabled = saved;
        result
    }

    /// Read access to the shared cost model, for dim shipping's gates.
    pub(crate) fn cost_model(&self) -> Option<&Rc<RefCell<CostModel>>> {
        self.cost_model.as_ref()
    }

    /// Read access to the catalog, for dim shipping's ship-target check.
    pub(crate) fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// Read access to the same-source pushdown, for dim shipping's island probe.
    pub(crate) fn single_source(&self) -> &SingleSourcePushdown {
        &self.single_source
    }

    /// Collect every base `Scan` in a subtree (CTE references excluded, since a
    /// CteRef is a leaf carrying no Scan). Ports `_collect_base_scans`.
    pub(crate) fn collect_base_scans<'a>(node: &'a LogicalPlan, out: &mut Vec<&'a Scan>) {
        if let LogicalPlan::Scan(scan) = node {
            out.push(scan);
            return;
        }
        for child in node.children() {
            Self::collect_base_scans(child, out);
        }
    }

    /// Plan a base `Scan` as a node (re-entering `plan_node`, matching Python's
    /// `_plan_node(scan)`), so a shipped or pushed base still gets its chance.
    pub(crate) fn plan_scan_node(&mut self, scan: &Scan) -> Result<PhysicalPlan, PhysicalError> {
        self.plan_node(&LogicalPlan::Scan(Box::new(scan.clone())))
    }

    // ---- straight-through lowerings ------------------------------------------

    /// Plan a scan node. Validates the source exists at plan time (kept even
    /// though the connection is not stored on the node), then annotates its row
    /// estimate for reduction orientation.
    fn plan_scan(&mut self, scan: &Scan) -> Result<PhysicalPlan, PhysicalError> {
        if self.catalog.get_datasource(&scan.datasource).is_none() {
            return Err(PhysicalError::DatasourceNotFound(scan.datasource.clone()));
        }
        let estimated_rows = self.scan_estimated_rows(scan)?;
        Ok(PhysicalPlan::Scan(Box::new(PhysicalScan {
            datasource: scan.datasource.clone(),
            schema_name: scan.schema_name.clone(),
            table_name: scan.table_name.clone(),
            columns: scan.columns.clone(),
            filters: scan.filters.clone(),
            sample: scan.sample.clone(),
            group_by: scan.group_by.clone(),
            grouping_sets: scan.grouping_sets.clone(),
            aggregates: scan.aggregates.clone(),
            output_names: scan.output_names.clone(),
            alias: scan.alias.clone(),
            limit: scan.limit,
            offset: scan.offset,
            order_by_keys: scan.order_by_keys.clone(),
            order_by_ascending: scan.order_by_ascending.clone(),
            order_by_nulls: scan.order_by_nulls.clone(),
            distinct: scan.distinct,
            dynamic_filter_keys: None,
            estimated_rows,
            column_ndv: scan.column_ndv.clone(),
            seeded_schema: None,
        })))
    }

    /// A scan's row estimate for reduction orientation: its own annotation (from
    /// join reordering), else the cost estimate, else None (UNKNOWN). A gap-fed
    /// value is an UPPER BOUND and IS stamped; only a truly None size abstains.
    fn scan_estimated_rows(&mut self, scan: &Scan) -> Result<Option<u64>, PhysicalError> {
        if let Some(rows) = scan.estimated_rows {
            return Ok(Some(rows));
        }
        let Some(cost_model) = &self.cost_model else {
            return Ok(None);
        };
        let estimate = cost_model
            .borrow_mut()
            .estimate(&LogicalPlan::Scan(Box::new(scan.clone())))?;
        Ok(estimate.rows)
    }

    /// Plan a filter node.
    fn plan_filter(&mut self, filter: &Filter) -> Result<PhysicalPlan, PhysicalError> {
        let input = self.plan_node(&filter.input)?;
        Ok(PhysicalPlan::Filter(PhysicalFilter {
            input: Box::new(input),
            predicate: filter.predicate.clone(),
        }))
    }

    /// Plan an explain node.
    fn plan_explain(&mut self, explain: &Explain) -> Result<PhysicalPlan, PhysicalError> {
        let child = self.plan_node(&explain.input)?;
        Ok(PhysicalPlan::Explain(PhysicalExplain {
            child: Box::new(child),
            format: explain.format,
        }))
    }

    /// Plan a Values node: emit the literal row tuples directly as an in-memory
    /// relation with the given output names. Pure - no child to plan.
    fn plan_values(values: &Values) -> PhysicalPlan {
        PhysicalPlan::Values(PhysicalValues {
            rows: values.rows.clone(),
            output_names: values.output_names.clone(),
        })
    }

    /// Plan a SubqueryScan: re-expose the input under a new correlation name so
    /// parent references resolve against the aliased scope.
    fn plan_subquery_scan(
        &mut self,
        subquery: &SubqueryScan,
    ) -> Result<PhysicalPlan, PhysicalError> {
        let input = self.plan_node(&subquery.input)?;
        Ok(PhysicalPlan::AliasedRelation(PhysicalAliasedRelation {
            input: Box::new(input),
            alias: subquery.alias.clone(),
        }))
    }

    /// Plan a decorrelation-produced Union over its inputs (distinct-aware).
    fn plan_union(&mut self, union: &Union) -> Result<PhysicalPlan, PhysicalError> {
        let mut inputs = Vec::with_capacity(union.inputs.len());
        for child in &union.inputs {
            inputs.push(self.plan_node(child)?);
        }
        Ok(PhysicalPlan::Union(PhysicalUnion {
            inputs,
            distinct: union.distinct,
        }))
    }

    /// Plan a SingleRowGuard: enforce at most one input row per key group.
    fn plan_single_row_guard(
        &mut self,
        guard: &SingleRowGuard,
    ) -> Result<PhysicalPlan, PhysicalError> {
        let input = self.plan_node(&guard.input)?;
        Ok(PhysicalPlan::SingleRowGuard(PhysicalSingleRowGuard {
            input: Box::new(input),
            keys: guard.keys.clone(),
        }))
    }

    /// Plan a GroupedLimit: keep the first rows within each key group.
    fn plan_grouped_limit(
        &mut self,
        grouped: &GroupedLimit,
    ) -> Result<PhysicalPlan, PhysicalError> {
        let input = self.plan_node(&grouped.input)?;
        Ok(PhysicalPlan::GroupedLimit(PhysicalGroupedLimit {
            input: Box::new(input),
            keys: grouped.keys.clone(),
            limit: grouped.limit,
            order_by_keys: grouped.order_by_keys.clone(),
            order_by_ascending: grouped.order_by_ascending.clone(),
            order_by_nulls: grouped.order_by_nulls.clone(),
        }))
    }

    // ---- projection + window detection ---------------------------------------

    /// Plan a projection; a window-bearing one becomes a `Window` node that runs
    /// in the merge engine, else a `Projection` (distinct propagated to the
    /// lowest scan-like node).
    fn plan_projection(&mut self, projection: &Projection) -> Result<PhysicalPlan, PhysicalError> {
        let mut input = self.plan_node(&projection.input)?;
        if projection_has_window(projection) {
            // DISTINCT over a window is rejected at parse, so no distinct handling.
            return Ok(PhysicalPlan::Window(PhysicalWindow {
                input: Box::new(input),
                expressions: projection.expressions.clone(),
                output_names: projection.aliases.clone(),
            }));
        }
        if projection.distinct {
            propagate_distinct(&mut input);
        }
        Ok(PhysicalPlan::Projection(PhysicalProjection {
            input: Box::new(input),
            expressions: projection.expressions.clone(),
            output_names: projection.aliases.clone(),
            distinct: projection.distinct,
            distinct_on: projection.distinct_on.clone(),
        }))
    }

    // ---- limit + sort (fold into pushdown targets, else wrap) ----------------

    /// Plan a limit node, folding it into a pushed-down set operation.
    fn plan_limit(
        &mut self,
        limit: &fq_plan::logical::Limit,
    ) -> Result<PhysicalPlan, PhysicalError> {
        let input = self.plan_node(&limit.input)?;
        if let PhysicalPlan::RemoteSetOp(mut remote) = input {
            remote.limit = limit.limit;
            remote.offset = limit.offset;
            return Ok(PhysicalPlan::RemoteSetOp(remote));
        }
        Ok(PhysicalPlan::Limit(PhysicalLimit {
            input: Box::new(input),
            limit: limit.limit,
            offset: limit.offset,
        }))
    }

    /// Plan a sort node, folding the ORDER BY onto a pushdown target (scan /
    /// remote join / remote set op), else wrapping it in a `Sort`.
    fn plan_sort(&mut self, sort: &fq_plan::logical::Sort) -> Result<PhysicalPlan, PhysicalError> {
        let input = self.plan_node(&sort.input)?;
        match input {
            PhysicalPlan::Scan(mut scan) => {
                scan.order_by_keys = Some(sort.sort_keys.clone());
                scan.order_by_ascending = Some(sort.ascending.clone());
                scan.order_by_nulls.clone_from(&sort.nulls_order);
                Ok(PhysicalPlan::Scan(scan))
            }
            PhysicalPlan::RemoteJoin(mut join) => {
                join.order_by_keys = Some(sort.sort_keys.clone());
                join.order_by_ascending = Some(sort.ascending.clone());
                join.order_by_nulls.clone_from(&sort.nulls_order);
                Ok(PhysicalPlan::RemoteJoin(join))
            }
            PhysicalPlan::RemoteSetOp(mut set_op) => {
                set_op.order_by_keys = Some(sort.sort_keys.clone());
                set_op.order_by_ascending = Some(sort.ascending.clone());
                set_op.order_by_nulls.clone_from(&sort.nulls_order);
                Ok(PhysicalPlan::RemoteSetOp(set_op))
            }
            other => Ok(PhysicalPlan::Sort(PhysicalSort {
                input: Box::new(other),
                sort_keys: sort.sort_keys.clone(),
                ascending: sort.ascending.clone(),
                nulls_order: sort.nulls_order.clone(),
            })),
        }
    }

    // ---- aggregate -----------------------------------------------------------

    /// Plan an aggregate node. A remote-join input folds the aggregate onto the
    /// join (both push as one query); otherwise a coordinator hash aggregate,
    /// stamped with its learned-stats group provenance.
    fn plan_aggregate(&mut self, aggregate: &Aggregate) -> Result<PhysicalPlan, PhysicalError> {
        let input = self.plan_node(&aggregate.input)?;
        if let PhysicalPlan::RemoteJoin(mut remote) = input {
            remote.group_by = Some(aggregate.group_by.clone());
            remote.grouping_sets.clone_from(&aggregate.grouping_sets);
            remote.aggregates = Some(aggregate.aggregates.clone());
            remote.output_names = Some(aggregate.output_names.clone());
            return Ok(PhysicalPlan::RemoteJoin(remote));
        }
        Ok(PhysicalPlan::HashAggregate(PhysicalHashAggregate {
            input: Box::new(input),
            group_by: aggregate.group_by.clone(),
            aggregates: aggregate.aggregates.clone(),
            output_names: aggregate.output_names.clone(),
            grouping_sets: aggregate.grouping_sets.clone(),
            group_observation: group_observation(aggregate),
        }))
    }
}

/// The learned-stats group provenance for a coordinator aggregate: the input's
/// subplan SIGNATURE plus the group column names. None for a rollup or a
/// non-plain-column group key (nothing stable to key on). Ports
/// `_group_observation`.
fn group_observation(aggregate: &Aggregate) -> Option<GroupObservation> {
    let has_rollup = aggregate
        .grouping_sets
        .as_ref()
        .is_some_and(|sets| !sets.is_empty());
    if has_rollup || aggregate.group_by.is_empty() {
        return None;
    }
    let columns = group_column_names(&aggregate.group_by)?;
    Some(GroupObservation {
        subject: subplan_signature(&aggregate.input),
        columns,
    })
}

/// Whether any projection expression contains a window function (nested included).
fn projection_has_window(projection: &Projection) -> bool {
    projection.expressions.iter().any(expression_has_window)
}

/// Whether an expression tree contains a Window node at any depth. Uses
/// `Expr::children` for descent (which does NOT descend into subqueries, matching
/// Python `expression_children`).
fn expression_has_window(expr: &Expr) -> bool {
    matches!(expr, Expr::Window { .. }) || expr.children().iter().any(|c| expression_has_window(c))
}

/// Mark the lowest scan-like node to emit DISTINCT (mutating descent through
/// single-child wrappers). Ports `_propagate_distinct`.
fn propagate_distinct(node: &mut PhysicalPlan) {
    match node {
        PhysicalPlan::Scan(scan) => scan.distinct = true,
        PhysicalPlan::RemoteJoin(remote) => remote.distinct = true,
        // Single-child wrappers: descend into `.input`.
        PhysicalPlan::Projection(n) => propagate_distinct(&mut n.input),
        PhysicalPlan::Window(n) => propagate_distinct(&mut n.input),
        PhysicalPlan::Filter(n) => propagate_distinct(&mut n.input),
        PhysicalPlan::HashAggregate(n) => propagate_distinct(&mut n.input),
        PhysicalPlan::Sort(n) => propagate_distinct(&mut n.input),
        PhysicalPlan::Limit(n) => propagate_distinct(&mut n.input),
        PhysicalPlan::AliasedRelation(n) => propagate_distinct(&mut n.input),
        PhysicalPlan::SingleRowGuard(n) => propagate_distinct(&mut n.input),
        PhysicalPlan::GroupedLimit(n) => propagate_distinct(&mut n.input),
        PhysicalPlan::Explain(n) => propagate_distinct(&mut n.child),
        // Everything else has no single `input` child -> stop the descent. This is
        // the ONE tolerated silent arm: it mirrors Python's "no `input` attribute
        // -> stop" (a scan/remote already marks; a binary/leaf has nowhere deeper
        // to mark). It is a DESCENT-STOP, not a node-handling gap.
        _ => {}
    }
}
