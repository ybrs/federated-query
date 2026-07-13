//! Same-source pushdown: absorbs a whole single-source subtree into ONE remote
//! SQL query. Ports `optimizer/single_source_pushdown.py`.
//!
//! The Python module accumulates sqlglot AST fragments and assembles once; here
//! the accumulator is TWO-LEVEL: `fq_plan::Expr` for the pieces
//! that undergo a later structural rewrite (the SELECT list, DISTINCT ON keys),
//! and eagerly-rendered `String` for the pieces that do not (FROM/JOIN/WHERE/
//! HAVING/GROUP BY/ORDER BY, each CTE body, each set-op branch). fq-emit renders
//! `Expr` + clause data to canonical Postgres TEXT; the FROM/JOIN/EXISTS/WITH text
//! builders live in `relation_sql.rs` + `render`.
//!
//! Two things sqlglot did for free and this module builds explicitly: a SEMI/ANTI
//! join becomes `WHERE [NOT] EXISTS(...)`, and the recursive-CTE base
//! case is a FROM-less `SELECT <items>` branch.
//!
//! The pure clause-shaping helpers are free functions (they touch no pass state);
//! only the recursive absorb/render dispatch and the three catalog-reading entry
//! points (`scan_output_columns`, `finish`, `resolve_datasource`) are methods on
//! the pass.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use fq_catalog::datasource::DataSourceCapability;
use fq_catalog::Catalog;
use fq_emit::{
    clauses, quote_ident, render_canonical, render_expr, EmitError, SelectPieces,
    CANONICAL_SOURCE_RESOLVER,
};
use fq_plan::expr::{aggregate_output_map, split_where_having, ColumnRef, Expr};
use fq_plan::logical::{
    Aggregate, Cte, CteRef, Filter, Join, JoinType, LateralJoin, Limit, LogicalPlan, Projection,
    Scan, SetOperation, Sort, SubqueryScan, Values,
};
use fq_plan::physical::{ColumnAliasMap, PhysicalRemoteQuery};

use crate::relation_sql::{cte_ref_sql, derived_table, table_ref};

/// Identity map from a base scan (keyed by its pointer, matching Python
/// `id(scan)`) to the merge-engine relation name it renders as, instead of
/// `"schema"."table"`.
///
/// The `*const Scan` key is sound ONLY under the single-tree-borrow invariant: the
/// whole pass borrows one immutable plan tree, and the caller builds the map from
/// the SAME `&Scan` nodes it hands to `render_correlated_sql` (the cross-source
/// LATERAL / recursive-CTE merge path, where exactly one tree is registered). A
/// future caller that registers scans from a different tree instance must switch
/// to a structural key.
pub type ScanNames = HashMap<*const Scan, String>;

/// One WITH definition: `name [(cols)] AS (body_sql)`. `body_sql` is the fully
/// rendered CTE body (canonical Postgres), with anchor-column aliasing already
/// applied during body render.
struct CteDef {
    name: String,
    columns: Option<Vec<String>>,
    body_sql: String,
}

/// Mutable accumulator for the clauses of one remote query. Borrows the input
/// plan (`'a`) so `scans` hold references rather than clones.
struct PushContext<'a> {
    // ---- target source ----
    datasource: Option<String>,

    // ---- SELECT list: Expr-level (rewritten before render) ----
    select_exprs: Vec<Expr>,
    output_names: Vec<String>,

    // ---- FROM / JOIN / predicates: rendered-String level (no later rewrite) ----
    from_clause: Option<String>,
    joins: Vec<String>,
    where_terms: Vec<String>,
    having_terms: Vec<String>,
    group_clause: Option<String>,
    order_clause: Option<String>,

    // ---- scalar clause data ----
    limit: Option<i64>,
    offset: i64,
    distinct: bool,
    distinct_on: Option<Vec<Expr>>,

    // ---- pushability / shape flags ----
    has_join: bool,
    has_computed: bool,
    has_aggregate: bool,
    has_subquery_relation: bool,
    has_derived_columns: bool,
    has_cte: bool,
    has_recursive_cte: bool,
    in_derived: bool,
    derived_count: u32,

    // ---- qualified-reference resolution + interior scans ----
    column_aliases: ColumnAliasMap,
    scans: Vec<&'a Scan>,
    scan_names: Option<&'a ScanNames>,

    // ---- CTE definitions ----
    ctes: Vec<CteDef>,
    visible_ctes: Vec<String>,
}

impl<'a> PushContext<'a> {
    /// A zero-initialized context (every clause empty/absent).
    fn new() -> Self {
        PushContext {
            datasource: None,
            select_exprs: Vec::new(),
            output_names: Vec::new(),
            from_clause: None,
            joins: Vec::new(),
            where_terms: Vec::new(),
            having_terms: Vec::new(),
            group_clause: None,
            order_clause: None,
            limit: None,
            offset: 0,
            distinct: false,
            distinct_on: None,
            has_join: false,
            has_computed: false,
            has_aggregate: false,
            has_subquery_relation: false,
            has_derived_columns: false,
            has_cte: false,
            has_recursive_cte: false,
            in_derived: false,
            derived_count: 0,
            column_aliases: ColumnAliasMap::new(),
            scans: Vec::new(),
            scan_names: None,
            ctes: Vec::new(),
            visible_ctes: Vec::new(),
        }
    }

    /// A fresh child context for a derived / branch / subquery sub-render: carries
    /// the scan-name map, marks `in_derived`, and presets the visible CTE names so
    /// a sub-render can reference an enclosing (or sibling) CTE.
    fn child(&self) -> PushContext<'a> {
        let mut ctx = PushContext::new();
        ctx.scan_names = self.scan_names;
        ctx.in_derived = true;
        ctx.visible_ctes = self.visible_names();
        ctx
    }

    /// CTE names a sub-render may reference: the enclosing set plus locally defined.
    fn visible_names(&self) -> Vec<String> {
        let mut names = self.visible_ctes.clone();
        for cte in &self.ctes {
            names.push(cte.name.clone());
        }
        names
    }
}

/// Which right-side filter handling a nullable-preserving join needs.
enum NullableRight {
    /// Render the right relation normally (its filter, if any, is collected).
    Normal,
    /// The right scan's filter must ride the JOIN CONDITION (the q13 differential):
    /// suppress its base-scan collection and carry the rendered filter as an ON
    /// conjunct.
    RideFilter(String),
    /// FULL / NATURAL / USING with a filtered right side has no ON to carry the
    /// conjunct; decline the whole subtree.
    Decline,
}

/// Same-source pushdown pass. Holds the shared catalog to resolve tables and
/// datasource capabilities.
pub struct SingleSourcePushdown {
    catalog: Arc<Catalog>,
}

impl SingleSourcePushdown {
    /// Build a pushdown pass over a shared catalog.
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self { catalog }
    }

    /// Try to collapse a subtree living on one source into a single remote query.
    /// `Ok(None)` declines (the planner falls back to local lowering); an
    /// `EmitError` propagates loudly (a subquery reaching emit is a decorrelation
    /// bug, never swallowed).
    pub fn try_build(&self, node: &LogicalPlan) -> Result<Option<PhysicalRemoteQuery>, EmitError> {
        // A top-level derived alias boundary: absorbing it here would drop the
        // alias, so the planner keeps it via PhysicalAliasedRelation.
        if matches!(node, LogicalPlan::SubqueryScan(_)) {
            return Ok(None);
        }
        let mut ctx = PushContext::new();
        if !self.absorb(node, &mut ctx)? {
            return Ok(None);
        }
        if !should_push(&ctx) {
            return Ok(None);
        }
        if ctx.select_exprs.is_empty() && !self.expand_star_select(&mut ctx) {
            return Ok(None);
        }
        let estimated_rows = root_estimate(node);
        let output_estimated_rows = output_estimate(node);
        self.finish(&ctx, estimated_rows, output_estimated_rows)
    }

    /// Render a single-source subtree to canonical Postgres SQL with base scans
    /// named for the merge engine (the cross-source LATERAL / recursive-CTE path).
    /// Outer/correlation column refs are kept verbatim. `None` when the subtree is
    /// not renderable as one query.
    pub fn render_correlated_sql<'a>(
        &self,
        node: &'a LogicalPlan,
        scan_names: &'a ScanNames,
    ) -> Result<Option<String>, EmitError> {
        let mut ctx = PushContext::new();
        ctx.scan_names = Some(scan_names);
        if !self.absorb(node, &mut ctx)? {
            return Ok(None);
        }
        Ok(Some(render(&ctx)?))
    }

    // ---- absorb dispatch -----------------------------------------------------

    /// Fold one plan node's clause into the context and recurse into its input.
    /// `Ok(false)` is the sanctioned structural decline (the whole subtree is not
    /// pushable), NOT a silent wrong answer.
    fn absorb<'a>(
        &self,
        node: &'a LogicalPlan,
        ctx: &mut PushContext<'a>,
    ) -> Result<bool, EmitError> {
        match node {
            LogicalPlan::Limit(n) => self.absorb_limit(n, ctx),
            LogicalPlan::Sort(n) => self.absorb_sort(n, ctx),
            LogicalPlan::Projection(n) => self.absorb_projection(n, ctx),
            LogicalPlan::Aggregate(n) => self.absorb_aggregate(n, ctx),
            LogicalPlan::Filter(n) => self.absorb_filter(n, ctx),
            LogicalPlan::LateralJoin(n) => self.absorb_lateral_join(n, ctx),
            LogicalPlan::Cte(n) => self.absorb_cte(n, ctx),
            LogicalPlan::Join(_)
            | LogicalPlan::Scan(_)
            | LogicalPlan::SubqueryScan(_)
            | LogicalPlan::SetOperation(_)
            | LogicalPlan::CteRef(_) => self.absorb_from(node, ctx),
            LogicalPlan::Union(_)
            | LogicalPlan::Values(_)
            | LogicalPlan::Explain(_)
            | LogicalPlan::SingleRowGuard(_)
            | LogicalPlan::GroupedLimit(_) => Ok(false),
        }
    }

    /// Build the FROM clause from a join tree, base scan, or derived table.
    fn absorb_from<'a>(
        &self,
        node: &'a LogicalPlan,
        ctx: &mut PushContext<'a>,
    ) -> Result<bool, EmitError> {
        match node {
            LogicalPlan::Scan(scan) => absorb_base_scan(scan, ctx),
            LogicalPlan::SubqueryScan(n) => self.absorb_subquery_scan_base(n, ctx),
            LogicalPlan::SetOperation(n) => self.absorb_set_operation_base(n, ctx),
            LogicalPlan::CteRef(n) => Ok(absorb_cte_ref_base(n, ctx)),
            LogicalPlan::Join(n) => self.absorb_join(n, ctx),
            _ => Ok(false),
        }
    }

    // ---- clause absorbers ----------------------------------------------------

    /// Record LIMIT / OFFSET, then descend (a direct overwrite, mirroring Python).
    fn absorb_limit<'a>(
        &self,
        limit: &'a Limit,
        ctx: &mut PushContext<'a>,
    ) -> Result<bool, EmitError> {
        ctx.limit = limit.limit.map(clamp_i64);
        ctx.offset = clamp_i64(limit.offset);
        self.absorb(&limit.input, ctx)
    }

    /// Record the ORDER BY clause, then descend.
    fn absorb_sort<'a>(
        &self,
        sort: &'a Sort,
        ctx: &mut PushContext<'a>,
    ) -> Result<bool, EmitError> {
        let nulls = sort.nulls_order.as_deref().unwrap_or(&[]);
        ctx.order_clause = clauses::order_by(
            &sort.sort_keys,
            &sort.ascending,
            nulls,
            &CANONICAL_SOURCE_RESOLVER,
        )?;
        self.absorb(&sort.input, ctx)
    }

    /// Record the SELECT list (with aliases), then descend. An unexpanded `*`
    /// declines (the preprocessor expands stars; a surviving one cannot render
    /// `* AS "*"`).
    fn absorb_projection<'a>(
        &self,
        projection: &'a Projection,
        ctx: &mut PushContext<'a>,
    ) -> Result<bool, EmitError> {
        // A second SELECT list would overwrite the one already absorbed; only the
        // stacked-projection collapse below may combine two.
        if !ctx.select_exprs.is_empty() {
            return Ok(false);
        }
        if let LogicalPlan::Projection(inner) = projection.input.as_ref() {
            return self.absorb_stacked_projection(projection, inner, ctx);
        }
        ctx.distinct = projection.distinct;
        ctx.distinct_on.clone_from(&projection.distinct_on);
        if has_star(&projection.expressions) {
            return Ok(false);
        }
        if is_columns_over_aggregate(projection) {
            return self.absorb_aggregate_projection(projection, ctx);
        }
        if has_computed_expression(&projection.expressions) {
            ctx.has_computed = true;
        }
        set_select(&projection.expressions, &projection.aliases, ctx)?;
        self.absorb(&projection.input, ctx)
    }

    /// Collapse projection-over-projection into ONE SELECT list, only when the
    /// outer projection is a bijective column rename covering every inner output
    /// exactly once (the inner expressions render under the OUTER aliases).
    fn absorb_stacked_projection<'a>(
        &self,
        outer: &'a Projection,
        inner: &'a Projection,
        ctx: &mut PushContext<'a>,
    ) -> Result<bool, EmitError> {
        if outer.distinct_on.is_some() {
            return Ok(false);
        }
        let Some(substituted) = substitute_bijective_rename(outer, inner) else {
            return Ok(false);
        };
        ctx.distinct = outer.distinct || inner.distinct;
        ctx.distinct_on.clone_from(&inner.distinct_on);
        set_select(&substituted, &outer.aliases, ctx)?;
        self.absorb(&inner.input, ctx)
    }

    /// Render a projection that selects (and often renames) an aggregate child's
    /// outputs: each selected column becomes the aggregate's real expression under
    /// the projection's alias.
    fn absorb_aggregate_projection<'a>(
        &self,
        projection: &'a Projection,
        ctx: &mut PushContext<'a>,
    ) -> Result<bool, EmitError> {
        let resolved = resolve_against_aggregate(&projection.expressions, &projection.input);
        set_select(&resolved, &projection.aliases, ctx)?;
        self.absorb(&projection.input, ctx)
    }

    /// Record GROUP BY and the aggregate SELECT list, then descend.
    fn absorb_aggregate<'a>(
        &self,
        aggregate: &'a Aggregate,
        ctx: &mut PushContext<'a>,
    ) -> Result<bool, EmitError> {
        ctx.has_aggregate = true;
        if ctx.select_exprs.is_empty() {
            set_select(&aggregate.aggregates, &aggregate.output_names, ctx)?;
        }
        ctx.group_clause = clauses::group_by(
            &aggregate.group_by,
            aggregate.grouping_sets.as_deref(),
            &CANONICAL_SOURCE_RESOLVER,
        )?;
        self.absorb(&aggregate.input, ctx)
    }

    /// Push a WHERE-level filter into the remote query; a filter directly over an
    /// Aggregate is a HAVING (binder-rewritten to the aggregate's output alias, not
    /// a remotely resolvable WHERE), so decline and keep it local above the pushed
    /// aggregate.
    fn absorb_filter<'a>(
        &self,
        filter: &'a Filter,
        ctx: &mut PushContext<'a>,
    ) -> Result<bool, EmitError> {
        if matches!(filter.input.as_ref(), LogicalPlan::Aggregate(_)) {
            return Ok(false);
        }
        ctx.where_terms
            .push(render_expr(&filter.predicate, &CANONICAL_SOURCE_RESOLVER)?);
        self.absorb(&filter.input, ctx)
    }

    // ---- derived FROM sources (recurse via a child context) ------------------

    /// Set a derived table `(SELECT ...) AS alias` as the FROM source.
    fn absorb_subquery_scan_base<'a>(
        &self,
        node: &'a SubqueryScan,
        ctx: &mut PushContext<'a>,
    ) -> Result<bool, EmitError> {
        let Some(rendered) = self.render_subquery_scan(node, ctx)? else {
            return Ok(false);
        };
        ctx.from_clause = Some(rendered);
        ctx.has_subquery_relation = true;
        Ok(true)
    }

    /// Set a UNION/INTERSECT/EXCEPT as the FROM source `(... UNION ...) AS subq_N`,
    /// but only inside a derived sub-render; a top-level set operation is left to
    /// the planner's set-operation path.
    fn absorb_set_operation_base<'a>(
        &self,
        node: &'a SetOperation,
        ctx: &mut PushContext<'a>,
    ) -> Result<bool, EmitError> {
        if !ctx.in_derived {
            return Ok(false);
        }
        let Some(rendered) = self.render_set_operation(node, ctx)? else {
            return Ok(false);
        };
        ctx.from_clause = Some(rendered);
        ctx.has_subquery_relation = true;
        Ok(true)
    }

    /// Add one join to a left-deep tree, rendering its right relation. Handles the
    /// nullable-right-filter carry and the SEMI/ANTI -> EXISTS rewrite.
    fn absorb_join<'a>(
        &self,
        join: &'a Join,
        ctx: &mut PushContext<'a>,
    ) -> Result<bool, EmitError> {
        if !join_kind_supported(join.join_type) || !join_is_pushable(join) {
            return Ok(false);
        }
        if !self.absorb_from(&join.left, ctx)? {
            return Ok(false);
        }
        let (suppress_filter, on_extra) = match nullable_right(join)? {
            NullableRight::Decline => return Ok(false),
            NullableRight::Normal => (false, None),
            NullableRight::RideFilter(text) => (true, Some(text)),
        };
        let Some(right_ref) = self.render_relation(&join.right, ctx, suppress_filter)? else {
            return Ok(false);
        };
        if matches!(join.join_type, JoinType::Semi | JoinType::Anti) {
            let condition = join
                .condition
                .as_ref()
                .expect("SEMI/ANTI join carries an ON condition");
            let cond = render_expr(condition, &CANONICAL_SOURCE_RESOLVER)?;
            let keyword = if join.join_type == JoinType::Anti {
                "NOT EXISTS"
            } else {
                "EXISTS"
            };
            ctx.where_terms
                .push(format!("{keyword}(SELECT 1 FROM {right_ref} WHERE {cond})"));
            ctx.has_join = true;
            return Ok(true);
        }
        if contributes_columns(join) {
            ctx.has_derived_columns = true;
        }
        ctx.joins
            .push(render_join_clause(join, &right_ref, on_extra)?);
        ctx.has_join = true;
        Ok(true)
    }

    /// Absorb a dependent join as `LEFT JOIN LATERAL (...) ON TRUE`. The right side
    /// keeps its outer column references verbatim; a cross-source right fails the
    /// datasource check inside the derived render and declines.
    fn absorb_lateral_join<'a>(
        &self,
        node: &'a LateralJoin,
        ctx: &mut PushContext<'a>,
    ) -> Result<bool, EmitError> {
        if !self.absorb_from(&node.left, ctx)? {
            return Ok(false);
        }
        let Some(right_ref) = self.render_relation(&node.right, ctx, false)? else {
            return Ok(false);
        };
        let prefix = join_prefix(node.join_type);
        ctx.joins
            .push(format!("{prefix} JOIN LATERAL {right_ref} ON TRUE"));
        ctx.has_join = true;
        ctx.has_derived_columns = true;
        Ok(true)
    }

    /// Render a CTE body, record it as a WITH definition, then descend. A recursive
    /// CTE registers its name BEFORE the body render so the body can self-reference.
    fn absorb_cte<'a>(&self, node: &'a Cte, ctx: &mut PushContext<'a>) -> Result<bool, EmitError> {
        if node.recursive {
            ctx.visible_ctes.push(node.name.clone());
        }
        let Some(body_sql) = self.render_cte_body(node, ctx)? else {
            return Ok(false);
        };
        ctx.ctes.push(CteDef {
            name: node.name.clone(),
            columns: node.column_names.clone(),
            body_sql,
        });
        ctx.has_cte = true;
        if node.recursive {
            ctx.has_recursive_cte = true;
        }
        self.absorb(&node.child, ctx)
    }

    // ---- set-op / CTE-body / derived-relation rendering ----------------------

    /// Build a derived table `(SELECT ...) AS alias`, keeping the user-visible
    /// alias. NOTE the STRICT `!=` source comparison (not `same_source`): a
    /// CTE-only inner body (datasource None) under a parent with a set source
    /// DECLINES here.
    fn render_subquery_scan<'a>(
        &self,
        node: &'a SubqueryScan,
        ctx: &mut PushContext<'a>,
    ) -> Result<Option<String>, EmitError> {
        let mut inner = ctx.child();
        if !self.absorb(&node.input, &mut inner)? || inner.select_exprs.is_empty() {
            return Ok(None);
        }
        if ctx.datasource.is_some() && inner.datasource != ctx.datasource {
            return Ok(None);
        }
        if ctx.datasource.is_none() {
            ctx.datasource.clone_from(&inner.datasource);
        }
        let body = render(&inner)?;
        Ok(Some(derived_table(&body, &node.alias)))
    }

    /// Build a projected sub-relation as `(SELECT ...) AS subq_N`. Same source
    /// check as `render_subquery_scan` (STRICT `!=`).
    fn render_derived<'a>(
        &self,
        node: &'a LogicalPlan,
        ctx: &mut PushContext<'a>,
    ) -> Result<Option<String>, EmitError> {
        let mut inner = ctx.child();
        if !self.absorb(node, &mut inner)? || inner.select_exprs.is_empty() {
            return Ok(None);
        }
        if ctx.datasource.is_some() && inner.datasource != ctx.datasource {
            return Ok(None);
        }
        if ctx.datasource.is_none() {
            ctx.datasource.clone_from(&inner.datasource);
        }
        let alias = derived_alias(ctx);
        let body = render(&inner)?;
        Ok(Some(derived_table(&body, &alias)))
    }

    /// Build a join's right side as a FROM/JOIN relation: a base scan stays a table
    /// reference; anything projected becomes a derived table. `suppress_filter`
    /// only meaningfully affects the base-scan branch.
    fn render_relation<'a>(
        &self,
        node: &'a LogicalPlan,
        ctx: &mut PushContext<'a>,
        suppress_filter: bool,
    ) -> Result<Option<String>, EmitError> {
        match node {
            LogicalPlan::SubqueryScan(n) => self.render_subquery_scan(n, ctx),
            LogicalPlan::CteRef(n) => {
                if cte_defined(&n.name, ctx) {
                    Ok(Some(cte_ref_sql(n)))
                } else {
                    Ok(None)
                }
            }
            LogicalPlan::Scan(scan) if !is_derived_relation(node) => {
                if claim_scan(scan, ctx, suppress_filter)? {
                    Ok(Some(scan_ref(scan, ctx)))
                } else {
                    Ok(None)
                }
            }
            _ => self.render_derived(node, ctx),
        }
    }

    /// Build a set operation as an aliased derived relation of its branches.
    fn render_set_operation<'a>(
        &self,
        node: &'a SetOperation,
        ctx: &mut PushContext<'a>,
    ) -> Result<Option<String>, EmitError> {
        let Some(left) = self.render_branch(&node.left, ctx, None)? else {
            return Ok(None);
        };
        let Some(right) = self.render_branch(&node.right, ctx, None)? else {
            return Ok(None);
        };
        let alias = derived_alias(ctx);
        Ok(Some(derived_table(
            &set_op_sql(node, &left, &right),
            &alias,
        )))
    }

    /// Build one set-operation branch as a standalone SELECT on the same source.
    /// `anchor_columns` (a CTE column list) overrides the branch's output names in
    /// place BEFORE render, so the anchor SELECT emits `<expr> AS <ctecol>` (the
    /// anchor-aliasing rewrite).
    fn render_branch<'a>(
        &self,
        node: &'a LogicalPlan,
        ctx: &mut PushContext<'a>,
        anchor_columns: Option<&[String]>,
    ) -> Result<Option<String>, EmitError> {
        if let LogicalPlan::Values(values) = node {
            return render_values_branch(values, anchor_columns);
        }
        let mut inner = ctx.child();
        if !self.absorb(node, &mut inner)? || inner.select_exprs.is_empty() {
            return Ok(None);
        }
        if !branch_source_compatible(&inner, ctx) {
            return Ok(None);
        }
        if let Some(cols) = anchor_columns {
            for (index, name) in cols.iter().enumerate() {
                if index < inner.output_names.len() {
                    inner.output_names[index].clone_from(name);
                }
            }
        }
        Ok(Some(render(&inner)?))
    }

    /// Build a CTE's body (the SELECT / set operation inside `name AS (...)`),
    /// threading the CTE's declared column names to the anchor branch.
    fn render_cte_body<'a>(
        &self,
        node: &'a Cte,
        ctx: &mut PushContext<'a>,
    ) -> Result<Option<String>, EmitError> {
        match node.cte_plan.as_ref() {
            LogicalPlan::SetOperation(set_op) => {
                self.render_cte_set_body(set_op, node.column_names.as_deref(), ctx)
            }
            other => self.render_branch(other, ctx, node.column_names.as_deref()),
        }
    }

    /// Build a set-operation CTE body (UNWRAPPED - no derived-table alias). Only
    /// the LEFT (anchor) branch receives `anchor_columns`.
    fn render_cte_set_body<'a>(
        &self,
        node: &'a SetOperation,
        anchor_columns: Option<&[String]>,
        ctx: &mut PushContext<'a>,
    ) -> Result<Option<String>, EmitError> {
        let Some(left) = self.render_branch(&node.left, ctx, anchor_columns)? else {
            return Ok(None);
        };
        let Some(right) = self.render_branch(&node.right, ctx, None)? else {
            return Ok(None);
        };
        Ok(Some(set_op_sql(node, &left, &right)))
    }

    // ---- catalog-reading star expansion + finish -----------------------------

    /// Build a unique-aliased SELECT list from each interior scan's needed columns
    /// (a bare `SELECT *` over a join). Declines when a column-contributing join
    /// pulled in a derived relation (its synthetic columns escape base-scan
    /// expansion). Cannot emit, so returns a plain bool.
    fn expand_star_select(&self, ctx: &mut PushContext) -> bool {
        if ctx.has_derived_columns {
            return false;
        }
        // A copyable vec of references; cloned so the loop may mutate `ctx`.
        let scans: Vec<&Scan> = ctx.scans.clone();
        let mut seen: HashSet<String> = HashSet::new();
        for scan in scans {
            let Some(names) = self.scan_output_columns(scan) else {
                return false;
            };
            expand_scan_columns(scan, &names, &mut seen, ctx);
        }
        true
    }

    /// The columns a scan must emit: its pruned list, or the catalog's full list
    /// when it reads `*`. None when the source table is unknown.
    fn scan_output_columns(&self, scan: &Scan) -> Option<Vec<String>> {
        if !scan.columns.is_empty() && !scan.columns.iter().any(|c| c == "*") {
            return Some(scan.columns.clone());
        }
        let table =
            self.catalog
                .get_table(&scan.datasource, &scan.schema_name, &scan.table_name)?;
        Some(table.columns.iter().map(|c| c.name.clone()).collect())
    }

    /// Resolve the connection, render SQL, and build the physical node - or decline
    /// (`Ok(None)`) when the source is unresolved or lacks JOIN capability.
    fn finish(
        &self,
        ctx: &PushContext,
        estimated_rows: Option<u64>,
        output_estimated_rows: Option<u64>,
    ) -> Result<Option<PhysicalRemoteQuery>, EmitError> {
        let Some(datasource) = self.resolve_datasource(ctx) else {
            return Ok(None);
        };
        let Some(source) = self.catalog.get_datasource(&datasource) else {
            return Ok(None);
        };
        if !source.supports_capability(DataSourceCapability::Joins) {
            return Ok(None);
        }
        let mut column_alias_map = ctx.column_aliases.clone();
        expose_computed_outputs(&mut column_alias_map, &ctx.output_names);
        let sql = render(ctx)?;
        // Fresh PhysicalRemoteQuery built from the rendered same-source SQL and the
        // push context: nothing of this type to copy from, so every field is listed
        // (the estimated_rows / output_estimated_rows / column_ndv stamps are
        // deliberate, seeded_schema / group_observation start None).
        Ok(Some(PhysicalRemoteQuery {
            datasource,
            sql,
            output_names: ctx.output_names.clone(),
            column_alias_map,
            estimated_rows,
            output_estimated_rows,
            column_ndv: remote_column_ndv(ctx),
            seeded_schema: None,
            group_observation: None,
        }))
    }

    /// Pick the target source, defaulting a pure-computation CTE (recursive or
    /// constant-only) to the sole source when exactly one exists. Ports
    /// `_resolve_datasource`.
    fn resolve_datasource(&self, ctx: &PushContext) -> Option<String> {
        if let Some(datasource) = &ctx.datasource {
            return Some(datasource.clone());
        }
        if !ctx.has_cte {
            return None;
        }
        // The sole target must be a real remote source the body can run against;
        // the internal materialized-view store is never one, so it is excluded.
        let names = self.catalog.remote_datasource_names();
        if names.len() == 1 {
            names.into_iter().next()
        } else {
            None
        }
    }
}

/// Whether two datasource names identify the same source. A None name matches
/// nothing (including None). The single definition both the pushdown builder and
/// the physical planner use.
pub fn same_source(left: Option<&str>, right: Option<&str>) -> bool {
    matches!((left, right), (Some(l), Some(r)) if l == r)
}

// ---- base-scan claiming + the FROM table reference (pass-free) --------------

/// Set the leftmost FROM source from a base scan and collect its filter.
fn absorb_base_scan<'a>(scan: &'a Scan, ctx: &mut PushContext<'a>) -> Result<bool, EmitError> {
    if scan.aggregates.is_some() || scan.group_by.is_some() {
        return absorb_aggregate_scan(scan, ctx);
    }
    if !claim_scan(scan, ctx, false)? {
        return Ok(false);
    }
    ctx.from_clause = Some(scan_ref(scan, ctx));
    absorb_scan_modifiers(scan, ctx);
    Ok(true)
}

/// Render a scan carrying folded aggregates/GROUP BY as the FROM source: its
/// aggregates become the SELECT list, its grouping the GROUP BY.
fn absorb_aggregate_scan<'a>(scan: &'a Scan, ctx: &mut PushContext<'a>) -> Result<bool, EmitError> {
    if !claim_source(scan, ctx) {
        return Ok(false);
    }
    if ctx.select_exprs.is_empty() {
        let aggregates = scan
            .aggregates
            .as_ref()
            .expect("aggregate scan carries aggregates");
        let output_names = scan
            .output_names
            .as_ref()
            .expect("aggregate scan carries output names");
        set_select(aggregates, output_names, ctx)?;
    }
    split_scan_filter(scan, ctx)?;
    ctx.group_clause = clauses::group_by(
        scan.group_by.as_deref().unwrap_or(&[]),
        scan.grouping_sets.as_deref(),
        &CANONICAL_SOURCE_RESOLVER,
    )?;
    ctx.from_clause = Some(scan_ref(scan, ctx));
    ctx.has_aggregate = true;
    Ok(true)
}

/// Route a folded aggregate-scan filter into WHERE / HAVING via the shared
/// `split_where_having` (the single source of truth), rendering each side.
fn split_scan_filter(scan: &Scan, ctx: &mut PushContext) -> Result<(), EmitError> {
    let Some(filters) = &scan.filters else {
        return Ok(());
    };
    let output_map = aggregate_output_map(
        scan.output_names
            .as_ref()
            .expect("aggregate scan carries output names"),
        scan.aggregates
            .as_ref()
            .expect("aggregate scan carries aggregates"),
    );
    let (where_pred, having_pred) = split_where_having(filters, &output_map);
    if let Some(where_pred) = where_pred {
        ctx.where_terms
            .push(render_expr(&where_pred, &CANONICAL_SOURCE_RESOLVER)?);
    }
    if let Some(having_pred) = having_pred {
        ctx.having_terms
            .push(render_expr(&having_pred, &CANONICAL_SOURCE_RESOLVER)?);
    }
    Ok(())
}

/// Set a CTE reference (the bare name) as the FROM source, only when the CTE is
/// defined within this same pushed WITH.
fn absorb_cte_ref_base(node: &CteRef, ctx: &mut PushContext) -> bool {
    if !cte_defined(&node.name, ctx) {
        return false;
    }
    ctx.from_clause = Some(cte_ref_sql(node));
    true
}

/// Confirm a plain (non-aggregate) scan shares the data source and collect its
/// filter (unless suppressed for a nullable-right ON carry). Fallible: a
/// subquery-bearing filter reaching emit RAISES, never dropped.
fn claim_scan<'a>(
    scan: &'a Scan,
    ctx: &mut PushContext<'a>,
    suppress_filter: bool,
) -> Result<bool, EmitError> {
    if scan.group_by.is_some() || scan.aggregates.is_some() {
        return Ok(false);
    }
    if !source_compatible(scan, ctx) {
        return Ok(false);
    }
    if ctx.datasource.is_none() {
        ctx.datasource = Some(scan.datasource.clone());
    }
    ctx.scans.push(scan);
    if !suppress_filter {
        if let Some(filters) = &scan.filters {
            ctx.where_terms
                .push(render_expr(filters, &CANONICAL_SOURCE_RESOLVER)?);
        }
    }
    Ok(true)
}

/// Bind an aggregate scan's data source (its filter is split separately).
fn claim_source<'a>(scan: &'a Scan, ctx: &mut PushContext<'a>) -> bool {
    if !source_compatible(scan, ctx) {
        return false;
    }
    if ctx.datasource.is_none() {
        ctx.datasource = Some(scan.datasource.clone());
    }
    ctx.scans.push(scan);
    true
}

/// Whether a scan may join this push: always true in merge-render mode (every scan
/// becomes a registered Arrow relation, cross-source allowed), else the same-source
/// check.
fn source_compatible(scan: &Scan, ctx: &PushContext) -> bool {
    if ctx.scan_names.is_some() {
        return true;
    }
    ctx.datasource.is_none() || same_source(ctx.datasource.as_deref(), Some(&scan.datasource))
}

/// Build the FROM table reference (always aliased) for a base scan. In merge-render
/// mode a registered scan renders as its Arrow relation name (no schema, no sample)
/// via `scan_names` pointer identity.
fn scan_ref(scan: &Scan, ctx: &PushContext) -> String {
    let alias = scan.alias.as_deref().unwrap_or(&scan.table_name);
    if let Some(names) = ctx.scan_names {
        if let Some(registered) = names.get(&std::ptr::from_ref(scan)) {
            return table_ref(registered, None, Some(alias), None);
        }
    }
    table_ref(
        &scan.table_name,
        Some(&scan.schema_name),
        Some(alias),
        scan.sample.as_deref(),
    )
}

/// Render an ORDER BY / LIMIT / OFFSET / DISTINCT the optimizer folded onto a base
/// scan; a clause already set by an explicit node above is never overwritten.
fn absorb_scan_modifiers(scan: &Scan, ctx: &mut PushContext) {
    adopt_scan_order(scan, ctx);
    adopt_scan_limit(scan, ctx);
    adopt_scan_offset(scan, ctx);
    adopt_scan_distinct(scan, ctx);
}

/// Adopt a scan's folded ORDER BY unless a Sort node already set one. A folded key
/// is a plain scan column (never a subquery), so the render cannot raise; `expect`
/// keeps that loud rather than swallowing.
fn adopt_scan_order(scan: &Scan, ctx: &mut PushContext) {
    if scan.order_by_keys.is_none() || ctx.order_clause.is_some() {
        return;
    }
    let keys = scan.order_by_keys.as_deref().unwrap_or(&[]);
    let ascending = scan.order_by_ascending.as_deref().unwrap_or(&[]);
    let nulls = scan.order_by_nulls.as_deref().unwrap_or(&[]);
    ctx.order_clause = clauses::order_by(keys, ascending, nulls, &CANONICAL_SOURCE_RESOLVER)
        .expect("folded scan ORDER BY keys render without a subquery");
}

/// Adopt a scan's folded LIMIT unless a Limit node already set one.
fn adopt_scan_limit(scan: &Scan, ctx: &mut PushContext) {
    if let Some(limit) = scan.limit {
        if ctx.limit.is_none() {
            ctx.limit = Some(clamp_i64(limit));
        }
    }
}

/// Adopt a scan's folded OFFSET unless a Limit node already set one.
fn adopt_scan_offset(scan: &Scan, ctx: &mut PushContext) {
    if scan.offset != 0 && ctx.offset == 0 {
        ctx.offset = clamp_i64(scan.offset);
    }
}

/// Adopt a scan's folded DISTINCT unless already set above.
fn adopt_scan_distinct(scan: &Scan, ctx: &mut PushContext) {
    if scan.distinct && !ctx.distinct {
        ctx.distinct = true;
    }
}

// ---- SELECT-list building + star expansion (pass-free) ----------------------

/// Build a unique-aliased SELECT list and record each column reference's
/// (qualifier, column) -> output-name mapping. REPLACES the SELECT list wholesale
/// (matching Python `context.select_items = items`).
fn set_select(exprs: &[Expr], names: &[String], ctx: &mut PushContext) -> Result<(), EmitError> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut items = Vec::with_capacity(exprs.len());
    let mut output_names = Vec::with_capacity(exprs.len());
    for (index, expr) in exprs.iter().enumerate() {
        let base = base_name(expr, names, index)?;
        let unique = unique_name(&base, &seen);
        seen.insert(unique.clone());
        output_names.push(unique.clone());
        items.push(expr.clone());
        record_alias(expr, &unique, ctx);
    }
    ctx.select_exprs = items;
    ctx.output_names = output_names;
    Ok(())
}

/// Append a scan's named columns as unique-aliased, QUALIFIED SELECT items (no
/// unqualified ref survives; the emitter resolves `alias.name`).
fn expand_scan_columns(
    scan: &Scan,
    names: &[String],
    seen: &mut HashSet<String>,
    ctx: &mut PushContext,
) {
    let alias = scan
        .alias
        .clone()
        .unwrap_or_else(|| scan.table_name.clone());
    for name in names {
        let unique = unique_name(name, seen);
        seen.insert(unique.clone());
        ctx.output_names.push(unique.clone());
        ctx.select_exprs.push(Expr::Column(ColumnRef::new(
            Some(alias.clone()),
            name.clone(),
            None,
        )));
        ctx.column_aliases
            .insert((Some(alias.clone()), name.clone()), unique);
    }
}

/// Whether a projection selects only column refs of an aggregate child.
fn is_columns_over_aggregate(projection: &Projection) -> bool {
    let Some(outputs) = aggregate_outputs(&projection.input) else {
        return false;
    };
    all_output_refs(&projection.expressions, outputs)
}

// ---- assembly (pass-free) ---------------------------------------------------

/// Assemble the remote SELECT text from the accumulated pieces, prefixed with a
/// leading WITH when CTEs were collected.
///
/// # Panics
/// Panics when reached without a FROM source set - an internal absorb-invariant
/// violation (a crash over a silent `SELECT ... FROM None`).
fn render(ctx: &PushContext) -> Result<String, EmitError> {
    let items = if ctx.select_exprs.is_empty() {
        "*".to_string()
    } else {
        clauses::select_list(
            &ctx.select_exprs,
            &ctx.output_names,
            &CANONICAL_SOURCE_RESOLVER,
        )?
    };
    let from_clause = ctx
        .from_clause
        .as_deref()
        .expect("render reached without a FROM source (absorb invariant)");
    let joins = if ctx.joins.is_empty() {
        None
    } else {
        Some(ctx.joins.join(" "))
    };
    let where_ = combine_and_text(&ctx.where_terms);
    let having = combine_and_text(&ctx.having_terms);
    let distinct_on = match &ctx.distinct_on {
        None => None,
        Some(keys) => Some(render_keys_csv(keys)?),
    };
    let pieces = SelectPieces {
        from_clause,
        select_items: &items,
        joins: joins.as_deref(),
        where_: where_.as_deref(),
        group: ctx.group_clause.as_deref(),
        having: having.as_deref(),
        distinct: ctx.distinct,
        distinct_on: distinct_on.as_deref(),
        order: ctx.order_clause.as_deref(),
        limit: ctx.limit,
        offset: ctx.offset,
    };
    let select = clauses::assemble_select(&pieces);
    Ok(with_prefix(ctx) + &select)
}

/// Attach a leading `WITH [RECURSIVE] ...` from the collected CTE definitions, or
/// nothing. CTE names/columns are UNQUOTED (engine-generated safe identifiers,
/// matching `exp.to_identifier` without `quoted=True`).
fn with_prefix(ctx: &PushContext) -> String {
    if ctx.ctes.is_empty() {
        return String::new();
    }
    let recursive = if ctx.has_recursive_cte {
        "RECURSIVE "
    } else {
        ""
    };
    let mut defs = Vec::with_capacity(ctx.ctes.len());
    for cte in &ctx.ctes {
        defs.push(cte_definition(cte));
    }
    format!("WITH {recursive}{} ", defs.join(", "))
}

/// Build one `name[(c1, c2)] AS (body_sql)` CTE definition.
fn cte_definition(cte: &CteDef) -> String {
    let mut text = cte.name.clone();
    if let Some(columns) = &cte.columns {
        text.push('(');
        text.push_str(&columns.join(", "));
        text.push(')');
    }
    text.push_str(" AS (");
    text.push_str(&cte.body_sql);
    text.push(')');
    text
}

/// AND a list of already-parenthesized predicate fragments into one WHERE/HAVING
/// text, or None when empty. Each term is a self-parenthesized `render_expr`
/// fragment or an `EXISTS(...)` primary, so string-joining with ` AND ` never
/// re-associates.
fn combine_and_text(terms: &[String]) -> Option<String> {
    if terms.is_empty() {
        None
    } else {
        Some(terms.join(" AND "))
    }
}

/// The DISTINCT ON key expressions rendered to a comma-separated fragment.
fn render_keys_csv(keys: &[Expr]) -> Result<String, EmitError> {
    let mut rendered = Vec::with_capacity(keys.len());
    for key in keys {
        rendered.push(render_expr(key, &CANONICAL_SOURCE_RESOLVER)?);
    }
    Ok(rendered.join(", "))
}

// ---- join / relation shaping (pass-free) ------------------------------------

/// Classify a nullable-preserving join's right-side filter handling. A
/// CLEAN-RUST deviation from Python's `model_copy(filters=None)` clone: keep the
/// `&Scan` borrow and thread `suppress_filter` into `claim_scan` instead, so the
/// scan's `scan_names` pointer identity is preserved.
fn nullable_right(join: &Join) -> Result<NullableRight, EmitError> {
    if !matches!(join.join_type, JoinType::Left | JoinType::Full) {
        return Ok(NullableRight::Normal);
    }
    let LogicalPlan::Scan(right) = join.right.as_ref() else {
        return Ok(NullableRight::Normal);
    };
    let Some(filters) = &right.filters else {
        return Ok(NullableRight::Normal);
    };
    if join.join_type == JoinType::Full || join.natural || join.using.is_some() {
        return Ok(NullableRight::Decline);
    }
    Ok(NullableRight::RideFilter(render_expr(
        filters,
        &CANONICAL_SOURCE_RESOLVER,
    )?))
}

/// Whether a join kind may push (any of INNER/LEFT/RIGHT/FULL/SEMI/ANTI; NOT
/// CROSS).
fn join_kind_supported(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Inner
            | JoinType::Left
            | JoinType::Right
            | JoinType::Full
            | JoinType::Semi
            | JoinType::Anti
    )
}

/// A join pushes when it has an ON condition, or is NATURAL/USING.
fn join_is_pushable(join: &Join) -> bool {
    join.condition.is_some() || join.natural || join.using.is_some()
}

/// The SQL JOIN prefix for a column-contributing join. SEMI/ANTI (existence
/// filters) and CROSS never reach here.
fn join_prefix(join_type: JoinType) -> &'static str {
    match join_type {
        JoinType::Inner => "INNER",
        JoinType::Left => "LEFT",
        JoinType::Right => "RIGHT",
        JoinType::Full => "FULL OUTER",
        JoinType::Cross | JoinType::Semi | JoinType::Anti => {
            unreachable!("join_prefix on a CROSS/SEMI/ANTI join")
        }
    }
}

/// Build one join clause text using ON, USING, or NATURAL. `on_extra` is a
/// nullable-side filter conjunct that must evaluate as part of the match condition
///.
fn render_join_clause(
    join: &Join,
    right_ref: &str,
    on_extra: Option<String>,
) -> Result<String, EmitError> {
    let prefix = join_prefix(join.join_type);
    if join.natural {
        return Ok(format!("NATURAL {prefix} JOIN {right_ref}"));
    }
    if let Some(columns) = &join.using {
        let mut quoted = Vec::with_capacity(columns.len());
        for column in columns {
            quoted.push(quote_ident(column));
        }
        return Ok(format!(
            "{prefix} JOIN {right_ref} USING ({})",
            quoted.join(", ")
        ));
    }
    let condition = join
        .condition
        .as_ref()
        .expect("column-contributing join carries an ON condition");
    let mut on = render_expr(condition, &CANONICAL_SOURCE_RESOLVER)?;
    if let Some(extra) = on_extra {
        on = format!("({on}) AND ({extra})");
    }
    Ok(format!("{prefix} JOIN {right_ref} ON {on}"))
}

/// Whether a join adds its right relation's columns to a `*` projection: SEMI/ANTI
/// contribute none; any other join exposes the right side, and a derived right side
/// escapes base-scan star expansion.
fn contributes_columns(join: &Join) -> bool {
    if matches!(join.join_type, JoinType::Semi | JoinType::Anti) {
        return false;
    }
    is_derived_relation(&join.right)
}

/// Whether a node renders as a derived table rather than a base table reference.
fn is_derived_relation(node: &LogicalPlan) -> bool {
    match node {
        LogicalPlan::Scan(scan) => scan.aggregates.is_some() || scan.group_by.is_some(),
        _ => true,
    }
}

/// Build a UNION/INTERSECT/EXCEPT branch text; `distinct` false renders the ALL
/// form (via the shared `set_op_keyword`).
fn set_op_sql(node: &SetOperation, left: &str, right: &str) -> String {
    format!(
        "{left} {} {right}",
        clauses::set_op_keyword(node.kind, node.distinct)
    )
}

/// Build a single constant row as a FROM-less `SELECT <items>` branch (the base
/// case of a recursive CTE), bypassing `assemble_select` (which always emits FROM).
/// A trailing alias is added only when it differs from the rendered expression.
fn render_values_branch(
    node: &Values,
    anchor_columns: Option<&[String]>,
) -> Result<Option<String>, EmitError> {
    if node.rows.len() != 1 {
        return Ok(None);
    }
    let mut items = Vec::with_capacity(node.output_names.len());
    for (index, (expr, name)) in node.rows[0]
        .iter()
        .zip(node.output_names.iter())
        .enumerate()
    {
        // A CTE column list, when present, names each output column, so a recursive
        // CTE's `SELECT <const>` anchor emits `<const> AS <ctecol>`. The merge engine
        // ignores a `WITH name(cols)` list, so aliasing here makes the CTE self-named
        // and its self-reference (`seq.n`) resolves.
        let target = anchor_columns
            .and_then(|columns| columns.get(index))
            .map_or(name.as_str(), String::as_str);
        let mut fragment = render_expr(expr, &CANONICAL_SOURCE_RESOLVER)?;
        if !target.is_empty() && target != render_canonical(expr)? {
            fragment = format!("{fragment} AS {}", quote_ident(target));
        }
        items.push(fragment);
    }
    Ok(Some(format!("SELECT {}", items.join(", "))))
}

/// Confirm a set-op branch shares the source, adopting it when one is unset
/// (LENIENT - a CTE-only branch has no scan and so no source of its own). Ports
/// `_branch_source_compatible`.
fn branch_source_compatible(inner: &PushContext, ctx: &mut PushContext) -> bool {
    if inner.datasource.is_none() {
        return true;
    }
    if ctx.datasource.is_some()
        && !same_source(inner.datasource.as_deref(), ctx.datasource.as_deref())
    {
        return false;
    }
    ctx.datasource.clone_from(&inner.datasource);
    true
}

/// A unique relation alias for the next derived table (`subq_0`, `subq_1`, ...).
fn derived_alias(ctx: &mut PushContext) -> String {
    let alias = format!("subq_{}", ctx.derived_count);
    ctx.derived_count += 1;
    alias
}

/// Whether a CTE name is defined here or in an enclosing WITH.
fn cte_defined(name: &str, ctx: &PushContext) -> bool {
    ctx.visible_ctes.iter().any(|n| n == name) || ctx.ctes.iter().any(|c| c.name == name)
}

// ---- estimates / NDV (READ pre-stamped fields; no CostModel) ----------------

/// The reduction-orientation FLOOR: the LARGEST base scan the subtree reads (NOT
/// the join OUTPUT estimate, which under-counts a fact island). Ports
/// `_root_estimate`.
fn root_estimate(node: &LogicalPlan) -> Option<u64> {
    let mut sizes = Vec::new();
    collect_scan_sizes(node, &mut sizes);
    sizes.into_iter().max()
}

/// Gather every base scan's cost estimate in the subtree.
fn collect_scan_sizes(node: &LogicalPlan, sizes: &mut Vec<u64>) {
    if let LogicalPlan::Scan(scan) = node {
        if let Some(rows) = scan.estimated_rows {
            sizes.push(rows);
        }
        return;
    }
    for child in node.children() {
        collect_scan_sizes(child, sizes);
    }
}

/// The subtree ROOT's own row estimate: descend row-count-preserving wrappers
/// (Projection, Sort) to the estimated Scan/Join; None when the root reshapes
/// cardinality. Ports `_output_estimate`.
fn output_estimate(node: &LogicalPlan) -> Option<u64> {
    let mut current = node;
    loop {
        match current {
            LogicalPlan::Projection(p) => current = &p.input,
            LogicalPlan::Sort(s) => current = &s.input,
            _ => break,
        }
    }
    match current {
        LogicalPlan::Scan(scan) => scan.estimated_rows,
        LogicalPlan::Join(join) => join.estimated_rows,
        _ => None,
    }
}

/// Base NDVs of interior scans' key columns, keyed by the remote OUTPUT name
/// carrying each (via the alias map, so only a column passing through unchanged is
/// mapped). Ports `_remote_column_ndv`.
fn remote_column_ndv(ctx: &PushContext) -> Option<BTreeMap<String, i64>> {
    let mut map = BTreeMap::new();
    for scan in &ctx.scans {
        let alias = scan.alias.as_deref().unwrap_or(&scan.table_name);
        if let Some(ndv) = &scan.column_ndv {
            for (column, count) in ndv {
                if let Some(output) = ctx
                    .column_aliases
                    .get(&(Some(alias.to_string()), column.clone()))
                {
                    map.insert(output.clone(), *count);
                }
            }
        }
    }
    if map.is_empty() {
        None
    } else {
        Some(map)
    }
}

// ---- pushability + finish helpers (pass-free) -------------------------------

/// Whether this subtree is worth replacing with one remote query. Ports
/// `_should_push`.
fn should_push(ctx: &PushContext) -> bool {
    if ctx.has_cte {
        return true;
    }
    if ctx.has_join || ctx.has_subquery_relation {
        return true;
    }
    if ctx.distinct_on.is_some() {
        // DISTINCT ON is only correct at the source (ORDER BY picks the row).
        return true;
    }
    ctx.has_computed && !ctx.has_aggregate
}

/// Map any output column with no source qualifier (a computed result like an
/// aggregate's value) under `(None, name)`, so a parent that reads this remote
/// query can resolve it. Operates on a cloned map so `ctx` stays read-only.
fn expose_computed_outputs(map: &mut ColumnAliasMap, output_names: &[String]) {
    let mapped: HashSet<String> = map.values().cloned().collect();
    for name in output_names {
        if !mapped.contains(name) {
            map.insert((None, name.clone()), name.clone());
        }
    }
}

// ---- expression / projection shaping (pass-free) ----------------------------

/// The desired (pre-dedup) output name for a projection item: the supplied name
/// when non-empty, else the canonical expression text.
fn base_name(expr: &Expr, names: &[String], index: usize) -> Result<String, EmitError> {
    if index < names.len() && !names[index].is_empty() {
        Ok(names[index].clone())
    } else {
        render_canonical(expr)
    }
}

/// `base` when free, else the first free `base_N`.
fn unique_name(base: &str, seen: &HashSet<String>) -> String {
    if !seen.contains(base) {
        return base.to_string();
    }
    let mut suffix = 1u32;
    loop {
        let candidate = format!("{base}_{suffix}");
        if !seen.contains(&candidate) {
            return candidate;
        }
        suffix += 1;
    }
}

/// Record a column reference's (qualifier, column) -> unique output name.
fn record_alias(expr: &Expr, unique: &str, ctx: &mut PushContext) {
    if let Expr::Column(column) = expr {
        ctx.column_aliases.insert(
            (column.table.clone(), column.column.clone()),
            unique.to_string(),
        );
    }
}

/// Output names of an aggregate-producing child (an Aggregate or an aggregate
/// Scan), else None.
fn aggregate_outputs(node: &LogicalPlan) -> Option<&[String]> {
    match node {
        LogicalPlan::Aggregate(aggregate) => Some(&aggregate.output_names),
        LogicalPlan::Scan(scan) if scan.aggregates.is_some() => scan.output_names.as_deref(),
        _ => None,
    }
}

/// The aggregate expressions of an aggregate-producing child.
fn aggregate_exprs(node: &LogicalPlan) -> &[Expr] {
    match node {
        LogicalPlan::Aggregate(aggregate) => &aggregate.aggregates,
        LogicalPlan::Scan(scan) => scan
            .aggregates
            .as_deref()
            .expect("aggregate scan carries aggregates"),
        _ => unreachable!("aggregate child is an Aggregate or an aggregate Scan"),
    }
}

/// Map each projected column to its aggregate child's source expression. A name not
/// among the outputs is a planner bug (the caller guards with
/// `is_columns_over_aggregate`), surfaced LOUD via `expect`.
fn resolve_against_aggregate(exprs: &[Expr], child: &LogicalPlan) -> Vec<Expr> {
    let outputs = aggregate_outputs(child).expect("guarded by is_columns_over_aggregate");
    let aggregates = aggregate_exprs(child);
    let mut resolved = Vec::with_capacity(exprs.len());
    for expr in exprs {
        let Expr::Column(column) = expr else {
            unreachable!("aggregate-projection items are column refs (guarded)");
        };
        let index = outputs
            .iter()
            .position(|output| output == &column.column)
            .expect("projected column is an aggregate output");
        resolved.push(aggregates[index].clone());
    }
    resolved
}

/// The inner expressions in outer order, when the outer projection is a bijective
/// column rename covering every inner output exactly once; else None (pure Expr
/// reorder, the clean-Rust equivalent of the Python AST reorder).
fn substitute_bijective_rename(outer: &Projection, inner: &Projection) -> Option<Vec<Expr>> {
    if outer.expressions.len() != inner.aliases.len() {
        return None;
    }
    let mut used: HashSet<&str> = HashSet::new();
    let mut substituted = Vec::with_capacity(outer.expressions.len());
    for expr in &outer.expressions {
        let source = rename_source(expr, inner, &used)?;
        if let Expr::Column(column) = expr {
            used.insert(column.column.as_str());
        }
        substituted.push(source.clone());
    }
    Some(substituted)
}

/// The inner expression an outer rename item maps to, or None when it is not a
/// fresh plain reference to one inner output column.
fn rename_source<'i>(expr: &Expr, inner: &'i Projection, used: &HashSet<&str>) -> Option<&'i Expr> {
    let Expr::Column(column) = expr else {
        return None;
    };
    if used.contains(column.column.as_str()) || !inner.aliases.contains(&column.column) {
        return None;
    }
    let index = inner
        .aliases
        .iter()
        .position(|alias| alias == &column.column)
        .expect("alias membership just checked");
    Some(&inner.expressions[index])
}

/// Whether every item is a bare column ref naming an aggregate output.
fn all_output_refs(exprs: &[Expr], outputs: &[String]) -> bool {
    exprs
        .iter()
        .all(|expr| matches!(expr, Expr::Column(column) if outputs.contains(&column.column)))
}

/// Whether any projection item is an unexpanded `*` reference.
fn has_star(exprs: &[Expr]) -> bool {
    exprs
        .iter()
        .any(|expr| matches!(expr, Expr::Column(column) if column.column == "*"))
}

/// Whether any projection item is more than a bare column reference.
fn has_computed_expression(exprs: &[Expr]) -> bool {
    exprs.iter().any(|expr| !matches!(expr, Expr::Column(_)))
}

/// Clamp a `u64` row count/offset to `i64` for the SELECT skeleton; a value beyond
/// `i64::MAX` (astronomically large) saturates rather than wrapping.
fn clamp_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_source_matches_equal_names_only() {
        assert!(same_source(Some("pg"), Some("pg")));
        assert!(!same_source(Some("pg"), Some("duck")));
        assert!(!same_source(Some("pg"), None));
        assert!(!same_source(None, None));
    }

    #[test]
    fn unique_name_appends_first_free_suffix() {
        let mut seen = HashSet::new();
        assert_eq!(unique_name("id", &seen), "id");
        seen.insert("id".to_string());
        assert_eq!(unique_name("id", &seen), "id_1");
        seen.insert("id_1".to_string());
        assert_eq!(unique_name("id", &seen), "id_2");
    }

    #[test]
    fn clamp_i64_saturates_beyond_max() {
        assert_eq!(clamp_i64(10), 10);
        assert_eq!(clamp_i64(u64::MAX), i64::MAX);
    }
}
