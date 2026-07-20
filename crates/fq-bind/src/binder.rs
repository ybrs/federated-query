//! The `Binder`: resolve names/types in a `LogicalPlan` against the catalog.
//!
//! Ports `parser/binder.py`. Each relational node binds its input, pushes the
//! relation scope that input exposes (alias -> Table), binds its own expressions
//! against that scope, and pops. The one column resolver is `resolve_in_scopes`
//! (qualified -> nearest scope defining the table; bare -> nearest defining the
//! column; ambiguity/no-scope raises). Post-bind every `ColumnRef` carries its
//! relation qualifier and `DataType`.
//!
//! Coverage: Scan / Projection / Filter / Join / Sort / Limit / Aggregate over
//! base tables, derived tables (SubqueryScan), CTE references + WITH (recursive
//! and non-recursive), set operations (with arity check), Values, Explain, the
//! subquery expressions (correlated - the subplan binds with the enclosing scopes
//! visible), HAVING / ORDER-BY output-alias + positional-ordinal resolution, and
//! the HAVING aggregate-call HOIST/widening (a HAVING aggregate EXPRESSION resolves
//! to its aggregate output, or an absent one becomes a hidden __agg_N output with a
//! restore projection), and the same hoist for ORDER BY keys carrying aggregate
//! calls. A recursive WITH binds its anchor first to fix the CTE's columns, then
//! registers the CTE so the recursive branch and child resolve against it.

use std::collections::HashMap;

use fq_catalog::{Catalog, Column, Table};
use fq_common::update;
use fq_plan::expr::ColumnRef;
use fq_plan::{
    Aggregate, Cte, CteRef, Explain, Expr, Filter, Join, LateralJoin, Limit, LogicalPlan,
    Projection, Scan, SetOperation, Sort, SubqueryScan, Values,
};

use crate::error::BindError;

/// One relation scope: alias (or table name) -> the table it exposes.
type Scope = Vec<(String, Table)>;

/// The name-resolution pass over one query's logical plan.
pub struct Binder<'a> {
    catalog: &'a Catalog,
    scopes: Vec<Scope>,
    /// Registered CTE name -> a synthetic Table of its output columns, so a
    /// CteRef resolves against it like an ordinary relation.
    ctes: HashMap<String, Table>,
    /// Output-alias overlays (name -> type), pushed when binding HAVING or ORDER
    /// BY over a Projection/Aggregate: a bare reference to an output alias (not a
    /// base-table column) resolves here first. A stack for nested queries.
    output_aliases: Vec<HashMap<String, fq_common::DataType>>,
}

/// Coerce an empty qualifier (a bare reference) to None so the catalog searches.
fn optional(value: &str) -> Option<&str> {
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}

impl<'a> Binder<'a> {
    /// A binder over `catalog`.
    pub fn new(catalog: &'a Catalog) -> Self {
        Self {
            catalog,
            scopes: Vec::new(),
            ctes: HashMap::new(),
            output_aliases: Vec::new(),
        }
    }

    /// Bind a logical plan, resolving every reference and typing every column.
    pub fn bind(&mut self, plan: LogicalPlan) -> Result<LogicalPlan, BindError> {
        match plan {
            LogicalPlan::Scan(scan) => Ok(LogicalPlan::Scan(Box::new(self.bind_scan(*scan)?))),
            LogicalPlan::Filter(filter) => self.bind_filter(filter),
            LogicalPlan::Projection(projection) => self.bind_projection(projection),
            LogicalPlan::Sort(sort) => self.bind_sort(sort),
            LogicalPlan::Limit(limit) => self.bind_limit(limit),
            LogicalPlan::Join(join) => self.bind_join(join),
            LogicalPlan::LateralJoin(node) => self.bind_lateral_join(node),
            LogicalPlan::Aggregate(aggregate) => self.bind_aggregate(aggregate),
            LogicalPlan::SubqueryScan(node) => self.bind_subquery_scan(node),
            LogicalPlan::SetOperation(node) => self.bind_set_operation(node),
            LogicalPlan::Cte(node) => self.bind_cte(node),
            LogicalPlan::CteRef(node) => self.bind_cte_ref(node),
            LogicalPlan::Values(node) => self.bind_values(node),
            LogicalPlan::Explain(node) => self.bind_explain(node),
            other => Err(BindError::Unsupported(format!(
                "plan node {}",
                node_name(&other)
            ))),
        }
    }

    /// Bind a scan: resolve its table, stamp the RESOLVED datasource/schema
    /// onto the scan (a bare reference arrives with an empty datasource and the
    /// parser's default schema; downstream planning needs the real owner), and
    /// prune its read-set to real column names (expanding a star), so a bound
    /// scan never carries a `*`.
    fn bind_scan(&self, mut scan: Scan) -> Result<Scan, BindError> {
        let table = self.resolve_scan_table(&scan)?;
        if let Some(qualifier) = table.qualifier() {
            scan.datasource = qualifier.datasource.clone();
            scan.schema_name = qualifier.schema_name.clone();
        }
        let kept = scan_read_columns(&scan.columns, table);
        guard_no_star(&scan.table_name, &kept)?;
        scan.columns = kept;
        Ok(scan)
    }

    /// The catalog `Table` a scan reads (searching when the reference is bare).
    /// A qualified reference to a datasource that failed to connect/load at
    /// construction raises the captured connector error naming the source
    /// instead of a generic not-found.
    fn resolve_scan_table(&self, scan: &Scan) -> Result<&'a Table, BindError> {
        if let Some(table) = self.catalog.resolve_table(
            optional(&scan.datasource),
            optional(&scan.schema_name),
            &scan.table_name,
        ) {
            return Ok(table);
        }
        if !scan.datasource.is_empty() {
            if let Some(error) = self.catalog.unavailable_error(&scan.datasource) {
                return Err(BindError::DatasourceUnavailable {
                    name: scan.datasource.clone(),
                    error: error.to_string(),
                });
            }
        }
        Err(BindError::TableNotFound(format!(
            "{}.{}.{}",
            scan.datasource, scan.schema_name, scan.table_name
        )))
    }

    /// Bind a Filter: bind the input, then the predicate against the input scope.
    /// A HAVING (Filter over an Aggregate) also resolves bare references to the
    /// aggregate's output aliases.
    fn bind_filter(&mut self, mut filter: Filter) -> Result<LogicalPlan, BindError> {
        let input = self.bind(*filter.input)?;
        let overlay = aggregate_overlay(&input);
        let predicate = self.with_output_aliases(overlay, |binder| {
            binder.with_scope(&input, |binder| binder.bind_expr(&filter.predicate))
        })?;
        // A HAVING (Filter over an Aggregate) cannot compute an aggregate per row:
        // hoist each aggregate call in the predicate to an aggregate OUTPUT
        // reference (a call equal to an existing output references it; otherwise a
        // hidden __agg_N output is appended and a projection restores the schema).
        if let LogicalPlan::Aggregate(aggregate) = input {
            return Ok(hoist_having_over_aggregate(aggregate, predicate));
        }
        // In-place on the owned node: set the (rebound) input and predicate; any
        // other field the Filter grows is preserved rather than reset.
        filter.input = Box::new(input);
        filter.predicate = predicate;
        Ok(LogicalPlan::Filter(filter))
    }

    /// Bind a Projection: bind the input, then each SELECT expression.
    fn bind_projection(&mut self, mut projection: Projection) -> Result<LogicalPlan, BindError> {
        let input = self.bind(*projection.input)?;
        let expressions = self.with_scope(&input, |binder| {
            binder.bind_expr_list(&projection.expressions)
        })?;
        let distinct_on = match &projection.distinct_on {
            Some(keys) => Some(self.with_scope(&input, |binder| binder.bind_expr_list(keys))?),
            None => None,
        };
        // In-place on the owned node: overwrite only the rebound fields; `aliases`
        // and `distinct` are preserved untouched (not re-listed, so never reset).
        projection.input = Box::new(input);
        projection.expressions = expressions;
        projection.distinct_on = distinct_on;
        Ok(LogicalPlan::Projection(projection))
    }

    /// Bind a Sort: bind each ORDER BY key against the input scope, also resolving
    /// output aliases and positional ordinals (`ORDER BY <alias>` / `ORDER BY n`).
    /// A Sort directly over a set operation resolves ONLY against the combined
    /// result's output columns (SQL scoping: the branch relations are not
    /// visible, and resolving through them would be ambiguous).
    fn bind_sort(&mut self, mut sort: Sort) -> Result<LogicalPlan, BindError> {
        let input = self.bind(*sort.input)?;
        if matches!(input, LogicalPlan::SetOperation(_)) {
            let columns = self.plan_output_columns(&input)?;
            let mut sort_keys = Vec::with_capacity(sort.sort_keys.len());
            for key in &sort.sort_keys {
                sort_keys.push(set_op_sort_key(key, &columns)?);
            }
            // In-place on the owned node: only input and sort_keys are rebound;
            // `ascending` and `nulls_order` are preserved untouched.
            sort.input = Box::new(input);
            sort.sort_keys = sort_keys;
            return Ok(LogicalPlan::Sort(sort));
        }
        let output_names = output_name_list(&input);
        let overlay = aggregate_overlay(&input).or_else(|| projection_overlay(&input));
        let sort_keys = self.with_output_aliases(overlay, |binder| {
            binder.with_scope(&input, |binder| {
                binder.bind_sort_keys(&sort.sort_keys, &output_names)
            })
        })?;
        if sort_keys.iter().any(needs_aggregate_hoist) && over_aggregate(&input) {
            sort.input = Box::new(input);
            return Ok(hoist_sort_over_aggregate(sort, sort_keys));
        }
        // In-place on the owned node: overwrite only input and sort_keys; `ascending`
        // and `nulls_order` are preserved untouched (not re-listed, so never reset).
        sort.input = Box::new(input);
        sort.sort_keys = sort_keys;
        Ok(LogicalPlan::Sort(sort))
    }

    /// Bind ORDER BY keys: a positional ordinal (`ORDER BY 2`) names the 2nd
    /// output; otherwise the key binds normally (an output alias resolves via the
    /// pushed overlay, a base column via the scope).
    fn bind_sort_keys(
        &mut self,
        keys: &[Expr],
        output_names: &[String],
    ) -> Result<Vec<Expr>, BindError> {
        let mut bound = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(index) = positional_index(key, output_names.len()) {
                let name = &output_names[index];
                let data_type = self.output_alias_type(name);
                bound.push(Expr::Column(fq_plan::ColumnRef::new(
                    None,
                    name.clone(),
                    data_type,
                )));
            } else {
                bound.push(self.bind_expr(key)?);
            }
        }
        Ok(bound)
    }

    /// Run `f` with `overlay` (if any) pushed as the current output-alias scope.
    fn with_output_aliases<T>(
        &mut self,
        overlay: Option<HashMap<String, fq_common::DataType>>,
        f: impl FnOnce(&mut Self) -> Result<T, BindError>,
    ) -> Result<T, BindError> {
        match overlay {
            Some(overlay) => {
                self.output_aliases.push(overlay);
                let result = f(self);
                self.output_aliases.pop();
                result
            }
            None => f(self),
        }
    }

    /// The type recorded for an output-alias name in the innermost overlay, if any.
    pub(crate) fn output_alias_type(&self, name: &str) -> Option<fq_common::DataType> {
        self.output_aliases
            .iter()
            .rev()
            .find_map(|overlay| overlay.get(name).copied())
    }

    /// Bind a Limit: only the input needs binding (LIMIT/OFFSET are constants).
    fn bind_limit(&mut self, mut limit: Limit) -> Result<LogicalPlan, BindError> {
        let input = self.bind(*limit.input)?;
        // In-place on the owned node: only the input is rebound; `limit`/`offset` are
        // preserved untouched (LIMIT/OFFSET are constants that binding does not change).
        limit.input = Box::new(input);
        Ok(LogicalPlan::Limit(limit))
    }

    /// Bind a Join: bind both sides, then the ON condition against both scopes.
    fn bind_join(&mut self, mut join: Join) -> Result<LogicalPlan, BindError> {
        let left = self.bind(*join.left)?;
        let right = self.bind(*join.right)?;
        let condition = match &join.condition {
            Some(condition) => {
                let mut scope = Scope::new();
                self.collect_scope(&left, &mut scope)?;
                self.collect_scope(&right, &mut scope)?;
                self.scopes.push(scope);
                let bound = self.bind_expr(condition);
                self.scopes.pop();
                Some(bound?)
            }
            None => None,
        };
        // In-place on the owned node: overwrite only the rebound left/right/condition;
        // `join_type`, `natural`, `using`, and the `estimated_rows`/`estimate_defaults`
        // estimates are preserved untouched (re-listing them risked resetting a stamp).
        join.left = Box::new(left);
        join.right = Box::new(right);
        join.condition = condition;
        Ok(LogicalPlan::Join(join))
    }

    /// Bind a LATERAL (dependent) join: bind the left, then bind the right WITH
    /// the left's relation scope pushed, so a correlated reference in the right
    /// resolves against the left. The lateral scope is popped before returning.
    fn bind_lateral_join(&mut self, mut node: LateralJoin) -> Result<LogicalPlan, BindError> {
        let left = self.bind(*node.left)?;
        let mut scope = Scope::new();
        self.collect_scope(&left, &mut scope)?;
        self.scopes.push(scope);
        let right = self.bind(*node.right);
        self.scopes.pop();
        // In-place on the owned node: overwrite only the rebound left/right;
        // `join_type` is preserved untouched.
        node.left = Box::new(left);
        node.right = Box::new(right?);
        Ok(LogicalPlan::LateralJoin(node))
    }

    /// Bind an Aggregate: bind the SELECT list (its `aggregates`), then the GROUP
    /// BY keys against the input scope. A bare GROUP BY key naming a SELECT output
    /// alias resolves to that (non-aggregate) output expression (Postgres allows
    /// `GROUP BY <alias>`); grouping by an aggregate alias is an error.
    fn bind_aggregate(&mut self, mut aggregate: Aggregate) -> Result<LogicalPlan, BindError> {
        let input = self.bind(*aggregate.input)?;
        let aggregates = self.with_scope(&input, |binder| {
            binder.bind_expr_list(&aggregate.aggregates)
        })?;
        let alias_map = select_alias_map(&aggregate.output_names, &aggregates);
        let group_by = self.with_scope(&input, |binder| {
            binder.bind_group_keys(&aggregate.group_by, &alias_map)
        })?;
        let grouping_sets = match &aggregate.grouping_sets {
            Some(sets) => {
                // Every grouping-set key binds exactly like a GROUP BY key: the
                // engine's aggregate fragment renders the sets, so an unbound
                // (unqualified) key there would defeat column resolution.
                let mut bound_sets = Vec::with_capacity(sets.len());
                for set in sets {
                    bound_sets.push(
                        self.with_scope(&input, |binder| binder.bind_group_keys(set, &alias_map))?,
                    );
                }
                Some(bound_sets)
            }
            None => None,
        };
        // In-place on the owned node: overwrite only the rebound input/aggregates/
        // group_by/grouping_sets; `output_names` is preserved untouched.
        aggregate.input = Box::new(input);
        aggregate.aggregates = aggregates;
        aggregate.group_by = group_by;
        aggregate.grouping_sets = grouping_sets;
        Ok(LogicalPlan::Aggregate(aggregate))
    }

    /// Bind GROUP BY keys, resolving a bare key that names a SELECT output alias
    /// to that output's (non-aggregate) expression.
    fn bind_group_keys(
        &mut self,
        keys: &[Expr],
        alias_map: &HashMap<String, Expr>,
    ) -> Result<Vec<Expr>, BindError> {
        let mut bound = Vec::with_capacity(keys.len());
        for key in keys {
            if let Expr::Column(column) = key {
                if column.table.is_none() {
                    if let Some(aliased) = alias_map.get(&column.column) {
                        bound.push(aliased.clone());
                        continue;
                    }
                }
            }
            bound.push(self.bind_expr(key)?);
        }
        Ok(bound)
    }

    /// Bind a derived table: bind its inner plan, applying any column-alias
    /// rename. The alias itself scopes references above it.
    fn bind_subquery_scan(&mut self, mut node: SubqueryScan) -> Result<LogicalPlan, BindError> {
        let input = self.bind(*node.input)?;
        let input = match &node.column_names {
            Some(names) => self.rename_derived_columns(input, names)?,
            None => input,
        };
        // In-place on the owned node: only the input is rebound; `alias` and
        // `column_names` are preserved untouched.
        node.input = Box::new(input);
        Ok(LogicalPlan::SubqueryScan(node))
    }

    /// Apply a derived table's column-alias list via a rename projection that
    /// reads each output by its physical name and re-aliases it.
    fn rename_derived_columns(
        &self,
        plan: LogicalPlan,
        names: &[String],
    ) -> Result<LogicalPlan, BindError> {
        let columns = self.plan_output_columns(&plan)?;
        if columns.len() != names.len() {
            return Err(BindError::Unsupported(format!(
                "derived-table column list has {} names but the subquery returns {}",
                names.len(),
                columns.len()
            )));
        }
        guard_unique_output_names(&columns)?;
        let expressions = columns
            .iter()
            .map(|column| {
                Expr::Column(fq_plan::ColumnRef::new(
                    None,
                    column.name.clone(),
                    Some(column.data_type),
                ))
            })
            .collect();
        // Fresh rename projection wrapping the derived-table plan - there is no base
        // Projection to copy from. Field list (input/expressions/aliases/distinct/
        // distinct_on) is the complete Projection struct.
        Ok(LogicalPlan::Projection(Projection {
            input: Box::new(plan),
            expressions,
            aliases: names.to_vec(),
            distinct: false,
            distinct_on: None,
        }))
    }

    /// Bind both branches of a set operation and check their arity.
    fn bind_set_operation(&mut self, mut node: SetOperation) -> Result<LogicalPlan, BindError> {
        let left = self.bind(*node.left)?;
        let right = self.bind(*node.right)?;
        let (left_width, right_width) = (left.schema().len(), right.schema().len());
        if left_width != right_width {
            return Err(BindError::SetOpArity {
                left: left_width,
                right: right_width,
            });
        }
        // In-place on the owned node: only the two branches are rebound; `kind` and
        // `distinct` are preserved untouched.
        node.left = Box::new(left);
        node.right = Box::new(right);
        Ok(LogicalPlan::SetOperation(node))
    }

    /// Bind a CTE: register its name as a relation (over its body's output
    /// columns) so the child and later CTEs resolve a CteRef against it.
    fn bind_cte(&mut self, mut node: Cte) -> Result<LogicalPlan, BindError> {
        if node.recursive {
            return self.bind_recursive_cte(node);
        }
        let cte_plan = self.bind(*node.cte_plan)?;
        let mut columns = self.plan_output_columns(&cte_plan)?;
        if let Some(names) = &node.column_names {
            columns = rename_columns(names, &columns)?;
        }
        let table = Table::new(node.name.clone(), columns);
        let saved = self.ctes.insert(node.name.clone(), table);
        let child = self.bind(*node.child);
        match saved {
            Some(previous) => {
                self.ctes.insert(node.name.clone(), previous);
            }
            None => {
                self.ctes.remove(&node.name);
            }
        }
        let child = child?;
        // In-place on the owned node: only cte_plan and child are rebound; `name`,
        // `column_names`, and `recursive` (guarded to false above) are preserved.
        node.cte_plan = Box::new(cte_plan);
        node.child = Box::new(child);
        Ok(LogicalPlan::Cte(node))
    }

    /// Bind a recursive CTE. Its body is a UNION of an anchor branch (which does
    /// not name the CTE) and a recursive branch (which does). The anchor binds
    /// first and fixes the CTE's output columns/types; the CTE is then registered
    /// so the recursive branch and the child resolve their references against it.
    fn bind_recursive_cte(&mut self, mut node: Cte) -> Result<LogicalPlan, BindError> {
        let LogicalPlan::SetOperation(mut body) = *node.cte_plan else {
            return Err(BindError::Unsupported(
                "recursive CTE body must be a UNION of an anchor and a recursive branch"
                    .to_string(),
            ));
        };
        let anchor = self.bind(*body.left)?;
        let mut columns = self.plan_output_columns(&anchor)?;
        if let Some(names) = &node.column_names {
            columns = rename_columns(names, &columns)?;
        }
        let table = Table::new(node.name.clone(), columns);
        let saved = self.ctes.insert(node.name.clone(), table);
        let branch = self.bind(*body.right);
        let child = self.bind(*node.child);
        match saved {
            Some(previous) => {
                self.ctes.insert(node.name.clone(), previous);
            }
            None => {
                self.ctes.remove(&node.name);
            }
        }
        let branch = branch?;
        let child = child?;
        let (anchor_width, branch_width) = (anchor.schema().len(), branch.schema().len());
        if anchor_width != branch_width {
            return Err(BindError::SetOpArity {
                left: anchor_width,
                right: branch_width,
            });
        }
        // In-place on the owned set operation: only the two branches are rebound;
        // kind and distinct are preserved untouched.
        body.left = Box::new(anchor);
        body.right = Box::new(branch);
        // In-place on the owned Cte: cte_plan becomes the rebound UNION body and
        // child the rebound child; name, column_names, and recursive are preserved.
        node.cte_plan = Box::new(LogicalPlan::SetOperation(body));
        node.child = Box::new(child);
        Ok(LogicalPlan::Cte(node))
    }

    /// Resolve a CTE reference to the registered CTE's output column names.
    fn bind_cte_ref(&mut self, mut node: CteRef) -> Result<LogicalPlan, BindError> {
        let table = self
            .ctes
            .get(&node.name)
            .ok_or_else(|| BindError::TableNotFound(format!("CTE {}", node.name)))?;
        let output_names: Vec<String> = table.columns.iter().map(|col| col.name.clone()).collect();
        // In-place on the owned node: only output_names is resolved here; `name`,
        // `alias`, and `columns` are preserved untouched.
        node.output_names = Some(output_names);
        Ok(LogicalPlan::CteRef(node))
    }

    /// Bind a constant Values node (its expressions reference no input columns).
    fn bind_values(&mut self, mut node: Values) -> Result<LogicalPlan, BindError> {
        let mut rows = Vec::with_capacity(node.rows.len());
        for row in &node.rows {
            rows.push(self.bind_expr_list(row)?);
        }
        // In-place on the owned node: only the rows are rebound; `output_names` is
        // preserved untouched.
        node.rows = rows;
        Ok(LogicalPlan::Values(node))
    }

    /// Bind an EXPLAIN's wrapped plan.
    fn bind_explain(&mut self, mut node: Explain) -> Result<LogicalPlan, BindError> {
        let input = self.bind(*node.input)?;
        // In-place on the owned node: only the input is rebound; `format` is
        // preserved untouched.
        node.input = Box::new(input);
        Ok(LogicalPlan::Explain(node))
    }

    /// Derive output-column metadata (name + type) from a bound plan.
    fn plan_output_columns(&self, plan: &LogicalPlan) -> Result<Vec<Column>, BindError> {
        match plan {
            LogicalPlan::Projection(node) => expression_columns(&plan.schema(), &node.expressions),
            LogicalPlan::Aggregate(node) => {
                expression_columns(&node.output_names, &node.aggregates)
            }
            LogicalPlan::Values(node) => match node.rows.first() {
                Some(first) => expression_columns(&node.output_names, first),
                None => Ok(Vec::new()),
            },
            LogicalPlan::SetOperation(node) => self.plan_output_columns(&node.left),
            LogicalPlan::Scan(node) => self.scan_output_columns(node),
            LogicalPlan::Join(node) => {
                let mut columns = self.plan_output_columns(&node.left)?;
                if !matches!(
                    node.join_type,
                    fq_plan::JoinType::Semi | fq_plan::JoinType::Anti
                ) {
                    columns.extend(self.plan_output_columns(&node.right)?);
                }
                Ok(columns)
            }
            LogicalPlan::CteRef(node) => self.cte_ref_columns(node),
            LogicalPlan::SubqueryScan(node) => self.plan_output_columns(&node.input),
            other => match other.children().as_slice() {
                [child] => self.plan_output_columns(child),
                _ => Err(BindError::Unsupported(format!(
                    "output columns of {}",
                    node_name(other)
                ))),
            },
        }
    }

    /// Output columns of a scan, resolved from the catalog for its read-set.
    fn scan_output_columns(&self, scan: &Scan) -> Result<Vec<Column>, BindError> {
        let table = self.resolve_scan_table(scan)?;
        let mut columns = Vec::with_capacity(scan.columns.len());
        for name in &scan.columns {
            let column = table
                .get_column(name)
                .ok_or_else(|| BindError::ColumnNotInTable {
                    column: name.clone(),
                    table: scan.table_name.clone(),
                })?;
            columns.push(column.clone());
        }
        Ok(columns)
    }

    /// The registered output columns of a CTE reference.
    fn cte_ref_columns(&self, node: &CteRef) -> Result<Vec<Column>, BindError> {
        self.ctes
            .get(&node.name)
            .map(|table| table.columns.clone())
            .ok_or_else(|| BindError::TableNotFound(format!("CTE {}", node.name)))
    }

    /// Run `f` with the scope exposed by `plan` pushed, popping afterward.
    fn with_scope<T>(
        &mut self,
        plan: &LogicalPlan,
        f: impl FnOnce(&mut Self) -> Result<T, BindError>,
    ) -> Result<T, BindError> {
        let mut scope = Scope::new();
        self.collect_scope(plan, &mut scope)?;
        self.scopes.push(scope);
        let result = f(self);
        self.scopes.pop();
        result
    }

    /// Bind a subquery's plan with the enclosing RELATION scopes still on the
    /// stack, so a correlated reference resolves against an outer relation. The
    /// enclosing query's OUTPUT ALIASES (a HAVING/ORDER-BY overlay) are NOT
    /// visible inside a subquery, so they are cleared for the subplan and
    /// restored after - else a bare reference that is out of the subquery's scope
    /// but matches an outer SELECT alias would wrongly bind instead of raising.
    pub(crate) fn bind_subplan(&mut self, plan: &LogicalPlan) -> Result<LogicalPlan, BindError> {
        let saved_aliases = std::mem::take(&mut self.output_aliases);
        // Genuine clone: `bind` consumes its plan by value, but the subquery expr node
        // that owns this subplan is reached through a `&Expr` borrow (bind_expr binds
        // borrowed trees), so there is no owned subplan to move - we own a copy to bind.
        let result = self.bind(plan.clone());
        self.output_aliases = saved_aliases;
        result
    }

    /// Collect alias -> Table entries from a bound plan subtree.
    fn collect_scope(&self, plan: &LogicalPlan, scope: &mut Scope) -> Result<(), BindError> {
        match plan {
            LogicalPlan::Scan(scan) => {
                let table = self.resolve_scan_table(scan)?;
                let alias = scan
                    .alias
                    .clone()
                    .unwrap_or_else(|| scan.table_name.clone());
                scope.push((alias, table.clone()));
                Ok(())
            }
            LogicalPlan::SubqueryScan(node) => {
                // A derived table exposes its output columns under its alias, like
                // a base table; inner relation aliases stay hidden (SQL scoping).
                let columns = self.plan_output_columns(&node.input)?;
                scope.push((node.alias.clone(), Table::new(node.alias.clone(), columns)));
                Ok(())
            }
            LogicalPlan::CteRef(node) => {
                let table = self
                    .ctes
                    .get(&node.name)
                    .ok_or_else(|| BindError::TableNotFound(format!("CTE {}", node.name)))?;
                let alias = node.alias.clone().unwrap_or_else(|| node.name.clone());
                scope.push((alias, table.clone()));
                Ok(())
            }
            other => {
                for child in other.children() {
                    self.collect_scope(child, scope)?;
                }
                Ok(())
            }
        }
    }

    /// Resolve a column reference innermost-first across the scope chain - the
    /// single column resolver.
    pub(crate) fn resolve_in_scopes(&self, column: &fq_plan::ColumnRef) -> Result<Expr, BindError> {
        match &column.table {
            Some(qualifier) => self.resolve_qualified(qualifier, &column.column),
            None => self.resolve_unqualified(&column.column),
        }
    }

    /// Resolve a table-qualified reference to the nearest scope defining its table
    /// (case-insensitive alias match; the qualifier normalizes to the alias case).
    fn resolve_qualified(&self, qualifier: &str, column: &str) -> Result<Expr, BindError> {
        for scope in self.scopes.iter().rev() {
            let Some((alias, table)) = scope_entry(scope, qualifier) else {
                continue;
            };
            let Some(found) = table.get_column(column) else {
                return Err(BindError::ColumnNotInTable {
                    column: column.to_string(),
                    table: qualifier.to_string(),
                });
            };
            // The reference normalizes to the CATALOG's column case (matching is
            // case-insensitive): downstream planes address physical columns by
            // exact name, so a user-cased spelling must not survive binding.
            return Ok(Expr::Column(fq_plan::ColumnRef::new(
                Some(alias.clone()),
                found.name.clone(),
                Some(found.data_type),
            )));
        }
        Err(BindError::TableNotInScope {
            table: qualifier.to_string(),
            column: column.to_string(),
        })
    }

    /// Resolve a bare column name to the nearest scope that defines it.
    fn resolve_unqualified(&self, column: &str) -> Result<Expr, BindError> {
        for scope in self.scopes.iter().rev() {
            if let Some(resolved) = match_in_scope(scope, column)? {
                return Ok(resolved);
            }
        }
        Err(BindError::ColumnNotInScope(column.to_string()))
    }
}

/// The (alias, table) in `scope` whose alias matches `qualifier`
/// case-insensitively (exact match preferred); None if absent.
fn scope_entry<'s>(scope: &'s Scope, qualifier: &str) -> Option<(&'s String, &'s Table)> {
    if let Some((alias, table)) = scope.iter().find(|(alias, _)| alias == qualifier) {
        return Some((alias, table));
    }
    scope
        .iter()
        .find(|(alias, _)| alias.eq_ignore_ascii_case(qualifier))
        .map(|(alias, table)| (alias, table))
}

/// Find a bare column in one scope, qualifying it with the owning alias and type;
/// ambiguity across the scope's tables is an error.
fn match_in_scope(scope: &Scope, column: &str) -> Result<Option<Expr>, BindError> {
    let mut found: Option<Expr> = None;
    for (alias, table) in scope {
        if let Some(catalog_column) = table.get_column(column) {
            if found.is_some() {
                return Err(BindError::AmbiguousColumn(column.to_string()));
            }
            // The reference normalizes to the CATALOG's column case (matching is
            // case-insensitive): downstream planes address physical columns by
            // exact name, so a user-cased spelling must not survive binding.
            found = Some(Expr::Column(fq_plan::ColumnRef::new(
                Some(alias.clone()),
                catalog_column.name.clone(),
                Some(catalog_column.data_type),
            )));
        }
    }
    Ok(found)
}

/// Resolve a scan's read-set to real column names, expanding a star or an
/// attributes-nothing read-set to every column the table exposes. Kept names
/// normalize to the CATALOG's case (matching is case-insensitive), never the
/// user's spelling - downstream planes address physical columns by exact name.
fn scan_read_columns(columns: &[String], table: &Table) -> Vec<String> {
    if columns.iter().any(|name| name == "*") {
        return all_column_names(table);
    }
    let kept: Vec<String> = columns
        .iter()
        .filter_map(|name| table.get_column(name).map(|column| column.name.clone()))
        .collect();
    if kept.is_empty() {
        return all_column_names(table);
    }
    kept
}

/// Every column name the table exposes, in catalog order.
fn all_column_names(table: &Table) -> Vec<String> {
    table.columns.iter().map(|col| col.name.clone()).collect()
}

/// Fail loudly if a star survived into a bound scan's read-set (no shortcut
/// column lists - a bound scan must name real columns).
fn guard_no_star(table_name: &str, columns: &[String]) -> Result<(), BindError> {
    if columns.iter().any(|name| name == "*") {
        return Err(BindError::Unsupported(format!(
            "scan of '{table_name}' still has a '*' column after binding"
        )));
    }
    Ok(())
}

/// Map each SELECT output alias to its bound expression, EXCLUDING outputs that
/// are aggregate calls (grouping by an aggregate alias is invalid; excluding it
/// lets that case fall through to a scope-resolution error).
fn select_alias_map(names: &[String], expressions: &[Expr]) -> HashMap<String, Expr> {
    names
        .iter()
        .zip(expressions.iter())
        .filter(|(_, expr)| !fq_plan::contains_aggregate(expr))
        .map(|(name, expr)| (name.clone(), expr.clone()))
        .collect()
}

/// The output-alias overlay (name -> type) of an Aggregate, or of the Aggregate
/// under a HAVING Filter; None for any other plan.
fn aggregate_overlay(plan: &LogicalPlan) -> Option<HashMap<String, fq_common::DataType>> {
    let aggregate = match plan {
        LogicalPlan::Aggregate(node) => node,
        LogicalPlan::Filter(filter) => match filter.input.as_ref() {
            LogicalPlan::Aggregate(node) => node,
            _ => return None,
        },
        _ => return None,
    };
    Some(
        aggregate
            .output_names
            .iter()
            .zip(aggregate.aggregates.iter())
            .map(|(name, expr)| (name.clone(), expr.get_type()))
            .collect(),
    )
}

/// Bind a HAVING predicate that sits over an Aggregate. Ports the Python
/// `_hoist_aggregate_calls` machinery: every aggregate call in the (already-bound)
/// predicate becomes an aggregate-OUTPUT reference, because a post-aggregate filter
/// cannot recompute an aggregate over the grouped rows. A call structurally equal to
/// an existing output references that output; a call absent from the SELECT list is
/// appended as a hidden `__agg_N` output and a projection above restores the declared
/// schema (so the hidden output never widens the result).
fn hoist_having_over_aggregate(aggregate: Aggregate, predicate: Expr) -> LogicalPlan {
    let original_names = aggregate.output_names.clone();
    let original_aggregates = aggregate.aggregates.clone();
    let mut aggregates = aggregate.aggregates.clone();
    let mut names = aggregate.output_names.clone();
    let rewritten = hoist_aggregate_calls(predicate, &mut aggregates, &mut names);
    if names.len() == original_names.len() {
        // No hidden output was appended: every call matched an existing output.
        // Fresh Filter over the unchanged aggregate with the rewritten predicate -
        // a genuine new node (the original Filter was consumed); field list complete.
        return LogicalPlan::Filter(Filter {
            input: Box::new(LogicalPlan::Aggregate(aggregate)),
            predicate: rewritten,
        });
    }
    // Widen the aggregate with the hidden outputs the predicate now names, in place.
    let widened = update!(aggregate, { aggregates = aggregates, output_names = names });
    // Fresh Filter over the widened aggregate - a genuine new node (the original
    // Filter was consumed above); its two fields (input, predicate) are both listed.
    let filtered = LogicalPlan::Filter(Filter {
        input: Box::new(LogicalPlan::Aggregate(widened)),
        predicate: rewritten,
    });
    let mut restore = Vec::with_capacity(original_names.len());
    for (name, agg) in original_names.iter().zip(&original_aggregates) {
        restore.push(Expr::Column(ColumnRef::new(
            None,
            name.clone(),
            Some(agg.get_type()),
        )));
    }
    // Restore the aggregate's declared schema above the hoisted filter, dropping the
    // hidden HAVING outputs - a genuine new Projection; field list complete.
    LogicalPlan::Projection(Projection {
        input: Box::new(filtered),
        expressions: restore,
        aliases: original_names,
        distinct: false,
        distinct_on: None,
    })
}

/// Whether a bound ORDER BY key contains an aggregate-scoped call (an aggregate
/// function or `GROUPING()`). Such a key cannot be recomputed above the
/// aggregate - the raw inputs are gone - so it must be hoisted onto aggregate
/// OUTPUTS, exactly like a HAVING predicate.
fn needs_aggregate_hoist(expr: &Expr) -> bool {
    if let Expr::FunctionCall {
        is_aggregate,
        function_name,
        ..
    } = expr
    {
        if *is_aggregate || function_name.eq_ignore_ascii_case("grouping") {
            return true;
        }
    }
    expr.children()
        .iter()
        .any(|child| needs_aggregate_hoist(child))
}

/// Whether a bound Sort input is an Aggregate, directly or under its HAVING
/// filter - the shapes whose ORDER BY keys hoist onto aggregate outputs.
fn over_aggregate(plan: &LogicalPlan) -> bool {
    match plan {
        LogicalPlan::Aggregate(_) => true,
        LogicalPlan::Filter(filter) => {
            matches!(filter.input.as_ref(), LogicalPlan::Aggregate(_))
        }
        _ => false,
    }
}

/// Bind ORDER BY over an Aggregate (optionally under its HAVING filter) when a
/// key carries an aggregate call: each call becomes a reference to a (hidden)
/// aggregate output the sort reads by name - `ORDER BY count(*)` where the
/// SELECT already computes it references THAT output instead of recomputing
/// over grouped rows (where the raw inputs no longer exist). A call absent
/// from the SELECT list is appended as a hidden `__agg_N` output and a
/// projection above restores the declared schema, the same hoist HAVING uses.
/// Widening only APPENDS outputs, so a HAVING filter in between keeps
/// resolving its own references unchanged.
fn hoist_sort_over_aggregate(mut sort: Sort, bound_keys: Vec<Expr>) -> LogicalPlan {
    let (aggregate, having) = match *sort.input {
        LogicalPlan::Aggregate(aggregate) => (aggregate, None),
        LogicalPlan::Filter(filter) => match *filter.input {
            LogicalPlan::Aggregate(aggregate) => (aggregate, Some(filter.predicate)),
            other => {
                unreachable!("over_aggregate admitted a filter over a non-aggregate: {other:?}")
            }
        },
        other => unreachable!("over_aggregate admitted a non-aggregate sort input: {other:?}"),
    };
    let original_names = aggregate.output_names.clone();
    let original_aggregates = aggregate.aggregates.clone();
    let mut aggregates = aggregate.aggregates.clone();
    let mut names = aggregate.output_names.clone();
    let mut rewritten = Vec::with_capacity(bound_keys.len());
    for key in bound_keys {
        rewritten.push(hoist_aggregate_calls(key, &mut aggregates, &mut names));
    }
    let widened_count = names.len() - original_names.len();
    // Widen the aggregate with any hidden outputs the keys now name, in place.
    let widened = update!(aggregate, { aggregates = aggregates, output_names = names });
    let mut sorted_input = LogicalPlan::Aggregate(widened);
    if let Some(predicate) = having {
        // Fresh Filter restoring the HAVING over the (possibly widened) aggregate -
        // a genuine new node (the original was consumed above); both fields listed.
        sorted_input = LogicalPlan::Filter(Filter {
            input: Box::new(sorted_input),
            predicate,
        });
    }
    // In-place on the owned Sort: only input and sort_keys change; `ascending`
    // and `nulls_order` are preserved untouched.
    sort.input = Box::new(sorted_input);
    sort.sort_keys = rewritten;
    let sorted = LogicalPlan::Sort(sort);
    if widened_count == 0 {
        return sorted;
    }
    let mut restore = Vec::with_capacity(original_names.len());
    for (name, agg) in original_names.iter().zip(&original_aggregates) {
        restore.push(Expr::Column(ColumnRef::new(
            None,
            name.clone(),
            Some(agg.get_type()),
        )));
    }
    // Restore the aggregate's declared schema above the sort, dropping the
    // hidden ORDER BY outputs - a genuine new Projection; field list complete.
    LogicalPlan::Projection(Projection {
        input: Box::new(sorted),
        expressions: restore,
        aliases: original_names,
        distinct: false,
        distinct_on: None,
    })
}

/// Replace each aggregate-scoped call (an aggregate function or `GROUPING()`) in
/// `expr` with an aggregate-output reference, appending hidden outputs as needed.
fn hoist_aggregate_calls(expr: Expr, aggregates: &mut Vec<Expr>, names: &mut Vec<String>) -> Expr {
    if let Expr::FunctionCall {
        is_aggregate,
        function_name,
        ..
    } = &expr
    {
        if *is_aggregate || function_name.eq_ignore_ascii_case("grouping") {
            return aggregate_call_ref(expr, aggregates, names);
        }
    }
    expr.map_children(&mut |child| hoist_aggregate_calls(child, aggregates, names))
}

/// The aggregate-output reference for one HAVING aggregate call: the existing
/// output that already computes it (structural equality), else a hidden appended one.
fn aggregate_call_ref(call: Expr, aggregates: &mut Vec<Expr>, names: &mut Vec<String>) -> Expr {
    let call_type = call.get_type();
    for (index, existing) in aggregates.iter().enumerate() {
        if index < names.len() && *existing == call {
            return Expr::Column(ColumnRef::new(None, names[index].clone(), Some(call_type)));
        }
    }
    let name = format!("__agg_{}", names.len());
    aggregates.push(call);
    names.push(name.clone());
    Expr::Column(ColumnRef::new(None, name, Some(call_type)))
}

/// The output-alias overlay of a Projection (alias -> type); None otherwise.
fn projection_overlay(plan: &LogicalPlan) -> Option<HashMap<String, fq_common::DataType>> {
    let LogicalPlan::Projection(projection) = plan else {
        return None;
    };
    Some(
        projection
            .aliases
            .iter()
            .zip(projection.expressions.iter())
            .map(|(name, expr)| (name.clone(), expr.get_type()))
            .collect(),
    )
}

/// The ordered output names of a Projection/Aggregate (for positional ORDER BY),
/// or empty for any other plan.
fn output_name_list(plan: &LogicalPlan) -> Vec<String> {
    match plan {
        LogicalPlan::Projection(_) => plan.schema(),
        LogicalPlan::Aggregate(node) => node.output_names.clone(),
        LogicalPlan::Filter(filter) => match filter.input.as_ref() {
            LogicalPlan::Aggregate(node) => node.output_names.clone(),
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}

/// The 0-based output index a positional ORDER BY key (`ORDER BY n`, integer
/// literal 1..=count) names, or None. A positional ordinal orders by the n-th
/// output, not by the constant n.
fn positional_index(key: &Expr, count: usize) -> Option<usize> {
    if let Expr::Literal {
        value: fq_plan::LiteralValue::Integer(number),
        ..
    } = key
    {
        if *number >= 1 && (*number as usize) <= count {
            return Some((*number as usize) - 1);
        }
    }
    None
}

/// Bind one ORDER BY key of a Sort over a set operation: an ordinal names the
/// n-th output column, a bare name matches an output column case-insensitively.
/// Anything else (a qualified reference, an expression) raises - the branch
/// relations are out of scope after a UNION/INTERSECT/EXCEPT.
fn set_op_sort_key(key: &Expr, columns: &[Column]) -> Result<Expr, BindError> {
    if let Some(index) = positional_index(key, columns.len()) {
        let column = &columns[index];
        return Ok(Expr::Column(fq_plan::ColumnRef::new(
            None,
            column.name.clone(),
            Some(column.data_type),
        )));
    }
    let Expr::Column(reference) = key else {
        return Err(BindError::Unsupported(
            "ORDER BY after a set operation must name an output column or ordinal".to_string(),
        ));
    };
    if reference.table.is_some() {
        return Err(BindError::Unsupported(
            "ORDER BY after a set operation cannot use a qualified column".to_string(),
        ));
    }
    let found = columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(&reference.column));
    match found {
        Some(column) => Ok(Expr::Column(fq_plan::ColumnRef::new(
            None,
            column.name.clone(),
            Some(column.data_type),
        ))),
        None => Err(BindError::ColumnNotInScope(reference.column.clone())),
    }
}

/// Pair output names with their expressions' inferred types as Column metadata.
fn expression_columns(names: &[String], expressions: &[Expr]) -> Result<Vec<Column>, BindError> {
    if names.len() != expressions.len() {
        return Err(BindError::Unsupported(format!(
            "output arity mismatch: {} names, {} expressions",
            names.len(),
            expressions.len()
        )));
    }
    let mut columns = Vec::with_capacity(names.len());
    for (name, expression) in names.iter().zip(expressions.iter()) {
        columns.push(Column::new(name.clone(), expression.get_type(), true));
    }
    Ok(columns)
}

/// Re-label output columns with an explicit CTE/derived-table column list; a
/// count mismatch raises rather than silently dropping or overrunning columns.
fn rename_columns(names: &[String], columns: &[Column]) -> Result<Vec<Column>, BindError> {
    if names.len() != columns.len() {
        return Err(BindError::Unsupported(format!(
            "column list has {} names but the query returns {}",
            names.len(),
            columns.len()
        )));
    }
    Ok(names
        .iter()
        .zip(columns.iter())
        .map(|(name, column)| Column::new(name.clone(), column.data_type, true))
        .collect())
}

/// Reject a column-alias rename when the subquery has duplicate output names (the
/// rename reads each output by its physical name; a duplicate would alias wrong).
fn guard_unique_output_names(columns: &[Column]) -> Result<(), BindError> {
    let mut seen = std::collections::HashSet::new();
    for column in columns {
        if !seen.insert(&column.name) {
            return Err(BindError::Unsupported(format!(
                "derived table with a column-alias list has duplicate output name '{}'",
                column.name
            )));
        }
    }
    Ok(())
}

/// A short node-type name for error messages.
fn node_name(plan: &LogicalPlan) -> &'static str {
    match plan {
        LogicalPlan::Scan(_) => "Scan",
        LogicalPlan::Projection(_) => "Projection",
        LogicalPlan::Filter(_) => "Filter",
        LogicalPlan::Join(_) => "Join",
        LogicalPlan::Aggregate(_) => "Aggregate",
        LogicalPlan::Sort(_) => "Sort",
        LogicalPlan::Limit(_) => "Limit",
        LogicalPlan::Union(_) => "Union",
        LogicalPlan::SetOperation(_) => "SetOperation",
        LogicalPlan::Explain(_) => "Explain",
        LogicalPlan::Cte(_) => "Cte",
        LogicalPlan::CteRef(_) => "CteRef",
        LogicalPlan::Values(_) => "Values",
        LogicalPlan::SubqueryScan(_) => "SubqueryScan",
        LogicalPlan::SingleRowGuard(_) => "SingleRowGuard",
        LogicalPlan::GroupedLimit(_) => "GroupedLimit",
        LogicalPlan::LateralJoin(_) => "LateralJoin",
    }
}
