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
//! base tables, derived tables (SubqueryScan), CTE references + non-recursive
//! WITH, set operations (with arity check), Values, Explain, the subquery
//! expressions (correlated - the subplan binds with the enclosing scopes
//! visible), and HAVING / ORDER-BY output-alias + positional-ordinal resolution.
//! Deferred (correctness refinements, do not block binding): the aggregate-call
//! HOIST/widening for ORDER BY / HAVING calls absent from the SELECT list, and
//! WITH RECURSIVE.

use std::collections::HashMap;

use fq_catalog::{Catalog, Column, Table};
use fq_plan::{
    Aggregate, Cte, CteRef, Explain, Expr, Filter, Join, Limit, LogicalPlan, Projection, Scan,
    SetOperation, Sort, SubqueryScan, Values,
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

    /// Bind a scan: resolve its table and prune its read-set to real column names
    /// (expanding a star), so a bound scan never carries a `*`.
    fn bind_scan(&self, mut scan: Scan) -> Result<Scan, BindError> {
        let table = self.resolve_scan_table(&scan)?;
        let kept = scan_read_columns(&scan.columns, table);
        guard_no_star(&scan.table_name, &kept)?;
        scan.columns = kept;
        Ok(scan)
    }

    /// The catalog `Table` a scan reads (searching when the reference is bare).
    fn resolve_scan_table(&self, scan: &Scan) -> Result<&'a Table, BindError> {
        self.catalog
            .resolve_table(
                optional(&scan.datasource),
                optional(&scan.schema_name),
                &scan.table_name,
            )
            .ok_or_else(|| {
                BindError::TableNotFound(format!(
                    "{}.{}.{}",
                    scan.datasource, scan.schema_name, scan.table_name
                ))
            })
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
    fn bind_sort(&mut self, mut sort: Sort) -> Result<LogicalPlan, BindError> {
        let input = self.bind(*sort.input)?;
        let output_names = output_name_list(&input);
        let overlay = aggregate_overlay(&input).or_else(|| projection_overlay(&input));
        let sort_keys = self.with_output_aliases(overlay, |binder| {
            binder.with_scope(&input, |binder| {
                binder.bind_sort_keys(&sort.sort_keys, &output_names)
            })
        })?;
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
        // In-place on the owned node: overwrite only the rebound input/aggregates/
        // group_by; `output_names` and `grouping_sets` are preserved untouched.
        aggregate.input = Box::new(input);
        aggregate.aggregates = aggregates;
        aggregate.group_by = group_by;
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
            return Err(BindError::Unsupported("WITH RECURSIVE".to_string()));
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
            return Ok(Expr::Column(fq_plan::ColumnRef::new(
                Some(alias.clone()),
                column,
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
            found = Some(Expr::Column(fq_plan::ColumnRef::new(
                Some(alias.clone()),
                column,
                Some(catalog_column.data_type),
            )));
        }
    }
    Ok(found)
}

/// Resolve a scan's read-set to real column names, expanding a star or an
/// attributes-nothing read-set to every column the table exposes.
fn scan_read_columns(columns: &[String], table: &Table) -> Vec<String> {
    if columns.iter().any(|name| name == "*") {
        return all_column_names(table);
    }
    let kept: Vec<String> = columns
        .iter()
        .filter(|name| table.get_column(name).is_some())
        .cloned()
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
