//! The `Binder`: resolve names/types in a `LogicalPlan` against the catalog.
//!
//! Ports `parser/binder.py`. Each relational node binds its input, pushes the
//! relation scope that input exposes (alias -> Table), binds its own expressions
//! against that scope, and pops. The one column resolver is `resolve_in_scopes`
//! (qualified -> nearest scope defining the table; bare -> nearest defining the
//! column; ambiguity/no-scope raises). Post-bind every `ColumnRef` carries its
//! relation qualifier and `DataType`.
//!
//! Coverage (this stage): base-table queries - Scan / Projection / Filter / Join
//! / Sort / Limit / Aggregate. Derived tables, CTEs, set operations, subquery
//! expressions, and the HAVING/ORDER-BY-over-aggregate hoists are the next
//! increments (each raises `BindError::Unsupported` until then).

use fq_catalog::{Catalog, Table};
use fq_plan::{Aggregate, Expr, Filter, Join, Limit, LogicalPlan, Projection, Scan, Sort};

use crate::error::BindError;

/// One relation scope: alias (or table name) -> the table it exposes.
type Scope = Vec<(String, Table)>;

/// The name-resolution pass over one query's logical plan.
pub struct Binder<'a> {
    catalog: &'a Catalog,
    scopes: Vec<Scope>,
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
    fn bind_filter(&mut self, filter: Filter) -> Result<LogicalPlan, BindError> {
        let input = self.bind(*filter.input)?;
        let predicate = self.with_scope(&input, |binder| binder.bind_expr(&filter.predicate))?;
        Ok(LogicalPlan::Filter(Filter {
            input: Box::new(input),
            predicate,
        }))
    }

    /// Bind a Projection: bind the input, then each SELECT expression.
    fn bind_projection(&mut self, projection: Projection) -> Result<LogicalPlan, BindError> {
        let input = self.bind(*projection.input)?;
        let expressions = self.with_scope(&input, |binder| {
            binder.bind_expr_list(&projection.expressions)
        })?;
        let distinct_on = match &projection.distinct_on {
            Some(keys) => Some(self.with_scope(&input, |binder| binder.bind_expr_list(keys))?),
            None => None,
        };
        Ok(LogicalPlan::Projection(Projection {
            input: Box::new(input),
            expressions,
            aliases: projection.aliases,
            distinct: projection.distinct,
            distinct_on,
        }))
    }

    /// Bind a Sort: bind the input, then each sort key against the input scope.
    fn bind_sort(&mut self, sort: Sort) -> Result<LogicalPlan, BindError> {
        let input = self.bind(*sort.input)?;
        let sort_keys = self.with_scope(&input, |binder| binder.bind_expr_list(&sort.sort_keys))?;
        Ok(LogicalPlan::Sort(Sort {
            input: Box::new(input),
            sort_keys,
            ascending: sort.ascending,
            nulls_order: sort.nulls_order,
        }))
    }

    /// Bind a Limit: only the input needs binding (LIMIT/OFFSET are constants).
    fn bind_limit(&mut self, limit: Limit) -> Result<LogicalPlan, BindError> {
        let input = self.bind(*limit.input)?;
        Ok(LogicalPlan::Limit(Limit {
            input: Box::new(input),
            limit: limit.limit,
            offset: limit.offset,
        }))
    }

    /// Bind a Join: bind both sides, then the ON condition against both scopes.
    fn bind_join(&mut self, join: Join) -> Result<LogicalPlan, BindError> {
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
        Ok(LogicalPlan::Join(Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: join.join_type,
            condition,
            natural: join.natural,
            using: join.using,
            estimated_rows: join.estimated_rows,
            estimate_defaults: join.estimate_defaults,
        }))
    }

    /// Bind an Aggregate: bind the input, then the GROUP BY keys and aggregate
    /// expressions against the input scope.
    fn bind_aggregate(&mut self, aggregate: Aggregate) -> Result<LogicalPlan, BindError> {
        let input = self.bind(*aggregate.input)?;
        let group_by =
            self.with_scope(&input, |binder| binder.bind_expr_list(&aggregate.group_by))?;
        let aggregates = self.with_scope(&input, |binder| {
            binder.bind_expr_list(&aggregate.aggregates)
        })?;
        Ok(LogicalPlan::Aggregate(Aggregate {
            input: Box::new(input),
            group_by,
            aggregates,
            output_names: aggregate.output_names,
            grouping_sets: aggregate.grouping_sets,
        }))
    }

    /// Run `f` with the scope exposed by `plan` pushed, popping afterward.
    fn with_scope<T>(
        &mut self,
        plan: &LogicalPlan,
        f: impl FnOnce(&Self) -> Result<T, BindError>,
    ) -> Result<T, BindError> {
        let mut scope = Scope::new();
        self.collect_scope(plan, &mut scope)?;
        self.scopes.push(scope);
        let result = f(self);
        self.scopes.pop();
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
            LogicalPlan::SubqueryScan(_) | LogicalPlan::CteRef(_) => Err(BindError::Unsupported(
                "derived table / CTE reference in scope".to_string(),
            )),
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
