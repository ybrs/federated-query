//! The `Converter`: polyglot-sql AST -> `fq_plan::LogicalPlan`.
//!
//! Holds the catalog (for star expansion) and owns the recursive descent: a
//! query converts to a plan, its expressions convert to `Expr` (see `expr.rs`),
//! and a subquery-bearing expression recurses back into `query`. Builds a SELECT
//! clause by clause in the Python order: FROM -> WHERE -> GROUP BY -> HAVING ->
//! SELECT -> ORDER BY -> LIMIT.
//!
//! Coverage: base tables + left-deep JOINs (explicit and comma/implicit) +
//! derived tables + CTE references in FROM, non-recursive WITH, catalog-driven
//! star expansion, WHERE, GROUP BY + the aggregates, HAVING, ORDER BY,
//! LIMIT/OFFSET, DISTINCT, VALUES, and the binary set operations. A window
//! function in WHERE / GROUP BY / HAVING raises (it is legal only in SELECT and
//! ORDER BY). WITH RECURSIVE still raises.

use std::cell::RefCell;
use std::collections::HashMap;

use fq_catalog::Catalog;
use fq_plan::expr::NullsOrder;
use fq_plan::{
    Aggregate, ColumnRef, Cte, CteRef, Expr, Filter, Join, JoinType, LateralJoin, Limit,
    LogicalPlan, Projection, Scan, SetOpKind, SetOperation, Sort, SubqueryScan, Values,
};
use polyglot_sql::expressions::{Expression, JoinKind, Ordered, Select, TableRef, With};
use polyglot_sql::traversal::ExpressionWalk;

use crate::error::ParseError;

/// The relation alias given to the subquery a QUALIFY clause wraps its input in,
/// so the QUALIFY predicate's columns qualify to a real relation after binding.
const QUALIFY_SUBQUERY_ALIAS: &str = "__qualify";

/// Where a FROM item's columns come from, for star expansion.
enum ColumnSource {
    /// A base table: resolve the column list from the catalog.
    Catalog {
        datasource: Option<String>,
        schema: Option<String>,
        table: String,
    },
    /// A derived table / VALUES: the column list is already known.
    Explicit(Vec<String>),
}

/// One FROM/JOIN item, kept for star expansion: the qualifier a reference uses
/// (`key` = alias or table name) and where its columns come from.
struct FromTable {
    key: String,
    columns: ColumnSource,
}

/// The recursive AST -> logical-plan converter.
pub struct Converter<'a> {
    catalog: &'a Catalog,
    /// In-scope CTE names -> their output column lists. A bare FROM reference to
    /// one becomes a `CteRef`; star expansion over it uses the recorded columns.
    /// Interior mutability so the `&self` descent can push/pop a WITH's names.
    ctes: RefCell<HashMap<String, Vec<String>>>,
}

impl<'a> Converter<'a> {
    /// A converter over `catalog` (used for star expansion).
    pub fn new(catalog: &'a Catalog) -> Self {
        Self {
            catalog,
            ctes: RefCell::new(HashMap::new()),
        }
    }

    /// Convert a query expression (SELECT, a set operation, a parenthesized
    /// subquery, or VALUES) into a logical plan.
    pub(crate) fn query(&self, expr: &Expression) -> Result<LogicalPlan, ParseError> {
        match expr {
            Expression::Select(select) => self.select(select),
            Expression::Union(union) => {
                self.set_operation(&union.left, &union.right, SetOpKind::Union, !union.all)
            }
            Expression::Intersect(intersect) => self.set_operation(
                &intersect.left,
                &intersect.right,
                SetOpKind::Intersect,
                !intersect.all,
            ),
            Expression::Except(except) => {
                self.set_operation(&except.left, &except.right, SetOpKind::Except, !except.all)
            }
            Expression::Subquery(subquery) => self.query(&subquery.this),
            Expression::Values(values) => self.values(values),
            other => Err(ParseError::Unsupported(format!(
                "query `{}`",
                other.variant_name()
            ))),
        }
    }

    /// Convert a binary set operation. `distinct` is the bare form; ALL preserves
    /// multiplicity.
    fn set_operation(
        &self,
        left: &Expression,
        right: &Expression,
        kind: SetOpKind,
        distinct: bool,
    ) -> Result<LogicalPlan, ParseError> {
        // Fresh set-operation node built from the parsed UNION/INTERSECT/EXCEPT - no
        // base to copy from. Field list (left/right/kind/distinct) is the complete
        // SetOperation struct.
        Ok(LogicalPlan::SetOperation(SetOperation {
            left: Box::new(self.query(left)?),
            right: Box::new(self.query(right)?),
            kind,
            distinct,
        }))
    }

    /// Convert a VALUES clause into a `Values` node of constant rows.
    fn values(
        &self,
        values: &polyglot_sql::expressions::Values,
    ) -> Result<LogicalPlan, ParseError> {
        let mut rows = Vec::with_capacity(values.expressions.len());
        for tuple in &values.expressions {
            let mut row = Vec::with_capacity(tuple.expressions.len());
            for cell in &tuple.expressions {
                row.push(self.expr(cell)?);
            }
            rows.push(row);
        }
        let width = rows.first().map_or(0, Vec::len);
        let output_names = if values.column_aliases.is_empty() {
            (0..width)
                .map(|index| format!("column{}", index + 1))
                .collect()
        } else {
            values
                .column_aliases
                .iter()
                .map(|ident| ident.name.clone())
                .collect()
        };
        // Fresh VALUES node built from the parsed constant rows - no base to copy
        // from. Field list (rows/output_names) is the complete Values struct.
        Ok(LogicalPlan::Values(Values { rows, output_names }))
    }

    /// Convert a SELECT into a logical plan, handling a leading WITH first.
    fn select(&self, select: &Select) -> Result<LogicalPlan, ParseError> {
        reject_unsupported_clauses(select)?;
        match &select.with {
            Some(with) => self.select_with(with, select),
            None => self.select_body(select),
        }
    }

    /// The SELECT clause pipeline (FROM -> WHERE -> projection -> ORDER -> LIMIT),
    /// with any WITH already bound into scope.
    fn select_body(&self, select: &Select) -> Result<LogicalPlan, ParseError> {
        let (from, from_tables) = self.build_from(select)?;
        let filtered = self.apply_where(from, select)?;
        let projected = self.apply_projection(filtered, select, &from_tables)?;
        let qualified = self.apply_qualify(projected, select)?;
        let sorted = self.apply_order_by(qualified, select)?;
        apply_limit(sorted, select)
    }

    /// Bind a WITH's CTEs into scope, build the body, and wrap it in nested `Cte`
    /// nodes (first CTE outermost). CTE names are pushed before any body is built
    /// so a CTE may reference an earlier one; they are popped afterward.
    fn select_with(&self, with: &With, select: &Select) -> Result<LogicalPlan, ParseError> {
        if with.recursive {
            return Err(ParseError::Unsupported("WITH RECURSIVE".to_string()));
        }
        let mut definitions = Vec::with_capacity(with.ctes.len());
        for cte in &with.ctes {
            let name = cte.alias.name.clone();
            let body = self.query(&cte.this)?;
            let columns: Vec<String> = if cte.columns.is_empty() {
                body.schema()
            } else {
                cte.columns.iter().map(|ident| ident.name.clone()).collect()
            };
            self.ctes.borrow_mut().insert(name.clone(), columns.clone());
            definitions.push((name, body, cte.columns.is_empty(), columns));
        }
        let mut plan = self.select_body(select)?;
        // Consume the definitions in reverse (first CTE outermost) so each body plan
        // is MOVED into its Cte node - no clone of a subtree. The scope entry is
        // dropped here too: every body plan is already built, so it is no longer read.
        for (name, body, inferred, columns) in definitions.into_iter().rev() {
            self.ctes.borrow_mut().remove(&name);
            // Fresh CTE wrapper binding one WITH definition over the plan below it,
            // built from the parsed WITH - no base to copy from. Field list
            // (name/cte_plan/child/recursive/column_names) is the complete Cte struct.
            plan = LogicalPlan::Cte(Cte {
                name,
                cte_plan: Box::new(body),
                child: Box::new(plan),
                recursive: false,
                column_names: if inferred { None } else { Some(columns) },
            });
        }
        Ok(plan)
    }

    /// Build the FROM tree (base table then each JOIN folded left-deep) and the
    /// per-item column sources used for star expansion.
    fn build_from(&self, select: &Select) -> Result<(LogicalPlan, Vec<FromTable>), ParseError> {
        let from = select
            .from
            .as_ref()
            .ok_or_else(|| ParseError::Unsupported("FROM-less SELECT".to_string()))?;
        let (first, rest) = from
            .expressions
            .split_first()
            .ok_or_else(|| ParseError::Unsupported("FROM-less SELECT".to_string()))?;
        let mut tables = Vec::new();
        let mut plan = self.convert_from_item(select, first, &mut tables)?;
        // A comma-separated FROM list is an implicit CROSS JOIN of its items; fold
        // each additional item left-deep. The equi-predicates that make it a real
        // inner join live in WHERE, and the optimizer's join ordering recovers the
        // join graph from the CROSS+filter region (so no join is missed here).
        for item in rest {
            plan = self.fold_comma_item(select, plan, item, &mut tables)?;
        }
        for join in &select.joins {
            plan = self.apply_join(select, plan, join, &mut tables)?;
        }
        Ok((plan, tables))
    }

    /// Fold one comma-separated FROM item onto the left-deep plan. A
    /// `LATERAL (subquery) alias` item is a dependent join whose right may
    /// reference the left, so it becomes an INNER `LateralJoin`; any other item
    /// is an implicit CROSS join.
    fn fold_comma_item(
        &self,
        select: &Select,
        left: LogicalPlan,
        item: &Expression,
        tables: &mut Vec<FromTable>,
    ) -> Result<LogicalPlan, ParseError> {
        if let Expression::Subquery(subquery) = item {
            if subquery.lateral {
                let right = self.derived_table(subquery, tables)?;
                return Ok(lateral_join(left, right, JoinType::Inner));
            }
        }
        let right = self.convert_from_item(select, item, tables)?;
        // Fresh CROSS join folding the next comma-FROM item onto the left-deep
        // plan - no base join to copy from. Field list is the complete Join struct
        // (no condition/estimates yet; the optimizer stamps those later).
        Ok(LogicalPlan::Join(Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinType::Cross,
            condition: None,
            natural: false,
            using: None,
            estimated_rows: None,
            estimate_defaults: None,
        }))
    }

    /// Convert one FROM/JOIN item (base table or derived table), recording its
    /// column source in `tables`.
    fn convert_from_item(
        &self,
        select: &Select,
        item: &Expression,
        tables: &mut Vec<FromTable>,
    ) -> Result<LogicalPlan, ParseError> {
        match item {
            Expression::Table(table) => Ok(self.table_or_cte_ref(select, table, tables)),
            Expression::Subquery(subquery) => self.derived_table(subquery, tables),
            other => Err(ParseError::Unsupported(format!(
                "FROM/JOIN {}",
                other.variant_name()
            ))),
        }
    }

    /// A bare table reference that names an in-scope CTE becomes a `CteRef`;
    /// otherwise it is a base-table `Scan`.
    fn table_or_cte_ref(
        &self,
        select: &Select,
        table: &TableRef,
        tables: &mut Vec<FromTable>,
    ) -> LogicalPlan {
        let key = table_key(table);
        let unqualified = table.catalog.is_none() && table.schema.is_none();
        if unqualified {
            if let Some(columns) = self.ctes.borrow().get(&table.name.name).cloned() {
                tables.push(FromTable {
                    key: key.clone(),
                    columns: ColumnSource::Explicit(columns.clone()),
                });
                // Fresh CteRef built from the parsed table reference to an in-scope
                // CTE - no base to copy from. Field list (name/alias/columns/
                // output_names) is the complete CteRef struct.
                return LogicalPlan::CteRef(CteRef {
                    name: table.name.name.clone(),
                    alias: table.alias.as_ref().map(|ident| ident.name.clone()),
                    columns: Some(columns_for_table(select, &key)),
                    output_names: Some(columns),
                });
            }
        }
        tables.push(FromTable {
            key: key.clone(),
            columns: ColumnSource::Catalog {
                datasource: table.catalog.as_ref().map(|ident| ident.name.clone()),
                schema: table.schema.as_ref().map(|ident| ident.name.clone()),
                table: table.name.name.clone(),
            },
        });
        LogicalPlan::Scan(Box::new(self.scan_from_table(select, table, &key)))
    }

    /// A derived table `(SELECT ...) AS alias`: convert the inner query and wrap
    /// it under its alias. Records the subplan's output columns for star
    /// expansion.
    fn derived_table(
        &self,
        subquery: &polyglot_sql::expressions::Subquery,
        tables: &mut Vec<FromTable>,
    ) -> Result<LogicalPlan, ParseError> {
        let alias = subquery
            .alias
            .as_ref()
            .map(|ident| ident.name.clone())
            .ok_or_else(|| ParseError::Unsupported("derived table without alias".to_string()))?;
        let input = self.query(&subquery.this)?;
        let column_names: Vec<String> = if subquery.column_aliases.is_empty() {
            input.schema()
        } else {
            subquery
                .column_aliases
                .iter()
                .map(|ident| ident.name.clone())
                .collect()
        };
        tables.push(FromTable {
            key: alias.clone(),
            columns: ColumnSource::Explicit(column_names.clone()),
        });
        let rename = if subquery.column_aliases.is_empty() {
            None
        } else {
            Some(column_names)
        };
        // Fresh boundary wrapper for a derived table `(SELECT ...) AS alias`, built
        // from the parsed subquery - no base to copy from. Field list
        // (input/alias/column_names) is the complete SubqueryScan struct.
        Ok(LogicalPlan::SubqueryScan(SubqueryScan {
            input: Box::new(input),
            alias,
            column_names: rename,
        }))
    }

    /// Fold one JOIN onto the left plan.
    fn apply_join(
        &self,
        select: &Select,
        left: LogicalPlan,
        join: &polyglot_sql::expressions::Join,
        tables: &mut Vec<FromTable>,
    ) -> Result<LogicalPlan, ParseError> {
        // Reject join modifiers the engine does not model rather than dropping
        // them silently (a trailing PIVOT / ASOF match condition changes results).
        if !join.pivots.is_empty() || join.match_condition.is_some() || join.join_hint.is_some() {
            return Err(ParseError::Unsupported("join modifier".to_string()));
        }
        if let Expression::Subquery(subquery) = &join.this {
            if subquery.lateral {
                return self.apply_lateral_join(left, join, subquery, tables);
            }
        }
        let right = self.convert_from_item(select, &join.this, tables)?;
        let (join_type, natural) = map_join_kind(join.kind)?;
        let condition = match &join.on {
            Some(on) => Some(self.expr(on)?),
            None => None,
        };
        let using = if join.using.is_empty() {
            None
        } else {
            Some(join.using.iter().map(|ident| ident.name.clone()).collect())
        };
        // Fresh join built from the parsed JOIN clause (kind + ON/USING) - no base
        // join to copy from. Field list is the complete Join struct (no estimates
        // yet; the optimizer stamps those later).
        Ok(LogicalPlan::Join(Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type,
            condition,
            natural,
            using,
            estimated_rows: None,
            estimate_defaults: None,
        }))
    }

    /// Fold an explicit `JOIN LATERAL (subquery) alias ON ...` onto the left
    /// plan as a `LateralJoin`. The right (a derived table) may reference the
    /// left. `LEFT JOIN LATERAL` keeps non-matching left rows (join type LEFT);
    /// plain/INNER/CROSS `JOIN LATERAL` is INNER. `LateralJoin` carries no ON
    /// predicate, so the ON must be absent or the literal `TRUE`; any other ON
    /// or a USING list cannot be represented and raises.
    fn apply_lateral_join(
        &self,
        left: LogicalPlan,
        join: &polyglot_sql::expressions::Join,
        subquery: &polyglot_sql::expressions::Subquery,
        tables: &mut Vec<FromTable>,
    ) -> Result<LogicalPlan, ParseError> {
        let join_type = lateral_join_type(join.kind)?;
        require_trivial_lateral_on(join)?;
        let right = self.derived_table(subquery, tables)?;
        Ok(lateral_join(left, right, join_type))
    }

    /// Wrap the input in a `Filter` for the WHERE clause, if present.
    fn apply_where(&self, input: LogicalPlan, select: &Select) -> Result<LogicalPlan, ParseError> {
        let Some(where_clause) = &select.where_clause else {
            return Ok(input);
        };
        reject_window_in_clause(&where_clause.this, "WHERE")?;
        // Fresh WHERE filter wrapping the FROM plan, built from the parsed predicate
        // - no base to copy from. Field list (input/predicate) is the complete Filter.
        Ok(LogicalPlan::Filter(Filter {
            input: Box::new(input),
            predicate: self.expr(&where_clause.this)?,
        }))
    }

    /// Realize the SELECT list: an `Aggregate` when GROUP BY is present or any
    /// output is an aggregate, else a `Projection` (carrying DISTINCT). HAVING
    /// wraps the aggregate. Stars are expanded first from the catalog.
    fn apply_projection(
        &self,
        input: LogicalPlan,
        select: &Select,
        from_tables: &[FromTable],
    ) -> Result<LogicalPlan, ParseError> {
        let mut expressions = Vec::new();
        let mut names = Vec::new();
        for (index, item) in select.expressions.iter().enumerate() {
            for (expr, name) in self.select_item(item, index, from_tables)? {
                expressions.push(expr);
                names.push(name);
            }
        }
        if select.distinct && expressions.iter().any(fq_plan::expr::contains_window) {
            // A windowed aggregate routes through the aggregate path, which ignores
            // DISTINCT; a ranking window would carry DISTINCT into a projection that
            // drops post-window duplicates. Either way DISTINCT would be silently
            // lost, so refuse the combination rather than return duplicate rows.
            return Err(ParseError::Unsupported(
                "SELECT DISTINCT with window functions".to_string(),
            ));
        }
        if select.group_by.is_some() || expressions.iter().any(fq_plan::contains_aggregate) {
            if select.distinct {
                return Err(ParseError::Unsupported(
                    "DISTINCT with GROUP BY".to_string(),
                ));
            }
            return self.build_aggregate(input, select, expressions, names);
        }
        // Fresh projection realizing the SELECT list over the input, built from the
        // parsed items - no base to copy from. Field list (input/expressions/aliases/
        // distinct/distinct_on) is the complete Projection struct.
        Ok(LogicalPlan::Projection(Projection {
            input: Box::new(input),
            expressions,
            aliases: names,
            distinct: select.distinct,
            distinct_on: self.distinct_on(select)?,
        }))
    }

    /// Convert a DISTINCT ON key list, if present.
    fn distinct_on(&self, select: &Select) -> Result<Option<Vec<Expr>>, ParseError> {
        let Some(keys) = &select.distinct_on else {
            return Ok(None);
        };
        let mut converted = Vec::with_capacity(keys.len());
        for key in keys {
            converted.push(self.expr(key)?);
        }
        Ok(Some(converted))
    }

    /// Expand one ROLLUP/CUBE/GROUPING SETS construct into the aggregate's
    /// explicit `grouping_sets`, with any leading normal GROUP BY keys prepended
    /// to every set. The returned `group_by` is the distinct union of keys
    /// across the sets (order-preserving): every pass downstream that reasons
    /// about grouping columns reads it, while the engine's aggregate fragment
    /// renders the sets themselves as `GROUPING SETS (...)`.
    fn expand_grouping(
        &self,
        normal_keys: &[Expr],
        construct: &Expression,
    ) -> Result<(Vec<Expr>, GroupingSets), ParseError> {
        // Polyglot's Postgres parser emits ROLLUP/GROUPING SETS as dedicated
        // variants but CUBE (and sometimes ROLLUP) as a generic Function call
        // named after the construct - both shapes convert here.
        let expanded = match construct {
            Expression::Rollup(rollup) => rollup_sets(&self.expr_list(&rollup.expressions)?),
            Expression::Cube(cube) => cube_sets(&self.expr_list(&cube.expressions)?),
            Expression::GroupingSets(sets) => self.explicit_grouping_sets(&sets.expressions)?,
            Expression::Function(function) if function.name.eq_ignore_ascii_case("rollup") => {
                rollup_sets(&self.expr_list(&function.args)?)
            }
            Expression::Function(function) if function.name.eq_ignore_ascii_case("cube") => {
                cube_sets(&self.expr_list(&function.args)?)
            }
            Expression::Function(function)
                if function.name.eq_ignore_ascii_case("grouping sets") =>
            {
                self.explicit_grouping_sets(&function.args)?
            }
            other => {
                return Err(ParseError::Unsupported(format!(
                    "grouping construct {}",
                    other.variant_name()
                )))
            }
        };
        let mut grouping_sets = Vec::with_capacity(expanded.len());
        for set in expanded {
            let mut keys = normal_keys.to_vec();
            keys.extend(set);
            grouping_sets.push(keys);
        }
        let mut group_by = Vec::new();
        for set in &grouping_sets {
            for key in set {
                if !group_by.contains(key) {
                    group_by.push(key.clone());
                }
            }
        }
        Ok((group_by, Some(grouping_sets)))
    }

    /// Convert a slice of polyglot expressions (grouping-construct keys).
    fn expr_list(&self, keys: &[Expression]) -> Result<Vec<Expr>, ParseError> {
        let mut converted = Vec::with_capacity(keys.len());
        for key in keys {
            converted.push(self.expr(key)?);
        }
        Ok(converted)
    }

    /// Convert explicit `GROUPING SETS ((a, b), (a), ())` elements: a tuple
    /// contributes its members, a parenthesized key one member, a bare key one.
    fn explicit_grouping_sets(
        &self,
        elements: &[Expression],
    ) -> Result<Vec<Vec<Expr>>, ParseError> {
        let mut sets = Vec::with_capacity(elements.len());
        for element in elements {
            match element {
                Expression::Tuple(tuple) => sets.push(self.expr_list(&tuple.expressions)?),
                Expression::Paren(paren) => sets.push(vec![self.expr(&paren.this)?]),
                other => sets.push(vec![self.expr(other)?]),
            }
        }
        Ok(sets)
    }

    /// Build the `Aggregate` node (+ a HAVING `Filter` when present). The
    /// aggregate's `aggregates` list is the whole SELECT list, positionally
    /// aligned with `output_names`.
    fn build_aggregate(
        &self,
        input: LogicalPlan,
        select: &Select,
        expressions: Vec<Expr>,
        names: Vec<String>,
    ) -> Result<LogicalPlan, ParseError> {
        let mut normal_keys = Vec::new();
        let mut constructs = Vec::new();
        if let Some(clause) = &select.group_by {
            if clause.totals {
                return Err(ParseError::Unsupported(
                    "GROUP BY ... WITH TOTALS".to_string(),
                ));
            }
            for key in &clause.expressions {
                if is_grouping_construct(key) {
                    constructs.push(key);
                } else {
                    reject_window_in_clause(key, "GROUP BY")?;
                    normal_keys.push(self.expr(key)?);
                }
            }
        }
        if constructs.len() > 1 {
            return Err(ParseError::Unsupported(
                "combining multiple ROLLUP/CUBE/GROUPING SETS".to_string(),
            ));
        }
        let (group_by, grouping_sets) = match constructs.pop() {
            Some(construct) => self.expand_grouping(&normal_keys, construct)?,
            None => (normal_keys, None),
        };
        // Fresh aggregate realizing GROUP BY + the aggregate SELECT list, built from
        // the parsed clauses - no base to copy from. Field list (input/group_by/
        // aggregates/output_names/grouping_sets) is the complete Aggregate struct.
        let aggregate = LogicalPlan::Aggregate(Aggregate {
            input: Box::new(input),
            group_by,
            aggregates: expressions,
            output_names: names,
            grouping_sets,
        });
        let Some(having) = &select.having else {
            return Ok(aggregate);
        };
        reject_window_in_clause(&having.this, "HAVING")?;
        // Fresh HAVING filter wrapping the aggregate, built from the parsed predicate
        // - no base to copy from. Field list (input/predicate) is the complete Filter.
        Ok(LogicalPlan::Filter(Filter {
            input: Box::new(aggregate),
            predicate: self.expr(&having.this)?,
        }))
    }

    /// One SELECT-list item expanded to `(expression, output_name)` pairs. A star
    /// expands from the catalog; an alias names the output; a bare column takes
    /// its own name; anything else gets a positional name.
    fn select_item(
        &self,
        item: &Expression,
        index: usize,
        from_tables: &[FromTable],
    ) -> Result<Vec<(Expr, String)>, ParseError> {
        match item {
            Expression::Star(star) => self.expand_star(star, from_tables),
            Expression::Alias(alias) => {
                Ok(vec![(self.expr(&alias.this)?, alias.alias.name.clone())])
            }
            Expression::Column(column) => Ok(vec![(self.expr(item)?, column.name.name.clone())]),
            other => Ok(vec![(self.expr(other)?, format!("col_{index}"))]),
        }
    }

    /// Expand `*` (all FROM items) or `alias.*` (one item) into explicit qualified
    /// column references, honoring the DuckDB/BigQuery star modifiers: EXCLUDE /
    /// EXCEPT drops columns, REPLACE substitutes a column's expression in place,
    /// RENAME renames the output column. Raises when a table's columns cannot be
    /// resolved - a star that cannot expand must fail loudly, never drop columns.
    fn expand_star(
        &self,
        star: &polyglot_sql::expressions::Star,
        from_tables: &[FromTable],
    ) -> Result<Vec<(Expr, String)>, ParseError> {
        let mut outputs = Vec::new();
        for table in from_tables {
            if star
                .table
                .as_ref()
                .is_some_and(|ident| ident.name != table.key)
            {
                continue;
            }
            for column in self.star_columns(table)? {
                if star_excludes(star, &column) {
                    continue;
                }
                outputs.push(self.star_output(star, &table.key, column)?);
            }
        }
        if outputs.is_empty() {
            return Err(ParseError::Unsupported(
                "cannot expand * : no matching table".to_string(),
            ));
        }
        Ok(outputs)
    }

    /// One expanded star column as (expression, output name). REPLACE swaps the
    /// column for its expression; RENAME renames the output. With neither, it is
    /// the plain qualified column reference.
    fn star_output(
        &self,
        star: &polyglot_sql::expressions::Star,
        key: &str,
        column: String,
    ) -> Result<(Expr, String), ParseError> {
        let expr = match star_replacement(star, &column) {
            Some(replacement) => self.expr(replacement)?,
            None => Expr::Column(ColumnRef::new(Some(key.to_string()), column.clone(), None)),
        };
        Ok((expr, star_output_name(star, column)))
    }

    /// The column names a FROM item contributes to a star.
    fn star_columns(&self, table: &FromTable) -> Result<Vec<String>, ParseError> {
        match &table.columns {
            ColumnSource::Explicit(columns) => Ok(columns.clone()),
            ColumnSource::Catalog {
                datasource,
                schema,
                table: name,
            } => self
                .catalog
                .resolve_table_columns(datasource.as_deref(), schema.as_deref(), name)
                .ok_or_else(|| {
                    ParseError::Unsupported(format!("cannot expand * : unknown table {name}"))
                }),
        }
    }

    /// Lower a QUALIFY clause: it filters on window-function outputs, which the
    /// engine computes in the projection below. The projected rows become a
    /// subquery and the QUALIFY predicate a `Filter` over it, so the predicate
    /// references the window outputs as ordinary columns. Only a predicate over a
    /// SELECT-list alias is modeled; an inline window in the predicate would build
    /// an invalid filter (windows are illegal in a filter), so it raises.
    fn apply_qualify(
        &self,
        input: LogicalPlan,
        select: &Select,
    ) -> Result<LogicalPlan, ParseError> {
        let Some(qualify) = &select.qualify else {
            return Ok(input);
        };
        let predicate = self.expr(&qualify.this)?;
        if fq_plan::expr::contains_window(&predicate) {
            return Err(ParseError::Unsupported(
                "QUALIFY with an inline window function; alias it in the SELECT list".to_string(),
            ));
        }
        let output_names = input.schema();
        // Fresh subquery boundary wrapping the projected rows so the QUALIFY
        // predicate can reference window outputs by name - no base to copy from.
        // Field list (input/alias/column_names) is the complete SubqueryScan struct.
        let subquery = LogicalPlan::SubqueryScan(SubqueryScan {
            input: Box::new(input),
            alias: QUALIFY_SUBQUERY_ALIAS.to_string(),
            column_names: None,
        });
        // Fresh QUALIFY filter over the subquery - no base to copy from. Field list
        // (input/predicate) is the complete Filter struct.
        let filtered = LogicalPlan::Filter(Filter {
            input: Box::new(subquery),
            predicate,
        });
        Ok(project_relation_columns(
            filtered,
            QUALIFY_SUBQUERY_ALIAS,
            output_names,
        ))
    }

    /// Wrap the input in a `Sort` for the ORDER BY clause, if present.
    fn apply_order_by(
        &self,
        input: LogicalPlan,
        select: &Select,
    ) -> Result<LogicalPlan, ParseError> {
        let Some(order_by) = &select.order_by else {
            return Ok(input);
        };
        let mut sort_keys = Vec::with_capacity(order_by.expressions.len());
        let mut ascending = Vec::with_capacity(order_by.expressions.len());
        let mut nulls_order = Vec::with_capacity(order_by.expressions.len());
        for ordered in &order_by.expressions {
            sort_keys.push(self.expr(&ordered.this)?);
            ascending.push(!ordered.desc);
            nulls_order.push(nulls_of(ordered));
        }
        // Fresh sort wrapping the input, built from the parsed ORDER BY keys - no
        // base to copy from. Field list (input/sort_keys/ascending/nulls_order) is
        // the complete Sort struct.
        Ok(LogicalPlan::Sort(Sort {
            input: Box::new(input),
            sort_keys,
            ascending,
            nulls_order: Some(nulls_order),
        }))
    }
}

/// Wrap the input in a `Limit` for LIMIT and/or OFFSET, if present. A free
/// function (it needs no catalog): LIMIT/OFFSET counts are plain literals.
fn apply_limit(input: LogicalPlan, select: &Select) -> Result<LogicalPlan, ParseError> {
    let limit = match &select.limit {
        Some(clause) => Some(limit_value(&clause.this)?),
        None => fetch_limit(select.fetch.as_ref())?,
    };
    let offset = match &select.offset {
        Some(clause) => limit_value(&clause.this)?,
        None => 0,
    };
    if limit.is_none() && offset == 0 {
        return Ok(input);
    }
    // Fresh limit wrapping the input, built from the parsed LIMIT/OFFSET counts - no
    // base to copy from. Field list (input/limit/offset) is the complete Limit struct.
    Ok(LogicalPlan::Limit(Limit {
        input: Box::new(input),
        limit,
        offset,
    }))
}

/// A `Projection` that re-exposes each of `names` as a column of relation
/// `qualifier`, preserving the visible output names. Used to give a QUALIFY
/// result a Projection root carrying the user-visible column names.
fn project_relation_columns(
    input: LogicalPlan,
    qualifier: &str,
    names: Vec<String>,
) -> LogicalPlan {
    let mut expressions = Vec::with_capacity(names.len());
    for name in &names {
        expressions.push(Expr::Column(ColumnRef::new(
            Some(qualifier.to_string()),
            name.clone(),
            None,
        )));
    }
    // Fresh projection re-exposing the subquery columns under their visible names -
    // no base to copy from. Field list (input/expressions/aliases/distinct/
    // distinct_on) is the complete Projection struct; this pass-through keeps no
    // DISTINCT.
    LogicalPlan::Projection(Projection {
        input: Box::new(input),
        expressions,
        aliases: names,
        distinct: false,
        distinct_on: None,
    })
}

/// The row count of a `FETCH FIRST n ROWS ONLY`, which is exactly `LIMIT n`.
/// PERCENT and WITH TIES change the row set (a fraction, or ties past the count)
/// and are not modeled, so they raise rather than being dropped.
fn fetch_limit(
    fetch: Option<&polyglot_sql::expressions::Fetch>,
) -> Result<Option<u64>, ParseError> {
    let Some(fetch) = fetch else {
        return Ok(None);
    };
    if fetch.percent || fetch.with_ties {
        return Err(ParseError::Unsupported(
            "FETCH FIRST with PERCENT or WITH TIES".to_string(),
        ));
    }
    match &fetch.count {
        Some(count) => Ok(Some(limit_value(count)?)),
        None => Ok(Some(1)),
    }
}

/// Raise on any SELECT clause this stage does not handle (the Python
/// `SUPPORTED_SELECT_ARGS` allowlist). JOIN, DISTINCT, and DISTINCT ON are handled
/// and so are absent here.
fn reject_unsupported_clauses(select: &Select) -> Result<(), ParseError> {
    let unsupported = [
        (select.prewhere.is_some(), "PREWHERE"),
        (!select.lateral_views.is_empty(), "LATERAL VIEW"),
        (select.distribute_by.is_some(), "DISTRIBUTE BY"),
        (select.cluster_by.is_some(), "CLUSTER BY"),
        (select.sort_by.is_some(), "SORT BY"),
        (select.windows.is_some(), "WINDOW"),
        (select.sample.is_some(), "TABLESAMPLE"),
        (select.top.is_some(), "TOP"),
        (select.limit_by.is_some(), "LIMIT BY"),
        (select.connect.is_some(), "CONNECT BY"),
        (select.into.is_some(), "SELECT INTO"),
    ];
    for (present, name) in unsupported {
        if present {
            return Err(ParseError::Unsupported(name.to_string()));
        }
    }
    Ok(())
}

/// The qualifier a reference uses for a table: its alias if any, else its name.
fn table_key(table: &TableRef) -> String {
    table
        .alias
        .as_ref()
        .map_or_else(|| table.name.name.clone(), |ident| ident.name.clone())
}

/// A dependent (INNER/LEFT) `LateralJoin` over the given sides. The right (a
/// derived table) may reference the left; the node carries no ON predicate.
fn lateral_join(left: LogicalPlan, right: LogicalPlan, join_type: JoinType) -> LogicalPlan {
    // Fresh dependent join over the left plan and lateral derived table - no base
    // to copy from. Field list (left/right/join_type) is the complete LateralJoin.
    LogicalPlan::LateralJoin(LateralJoin {
        left: Box::new(left),
        right: Box::new(right),
        join_type,
    })
}

/// Map the join kind of a `JOIN LATERAL` to its `LateralJoin` join type. LEFT
/// keeps unmatched left rows; INNER/CROSS/plain drops them. Other kinds
/// (RIGHT/FULL/SEMI/...) have no lateral form and raise.
fn lateral_join_type(kind: JoinKind) -> Result<JoinType, ParseError> {
    match kind {
        JoinKind::Left | JoinKind::LeftLateral => Ok(JoinType::Left),
        JoinKind::Inner | JoinKind::Cross | JoinKind::Lateral => Ok(JoinType::Inner),
        other => Err(ParseError::Unsupported(format!("{other:?} JOIN LATERAL"))),
    }
}

/// A `LateralJoin` node carries no ON predicate, so an explicit `JOIN LATERAL`
/// may only specify `ON TRUE` (or omit ON). Any other ON expression or a USING
/// list carries a filter the node cannot represent and raises rather than being
/// silently dropped.
fn require_trivial_lateral_on(join: &polyglot_sql::expressions::Join) -> Result<(), ParseError> {
    if !join.using.is_empty() {
        return Err(ParseError::Unsupported(
            "JOIN LATERAL ... USING".to_string(),
        ));
    }
    match &join.on {
        None => Ok(()),
        Some(Expression::Boolean(boolean)) if boolean.value => Ok(()),
        Some(_) => Err(ParseError::Unsupported(
            "JOIN LATERAL with a non-trivial ON predicate".to_string(),
        )),
    }
}

/// Map a polyglot join kind to `(JoinType, natural)`; exotic kinds raise.
fn map_join_kind(kind: JoinKind) -> Result<(JoinType, bool), ParseError> {
    let mapped = match kind {
        JoinKind::Inner => (JoinType::Inner, false),
        JoinKind::Left => (JoinType::Left, false),
        JoinKind::Right => (JoinType::Right, false),
        JoinKind::Full | JoinKind::Outer => (JoinType::Full, false),
        // An implicit join is a comma-separated FROM item mixed into an
        // explicit-JOIN chain (`FROM a LEFT JOIN b ON ..., c, d`): a CROSS product
        // whose equi-predicates live in WHERE, folded left-deep exactly like a
        // plain comma FROM list and like an explicit CROSS JOIN.
        JoinKind::Cross | JoinKind::Implicit => (JoinType::Cross, false),
        JoinKind::Semi | JoinKind::LeftSemi => (JoinType::Semi, false),
        JoinKind::Anti | JoinKind::LeftAnti => (JoinType::Anti, false),
        JoinKind::Natural => (JoinType::Inner, true),
        JoinKind::NaturalLeft => (JoinType::Left, true),
        JoinKind::NaturalRight => (JoinType::Right, true),
        JoinKind::NaturalFull => (JoinType::Full, true),
        other => return Err(ParseError::Unsupported(format!("{other:?} JOIN"))),
    };
    Ok(mapped)
}

impl Converter<'_> {
    /// Assemble a `Scan` for one table. The scan over-collects a SUPERSET of the
    /// columns it will need; the binder validates and projection pushdown trims to
    /// the exact required set (via the exhaustive `fq_plan` walker). The superset is
    /// the table's FULL catalog column list when the table is known, because
    /// polyglot's `get_columns` reference analysis UNDER-collects - it does not
    /// descend into typed-function arguments (SUM/SUBSTRING/EXTRACT/...), so a
    /// column referenced only inside such a call would be dropped and a scan feeding
    /// a merge-engine aggregate/projection could omit it (the q18/q22 class). Falls
    /// back to reference analysis when the table is not in the catalog (a
    /// catalog-free parse).
    fn scan_from_table(&self, select: &Select, table: &TableRef, key: &str) -> Scan {
        let datasource = table
            .catalog
            .as_ref()
            .map_or_else(String::new, |ident| ident.name.clone());
        let schema_name = table
            .schema
            .as_ref()
            .map_or_else(|| "public".to_string(), |ident| ident.name.clone());
        let columns = self
            .catalog
            .resolve_table_columns(
                table.catalog.as_ref().map(|ident| ident.name.as_str()),
                table.schema.as_ref().map(|ident| ident.name.as_str()),
                &table.name.name,
            )
            .filter(|cols| !cols.is_empty())
            .unwrap_or_else(|| columns_for_table(select, key));
        let mut scan = Scan::new(datasource, schema_name, table.name.name.clone(), columns);
        // Every table carries an alias - its explicit alias, else its own name (ports
        // Python `_extract_table_alias` -> `alias_or_name`). This is the invariant the
        // rest of the engine relies on: a column bound to an unaliased table qualifies
        // to the table name, and the physical column_aliases map is keyed on the alias,
        // so an absent alias makes those columns unresolvable (they vanish from join
        // outputs - the q18 bug). Never leave a scan unaliased.
        scan.alias = Some(
            table
                .alias
                .as_ref()
                .map_or_else(|| table.name.name.clone(), |ident| ident.name.clone()),
        );
        scan
    }
}

/// Whether a star's EXCLUDE/EXCEPT list drops `column`.
fn star_excludes(star: &polyglot_sql::expressions::Star, column: &str) -> bool {
    star.except.as_ref().is_some_and(|excepts| {
        excepts
            .iter()
            .any(|ident| ident.name.eq_ignore_ascii_case(column))
    })
}

/// The REPLACE expression substituting `column`, if the star names one.
fn star_replacement<'a>(
    star: &'a polyglot_sql::expressions::Star,
    column: &str,
) -> Option<&'a Expression> {
    let replacements = star.replace.as_ref()?;
    for replacement in replacements {
        if replacement.alias.name.eq_ignore_ascii_case(column) {
            return Some(&replacement.this);
        }
    }
    None
}

/// The output name for `column`, applying a RENAME (old -> new) if the star
/// names one, else the column's own name.
fn star_output_name(star: &polyglot_sql::expressions::Star, column: String) -> String {
    if let Some(renames) = &star.rename {
        for (old, new) in renames {
            if old.name.eq_ignore_ascii_case(&column) {
                return new.name.clone();
            }
        }
    }
    column
}

/// Every root expression that may reference columns.
fn column_roots(select: &Select) -> Vec<&Expression> {
    let mut roots: Vec<&Expression> = select.expressions.iter().collect();
    if let Some(where_clause) = &select.where_clause {
        roots.push(&where_clause.this);
    }
    if let Some(group_by) = &select.group_by {
        roots.extend(group_by.expressions.iter());
    }
    if let Some(having) = &select.having {
        roots.push(&having.this);
    }
    if let Some(order_by) = &select.order_by {
        for ordered in &order_by.expressions {
            roots.push(&ordered.this);
        }
    }
    for join in &select.joins {
        if let Some(on) = &join.on {
            roots.push(on);
        }
    }
    roots
}

/// Distinct column names referenced for a table `key`: those qualified by `key`
/// plus every unqualified reference (over-collection; the binder resolves which
/// table owns an unqualified column).
fn columns_for_table(select: &Select, key: &str) -> Vec<String> {
    let mut columns: Vec<String> = Vec::new();
    for root in column_roots(select) {
        collect_columns(root, key, &mut columns);
    }
    columns
}

/// Collect column names referenced in `root` that belong to table `key` (or are
/// unqualified), INCLUDING columns inside aggregate arguments. polyglot's
/// `get_columns` stops at aggregate boundaries (`get_columns(SUM(x))` is empty),
/// so an aggregate-argument column would never be over-collected; a scan feeding a
/// merge-engine aggregate would then omit it and the aggregate could not find it
/// (the q18-class column drop). Aggregate args are gathered separately and
/// recursed into (so a column and a nested aggregate in the same arg both count).
fn collect_columns(root: &Expression, key: &str, columns: &mut Vec<String>) {
    for found in polyglot_sql::traversal::get_columns(root) {
        if let Expression::Column(column) = found {
            let matches = match &column.table {
                Some(qualifier) => qualifier.name == key,
                None => true,
            };
            if matches && !columns.contains(&column.name.name) {
                columns.push(column.name.name.clone());
            }
        }
    }
    for arg in aggregate_arguments(root) {
        collect_columns(arg, key, columns);
    }
}

/// The argument expression of every aggregate call reachable in `root`
/// (SUM/AVG/MIN/MAX carry `AggFunc.this`; COUNT carries an optional `CountFunc.this`,
/// absent for `COUNT(*)`). `find_all` does not cross an aggregate boundary, so these
/// are the args `get_columns` misses.
// The AggFunc arm and the StringAgg arm both push a single `.this` argument but
// bind distinct polyglot function types (AggFunc vs StringAggFunc), so they cannot
// share one pattern; the identical bodies are unavoidable, not a copy-paste slip.
#[allow(clippy::match_same_arms)]
fn aggregate_arguments(root: &Expression) -> Vec<&Expression> {
    let mut args = Vec::new();
    for aggregate in root.find_all(is_aggregate) {
        match aggregate {
            Expression::Sum(agg)
            | Expression::Avg(agg)
            | Expression::Min(agg)
            | Expression::Max(agg)
            | Expression::StddevSamp(agg)
            | Expression::StddevPop(agg)
            | Expression::Stddev(agg)
            | Expression::Variance(agg)
            | Expression::VarPop(agg)
            | Expression::VarSamp(agg)
            | Expression::ArrayAgg(agg)
            | Expression::LogicalAnd(agg)
            | Expression::LogicalOr(agg)
            | Expression::Median(agg) => args.push(&agg.this),
            Expression::StringAgg(agg) => args.push(&agg.this),
            Expression::WithinGroup(within) => {
                for ordered in &within.order_by {
                    args.push(&ordered.this);
                }
            }
            Expression::Count(count) => {
                if let Some(inner) = &count.this {
                    args.push(inner);
                }
            }
            _ => {}
        }
    }
    args
}

/// Whether an expression is one of the aggregate calls.
fn is_aggregate(expr: &Expression) -> bool {
    matches!(
        expr,
        Expression::Sum(_)
            | Expression::Avg(_)
            | Expression::Min(_)
            | Expression::Max(_)
            | Expression::Count(_)
            | Expression::StddevSamp(_)
            | Expression::StddevPop(_)
            | Expression::Stddev(_)
            | Expression::Variance(_)
            | Expression::VarPop(_)
            | Expression::VarSamp(_)
            | Expression::ArrayAgg(_)
            | Expression::LogicalAnd(_)
            | Expression::LogicalOr(_)
            | Expression::StringAgg(_)
            | Expression::Median(_)
            | Expression::WithinGroup(_)
    )
}

/// Raise when `node` uses a window function directly in a `clause` where one is
/// illegal (WHERE / GROUP BY / HAVING). Window functions evaluate after those
/// clauses, so one there is invalid SQL and must fail loudly rather than reach a
/// source or the merge path. Only this clause's own scope is checked: the walk
/// prunes at any nested query, whose windows belong to that subquery's SELECT
/// list (a ranked derived table filtered by an outer predicate is legal).
fn reject_window_in_clause(node: &Expression, clause: &str) -> Result<(), ParseError> {
    if contains_window_in_scope(node) {
        return Err(ParseError::Unsupported(format!(
            "window functions are not allowed in {clause}"
        )));
    }
    Ok(())
}

/// Whether `node` contains a window function in its own scope, not descending
/// into a nested query (a subquery's windows belong to that subquery's SELECT).
fn contains_window_in_scope(node: &Expression) -> bool {
    if matches!(node, Expression::WindowFunction(_)) {
        return true;
    }
    if polyglot_sql::traversal::is_query(node)
        || polyglot_sql::traversal::is_subquery(node)
        || polyglot_sql::traversal::is_set_operation(node)
    {
        return false;
    }
    node.children()
        .iter()
        .any(|child| contains_window_in_scope(child))
}

/// The NULLS FIRST/LAST of an ORDER BY key, or None when unspecified.
pub(crate) fn nulls_of(ordered: &Ordered) -> Option<NullsOrder> {
    ordered.nulls_first.map(|first| {
        if first {
            NullsOrder::First
        } else {
            NullsOrder::Last
        }
    })
}

/// Read a LIMIT/OFFSET count as a non-negative integer literal.
fn limit_value(expr: &Expression) -> Result<u64, ParseError> {
    if let Expression::Literal(literal) = expr {
        if let polyglot_sql::expressions::Literal::Number(text) = literal.as_ref() {
            if let Ok(value) = text.parse::<u64>() {
                return Ok(value);
            }
        }
    }
    Err(ParseError::Unsupported(
        "non-integer LIMIT/OFFSET".to_string(),
    ))
}

/// An aggregate's expanded grouping sets (`None` for a plain GROUP BY).
type GroupingSets = Option<Vec<Vec<Expr>>>;

/// `ROLLUP(k1..kn)`: the n+1 prefixes `[k1..kn], ..., [k1], []`.
fn rollup_sets(keys: &[Expr]) -> Vec<Vec<Expr>> {
    let mut sets = Vec::with_capacity(keys.len() + 1);
    for size in (0..=keys.len()).rev() {
        sets.push(keys[..size].to_vec());
    }
    sets
}

/// `CUBE(k1..kn)`: every subset of the keys, in bitmask order.
fn cube_sets(keys: &[Expr]) -> Vec<Vec<Expr>> {
    let count = 1usize << keys.len();
    let mut sets = Vec::with_capacity(count);
    for mask in 0..count {
        let mut subset = Vec::new();
        for (index, key) in keys.iter().enumerate() {
            if mask & (1 << index) != 0 {
                subset.push(key.clone());
            }
        }
        sets.push(subset);
    }
    sets
}

/// Whether a GROUP BY key is a ROLLUP/CUBE/GROUPING SETS construct - either the
/// dedicated polyglot variant or the generic-Function spelling its Postgres
/// parser emits for CUBE.
fn is_grouping_construct(key: &Expression) -> bool {
    match key {
        Expression::Rollup(_) | Expression::Cube(_) | Expression::GroupingSets(_) => true,
        Expression::Function(function) => {
            function.name.eq_ignore_ascii_case("rollup")
                || function.name.eq_ignore_ascii_case("cube")
                || function.name.eq_ignore_ascii_case("grouping sets")
        }
        _ => false,
    }
}
