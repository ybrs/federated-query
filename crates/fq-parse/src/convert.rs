//! The `Converter`: polyglot-sql AST -> `fq_plan::LogicalPlan`.
//!
//! Holds the catalog (for star expansion) and owns the recursive descent: a
//! query converts to a plan, its expressions convert to `Expr` (see `expr.rs`),
//! and a subquery-bearing expression recurses back into `query`. Builds a SELECT
//! clause by clause in the Python order: FROM -> WHERE -> GROUP BY -> HAVING ->
//! SELECT -> ORDER BY -> LIMIT.
//!
//! Coverage: base tables + left-deep JOINs (explicit and comma/implicit) +
//! derived tables + CTE references in FROM, WITH (recursive and non-recursive),
//! a static PIVOT rewritten to conditional aggregation, catalog-driven star
//! expansion, WHERE, GROUP BY + the aggregates, HAVING, ORDER BY, LIMIT/OFFSET,
//! DISTINCT, VALUES, and the binary set operations. A window function in WHERE /
//! GROUP BY / HAVING raises (it is legal only in SELECT and ORDER BY).

use std::cell::RefCell;
use std::collections::HashMap;

use fq_catalog::Catalog;
use fq_plan::expr::{LiteralValue, NullsOrder};
use fq_plan::{
    Aggregate, BinaryOpType, ColumnRef, Cte, CteRef, Expr, Filter, Join, JoinType, LateralJoin,
    Limit, LogicalPlan, Projection, Scan, SetOpKind, SetOperation, Sort, SubqueryScan, Values,
};
use polyglot_sql::expressions::{
    Expression, JoinKind, Literal, Null, Ordered, Sample, SampleMethod, Select, TableRef, With,
};
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

/// The clauses a parsed set-operation node carries for its ENTIRE combined
/// result (`SELECT .. UNION SELECT .. ORDER BY x LIMIT n`): polyglot attaches
/// them to the Union/Intersect/Except node, not to either operand.
#[derive(Clone, Copy)]
struct SetOpModifiers<'e> {
    with: Option<&'e With>,
    order_by: Option<&'e polyglot_sql::expressions::OrderBy>,
    limit: Option<&'e Expression>,
    offset: Option<&'e Expression>,
}

/// A set operation polyglot swallowed into a scalar-subquery operand. Parsing
/// `<select ending in a parenthesized subquery> UNION ALL <query>` attaches the
/// trailing set operation to the subquery expression inside the first select's
/// final WHERE/HAVING clause instead of combining the two statements; these are
/// the detached pieces needed to rebuild the intended statement-level set
/// operation.
struct SwallowedSetOp {
    kind: SetOpKind,
    distinct: bool,
    right: Expression,
    with: Option<With>,
    order_by: Option<polyglot_sql::expressions::OrderBy>,
    limit: Option<Expression>,
    offset: Option<Expression>,
}

/// Whether the rightmost operand spine of a predicate ends in the swallowed
/// shape: a BARE set operation whose left operand is a parenthesized subquery.
/// A genuine scalar set operation (`x > (SELECT .. UNION SELECT ..)`) is fully
/// enclosed in a Subquery node and never appears bare inside an expression, so
/// the bare node is unambiguously the parser mis-attachment.
fn spine_has_swallowed_set_op(expr: &Expression) -> bool {
    match expr {
        Expression::Union(union) => matches!(union.left, Expression::Subquery(_)),
        Expression::Intersect(intersect) => matches!(intersect.left, Expression::Subquery(_)),
        Expression::Except(except) => matches!(except.left, Expression::Subquery(_)),
        Expression::Not(unary) => spine_has_swallowed_set_op(&unary.this),
        other => match crate::expr::binary_op_type(other) {
            Some(_) => spine_has_swallowed_set_op(&crate::expr::binary_operands(other).right),
            None => false,
        },
    }
}

/// Remove the swallowed set operation from the rightmost operand spine, leaving
/// the scalar subquery it displaced in its place. Called only where
/// `spine_has_swallowed_set_op` matched; the descent mirrors it exactly.
fn detach_swallowed_set_op(expr: &mut Expression) -> SwallowedSetOp {
    match expr {
        Expression::Union(_) | Expression::Intersect(_) | Expression::Except(_) => {
            // The placeholder Null is overwritten with the set operation's own left
            // operand immediately below; it never survives into the tree.
            let node = std::mem::replace(expr, Expression::Null(Null));
            let (scalar_subquery, tail) = split_set_op_node(node);
            *expr = scalar_subquery;
            tail
        }
        Expression::Not(unary) => detach_swallowed_set_op(&mut unary.this),
        other => detach_swallowed_set_op(&mut crate::expr::binary_operands_mut(other).right),
    }
}

/// Break a swallowed set-operation node into the scalar subquery it displaced
/// (its left operand) and the statement-level pieces: the operator, the right
/// branch, and the whole-result modifiers the parser attached to the node.
fn split_set_op_node(node: Expression) -> (Expression, SwallowedSetOp) {
    // The polyglot set-op structs implement Drop, so their fields cannot be
    // moved out by destructuring; each one is taken out by replacement instead.
    match node {
        Expression::Union(mut union) => take_set_op_parts(
            SetOpKind::Union,
            !union.all,
            &mut union.left,
            &mut union.right,
            &mut union.with,
            &mut union.order_by,
            &mut union.limit,
            &mut union.offset,
        ),
        Expression::Intersect(mut intersect) => take_set_op_parts(
            SetOpKind::Intersect,
            !intersect.all,
            &mut intersect.left,
            &mut intersect.right,
            &mut intersect.with,
            &mut intersect.order_by,
            &mut intersect.limit,
            &mut intersect.offset,
        ),
        Expression::Except(mut except) => take_set_op_parts(
            SetOpKind::Except,
            !except.all,
            &mut except.left,
            &mut except.right,
            &mut except.with,
            &mut except.order_by,
            &mut except.limit,
            &mut except.offset,
        ),
        other => unreachable!("split_set_op_node called on `{}`", other.variant_name()),
    }
}

/// Take the swallowed node's operands and whole-result modifiers out of its
/// fields (leaving Null/None behind; the emptied node is dropped by the
/// caller), pairing the displaced scalar subquery with the statement pieces.
/// `distinct` already carries the `!all` mapping the regular set-operation
/// conversion uses.
#[allow(clippy::too_many_arguments)]
fn take_set_op_parts(
    kind: SetOpKind,
    distinct: bool,
    left: &mut Expression,
    right: &mut Expression,
    with: &mut Option<With>,
    order_by: &mut Option<polyglot_sql::expressions::OrderBy>,
    limit: &mut Option<Box<Expression>>,
    offset: &mut Option<Box<Expression>>,
) -> (Expression, SwallowedSetOp) {
    let scalar_subquery = std::mem::replace(left, Expression::Null(Null));
    // Owned pieces of the mis-attached node; the field list mirrors
    // SetOpModifiers plus the operator and right branch.
    let tail = SwallowedSetOp {
        kind,
        distinct,
        right: std::mem::replace(right, Expression::Null(Null)),
        with: with.take(),
        order_by: order_by.take(),
        limit: limit.take().map(|expr| *expr),
        offset: offset.take().map(|expr| *expr),
    };
    (scalar_subquery, tail)
}

/// Detach a swallowed set operation from a select's final predicate clause,
/// returning the repaired select and the detached statement-level pieces, or
/// None when the select is well-formed. Only WHERE and HAVING can end the
/// statement text with a scalar subquery here; HAVING is probed first because
/// it is the later clause. The select is cloned only when a swallowed node is
/// actually present.
fn detach_from_select(select: &Select) -> Option<(Select, SwallowedSetOp)> {
    if let Some(having) = &select.having {
        if spine_has_swallowed_set_op(&having.this) {
            let mut repaired = select.clone();
            let clause = repaired.having.as_mut().expect("clone keeps HAVING");
            let tail = detach_swallowed_set_op(&mut clause.this);
            return Some((repaired, tail));
        }
    }
    if let Some(where_clause) = &select.where_clause {
        if spine_has_swallowed_set_op(&where_clause.this) {
            let mut repaired = select.clone();
            let clause = repaired.where_clause.as_mut().expect("clone keeps WHERE");
            let tail = detach_swallowed_set_op(&mut clause.this);
            return Some((repaired, tail));
        }
    }
    None
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
            Expression::Select(select) => match detach_from_select(select) {
                Some((repaired, tail)) => self.reattached_set_operation(repaired, &tail),
                None => self.select(select),
            },
            Expression::Union(union) => self.set_operation(
                &union.left,
                &union.right,
                SetOpKind::Union,
                !union.all,
                SetOpModifiers {
                    with: union.with.as_ref(),
                    order_by: union.order_by.as_ref(),
                    limit: union.limit.as_deref(),
                    offset: union.offset.as_deref(),
                },
            ),
            Expression::Intersect(intersect) => self.set_operation(
                &intersect.left,
                &intersect.right,
                SetOpKind::Intersect,
                !intersect.all,
                SetOpModifiers {
                    with: intersect.with.as_ref(),
                    order_by: intersect.order_by.as_ref(),
                    limit: intersect.limit.as_deref(),
                    offset: intersect.offset.as_deref(),
                },
            ),
            Expression::Except(except) => self.set_operation(
                &except.left,
                &except.right,
                SetOpKind::Except,
                !except.all,
                SetOpModifiers {
                    with: except.with.as_ref(),
                    order_by: except.order_by.as_ref(),
                    limit: except.limit.as_deref(),
                    offset: except.offset.as_deref(),
                },
            ),
            Expression::Subquery(subquery) => self.query(&subquery.this),
            Expression::Values(values) => self.values(values),
            other => Err(ParseError::Unsupported(format!(
                "query `{}`",
                other.variant_name()
            ))),
        }
    }

    /// Convert a binary set operation. `distinct` is the bare form; ALL preserves
    /// multiplicity. The node's own ORDER BY / LIMIT / OFFSET apply to the entire
    /// combined result and wrap the `SetOperation` in `Sort` / `Limit` nodes; a
    /// WITH bound to the set operation itself is not supported and raises.
    fn set_operation(
        &self,
        left: &Expression,
        right: &Expression,
        kind: SetOpKind,
        distinct: bool,
        modifiers: SetOpModifiers<'_>,
    ) -> Result<LogicalPlan, ParseError> {
        if modifiers.with.is_some() {
            return Err(ParseError::Unsupported(
                "WITH bound to a set operation".to_string(),
            ));
        }
        // Fresh set-operation node built from the parsed UNION/INTERSECT/EXCEPT - no
        // base to copy from. Field list (left/right/kind/distinct) is the complete
        // SetOperation struct.
        let combined = LogicalPlan::SetOperation(SetOperation {
            left: Box::new(self.query(left)?),
            right: Box::new(self.query(right)?),
            kind,
            distinct,
        });
        let sorted = self.apply_order_keys(combined, modifiers.order_by)?;
        set_op_limit(sorted, modifiers.limit, modifiers.offset)
    }

    /// Rebuild the statement-level set operation polyglot swallowed into a
    /// select's final predicate: `repaired` (its scalar subquery restored) is
    /// the left branch; `tail` supplies the operator, the right branch, and the
    /// whole-result modifiers the parser attached to the swallowed node. The
    /// right branch converts through `query`, so a chain of swallowed branches
    /// (each select's HAVING hiding the next) unrolls recursively.
    fn reattached_set_operation(
        &self,
        repaired: Select,
        tail: &SwallowedSetOp,
    ) -> Result<LogicalPlan, ParseError> {
        let left = Expression::Select(Box::new(repaired));
        self.set_operation(
            &left,
            &tail.right,
            tail.kind,
            tail.distinct,
            SetOpModifiers {
                with: tail.with.as_ref(),
                order_by: tail.order_by.as_ref(),
                limit: tail.limit.as_ref(),
                offset: tail.offset.as_ref(),
            },
        )
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
    /// with any WITH already bound into scope. A FROM-less SELECT is a single
    /// constant row (a one-row `Values`).
    fn select_body(&self, select: &Select) -> Result<LogicalPlan, ParseError> {
        if select.from.is_none() {
            return self.values_select(select);
        }
        let (from, from_tables) = self.build_from(select)?;
        let filtered = self.apply_where(from, select)?;
        let projected = self.apply_projection(filtered, select, &from_tables)?;
        let qualified = self.apply_qualify(projected, select)?;
        let sorted = self.apply_order_by(qualified, select)?;
        apply_limit(sorted, select)
    }

    /// A FROM-less SELECT (`SELECT 1 AS n`) is a single constant row, modelled as
    /// a one-row `Values` relation over the converted projection expressions.
    /// Clauses that require an input relation (WHERE / GROUP BY / HAVING / ORDER
    /// BY / LIMIT / OFFSET / DISTINCT / QUALIFY / a star) have nothing to act on
    /// and raise rather than being silently dropped.
    fn values_select(&self, select: &Select) -> Result<LogicalPlan, ParseError> {
        let has_limit = select.limit.is_some() || select.offset.is_some() || select.fetch.is_some();
        let unsupported = [
            (select.where_clause.is_some(), "WHERE"),
            (select.group_by.is_some(), "GROUP BY"),
            (select.having.is_some(), "HAVING"),
            (select.order_by.is_some(), "ORDER BY"),
            (has_limit, "LIMIT/OFFSET"),
            (select.distinct || select.distinct_on.is_some(), "DISTINCT"),
            (!select.joins.is_empty(), "JOIN"),
            (select.qualify.is_some(), "QUALIFY"),
        ];
        for (present, name) in unsupported {
            if present {
                return Err(ParseError::Unsupported(format!(
                    "FROM-less SELECT with {name}"
                )));
            }
        }
        let mut row = Vec::with_capacity(select.expressions.len());
        let mut names = Vec::with_capacity(select.expressions.len());
        for item in &select.expressions {
            if matches!(item, Expression::Star(_)) {
                return Err(ParseError::Unsupported(
                    "FROM-less SELECT with a star projection".to_string(),
                ));
            }
            for (expr, name) in self.select_item(item, &[])? {
                row.push(expr);
                names.push(name);
            }
        }
        // Fresh VALUES node holding the one constant row - no base to copy from.
        // Field list (rows/output_names) is the complete Values struct.
        Ok(LogicalPlan::Values(Values {
            rows: vec![row],
            output_names: names,
        }))
    }

    /// Bind a WITH's CTEs into scope, build the body, and wrap it in nested `Cte`
    /// nodes (first CTE outermost). CTE names are pushed before any body is built
    /// so a CTE may reference an earlier one; they are popped afterward. A
    /// `WITH RECURSIVE` marks every resulting `Cte` recursive and registers each
    /// name BEFORE its own body is converted, so the body's self-reference (in the
    /// recursive branch of its UNION) resolves as a `CteRef`.
    fn select_with(&self, with: &With, select: &Select) -> Result<LogicalPlan, ParseError> {
        let mut definitions = Vec::with_capacity(with.ctes.len());
        for cte in &with.ctes {
            let name = cte.alias.name.clone();
            let declared: Vec<String> =
                cte.columns.iter().map(|ident| ident.name.clone()).collect();
            if with.recursive {
                // The recursive body names the CTE itself, so the name is in scope
                // before its body is built. A recursive CTE has no body schema to
                // infer from yet, so its columns must be spelled out in the WITH.
                if declared.is_empty() {
                    return Err(ParseError::Unsupported(
                        "WITH RECURSIVE without an explicit column list".to_string(),
                    ));
                }
                self.ctes
                    .borrow_mut()
                    .insert(name.clone(), declared.clone());
            }
            let body = self.query(&cte.this)?;
            let columns: Vec<String> = if cte.columns.is_empty() {
                body.schema()
            } else {
                declared
            };
            // A non-recursive CTE registers only now, after its body is built (its
            // name is not visible inside its own body); a recursive one is already
            // registered above.
            if !with.recursive {
                self.ctes.borrow_mut().insert(name.clone(), columns.clone());
            }
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
                recursive: with.recursive,
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
            Expression::Table(table) => self.table_or_cte_ref(select, table, tables),
            Expression::Subquery(subquery) => self.derived_table(subquery, tables),
            Expression::Pivot(pivot) => self.convert_pivot(select, pivot, tables),
            other => Err(ParseError::Unsupported(format!(
                "FROM/JOIN {}",
                other.variant_name()
            ))),
        }
    }

    /// Rewrite a static `PIVOT` into portable conditional aggregation: each
    /// `IN` value becomes `agg(CASE WHEN pivot_col = value THEN arg END)` named for
    /// that value, grouped by every source column that is neither the pivot column
    /// nor the aggregate's argument. The rewrite is a derived table over the source
    /// so the surrounding `SELECT *` expands over the pivoted output columns. Only
    /// a single-aggregate, single-column, literal-`IN` PIVOT is modelled; every
    /// other shape (UNPIVOT, multiple aggregates, `COUNT(*)`, an `IN` subquery, an
    /// output-alias list) raises rather than silently dropping the pivot.
    fn convert_pivot(
        &self,
        select: &Select,
        pivot: &polyglot_sql::expressions::Pivot,
        tables: &mut Vec<FromTable>,
    ) -> Result<LogicalPlan, ParseError> {
        let source = self.pivot_source_scan(select, pivot)?;
        let aggregate = self.pivot_aggregate(pivot)?;
        let (pivot_col, pivot_col_name) = self.pivot_column(pivot)?;
        let group_columns = pivot_group_columns(&source, &pivot_col_name, &aggregate.arg_name);

        let mut group_by = Vec::with_capacity(group_columns.len());
        let mut aggregates = Vec::new();
        let mut output_names = Vec::new();
        for column in &group_columns {
            group_by.push(Expr::Column(ColumnRef::new(None, column.clone(), None)));
            aggregates.push(Expr::Column(ColumnRef::new(None, column.clone(), None)));
            output_names.push(column.clone());
        }
        self.pivot_value_columns(
            pivot,
            &aggregate,
            &pivot_col,
            &mut aggregates,
            &mut output_names,
        )?;

        let alias = pivot_alias(pivot, &source);
        // Fresh conditional-aggregation node realizing the pivot: no base to copy
        // from. Field list (input/group_by/aggregates/output_names/grouping_sets)
        // is the complete Aggregate struct.
        let plan = LogicalPlan::Aggregate(Aggregate {
            input: Box::new(LogicalPlan::Scan(Box::new(source))),
            group_by,
            aggregates,
            output_names: output_names.clone(),
            grouping_sets: None,
        });
        tables.push(FromTable {
            key: alias.clone(),
            columns: ColumnSource::Explicit(output_names),
        });
        // Fresh boundary wrapping the pivot rewrite so the outer query references it
        // by one relation alias - no base to copy from. Field list
        // (input/alias/column_names) is the complete SubqueryScan struct; the body
        // already carries the pivot output names, so no rename list is needed.
        Ok(LogicalPlan::SubqueryScan(SubqueryScan {
            input: Box::new(plan),
            alias,
            column_names: None,
        }))
    }

    /// The base-table scan a PIVOT reads. A PIVOT over anything but a plain table
    /// (a subquery, another pivot) is not modelled and raises.
    fn pivot_source_scan(
        &self,
        select: &Select,
        pivot: &polyglot_sql::expressions::Pivot,
    ) -> Result<Scan, ParseError> {
        reject_unsupported_pivot_shape(pivot)?;
        let Expression::Table(table) = &pivot.this else {
            return Err(ParseError::Unsupported(
                "PIVOT over a non-table source".to_string(),
            ));
        };
        self.scan_from_table(select, table, &table_key(table))
    }

    /// The single aggregate a PIVOT applies, as its function call plus the name of
    /// its one argument column and any `AS` output-name suffix. Multiple aggregates,
    /// `COUNT(*)`, or a non-column argument raise.
    fn pivot_aggregate(
        &self,
        pivot: &polyglot_sql::expressions::Pivot,
    ) -> Result<PivotAggregate, ParseError> {
        let [field] = pivot.expressions.as_slice() else {
            return Err(ParseError::Unsupported(
                "PIVOT with other than one aggregate".to_string(),
            ));
        };
        let (aggregate_ast, suffix) = match field {
            Expression::Alias(alias) => (&alias.this, Some(alias.alias.name.clone())),
            other => (other, None),
        };
        let call = self.expr(aggregate_ast)?;
        let Expr::FunctionCall {
            args,
            is_aggregate: true,
            ..
        } = &call
        else {
            return Err(ParseError::Unsupported(
                "PIVOT aggregate must be an aggregate function".to_string(),
            ));
        };
        let [Expr::Column(arg)] = args.as_slice() else {
            return Err(ParseError::Unsupported(
                "PIVOT aggregate must take exactly one column argument".to_string(),
            ));
        };
        Ok(PivotAggregate {
            arg_name: arg.column.clone(),
            call,
            suffix,
        })
    }

    /// The pivot column of the `FOR ... IN` clause, as its bound reference plus its
    /// name. A pivot on other than one bare column, or an `IN` over a subquery,
    /// raises.
    fn pivot_column(
        &self,
        pivot: &polyglot_sql::expressions::Pivot,
    ) -> Result<(Expr, String), ParseError> {
        let [Expression::In(field)] = pivot.fields.as_slice() else {
            return Err(ParseError::Unsupported(
                "PIVOT with other than one FOR column".to_string(),
            ));
        };
        if field.query.is_some() {
            return Err(ParseError::Unsupported(
                "PIVOT with an IN subquery (dynamic pivot)".to_string(),
            ));
        }
        let Expression::Column(column) = &field.this else {
            return Err(ParseError::Unsupported(
                "PIVOT FOR must be a bare column".to_string(),
            ));
        };
        Ok((self.expr(&field.this)?, column.name.name.clone()))
    }

    /// Append one conditional-aggregate output column per `IN` value: the value
    /// names the column (suffixed by any aggregate `AS` alias), and its expression
    /// is the aggregate over `CASE WHEN pivot_col = value THEN arg END`. A
    /// non-literal `IN` value raises.
    fn pivot_value_columns(
        &self,
        pivot: &polyglot_sql::expressions::Pivot,
        aggregate: &PivotAggregate,
        pivot_col: &Expr,
        aggregates: &mut Vec<Expr>,
        output_names: &mut Vec<String>,
    ) -> Result<(), ParseError> {
        let [Expression::In(field)] = pivot.fields.as_slice() else {
            return Err(ParseError::Unsupported(
                "PIVOT with other than one FOR column".to_string(),
            ));
        };
        for value_ast in &field.expressions {
            let value = self.expr(value_ast)?;
            let Expr::Literal { value: literal, .. } = &value else {
                return Err(ParseError::Unsupported(
                    "PIVOT IN value must be a literal".to_string(),
                ));
            };
            let output = match &aggregate.suffix {
                Some(suffix) => format!("{}_{}", pivot_value_name(literal)?, suffix),
                None => pivot_value_name(literal)?,
            };
            aggregates.push(aggregate.conditioned(pivot_col.clone(), value));
            output_names.push(output);
        }
        Ok(())
    }

    /// A bare table reference that names an in-scope CTE becomes a `CteRef`;
    /// otherwise it is a base-table `Scan`.
    fn table_or_cte_ref(
        &self,
        select: &Select,
        table: &TableRef,
        tables: &mut Vec<FromTable>,
    ) -> Result<LogicalPlan, ParseError> {
        let key = table_key(table);
        let unqualified = table.catalog.is_none() && table.schema.is_none();
        if unqualified {
            if let Some(columns) = self.ctes.borrow().get(&table.name.name).cloned() {
                if table.table_sample.is_some() {
                    return Err(ParseError::Unsupported(
                        "TABLESAMPLE on a CTE reference".to_string(),
                    ));
                }
                tables.push(FromTable {
                    key: key.clone(),
                    columns: ColumnSource::Explicit(columns.clone()),
                });
                // Fresh CteRef built from the parsed table reference to an in-scope
                // CTE - no base to copy from. Field list (name/alias/columns/
                // output_names) is the complete CteRef struct.
                return Ok(LogicalPlan::CteRef(CteRef {
                    name: table.name.name.clone(),
                    alias: table.alias.as_ref().map(|ident| ident.name.clone()),
                    columns: Some(columns_for_table(select, &key)),
                    output_names: Some(columns),
                }));
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
        Ok(LogicalPlan::Scan(Box::new(
            self.scan_from_table(select, table, &key)?,
        )))
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
        for item in &select.expressions {
            for (expr, name) in self.select_item(item, from_tables)? {
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
    /// its own name; anything else takes the SQL text of the expression as its
    /// visible name (an unaliased `COUNT(*)` presents as `COUNT(*)`).
    fn select_item(
        &self,
        item: &Expression,
        from_tables: &[FromTable],
    ) -> Result<Vec<(Expr, String)>, ParseError> {
        match item {
            Expression::Star(star) => self.expand_star(star, from_tables),
            Expression::Alias(alias) => {
                Ok(vec![(self.expr(&alias.this)?, alias.alias.name.clone())])
            }
            Expression::Column(column) => Ok(vec![(self.expr(item)?, column.name.name.clone())]),
            other => Ok(vec![(self.expr(other)?, output_name_for(other)?)]),
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

    /// Wrap the input in a `Sort` for the SELECT's ORDER BY clause, if present.
    fn apply_order_by(
        &self,
        input: LogicalPlan,
        select: &Select,
    ) -> Result<LogicalPlan, ParseError> {
        self.apply_order_keys(input, select.order_by.as_ref())
    }

    /// Wrap the input in a `Sort` for the given ORDER BY keys, if any. Shared by
    /// the SELECT pipeline and set-operation modifiers (an ORDER BY after a
    /// UNION/INTERSECT/EXCEPT sorts the entire combined result).
    fn apply_order_keys(
        &self,
        input: LogicalPlan,
        order_by: Option<&polyglot_sql::expressions::OrderBy>,
    ) -> Result<LogicalPlan, ParseError> {
        let Some(order_by) = order_by else {
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

/// Wrap the input in a `Limit` for a set operation's trailing LIMIT/OFFSET, if
/// present. The counts are bare literal expressions on the set-operation node
/// (unlike a SELECT's clause objects), so this converts them directly.
fn set_op_limit(
    input: LogicalPlan,
    limit: Option<&Expression>,
    offset: Option<&Expression>,
) -> Result<LogicalPlan, ParseError> {
    let limit_count = limit.map(limit_value).transpose()?;
    let offset_count = offset.map(limit_value).transpose()?.unwrap_or(0);
    if limit_count.is_none() && offset_count == 0 {
        return Ok(input);
    }
    // Fresh limit wrapping the combined set-operation result - no base to copy
    // from. Field list (input/limit/offset) is the complete Limit struct.
    Ok(LogicalPlan::Limit(Limit {
        input: Box::new(input),
        limit: limit_count,
        offset: offset_count,
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

/// The single aggregate of a PIVOT, prepared for rewriting into conditional
/// aggregation: the whole aggregate call, the name of its one argument column
/// (excluded from the implicit GROUP BY), and any `AS` alias that suffixes each
/// output column name.
struct PivotAggregate {
    arg_name: String,
    call: Expr,
    suffix: Option<String>,
}

impl PivotAggregate {
    /// The aggregate applied to `CASE WHEN pivot_col = value THEN arg END`: a copy
    /// of the original call with its single argument replaced by that CASE, so the
    /// function name and DISTINCT flag are carried unchanged.
    fn conditioned(&self, pivot_col: Expr, value: Expr) -> Expr {
        let arg = match &self.call {
            Expr::FunctionCall { args, .. } => args[0].clone(),
            other => unreachable!("pivot aggregate is a validated function call, got {other:?}"),
        };
        // Fresh equality between the pivot column and one IN value - no base to copy
        // from. Field list (op/left/right) is the complete BinaryOp variant.
        let condition = Expr::BinaryOp {
            op: BinaryOpType::Eq,
            left: Box::new(pivot_col),
            right: Box::new(value),
        };
        // Fresh single-branch CASE routing the argument only for the matching value
        // - no base to copy from. Field list (when_clauses/else_result) is the
        // complete Case variant; a non-matching row yields NULL (absent ELSE).
        let case = Expr::Case {
            when_clauses: vec![(condition, arg)],
            else_result: None,
        };
        let mut call = self.call.clone();
        if let Expr::FunctionCall { args, .. } = &mut call {
            args[0] = case;
        }
        call
    }
}

/// Reject the PIVOT shapes the conditional-aggregation rewrite does not model, so
/// each fails loudly rather than silently producing wrong columns: UNPIVOT, the
/// DuckDB simplified `USING` form, an inner GROUP BY, UNPIVOT `INTO`, an output
/// column-alias list, and the UNPIVOT null-handling / default clauses.
fn reject_unsupported_pivot_shape(
    pivot: &polyglot_sql::expressions::Pivot,
) -> Result<(), ParseError> {
    let unsupported = [
        (pivot.unpivot, "UNPIVOT"),
        (
            !pivot.using.is_empty(),
            "PIVOT ... USING (DuckDB simplified form)",
        ),
        (pivot.group.is_some(), "PIVOT with an explicit GROUP BY"),
        (pivot.into.is_some(), "UNPIVOT ... INTO"),
        (
            !pivot.alias_columns.is_empty(),
            "PIVOT with an output column-alias list",
        ),
        (pivot.include_nulls.is_some(), "UNPIVOT null handling"),
        (pivot.default_on_null.is_some(), "PIVOT ... DEFAULT ON NULL"),
    ];
    for (present, name) in unsupported {
        if present {
            return Err(ParseError::Unsupported(name.to_string()));
        }
    }
    Ok(())
}

/// The implicit GROUP BY of a PIVOT: every source column except the pivot column
/// and the aggregate's argument, in the source's column order (DuckDB's rule).
fn pivot_group_columns(source: &Scan, pivot_col: &str, arg: &str) -> Vec<String> {
    let mut columns = Vec::new();
    for column in &source.columns {
        if column != pivot_col && column != arg {
            columns.push(column.clone());
        }
    }
    columns
}

/// The relation alias the pivoted output is referenced by: the PIVOT's own alias
/// if written, else the source table's alias/name (so `SELECT *` over an unaliased
/// `FROM t PIVOT(...)` qualifies to `t`).
fn pivot_alias(pivot: &polyglot_sql::expressions::Pivot, source: &Scan) -> String {
    if let Some(alias) = &pivot.alias {
        return alias.name.clone();
    }
    source
        .alias
        .clone()
        .unwrap_or_else(|| source.table_name.clone())
}

/// The output-column name a PIVOT `IN` value contributes (before any aggregate
/// alias suffix). A float or NULL pivot value has no well-defined column name and
/// raises.
fn pivot_value_name(value: &LiteralValue) -> Result<String, ParseError> {
    match value {
        LiteralValue::String(text) => Ok(text.clone()),
        LiteralValue::Integer(number) => Ok(number.to_string()),
        LiteralValue::Boolean(flag) => Ok(flag.to_string()),
        LiteralValue::Float(_) | LiteralValue::Null => Err(ParseError::Unsupported(
            "PIVOT IN value must be a string, integer, or boolean literal".to_string(),
        )),
    }
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
    fn scan_from_table(
        &self,
        select: &Select,
        table: &TableRef,
        key: &str,
    ) -> Result<Scan, ParseError> {
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
        // TABLESAMPLE rides on the scan as canonical Postgres-form text; the
        // FROM-clause renderer appends it verbatim and the fq-emit transpile
        // boundary rewrites it to the source dialect (DuckDB's PERCENT form).
        // Dropping it would silently return full-table rows for a sampled scan.
        if let Some(sample) = table.table_sample.as_ref() {
            scan.sample = Some(render_table_sample(sample)?);
        }
        Ok(scan)
    }
}

/// The visible output name for an unaliased projection that is neither a bare
/// column nor a star: the SQL text of the expression (an unaliased `COUNT(*)`
/// presents to the caller as `COUNT(*)`, an arithmetic `quantity * price` as
/// `quantity * price`). Rendered by polyglot's generator so the name matches the
/// canonical rendering the pushed query carries.
fn output_name_for(item: &Expression) -> Result<String, ParseError> {
    polyglot_sql::generate(item, polyglot_sql::DialectType::PostgreSQL)
        .map_err(|error| ParseError::Unsupported(format!("projection name: {error:?}")))
}

/// Render a table's TABLESAMPLE clause to canonical Postgres-form text
/// (`TABLESAMPLE BERNOULLI (10)`). Postgres admits only BERNOULLI and SYSTEM with
/// a single numeric size; seeded, bucketed, DuckDB `USING SAMPLE`, and
/// ClickHouse-offset samples are not carried and raise rather than drop silently.
fn render_table_sample(sample: &Sample) -> Result<String, ParseError> {
    if sample.seed.is_some()
        || sample.offset.is_some()
        || sample.bucket_numerator.is_some()
        || sample.bucket_denominator.is_some()
        || sample.bucket_field.is_some()
        || sample.is_using_sample
    {
        return Err(ParseError::Unsupported(
            "TABLESAMPLE with SEED, BUCKET, OFFSET, or USING SAMPLE".to_string(),
        ));
    }
    let method = match sample.method {
        SampleMethod::Bernoulli => "BERNOULLI",
        SampleMethod::System => "SYSTEM",
        SampleMethod::Block
        | SampleMethod::Row
        | SampleMethod::Percent
        | SampleMethod::Bucket
        | SampleMethod::Reservoir => {
            return Err(ParseError::Unsupported(format!(
                "TABLESAMPLE method {:?}",
                sample.method
            )));
        }
    };
    let Expression::Literal(literal) = &sample.size else {
        return Err(ParseError::Unsupported(
            "TABLESAMPLE size must be a numeric literal".to_string(),
        ));
    };
    let Literal::Number(size) = literal.as_ref() else {
        return Err(ParseError::Unsupported(
            "TABLESAMPLE size must be a numeric literal".to_string(),
        ));
    };
    Ok(format!("TABLESAMPLE {method} ({size})"))
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
