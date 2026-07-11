//! Canonical signature for a logical subplan. Ports `optimizer/subplan_signature.py`.
//!
//! BYTE-CRITICAL: the canonical JSON string and its sha1, plus
//! `scan_predicate_template`, feed the SQLite learned-catalog PRIMARY KEYs
//! (`group_stats.subject`, `predicate_stats.predicate_template`,
//! `subplan_stats.subplan_signature`). They MUST reproduce the Python bytes
//! exactly, so an existing catalog keeps matching. The manual JSON-string build
//! (separators `", "` / `": "`) mirrors `fq-catalog::stats_catalog::group_key`.
//!
//! A signature identifies a subplan MODULO alias names and constant values.
//!
//! ASCII CAVEAT (identical to `group_key`): identifiers are ASCII in practice, so
//! `serde_json::to_string` matches Python's `json.dumps`. A non-ASCII identifier
//! would diverge (Python `ensure_ascii=True` emits `\uXXXX`, serde raw UTF-8);
//! that divergence is accepted and undocumented workloads do not hit it.

use std::collections::HashMap;

use sha1::{Digest, Sha1};

use fq_plan::expr::{column_refs, BinaryOpType, ColumnRef, Expr};
use fq_plan::logical::{LogicalPlan, Scan};

/// The canonical signature (a lowercase sha1 hex digest) of a logical subplan.
pub fn subplan_signature(node: &LogicalPlan) -> String {
    let alias_map = alias_map(node);
    let mut tables = Vec::new();
    collect_scans(node, &mut tables);
    tables.sort();
    let mut joins = Vec::new();
    collect_join_shapes(node, &alias_map, &mut joins);
    joins.sort();
    let mut filters = Vec::new();
    collect_filter_shapes(node, &alias_map, &mut filters);
    filters.sort();

    // Keys emitted in sorted order (filters, joins, tables) with json.dumps'
    // default separators, so the bytes match Python's sort_keys=True dump.
    let canonical = format!(
        "{{\"filters\": {}, \"joins\": {}, \"tables\": {}}}",
        json_string_list(&filters),
        json_string_list(&joins),
        json_string_list(&tables),
    );
    let mut hasher = Sha1::new();
    hasher.update(canonical.as_bytes());
    format!("{:x}", hasher.finalize())
}

/// A JSON array of strings with the `", "` element separator (json.dumps
/// default), each element escaped exactly as Python's json module does.
fn json_string_list(items: &[String]) -> String {
    let mut parts = Vec::with_capacity(items.len());
    for item in items {
        parts.push(serde_json::to_string(item).expect("string serializes"));
    }
    format!("[{}]", parts.join(", "))
}

/// The constant-neutral TEMPLATE of a scan's pushed filter: its conjunct shapes
/// (operator + alias-neutral base columns, constants dropped), sorted and joined
/// by `&`. `None` when the scan carries no filter. Keys `predicate_stats`.
pub fn scan_predicate_template(scan: &Scan) -> Option<String> {
    let filters = scan.filters.as_ref()?;
    let mut alias_map = HashMap::new();
    alias_map.insert(scan_qualifier(scan), table_name(scan));
    let mut shapes = Vec::new();
    for conjunct in fq_plan::expr::split_conjuncts(filters) {
        shapes.push(conjunct_shape(conjunct, &alias_map));
    }
    shapes.sort();
    Some(shapes.join("&"))
}

/// The group-by column names when every key is a plain QUALIFIED column, else
/// `None` (an expression key has no stable name to key `group_stats` on). Shared
/// by the write provenance, the cost-model read, and the stamp.
pub fn group_column_names(group_by: &[Expr]) -> Option<Vec<String>> {
    let mut names = Vec::with_capacity(group_by.len());
    for key in group_by {
        match key {
            // A qualified column: table set AND non-empty (Python's `not key.table`
            // treats an empty-string qualifier as unqualified, so `Some("")` must
            // decline the whole key list rather than key a learned lookup on it).
            Expr::Column(column) if column.table.as_deref().is_some_and(|t| !t.is_empty()) => {
                names.push(column.column.clone());
            }
            _ => return None,
        }
    }
    Some(names)
}

/// The Python `type(expr).__name__` for an expression node - BYTE-CRITICAL: it
/// feeds `_conjunct_shape`'s operator (for non-BinaryOp conjuncts) and the
/// selectivity provenance TypeName. NOT the snake_case `variant_label`.
pub fn python_class_name(expr: &Expr) -> &'static str {
    match expr {
        Expr::Column(_) => "ColumnRef",
        Expr::Literal { .. } => "Literal",
        Expr::BinaryOp { .. } => "BinaryOp",
        Expr::UnaryOp { .. } => "UnaryOp",
        Expr::FunctionCall { .. } => "FunctionCall",
        Expr::Case { .. } => "CaseExpr",
        Expr::InList { .. } => "InList",
        Expr::Between { .. } => "BetweenExpression",
        Expr::Cast { .. } => "Cast",
        Expr::Window { .. } => "WindowExpr",
        Expr::Extract { .. } => "Extract",
        Expr::Interval { .. } => "Interval",
        Expr::Tuple { .. } => "TupleExpression",
        Expr::Subquery { .. } => "SubqueryExpression",
        Expr::Exists { .. } => "ExistsExpression",
        Expr::InSubquery { .. } => "InSubquery",
        Expr::QuantifiedComparison { .. } => "QuantifiedComparison",
    }
}

/// The qualifier columns use to name this scan: its alias, else its table name.
pub fn scan_qualifier(scan: &Scan) -> String {
    scan.alias
        .clone()
        .unwrap_or_else(|| scan.table_name.clone())
}

/// A scan's alias-independent `ds.schema.table` identity.
fn table_name(scan: &Scan) -> String {
    format!(
        "{}.{}.{}",
        scan.datasource, scan.schema_name, scan.table_name
    )
}

/// Append each scan's qualified table name in the subtree (multiplicity kept).
fn collect_scans(node: &LogicalPlan, names: &mut Vec<String>) {
    if let LogicalPlan::Scan(scan) = node {
        names.push(table_name(scan));
        return;
    }
    for child in node.children() {
        collect_scans(child, names);
    }
}

/// Map each scan's qualifier to its base table name, for alias-neutral rendering.
fn alias_map(node: &LogicalPlan) -> HashMap<String, String> {
    let mut mapping = HashMap::new();
    collect_aliases(node, &mut mapping);
    mapping
}

/// Record every scan's qualifier -> base table name in the subtree.
fn collect_aliases(node: &LogicalPlan, mapping: &mut HashMap<String, String>) {
    if let LogicalPlan::Scan(scan) = node {
        mapping.insert(scan_qualifier(scan), table_name(scan));
        return;
    }
    for child in node.children() {
        collect_aliases(child, mapping);
    }
}

/// A column reference rendered alias-neutrally as `base_table.column`, or
/// `?.column` when its qualifier resolves to no scan in this subtree. The `?`
/// fallback is deliberate: the signature stays lenient, still producing a stable
/// key rather than raising.
fn base_column(reference: &ColumnRef, alias_map: &HashMap<String, String>) -> String {
    let table = reference
        .table
        .as_deref()
        .and_then(|qualifier| alias_map.get(qualifier))
        .map_or("?", String::as_str);
    format!("{}.{}", table, reference.column)
}

/// Append the equi-key shapes of every Join condition in the subtree.
fn collect_join_shapes(
    node: &LogicalPlan,
    alias_map: &HashMap<String, String>,
    shapes: &mut Vec<String>,
) {
    if let LogicalPlan::Join(join) = node {
        if let Some(condition) = &join.condition {
            for conjunct in fq_plan::expr::split_conjuncts(condition) {
                if let Some(shape) = equi_shape(conjunct, alias_map) {
                    shapes.push(shape);
                }
            }
        }
    }
    for child in node.children() {
        collect_join_shapes(child, alias_map, shapes);
    }
}

/// A `col = col` equality as a sorted `a|b` base-column pair, or `None`.
fn equi_shape(conjunct: &Expr, alias_map: &HashMap<String, String>) -> Option<String> {
    let Expr::BinaryOp {
        op: BinaryOpType::Eq,
        left,
        right,
    } = conjunct
    else {
        return None;
    };
    let (Expr::Column(left), Expr::Column(right)) = (&**left, &**right) else {
        return None;
    };
    let mut pair = [base_column(left, alias_map), base_column(right, alias_map)];
    pair.sort();
    Some(pair.join("|"))
}

/// Append the conjunct shapes of every filter predicate in the subtree (a Filter
/// node's predicate and a Scan's folded filters both count).
fn collect_filter_shapes(
    node: &LogicalPlan,
    alias_map: &HashMap<String, String>,
    shapes: &mut Vec<String>,
) {
    if let Some(predicate) = node_predicate(node) {
        for conjunct in fq_plan::expr::split_conjuncts(predicate) {
            shapes.push(conjunct_shape(conjunct, alias_map));
        }
    }
    for child in node.children() {
        collect_filter_shapes(child, alias_map, shapes);
    }
}

/// A node's filter predicate: a Filter's, or a Scan's folded filters.
fn node_predicate(node: &LogicalPlan) -> Option<&Expr> {
    match node {
        LogicalPlan::Filter(filter) => Some(&filter.predicate),
        LogicalPlan::Scan(scan) => scan.filters.as_ref(),
        _ => None,
    }
}

/// One filter conjunct as `op(base_col,base_col,...)` - its operator and the
/// base columns it references (bare-comma separated), constants dropped. For a
/// BinaryOp the operator is its SQL token; for anything else the Python class
/// name (BYTE-CRITICAL).
fn conjunct_shape(conjunct: &Expr, alias_map: &HashMap<String, String>) -> String {
    let operator = match conjunct {
        Expr::BinaryOp { op, .. } => op.value(),
        other => python_class_name(other),
    };
    let mut columns = Vec::new();
    for reference in column_refs(conjunct) {
        columns.push(base_column(reference, alias_map));
    }
    columns.sort();
    format!("{}({})", operator, columns.join(","))
}

#[cfg(test)]
mod tests {
    use super::*;
    use fq_common::DataType;
    use fq_plan::expr::{BinaryOpType, LiteralValue};
    use fq_plan::logical::{Filter, Join, JoinType};

    // --- BYTE-CRITICAL goldens, generated by running the Python module on the
    // same logical shapes (see the task report for the exact python3 command).

    /// duck.main.lineitem alias l JOIN duck.main.orders alias o on
    /// l.l_orderkey = o.o_orderkey, filtered by l.l_shipdate < DATE '1998-09-01'.
    const SIG_CANONICAL: &str = concat!(
        "{\"filters\": [\"<(duck.main.lineitem.l_shipdate)\"], ",
        "\"joins\": [\"duck.main.lineitem.l_orderkey|duck.main.orders.o_orderkey\"], ",
        "\"tables\": [\"duck.main.lineitem\", \"duck.main.orders\"]}"
    );
    const SIG_SHA1: &str = "b70456267895e208b571301d79b92bf9f3f85ddf";
    const BARE_SIG_SHA1: &str = "54c07a96b64470904b3b0defadd0738434ee5ad3";

    fn scan(table: &str, columns: &[&str], alias: Option<&str>, filters: Option<Expr>) -> Scan {
        let mut node = Scan::new(
            "duck",
            "main",
            table,
            columns.iter().map(|c| (*c).to_string()).collect(),
        );
        node.alias = alias.map(str::to_string);
        node.filters = filters;
        node
    }

    fn col(table: &str, name: &str) -> Expr {
        Expr::Column(ColumnRef::new(
            Some(table.to_string()),
            name,
            Some(DataType::Integer),
        ))
    }

    fn eq(left: Expr, right: Expr) -> Expr {
        Expr::BinaryOp {
            op: BinaryOpType::Eq,
            left: Box::new(left),
            right: Box::new(right),
        }
    }

    fn date_cast(value: &str) -> Expr {
        Expr::Cast {
            expr: Box::new(Expr::Literal {
                value: LiteralValue::String(value.to_string()),
                data_type: DataType::Varchar,
            }),
            target_type: "DATE".to_string(),
            data_type: Some(DataType::Date),
        }
    }

    /// The golden signature fixture: Filter over Join, aliases l and o.
    fn golden_plan(alias_left: &str, alias_right: &str, literal: &str) -> LogicalPlan {
        let left = LogicalPlan::Scan(Box::new(scan(
            "lineitem",
            &["l_orderkey", "l_shipdate"],
            Some(alias_left),
            None,
        )));
        let right = LogicalPlan::Scan(Box::new(scan(
            "orders",
            &["o_orderkey"],
            Some(alias_right),
            None,
        )));
        let join = LogicalPlan::Join(Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinType::Inner,
            condition: Some(eq(
                col(alias_left, "l_orderkey"),
                col(alias_right, "o_orderkey"),
            )),
            natural: false,
            using: None,
            estimated_rows: None,
            estimate_defaults: None,
        });
        let predicate = Expr::BinaryOp {
            op: BinaryOpType::Lt,
            left: Box::new(col(alias_left, "l_shipdate")),
            right: Box::new(date_cast(literal)),
        };
        LogicalPlan::Filter(Filter {
            input: Box::new(join),
            predicate,
        })
    }

    #[test]
    fn signature_matches_python_golden() {
        let plan = golden_plan("l", "o", "1998-09-01");
        // Reproduce the canonical string byte-for-byte via the internal build.
        let alias_map = alias_map(&plan);
        let mut tables = Vec::new();
        collect_scans(&plan, &mut tables);
        tables.sort();
        let mut joins = Vec::new();
        collect_join_shapes(&plan, &alias_map, &mut joins);
        joins.sort();
        let mut filters = Vec::new();
        collect_filter_shapes(&plan, &alias_map, &mut filters);
        filters.sort();
        let canonical = format!(
            "{{\"filters\": {}, \"joins\": {}, \"tables\": {}}}",
            json_string_list(&filters),
            json_string_list(&joins),
            json_string_list(&tables),
        );
        assert_eq!(canonical, SIG_CANONICAL);
        assert_eq!(subplan_signature(&plan), SIG_SHA1);
    }

    #[test]
    fn signature_is_alias_and_constant_neutral() {
        let base = subplan_signature(&golden_plan("l", "o", "1998-09-01"));
        // Different aliases, different literal -> identical signature.
        let variant = subplan_signature(&golden_plan("x", "y", "2000-01-01"));
        assert_eq!(base, variant);
        assert_eq!(base, SIG_SHA1);
    }

    #[test]
    fn self_join_keeps_table_multiplicity() {
        let left = LogicalPlan::Scan(Box::new(scan("lineitem", &["l_orderkey"], Some("a"), None)));
        let right = LogicalPlan::Scan(Box::new(scan("lineitem", &["l_orderkey"], Some("b"), None)));
        let mut tables = Vec::new();
        let join = LogicalPlan::Join(Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: JoinType::Inner,
            condition: Some(eq(col("a", "l_orderkey"), col("b", "l_orderkey"))),
            natural: false,
            using: None,
            estimated_rows: None,
            estimate_defaults: None,
        });
        collect_scans(&join, &mut tables);
        assert_eq!(tables.len(), 2);
    }

    #[test]
    fn bare_scan_signature_matches_python_golden() {
        let plan = LogicalPlan::Scan(Box::new(scan(
            "nation",
            &["n_nationkey", "n_name"],
            None,
            None,
        )));
        assert_eq!(subplan_signature(&plan), BARE_SIG_SHA1);
    }

    #[test]
    fn unresolved_qualifier_renders_question_mark() {
        let mut empty = HashMap::new();
        empty.insert("known".to_string(), "duck.main.t".to_string());
        let reference = ColumnRef::new(Some("missing".to_string()), "c", None);
        assert_eq!(base_column(&reference, &empty), "?.c");
    }

    #[test]
    fn scan_predicate_template_matches_python_golden() {
        // (l.l_shipdate >= '1994-01-01') AND (l.l_shipdate < '1995-01-01')
        //   AND (l.l_discount > 0.05)
        let ge = Expr::BinaryOp {
            op: BinaryOpType::Gte,
            left: Box::new(col("l", "l_shipdate")),
            right: Box::new(Expr::Literal {
                value: LiteralValue::String("1994-01-01".into()),
                data_type: DataType::Varchar,
            }),
        };
        let lt = Expr::BinaryOp {
            op: BinaryOpType::Lt,
            left: Box::new(col("l", "l_shipdate")),
            right: Box::new(Expr::Literal {
                value: LiteralValue::String("1995-01-01".into()),
                data_type: DataType::Varchar,
            }),
        };
        let gt = Expr::BinaryOp {
            op: BinaryOpType::Gt,
            left: Box::new(col("l", "l_discount")),
            right: Box::new(Expr::Literal {
                value: LiteralValue::Float(0.05),
                data_type: DataType::Double,
            }),
        };
        let filters = fq_plan::expr::combine_and(vec![ge, lt, gt]).unwrap();
        let node = scan(
            "lineitem",
            &["l_shipdate", "l_discount"],
            Some("l"),
            Some(filters),
        );
        assert_eq!(
            scan_predicate_template(&node).unwrap(),
            "<(duck.main.lineitem.l_shipdate)&>(duck.main.lineitem.l_discount)&>=(duck.main.lineitem.l_shipdate)"
        );
    }

    #[test]
    fn scan_predicate_template_none_without_filter() {
        let node = scan("nation", &["n_name"], None, None);
        assert_eq!(scan_predicate_template(&node), None);
    }

    #[test]
    fn group_column_names_none_on_expression_key() {
        let plain = vec![col("t", "a"), col("t", "b")];
        assert_eq!(
            group_column_names(&plain),
            Some(vec!["a".to_string(), "b".to_string()])
        );
        // An unqualified column or a non-column key -> None.
        let unqualified = vec![Expr::Column(ColumnRef::new(None, "a", None))];
        assert_eq!(group_column_names(&unqualified), None);
    }

    #[test]
    fn python_class_name_maps_variants() {
        assert_eq!(
            python_class_name(&Expr::Between {
                value: Box::new(col("t", "a")),
                lower: Box::new(col("t", "b")),
                upper: Box::new(col("t", "c")),
            }),
            "BetweenExpression"
        );
        assert_eq!(
            python_class_name(&Expr::Case {
                when_clauses: vec![],
                else_result: None,
            }),
            "CaseExpr"
        );
    }
}
