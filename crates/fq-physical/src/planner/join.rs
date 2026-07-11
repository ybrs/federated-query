//! Join lowering: remote-join candidacy, hash-vs-nested-loop selection, equi-key
//! extraction + orientation, build-side choice, and dynamic-filter marking. Ports
//! the `_plan_join` family of `physical_planner.py`.

use std::collections::{BTreeMap, HashSet};

use fq_catalog::datasource::DataSourceCapability;
use fq_optimize::pushdown::is_equi_predicate;
use fq_plan::expr::{split_conjuncts, BinaryOpType, ColumnRef, Expr, LiteralValue};
use fq_plan::logical::{Join, JoinType, LogicalPlan, Scan, SubqueryScan};
use fq_plan::physical::{
    BuildSide, PhysicalHashJoin, PhysicalNestedLoopJoin, PhysicalPlan, PhysicalRemoteJoin,
    PhysicalScan,
};

use super::orient::{larger_estimated_side, Side};
use super::PhysicalPlanner;
use crate::error::PhysicalError;
use crate::single_source::same_source;

/// Relation qualifiers and bare column names exposed by each join side (a plain
/// struct; the Python `_JoinSides` StateModel ceremony is retired).
struct JoinSides {
    left_aliases: HashSet<String>,
    right_aliases: HashSet<String>,
    left_names: HashSet<String>,
    right_names: HashSet<String>,
}

impl PhysicalPlanner {
    /// Plan a join: annotate bare-scan sides, try remote pushdown, then a hash
    /// join, else a nested loop.
    pub(super) fn plan_join(&mut self, join: &Join) -> Result<PhysicalPlan, PhysicalError> {
        let join = self.annotate_join_scans(join)?;
        let left = self.plan_node(&join.left)?;
        let right = self.plan_node(&join.right)?;

        if self.is_remote_join_candidate(&join, &left, &right) {
            return Ok(build_remote_join(&join, left, right));
        }
        if join.condition.is_none() && (join.natural || join.using.is_some()) {
            // A cross-source NATURAL/USING join whose name-match equality the merge
            // engine cannot resolve; a conditionless nested loop would be a silent
            // Cartesian product.
            return Err(PhysicalError::CrossSourceNaturalUsingJoin);
        }
        match extract_join_keys(join.condition.as_ref()) {
            Some((left_keys, right_keys)) if !left_keys.is_empty() => {
                build_hash_join(&join, &left_keys, &right_keys, left, right)
            }
            _ => Ok(build_nested_loop(&join, left, right)),
        }
    }

    /// Fill estimate + join-key NDVs on bare-Scan sides that carry none (join
    /// ordering only annotates INNER regions). Without a cost model, or for
    /// non-Scan sides, nothing changes. Ports `_annotate_join_scans`.
    fn annotate_join_scans(&mut self, join: &Join) -> Result<Join, PhysicalError> {
        if self.cost_model().is_none() || join.condition.is_none() {
            return Ok(join.clone());
        }
        let condition = join.condition.clone().expect("condition present");
        let mut annotated = join.clone();
        if let Some(scan) = self.annotated_scan(&join.left, &condition)? {
            annotated.left = Box::new(LogicalPlan::Scan(Box::new(scan)));
        }
        if let Some(scan) = self.annotated_scan(&join.right, &condition)? {
            annotated.right = Box::new(LogicalPlan::Scan(Box::new(scan)));
        }
        Ok(annotated)
    }

    /// A join side as an estimate+NDV-annotated Scan, or None to keep it as-is.
    /// Ports `_annotated_scan`. Borrow discipline: the NDV loop (all `borrow()`s)
    /// runs and releases BEFORE the estimate `borrow_mut()`.
    fn annotated_scan(
        &mut self,
        node: &LogicalPlan,
        condition: &Expr,
    ) -> Result<Option<Scan>, PhysicalError> {
        let LogicalPlan::Scan(scan) = node else {
            return Ok(None);
        };
        if scan.estimated_rows.is_some() {
            return Ok(None);
        }
        let mut ndv_map: BTreeMap<String, i64> = BTreeMap::new();
        if let Some(cost_model) = self.cost_model() {
            let scan_as_plan = LogicalPlan::Scan(scan.clone());
            let borrowed = cost_model.borrow();
            for reference in side_key_refs(scan, condition) {
                if let Some(value) = borrowed.column_ndv(&scan_as_plan, reference)? {
                    ndv_map.insert(reference.column.clone(), value);
                }
            }
        }
        let estimated = self.scan_estimated_rows(scan)?;
        if estimated.is_none() && ndv_map.is_empty() {
            return Ok(None);
        }
        let mut annotated = (**scan).clone();
        annotated.estimated_rows = estimated;
        annotated.column_ndv = if ndv_map.is_empty() {
            None
        } else {
            Some(ndv_map)
        };
        Ok(Some(annotated))
    }

    /// Whether a join can run on one source: INNER/LEFT/RIGHT with a condition and
    /// both sides plain scans on the same datasource that supports JOINS. Ports
    /// `_is_remote_join_candidate` (datasource resolved by name, not off the node).
    fn is_remote_join_candidate(
        &self,
        join: &Join,
        left: &PhysicalPlan,
        right: &PhysicalPlan,
    ) -> bool {
        if !matches!(
            join.join_type,
            JoinType::Inner | JoinType::Left | JoinType::Right
        ) || join.condition.is_none()
        {
            return false;
        }
        let (PhysicalPlan::Scan(left_scan), PhysicalPlan::Scan(right_scan)) = (left, right) else {
            return false;
        };
        if !same_source(Some(&left_scan.datasource), Some(&right_scan.datasource)) {
            return false;
        }
        let Some(datasource) = self.catalog().get_datasource(&left_scan.datasource) else {
            return false;
        };
        if !datasource.supports_capability(DataSourceCapability::Joins) {
            return false;
        }
        if left_scan.group_by.is_some() || left_scan.aggregates.is_some() {
            return false;
        }
        if right_scan.group_by.is_some() || right_scan.aggregates.is_some() {
            return false;
        }
        extract_join_keys(join.condition.as_ref()).is_some()
    }
}

/// Build a hash join: orient the equi-keys onto their sides (may RAISE), pick the
/// build side, then mark the probe scan's dynamic filter. Ports
/// `_plan_hash_join_or_none`.
fn build_hash_join(
    join: &Join,
    left_keys: &[ColumnRef],
    right_keys: &[ColumnRef],
    left: PhysicalPlan,
    right: PhysicalPlan,
) -> Result<PhysicalPlan, PhysicalError> {
    let (left_keys, right_keys) = orient_join_keys(left_keys, right_keys, join)?;
    let build_side = choose_build_side(join.join_type, &left, &right);
    let mut hash_join = PhysicalHashJoin {
        left: Box::new(left),
        right: Box::new(right),
        join_type: join.join_type,
        left_keys: left_keys.into_iter().map(Expr::Column).collect(),
        right_keys: right_keys.into_iter().map(Expr::Column).collect(),
        build_side,
        estimated_rows: join.estimated_rows,
        estimate_defaults: join.estimate_defaults.clone(),
    };
    mark_dynamic_filter(&mut hash_join);
    Ok(PhysicalPlan::HashJoin(hash_join))
}

/// Build a single-source `PhysicalRemoteJoin`, carrying either side's pushed
/// ORDER BY. Ports `_try_plan_remote_join` (candidacy is checked by the caller).
fn build_remote_join(join: &Join, left: PhysicalPlan, right: PhysicalPlan) -> PhysicalPlan {
    let (order_by_keys, order_by_ascending, order_by_nulls) = pushed_order_by(&left, &right);
    PhysicalPlan::RemoteJoin(Box::new(PhysicalRemoteJoin {
        left: Box::new(left),
        right: Box::new(right),
        join_type: join.join_type,
        condition: join.condition.clone().expect("candidate has a condition"),
        group_by: None,
        grouping_sets: None,
        aggregates: None,
        output_names: None,
        distinct: false,
        order_by_keys,
        order_by_ascending,
        order_by_nulls,
    }))
}

/// Prefer the LEFT scan's pushed ORDER BY, else the RIGHT scan's (only when left
/// had none). Non-scan sides carry no pushed order.
type PushedOrderBy = (
    Option<Vec<Expr>>,
    Option<Vec<bool>>,
    Option<Vec<Option<fq_plan::expr::NullsOrder>>>,
);
fn pushed_order_by(left: &PhysicalPlan, right: &PhysicalPlan) -> PushedOrderBy {
    if let PhysicalPlan::Scan(scan) = left {
        if scan.order_by_keys.is_some() {
            return (
                scan.order_by_keys.clone(),
                scan.order_by_ascending.clone(),
                scan.order_by_nulls.clone(),
            );
        }
    }
    if let PhysicalPlan::Scan(scan) = right {
        if scan.order_by_keys.is_some() {
            return (
                scan.order_by_keys.clone(),
                scan.order_by_ascending.clone(),
                scan.order_by_nulls.clone(),
            );
        }
    }
    (None, None, None)
}

/// Fall back to a nested-loop join (no equi-keys, or no join condition). Ports
/// `_plan_nested_loop`.
fn build_nested_loop(join: &Join, left: PhysicalPlan, right: PhysicalPlan) -> PhysicalPlan {
    PhysicalPlan::NestedLoopJoin(PhysicalNestedLoopJoin {
        left: Box::new(left),
        right: Box::new(right),
        join_type: join.join_type,
        condition: join.condition.clone(),
        estimated_rows: join.estimated_rows,
        estimate_defaults: join.estimate_defaults.clone(),
    })
}

/// The condition's equi-key column refs this scan owns (matched by its alias or
/// table name). Ports `_side_key_refs`.
fn side_key_refs<'a>(scan: &Scan, condition: &'a Expr) -> Vec<&'a ColumnRef> {
    let mut names: HashSet<&str> = HashSet::new();
    names.insert(scan.table_name.as_str());
    if let Some(alias) = &scan.alias {
        names.insert(alias.as_str());
    }
    let mut refs = Vec::new();
    for conjunct in split_conjuncts(condition) {
        if !is_equi_predicate(conjunct) {
            continue;
        }
        let Expr::BinaryOp { left, right, .. } = conjunct else {
            continue;
        };
        for operand in [left.as_ref(), right.as_ref()] {
            if let Expr::Column(reference) = operand {
                if reference
                    .table
                    .as_deref()
                    .is_some_and(|table| names.contains(table))
                {
                    refs.push(reference);
                }
            }
        }
    }
    refs
}

/// Extract `(left_keys, right_keys)` equi-join columns from a condition, or None
/// when it is not an equi-join. Recurses the top-level AND chain. Ports
/// `_extract_join_keys`.
fn extract_join_keys(condition: Option<&Expr>) -> Option<(Vec<ColumnRef>, Vec<ColumnRef>)> {
    let condition = condition?;
    match condition {
        Expr::BinaryOp {
            op: BinaryOpType::Eq,
            left,
            right,
        } => match (left.as_ref(), right.as_ref()) {
            (Expr::Column(l), Expr::Column(r)) => Some((vec![l.clone()], vec![r.clone()])),
            _ => None,
        },
        Expr::BinaryOp {
            op: BinaryOpType::And,
            left,
            right,
        } => {
            let (mut left_keys, mut right_keys) = extract_join_keys(Some(left))?;
            let (more_left, more_right) = extract_join_keys(Some(right))?;
            left_keys.extend(more_left);
            right_keys.extend(more_right);
            Some((left_keys, right_keys))
        }
        // Not an equi-join (a size/shape default, not a node-handling gap).
        _ => None,
    }
}

/// Assign each equality's columns to the join side that owns them. Ports
/// `_orient_join_keys`.
fn orient_join_keys(
    left_keys: &[ColumnRef],
    right_keys: &[ColumnRef],
    join: &Join,
) -> Result<(Vec<ColumnRef>, Vec<ColumnRef>), PhysicalError> {
    let sides = JoinSides {
        left_aliases: relation_aliases(&join.left),
        right_aliases: relation_aliases(&join.right),
        left_names: join.left.schema().into_iter().collect(),
        right_names: join.right.schema().into_iter().collect(),
    };
    let mut oriented_left = Vec::with_capacity(left_keys.len());
    let mut oriented_right = Vec::with_capacity(right_keys.len());
    for (first, second) in left_keys.iter().zip(right_keys.iter()) {
        let (first, second) = orient_key_pair(first, second, &sides)?;
        oriented_left.push(first);
        oriented_right.push(second);
    }
    Ok((oriented_left, oriented_right))
}

/// Place one equality's columns onto the (left, right) sides: qualifiers first,
/// then bare names (which may RAISE). Ports `_orient_key_pair`.
fn orient_key_pair(
    first: &ColumnRef,
    second: &ColumnRef,
    sides: &JoinSides,
) -> Result<(ColumnRef, ColumnRef), PhysicalError> {
    if let Some(pair) = orient_by_alias(first, second, sides) {
        return Ok(pair);
    }
    orient_by_name(first, second, sides)
}

/// Orient using the columns' table qualifiers, if both resolve. Ports
/// `_orient_by_alias`.
fn orient_by_alias(
    first: &ColumnRef,
    second: &ColumnRef,
    sides: &JoinSides,
) -> Option<(ColumnRef, ColumnRef)> {
    let first_table = first.table.as_deref();
    let second_table = second.table.as_deref();
    if in_set(first_table, &sides.left_aliases) && in_set(second_table, &sides.right_aliases) {
        return Some((first.clone(), second.clone()));
    }
    if in_set(first_table, &sides.right_aliases) && in_set(second_table, &sides.left_aliases) {
        return Some((second.clone(), first.clone()));
    }
    None
}

/// Orient using bare column names when qualifiers do not resolve; RAISE when
/// neither side resolves (keeping the original order would key the join on
/// mismatched columns). Ports `_orient_by_name`.
fn orient_by_name(
    first: &ColumnRef,
    second: &ColumnRef,
    sides: &JoinSides,
) -> Result<(ColumnRef, ColumnRef), PhysicalError> {
    if sides.left_names.contains(&first.column) && sides.right_names.contains(&second.column) {
        return Ok((first.clone(), second.clone()));
    }
    if sides.right_names.contains(&first.column) && sides.left_names.contains(&second.column) {
        return Ok((second.clone(), first.clone()));
    }
    Err(PhysicalError::UnorientableJoinKeys {
        first: first.column.clone(),
        second: second.column.clone(),
    })
}

/// Whether an optional qualifier is present in a set of aliases.
fn in_set(qualifier: Option<&str>, aliases: &HashSet<String>) -> bool {
    qualifier.is_some_and(|name| aliases.contains(name))
}

/// The relation aliases/names a subtree EXPOSES. Stops at a SubqueryScan boundary
/// (its alias is exposed, its internals are not). Ports `_collect_relation_aliases`
/// - deliberately NOT the decorrelator's inner-alias walk (which recurses into
///   subqueries); the two answer different questions.
fn relation_aliases(plan: &LogicalPlan) -> HashSet<String> {
    let mut aliases = HashSet::new();
    collect_relation_aliases(plan, &mut aliases);
    aliases
}

/// Recursive helper for `relation_aliases`.
fn collect_relation_aliases(plan: &LogicalPlan, aliases: &mut HashSet<String>) {
    match plan {
        LogicalPlan::Scan(scan) => {
            aliases.insert(
                scan.alias
                    .clone()
                    .unwrap_or_else(|| scan.table_name.clone()),
            );
        }
        LogicalPlan::SubqueryScan(SubqueryScan { alias, .. }) => {
            aliases.insert(alias.clone());
        }
        other => {
            for child in other.children() {
                collect_relation_aliases(child, aliases);
            }
        }
    }
}

/// Choose the hash-join build side. Ports `_choose_build_side`: SEMI/ANTI/outer
/// fix the built (right) side; otherwise build the SMALLER estimated side, else
/// fall back to the side with a selective equality filter.
fn choose_build_side(join_type: JoinType, left: &PhysicalPlan, right: &PhysicalPlan) -> BuildSide {
    if join_type != JoinType::Inner {
        return BuildSide::Right;
    }
    match larger_estimated_side(left, right) {
        Some(Side::Right) => return BuildSide::Left,
        Some(Side::Left) => return BuildSide::Right,
        None => {}
    }
    if has_selective_filter(left) && !has_selective_filter(right) {
        BuildSide::Left
    } else {
        BuildSide::Right
    }
}

/// Whether the plan's underlying scan filters on a `column = literal`. Ports
/// `_has_selective_filter`.
fn has_selective_filter(plan: &PhysicalPlan) -> bool {
    match find_scan(plan) {
        Some(scan) => filter_has_literal_equality(scan.filters.as_ref()),
        None => false,
    }
}

/// Descend single-child wrappers to the underlying scan, if any. Ports
/// `_find_scan`.
fn find_scan(plan: &PhysicalPlan) -> Option<&PhysicalScan> {
    let mut current = plan;
    loop {
        if let PhysicalPlan::Scan(scan) = current {
            return Some(scan);
        }
        let children = current.children();
        if children.len() != 1 {
            return None;
        }
        current = children[0];
    }
}

/// Whether a predicate contains a `column = literal` conjunct. Ports
/// `_filter_has_literal_equality` + `_and_has_literal_equality`.
fn filter_has_literal_equality(predicate: Option<&Expr>) -> bool {
    let Some(Expr::BinaryOp { op, left, right }) = predicate else {
        return false;
    };
    match op {
        BinaryOpType::Eq => is_column_literal_eq(left, right),
        BinaryOpType::And => {
            filter_has_literal_equality(Some(left)) || filter_has_literal_equality(Some(right))
        }
        _ => false,
    }
}

/// Whether an equality compares a column reference to a literal (either order).
/// Ports `_is_column_literal_eq`. A NULL literal still counts as a literal.
fn is_column_literal_eq(left: &Expr, right: &Expr) -> bool {
    matches!(
        (left, right),
        (Expr::Column(_), Expr::Literal { .. }) | (Expr::Literal { .. }, Expr::Column(_))
    ) && !matches!(
        (left, right),
        (
            Expr::Literal {
                value: LiteralValue::Null,
                ..
            },
            Expr::Literal { .. }
        )
    )
}

/// Mark the probe-side scan that will receive a runtime IN filter. Ports
/// `_mark_dynamic_filter`: INNER single-key joins only, and only when the BUILD
/// side can produce its keys standalone (a plain scan or a collapsed remote
/// query). Borrows are sequenced so no immutable read overlaps the probe mutation.
fn mark_dynamic_filter(hash_join: &mut PhysicalHashJoin) {
    if hash_join.join_type != JoinType::Inner {
        return;
    }
    // The probe is the side that is NOT the build side.
    let probe_is_right = matches!(hash_join.build_side, BuildSide::Left);
    let keys = {
        let probe_keys = if probe_is_right {
            &hash_join.right_keys
        } else {
            &hash_join.left_keys
        };
        if probe_keys.len() != 1 {
            return;
        }
        probe_keys.clone()
    };
    {
        let build_side_node = match hash_join.build_side {
            BuildSide::Right => hash_join.right.as_ref(),
            BuildSide::Left => hash_join.left.as_ref(),
        };
        if !matches!(
            build_side_node,
            PhysicalPlan::Scan(_) | PhysicalPlan::RemoteQuery(_)
        ) {
            return;
        }
    }
    let probe_node = if probe_is_right {
        hash_join.right.as_mut()
    } else {
        hash_join.left.as_mut()
    };
    if let PhysicalPlan::Scan(scan) = probe_node {
        scan.dynamic_filter_keys = Some(keys);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fq_common::DataType;

    fn scan_node(datasource: &str, table: &str, alias: &str, rows: Option<u64>) -> PhysicalPlan {
        let scan = PhysicalScan {
            datasource: datasource.to_string(),
            schema_name: "public".to_string(),
            table_name: table.to_string(),
            columns: vec!["k".to_string()],
            filters: None,
            sample: None,
            group_by: None,
            grouping_sets: None,
            aggregates: None,
            output_names: None,
            alias: Some(alias.to_string()),
            limit: None,
            offset: 0,
            order_by_keys: None,
            order_by_ascending: None,
            order_by_nulls: None,
            distinct: false,
            dynamic_filter_keys: None,
            estimated_rows: rows,
            column_ndv: None,
            seeded_schema: None,
        };
        PhysicalPlan::Scan(Box::new(scan))
    }

    #[test]
    fn inner_builds_the_smaller_estimated_side() {
        // small (400) on the left, big (800000) on the right: build the smaller.
        let small = scan_node("duck", "supplier", "s", Some(400));
        let big = scan_node("duck", "partsupp", "ps", Some(800_000));
        assert_eq!(
            choose_build_side(JoinType::Inner, &small, &big),
            BuildSide::Left
        );
        // Swap the sides: the smaller (now right) is built.
        let small = scan_node("duck", "supplier", "s", Some(400));
        let big = scan_node("duck", "partsupp", "ps", Some(800_000));
        assert_eq!(
            choose_build_side(JoinType::Inner, &big, &small),
            BuildSide::Right
        );
    }

    #[test]
    fn inner_tie_falls_back_to_structural_choice() {
        // Equal estimates: no cardinality winner, no selective filter -> right.
        let left = scan_node("duck", "a", "a", Some(1000));
        let right = scan_node("duck", "b", "b", Some(1000));
        assert_eq!(
            choose_build_side(JoinType::Inner, &left, &right),
            BuildSide::Right
        );
    }

    #[test]
    fn inner_missing_estimate_falls_back_to_structural() {
        let left = scan_node("duck", "a", "a", Some(1000));
        let right = scan_node("duck", "b", "b", None);
        assert_eq!(
            choose_build_side(JoinType::Inner, &left, &right),
            BuildSide::Right
        );
    }

    #[test]
    fn semi_join_build_side_is_fixed_right() {
        // SEMI: the preserved left is the probe, the right is always the build.
        let big = scan_node("duck", "li", "l", Some(6_000_000));
        let small = scan_node("duck", "part", "p", Some(200));
        assert_eq!(
            choose_build_side(JoinType::Semi, &big, &small),
            BuildSide::Right
        );
    }

    #[test]
    fn selective_filter_side_becomes_build_when_estimates_tie() {
        // Give the left a `region = 'x'` filter; equal (absent) estimates.
        let PhysicalPlan::Scan(mut left) = scan_node("duck", "a", "a", None) else {
            unreachable!()
        };
        left.filters = Some(Expr::BinaryOp {
            op: BinaryOpType::Eq,
            left: Box::new(Expr::Column(ColumnRef::new(
                Some("a".to_string()),
                "region",
                Some(DataType::Varchar),
            ))),
            right: Box::new(Expr::Literal {
                value: LiteralValue::String("x".to_string()),
                data_type: DataType::Varchar,
            }),
        });
        let left = PhysicalPlan::Scan(left);
        let right = scan_node("duck", "b", "b", None);
        assert_eq!(
            choose_build_side(JoinType::Inner, &left, &right),
            BuildSide::Left
        );
    }
}
