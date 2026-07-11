//! The cost model: the provenance-carrying cardinality estimator. Ports
//! `optimizer/cost.py`. Clean-Rust-portable (the provenance STRINGS are cosmetic;
//! only gap PRESENCE gates decisions).
//!
//! `estimate` is an EXHAUSTIVE per-node dispatch: every `LogicalPlan` variant has
//! exactly one arm, and `Explain` RAISES (`NoRule`) rather than defaulting.

mod aggregate;
mod join;
mod ordinal;
mod scan;
mod selectivity;

use std::collections::HashMap;

use fq_common::CostConfig;
use fq_plan::expr::{ColumnRef, Expr};
use fq_plan::logical::{Cte, LogicalPlan, Scan, SetOpKind};

use crate::error::{EstimateError, Result};
use crate::estimate_defaults::{add_rows, combine_defaults, min_rows, CardinalityEstimate};
use crate::statistics::StatisticsCollector;

/// The cost model. `config` is carried for the physical-costing seam (cpu/io/
/// network costs) and is UNUSED by estimation; `stats` (None disables all
/// catalog-derived estimates); `cte_bodies` is the per-walk CTE registry.
pub struct CostModel {
    config: CostConfig,
    stats: Option<StatisticsCollector>,
    cte_bodies: HashMap<String, LogicalPlan>,
}

impl CostModel {
    /// Build a cost model over an optional statistics collector.
    pub fn new(config: CostConfig, stats: Option<StatisticsCollector>) -> Self {
        Self {
            config,
            stats,
            cte_bodies: HashMap::new(),
        }
    }

    /// The cost configuration (the physical-costing seam; unused by estimation).
    pub fn config(&self) -> &CostConfig {
        &self.config
    }

    /// Drop every registered CTE body - called at the START of a plan walk so a
    /// previous query's same-named CTE can never feed this one's estimates.
    pub fn reset_cte_registry(&mut self) {
        self.cte_bodies.clear();
    }

    /// Make a CTE's body available to `CteRef` estimation by name. A RECURSIVE
    /// body references its own name and would loop the estimator, so it is NOT
    /// registered - its references estimate as UNKNOWN.
    pub fn register_cte(&mut self, cte: &Cte) {
        if !cte.recursive {
            self.cte_bodies
                .insert(cte.name.clone(), (*cte.cte_plan).clone());
        }
    }

    /// Cardinality estimate of a subtree, carrying the provenance of every gap.
    /// EXHAUSTIVE: every node type has one arm; `Explain` RAISES.
    pub fn estimate(&mut self, plan: &LogicalPlan) -> Result<CardinalityEstimate> {
        match plan {
            LogicalPlan::Scan(scan) => self.estimate_scan(scan),
            LogicalPlan::Filter(filter) => self.estimate_filter(filter),
            LogicalPlan::Projection(node) => self.estimate(&node.input),
            LogicalPlan::Sort(node) => self.estimate(&node.input),
            LogicalPlan::SubqueryScan(node) => self.estimate(&node.input),
            LogicalPlan::Join(join) => self.estimate_join(join),
            LogicalPlan::Aggregate(aggregate) => self.estimate_aggregate(aggregate),
            LogicalPlan::Limit(node) => self.estimate_limit(node),
            LogicalPlan::Union(node) => self.estimate_union(node),
            LogicalPlan::SetOperation(node) => self.estimate_setop(node),
            LogicalPlan::Cte(cte) => {
                self.register_cte(cte);
                self.estimate(&cte.child)
            }
            LogicalPlan::CteRef(node) => self.estimate_cte_ref(&node.name),
            LogicalPlan::Values(node) => Ok(CardinalityEstimate::known(
                node.rows.len() as u64,
                Vec::new(),
            )),
            LogicalPlan::SingleRowGuard(node) => self.estimate(&node.input),
            LogicalPlan::GroupedLimit(node) => self.estimate(&node.input),
            LogicalPlan::LateralJoin(node) => {
                let left = self.estimate(&node.left)?;
                Ok(CardinalityEstimate::unknown(combine_defaults(
                    &[&left],
                    &["lateral_multiplicity".to_string()],
                )))
            }
            LogicalPlan::Explain(_) => Err(EstimateError::NoRule("Explain")),
        }
    }

    /// LIMIT caps the input estimate at `offset + limit`. The cap is a literal, so
    /// it bounds the output even over an UNKNOWN input; an OFFSET with no row cap
    /// leaves the input bound unchanged.
    fn estimate_limit(&mut self, node: &fq_plan::logical::Limit) -> Result<CardinalityEstimate> {
        let input = self.estimate(&node.input)?;
        let cap = node.limit.map(|limit| node.offset.saturating_add(limit));
        let rows = match cap {
            Some(cap) => min_rows(input.rows, Some(cap)),
            None => input.rows,
        };
        Ok(CardinalityEstimate::of(rows, input.defaults_used))
    }

    /// A UNION's upper bound: the sum of its inputs. Any unknown branch makes the
    /// sum unknown (DISTINCT only shrinks a concatenation, so the sum bounds it).
    fn estimate_union(&mut self, node: &fq_plan::logical::Union) -> Result<CardinalityEstimate> {
        let mut total = Some(0u64);
        let mut parents = Vec::with_capacity(node.inputs.len());
        for input in &node.inputs {
            let estimate = self.estimate(input)?;
            total = add_rows(total, estimate.rows);
            parents.push(estimate);
        }
        let parent_refs: Vec<&CardinalityEstimate> = parents.iter().collect();
        Ok(CardinalityEstimate::of(
            total,
            combine_defaults(&parent_refs, &[]),
        ))
    }

    /// Set-operation bounds: UNION sums, INTERSECT takes the smaller side, EXCEPT
    /// keeps at most the left side.
    fn estimate_setop(
        &mut self,
        node: &fq_plan::logical::SetOperation,
    ) -> Result<CardinalityEstimate> {
        let left = self.estimate(&node.left)?;
        let right = self.estimate(&node.right)?;
        let rows = match node.kind {
            SetOpKind::Union => add_rows(left.rows, right.rows),
            SetOpKind::Intersect => min_rows(left.rows, right.rows),
            SetOpKind::Except => left.rows,
        };
        Ok(CardinalityEstimate::of(
            rows,
            combine_defaults(&[&left, &right], &[]),
        ))
    }

    /// A CTE reference estimates its registered BODY (a real subtree with real
    /// statistics). An unregistered name (a recursive CTE, or an estimate outside
    /// a registering walk) is honestly UNKNOWN.
    fn estimate_cte_ref(&mut self, name: &str) -> Result<CardinalityEstimate> {
        if let Some(body) = self.cte_bodies.get(name).cloned() {
            return self.estimate(&body);
        }
        Ok(CardinalityEstimate::unknown(vec![format!(
            "row_count(cte {name})"
        )]))
    }

    /// C_out: the sum of every join's estimated output rows in a subtree. An
    /// UNKNOWN join contributes nothing. THE SEAM where a locality/network term is
    /// later added (join-ordering milestone) without touching the enumerator.
    pub fn join_tree_cost(&mut self, plan: &LogicalPlan) -> Result<f64> {
        let mut total = 0.0;
        if let LogicalPlan::Join(_) = plan {
            if let Some(rows) = self.estimate(plan)?.rows {
                total += rows as f64;
            }
        }
        for child in plan.children() {
            total += self.join_tree_cost(child)?;
        }
        Ok(total)
    }

    /// The base NDV of a qualified column resolved inside a subtree, from source
    /// statistics; `None` when the owner is opaque or has no NDV. RAISES when the
    /// column resolves to no relation. Entry for the join-ordering estimator.
    pub fn column_ndv(&self, subtree: &LogicalPlan, reference: &ColumnRef) -> Result<Option<i64>> {
        let qualifier = reference.table.as_deref().unwrap_or_default();
        let owner = self.find_relation(subtree, qualifier)?.ok_or_else(|| {
            EstimateError::NoOwningRelation {
                relation: qualifier.to_string(),
                column: reference.column.clone(),
            }
        })?;
        self.owner_column_ndv(owner, &reference.column)
    }

    /// Tracked selectivity of one predicate with NO table statistics in scope
    /// (cross-relation residual conjuncts). Entry for the join-ordering estimator.
    // Takes &self for API symmetry with column_ndv / group_key_dimension (the
    // three join-ordering entry points), though this one needs no state.
    #[allow(clippy::unused_self)]
    pub fn conjunct_selectivity(
        &self,
        predicate: &Expr,
        target: &str,
    ) -> Result<(Option<f64>, Vec<String>)> {
        Ok(selectivity::tracked_selectivity(predicate, None, target))
    }

    /// The (owner relation, NDV) a GROUP BY key resolves to over its input. The
    /// owner is `None` when the key is not a qualified column or resolves to no
    /// relation; the NDV falls back to the owner's ROW COUNT as an upper bound
    /// when the column has no histogram - a widening CONFINED to this dim-shipping
    /// gate. Entry for the dim-shipping gate.
    pub fn group_key_dimension<'a>(
        &self,
        input: &'a LogicalPlan,
        key: &Expr,
    ) -> Result<(Option<&'a LogicalPlan>, Option<i64>)> {
        let Expr::Column(column) = key else {
            return Ok((None, None));
        };
        let Some(qualifier) = column.table.as_deref() else {
            return Ok((None, None));
        };
        let Some(owner) = self.find_relation(input, qualifier)? else {
            return Ok((None, None));
        };
        Ok((Some(owner), self.bounded_owner_ndv(owner, &column.column)?))
    }

    // --- shared relation resolution / NDV (join + group estimation) ------------

    /// The relation node owning a qualifier in a subtree, or `None`. RAISES on a
    /// duplicate qualifier: resolving a key against an ambiguous alias would
    /// silently pick a side and mis-estimate.
    // A method (not a free fn) to group with the resolution/NDV API the join and
    // aggregate estimators call through self; it delegates to a free walker.
    #[allow(clippy::unused_self)]
    pub(crate) fn find_relation<'a>(
        &self,
        node: &'a LogicalPlan,
        qualifier: &str,
    ) -> Result<Option<&'a LogicalPlan>> {
        let mut matches = Vec::new();
        collect_relations(node, qualifier, &mut matches);
        if matches.len() > 1 {
            return Err(EstimateError::AmbiguousQualifier(qualifier.to_string()));
        }
        Ok(matches.into_iter().next())
    }

    /// The base NDV of a scan column from source statistics, else `None`.
    pub(crate) fn owner_column_ndv(
        &self,
        owner: &LogicalPlan,
        column: &str,
    ) -> Result<Option<i64>> {
        let LogicalPlan::Scan(scan) = owner else {
            return Ok(None);
        };
        let Some(stats) = self.stats.as_ref() else {
            return Ok(None);
        };
        let Some(table_stats) = stats.get_table_statistics(
            &scan.datasource,
            &scan.schema_name,
            &scan.table_name,
            &[column.to_string()],
        )?
        else {
            return Ok(None);
        };
        Ok(table_stats
            .column_stats
            .get(column)
            .and_then(|col_stats| col_stats.num_distinct))
    }

    /// A column's NDV, falling back to the owner's table row count as an upper
    /// bound when the column has no histogram. Gate-facing only.
    fn bounded_owner_ndv(&self, owner: &LogicalPlan, column: &str) -> Result<Option<i64>> {
        if let Some(ndv) = self.owner_column_ndv(owner, column)? {
            return Ok(Some(ndv));
        }
        self.owner_row_count(owner)
    }

    /// The owning scan's table row count from source statistics, or `None`.
    fn owner_row_count(&self, owner: &LogicalPlan) -> Result<Option<i64>> {
        let LogicalPlan::Scan(scan) = owner else {
            return Ok(None);
        };
        let Some(stats) = self.stats.as_ref() else {
            return Ok(None);
        };
        let table_stats = stats.get_table_statistics(
            &scan.datasource,
            &scan.schema_name,
            &scan.table_name,
            &[],
        )?;
        Ok(table_stats.and_then(|stats| stats.row_count))
    }
}

/// Append every relation in the subtree whose exposed name is the qualifier.
/// Scans by alias or table name, derived tables and CTE references by their
/// exposed alias; stops descent at each.
fn collect_relations<'a>(
    node: &'a LogicalPlan,
    qualifier: &str,
    matches: &mut Vec<&'a LogicalPlan>,
) {
    match node {
        LogicalPlan::Scan(scan) => {
            let name = scan.alias.as_deref().unwrap_or(&scan.table_name);
            if name == qualifier {
                matches.push(node);
            }
        }
        LogicalPlan::SubqueryScan(subquery) => {
            if subquery.alias == qualifier {
                matches.push(node);
            }
        }
        LogicalPlan::CteRef(cte_ref) => {
            let name = cte_ref.alias.as_deref().unwrap_or(&cte_ref.name);
            if name == qualifier {
                matches.push(node);
            }
        }
        other => {
            for child in other.children() {
                collect_relations(child, qualifier, matches);
            }
        }
    }
}

/// A scan's `ds.schema.table` identity used in provenance entries.
fn scan_target(scan: &Scan) -> String {
    format!(
        "{}.{}.{}",
        scan.datasource, scan.schema_name, scan.table_name
    )
}

/// A group/join key's display name for provenance entries.
fn key_name(key: &Expr) -> String {
    match key {
        Expr::Column(column) => column.column.clone(),
        other => crate::subplan_signature::python_class_name(other).to_string(),
    }
}

/// Convert an optional source NDV (i64) to the u64 row-arithmetic domain; a
/// negative count (a source contract violation) reads as UNKNOWN.
fn ndv_u64(value: Option<i64>) -> Option<u64> {
    value.and_then(|number| u64::try_from(number).ok())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use fq_catalog::datasource::{
        ColumnStatistics, DataSource, DataSourceCapability, StatValue, TableMetadata,
        TableStatistics,
    };
    use fq_catalog::{Catalog, CatalogError};
    use fq_common::DataType;
    use fq_plan::expr::{BinaryOpType, ColumnRef, Expr, LiteralValue};
    use fq_plan::logical::{
        Cte, CteRef, Explain, ExplainFormat, Filter, Join, JoinType, LateralJoin, Scan, Values,
    };

    /// A table's row count and per-column statistics.
    type TableStats = (Option<i64>, BTreeMap<String, ColumnStatistics>);

    /// A configurable in-crate source: a map of table -> (row_count, per-column
    /// stats). `Send + Sync` as the trait requires.
    struct FakeSource {
        tables: BTreeMap<String, TableStats>,
    }

    impl DataSource for FakeSource {
        #[allow(clippy::unnecessary_literal_bound)]
        fn name(&self) -> &str {
            "duck"
        }
        fn capabilities(&self) -> Vec<DataSourceCapability> {
            vec![]
        }
        fn list_schemas(&self) -> std::result::Result<Vec<String>, CatalogError> {
            Ok(vec!["main".to_string()])
        }
        fn list_tables(&self, _schema: &str) -> std::result::Result<Vec<String>, CatalogError> {
            Ok(self.tables.keys().cloned().collect())
        }
        fn get_table_metadata(
            &self,
            schema: &str,
            table: &str,
        ) -> std::result::Result<TableMetadata, CatalogError> {
            Ok(TableMetadata {
                schema_name: schema.to_string(),
                table_name: table.to_string(),
                columns: vec![],
                row_count: None,
                size_bytes: None,
            })
        }
        fn get_table_statistics(
            &self,
            _schema: &str,
            table: &str,
            columns: &[String],
        ) -> std::result::Result<Option<TableStatistics>, CatalogError> {
            let Some((row_count, all_stats)) = self.tables.get(table) else {
                return Ok(None);
            };
            let mut column_stats = BTreeMap::new();
            for column in columns {
                if let Some(stats) = all_stats.get(column) {
                    column_stats.insert(column.clone(), stats.clone());
                }
            }
            Ok(Some(TableStatistics {
                row_count: *row_count,
                total_size_bytes: 0,
                column_stats,
            }))
        }
        fn map_native_type(&self, type_str: &str) -> std::result::Result<DataType, CatalogError> {
            fq_catalog::datasource::map_native_type_default(type_str)
        }
    }

    fn ndv_stats(num_distinct: i64) -> ColumnStatistics {
        ColumnStatistics {
            num_distinct: Some(num_distinct),
            null_fraction: 0.0,
            avg_width: 8,
            min_value: None,
            max_value: None,
        }
    }

    /// A table spec for `model`: (name, row_count, per-column NDV stats).
    type TableSpec<'a> = (&'a str, Option<i64>, Vec<(&'a str, ColumnStatistics)>);

    /// Build a cost model over a fake source describing the given tables.
    fn model(tables: Vec<TableSpec>) -> CostModel {
        let mut table_map = BTreeMap::new();
        for (name, rows, columns) in tables {
            let mut column_map = BTreeMap::new();
            for (column, stats) in columns {
                column_map.insert(column.to_string(), stats);
            }
            table_map.insert(name.to_string(), (rows, column_map));
        }
        let mut catalog = Catalog::new();
        catalog.register_datasource(Arc::new(FakeSource { tables: table_map }));
        let stats = StatisticsCollector::new(Arc::new(catalog), None, None);
        CostModel::new(CostConfig::default(), Some(stats))
    }

    fn scan(table: &str, columns: &[&str], alias: &str) -> LogicalPlan {
        let mut node = Scan::new(
            "duck",
            "main",
            table,
            columns.iter().map(|c| (*c).to_string()).collect(),
        );
        node.alias = Some(alias.to_string());
        LogicalPlan::Scan(Box::new(node))
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

    fn join(
        left: LogicalPlan,
        right: LogicalPlan,
        kind: JoinType,
        condition: Option<Expr>,
    ) -> LogicalPlan {
        LogicalPlan::Join(Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: kind,
            condition,
            natural: false,
            using: None,
            estimated_rows: None,
            estimate_defaults: None,
        })
    }

    #[test]
    fn explain_raises_no_rule() {
        let mut cost = model(vec![]);
        let plan = LogicalPlan::Explain(Explain {
            input: Box::new(scan("t", &["a"], "t")),
            format: ExplainFormat::Text,
        });
        assert!(matches!(
            cost.estimate(&plan),
            Err(EstimateError::NoRule("Explain"))
        ));
    }

    #[test]
    fn values_is_its_row_count() {
        let mut cost = model(vec![]);
        let plan = LogicalPlan::Values(Values {
            rows: vec![vec![], vec![], vec![]],
            output_names: vec!["x".to_string()],
        });
        assert_eq!(cost.estimate(&plan).unwrap().rows, Some(3));
    }

    #[test]
    fn lateral_join_is_unknown_with_provenance() {
        let mut cost = model(vec![("t", Some(10), vec![])]);
        let plan = LogicalPlan::LateralJoin(LateralJoin {
            left: Box::new(scan("t", &["a"], "t")),
            right: Box::new(scan("t", &["a"], "u")),
            join_type: JoinType::Inner,
        });
        let estimate = cost.estimate(&plan).unwrap();
        assert_eq!(estimate.rows, None);
        assert!(estimate
            .defaults_used
            .contains(&"lateral_multiplicity".to_string()));
    }

    #[test]
    fn unknown_base_rows_propagate_through_filter() {
        // No stats for table "z" (not registered) -> None rows; a filter over it
        // stays None (a fraction of an unknown is unknown).
        let mut cost = model(vec![]);
        let scan_node = scan("z", &["a"], "z");
        let filter = LogicalPlan::Filter(Filter {
            input: Box::new(scan_node),
            predicate: eq(
                col("z", "a"),
                Expr::Literal {
                    value: LiteralValue::Integer(3),
                    data_type: DataType::Integer,
                },
            ),
        });
        let estimate = cost.estimate(&filter).unwrap();
        assert_eq!(estimate.rows, None);
        assert!(estimate
            .defaults_used
            .iter()
            .any(|d| d.starts_with("row_count(")));
    }

    #[test]
    fn scan_applies_equality_selectivity() {
        // 1000 rows, filter a = 3 with ndv 4 -> 250 rows.
        let mut range = ndv_stats(4);
        range.min_value = Some(StatValue::Integer(0));
        range.max_value = Some(StatValue::Integer(3));
        let mut cost = model(vec![("t", Some(1000), vec![("a", range)])]);
        let mut node = Scan::new("duck", "main", "t", vec!["a".to_string()]);
        node.alias = Some("t".to_string());
        node.filters = Some(eq(
            col("t", "a"),
            Expr::Literal {
                value: LiteralValue::Integer(3),
                data_type: DataType::Integer,
            },
        ));
        let estimate = cost.estimate(&LogicalPlan::Scan(Box::new(node))).unwrap();
        assert_eq!(estimate.rows, Some(250));
        assert!(estimate.defaults_used.is_empty());
    }

    #[test]
    fn inner_join_uses_ndv_formula() {
        // L=100, R=10, equi key ndv max = 10 -> 100*10/10 = 100.
        let mut cost = model(vec![
            ("l", Some(100), vec![("k", ndv_stats(10))]),
            ("r", Some(10), vec![("k", ndv_stats(10))]),
        ]);
        let plan = join(
            scan("l", &["k"], "l"),
            scan("r", &["k"], "r"),
            JoinType::Inner,
            Some(eq(col("l", "k"), col("r", "k"))),
        );
        assert_eq!(cost.estimate(&plan).unwrap().rows, Some(100));
    }

    #[test]
    fn composite_key_denominator_is_capped() {
        // Two equi keys, each ndv 50000 -> naive denom 2.5e9, capped at min(L,R)=100.
        // L=100, R=200 -> 100*200/100 = 200.
        let mut cost = model(vec![
            (
                "l",
                Some(100),
                vec![("k1", ndv_stats(50_000)), ("k2", ndv_stats(50_000))],
            ),
            (
                "r",
                Some(200),
                vec![("k1", ndv_stats(50_000)), ("k2", ndv_stats(50_000))],
            ),
        ]);
        let condition = Expr::BinaryOp {
            op: BinaryOpType::And,
            left: Box::new(eq(col("l", "k1"), col("r", "k1"))),
            right: Box::new(eq(col("l", "k2"), col("r", "k2"))),
        };
        let plan = join(
            scan("l", &["k1", "k2"], "l"),
            scan("r", &["k1", "k2"], "r"),
            JoinType::Inner,
            Some(condition),
        );
        assert_eq!(cost.estimate(&plan).unwrap().rows, Some(200));
    }

    #[test]
    fn anti_join_not_emptied_for_many_to_many() {
        // A high-fanout inner (many-to-many) must NOT empty the ANTI result: the
        // occupancy estimate keeps it >= 1 and below left rows.
        let mut cost = model(vec![
            ("l", Some(100), vec![("k", ndv_stats(2))]),
            ("r", Some(100), vec![("k", ndv_stats(2))]),
        ]);
        let plan = join(
            scan("l", &["k"], "l"),
            scan("r", &["k"], "r"),
            JoinType::Anti,
            Some(eq(col("l", "k"), col("r", "k"))),
        );
        let rows = cost.estimate(&plan).unwrap().rows.unwrap();
        assert!(
            (1..100).contains(&rows),
            "anti rows {rows} should be in (0, left)"
        );
    }

    #[test]
    fn left_join_is_bounded_below_by_left_rows() {
        // Inner would be tiny (ndv 1000 over 100x100), but LEFT keeps >= left.
        let mut cost = model(vec![
            ("l", Some(100), vec![("k", ndv_stats(1000))]),
            ("r", Some(100), vec![("k", ndv_stats(1000))]),
        ]);
        let plan = join(
            scan("l", &["k"], "l"),
            scan("r", &["k"], "r"),
            JoinType::Left,
            Some(eq(col("l", "k"), col("r", "k"))),
        );
        assert!(cost.estimate(&plan).unwrap().rows.unwrap() >= 100);
    }

    #[test]
    fn pushed_group_by_bounded_by_ndv_then_rows() {
        // 1000 rows, group key ndv 30 -> 30 groups (< rows).
        let mut node = Scan::new("duck", "main", "t", vec!["g".to_string()]);
        node.alias = Some("t".to_string());
        node.group_by = Some(vec![col("t", "g")]);
        let mut cost = model(vec![("t", Some(1000), vec![("g", ndv_stats(30))])]);
        assert_eq!(
            cost.estimate(&LogicalPlan::Scan(Box::new(node)))
                .unwrap()
                .rows,
            Some(30)
        );
    }

    #[test]
    fn coordinator_aggregate_bounded_by_group_ndv() {
        use fq_plan::logical::Aggregate;
        // A GROUP BY over a scan: group key ndv 30 -> 30 groups.
        let aggregate = LogicalPlan::Aggregate(Aggregate {
            input: Box::new(scan("t", &["g"], "t")),
            group_by: vec![col("t", "g")],
            aggregates: vec![],
            output_names: vec!["g".to_string()],
            grouping_sets: None,
        });
        let mut cost = model(vec![("t", Some(1000), vec![("g", ndv_stats(30))])]);
        assert_eq!(cost.estimate(&aggregate).unwrap().rows, Some(30));
    }

    #[test]
    fn global_aggregate_is_one_row() {
        use fq_plan::logical::Aggregate;
        let aggregate = LogicalPlan::Aggregate(Aggregate {
            input: Box::new(scan("t", &["a"], "t")),
            group_by: vec![],
            aggregates: vec![],
            output_names: vec!["c".to_string()],
            grouping_sets: None,
        });
        let mut cost = model(vec![("t", Some(1000), vec![])]);
        assert_eq!(cost.estimate(&aggregate).unwrap().rows, Some(1));
    }

    #[test]
    fn cte_ref_estimates_registered_body() {
        // WITH c AS (scan t) SELECT ... : the CteRef estimates t (1000 rows).
        let cte = LogicalPlan::Cte(Cte {
            name: "c".to_string(),
            cte_plan: Box::new(scan("t", &["a"], "t")),
            child: Box::new(LogicalPlan::CteRef(CteRef {
                name: "c".to_string(),
                alias: None,
                columns: None,
                output_names: None,
            })),
            recursive: false,
            column_names: None,
        });
        let mut cost = model(vec![("t", Some(1000), vec![])]);
        assert_eq!(cost.estimate(&cte).unwrap().rows, Some(1000));
    }

    #[test]
    fn recursive_cte_ref_is_unknown() {
        let cte = LogicalPlan::Cte(Cte {
            name: "c".to_string(),
            cte_plan: Box::new(scan("t", &["a"], "t")),
            child: Box::new(LogicalPlan::CteRef(CteRef {
                name: "c".to_string(),
                alias: None,
                columns: None,
                output_names: None,
            })),
            recursive: true,
            column_names: None,
        });
        let mut cost = model(vec![("t", Some(1000), vec![])]);
        let estimate = cost.estimate(&cte).unwrap();
        assert_eq!(estimate.rows, None);
        assert!(estimate
            .defaults_used
            .iter()
            .any(|d| d == "row_count(cte c)"));
    }

    #[test]
    fn reset_cte_registry_scopes_per_walk() {
        let mut cost = model(vec![("t", Some(1000), vec![])]);
        let register = Cte {
            name: "c".to_string(),
            cte_plan: Box::new(scan("t", &["a"], "t")),
            child: Box::new(scan("t", &["a"], "t")),
            recursive: false,
            column_names: None,
        };
        cost.register_cte(&register);
        // A bare CteRef now resolves to the body.
        let cte_ref = LogicalPlan::CteRef(CteRef {
            name: "c".to_string(),
            alias: None,
            columns: None,
            output_names: None,
        });
        assert_eq!(cost.estimate(&cte_ref).unwrap().rows, Some(1000));
        // After a reset the same reference is unknown again.
        cost.reset_cte_registry();
        assert_eq!(cost.estimate(&cte_ref).unwrap().rows, None);
    }

    #[test]
    fn learned_predicate_rows_consulted_when_engine_prices_a_gap() {
        use fq_catalog::StatsCatalog;
        // A LIKE filter: the engine cannot price it (gap), so the LEARNED template
        // output (consulted BEFORE the source planner) supplies the row count.
        #[allow(clippy::arc_with_non_send_sync)]
        let learned = Arc::new(StatsCatalog::open(":memory:").unwrap());
        // Template for `name LIKE '%x%'` is `LIKE(duck.main.t.name)`.
        learned
            .record_predicate(
                "duck",
                "main",
                "t",
                "LIKE(duck.main.t.name)",
                Some(1000),
                5,
                "",
            )
            .unwrap();
        let mut catalog = Catalog::new();
        let mut table_map = BTreeMap::new();
        table_map.insert("t".to_string(), (Some(1000), BTreeMap::new()));
        catalog.register_datasource(Arc::new(FakeSource { tables: table_map }));
        let stats = StatisticsCollector::new(Arc::new(catalog), Some(learned), None);
        let mut cost = CostModel::new(CostConfig::default(), Some(stats));

        let mut node = Scan::new("duck", "main", "t", vec!["name".to_string()]);
        node.alias = Some("t".to_string());
        node.filters = Some(Expr::BinaryOp {
            op: BinaryOpType::Like,
            left: Box::new(col("t", "name")),
            right: Box::new(Expr::Literal {
                value: LiteralValue::String("%x%".into()),
                data_type: DataType::Varchar,
            }),
        });
        let estimate = cost.estimate(&LogicalPlan::Scan(Box::new(node))).unwrap();
        // The measured output (5) beats the no-reduction bound (1000); no gap.
        assert_eq!(estimate.rows, Some(5));
        assert!(estimate.defaults_used.is_empty());
    }

    #[test]
    fn join_tree_cost_sums_join_outputs() {
        let mut cost = model(vec![
            ("l", Some(100), vec![("k", ndv_stats(10))]),
            ("r", Some(10), vec![("k", ndv_stats(10))]),
        ]);
        let plan = join(
            scan("l", &["k"], "l"),
            scan("r", &["k"], "r"),
            JoinType::Inner,
            Some(eq(col("l", "k"), col("r", "k"))),
        );
        // One join, output 100.
        assert!((cost.join_tree_cost(&plan).unwrap() - 100.0).abs() < 1e-9);
    }

    #[test]
    fn ambiguous_qualifier_raises() {
        // A self-join exposes the alias "l" twice under one side, so resolving a
        // key against it is ambiguous.
        let mut cost = model(vec![("t", Some(10), vec![("k", ndv_stats(5))])]);
        let dup_side = join(
            scan("t", &["k"], "l"),
            scan("t", &["k"], "l"),
            JoinType::Inner,
            None,
        );
        let plan = join(
            dup_side,
            scan("t", &["k"], "r"),
            JoinType::Inner,
            Some(eq(col("l", "k"), col("r", "k"))),
        );
        assert!(matches!(
            cost.estimate(&plan),
            Err(EstimateError::AmbiguousQualifier(_))
        ));
    }
}
