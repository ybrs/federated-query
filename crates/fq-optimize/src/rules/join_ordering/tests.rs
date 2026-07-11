//! Tests for the cost-based join ordering rule.
//!
//! The ENUMERATOR is exercised with a stub estimator (hand-fed cardinalities and
//! per-edge selectivities, no databases), so every assertion is about ordering
//! decisions, cross-product avoidance and determinism. The RULE is driven over a
//! real `CostModel` above an in-crate fake source with seeded statistics.

use super::*;

use std::collections::BTreeMap;
use std::sync::Arc;

use fq_catalog::datasource::{
    map_native_type_default, ColumnStatistics, DataSource, DataSourceCapability, TableMetadata,
    TableStatistics,
};
use fq_catalog::{Catalog, CatalogError};
use fq_common::{CostConfig, DataType};
use fq_plan::expr::{BinaryOpType, ColumnRef, Expr, LiteralValue};
use fq_plan::logical::{CteRef, Filter, Join, JoinType, LogicalPlan, Projection, Scan, Values};

use crate::statistics::StatisticsCollector;

/// Whether two costs coincide within a tolerance (float comparisons are pedantic).
fn close(actual: f64, expected: f64) -> bool {
    (actual - expected).abs() < 1e-9
}

// ------------------------------- shared builders ----------------------------

/// A qualified column reference `table.column` (integer).
fn col(table: &str, column: &str) -> Expr {
    Expr::Column(ColumnRef::new(
        Some(table.to_string()),
        column,
        Some(DataType::Integer),
    ))
}

/// `left = right`.
fn eq(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        op: BinaryOpType::Eq,
        left: Box::new(left),
        right: Box::new(right),
    }
}

/// `left > right`.
fn gt(left: Expr, right: Expr) -> Expr {
    Expr::BinaryOp {
        op: BinaryOpType::Gt,
        left: Box::new(left),
        right: Box::new(right),
    }
}

/// A named single-column scan under a datasource, aliased to its own name.
fn named_scan(source: &str, name: &str) -> LogicalPlan {
    let mut scan = Scan::new(source, "s", name, vec!["k".to_string()]);
    scan.alias = Some(name.to_string());
    LogicalPlan::Scan(Box::new(scan))
}

/// A scan atom exposing one qualifier (its own name), reading from `source`.
fn sourced_atom(index: usize, name: &str, source: &str) -> JoinAtom {
    JoinAtom {
        index,
        plan: named_scan(source, name),
        qualifiers: [name.to_string()].into_iter().collect(),
    }
}

/// An equi conjunct connecting the two atoms of `pair`.
fn edge(pair: (usize, usize), names: (&str, &str)) -> JoinConjunct {
    JoinConjunct {
        expression: eq(col(names.0, "k"), col(names.1, "k")),
        atom_indexes: [pair.0, pair.1].into_iter().collect(),
        is_equi: true,
    }
}

/// A non-equi conjunct over the two atoms of `pair` (expression content is only
/// consulted by the real estimator, never the stub).
fn non_equi(pair: (usize, usize), names: (&str, &str)) -> JoinConjunct {
    JoinConjunct {
        expression: gt(col(names.0, "k"), col(names.1, "k")),
        atom_indexes: [pair.0, pair.1].into_iter().collect(),
        is_equi: false,
    }
}

/// A region of named atoms (all from source `ds`) plus (i, j) equi edges.
fn region(names: &[&str], edges: &[(usize, usize)]) -> JoinRegion {
    let sources = vec!["ds"; names.len()];
    sourced_region(names, &sources, edges)
}

/// A region whose atoms carry the given per-index datasource names.
fn sourced_region(names: &[&str], sources: &[&str], edges: &[(usize, usize)]) -> JoinRegion {
    let mut atoms = Vec::new();
    for (index, name) in names.iter().enumerate() {
        atoms.push(sourced_atom(index, name, sources[index]));
    }
    let mut conjuncts = Vec::new();
    for &(first, second) in edges {
        conjuncts.push(edge((first, second), (names[first], names[second])));
    }
    JoinRegion { atoms, conjuncts }
}

/// The flat atom-index order of a single-component result.
fn sequence(order: &RegionOrder) -> Vec<usize> {
    assert_eq!(order.components.len(), 1, "expected one component");
    let component = &order.components[0];
    let mut result = vec![component.first_atom];
    for step in &component.steps {
        result.push(step.atom_index);
    }
    result
}

// ------------------------------- the stub estimator -------------------------

/// Cardinalities from a rows-per-atom table and a selectivity-per-edge map keyed
/// by the conjunct's atom_indexes set.
struct StubEstimator {
    rows_by_atom: Vec<Option<u64>>,
    selectivity_by_edge: BTreeMap<BTreeSet<usize>, f64>,
}

impl RegionEstimator for StubEstimator {
    fn atom_estimate(
        &self,
        _region: &JoinRegion,
        atom_index: usize,
        _local_positions: &[usize],
    ) -> Result<CardinalityEstimate, OptimizeError> {
        Ok(CardinalityEstimate::of(
            self.rows_by_atom[atom_index],
            Vec::new(),
        ))
    }

    fn join_estimate(
        &self,
        region: &JoinRegion,
        left: &CardinalityEstimate,
        _atom_index: usize,
        atom: &CardinalityEstimate,
        positions: &[usize],
    ) -> Result<CardinalityEstimate, OptimizeError> {
        Ok(self.product(region, left, atom, positions))
    }

    fn cross_estimate(
        &self,
        region: &JoinRegion,
        left: &CardinalityEstimate,
        right: &CardinalityEstimate,
        positions: &[usize],
    ) -> Result<CardinalityEstimate, OptimizeError> {
        Ok(self.product(region, left, right, positions))
    }
}

impl StubEstimator {
    /// left x right x the placed conjuncts' hand-fed selectivities.
    fn product(
        &self,
        region: &JoinRegion,
        left: &CardinalityEstimate,
        right: &CardinalityEstimate,
        positions: &[usize],
    ) -> CardinalityEstimate {
        let mut rows = left.rows.unwrap() as f64 * right.rows.unwrap() as f64;
        for &position in positions {
            rows *= self.selectivity_by_edge[&region.conjuncts[position].atom_indexes];
        }
        CardinalityEstimate::known((rows as u64).max(1), Vec::new())
    }
}

/// A stub over per-atom rows and (edge, selectivity) pairs.
fn stub(rows: &[u64], selectivity: &[((usize, usize), f64)]) -> StubEstimator {
    let rows_by_atom = rows.iter().map(|&count| Some(count)).collect();
    let mut selectivity_by_edge = BTreeMap::new();
    for &((first, second), value) in selectivity {
        selectivity_by_edge.insert([first, second].into_iter().collect(), value);
    }
    StubEstimator {
        rows_by_atom,
        selectivity_by_edge,
    }
}

// ------------------------- graph decomposition tests ------------------------

#[test]
fn adjacency_uses_equi_edges_only() {
    let mut non_equi_region = region(&["a", "b"], &[]);
    // A non-equi conjunct over {0,1} must NOT make the atoms adjacent.
    non_equi_region.conjuncts.push(non_equi((0, 1), ("a", "b")));
    let graph = adjacency(&non_equi_region);
    assert!(graph[&0].is_empty());
    assert!(graph[&1].is_empty());
    // The same pair as an EQUI edge does connect them.
    let equi_region = region(&["a", "b"], &[(0, 1)]);
    assert!(adjacency(&equi_region)[&0].contains(&1));
}

#[test]
fn connected_components_partition_the_graph() {
    let region = region(&["a", "b", "c", "d"], &[(0, 1), (2, 3)]);
    assert_eq!(connected_components(&region), vec![vec![0, 1], vec![2, 3]]);
}

#[test]
fn a_non_equi_only_pair_yields_two_singletons() {
    let region = region(&["a", "b"], &[]);
    assert_eq!(connected_components(&region), vec![vec![0], vec![1]]);
}

#[test]
fn newly_covered_places_a_multi_atom_conjunct_once() {
    let region = region(&["a", "b", "c"], &[(0, 1), (1, 2)]);
    // Growing {0} by atom 1 covers the (0,1) edge (position 0) exactly.
    assert_eq!(newly_covered(&region, 1 << 0, 1), vec![0]);
    // Growing {0,1} by atom 2 covers the (1,2) edge (position 1), not (0,1).
    assert_eq!(newly_covered(&region, (1 << 0) | (1 << 1), 2), vec![1]);
}

#[test]
fn spanning_positions_finds_only_cross_spanning_conjuncts() {
    let mut region = region(&["a", "b"], &[]);
    region.conjuncts.push(non_equi((0, 1), ("a", "b")));
    let covered = [0].into_iter().collect();
    let incoming = [1].into_iter().collect();
    assert_eq!(spanning_positions(&region, &covered, &incoming), vec![0]);
    // A conjunct wholly inside one side does not span.
    assert!(spanning_positions(&region, &incoming, &BTreeSet::new()).is_empty());
}

// ---------------------- extended_transfer / atom_source ---------------------

#[test]
fn atom_source_is_the_single_scan_datasource() {
    assert_eq!(
        atom_source(&named_scan("duck", "a")),
        Some("duck".to_string())
    );
}

#[test]
fn atom_source_of_a_same_source_join_is_that_source() {
    let same = LogicalPlan::Join(Join {
        left: Box::new(named_scan("duck", "a")),
        right: Box::new(named_scan("duck", "b")),
        join_type: JoinType::Left,
        condition: Some(eq(col("a", "k"), col("b", "k"))),
        natural: false,
        using: None,
        estimated_rows: None,
        estimate_defaults: None,
    });
    assert_eq!(atom_source(&same), Some("duck".to_string()));
}

#[test]
fn atom_source_is_none_for_mixed_cteref_and_values() {
    let mixed = LogicalPlan::Join(Join {
        left: Box::new(named_scan("duck", "a")),
        right: Box::new(named_scan("pg", "c")),
        join_type: JoinType::Left,
        condition: None,
        natural: false,
        using: None,
        estimated_rows: None,
        estimate_defaults: None,
    });
    assert_eq!(atom_source(&mixed), None);
    let cte_ref = LogicalPlan::CteRef(CteRef {
        name: "w".to_string(),
        alias: Some("w".to_string()),
        columns: None,
        output_names: None,
    });
    assert_eq!(atom_source(&cte_ref), None);
    let values = LogicalPlan::Values(Values {
        rows: vec![vec![]],
        output_names: vec!["x".to_string()],
    });
    assert_eq!(atom_source(&values), None);
}

/// A pure single-atom pg island prefix over 1000 rows.
fn pg_island() -> Candidate {
    Candidate {
        cost: 0.0,
        sequence: vec![0],
        steps: Vec::new(),
        estimate: CardinalityEstimate::known(1000, Vec::new()),
        transfer: 0.0,
        island_source: Some("pg".to_string()),
    }
}

#[test]
fn extended_transfer_ships_nothing_within_one_source() {
    let (transfer, island) = extended_transfer(&pg_island(), Some("pg"), 500);
    assert!(close(transfer, 0.0));
    assert_eq!(island, Some("pg".to_string()));
}

#[test]
fn extended_transfer_ships_island_and_atom_on_a_source_switch() {
    // Switching to a duck atom ships the pg island's 1000 rows plus the atom's 500.
    let (transfer, island) = extended_transfer(&pg_island(), Some("duck"), 500);
    assert!(close(transfer, 1500.0));
    assert_eq!(island, None);
    // A coordinator-resident (None) atom adds nothing on the break; only the
    // island's own result crosses.
    let (coord_transfer, coord_island) = extended_transfer(&pg_island(), None, 500);
    assert!(close(coord_transfer, 1000.0));
    assert_eq!(coord_island, None);
}

// ------------------------------- DP ordering tests --------------------------

/// The q05 cyclic shape (single-source): rows and per-edge selectivities.
fn q05_shape() -> (JoinRegion, StubEstimator) {
    let names = [
        "region", "nation", "supplier", "customer", "orders", "lineitem",
    ];
    let edges = [(0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (2, 5)];
    let rows = [1, 25, 10_000, 150_000, 225_000, 6_000_000];
    let selectivity = [
        ((0, 1), 1.0 / 5.0),
        ((1, 2), 1.0 / 25.0),
        ((2, 3), 1.0 / 25.0),
        ((3, 4), 1.0 / 150_000.0),
        ((4, 5), 1.0 / 1_500_000.0),
        ((2, 5), 1.0 / 10_000.0),
    ];
    (region(&names, &edges), stub(&rows, &selectivity))
}

#[test]
fn dp_avoids_the_nationkey_trap_pair() {
    let (region, estimator) = q05_shape();
    let order = choose_order(&region, &estimator, 10).unwrap().unwrap();
    // No step comes near the 60M nationkey blow-up.
    for step in &order.components[0].steps {
        assert!(step.estimate.rows.unwrap() < 2_000_000);
    }
    // The order never begins by joining supplier(2) and customer(3) directly.
    let sequence = sequence(&order);
    let first_two: BTreeSet<usize> = sequence[..2].iter().copied().collect();
    assert_ne!(first_two, [2, 3].into_iter().collect());
}

#[test]
fn tie_break_picks_the_lexicographically_smallest_sequence() {
    // A symmetric star centered on c(2): a(0) and b(1) each join c but not each
    // other, so every connected order costs the same 200. The identity [0,1,2] is
    // NOT connected (0-1 share no edge); the tie-break picks the smallest VALID
    // sequence, [0,2,1].
    let region = region(&["a", "b", "c"], &[(0, 2), (1, 2)]);
    let order = choose_order(
        &region,
        &stub(&[100, 100, 100], &[((0, 2), 0.01), ((1, 2), 0.01)]),
        10,
    )
    .unwrap()
    .unwrap();
    assert_eq!(sequence(&order), vec![0, 2, 1]);
}

#[test]
fn dp_never_generates_an_intra_component_cross() {
    // part(0) and supplier(1) share no edge; both join lineitem(2). Every step
    // must place a connecting edge.
    let region = region(&["part", "supplier", "lineitem"], &[(0, 2), (1, 2)]);
    let estimator = stub(
        &[200, 10_000, 6_000_000],
        &[((0, 2), 1.0 / 200_000.0), ((1, 2), 1.0 / 10_000.0)],
    );
    let order = choose_order(&region, &estimator, 10).unwrap().unwrap();
    for step in &order.components[0].steps {
        assert!(
            !step.conjunct_positions.is_empty(),
            "a step joined with no connecting edge"
        );
    }
}

#[test]
fn locality_flips_the_winner_toward_a_same_source_island() {
    // A pg-pg-duck-duck cycle with equal cardinalities: the order must lead with
    // one source's island rather than interleaving.
    let names = ["p1", "p2", "d1", "d2"];
    let sources = ["pg", "pg", "duck", "duck"];
    let edges = [(0, 1), (1, 2), (2, 3), (0, 3)];
    let selectivity = [
        ((0, 1), 1.0 / 1000.0),
        ((1, 2), 1.0 / 1000.0),
        ((2, 3), 1.0 / 1000.0),
        ((0, 3), 1.0 / 1000.0),
    ];
    let region = sourced_region(&names, &sources, &edges);
    let order = choose_order(&region, &stub(&[1000, 1000, 1000, 1000], &selectivity), 10)
        .unwrap()
        .unwrap();
    let sequence = sequence(&order);
    let first_two: BTreeSet<&str> = [sources[sequence[0]], sources[sequence[1]]]
        .into_iter()
        .collect();
    assert_eq!(first_two.len(), 1, "interleaved order {sequence:?}");
}

#[test]
fn locality_leads_with_the_fact_island_on_q05() {
    // The transfer term leads with the duck fact island (orders/lineitem) instead
    // of shipping all 6M lineitem rows.
    let names = [
        "region", "nation", "supplier", "customer", "orders", "lineitem",
    ];
    let sources = ["pg", "pg", "pg", "pg", "duck", "duck"];
    let edges = [(0, 1), (1, 2), (2, 3), (3, 4), (4, 5), (2, 5)];
    let rows = [1, 25, 10_000, 150_000, 228_000, 6_000_000];
    let selectivity = [
        ((0, 1), 1.0 / 5.0),
        ((1, 2), 1.0 / 25.0),
        ((2, 3), 1.0 / 25.0),
        ((3, 4), 1.0 / 150_000.0),
        ((4, 5), 1.0 / 1_500_000.0),
        ((2, 5), 1.0 / 10_000.0),
    ];
    let region = sourced_region(&names, &sources, &edges);
    let order = choose_order(&region, &stub(&rows, &selectivity), 10)
        .unwrap()
        .unwrap();
    let sequence = sequence(&order);
    let leading: BTreeSet<usize> = [sequence[0], sequence[1]].into_iter().collect();
    assert_eq!(leading, [4, 5].into_iter().collect());
}

#[test]
fn choose_order_is_deterministic_across_runs() {
    let (region, _) = q05_shape();
    let first = choose_order(&region, &q05_shape().1, 10).unwrap();
    let second = choose_order(&region, &q05_shape().1, 10).unwrap();
    assert_eq!(first, second);
}

#[test]
fn single_atom_region_has_no_steps() {
    let region = region(&["only"], &[]);
    let order = choose_order(&region, &stub(&[42], &[]), 10)
        .unwrap()
        .unwrap();
    assert_eq!(order.components.len(), 1);
    assert_eq!(order.components[0].first_atom, 0);
    assert!(order.components[0].steps.is_empty());
    assert_eq!(order.components[0].total.rows, Some(42));
}

#[test]
fn every_multi_atom_conjunct_is_placed_exactly_once_in_steps() {
    let (region, estimator) = q05_shape();
    let order = choose_order(&region, &estimator, 10).unwrap().unwrap();
    let mut placed = Vec::new();
    for component in &order.components {
        for step in &component.steps {
            placed.extend(step.conjunct_positions.iter().copied());
        }
    }
    placed.sort_unstable();
    assert_eq!(placed, (0..region.conjuncts.len()).collect::<Vec<_>>());
}

// ------------------------------- GOO fallback -------------------------------

#[test]
fn greedy_used_above_dp_limit_stays_fully_connected() {
    // A 12-atom chain forces the greedy path (max_dp = 10).
    let count = 12usize;
    let names: Vec<String> = (0..count).map(|index| format!("t{index}")).collect();
    let name_refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let mut edges = Vec::new();
    let mut rows = Vec::new();
    let mut selectivity = Vec::new();
    for index in 0..count {
        rows.push(10 * (index as u64 + 1));
        if index > 0 {
            edges.push((index - 1, index));
            selectivity.push(((index - 1, index), 0.01));
        }
    }
    let region = region(&name_refs, &edges);
    let order = choose_order(&region, &stub(&rows, &selectivity), 10)
        .unwrap()
        .unwrap();
    let mut sequence = sequence(&order);
    sequence.sort_unstable();
    assert_eq!(sequence, (0..count).collect::<Vec<_>>());
    for step in &order.components[0].steps {
        assert!(!step.conjunct_positions.is_empty());
    }
}

// ------------------------- component combination tests ----------------------

#[test]
fn disconnected_components_cross_smallest_first() {
    let region = region(&["a", "b", "c", "d"], &[(0, 1), (2, 3)]);
    let estimator = stub(
        &[1000, 1000, 10, 10],
        &[((0, 1), 1.0 / 1000.0), ((2, 3), 1.0 / 10.0)],
    );
    let order = choose_order(&region, &estimator, 10).unwrap().unwrap();
    assert_eq!(order.components.len(), 2);
    assert_eq!(order.cross_steps.len(), 1);
    // c-d yields 10 rows, a-b yields 1000: the smaller component leads.
    assert!([2, 3].contains(&order.components[0].first_atom));
    assert_eq!(order.cross_steps[0].estimate.rows, Some(10_000));
}

#[test]
fn a_spanning_non_equi_conjunct_rides_on_the_cross_step() {
    // Two atoms linked ONLY by a non-equi conjunct are separate components; the
    // conjunct rides on the CROSS between them, never lost.
    let mut region = region(&["a", "b"], &[]);
    region.conjuncts.push(non_equi((0, 1), ("a", "b")));
    let order = choose_order(&region, &stub(&[10, 20], &[((0, 1), 0.5)]), 10)
        .unwrap()
        .unwrap();
    assert_eq!(order.components.len(), 2);
    assert_eq!(order.cross_steps.len(), 1);
    assert_eq!(order.cross_steps[0].conjunct_positions, vec![0]);
}

// ---------------------------- non-equi edge handling ------------------------

#[test]
fn a_step_never_pairs_on_a_non_equi_edge_and_still_places_it() {
    // q07: n1(0) and n2(1) share only a non-equi OR; every step must place an
    // equi key, and the OR is placed exactly once.
    let names = ["n1", "n2", "supplier", "lineitem", "orders", "customer"];
    let edges = [(0, 2), (2, 3), (3, 4), (4, 5), (1, 5)];
    let mut region = region(&names, &edges);
    region.conjuncts.push(JoinConjunct {
        expression: Expr::BinaryOp {
            op: BinaryOpType::Or,
            left: Box::new(col("n1", "n_name")),
            right: Box::new(col("n2", "n_name")),
        },
        atom_indexes: [0, 1].into_iter().collect(),
        is_equi: false,
    });
    let rows = [25, 25, 10_000, 6_000_000, 1_500_000, 150_000];
    let selectivity = [
        ((0, 2), 1.0 / 25.0),
        ((2, 3), 1.0 / 10_000.0),
        ((3, 4), 1.0 / 1_500_000.0),
        ((4, 5), 1.0 / 150_000.0),
        ((1, 5), 1.0 / 25.0),
        ((0, 1), 0.0032),
    ];
    let order = choose_order(&region, &stub(&rows, &selectivity), 10)
        .unwrap()
        .unwrap();
    assert_eq!(order.components.len(), 1);
    for step in &order.components[0].steps {
        assert!(
            any_equi(&region, &step.conjunct_positions),
            "a step joined without any equi key"
        );
    }
    let or_position = region.conjuncts.len() - 1;
    let placements = order.components[0]
        .steps
        .iter()
        .filter(|step| step.conjunct_positions.contains(&or_position))
        .count();
    assert_eq!(placements, 1);
}

// ------------------------------- the rule (real cost) -----------------------

/// A configurable in-crate source: a map of table -> (row_count, per-column NDV).
struct FakeSource {
    source_name: String,
    tables: BTreeMap<String, (Option<i64>, BTreeMap<String, ColumnStatistics>)>,
}

impl DataSource for FakeSource {
    fn name(&self) -> &str {
        &self.source_name
    }
    fn capabilities(&self) -> Vec<DataSourceCapability> {
        vec![]
    }
    fn list_schemas(&self) -> std::result::Result<Vec<String>, CatalogError> {
        Ok(vec!["s".to_string()])
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
        map_native_type_default(type_str)
    }
}

/// A column-NDV statistics entry.
fn ndv_stats(num_distinct: i64) -> ColumnStatistics {
    ColumnStatistics {
        num_distinct: Some(num_distinct),
        null_fraction: 0.0,
        avg_width: 8,
        min_value: None,
        max_value: None,
    }
}

/// A seeded table spec: (name, row_count, per-column (name, NDV)).
type TableSeed<'a> = (&'a str, i64, &'a [(&'a str, i64)]);

/// A cost model over a fake `ds` source describing the given tables.
fn model(tables: &[TableSeed]) -> CostModel {
    let mut table_map = BTreeMap::new();
    for &(name, rows, columns) in tables {
        let mut column_map = BTreeMap::new();
        for &(column, ndv) in columns {
            column_map.insert(column.to_string(), ndv_stats(ndv));
        }
        table_map.insert(name.to_string(), (Some(rows), column_map));
    }
    let mut catalog = Catalog::new();
    catalog.register_datasource(Arc::new(FakeSource {
        source_name: "ds".to_string(),
        tables: table_map,
    }));
    let stats = StatisticsCollector::new(Arc::new(catalog), None, None);
    CostModel::new(CostConfig::default(), Some(stats))
}

/// The q08/q09 killer shape's cost model: part and supplier share no predicate;
/// both join lineitem.
fn q09_model() -> CostModel {
    model(&[
        ("part", 200_000, &[("p_partkey", 200_000)]),
        ("supplier", 10_000, &[("s_suppkey", 10_000)]),
        (
            "lineitem",
            6_000_000,
            &[("l_partkey", 200_000), ("l_suppkey", 10_000)],
        ),
    ])
}

/// A qualified scan of a seeded table.
fn scan(table: &str, alias: &str, columns: &[&str]) -> LogicalPlan {
    let mut node = Scan::new(
        "ds",
        "s",
        table,
        columns.iter().map(|c| (*c).to_string()).collect(),
    );
    node.alias = Some(alias.to_string());
    LogicalPlan::Scan(Box::new(node))
}

/// An inner/cross join with an explicit (or no) condition.
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

/// FROM-order left-deep: (part CROSS supplier) JOIN lineitem ON p=l AND s=l.
fn q09_from_order_tree() -> LogicalPlan {
    let cross = join(
        scan("part", "p", &["p_partkey"]),
        scan("supplier", "su", &["s_suppkey"]),
        JoinType::Inner,
        None,
    );
    let condition = Expr::BinaryOp {
        op: BinaryOpType::And,
        left: Box::new(eq(col("p", "p_partkey"), col("l", "l_partkey"))),
        right: Box::new(eq(col("su", "s_suppkey"), col("l", "l_suppkey"))),
    };
    join(
        cross,
        scan("lineitem", "l", &["l_partkey", "l_suppkey"]),
        JoinType::Inner,
        Some(condition),
    )
}

/// Collect every Join node in a plan tree.
fn collect_joins<'a>(plan: &'a LogicalPlan, found: &mut Vec<&'a Join>) {
    if let LogicalPlan::Join(join) = plan {
        found.push(join);
    }
    for child in plan.children() {
        collect_joins(child, found);
    }
}

/// Every scan in a tree keyed by its table name.
fn scans_by_table(plan: &LogicalPlan) -> BTreeMap<String, Scan> {
    let mut found = BTreeMap::new();
    collect_scans(plan, &mut found);
    found
}

/// Walk the tree collecting scans by table name.
fn collect_scans(plan: &LogicalPlan, found: &mut BTreeMap<String, Scan>) {
    if let LogicalPlan::Scan(scan) = plan {
        found.insert(scan.table_name.clone(), (**scan).clone());
    }
    for child in plan.children() {
        collect_scans(child, found);
    }
}

#[test]
fn q09_shape_leaves_no_conditionless_join() {
    let rule = JoinOrdering::new(q09_model(), 10);
    let result = rule.apply(q09_from_order_tree()).unwrap();
    let mut joins = Vec::new();
    collect_joins(&result, &mut joins);
    assert_eq!(joins.len(), 2);
    for join in joins {
        assert!(join.condition.is_some(), "an emitted join has no condition");
    }
}

#[test]
fn q09_shape_annotates_every_join_estimate() {
    let rule = JoinOrdering::new(q09_model(), 10);
    let result = rule.apply(q09_from_order_tree()).unwrap();
    let mut joins = Vec::new();
    collect_joins(&result, &mut joins);
    for join in joins {
        assert!(join.estimated_rows.unwrap() > 0);
        // Fully seeded statistics: no gaps recorded (an empty, present vector).
        assert!(join.estimate_defaults.as_ref().unwrap().is_empty());
    }
}

#[test]
fn rule_is_idempotent() {
    let rule = JoinOrdering::new(q09_model(), 10);
    let once = rule.apply(q09_from_order_tree()).unwrap();
    let twice = rule.apply(once.clone()).unwrap();
    assert_eq!(once, twice);
}

#[test]
fn left_join_root_is_untouched() {
    let left = join(
        scan("part", "p", &["p_partkey"]),
        scan("lineitem", "l", &["l_partkey", "l_suppkey"]),
        JoinType::Left,
        Some(eq(col("p", "p_partkey"), col("l", "l_partkey"))),
    );
    let rule = JoinOrdering::new(q09_model(), 10);
    assert_eq!(rule.apply(left.clone()).unwrap(), left);
}

#[test]
fn nested_region_below_projection_reorders() {
    let tree = LogicalPlan::Projection(Projection {
        input: Box::new(q09_from_order_tree()),
        expressions: vec![col("l", "l_partkey")],
        aliases: vec!["l_partkey".to_string()],
        distinct: false,
        distinct_on: None,
    });
    let rule = JoinOrdering::new(q09_model(), 10);
    let result = rule.apply(tree).unwrap();
    let mut joins = Vec::new();
    collect_joins(&result, &mut joins);
    assert_eq!(joins.len(), 2);
    for join in joins {
        assert!(join.condition.is_some());
    }
}

#[test]
fn two_atom_join_with_condition_is_untouched() {
    let tree = join(
        scan("part", "p", &["p_partkey"]),
        scan("lineitem", "l", &["l_partkey", "l_suppkey"]),
        JoinType::Inner,
        Some(eq(col("p", "p_partkey"), col("l", "l_partkey"))),
    );
    let rule = JoinOrdering::new(q09_model(), 10);
    assert_eq!(rule.apply(tree.clone()).unwrap(), tree);
}

#[test]
fn two_atom_cross_with_filter_becomes_inner() {
    let cross = join(
        scan("part", "p", &["p_partkey"]),
        scan("lineitem", "l", &["l_partkey", "l_suppkey"]),
        JoinType::Cross,
        None,
    );
    let tree = LogicalPlan::Filter(Filter {
        input: Box::new(cross),
        predicate: eq(col("p", "p_partkey"), col("l", "l_partkey")),
    });
    let rule = JoinOrdering::new(q09_model(), 10);
    let result = rule.apply(tree).unwrap();
    let mut joins = Vec::new();
    collect_joins(&result, &mut joins);
    assert_eq!(joins.len(), 1);
    assert_eq!(joins[0].join_type, JoinType::Inner);
    assert!(joins[0].condition.is_some());
}

#[test]
fn local_filter_folds_into_scan() {
    let cross = join(
        scan("part", "p", &["p_partkey"]),
        scan("lineitem", "l", &["l_partkey", "l_suppkey"]),
        JoinType::Cross,
        None,
    );
    let local = gt(
        col("p", "p_partkey"),
        Expr::Literal {
            value: LiteralValue::Integer(10),
            data_type: DataType::Integer,
        },
    );
    let connect = eq(col("p", "p_partkey"), col("l", "l_partkey"));
    let tree = LogicalPlan::Filter(Filter {
        input: Box::new(cross),
        predicate: Expr::BinaryOp {
            op: BinaryOpType::And,
            left: Box::new(local),
            right: Box::new(connect),
        },
    });
    let rule = JoinOrdering::new(q09_model(), 10);
    let result = rule.apply(tree).unwrap();
    let scans = scans_by_table(&result);
    assert!(
        scans["part"].filters.is_some(),
        "the single-atom filter folded into the part scan"
    );
}

#[test]
fn bare_scan_atom_is_stamped_with_estimate_and_key_ndv() {
    let rule = JoinOrdering::new(q09_model(), 10);
    let result = rule.apply(q09_from_order_tree()).unwrap();
    let scans = scans_by_table(&result);
    for scan in scans.values() {
        assert!(scan.estimated_rows.unwrap() > 0);
        assert!(
            scan.column_ndv.is_some(),
            "a join-key scan has no stamped NDV"
        );
    }
    assert_eq!(
        scans["part"].column_ndv.as_ref().unwrap()["p_partkey"],
        200_000
    );
}

// ------------------------- predicate conservation guard ---------------------

#[test]
fn verify_placement_accepts_a_complete_placement() {
    assert!(verify_placement(3, &[2, 0, 1]).is_ok());
}

#[test]
fn verify_placement_raises_on_a_dropped_conjunct() {
    // Two of three conjuncts placed: a dropped predicate manufactures wrong rows.
    assert!(matches!(
        verify_placement(3, &[0, 1]),
        Err(OptimizeError::JoinOrder(_))
    ));
}

#[test]
fn verify_placement_raises_on_a_duplicated_conjunct() {
    assert!(matches!(
        verify_placement(2, &[0, 0, 1]),
        Err(OptimizeError::JoinOrder(_))
    ));
}

#[test]
fn a_broken_emit_that_drops_a_conjunct_triggers_the_guard() {
    // A region of two equi-connected atoms plus a non-equi residual conjunct over
    // the same pair. A DELIBERATELY broken order omits the residual's position
    // from its only step, so emit places just the equi key - the conservation
    // guard must then raise on the dropped predicate (a simulated enumerator bug).
    let region = JoinRegion {
        atoms: vec![sourced_atom(0, "p", "ds"), sourced_atom(1, "l", "ds")],
        conjuncts: vec![
            edge((0, 1), ("p", "l")),     // position 0: placed on the step
            non_equi((0, 1), ("p", "l")), // position 1: dropped by the broken order
        ],
    };
    let broken_order = RegionOrder {
        components: vec![ComponentOrder {
            first_atom: 0,
            steps: vec![JoinStep {
                atom_index: 1,
                estimate: CardinalityEstimate::known(10, Vec::new()),
                conjunct_positions: vec![0],
            }],
            total: CardinalityEstimate::known(10, Vec::new()),
        }],
        cross_steps: Vec::new(),
    };
    let rule = JoinOrdering::new(
        model(&[("p", 100, &[("k", 100)]), ("l", 1000, &[("k", 1000)])]),
        10,
    );
    let mut placed = Vec::new();
    rule.emit(&region, &broken_order, &mut placed).unwrap();
    assert_eq!(placed, vec![0], "only the equi key was placed");
    assert!(matches!(
        verify_placement(region.conjuncts.len(), &placed),
        Err(OptimizeError::JoinOrder(_))
    ));
}

// ----------------------------- decline paths --------------------------------

#[test]
fn an_unknown_atom_leaves_the_written_order() {
    // No stats for part/supplier (absent from the model): their estimates are
    // UNKNOWN, so the 3-atom region declines and the tree is preserved verbatim.
    let rule = JoinOrdering::new(
        model(&[("lineitem", 6_000_000, &[("l_partkey", 200_000)])]),
        10,
    );
    let tree = q09_from_order_tree();
    assert_eq!(rule.apply(tree.clone()).unwrap(), tree);
}

// --------------------------- malformed-plan raise ---------------------------

#[test]
fn unqualified_region_predicate_raises() {
    // An unqualified column in a join condition is malformed: extract_region
    // raises a JoinGraphError, which propagates as an OptimizeError.
    let tree = join(
        scan("part", "p", &["p_partkey"]),
        scan("lineitem", "l", &["l_partkey"]),
        JoinType::Inner,
        Some(eq(
            Expr::Column(ColumnRef::new(None, "p_partkey", Some(DataType::Integer))),
            col("l", "l_partkey"),
        )),
    );
    let rule = JoinOrdering::new(q09_model(), 10);
    assert!(matches!(rule.apply(tree), Err(OptimizeError::JoinGraph(_))));
}
