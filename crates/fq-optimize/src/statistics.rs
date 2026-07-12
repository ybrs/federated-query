//! Session-cached, per-column statistics collection. Ports
//! `optimizer/statistics.py`. Sits OVER the `DataSource` trait (source stats) and
//! the `StatsCatalog` store (learned overlay), with three caches.
//!
//! HONEST UNKNOWNS: a `None` returned by the source/catalog is the honest-unknown
//! path; a driver `Err` PROPAGATES (fail loud) rather than being mapped to None.
//!
//! INTERIOR MUTABILITY: the per-column and planner caches are pure memoization
//! behind `Mutex`es, letting every query method take `&self` AND letting one
//! collector be shared across queries via `Arc` (the Runtime holds it for the
//! whole session - a per-query collector would re-fetch every statistic on
//! every plan). This is not "immutability" - a cache detail kept off the public
//! signature; the locks are uncontended (planning is single-threaded).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use fq_catalog::datasource::{ColumnStatistics, DataSource, RenderDialect, TableStatistics};
use fq_catalog::Catalog;
use fq_common::PlanBudget;
use fq_emit::{quote_ident, render_canonical, to_source_sql, Dialect};
use fq_plan::logical::Scan;

use crate::error::{EstimateError, Result};

/// Whether source-planner scan estimates are on. Shares the probe family's kill
/// switch (`FEDQ_STATS_PROBE=0`); anything else, including unset, is on.
fn ask_source_enabled() -> bool {
    std::env::var("FEDQ_STATS_PROBE").map_or(true, |value| value != "0")
}

/// The accumulated statistics state for one (datasource, schema, table).
struct TableStatsEntry {
    row_count: Option<i64>,
    total_size_bytes: i64,
    column_stats: BTreeMap<String, ColumnStatistics>,
    /// Every column ever requested, present or absent - so an absent column is
    /// fetched exactly once per session (its absence is itself cached).
    attempted_columns: HashSet<String>,
}

impl TableStatsEntry {
    /// The accumulated state as a `TableStatistics` snapshot.
    fn merged_view(&self) -> TableStatistics {
        TableStatistics {
            row_count: self.row_count,
            total_size_bytes: self.total_size_bytes,
            column_stats: self.column_stats.clone(),
        }
    }
}

/// Collects and caches per-column statistics from data-source catalogs, with a
/// learned (measured) overlay.
pub struct StatisticsCollector {
    catalog: Arc<Catalog>,
    stats_catalog: Option<Arc<fq_catalog::StatsCatalog>>,
    learned_ttl_seconds: Option<i64>,
    /// The planning wall-clock budget shared with the pipeline. Checked around
    /// every SOURCE fetch, so a plan-time data scan is killed at the fetch that
    /// broke the budget and named in the error, not discovered later at a stage
    /// boundary. `None` only for collectors outside a planning pipeline (tests).
    plan_budget: Mutex<Option<PlanBudget>>,
    cache: Mutex<HashMap<(String, String, String), TableStatsEntry>>,
    // Source-planner (EXPLAIN) estimates keyed by (datasource, rendered SQL),
    // memoized by `scan_planner_estimate`.
    planner_estimates: Mutex<HashMap<(String, String), Option<i64>>>,
}

impl StatisticsCollector {
    /// Wire the collector to the metadata catalog it resolves datasources
    /// through, and optionally a learned-stats catalog whose measured row counts
    /// / NDVs OVERLAY the source's (`None` disables the read path).
    pub fn new(
        catalog: Arc<Catalog>,
        stats_catalog: Option<Arc<fq_catalog::StatsCatalog>>,
        learned_ttl_seconds: Option<i64>,
    ) -> Self {
        Self {
            catalog,
            stats_catalog,
            learned_ttl_seconds,
            plan_budget: Mutex::new(None),
            cache: Mutex::new(HashMap::new()),
            planner_estimates: Mutex::new(HashMap::new()),
        }
    }

    /// The learned-stats catalog, or `None` when the read path is disabled.
    pub fn stats_catalog(&self) -> Option<&fq_catalog::StatsCatalog> {
        self.stats_catalog.as_deref()
    }

    /// The learned-stats TTL (seconds), or `None` for no freshness bound.
    pub fn learned_ttl_seconds(&self) -> Option<i64> {
        self.learned_ttl_seconds
    }

    /// Attach the pipeline's planning budget so source fetches are killed the
    /// moment they push planning past it.
    pub fn set_plan_budget(&self, budget: PlanBudget) {
        *self.plan_budget.lock().expect("budget lock poisoned") = Some(budget);
    }

    /// Raise `PlanBudget` when the shared planning clock has run out, naming
    /// the fetch (`context`) the collector was working on.
    fn check_plan_budget(&self, datasource: &str, schema: &str, table: &str) -> Result<()> {
        let Some(budget) = *self.plan_budget.lock().expect("budget lock poisoned") else {
            return Ok(());
        };
        if !budget.exceeded() {
            return Ok(());
        }
        Err(EstimateError::PlanBudget {
            elapsed_ms: budget.elapsed_ms(),
            budget_ms: budget.limit_ms(),
            context: format!("statistics fetch for {datasource}.{schema}.{table}"),
        })
    }

    /// Statistics for a table covering at least the requested columns, with
    /// learned (measured) values overlaid over the source's when available.
    ///
    /// `None` only when neither the source nor the catalog knows anything; an
    /// unknown datasource name RAISES (a typo would silently disable costing).
    pub fn get_table_statistics(
        &self,
        datasource: &str,
        schema: &str,
        table: &str,
        columns: &[String],
    ) -> Result<Option<TableStatistics>> {
        let source_stats = self.source_statistics(datasource, schema, table, columns)?;
        if self.stats_catalog.is_none() {
            return Ok(source_stats);
        }
        self.overlay_learned(datasource, schema, table, columns, source_stats)
    }

    /// The source's own statistics (cached), or `None` when it has none.
    fn source_statistics(
        &self,
        datasource: &str,
        schema: &str,
        table: &str,
        columns: &[String],
    ) -> Result<Option<TableStatistics>> {
        let source = self
            .catalog
            .get_datasource(datasource)
            .ok_or_else(|| EstimateError::UnknownDatasource(datasource.to_string()))?;
        self.entry_covering(source.as_ref(), datasource, schema, table, columns)
    }

    /// The cache entry's merged view after fetching any columns it does not cover
    /// yet. `None` when the source has no statistics and no entry exists.
    fn entry_covering(
        &self,
        source: &dyn DataSource,
        datasource: &str,
        schema: &str,
        table: &str,
        columns: &[String],
    ) -> Result<Option<TableStatistics>> {
        let key = (
            datasource.to_string(),
            schema.to_string(),
            table.to_string(),
        );
        let missing = self.missing_columns(&key, columns);
        let has_entry = self
            .cache
            .lock()
            .expect("stats cache lock poisoned")
            .contains_key(&key);
        if has_entry && missing.is_empty() {
            // An entry exists and covers every requested column (or nothing was
            // requested): return its snapshot without a round trip.
            return Ok(self
                .cache
                .lock()
                .expect("stats cache lock poisoned")
                .get(&key)
                .map(TableStatsEntry::merged_view));
        }
        // Budget-check BOTH sides of the source round trip: before, so an
        // already-exhausted plan does not start another fetch; after, so the
        // fetch that itself blew the budget (a plan-time data scan) is the one
        // named in the kill.
        self.check_plan_budget(datasource, schema, table)?;
        let fetched = source.get_table_statistics(schema, table, &missing)?;
        self.check_plan_budget(datasource, schema, table)?;
        let Some(fetched) = fetched else {
            // Source has nothing new; return the existing entry unchanged.
            return Ok(self
                .cache
                .lock()
                .expect("stats cache lock poisoned")
                .get(&key)
                .map(TableStatsEntry::merged_view));
        };
        Ok(Some(self.absorb(key, fetched, &missing)))
    }

    /// The requested columns the entry has not attempted yet (all of them when no
    /// entry), preserving the caller's order.
    fn missing_columns(&self, key: &(String, String, String), columns: &[String]) -> Vec<String> {
        let cache = self.cache.lock().expect("stats cache lock poisoned");
        let entry = cache.get(key);
        let mut missing = Vec::new();
        for column in columns {
            if entry.is_none_or(|entry| !entry.attempted_columns.contains(column)) {
                missing.push(column.clone());
            }
        }
        missing
    }

    /// Merge one fetch into the cache entry (created from the fetch on the first
    /// touch), mark every missing column attempted, and return the snapshot.
    fn absorb(
        &self,
        key: (String, String, String),
        fetched: TableStatistics,
        missing: &[String],
    ) -> TableStatistics {
        let mut cache = self.cache.lock().expect("stats cache lock poisoned");
        let entry = cache.entry(key).or_insert_with(|| TableStatsEntry {
            row_count: fetched.row_count,
            total_size_bytes: fetched.total_size_bytes,
            column_stats: BTreeMap::new(),
            attempted_columns: HashSet::new(),
        });
        for column in missing {
            entry.attempted_columns.insert(column.clone());
        }
        for (name, stats) in fetched.column_stats {
            entry.column_stats.insert(name, stats);
        }
        entry.merged_view()
    }

    /// FILL a table's missing statistics from the learned catalog (a row count or
    /// a column NDV the source does not provide) WITHOUT overriding present source
    /// values. Returns the source stats unchanged when nothing was learned.
    fn overlay_learned(
        &self,
        datasource: &str,
        schema: &str,
        table: &str,
        columns: &[String],
        source_stats: Option<TableStatistics>,
    ) -> Result<Option<TableStatistics>> {
        let catalog = self
            .stats_catalog
            .as_ref()
            .expect("overlay_learned only runs when the learned catalog is present");
        let rows = catalog.table_rows(datasource, schema, table, self.learned_ttl_seconds)?;
        let mut ndvs = BTreeMap::new();
        for column in columns {
            if let Some(ndv) =
                catalog.column_ndv(datasource, schema, table, column, self.learned_ttl_seconds)?
            {
                ndvs.insert(column.clone(), ndv);
            }
        }
        if rows.is_none() && ndvs.is_empty() {
            return Ok(source_stats);
        }
        Ok(Some(merge_learned(source_stats, rows, &ndvs)))
    }

    /// The SOURCE PLANNER's row estimate for a filtered logical scan, or `None`
    /// (source offers none / kill switch off). Consulted by the cost model when
    /// its own predicate pricing left gaps: the source prices the predicate from
    /// ITS statistics (DuckDB EXPLAIN `~N rows`, pg EXPLAIN JSON) - an O(1)
    /// catalog read at the source, NEVER an execution. Memoized per
    /// (datasource, rendered SQL) for the collector's lifetime.
    pub fn scan_planner_estimate(&self, scan: &Scan) -> Result<Option<i64>> {
        if !ask_source_enabled() {
            return Ok(None);
        }
        // An unknown datasource RAISES: a typo would silently disable the
        // estimate for every query.
        let source = self
            .catalog
            .get_datasource(&scan.datasource)
            .ok_or_else(|| EstimateError::UnknownDatasource(scan.datasource.clone()))?;
        let sql = render_probe_scan(scan, source.render_dialect())?;
        let key = (scan.datasource.clone(), sql.clone());
        if let Some(cached) = self
            .planner_estimates
            .lock()
            .expect("planner-estimate lock poisoned")
            .get(&key)
        {
            return Ok(*cached);
        }
        let estimate = source.estimate_scan_rows(&scan.schema_name, &scan.table_name, &sql)?;
        self.planner_estimates
            .lock()
            .expect("planner-estimate lock poisoned")
            .insert(key, estimate);
        Ok(estimate)
    }

    /// Drop every cached statistic (a fresh session's view).
    pub fn clear_cache(&self) {
        self.cache
            .lock()
            .expect("stats cache lock poisoned")
            .clear();
    }
}

/// The scan rendered in the source dialect exactly as execution would render it
/// (same emitter, same transpile boundary), minus any grouping: the estimate
/// prices the FILTERED INPUT rows, which is what the group clamp consumes.
fn render_probe_scan(scan: &Scan, dialect: RenderDialect) -> Result<String> {
    let mut items = Vec::with_capacity(scan.columns.len());
    for column in &scan.columns {
        items.push(quote_ident(column));
    }
    let mut from_clause = format!(
        "{}.{}",
        quote_ident(&scan.schema_name),
        quote_ident(&scan.table_name)
    );
    if let Some(alias) = scan.alias.as_deref() {
        from_clause.push_str(" AS ");
        from_clause.push_str(&quote_ident(alias));
    }
    let mut sql = format!("SELECT {} FROM {}", items.join(", "), from_clause);
    if let Some(filters) = scan.filters.as_ref() {
        sql.push_str(" WHERE ");
        sql.push_str(&render_canonical(filters)?);
    }
    Ok(to_source_sql(&sql, emit_dialect(dialect))?)
}

/// Map a source's declared render dialect onto the emitter's dialect enum.
fn emit_dialect(dialect: RenderDialect) -> Dialect {
    match dialect {
        RenderDialect::Postgres => Dialect::Postgres,
        RenderDialect::DuckDb => Dialect::DuckDb,
        RenderDialect::ClickHouse => Dialect::ClickHouse,
    }
}

/// A `TableStatistics` with learned values FILLING the gaps the source left (a
/// `None` row count, a column with no distinct count); present source values are
/// kept. `source_stats` may be `None` (source provided nothing).
fn merge_learned(
    source_stats: Option<TableStatistics>,
    rows: Option<i64>,
    ndvs: &BTreeMap<String, i64>,
) -> TableStatistics {
    let (source_rows, total_size_bytes, mut column_stats) = match source_stats {
        Some(stats) => (stats.row_count, stats.total_size_bytes, stats.column_stats),
        None => (None, 0, BTreeMap::new()),
    };
    // Source value KEPT when present; learned only fills a None.
    let row_count = source_rows.or(rows);
    for (column, ndv) in ndvs {
        let existing = column_stats.get(column).cloned();
        column_stats.insert(column.clone(), filled_column(existing, *ndv));
    }
    TableStatistics {
        row_count,
        total_size_bytes,
        column_stats,
    }
}

/// A column's stats with the learned NDV FILLED IN only where the source lacks
/// one: a present source distinct count is kept (fill, not override).
fn filled_column(existing: Option<ColumnStatistics>, ndv: i64) -> ColumnStatistics {
    match existing {
        Some(stats) if stats.num_distinct.is_some() => stats,
        Some(stats) => ColumnStatistics {
            num_distinct: Some(ndv),
            ..stats
        },
        None => ColumnStatistics {
            num_distinct: Some(ndv),
            null_fraction: 0.0,
            avg_width: 8,
            min_value: None,
            max_value: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;

    use fq_catalog::datasource::{DataSourceCapability, TableMetadata};
    use fq_catalog::CatalogError;
    use fq_common::DataType;

    /// A fake source with a fetch call-counter, so tests prove lazy fetch and
    /// absence-caching. It serves NDV for the columns in `known_columns` only.
    /// `Send + Sync` (atomics/mutex), as the `DataSource` trait requires.
    struct FakeSource {
        row_count: Option<i64>,
        known_columns: Vec<String>,
        calls: AtomicUsize,
        last_requested: Mutex<Vec<String>>,
    }

    impl FakeSource {
        fn new(row_count: Option<i64>, known_columns: &[&str]) -> Self {
            Self {
                row_count,
                known_columns: known_columns.iter().map(|c| (*c).to_string()).collect(),
                calls: AtomicUsize::new(0),
                last_requested: Mutex::new(Vec::new()),
            }
        }

        fn call_count(&self) -> usize {
            self.calls.load(Ordering::SeqCst)
        }

        fn last_requested(&self) -> Vec<String> {
            self.last_requested.lock().unwrap().clone()
        }
    }

    impl DataSource for FakeSource {
        // The trait signature is `-> &str`; the literal is intentional here.
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
            Ok(vec!["t".to_string()])
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
                row_count: self.row_count,
                size_bytes: None,
            })
        }
        fn get_table_statistics(
            &self,
            _schema: &str,
            _table: &str,
            columns: &[String],
        ) -> std::result::Result<Option<TableStatistics>, CatalogError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            *self.last_requested.lock().unwrap() = columns.to_vec();
            let mut column_stats = BTreeMap::new();
            for column in columns {
                if self.known_columns.contains(column) {
                    column_stats.insert(
                        column.clone(),
                        ColumnStatistics {
                            num_distinct: Some(42),
                            null_fraction: 0.0,
                            avg_width: 8,
                            min_value: None,
                            max_value: None,
                        },
                    );
                }
            }
            Ok(Some(TableStatistics {
                row_count: self.row_count,
                total_size_bytes: 0,
                column_stats,
            }))
        }
        fn map_native_type(&self, type_str: &str) -> std::result::Result<DataType, CatalogError> {
            fq_catalog::datasource::map_native_type_default(type_str)
        }
    }

    fn collector_with(source: Arc<FakeSource>) -> StatisticsCollector {
        let mut catalog = Catalog::new();
        catalog.register_datasource(source);
        StatisticsCollector::new(Arc::new(catalog), None, None)
    }

    #[test]
    fn lazy_fetch_only_requests_missing_columns() {
        let source = Arc::new(FakeSource::new(Some(100), &["a", "b"]));
        let collector = collector_with(source.clone());
        collector
            .get_table_statistics("duck", "main", "t", &["a".to_string()])
            .unwrap();
        assert_eq!(source.call_count(), 1);
        assert_eq!(source.last_requested(), vec!["a".to_string()]);
        // Requesting a already cached, b missing -> fetch only b.
        collector
            .get_table_statistics("duck", "main", "t", &["a".to_string(), "b".to_string()])
            .unwrap();
        assert_eq!(source.call_count(), 2);
        assert_eq!(source.last_requested(), vec!["b".to_string()]);
    }

    #[test]
    fn absent_column_is_fetched_once() {
        // "z" has no source stats; a second request must NOT re-fetch it.
        let source = Arc::new(FakeSource::new(Some(100), &["a"]));
        let collector = collector_with(source.clone());
        collector
            .get_table_statistics("duck", "main", "t", &["z".to_string()])
            .unwrap();
        collector
            .get_table_statistics("duck", "main", "t", &["z".to_string()])
            .unwrap();
        assert_eq!(source.call_count(), 1, "absence is cached");
    }

    #[test]
    fn clear_cache_forces_a_refetch() {
        let source = Arc::new(FakeSource::new(Some(100), &["a"]));
        let collector = collector_with(source.clone());
        collector
            .get_table_statistics("duck", "main", "t", &["a".to_string()])
            .unwrap();
        collector.clear_cache();
        collector
            .get_table_statistics("duck", "main", "t", &["a".to_string()])
            .unwrap();
        assert_eq!(source.call_count(), 2);
    }

    #[test]
    fn exhausted_plan_budget_kills_the_source_fetch_and_names_it() {
        let source = Arc::new(FakeSource::new(Some(100), &["a"]));
        let collector = collector_with(source.clone());
        // A zero budget is exceeded before the first fetch starts.
        collector.set_plan_budget(fq_common::PlanBudget::start(0));
        let error = collector
            .get_table_statistics("duck", "main", "t", &["a".to_string()])
            .unwrap_err();
        assert!(matches!(error, EstimateError::PlanBudget { .. }));
        // The kill names the fetch it stopped, and the fetch never ran.
        assert!(error.to_string().contains("duck.main.t"));
        assert_eq!(source.call_count(), 0);
    }

    #[test]
    fn a_generous_plan_budget_does_not_interfere() {
        let source = Arc::new(FakeSource::new(Some(100), &["a"]));
        let collector = collector_with(source);
        collector.set_plan_budget(fq_common::PlanBudget::start(60_000));
        let stats = collector
            .get_table_statistics("duck", "main", "t", &["a".to_string()])
            .unwrap();
        assert!(stats.is_some());
    }

    #[test]
    fn unknown_datasource_raises() {
        let source = Arc::new(FakeSource::new(Some(100), &["a"]));
        let collector = collector_with(source);
        let error = collector
            .get_table_statistics("nope", "main", "t", &[])
            .unwrap_err();
        assert!(matches!(error, EstimateError::UnknownDatasource(_)));
    }

    #[test]
    fn learned_overlay_fills_only_the_gaps() {
        use fq_catalog::StatsCatalog;
        // Source knows column "a" (ndv 42) but NOT the row count.
        let source = Arc::new(FakeSource::new(None, &["a"]));
        let mut catalog = Catalog::new();
        catalog.register_datasource(source);
        // StatsCatalog is not Sync (it holds a rusqlite Connection); the
        // collector uses it single-threaded within one plan walk, and the Arc
        // shape matches the collector's field type.
        #[allow(clippy::arc_with_non_send_sync)]
        let learned = Arc::new(StatsCatalog::open(":memory:").unwrap());
        // Learned: table rows = 500, and an NDV for the un-measured column "b".
        learned.record_table_rows("duck", "main", "t", 500).unwrap();
        learned
            .record_column_ndv("duck", "main", "t", "b", 7)
            .unwrap();
        // A conflicting learned NDV for "a" must NOT override the source's 42.
        learned
            .record_column_ndv("duck", "main", "t", "a", 999)
            .unwrap();
        let collector = StatisticsCollector::new(Arc::new(catalog), Some(learned), None);

        let stats = collector
            .get_table_statistics("duck", "main", "t", &["a".to_string(), "b".to_string()])
            .unwrap()
            .unwrap();
        // Row count filled from learned (source had None).
        assert_eq!(stats.row_count, Some(500));
        // "a" kept the SOURCE value (fill-only, no override).
        assert_eq!(stats.column_stats["a"].num_distinct, Some(42));
        // "b" filled from learned.
        assert_eq!(stats.column_stats["b"].num_distinct, Some(7));
    }

    #[test]
    fn scan_planner_estimate_abstains_when_the_source_offers_none() {
        // FakeSource keeps the trait's default estimate (None): the collector
        // must pass the absence through, never fabricate.
        let source = Arc::new(FakeSource::new(Some(100), &["a"]));
        let collector = collector_with(source);
        let scan = Scan::new("duck", "main", "t", vec!["a".to_string()]);
        assert_eq!(collector.scan_planner_estimate(&scan).unwrap(), None);
    }

    #[test]
    fn scan_planner_estimate_reads_a_real_duckdb_explain() {
        // End to end over a REAL source: the probe renders in the source
        // dialect and the estimate comes from DuckDB's EXPLAIN - an O(1)
        // catalog read, never a data scan or an execution.
        let source = fq_connectors::DuckDbSource::open_in_memory("duck").unwrap();
        source
            .execute_batch("CREATE TABLE t (a INTEGER); INSERT INTO t SELECT * FROM range(100);")
            .unwrap();
        let mut catalog = Catalog::new();
        catalog.register_datasource(Arc::new(source));
        let collector = StatisticsCollector::new(Arc::new(catalog), None, None);
        let scan = Scan::new("duck", "main", "t", vec!["a".to_string()]);
        let estimate = collector.scan_planner_estimate(&scan).unwrap();
        assert_eq!(estimate, Some(100));
    }
}
