//! Refresh planning for materialized views: which base tables a view's stored
//! SELECT reads, whether the config declares change keys for them, and whether
//! the view's SHAPE admits a delta refresh. The store mechanics live in
//! fq-accel; this module is the policy over them.
//!
//! ADMISSIBLE SHAPES. A delta refresh is chosen only when the bound plan of
//! the stored SELECT is a chain of row-wise wrappers over exactly ONE scan:
//!
//! - `Scan` alone, or wrapped by any stack of
//! - plain `Projection` (no DISTINCT / DISTINCT ON) and
//! - `Filter`,
//!
//! where every projection expression and filter predicate is free of function
//! calls, window functions, and subqueries. Under that shape each source row
//! maps to at most one view row independently of every other row, so rows
//! that arrived after the watermark can be transformed and APPENDED (monotonic
//! change key), and rows can be replaced/deleted BY KEY (primary-key change
//! key). Function calls are excluded because the engine carries no volatility
//! metadata - `random()` in a projection would make the delta rows disagree
//! with the stored rows. Everything else - joins, aggregates, DISTINCT, set
//! operations, CTEs, sorts, limits, VALUES, lateral joins - falls back to a
//! whole re-pull, and the refresh status row names the reason.
//!
//! The change-key column must additionally be EXPOSED by the view's output (a
//! projection may alias it but must not drop it): the append watermark is
//! computed over view rows, the delta filter wraps the view's own SELECT, and
//! a merge keys view rows - all of which need the column in the output.

use std::collections::BTreeMap;

use fq_accel::{ViewColumn, Watermark};
use fq_catalog::Catalog;
use fq_common::{ChangeKey, Config, DataType};
use fq_plan::logical::{LogicalPlan, Projection, Scan};
use fq_plan::Expr;

use crate::error::RuntimeError;

/// One base table of a view's stored SELECT.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct BaseTable {
    pub datasource: String,
    pub schema: String,
    pub table: String,
}

impl BaseTable {
    /// The `datasource.schema.table` key the token map uses.
    pub fn token_key(&self) -> String {
        format!("{}.{}.{}", self.datasource, self.schema, self.table)
    }
}

/// How REFRESH pulls this view.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum RefreshDecision {
    /// Monotonic change key: pull rows past the watermark, append.
    Append {
        /// The view OUTPUT column (post-alias) carrying the monotonic key.
        output_column: String,
    },
    /// Primary-key change key: re-pull whole, merge by key, rewrite only
    /// affected chunks.
    Merge {
        /// The view OUTPUT column (post-alias) carrying the key.
        key_column: String,
    },
    /// Re-pull whole; `reason` is surfaced in the refresh status row.
    Whole { reason: String },
}

/// The refresh plan of one view: its base tables (for token capture) and the
/// pull decision.
#[derive(Debug)]
pub(crate) struct RefreshPlan {
    pub base_tables: Vec<BaseTable>,
    pub decision: RefreshDecision,
}

/// Classify a view's stored SELECT against the catalog and the datasource
/// change-key declarations. Parsing or binding failures propagate (the same
/// loud errors executing the SELECT would raise).
pub(crate) fn classify(
    catalog: &Catalog,
    config: &Config,
    definition_sql: &str,
) -> Result<RefreshPlan, RuntimeError> {
    let parsed = fq_parse::parse_with_catalog(definition_sql, catalog)?;
    let bound = fq_bind::bind(catalog, parsed)?;
    let base_tables = collect_base_tables(&bound);
    let decision = decide(config, &bound, &base_tables);
    Ok(RefreshPlan {
        base_tables,
        decision,
    })
}

/// The pull decision for a bound definition plan.
fn decide(config: &Config, bound: &LogicalPlan, base_tables: &[BaseTable]) -> RefreshDecision {
    if base_tables.is_empty() {
        return RefreshDecision::Whole {
            reason: "the view reads no base table".to_string(),
        };
    }
    for table in base_tables {
        if change_key_for(config, table).is_none() {
            return RefreshDecision::Whole {
                reason: format!("table '{}' has no declared change key", table.token_key()),
            };
        }
    }
    let chain = match admissible_chain(bound) {
        Ok(chain) => chain,
        Err(reason) => return RefreshDecision::Whole { reason },
    };
    // The admissible shape has exactly one scan, and its table passed the
    // declaration check above.
    let scan_table = BaseTable {
        datasource: chain.scan.datasource.clone(),
        schema: chain.scan.schema_name.clone(),
        table: chain.scan.table_name.clone(),
    };
    let change_key = change_key_for(config, &scan_table).expect("declaration checked above");
    match map_to_output(&chain, change_key.column()) {
        Err(reason) => RefreshDecision::Whole { reason },
        Ok(output_column) => match change_key {
            ChangeKey::Monotonic { .. } => RefreshDecision::Append { output_column },
            ChangeKey::PrimaryKey { .. } => RefreshDecision::Merge {
                key_column: output_column,
            },
        },
    }
}

/// The declared change key of one base table, matched case-insensitively on
/// its `schema.table` config key. None when the datasource is not configured
/// (the materialized-view store itself) or the table is not declared.
fn change_key_for<'a>(config: &'a Config, table: &BaseTable) -> Option<&'a ChangeKey> {
    let datasource = config.datasources.get(&table.datasource)?;
    let wanted = format!("{}.{}", table.schema, table.table);
    for (key, change_key) in &datasource.change_keys {
        if key.eq_ignore_ascii_case(&wanted) {
            return Some(change_key);
        }
    }
    None
}

/// Every distinct base table the plan scans, in first-appearance order.
fn collect_base_tables(plan: &LogicalPlan) -> Vec<BaseTable> {
    let mut tables: Vec<BaseTable> = Vec::new();
    collect_scans(plan, &mut tables);
    tables
}

/// Depth-first scan collection (children() covers CTE bodies and set-op arms).
fn collect_scans(plan: &LogicalPlan, tables: &mut Vec<BaseTable>) {
    if let LogicalPlan::Scan(scan) = plan {
        let table = BaseTable {
            datasource: scan.datasource.clone(),
            schema: scan.schema_name.clone(),
            table: scan.table_name.clone(),
        };
        if !tables.contains(&table) {
            tables.push(table);
        }
    }
    for child in plan.children() {
        collect_scans(child, tables);
    }
}

/// The admitted wrapper chain: the single scan at the bottom and the
/// projections above it, through which the change-key column's output name is
/// resolved bottom-up.
struct AdmissibleChain<'a> {
    scan: &'a Scan,
    /// Projections from the scan UPWARD (innermost first; recursion pushes
    /// while unwinding).
    projections: Vec<&'a Projection>,
}

/// Walk down the plan admitting only plain projections and filters over one
/// scan; any other node (or a non-deterministic expression) declines with the
/// reason the status row reports.
fn admissible_chain(plan: &LogicalPlan) -> Result<AdmissibleChain<'_>, String> {
    match plan {
        LogicalPlan::Scan(scan) => {
            require_plain_scan(scan)?;
            Ok(AdmissibleChain {
                scan,
                projections: Vec::new(),
            })
        }
        LogicalPlan::Projection(projection) => {
            if projection.distinct || projection.distinct_on.is_some() {
                return Err(
                    "view shape is not delta-maintainable (DISTINCT projection)".to_string()
                );
            }
            require_deterministic_all(&projection.expressions)?;
            let mut chain = admissible_chain(&projection.input)?;
            chain.projections.push(projection);
            Ok(chain)
        }
        LogicalPlan::Filter(filter) => {
            require_deterministic(&filter.predicate)?;
            admissible_chain(&filter.input)
        }
        other => Err(format!(
            "view shape is not delta-maintainable ({} node)",
            node_kind(other)
        )),
    }
}

/// A bound scan carries no folded clauses (folding happens in the optimizer,
/// which never sees this plan); any folded clause here would change the
/// row-wise delta semantics, so it declines defensively.
fn require_plain_scan(scan: &Scan) -> Result<(), String> {
    let plain = scan.filters.is_none()
        && scan.sample.is_none()
        && scan.group_by.is_none()
        && scan.grouping_sets.is_none()
        && scan.aggregates.is_none()
        && scan.limit.is_none()
        && scan.offset == 0
        && scan.order_by_keys.is_none()
        && !scan.distinct;
    if plain {
        return Ok(());
    }
    Err("view shape is not delta-maintainable (scan carries folded clauses)".to_string())
}

/// Decline any expression the engine cannot prove row-wise deterministic: a
/// function call (no volatility metadata - it could be random()/now()), a
/// window function, or a subquery.
fn require_deterministic(expr: &Expr) -> Result<(), String> {
    match expr {
        Expr::FunctionCall { function_name, .. } => {
            return Err(format!(
                "view shape is not delta-maintainable (function call '{function_name}'; \
                 the engine cannot prove it deterministic)"
            ));
        }
        Expr::Window { .. } => {
            return Err("view shape is not delta-maintainable (window function)".to_string());
        }
        Expr::Subquery { .. }
        | Expr::Exists { .. }
        | Expr::InSubquery { .. }
        | Expr::QuantifiedComparison { .. } => {
            return Err("view shape is not delta-maintainable (subquery)".to_string());
        }
        Expr::Column(_)
        | Expr::Literal { .. }
        | Expr::BinaryOp { .. }
        | Expr::UnaryOp { .. }
        | Expr::Case { .. }
        | Expr::InList { .. }
        | Expr::Between { .. }
        | Expr::Like { .. }
        | Expr::Cast { .. }
        | Expr::Extract { .. }
        | Expr::Interval { .. }
        | Expr::Tuple { .. } => {}
    }
    require_deterministic_all(expr.children())
}

/// `require_deterministic` over a collection.
fn require_deterministic_all<'a>(exprs: impl IntoIterator<Item = &'a Expr>) -> Result<(), String> {
    for expr in exprs {
        require_deterministic(expr)?;
    }
    Ok(())
}

/// Resolve the change-key SOURCE column to the view's OUTPUT column name by
/// following it up through the projection chain: each projection must carry a
/// bare reference to it (possibly aliased); a projection that drops or
/// computes over it declines.
fn map_to_output(chain: &AdmissibleChain<'_>, source_column: &str) -> Result<String, String> {
    let mut current = source_column.to_string();
    for projection in &chain.projections {
        current = project_column(projection, &current, source_column)?;
    }
    Ok(current)
}

/// The output name `column` takes through one projection: the alias of the
/// first expression that is a bare reference to it.
fn project_column(
    projection: &Projection,
    column: &str,
    source_column: &str,
) -> Result<String, String> {
    for (expr, alias) in projection.expressions.iter().zip(&projection.aliases) {
        if let Expr::Column(reference) = expr {
            if reference.column.eq_ignore_ascii_case(column) {
                return Ok(alias.clone());
            }
        }
    }
    Err(format!(
        "change-key column '{source_column}' is not exposed by the view's SELECT; \
         a delta needs it in the output"
    ))
}

/// The plan-node kind for a decline reason. Exhaustive: a new node forces an
/// arm here.
fn node_kind(plan: &LogicalPlan) -> &'static str {
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

/// Read the current version token of every base table. A connector that
/// cannot answer contributes no entry (the refresh then pulls); a datasource
/// the bound plan names but the catalog lost is a bug and raises.
pub(crate) fn read_tokens(
    catalog: &Catalog,
    base_tables: &[BaseTable],
) -> Result<BTreeMap<String, String>, RuntimeError> {
    let mut tokens = BTreeMap::new();
    for table in base_tables {
        let datasource = catalog.get_datasource(&table.datasource).ok_or_else(|| {
            RuntimeError::MaterializedView(format!(
                "datasource '{}' of the view's definition is not registered",
                table.datasource
            ))
        })?;
        if let Some(token) = datasource.source_token(&table.schema, &table.table)? {
            tokens.insert(table.token_key(), token);
        }
    }
    Ok(tokens)
}

/// Whether a refresh may skip its pull: every base table must have BOTH a
/// stored and a current token and they must all match. Any absent token means
/// "unknown", and unknown never skips. A view with no base tables never skips
/// either (re-running a constant SELECT is cheap and always correct).
pub(crate) fn tokens_allow_skip(
    stored: &BTreeMap<String, String>,
    current: &BTreeMap<String, String>,
    base_tables: &[BaseTable],
) -> bool {
    if base_tables.is_empty() {
        return false;
    }
    for table in base_tables {
        let key = table.token_key();
        match (stored.get(&key), current.get(&key)) {
            (Some(old), Some(new)) if old == new => {}
            _ => return false,
        }
    }
    true
}

/// The delta-pull SQL: the stored SELECT wrapped as a derived table, its
/// output columns re-projected in stored order, filtered to rows past the
/// watermark. The engine's own optimizer pushes the filter into the source
/// scan, so the source transfers only the delta.
pub(crate) fn delta_sql(
    definition_sql: &str,
    columns: &[ViewColumn],
    watermark_column: &str,
    watermark: Option<&Watermark>,
) -> String {
    let body = definition_sql.trim().trim_end_matches(';').trim();
    let mut projected = Vec::with_capacity(columns.len());
    for column in columns {
        projected.push(quote_identifier(&column.name));
    }
    let select = format!("SELECT {} FROM ({body}) AS fq_delta", projected.join(", "));
    match watermark {
        // No watermark = the view holds no rows; every row is the delta.
        None => select,
        Some(watermark) => format!(
            "{select} WHERE {} > {}",
            quote_identifier(watermark_column),
            watermark.sql_literal()
        ),
    }
}

/// Double-quote an identifier, escaping embedded quotes.
fn quote_identifier(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// Validate every `change_keys` declaration against the loaded catalog: the
/// table must exist on its datasource, the column must exist on the table,
/// and a MONOTONIC column's type must be able to carry an exact watermark
/// (integer, text, date, or timestamp). Runs at runtime construction so a
/// typo'd declaration fails the config load, not a refresh months later.
pub(crate) fn validate_change_keys(catalog: &Catalog, config: &Config) -> Result<(), RuntimeError> {
    for datasource in config.datasources.values() {
        for (table_key, change_key) in &datasource.change_keys {
            validate_one_change_key(catalog, &datasource.name, table_key, change_key)?;
        }
    }
    Ok(())
}

/// Validate one declaration (see `validate_change_keys`).
fn validate_one_change_key(
    catalog: &Catalog,
    datasource: &str,
    table_key: &str,
    change_key: &ChangeKey,
) -> Result<(), RuntimeError> {
    let (schema, table) = table_key
        .split_once('.')
        .expect("config parsing enforced the schema.table form");
    let found = catalog
        .resolve_table(Some(datasource), Some(schema), table)
        .ok_or_else(|| {
            RuntimeError::Config(format!(
                "change_keys of datasource '{datasource}' names unknown table '{table_key}'"
            ))
        })?;
    let column_name = change_key.column();
    let column = found
        .columns
        .iter()
        .find(|column| column.name.eq_ignore_ascii_case(column_name))
        .ok_or_else(|| {
            RuntimeError::Config(format!(
                "change_keys of datasource '{datasource}' names unknown column \
                 '{column_name}' on table '{table_key}'"
            ))
        })?;
    if let ChangeKey::Monotonic { .. } = change_key {
        require_watermark_type(datasource, table_key, column_name, column.data_type)?;
    }
    Ok(())
}

/// The engine types a monotonic watermark can carry exactly. Anything else
/// (floats, decimals, booleans, intervals) raises at load: the declaration
/// could never drive a delta, so accepting it would silently degrade every
/// refresh to a whole re-pull.
fn require_watermark_type(
    datasource: &str,
    table_key: &str,
    column: &str,
    data_type: DataType,
) -> Result<(), RuntimeError> {
    match data_type {
        DataType::Integer
        | DataType::BigInt
        | DataType::Date
        | DataType::Timestamp
        | DataType::Text
        | DataType::Varchar => Ok(()),
        DataType::Float
        | DataType::Double
        | DataType::Decimal
        | DataType::Boolean
        | DataType::Interval
        | DataType::Null => Err(RuntimeError::Config(format!(
            "change_keys of datasource '{datasource}': monotonic column '{column}' of \
             '{table_key}' has type {}, which cannot carry an exact watermark; declare \
             an integer, text, date, or timestamp column (or a primary_key for merge)",
            data_type.value()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fq_common::{CostConfig, DataSourceConfig, ExecutorConfig, OptimizerConfig, ServerConfig};
    use fq_connectors::DuckDbSource;
    use std::sync::Arc;

    /// A catalog over an in-memory DuckDB seeded with events(id, updated_at,
    /// payload) and customers(c_id, c_name).
    fn seeded_catalog() -> Catalog {
        let source = DuckDbSource::open_in_memory("duck").expect("open");
        source
            .execute_batch(
                "CREATE TABLE events (id BIGINT, updated_at TIMESTAMP, payload VARCHAR, \
                     score DOUBLE);
                 CREATE TABLE customers (c_id INTEGER, c_name VARCHAR);",
            )
            .expect("seed");
        let mut catalog = Catalog::new();
        catalog.register_datasource(Arc::new(source));
        catalog.load_metadata().expect("metadata");
        catalog
    }

    /// A config whose `duck` datasource declares `keys` (as YAML-parsed pairs).
    fn config_with(keys: &[(&str, ChangeKey)]) -> Config {
        let mut change_keys = BTreeMap::new();
        for (table, key) in keys {
            change_keys.insert((*table).to_string(), key.clone());
        }
        let mut datasources = BTreeMap::new();
        datasources.insert(
            "duck".to_string(),
            DataSourceConfig {
                name: "duck".to_string(),
                ty: "duckdb".to_string(),
                config: BTreeMap::new(),
                capabilities: Vec::new(),
                change_keys,
            },
        );
        Config {
            datasources,
            optimizer: OptimizerConfig::default(),
            executor: ExecutorConfig::default(),
            cost: CostConfig::default(),
            server: ServerConfig::default(),
            accelerator: fq_common::AcceleratorConfig::default(),
            source_path: None,
        }
    }

    /// The monotonic declaration on events.id.
    fn monotonic_events() -> Config {
        config_with(&[(
            "main.events",
            ChangeKey::Monotonic {
                column: "id".to_string(),
            },
        )])
    }

    #[test]
    fn a_plain_scan_under_a_monotonic_key_appends() {
        let plan = classify(
            &seeded_catalog(),
            &monotonic_events(),
            "SELECT id, payload FROM duck.main.events",
        )
        .expect("classify");
        assert_eq!(plan.base_tables.len(), 1);
        assert_eq!(plan.base_tables[0].token_key(), "duck.main.events");
        assert_eq!(
            plan.decision,
            RefreshDecision::Append {
                output_column: "id".to_string()
            }
        );
    }

    #[test]
    fn filters_and_alias_projections_stay_admissible() {
        let plan = classify(
            &seeded_catalog(),
            &monotonic_events(),
            "SELECT id AS event_id, payload FROM duck.main.events WHERE payload <> 'x'",
        )
        .expect("classify");
        // The watermark tracks the OUTPUT name the projection gave the column.
        assert_eq!(
            plan.decision,
            RefreshDecision::Append {
                output_column: "event_id".to_string()
            }
        );
    }

    #[test]
    fn a_primary_key_declaration_merges() {
        let config = config_with(&[(
            "main.customers",
            ChangeKey::PrimaryKey {
                column: "c_id".to_string(),
            },
        )]);
        let plan = classify(
            &seeded_catalog(),
            &config,
            "SELECT c_id, c_name FROM duck.main.customers",
        )
        .expect("classify");
        assert_eq!(
            plan.decision,
            RefreshDecision::Merge {
                key_column: "c_id".to_string()
            }
        );
    }

    #[test]
    fn an_undeclared_table_re_pulls_whole() {
        let plan = classify(
            &seeded_catalog(),
            &config_with(&[]),
            "SELECT id FROM duck.main.events",
        )
        .expect("classify");
        match plan.decision {
            RefreshDecision::Whole { reason } => {
                assert!(reason.contains("no declared change key"), "{reason}");
            }
            other => panic!("expected Whole, got {other:?}"),
        }
    }

    #[test]
    fn joins_and_aggregates_decline_with_the_node_kind() {
        let config = config_with(&[
            (
                "main.events",
                ChangeKey::Monotonic {
                    column: "id".to_string(),
                },
            ),
            (
                "main.customers",
                ChangeKey::PrimaryKey {
                    column: "c_id".to_string(),
                },
            ),
        ]);
        let catalog = seeded_catalog();
        let join = classify(
            &catalog,
            &config,
            "SELECT e.id, c.c_name FROM duck.main.events e \
             JOIN duck.main.customers c ON e.id = c.c_id",
        )
        .expect("classify");
        match join.decision {
            RefreshDecision::Whole { reason } => assert!(reason.contains("Join"), "{reason}"),
            other => panic!("expected Whole, got {other:?}"),
        }
        let aggregate = classify(
            &catalog,
            &config,
            "SELECT count(*) AS c FROM duck.main.events",
        )
        .expect("classify");
        match aggregate.decision {
            RefreshDecision::Whole { reason } => {
                assert!(reason.contains("Aggregate"), "{reason}");
            }
            other => panic!("expected Whole, got {other:?}"),
        }
    }

    #[test]
    fn dropping_the_change_key_column_declines() {
        let plan = classify(
            &seeded_catalog(),
            &monotonic_events(),
            "SELECT payload FROM duck.main.events",
        )
        .expect("classify");
        match plan.decision {
            RefreshDecision::Whole { reason } => {
                assert!(reason.contains("not exposed"), "{reason}");
            }
            other => panic!("expected Whole, got {other:?}"),
        }
    }

    #[test]
    fn function_calls_and_distinct_decline() {
        let catalog = seeded_catalog();
        let config = monotonic_events();
        let function = classify(
            &catalog,
            &config,
            "SELECT id, upper(payload) AS p FROM duck.main.events",
        )
        .expect("classify");
        match function.decision {
            RefreshDecision::Whole { reason } => {
                assert!(reason.contains("function call"), "{reason}");
            }
            other => panic!("expected Whole, got {other:?}"),
        }
        let distinct = classify(
            &catalog,
            &config,
            "SELECT DISTINCT id FROM duck.main.events",
        )
        .expect("classify");
        match distinct.decision {
            RefreshDecision::Whole { reason } => {
                assert!(reason.contains("DISTINCT"), "{reason}");
            }
            other => panic!("expected Whole, got {other:?}"),
        }
    }

    #[test]
    fn delta_sql_wraps_the_definition_and_filters_past_the_watermark() {
        let columns = vec![
            ViewColumn {
                name: "event_id".to_string(),
                data_type: DataType::BigInt,
                nullable: true,
            },
            ViewColumn {
                name: "payload".to_string(),
                data_type: DataType::Text,
                nullable: true,
            },
        ];
        let sql = delta_sql(
            "SELECT id AS event_id, payload FROM duck.main.events;",
            &columns,
            "event_id",
            Some(&Watermark::Int(41)),
        );
        assert_eq!(
            sql,
            "SELECT \"event_id\", \"payload\" FROM \
             (SELECT id AS event_id, payload FROM duck.main.events) AS fq_delta \
             WHERE \"event_id\" > 41"
        );
        // No watermark (empty view): every row is the delta.
        let all = delta_sql("SELECT id FROM t", &columns[..1], "event_id", None);
        assert!(!all.contains("WHERE"), "{all}");
    }

    #[test]
    fn token_skip_requires_every_table_known_and_equal() {
        let table = BaseTable {
            datasource: "duck".to_string(),
            schema: "main".to_string(),
            table: "events".to_string(),
        };
        let tables = vec![table];
        let mut stored = BTreeMap::new();
        stored.insert("duck.main.events".to_string(), "t0".to_string());
        let mut current = stored.clone();
        assert!(tokens_allow_skip(&stored, &current, &tables));
        current.insert("duck.main.events".to_string(), "t1".to_string());
        assert!(!tokens_allow_skip(&stored, &current, &tables));
        assert!(!tokens_allow_skip(&stored, &BTreeMap::new(), &tables));
        assert!(!tokens_allow_skip(&BTreeMap::new(), &current, &tables));
        // No base tables: never skip.
        assert!(!tokens_allow_skip(&stored, &stored.clone(), &[]));
    }

    #[test]
    fn validation_rejects_unknown_tables_columns_and_bad_watermark_types() {
        let catalog = seeded_catalog();
        let unknown_table = config_with(&[(
            "main.ghost",
            ChangeKey::Monotonic {
                column: "id".to_string(),
            },
        )]);
        let error = validate_change_keys(&catalog, &unknown_table).unwrap_err();
        assert!(format!("{error}").contains("unknown table"), "{error}");

        let unknown_column = config_with(&[(
            "main.events",
            ChangeKey::PrimaryKey {
                column: "ghost".to_string(),
            },
        )]);
        let error = validate_change_keys(&catalog, &unknown_column).unwrap_err();
        assert!(format!("{error}").contains("unknown column"), "{error}");

        // A DOUBLE column cannot carry an exact watermark; a monotonic
        // declaration on it raises at load.
        let bad_type = config_with(&[(
            "main.events",
            ChangeKey::Monotonic {
                column: "score".to_string(),
            },
        )]);
        let error = validate_change_keys(&catalog, &bad_type).unwrap_err();
        assert!(format!("{error}").contains("watermark"), "{error}");

        // The same column IS a valid primary key (merge has no type limit).
        let pk_ok = config_with(&[(
            "main.events",
            ChangeKey::PrimaryKey {
                column: "score".to_string(),
            },
        )]);
        validate_change_keys(&catalog, &pk_ok).expect("primary key on a double is fine");
    }

    #[test]
    fn validation_accepts_the_declared_forms() {
        let catalog = seeded_catalog();
        let config = config_with(&[
            (
                "main.events",
                ChangeKey::Monotonic {
                    column: "updated_at".to_string(),
                },
            ),
            (
                "main.customers",
                ChangeKey::PrimaryKey {
                    column: "c_id".to_string(),
                },
            ),
        ]);
        validate_change_keys(&catalog, &config).expect("valid declarations");
    }
}
