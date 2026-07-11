// Imported fedqrs engine core - already validated by the TPC tallies; pedantic-
// cleaned incrementally, not on import.
#![allow(clippy::all, clippy::pedantic)]
//! Render a `ScanSpec` to dialect-correct source SQL, in Rust.
//!
//! The SELECT skeleton (columns, FROM, LIMIT) is hand-built; filter expressions
//! are rendered by DataFusion's unparser so dialect-divergent syntax (quoting,
//! literal formatting) is handled without a bespoke per-dialect emitter. The
//! dynamic semi-join filter arrives as an already-built `Expr` and is ANDed in,
//! so the build-side key values are formatted here in Rust and never seen by
//! Python.

use datafusion::common::DataFusionError;
use datafusion::logical_expr::Expr;
use datafusion::sql::unparser::dialect::{DefaultDialect, Dialect, PostgreSqlDialect};
use datafusion::sql::unparser::Unparser;

use crate::core::expr::to_df_expr;
use crate::core::ir::{IrExpr, ScanSpec};
use crate::core::types::DsKind;

/// Render a DataFusion expression to SQL text in the default dialect. Used when
/// building a local fragment's SQL (aggregate), which DataFusion itself parses.
pub fn render_expr(e: &Expr) -> Result<String, DataFusionError> {
    let unparser = Unparser::new(&DefaultDialect {});
    Ok(unparser.expr_to_sql(e)?.to_string())
}

/// Quote an identifier for Postgres/DuckDB (double quotes, doubled internally).
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// `"schema"."table" [AS "alias"]`
fn table_ref(scan: &ScanSpec, table: &str) -> String {
    let mut s = String::new();
    if let Some(schema) = &scan.schema {
        s.push_str(&quote_ident(schema));
        s.push('.');
    }
    s.push_str(&quote_ident(table));
    if let Some(alias) = &scan.alias {
        s.push_str(" AS ");
        s.push_str(&quote_ident(alias));
    }
    s
}

fn dialect_for(kind: DsKind) -> Box<dyn Dialect> {
    match kind {
        DsKind::Postgres => Box::new(PostgreSqlDialect {}),
        // DuckDB SQL is standard enough for the default dialect for now.
        DsKind::DuckDb => Box::new(DefaultDialect {}),
        // Parquet is read through DataFusion; the default dialect matches it.
        DsKind::Parquet => Box::new(DefaultDialect {}),
    }
}

/// Render `scan` to a SQL string. `extra_filter`, if present, is ANDed into the
/// WHERE clause (this is the runtime `col IN (...)` semi-join reduction).
pub fn scan_sql(
    kind: DsKind,
    scan: &ScanSpec,
    extra_filter: Option<Expr>,
) -> Result<String, DataFusionError> {
    // Pre-rendered SQL (a complex single-source subtree). A dynamic filter is
    // applied by wrapping the opaque SQL as a derived table and filtering its
    // OUTPUT columns - which is exactly what the injected column names.
    if let Some(raw) = &scan.raw_sql {
        return match extra_filter {
            None => Ok(raw.clone()),
            Some(filter) => wrapped_raw_sql(kind, raw, &filter),
        };
    }

    let table = scan
        .table
        .as_ref()
        .ok_or_else(|| DataFusionError::Plan("structured scan needs a 'table'".into()))?;
    if scan.columns.is_empty() {
        return Err(DataFusionError::Plan(format!(
            "scan of '{table}' has no output columns"
        )));
    }

    let mut sql = String::from("SELECT ");
    if scan.distinct {
        sql.push_str("DISTINCT ");
    }
    let cols: Vec<String> = scan.columns.iter().map(|c| quote_ident(c)).collect();
    sql.push_str(&cols.join(", "));
    sql.push_str(" FROM ");
    sql.push_str(&table_ref(scan, table));

    // Combine the static filter with the dynamic one (either may be absent).
    let filter: Option<Expr> = match (&scan.filter, extra_filter) {
        (Some(f), Some(x)) => Some(to_df_expr(f)?.and(x)),
        (Some(f), None) => Some(to_df_expr(f)?),
        (None, Some(x)) => Some(x),
        (None, None) => None,
    };
    if let Some(f) = filter {
        let dialect = dialect_for(kind);
        let unparser = Unparser::new(dialect.as_ref());
        let rendered = unparser.expr_to_sql(&f)?;
        sql.push_str(" WHERE ");
        sql.push_str(&rendered.to_string());
    }

    if let Some(n) = scan.limit {
        sql.push_str(&format!(" LIMIT {n}"));
    }
    Ok(sql)
}

/// A pre-rendered subtree filtered on its output columns:
/// `SELECT * FROM (<raw>) AS "fedq_probe" WHERE <filter>`.
fn wrapped_raw_sql(kind: DsKind, raw: &str, filter: &Expr) -> Result<String, DataFusionError> {
    let dialect = dialect_for(kind);
    let unparser = Unparser::new(dialect.as_ref());
    let rendered = unparser.expr_to_sql(filter)?;
    Ok(format!(
        "SELECT * FROM ({raw}) AS \"fedq_probe\" WHERE {rendered}"
    ))
}

/// Render the scan's static filter to SQL (for the parallel-scan strategy,
/// which applies the base predicate but not the dynamic key filter).
pub fn base_filter_sql(kind: DsKind, scan: &ScanSpec) -> Result<Option<String>, DataFusionError> {
    match &scan.filter {
        Some(f) => Ok(Some(render_filter(kind, f)?)),
        None => Ok(None),
    }
}

/// The scan's SELECT column list as SQL (for the parallel-scan strategy).
pub fn select_list_sql(scan: &ScanSpec) -> String {
    let cols: Vec<String> = scan.columns.iter().map(|c| quote_ident(c)).collect();
    cols.join(", ")
}

/// Build the temp-table pushdown query: the probe scan restricted to rows whose
/// `inject_col` appears in the ingested keys temp table (a server-side semi-join).
pub fn temp_join_sql(
    kind: DsKind,
    scan: &ScanSpec,
    temp_table: &str,
    key_col: &str,
    inject_col: &str,
) -> Result<String, DataFusionError> {
    let membership = format!(
        "{} IN (SELECT {} FROM {})",
        quote_ident(inject_col),
        quote_ident(key_col),
        quote_ident(temp_table)
    );
    // A pre-rendered subtree probe: wrap it and apply the membership test to
    // its output columns (a raw spec carries its own filters inside).
    if let Some(raw) = &scan.raw_sql {
        return Ok(format!(
            "SELECT * FROM ({raw}) AS \"fedq_probe\" WHERE {membership}"
        ));
    }
    let table = scan
        .table
        .as_ref()
        .ok_or_else(|| DataFusionError::Plan("temp-join needs a structured table".into()))?;
    if scan.columns.is_empty() {
        return Err(DataFusionError::Plan(
            "temp-join scan has no columns".into(),
        ));
    }

    let mut sql = String::from("SELECT ");
    sql.push_str(&select_list_sql(scan));
    sql.push_str(" FROM ");
    sql.push_str(&table_ref(scan, table));

    let mut clauses = Vec::new();
    if let Some(f) = &scan.filter {
        clauses.push(render_filter(kind, f)?);
    }
    clauses.push(membership);
    sql.push_str(" WHERE ");
    sql.push_str(&clauses.join(" AND "));
    Ok(sql)
}

/// Render one IR filter expression to SQL in the source dialect.
fn render_filter(kind: DsKind, filter: &IrExpr) -> Result<String, DataFusionError> {
    let dialect = dialect_for(kind);
    let unparser = Unparser::new(dialect.as_ref());
    Ok(unparser.expr_to_sql(&to_df_expr(filter)?)?.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ir::ScanSpec;

    fn scan(json: &str) -> ScanSpec {
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn renders_columns_table_and_filter() {
        let s = scan(
            r#"{"schema":"public","table":"t","columns":["a","b"],
                "filter":{"node":"binary","op":">","left":{"node":"column","name":"a"},
                          "right":{"node":"literal","value":{"lit":"int","value":5}}}}"#,
        );
        let sql = scan_sql(DsKind::Postgres, &s, None).unwrap();
        assert!(
            sql.starts_with("SELECT \"a\", \"b\" FROM \"public\".\"t\""),
            "{sql}"
        );
        assert!(sql.contains("WHERE"), "{sql}");
        assert!(sql.contains('5'), "{sql}");
    }

    #[test]
    fn raw_sql_passes_through_and_wraps_for_injection() {
        let s = scan(r#"{"raw_sql":"SELECT 1 AS x"}"#);
        assert_eq!(
            scan_sql(DsKind::Postgres, &s, None).unwrap(),
            "SELECT 1 AS x"
        );
        // a dynamic filter wraps the opaque SQL and filters its outputs
        let sql = scan_sql(DsKind::Postgres, &s, Some(datafusion::prelude::lit(true))).unwrap();
        assert!(
            sql.starts_with("SELECT * FROM (SELECT 1 AS x) AS \"fedq_probe\" WHERE"),
            "{sql}"
        );
    }

    #[test]
    fn temp_join_wraps_a_raw_sql_probe() {
        let s = scan(r#"{"raw_sql":"SELECT l FROM t"}"#);
        let sql = temp_join_sql(DsKind::DuckDb, &s, "keys_tmp", "k", "l").unwrap();
        assert_eq!(
            sql,
            "SELECT * FROM (SELECT l FROM t) AS \"fedq_probe\" \
             WHERE \"l\" IN (SELECT \"k\" FROM \"keys_tmp\")"
        );
    }

    #[test]
    fn temp_join_is_a_membership_semijoin() {
        let s = scan(r#"{"schema":"public","table":"probe","columns":["id","v"]}"#);
        let sql = temp_join_sql(DsKind::Postgres, &s, "keys_tmp", "k", "id").unwrap();
        assert!(sql.contains("FROM \"public\".\"probe\""), "{sql}");
        assert!(
            sql.contains("\"id\" IN (SELECT \"k\" FROM \"keys_tmp\")"),
            "{sql}"
        );
    }

    #[test]
    fn base_filter_and_select_list_helpers() {
        let s = scan(r#"{"table":"t","columns":["a","b"]}"#);
        assert_eq!(select_list_sql(&s), "\"a\", \"b\"");
        assert!(base_filter_sql(DsKind::Postgres, &s).unwrap().is_none());
    }
}
