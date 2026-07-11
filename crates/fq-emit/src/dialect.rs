//! The single transpile boundary: canonical Postgres SQL text -> a source's own
//! dialect, via polyglot-sql. Ports `plan/physical.py::to_source_sql`.

use polyglot_sql::DialectType;

use crate::error::EmitError;

/// A SQL dialect the engine renders for. The canonical emitter always produces
/// Postgres form; `to_source_sql` transpiles that to one of these targets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Dialect {
    Postgres,
    DuckDb,
    ClickHouse,
    DataFusion,
}

impl Dialect {
    /// The polyglot dialect this maps to (the transpile target).
    pub fn dialect_type(self) -> DialectType {
        match self {
            Dialect::Postgres => DialectType::PostgreSQL,
            Dialect::DuckDb => DialectType::DuckDB,
            Dialect::ClickHouse => DialectType::ClickHouse,
            Dialect::DataFusion => DialectType::DataFusion,
        }
    }

    /// The lowercase name, used only in transpile-error text.
    pub fn name(self) -> &'static str {
        match self {
            Dialect::Postgres => "postgres",
            Dialect::DuckDb => "duckdb",
            Dialect::ClickHouse => "clickhouse",
            Dialect::DataFusion => "datafusion",
        }
    }
}

/// Transpile canonical Postgres-form SQL into `target`'s dialect. The single
/// to_source_sql boundary: Postgres->Postgres is a proven identity, other targets
/// apply polyglot's cross-dialect rewrites (function names, TABLESAMPLE,
/// ordered-set aggregates). Raises if transpile fails or yields other than one
/// statement.
///
/// It is deliberately NOT short-circuited for the Postgres target: routing every
/// target (Postgres included) through polyglot re-parses our canonical text, so
/// the "our emitted SQL is valid, parseable Postgres" invariant is checked
/// uniformly and a malformed string can never be silently passed through.
pub fn to_source_sql(pg_sql: &str, target: Dialect) -> Result<String, EmitError> {
    let statements =
        polyglot_sql::transpile(pg_sql, DialectType::PostgreSQL, target.dialect_type()).map_err(
            |error| EmitError::Transpile {
                dialect: target.name(),
                reason: error.to_string(),
            },
        )?;
    let [statement] = statements.as_slice() else {
        return Err(EmitError::Transpile {
            dialect: target.name(),
            reason: format!("expected exactly one statement, got {}", statements.len()),
        });
    };
    Ok(statement.clone())
}

#[cfg(test)]
mod tests {
    use super::{to_source_sql, Dialect};
    use crate::error::EmitError;

    #[test]
    fn string_agg_becomes_listagg_for_duckdb() {
        let sql = "SELECT STRING_AGG(x, ',') FROM \"main\".\"t\"";
        let out = to_source_sql(sql, Dialect::DuckDb).unwrap();
        assert!(
            out.contains("LISTAGG"),
            "duckdb form should use LISTAGG: {out}"
        );
        assert!(!out.contains("STRING_AGG"), "no STRING_AGG left: {out}");
    }

    #[test]
    fn string_agg_stays_string_agg_for_postgres() {
        let sql = "SELECT STRING_AGG(x, ',') FROM \"main\".\"t\"";
        let out = to_source_sql(sql, Dialect::Postgres).unwrap();
        assert!(
            out.contains("STRING_AGG"),
            "postgres keeps STRING_AGG: {out}"
        );
        assert!(!out.contains("LISTAGG"), "no LISTAGG for postgres: {out}");
    }

    #[test]
    fn tablesample_gains_percent_for_duckdb() {
        let sql = "SELECT a FROM \"main\".\"t\" TABLESAMPLE BERNOULLI (10)";
        let out = to_source_sql(sql, Dialect::DuckDb).unwrap();
        assert!(
            out.contains("PERCENT"),
            "duckdb tablesample uses PERCENT: {out}"
        );
    }

    #[test]
    fn malformed_input_surfaces_transpile_error() {
        // Clearly invalid SQL must raise, never be silently swallowed.
        let result = to_source_sql("SELECT FROM WHERE )(", Dialect::DuckDb);
        assert!(
            matches!(result, Err(EmitError::Transpile { .. })),
            "got {result:?}"
        );
    }
}
