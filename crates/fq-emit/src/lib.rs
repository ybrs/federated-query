//! fq-emit: `fq_plan::Expr` + clause nodes -> canonical Postgres-form SQL text,
//! plus the single transpile boundary that turns that canonical form into each
//! source's own dialect.
//!
//! Ports `plan/emit/{expressions,clauses,resolver}.py` and the
//! `plan/physical.py::to_source_sql` boundary. The emitter builds Postgres SQL
//! text directly (hand-built strings, not via polyglot's AST); polyglot is used
//! only to transpile that finished Postgres SQL per dialect, exactly as the
//! Python engine used sqlglot's transpiler.
//!
//! DESIGN INVARIANT (load-bearing): every canonical string this crate emits MUST
//! be valid Postgres that polyglot's Postgres parser round-trips, because
//! `to_source_sql` RE-PARSES it. The round-trip tests pin this.
//!
//! Not in this crate (belongs elsewhere):
//! - The DataFusion/`ScanSpec` runtime dynamic-filter renderer lives in
//!   fq-exec.
//! - The per-physical-node query assembly (`PhysicalRemoteQuery._build_query`,
//!   single-source pushdown, injected/lateral island SQL, FROM/JOIN rendering)
//!   belongs to fq-physical, which composes this crate's clause builders.
//! - The EXPLAIN document builder belongs to fq-runtime.

pub mod clauses;
pub mod dialect;
pub mod error;
pub mod expr;
pub mod ident;
pub mod resolver;

pub use clauses::{assemble_select, group_by, order_by, select_list, set_op_keyword, SelectPieces};
pub use dialect::{to_source_sql, Dialect};
pub use error::EmitError;
pub use expr::render_expr;
pub use ident::quote_ident;
pub use resolver::{
    AliasMap, ColumnResolver, MergeResolver, SourceResolver, CANONICAL_SOURCE_RESOLVER,
};

use fq_plan::Expr;

/// Render an expression to canonical Postgres text with the canonical source
/// resolver - the `Expression.to_sql()` shortcut for diagnostics/string callers.
pub fn render_canonical(expr: &Expr) -> Result<String, EmitError> {
    render_expr(expr, &CANONICAL_SOURCE_RESOLVER)
}
