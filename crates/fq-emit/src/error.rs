//! Emit errors.
//!
//! NEVER SILENT: an unresolved qualified column, or a subquery-bearing
//! expression that reached SQL emission, RAISES a typed error; the transpile
//! boundary raises when polyglot fails or returns other than one statement.
//!
//! RETIRED-BY-TYPE-SYSTEM (no runtime variant, the compiler is the guard).
//! Adding any such variant would be dead, unreachable code, so it is not added:
//! - the Python `_literal_to_ast` "no SQL literal mapping for data type" raise -
//!   `LiteralValue`/`DataType` are closed enums matched exhaustively.
//! - the Python `visit_binary_op`/`visit_unary_op` "no SQL mapping for operator"
//!   raises - `BinaryOpType`/`UnaryOpType` are closed enums matched exhaustively.

use thiserror::Error;

/// A failure rendering a plan expression/clause to SQL or transpiling it.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum EmitError {
    /// A qualified column reference the merge relation does not expose. Ported
    /// from `ColumnResolutionError`; raised rather than resolving by bare name,
    /// which could bind to a same-named column of another relation.
    #[error("unresolved column reference {table}.{column}; relation exposes {exposes:?}")]
    ColumnResolution {
        table: String,
        column: String,
        exposes: Vec<String>,
    },

    /// A subquery-bearing expression reached SQL emission (it must be
    /// decorrelated first). Carries the node label; message mirrors the Python
    /// "<Node> reached SQL emission; decorrelate it first".
    #[error("{0} reached SQL emission; decorrelate it first")]
    SubqueryReachedEmit(&'static str),

    /// polyglot transpile failed or did not return exactly one statement.
    #[error("transpile to {dialect} failed: {reason}")]
    Transpile {
        dialect: &'static str,
        reason: String,
    },

    /// A literal with no valid canonical-SQL token (a non-finite float: NaN /
    /// Infinity). Emitting `NaN`/`inf` would produce text no SQL parser accepts,
    /// so we RAISE rather than ship an unparseable canonical string. Carries the
    /// offending value's text (an f64 is not `Eq`, so it is stringified here).
    #[error("no canonical SQL literal for non-finite float {0}")]
    UnrepresentableLiteral(String),
}
