//! fq-plan: the engine's node model - expression, logical, and (later) physical
//! plan enums plus the shared expression-tree walkers. Ports
//! `plan/expressions.py`, `plan/logical.py`, `plan/physical.py`,
//! `plan/arrow_types.py`.
//!
//! Design: each of `Expr` / `LogicalPlan` /
//! `PhysicalPlan` is ONE enum, and every traversal is an exhaustive `match` with
//! no `_` arm, so a new variant breaks every walker at compile time. That is the
//! compiler-enforced replacement for the Python walker-exhaustiveness tests.
//!
//! Not in this crate, by design: `arrow_type_for` lives in fq-exec (the only
//! crate holding the `arrow` dependency); SQL rendering lives in fq-emit; the
//! EXPLAIN document builder lives with the runtime.

pub mod expr;
pub mod logical;
pub mod physical;

pub use expr::{
    aggregate_output_map, and_expressions, column_refs, combine_and, combine_or,
    contains_aggregate, contains_grouping, split_conjuncts, split_disjuncts, split_where_having,
    BinaryOpType, ColumnRef, Expr, LiteralValue, NullsOrder, Quantifier, UnaryOpType,
};
pub use logical::{
    Aggregate, AggregateFunction, Cte, CteRef, Explain, ExplainFormat, Filter, GroupedLimit, Join,
    JoinType, LateralJoin, Limit, LogicalPlan, Projection, Scan, SetOpKind, SetOperation,
    SingleRowGuard, Sort, SubqueryScan, Union, Values,
};
pub use physical::{
    output_column_names, physical_column_name, BuildSide, ColumnAliasMap, DatasourceKind,
    GroupObservation, PhysicalPlan, PhysicalRemoteQuery, SeededSchema,
};
