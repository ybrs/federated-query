//! fq-plan: the engine's node model - expression, logical, and (later) physical
//! plan enums plus the shared expression-tree walkers. Ports
//! `plan/expressions.py`, `plan/logical.py`, `plan/physical.py`,
//! `plan/arrow_types.py`.
//!
//! Design (from the rewrite plan): each of `Expr` / `LogicalPlan` /
//! `PhysicalPlan` is ONE enum, and every traversal is an exhaustive `match` with
//! no `_` arm, so a new variant breaks every walker at compile time. That is the
//! compiler-enforced replacement for the Python walker-exhaustiveness tests.
//!
//! Build stages within this crate:
//! - (A) `expr` + `logical` [done].
//! - (B) `physical` [done - node structs + `children` + the typed `schema` + the
//!   `column_aliases` resolution map and `physical_column_name`, the last two
//!   added as a step-building (fq-physical) prerequisite].
//! - (C) [partial] `DataType::is_renderable` (in fq-common) + the shared
//!   `split_where_having` / `aggregate_output_map` utilities [done here].
//!   Deferred out of fq-plan to their real consumers, never written blind:
//!   `arrow_type_for` -> fq-exec (needs the `arrow` crate); the EXPLAIN document
//!   builder -> fq-runtime/fq-emit (it needs `estimated_cost`, expression
//!   rendering, a physical-plan producer, and the e2e shape tests, none of which
//!   exist yet).

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
