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
//! Build stages within this crate: (A) `expr` + `logical` [done], (B) `physical`,
//! (C) `arrow_types` + the EXPLAIN document. Stages B and C are not yet present.

pub mod expr;
pub mod logical;

pub use expr::{
    and_expressions, column_refs, combine_and, combine_or, contains_aggregate, split_conjuncts,
    split_disjuncts, BinaryOpType, ColumnRef, Expr, LiteralValue, NullsOrder, Quantifier,
    UnaryOpType,
};
pub use logical::{
    Aggregate, AggregateFunction, Cte, CteRef, Explain, ExplainFormat, Filter, GroupedLimit, Join,
    JoinType, LateralJoin, Limit, LogicalPlan, Projection, Scan, SetOpKind, SetOperation,
    SingleRowGuard, Sort, SubqueryScan, Union, Values,
};
