//! `fq-exec` - the federated-query execution engine.
//!
//! This is the imported `fedqrs` engine (`src/engine.rs`, `src/connectors.rs`,
//! and the `core` crate), de-pyo3-ified: every `PyResult` / `PyErr` is now an
//! `ExecError`, and the pyo3 FFI entry points (`lib.rs`, `ffi.rs`) are dropped
//! (they move to the later `fedq-py` crate).
//!
//! The execution LOGIC is unchanged from the TPC-validated engine. It keeps lazy
//! fragment fusion into DataFusion regions, the `FairSpillPool` with tracked
//! collection, binding spill, the sort-merge-join retry, the semi-join
//! reductions (inline-IN, temp-table, parquet delivery), ship execution, the
//! prefetch pools, ctid-parallel reads, and the per-step observation stream.
//!
//! The step bridge: `bridge::execute_plan` runs the whole
//! Rust pipeline end to end. It consumes `fq_physical::build_steps` output (the
//! `Step` list holding `fq_plan::Expr`), lowers each expression to the engine's
//! `IrExpr` (`bridge::serialize_expr`, the ported `rust_ir.py` `_serialize_*`
//! layer), converts the steps/fragments into `core::ir`, and hands them to
//! `execute`. The JSON `Ir` serde entry stays only so the imported unit tests
//! keep running; the JSON boundary itself moves to the later `fedq-py` crate.

pub mod bridge;
pub mod connectors;
pub mod core;
pub mod engine;
pub mod error;

pub use bridge::{execute_plan, serialize_expr, to_ir, PlanExecution};
pub use engine::execute;
pub use error::{ExecError, ExecResult};
