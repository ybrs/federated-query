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
//! MILESTONE C step 1: the engine still consumes the JSON `Ir` (`core::ir`); the
//! next milestone swaps that for `fq_physical::build_steps` output (the `Step`
//! list) and the `fq_plan::Expr -> DataFusion Expr` lowering.

pub mod connectors;
pub mod core;
pub mod engine;
pub mod error;

pub use engine::execute;
pub use error::{ExecError, ExecResult};
