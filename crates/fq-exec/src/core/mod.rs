//! The pyo3-free engine core imported from `fedqrs`'s `core` crate: the
//! execution IR, the expression-to-DataFusion translation, the source-SQL
//! emitter, and the scan-partitioning / selectivity helpers.
//!
//! These modules were their own `fedqrs-core` crate; here they are a submodule
//! of `fq-exec` so the whole engine lives in one workspace crate. The JSON IR
//! (`ir.rs` serde) is retained so the imported unit tests keep running; the
//! engine's real entry is the Step bridge (`bridge`), which builds `core::ir`
//! values from `fq_plan` types in-process.

pub mod expr;
pub mod ir;
pub mod partition;
pub mod sql;
pub mod types;
