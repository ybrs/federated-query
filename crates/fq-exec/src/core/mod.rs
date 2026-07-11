//! The pyo3-free engine core imported from `fedqrs`'s `core` crate: the
//! execution IR, the expression-to-DataFusion translation, the source-SQL
//! emitter, and the scan-partitioning / selectivity helpers.
//!
//! These modules were their own `fedqrs-core` crate; here they are a submodule
//! of `fq-exec` so the whole engine lives in one workspace crate. The JSON IR
//! (`ir.rs` serde) is retained for this milestone so the ported unit tests run;
//! the Step-bridge that replaces it with `fq_plan` types lands in the next
//! milestone (see HANDOFF "MILESTONE C").

pub mod expr;
pub mod ir;
pub mod partition;
pub mod sql;
pub mod types;
