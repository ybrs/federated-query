//! One integration-test binary for the crate: each module is a test suite.
//! A single binary means a single link of the DataFusion/Arrow dependency
//! stack per `cargo test`, instead of one link per suite.

mod dim_shipping;
mod orient;
mod plan;
mod single_source;
mod steps;
