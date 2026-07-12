//! One integration-test binary for the crate: each module is a test suite;
//! `common` is the shared fixture/helper module. A single binary means a
//! single link per `cargo test`, instead of one link per suite.

mod common;
mod dependent;
mod disjunctive;
mod flattenable;
mod scalar;
mod scope;
mod synthetic_types;
