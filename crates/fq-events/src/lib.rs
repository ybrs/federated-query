//! fq-events: event analytics over event-shaped tables.
//!
//! An EVENT VIEW is a materialized view (fq-accel owns its chunks, schema,
//! and lifecycle) whose columns are mapped onto the event roles - entity id,
//! timestamp, event name; every remaining column is a property - and whose
//! chunks satisfy the materialization contract: rows globally sorted by
//! (entity ASC, timestamp ASC), no NULL in a role column. The sort makes the
//! stream entity-partitioned and time-ordered, so every sequence analysis is
//! a single linear scan holding one entity's events at a time.
//!
//! The crate owns:
//!
//! - [`EventViewRegistry`]: the `event_views` role table beside the
//!   materialized-view registry in the config's stats SQLite.
//! - [`build_event_view`]: contract establishment at build - validate roles,
//!   reject NULLs, sort - producing the batches the store persists.
//! - [`EventStream`]: the kernels' normalized, contract-VERIFYING cursor over
//!   stored batches (a scan raises on a stream whose ordering regressed).
//! - [`run_funnel`] / [`run_segment`]: the analysis kernels. Path analysis is
//!   not implemented; the statement surface raises before reaching here.
//!
//! fq-runtime composes this crate with fq-accel: it executes the defining
//! SELECT, enforces the contract here, persists via the materialized-view
//! machinery, and dispatches FUNNEL / SEGMENT statements to the kernels over
//! the view's chunk files. Freshness is the materialized-view contract:
//! serving trusts the last pull; only REFRESH EVENT VIEW moves data forward.

pub mod chunks;
pub mod contract;
pub mod error;
pub mod funnel;
pub mod registry;
pub mod segment;
pub mod stream;

pub use chunks::read_chunks;
pub use contract::build_event_view;
pub use error::EventError;
pub use funnel::{funnel_schema, run_funnel, MAX_STEPS};
pub use registry::EventViewRegistry;
pub use segment::{run_segment, segment_schema};
pub use stream::EventStream;
