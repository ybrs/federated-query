# fq-accel: the query accelerator (materialized-view substitution)

This crate is the accelerator: it stores a materialized view's rows as Arrow IPC
chunks and, at plan time, reads those chunks in place of recomputing a query
subtree that matches the view's definition. You never rewrite a query to use a
view. You create the view, and eligible later queries whose optimized subtrees
match its definition can read the stored chunks automatically. Matching also
checks output names, ordering constraints, and an estimated cost gate.

## Read the guide

1. [Getting started](docs/getting-started.md): fixture, storage, CREATE, and the
   difference between reading a view explicitly and automatic substitution.
2. [Automatic substitution](docs/automatic-substitution.md): unchanged SQL,
   structural matching, cosmetic differences, and replacing part of a larger query.
3. [Matching boundaries and controls](docs/matching-limits.md): predicates,
   columns, ordering, cost gate, and the session switch.
4. [Freshness and refresh](docs/freshness.md): stale reads, live bypass, and
   explicit catch-up.
5. [Observability](docs/observability.md): EXPLAIN, reuse counters, and the
   difference between estimated work and measured latency.
6. [Current limitations](docs/limitations.md): matching restrictions and storage
   management.

The examples retain captured EXPLAIN output and result values from real
`fedq.Runtime` executions over a seeded DuckDB source. SQL and its captured
output stay together in each part.

For a server-based example over PostgreSQL, MySQL, and ClickHouse, see the
[game-studio accelerator showcase](../../tutorials/game-studio/06-acceleration.md).
It includes actual cold/warm measurements, upstream inserts, and refresh.

## Implementation

- [Stored chunks and lifecycle](src/accelerator.rs)
- [Automatic plan substitution](../fq-runtime/src/substitute.rs)
- [Refresh classification](../fq-runtime/src/delta.rs)
- [Substitution integration tests](../fq-runtime/tests/it/substitution.rs)
