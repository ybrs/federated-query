# Nested Parquet types and UNNEST

## Purpose

Query the Firebase Parquet export directly through fedq. Combine daily files
with a persisted UNION ALL view, build one event dataset from its scalar role
columns, and extract nested event properties using DataFusion's existing
operators.

## Contracts

- Preserve list element types and struct field names, types, and nullability
  throughout the catalog, binding, plans, Arrow execution, and result schema.
- Read only Parquet footers during registration and planning.
- Nested columns remain queryable; projection pruning may omit unused columns
  but registration never hides them or coerces them to strings.
- Bind struct field access against the actual schema. Missing fields, scalar
  inputs to UNNEST, invalid aliases, and incompatible UNION inputs raise.
- UNNEST expands lists using DuckDB semantics. Empty and null lists produce no
  rows for ordinary expansion; outer expansion preserves the parent row.
- Every manufactured column has a relation qualifier after binding. No star
  placeholders survive in bound scans.
- Multiple list expansions preserve the SQL form's semantics. SELECT-list
  expansion zips lists with null padding; independent FROM expansions multiply
  rows. Ordinality records the position within the original list.
- Reuse DataFusion's list expansion and struct access operators. Keep
  coordinator execution when a source cannot preserve these semantics.
- Event datasets retain one row per original event. Parameter expansion is
  composed with SQL extraction/aggregation before ingestion when properties
  are promoted from event_params.

## Implementation sequence

1. Extend the shared type model and recursive Arrow mappings. Update callers
   to preserve complete types through copies, schemas, and persisted metadata.
2. Register nested Parquet schemas and prove scalar projections over the
   Firebase export work without reading nested columns unnecessarily.
3. Add typed struct field access, including alias qualification and rejection
   of missing fields.
4. Add logical and physical UNNEST representation, parser/binder integration,
   optimizer traversal, and DataFusion lowering for SELECT and FROM forms.
5. Cover list-of-struct values, nulls, empty lists, repeated elements, unequal
   lengths, outer expansion, ordinality, views, unions, and invalid queries.
   Compare execution and result types with DuckDB.
6. Use the actual export to create the combined view, inspect event names,
   build an event dataset, and run paths, funnels, and retention.
7. Run ./run_all_tests.sh in the active virtualenv and read its complete log.
   Update user-facing usage documentation with working examples.

## Evidence and work tracking

The initial CLI rejected event_params during registration because its shared
type model contained only scalar types. Recursive Arrow metadata, typed struct
access, and SELECT/FROM expansion are now implemented. The expansion boundary
lowers to DataFusion's native physical UNNEST operator.

Direct execution registered all 111 daily files and created firebase_events.
The view count and an independent DuckDB scan both returned 1,227,190 events
and 22,744 actors. A second event dataset was built with the same counts. Paths, funnels,
and retention returned results across separate CLI sessions. Nested CROSS JOIN
and matching LEFT JOIN queries also returned results from the actual export.

The complete suite exposed parser, physical column naming, and ordered array
aggregation defects. These are fixed and checked against DuckDB values and
result types. The final ./run_all_tests.sh run passed all 8 stages: 1,282 Rust
tests and 87,238 Python tests passed, with 0 failures and 2 expected Python
failures. The 6 UNNEST integration test functions cover the expansion contracts,
invalid queries, and persisted nested views. Complete evidence is retained in
run_all_tests.log and run-e2e-logs/.

The requested document describes implementation and acceptance contracts.
Task tracking belongs in taskq; its MCP tools are unavailable in this session.
