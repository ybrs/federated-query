# Changelog

All notable changes to this project are recorded here, newest first.
History before this file exists only in `git log`.

## [Unreleased]

### Added

- Composite `TIEBREAK (event_id, uuid)` ordering for event datasets: the
  builder carries the ordering columns through its spill, compares them
  lexicographically in the shard sort it already performs, and stamps each
  row's 1-based rank within its (actor, timestamp) group as the persisted
  tiebreak. Single-column and synthetic-ordinal tiebreaks are unchanged;
  existing datasets read back identically. Duplicate or null ordering keys
  raise.
- `perf_compare` event-build case: times `CREATE EVENT DATASET` from
  statement issue to return, cold in a fresh process and warm as
  DROP+CREATE medians on one live runtime
  (`compare.py --event-config/--event-sql/--event-name`).
- Native Parquet manifest ingestion over explicit file lists, with
  JSON string extraction (`json_extract_string`) running natively on the
  source and streaming SELECT-defined dataset builds.

### Changed

- The native Parquet ingestion example declares
  `TIEBREAK (event_id, uuid)` instead of computing `ROW_NUMBER()` in the
  source query, removing a full repartition and sort of the source ahead
  of the builder's own sort. Measured on identical 132.7M-event input
  (2 CPUs): ~141.8s statement wall clock, down from ~988.8s internal
  build counter with the window; a DuckDB-view build of the same data
  with the same tiebreak takes ~174.8s and produces a byte-identical
  dataset and paths index.
- Client names no longer appear anywhere in tracked repository content;
  the example and config use neutral `native-parquet` names. Client
  demo material lives only under the git-ignored `vendor/` tree.

### Fixed

- State-file header reads go through SQLite's VFS so closing a
  descriptor cannot release the process's SQLite POSIX locks.
