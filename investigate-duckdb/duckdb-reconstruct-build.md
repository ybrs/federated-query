# DuckDB Reconstructor Build

This folder contains two pieces:

- `duckdb_reconstruct.py`: Python CLI/REPL for rendering reconstructed SQL.
- `duckdb_plan_dump.cpp`: C++ helper that calls DuckDB `Connection::ExtractPlan()` and emits enriched optimized logical-plan JSON.

The helper is needed for cases where stock `EXPLAIN (FORMAT JSON)` drops expression details. Example:

```sql
SELECT foo FROM (SELECT 1 AS foo FROM bar);
```

Stock EXPLAIN only exposes the projected name `foo`; `duckdb_plan_dump` exposes the real optimized projection expression `1`.

## Prerequisites

The local DuckDB source tree must be present at:

```text
duckdb-src/
```

The helper links against:

```text
duckdb-src/build/release/src/libduckdb.so
```

If that library is missing, the Makefile target builds it from `duckdb-src`.

## Build

Build the helper:

```bash
make plan-dump
```

Build DuckDB's shared library first, without compiling the helper:

```bash
make plan-dump-duckdb
```

Remove only the helper binary:

```bash
make plan-dump-clean
```

## Smoke Test

Run the helper directly against a root derived-subquery case:

```bash
make plan-dump-smoke
```

Expected JSON includes the inner projection:

```json
"Expression Names": "foo",
"Expressions": "1"
```

## Python Tests

Run the reconstructor tests:

```bash
make reconstruct-test
```

Equivalent direct command:

```bash
python -m pytest tests/test_duckdb_reconstruct.py -q
```

## Use The REPL

```bash
python duckdb_reconstruct.py repl
```

Example:

```sql
duckdb-reconstruct> select foo from (select 1 as foo from bar);
```

Expected reconstructed SQL:

```sql
SELECT 1 AS foo
FROM bar;
```

## Direct Compile Command

The Makefile compiles with:

```bash
g++ -std=c++17 duckdb_plan_dump.cpp \
  -I duckdb-src/src/include \
  -I duckdb-src/third_party/fmt/include \
  -I duckdb-src/third_party/utf8proc/include \
  -I duckdb-src/third_party/yyjson/include \
  -I duckdb-src/third_party/fastpforlib \
  -I duckdb-src/third_party/fast_float \
  -I duckdb-src/third_party/re2 \
  -I duckdb-src/third_party/miniz \
  -I duckdb-src/third_party/concurrentqueue \
  -I duckdb-src/third_party/pcg \
  -L duckdb-src/build/release/src \
  -lduckdb \
  -Wl,-rpath,$PWD/duckdb-src/build/release/src \
  -o duckdb_plan_dump
```

Prefer `make plan-dump`; this raw command is here for debugging.
