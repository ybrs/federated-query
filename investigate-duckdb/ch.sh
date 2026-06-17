#!/bin/bash
# Thin wrapper around the local ClickHouse client (native protocol on :9000).
exec /workspace/federated-query/investigate-duckdb/clickhouse/clickhouse client \
  --host 127.0.0.1 "$@"
