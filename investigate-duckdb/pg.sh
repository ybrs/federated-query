#!/bin/bash
# Thin wrapper around the bundled psql for the duckpoc investigation DB.
exec /workspace/federated-query/postgres-17/bin/psql -h localhost -p 5432 -U postgres "$@"
