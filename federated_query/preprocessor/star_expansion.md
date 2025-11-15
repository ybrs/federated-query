## Middleware Flow

We rewrite queries before it hits the engine. We currently have the following
- Star expansion: We replace star (select * ...) with real column names 

These rewrites will act similar to middlewares. So we keep a "query_context" object through the lifetime of the query. 

For example we rewrite column names but we use internal names 

query: select * from pg_connection.users will result 

query_context:
   {"columns": [{"internal_name": "pg_connection.users.id", "visible_name": "id"}}
query:
   "select pg_connection.users.id from pg_connection.users"

When returning to the user we will use query_context to replace column names with visible names

end result (visible to the user)

```
id
--
1
2
```

We will have more than one processor. So any processor can attach it's own metadata to query_context

## Query Expansion Rewrite Flow

```
raw SQL ──► QueryPreprocessor (expands stars etc.) ──► Parser ──► Binder ──► ...
```

1. `QueryPreprocessor.preprocess(sql)` parses the statement via sqlglot.
2. For each `SELECT` block with a star, `_collect_table_metadata` gathers column lists from the catalog.
   - Bare tables: use datasource/schema/table names to look up columns; generate an internal alias like `datasource.schema.table` when no alias exists.
   - Subqueries/derived tables: raise `StarExpansionError` unless/until we have metadata for their outputs.
3. `_rewrite_select_expressions` replaces every star with the explicit columns, tagging each column with `internal_name` and `visible_name` metadata.
4. The rewritten SQL string (or AST) is handed to the parser, which now only sees explicit projections.

## Failure Cases (raise `StarExpansionError`)

- Catalog metadata missing for `datasource.schema.table` referenced by the star.
- Unsupported source type (derived table/subquery, VALUES clause, table-valued function) because we cannot enumerate columns.

These failures must surface before binding so callers immediately know expansion could not be performed.

# SELECT * Expansion Contract

## Overview

The query preprocessor is responsible for rewriting every `SELECT *` (and `alias.*`) into explicit column references **before** the SQL reaches the parser/binder. The rewrite is mandatory for all queries; no star projection is allowed past preprocessing.

## Contract

1. **Always expand stars**
   - Every FROM/JOIN source must produce a concrete column list.
   - Bare tables without SQL aliases still expand by synthesizing an internal identifier (e.g., `testdb.main.orders`).
2. **Dual column names**
   - Each expanded column keeps two names:
     - `internal_name`: fully-qualified identifier used during planning/execution (`datasource.schema.table.column`).
     - `visible_name`: user-facing name (original column name or explicit `AS alias`).
   - The engine propagates `internal_name` through planning, pushdown, joins, etc., and only swaps to `visible_name` when materializing the final result table.
3. **Fail fast**
   - If the catalog lacks metadata for any starred source, or if the source is unsupported (derived table without column info, table function, VALUES clause, etc.), the preprocessor raises `StarExpansionError` immediately.

