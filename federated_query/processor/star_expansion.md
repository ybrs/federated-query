## Middleware Flow

We rewrite queries before it hits the engine. We currently have the following
- Star expansion: We replace star (select * ...) with real column names 

These rewrites will act similar to middlewares. So we keep a "query_context" object through the lifetime of the query. 

For example we rewrite column names but we use internal names 

query: select * from pg_connection.users will result 

query_context:
   {"columns": [{"internal_name": "pg_connection.users.id", "visible_name": "id"}]}
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

So the flow will be like this 
```
[middleware1] -> [middleware2] -> [execute query] -> [result (arrow table)] -> [middleware2] -> [middleware1] -> [user] 
```

For context tracking we do not want "last/first" kind of things. You can not even use "last" or "first" in any functions name. You can't use last/first etc. in a variable name. We don't like stacking things.

What we want is 
1- write a QueryExecutor class that has these properties input_query: "users query", queryContext:QueryContext instance of.
2- Then this executor will run middlewares get the query, pass it to logical/physical planners, executors etc. with each step having a reference to itself. So you have to touch signature of many functions.
3- It's expected that the existing tests will need a refactor too.

This is not a small task. Think wisely.


## Query Expansion Rewrite Flow

1. `QueryPreprocessor.preprocess(sql)` parses the statement via sqlglot.
2. For each `SELECT` block with a star, `_collect_table_metadata` gathers column lists from the catalog.
   - Bare tables: use datasource/schema/table names to look up columns; generate an internal alias like `datasource.schema.table` when no alias exists.
   - Subqueries/derived tables: raise `StarExpansionError` unless/until we have metadata for their outputs.
3. `_rewrite_select_expressions` replaces every star with the explicit columns, tagging each column with `internal_name` and `visible_name` metadata.
4. The rewritten SQL string (or AST) is handed to the parser, which now only sees explicit projections.


Examples:

Catalog: datasource: pg, tables: [foo: columns: [id, name]]
Query: select * from pg.foo
Rewritten: select pg.foo.id, pg.foo.name from pg.foo
QueryContext: {rewrite: columns: [
   {internal_name: pg.foo.id, 
    visible_name: id
   },    {internal_name: pg.foo.id, 
    visible_name: id
   },]}

Query: select * from pg.foo bar
Rewritten: select pg.bar.id, pg.bar.name from pg.foo bar
QueryContext: columns: [
   {internal_name: pg.bar.id, visible_name: id},
   {internal_name: pg.bar.name, visible_name: name},
]

Query: select *, id as foo_id from pg.foo bar
Rewritten: select pg.bar.id, pg.bar.name, id as foo_id from pg.foo bar
QueryContext: columns: [
   {internal_name: pg.bar.id, visible_name: id},
   {internal_name: pg.bar.name, visible_name: name},
   {internal_name: pg.bar.id, alias: foo_id, visible_name: foo_id},
]


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

