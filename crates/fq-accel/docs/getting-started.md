[Accelerator documentation](../README.md)

# Getting started

The fixture every example uses is one DuckDB datasource `wh` with two tables:

```text
orders(o_orderkey, o_custkey, o_region, o_total)   -- 12 rows
  US:   keys 1,2,3,11     totals 100,200,150,120   sum 570
  EU:   keys 4,5,6,12     totals  50, 70, 80, 45   sum 245
  ASIA: keys 7,8,9,10     totals  30, 40, 60, 90   sum 220
region_dim(rd_name, rd_manager)                    -- US/Alice, EU/Bob, ASIA/Carol
```

The config is one file; engine state hangs off the derived storage root
(`<stem>.data/`, or an explicit `storage.root`): each database owns
`databases/<database-id>/{metadata.sqlite,materialized/,scratch/}`,
and the view registry is the shared relation namespace in that database's
`metadata.sqlite` (chunks stay under its `materialized/`). The DDL is `CREATE / REFRESH /
DROP MATERIALIZED VIEW` with an optional `change_keys` entry.

## Create, refresh, and substitute

Three moving parts, kept strictly separate:

- CREATE defines a view and pulls its rows once into the store.
- REFRESH is the ONLY thing that moves a view's rows forward. Serving never
  checks the source ([freshness contract](freshness.md)). Freshness is your refresh
  contract, not a background process.
- SUBSTITUTION serves queries you did NOT rewrite. The planner matches a
  registered view against subtrees of the incoming optimized plan and, on a
  match that clears the gates, reads the view's chunks instead of recomputing.

A read of stored chunks is ALWAYS labeled `MaterializedScan [<view>]` in
EXPLAIN, so an accelerated plan is never indistinguishable from a cold one.
Create a view:

```sql
CREATE MATERIALIZED VIEW region_rev AS
  SELECT o_region, sum(o_total) AS revenue FROM wh.main.orders GROUP BY o_region;
```

A plain read of the view by name is a `MaterializedScan` (this is just
`FROM <view>`, no substitution involved):

```sql
EXPLAIN SELECT o_region, revenue FROM public.region_rev;
```

```text
Projection
  MaterializedScan [region_rev] :: SELECT "o_region", "revenue" FROM "public"."region_rev" AS "region_rev"
```

The point of the accelerator is that you get the SAME `MaterializedScan` WITHOUT
naming the view - by issuing the query the view was defined from
([automatic substitution](automatic-substitution.md)).

[Accelerator documentation](../README.md)
