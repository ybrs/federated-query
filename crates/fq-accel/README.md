# fq-accel: the query accelerator (materialized-view substitution)

This crate is the accelerator: it stores a materialized view's rows as Arrow IPC
chunks and, at plan time, reads those chunks in place of recomputing a query
subtree that matches the view's definition. You never rewrite a query to use a
view. You create the view, and any later query whose shape matches the view's
definition is served from the stored chunks automatically.

This document is the user flow end to end. Every EXPLAIN dump and every
`SHOW MATERIALIZED VIEWS` row below was produced by running the statement
through `fedq.Runtime` over a seeded DuckDB file, and pasted verbatim. Nothing
here is hypothetical.

The fixture every example uses is one DuckDB datasource `wh` with two tables:

```
orders(o_orderkey, o_custkey, o_region, o_total)   -- 12 rows
  US:   keys 1,2,3,11     totals 100,200,150,120   sum 570
  EU:   keys 4,5,6,12     totals  50, 70, 80, 45   sum 245
  ASIA: keys 7,8,9,10     totals  30, 40, 60, 90   sum 220
region_dim(rd_name, rd_manager)                    -- US/Alice, EU/Bob, ASIA/Carol
```

The config is one file; the view store hangs off its stem (`<stem>.mv/` for
chunks, `<stem>.stats.sqlite` for the registry). The DDL is `CREATE / REFRESH /
DROP MATERIALIZED VIEW` with an optional `change_keys` entry.

--------------------------------------------------------------------------------

## 1. The mental model

Three moving parts, kept strictly separate:

- CREATE defines a view and pulls its rows once into the store.
- REFRESH is the ONLY thing that moves a view's rows forward. Serving never
  checks the source (section 3, stale-by-choice). Freshness is your refresh
  contract, not a background process.
- SUBSTITUTION serves queries you did NOT rewrite. The planner matches a
  registered view against subtrees of the incoming optimized plan and, on a
  match that clears the gates, reads the view's chunks instead of recomputing.

A read of stored chunks is ALWAYS labeled `MaterializedScan [<view>]` in
EXPLAIN, so an accelerated plan is never indistinguishable from a cold one.
Create a view:

```
CREATE MATERIALIZED VIEW region_rev AS
  SELECT o_region, sum(o_total) AS revenue FROM wh.main.orders GROUP BY o_region
```

A plain read of the view by name is a `MaterializedScan` (this is just
`FROM <view>`, no substitution involved):

```
EXPLAIN SELECT o_region, revenue FROM public.region_rev

Projection
  MaterializedScan [region_rev] :: SELECT "o_region", "revenue" FROM "public"."region_rev" AS "region_rev"
```

The point of the accelerator is that you get the SAME `MaterializedScan` WITHOUT
naming the view - by issuing the query the view was defined from (section 2).

--------------------------------------------------------------------------------

## 2. When substitution fires

Substitution matches on the OPTIMIZED logical plan. The view's defining SELECT
is run through the same parse -> bind -> decorrelate -> optimize pipeline the
incoming query uses, giving a canonical plan; a subtree of the incoming plan
that is structurally EQUAL to that canonical plan is replaced with a scan of the
view. Largest matching subtree wins.

### 2a. The exact definition, re-issued

```
EXPLAIN SELECT o_region, sum(o_total) AS revenue FROM wh.main.orders GROUP BY o_region

MaterializedScan [region_rev] :: SELECT "o_region", "revenue" FROM "public"."region_rev"
```

The five-region-times-four-row scan and its aggregate collapse to a read of the
three stored rows.

### 2b. The same query, cosmetically different

Matching is on the optimized plan, so differences the pipeline erases before the
plan is formed do not matter, and differences it carries INTO the plan do. Run
against `region_rev`, here is exactly what survives and what does not:

```
exact                            -> SUBSTITUTES
lowercase keywords + odd whitespace/newlines  -> SUBSTITUTES
UPPERCASE keywords               -> SUBSTITUTES
explicit table alias = table name (FROM orders AS orders)  -> SUBSTITUTES
different table alias (FROM orders t, t.o_region ...)      -> cold
GROUP BY ordinal (GROUP BY 1 instead of GROUP BY o_region) -> cold
different OUTPUT alias (sum(o_total) AS total_rev)         -> cold
```

Be precise about the boundary. The optimize stage normalizes lexical noise -
whitespace, newlines, keyword case, and quoting all disappear before the plan
exists, so those variants match. It does NOT canonicalize:

- TABLE ALIASES. A user alias becomes the qualifier stamped on every column
  reference in the plan. `FROM orders t` carries `t.o_region`; the view's
  canonical carries `orders.o_region` (the default alias is the table name).
  Different qualifier, different plan, no match. An explicit alias that happens
  to equal the default (`FROM orders AS orders`) matches because it produces the
  identical qualifier.
- ORDINAL GROUP BY. `GROUP BY 1` stays an ordinal in the plan; `GROUP BY
  o_region` is a column. Different nodes, no match.
- OUTPUT COLUMN NAMES. The stored view reports its own column names, and the
  planner also enforces a name guard: the matched subtree's output names must
  equal the view's stored names in order. Rename the output (`AS total_rev`) and
  the guard declines.

This is a conservative design: a difference the optimizer does not fold declines
rather than risking a wrong match. It never returns a wrong answer; at worst it
misses a substitution you might have expected.

### 2c. A view matching a subtree of a bigger query

The view does not have to be the whole query. Here the `region_rev` aggregate
appears as a derived table feeding a join to `region_dim`, and only that subtree
is substituted:

```
EXPLAIN SELECT g.o_region, g.revenue, d.rd_manager
        FROM (SELECT o_region, sum(o_total) AS revenue
              FROM wh.main.orders GROUP BY o_region) g
        JOIN wh.main.region_dim d ON g.o_region = d.rd_name

Projection
  HashJoin [INNER]
    AliasedRelation
      MaterializedScan [region_rev] :: SELECT "o_region", "revenue" FROM "public"."region_rev"
    Scan [wh] +inject(rd_name) :: SELECT "rd_name", "rd_manager" FROM "main"."region_dim" AS "d"
```

The aggregate side reads the view; the dimension side still scans the source.
The rest of the plan (the join, the projection) is built as usual over the
reduced tree.

--------------------------------------------------------------------------------

## 3. When substitution does NOT fire

Each case below is a real EXPLAIN of the COLD plan - the query recomputes
against the source. The examples in 3a-3c use a filtered projection view:

```
CREATE MATERIALIZED VIEW us_orders AS
  SELECT o_orderkey, o_region, o_total FROM wh.main.orders WHERE o_region = 'US'
```

whose exact re-issue substitutes (positive control):

```
MaterializedScan [us_orders] :: SELECT "o_orderkey", "o_region", "o_total" FROM "public"."us_orders"
```

### 3a. A different constant

The view holds `o_region = 'US'`. A query for `'EU'` is a different plan - the
constant is part of the shape - and does not match:

```
EXPLAIN SELECT o_orderkey, o_region, o_total FROM wh.main.orders WHERE o_region = 'EU'

Projection
  Scan [wh] :: SELECT "o_orderkey", "o_region", "o_total" FROM "main"."orders" AS "orders" WHERE ("orders"."o_region" = 'EU')
```

The store holds the rows for the specific constant the view was created with. A
US view never serves an EU query. Create a separate view per constant you want
accelerated.

### 3b. Fewer (or more) columns

Dropping `o_total` from the projection is a different column set, so no match:

```
EXPLAIN SELECT o_orderkey, o_region FROM wh.main.orders WHERE o_region = 'US'

Projection
  Scan [wh] :: SELECT "o_orderkey", "o_region" FROM "main"."orders" AS "orders" WHERE ("orders"."o_region" = 'US')
```

Matching is exact on the column set; a subset is not a match. If you want the
two-column read accelerated, create the two-column view.

### 3c. An extra filter (no subsumption)

Adding `AND o_total > 50` to a query the view does NOT carry declines, even
though every qualifying row is a subset of the view's rows:

```
EXPLAIN SELECT o_orderkey, o_region, o_total FROM wh.main.orders
        WHERE o_region = 'US' AND o_total > 50

Projection
  Scan [wh] :: SELECT "o_orderkey", "o_region", "o_total" FROM "main"."orders" AS "orders" WHERE (("orders"."o_region" = 'US') AND ("orders"."o_total" > 50))
```

This is a deliberate design decision, not a missing optimization: the
accelerator does NOT do predicate subsumption. A view for `o_region = 'US'`
never serves a strictly narrower or wider predicate. What to do instead: create
the narrower view (`WHERE o_region = 'US' AND o_total > 50`) if that exact query
recurs in your workload.

### 3d. A shape that fixes order or row selection anywhere within it

A subtree that pins output order or which rows survive - a Sort, a LIMIT, or a
scan carrying a folded ORDER BY / LIMIT, at the subtree root OR nested under a
Projection/Limit - is never substituted. Stored chunks are read as an unordered
SET, so serving such a shape could drop its ORDER BY or, across multiple chunks,
reorder its rows. The guard walks the WHOLE matched subtree, so all three of the
shapes below decline even though each collapses rows and would clear the cost
gate. Each EXPLAIN is the COLD plan the query recomputes.

A pure sort (output equals input, 12 rows):

```
CREATE MATERIALIZED VIEW sortedall AS
  SELECT o_orderkey, o_total FROM wh.main.orders ORDER BY o_total DESC

EXPLAIN SELECT o_orderkey, o_total FROM wh.main.orders ORDER BY o_total DESC

Projection
  Scan [wh] :: SELECT "o_orderkey", "o_total" FROM "main"."orders" AS "orders" ORDER BY "orders"."o_total" DESC NULLS FIRST
```

A pure LIMIT (row selection, 12 -> 5):

```
CREATE MATERIALIZED VIEW lim_all AS
  SELECT o_orderkey, o_total FROM wh.main.orders LIMIT 5

EXPLAIN SELECT o_orderkey, o_total FROM wh.main.orders LIMIT 5

Projection
  Limit
    Scan [wh] :: SELECT "o_orderkey", "o_total" FROM "main"."orders" AS "orders" LIMIT 5
```

An ORDER BY that also carries a LIMIT (12 -> 3). After optimization this is
rooted in a `Projection` over `Limit` over a `Scan` with the folded ORDER BY, so
the ordering lives INSIDE the subtree, not at its root - the guard walks down and
declines:

```
CREATE MATERIALIZED VIEW top3 AS
  SELECT o_orderkey, o_total FROM wh.main.orders ORDER BY o_total DESC LIMIT 3

EXPLAIN SELECT o_orderkey, o_total FROM wh.main.orders ORDER BY o_total DESC LIMIT 3

Projection
  Limit
    Scan [wh] :: SELECT "o_orderkey", "o_total" FROM "main"."orders" AS "orders" ORDER BY "orders"."o_total" DESC NULLS FIRST LIMIT 3
```

What to do instead: put the ORDER BY in the OUTER query, above any view you
expect to be substituted. An outer sort re-sorts the substituted scan in the
ordinary way and is always safe; only ordering INSIDE the matched subtree is at
risk, and that is exactly what this guard declines.

### 3e. The cost gate declines a trivial view

A view whose output is its whole base table saves nothing: recompute reads the
same rows the cache would.

```
CREATE MATERIALIZED VIEW all_orders AS
  SELECT o_orderkey, o_region, o_total FROM wh.main.orders

EXPLAIN SELECT o_orderkey, o_region, o_total FROM wh.main.orders

Projection
  Scan [wh] :: SELECT "o_orderkey", "o_region", "o_total" FROM "main"."orders" AS "orders"
```

How to read the gate: recompute cost is the total unfiltered base-table rows the
subtree would scan (here 12); read cost is the view's stored row count (here also
12). Substitution fires only when recompute is STRICTLY greater than the read. A
`SELECT * FROM t` view has recompute == read, so the gate declines - a fresh
source scan is no more expensive than opening the cache. A view earns
substitution by COLLAPSING work: a filter, an aggregate, or a join that turns
many input rows into few stored rows (like `region_rev`: 12 in, 3 out).

### 3f. The kill switch

`accelerator.enable_substitution` is a session setting, read fresh on every
plan, so toggling it takes effect on the next query:

```
SET accelerator.enable_substitution = false

EXPLAIN SELECT o_region, sum(o_total) AS revenue FROM wh.main.orders GROUP BY o_region

Scan [wh] :: SELECT "orders"."o_region" AS "o_region", SUM("orders"."o_total") AS "revenue" FROM "main"."orders" AS "orders" GROUP BY "orders"."o_region"
```

With it off, `region_rev` is registered but never consulted; the query
recomputes. `RESET accelerator.enable_substitution` restores the default (on).

### 3g. Stale by choice - substitution serves the OLD rows

Serving trusts the last pull. If the source changes and you do not REFRESH, the
substituted read keeps returning the rows the view was built from. This is the
freshness contract, demonstrated end to end (the source is mutated between two
runtime processes so the change is genuinely committed to the file):

```
CREATE MATERIALIZED VIEW us_total AS
  SELECT sum(o_total) AS s FROM wh.main.orders WHERE o_region = 'US'

-- first query populates and serves from the view:
SELECT sum(o_total) AS s FROM wh.main.orders WHERE o_region = 'US'   -> s = 570

-- the source is then mutated: INSERT ... ('US', 1000.0)

-- substitution ON: still the last pull
SELECT ... WHERE o_region = 'US'                                     -> s = 570
  MaterializedScan [us_total] :: SELECT "s" FROM "public"."us_total"

-- kill switch OFF: recompute sees the live source
SET accelerator.enable_substitution = false
SELECT ... WHERE o_region = 'US'                                     -> s = 1570

-- REFRESH moves the view forward, then substitution serves the new rows
REFRESH MATERIALIZED VIEW us_total
  -> "REFRESH MATERIALIZED VIEW (whole re-pull: table 'wh.main.orders' has no declared change key)"
SELECT ... WHERE o_region = 'US'                                     -> s = 1570
```

Substitution did not return a wrong answer - it returned exactly the rows the
view holds. Bringing those rows up to date is YOUR REFRESH, never a query-path
check. (A table with a declared `change_keys` entry refreshes incrementally
instead of whole.)

--------------------------------------------------------------------------------

## 4. Benefits: use_count and cost_saved

Every SERVED substitution advances the view's counters, visible in
`SHOW MATERIALIZED VIEWS` (columns: `name, rows, bytes, created, refreshed,
use_count, cost_saved`). Starting from a fresh `region_rev`:

```
SHOW MATERIALIZED VIEWS
name=region_rev  rows=3  use_count=0  cost_saved=0.0

-- two substituted runs of the exact aggregate query
SHOW MATERIALIZED VIEWS
name=region_rev  rows=3  use_count=2  cost_saved=18.0

-- an EXPLAIN plans but does not serve, so it records nothing
SHOW MATERIALIZED VIEWS
name=region_rev  rows=3  use_count=2  cost_saved=18.0
```

`cost_saved` accrues the gate's estimated saving per reuse: recompute rows minus
stored rows, here 12 - 3 = 9 per run, so 18 after two runs. The counters persist
in the registry, so a NEW runtime over the same store reads them back:

```
-- fresh process, same store
SHOW MATERIALIZED VIEWS
name=region_rev  rows=3  use_count=2  cost_saved=18.0
```

A note on measured wins. `cost_saved` is an ESTIMATE in ROW units (input rows
elided minus stored rows read), not a measured time. This tutorial deliberately
quotes no latency numbers: per the repo's rule, engine perf claims come only
from `benchmarks/perf_compare`. For a like-for-like measured comparison of the
same store machinery (build once, serve warm, agree with a baseline signature),
follow a real benchmark harness that reports engine-versus-baseline ratios
rather than a counter.

--------------------------------------------------------------------------------

## 5. Shortcomings (honest list)

- EXACT-MATCH ONLY. No predicate subsumption, no column-subset matching, no
  ordinal/alias canonicalization beyond what the optimizer already folds. A view
  serves only queries whose optimized subtree is structurally identical to its
  definition (section 3a-3c). A different constant, column set, or filter, or a
  user table alias, all decline.
- PER-SESSION KILL SWITCH. `accelerator.enable_substitution` is a session
  setting, not a per-view or per-query control. You cannot disable substitution
  for one view while keeping it for another; it is all-or-nothing per session.
- ESTIMATED cost_saved. The counter is in ROW units from the cost model's
  estimate, not measured milliseconds. Do not read it as a timing.
- NO EVICTION. The store grows as you create and refresh views; there is no size
  budget and no automatic eviction yet. `DROP MATERIALIZED VIEW` is the only way
  to reclaim a view's chunks.
- ORDER-FIXING SHAPES ARE NEVER SUBSTITUTED. A subtree that fixes output order
  or row selection anywhere within it - a Sort, a LIMIT, or a scan with a folded
  ORDER BY / LIMIT, at the root OR nested under a Projection/Limit - always
  declines (section 3d). This is a correctness guard, not a missing optimization:
  a chunk read is an unordered SET, so serving such a shape could drop its order.
  You therefore cannot accelerate an ORDER BY / LIMIT view directly; put the
  ORDER BY in the OUTER query, above the view, so the plan re-sorts the
  substituted scan.
