[Accelerator documentation](../README.md)

# Matching boundaries and controls

Each case below is a real EXPLAIN of the COLD plan - the query recomputes
against the source. The first three examples use a filtered projection view:

```sql
CREATE MATERIALIZED VIEW us_orders AS
  SELECT o_orderkey, o_region, o_total FROM wh.main.orders WHERE o_region = 'US';
```

whose exact re-issue substitutes (positive control):

```text
MaterializedScan [us_orders] :: SELECT "o_orderkey", "o_region", "o_total" FROM "public"."us_orders"
```

## A different constant

The view holds `o_region = 'US'`. A query for `'EU'` is a different plan - the
constant is part of the shape - and does not match:

```sql
EXPLAIN SELECT o_orderkey, o_region, o_total FROM wh.main.orders WHERE o_region = 'EU';
```

```text
Projection
  Scan [wh] :: SELECT "o_orderkey", "o_region", "o_total" FROM "main"."orders" AS "orders" WHERE ("orders"."o_region" = 'EU')
```

The store holds the rows for the specific constant the view was created with. A
US view never serves an EU query. Create a separate view per constant you want
accelerated.

## Fewer (or more) columns

Dropping `o_total` from the projection is a different column set, so no match:

```sql
EXPLAIN SELECT o_orderkey, o_region FROM wh.main.orders WHERE o_region = 'US';
```

```text
Projection
  Scan [wh] :: SELECT "o_orderkey", "o_region" FROM "main"."orders" AS "orders" WHERE ("orders"."o_region" = 'US')
```

Matching is exact on the column set; a subset is not a match. If you want the
two-column read accelerated, create the two-column view.

## An extra filter (no subsumption)

Adding `AND o_total > 50` to a query the view does NOT carry declines, even
though every qualifying row is a subset of the view's rows:

```sql
EXPLAIN SELECT o_orderkey, o_region, o_total FROM wh.main.orders
        WHERE o_region = 'US' AND o_total > 50;
```

```text
Projection
  Scan [wh] :: SELECT "o_orderkey", "o_region", "o_total" FROM "main"."orders" AS "orders" WHERE (("orders"."o_region" = 'US') AND ("orders"."o_total" > 50))
```

This is a deliberate design decision, not a missing optimization: the
accelerator does NOT do predicate subsumption. A view for `o_region = 'US'`
never serves a strictly narrower or wider predicate. What to do instead: create
the narrower view (`WHERE o_region = 'US' AND o_total > 50`) if that exact query
recurs in your workload.

## Ordering and row selection

A subtree that pins output order or which rows survive - a Sort, a LIMIT, or a
scan carrying a folded ORDER BY / LIMIT, at the subtree root OR nested under a
Projection/Limit - is never substituted. Stored chunks are read as an unordered
SET, so serving such a shape could drop its ORDER BY or, across multiple chunks,
reorder its rows. The guard walks the WHOLE matched subtree, so all three of the
shapes below decline even though each collapses rows and would clear the cost
gate. Each EXPLAIN is the COLD plan the query recomputes.

A pure sort (output equals input, 12 rows):

```sql
CREATE MATERIALIZED VIEW sortedall AS
  SELECT o_orderkey, o_total FROM wh.main.orders ORDER BY o_total DESC;

EXPLAIN SELECT o_orderkey, o_total FROM wh.main.orders ORDER BY o_total DESC;
```

```text
Projection
  Scan [wh] :: SELECT "o_orderkey", "o_total" FROM "main"."orders" AS "orders" ORDER BY "orders"."o_total" DESC NULLS FIRST
```

A pure LIMIT (row selection, 12 -> 5):

```sql
CREATE MATERIALIZED VIEW lim_all AS
  SELECT o_orderkey, o_total FROM wh.main.orders LIMIT 5;

EXPLAIN SELECT o_orderkey, o_total FROM wh.main.orders LIMIT 5;
```

```text
Projection
  Limit
    Scan [wh] :: SELECT "o_orderkey", "o_total" FROM "main"."orders" AS "orders" LIMIT 5
```

An ORDER BY that also carries a LIMIT (12 -> 3). After optimization this is
rooted in a `Projection` over `Limit` over a `Scan` with the folded ORDER BY, so
the ordering lives INSIDE the subtree, not at its root - the guard walks down and
declines:

```sql
CREATE MATERIALIZED VIEW top3 AS
  SELECT o_orderkey, o_total FROM wh.main.orders ORDER BY o_total DESC LIMIT 3;

EXPLAIN SELECT o_orderkey, o_total FROM wh.main.orders ORDER BY o_total DESC LIMIT 3;
```

```text
Projection
  Limit
    Scan [wh] :: SELECT "o_orderkey", "o_total" FROM "main"."orders" AS "orders" ORDER BY "orders"."o_total" DESC NULLS FIRST LIMIT 3
```

What to do instead: put the ORDER BY in the OUTER query, above any view you
expect to be substituted. An outer sort re-sorts the substituted scan in the
ordinary way and is always safe; only ordering INSIDE the matched subtree is at
risk, and that is exactly what this guard declines.

## The cost gate declines a trivial view

A view whose output is its whole base table saves nothing: recompute reads the
same rows the cache would.

```sql
CREATE MATERIALIZED VIEW all_orders AS
  SELECT o_orderkey, o_region, o_total FROM wh.main.orders;

EXPLAIN SELECT o_orderkey, o_region, o_total FROM wh.main.orders;
```

```text
Projection
  Scan [wh] :: SELECT "o_orderkey", "o_region", "o_total" FROM "main"."orders" AS "orders"
```

How to read the gate: recompute cost is the total unfiltered base-table rows the
subtree would scan (here 12); read cost is the view's stored row count (here also
12). Substitution fires only when recompute is STRICTLY greater than the read. A
`SELECT * FROM t` view has recompute == read, so the gate declines. This is a
row-work estimate, not a measurement of source or cache latency. A view earns
substitution by COLLAPSING work: a filter, an aggregate, or a join that turns
many input rows into few stored rows (like `region_rev`: 12 in, 3 out).

## The kill switch

`accelerator.enable_substitution` is a session setting, read fresh on every
plan, so toggling it takes effect on the next query:

```sql
SET accelerator.enable_substitution = false;

EXPLAIN SELECT o_region, sum(o_total) AS revenue FROM wh.main.orders GROUP BY o_region;
```

```text
Scan [wh] :: SELECT "orders"."o_region" AS "o_region", SUM("orders"."o_total") AS "revenue" FROM "main"."orders" AS "orders" GROUP BY "orders"."o_region"
```

With it off, `region_rev` is registered but never consulted; the query
recomputes. `RESET accelerator.enable_substitution` restores the default (on).

[Accelerator documentation](../README.md)
