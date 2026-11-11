# 7. Automatic substitution: reuse support work in a new revenue query

The studio already reviews support effort by subscription plan. Now finance
asks a related question: **which two plans consume the most gameplay support,
and how much settled revenue has each generated?**

This is a new query. Its support branch combines PostgreSQL players, MySQL
tickets, and ClickHouse activity; its revenue branch joins two billing tables.
FedQ can reuse the support computation while continuing to fetch revenue from
PostgreSQL. The application keeps submitting SQL against the original tables.

This chapter follows a real execution through FedQ server against the
[Docker setup](setup.md). It shows a successful substitution inside a join,
with both ORDER BY and LIMIT in the application query.

## The application query

Keep the support view from [chapter 6](06-acceleration.md). First obtain the
live plan and answer with substitution disabled:

```sql
SET accelerator.enable_substitution = false;
```

Submit this SELECT to the connected psql session:

```sql
SELECT review.subscription_plan,
       review.supported_players,
       review.gameplay_tickets,
       review.handling_minutes,
       revenue.revenue_cents
FROM (
    SELECT p.subscription_plan,
           COUNT(DISTINCT p.player_id) AS supported_players,
           COUNT(*) AS gameplay_tickets,
           SUM(t.handling_minutes) AS handling_minutes
    FROM billing.public.players AS p
    JOIN crm.support.tickets AS t ON p.player_id = t.player_id
    JOIN (
        SELECT e.player_id
        FROM gameplay.gameplay.events AS e
        WHERE e.event_name = 'session_start'
        GROUP BY e.player_id
    ) AS active ON p.player_id = active.player_id
    WHERE t.category = 'gameplay'
    GROUP BY p.subscription_plan
) AS review
JOIN (
    SELECT customer.subscription_plan,
           SUM(payment.amount_cents) AS revenue_cents
    FROM billing.public.players AS customer
    JOIN billing.public.transactions AS payment
        ON customer.player_id = payment.player_id
    WHERE payment.status = 'settled'
    GROUP BY customer.subscription_plan
) AS revenue ON review.subscription_plan = revenue.subscription_plan
ORDER BY review.handling_minutes DESC
LIMIT 2;
```

`review` computes support effort for active players. `revenue` computes all
settled payment cents per plan, including purchases by players who did not
contact support. FedQ joins those two plan-level summaries, ranks by handling
minutes, and returns two plans.

The original execution reads four source fragments: billing players, CRM
gameplay tickets, ClickHouse active-player keys, and a second billing fragment
that computes revenue. The support joins and aggregate execute on FedQ.

The result is:

| subscription_plan | supported_players | gameplay_tickets | handling_minutes | revenue_cents |
| --- | ---: | ---: | ---: | ---: |
| free | 140 | 239 | 5839 | 145793 |
| monthly | 53 | 90 | 2259 | 122466 |

## Store the reusable support computation

The support view from chapter 6 stores the computation inside `review`. If
starting with this chapter, create it with the following SQL; when continuing
from chapter 6, keep the view already created there:

```sql
CREATE MATERIALIZED VIEW studio_accelerator_support AS
SELECT p.subscription_plan,
       COUNT(DISTINCT p.player_id) AS supported_players,
       COUNT(*) AS gameplay_tickets,
       SUM(t.handling_minutes) AS handling_minutes
FROM billing.public.players AS p
JOIN crm.support.tickets AS t ON p.player_id = t.player_id
JOIN (
    SELECT e.player_id
    FROM gameplay.gameplay.events AS e
    WHERE e.event_name = 'session_start'
    GROUP BY e.player_id
) AS active ON p.player_id = active.player_id
WHERE t.category = 'gameplay'
GROUP BY p.subscription_plan;
```

This creates three stored rows, one per plan, containing `subscription_plan`,
`supported_players`, `gameplay_tickets`, and `handling_minutes`. The observed
store size was 2650 bytes of framed Arrow chunks. It stores the support
summary; revenue remains outside this materialized definition.

The view definition is the computation inside `review` in the application
query. Its player aliases, predicates, aggregate expressions, and four output
names match that branch. That is the reusable boundary.

## Run the original application query again

Restore substitution, then send the same full SELECT from the start of this
chapter:

```sql
RESET accelerator.enable_substitution;
```

The SELECT is unchanged. It still names
`billing.public.players`, `crm.support.tickets`, and
`gameplay.gameplay.events`; it does not name `studio_accelerator_support`.

The captured EXPLAIN has this operator structure, with cost annotations omitted:

```text
Limit
  Sort
    Projection
      HashJoin [INNER] keys=("review"."subscription_plan" = "revenue"."subscription_plan") build=Right
        AliasedRelation
          MaterializedScan [studio_accelerator_support]
        AliasedRelation
          RemoteQuery [billing] +inject(subscription_plan)
```

**Automatic substitution happened.** The support branch's two joins and
aggregation became `MaterializedScan [studio_accelerator_support]`. Its three
upstream data reads disappeared from this execution. The revenue branch
remains a live PostgreSQL request.

The outer `HashJoin` combines cached support rows with fresh revenue rows.
`Sort` orders the combined answer by handling minutes descending, then
`Limit` returns two rows. The application query's ORDER BY and LIMIT both
execute above the cached support computation. This is an eligible query,
and the actual plan shows FedQ using the cache inside it.

## What actually executes: one local read and one billing request

After that SELECT, run:

```sql
SHOW PUSHDOWN FRAGMENTS;
```

The journal contains exactly two fragments. The first reads the accelerator's
published Arrow rows through its local relation:

```sql
SELECT
  "subscription_plan",
  "supported_players",
  "gameplay_tickets",
  "handling_minutes"
FROM "public"."studio_accelerator_support";
```

The second is sent to PostgreSQL:

```sql
SELECT
  "customer"."subscription_plan" AS "subscription_plan",
  SUM("payment"."amount_cents") AS "revenue_cents"
FROM "public"."players" AS "customer"
INNER JOIN "public"."transactions" AS "payment"
  ON (
    "customer"."player_id" = "payment"."player_id"
  )
WHERE
  (
    "payment"."status" = 'settled'
  )
GROUP BY
  "customer"."subscription_plan";
```

PostgreSQL performs the player/payment join, removes unsettled payments, and
sums cents by subscription plan. It sends that compact relation back to FedQ.
MySQL and ClickHouse receive no data-query fragments for this SELECT. Their
contribution already lives in the three cached support rows. PostgreSQL's
support-population scan is also replaced; PostgreSQL is contacted here for
the independent revenue branch.

The resulting data path is:

```text
Before:
  billing players -----+
  CRM gameplay tickets +--> support joins + aggregate --+
  ClickHouse activity -+                               |
                                                       +--> join --> sort --> limit
  billing players + payments --> revenue aggregate ----+

After automatic substitution:
  stored support rows ---------------------------------+
                                                       +--> join --> sort --> limit
  billing players + payments --> revenue aggregate ----+
```

The returned rows are exactly the two rows shown earlier, in the same order.
Both the original execution and substituted execution were compared against
DuckDB over the original fixture rows, including every output name and value.

## Confirm the reuse and continue

```sql
SHOW MATERIALIZED VIEWS;
```

For `studio_accelerator_support`, the captured metadata changed from `use_count=0`
to `use_count=1` after the substituted SELECT in this isolated capture. A view
already used in chapter 6 increments its existing count by one. It still
reported three stored rows. The plan, the two executed fragments, the unchanged answer, and that
counter change demonstrate reuse together.

The full [captured execution](evidence/substitution-revenue.json) contains
both plans, both result sets, all source fragments, the DuckDB reference, and
the view metadata before and after reuse. This run measured correctness and
placement. The separate [measurement chapter](09-accelerator-measurements.md)
reports the timed accelerator experiment.

Leave `studio_accelerator_support` in place for
[the next chapter](08-refresh-and-freshness.md), which adds CRM tickets and
refreshes that same view. The isolated evidence capture removed its own view
after verifying the result and left all upstream rows unchanged.

[Chapter 11](11-when-substitution-does-not-apply.md) shows the cases where
FedQ recomputes instead of substituting a cached result.

---

[Contents](README.md) | [Previous: Acceleration](06-acceleration.md) | [Next: Freshness and refresh](08-refresh-and-freshness.md)
