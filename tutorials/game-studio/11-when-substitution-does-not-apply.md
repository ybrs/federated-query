# 11. When the accelerator leaves a query on the live sources

[Chapter 7](07-automatic-substitution.md) demonstrated successful substitution
inside a larger join, including outer ORDER BY and LIMIT. This chapter shows
where reuse stops. These cases were also executed through the real FedQ server
against the Docker sources, and every returned row was checked against DuckDB.

A refusal to substitute means the query executes against its sources and
returns the requested answer. It does not mean the SQL fails. The exception
is invalid SQL: it must raise, with or without cached data available.

The recorded checks were:

| Case | Substitution? | Returned rows |
| --- | --- | ---: |
| Exact support computation, positive control | Yes | 3 |
| Different ticket category | No | 3 |
| Additional handling-time filter | No | 3 |
| Different output alias | No | 3 |
| Fewer output columns | No | 3 |
| Session switch disabled | No | 3 |
| Cache holds the whole unfiltered player table | No | 300 |
| Cached definition includes ORDER BY and LIMIT | No | 3 |
| Filtered monthly-player projection, positive control | Yes | 68 |

## A different category needs a different answer

The support view from [chapter 6](06-acceleration.md) holds gameplay support.
This query asks about billing tickets:

```sql
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
WHERE t.category = 'billing'
GROUP BY p.subscription_plan;
```

FedQ recomputed the billing-ticket answer. It did not return the cached
gameplay-ticket rows. The predicate's constant is part of the matched plan.
Its captured operator structure was:

```text
HashAggregate group=["p"."subscription_plan"]
  HashJoin [INNER] keys=("p"."player_id" = "active"."player_id") build=Right
    HashJoin [INNER] keys=("p"."player_id" = "t"."player_id") build=Left
      Scan [billing]
      Scan [crm] filter=("t"."player_id" IN (...))
    AliasedRelation
      Scan [gameplay] +inject(player_id)
```

The journal contains billing, CRM, and gameplay fragments. The CRM fragment
filters `t.category = 'billing'`. There is no `MaterializedScan` in this plan.

## An additional filter does not reuse a broader cached definition

This asks for gameplay tickets with more than 30 handling minutes:

```sql
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
WHERE t.category = 'gameplay' AND t.handling_minutes > 30
GROUP BY p.subscription_plan;
```

The existing view summarizes all gameplay tickets. Its aggregate rows do not
retain ticket-level handling times, so they cannot answer this narrower
question. The actual execution read all three sources and applied the new
predicate to the CRM branch. This is also an exact-match boundary: the
accelerator does not derive a new matching plan by predicate subsumption.

## Output names and column sets must match

Changing the output name creates a different matching shape:

```sql
SELECT p.subscription_plan,
       COUNT(DISTINCT p.player_id) AS supported_players,
       COUNT(*) AS ticket_count,
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

The returned count column is named `ticket_count`, as requested. The query
recomputed from all three sources and did not substitute the four-column
support definition.

Dropping `supported_players` also declined substitution:

```sql
SELECT p.subscription_plan,
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

FedQ returned the requested three columns through the live plan. The current
matcher does not reuse a view merely because it contains a superset of the
requested output columns. Both cases returned the correct three plan rows;
the stored view's reuse counter did not advance.

## The session switch forces a live query

Even the exact support SELECT recomputes with substitution disabled:

```sql
SET accelerator.enable_substitution = false;
```

```sql
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

The plan and fragment journal read billing, CRM, and gameplay. No cached read
was served and the view counter stayed unchanged. Restore normal matching on
the same connection:

```sql
RESET accelerator.enable_substitution;
```

## Copying an entire table does not clear the cost gate

Create a view that stores every player's ID and plan:

```sql
CREATE MATERIALIZED VIEW studio_players_boundary AS
SELECT p.player_id, p.subscription_plan
FROM billing.public.players AS p;
```

Then submit the original source SELECT:

```sql
SELECT p.player_id, p.subscription_plan FROM billing.public.players AS p;
```

The actual plan was:

```text
Projection
  Scan [billing]
```

The fixture has 300 source players and the view stores those same 300 rows.
The gate compares estimated base-table input rows with stored rows; reuse
requires a strict reduction. Here it sees 300 versus 300 and leaves the source
scan in place. This is a row-work decision, not a measured latency comparison.
An explicit `SELECT` from the view by name can still read the stored rows.

## An ordered, limited definition is not substituted

This view stores the first three players:

```sql
CREATE MATERIALIZED VIEW studio_ordered_boundary AS
SELECT p.player_id, p.subscription_plan
FROM billing.public.players AS p
ORDER BY p.player_id
LIMIT 3;
```

Reissue its definition against the source:

```sql
SELECT p.player_id, p.subscription_plan FROM billing.public.players AS p ORDER BY p.player_id LIMIT 3;
```

The captured structure was:

```text
Projection
  Limit
    Scan [billing] :: SELECT "player_id", "subscription_plan" FROM "public"."players" AS "p" ORDER BY "p"."player_id" LIMIT 3
```

The source scan keeps its ordering and limit. No materialized read replaces
it, even though the stored result has only three rows. Chunk reads do not
promise this ordering, so the matcher rejects this definition.

This boundary is different from [the working ranked query in chapter 7](07-automatic-substitution.md).
There, the cached support computation is unordered, and the larger query's
ORDER BY and LIMIT execute above the cached branch. The real plan substitutes
that branch and still returns the correctly ranked top two plans.

## A filtered projection does substitute

A useful positive control distinguishes the cost-gate refusal from a general
inability to accelerate player scans:

```sql
CREATE MATERIALIZED VIEW studio_monthly_boundary AS
SELECT p.player_id, p.subscription_plan
FROM billing.public.players AS p
WHERE p.subscription_plan = 'monthly';
```

Reissue that SELECT against the source:

```sql
SELECT p.player_id, p.subscription_plan FROM billing.public.players AS p WHERE p.subscription_plan = 'monthly';
```

The actual plan was:

```text
MaterializedScan [studio_monthly_boundary]
```

It returned 68 monthly players, all matching the DuckDB reference. The source
has 300 players; the stored filtered relation has 68 rows. The exact match and
strict row-work reduction both hold, and the counter advanced by one.

## Freshness and latency are separate limits

Substitution does not check for new upstream rows or refresh them on reads.
[Chapter 8](08-refresh-and-freshness.md) demonstrates stale results after
actual CRM inserts, a fresh live bypass, and explicit catch-up. That joined
aggregate refreshes through a whole re-pull; declaring change keys does not
make joined aggregates incrementally refreshable.

A cache hit also does not guarantee lower end-to-end latency. The
[measured experiment](09-accelerator-measurements.md) records faster cold
cached reads and slower warm cached reads on this fixture. Those measurements
remain part of the showcase.

## Cleanup and evidence

After trying the three standalone player views above:

```sql
DROP MATERIALIZED VIEW studio_players_boundary;
DROP MATERIALIZED VIEW studio_ordered_boundary;
DROP MATERIALIZED VIEW studio_monthly_boundary;
```

The isolated capture removed every view it created and did not mutate upstream
rows. Its [evidence](evidence/substitution-boundaries.json) contains all nine
checks, cached definitions, complete answers, plans, native source fragments,
and counters. Each EXPLAIN left the reuse counter unchanged; each successful
substituted execution advanced it once, and every refusal left it unchanged.

[Contents](README.md) | [Successful automatic substitution](07-automatic-substitution.md)
