# 2. Correlated questions become semi-joins

The game team now wants a follow-up list: players who both contacted gameplay
support and failed level 7 on version 1.1. Writing two existence conditions
is a natural way to state that question:

```sql
SELECT p.player_id, p.subscription_plan
FROM billing.public.players AS p
WHERE EXISTS (
    SELECT 1
    FROM crm.support.tickets AS t
    WHERE t.player_id = p.player_id AND t.category = 'gameplay'
)
AND EXISTS (
    SELECT 1
    FROM gameplay.gameplay.events AS e
    WHERE e.player_id = p.player_id
      AND e.game_version = '1.1'
      AND e.level_number = 7
      AND e.event_name = 'level_failed'
)
ORDER BY p.player_id;
```

Both subqueries are correlated: their `p.player_id` references belong to the
outer PostgreSQL player relation. Executing the SQL as a literal outer loop
would suggest checking CRM and ClickHouse separately for each player. That
is not how the captured FedQ plan executes this statement.

## Decorrelation exposes the join keys

FedQ extracts the equality conditions connecting the outer player to each
inner relation. It turns the first `EXISTS` into a semi-join against gameplay
tickets and the second into a semi-join against qualifying failed attempts.
The source-local predicates stay with their source relations:

```text
Before: a player row with two correlated EXISTS expressions

After:
  players
    SEMI JOIN gameplay tickets      ON matching player_id
    SEMI JOIN version-1.1 failures  ON matching player_id
```

A semi-join returns a left-side row when at least one right-side match exists.
It does not append every matching ticket or attempt to the result. A player
with several tickets and several failures therefore still appears once.
That is the meaning of `EXISTS` preserved as a relational operation, rather
than approximated with a raw inner join that would multiply rows.

The captured physical plan makes that rewrite visible:

```text
Sort
  Projection
    HashJoin [SEMI] keys=("p"."player_id" = "__subq_1"."__subq_1_k0") build=Right
      HashJoin [SEMI] keys=("p"."player_id" = "__subq_0"."__subq_0_k0") build=Right
        Scan [billing] +inject(player_id)
        AliasedRelation
          Projection
            Scan [crm]
      AliasedRelation
        Projection
          Scan [gameplay]
```

The two `HashJoin [SEMI]` nodes are the important change. Names such as
`__subq_0_k0` are generated join-key outputs from the subquery rewrite. The
formerly outer reference is now an explicit equality between two relational
inputs. The downstream optimizer and executor can reason about that join
without retaining a correlated expression inside the filter.

## Each upstream receives a query it can execute independently

Billing supplies the player identity and plan:

```sql
SELECT
  "player_id",
  "subscription_plan"
FROM "public"."players" AS "p";
```

CRM applies its own gameplay-category filter:

```sql
SELECT
  `player_id`,
  `category`
FROM `support`.`tickets` AS `t`
WHERE
  (
    `t`.`category` = 'gameplay'
  );
```

ClickHouse applies all three event predicates together:

```sql
SELECT
  "player_id",
  "event_name",
  "level_number",
  "game_version"
FROM "gameplay"."events" AS "e"
WHERE
  (
    (
      (
        "e"."game_version" = '1.1'
      ) AND (
        "e"."level_number" = 7
      )
    )
    AND (
      "e"."event_name" = 'level_failed'
    )
  );
```

There is no PostgreSQL alias `p` inside either upstream predicate, and no
player-specific parameter to bind repeatedly. Those relationships have moved
into FedQ's semi-joins. The captured fragment journal contains these three
source subplans, while the coordinator performs the existence tests over
their Arrow results.

The verified default result contains 109 players. They form a follow-up
cohort, not proof that a particular ticket concerns that level failure. The
query does not link ticket IDs to event IDs or impose a time window.

The `+inject` marker on the billing scan in this EXPLAIN is a planned
reduction target. The recorded fragments for this particular execution have
`has_dynamic_filter = false`; the explanation therefore does not claim an
additional runtime key filter was applied to these reads. Decorrelation and
source predicate pushdown are visible independently of that optimization.

---

[Contents](README.md) | [Previous: 01-support-by-plan.md](01-support-by-plan.md) | [Next: 03-acquisition-to-revenue.md](03-acquisition-to-revenue.md)
