# 1. Support by subscription plan: follow one query all the way through

The support lead needs both total workload and workload relative to the
number of players. A plan with more tickets may simply have more players.
The analyst writes the business question directly:

```sql
WITH support_per_player AS (
    SELECT t.player_id, COUNT(*) AS tickets,
           SUM(t.handling_minutes) AS handling_minutes,
           SUM(CASE WHEN t.closed_at IS NULL THEN 1 ELSE 0 END) AS open_tickets
    FROM crm.support.tickets AS t
    GROUP BY t.player_id
)
SELECT p.subscription_plan, COUNT(*) AS players,
       SUM(COALESCE(s.tickets, 0)) AS tickets,
       SUM(COALESCE(s.open_tickets, 0)) AS open_tickets,
       SUM(COALESCE(s.handling_minutes, 0)) AS handling_minutes,
       SUM(COALESCE(s.tickets, 0)) * 100.0 / COUNT(*) AS tickets_per_100_players
FROM billing.public.players AS p
LEFT JOIN support_per_player AS s ON p.player_id = s.player_id
GROUP BY p.subscription_plan
ORDER BY p.subscription_plan;
```

## First, FedQ identifies two kinds of work

The CTE `support_per_player` uses only `crm.support.tickets`. Its grouping key,
its counts, its sum of handling minutes, and its open-ticket condition all
belong to MySQL. The source already knows how to compute that whole grouped
relation. Sending the aggregation upstream means the coordinator can receive
one support summary per player instead of reconstructing every ticket.

The outer relation contains a PostgreSQL field, `p.subscription_plan`, and
MySQL-derived support totals. MySQL cannot group those totals by subscription
plan using this source-local fragment: the plan assignments live in billing.
PostgreSQL cannot calculate the ticket summaries from its own tables either.
The captured plan therefore leaves the player/support join and the final
plan-level aggregation with FedQ.

There is no correlated subquery expression to eliminate in this statement.
The CTE is already a named relational input. FedQ still runs its decorrelation
pass, but calling the CTE itself a decorrelation would misdescribe the rewrite.

## The request to billing supplies the population

The recorded PostgreSQL fragment is:

```sql
SELECT
  "player_id",
  "registered_at",
  "subscription_plan"
FROM "public"."players" AS "p";
```

Notice that the FedQ datasource prefix `billing` has disappeared. The upstream
PostgreSQL connection already selects billing's database, so the local table
name is `public.players`. The qualifier was necessary for FedQ to establish
ownership; it is not a schema that PostgreSQL needs to invent.

This branch supplies player identity and plan membership. It must include
players who have no tickets, because they belong in the rate's denominator.
The recorded scan also includes `registered_at`; that field does not
participate in this answer. The snippet shows the actual scan, including that
extra column, rather than an idealized projection.

## The request to CRM computes the player-level support relation

FedQ sends MySQL this fragment:

```sql
SELECT
  `t`.`player_id` AS `player_id`,
  COUNT(*) AS `tickets`,
  SUM(`t`.`handling_minutes`) AS `handling_minutes`,
  SUM(CASE WHEN (
    `t`.`closed_at` IS NULL
  ) THEN 1 ELSE 0 END) AS `open_tickets`
FROM `support`.`tickets` AS `t`
GROUP BY
  `t`.`player_id`;
```

Every aggregate here is source-local. MySQL counts each player's tickets,
sums their recorded handling minutes, and turns `closed_at IS NULL` into an
open-ticket indicator before summing it. An open ticket contributes its
handling effort so far. This is agent effort, not the elapsed interval from
opening to closure.

The backticks are evidence of dialect rendering: the analyst wrote ordinary
qualified SQL once, while the statement handed to CRM is MySQL SQL. The
fragment's `GROUP BY player_id` is also evidence of aggregation pushdown. A
plan label saying only `Scan [crm]` would hide this distinction if we did not
read the SQL attached to it.

The fixed fixture contains 441 tickets for 233 players. Consequently this
branch has 233 grouped rows. That row-count reduction follows from the
executed grouping and was checked against the fixture; it is not an elapsed
time claim. The 441 raw ticket rows are not the relation FedQ feeds into the
cross-source join for this plan.

## The streams arrive with compatible join keys and different payloads

The useful shape of the two inputs is:

```text
billing batches                         CRM summary batches
player_id, subscription_plan, ...        player_id, tickets,
                                        handling_minutes, open_tickets
             \                            /
              \                          /
               +--- LEFT HASH JOIN ------+
                         on player_id
                              |
                  one row per billing player
                              |
                  GROUP BY subscription_plan
```

Arrow is the common representation at that boundary. It is not an assertion
that MySQL natively sends an Arrow stream: its connector converts protocol
values into Arrow arrays before the coordinator consumes them. The catalog
and execution schemas let the coordinator orient the `player_id` join keys
and interpret the numeric support columns.

## FedQ builds the support side, then preserves every player

The actual plan says `HashJoin [LEFT] ... build=Right`. Its right input is
`support_per_player`. FedQ builds the hash-join state from those support rows
and probes it using the billing population. A player with a support summary
gets one joined row containing that summary. A player without one still gets
a row, with NULL support fields.

These are examples of the joined values after applying the query's
`COALESCE` expressions, verified against the default fixture:

| Player | Plan | Tickets | Open tickets | Handling minutes |
| ---: | --- | ---: | ---: | ---: |
| 1 | free | 2 | 0 | 62 |
| 2 | free | 2 | 1 | 75 |
| 5 | annual | 2 | 0 | 35 |
| 10 | free | 0 | 0 | 0 |

Player 10 explains why the join is left-sided. That player contributes one
to the free-plan population and zero to its support totals. An inner join
would remove that player and change the denominator. Joining raw tickets
instead would repeat players who opened several tickets; a plain `COUNT(*)`
would then count joined ticket rows rather than the player population.

A NULL produced by an unmatched left join means there was no matching support
row. It does not mean that a failed CRM request is treated as zero support:
an upstream execution failure raises instead of supplying a fabricated empty
relation.

## The final aggregation answers the business question

Only after matching plan membership to support totals can FedQ group by
`subscription_plan`. At this point `COUNT(*)` counts players, while the sums
count their tickets and effort. FedQ evaluates the rate from the plan-level
ticket sum and player count, then orders the output for the client.

| Plan | Players | Tickets | Tickets per 100 players | Handling minutes |
| --- | ---: | ---: | ---: | ---: |
| annual | 27 | 43 | 159.26 | 991 |
| free | 205 | 286 | 139.51 | 6926 |
| monthly | 68 | 112 | 164.71 | 2840 |

The free plan accounts for the greatest total support effort. The monthly
plan has the highest ticket rate per player. Those observations address
different staffing and product questions; sorting raw ticket counts alone
would lose that distinction. These are synthetic fixture results.

Here is the same execution story in the captured physical plan. Read from
its source leaves toward its root to follow data into the final result:

```text
Sort
  AliasedRelation
    HashAggregate group=["p"."subscription_plan"]
      HashJoin [LEFT] keys=("p"."player_id" = "s"."player_id") build=Right
        Scan [billing]
        CteScan [support_per_player] #0
          Cte [support_per_player] #0
            Scan [crm]
```

`Cte` and `CteScan` connect the support producer to its consumer; matching IDs
identify the producer within this plan. `HashJoin` is where the two systems'
results meet. The `HashAggregate` above it is the plan-level grouping that
could not be completed by either upstream alone. `Sort` prepares the final
ordered rows. FedQ server then encodes the result for the PostgreSQL client;
the analyst sees the column names requested by the original SELECT.

---

[Contents](README.md) | [Previous: 00-studio-and-execution.md](00-studio-and-execution.md) | [Next: 02-correlated-questions.md](02-correlated-questions.md)
