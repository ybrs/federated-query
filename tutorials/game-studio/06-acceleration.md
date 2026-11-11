# 6. The accelerator: reuse a three-source answer, then bring it forward

The studio repeats a support review throughout the day. The question is:
**among players who have produced a session-start event, how much gameplay
support does each subscription plan consume?** Billing owns the plan,
ClickHouse establishes activity membership, and MySQL owns the tickets.

FedQ can materialize this joined result and recognize a subsequent execution
of the original SELECT as a candidate for reuse. That changes the data path:
three upstream branches and a coordinator aggregate can become a local read
of three stored aggregate rows.

The real experiment also exposes two limits that matter to this dashboard.
Inserting new upstream rows did not update the stored result automatically.
An explicit refresh did catch up, by re-running the full join. And while cold
cached reads were faster in both published runs, warm cached reads were
slower than the warm live join on this fixture. The
[runtime measurements](09-accelerator-measurements.md) include that regression
rather than treating a cache hit as proof of speed.

## Start with a real three-source join

Here is the SELECT submitted to FedQ server:

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

`active` is a set of player IDs, not the raw session stream. Grouping those
keys in ClickHouse prevents a player's several sessions from multiplying
that player's ticket rows. The coordinator joins each matching ticket to its
player and activity membership, then groups the result by subscription plan.
`supported_players` counts distinct players with matching gameplay tickets;
it is not the whole subscription-plan population from the first chapter.

In the original fixture, the captured live plan is:

```text
HashAggregate group=["p"."subscription_plan"]
  HashJoin [INNER] keys=("p"."player_id" = "active"."player_id") build=Right eq_selectivity(gameplay.gameplay.events.event_name), group_ndv(gameplay.gameplay.events.player_id), ndv(active.player_id)]
    HashJoin [INNER] keys=("p"."player_id" = "t"."player_id") build=Left
      Scan [billing]
      Scan [crm] filter=("t"."player_id" IN (...))
    AliasedRelation
      Scan [gameplay] +inject(player_id)
```

The three source fragments explain what is recomputed on a normal read.
PostgreSQL supplies player identity and plan:

```sql
SELECT
  "player_id",
  "subscription_plan"
FROM "public"."players" AS "p";
```

MySQL keeps gameplay tickets and their handling effort:

```sql
SELECT
  `player_id`,
  `category`,
  `handling_minutes`
FROM `support`.`tickets` AS `t`
WHERE
  (
    `t`.`category` = 'gameplay'
  );
```

ClickHouse filters session starts and returns one key per active player:

```sql
SELECT
  "e"."player_id" AS "player_id"
FROM "gameplay"."events" AS "e"
WHERE
  (
    "e"."event_name" = 'session_start'
  )
GROUP BY
  "e"."player_id";
```

The cross-source joins and final aggregates happen in FedQ. Before creating
any materialization, repeated SELECTs still have those data-plane fragments;
a warmed connection is not itself a persisted query result.

For the default fixture the answer is as follows. Tables in this chapter are
sorted by plan for reading; the defining SELECT has no ORDER BY.

| Plan | Supported players | Gameplay tickets | Handling minutes |
| --- | ---: | ---: | ---: |
| annual | 22 | 34 | 786 |
| free | 140 | 239 | 5839 |
| monthly | 53 | 90 | 2259 |

## Materialize the answer: what is actually stored?

The studio explicitly creates the materialized view. This statement executes
the full defining SELECT and persists its result:

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

The object being stored is the **three-row joined aggregate**, with these
four columns:

```text
subscription_plan | supported_players | gameplay_tickets | handling_minutes
```

It is not a copy of the player, ticket, or event tables. It is not a separate
cached response for each of the three source fragments. The aggregation has
already happened; individual ticket IDs and event rows are absent from this
stored result.

FedQ writes the result as framed Arrow IPC chunks in the selected database's
`materialized/` directory. Its `metadata.sqlite` registry records the defining
SQL, output schema, chunk list, measured row count, persisted byte size, and
creation/refresh information. The chunk list is authoritative: serving a
view uses its published generation of chunks, not a directory listing that
might mix generations.

The captured `SHOW MATERIALIZED VIEWS` metadata reported **3 rows and 2650
persisted bytes**. Those bytes include the framed Arrow representation and
its schema/metadata; they are not a measurement of RAM used or network bytes
saved. Exact chunk copies and hashes are retained by the experiment.

```text
CREATE MATERIALIZED VIEW
    source SQL -> three real source results -> joins and aggregation
                                                |
                                                v
                                      3 result rows in Arrow
                                                |
                                                v
                                 durable chunks + registry publication
```

Creation has a real cost. The [build timing](09-accelerator-measurements.md) covers the entire CREATE statement:
planning, source work, local execution, writing chunks, publishing metadata,
and returning its status to the client. The harness drops the experiment's
previous view before each repeated build sample; that DROP is preparation
outside the timed CREATE, not work subtracted from a reported build time.

## Automatic substitution: the original SQL reads the cache

The analyst sends the original three-source SELECT again, unchanged. FedQ
still parses and binds it. The accelerator compares its normalized logical
plan with eligible materialized definitions before physical execution.
For this query, the structure and ordered output names match, and the
row-cost gate prefers reading the three measured result rows to recomputing
the estimated base-table input work.

The materialized boundary contains no ORDER BY or LIMIT. Stored chunks are
read as a set, so this example does not ask substitution to preserve an order
that the stored representation does not promise.

The captured replacement plan is:

```text
MaterializedScan [studio_accelerator_support]
```

That single leaf replaces both joins, the active-player grouping, and the
plan-level aggregate in the data plane. The client did not replace its SQL
with a hand-written query against the view. The engine recognized the
matching computation and selected the stored result itself.

The executed fragment journal identifies one `accel:<local materialized
root>` datasource, rather than `billing`, `crm`, and `gameplay`. Its local
read is:

```sql
SELECT "subscription_plan", "supported_players", "gameplay_tickets", "handling_minutes" FROM "public"."studio_accelerator_support";
```

Here `public.studio_accelerator_support` is the accelerator's local relation,
not a new table fetched from upstream PostgreSQL. The coordinator reads the
published Arrow chunks and returns the same three output rows.

The data-path difference is:

```text
Live:
  billing rows --------+
  CRM gameplay tickets +--> FedQ joins + aggregate --> 3 answer rows
  active event keys ---+

Substituted:
  same client SELECT -> bind/normalize/match -> stored Arrow rows --> answer
```

Parsing, binding, matching, local execution, accounting, and the client/server
protocol still have costs. A substituted data plane is not a bypass of the
entire runtime. It also does not establish that an offline upstream is safe:
source registration and metadata used to bind the original query can still
require those sources to be available.

---

[Contents](README.md) | [Previous: 05-channel-health.md](05-channel-health.md) | [Next: 07-automatic-substitution.md](07-automatic-substitution.md)
