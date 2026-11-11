# 5. Four domains: assemble channel health without multiplying facts

The studio can now put acquisition, revenue, support effort, and activity in
one channel review. The SQL expresses each operational metric at player grain
before bringing the four domains together:

```sql
WITH player_revenue AS (
    SELECT t.player_id, SUM(t.amount_cents) AS revenue_cents
    FROM billing.public.transactions AS t
    WHERE t.status = 'settled'
    GROUP BY t.player_id
), player_support AS (
    SELECT t.player_id, SUM(t.handling_minutes) AS support_minutes
    FROM crm.support.tickets AS t
    GROUP BY t.player_id
), player_activity AS (
    SELECT e.player_id, COUNT(*) AS sessions
    FROM gameplay.gameplay.events AS e
    WHERE e.event_name = 'session_start'
    GROUP BY e.player_id
)
SELECT w.channel, COUNT(*) AS registered_players,
       SUM(COALESCE(r.revenue_cents, 0)) AS revenue_cents,
       SUM(COALESCE(s.support_minutes, 0)) AS support_minutes,
       SUM(COALESCE(a.sessions, 0)) AS sessions
FROM acquisition.main.web_events AS w
LEFT JOIN player_revenue AS r ON w.player_id = r.player_id
LEFT JOIN player_support AS s ON w.player_id = s.player_id
LEFT JOIN player_activity AS a ON w.player_id = a.player_id
WHERE w.event_name = 'registration'
GROUP BY w.channel
ORDER BY w.channel;
```

## Each upstream owns a complete piece of the calculation

The acquisition branch keeps registration events and their channel mapping:

```sql
SELECT
  "web_event_id",
  "visitor_id",
  "player_id",
  "occurred_at",
  "event_name",
  "channel"
FROM "main"."web_events" AS "w"
WHERE
  (
    "w"."event_name" = 'registration'
  );
```

PostgreSQL owns settled revenue per player, so FedQ sends it both the payment
predicate and the aggregation:

```sql
SELECT
  "t"."player_id" AS "player_id",
  SUM("t"."amount_cents") AS "revenue_cents"
FROM "public"."transactions" AS "t"
WHERE
  (
    "t"."status" = 'settled'
  )
GROUP BY
  "t"."player_id";
```

MySQL owns handling effort per player, independent of acquisition channel:

```sql
SELECT
  `t`.`player_id` AS `player_id`,
  SUM(`t`.`handling_minutes`) AS `support_minutes`
FROM `support`.`tickets` AS `t`
GROUP BY
  `t`.`player_id`;
```

ClickHouse owns session starts. It can discard level outcomes and count the
sessions per player without knowing anything about billing or support:

```sql
SELECT
  "e"."player_id" AS "player_id",
  COUNT(*) AS "sessions"
FROM "gameplay"."events" AS "e"
WHERE
  (
    "e"."event_name" = 'session_start'
  )
GROUP BY
  "e"."player_id";
```

None of the three databases can compute the final channel review using only
its own data. Each can produce its player-level metric. The Parquet branch
supplies the shared mapping from player to channel, and FedQ supplies the
cross-domain joins and final grouping.

## The coordinator joins summaries, not three raw fact tables

```text
PostgreSQL: (player_id, revenue_cents) -------+
                                            |
MySQL:      (player_id, support_minutes) ----+--> LEFT JOIN onto registrations
                                            |         (player_id, channel)
ClickHouse: (player_id, sessions) ------------+                 |
                                                              v
                                                     GROUP BY channel
                                                              |
                                                              v
                                             players, cash, effort, sessions
```

Each metric relation has at most one row per player. Under the fixture's
single-registration contract, the left joins preserve the registration
population instead of expanding it. A missing metric becomes zero at the
final sums, while the player still contributes one to the channel population.

For comparison, a hypothetical player with two payments, three tickets, and
four sessions could produce 24 rows if those raw fact tables were joined by
player first. Summing money or effort over that expansion would repeat facts.
The per-player aggregations prevent that multiplication while also allowing
the expensive grouping work to execute at the systems that own the facts.

```text
Sort
  AliasedRelation
    HashAggregate group=["w"."channel"]
      HashJoin [LEFT] keys=("w"."player_id" = "a"."player_id") build=Right
        HashJoin [LEFT] keys=("w"."player_id" = "s"."player_id") build=Right
          HashJoin [LEFT] keys=("w"."player_id" = "r"."player_id") build=Right
            Scan [acquisition]
            CteScan [player_revenue] #0
              Cte [player_revenue] #0
                Scan [billing]
          CteScan [player_support] #1
            Cte [player_support] #1
              Scan [crm]
        CteScan [player_activity] #2
          Cte [player_activity] #2
            Scan [gameplay]
```

The verified creator-channel row contains 80 registered players, 148883
revenue cents, 3439 support minutes, and 1091 sessions. The revenue agrees
with the earlier acquisition query because both sum the same settled-player
payments under the same one-registration mapping.

The coordinator is doing real relational work here. It is not concatenating
four result sets and asking the client to reconcile them. It performs the
three left joins, preserves their NULL semantics, aggregates the metrics at
channel grain, and returns one result relation to the client.

---

[Contents](README.md) | [Previous: 04-gameplay-and-support.md](04-gameplay-and-support.md) | [Next: 06-acceleration.md](06-acceleration.md)
