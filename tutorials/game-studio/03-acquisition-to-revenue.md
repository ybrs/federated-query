# 3. Acquisition to revenue: two grains meet at player identity

Marketing's question crosses Parquet and PostgreSQL: which channels bring
visitors who register and eventually pay during this observation period?

```sql
WITH funnel AS (
    SELECT w.channel,
           COUNT(DISTINCT w.visitor_id) AS visitors,
           COUNT(DISTINCT w.player_id) AS registrations
    FROM acquisition.main.web_events AS w
    GROUP BY w.channel
), revenue_per_player AS (
    SELECT t.player_id, SUM(t.amount_cents) AS revenue_cents
    FROM billing.public.transactions AS t
    WHERE t.status = 'settled'
    GROUP BY t.player_id
), channel_revenue AS (
    SELECT w.channel, COUNT(r.player_id) AS paying_players,
           SUM(COALESCE(r.revenue_cents, 0)) AS revenue_cents
    FROM acquisition.main.web_events AS w
    LEFT JOIN revenue_per_player AS r ON w.player_id = r.player_id
    WHERE w.event_name = 'registration'
    GROUP BY w.channel
)
SELECT f.channel, f.visitors, f.registrations,
       COALESCE(c.paying_players, 0) AS paying_players,
       COALESCE(c.revenue_cents, 0) AS revenue_cents,
       f.registrations * 100.0 / f.visitors AS registration_percent,
       COALESCE(c.paying_players, 0) * 100.0 / NULLIF(f.registrations, 0) AS payer_percent
FROM funnel AS f
LEFT JOIN channel_revenue AS c ON f.channel = c.channel
ORDER BY f.channel;
```

There are three different intermediate relations here. The funnel is one row
per channel. Registrations are one row per registered player. Revenue is one
row per player with settled payments. FedQ must preserve those grains while
combining the domains.

## The Parquet branch computes the funnel before joining revenue

The first acquisition fragment is:

```sql
SELECT
  "w"."channel" AS "channel",
  COUNT(DISTINCT "w"."visitor_id") AS "visitors",
  COUNT(DISTINCT "w"."player_id") AS "registrations"
FROM "main"."web_events" AS "w"
GROUP BY
  "w"."channel";
```

This work can stay inside the file-source branch: all the needed fields are
in `web_events`. The Parquet adapter groups the events by channel and counts
distinct visitor IDs and non-NULL player IDs. An anonymous visitor contributes
to `visitors` without contributing to `registrations`. Repeated events cannot
multiply a visitor merely because several rows carry the same identity.

This is a source-local fragment evaluated over files, not a SQL statement
sent to a remote Parquet database. The resulting Arrow relation has the
shape `(channel, visitors, registrations)`.

## A separate acquisition branch carries the player-to-channel mapping

The second fragment reads registrations:

```sql
SELECT
  "visitor_id",
  "player_id",
  "event_name",
  "channel"
FROM "main"."web_events" AS "w"
WHERE
  (
    "w"."event_name" = 'registration'
  );
```

The filter belongs at the file source because it refers only to its
`event_name` column. The output retains `player_id` so that FedQ can connect
the acquisition record to billing. The channel-level funnel cannot replace
this relation: after grouping only by channel, it no longer has the player
identity needed for the revenue join.

That is why one datasource can have more than one fragment. The split follows
the work and the required output grain, not a rule that every database gets
exactly one query.

## PostgreSQL removes failed payments and sums settled cents

Billing receives:

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

`status = 'settled'` is applied before the aggregate, so a failed payment
attempt contributes no revenue. Several settled payments from the same
player become one row before anything is joined to web events. PostgreSQL
can do both operations without knowing a player's marketing channel.

Its Arrow output is `(player_id, revenue_cents)`. It cannot produce the final
channel totals from this fragment because the channel mapping belongs to
Parquet. Conversely, the file-source branch cannot decide which payments
settled because it does not own billing's transaction rows.

## FedQ joins at player grain, then changes grain to channel

The coordinator left joins registrations to player revenue. With the fixture's
one-registration-per-player contract, each registration remains one row.
A player with revenue counts once as a payer, regardless of how many items
they bought. A player without settled payments remains in the registration
population and contributes zero revenue.

Next, FedQ groups those joined rows by channel to produce `channel_revenue`.
Only now do both inputs to the final join have channel grain: the earlier
Parquet funnel and this cross-domain revenue summary. Their left join keeps
a channel even when it has visitors but no registrations. `NULLIF` leaves
the payer percentage NULL when there are no registrations to divide by.

```text
Sort
  Projection
    HashJoin [LEFT] keys=("f"."channel" = "c"."channel") build=Right
      CteScan [funnel] #0
        Cte [funnel] #0
          Scan [acquisition]
      CteScan [channel_revenue] #1
        Cte [channel_revenue] #1
          HashAggregate group=["w"."channel"]
            HashJoin [LEFT] keys=("w"."player_id" = "r"."player_id") build=Right
              Scan [acquisition]
              CteScan [revenue_per_player] #2
                Cte [revenue_per_player] #2
                  Scan [billing]
```

For example, the creator channel has 155 visitors, 80 registrations, 62 paying
players, and 148883 revenue cents in the verified fixture. Each number comes
from the relevant stage: visitor identity from the funnel, payer existence
from the player-revenue join, and cash from settled transactions. It is not
one count repeated over a large raw-event join.

The revenue is cash collected during this period, including prepaid annual
subscriptions and item purchases. It is not monthly recurring revenue or
lifetime value. No advertising-spend relation is present, so the query does
not infer acquisition cost or profitability.

---

[Contents](README.md) | [Previous: 02-correlated-questions.md](02-correlated-questions.md) | [Next: 04-gameplay-and-support.md](04-gameplay-and-support.md)
