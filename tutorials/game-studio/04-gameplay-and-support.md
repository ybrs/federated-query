# 4. Gameplay and support: keep the rate separate from the contact join

The studio deliberately planted a gameplay problem in the fixture: version
1.1 has a higher failure probability at level 7. The game team wants to see
whether affected players also appear in gameplay support records.

```sql
WITH attempts AS (
    SELECT e.game_version, e.level_number, COUNT(*) AS attempts,
           SUM(CASE WHEN e.event_name = 'level_failed' THEN 1 ELSE 0 END) AS failures
    FROM gameplay.gameplay.events AS e
    WHERE e.event_name IN ('level_failed', 'level_up')
    GROUP BY e.game_version, e.level_number
), contacts AS (
    SELECT e.game_version, e.level_number,
           COUNT(DISTINCT t.ticket_id) AS tickets_from_affected_players
    FROM gameplay.gameplay.events AS e
    JOIN crm.support.tickets AS t ON e.player_id = t.player_id
    WHERE e.event_name = 'level_failed' AND t.category = 'gameplay'
    GROUP BY e.game_version, e.level_number
)
SELECT a.game_version, a.level_number, a.attempts, a.failures,
       a.failures * 100.0 / a.attempts AS failure_percent,
       COALESCE(c.tickets_from_affected_players, 0) AS tickets_from_affected_players
FROM attempts AS a
LEFT JOIN contacts AS c
  ON a.game_version = c.game_version AND a.level_number = c.level_number
ORDER BY a.game_version, a.level_number;
```

This question needs two different views of the same ClickHouse table. One
establishes the attempt denominator. The other supplies failed-player keys
for the cross-source support join. Combining them too early would make the
rate depend on how many tickets a player opened.

## ClickHouse computes the attempt and failure totals

The first event fragment is:

```sql
SELECT
  "e"."game_version" AS "game_version",
  "e"."level_number" AS "level_number",
  COUNT(*) AS "attempts",
  SUM(CASE WHEN (
    "e"."event_name" = 'level_failed'
  ) THEN 1 ELSE 0 END) AS "failures"
FROM "gameplay"."events" AS "e"
WHERE
  (
    "e"."event_name" IN ('level_failed', 'level_up')
  )
GROUP BY
  "e"."game_version",
  "e"."level_number";
```

Both successful and failed outcomes are needed here. ClickHouse filters out
other event types, then groups the remaining attempts by version and level.
It returns `(game_version, level_number, attempts, failures)`. The coordinator
receives an already grouped relation rather than joining every attempt to
every support contact before counting.

The event predicate is not just an opportunity to reduce a read; it defines
the population in the rate. Replacing it with `event_name = 'level_failed'`
would make every remaining attempt a failure and change the question.

## The second event branch and CRM branch identify support associations

For the contact relation, only failed attempts are relevant. This ClickHouse
fragment has a different filter and retains player identity:

```sql
SELECT
  "player_id",
  "event_name",
  "level_number",
  "game_version"
FROM "gameplay"."events" AS "e"
WHERE
  (
    "e"."event_name" = 'level_failed'
  );
```

CRM applies the support-side predicate itself:

```sql
SELECT
  `ticket_id`,
  `player_id`,
  `category`
FROM `support`.`tickets` AS `t`
WHERE
  (
    `t`.`category` = 'gameplay'
  );
```

`category = 'gameplay'` needs only CRM data, so MySQL can reject other ticket
categories before returning rows. It retains `ticket_id` because the
coordinator must count distinct tickets after joining. It retains
`player_id` because this is the key that connects the two systems.

At FedQ, the inner join associates failed attempts with the matching player's
gameplay tickets. One ticket can match several failed attempts, so the
coordinator groups by version and level and uses `COUNT(DISTINCT ticket_id)`.
Counting raw joined rows would overstate the number of support contacts.

## A runtime reduction can use the other side's keys

The captured plan marks the failed-attempt scan with `+inject(player_id)`,
and this fragment's recorded `has_dynamic_filter` is true. The right side of
that inner join is CRM. Its player keys can restrict the failed-event read
to players who could actually match a gameplay ticket.

Conceptually, the extra restriction is:

```text
failed-event player_id must belong to the CRM gameplay-ticket key set
```

This is a set filter derived from the execution graph, not one ClickHouse
request per ticket. The journal shows the base fragment above and flags the
runtime filter; it does not expose the appended key list. We therefore show
that dependency rather than inventing an exact final `IN (...)` payload.

The restriction is safe for the contact branch because its join is inner:
an event whose player has no gameplay ticket cannot contribute a joined
contact. It must not be applied to the separate attempt-denominator branch.
Players without support contacts still belong in the gameplay failure rate.

## FedQ brings the two result grains back together

The contact aggregation and the attempt aggregation now both have version/
level grain. FedQ left joins them on those two keys, fills absent contact
counts with zero, calculates the failure percentage from attempts and
failures, and sorts the answer.

```text
Sort
  Projection
    HashJoin [LEFT] keys=("a"."game_version" = "c"."game_version", "a"."level_number" = "c"."level_number") build=Right
      CteScan [attempts] #0
        Cte [attempts] #0
          Scan [gameplay]
      CteScan [contacts] #1
        Cte [contacts] #1
          HashAggregate group=["e"."game_version", "e"."level_number"]
            HashJoin [INNER] keys=("e"."player_id" = "t"."player_id") build=Right
              Scan [gameplay] filter=("e"."player_id" IN (...)) +inject(player_id)
              Scan [crm]
```

In the verified fixture, level 7 has 35 failures out of 215 attempts on version
1.0, and 155 out of 208 on version 1.1: approximately 16.28% and 74.52%.
Tickets from affected players are 57 and 192 respectively. The separate
branches explain why these are different measures, not alternative counts
of the same joined rows.

The player match is an association. A ticket can predate the failure or
concern another level. Ticket counts are distinct within each version/level
group and must not be summed as a global unique-ticket count. Ticket-to-event
IDs and a supported temporal attribution rule would be needed for a stronger
claim about which failure caused a contact.

---

[Contents](README.md) | [Previous: 03-acquisition-to-revenue.md](03-acquisition-to-revenue.md) | [Next: 05-channel-health.md](05-channel-health.md)
