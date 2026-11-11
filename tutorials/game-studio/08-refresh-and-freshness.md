# 8. Upstream writes, stale reads, and refresh

Continue with the view created in [the acceleration chapter](06-acceleration.md).
The application now opens two new gameplay tickets for player 1 in MySQL.
These are real inserts into the Compose `mysql` service's `support` database,
not changes to the DuckDB correctness oracle or a mocked connector:

```sql
INSERT INTO tickets
    (ticket_id, player_id, opened_at, closed_at, category, handling_minutes)
VALUES
    (900000001, 1, '2026-01-30 12:00:00', NULL, 'gameplay', 17),
    (900000002, 1, '2026-01-30 13:00:00', NULL, 'gameplay', 23);
```

Player 1 already belongs to the free-plan support population and has
session-start events. Therefore the expected free-plan change is **two more
tickets and 40 more handling minutes**, with no change to the count of distinct
supported players. Billing and ClickHouse are unchanged.

When the analyst sends the same SELECT with substitution enabled, the result
is still the old stored generation:

| State | Supported free-plan players | Gameplay tickets | Handling minutes |
| --- | ---: | ---: | ---: |
| Before inserts | 140 | 239 | 5839 |
| Cached SELECT after inserts | 140 | 239 | 5839 |
| Live SELECT after inserts | 140 | 241 | 5879 |
| Cached SELECT after explicit refresh | 140 | 241 | 5879 |

The cached-after-insert plan is still a `MaterializedScan`. Its persisted
chunk content hash is identical to the one captured before the inserts.
`use_count` advances when the stored answer is served; `refreshed` remains
empty at this point. Serving the view has not checked upstream version tokens
or scheduled a new pull.

This is the freshness boundary to take seriously: **a repeated query does
not automatically catch up to upstream writes**. The materialization stores
the result from its last successful pull. Its definition and the incoming
query can still match even while the source rows have changed.

The experiment proves the new data is actually visible by temporarily
bypassing substitution on the same live server connection:

```sql
SET accelerator.enable_substitution = false;
```

It then sends the same original SELECT. The plan and executed fragments
return to the three-source path, and the free-plan row becomes 241 tickets
and 5879 minutes. This control separates successful upstream ingestion from
freshness of the stored result.

## Refresh catches up by rebuilding this joined result

With substitution enabled again, the studio explicitly asks for refresh:

```sql
SET accelerator.enable_substitution = true;
```

```sql
REFRESH MATERIALIZED VIEW studio_accelerator_support;
```

The actual status was:

```text
REFRESH MATERIALIZED VIEW (whole re-pull: table 'billing.public.players' has no declared change key)
```

FedQ re-plans and executes the stored definition against its sources. This
DDL pull disables materialized-view substitution internally: rebuilding a
view from its own stale result would never bring it forward. New result
chunks are written, their schema is checked against the existing view, and
the refreshed generation is published in the registry.

The next original SELECT is again a `MaterializedScan`, but now it returns
241 free-plan gameplay tickets and 5879 handling minutes. The view still
contains three rows and 2650 bytes; its chunk hash and `refreshed` timestamp
have changed. A constant row count or byte size alone would not prove that
the stored aggregate is current, so the harness also checks every result
value against the post-insert oracle.

This is **whole refresh**, not incremental join maintenance. The current
append/primary-key refresh admission is for eligible row-wise, single-scan
shapes. This definition has joins, a grouped active-player relation, and a
final aggregation with a distinct count. The recorded status first reports
the missing change-key declaration. No accelerator `change_keys` are declared
in this configuration; a database's own PRIMARY KEY is not that declaration.
Adding keys alone would not make this multi-source aggregate a supported
append-only shape.

For these particular inserts we can describe the expected delta by hand,
but a general refresh must also handle newly supported players, changed
activity membership, and plan changes. This experiment does not claim that
FedQ incrementally updates those joined aggregates.

There is an inspection limitation worth making explicit. CREATE and REFRESH
source pulls currently do not replace the user-SELECT fragment journal.
Reading `SHOW PUSHDOWN FRAGMENTS` immediately after REFRESH can therefore show
the preceding SELECT's fragments. The harness retains this fact in its JSON
and uses the release server's `FEDQRS_TRACE_SQL=1` output to inspect actual
refresh data reads. The `refresh` section of `live.server.log` contains the
billing, MySQL, and ClickHouse fetches. The refresh status, changed chunks,
and verified post-refresh answer provide the other checks.

---

[Contents](README.md) | [Previous: 07-automatic-substitution.md](07-automatic-substitution.md) | [Next: 09-accelerator-measurements.md](09-accelerator-measurements.md)
