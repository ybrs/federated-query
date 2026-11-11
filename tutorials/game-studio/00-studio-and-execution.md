# The studio and the execution pipeline

The support lead at Lantern Quest has a question: **which subscription plans
consume the most support effort?** Billing can identify a player's plan, but
it cannot see their tickets. The CRM can count tickets and handling minutes,
but it does not own subscription data. The answer belongs to neither system
on its own.

The same boundary appears elsewhere in the studio. Marketing can count
registrations without knowing who paid. The game team can see level failures
without knowing who contacted support. Each team has a useful view of the
player, but a decision often needs several views at once.

FedQ accepts a SQL statement spanning those systems, turns it into work each
system can execute, and finishes the work that crosses their boundaries. This
showcase follows that process: from the SQL the analyst writes, through the
rewrites and source queries, to the Arrow batches and joins that produce the
answer. The query plans and fragments are captured from the running engine;
they are evidence for the explanation, rather than commands the reader must
run to discover what happened.

You can read the story without starting the databases. [the setup chapter](setup.md) contains
the setup and server commands for reproducing it. The [contents](README.md) link to all seven business queries, their source SQL,
and the accelerator chapters.

## The studio's data, and what each system knows

| Owner | Storage | FedQ table | One row represents |
| --- | --- | --- | --- |
| Billing | PostgreSQL | `billing.public.players` | A registered player and their subscription plan |
| Billing | PostgreSQL | `billing.public.transactions` | A payment attempt, in USD cents |
| Support | MySQL | `crm.support.tickets` | A ticket, its handling effort, and optional closure |
| Game team | ClickHouse | `gameplay.gameplay.events` | A session start or level outcome |
| Marketing | Parquet | `acquisition.main.web_events` | A page view, signup start, or registration |

`player_id` connects the registered identities. Web visitors initially have
only `visitor_id`; the registration event establishes the connection to a
player. A visitor has one acquisition channel and at most one registration.
Those contracts matter: joining every page view directly to every payment
would answer a different question from joining one registration per player.

The deterministic fixture contains 300 registered players and 300 additional
visitors who never register. Its observation period is January 2026, and its
timestamps represent UTC. Players keep their `free`, `monthly`, or `annual`
plan throughout this fixture. Some payments fail, some players never buy,
some players never contact support, and some tickets remain open.

## What happens after the analyst sends SQL

The client sends one statement to FedQ server over the PostgreSQL wire
protocol. The server gives that statement to the same Rust runtime used by
the embedded CLI and Python library. The runtime does substantially more
than forwarding pieces of SQL to different connections:

```text
SQL from the client
        |
        v
Parse and bind names/types against the catalog
        |
        v
Decorrelate subquery expressions into relational operations
        |
        v
Optimize the relational plan and choose physical operations
        |
        v
Identify source-local work and render each source's SQL dialect
        |
        +------------+------------+------------+
        v            v            v            v
   PostgreSQL      MySQL      ClickHouse    Parquet adapter
        |            |            |            |
        +------------+------------+------------+
                     |
                     v
              Arrow record batches
                     |
                     v
      FedQ / DataFusion joins, aggregates, projections, sorting
                     |
                     v
           Result columns and rows returned to the client
```

**Binding establishes ownership.** In `crm.support.tickets`, `crm` is a FedQ
datasource, `support` is a schema/database inside that source, and `tickets`
is its table. FedQ resolves each column to its owning relation and its type.
A misspelled column raises before the engine can manufacture an answer from
some other field. The ownership information also tells later passes which
expressions can be evaluated together at one source.

**Decorrelation changes the shape of work.** An `EXISTS` containing a reference
to an outer player can become a semi-join against a set of matching players.
That gives the optimizer a relational operation to place and optimize. It
also avoids treating a correlated predicate as an instruction to make one
remote request per outer row. [The support query](01-support-by-plan.md) already expresses its
work as a grouped relation and a join; the second shows an actual correlated
query and the semi-joins FedQ creates from it.

**Splitting follows the relational plan.** A filter and aggregation involving
only CRM columns are candidates to execute together in MySQL. A join between
CRM tickets and PostgreSQL players crosses a source boundary. The physical
planner decides where that join happens, using the available statistics,
source capabilities, and its placement rules. In the captured examples the
cross-source joins happen at the coordinator. FedQ also has dimension-shipping
paths; a cross-source join does not imply that every possible plan must use
the same placement.

**Translation happens after deciding what to send.** FedQ emits a canonical
PostgreSQL-form representation, then renders the source dialect at one
boundary. A MySQL fragment uses MySQL identifier quoting and supported syntax;
a PostgreSQL fragment uses PostgreSQL's. This is not a string split on table
names: the engine is rendering independently executable relational subplans.
Expression semantics remain DuckDB's, including numeric division; placement
must not change the meaning of the original expression.

**Results meet in Arrow.** The connector layer presents source results as
Arrow schemas and record batches. PostgreSQL's ADBC path decodes into Arrow;
ClickHouse is queried over HTTP with `FORMAT ArrowStream`; the MySQL connector
builds Arrow arrays from MySQL protocol values. The Parquet adapter evaluates
its branch over the registered files in-process. There is no remote Parquet
SQL server.

The common Arrow representation lets the coordinator join data from different
protocols without implementing a separate join engine for every source pair.
FedQ's execution bridge maps coordinator work into DataFusion execution
regions. Compatible local operations can be fused into those regions rather
than each requiring a separately collected intermediate table.

There are still buffering boundaries: the current connector fetch interface
can collect batches, a hash join builds state for one side, and aggregation
and sorting also need state. Independent source reads can be prefetched;
a read that depends on runtime join keys has a dependency on the operation
producing those keys.

---

[Contents](README.md) | [Next: 01-support-by-plan.md](01-support-by-plan.md)
