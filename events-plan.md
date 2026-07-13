# Event analytics plan - event views + funnel / segmentation / paths

Event analytics over event-shaped tables (an entity id, a timestamp, an event
name, arbitrary property columns): FUNNELS (ordered step conversion within a
time window), SEGMENTATION (a metric over events, filtered and bucketed by
time), and PATHS (most-common event sequences). The hard part is the surface
and the data structure; both are decided here. The feature is built on the
materialized-view machinery (`fq-accel`): an EVENT VIEW *is* a materialized
view with a sort/partition contract, and staleness is the MV refresh contract
(user-triggered REFRESH, never silent).

## 1. Crate: fq-events, and its DAG position

New crate `crates/fq-events`. It owns everything event-specific:

- the event-view ROLE registry (which columns are entity/timestamp/event and
  the optional tiebreak),
- the materialization CONTRACT (validate roles, sort by (entity, timestamp)
  extended by the tiebreak when declared, raise on violations),
- the analysis KERNELS (funnel, segmentation, paths) that run over the sorted
  chunks.

Why a separate crate and not:

- NOT `fq-accel`: fq-accel is generic, content-agnostic MV storage (chunks +
  registry + lifecycle). Event semantics (roles, ordering contract, sequence
  kernels) are a different concern layered ON TOP of it; folding them in would
  make every MV carry event baggage.
- NOT `fq-exec`: fq-exec is the imported DataFusion execution engine and stays
  generic. Funnel matching is not a relational operator DataFusion offers, and
  teaching DataFusion a custom UDAF class for three analyses buys nothing the
  post-scan kernel does not (section 7).
- NOT `fq-parse`: the parser recognizes the statement FORMS; the semantics and
  kernels do not belong in a parsing crate.

DAG edges:

- `fq-events -> fq-common, arrow, rusqlite, chrono` (a leaf beside fq-accel;
  it does NOT depend on fq-accel - the composition happens in fq-runtime,
  exactly as fq-runtime already composes fq-accel with the pipeline).
- `fq-parse -> fq-common` (unchanged): the parsed statement SPEC types
  (`EventRoleColumns`, `FunnelSpec`, `SegmentSpec`, `EventWindow`,
  `TimeBucket`, ...) live in `fq-common::events` as pure data. The surface
  (fq-parse) produces them, the kernels (fq-events) consume them; fq-common is
  the one crate both already depend on, so the contract types add ZERO new DAG
  edges. (Rejected: spec types in fq-parse would force fq-events to depend on
  the parser and pull polyglot-sql into a kernel crate; spec types in
  fq-events would force fq-parse to depend on a kernel crate.)
- `fq-runtime -> fq-events` (new): the runtime executes the defining SELECT,
  hands batches to fq-events for contract enforcement, delegates persistence
  to fq-accel, and dispatches FUNNEL/SEGMENT statements to the kernels.

## 2. Declaring an event view - the DDL

Concrete syntax, consistent with the MV DDL (same lexical classifier in
`fq-parse/src/statement.rs`, same flat namespace, same loud rejection of every
unsupported option):

```
CREATE EVENT VIEW page_events
  ENTITY user_id
  TIMESTAMP occurred_at
  EVENT event_name
  [TIEBREAK sequence_no]
  AS SELECT user_id, occurred_at, event_name, sequence_no, device, country
     FROM warehouse.public.web_events

REFRESH EVENT VIEW page_events
DROP EVENT VIEW page_events
```

Decisions:

- ROLES NAME OUTPUT COLUMNS of the defining SELECT, in the fixed order ENTITY,
  TIMESTAMP, EVENT, then the optional TIEBREAK (a fixed order keeps the
  grammar and error messages unambiguous; a missing or misordered clause
  raises naming what was expected). The declared roles must name DISTINCT
  existing output columns (a tiebreak sharing a role column adds no ordering
  information and raises as a declaration mistake).
- TIEBREAK (optional) names a column that orders events sharing an (entity,
  timestamp) pair - a source sequence number, a log offset, an ingestion id.
  Declaring one makes equal-timestamp order a property of the DATA instead of
  the documented event-name fallback (section 5).
- PROPERTIES ARE IMPLICIT: every output column that is not a role column is a
  property. No PROPERTIES clause - the SELECT list already IS the declaration
  of what the view carries, and a separate list would be a second place for it
  to be wrong.
- ANY SOURCE TABLE (or join, or filter): the mapping is over a SELECT, not a
  table name, so renames, casts, filters, and cross-source joins are all
  expressible in the one place the engine already handles them.
- ROLE TYPE CONTRACT (checked at build, raises `InvalidRoles`):
  entity: INTEGER / BIGINT / TEXT-like; timestamp: TIMESTAMP (any unit) /
  DATE; event: TEXT-like; tiebreak: an orderable EXACT type - INTEGER /
  BIGINT / TEXT-like / DATE / TIMESTAMP (no float, no bool). Anything else
  has no defined ordering/equality semantics for sequence analysis and is
  refused, never coerced. NULL in any role column (the tiebreak included)
  raises at build naming the view, column, and row.
- LIFECYCLE mirrors the MV DDL: no IF [NOT] EXISTS, no schema-qualified names,
  no storage options - each raises with the same rationale as the MV forms.
- `REFRESH EVENT VIEW` re-executes the stored SELECT and re-applies the whole
  contract (validate + sort) before atomically swapping chunks - it delegates
  to the same `Accelerator::refresh_view` publication path (new generation
  first, one catalog swap, unlink after), including the fixed-shape check (a
  drifted source schema raises; the view keeps serving its previous rows).
- CROSS-FORM GUARDS: `REFRESH/DROP MATERIALIZED VIEW` on an event view raises
  and names the EVENT VIEW form (a plain-MV refresh would skip the sort and
  silently break the contract); `REFRESH/DROP EVENT VIEW` on a plain
  materialized view raises symmetrically. `CREATE EVENT VIEW` under a name
  that already resolves anywhere raises (same rule as MV creation).

## 3. Data structure - the sorted, entity-partitioned materialization

The performant representation for sequence queries is per-entity time-ordered
event streams. Decision: an event view is a materialized view whose chunks are
GLOBALLY SORTED BY (entity ASC, timestamp ASC), extended to (entity ASC,
timestamp ASC, tiebreak ASC) when the view declares a TIEBREAK role.

- Sorting by entity first makes the stream ENTITY-PARTITIONED: all events of
  one entity are contiguous, so every sequence kernel is a single linear scan
  holding ONE entity's events at a time - no hash of entity -> event list, no
  shuffle, memory bounded by the largest single entity.
- Sorting by timestamp within the entity gives the kernels time order for
  free; funnel matching and path extraction never sort at query time.
- Distinct-entity counting degenerates to boundary counting (entity changes),
  and per-bucket distinct entities to a per-cell "last entity seen" compare,
  because within any filtered substream entities still arrive in sorted order.

Storage is EXACTLY the fq-accel chunk store: one directory of framed Arrow IPC
chunk files, the registry row in `<config-stem>.stats.sqlite`, the same
create/refresh/drop publication ordering. fq-events adds one table beside it:

```sql
event_views (
  name             TEXT PRIMARY KEY,  -- also the materialized_views name
  entity_column    TEXT NOT NULL,
  timestamp_column TEXT NOT NULL,
  event_column     TEXT NOT NULL,
  tiebreak_column  TEXT,              -- NULL = no declared tiebreak
  created_at       TEXT NOT NULL
)
```

A registry written before the tiebreak role existed migrates on open (the
column is added; existing rows read back as NULL = no tiebreak).

An event view = its `materialized_views` row (chunks, schema, sizes,
timestamps) + its `event_views` row (the roles). Consequences:

- The view is QUERYABLE AS A PLAIN RELATION (`SELECT ... FROM page_events`)
  through the existing `MaterializedViewSource` with zero new binder/planner
  code - which is also what makes the identity-with-the-underlying-table tests
  possible.
- CREATE writes the MV first, then registers roles. A crash between the two
  leaves a plain (roleless) materialized view: still consistent, queryable,
  and removable via `DROP MATERIALIZED VIEW`; the event-view name stays free.
  DROP unregisters roles FIRST, then drops the MV: a crash between the two
  again leaves only a plain MV. No cross-table transaction is needed because
  every intermediate state is a valid plain MV.

### The contract is enforced, twice

- AT BUILD: the runtime executes the SELECT, fq-events validates roles
  (existence, distinctness, types) and REJECTS any NULL in a role column
  (`ContractViolation` naming the view, column, and row ordinal - an event
  without an entity, a time, or a name is not an event, and a declared
  tiebreak that is missing on a row cannot break ties), then sorts the result
  by (entity, timestamp[, tiebreak]) and hands the sorted batches to fq-accel.
  The contract holds by construction; a violated precondition raises and
  nothing is persisted.
- AT SCAN: every kernel streams through a cursor that re-verifies monotonicity
  ((entity, timestamp) non-decreasing, and the tiebreak non-decreasing within
  equal (entity, timestamp) when the view declares one) and role non-nullness
  as it reads, and raises `ContractViolation` on the first regression. This
  defends against out-of-band chunk edits and any future write-path bug: a
  kernel NEVER returns numbers computed over a stream whose ordering premise
  is broken.

Ties (DECIDED): rows equal in every sort key are stored in UNSPECIFIED
relative order, so analysis semantics must never consult that residual order.
Where an analysis needs an order among equal-timestamp DISTINCT events (path
extraction), the pinned rule is: the declared TIEBREAK column when the view
has one, and EVENT NAME ascending otherwise (event name also breaks ties
WITHIN one tiebreak value). This is a deterministic, documented order - not a
claim about true order - so identical data always yields identical results
across rebuilds. Analyses that can be tie-independent stay tie-independent
(funnel's strict-increase rule, collapse of equal-timestamp duplicates).

## 4. Query surface - dedicated statements

Three candidate shapes were considered:

- CLICKHOUSE-STYLE AGGREGATE FUNCTIONS (`windowFunnel(window)(ts, cond...)`,
  `sequenceMatch`): maximally composable, but the semantic knobs hide in
  function-name suffixes and positional args, the user must hand-assemble the
  per-user grouping and the level histogram around every call, and the engine
  would have to teach the binder, optimizer, and DataFusion a new aggregate
  class - a large permanent surface for three analyses.
- TABLE FUNCTIONS in FROM (`SELECT ... FROM FUNNEL(...)`): composes with
  SELECT, but the parse pipeline (polyglot-sql -> Converter) has no
  table-function machinery; building it for three fixed shapes buys
  composition that can be added later by exposing results as relations.
- DEDICATED STATEMENTS (chosen): the Amplitude/Mixpanel model - a funnel/
  segmentation/path query is a STRUCTURED OBJECT (steps, window, measure,
  bucket), not an expression - rendered as a SQL-flavored statement. It
  matches the engine's one existing non-SELECT precedent (the MV DDL,
  classified lexically before the SQL parser), keeps every semantic knob a
  NAMED clause, and returns an ordinary Arrow relation.

The trade-off accepted: v1 results are terminal (not embeddable in a FROM
clause). The extension path is exposing each analysis as a relation source,
which the dedicated-statement grammar does not preclude.

```
FUNNEL OVER <event_view>
  STEPS ('signup', 'activate', 'purchase')     -- 2..16 string event names
  WITHIN 7 DAYS                                 -- SECONDS|MINUTES|HOURS|DAYS

SEGMENT OVER <event_view>
  MEASURE EVENTS | ENTITIES                     -- event count vs distinct entities
  [EVENT 'purchase']                            -- optional single-event filter
  BY HOUR | DAY | WEEK | MONTH                  -- UTC calendar time bucket

PATHS OVER <event_view>
  [STARTING AT 'signup']                        -- optional anchor event
  MAX DEPTH <n>                                 -- positive step bound
  TOP <k>                                       -- positive result cut
```

Clauses appear in the fixed order shown; a misplaced or unknown clause raises.
Property filtering and property group-by (`SEGMENT ... WHERE device = 'ios'
GROUP BY country`) are part of the surface design but NOT implemented in v1:
the clauses are recognized and raise `Unsupported` naming them (the interim
workaround is a second event view whose SELECT carries the filter). Time-range
clauses (`FROM ... TO ...`) are likewise a recognized-and-raising extension.

Result relations (fixed schemas, also served by `describe`):

- FUNNEL: `step BIGINT, event TEXT, entities BIGINT,
  conversion_from_start DOUBLE, conversion_from_previous DOUBLE`
- SEGMENT: `bucket TIMESTAMP, value BIGINT` (buckets with no events are not
  emitted; gap filling is presentation, not measurement)
- PATHS: `path TEXT (step names joined with " -> "), entities BIGINT,
  depth BIGINT`, ordered by entities DESC then path ASC (the deterministic
  cut among equal counts), at most TOP k rows

## 5. Semantics - pinned, not configurable

FUNNEL. For each entity, over its time-ordered events:

- An ATTEMPT starts at EVERY event whose name equals step 1.
- Within an attempt anchored at time t0, step k (k >= 2) matches the EARLIEST
  event named steps[k] with timestamp STRICTLY GREATER than the step k-1
  match and timestamp <= t0 + window (window INCLUSIVE at the boundary; it is
  anchored at the attempt's step-1 event - the Amplitude model - not rolling).
- ORDERING IS NON-STRICT: events between matched steps are ignored (an
  intervening `purchase` does not break `signup -> activate`).
- TIES NEVER ADVANCE: equal timestamps cannot satisfy "strictly greater".
  This is forced by the storage contract (section 3): tie order in the sorted
  stream is unspecified, so any semantics that consulted it (ClickHouse's
  default windowFunnel accepts equal timestamps) would be nondeterministic
  across rebuilds of identical data. Determinism outranks leniency; the
  strict-increase rule is the only tie-order-independent choice.
- RE-ENTRY: an entity's depth is the MAXIMUM depth over all its attempts (a
  user who stalls, then later completes the funnel, counts at the deeper
  step). Each entity is counted ONCE.
- COUNTING: `entities` at step k = distinct entities with depth >= k, so the
  column is non-increasing down the steps. `conversion_from_start` =
  entities[k] / entities[1]; `conversion_from_previous` = entities[k] /
  entities[k-1]; a zero denominator yields NULL (never a fabricated 0.0), and
  step 1's `conversion_from_previous` is NULL (there is no previous step).
- A non-positive WITHIN raises (with strict-increase ties, a zero window can
  never convert past step 1 - the query is a mistake, not a zero).
- Duplicate step names are legal (`('view','view')` = two views within the
  window); matching is per step index, so the second 'view' must be strictly
  later.

Time arithmetic runs in the timestamp column's NATIVE unit (no lossy
normalization): the window converts to that unit with checked arithmetic
(overflow raises). Over a DATE timestamp column only whole-day windows are
meaningful, so a sub-day WITHIN unit raises.

SEGMENTATION. A linear scan over the same sorted stream:

- MEASURE EVENTS counts events per bucket; MEASURE ENTITIES counts distinct
  entities per bucket - O(1) memory per bucket via the sort contract: within a
  bucket's substream entities arrive in non-decreasing order, so "distinct" is
  "changed since this bucket's last contributor".
- Buckets are UTC calendar truncations (HOUR, DAY, WEEK = ISO Monday 00:00,
  MONTH = first of month); timestamps with a timezone are interpreted as the
  instants they are (bucketed in UTC). Output is ordered by bucket.
- `EVENT 'name'` filters by exact event-name equality before measuring.

PATHS. For each entity, over its time-ordered events:

- the path is the entity's event-name sequence starting at the anchor: the
  first event named by `STARTING AT` (an entity with no such event
  contributes no path), or the entity's first event when no anchor is given;
- CONSECUTIVE DUPLICATES COLLAPSE into one step (a page-reload storm is one
  step), which also makes equal-timestamp reorderings of the SAME event
  invisible;
- TIE RULE (DECIDED): the order among equal-timestamp DISTINCT events is the
  view's declared TIEBREAK column when there is one, and EVENT NAME ascending
  otherwise (event name also orders events sharing one tiebreak value). A
  deterministic, documented order - not a claim about true order; determinism
  pinned by tests that permute equal-timestamp input row order and assert
  byte-identical results in both modes;
- the path keeps at most MAX DEPTH collapsed steps; later events are dropped;
- identical paths count across entities (each entity contributes exactly one
  path); the TOP k most common return as `path TEXT (names joined with
  " -> "), entities BIGINT, depth BIGINT`, ordered by entities DESC then path
  ASC. A non-positive MAX DEPTH or TOP raises at parse and again at the
  kernel.

## 6. Where computation runs

- SOURCES (pushdown): everything INSIDE the defining SELECT - filters, joins,
  projections, casts - plans and pushes exactly like any query, because the
  build path IS `Runtime::execute_query`. This is where per-source work
  belongs: reduce the event stream before it is materialized.
- THE SORT: in the engine at build time (arrow lexsort over the executed
  result), not pushed as ORDER BY. Pushing it would burden every source
  dialect for zero gain (the engine re-sorts anyway to GUARANTEE the contract
  rather than trust a remote's collation) and the build path is explicitly
  off the query path, so its cost is paid once per REFRESH.
- DATAFUSION: plain relational reads of an event view (`SELECT ... FROM ev`)
  go through the existing MV chunk-scan path untouched.
- CUSTOM KERNELS (fq-events): funnel, segmentation, and paths are LINEAR
  operators over the sorted stream - a shape DataFusion does not express
  (funnel windows are not window frames; per-entity max-depth with re-entry
  is not an aggregate composition). Decision: POST-SCAN KERNELS in fq-events
  reading the view's chunks directly (Arrow IPC readers over the chunk paths
  the registry publishes), NOT DataFusion UDAFs/window functions. Rationale:
  the stream is already partitioned and sorted, so the kernels are
  single-pass with per-entity working sets - a UDAF would re-partition and
  re-buffer inside DataFusion's aggregate machinery to reconstruct exactly
  the layout the store already guarantees, and would drag plan/optimizer
  surface with it. The funnel kernel is O(step-1 occurrences x window span)
  per entity in the worst case; the classic O(events x steps) DP is the
  refinement path, blocked on a measured need (no perf claim without the
  harness, per CLAUDE.md). The paths kernel buffers one equal-(entity,
  timestamp) group at a time plus the per-path tallies.
- STALENESS: the MV contract verbatim. Serving trusts the last pull; FUNNEL /
  SEGMENT / PATHS / plain reads never check the source; `REFRESH EVENT VIEW`
  is the only thing that moves the data forward. Never silent: freshness is
  visible (`refreshed_at` in the registry), and the analyses run over exactly
  what a plain SELECT of the view shows.

## 7. Implemented vs design-only (this milestone)

IMPLEMENTED end to end:

- `crates/fq-events`: registry (tiebreak-aware, with open-time migration),
  contract (validate + sort + scan-time verification, tiebreak included),
  funnel kernel, segmentation kernel, paths kernel, chunk reading.
- `fq-common::events`: the spec types.
- fq-parse: classification + full clause parsing of CREATE/REFRESH/DROP EVENT
  VIEW (TIEBREAK included), FUNNEL, SEGMENT, PATHS; the
  recognized-but-unimplemented clauses raise `Unsupported` naming the
  surface.
- fq-runtime: DDL execution (create/refresh/drop delegating to fq-accel),
  FUNNEL / SEGMENT / PATHS dispatch, cross-form guards, `describe` for every
  new statement.

DESIGN-ONLY (each raises loudly, naming itself):

- Property WHERE / property GROUP BY, and FROM/TO time ranges on the
  analyses.
- Analyses as composable relations (FROM-clause embedding).
- Incremental (delta) event-view refresh: refresh is a whole re-pull, exactly
  the MV machinery's contract; the append-only-source delta path belongs to
  the accelerator's change-key design (accelerator-plan.md section 2) and
  lands there first.

## 8. Testing

- Hand-computed funnel fixtures (fq-events unit tests + one end-to-end run
  over a real DuckDB source): a six-user event set where every step count is
  verifiable by eye, covering window-boundary inclusivity (exactly t0+window
  converts; one unit past does not), tie non-advancement, re-entry counting at
  the deeper attempt, non-strict ordering (intervening events), no-step-1
  entities, and the empty view.
- Contract violations raise: NULL role values at build; unsorted / mid-stream
  regressed chunks at scan; missing / duplicate / mistyped role columns;
  sub-day windows over DATE.
- DDL lifecycle: create -> query -> refresh (source mutated; counts change to
  the new hand-computed values) -> drop (relation and roles gone); a fresh
  runtime over the same config sees the view; cross-form guards raise both
  ways; name collisions raise.
- Identity with the underlying table: `SELECT` over the event view returns
  exactly the defining SELECT's rows (same values under a shared ORDER BY) -
  the sort/partition contract changes physical order only, never content.
- Segmentation fixtures: per-day event counts and distinct-entity counts,
  hand-computed, including an entity spanning multiple buckets.
- Paths fixtures: consecutive-duplicate collapse, the tie rule in BOTH modes
  (declared tiebreak vs event-name fallback, on a fixture where they
  differ), MAX DEPTH truncation, STARTING AT anchoring (including an entity
  with no anchor), TOP cut and the entities-DESC-then-path order, the empty
  view; determinism pinned by permuting equal-timestamp input row order and
  asserting byte-identical (Arrow IPC) result tables in both modes.
- Tiebreak DDL: parse (optional clause, fixed position), validation raises
  (missing column, shared role column, unorderable type, NULL values naming
  view/column/row), the third sort key at build, scan-time tiebreak
  monotonicity, registry round-trip and pre-tiebreak registry migration, and
  the create -> PATHS -> refresh path end to end over a real DuckDB source.
- Statement classification: every new form parses; every rejected option
  raises naming itself (GROUP BY, WHERE, IF EXISTS, ...).

## 9. Non-goals for v1

- No automatic freshness, no query-path source checks (the MV contract).
- No configurable semantics: one funnel semantics (section 5), pinned by
  tests, not a knob per query. A second mode (e.g. strictly-ordered funnels)
  is a new named clause with its own tests, never a reinterpretation.
- No cross-view analyses (a funnel spans ONE event view).
- No approximate algorithms (HLL entity counts, sampled paths): exact counts
  only; approximation is an optimization to be justified by measurement.
- No event-time watermarking / late-data handling: the view is a snapshot of
  its last pull; REFRESH re-sorts whatever the SELECT now returns.
