# Dynamic catalog plan - CREATE / DROP / SHOW DATASOURCE at runtime

Datasources become manageable at runtime through SQL DDL: `CREATE DATASOURCE
<name> TYPE <kind> WITH (...)`, `DROP DATASOURCE <name>`, `SHOW DATASOURCES`.
The YAML `datasources:` section is demoted to BOOTSTRAP - a starting set loaded
at construction - while the durable, operator-editable catalog lives in the
stats SQLite next to the config, exactly where `materialized_views` already
lives. A dynamic source survives process restart, is validated (connected +
metadata-loaded) before it persists, and is visible to the session that created
it and to every session opened afterward.

This is the same shape the engine already has for materialized views: a lexical
DDL classifier before the SQL pipeline (`fq-parse/src/statement.rs`), a
persisted registry row in `<config-stem>.stats.sqlite`, a copy-and-swap of the
runtime's private catalog snapshot (`Runtime::install_views`), and a
process-wide exec-plane registration keyed on the runtime's session id
(`fq_exec::connectors::register`). CREATE DATASOURCE joins that family; it does
not invent a new persistence, visibility, or lifecycle model.

## 0. What already exists (the machinery this reuses)

- CONFIG SHAPE. `fq_common::DataSourceConfig { name, ty, config:
  BTreeMap<String, Value>, capabilities, change_keys }` (`config.rs`). The YAML
  block splits `type`, `capabilities`, `change_keys` out and keeps every other
  key as a connection param in `config`. Per-kind param allowlists live in
  `fq-runtime/src/lib.rs` (`reject_unknown_keys`): a typo'd param raises at
  load. Dynamic-catalog params reuse these exact names and allowlists - one
  vocabulary for YAML and DDL.
- REGISTRATION. `register_datasource(session, &mut catalog, ds)` (`lib.rs`)
  dispatches on `ds.ty` to the five kinds (duckdb / postgres / clickhouse /
  mysql / parquet). Each builds an `fq_connectors` catalog handle (CONNECTS and
  is later metadata-loaded) AND an exec-plane `DsSpec` via
  `connectors::spec_from_kind`, then `connectors::register(session, name,
  spec)`. This is the whole per-source registration surface CREATE reuses.
- LIVE CATALOG MUTATION. `Runtime` holds `catalog: RwLock<Arc<Catalog>>`,
  `config: RwLock<Config>`, `stats: RwLock<Arc<StatisticsCollector>>`.
  `install_views` is the precedent: clone the catalog, insert/register the
  change, swap the `Arc`, rebuild the stats collector over the new catalog,
  re-point the exec data plane. A query reads ONE snapshot per plan
  (`catalog_snapshot`), so a concurrent swap never changes metadata
  mid-pipeline.
- PERSISTED REGISTRY PRECEDENT. `fq-accel/src/catalog.rs`'s
  `materialized_views` table: `PRAGMA journal_mode=WAL`, `synchronous` ON
  (durable state, unlike the correctness-neutral learned stats which run
  `synchronous=OFF`), an open-time `PRAGMA table_info` migration
  (`add_benefit_columns`), `INSERT OR IGNORE` + rowcount check for
  first-writer-wins, and a `deleted_at` tombstone. The dynamic-datasource table
  copies this pattern.
- SESSION SCOPING. The exec plane keys every entry on `(SessionId, name)`;
  `Runtime::drop` prunes by session id; `prune_session` leaves peers untouched.
  A datasource registered into one runtime is invisible to another runtime's
  reads (this is the recent session-scoping fix, and it is exactly what makes
  per-session visibility below correct).
- SERVER MODEL. `fedq-server` loads the config ONCE at startup
  (`main.rs::load_config`) and clones it into each connection's `Session`
  (`session.rs::Session::spawn`), which builds its OWN `Runtime` from it on a
  dedicated worker thread. There is no shared mutable catalog across
  connections; each connection's `Runtime` is independent. A new MV created in
  connection A is already invisible to connection B until B reconnects - the
  dynamic-catalog visibility model is identical.

## 1. Syntax

The classifier is lexical (a quote-aware tokenizer, no full SQL parse), added to
`classify_statement` next to the materialized-view forms: `CREATE` with second
word `DATASOURCE`, `DROP` with second word `DATASOURCE`, `SHOW DATASOURCES`.
Every unsupported option raises `ParseError::Unsupported` naming it - never a
silent partial acceptance, matching the MV DDL contract.

```
CREATE DATASOURCE <name> TYPE <kind>
  WITH ( key = 'value' [, key = 'value' ...] )

DROP DATASOURCE <name>

SHOW DATASOURCES
```

- NAME is a single unqualified identifier (a bare word lowercases per the
  Postgres rule; a `"Quoted"` name keeps its exact spelling), reusing
  `statement::view_name`. A datasource is the top namespace of a
  `datasource.schema.table` reference, so a dotted name raises: the datasource
  namespace is flat, exactly like the view store.
- TYPE `<kind>` names one of the five supported kinds as a bare word.
  Rationale for `TYPE` over Trino's `USING <connector>`: our config field is
  literally `type: postgres`, so `TYPE postgres` is the same vocabulary an
  operator already writes in YAML. An unknown kind raises at classify (the same
  set `spec_from_kind` and `register_datasource` accept).
- WITH ( ... ) carries the connection params as `key = 'value'` pairs. Keys are
  the EXACT YAML param names (`host`, `port`, `user`, `password`, `database` /
  `dbname`, `schemas`, `path`, `dir`, `adbc_driver`, ...), validated against the
  same per-kind allowlist `reject_unknown_keys` enforces for YAML - a typo'd
  param raises naming it. Rationale for `WITH (...)` over `OPTIONS(...)`: it
  matches Trino and the SQL-standard-ish spelling, and the MV DDL already
  reserves `WITH` for options (there it is rejected; here it carries the params).
  Values are string literals; a bare integer is accepted for numeric params
  (`port = 5432`). A list-valued param (`schemas`) is a comma-separated string,
  `schemas = 'public,inventory'`, split on comma - Trino's rule that every
  WITH value is a string, applied to our one list param.
- IF NOT EXISTS: REJECTED (raises). A create that silently does nothing hides a
  name collision - identical reasoning to `CREATE MATERIALIZED VIEW IF NOT
  EXISTS`.
- OR REPLACE: REJECTED in v1. A replace silently repoints a live name while
  other sessions may hold a handle to the old source (section 6); make the
  repoint two explicit, auditable steps: `DROP DATASOURCE` then `CREATE
  DATASOURCE`. Revisit only on measured demand.
- ALTER DATASOURCE: NOT in v1. An ALTER is a repoint with partial-failure
  semantics (which fields committed if the new connection fails to validate?);
  DROP+CREATE has one atomic validate-then-persist step and no half-changed
  state. Recommend against adding it; if added later it is sugar over
  drop+create, never an in-place mutation of a live connection spec.
- IF EXISTS on DROP: REJECTED. A drop that silently does nothing hides a typo'd
  name - identical to `DROP MATERIALIZED VIEW IF EXISTS`.
- CASCADE / RESTRICT on DROP: REJECTED. Dependency handling is decided by the
  engine (section 7: DROP raises if a persisted view depends on the source), not
  by a statement option.

### SHOW DATASOURCES output

Columns: `name TEXT, type TEXT, origin TEXT, summary TEXT, created TEXT`.

- `origin` is `bootstrap` (loaded from YAML) or `dynamic` (created via DDL,
  persisted).
- `summary` is a FAIL-CLOSED redacted rendering of the connection params: it
  emits only an ALLOWLIST of display-safe keys (`host`, `port`, `database`,
  `schemas`, `path`, `dir`) and NEVER a value for any other key. A password, or
  any param not on the safe list, is omitted entirely - a param added later is
  redacted by default, never leaked by omission from a denylist (section 5).
  Example: `host=pg1 port=5432 database=sales schemas=public`.
- `created` is the persisted `created_at` for a dynamic source, empty for a
  bootstrap source.

### DROP DATASOURCE behavior

- On a `dynamic` source: remove the persisted row, purge the source's learned
  stats (section 7), unregister it from THIS runtime (catalog swap + exec-plane
  prune of `(session, name)`), in one flow. A crash between the row delete and
  the chunk/stat cleanup leaves an already-removed source (the registry row is
  the source of truth), so no orphan is queryable.
- On a `bootstrap` (YAML) source: RAISES `'x' is a bootstrap datasource
  defined in the config file; remove it from the YAML, not via DROP`. A DROP
  that only unregistered it from the live session would resurrect the source on
  the next reconnect (it is still in YAML) - a silent non-persistence. Loud
  refusal instead.
- When a persisted materialized view or event view's definition references the
  source: RAISES naming the dependent view(s) (section 7). The operator drops
  the views first. This replaces CASCADE.

## 2. Persistence

### Schema

One new table in the stats SQLite (the same file as `materialized_views` and
the learned stats), opened by the accelerator's registry connection so it shares
the WAL:

```sql
dynamic_datasources (
  name          TEXT PRIMARY KEY,   -- the flat-namespace datasource name
  type          TEXT NOT NULL,      -- kind: duckdb|postgres|clickhouse|mysql|parquet
  params        TEXT NOT NULL,      -- JSON {param: <scalar | secret-ref>}
  capabilities  TEXT,               -- JSON array (mirrors DataSourceConfig.capabilities)
  change_keys   TEXT,               -- JSON (mirrors DataSourceConfig.change_keys)
  created_at    TEXT NOT NULL,      -- RFC3339
  updated_by    TEXT                -- creating principal (server user, or 'bootstrap-import')
)
```

DECIDED: typed columns for the queried/identity fields (`name`, `type`,
`created_at`), a JSON BLOB for `params`. Rationale: the param set differs per
kind (`path` vs `dir` vs `host/port/user/password/database/schemas`), which is
exactly the heterogeneous `DataSourceConfig.config: BTreeMap<String, Value>` the
in-memory struct already holds. Typed param columns would force a wide sparse
table or five per-kind tables for zero query benefit - the params are read as a
unit to rebuild the source, never filtered on. `capabilities` and `change_keys`
serialize the same way, mirroring their in-memory shape one-to-one, so a
persisted row round-trips into a `DataSourceConfig` with no field re-listing.

VERSIONING: an open-time `PRAGMA table_info('dynamic_datasources')` migration
adds any absent column with a default, exactly like `add_benefit_columns` and
the event-view tiebreak migration. `synchronous` stays ON for this table (it is
durable operator state, unlike the `synchronous=OFF` learned stats). The
open-time busy-retry (`with_busy_retry`) already covers the WAL/DDL contention
of concurrent first opens.

### Bootstrap merge on `Runtime::from_config`

`from_config` builds the effective datasource set from TWO sources: the YAML
`config.datasources` (bootstrap) and every row of `dynamic_datasources`
(persisted). The merge runs before `register_datasource`, producing one
`DataSourceConfig` list.

NAME COLLISION POLICY - DECIDED: a name present in BOTH the YAML and the
persisted table RAISES at construction, naming the collision. Rationale (the
no-silent-fail law): the two rows may carry different connection params, and
picking either silently would make the effective source ambiguous and
deploy-dependent. There is no correct winner to guess. The operator resolves it
by removing one (drop the dynamic row, or delete the YAML block). This is
stricter than Trino, which keeps static (file) and dynamic catalogs in SEPARATE
namespaces so a collision cannot occur; we have ONE flat namespace, so the
collision is real and must be loud. (Because CREATE validates a name against the
live catalog before persisting - section 6 - a dynamic row can only collide with
a YAML source that was ADDED to the config file AFTER the dynamic source was
created; construction is the only place this surfaces, and it raises.)

The persisted set does NOT mutate the cloned in-memory `Config` a `Session`
carries; the merge happens inside `from_config`, reading the table directly.
This keeps the server's shared `Config` immutable and the per-connection
`Runtime` the sole owner of its effective catalog.

### A persisted source that no longer connects

DECIDED: fail loudly but SCOPED - construction connects and metadata-loads each
persisted source just like a YAML source, but a failure marks THAT ONE source
UNAVAILABLE (capturing the real connector error) instead of aborting the whole
runtime; any bind that resolves a table in an unavailable source RAISES the
captured error naming the source. Sources that connect serve normally.

Rationale: a YAML source failing construction is operator-controlled at deploy
time, so aborting the whole runtime is acceptable there and is left UNCHANGED. A
dynamic source is added post-deploy and may be transiently offline (a laptop DB,
a rotated network); letting one such source make EVERY new server connection
unbuildable is a worse, wider failure than deferring its raise to the reference.
This is NOT a silent fail: an unreferenced unavailable source blocks nothing,
and a referenced one raises the real source error at bind - a query against it
never returns empty or wrong rows. It is a deliberate, stated divergence from
YAML-source behavior, justified by the different operator relationship. Because
CREATE validates before persisting, a source only ever persists in a known-good
state; unavailability is purely a later transient-outage concern, and a
reconnect (or the source coming back) clears it.

## 3. Visibility and sessions

DECIDED: SET-like, per-session visibility. `CREATE DATASOURCE` in session A:

- (a) PERSISTS to `dynamic_datasources` (durable, one short transaction).
- (b) REGISTERS into A's live runtime IMMEDIATELY: build + validate the source,
  `connectors::register(A.session, name, spec)`, then the `install_views`
  copy-and-swap (clone catalog, insert the source's schemas, swap the `Arc`,
  rebuild the stats collector). A's very next query resolves it.
- (c) NEW sessions pick it up at `from_config` (they read the persisted table).

EXISTING OTHER sessions do NOT see it until they reconnect.

Rationale, and the rejected alternative: propagating a new source to LIVE peer
sessions would require a process-wide catalog-generation counter checked per
query, and - on a bump - a catalog REBUILD and stats-collector reset in every
peer. The check itself is O(1) and fits the O(metadata) planning budget, but the
REBUILD does not, and invalidating peers' cached connections and learned-stats
overlay mid-workload is exactly the cross-session churn the session-scoping fix
removed. Critically there is no correctness need: a datasource is a namespace,
so a peer's queries that do not name the new source are unaffected, and a peer
that wants it reconnects (cheap). This mirrors `SET` (session-mutable, read
fresh per query within the session, invisible to peers) and is IDENTICAL to how
`CREATE MATERIALIZED VIEW` / `CREATE EVENT VIEW` already behave: `install_views`
swaps only the calling runtime's catalog, and a peer connection sees the new
view only after its own `from_config` re-reads the store on reconnect. Dynamic
datasources add no new visibility model.

fedq-server implication: no server restart is needed for a new source to reach
new connections; the shared startup `Config` is never mutated - each new
`Session`'s `from_config` reads the persisted table. A dynamic source is only
available on a runtime that has a stats SQLite (a `source_path`); a path-less
programmatic config has nowhere to persist, so `CREATE DATASOURCE` on it raises
the same way MV DDL does (`accelerator()` returns the no-store error).

## 4. Validation

`CREATE DATASOURCE` CONNECTS and METADATA-LOADS the source before ANYTHING
persists, exactly the sequence `from_config` runs (`register_datasource` then
`load_metadata`): build the `fq_connectors` handle, open the connection, load
its schema metadata into a throwaway catalog. Only on success does it (1) write
the `dynamic_datasources` row and (2) install into the live runtime. On failure
NOTHING persists and NOTHING installs, and the statement RAISES the REAL
connector error - no wrapping, no beautified message (the recent
error-surfacing fix): the error names the datasource, the kind, and whether it
failed at connect or at metadata load.

TIMEOUT: the planning budget does NOT apply (this is DDL doing deliberate I/O,
not O(metadata) planning). A dedicated bound, `catalog.create_connect_timeout_ms`
(a registered setting, default 10000), caps the connect+metadata-load so a wrong
host raises promptly instead of hanging on the driver's default TCP timeout.

## 5. Credentials (phase 1) and the phase-2 secret-reference shape

PHASE 1 stores credentials in `dynamic_datasources.params` as plaintext scalars,
BUT redacts them everywhere they could surface, from day one:

- SHOW DATASOURCES: the fail-closed allowlist rendering of section 1 (safe keys
  only; password and anything else omitted).
- EXPLAIN: plan nodes carry table and source NAMES, never connection params or a
  source URI, so a plan dump has nothing to leak; a guard test asserts no
  password string appears in any EXPLAIN output.
- ERRORS and LOGS: the connect/metadata error text names the datasource and
  kind, never the params; the persisted-row logging (if any) logs `name`,
  `type`, and the safe-key summary only.

The redaction is FAIL-CLOSED: rendering emits an explicit safe-key allowlist and
treats every other key as secret, so a newly added credential-ish param is
redacted without a code change. This is the defensive direction (a denylist of
"known secret keys" would leak a param someone forgot to add).

PHASE-2 SHAPE PINNED NOW (implemented under ticket 124): each value in the
`params` JSON is EITHER a scalar literal OR a secret-reference object -
`{"env": "PGPASS_X"}`, `{"file": "/run/secrets/pg"}`, and (future)
`{"command": [...]}` - resolved at CONNECT time (both in `from_config` and in
CREATE validation), never stored resolved. Because the `params` column is
already a JSON value position, phase 2 adds the object form with ZERO column
migration: phase 1 writes scalars, phase 2 additionally writes references. The
DDL surface for authoring a reference (`WITH (password_env = 'PGPASS_X')` or
`WITH (password = env('PGPASS_X'))`) is ticket 124's to decide; this plan pins
only the STORAGE shape so the column is future-proof today.

## 6. Concurrency

- TWO CREATEs of one name racing. Both validate (connect) independently, then
  race the insert. The insert is `INSERT OR IGNORE` on the `name` primary key
  followed by a rows-affected check (the `materialized_views` pattern):
  rows_affected == 0 means a peer won, and the loser RAISES `CREATE DATASOURCE:
  'name' already exists` and DISCARDS its validated connection (the accelerator's
  "loser discards its own chunks" analogue). One short WAL transaction, one
  writer; the open-time busy-retry covers WAL/DDL contention. The loser never
  silently no-ops.
- DROP while a PEER session queries the source. The peer holds its OWN catalog
  snapshot and its OWN `(peer_session, name)` exec-plane spec; the DROP removes
  the persisted row and unregisters only the DROPPER's `(session, name)`. The
  peer's in-flight AND subsequent queries keep resolving the source until the
  peer reconnects, at which point its `from_config` no longer finds the row.
  This is stronger than Trino ("dropping a catalog does not interrupt running
  queries, but makes it unavailable to new queries" - cluster-wide): for us a
  peer session keeps the source for NEW queries too, until reconnect, because a
  peer's catalog is a private, immutable-per-plan snapshot. Stated as the
  semantics; it is safe because the source is a namespace (no shared mutable
  state), and it is the exact same lag as SET and MV DDL.

## 7. Interactions

- LEARNED STATS keyed by datasource NAME. `table_stats`, `predicate_stats`, and
  `source_identity` key on `datasource`; `group_stats` keys on a `subject` that
  may BE a datasource-qualified table. A `DROP` then a `CREATE` reusing the name
  but pointing elsewhere would let stale learned rows poison the new source's
  planning. DECIDED: PURGE the name's learned rows on DROP - delete from
  `table_stats`, `predicate_stats`, `source_identity` where `datasource = name`,
  and from `group_stats` where the `subject` is that datasource's (only those;
  subplan-signature subjects are left, they self-heal) - in the same flow that
  removes the `dynamic_datasources` row. Rationale vs a generation id: a
  generation would have to thread through every learned-stats primary key AND the
  accelerator's fragment/source_fingerprint keys - a wide schema change for a
  rare event - whereas learned stats are correctness-NEUTRAL (they only steer
  plan choice) and SELF-HEAL on the next measurement, so a purge is safe and
  simple. `source_identity`'s existing repoint-fingerprint would ALSO catch a
  changed source, but purge-on-drop is the clean primary mechanism.
- MATERIALIZED / EVENT VIEWS referencing the source. `DROP DATASOURCE` scans the
  persisted view definitions (`materialized_views.definition_sql`, the event-view
  registry) for a base table in the datasource and RAISES naming the dependent
  view(s) rather than orphaning a view whose next refresh would fail to bind.
  This is the dependency guard that replaces CASCADE (section 1).
- CATALOG-METADATA CACHE. The `StatisticsCollector` caches source stats and
  planner estimates per session; a CREATE or DROP rebuilds the collector over
  the swapped catalog (the `install_views` step already does this), so no stale
  metadata survives the DDL. New sessions build a fresh collector.
- EXPLAIN LABELING. A scan against a dynamic source renders identically to one
  against a bootstrap source - the plan does not carry origin, and (per section
  5) carries no connection params. Origin is a SHOW DATASOURCES concern, not a
  plan concern.

## 8. Phases with gates

Each phase stays behind the existing TPC-DS / TPC-H correctness harness and the
`benchmarks/perf_compare` suite (CLAUDE.md rule 3). The dynamic-catalog path is
inert for a config with no dynamic sources, so the OFF path must be identical to
today; that identity is itself a gate.

- PHASE V1 - CREATE / DROP / SHOW for all five kinds.
  Scope: the lexical classifier (`CREATE/DROP DATASOURCE`, `SHOW DATASOURCES`)
  with every unsupported option raising; the `dynamic_datasources` table with
  its open-time migration; bootstrap merge with the raise-on-collision policy;
  connect-and-metadata-validate before persist with the connect timeout;
  per-session visibility + new-session pickup; the unavailable-source
  (bind-time raise) path; purge-on-drop of learned rows; the MV/event-view
  dependency guard; fail-closed credential redaction; plaintext credential
  storage.
  Test strategy: statement-classification unit tests (every form parses, every
  rejected option - IF NOT EXISTS, OR REPLACE, ALTER, IF EXISTS, CASCADE,
  qualified name, unknown kind, unknown param - raises naming itself); a
  persistence round-trip + open-migration test (write a row on an old-shape
  table, reopen, read it back); e2e cases in the `e2e_federated` harness -
  create a Postgres source and a DuckDB source at runtime and query them in the
  SAME session; reconnect a fresh runtime over the same stats SQLite and confirm
  it sees them; DROP one and confirm a subsequent reference raises a bind error;
  a YAML/dynamic name collision raises at construction; a DROP of a bootstrap
  source raises; a concurrent-create loser raises `already exists`; a DROP
  purges the name's learned-stats rows (assert the rows are gone); a DROP with a
  dependent materialized view raises naming the view; a redaction test greps
  SHOW / EXPLAIN / captured logs and asserts no password appears.
  Gate: the TPC-DS / TPC-H suites unchanged (OFF path identical); every
  create-then-query e2e case correct; the redaction grep clean; perf_compare
  warm/cold totals unchanged (construction reads one extra small table).

- PHASE V2 - secret references (ticket 124).
  Scope: the `params` value gains the env / file / (later) command reference
  object; resolution at connect time in both `from_config` and CREATE; the DDL
  authoring surface; redaction already in place from v1.
  Test strategy: a source created with a `password_env` reference connects when
  the env var is set and raises naming the missing var when it is not; a test
  reads the RAW persisted row and asserts it holds only the reference, never the
  secret; a from_config over a persisted reference resolves it.
  Gate: no secret ever appears in the SQLite file, SHOW, EXPLAIN, errors, or
  logs; a referenced source connects; a missing reference raises loudly.

- PHASE V3 - ACLs (ticket 125), SCOPING ONLY, its own design.
  CREATE / DROP DATASOURCE become privileged operations; GRANT / REVOKE on
  datasources, schemas, and tables; enforcement at BIND time (an unauthorized
  reference raises a bind error through the one resolver path); SHOW GRANTS;
  persisted alongside the dynamic catalog in the stats SQLite. This depends on
  the v1 persistence and gets a full design doc under ticket 125; it is out of
  scope here beyond noting that the `updated_by` column and the single
  bind-time resolver are the hooks it will build on.

## 9. Trino: what we copy and what we reject

COPIED:

- RUNTIME DDL for catalog lifecycle (`CREATE CATALOG` / `DROP CATALOG` ->
  `CREATE DATASOURCE` / `DROP DATASOURCE`) with an on-disk store that survives
  restart. This is the whole feature; Trino validates the shape.
- `WITH ( key = 'value' )` property syntax with all values as strings - it maps
  cleanly onto our heterogeneous per-kind params and onto the JSON params blob.
- DROP DOES NOT INTERRUPT RUNNING QUERIES. Trino's rule; ours is the same and
  stronger (a peer session keeps the source until reconnect), which falls out of
  per-session catalog snapshots for free.
- A DEDICATED MANAGEMENT MODE. Trino gates dynamic catalogs behind
  `catalog.management=dynamic`. We do not need a global mode (the feature is
  always available when a stats SQLite exists), but we take the lesson that the
  capability is explicit and store-backed.

REJECTED, with reasons:

- CREDENTIALS IN THE LOGGED STATEMENT. Trino's documented caveat is that the
  full `CREATE CATALOG` text, passwords included, is logged and shown in the Web
  UI. We reject this outright: fail-closed redaction from day one (section 5) and
  a phase-2 secret-reference model so the secret is never in the statement at
  all. This is the single biggest thing NOT to copy.
- `${ENV:VAR}` INLINE-IN-VALUE INTERPOLATION. Trino resolves env vars by string
  substitution inside a property value. We instead make a secret a typed
  reference object in the stored params (section 5), so the reference is
  explicit, redaction knows a value is a secret, and resolution is at connect
  time - not a string rewrite that can accidentally interpolate a
  non-secret value.
- CROSS-WORKER DISTRIBUTION. Trino's coordinator pushes catalog config to
  workers as they join. We have no coordinator/worker split; a datasource is
  registered into one runtime and persisted for future runtimes. The
  distribution problem does not exist here, so we do not build for it.
- SILENT-ISH DROP PRUNING and CONNECTOR RESOURCE LEAKS. Trino prunes dropped
  catalogs on a timer (default 5s) and documents that several connectors do not
  fully release resources on drop. Our DROP is synchronous within the dropping
  runtime (exec-plane prune of `(session, name)`), and `Runtime::drop` already
  prunes every source a session opened, so there is no timer and no leak path to
  inherit.
- ALL-VALUES-ARE-STRINGS taken to the extreme. We keep Trino's string-valued
  WITH surface but accept a bare integer for numeric params for ergonomics
  (`port = 5432`), matching how YAML already accepts `port: 5432`.

## 10. Non-goals for v1

- No OR REPLACE, no ALTER DATASOURCE: a repoint is DROP + CREATE (section 1).
- No secret references yet: credentials are plaintext-in-catalog but redacted
  everywhere (phase v2 / ticket 124).
- No ACLs: any connection that can issue DDL can create/drop a source (phase v3
  / ticket 125).
- No live propagation to EXISTING peer sessions: a peer sees a new/dropped
  source on reconnect (section 3), like SET and MV DDL.
- No new source KIND: the five existing kinds only (duckdb / postgres /
  clickhouse / mysql / parquet); a sixth kind is a connector task, not a
  dynamic-catalog task.
- No cross-config sharing: the dynamic catalog is per stats SQLite (per config),
  the same one-store-per-config boundary the accelerator and learned stats keep.
