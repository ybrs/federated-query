# ACL and user system plan - CREATE/ALTER/DROP USER, GRANT/REVOKE, bind-time enforcement

Identity and access control become manageable at runtime through SQL DDL:
`CREATE USER <name> WITH PASSWORD '...'`, `ALTER USER`, `DROP USER`, `GRANT
SELECT ON DATASOURCE|SCHEMA|TABLE ... TO <grantee>`, `REVOKE ...`, `SHOW USERS`,
`SHOW GRANTS`. Users authenticate to `fedq-server` over SCRAM-SHA-256 (pgwire).
Every base-table reference is authorized at BIND time through the single resolver
(`fq_bind::resolve_scan_table`), so an unauthorized reference raises a bind error
and never returns a row. Passwords are NEVER stored: the store holds a
PostgreSQL-`pg_authid`-shaped SCRAM VERIFIER (salt, iteration count, StoredKey,
ServerKey). Users and grants persist in the same stats SQLite that already holds
`dynamic_datasources` and `materialized_views`, opened by the accelerator's
durable (`synchronous=ON`) connection.

This follows the dynamic-catalog family - a lexical DDL classifier
(`fq-parse/src/statement.rs`), persisted registry rows in
`<config-stem>.stats.sqlite` (`fq-accel/src/catalog.rs`), a `Runtime` handler
module (mirroring `dynamic_catalog.rs`) - with ONE deliberate divergence: grant
visibility is NOT a frozen per-session snapshot, because privilege staleness
EXTENDS access (section 8).

## 0. What already exists (the machinery this reuses)

- SCRAM AUTH, PARTIAL. `fedq-server` already speaks SCRAM-SHA-256
  (`crates/fedq-server/src/auth.rs`, `handler.rs`). `ConfigAuthSource` implements
  pgwire's `AuthSource`; `get_password` returns the configured user's salt and a
  stored secret. Users live in `config.server.users`
  (`fq_common::ServerConfig { users: Vec<UserCredential> }`,
  `UserCredential { name, salt, salted_password }`,
  `config.rs:244-265`). An empty list = trust auth (every connection accepted);
  a non-empty list = SCRAM required (`handler.rs::build_startup`). The
  `hash-password <user> <pw>` subcommand (`main.rs`) derives the stored secret so
  the plaintext never enters the config. THE ISSUE this plan fixes: what is stored
  today is the SCRAM SaltedPassword, the master secret from which client
  authentication can be forged (section 1).
- STATEMENT CLASSIFIER. `classify_statement` (`fq-parse/src/statement.rs:107`) is
  a quote-aware lexical classifier keyed on the first (and often second) word,
  returning a `Statement<'a>` variant; unrecognized text falls through to
  `Statement::Query` and the normal parser. Datasource DDL was added here
  (`CREATE/DROP DATASOURCE`, `SHOW DATASOURCES`); ACL DDL joins the same match.
- DDL DISPATCH + HANDLER MODULE. `Runtime::execute` (`fq-runtime/src/lib.rs:192`)
  matches the classified `Statement` and dispatches to a `pub(crate)` handler;
  every handler returns `Result<(SchemaRef, Vec<RecordBatch>), RuntimeError>`
  (DDL returns a one-row `status` batch via `status_result`; SHOW builds a real
  multi-column batch). `Runtime::describe` (`lib.rs:288`) is the parallel match
  that resolves result columns without executing. `dynamic_catalog.rs` is the
  template ACL DDL copies; ACL handlers go in a new `acl.rs` module declared
  beside `mod dynamic_catalog;`.
- PERSISTED REGISTRY. `fq-accel/src/catalog.rs`'s `ViewCatalog` owns a
  `Mutex<rusqlite::Connection>` over `<config-stem>.stats.sqlite` with
  `PRAGMA journal_mode=WAL` and `synchronous` left ON (durable operator state,
  unlike the learned stats which run `synchronous=OFF`). It carries the
  `INSERT OR IGNORE` + rows-affected first-writer-wins pattern
  (`insert_datasource`), the open-time `pragma_table_info` column migration
  (`add_datasource_optional_columns`), and the busy-retry open (`with_busy_retry`,
  `BUSY_TIMEOUT=5s`). Users and grants become two more tables in this file, added
  to `ensure_schema` (`catalog.rs:411`). A `Clock` closure
  (`Arc<dyn Fn() -> String>`, RFC3339) stamps `created_at` at insert time.
- THE ONE RESOLVER. `fq_bind::Binder::resolve_scan_table`
  (`fq-bind/src/binder.rs:114`) is the SOLE function that turns a table reference
  into catalog metadata (the only caller of `Catalog::resolve_table`). Every
  base-table access - `bind_scan`, `scan_output_columns`, `collect_scope` -
  funnels through it. It raises `BindError::TableNotFound` for an unknown
  reference (`binder.rs:130`). This is the single privilege hook. The binder holds
  only `catalog: &Catalog` today (`binder.rs:37`); there is no principal.
  `fq_bind::bind(catalog, plan)` has ONE production caller:
  `Runtime::optimize` (`lib.rs:341`). EXPLAIN binds through the identical pipeline
  (`Runtime::explain` -> `plan` -> `optimize` -> `bind`), so enforcement is
  automatically consistent for EXPLAIN.
- SETTINGS REGISTRY. A static `SETTINGS: &[SettingDef]` slice
  (`fq-runtime/src/settings.rs:229`); each entry names a dotted key, a
  `Mutability` (Static / SessionMutable), a default, a `read(&Config)`, and an
  `apply(&mut Config, ...)`. A completeness test fails if a scalar config field
  is unregistered. The SCRAM iteration-count setting is registered here.
- PER-CONNECTION RUNTIME. `fedq-server` builds one `Session` per connection
  (`session.rs`), each owning a dedicated worker thread that builds its OWN
  `Runtime::from_config` and services jobs in order. There is no shared mutable
  catalog across connections. The connecting username is currently used ONLY
  inside the pgwire SASL handshake and then DISCARDED - `Session`/`Runtime` never
  learn who connected (`do_query` takes `_client`). Adding a principal is new
  plumbing (section 9).

## 1. Threat model and the SCRAM verifier (the non-negotiable)

REQUIREMENT (non-negotiable): no plaintext password anywhere, ever. Password
storage is a SCRAM-SHA-256 VERIFIER in the exact shape PostgreSQL stores in
`pg_authid.rolpassword`.

### Derivation (RFC 5802 / RFC 7677), what is stored, what is discarded

Given a plaintext `password`, a fresh 16-byte random `salt`, and an iteration
count `i` (>= 4096):

```
SaltedPassword = PBKDF2-HMAC-SHA-256(Normalize(password), salt, i)   [32 bytes]
ClientKey      = HMAC-SHA-256(SaltedPassword, "Client Key")
StoredKey      = SHA-256(ClientKey)
ServerKey      = HMAC-SHA-256(SaltedPassword, "Server Key")
```

STORED (the verifier): `salt`, `i`, `StoredKey`, `ServerKey`. Serialized as the
literal `pg_authid` string in one column:

```
SCRAM-SHA-256$<i>:<base64(salt)>$<base64(StoredKey)>:<base64(ServerKey)>
```

DISCARDED and never persisted: the plaintext, `SaltedPassword`, and `ClientKey`.
`Normalize` is SASLprep (already pulled in via pgwire's `stringprep`).

### Why a store compromise does not yield the password - and what it DOES yield

Stated plainly, because honesty here is the point:

- The plaintext is NOT recoverable. `StoredKey`/`ServerKey` are outputs of
  SHA-256 / HMAC over `SaltedPassword`, which is PBKDF2 over the password; all one-
  way. An attacker can only mount an OFFLINE dictionary/brute-force attack whose
  work factor is `i` PBKDF2 iterations per guess. `i` (section 9) is that work
  factor.
- Client authentication is NOT directly forgeable. To authenticate, a client must
  send `ClientProof = ClientKey XOR HMAC(StoredKey, AuthMessage)`. The attacker
  has `StoredKey` (so can compute the HMAC term) but needs `ClientKey`, and
  `StoredKey = SHA-256(ClientKey)` is preimage-resistant. So a stolen verifier
  does NOT let the attacker log in as the user (short of the offline attack).
- What a stolen verifier DOES yield, honestly: (a) `ServerKey` lets the attacker
  forge a valid `ServerSignature`, i.e. IMPERSONATE THE SERVER to a client (a MITM
  can pose as the real fedq-server); (b) `StoredKey` lets the attacker VERIFY
  proofs (act as an authenticator for that user); (c) the offline attack above.
  It does NOT yield live client-auth capability against a third-party server.

CONTRAST with what `fedq-server` stores TODAY (`SaltedPassword`, via pgwire's
`Password`): `SaltedPassword` IS the master secret. From it,
`ClientKey = HMAC(SaltedPassword, "Client Key")` is trivially derived, so a
compromise of today's store DOES yield full client authentication. Storing the
verifier instead of `SaltedPassword` is STRICTLY stronger, and is exactly why
PostgreSQL stores the verifier and not `SaltedPassword`. This plan replaces the
storage.

### The pgwire tension (the biggest engineering decision - see also section 15)

pgwire 0.40.4's stock SCRAM server (`ScramAuth` /
`pgwire::api::auth::sasl::scram`) verifies the client proof by RECOMPUTING from
`SaltedPassword`: its `AuthSource::get_password` returns a `Password` whose bytes
are `SaltedPassword`, and `compute_client_proof` does
`client_key = HMAC(salted_password, "Client Key")`. It has NO path that verifies
from `StoredKey`/`ServerKey`. Therefore storing the verifier the maintainer
requires is INCOMPATIBLE with pgwire's stock `ScramAuth`.

DECIDED: vendor a small RFC-5802-correct SCRAM server SASL `StartupHandler` in
`fedq-server` that verifies from the stored `StoredKey` + `ServerKey` (recover
`ClientKey` from the proof, check `SHA-256(ClientKey) == StoredKey`, sign with
`ServerKey`). This is the standard server-side SCRAM verification real PostgreSQL
performs. It reuses pgwire's public SASL wire messages (`Authentication::SASL`,
`SASLContinue`, `SASLFinal`, `PasswordMessageFamily`) and the `ring` primitives
already linked; only the ~5 small SCRAM message parsers (private in pgwire) and
the proof-verification math are reimplemented (~250-350 lines). Rationale: the
project already vendors focused dependency patches (the DuckDB streaming vtab
fork) when a stock crate cannot express a required behavior; storing the weaker
`SaltedPassword` to keep pgwire stock would violate a non-negotiable and ship a
genuinely weaker credential store. This is flagged for maintainer blessing in
section 15 as it is the largest single build item.

## 2. SQL surface v1

Lexical classifier arms next to the datasource forms
(`classify_statement`, `statement.rs`). Every unsupported option raises
`ParseError::Unsupported` naming it - never a silent partial accept, matching the
datasource/MV DDL contract.

```
CREATE USER <name> WITH PASSWORD '<plaintext>' [SUPERUSER]
ALTER  USER <name> WITH PASSWORD '<plaintext>'
ALTER  USER <name> WITH [SUPERUSER | NOSUPERUSER]
DROP   USER <name>

GRANT  SELECT ON DATASOURCE <ds>            TO <grantee>
GRANT  SELECT ON SCHEMA     <ds>.<schema>   TO <grantee>
GRANT  SELECT ON TABLE      <ds>.<schema>.<table> TO <grantee>
REVOKE SELECT ON DATASOURCE <ds>            FROM <grantee>
REVOKE ... (SCHEMA / TABLE forms mirror GRANT)

SHOW USERS
SHOW GRANTS [FOR <grantee>]
```

- `<grantee>` is a user name or the reserved pseudo-grantee `PUBLIC` (section 3).
- The plaintext password in `CREATE/ALTER USER` is hashed to a verifier IN THE
  RUNTIME (server-side); it is spliced through the extended-protocol `$n` path
  like any literal but is NEVER logged or echoed (section 10). The verifier
  derivation (section 1) runs on the worker thread; the plaintext lives only for
  the duration of the call frame.
- `WITH PASSWORD` value: a single-quoted string literal (the only accepted form);
  an empty password raises.
- Object path grammar: `DATASOURCE` takes one identifier; `SCHEMA` takes a two-
  part dotted `<ds>.<schema>`; `TABLE` takes a three-part `<ds>.<schema>.<table>`.
  A wrong arity for the level raises at classify. Bare-word identifiers lowercase
  (Postgres rule); a `"Quoted"` identifier keeps its spelling, reusing
  `statement::view_name`.
- `IF EXISTS` / `IF NOT EXISTS`: REJECTED on all USER forms (a silent no-op hides
  a typo or a name collision - identical reasoning to the datasource DDL).
- `CASCADE` on `DROP USER`: REJECTED. Dependency handling (a user's grants) is
  decided by the engine (grants are removed with the user, section 7), not by a
  statement option.
- Privileges other than `SELECT`, and grantee lists / multiple objects per
  statement: REJECTED in v1 (one privilege, one object, one grantee per
  statement). Rationale: the engine is read-only, so `SELECT` is the only data
  privilege that exists (section 4); multi-target grants are pure sugar deferred
  to v2.

### SHOW output

- `SHOW USERS`: columns `name TEXT, superuser TEXT, created TEXT`. NEVER a verifier
  field (section 10).
- `SHOW GRANTS [FOR <grantee>]`: columns
  `grantee TEXT, privilege TEXT, object_kind TEXT, object_path TEXT,
   granted_by TEXT, created TEXT`. With no `FOR`, a superuser sees all grants; a
  non-superuser sees only their own and `PUBLIC`'s (a user may inspect their own
  access, never others'). `SHOW USERS` / `SHOW GRANTS FOR <other>` require
  superuser.

## 3. Identity model v1

- USERS ONLY + A SUPERUSER FLAG. A user is `{name, verifier, superuser:bool}`. No
  roles, no groups. DEFERRED explicitly: roles/groups (`CREATE ROLE`, role
  membership) are v2 - they add a membership-closure evaluation to every grant
  check and a second grantee namespace, neither of which v1 needs.
- PUBLIC. DECIDED: include `PUBLIC` as a reserved pseudo-grantee. `GRANT SELECT ...
  TO PUBLIC` authorizes every authenticated user; a grant check tests the user's
  own grants OR `PUBLIC`'s. Rationale: it is one reserved name, and it is the
  natural, auditable way to make a datasource world-readable without an explicit
  grant per user. `PUBLIC` cannot be created, altered, dropped, or logged into
  (it has no verifier); using it as a USER name raises.
- WHO MAY RUN USER/GRANT DDL: superuser only, in v1. `CREATE/ALTER/DROP USER`,
  `GRANT`, `REVOKE`, and `SHOW USERS` are superuser-gated at dispatch (section 6).
  A non-superuser issuing them gets an explicit permission error (a capability
  check names no object whose existence could leak - section 6).

## 4. Privilege set and object hierarchy

- V1 GRANTABLE PRIVILEGE = `{SELECT}` only. The engine is READ-ONLY over sources
  (no INSERT/UPDATE/DELETE/DDL against source tables exist), so `SELECT` is the
  only meaningful data privilege. Reserving the word `SELECT` (rather than an
  implicit "read") keeps the surface Postgres-shaped for a later `USAGE`/`INSERT`
  expansion without a grammar change.
- SYSTEM CAPABILITIES ARE NOT GRANTABLE IN V1; they are the SUPERUSER FLAG. All
  server-mutating DDL - `CREATE/DROP DATASOURCE`, `CREATE/REFRESH/DROP
  MATERIALIZED VIEW` and user/grant DDL -
  requires superuser. Rationale: inventing grantable system privileges
  (`CREATE DATASOURCE` as a per-user right) in v1 multiplies the privilege space
  for a feature only an operator uses; a single superuser flag covers every admin
  operation cleanly and is trivially auditable. Making these individually
  grantable is a named v2 item.
- `SET` NEEDS NO PRIVILEGE. A `SET` of a `SessionMutable` setting affects only the
  caller's own session `Config` snapshot (no cross-user effect), so any user may
  `SET`. A `Static` (protected) setting already raises on `SET` for everyone
  (`Mutability::Static`), so there is nothing extra to gate. This is a decision:
  session-local settings are not a privilege boundary.
- OBJECT HIERARCHY - CONTAINMENT SEMANTICS. A grant at a higher level implies all
  objects beneath it:
  - `GRANT SELECT ON DATASOURCE d` authorizes every schema and table under `d`.
  - `GRANT SELECT ON SCHEMA d.s` authorizes every table under `d.s`.
  - `GRANT SELECT ON TABLE d.s.t` authorizes exactly that table.
  A table reference is authorized iff a covering grant exists for the user OR
  `PUBLIC` at ANY of the three levels of that table's fully-qualified path. There
  is NO wildcard syntax; the LEVEL is the wildcard (a datasource-level grant is
  the "everything under d" wildcard). This matches natural containment and the
  ticket's "datasource grant implies schemas/tables under it".
- The authorizing path is the table's REAL qualifier. A bare reference
  (`FROM users`) arrives with empty datasource/schema; the true owner is known
  only AFTER `Catalog::resolve_table` returns the `Table`, via
  `table.qualifier() -> TableQualifier { datasource, schema_name }`
  (`fq-catalog/src/schema.rs:106`). The grant check reads the RESOLVED qualifier,
  never the raw scan fields, so a bare reference is authorized against its actual
  datasource/schema/table (section 6).

## 5. Bootstrap and the first superuser

- SOURCE OF THE FIRST SUPERUSER: the config `server.users` section. Each config
  user carries the verifier (reshaped, below) plus an optional `superuser: true`.
  `fedq-server hash-password [--superuser] <user> <pw>` emits the YAML block with
  the verifier; the operator pastes it under `server.users`. At first
  `Runtime::from_config` over a store that has no `users` rows, the config users
  are IMPORTED into the persisted `users` table (a bootstrap merge exactly like
  the dynamic-datasource bootstrap merge - read the config list, write missing
  rows), stamped `created_by = 'bootstrap-import'`. Thereafter the persisted table
  is authoritative and the config `server.users` is the bootstrap seed only.
- CONFIG SHAPE MIGRATION. `UserCredential { name, salt, salted_password }` becomes
  `UserCredential { name, verifier, superuser }` where `verifier` is the
  `pg_authid` string (section 1). Per the no-compat-cruft rule the
  `salted_password` field is REPLACED, not shimmed. A one-time forward
  computation exists for any operator holding the old form: `StoredKey`/`ServerKey`
  ARE derivable from a stored `salted_password` (`StoredKey =
  SHA-256(HMAC(salted_password, "Client Key"))`, `ServerKey =
  HMAC(salted_password, "Server Key")`), so `hash-password` gains an
  `upgrade-credential` mode that rewrites an old block into a verifier block
  without the plaintext. Going forward `hash-password` emits ONLY the verifier;
  `salted_password` is never written again.
- LOUD SAFE DEFAULT - NO SILENT OPEN SERVER. If auth is ENABLED (`server.users`
  non-empty) but NO user is a superuser, `fedq-server` REFUSES TO START, naming
  the problem ("authentication is enabled but no user is a superuser; no principal
  could administer grants"). First-run interactive prompting is not an option for
  a server; the safe default is a loud refusal that forces the operator to define
  an administrator. A fresh store with an EMPTY config user list stays in trust
  mode (below).
- TRUST MODE = NO ENFORCEMENT. When `server.users` is empty (the current default),
  the server is in trust mode: every connection is accepted and runs as an
  IMPLICIT SUPERUSER, and bind-time grant checks are bypassed entirely. This
  preserves today's behavior for a single-tenant deployment and makes ACL
  enforcement an opt-in that turns on exactly when authentication does. A server
  cannot be half-secured: enforcement is on iff auth is on.
- PASSWORD-CHANGE / DROP effects on live sessions: section 8.

## 6. Enforcement point

Two enforcement layers, both loud.

### Bind-time data authorization (the one resolver)

The privilege check hooks `resolve_scan_table` (`fq-bind/src/binder.rs:114`),
AFTER `Catalog::resolve_table` succeeds (so the real `table.qualifier()` is
known) and BEFORE the `Table` is returned. It asks the authorizer: does the
current principal (or `PUBLIC`) hold `SELECT` at the table's datasource, schema,
or table level? If not, it raises. Because every base-table access funnels
through this one function (verified: it is the sole caller of
`catalog.resolve_table`), and columns resolve only against already-in-scope
tables (a denied table's columns never enter any scope), this single hook covers
tables, schemas, datasources, EXPLAIN, and column exposure with no second hook.

PLUMBING. The binder gains a principal + an authorizer handle. `Binder::new`,
the `fq_bind::bind` free function, and its one production call site
(`Runtime::optimize`, `lib.rs:341`) take a `&Principal` and a grants reader. The
`Runtime` gains a `principal: Principal { name, superuser }` field set at
construction from the authenticated identity (section 9); trust-mode / embedded
builds an implicit-superuser principal. A superuser principal short-circuits the
check (always authorized), so a superuser's queries pay nothing.

### Existence-leak decision

DECIDED: an unauthorized reference is INDISTINGUISHABLE from a nonexistent one at
the client. The unauthorized case raises `BindError::TableNotFound` with the SAME
message shape as a genuinely missing table ("Table not found:
<ds>.<schema>.<table>"). Rationale: returning a distinct "access denied" for a
table that EXISTS but is not granted, versus "not found" for one that does not
exist, is an existence oracle an unauthorized user could probe to enumerate the
catalog. The engine chooses non-leak; this is a clean one-function decision
because the hook is the single resolver. This does NOT violate the invalid-query
law: the reference is REFUSED (a `BindError` is raised, no rows are ever
returned) - only the WORDING withholds existence. The real reason is preserved
for the operator server-side (the denied-access audit line, section 10) and via a
superuser's `SHOW GRANTS`, so we never lie to the operator, only to the
unauthorized client. A new `BindError` variant is NOT added for the deny path (it
reuses `TableNotFound`) precisely so the two cases cannot be told apart; the
audit layer, not the error type, carries the distinction.

### Statement-level DDL authorization (at dispatch)

The superuser-gated statements (section 3/4) are checked in `Runtime::execute`
BEFORE the handler runs. A non-superuser gets an EXPLICIT permission error
("permission denied: CREATE DATASOURCE requires superuser") - here an explicit
message is correct, because a capability check names no object whose existence
could leak (the statement kind is not a secret). This layer is separate from the
bind-time data check and does not touch the resolver.

### EXPLAIN

`EXPLAIN <query>` binds through the identical pipeline (`Runtime::explain` ->
`plan` -> `optimize` -> `fq_bind::bind`), so EXPLAIN of an unauthorized table hits
the same resolver and raises the same non-leaking not-found - a plan (and its
pushed source SQL) is NEVER revealed for a table the user cannot query. EXPLAIN of
an AUTHORIZED table reveals its pushed SQL as normal (that is the user's own
data). `EXPLAIN` of a non-query (DDL/SET) never binds and is governed by the
dispatch-level check.

## 7. Persistence schema

Two tables in the accelerator's `ViewCatalog` connection (durable,
`synchronous=ON`), created in `ensure_schema` (`catalog.rs:411`) and migrated by
the `pragma_table_info` pattern (`add_*_optional_columns`). NOT in the
`StatsCatalog` connection, which runs `synchronous=OFF` (correctness-neutral
learned stats); authz is durable state.

```sql
users (
  name        TEXT PRIMARY KEY,   -- lowercased unless quoted at create
  verifier    TEXT NOT NULL,      -- pg_authid string: SCRAM-SHA-256$i:salt$storedkey:serverkey
  superuser   INTEGER NOT NULL DEFAULT 0,
  created_at  TEXT NOT NULL,      -- RFC3339, stamped by the Clock
  created_by  TEXT                -- creating principal, or 'bootstrap-import'
)

grants (
  grantee     TEXT NOT NULL,      -- a user name or 'PUBLIC'
  privilege   TEXT NOT NULL,      -- 'SELECT' in v1
  object_kind TEXT NOT NULL,      -- 'datasource' | 'schema' | 'table'
  object_path TEXT NOT NULL,      -- 'd' | 'd.s' | 'd.s.t' (canonical lowercased/quoted form)
  granted_by  TEXT NOT NULL,      -- the superuser principal that ran GRANT
  created_at  TEXT NOT NULL,      -- RFC3339
  PRIMARY KEY (grantee, privilege, object_kind, object_path)
)

acl_meta (
  key         TEXT PRIMARY KEY,   -- single row: key = 'generation'
  value       INTEGER NOT NULL    -- monotonic generation counter (section 8)
)
```

- INDEX. `grants` gets an index on `(grantee, object_path)` for the bind-time
  lookup; the check queries the three candidate paths for `{user, PUBLIC}`.
- USER UNIQUENESS = `name` PK; a `CREATE USER` of an existing name is
  `INSERT OR IGNORE` + rows-affected == 0 -> raise `already exists` (the datasource
  loser-raises shape). `DROP USER` deletes the user row AND every `grants` row
  where `grantee = name`, in one transaction (a dropped user leaves no dangling
  grant).
- GRANT UNIQUENESS = the composite PK. `GRANT` is IDEMPOTENT: `INSERT OR IGNORE`,
  success whether or not the row already existed - a re-GRANT is a no-op success,
  matching Postgres (a grant is an additive, revocable right, not a namespace
  claim, so re-asserting it is not an error). `REVOKE` of a grant that matches NO
  row RAISES ("no such grant to revoke: SELECT ON TABLE d.s.t FROM u") - a revoke
  that silently does nothing hides a typo, consistent with the codebase's loud
  DROP stance.
- MIGRATION. `ensure_schema` gains `conn.execute(USERS_SCHEMA)`,
  `conn.execute(GRANTS_SCHEMA)`, `conn.execute(ACL_META_SCHEMA)` and a
  `pragma_table_info` add-column pass for each, wrapped by the existing
  `with_busy_retry(|| ensure_schema(&conn))` at open (no new retry wrapping).
- PASSWORD CHANGE INVALIDATES: only the STORED VERIFIER. `ALTER USER ... WITH
  PASSWORD` overwrites the `verifier`. It does NOT invalidate any EXISTING
  authenticated connection (authentication happened at that connection's
  handshake; the engine does not re-authenticate mid-session). New connections use
  the new verifier. `DROP USER` cuts a live session off at its NEXT query, because
  grant checks read the live store (section 8): a dropped user's next bind finds
  no grants and is denied.

## 8. Visibility semantics across live sessions (the hard call)

This is the ONE place this plan deviates from the dynamic-catalog per-session-
snapshot philosophy, and it deviates deliberately.

THE PROBLEM. Dynamic datasources use a frozen per-session catalog snapshot: a
peer sees a `CREATE`/`DROP DATASOURCE` only on reconnect, and that is safe because
a datasource is a namespace - a stale peer keeps using a source that still works,
and a peer that does not name the new source is unaffected. Privileges are
different. A `REVOKE` that a connected peer does not see until reconnect means the
peer RETAINS access it was supposed to lose, for the entire life of its connection
(possibly hours). Staleness here EXTENDS ACCESS - it is a security regression, the
exact opposite of the benign datasource case. A frozen snapshot for grants is
therefore NOT acceptable.

DECIDED: grant and user state is read LIVE, not from a frozen snapshot, gated by a
cheap per-query generation check.

- The `grants`/`users` tables live in the shared `<config-stem>.stats.sqlite`
  (WAL, many readers + one writer). Each connection's `Runtime` has its own
  connection over the SAME file, so a `GRANT`/`REVOKE`/`ALTER USER`/`DROP USER`
  committed by any connection is immediately visible to every other connection's
  reads (WAL read-committed). There is no cross-process propagation to build - the
  shared file IS the propagation.
- To avoid a SQLite hit per referenced table per query, each `Runtime` caches its
  principal's effective grant set in memory, gated by the `acl_meta.generation`
  counter. Every user/grant DDL write INCREMENTS `generation` in the SAME
  transaction as the write. At bind time the `Runtime` reads the current
  generation (ONE indexed single-row `SELECT` - microseconds on a WAL reader); if
  it equals the cached generation, the in-memory grant set is used with no further
  I/O; if it changed, the principal's grants are reloaded once and the cache
  updated. Cost per query is therefore exactly ONE `O(1)` integer read regardless
  of how many tables the query names, plus an occasional reload only when
  privileges actually changed.
- BUDGET ADJUDICATION. Rule 1 (planning is O(metadata), with a 100ms budget) is
  not violated: the generation read is `O(1)` and reads the ACL catalog, not data;
  a reload reads a handful of the principal's grant rows (`O(grants-for-user)`,
  bounded and tiny), not source data. The maintainer flagged this as "the one
  place the O(metadata) budget argument may lose to security" - it does not even
  lose, because the security-correct read here is cheaper than a single stats
  fetch the planner already performs. Even a per-table direct grant `SELECT`
  (skipping the cache entirely) would be a few indexed point-reads well inside
  budget; the generation cache is a further optimization, not a necessity.
- CONSEQUENCE, stated as the semantics: a `REVOKE`, a `GRANT`, an
  `ALTER USER ... SUPERUSER`, and a `DROP USER` take effect on EVERY live session
  at that session's NEXT query (bounded by one generation read), NOT on reconnect.
  This is the safe direction for all four: a revoke removes access promptly, a
  superuser downgrade demotes promptly, a dropped user is denied promptly. The
  principal's `superuser` flag is refreshed from `users` on the same generation
  bump, so an in-flight session cannot keep administering after its superuser bit
  is cleared.
- PASSWORD CHANGES are the exception that needs no live propagation: the verifier
  is consulted ONLY at a new connection's handshake, so an `ALTER USER ... WITH
  PASSWORD` naturally affects only future connections and an already-authenticated
  session is unaffected. This is intentional and stated (section 7): we do not
  forcibly disconnect a live session on a password change; forced disconnect is a
  named v2 item if operators need it.
- WHAT WE DO NOT DO IN V1: forcibly terminate a live session on `DROP USER` /
  `REVOKE`. The live grant check already denies that session's next QUERY, which
  is the access that matters; killing the socket mid-query is a v2 refinement
  (it requires reaching across the per-connection worker boundary). The window a
  revoked user keeps is "until their current in-flight query finishes", not "until
  reconnect" - the same bounded window a single statement takes.

## 9. SCRAM integration - exact fedq-server changes

- AUTH SOURCE BY USERNAME FROM THE STORE. Replace `ConfigAuthSource` (which reads
  `config.server.users` and returns `SaltedPassword`) with a `StoreAuthSource`
  that looks up the verifier by username from the persisted `users` table (the
  same store the `Runtime` uses; the config users are the bootstrap seed already
  imported at first construction, section 5). It returns the parsed
  `StoredKey`/`ServerKey`/`salt`/`iterations`.
- VENDORED VERIFICATION (section 1). Because pgwire's stock `ScramAuth` cannot
  verify from a verifier, `fedq-server` runs its own SCRAM SASL `StartupHandler`:
  advertise `SCRAM-SHA-256`; on client-first, look up the verifier and send
  server-first carrying `salt` + the USER'S stored iteration count; on client-
  final, recover `ClientKey = ClientProof XOR HMAC(StoredKey, AuthMessage)`, check
  `SHA-256(ClientKey) == StoredKey`, and reply with
  `ServerSignature = HMAC(ServerKey, AuthMessage)`. On success the connection's
  authenticated username is captured and threaded into `Session::spawn` ->
  `Runtime::from_config` as the `Principal` (the plumbing that does not exist
  today - section 0); the `Runtime` resolves the principal's `superuser` flag from
  the `users` table at construction (authoritative, never trusted from the wire).
- UNKNOWN USER WITHOUT AN ENUMERATION / TIMING ORACLE (the mock-verifier trick).
  When the username is unknown, the handler MUST NOT fast-reject (a fast path is a
  timing oracle and reveals nonexistence). Instead it synthesizes a DETERMINISTIC
  mock verifier from the username and a server-wide secret
  (`salt = HMAC(server_mock_secret, username)` truncated; keys derived likewise)
  and runs the FULL handshake against it. The proof check fails at the end exactly
  as a wrong password does, on the same code path, with the same
  `InvalidPassword(username)` error and the same timing. An attacker cannot
  distinguish "no such user" from "wrong password". This mirrors PostgreSQL's
  `scram_mock_salt`. The `server_mock_secret` is a random per-server value
  generated at startup (never persisted to a client-visible place).
- ITERATION COUNT SETTING. Register `server.scram_iterations` (default 4096,
  `Mutability::Static`, MINIMUM 4096 enforced at load - RFC 7677 floor). It is the
  work factor for NEW verifiers derived by `CREATE/ALTER USER` and `hash-password`.
  The iteration count is stored PER USER inside the verifier and advertised per-
  handshake from the user's stored value, so raising the server default does not
  break existing users and users can carry different counts. This is strictly more
  correct than today's single global 4096.
- CHANNEL BINDING / TLS STANCE. v1 advertises `SCRAM-SHA-256` only (no
  `-PLUS`), because `fedq-server` runs plaintext TCP (`process_socket(socket,
  None, ...)` - the `None` is the TLS slot) and channel binding requires a server
  certificate. Stated honestly: v1 SCRAM prevents password DISCLOSURE and offline
  reuse of the wire exchange, but a plaintext transport still exposes QUERY DATA
  and is MITM-able; SCRAM alone is not transport security. TLS termination (which
  also unlocks `SCRAM-SHA-256-PLUS` channel binding, already supported by
  pgwire's `configure_certificate`) is a named near-term follow-on, out of v1
  scope but explicitly recommended before any untrusted-network deployment.

## 10. Redaction and audit

- FAIL-CLOSED REDACTION. `SHOW USERS` emits an allowlist of display-safe columns
  (`name`, `superuser`, `created`) and NEVER the `verifier` column or any part of
  it. The verifier is not a plaintext password but is sensitive (enables the
  offline attack and server impersonation, section 1), so it is treated as a
  secret everywhere: not in `SHOW`, not in `EXPLAIN` (plans carry table/source
  names, never credentials - a guard test greps EXPLAIN output for any verifier
  substring), not in error text (a failed auth names the user, never the
  credential - already true in the pgwire path), not in logs. `Config`'s derived
  `Debug` must not print the verifier: `UserCredential` gets a custom `Debug` that
  renders `verifier: <redacted>` (today `Config` derives `Debug` and would print
  connection secrets verbatim - the same fail-closed treatment is applied). The
  plaintext from `CREATE/ALTER USER ... WITH PASSWORD` is redacted from any
  statement logging by the same allowlist that the datasource DDL uses for
  connection params.
- V1 AUDIT = SERVER-LOG LINES, NOT A QUERYABLE TABLE. DECIDED: v1 writes
  structured audit LINES to the server log (off the client path) for: auth events
  (login success / failure, with the username and outcome, never the credential);
  DDL events (`CREATE/ALTER/DROP USER`, `GRANT`, `REVOKE`, with actor + target +
  object, never the password); and DENIED bind-time accesses (the actor, the real
  object, and "denied SELECT"). The denied-access line is what closes the
  operator-debuggability gap the non-leak decision (section 6) opens: the client
  sees a non-leaking not-found, but the operator's log records the true "user X
  denied SELECT on d.s.t". A PERSISTED, QUERYABLE audit table with retention is a
  named v2 item - v1's log lines give the operator the record without building a
  retention/rotation subsystem.

## 11. Concurrency

- CONCURRENT USER/GRANT DDL. Writes go through `ViewCatalog`'s
  `Mutex<Connection>` within a process and SQLite's single-writer WAL across
  connections, with the busy-retry already in place. `CREATE USER` /
  `GRANT` use `INSERT OR IGNORE` + rows-affected (first-writer-wins for users;
  idempotent for grants). Each DDL write and its `acl_meta.generation` increment
  are ONE transaction, so a peer never observes a grant change without the matching
  generation bump (the invariant the section-8 cache relies on).
- CONCURRENT AUTH + DDL. A `StoreAuthSource` verifier lookup during a handshake is
  a WAL read concurrent with a `GRANT` write - safe (read-committed). A verifier
  updated by `ALTER USER ... WITH PASSWORD` mid-handshake is a benign race: the
  handshake used whichever committed verifier its read saw; the next handshake
  sees the new one.
- BIND-TIME READ VS DDL WRITE. The generation read + grant reload (section 8) is a
  WAL read concurrent with any DDL write; the worst case is a bind that reloads
  grants one query later than a concurrent commit, which is exactly the "next
  query" semantics stated. No lock is held across planning.

## 12. Embedded vs server boundary (honest)

DECIDED: for embedded `fedq-py` / the CLI, the default identity is SUPERUSER and
ACL enforcement is effectively off; enforcement lives primarily in `fedq-server`.

Rationale, stated plainly: an embedded process OWNS the config file and the stats
SQLite on the local filesystem. It can read every verifier, rewrite the `grants`
table, or point at a different store - so enforcing ACLs against the process that
owns the files provides NO real security boundary; the actual boundary is OS file
permissions on the config and the SQLite. Pretending otherwise would be security
theater. The embedded principal is therefore an implicit superuser: it can still
RUN user/grant DDL (to administer the store that `fedq-server` will enforce
against remote clients), and `SHOW USERS`/`SHOW GRANTS` work, but a bind-time
grant check never denies it. The security perimeter is the wire server: a remote
client authenticates over SCRAM and is enforced; a local process with file access
is trusted by construction. This boundary is documented, not hidden.

## 13. Phases with gates

Each phase stays behind the TPC-DS / TPC-H correctness harness and
`benchmarks/perf_compare`. The ACL path is inert in trust mode (no users
configured), so the OFF path must be byte-identical to today - that identity is
itself a gate.

- PHASE A1 - VERIFIER STORAGE + VENDORED SCRAM (no enforcement yet).
  Scope: reshape `UserCredential` to the verifier; `hash-password` emits the
  verifier and gains `upgrade-credential`; the vendored SCRAM `StartupHandler`
  verifying from `StoredKey`/`ServerKey`; the mock-verifier unknown-user path; the
  `server.scram_iterations` setting; capture the authenticated username into a
  `Principal` on the `Runtime`.
  Test strategy: a login round-trip against the vendored handler with a correct
  password succeeds and with a wrong password fails `INVALID_PASSWORD`; an unknown
  user fails identically (assert same SqlState AND indistinguishable path); a
  verifier round-trips (derive -> store string -> parse -> verify); a guard test
  greps logs/EXPLAIN/SHOW for any verifier or plaintext substring (clean);
  `upgrade-credential` turns an old `salted_password` block into a verifier that
  authenticates.
  Gate: TPC-DS/TPC-H unchanged (trust path identical); real Postgres clients
  (`psql`, tokio-postgres) authenticate; no secret in any output.

- PHASE A2 - USER/GRANT DDL + PERSISTENCE.
  Scope: the classifier arms and `acl.rs` handlers for `CREATE/ALTER/DROP USER`,
  `GRANT`/`REVOKE`, `SHOW USERS`/`SHOW GRANTS`; the `users`/`grants`/`acl_meta`
  tables with the open-time migration; superuser-gated dispatch; config bootstrap
  import; refuse-start-without-superuser.
  Test strategy: statement-classification units (every form parses; every rejected
  option - IF [NOT] EXISTS, CASCADE, non-SELECT privilege, multi-target, wrong
  object arity, PUBLIC as a user - raises naming itself); persistence round-trip +
  open-migration (write old-shape table, reopen, read back); a concurrent
  `CREATE USER` loser raises `already exists`; a `REVOKE` of a nonexistent grant
  raises; `DROP USER` removes the user's grants; a non-superuser running DDL gets
  the explicit permission error; refuse-to-start with users-but-no-superuser.
  Gate: DDL round-trips across a fresh `Runtime` over the same store; superuser
  gating holds; redaction grep clean.

- PHASE A3 - BIND-TIME ENFORCEMENT + LIVE VISIBILITY.
  Scope: the `resolve_scan_table` hook; the principal + authorizer plumbing
  through `fq_bind::bind`; containment semantics across the three object levels +
  `PUBLIC`; the non-leaking not-found; EXPLAIN consistency; the generation-gated
  live grant cache; the server-log audit lines.
  Test strategy: a user with a table-level grant can query that table and is
  denied (as not-found) a sibling table; a datasource-level grant covers all
  tables under it; a `PUBLIC` grant authorizes every user; EXPLAIN of an
  unauthorized table raises identically to a query; an unauthorized reference and
  a nonexistent one produce the SAME client error (existence non-leak); a live
  `REVOKE` in one session denies the peer's NEXT query (assert the bounded-window
  semantics of section 8); a `DROP USER` denies a live session's next query;
  denied-access audit lines record the real object.
  Gate: enforcement correct for all three levels + PUBLIC; non-leak holds
  (unauthorized == not-found at the client); live revoke visible next query;
  perf_compare warm/cold totals unchanged with enforcement ON for a superuser
  (short-circuit) and within one O(1) generation read for a normal user; trust-
  mode path byte-identical to today.

## 14. PostgreSQL model - what we copy, what we defer

COPIED:
- The `pg_authid` SCRAM VERIFIER shape (`SCRAM-SHA-256$i:salt$storedkey:serverkey`)
  and RFC-5802 server-side verification from `StoredKey`/`ServerKey` - the whole
  point of section 1, and what makes a stolen store non-usable for client auth.
- The `scram_mock_salt` unknown-user trick (section 9) to defeat user enumeration
  and timing oracles.
- `PUBLIC` as a pseudo-grantee, and GRANT/REVOKE `ON` an object with containment
  (database/schema/table -> datasource/schema/table).
- A `SUPERUSER` flag that bypasses grant checks.
- GRANT idempotency (re-granting is not an error).

DEFERRED, explicitly (each a one-line v2+ item):
- ROLES / GROUPS and role membership (v1 is users only).
- GRANT OPTION (delegated granting) - v1 grants are superuser-issued only.
- GRANTABLE SYSTEM PRIVILEGES (`CREATE DATASOURCE` etc. as per-user rights) - v1
  gates all admin DDL behind the superuser flag.
- DEFAULT PRIVILEGES (`ALTER DEFAULT PRIVILEGES`) - no auto-grant on new objects.
- ROW-LEVEL and COLUMN-LEVEL security - v1 authorizes at table granularity (the
  column read-set is known at `bind_scan` if column-level is ever needed).
- A QUERYABLE, RETAINED AUDIT TABLE - v1 audits to the server log only.
- PER-SOURCE CREDENTIAL PASSTHROUGH (map a fedq user to a downstream source
  credential) - v1 uses the datasource's single configured credential for all
  users.
- TLS + `SCRAM-SHA-256-PLUS` channel binding, and FORCED DISCONNECT on
  `DROP USER`/`REVOKE`/password change (v1 denies at the next query, not mid-
  socket).
- `REVOKE` diverges from Postgres on one point by DECISION: Postgres warns on a
  no-op revoke; we RAISE (the codebase's loud-failure law).

## 15. Open questions for the maintainer (please adjudicate)

1. VENDORED SCRAM VERIFICATION (the big one). Honoring "store StoredKey+ServerKey,
   not SaltedPassword" REQUIRES not using pgwire 0.40's stock `ScramAuth`, because
   it can only verify from `SaltedPassword` (section 1, verified against the
   vendored pgwire source). This plan pins a vendored ~250-350-line RFC-5802 SCRAM
   server SASL handler in `fedq-server`. The alternatives are (a) store the weaker
   `SaltedPassword` and keep pgwire stock - REJECTED here as it violates the non-
   negotiable and is genuinely weaker; (b) upstream a verifier-based `AuthSource`
   to pgwire and wait - not viable to block on. Please confirm the vendor decision
   (it is the largest single build item and the one place we diverge from a stock
   dependency's auth path).
2. EXISTENCE NON-LEAK vs LOUD-FAILURE ETHOS. Section 6 makes an unauthorized
   reference indistinguishable from a nonexistent one at the client (a not-found
   message), with the true "denied" reason only in the server audit log. This is
   standard security practice but is a mild tension with the project's "fail
   loudly, name the cause" instinct toward the CLIENT. Confirm non-leak is
   preferred over an explicit client-side "access denied" (the latter is an
   enumeration oracle).
3. TRUST-MODE = NO ENFORCEMENT. Section 5 keeps today's behavior: no configured
   users -> every connection is an implicit superuser and grant checks are
   bypassed. This means a deployment must turn on auth to get ANY enforcement.
   Confirm this opt-in coupling (auth on iff enforcement on) rather than, say, a
   separate "enforcement even in trust mode" mode.
4. EMBEDDED = IMPLICIT SUPERUSER. Section 12 declares the embedded `fedq-py`/CLI
   an implicit superuser with enforcement effectively off, on the honest grounds
   that a process owning the files has no real boundary to enforce. Confirm this
   is the intended boundary (the alternative - enforcing ACLs in-process - would
   be security theater but some operators expect it for defense-in-depth).
5. FORCED DISCONNECT. v1 denies a revoked/dropped user at their NEXT query, not
   mid-socket (section 8). If the maintainer needs a hard "kill the session now"
   on `REVOKE`/`DROP USER`, that reaches across the per-connection worker boundary
   and should be scoped as its own item - flag if it is a v1 requirement.

## 16. Non-goals for v1

- No roles/groups, no GRANT OPTION, no grantable system privileges, no default
  privileges (section 14) - superuser gates all admin; grants are superuser-issued
  SELECT only.
- No row/column-level security - table granularity only.
- No queryable audit table - server-log audit lines only.
- No TLS / channel binding - plaintext TCP with SCRAM password protection only;
  transport security is a named follow-on.
- No forced disconnect on REVOKE/DROP/password change - enforcement at the next
  query.
- No per-source credential passthrough - one configured credential per datasource,
  shared by all fedq users.
- No enforcement against the embedded/CLI process - the wire server is the
  perimeter.
