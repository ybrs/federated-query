//! User and access-control DDL, bind-time enforcement, and the live grant cache.
//!
//! `CREATE/ALTER/DROP USER`, `GRANT`/`REVOKE`, and `SHOW USERS`/`SHOW GRANTS`
//! persist into the accelerator's durable store (the `users`/`grants`/`acl_meta`
//! tables). Every base-table reference is authorized at BIND time through the one
//! resolver (`fq_bind::resolve_scan_table`): a non-superuser without a covering
//! grant gets a non-leaking not-found, indistinguishable from a missing table at
//! the client, with the real reason audited server-side.
//!
//! Grant state is read LIVE (not a frozen per-session snapshot), because
//! privilege staleness EXTENDS access. A per-query O(1) generation read gates a
//! cheap reload: the cached grant set is reused until a `GRANT`/`REVOKE`/`ALTER
//! USER`/`DROP USER` (any of which bumps the generation) makes it stale, so a
//! revoke or superuser downgrade takes effect on every live session at its NEXT
//! query.

use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{RecordBatch, StringArray};
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema, SchemaRef};

use fq_accel::{Accelerator, Grant};
use fq_common::{DataType, Principal, UserCredential};
use fq_parse::{GrantObject, Statement};
use fq_scram::Verifier;

use crate::error::RuntimeError;
use crate::materialized::status_result;
use crate::Runtime;

/// The single grantable privilege in v1: the engine is read-only over sources,
/// so SELECT is the only meaningful data privilege.
const SELECT: &str = "SELECT";

/// The reserved pseudo-grantee that authorizes every authenticated user. It
/// cannot be created, altered, dropped, or logged into (it has no verifier).
const PUBLIC: &str = "PUBLIC";

/// This runtime's cached authorization state. `enforce` is false for the
/// embedded / trust-mode build (an implicit superuser), true only for a wire
/// principal under configured auth. The cached `paths` are the lowercased object
/// paths the principal and PUBLIC hold SELECT on, valid while `generation`
/// matches the store's counter.
pub(crate) struct AclState {
    /// The authenticated principal name; `None` for the embedded process.
    principal_name: Option<String>,
    /// The effective superuser flag: true for embedded/trust, else the store's
    /// authoritative value (never trusted from the wire), refreshed live.
    superuser: bool,
    /// Whether bind-time grant checks are enforced for this runtime.
    enforce: bool,
    /// The ACL generation the cached `paths`/`superuser` were loaded at.
    generation: i64,
    /// The lowercased object paths (datasource | d.s | d.s.t) the principal and
    /// PUBLIC hold SELECT on. Empty until first load.
    paths: HashSet<String>,
}

impl AclState {
    /// The embedded default: an implicit superuser with enforcement off. A wire
    /// connection later calls `set_principal` to enable enforcement.
    pub(crate) fn embedded() -> Self {
        Self {
            principal_name: None,
            superuser: true,
            enforce: false,
            // -1 forces a reload the first time enforcement turns on.
            generation: -1,
            paths: HashSet::new(),
        }
    }
}

/// A snapshot of the authorization state a single bind consults: the effective
/// superuser flag and the held object paths, plus the principal name for the
/// denied-access audit line.
pub(crate) struct RuntimeAuthorizer {
    principal: String,
    paths: HashSet<String>,
}

impl fq_bind::Authorizer for RuntimeAuthorizer {
    /// Authorize a resolved base table by its REAL qualifier: the principal (or
    /// PUBLIC) holds SELECT iff a covering grant exists at ANY of the table's
    /// three containment levels. A denial writes the true reason to the audit
    /// log; the binder raises a non-leaking not-found to the client.
    fn authorize_select(&self, datasource: &str, schema: &str, table: &str) -> bool {
        let candidates = [
            datasource.to_lowercase(),
            format!("{datasource}.{schema}").to_lowercase(),
            format!("{datasource}.{schema}.{table}").to_lowercase(),
        ];
        let authorized = candidates.iter().any(|path| self.paths.contains(path));
        if !authorized {
            tracing::warn!(
                target: "fedq::audit",
                actor = %self.principal,
                object = %format!("{datasource}.{schema}.{table}"),
                "denied SELECT"
            );
        }
        authorized
    }
}

/// Import the config's bootstrap users into the persisted store (idempotent
/// `INSERT OR IGNORE`, stamped `bootstrap-import`). Each verifier is validated as
/// well-formed pg_authid before storing, so a malformed config credential fails
/// loudly at import rather than at a login. Thereafter the persisted table is
/// authoritative and the config list is only the seed.
pub fn import_bootstrap_users(
    accel: &Accelerator,
    users: &[UserCredential],
) -> Result<(), RuntimeError> {
    for user in users {
        Verifier::parse(&user.verifier)
            .map_err(|error| RuntimeError::Acl(format!("server user '{}': {error}", user.name)))?;
        reject_reserved_name(&user.name)?;
        accel.create_user(
            &user.name,
            &user.verifier,
            user.superuser,
            "bootstrap-import",
        )?;
    }
    Ok(())
}

/// Refuse a server startup where auth is enabled but no user is a superuser: no
/// principal could administer grants, so the safe default is a loud refusal.
pub fn require_a_superuser(accel: &Accelerator) -> Result<(), RuntimeError> {
    if accel.count_superusers()? == 0 {
        return Err(RuntimeError::Acl(
            "authentication is enabled but no user is a superuser; no principal could \
             administer grants (define an administrator with hash-password --superuser)"
                .to_string(),
        ));
    }
    Ok(())
}

/// Raise when a name collides with the reserved `PUBLIC` pseudo-grantee
/// (case-insensitive): PUBLIC is not a real user and cannot be created/altered/
/// dropped or logged into.
fn reject_reserved_name(name: &str) -> Result<(), RuntimeError> {
    if name.eq_ignore_ascii_case(PUBLIC) {
        return Err(RuntimeError::Acl(
            "'PUBLIC' is a reserved pseudo-grantee and cannot be used as a user name".to_string(),
        ));
    }
    Ok(())
}

/// The admin statements gated behind the superuser flag, with the action name
/// for the permission-denied message; `None` for any statement any user may run.
pub(crate) fn superuser_action(statement: &Statement) -> Option<&'static str> {
    match statement {
        Statement::CreateUser { .. } => Some("CREATE USER"),
        Statement::AlterUserPassword { .. } | Statement::AlterUserSuperuser { .. } => {
            Some("ALTER USER")
        }
        Statement::DropUser { .. } => Some("DROP USER"),
        Statement::Grant { .. } => Some("GRANT"),
        Statement::Revoke { .. } => Some("REVOKE"),
        Statement::ShowUsers => Some("SHOW USERS"),
        Statement::CreateDatasource { .. } => Some("CREATE DATASOURCE"),
        Statement::DropDatasource { .. } => Some("DROP DATASOURCE"),
        Statement::CreateMaterializedView { .. } => Some("CREATE MATERIALIZED VIEW"),
        Statement::RefreshMaterializedView { .. } => Some("REFRESH MATERIALIZED VIEW"),
        Statement::DropMaterializedView { .. } => Some("DROP MATERIALIZED VIEW"),
        Statement::CreateEventView { .. } => Some("CREATE EVENT VIEW"),
        Statement::RefreshEventView { .. } => Some("REFRESH EVENT VIEW"),
        Statement::DropEventView { .. } => Some("DROP EVENT VIEW"),
        _ => None,
    }
}

impl Runtime {
    /// Set the authenticated wire principal for this runtime and turn on
    /// enforcement when auth is configured. In trust mode (no configured users)
    /// even a wire connection runs as an implicit superuser with enforcement off.
    /// The superuser flag is resolved from the store (authoritative), never
    /// trusted from the wire.
    pub fn set_principal(&self, name: &str) -> Result<(), RuntimeError> {
        let auth_on = !self
            .config
            .read()
            .expect("config lock poisoned")
            .server
            .users
            .is_empty();
        {
            let mut state = self.authz.write().expect("authz lock poisoned");
            state.principal_name = Some(name.to_string());
            if !auth_on {
                state.superuser = true;
                state.enforce = false;
                return Ok(());
            }
            state.enforce = true;
            // Force a reload on the next refresh so the store's superuser flag and
            // grants are read fresh for the new principal.
            state.generation = -1;
        }
        self.refresh_acl()
    }

    /// Reload the cached grant set and superuser flag if the store's generation
    /// changed since the last load. A no-op when enforcement is off (embedded /
    /// trust). Cost per query is one O(1) generation read, plus an occasional
    /// bounded grant reload only when privileges actually changed.
    pub(crate) fn refresh_acl(&self) -> Result<(), RuntimeError> {
        if !self.authz.read().expect("authz lock poisoned").enforce {
            return Ok(());
        }
        let accel = self.accelerator()?;
        let generation = accel.acl_generation()?;
        if self.authz.read().expect("authz lock poisoned").generation == generation {
            return Ok(());
        }
        let name = self
            .authz
            .read()
            .expect("authz lock poisoned")
            .principal_name
            .clone()
            .unwrap_or_default();
        let superuser = accel.get_user(&name)?.is_some_and(|user| user.superuser);
        let mut paths = HashSet::new();
        for grant in accel.grants_for(&name)? {
            paths.insert(grant.object_path.to_lowercase());
        }
        for grant in accel.grants_for(PUBLIC)? {
            paths.insert(grant.object_path.to_lowercase());
        }
        let mut state = self.authz.write().expect("authz lock poisoned");
        state.superuser = superuser;
        state.paths = paths;
        state.generation = generation;
        Ok(())
    }

    /// Bind `plan` under this runtime's live authorization: a superuser (or the
    /// embedded/trust build) binds with no enforcement; an enforced non-superuser
    /// binds through an authorizer over the current grant set. Called by
    /// `optimize` after refreshing the ACL cache.
    pub(crate) fn bind_with_authz(
        &self,
        catalog: &fq_catalog::Catalog,
        plan: fq_plan::LogicalPlan,
    ) -> Result<fq_plan::LogicalPlan, RuntimeError> {
        self.refresh_acl()?;
        let state = self.authz.read().expect("authz lock poisoned");
        if !state.enforce || state.superuser {
            drop(state);
            return Ok(fq_bind::bind(catalog, plan)?);
        }
        let principal = Principal::user(state.principal_name.clone().unwrap_or_default(), false);
        let authorizer = RuntimeAuthorizer {
            principal: principal.name.clone(),
            paths: state.paths.clone(),
        };
        drop(state);
        Ok(fq_bind::bind_as(catalog, plan, &principal, &authorizer)?)
    }

    /// Raise unless the current principal is a superuser (refreshed live), for the
    /// admin-DDL dispatch gate. An explicit message is correct: the statement kind
    /// is not a secret.
    pub(crate) fn require_superuser(&self, action: &str) -> Result<(), RuntimeError> {
        self.refresh_acl()?;
        if self.authz.read().expect("authz lock poisoned").superuser {
            return Ok(());
        }
        Err(RuntimeError::PermissionDenied(format!(
            "{action} requires superuser"
        )))
    }

    /// The current principal name for the `granted_by`/`created_by` provenance and
    /// audit lines; the embedded process reports `embedded`.
    fn actor(&self) -> String {
        self.authz
            .read()
            .expect("authz lock poisoned")
            .principal_name
            .clone()
            .unwrap_or_else(|| "embedded".to_string())
    }

    /// The SCRAM iteration count NEW verifiers are derived with (the config
    /// default; existing verifiers carry their own count).
    fn scram_iterations(&self) -> u32 {
        self.config
            .read()
            .expect("config lock poisoned")
            .server
            .scram_iterations
    }

    /// `CREATE USER <name> WITH PASSWORD '<pw>' [SUPERUSER]`: hash the plaintext to
    /// a verifier server-side (the plaintext lives only for this call frame and is
    /// never stored or logged), then persist it. A name that already exists raises
    /// `already exists` (first-writer-wins); the reserved PUBLIC raises.
    pub(crate) fn create_user(
        &self,
        name: &str,
        password: &str,
        superuser: bool,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let accel = self.accelerator()?;
        reject_reserved_name(name)?;
        let verifier = self.derive_verifier(password)?;
        let actor = self.actor();
        if !accel.create_user(name, &verifier, superuser, &actor)? {
            return Err(RuntimeError::Acl(format!(
                "CREATE USER: user '{name}' already exists"
            )));
        }
        audit_ddl("CREATE USER", &actor, name);
        status_result("CREATE USER")
    }

    /// `ALTER USER <name> WITH PASSWORD '<pw>'`: overwrite the stored verifier. It
    /// does NOT invalidate a live authenticated session (authentication happened at
    /// that connection's handshake); new connections use the new verifier.
    pub(crate) fn alter_user_password(
        &self,
        name: &str,
        password: &str,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let accel = self.accelerator()?;
        reject_reserved_name(name)?;
        let verifier = self.derive_verifier(password)?;
        if !accel.set_user_password(name, &verifier)? {
            return Err(RuntimeError::Acl(format!(
                "ALTER USER: user '{name}' does not exist"
            )));
        }
        audit_ddl("ALTER USER PASSWORD", &self.actor(), name);
        status_result("ALTER USER")
    }

    /// `ALTER USER <name> WITH [SUPERUSER | NOSUPERUSER]`: set the superuser flag.
    /// The change takes effect on a live session at its next query (the flag is
    /// refreshed on the generation bump).
    pub(crate) fn alter_user_superuser(
        &self,
        name: &str,
        superuser: bool,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let accel = self.accelerator()?;
        reject_reserved_name(name)?;
        if !accel.set_user_superuser(name, superuser)? {
            return Err(RuntimeError::Acl(format!(
                "ALTER USER: user '{name}' does not exist"
            )));
        }
        audit_ddl("ALTER USER SUPERUSER", &self.actor(), name);
        status_result("ALTER USER")
    }

    /// `DROP USER <name>`: remove the user and every grant it holds in one
    /// transaction. A dropped user's live session is denied at its next query (the
    /// grant check reads the live store).
    pub(crate) fn drop_user(
        &self,
        name: &str,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let accel = self.accelerator()?;
        reject_reserved_name(name)?;
        if !accel.drop_user(name)? {
            return Err(RuntimeError::Acl(format!(
                "DROP USER: user '{name}' does not exist"
            )));
        }
        audit_ddl("DROP USER", &self.actor(), name);
        status_result("DROP USER")
    }

    /// `GRANT SELECT ON <object> TO <grantee>`: idempotently authorize a grantee
    /// on an object at one containment level. A grantee that is neither PUBLIC nor
    /// an existing user raises (a typo'd grantee is a mistake, not a right).
    pub(crate) fn grant(
        &self,
        object: &GrantObject,
        grantee: &str,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let accel = self.accelerator()?;
        require_grantee_exists(accel, grantee)?;
        let actor = self.actor();
        accel.grant(grantee, SELECT, object.kind(), &object.path(), &actor)?;
        audit_grant("GRANT", &actor, grantee, object);
        status_result("GRANT")
    }

    /// `REVOKE SELECT ON <object> FROM <grantee>`: remove a grant. A revoke that
    /// matches no row RAISES (a silent no-op hides a typo).
    pub(crate) fn revoke(
        &self,
        object: &GrantObject,
        grantee: &str,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let accel = self.accelerator()?;
        if !accel.revoke(grantee, SELECT, object.kind(), &object.path())? {
            return Err(RuntimeError::Acl(format!(
                "no such grant to revoke: SELECT ON {} {} FROM {grantee}",
                object.kind().to_uppercase(),
                object.path()
            )));
        }
        audit_grant("REVOKE", &self.actor(), grantee, object);
        status_result("REVOKE")
    }

    /// `SHOW USERS`: one row per user (name, superuser, created). The verifier is
    /// NEVER a column (fail-closed allowlist). Superuser-gated at dispatch.
    pub(crate) fn show_users(&self) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let users = self.accelerator()?.list_users()?;
        let mut names = Vec::with_capacity(users.len());
        let mut superusers = Vec::with_capacity(users.len());
        let mut created = Vec::with_capacity(users.len());
        for user in users {
            names.push(user.name);
            superusers.push(user.superuser.to_string());
            created.push(user.created_at);
        }
        let schema = users_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(names)),
                Arc::new(StringArray::from(superusers)),
                Arc::new(StringArray::from(created)),
            ],
        )
        .map_err(|error| RuntimeError::ResultShape(error.to_string()))?;
        Ok((schema, vec![batch]))
    }

    /// `SHOW GRANTS [FOR <grantee>]`. With no FOR, a superuser sees all grants; a
    /// non-superuser sees only their own and PUBLIC's. `SHOW GRANTS FOR <other>`
    /// requires superuser (a user may inspect their own access, never others').
    pub(crate) fn show_grants(
        &self,
        grantee: Option<&str>,
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        self.refresh_acl()?;
        let grants = self.collect_grants(grantee)?;
        grants_batch(&grants)
    }

    /// Gather the grant rows a SHOW GRANTS should return, enforcing that a
    /// non-superuser only sees their own access.
    fn collect_grants(&self, grantee: Option<&str>) -> Result<Vec<Grant>, RuntimeError> {
        let accel = self.accelerator()?;
        let (superuser, name) = {
            let state = self.authz.read().expect("authz lock poisoned");
            (
                state.superuser,
                state.principal_name.clone().unwrap_or_default(),
            )
        };
        match grantee {
            Some(target) => {
                if !superuser && !target.eq_ignore_ascii_case(&name) && target != PUBLIC {
                    return Err(RuntimeError::PermissionDenied(
                        "SHOW GRANTS FOR another principal requires superuser".to_string(),
                    ));
                }
                Ok(accel.grants_for(target)?)
            }
            None if superuser => Ok(accel.list_grants()?),
            None => {
                let mut grants = accel.grants_for(&name)?;
                grants.extend(accel.grants_for(PUBLIC)?);
                Ok(grants)
            }
        }
    }

    /// Hash a plaintext password to a stored verifier with a fresh random salt at
    /// the configured iteration count. The plaintext is used only here and never
    /// stored or logged.
    fn derive_verifier(&self, password: &str) -> Result<String, RuntimeError> {
        let verifier = fq_scram::derive_verifier_random_salt(password, self.scram_iterations())
            .map_err(|error| RuntimeError::Acl(error.to_string()))?;
        Ok(verifier.to_authid_string())
    }
}

/// Raise unless `grantee` is PUBLIC or an existing user (a typo'd grantee names
/// no right).
fn require_grantee_exists(accel: &Accelerator, grantee: &str) -> Result<(), RuntimeError> {
    if grantee == PUBLIC || accel.get_user(grantee)?.is_some() {
        return Ok(());
    }
    Err(RuntimeError::Acl(format!(
        "GRANT: grantee '{grantee}' is not a user (grant to an existing user or PUBLIC)"
    )))
}

/// Audit one user DDL event: actor + action + target user, never the password.
fn audit_ddl(action: &str, actor: &str, target: &str) {
    tracing::info!(target: "fedq::audit", %actor, %target, "{action}");
}

/// Audit one GRANT/REVOKE event: actor + grantee + object, never a credential.
fn audit_grant(action: &str, actor: &str, grantee: &str, object: &GrantObject) {
    tracing::info!(
        target: "fedq::audit",
        %actor,
        %grantee,
        object = %format!("{} {}", object.kind(), object.path()),
        "{action}"
    );
}

/// The ordered result columns of `SHOW USERS` (never a verifier column).
const USER_COLUMNS: [&str; 3] = ["name", "superuser", "created"];

/// The ordered result columns of `SHOW GRANTS`.
const GRANT_COLUMNS: [&str; 6] = [
    "grantee",
    "privilege",
    "object_kind",
    "object_path",
    "granted_by",
    "created",
];

/// The `SHOW USERS` result schema (three text columns).
fn users_schema() -> SchemaRef {
    text_schema(&USER_COLUMNS)
}

/// The `SHOW GRANTS` result schema (six text columns).
fn grants_schema() -> SchemaRef {
    text_schema(&GRANT_COLUMNS)
}

/// A schema of all-text columns for the given names.
fn text_schema(columns: &[&str]) -> SchemaRef {
    let mut fields = Vec::with_capacity(columns.len());
    for name in columns {
        fields.push(Field::new(*name, ArrowDataType::Utf8, false));
    }
    Arc::new(Schema::new(fields))
}

/// The (column name, engine type) pairs `describe` reports for SHOW USERS.
pub(crate) fn users_describe_columns() -> Vec<(String, DataType)> {
    describe_text(&USER_COLUMNS)
}

/// The (column name, engine type) pairs `describe` reports for SHOW GRANTS.
pub(crate) fn grants_describe_columns() -> Vec<(String, DataType)> {
    describe_text(&GRANT_COLUMNS)
}

/// All-text describe columns for the given names.
fn describe_text(columns: &[&str]) -> Vec<(String, DataType)> {
    let mut out = Vec::with_capacity(columns.len());
    for name in columns {
        out.push((name.to_string(), DataType::Text));
    }
    out
}

/// Assemble a SHOW GRANTS result batch from the grant rows.
fn grants_batch(grants: &[Grant]) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
    let mut columns: [Vec<String>; 6] = Default::default();
    for grant in grants {
        columns[0].push(grant.grantee.clone());
        columns[1].push(grant.privilege.clone());
        columns[2].push(grant.object_kind.clone());
        columns[3].push(grant.object_path.clone());
        columns[4].push(grant.granted_by.clone());
        columns[5].push(grant.created_at.clone());
    }
    let schema = grants_schema();
    let arrays: Vec<Arc<dyn arrow::array::Array>> = columns
        .into_iter()
        .map(|values| Arc::new(StringArray::from(values)) as Arc<dyn arrow::array::Array>)
        .collect();
    let batch = RecordBatch::try_new(Arc::clone(&schema), arrays)
        .map_err(|error| RuntimeError::ResultShape(error.to_string()))?;
    Ok((schema, vec![batch]))
}
