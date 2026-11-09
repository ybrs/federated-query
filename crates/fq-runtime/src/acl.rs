//! User and access-control DDL, bind-time enforcement, and the live grant cache.
//!
//! `CREATE/ALTER/DROP USER`, `GRANT`/`REVOKE`, and `SHOW USERS`/`SHOW GRANTS`
//! persist into the identity catalog (the `users`/`grants`/`acl_meta`
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

use fq_catalog::store::{Grant, ServerCatalog};
use fq_common::database::{PrincipalId, RelationKind};
use fq_common::security::{AuthMode, DatabasePrivilege, GrantSubject, ObjectKey};
use fq_common::{DataType, UserCredential};
use fq_parse::{GrantObject, Statement};
use fq_scram::Verifier;

use crate::error::RuntimeError;
use crate::materialized::status_result;
use crate::Runtime;

/// The reserved pseudo-grantee that authorizes every authenticated user. It
/// cannot be created, altered, dropped, or logged into (it has no verifier).
const PUBLIC: &str = "PUBLIC";

/// Live identity and database-local permissions, invalidated independently by each store.
pub(crate) struct AclState {
    principal_name: Option<String>,
    principal_id: Option<PrincipalId>,
    superuser: bool,
    database_admin: bool,
    enforce: bool,
    root_generation: i64,
    local_generation: i64,
    objects: HashSet<ObjectKey>,
}

impl AclState {
    /// Embedded callers have explicit process-owner authority.
    pub(crate) fn embedded() -> Self {
        Self {
            principal_name: None,
            principal_id: None,
            superuser: true,
            database_admin: true,
            enforce: false,
            root_generation: -1,
            local_generation: -1,
            objects: HashSet::new(),
        }
    }
}

/// One bind's exact structured SELECT permissions and audit identity.
pub(crate) struct RuntimeAuthorizer {
    principal: String,
    objects: HashSet<ObjectKey>,
}

impl fq_bind::Authorizer for RuntimeAuthorizer {
    /// Match real catalog components without flattening identifiers into dotted strings.
    fn authorize_select(&self, datasource: &str, schema: &str, table: &str) -> bool {
        let candidates = [
            ObjectKey::Datasource {
                datasource: datasource.to_string(),
            },
            ObjectKey::Schema {
                datasource: datasource.to_string(),
                schema: schema.to_string(),
            },
            ObjectKey::Table {
                datasource: datasource.to_string(),
                schema: schema.to_string(),
                table: table.to_string(),
            },
        ];
        let authorized = candidates
            .iter()
            .any(|object| self.objects.contains(object));
        if !authorized {
            tracing::warn!(target: "fedq::audit", actor = %self.principal,
                object = %format!("{datasource}.{schema}.{table}"), "denied SELECT");
        }
        authorized
    }

    /// Match one stored relation's durable identity in the grant set.
    fn authorize_object(&self, object: &ObjectKey) -> bool {
        let authorized = self.objects.contains(object);
        if !authorized {
            tracing::warn!(target: "fedq::audit", actor = %self.principal,
                object = %object.label(), "denied SELECT");
        }
        authorized
    }
}

/// Validate credential syntax and atomically seed the authoritative identity store once.
pub fn import_bootstrap_users(
    identities: &ServerCatalog,
    users: &[UserCredential],
) -> Result<(), RuntimeError> {
    for user in users {
        Verifier::parse(&user.verifier)?;
        reject_reserved_name(&user.name)?;
    }
    identities.seed_users(users)?;
    Ok(())
}

/// Refuse a server startup where auth is enabled but no user is a superuser: no
/// principal could administer grants, so the safe default is a loud refusal.
pub fn require_a_superuser(identities: &ServerCatalog) -> Result<(), RuntimeError> {
    if identities.count_superusers()? == 0 {
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

/// The authority required before dispatch, exhaustively classified by statement kind.
pub(crate) fn statement_authority(statement: &Statement) -> Authority {
    match statement {
        Statement::CreateDatabase { .. }
        | Statement::DropDatabase { .. }
        | Statement::CreateUser { .. }
        | Statement::AlterUserPassword { .. }
        | Statement::AlterUserSuperuser { .. }
        | Statement::DropUser { .. }
        | Statement::ShowUsers
        | Statement::CreateDatasource { .. }
        | Statement::DropDatasource { .. }
        | Statement::GrantDatabase { .. }
        | Statement::RevokeDatabase { .. } => Authority::Server,
        Statement::CreateMaterializedView { .. }
        | Statement::RefreshMaterializedView { .. }
        | Statement::DropMaterializedView { .. }
        | Statement::CreateView { .. }
        | Statement::DropView { .. }
        | Statement::Grant { .. }
        | Statement::Revoke { .. }
        | Statement::SetSetting { .. }
        | Statement::ResetSetting { .. } => Authority::Database,
        Statement::Query(_)
        | Statement::ShowDatabases
        | Statement::ShowViews
        | Statement::ShowSettings
        | Statement::ShowSetting { .. }
        | Statement::ShowPushdownFragments
        | Statement::ShowMaterializedViews
        | Statement::ShowDatasources
        | Statement::ShowTables { .. }
        | Statement::DescribeTable { .. }
        | Statement::ShowGrants { .. } => Authority::Read,
    }
}

/// Server administration and selected-database administration are separate authorities.
pub(crate) enum Authority {
    Read,
    Database,
    Server,
}

impl Runtime {
    /// Bind a name to its current stable identity exactly once for this runtime.
    pub fn set_principal(&self, name: &str) -> Result<(), RuntimeError> {
        self.state_store.check()?;
        if self.config_snapshot().server.auth == AuthMode::Trust {
            self.authz
                .write()
                .expect("authz lock poisoned")
                .principal_name = Some(name.to_string());
            return Ok(());
        }
        {
            let mut state = self.authz.write().expect("authz lock poisoned");
            state.enforce = true;
            state.superuser = false;
            state.database_admin = false;
            state.root_generation = -1;
        }
        let user = self
            .server_catalog()?
            .get_user(name)?
            .ok_or_else(identity_denied)?;
        self.set_principal_by_id(user.id)
    }

    /// Bind the identity proven at startup without resolving a possibly reused name.
    pub fn set_principal_by_id(&self, id: PrincipalId) -> Result<(), RuntimeError> {
        self.state_store.check()?;
        {
            let mut state = self.authz.write().expect("authz lock poisoned");
            if state.principal_id.is_some_and(|current| current != id) {
                return Err(RuntimeError::PermissionDenied(
                    "session identity is already bound".to_string(),
                ));
            }
            state.principal_id = Some(id);
            state.enforce = true;
            state.superuser = false;
            state.database_admin = false;
            state.root_generation = -1;
        }
        self.refresh_acl()
    }

    /// Check independent root/local generations, then load coherent authorization snapshots.
    pub(crate) fn refresh_acl(&self) -> Result<(), RuntimeError> {
        let state = self.authz.read().expect("authz lock poisoned");
        if !state.enforce {
            return Ok(());
        }
        let principal = state.principal_id.ok_or_else(identity_denied)?;
        let root_generation = self.server_catalog()?.acl_generation()?;
        let local_generation = self.database_catalog()?.acl_generation()?;
        if state.root_generation == root_generation && state.local_generation == local_generation {
            return Ok(());
        }
        drop(state);
        self.reload_access(principal)
    }

    /// Refresh stable identity, CONNECT/ADMIN and local SELECT grants as one cache replacement.
    fn reload_access(&self, principal: PrincipalId) -> Result<(), RuntimeError> {
        let database = self.database_lease.as_ref().ok_or_else(identity_denied)?;
        let (root_generation, user, access) = self
            .server_catalog()?
            .authorization_snapshot(database.descriptor.id, principal)?;
        let user = user.ok_or_else(identity_denied)?;
        require_connect(&user, access)?;
        let (local_generation, grants) = self
            .database_catalog()?
            .access_snapshot(GrantSubject::User(principal))?;
        let mut state = self.authz.write().expect("authz lock poisoned");
        state.principal_name = Some(user.name);
        state.superuser = user.superuser;
        state.database_admin = access == Some(DatabasePrivilege::Admin);
        state.objects = grants.into_iter().map(|grant| grant.object).collect();
        state.root_generation = root_generation;
        state.local_generation = local_generation;
        Ok(())
    }

    /// Enforce the exact authority for a statement before inspecting its target objects.
    pub(crate) fn authorize_statement(&self, statement: &Statement) -> Result<(), RuntimeError> {
        self.refresh_acl()?;
        let state = self.authz.read().expect("authz lock poisoned");
        if state.superuser {
            return Ok(());
        }
        match statement_authority(statement) {
            Authority::Read => Ok(()),
            Authority::Database if state.database_admin => Ok(()),
            Authority::Database => Err(RuntimeError::PermissionDenied(
                "database ADMIN required".to_string(),
            )),
            Authority::Server => Err(RuntimeError::PermissionDenied(
                "server SUPERUSER required".to_string(),
            )),
        }
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
        let superuser = !state.enforce || state.superuser || state.database_admin;
        let authorizer = RuntimeAuthorizer {
            principal: state
                .principal_name
                .clone()
                .unwrap_or_else(|| "embedded".to_string()),
            objects: state.objects.clone(),
        };
        drop(state);
        Ok(fq_bind::bind_in_context(
            catalog,
            plan,
            superuser,
            &authorizer,
        )?)
    }

    /// Consult the statement's current structured local SELECT permissions.
    pub(crate) fn can_select_object(&self, object: &ObjectKey) -> bool {
        let state = self.authz.read().expect("authz lock poisoned");
        state.superuser || state.database_admin || state.objects.contains(object)
    }

    /// Filter database names using only server-owned admission rights.
    pub(crate) fn can_connect_database(
        &self,
        database: fq_common::database::DatabaseId,
    ) -> Result<bool, RuntimeError> {
        let state = self.authz.read().expect("authz lock poisoned");
        if state.superuser {
            return Ok(true);
        }
        let principal = state.principal_id.ok_or_else(identity_denied)?;
        drop(state);
        Ok(self
            .server_catalog()?
            .database_access(database, principal)?
            .is_some())
    }

    /// The current principal name for the `granted_by`/`created_by` provenance and
    /// audit lines; the embedded process reports `embedded`.
    pub(crate) fn actor(&self) -> String {
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
        let identities = self.server_catalog()?;
        reject_reserved_name(name)?;
        let verifier = self.derive_verifier(password)?;
        let actor = self.actor();
        if !identities.create_user(name, &verifier, superuser, &actor)? {
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
        let identities = self.server_catalog()?;
        reject_reserved_name(name)?;
        let verifier = self.derive_verifier(password)?;
        if !identities.set_user_password(name, &verifier)? {
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
        let identities = self.server_catalog()?;
        reject_reserved_name(name)?;
        if !identities.set_user_superuser(name, superuser)? {
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
        let identities = self.server_catalog()?;
        reject_reserved_name(name)?;
        if !identities.drop_user(name)? {
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
        let subject = self.grant_subject(grantee)?;
        let target = self.grant_target(object)?;
        let actor = self.actor();
        self.database_catalog()?.grant(subject, &target, &actor)?;
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
        let subject = self.grant_subject(grantee)?;
        let target = self.grant_target(object)?;
        if !self.database_catalog()?.revoke(subject, &target)? {
            return Err(RuntimeError::Acl(format!(
                "no such SELECT grant to revoke from {grantee}"
            )));
        }
        audit_grant("REVOKE", &self.actor(), grantee, object);
        status_result("REVOKE")
    }

    /// `SHOW USERS`: one row per user (name, superuser, created). The verifier is
    /// NEVER a column (fail-closed allowlist). Superuser-gated at dispatch.
    pub(crate) fn show_users(&self) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let users = self.server_catalog()?.list_users()?;
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
        self.grants_batch(&grants)
    }

    /// Gather the grant rows a SHOW GRANTS should return, enforcing that a
    /// non-superuser only sees their own access.
    fn collect_grants(&self, grantee: Option<&str>) -> Result<Vec<Grant>, RuntimeError> {
        let state = self.authz.read().expect("authz lock poisoned");
        let administrator = state.superuser || state.database_admin;
        let name = state.principal_name.clone();
        let subject = state.principal_id.map(GrantSubject::User);
        drop(state);
        let target = match grantee {
            Some(target) => {
                if !administrator && Some(target) != name.as_deref() && target != PUBLIC {
                    return Err(RuntimeError::PermissionDenied(
                        "SHOW GRANTS for another user requires database ADMIN".to_string(),
                    ));
                }
                Some(self.grant_subject(target)?)
            }
            None if administrator => None,
            None => subject,
        };
        Ok(self.database_catalog()?.list_grants(target)?)
    }

    /// Resolve a grantee to its stable identity, reserving PUBLIC as a separate subject.
    pub(crate) fn grant_subject(&self, name: &str) -> Result<GrantSubject, RuntimeError> {
        if name == PUBLIC {
            return Ok(GrantSubject::Public);
        }
        let user = self
            .server_catalog()?
            .get_user(name)?
            .ok_or_else(|| RuntimeError::Acl(format!("user '{name}' does not exist")))?;
        Ok(GrantSubject::User(user.id))
    }

    /// Resolve a local source target to the same real components consulted by the binder.
    fn grant_target(&self, object: &GrantObject) -> Result<ObjectKey, RuntimeError> {
        let catalog = self.catalog_snapshot();
        match object {
            GrantObject::View(name) => self.relation_grant_target(name, RelationKind::View),
            GrantObject::MaterializedView(name) => {
                self.relation_grant_target(name, RelationKind::MaterializedView)
            }
            GrantObject::Datasource(name) => {
                let datasource = catalog
                    .datasource_names()
                    .into_iter()
                    .find(|actual| actual.eq_ignore_ascii_case(name))
                    .ok_or_else(|| {
                        RuntimeError::Acl(format!("datasource '{name}' does not exist"))
                    })?;
                Ok(ObjectKey::Datasource { datasource })
            }
            GrantObject::Schema { datasource, schema } => {
                for (actual_source, actual_schema) in catalog.schema_names() {
                    if actual_source.eq_ignore_ascii_case(datasource)
                        && actual_schema.eq_ignore_ascii_case(schema)
                    {
                        return Ok(ObjectKey::Schema {
                            datasource: actual_source,
                            schema: actual_schema,
                        });
                    }
                }
                Err(RuntimeError::Acl(format!(
                    "schema '{datasource}.{schema}' does not exist"
                )))
            }
            GrantObject::Table {
                datasource,
                schema,
                table,
            } => {
                let table = catalog
                    .resolve_table(Some(datasource), Some(schema), table)
                    .ok_or_else(|| {
                        RuntimeError::Acl(format!("table '{}' does not exist", object.path()))
                    })?;
                let qualifier = table.qualifier().expect("catalog table has no qualifier");
                Ok(ObjectKey::Table {
                    datasource: qualifier.datasource.clone(),
                    schema: qualifier.schema_name.clone(),
                    table: table.name.clone(),
                })
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

/// Reject an identity that no longer exists without falling back to a name lookup.
fn identity_denied() -> RuntimeError {
    RuntimeError::PermissionDenied("authenticated identity no longer exists".to_string())
}

/// Require selected-database admission before loading its sources or serving any statement.
pub(crate) fn require_connect(
    user: &fq_catalog::store::User,
    access: Option<DatabasePrivilege>,
) -> Result<(), RuntimeError> {
    if user.superuser || access.is_some() {
        return Ok(());
    }
    Err(RuntimeError::PermissionDenied(
        "database CONNECT required".to_string(),
    ))
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
impl Runtime {
    /// Assemble authorized local grant rows with current display names.
    fn grants_batch(
        &self,
        grants: &[Grant],
    ) -> Result<(SchemaRef, Vec<RecordBatch>), RuntimeError> {
        let mut columns: [Vec<String>; 6] = Default::default();
        for grant in grants {
            let name = match grant.subject {
                GrantSubject::Public => PUBLIC.to_string(),
                GrantSubject::User(id) => self
                    .server_catalog()?
                    .get_user_by_id(id)?
                    .map_or_else(|| id.to_string(), |user| user.name),
            };
            columns[0].push(name);
            columns[1].push("SELECT".to_string());
            columns[2].push(grant.object.kind().to_string());
            columns[3].push(grant.object.label());
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
}
