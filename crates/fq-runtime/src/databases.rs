//! Selected database resources used by runtime construction and authentication.

use crate::{DatabaseLease, DatabaseManager, Runtime, RuntimeError};
use fq_catalog::store::{DatabaseCatalog, ServerCatalog, StoreError};
use fq_common::Config;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Old-regime state siblings the engine never adopts or deletes.
const OLD_LAYOUT_SUFFIXES: [&str; 3] = [".stats.sqlite", ".events", ".mv"];

/// Open the configured physical storage root when persistence is configured.
pub(crate) fn open_manager(config: &Config) -> Result<Option<DatabaseManager>, RuntimeError> {
    let root = config.storage.resolve(config.source_path.as_deref())?;
    let Some(root) = root else {
        return Ok(None);
    };
    reject_abandoned_layout(config, &root)?;
    Ok(Some(DatabaseManager::open(&root)?))
}

/// List old-regime artifacts sitting next to the config file.
fn old_layout_artifacts(config_path: &str) -> Result<Vec<PathBuf>, RuntimeError> {
    let path = Path::new(config_path);
    let directory = path.parent().unwrap_or_else(|| Path::new("."));
    let stem = path
        .file_stem()
        .unwrap_or_else(|| std::ffi::OsStr::new("fedq"))
        .to_string_lossy();
    let mut found = Vec::new();
    for suffix in OLD_LAYOUT_SUFFIXES {
        let candidate = directory.join(format!("{stem}{suffix}"));
        if candidate.try_exists()? {
            found.push(candidate);
        }
    }
    Ok(found)
}

/// Refuse a fresh storage root while old-regime state sits next to the config:
/// adopting it would mix layouts and deleting it would lose data.
fn reject_abandoned_layout(config: &Config, root: &Path) -> Result<(), RuntimeError> {
    let Some(config_path) = config.source_path.as_deref() else {
        return Ok(());
    };
    if root.join("server.sqlite").try_exists()? {
        return Ok(());
    }
    let found = old_layout_artifacts(config_path)?;
    if found.is_empty() {
        return Ok(());
    }
    let mut names = Vec::new();
    for path in &found {
        names.push(path.display().to_string());
    }
    Err(RuntimeError::Config(format!(
        "old engine state ({}) sits next to the config while the storage root '{}' \
         was never created; move or delete the old state, the engine never adopts \
         or removes it",
        names.join(", "),
        root.display()
    )))
}

/// Open identities without loading project sources or materialized results.
pub fn open_server_catalog(config: &Config) -> Result<Option<Arc<ServerCatalog>>, RuntimeError> {
    Ok(open_manager(config)?.map(|manager| Arc::clone(manager.server_catalog())))
}

/// Hold the selected namespace, or require main for an explicitly transient runtime.
pub(crate) fn acquire_database(
    manager: Option<&DatabaseManager>,
    name: &str,
) -> Result<Option<Arc<DatabaseLease>>, RuntimeError> {
    match manager {
        Some(manager) => Ok(Some(Arc::new(manager.acquire(name)?))),
        None if name == "main" => Ok(None),
        None => Err(StoreError::DatabaseMissing(name.to_string()).into()),
    }
}

/// Open the selected local definitions after its persistent identity was verified.
pub(crate) fn open_database_catalog(
    database: Option<&DatabaseLease>,
) -> Result<Option<DatabaseCatalog>, RuntimeError> {
    database
        .map(|database| {
            DatabaseCatalog::open(&database.paths.metadata.to_string_lossy())
                .map_err(RuntimeError::from)
        })
        .transpose()
}

/// Identify both root and selected local metadata for runtime loss detection.
pub(crate) fn guarded_paths(
    manager: Option<&DatabaseManager>,
    database: Option<&DatabaseLease>,
) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if let Some(manager) = manager {
        paths.push(manager.server_path());
    }
    if let Some(database) = database {
        paths.push(database.paths.metadata.clone());
    }
    paths
}

impl Runtime {
    /// Require durable identities for user and server permission statements.
    pub(crate) fn server_catalog(&self) -> Result<&ServerCatalog, RuntimeError> {
        self.server_catalog.as_deref().ok_or_else(|| {
            RuntimeError::Acl("this runtime has no configured identity catalog".to_string())
        })
    }

    /// Require the selected local catalog for persistent relation definitions.
    pub(crate) fn database_catalog(&self) -> Result<&DatabaseCatalog, RuntimeError> {
        self.database_catalog.as_ref().ok_or_else(|| {
            RuntimeError::Config("this runtime has no configured database catalog".to_string())
        })
    }

    /// The fixed SQL namespace selected when this runtime was constructed.
    pub fn database_name(&self) -> &str {
        self.database_lease
            .as_ref()
            .map_or("main", |database| database.descriptor.name.as_str())
    }
}

impl Runtime {
    /// Require configured physical storage for server database lifecycle statements.
    fn manager(&self) -> Result<&DatabaseManager, RuntimeError> {
        self.database_manager.as_ref().ok_or_else(|| {
            RuntimeError::Config(
                "database lifecycle requires configured persistent storage".to_string(),
            )
        })
    }

    /// Create one database through its crash-recoverable physical lifecycle.
    pub(crate) fn create_database(
        &self,
        name: &str,
    ) -> Result<(arrow::datatypes::SchemaRef, Vec<arrow::array::RecordBatch>), RuntimeError> {
        self.manager()?.create(name, &self.actor())?;
        crate::materialized::status_result("CREATE DATABASE")
    }

    /// Remove one inactive database without changing another database's files.
    pub(crate) fn drop_database(
        &self,
        name: &str,
    ) -> Result<(arrow::datatypes::SchemaRef, Vec<arrow::array::RecordBatch>), RuntimeError> {
        if self.database_name() != "main" {
            return Err(RuntimeError::PermissionDenied(
                "DROP DATABASE requires a connection to main".to_string(),
            ));
        }
        self.manager()?.drop_database(name)?;
        crate::materialized::status_result("DROP DATABASE")
    }

    /// List ready registrations after the statement's administration check.
    pub(crate) fn show_databases(
        &self,
    ) -> Result<(arrow::datatypes::SchemaRef, Vec<arrow::array::RecordBatch>), RuntimeError> {
        let mut names = Vec::new();
        for record in self.server_catalog()?.list_databases()? {
            if record.state == fq_common::database::DatabaseState::Ready
                && self.can_connect_database(record.id)?
            {
                names.push(record.name);
            }
        }
        crate::introspect::text_result(&["name"], vec![names])
    }
}

/// Authorize the exact leased database identity before connecting any local sources.
pub(crate) fn authorize_admission(
    manager: Option<&DatabaseManager>,
    database: Option<&DatabaseLease>,
    principal: fq_common::database::PrincipalId,
) -> Result<(), RuntimeError> {
    let manager = manager.ok_or_else(|| {
        RuntimeError::PermissionDenied(
            "authenticated sessions require persistent storage".to_string(),
        )
    })?;
    let database = database.expect("persistent manager must hold a database lease");
    let (_, user, access) = manager
        .server_catalog()
        .authorization_snapshot(database.descriptor.id, principal)?;
    let user = user.ok_or_else(|| {
        RuntimeError::PermissionDenied("authenticated identity no longer exists".to_string())
    })?;
    crate::acl::require_connect(&user, access)
}

impl Runtime {
    /// Assign server-owned admission or local management authority to a stable principal.
    pub(crate) fn grant_database(
        &self,
        name: &str,
        grantee: &str,
        privilege: fq_common::security::DatabasePrivilege,
    ) -> Result<(arrow::datatypes::SchemaRef, Vec<arrow::array::RecordBatch>), RuntimeError> {
        let database = self.server_catalog()?.get_database(name)?;
        let subject = self.grant_subject(grantee)?;
        self.server_catalog()?
            .grant_database(database.id, subject, privilege, &self.actor())?;
        crate::materialized::status_result("GRANT")
    }

    /// Revoke a concrete admission/management grant and raise when it did not exist.
    pub(crate) fn revoke_database(
        &self,
        name: &str,
        grantee: &str,
        privilege: fq_common::security::DatabasePrivilege,
    ) -> Result<(arrow::datatypes::SchemaRef, Vec<arrow::array::RecordBatch>), RuntimeError> {
        let database = self.server_catalog()?.get_database(name)?;
        let subject = self.grant_subject(grantee)?;
        if !self
            .server_catalog()?
            .revoke_database(database.id, subject, privilege)?
        {
            return Err(RuntimeError::Acl(
                "no such database grant to revoke".to_string(),
            ));
        }
        crate::materialized::status_result("REVOKE")
    }
}
