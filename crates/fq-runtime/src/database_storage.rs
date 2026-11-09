//! Database directories and crash-recoverable lifecycle over the server catalog.

use std::fs::{File, OpenOptions, TryLockError};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use fq_catalog::store::{DatabaseCatalog, ServerCatalog, StoreError};
use fq_common::database::{DatabaseDescriptor, DatabaseId, DatabasePaths, DatabaseState};

use crate::RuntimeError;

/// The server catalog and storage root used to create, acquire, and retire databases.
pub struct DatabaseManager {
    root: PathBuf,
    server: Arc<ServerCatalog>,
}

/// A selected database whose shared filesystem lease prevents directory removal.
pub struct DatabaseLease {
    pub descriptor: DatabaseDescriptor,
    pub paths: DatabasePaths,
    _lock: File,
}

impl DatabaseManager {
    /// Open the storage root, recover unfinished operations, and bootstrap main.
    pub fn open(root: &Path) -> Result<Self, RuntimeError> {
        std::fs::create_dir_all(root.join("databases"))?;
        std::fs::create_dir_all(root.join("locks"))?;
        let bootstrap = open_lock(&root.join("bootstrap.lock"))?;
        bootstrap.lock()?;
        require_catalog_presence(root)?;
        let manager = Self {
            root: std::path::absolute(root)?,
            server: Arc::new(ServerCatalog::open(
                &root.join("server.sqlite").to_string_lossy(),
            )?),
        };
        manager.recover()?;
        manager.ensure_main()?;
        Ok(manager)
    }

    /// Bootstrap main on a never-served root, and refuse to recreate it on a
    /// root that already completed bootstrap: a missing main registration
    /// there is catalog corruption, not a fresh start.
    fn ensure_main(&self) -> Result<(), RuntimeError> {
        match self.server.find_database("main")? {
            Some(record) if record.state == DatabaseState::Ready => {
                self.server.mark_bootstrap_complete()?;
                Ok(())
            }
            Some(_) => Err(RuntimeError::StateStoreLost(
                "main is registered but not ready after recovery".to_string(),
            )),
            None => self.bootstrap_main(),
        }
    }

    /// Create main only when bootstrap never completed and nothing else is
    /// registered or stored: an interrupted first bootstrap is the only
    /// state that may legitimately reach this point without a main.
    fn bootstrap_main(&self) -> Result<(), RuntimeError> {
        if self.server.bootstrap_complete()? {
            return Err(RuntimeError::StateStoreLost(
                "main registration is missing on a bootstrapped storage root".to_string(),
            ));
        }
        if !self.server.list_databases()?.is_empty() || self.has_database_directories()? {
            return Err(RuntimeError::StateStoreLost(
                "databases exist but main is missing and bootstrap never completed".to_string(),
            ));
        }
        self.create("main", "bootstrap")?;
        self.server.mark_bootstrap_complete()?;
        Ok(())
    }

    /// Whether any per-database directory exists under this root.
    fn has_database_directories(&self) -> Result<bool, RuntimeError> {
        Ok(std::fs::read_dir(self.root.join("databases"))?
            .next()
            .transpose()?
            .is_some())
    }

    /// The root metadata file guarded by runtimes using this manager.
    pub fn server_path(&self) -> PathBuf {
        self.root.join("server.sqlite")
    }

    /// The identity and registration catalog independent of all project stores.
    pub fn server_catalog(&self) -> &Arc<ServerCatalog> {
        &self.server
    }

    /// Create a complete database directory before publishing it as ready.
    pub fn create(&self, name: &str, actor: &str) -> Result<DatabaseDescriptor, RuntimeError> {
        let id = DatabaseId::new();
        let paths = DatabasePaths::new(&self.root, id);
        let lock = open_lock(&paths.lifecycle_lock)?;
        lock.lock()?;
        let record = self.server.reserve_database(id, name, actor)?;
        self.create_files(&record, &paths)?;
        self.server.publish_database(record.id)?;
        Ok(self.server.get_database(name)?)
    }

    /// Acquire one ready database and recheck registration under its shared lock.
    pub fn acquire(&self, name: &str) -> Result<DatabaseLease, RuntimeError> {
        let record = self.require_ready(name)?;
        let paths = DatabasePaths::new(&self.root, record.id);
        let lock = open_lock(&paths.lifecycle_lock)?;
        try_shared_lock(&lock, name)?;
        let current = self.require_ready(name)?;
        if current.id != record.id {
            return Err(StoreError::DatabaseMissing(name.to_string()).into());
        }
        require_database_metadata(&paths, record.id)?;
        Ok(DatabaseLease {
            descriptor: record,
            paths,
            _lock: lock,
        })
    }

    /// Retire and remove an inactive database without scanning other databases' data.
    pub fn drop_database(&self, name: &str) -> Result<(), RuntimeError> {
        if name == "main" {
            return Err(RuntimeError::PermissionDenied(
                "main cannot be dropped".to_string(),
            ));
        }
        let record = self.require_ready(name)?;
        let paths = DatabasePaths::new(&self.root, record.id);
        let lock = open_lock(&paths.lifecycle_lock)?;
        try_exclusive_lock(&lock, name)?;
        self.server.retire_database(record.id)?;
        remove_database_directory(&paths.directory)?;
        sync_directory(&self.root.join("databases"))?;
        self.server.purge_database(record.id)?;
        Ok(())
    }

    /// Require a ready registration without treating an incomplete database as main.
    fn require_ready(&self, name: &str) -> Result<DatabaseDescriptor, RuntimeError> {
        let record = self.server.get_database(name)?;
        if record.state != DatabaseState::Ready {
            return Err(StoreError::DatabaseMissing(name.to_string()).into());
        }
        Ok(record)
    }

    /// Initialize only the directory and metadata belonging to the reserved identity.
    fn create_files(
        &self,
        record: &DatabaseDescriptor,
        paths: &DatabasePaths,
    ) -> Result<(), RuntimeError> {
        std::fs::create_dir(&paths.directory)?;
        for directory in [&paths.materialized, &paths.scratch] {
            std::fs::create_dir(directory)?;
        }
        let catalog = DatabaseCatalog::open(&paths.metadata.to_string_lossy())?;
        catalog.initialize_identity(record.id)?;
        drop(catalog);
        File::open(&paths.metadata)?.sync_all()?;
        sync_directory(&paths.directory)?;
        sync_directory(&self.root.join("databases"))?;
        Ok(())
    }

    /// Finish unpublished creation and retired deletion before serving storage.
    fn recover(&self) -> Result<(), RuntimeError> {
        for record in self.server.list_databases()? {
            if record.state != DatabaseState::Ready {
                self.recover_record(&record)?;
            }
        }
        Ok(())
    }

    /// Clean one incomplete directory while holding its exclusive lifecycle lock.
    fn recover_record(&self, record: &DatabaseDescriptor) -> Result<(), RuntimeError> {
        let paths = DatabasePaths::new(&self.root, record.id);
        let lock = open_lock(&paths.lifecycle_lock)?;
        match try_exclusive_lock(&lock, &record.name) {
            Ok(()) => {}
            Err(RuntimeError::DatabaseInUse(_)) => return Ok(()),
            Err(error) => return Err(error),
        }
        if !self.recovery_required(record)? {
            return Ok(());
        }
        remove_database_directory(&paths.directory)?;
        sync_directory(&self.root.join("databases"))?;
        self.server.purge_database(record.id)?;
        Ok(())
    }
    /// Recheck a lifecycle snapshot under its lock before deleting any directory.
    fn recovery_required(&self, record: &DatabaseDescriptor) -> Result<bool, RuntimeError> {
        let current = self.server.find_database(&record.name)?;
        Ok(current.is_some_and(|current| {
            current.id == record.id && current.state != DatabaseState::Ready
        }))
    }
}

/// Open the stable lock inode without truncating or replacing another holder's file.
fn open_lock(path: &Path) -> Result<File, std::io::Error> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
}

/// Try the shared session lease without waiting on another lifecycle operation.
fn try_shared_lock(file: &File, name: &str) -> Result<(), RuntimeError> {
    map_lock_result(file.try_lock_shared(), name)
}

/// Refuse to remove a database while a runtime holds a shared lease.
fn try_exclusive_lock(file: &File, name: &str) -> Result<(), RuntimeError> {
    map_lock_result(file.try_lock(), name)
}

/// Preserve native IO failures and report ordinary lock contention explicitly.
fn map_lock_result(result: Result<(), TryLockError>, name: &str) -> Result<(), RuntimeError> {
    match result {
        Ok(()) => Ok(()),
        Err(TryLockError::WouldBlock) => Err(RuntimeError::DatabaseInUse(name.to_string())),
        Err(TryLockError::Error(error)) => Err(error.into()),
    }
}

/// Verify an existing selected catalog instead of recreating missing metadata.
fn require_database_metadata(
    paths: &DatabasePaths,
    id: fq_common::database::DatabaseId,
) -> Result<(), RuntimeError> {
    let metadata = std::fs::metadata(&paths.metadata)?;
    if !metadata.is_file() {
        return Err(RuntimeError::Config(
            "database metadata is not a regular file".to_string(),
        ));
    }
    DatabaseCatalog::open(&paths.metadata.to_string_lossy())?.verify_identity(id)?;
    Ok(())
}

/// Remove a retired directory, allowing recovery of an already removed path.
fn remove_database_directory(path: &Path) -> Result<(), std::io::Error> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

/// Persist directory entry changes before publishing successful lifecycle completion.
fn sync_directory(path: &Path) -> Result<(), std::io::Error> {
    File::open(path)?.sync_all()
}

/// Refuse to recreate a lost root catalog while database directories still exist.
fn require_catalog_presence(root: &Path) -> Result<(), RuntimeError> {
    if root.join("server.sqlite").try_exists()? {
        return Ok(());
    }
    if std::fs::read_dir(root.join("databases"))?
        .next()
        .transpose()?
        .is_some()
    {
        return Err(RuntimeError::StateStoreLost(
            "server catalog is missing while database directories remain".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Allocate a fresh root independent of test execution order and prior runs.
    fn storage_root() -> PathBuf {
        std::env::temp_dir().join(format!("fq_database_storage_{}", DatabaseId::new()))
    }

    /// Directory ownership keeps data and metadata for different databases separate.
    #[test]
    fn separate_directories_and_drop_cleanup() {
        let root = storage_root();
        let manager = DatabaseManager::open(&root).expect("open manager");
        manager.create("project_a", "admin").expect("create A");
        manager.create("project_b", "admin").expect("create B");
        let project_a = manager.acquire("project_a").expect("acquire A");
        let project_b = manager.acquire("project_b").expect("acquire B");
        assert_ne!(project_a.paths.directory, project_b.paths.directory);
        let removed = project_a.paths.directory.clone();
        assert!(matches!(
            manager.drop_database("project_a"),
            Err(RuntimeError::DatabaseInUse(_))
        ));
        drop(project_a);
        manager.drop_database("project_a").expect("drop A");
        assert!(!removed.exists());
        assert!(project_b.paths.metadata.is_file());
        assert!(manager.acquire("project_a").is_err());
    }

    /// Bootstrap main is durable and protected from removal.
    #[test]
    fn main_is_bootstrapped_once_and_cannot_be_dropped() {
        let root = storage_root();
        let first = DatabaseManager::open(&root).expect("first open");
        let identity = first.acquire("main").expect("main").descriptor.id;
        assert!(first.drop_database("main").is_err());
        drop(first);
        let second = DatabaseManager::open(&root).expect("reopen");
        assert_eq!(
            second.acquire("main").expect("same main").descriptor.id,
            identity
        );
        assert!(second.acquire("missing").is_err());
    }

    /// A new same-name database cannot inherit paths or cached file identity.
    #[test]
    fn recreated_database_gets_another_directory() {
        let manager = DatabaseManager::open(&storage_root()).expect("open");
        let old = manager.create("project", "admin").expect("create");
        manager.drop_database("project").expect("drop");
        let new = manager.create("project", "admin").expect("recreate");
        assert_ne!(old.id, new.id);
    }

    /// An interrupted unpublished creation is cleaned up on restart.
    #[test]
    fn startup_recovers_incomplete_creation() {
        let root = storage_root();
        let manager = DatabaseManager::open(&root).expect("open");
        let record = manager
            .server
            .reserve_database(DatabaseId::new(), "partial", "admin")
            .expect("reserve");
        let paths = DatabasePaths::new(&root, record.id);
        std::fs::create_dir(&paths.directory).expect("partial directory");
        drop(manager);
        let reopened = DatabaseManager::open(&root).expect("recover");
        assert!(reopened
            .server
            .find_database("partial")
            .expect("lookup")
            .is_none());
        assert!(!paths.directory.exists());
    }

    /// A stale recovery snapshot never removes a database already published ready.
    #[test]
    fn recovery_rechecks_ready_state_under_lock() {
        let manager = DatabaseManager::open(&storage_root()).expect("open");
        let mut snapshot = manager.create("project", "admin").expect("create");
        snapshot.state = DatabaseState::Creating;
        manager.recover_record(&snapshot).expect("stale recovery");
        assert!(manager
            .acquire("project")
            .expect("still ready")
            .paths
            .metadata
            .is_file());
    }

    /// A bootstrapped root whose main registration vanished raises instead
    /// of silently recreating an empty main over orphaned directories.
    #[test]
    fn missing_main_registration_on_bootstrapped_root_raises() {
        let root = storage_root();
        let manager = DatabaseManager::open(&root).expect("open");
        let main = manager.acquire("main").expect("main");
        let id = main.descriptor.id;
        drop(main);
        manager.server.retire_database(id).expect("retire");
        manager.server.purge_database(id).expect("purge");
        drop(manager);
        assert!(matches!(
            DatabaseManager::open(&root),
            Err(RuntimeError::StateStoreLost(_))
        ));
    }

    /// An interrupted first bootstrap (main reserved, never published) is
    /// recovered and main is created on the next open.
    #[test]
    fn interrupted_first_bootstrap_recovers() {
        let root = storage_root();
        std::fs::create_dir_all(root.join("databases")).expect("databases dir");
        std::fs::create_dir_all(root.join("locks")).expect("locks dir");
        let server = ServerCatalog::open(&root.join("server.sqlite").to_string_lossy())
            .expect("server catalog");
        server
            .reserve_database(DatabaseId::new(), "main", "bootstrap")
            .expect("reserve main");
        drop(server);
        let manager = DatabaseManager::open(&root).expect("recover and bootstrap");
        assert!(manager.acquire("main").is_ok());
        assert!(manager.server.bootstrap_complete().expect("marker"));
    }

    /// Losing root metadata is fatal instead of silently creating a new namespace.
    #[test]
    fn root_catalog_loss_is_not_reinitialized() {
        let root = storage_root();
        let manager = DatabaseManager::open(&root).expect("open");
        drop(manager);
        std::fs::remove_file(root.join("server.sqlite")).expect("remove catalog");
        assert!(matches!(
            DatabaseManager::open(&root),
            Err(RuntimeError::StateStoreLost(_))
        ));
    }
}
