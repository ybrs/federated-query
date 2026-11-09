//! Physical locations for integration fixtures using a real selected database.

use fq_common::database::DatabasePaths;
use std::path::Path;

/// Resolve main through the same registry and lease validation as production runtimes.
pub(super) fn main_paths(root: &Path) -> DatabasePaths {
    let manager = fq_runtime::DatabaseManager::open(root).expect("open database manager");
    manager.acquire("main").expect("main database").paths
}
