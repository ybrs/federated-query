//! Durable database identities and filesystem locations derived from those IDs.

use std::fmt;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Define a distinct UUID-backed identity that cannot be confused with a name.
macro_rules! identity {
    ($name:ident) => {
        #[doc = concat!("A durable ", stringify!($name), " independent of SQL names.")]
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        pub struct $name(pub Uuid);

        impl $name {
            /// Allocate a fresh identity without reusing a deleted object's ID.
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }
        }

        impl Default for $name {
            /// Allocate a fresh identity for a newly constructed object.
            fn default() -> Self {
                Self::new()
            }
        }

        impl fmt::Display for $name {
            /// Render the canonical UUID used in metadata and safe path components.
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(formatter)
            }
        }

        impl FromStr for $name {
            type Err = uuid::Error;

            /// Parse a stored identity, rejecting malformed UUID data.
            fn from_str(text: &str) -> Result<Self, Self::Err> {
                Ok(Self(Uuid::parse_str(text)?))
            }
        }
    };
}

identity!(DatabaseId);
identity!(PrincipalId);
identity!(RelationId);

/// Whether a registered database may accept new runtime leases.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DatabaseState {
    Creating,
    Ready,
    Dropping,
}

impl DatabaseState {
    /// The exact state token stored in the server registry.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Creating => "creating",
            Self::Ready => "ready",
            Self::Dropping => "dropping",
        }
    }
}

/// One registered database; its directory depends on its ID, never its name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabaseDescriptor {
    pub id: DatabaseId,
    pub name: String,
    pub state: DatabaseState,
    pub created_at: String,
    pub created_by: String,
}

/// All durable and temporary storage owned by one database.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DatabasePaths {
    pub directory: PathBuf,
    pub metadata: PathBuf,
    pub materialized: PathBuf,
    pub scratch: PathBuf,
    pub lifecycle_lock: PathBuf,
    pub mutation_lock: PathBuf,
}

impl DatabasePaths {
    /// Derive owned locations using a validated identity as the only component.
    pub fn new(storage_root: &Path, id: DatabaseId) -> Self {
        let directory = storage_root.join("databases").join(id.to_string());
        Self {
            metadata: directory.join("metadata.sqlite"),
            materialized: directory.join("materialized"),
            scratch: directory.join("scratch"),
            lifecycle_lock: storage_root.join("locks").join(format!("{id}.lock")),
            mutation_lock: directory.join("mutation.lock"),
            directory,
        }
    }
}

/// A relation's storage and authorization category within one database namespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RelationKind {
    View,
    MaterializedView,
}

impl RelationKind {
    /// The exact durable kind token, independent of a relation's display name.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::View => "view",
            Self::MaterializedView => "materialized_view",
        }
    }

    /// Decode only a supported current relation category.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "view" => Some(Self::View),
            "materialized_view" => Some(Self::MaterializedView),
            _ => None,
        }
    }
}

/// A relation reservation is invisible until its publication transaction succeeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RelationState {
    Creating,
    Ready,
    Dropping,
}

impl RelationState {
    /// Render the exact state token stored in local metadata.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Creating => "creating",
            Self::Ready => "ready",
            Self::Dropping => "dropping",
        }
    }

    /// Reject unknown lifecycle tokens instead of treating them as live relations.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "creating" => Some(Self::Creating),
            "ready" => Some(Self::Ready),
            "dropping" => Some(Self::Dropping),
            _ => None,
        }
    }
}

/// A local relation identity retained across replacement but never across drop/recreate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationRecord {
    pub id: RelationId,
    pub name: String,
    pub kind: RelationKind,
    pub state: RelationState,
    pub revision: i64,
    pub created_by: String,
    pub created_at: String,
}

/// Parser-normalized SQL components, independent of any filesystem path.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RelationName {
    pub database: Option<String>,
    pub name: String,
}

impl fmt::Display for RelationName {
    /// Quote components so names containing dots remain distinguishable in diagnostics.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(database) = &self.database {
            write!(formatter, "\"{}\".", database.replace('"', "\"\""))?;
        }
        write!(formatter, "\"{}\"", self.name.replace('"', "\"\""))
    }
}

/// A stored relation's durable identity within its owning database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StoredRelationRef {
    pub database: DatabaseId,
    pub relation: RelationId,
}
