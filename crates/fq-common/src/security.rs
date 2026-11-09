//! Authentication mode is explicit and independent of bootstrap credentials.

use serde::{Deserialize, Serialize};

/// The server's fixed startup authentication policy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AuthMode {
    #[default]
    Trust,
    Scram,
}

/// The identity a database permission applies to, independent of display names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum GrantSubject {
    Public,
    User(crate::database::PrincipalId),
}

impl GrantSubject {
    /// Render an unambiguous durable subject key for the metadata store.
    pub fn storage_key(self) -> String {
        match self {
            Self::Public => "PUBLIC".to_string(),
            Self::User(id) => id.to_string(),
        }
    }
}

/// Database administration is distinct from global server superuser authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabasePrivilege {
    Connect,
    Admin,
}

impl DatabasePrivilege {
    /// The exact privilege token persisted by the server catalog.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Connect => "CONNECT",
            Self::Admin => "ADMIN",
        }
    }
}

/// A structured local SELECT target; identifier components never share a delimiter.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ObjectKey {
    Datasource {
        datasource: String,
    },
    Schema {
        datasource: String,
        schema: String,
    },
    Table {
        datasource: String,
        schema: String,
        table: String,
    },
    View {
        relation: crate::database::RelationId,
    },
    MaterializedView {
        relation: crate::database::RelationId,
    },
}

impl ObjectKey {
    /// The owning local relation, when a target is identified independently of its name.
    pub fn relation(&self) -> Option<(crate::database::RelationId, crate::database::RelationKind)> {
        use crate::database::RelationKind;
        match self {
            Self::View { relation } => Some((*relation, RelationKind::View)),
            Self::MaterializedView { relation } => {
                Some((*relation, RelationKind::MaterializedView))
            }
            Self::Datasource { .. } | Self::Schema { .. } | Self::Table { .. } => None,
        }
    }

    /// The SQL-facing target category for grant inspection.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Datasource { .. } => "datasource",
            Self::Schema { .. } => "schema",
            Self::Table { .. } => "table",
            Self::View { .. } => "view",
            Self::MaterializedView { .. } => "materialized_view",
        }
    }

    /// A display label; authorization always compares the structured target.
    pub fn label(&self) -> String {
        match self {
            Self::Datasource { datasource } => datasource.clone(),
            Self::Schema { datasource, schema } => format!("{datasource}.{schema}"),
            Self::Table {
                datasource,
                schema,
                table,
            } => format!("{datasource}.{schema}.{table}"),
            Self::View { relation } | Self::MaterializedView { relation } => relation.to_string(),
        }
    }
}
