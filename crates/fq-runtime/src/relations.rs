//! Relation names and grant targets within the selected database.

use crate::Runtime;
use crate::RuntimeError;
use fq_common::database::{RelationKind, RelationName};
use fq_common::security::ObjectKey;

impl Runtime {
    /// Resolve a parser-normalized relation name without touching any foreign database.
    pub(crate) fn local_relation_name<'a>(
        &self,
        name: &'a RelationName,
    ) -> Result<&'a str, RuntimeError> {
        if name
            .database
            .as_deref()
            .is_some_and(|database| database != self.database_name())
        {
            return Err(RuntimeError::PermissionDenied(
                "cross-database relation references are not permitted".to_string(),
            ));
        }
        Ok(&name.name)
    }

    /// Resolve grants against a live local relation of the requested concrete category.
    pub(crate) fn relation_grant_target(
        &self,
        name: &RelationName,
        kind: RelationKind,
    ) -> Result<ObjectKey, RuntimeError> {
        let name = self.local_relation_name(name)?;
        let relation = self
            .database_catalog()?
            .find_relation(name)?
            .ok_or_else(|| fq_catalog::store::StoreError::RelationMissing(name.to_string()))?;
        if relation.kind != kind || relation.state != fq_common::database::RelationState::Ready {
            return Err(fq_catalog::store::StoreError::RelationMissing(name.to_string()).into());
        }
        Ok(match kind {
            RelationKind::View => ObjectKey::View {
                relation: relation.id,
            },
            RelationKind::MaterializedView => ObjectKey::MaterializedView {
                relation: relation.id,
            },
        })
    }
}
