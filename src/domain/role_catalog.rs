//! Role catalog write-model entities used as local runtime role references.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::PrimitiveDateTime;

use crate::domain::shared::ids::RoleId;
use crate::error::IdentityError;
use crate::inbound::events::RoleDefinitionSnapshot;

/// Enumerates the states allowed for a local role catalog entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoleCatalogStatus {
    /// Entry is current and may be referenced by member write flows.
    Active,
    /// Entry is retained for reference but no longer recommended for use.
    Deprecated,
    /// Local fingerprint no longer matches the upstream source snapshot.
    SourceDrift,
}

impl RoleCatalogStatus {
    /// Parses a persisted status string into the strongly-typed enum.
    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "active" => Some(Self::Active),
            "deprecated" => Some(Self::Deprecated),
            "source_drift" => Some(Self::SourceDrift),
            _ => None,
        }
    }

    /// Returns the canonical database representation of the status enum.
    pub fn as_db(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Deprecated => "deprecated",
            Self::SourceDrift => "source_drift",
        }
    }
}

/// Represents a method-library role snapshot cached as an identity-local index row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RoleCatalogEntry {
    /// Stable method-library role id used as the local primary key.
    pub role_id: RoleId,
    /// Cached role display name consumed by query and validation flows.
    pub role_name: String,
    /// Upstream role version retained for compatibility and drift diagnostics.
    pub role_version: String,
    /// Upstream source reference snapshot stored as ref-only JSON.
    pub source_ref_json: Value,
    /// Fingerprint used to detect source drift against upstream snapshots.
    pub fingerprint: String,
    /// Current local index status.
    pub status: RoleCatalogStatus,
    /// Last successful synchronization timestamp.
    pub updated_at: PrimitiveDateTime,
}

impl RoleCatalogEntry {
    /// Creates a local role catalog entry from the minimal method-library snapshot view.
    ///
    /// # Errors
    ///
    /// Returns an error when the upstream status cannot be mapped to the local status model.
    pub fn from_role_definition_snapshot(
        snapshot: RoleDefinitionSnapshot,
        updated_at: PrimitiveDateTime,
    ) -> Result<Self, IdentityError> {
        let status = RoleCatalogStatus::from_db(snapshot.status.as_str()).ok_or(
            IdentityError::PersistenceData {
                message: format!("unknown role catalog status `{}`", snapshot.status),
            },
        )?;

        Ok(Self {
            role_id: snapshot.role_id,
            role_name: snapshot.role_name,
            role_version: snapshot.role_version,
            source_ref_json: snapshot.source_ref,
            fingerprint: snapshot.fingerprint,
            status,
            updated_at,
        })
    }
}
