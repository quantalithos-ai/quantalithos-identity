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
        Self::from_role_definition_snapshot_ref(&snapshot, updated_at)
    }

    /// Creates a local role catalog entry from a borrowed method-library snapshot view.
    ///
    /// # Errors
    ///
    /// Returns an error when the upstream status cannot be mapped to the local status model.
    pub fn from_role_definition_snapshot_ref(
        snapshot: &RoleDefinitionSnapshot,
        updated_at: PrimitiveDateTime,
    ) -> Result<Self, IdentityError> {
        let status = status_from_snapshot(snapshot)?;

        Ok(Self {
            role_id: snapshot.role_id.clone(),
            role_name: snapshot.role_name.clone(),
            role_version: snapshot.role_version.clone(),
            source_ref_json: snapshot.source_ref.clone(),
            fingerprint: snapshot.fingerprint.clone(),
            status,
            updated_at,
        })
    }

    /// Applies one authoritative method-library snapshot onto the local index row.
    ///
    /// # Errors
    ///
    /// Returns an error when the upstream status cannot be mapped to the local status model.
    pub fn apply_snapshot(
        &mut self,
        snapshot: &RoleDefinitionSnapshot,
        updated_at: PrimitiveDateTime,
    ) -> Result<(), IdentityError> {
        self.role_name = snapshot.role_name.clone();
        self.role_version = snapshot.role_version.clone();
        self.source_ref_json = snapshot.source_ref.clone();
        self.fingerprint = snapshot.fingerprint.clone();
        self.status = status_from_snapshot(snapshot)?;
        self.updated_at = updated_at;
        Ok(())
    }

    /// Returns whether the local index row already matches the authoritative source snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when the upstream status cannot be mapped to the local status model.
    pub fn matches_snapshot(
        &self,
        snapshot: &RoleDefinitionSnapshot,
    ) -> Result<bool, IdentityError> {
        let status = status_from_snapshot(snapshot)?;
        Ok(self.role_id == snapshot.role_id
            && self.role_name == snapshot.role_name
            && self.role_version == snapshot.role_version
            && self.source_ref_json == snapshot.source_ref
            && self.fingerprint == snapshot.fingerprint
            && self.status == status)
    }

    /// Marks the local index row as deprecated while keeping the source reference snapshot.
    pub fn mark_deprecated(&mut self, updated_at: PrimitiveDateTime) {
        self.status = RoleCatalogStatus::Deprecated;
        self.updated_at = updated_at;
    }

    /// Marks the local index row as drifted after source reconciliation detected a mismatch.
    pub fn mark_source_drift(
        &mut self,
        fingerprint: impl Into<String>,
        updated_at: PrimitiveDateTime,
    ) {
        self.fingerprint = fingerprint.into();
        self.status = RoleCatalogStatus::SourceDrift;
        self.updated_at = updated_at;
    }

    /// Renames the local cached display name without touching the source role identifier.
    pub fn rename(&mut self, name: impl Into<String>, updated_at: PrimitiveDateTime) {
        self.role_name = name.into();
        self.updated_at = updated_at;
    }
}

fn status_from_snapshot(
    snapshot: &RoleDefinitionSnapshot,
) -> Result<RoleCatalogStatus, IdentityError> {
    RoleCatalogStatus::from_db(snapshot.status.as_str()).ok_or(IdentityError::PersistenceData {
        message: format!("unknown role catalog status `{}`", snapshot.status),
    })
}
