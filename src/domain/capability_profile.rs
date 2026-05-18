//! Capability profile aggregate and ref-only evidence types owned by identity.

use serde::{Deserialize, Serialize};
use serde_json::json;
use time::{OffsetDateTime, PrimitiveDateTime};

use crate::domain::shared::context::ActorContext;
use crate::domain::shared::ids::{CapabilityProfileId, GlobalMemberId};
use crate::error::IdentityError;

/// Ref-only pointer to external artifact evidence retained by identity.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ArtifactRef {
    /// Stable artifact identifier owned by the artifact domain.
    pub artifact_id: String,
    /// Artifact kind or collection identifier used for downstream routing.
    pub artifact_kind: String,
    /// Optional immutable version or revision reference.
    pub artifact_version: Option<String>,
}

impl ArtifactRef {
    /// Returns an error when the ref is missing required fields.
    pub fn validate(&self) -> Result<(), IdentityError> {
        if self.artifact_id.trim().is_empty() {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_INVALID_ARGUMENT",
                message: "artifact_id must not be blank".to_string(),
            });
        }
        if self.artifact_kind.trim().is_empty() {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_INVALID_ARGUMENT",
                message: "artifact_kind must not be blank".to_string(),
            });
        }

        Ok(())
    }
}

/// One persisted capability item retained as part of the member capability profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapabilityItem {
    /// Stable capability entry id scoped to the capability profile.
    pub capability_id: String,
    /// User-facing capability label.
    pub capability_name: String,
    /// Optional proficiency summary controlled by the caller.
    pub proficiency: Option<String>,
    /// Optional short notes describing the capability.
    pub notes: Option<String>,
}

impl CapabilityItem {
    /// Returns an error when the capability item is missing required fields.
    pub fn validate(&self) -> Result<(), IdentityError> {
        if self.capability_id.trim().is_empty() {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_INVALID_ARGUMENT",
                message: "capability_id must not be blank".to_string(),
            });
        }
        if self.capability_name.trim().is_empty() {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_INVALID_ARGUMENT",
                message: "capability_name must not be blank".to_string(),
            });
        }

        Ok(())
    }
}

/// Write-model aggregate that stores capability facts and ref-only evidence for one member.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CapabilityProfile {
    /// Stable capability profile id owned by identity.
    pub capability_profile_id: CapabilityProfileId,
    /// Member owning this capability profile.
    pub global_member_id: GlobalMemberId,
    /// Persisted capability items.
    pub capabilities: Vec<CapabilityItem>,
    /// Ref-only evidence used to support the capability items.
    pub evidence_refs: Vec<ArtifactRef>,
    /// Optimistic-lock version incremented after each successful update.
    pub version: i64,
    /// Timestamp when the profile was last updated.
    pub updated_at: PrimitiveDateTime,
}

/// Command payload used to replace one member capability profile.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UpdateCapabilityProfileCommand {
    /// Target member id.
    pub global_member_id: GlobalMemberId,
    /// Full capability set to persist.
    pub capabilities: Vec<CapabilityItem>,
    /// Ref-only evidence list supporting the capability set.
    pub evidence_refs: Vec<ArtifactRef>,
    /// Optional optimistic-lock version expected by the caller.
    pub expected_version: Option<i64>,
}

/// Summary returned by capability-profile write paths.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CapabilityProfileSummary {
    /// Stable capability profile id.
    pub capability_profile_id: CapabilityProfileId,
    /// Target member id.
    pub global_member_id: GlobalMemberId,
    /// Current capability items.
    pub capabilities: Vec<CapabilityItem>,
    /// Current ref-only evidence list.
    pub evidence_refs: Vec<ArtifactRef>,
    /// Current profile version.
    pub version: i64,
}

impl CapabilityProfile {
    /// Creates an empty capability profile for the provided member.
    pub fn create_for_member(
        global_member_id: GlobalMemberId,
        initial_items: Vec<CapabilityItem>,
        _actor: &ActorContext,
    ) -> Result<Self, IdentityError> {
        validate_capabilities(&initial_items)?;
        let now = current_timestamp();

        Ok(Self {
            capability_profile_id: CapabilityProfileId::new(format!(
                "capability-profile:{}",
                global_member_id.as_str()
            )),
            global_member_id,
            capabilities: initial_items,
            evidence_refs: Vec::new(),
            version: 0,
            updated_at: now,
        })
    }

    /// Replaces the profile capability items and evidence refs.
    pub fn replace_capabilities(
        &mut self,
        capabilities: Vec<CapabilityItem>,
        evidence_refs: Vec<ArtifactRef>,
        _actor: &ActorContext,
    ) -> Result<(), IdentityError> {
        validate_capabilities(&capabilities)?;
        validate_artifact_refs(&evidence_refs)?;

        self.capabilities = capabilities;
        self.evidence_refs = evidence_refs;
        self.version += 1;
        self.updated_at = current_timestamp();
        Ok(())
    }

    /// Returns the command-side summary for the current profile.
    pub fn summary(&self) -> CapabilityProfileSummary {
        CapabilityProfileSummary {
            capability_profile_id: self.capability_profile_id.clone(),
            global_member_id: self.global_member_id.clone(),
            capabilities: self.capabilities.clone(),
            evidence_refs: self.evidence_refs.clone(),
            version: self.version,
        }
    }

    /// Returns a projection-safe JSON summary of the current capability profile.
    pub fn summary_json(&self) -> serde_json::Value {
        json!({
            "capability_profile_id": self.capability_profile_id.as_str(),
            "items": self.capabilities,
            "evidence_refs": self.evidence_refs,
            "version": self.version,
        })
    }
}

fn validate_capabilities(items: &[CapabilityItem]) -> Result<(), IdentityError> {
    for item in items {
        item.validate()?;
    }

    Ok(())
}

fn validate_artifact_refs(refs: &[ArtifactRef]) -> Result<(), IdentityError> {
    for artifact_ref in refs {
        artifact_ref.validate()?;
    }

    Ok(())
}

fn current_timestamp() -> PrimitiveDateTime {
    let now = OffsetDateTime::now_utc();
    PrimitiveDateTime::new(now.date(), now.time())
}
