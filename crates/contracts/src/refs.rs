//! Typed refs and shared markers for identity public contracts.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::errors::ContractError;

macro_rules! string_newtype {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Creates a new opaque typed value.
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            /// Returns the wrapped string.
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Consumes the wrapper and returns the inner string.
            pub fn into_inner(self) -> String {
                self.0
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self::new(value)
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self::new(value)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }
    };
}

macro_rules! validated_string_newtype {
    ($name:ident, $field:literal, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Creates a new validated opaque typed value.
            pub fn new(value: impl Into<String>) -> Result<Self, ContractError> {
                Ok(Self(ensure_non_empty($field, value)?))
            }

            /// Returns the wrapped string.
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Consumes the wrapper and returns the inner string.
            pub fn into_inner(self) -> String {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }
    };
}

fn ensure_non_empty(field: &str, value: impl Into<String>) -> Result<String, ContractError> {
    let value = value.into();
    if value.trim().is_empty() {
        return Err(ContractError::invalid_value(
            field,
            "value must be non-empty",
        ));
    }

    Ok(value)
}

fn ensure_source_owner(
    field: &str,
    source_ref: &IdentitySourceRef,
    expected: &[IdentitySourceOwner],
) -> Result<(), ContractError> {
    if expected.iter().any(|owner| *owner == source_ref.owner()) {
        return Ok(());
    }

    Err(ContractError::invalid_value(
        field,
        format!(
            "source owner {:?} is not allowed for this typed ref",
            source_ref.owner()
        ),
    ))
}

validated_string_newtype!(
    ExternalSourceRef,
    "external_source_ref",
    "Opaque external source reference used by identity typed refs."
);
validated_string_newtype!(
    GlobalMemberId,
    "global_member_id",
    "Stable opaque identifier for an identity global member."
);
validated_string_newtype!(
    RoleCapabilitySummaryId,
    "role_capability_summary_id",
    "Stable opaque identifier for an identity-owned role capability summary."
);
validated_string_newtype!(
    RoleCapabilitySourceSnapshotId,
    "role_capability_source_snapshot_id",
    "Stable opaque identifier for an identity-owned role capability source snapshot."
);
validated_string_newtype!(
    CareerRecordId,
    "career_record_id",
    "Stable opaque identifier for an identity career record."
);
validated_string_newtype!(
    MemoryReferenceId,
    "memory_reference_id",
    "Stable opaque identifier for an identity memory reference relation."
);
validated_string_newtype!(
    ProjectionStateId,
    "projection_state_id",
    "Stable opaque identifier for an identity projection state."
);
validated_string_newtype!(
    ReferenceResolutionStateId,
    "reference_resolution_state_id",
    "Stable opaque identifier for an identity reference resolution state."
);

/// Entry channel that explains why identity code is executing.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityOperationChannel {
    /// Explicit command path that may write identity truth after all guards pass.
    Command,
    /// Query or read path that must not mutate truth.
    Query,
    /// Inbound consumer path from a subscribed event source.
    Consumer,
    /// Operations or maintenance job path.
    Job,
    /// Handoff callback or receipt path.
    HandoffCallback,
    /// Projection rebuild or read-model maintenance path.
    ProjectionMaintenance,
}

impl IdentityOperationChannel {
    /// Returns whether this channel may enter a core truth write guard.
    pub fn allows_core_truth_write(&self) -> bool {
        matches!(self, Self::Command | Self::Consumer)
    }

    /// Returns whether this channel is read-only by design.
    pub fn is_read_only(&self) -> bool {
        matches!(self, Self::Query | Self::ProjectionMaintenance)
    }

    /// Returns whether the entrypoint must supply actor or trace metadata.
    pub fn requires_entry_metadata(&self) -> bool {
        true
    }
}

/// Owner class for a body-free identity source reference.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentitySourceOwner {
    /// User, account, or credential source used only as evidence or creation source.
    Account,
    /// Runtime or execution-side source.
    Runtime,
    /// Work or project participation source.
    Work,
    /// Method-library role or capability source.
    MethodLibrary,
    /// Memory or archive source.
    MemoryArchive,
    /// Governance basis or decision source.
    Governance,
    /// Identity-owned source produced by accepted identity truth.
    Identity,
}

/// Typed reference to an identity global member truth object.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct GlobalMemberRef {
    /// Stable member identifier.
    pub member_id: GlobalMemberId,
}

impl GlobalMemberRef {
    /// Creates a typed ref from a validated member identifier.
    pub fn from_id(member_id: GlobalMemberId) -> Self {
        Self { member_id }
    }

    /// Returns whether both refs refer to the same member.
    pub fn same_member(&self, other: &GlobalMemberRef) -> bool {
        self.member_id.same_id(&other.member_id)
    }

    /// Returns the contained member identifier.
    pub fn id(&self) -> &GlobalMemberId {
        &self.member_id
    }
}

impl GlobalMemberId {
    /// Returns whether both typed identifiers are equal.
    pub fn same_id(&self, other: &GlobalMemberId) -> bool {
        self == other
    }
}

/// Typed reference to an identity projection state.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ProjectionStateRef {
    /// Stable projection state identifier.
    pub projection_state_id: ProjectionStateId,
}

impl ProjectionStateRef {
    /// Creates a typed ref from a validated projection state identifier.
    pub fn from_id(projection_state_id: ProjectionStateId) -> Self {
        Self {
            projection_state_id,
        }
    }
}

/// Typed reference to an identity reference resolution state.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct ReferenceResolutionStateRef {
    /// Stable reference resolution state identifier.
    pub resolution_state_id: ReferenceResolutionStateId,
}

impl ReferenceResolutionStateRef {
    /// Creates a typed ref from a validated reference resolution state identifier.
    pub fn from_id(resolution_state_id: ReferenceResolutionStateId) -> Self {
        Self {
            resolution_state_id,
        }
    }
}

/// Body-free reference to the source that caused or supports an identity fact.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct IdentitySourceRef {
    /// Source owner or system class.
    pub source_owner: IdentitySourceOwner,
    /// Opaque source-side reference.
    pub external_ref: ExternalSourceRef,
}

impl IdentitySourceRef {
    /// Creates a new body-free source ref.
    pub fn new(
        source_owner: IdentitySourceOwner,
        external_ref: ExternalSourceRef,
    ) -> Result<Self, ContractError> {
        Ok(Self {
            source_owner,
            external_ref,
        })
    }

    /// Returns the source owner.
    pub fn owner(&self) -> IdentitySourceOwner {
        self.source_owner
    }

    /// Returns whether both refs describe the same source.
    pub fn same_source(&self, other: &IdentitySourceRef) -> bool {
        self.source_owner == other.source_owner && self.external_ref == other.external_ref
    }
}

/// Body-free reason reference for holding an identity member anchor.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct IdentityAnchorReasonRef {
    /// Reason category for the anchor hold.
    pub reason_kind: IdentityAnchorReasonKind,
    /// Opaque reason source reference.
    pub source_ref: IdentitySourceRef,
}

impl IdentityAnchorReasonRef {
    /// Creates a new anchor-hold reason ref.
    pub fn new(
        reason_kind: IdentityAnchorReasonKind,
        source_ref: IdentitySourceRef,
    ) -> Result<Self, ContractError> {
        Ok(Self {
            reason_kind,
            source_ref,
        })
    }

    /// Returns whether this reason can justify a tombstone hold.
    pub fn supports_tombstone_hold(&self) -> bool {
        matches!(
            self.reason_kind,
            IdentityAnchorReasonKind::Tombstoned | IdentityAnchorReasonKind::GovernanceHold
        )
    }

    /// Returns whether both reasons are the same.
    pub fn same_reason(&self, other: &IdentityAnchorReasonRef) -> bool {
        self.reason_kind == other.reason_kind && self.source_ref.same_source(&other.source_ref)
    }
}

/// Category of reason for keeping a member anchor permanently occupied.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityAnchorReasonKind {
    /// Anchor is held because the member was retired.
    Retired,
    /// Anchor is held because the member was tombstoned.
    Tombstoned,
    /// Anchor is held because the source identity was superseded but must remain non-reusable.
    SupersededHold,
    /// Anchor is held by governance or compliance restriction.
    GovernanceHold,
}

/// Body-free reason reference for an identity lifecycle transition.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct LifecycleReasonRef {
    /// Reason category for the lifecycle transition.
    pub reason_kind: LifecycleReasonKind,
    /// Opaque reason source reference.
    pub source_ref: IdentitySourceRef,
}

impl LifecycleReasonRef {
    /// Creates a new lifecycle reason ref.
    pub fn new(
        reason_kind: LifecycleReasonKind,
        source_ref: IdentitySourceRef,
    ) -> Result<Self, ContractError> {
        Ok(Self {
            reason_kind,
            source_ref,
        })
    }

    /// Returns whether the reason is a terminal lifecycle reason candidate.
    pub fn is_terminal_reason(&self) -> bool {
        matches!(
            self.reason_kind,
            LifecycleReasonKind::Retirement
                | LifecycleReasonKind::Tombstone
                | LifecycleReasonKind::GovernanceBasis
        )
    }

    /// Returns whether both reasons are the same.
    pub fn same_reason(&self, other: &LifecycleReasonRef) -> bool {
        self.reason_kind == other.reason_kind && self.source_ref.same_source(&other.source_ref)
    }
}

/// Category of reason for changing a member lifecycle state.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleReasonKind {
    /// Initial lifecycle created together with the member anchor.
    InitialProvisioned,
    /// Member was manually paused.
    ManualPause,
    /// Member was manually resumed.
    ManualResume,
    /// Member was retired.
    Retirement,
    /// Member was tombstoned.
    Tombstone,
    /// Lifecycle changed because a governance basis required it.
    GovernanceBasis,
    /// Lifecycle changed because an external source became invalid or unavailable.
    SourceInvalidated,
}

/// Body-free risk classification reference for a lifecycle action.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct LifecycleRiskRef {
    /// Risk class of the lifecycle action.
    pub risk_kind: LifecycleRiskKind,
    /// Source of the risk classification.
    pub source_ref: IdentitySourceRef,
}

impl LifecycleRiskRef {
    /// Creates a new lifecycle risk ref.
    pub fn new(
        risk_kind: LifecycleRiskKind,
        source_ref: IdentitySourceRef,
    ) -> Result<Self, ContractError> {
        Ok(Self {
            risk_kind,
            source_ref,
        })
    }

    /// Returns whether the risk class requires a governance basis.
    pub fn requires_governance_basis(&self) -> bool {
        matches!(
            self.risk_kind,
            LifecycleRiskKind::High | LifecycleRiskKind::Critical
        )
    }

    /// Returns whether both risks are the same.
    pub fn same_risk(&self, other: &LifecycleRiskRef) -> bool {
        self.risk_kind == other.risk_kind && self.source_ref.same_source(&other.source_ref)
    }
}

/// Risk class for identity lifecycle actions.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleRiskKind {
    /// Low-risk lifecycle action that does not require governance basis.
    Low,
    /// Medium-risk action that may require additional validation.
    Medium,
    /// High-risk action that requires governance basis before acceptance.
    High,
    /// Critical action that requires governance basis and explicit terminal handling.
    Critical,
}

/// Body-free reference to a governance basis used by identity.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct GovernanceBasisRef {
    /// Governance basis category.
    pub basis_kind: GovernanceBasisKind,
    /// Opaque governance-side reference.
    pub external_ref: ExternalSourceRef,
}

impl GovernanceBasisRef {
    /// Creates a new governance basis ref.
    pub fn new(
        basis_kind: GovernanceBasisKind,
        external_ref: ExternalSourceRef,
    ) -> Result<Self, ContractError> {
        Ok(Self {
            basis_kind,
            external_ref,
        })
    }

    /// Returns whether both basis refs are the same.
    pub fn same_basis(&self, other: &GovernanceBasisRef) -> bool {
        self.basis_kind == other.basis_kind && self.external_ref == other.external_ref
    }

    /// Converts the basis ref into a body-free governance source ref.
    pub fn to_source_ref(&self) -> IdentitySourceRef {
        IdentitySourceRef {
            source_owner: IdentitySourceOwner::Governance,
            external_ref: self.external_ref.clone(),
        }
    }
}

/// Category of governance basis referenced by identity.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GovernanceBasisKind {
    /// Governance gate or decision basis.
    GateDecision,
    /// Approval or responsibility-chain basis.
    Approval,
    /// Policy or shared-rule basis.
    Policy,
    /// Compliance or control basis.
    ComplianceControl,
    /// Manually recorded governance exception.
    GovernanceException,
}

/// Body-free resolver summary for a governance basis referenced by identity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GovernanceBasisSummary {
    /// Governance basis ref that was resolved.
    pub basis_ref: GovernanceBasisRef,
    /// Resolution state returned by the governance basis resolver.
    pub basis_state: GovernanceBasisState,
    /// Risk class that the basis is allowed to support.
    pub supports_risk_ref: Option<LifecycleRiskRef>,
}

impl GovernanceBasisSummary {
    /// Creates a body-free basis summary from resolver output.
    pub fn from_resolver(
        basis_ref: GovernanceBasisRef,
        basis_state: GovernanceBasisState,
        supports_risk_ref: Option<LifecycleRiskRef>,
    ) -> Self {
        Self {
            basis_ref,
            basis_state,
            supports_risk_ref,
        }
    }

    /// Returns whether the basis is valid for the requested risk class.
    pub fn is_valid_for(&self, risk_ref: &LifecycleRiskRef) -> bool {
        self.basis_state == GovernanceBasisState::Valid
            && self
                .supports_risk_ref
                .as_ref()
                .is_some_and(|supported| supported.same_risk(risk_ref))
    }

    /// Returns whether the summary should be rechecked before reuse.
    pub fn requires_recheck(&self) -> bool {
        matches!(
            self.basis_state,
            GovernanceBasisState::Stale | GovernanceBasisState::Unavailable
        )
    }
}

/// Resolution state for a governance basis summary.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GovernanceBasisState {
    /// Basis exists and can be used for the requested action class.
    Valid,
    /// Basis exists but is stale and must not be silently accepted as current.
    Stale,
    /// Basis cannot be resolved because governance dependency is unavailable.
    Unavailable,
    /// Basis was resolved but does not authorize the requested action class.
    InvalidForAction,
    /// Basis ref does not point to an existing governance basis.
    NotFound,
}

/// Typed reference to an identity role capability summary truth object.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct RoleCapabilitySummaryRef {
    /// Stable summary identifier.
    pub summary_id: RoleCapabilitySummaryId,
}

impl RoleCapabilitySummaryRef {
    /// Creates a typed ref from a validated summary identifier.
    pub fn from_id(summary_id: RoleCapabilitySummaryId) -> Self {
        Self { summary_id }
    }

    /// Returns whether both refs refer to the same summary.
    pub fn same_summary(&self, other: &RoleCapabilitySummaryRef) -> bool {
        self.summary_id == other.summary_id
    }
}

/// Typed reference to an identity role capability source snapshot.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct RoleCapabilitySourceSnapshotRef {
    /// Stable source snapshot identifier.
    pub snapshot_id: RoleCapabilitySourceSnapshotId,
}

impl RoleCapabilitySourceSnapshotRef {
    /// Creates a typed ref from a validated source snapshot identifier.
    pub fn from_id(snapshot_id: RoleCapabilitySourceSnapshotId) -> Self {
        Self { snapshot_id }
    }

    /// Returns whether both refs refer to the same source snapshot.
    pub fn same_snapshot(&self, other: &RoleCapabilitySourceSnapshotRef) -> bool {
        self.snapshot_id == other.snapshot_id
    }
}

/// Role or capability source category owned outside identity.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoleCapabilitySourceKind {
    /// Method-library role definition source.
    RoleDefinition,
    /// Method-library capability definition source.
    CapabilityDefinition,
    /// Method-library bundle that contains both role and capability markers.
    RoleCapabilityBundle,
    /// Method-library source profile or catalog entry used only as a body-free summary source.
    MethodSourceProfile,
}

impl RoleCapabilitySourceKind {
    /// Returns whether the source category may be used as a role source.
    pub fn supports_role(&self) -> bool {
        matches!(
            self,
            Self::RoleDefinition | Self::RoleCapabilityBundle | Self::MethodSourceProfile
        )
    }

    /// Returns whether the source category may be used as a capability source.
    pub fn supports_capability(&self) -> bool {
        matches!(
            self,
            Self::CapabilityDefinition | Self::RoleCapabilityBundle | Self::MethodSourceProfile
        )
    }

    /// Returns whether the source category is method-owned.
    pub fn is_method_owned(&self) -> bool {
        true
    }
}

/// Body-free canonical source ref for role or capability material.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct RoleCapabilitySourceRef {
    /// Source category.
    pub source_kind: RoleCapabilitySourceKind,
    /// Method-library owned body-free source ref.
    pub source_ref: IdentitySourceRef,
}

impl RoleCapabilitySourceRef {
    /// Creates a new canonical source ref.
    pub fn new(
        source_kind: RoleCapabilitySourceKind,
        source_ref: IdentitySourceRef,
    ) -> Result<Self, ContractError> {
        ensure_source_owner(
            "role_capability_source_ref",
            &source_ref,
            &[IdentitySourceOwner::MethodLibrary],
        )?;

        Ok(Self {
            source_kind,
            source_ref,
        })
    }

    /// Returns whether both refs describe the same source.
    pub fn same_source(&self, other: &RoleCapabilitySourceRef) -> bool {
        self.source_kind == other.source_kind && self.source_ref.same_source(&other.source_ref)
    }

    /// Returns whether this source can be wrapped as a role source.
    pub fn supports_role(&self) -> bool {
        self.source_kind.supports_role()
    }

    /// Returns whether this source can be wrapped as a capability source.
    pub fn supports_capability(&self) -> bool {
        self.source_kind.supports_capability()
    }
}

/// Body-free role source ref accepted by identity role capability summary.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct RoleSourceRef {
    /// Canonical role-capability source ref that supports role usage.
    pub source_ref: RoleCapabilitySourceRef,
}

impl RoleSourceRef {
    /// Wraps a canonical source as a role source.
    pub fn from_source(source_ref: RoleCapabilitySourceRef) -> Result<Self, ContractError> {
        if !source_ref.supports_role() {
            return Err(ContractError::invalid_value(
                "role_source_ref",
                "source kind does not support role usage",
            ));
        }

        Ok(Self { source_ref })
    }

    /// Returns the canonical source ref.
    pub fn canonical_source(&self) -> &RoleCapabilitySourceRef {
        &self.source_ref
    }

    /// Returns whether both role sources are equal.
    pub fn same_role_source(&self, other: &RoleSourceRef) -> bool {
        self.source_ref.same_source(&other.source_ref)
    }
}

/// Body-free capability source ref accepted by identity role capability summary.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct CapabilitySourceRef {
    /// Canonical role-capability source ref that supports capability usage.
    pub source_ref: RoleCapabilitySourceRef,
}

impl CapabilitySourceRef {
    /// Wraps a canonical source as a capability source.
    pub fn from_source(source_ref: RoleCapabilitySourceRef) -> Result<Self, ContractError> {
        if !source_ref.supports_capability() {
            return Err(ContractError::invalid_value(
                "capability_source_ref",
                "source kind does not support capability usage",
            ));
        }

        Ok(Self { source_ref })
    }

    /// Returns the canonical source ref.
    pub fn canonical_source(&self) -> &RoleCapabilitySourceRef {
        &self.source_ref
    }

    /// Returns whether both capability sources are equal.
    pub fn same_capability_source(&self, other: &CapabilitySourceRef) -> bool {
        self.source_ref.same_source(&other.source_ref)
    }
}

/// Opaque source-side version marker for role or capability source material.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct RoleCapabilitySourceVersionRef {
    /// Source this version belongs to.
    pub source_ref: RoleCapabilitySourceRef,
    /// Opaque source-side version token.
    pub version_token: String,
}

impl RoleCapabilitySourceVersionRef {
    /// Creates a new source version ref.
    pub fn new(
        source_ref: RoleCapabilitySourceRef,
        version_token: impl Into<String>,
    ) -> Result<Self, ContractError> {
        Ok(Self {
            source_ref,
            version_token: ensure_non_empty(
                "role_capability_source_version_ref.version_token",
                version_token,
            )?,
        })
    }

    /// Returns whether the version belongs to the provided source.
    pub fn belongs_to(&self, source_ref: &RoleCapabilitySourceRef) -> bool {
        self.source_ref.same_source(source_ref)
    }

    /// Returns whether both version refs are equal.
    pub fn same_version(&self, other: &RoleCapabilitySourceVersionRef) -> bool {
        self.version_token == other.version_token && self.source_ref.same_source(&other.source_ref)
    }
}

/// Body-free evidence category for a capability assertion.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapabilityEvidenceKind {
    /// Evidence from a method-library artifact or catalog marker.
    MethodArtifact,
    /// Evidence from governance-approved summary or basis.
    GovernanceBasis,
    /// Evidence from work participation summary.
    WorkParticipationSummary,
    /// Evidence from an explicit identity-side safe marker.
    IdentitySafeMarker,
}

/// Body-free evidence reference for a role or capability summary.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct CapabilityEvidenceRef {
    /// Evidence category.
    pub evidence_kind: CapabilityEvidenceKind,
    /// Opaque evidence source ref.
    pub source_ref: IdentitySourceRef,
}

impl CapabilityEvidenceRef {
    /// Creates a new evidence ref.
    pub fn new(
        evidence_kind: CapabilityEvidenceKind,
        source_ref: IdentitySourceRef,
    ) -> Result<Self, ContractError> {
        let expected_owners: &[IdentitySourceOwner] = match evidence_kind {
            CapabilityEvidenceKind::MethodArtifact => &[IdentitySourceOwner::MethodLibrary],
            CapabilityEvidenceKind::GovernanceBasis => &[IdentitySourceOwner::Governance],
            CapabilityEvidenceKind::WorkParticipationSummary => &[IdentitySourceOwner::Work],
            CapabilityEvidenceKind::IdentitySafeMarker => &[IdentitySourceOwner::Identity],
        };

        ensure_source_owner("capability_evidence_ref", &source_ref, expected_owners)?;

        Ok(Self {
            evidence_kind,
            source_ref,
        })
    }

    /// Returns whether both evidence refs are equal.
    pub fn same_evidence(&self, other: &CapabilityEvidenceRef) -> bool {
        self.evidence_kind == other.evidence_kind && self.source_ref.same_source(&other.source_ref)
    }

    /// Returns whether the evidence ref is body-free.
    pub fn is_body_free(&self) -> bool {
        true
    }
}

/// Body-free reference to a redaction-safe role or capability summary.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct RoleCapabilitySafeSummaryRef {
    /// Source this safe summary describes.
    pub source_ref: RoleCapabilitySourceRef,
    /// Opaque safe summary marker.
    pub safe_summary_token: String,
}

impl RoleCapabilitySafeSummaryRef {
    /// Creates a new safe summary ref.
    pub fn new(
        source_ref: RoleCapabilitySourceRef,
        safe_summary_token: impl Into<String>,
    ) -> Result<Self, ContractError> {
        Ok(Self {
            source_ref,
            safe_summary_token: ensure_non_empty(
                "role_capability_safe_summary_ref.safe_summary_token",
                safe_summary_token,
            )?,
        })
    }

    /// Returns whether the safe summary belongs to the provided source.
    pub fn belongs_to_source(&self, source_ref: &RoleCapabilitySourceRef) -> bool {
        self.source_ref.same_source(source_ref)
    }

    /// Returns whether both safe summary refs are equal.
    pub fn same_safe_summary(&self, other: &RoleCapabilitySafeSummaryRef) -> bool {
        self.safe_summary_token == other.safe_summary_token
            && self.source_ref.same_source(&other.source_ref)
    }
}

/// Body-free reason reference for a role or capability summary change.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct RoleCapabilityChangeReasonRef {
    /// Reason kind for the role or capability change.
    pub reason_kind: RoleCapabilityChangeReasonKind,
    /// Body-free source that explains where the reason came from.
    pub source_ref: IdentitySourceRef,
}

impl RoleCapabilityChangeReasonRef {
    /// Creates a new change reason ref.
    pub fn new(
        reason_kind: RoleCapabilityChangeReasonKind,
        source_ref: IdentitySourceRef,
    ) -> Result<Self, ContractError> {
        Ok(Self {
            reason_kind,
            source_ref,
        })
    }

    /// Returns whether the reason is source-driven.
    pub fn is_source_driven(&self) -> bool {
        matches!(
            self.reason_kind,
            RoleCapabilityChangeReasonKind::SourceChanged
                | RoleCapabilityChangeReasonKind::SourceUnavailable
        )
    }

    /// Returns whether the reason implies reconciliation trace or report follow-up.
    pub fn requires_reconciliation_trace(&self) -> bool {
        self.reason_kind == RoleCapabilityChangeReasonKind::ReconciliationCorrection
    }
}

/// Reason category for role or capability summary changes.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoleCapabilityChangeReasonKind {
    /// Explicit member summary maintenance command.
    ManualSummaryMaintenance,
    /// Method-library source was changed.
    SourceChanged,
    /// Source became unavailable or unrecognized.
    SourceUnavailable,
    /// Reconciliation found drift that requires a controlled update.
    ReconciliationCorrection,
    /// Legacy or migration-safe summary import.
    MigrationImport,
}

/// Body-free material category for role or capability summary changes.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RoleCapabilityChangeMaterialKind {
    /// Safe summary marker only.
    SafeSummaryMarker,
    /// Source reference and source version marker only.
    SourceVersionMarker,
    /// Evidence references only.
    EvidenceRefsOnly,
    /// Forbidden definition body was presented and must be rejected.
    ForbiddenDefinitionBody,
    /// Forbidden method body was presented and must be rejected.
    ForbiddenMethodBody,
    /// Forbidden evidence or artifact body was presented and must be rejected.
    ForbiddenEvidenceBody,
    /// Automatic scoring or performance inference material was presented and must be rejected.
    ForbiddenAutomaticScoring,
}

/// Material marker used by policy to block forbidden role or capability payloads.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RoleCapabilityChangeMaterialMarker {
    /// Material category.
    pub material_kind: RoleCapabilityChangeMaterialKind,
    /// Optional source marker that supplied the material.
    pub source_ref: Option<IdentitySourceRef>,
}

impl RoleCapabilityChangeMaterialMarker {
    /// Creates a new material marker.
    pub fn new(
        material_kind: RoleCapabilityChangeMaterialKind,
        source_ref: Option<IdentitySourceRef>,
    ) -> Self {
        Self {
            material_kind,
            source_ref,
        }
    }

    /// Returns whether the material category is forbidden.
    pub fn is_forbidden(&self) -> bool {
        matches!(
            self.material_kind,
            RoleCapabilityChangeMaterialKind::ForbiddenDefinitionBody
                | RoleCapabilityChangeMaterialKind::ForbiddenMethodBody
                | RoleCapabilityChangeMaterialKind::ForbiddenEvidenceBody
                | RoleCapabilityChangeMaterialKind::ForbiddenAutomaticScoring
        )
    }

    /// Returns whether the material only carries body-free markers.
    pub fn is_safe_marker_only(&self) -> bool {
        matches!(
            self.material_kind,
            RoleCapabilityChangeMaterialKind::SafeSummaryMarker
                | RoleCapabilityChangeMaterialKind::SourceVersionMarker
                | RoleCapabilityChangeMaterialKind::EvidenceRefsOnly
        )
    }

    /// Returns a stable rejection reason code for forbidden material.
    pub fn rejection_reason_code(&self) -> Option<&'static str> {
        match self.material_kind {
            RoleCapabilityChangeMaterialKind::ForbiddenDefinitionBody => {
                Some("forbidden_definition_body")
            }
            RoleCapabilityChangeMaterialKind::ForbiddenMethodBody => Some("forbidden_method_body"),
            RoleCapabilityChangeMaterialKind::ForbiddenEvidenceBody => {
                Some("forbidden_evidence_body")
            }
            RoleCapabilityChangeMaterialKind::ForbiddenAutomaticScoring => {
                Some("forbidden_automatic_scoring")
            }
            _ => None,
        }
    }
}

/// Typed reference to an identity-owned career record.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct CareerRecordRef {
    /// Stable career record identifier.
    pub record_id: CareerRecordId,
}

impl CareerRecordRef {
    /// Creates a typed ref from a validated career record identifier.
    pub fn from_id(record_id: CareerRecordId) -> Self {
        Self { record_id }
    }

    /// Returns whether both refs refer to the same career record.
    pub fn same_record(&self, other: &CareerRecordRef) -> bool {
        self.record_id == other.record_id
    }
}

/// Body-free reference to a work-owned project participation source.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct ProjectParticipationRef {
    /// Work-owned source ref for project participation.
    pub source_ref: IdentitySourceRef,
}

impl ProjectParticipationRef {
    /// Creates a typed project participation ref from a work-owned source ref.
    pub fn from_work_source(source_ref: IdentitySourceRef) -> Result<Self, ContractError> {
        ensure_source_owner(
            "project_participation_ref",
            &source_ref,
            &[IdentitySourceOwner::Work],
        )?;

        Ok(Self { source_ref })
    }
}

/// Work source category used by identity career append.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkSourceKind {
    /// Accepted project participation fact owned by work.
    ProjectParticipationAccepted,
    /// Work-side correction or replacement marker.
    WorkCorrection,
    /// Migration-safe work participation import.
    MigrationImport,
    /// Work participation source that requires review before accepted career append.
    PendingReviewMarker,
}

/// Body-free work source ref used by career append and correction.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct WorkSourceRef {
    /// Work source category.
    pub source_kind: WorkSourceKind,
    /// Work-owned body-free source reference.
    pub source_ref: IdentitySourceRef,
}

impl WorkSourceRef {
    /// Creates a new work source ref.
    pub fn new(
        source_kind: WorkSourceKind,
        source_ref: IdentitySourceRef,
    ) -> Result<Self, ContractError> {
        ensure_source_owner("work_source_ref", &source_ref, &[IdentitySourceOwner::Work])?;

        Ok(Self {
            source_kind,
            source_ref,
        })
    }

    /// Returns whether both work sources are the same.
    pub fn same_source(&self, other: &WorkSourceRef) -> bool {
        self.source_kind == other.source_kind && self.source_ref.same_source(&other.source_ref)
    }

    /// Returns whether the source is a pending-review marker.
    pub fn is_pending_review_marker(&self) -> bool {
        self.source_kind == WorkSourceKind::PendingReviewMarker
    }
}

/// Stable source marker used to prevent duplicate career records for the same work source.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct CareerSourceMarkerRef {
    /// Member that the source marker is associated with.
    pub member_ref: GlobalMemberRef,
    /// Work source represented by this marker.
    pub work_source_ref: WorkSourceRef,
    /// Opaque source-side or resolver-provided marker token.
    pub marker_token: String,
}

impl CareerSourceMarkerRef {
    /// Creates a new career source marker.
    pub fn new(
        member_ref: GlobalMemberRef,
        work_source_ref: WorkSourceRef,
        marker_token: impl Into<String>,
    ) -> Result<Self, ContractError> {
        Ok(Self {
            member_ref,
            work_source_ref,
            marker_token: ensure_non_empty("career_source_marker_ref.marker_token", marker_token)?,
        })
    }

    /// Returns whether both source markers are the same.
    pub fn same_marker(&self, other: &CareerSourceMarkerRef) -> bool {
        self.member_ref.same_member(&other.member_ref)
            && self.work_source_ref.same_source(&other.work_source_ref)
            && self.marker_token == other.marker_token
    }
}

/// Body-free reference to a redaction-safe career summary.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct CareerSafeSummaryRef {
    /// Work source this safe summary describes.
    pub work_source_ref: WorkSourceRef,
    /// Opaque safe summary marker.
    pub safe_summary_token: String,
}

impl CareerSafeSummaryRef {
    /// Creates a new safe summary ref.
    pub fn new(
        work_source_ref: WorkSourceRef,
        safe_summary_token: impl Into<String>,
    ) -> Result<Self, ContractError> {
        Ok(Self {
            work_source_ref,
            safe_summary_token: ensure_non_empty(
                "career_safe_summary_ref.safe_summary_token",
                safe_summary_token,
            )?,
        })
    }

    /// Returns whether the safe summary belongs to the provided work source.
    pub fn belongs_to_source(&self, work_source_ref: &WorkSourceRef) -> bool {
        self.work_source_ref.same_source(work_source_ref)
    }
}

/// Body-free reason reference for a career append or correction.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct CareerAppendReasonRef {
    /// Reason category for the career append.
    pub reason_kind: CareerAppendReasonKind,
    /// Body-free reason source.
    pub source_ref: IdentitySourceRef,
}

impl CareerAppendReasonRef {
    /// Creates a new career append reason ref.
    pub fn new(
        reason_kind: CareerAppendReasonKind,
        source_ref: IdentitySourceRef,
    ) -> Result<Self, ContractError> {
        Ok(Self {
            reason_kind,
            source_ref,
        })
    }

    /// Returns whether the reason describes a correction append.
    pub fn is_correction(&self) -> bool {
        self.reason_kind == CareerAppendReasonKind::CorrectionAppend
    }

    /// Returns whether the reason is driven by a source-side event or import.
    pub fn is_source_driven(&self) -> bool {
        matches!(
            self.reason_kind,
            CareerAppendReasonKind::WorkParticipationAccepted
                | CareerAppendReasonKind::MigrationImport
                | CareerAppendReasonKind::SourcePendingReview
        )
    }
}

/// Reason category for career history append.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CareerAppendReasonKind {
    /// Explicit career append command.
    ManualAppend,
    /// Work participation accepted event.
    WorkParticipationAccepted,
    /// Correction of an existing career record.
    CorrectionAppend,
    /// Migration-safe import.
    MigrationImport,
    /// Source requires review and cannot enter accepted mainline yet.
    SourcePendingReview,
}

/// Body-free resolver summary for a work participation source used by career append.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkParticipationSourceSummary {
    /// Project participation source ref.
    pub project_participation_ref: ProjectParticipationRef,
    /// Work source ref for the append.
    pub work_source_ref: WorkSourceRef,
    /// Stable source marker for duplicate detection.
    pub source_marker_ref: CareerSourceMarkerRef,
    /// Redaction-safe career summary marker, when available.
    pub safe_summary_ref: Option<CareerSafeSummaryRef>,
    /// Resolution state of the work source.
    pub source_state: WorkParticipationSourceState,
}

impl WorkParticipationSourceSummary {
    /// Creates a body-free source summary from resolver output.
    pub fn from_resolver(
        project_participation_ref: ProjectParticipationRef,
        work_source_ref: WorkSourceRef,
        source_marker_ref: CareerSourceMarkerRef,
        safe_summary_ref: Option<CareerSafeSummaryRef>,
        source_state: WorkParticipationSourceState,
    ) -> Self {
        Self {
            project_participation_ref,
            work_source_ref,
            source_marker_ref,
            safe_summary_ref,
            source_state,
        }
    }

    /// Returns whether the source is trusted for an accepted career append.
    pub fn is_trusted(&self) -> bool {
        self.source_state == WorkParticipationSourceState::Trusted
            && self.safe_summary_ref.is_some()
    }

    /// Returns whether the source requires formal review.
    pub fn requires_review(&self) -> bool {
        matches!(
            self.source_state,
            WorkParticipationSourceState::PendingReview
                | WorkParticipationSourceState::Unresolved
                | WorkParticipationSourceState::Untrusted
        )
    }

    /// Returns whether a safe summary marker is present.
    pub fn has_safe_summary(&self) -> bool {
        self.safe_summary_ref.is_some()
    }
}

/// Resolution state for a work participation source summary.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkParticipationSourceState {
    /// Source is trusted for accepted career append.
    Trusted,
    /// Source exists but requires formal review before accepted append.
    PendingReview,
    /// Source cannot be mapped to a member or source marker.
    Unresolved,
    /// Source is known but not trusted for career append.
    Untrusted,
    /// Work dependency is unavailable.
    Unavailable,
}

/// Requested career record change intent.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CareerRecordChangeIntent {
    /// Append a new career record.
    AppendNew,
    /// Append a new correction record.
    AppendCorrection,
    /// Hold the source marker for formal review.
    MarkSourcePendingReview,
    /// Forbidden in-place update of an existing record.
    ForbiddenInPlaceUpdate,
    /// Forbidden delete of an existing record.
    ForbiddenDelete,
    /// Forbidden reorder of career history.
    ForbiddenReorder,
}

/// Body-free career append material category.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CareerAppendMaterialKind {
    /// Safe career summary marker only.
    SafeSummaryMarker,
    /// Work source marker only.
    SourceMarkerOnly,
    /// Correction marker only.
    CorrectionMarkerOnly,
    /// Forbidden project body was presented.
    ForbiddenProjectBody,
    /// Forbidden work item body was presented.
    ForbiddenWorkItemBody,
    /// Forbidden project-member body was presented.
    ForbiddenProjectMemberBody,
    /// Forbidden artifact or work evidence body was presented.
    ForbiddenArtifactBody,
}

/// Material marker used by career append policy.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CareerAppendMaterialMarker {
    /// Material category.
    pub material_kind: CareerAppendMaterialKind,
    /// Optional body-free source marker for the material.
    pub source_ref: Option<IdentitySourceRef>,
}

/// Typed reference to an identity-owned memory reference relation.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct MemoryReferenceRef {
    /// Stable memory reference identifier.
    pub reference_id: MemoryReferenceId,
}

impl MemoryReferenceRef {
    /// Creates a typed ref from a validated memory reference identifier.
    pub fn from_id(reference_id: MemoryReferenceId) -> Self {
        Self { reference_id }
    }

    /// Returns whether both refs refer to the same memory relation.
    pub fn same_reference(&self, other: &MemoryReferenceRef) -> bool {
        self.reference_id == other.reference_id
    }
}

/// External memory or archive carrier category.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryCarrierKind {
    /// Active memory carrier reference.
    Memory,
    /// Archive or cold-storage carrier reference.
    Archive,
    /// Migration or handoff marker.
    ArchiveHandoff,
}

/// Body-free reference to an external memory carrier.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct MemoryRef {
    /// Memory or archive owned source ref.
    pub source_ref: IdentitySourceRef,
}

impl MemoryRef {
    /// Creates a typed memory ref from a memory-archive source ref.
    pub fn from_source(source_ref: IdentitySourceRef) -> Result<Self, ContractError> {
        ensure_source_owner(
            "memory_ref",
            &source_ref,
            &[IdentitySourceOwner::MemoryArchive],
        )?;

        Ok(Self { source_ref })
    }
}

/// Body-free reference to an external archive or cold-storage carrier.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct ArchiveRef {
    /// Archive-owned source ref.
    pub source_ref: IdentitySourceRef,
}

impl ArchiveRef {
    /// Creates a typed archive ref from a memory-archive source ref.
    pub fn from_source(source_ref: IdentitySourceRef) -> Result<Self, ContractError> {
        ensure_source_owner(
            "archive_ref",
            &source_ref,
            &[IdentitySourceOwner::MemoryArchive],
        )?;

        Ok(Self { source_ref })
    }
}

/// Body-free archive handoff or migration marker.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct ArchiveHandoffRef {
    /// Handoff marker source ref.
    pub source_ref: IdentitySourceRef,
    /// Opaque handoff marker token.
    pub handoff_token: String,
}

impl ArchiveHandoffRef {
    /// Creates a new handoff marker.
    pub fn new(
        source_ref: IdentitySourceRef,
        handoff_token: impl Into<String>,
    ) -> Result<Self, ContractError> {
        ensure_source_owner(
            "archive_handoff_ref",
            &source_ref,
            &[
                IdentitySourceOwner::MemoryArchive,
                IdentitySourceOwner::Identity,
            ],
        )?;

        Ok(Self {
            source_ref,
            handoff_token: ensure_non_empty("archive_handoff_ref.handoff_token", handoff_token)?,
        })
    }

    /// Returns whether both handoff refs are equal.
    pub fn same_handoff(&self, other: &ArchiveHandoffRef) -> bool {
        self.handoff_token == other.handoff_token && self.source_ref.same_source(&other.source_ref)
    }
}

/// Body-free source ref for a memory reference change.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct MemoryReferenceSourceRef {
    /// Source category for the memory reference change.
    pub source_kind: MemoryReferenceSourceKind,
    /// Body-free source ref.
    pub source_ref: IdentitySourceRef,
}

impl MemoryReferenceSourceRef {
    /// Creates a new memory reference source ref.
    pub fn new(
        source_kind: MemoryReferenceSourceKind,
        source_ref: IdentitySourceRef,
    ) -> Result<Self, ContractError> {
        let allowed = match source_kind {
            MemoryReferenceSourceKind::MemorySourceEvent => {
                &[IdentitySourceOwner::MemoryArchive][..]
            }
            MemoryReferenceSourceKind::ArchiveHandoffResult => &[
                IdentitySourceOwner::MemoryArchive,
                IdentitySourceOwner::Identity,
            ][..],
            MemoryReferenceSourceKind::ManualCommand => &[IdentitySourceOwner::Identity][..],
            MemoryReferenceSourceKind::MigrationImport => &[
                IdentitySourceOwner::Identity,
                IdentitySourceOwner::MemoryArchive,
            ][..],
            MemoryReferenceSourceKind::ReferenceRefreshMarker => {
                &[IdentitySourceOwner::Identity][..]
            }
        };
        ensure_source_owner("memory_reference_source_ref", &source_ref, allowed)?;

        Ok(Self {
            source_kind,
            source_ref,
        })
    }

    /// Returns whether this source describes a handoff result.
    pub fn is_handoff_result(&self) -> bool {
        self.source_kind == MemoryReferenceSourceKind::ArchiveHandoffResult
    }
}

/// Source category for memory reference changes.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryReferenceSourceKind {
    /// Explicit maintain command.
    ManualCommand,
    /// Memory carrier source-state event.
    MemorySourceEvent,
    /// Archive or cold-storage handoff result.
    ArchiveHandoffResult,
    /// Migration-safe import.
    MigrationImport,
    /// Reference refresh or reconciliation marker.
    ReferenceRefreshMarker,
}

/// Body-free reference to a redaction-safe memory or archive summary.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct MemorySafeSummaryRef {
    /// Carrier source this safe summary describes.
    pub source_ref: MemoryReferenceSourceRef,
    /// Opaque safe summary marker.
    pub safe_summary_token: String,
}

impl MemorySafeSummaryRef {
    /// Creates a new safe summary ref.
    pub fn new(
        source_ref: MemoryReferenceSourceRef,
        safe_summary_token: impl Into<String>,
    ) -> Result<Self, ContractError> {
        Ok(Self {
            source_ref,
            safe_summary_token: ensure_non_empty(
                "memory_safe_summary_ref.safe_summary_token",
                safe_summary_token,
            )?,
        })
    }

    /// Returns whether the safe summary belongs to the provided source.
    pub fn belongs_to_source(&self, source_ref: &MemoryReferenceSourceRef) -> bool {
        self.source_ref == *source_ref
    }
}

/// Body-free reason reference for a memory reference change.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct MemoryReferenceReasonRef {
    /// Reason category.
    pub reason_kind: MemoryReferenceReasonKind,
    /// Body-free reason source.
    pub source_ref: IdentitySourceRef,
}

impl MemoryReferenceReasonRef {
    /// Creates a new memory reference reason ref.
    pub fn new(
        reason_kind: MemoryReferenceReasonKind,
        source_ref: IdentitySourceRef,
    ) -> Result<Self, ContractError> {
        Ok(Self {
            reason_kind,
            source_ref,
        })
    }
}

/// Reason category for memory reference changes.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryReferenceReasonKind {
    /// Link or refresh requested by command.
    ManualMaintain,
    /// Carrier source state changed.
    SourceStateChanged,
    /// Archive or migration result was received.
    ArchiveHandoffResult,
    /// Source is unavailable or pending verification.
    SourcePendingVerification,
    /// Migration-safe import.
    MigrationImport,
}

/// Body-free source summary for memory or archive reference changes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MemoryReferenceSourceSummary {
    /// Memory reference source.
    pub source_ref: MemoryReferenceSourceRef,
    /// Optional memory carrier ref.
    pub memory_ref: Option<MemoryRef>,
    /// Optional archive carrier ref.
    pub archive_ref: Option<ArchiveRef>,
    /// Optional archive handoff marker.
    pub archive_handoff_ref: Option<ArchiveHandoffRef>,
    /// Optional redaction-safe summary marker.
    pub safe_summary_ref: Option<MemorySafeSummaryRef>,
    /// Source state returned by resolver or event mapper.
    pub source_state: MemoryReferenceSourceState,
}

impl MemoryReferenceSourceSummary {
    /// Creates a body-free source summary from resolver or event output.
    pub fn from_resolver(
        source_ref: MemoryReferenceSourceRef,
        memory_ref: Option<MemoryRef>,
        archive_ref: Option<ArchiveRef>,
        archive_handoff_ref: Option<ArchiveHandoffRef>,
        safe_summary_ref: Option<MemorySafeSummaryRef>,
        source_state: MemoryReferenceSourceState,
    ) -> Self {
        Self {
            source_ref,
            memory_ref,
            archive_ref,
            archive_handoff_ref,
            safe_summary_ref,
            source_state,
        }
    }

    /// Returns whether at least one formal marker is present.
    pub fn has_reference(&self) -> bool {
        self.memory_ref.is_some()
            || self.archive_ref.is_some()
            || self.archive_handoff_ref.is_some()
    }

    /// Returns whether the source is trusted for an accepted relation change.
    pub fn is_trusted(&self) -> bool {
        matches!(
            self.source_state,
            MemoryReferenceSourceState::Trusted | MemoryReferenceSourceState::HandoffResultAccepted
        ) && self.has_reference()
    }

    /// Returns whether the source requires verification or refresh.
    pub fn requires_verification(&self) -> bool {
        matches!(
            self.source_state,
            MemoryReferenceSourceState::Stale
                | MemoryReferenceSourceState::Unavailable
                | MemoryReferenceSourceState::PendingVerification
                | MemoryReferenceSourceState::HandoffResultFailed
                | MemoryReferenceSourceState::Untrusted
        )
    }
}

/// Source state for memory or archive reference changes.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryReferenceSourceState {
    /// Source is trusted and usable for a linked relation.
    Trusted,
    /// Source is stale and requires refresh.
    Stale,
    /// Source cannot currently be resolved.
    Unavailable,
    /// Source requires formal verification before accepted relation.
    PendingVerification,
    /// Archive or migration result is accepted as a marker.
    HandoffResultAccepted,
    /// Archive or migration result failed.
    HandoffResultFailed,
    /// Source is unrecognized or not trusted.
    Untrusted,
}

/// Requested memory reference change intent.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryReferenceChangeIntent {
    /// Link a memory reference.
    LinkMemory,
    /// Refresh relation state from a source marker.
    RefreshState,
    /// Attach archive or cold-storage reference.
    AttachArchive,
    /// Record archive handoff result marker.
    RecordArchiveHandoffResult,
    /// Mark the relation pending verification.
    MarkPendingVerification,
    /// Forbidden write to external carrier truth.
    ForbiddenExternalOwnerWrite,
    /// Forbidden delete of external memory or archive body.
    ForbiddenExternalBodyDelete,
}

/// Body-free material category for memory reference changes.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryReferenceChangeMaterialKind {
    /// Safe memory or archive summary marker only.
    SafeSummaryMarker,
    /// Memory or archive refs only.
    ReferenceMarkersOnly,
    /// Archive handoff marker only.
    HandoffMarkerOnly,
    /// Forbidden memory body was presented.
    ForbiddenMemoryBody,
    /// Forbidden embedding or index material was presented.
    ForbiddenEmbeddingOrIndex,
    /// Forbidden archive package or package metadata was presented.
    ForbiddenArchivePackage,
    /// Forbidden artifact, conversation, or receipt body was presented.
    ForbiddenExternalBody,
}

/// Material marker used by memory reference policy.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MemoryReferenceChangeMaterialMarker {
    /// Material category.
    pub material_kind: MemoryReferenceChangeMaterialKind,
    /// Optional body-free source marker.
    pub source_ref: Option<IdentitySourceRef>,
}

/// Projection source cursor marker used by projection freshness helpers.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct IdentityProjectionCursorRef {
    /// Opaque projection source cursor marker.
    pub source_cursor_ref: IdentitySourceRef,
}

impl IdentityProjectionCursorRef {
    /// Creates a new projection source cursor marker.
    pub fn new(source_cursor_ref: IdentitySourceRef) -> Self {
        Self { source_cursor_ref }
    }
}

/// External reference category used by identity without owning external truth.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExternalReferenceKind {
    /// Method-library role or capability source.
    MethodSource,
    /// Work participation source.
    WorkParticipation,
    /// Governance basis source.
    GovernanceBasis,
    /// Memory carrier source.
    Memory,
    /// Archive package or handoff target source.
    Archive,
    /// Runtime or observability source marker.
    RuntimeSignal,
}

/// Body-free reference to an external truth owner.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct ExternalReferenceRef {
    /// External reference category.
    pub reference_kind: ExternalReferenceKind,
    /// Opaque external reference marker.
    pub source_ref: IdentitySourceRef,
}

impl ExternalReferenceRef {
    /// Creates a new external reference marker.
    pub fn new(reference_kind: ExternalReferenceKind, source_ref: IdentitySourceRef) -> Self {
        Self {
            reference_kind,
            source_ref,
        }
    }
}

/// Identity object category that owns the local use of an external reference.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityReferenceOwnerKind {
    /// Role capability summary or source snapshot.
    RoleCapability,
    /// Career record or career source marker.
    CareerRecord,
    /// Memory reference relation.
    MemoryReference,
    /// Lifecycle governance basis.
    LifecycleBasis,
    /// Projection or maintenance marker.
    Maintenance,
}

/// Body-free reference to the local identity owner of an external reference.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct IdentityReferenceOwnerRef {
    /// Owner category.
    pub owner_kind: IdentityReferenceOwnerKind,
    /// Opaque owner marker.
    pub owner_ref: IdentitySourceRef,
}

impl IdentityReferenceOwnerRef {
    /// Creates a new local owner marker for an external reference.
    pub fn new(owner_kind: IdentityReferenceOwnerKind, owner_ref: IdentitySourceRef) -> Self {
        Self {
            owner_kind,
            owner_ref,
        }
    }
}

/// External source version marker.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct ExternalSourceVersionRef {
    /// Opaque external version marker.
    pub version_ref: IdentitySourceRef,
}

impl ExternalSourceVersionRef {
    /// Creates a new external source version marker.
    pub fn new(version_ref: IdentitySourceRef) -> Self {
        Self { version_ref }
    }
}

/// Body-free safe summary marker for a resolved external reference.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExternalReferenceSafeSummaryRef {
    /// External reference being summarized.
    pub external_reference_ref: ExternalReferenceRef,
    /// Opaque safe summary source marker.
    pub safe_summary_ref: IdentitySourceRef,
}

impl ExternalReferenceSafeSummaryRef {
    /// Creates a new safe summary marker for an external reference.
    pub fn new(
        external_reference_ref: ExternalReferenceRef,
        safe_summary_ref: IdentitySourceRef,
    ) -> Self {
        Self {
            external_reference_ref,
            safe_summary_ref,
        }
    }
}

/// Maintenance scope marker for rebuild, refresh, and reconciliation paths.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct MaintenanceScopeRef {
    /// Opaque maintenance scope marker.
    pub scope_ref: IdentitySourceRef,
}

impl MaintenanceScopeRef {
    /// Creates a new maintenance scope marker.
    pub fn new(scope_ref: IdentitySourceRef) -> Self {
        Self { scope_ref }
    }
}

/// Formal outbox delivery attempt marker.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct OutboxDeliveryAttemptRef {
    /// Opaque attempt source marker.
    pub attempt_ref: IdentitySourceRef,
}

impl OutboxDeliveryAttemptRef {
    /// Creates a new outbox delivery attempt marker.
    pub fn new(attempt_ref: IdentitySourceRef) -> Self {
        Self { attempt_ref }
    }
}

/// Safe outbox delivery issue marker.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct OutboxDeliveryIssueRef {
    /// Opaque issue source marker.
    pub issue_ref: IdentitySourceRef,
}

impl OutboxDeliveryIssueRef {
    /// Creates a new outbox delivery issue marker.
    pub fn new(issue_ref: IdentitySourceRef) -> Self {
        Self { issue_ref }
    }
}

/// Identity accepted change category.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityChangeKind {
    /// Member anchor was established or held.
    MemberAnchorChanged,
    /// Lifecycle state changed.
    LifecycleChanged,
    /// Role capability summary changed.
    RoleCapabilitySummaryChanged,
    /// Career record was appended or corrected.
    CareerRecordChanged,
    /// Memory reference changed.
    MemoryReferenceChanged,
    /// Trace correction was appended.
    TraceCorrectionAppended,
    /// Derived marker changed without creating core truth.
    DerivedMarkerChanged,
}

/// Body-free change kind marker used by trace and outbox helpers.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct IdentityChangeKindRef {
    /// Change kind category.
    pub change_kind: IdentityChangeKind,
    /// Optional body-free source marker for versioned change kinds.
    pub source_ref: Option<IdentitySourceRef>,
}

impl IdentityChangeKindRef {
    /// Creates a new change kind marker.
    pub fn new(change_kind: IdentityChangeKind, source_ref: Option<IdentitySourceRef>) -> Self {
        Self {
            change_kind,
            source_ref,
        }
    }

    /// Returns whether both change kind markers are equal.
    pub fn same_kind(&self, other: &IdentityChangeKindRef) -> bool {
        self.change_kind == other.change_kind && self.source_ref == other.source_ref
    }
}

string_newtype!(AuditTrailRef, "Audit trail reference.");
string_newtype!(HandoffScopeRef, "Handoff scope marker.");
string_newtype!(HandoffTargetRef, "Handoff target marker.");
string_newtype!(
    TraceHandoffSafeMaterialRef,
    "Safe trace handoff material marker."
);

/// Formal handoff attempt marker.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct HandoffAttemptRef {
    /// Opaque attempt source marker.
    pub attempt_ref: IdentitySourceRef,
}

impl HandoffAttemptRef {
    /// Creates a new handoff attempt marker.
    pub fn new(attempt_ref: IdentitySourceRef) -> Self {
        Self { attempt_ref }
    }
}

/// Safe handoff issue marker.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct HandoffIssueRef {
    /// Opaque issue source marker.
    pub issue_ref: IdentitySourceRef,
}

impl HandoffIssueRef {
    /// Creates a new handoff issue marker.
    pub fn new(issue_ref: IdentitySourceRef) -> Self {
        Self { issue_ref }
    }
}

string_newtype!(
    IdentityApiRequestMarkerRef,
    "Body-free API request material marker."
);
string_newtype!(
    IdentityAuditSubjectRef,
    "Canonical audit subject reference."
);
string_newtype!(
    IdentityCanonicalRequestMarkerRef,
    "Canonical request material marker."
);
string_newtype!(
    IdentityConsumerBindingRef,
    "Inbound consumer binding marker."
);
string_newtype!(
    IdentityConsumerReceiptRef,
    "Public consumer receipt reference."
);
string_newtype!(IdentityDegradedMarkerRef, "Safe degraded marker reference.");
string_newtype!(
    IdentityEventEnvelopeMarkerRef,
    "Inbound event envelope marker."
);
string_newtype!(IdentityJobCursorRef, "Operations job cursor marker.");
string_newtype!(IdentityJobReportRef, "Public job report reference.");
string_newtype!(
    IdentityJobRunMetadataRef,
    "Body-free job run metadata marker."
);
string_newtype!(IdentityJobRunRef, "Operations job run reference.");
string_newtype!(IdentityJobScopeMarkerRef, "Operations job scope marker.");
string_newtype!(IdentityMaintenanceTargetRef, "Maintenance target marker.");
string_newtype!(
    IdentityOutboxPayloadMarkerRef,
    "Body-free outbound payload marker."
);
string_newtype!(IdentityOutboxRecordRef, "Identity outbox record reference.");
string_newtype!(
    IdentityOutboxSubjectRef,
    "Canonical outbound subject reference."
);
string_newtype!(IdentityProjectionRef, "Projection reference.");
string_newtype!(
    IdentityRedactionMarkerRef,
    "Safe redaction marker reference."
);
string_newtype!(
    IdentityRequestDigestValue,
    "Canonical request digest value."
);
string_newtype!(IdentitySourceEventRef, "Inbound source event reference.");
string_newtype!(IdentityStoredResultRef, "Stored replay surface reference.");
string_newtype!(IdentityTraceContextRef, "Runtime trace context marker.");
string_newtype!(IdentityTraceRecordRef, "Identity trace record reference.");
string_newtype!(
    IdentityTraceSubjectRef,
    "Canonical trace subject reference."
);
string_newtype!(
    IdentityTruthCursor,
    "Committed identity truth cursor marker."
);
string_newtype!(
    ReconciliationFindingIntentRef,
    "Reconciliation finding intent marker."
);
string_newtype!(
    ReconciliationFindingRef,
    "Reconciliation finding reference."
);
string_newtype!(ReconciliationReportRef, "Reconciliation report reference.");
string_newtype!(TopicKeyRef, "Topic binding key marker.");
string_newtype!(VisibilityContextRef, "Visibility context marker.");
string_newtype!(VisibilityResultRef, "Visibility result marker.");
string_newtype!(VisibilityScopeRef, "Visibility scope marker.");
string_newtype!(
    IdentityVisibilityDecisionRef,
    "Public visibility decision reference."
);
string_newtype!(HandoffReceiptRef, "Formal handoff receipt reference.");

pub use crate::receipts::{MaintenanceIssueRef, TraceHandoffIntentRef};

/// Identity-side timestamp captured from the configured clock source.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct IdentityTimestamp {
    /// Milliseconds since Unix epoch from the configured clock source.
    pub epoch_millis: i64,
}

impl IdentityTimestamp {
    /// Builds a timestamp from a validated clock value.
    pub fn from_clock(epoch_millis: i64) -> Result<Self, crate::errors::ContractError> {
        if epoch_millis < 0 {
            return Err(crate::errors::ContractError::invalid_value(
                "identity_timestamp",
                "epoch_millis must be non-negative",
            ));
        }

        Ok(Self { epoch_millis })
    }

    /// Returns whether two timestamps refer to the same instant.
    pub fn same_instant(&self, other: &IdentityTimestamp) -> bool {
        self.epoch_millis == other.epoch_millis
    }

    /// Returns whether this timestamp is after the other timestamp.
    pub fn is_after(&self, other: &IdentityTimestamp) -> bool {
        self.epoch_millis > other.epoch_millis
    }
}

/// Public read surface kind used by query visibility and read shells.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityReadSurfaceKind {
    /// Member summary read surface.
    Summary,
    /// Identity trace read surface.
    Trace,
    /// Audit trail read surface.
    Audit,
    /// Projection state read surface.
    Projection,
    /// Reference resolution read surface.
    Reference,
    /// Reconciliation report read surface.
    Report,
    /// Outbox read surface.
    Outbox,
    /// Handoff read surface.
    Handoff,
}

/// Public projection freshness marker reused by query and job shells.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProjectionFreshnessMarkerRef {
    /// Projection being described.
    pub projection_ref: IdentityProjectionRef,
    /// Public freshness state copied from projection state.
    pub state_kind: String,
}
