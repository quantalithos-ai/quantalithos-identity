//! Public command envelopes and accepted side-effect summary shell.

use core_contracts::actor::ActorRef;
use serde::{Deserialize, Serialize};

use crate::metadata::{
    IdentityCommandMetadata, IdentityProtocolRejection, IdentityRequestDigestMarker,
};
use crate::protocol::IdentityCommandName;
use crate::refs::{
    ArchiveHandoffRef, ArchiveRef, CapabilityEvidenceRef, CapabilitySourceRef,
    CareerAppendMaterialMarker, CareerAppendReasonRef, CareerRecordChangeIntent, CareerRecordRef,
    CareerRecordStateKind, CareerSafeSummaryRef, GlobalLifecycleStateKind, GlobalMemberRef,
    GovernanceBasisRef, IdentityAnchorReasonRef, IdentityAnchorStateKind, IdentityAuditSubjectRef,
    IdentityOutboxRecordRef, IdentityProjectionRef, IdentitySourceRef, IdentityStoredResultRef,
    IdentityTraceRecordRef, IdentityTruthCursor, LifecycleReasonRef, LifecycleRiskRef, MemoryRef,
    MemoryReferenceChangeIntent, MemoryReferenceChangeMaterialMarker, MemoryReferenceReasonRef,
    MemoryReferenceRef, MemoryReferenceSourceRef, MemoryReferenceStateKind, MemorySafeSummaryRef,
    ProjectParticipationRef, RoleCapabilityChangeMaterialMarker, RoleCapabilityChangeReasonRef,
    RoleCapabilitySafeSummaryRef, RoleCapabilitySourceRef, RoleCapabilitySourceSnapshotRef,
    RoleCapabilitySourceStateKind, RoleCapabilitySummaryRef, RoleCapabilitySummaryStateKind,
    RoleSourceRef, WorkSourceRef,
};

/// Public command request envelope.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityCommandRequest<T> {
    /// Caller actor reference extracted at the entry boundary.
    pub actor_ref: ActorRef,
    /// Stable public command name.
    pub command_name: IdentityCommandName,
    /// Public request metadata shell.
    pub metadata: IdentityCommandMetadata,
    /// Canonical digest marker.
    pub digest: IdentityRequestDigestMarker,
    /// Typed command body.
    pub body: T,
}

/// Public accepted command response envelope.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityCommandResponse<T> {
    /// Stable public command name.
    pub command_name: IdentityCommandName,
    /// Stored replay result reference.
    pub result_ref: IdentityStoredResultRef,
    /// Typed accepted result body.
    pub result: T,
    /// Public accepted side-effect summary.
    pub effect: IdentityCommandEffectPublicSummary,
}

/// Public command outcome shell.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum IdentityCommandOutcome<T> {
    /// Accepted response shell.
    Accepted(IdentityCommandResponse<T>),
    /// Rejected response shell.
    Rejected(IdentityProtocolRejection),
}

/// Public accepted side-effect summary for command results.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityCommandEffectPublicSummary {
    /// Accepted truth cursor marker.
    pub accepted_cursor_ref: IdentityTruthCursor,
    /// Appended trace references.
    pub trace_refs: Vec<IdentityTraceRecordRef>,
    /// Audit subject references associated with the accepted write.
    pub audit_subject_refs: Vec<IdentityAuditSubjectRef>,
    /// Outbox record references created by the accepted write.
    pub outbox_refs: Vec<IdentityOutboxRecordRef>,
    /// Projection refs marked stale by the accepted write.
    pub stale_projection_refs: Vec<IdentityProjectionRef>,
}

/// Request body for establishing a platform-level global member identity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EstablishGlobalMemberRequest {
    /// Optional caller-proposed member ref.
    pub requested_member_ref: Option<GlobalMemberRef>,
    /// Body-free source marker used to establish the member.
    pub source_ref: IdentitySourceRef,
    /// Optional reason marker used by accepted trace or anchor material.
    pub anchor_reason_ref: Option<IdentityAnchorReasonRef>,
    /// Reason marker for the initial lifecycle state created with the member.
    pub initial_lifecycle_reason_ref: LifecycleReasonRef,
}

/// Accepted command result for `EstablishGlobalMember`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GlobalMemberCommandResult {
    /// Established member ref.
    pub member_ref: GlobalMemberRef,
    /// Final anchor state after establishment.
    pub anchor_state_kind: IdentityAnchorStateKind,
    /// Initial lifecycle state created for the member.
    pub lifecycle_state_kind: GlobalLifecycleStateKind,
    /// Body-free source marker that established the member.
    pub source_ref: IdentitySourceRef,
}

/// Request body for explicitly changing a member global lifecycle state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct UpdateGlobalLifecycleStateRequest {
    /// Member whose lifecycle state will be updated.
    pub member_ref: GlobalMemberRef,
    /// Requested target lifecycle state.
    pub target_state: GlobalLifecycleStateKind,
    /// Body-free lifecycle reason marker.
    pub reason_ref: LifecycleReasonRef,
    /// Optional governance basis marker for high-risk lifecycle changes.
    pub basis_ref: Option<GovernanceBasisRef>,
    /// Lifecycle action risk marker used for high-risk precheck.
    pub action_risk_ref: Option<LifecycleRiskRef>,
}

/// Accepted command result for `UpdateGlobalLifecycleState`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GlobalLifecycleCommandResult {
    /// Member whose lifecycle state changed.
    pub member_ref: GlobalMemberRef,
    /// Lifecycle state after the command.
    pub lifecycle_state_kind: GlobalLifecycleStateKind,
    /// Body-free lifecycle reason marker.
    pub reason_ref: LifecycleReasonRef,
    /// Governance basis marker persisted for the lifecycle change when present.
    pub basis_ref: Option<GovernanceBasisRef>,
    /// Anchor state after terminal handling when changed by the flow.
    pub anchor_state_kind: Option<IdentityAnchorStateKind>,
}

/// Request body for maintaining an identity-owned role/capability summary for a member.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaintainRoleCapabilitySummaryRequest {
    /// Member whose role/capability summary is maintained.
    pub member_ref: GlobalMemberRef,
    /// Optional caller-known summary ref.
    pub requested_summary_ref: Option<RoleCapabilitySummaryRef>,
    /// Method-library source ref used to resolve a body-free source snapshot.
    pub source_ref: RoleCapabilitySourceRef,
    /// Optional role source wrapper used by the accepted summary.
    pub role_source_ref: Option<RoleSourceRef>,
    /// Capability source wrappers used by the accepted summary.
    pub capability_source_refs: Vec<CapabilitySourceRef>,
    /// Capability evidence refs; never evidence body.
    pub evidence_refs: Vec<CapabilityEvidenceRef>,
    /// Optional body-free safe summary marker supplied by caller or resolver.
    pub safe_summary_ref: Option<RoleCapabilitySafeSummaryRef>,
    /// Body-free reason marker for this summary change.
    pub change_reason_ref: RoleCapabilityChangeReasonRef,
    /// Material classification used to reject forbidden method/definition/evidence body.
    pub change_material_marker: RoleCapabilityChangeMaterialMarker,
}

/// Accepted command result for `MaintainRoleCapabilitySummary`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RoleCapabilityCommandResult {
    /// Member whose summary changed.
    pub member_ref: GlobalMemberRef,
    /// Identity-owned role/capability summary ref.
    pub summary_ref: RoleCapabilitySummaryRef,
    /// Source snapshot used by the summary.
    pub source_snapshot_ref: RoleCapabilitySourceSnapshotRef,
    /// Final summary state.
    pub summary_state_kind: RoleCapabilitySummaryStateKind,
    /// Source snapshot state used by the accepted result.
    pub source_state_kind: RoleCapabilitySourceStateKind,
    /// Optional role source wrapper.
    pub role_source_ref: Option<RoleSourceRef>,
    /// Capability source wrappers.
    pub capability_source_refs: Vec<CapabilitySourceRef>,
    /// Evidence refs retained by the summary.
    pub evidence_refs: Vec<CapabilityEvidenceRef>,
    /// Body-free safe summary marker.
    pub safe_summary_ref: Option<RoleCapabilitySafeSummaryRef>,
}

/// Request body for appending an identity-owned career history record.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AppendCareerRecordRequest {
    /// Member whose career history receives an append-only record.
    pub member_ref: GlobalMemberRef,
    /// Optional caller-known career record ref.
    pub requested_career_record_ref: Option<CareerRecordRef>,
    /// Requested append/correction/pending-review intent.
    pub change_intent: CareerRecordChangeIntent,
    /// Work-owned participation ref; never ProjectMember body.
    pub project_participation_ref: ProjectParticipationRef,
    /// Work source marker for this append.
    pub work_source_ref: WorkSourceRef,
    /// Stable duplicate-source marker.
    pub source_marker_ref: crate::refs::CareerSourceMarkerRef,
    /// Optional redaction-safe career summary marker.
    pub career_summary_ref: Option<CareerSafeSummaryRef>,
    /// Body-free reason marker for append or correction.
    pub append_reason_ref: CareerAppendReasonRef,
    /// Original career record being explained, required for correction intent.
    pub original_record_ref: Option<CareerRecordRef>,
    /// Material classification used to reject project/work body.
    pub append_material_marker: CareerAppendMaterialMarker,
}

/// Accepted command result for `AppendCareerRecord`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CareerRecordCommandResult {
    /// Member whose career history changed.
    pub member_ref: GlobalMemberRef,
    /// Appended career record ref.
    pub career_record_ref: CareerRecordRef,
    /// Final state of the appended record.
    pub record_state_kind: CareerRecordStateKind,
    /// Work-owned participation source.
    pub project_participation_ref: ProjectParticipationRef,
    /// Work source marker used for this append.
    pub work_source_ref: WorkSourceRef,
    /// Duplicate source marker.
    pub source_marker_ref: crate::refs::CareerSourceMarkerRef,
    /// Redaction-safe career summary marker.
    pub career_summary_ref: Option<CareerSafeSummaryRef>,
    /// Original record explained by this correction, when applicable.
    pub correction_of_ref: Option<CareerRecordRef>,
    /// Existing record marked as superseded by this correction, when applicable.
    pub superseded_record_ref: Option<CareerRecordRef>,
}

/// Request body for maintaining an identity-owned memory/archive reference relation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MaintainMemoryReferenceRequest {
    /// Member whose memory/archive relation is maintained.
    pub member_ref: GlobalMemberRef,
    /// Optional caller-known relation ref.
    pub requested_memory_reference_ref: Option<MemoryReferenceRef>,
    /// Requested relation change intent.
    pub change_intent: MemoryReferenceChangeIntent,
    /// External memory carrier ref.
    pub memory_ref: Option<MemoryRef>,
    /// External archive carrier ref.
    pub archive_ref: Option<ArchiveRef>,
    /// Archive handoff or migration marker.
    pub archive_handoff_ref: Option<ArchiveHandoffRef>,
    /// Source marker for resolver summary.
    pub source_ref: MemoryReferenceSourceRef,
    /// Optional redaction-safe memory/archive summary marker.
    pub safe_summary_ref: Option<MemorySafeSummaryRef>,
    /// Body-free reason marker for this relation change.
    pub reason_ref: MemoryReferenceReasonRef,
    /// Material classification used to reject memory/archive/receipt body.
    pub change_material_marker: MemoryReferenceChangeMaterialMarker,
}

/// Accepted command result for `MaintainMemoryReference`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MemoryReferenceCommandResult {
    /// Member whose memory/archive relation changed.
    pub member_ref: GlobalMemberRef,
    /// Identity-owned memory reference relation ref.
    pub memory_reference_ref: MemoryReferenceRef,
    /// Final relation state kind.
    pub reference_state_kind: MemoryReferenceStateKind,
    /// External memory carrier ref.
    pub memory_ref: Option<MemoryRef>,
    /// External archive carrier ref.
    pub archive_ref: Option<ArchiveRef>,
    /// Archive handoff or migration marker.
    pub archive_handoff_ref: Option<ArchiveHandoffRef>,
    /// Source marker used for this relation state.
    pub source_ref: MemoryReferenceSourceRef,
    /// Redaction-safe summary marker.
    pub safe_summary_ref: Option<MemorySafeSummaryRef>,
    /// Body-free reason marker.
    pub reason_ref: MemoryReferenceReasonRef,
}
