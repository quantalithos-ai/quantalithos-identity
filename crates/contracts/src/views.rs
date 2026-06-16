//! Public read-model and visibility support shells.

use core_contracts::actor::ActorRef;
use serde::{Deserialize, Serialize};

use crate::errors::ContractError;
use crate::metadata::IdentityDegradedKind;
use crate::receipts::MaintenanceIssueRef;
use crate::refs::{
    ArchiveHandoffRef, ArchiveRef, AuditScopeRef, AuditTrailRef, CapabilityEvidenceRef,
    CapabilitySourceRef, CareerAppendReasonRef, CareerRecordRef, CareerRecordStateKind,
    CareerSafeSummaryRef, CareerSourceMarkerRef, ConsumerRef, GlobalLifecycleStateKind,
    GlobalMemberRef, GovernanceBasisRef, HandoffAttemptRef, HandoffIssueRef, HandoffReceiptRef,
    HandoffScopeRef, HandoffStateKind, HandoffTargetRef, IdentityAnchorReasonRef,
    IdentityAnchorStateKind, IdentityAuditSubjectRef, IdentityChangeKindRef,
    IdentityChangeReasonRef, IdentityDegradedMarkerRef, IdentityMaintenanceTargetRef,
    IdentityOutboxPayloadMarkerRef, IdentityOutboxRecordRef, IdentityOutboxSubjectRef,
    IdentityProjectionCursorRef, IdentityProjectionRef, IdentityReadSubjectRef,
    IdentityReadSurfaceKind, IdentityRedactionMarkerRef, IdentitySourceRef, IdentityTimestamp,
    IdentityTraceRecordRef, IdentityTruthCursor, LifecycleReasonRef, MaintenanceScopeRef,
    MemberSummaryViewRef, MemoryRef, MemoryReferenceReasonRef, MemoryReferenceRef,
    MemoryReferenceSourceRef, MemoryReferenceStateKind, MemorySafeSummaryRef,
    OutboxDeliveryAttemptRef, OutboxDeliveryIssueRef, OutboxStateKind, ProjectParticipationRef,
    ProjectionFreshnessMarkerRef, ProjectionStateKind, ProjectionStateRef,
    ReconciliationFindingRef, ReconciliationReportRef, ReconciliationReportStateKind,
    RedactionProfileRef, ReferenceResolutionStateKind, ReferenceResolutionStateRef,
    RoleCapabilitySafeSummaryRef, RoleCapabilitySourceSnapshotRef, RoleCapabilitySourceStateKind,
    RoleCapabilitySummaryRef, RoleCapabilitySummaryStateKind, RoleSourceRef, TopicKeyRef,
    TraceHandoffSafeMaterialRef, VisibilityContextRef, VisibilityResultRef, VisibilityScopeRef,
};

/// Safe summary marker for the identity anchor slice.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct MemberAnchorSafeSummaryRef {
    /// Body-free source marker for the anchor summary slice.
    pub source_ref: IdentitySourceRef,
}

impl MemberAnchorSafeSummaryRef {
    /// Creates a new anchor safe summary marker.
    pub fn new(source_ref: IdentitySourceRef) -> Self {
        Self { source_ref }
    }
}

/// Safe summary marker for the lifecycle slice.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct LifecycleSafeSummaryRef {
    /// Body-free source marker for the lifecycle summary slice.
    pub source_ref: IdentitySourceRef,
}

impl LifecycleSafeSummaryRef {
    /// Creates a new lifecycle safe summary marker.
    pub fn new(source_ref: IdentitySourceRef) -> Self {
        Self { source_ref }
    }
}

/// Member summary slice category.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemberSummarySliceKind {
    /// Anchor slice.
    Anchor,
    /// Lifecycle slice.
    Lifecycle,
    /// Role and capability slice.
    RoleCapability,
    /// Career slice.
    Career,
    /// Memory reference slice.
    MemoryReference,
}

/// Body-free reference to a member summary slice.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct MemberSummarySliceRef {
    /// Slice category.
    pub slice_kind: MemberSummarySliceKind,
    /// Member that owns this slice.
    pub member_ref: GlobalMemberRef,
    /// Body-free safe summary source for the slice.
    pub safe_summary_source_ref: IdentitySourceRef,
}

impl MemberSummarySliceRef {
    /// Creates a new body-free member summary slice marker.
    pub fn new(
        slice_kind: MemberSummarySliceKind,
        member_ref: GlobalMemberRef,
        safe_summary_source_ref: IdentitySourceRef,
    ) -> Self {
        Self {
            slice_kind,
            member_ref,
            safe_summary_source_ref,
        }
    }
}

/// Visibility access state from a resolver or prepared context.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityVisibilityAccessState {
    /// Subject is visible without redaction.
    Visible,
    /// Subject is visible only after redaction.
    Redacted,
    /// Subject is not visible.
    NotVisible,
    /// Visibility check is partial or degraded.
    Degraded,
    /// Visibility dependency is unavailable.
    Unavailable,
}

/// Material category for read, trace, and audit output.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityReadMaterialKind {
    /// Safe summary refs only.
    SafeSummaryRefs,
    /// Trace refs and safe markers only.
    TraceRefsOnly,
    /// Audit refs and safe markers only.
    AuditRefsOnly,
    /// Redacted safe material.
    RedactedSafeMaterial,
    /// Forbidden external body.
    ForbiddenExternalBody,
    /// Forbidden raw log or debug body.
    ForbiddenRawDiagnostic,
    /// Forbidden secret or credential material.
    ForbiddenSecret,
}

/// Body-free read material marker consumed by visibility policy.
#[derive(Clone, Debug, Eq, Hash, PartialEq, Serialize, Deserialize)]
pub struct IdentityReadMaterialMarker {
    /// Material category.
    pub material_kind: IdentityReadMaterialKind,
    /// Optional source marker.
    pub source_ref: Option<IdentitySourceRef>,
}

impl IdentityReadMaterialMarker {
    /// Creates a new read material marker.
    pub fn new(
        material_kind: IdentityReadMaterialKind,
        source_ref: Option<IdentitySourceRef>,
    ) -> Self {
        Self {
            material_kind,
            source_ref,
        }
    }

    /// Returns whether the material remains body-free.
    pub fn is_body_free(&self) -> bool {
        matches!(
            self.material_kind,
            IdentityReadMaterialKind::SafeSummaryRefs
                | IdentityReadMaterialKind::TraceRefsOnly
                | IdentityReadMaterialKind::AuditRefsOnly
                | IdentityReadMaterialKind::RedactedSafeMaterial
        )
    }
}

/// Prepared visibility input consumed by visibility policy.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityVisibilityAccessSummary {
    /// Canonical read subject resolved for this query target.
    pub read_subject_ref: IdentityReadSubjectRef,
    /// Consumer requesting the material.
    pub consumer_ref: ConsumerRef,
    /// Optional actor represented by the request.
    pub actor_ref: Option<ActorRef>,
    /// Visibility context marker.
    pub visibility_context_ref: VisibilityContextRef,
    /// Visibility scope marker.
    pub scope_ref: VisibilityScopeRef,
    /// Access state.
    pub access_state: IdentityVisibilityAccessState,
    /// Optional redaction profile marker.
    pub redaction_profile_ref: Option<RedactionProfileRef>,
    /// Optional public redaction marker copied into query surface when needed.
    pub redaction_marker_ref: Option<IdentityRedactionMarkerRef>,
    /// Body-free result marker.
    pub visibility_result_ref: VisibilityResultRef,
    /// Optional body-free degraded marker copied into degraded-like public surface.
    pub degraded_marker_ref: Option<IdentityDegradedMarkerRef>,
    /// Optional safe degraded classifier copied into public degraded surface.
    pub degraded_kind: Option<IdentityDegradedKind>,
}

/// Member-facing identity summary view built from body-free safe summary refs.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MemberSummaryView {
    /// Stable summary view ref.
    pub view_ref: MemberSummaryViewRef,
    /// Member represented by this summary.
    pub member_ref: GlobalMemberRef,
    /// Visibility scope for which this view was materialized.
    pub visibility_scope_ref: VisibilityScopeRef,
    /// Anchor safe summary slice.
    pub anchor_slice_ref: MemberSummarySliceRef,
    /// Lifecycle safe summary slice.
    pub lifecycle_slice_ref: MemberSummarySliceRef,
    /// Optional role and capability safe summary slices.
    pub role_capability_slice_refs: Vec<MemberSummarySliceRef>,
    /// Career safe summary slices.
    pub career_slice_refs: Vec<MemberSummarySliceRef>,
    /// Memory reference safe summary slices.
    pub memory_slice_refs: Vec<MemberSummarySliceRef>,
    /// Visibility result for this read surface.
    pub visibility_result_ref: VisibilityResultRef,
    /// Public read surface category.
    pub read_surface_kind: IdentityReadSurfaceKind,
    /// Optional committed truth cursor covered by this projection.
    pub source_cursor_ref: Option<IdentityTruthCursor>,
    /// Optional public freshness marker copied into stale-visible query surfaces.
    pub projection_freshness_ref: Option<ProjectionFreshnessMarkerRef>,
    /// Read material marker used to prevent forbidden bodies.
    pub read_material_marker: IdentityReadMaterialMarker,
}

impl MemberSummaryView {
    /// Creates a member summary view from formal projection inputs.
    #[allow(clippy::too_many_arguments)]
    pub fn from_projection(
        view_ref: MemberSummaryViewRef,
        member_ref: GlobalMemberRef,
        visibility_scope_ref: VisibilityScopeRef,
        anchor_slice_ref: MemberSummarySliceRef,
        lifecycle_slice_ref: MemberSummarySliceRef,
        role_capability_slice_refs: Vec<MemberSummarySliceRef>,
        career_slice_refs: Vec<MemberSummarySliceRef>,
        memory_slice_refs: Vec<MemberSummarySliceRef>,
        visibility_result_ref: VisibilityResultRef,
        source_cursor_ref: Option<IdentityTruthCursor>,
        projection_freshness_ref: Option<ProjectionFreshnessMarkerRef>,
        read_material_marker: IdentityReadMaterialMarker,
    ) -> Result<Self, ContractError> {
        if anchor_slice_ref.member_ref != member_ref || lifecycle_slice_ref.member_ref != member_ref
        {
            return Err(ContractError::invalid_value(
                "member_summary_view",
                "anchor and lifecycle slices must belong to the same member",
            ));
        }

        Ok(Self {
            view_ref,
            member_ref,
            visibility_scope_ref,
            anchor_slice_ref,
            lifecycle_slice_ref,
            role_capability_slice_refs,
            career_slice_refs,
            memory_slice_refs,
            visibility_result_ref,
            read_surface_kind: IdentityReadSurfaceKind::Found,
            source_cursor_ref,
            projection_freshness_ref,
            read_material_marker,
        })
    }

    /// Returns whether the view belongs to the provided member.
    pub fn belongs_to(&self, member_ref: &GlobalMemberRef) -> bool {
        &self.member_ref == member_ref
    }

    /// Returns whether the view was materialized for the provided visibility scope.
    pub fn matches_visibility_scope(&self, visibility_scope_ref: &VisibilityScopeRef) -> bool {
        &self.visibility_scope_ref == visibility_scope_ref
    }

    /// Returns whether the required anchor and lifecycle slices are present.
    pub fn has_required_slices(&self) -> bool {
        self.anchor_slice_ref.member_ref == self.member_ref
            && self.lifecycle_slice_ref.member_ref == self.member_ref
    }

    /// Returns whether the read surface is visible or redacted.
    pub fn is_visible(&self) -> bool {
        matches!(
            self.read_surface_kind,
            IdentityReadSurfaceKind::Found
                | IdentityReadSurfaceKind::Redacted
                | IdentityReadSurfaceKind::Stale
        )
    }

    /// Returns whether the surface is stale or degraded.
    pub fn is_stale_or_degraded(&self) -> bool {
        matches!(
            self.read_surface_kind,
            IdentityReadSurfaceKind::Stale | IdentityReadSurfaceKind::Degraded
        )
    }

    /// Asserts that the view remains body-free.
    pub fn assert_body_free(&self) -> Result<(), ContractError> {
        if self.read_material_marker.is_body_free() {
            return Ok(());
        }

        Err(ContractError::invalid_value(
            "read_material_marker",
            "member summary view must remain body-free",
        ))
    }
}

/// Public view for a member anchor read.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GlobalMemberAnchorView {
    /// Member represented by this anchor view.
    pub member_ref: GlobalMemberRef,
    /// Current anchor state kind.
    pub anchor_state_kind: IdentityAnchorStateKind,
    /// Optional body-free reason marker associated with the anchor state.
    pub anchor_reason_ref: Option<IdentityAnchorReasonRef>,
    /// Last anchor state change time.
    pub anchor_changed_at: IdentityTimestamp,
    /// Optional body-free source marker. Redaction may omit this field.
    pub source_ref: Option<IdentitySourceRef>,
    /// Stable summary view ref when projection lookup succeeded.
    pub member_summary_view_ref: Option<MemberSummaryViewRef>,
    /// Anchor safe summary slice when loaded from a projection.
    pub anchor_slice_ref: Option<MemberSummarySliceRef>,
}

/// Public view for a member global lifecycle read.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GlobalLifecycleSummaryView {
    /// Member represented by this lifecycle view.
    pub member_ref: GlobalMemberRef,
    /// Current global lifecycle state kind.
    pub lifecycle_state_kind: GlobalLifecycleStateKind,
    /// Optional body-free lifecycle reason marker.
    pub reason_ref: Option<LifecycleReasonRef>,
    /// Optional body-free governance basis marker.
    pub basis_ref: Option<GovernanceBasisRef>,
    /// Actor that last changed lifecycle, when visible.
    pub changed_by_ref: Option<ActorRef>,
    /// Last lifecycle change time.
    pub changed_at: IdentityTimestamp,
    /// Stable summary view ref when projection lookup succeeded.
    pub member_summary_view_ref: Option<MemberSummaryViewRef>,
    /// Lifecycle safe summary slice when loaded from a projection.
    pub lifecycle_slice_ref: Option<MemberSummarySliceRef>,
}

/// Public view for a member role/capability summary read.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RoleCapabilitySummaryView {
    /// Member represented by this summary.
    pub member_ref: GlobalMemberRef,
    /// Identity-owned summary ref.
    pub summary_ref: RoleCapabilitySummaryRef,
    /// Summary state kind.
    pub summary_state_kind: RoleCapabilitySummaryStateKind,
    /// Source snapshot used by the summary.
    pub source_snapshot_ref: RoleCapabilitySourceSnapshotRef,
    /// Source state, when the snapshot was loaded.
    pub source_state_kind: Option<RoleCapabilitySourceStateKind>,
    /// Optional role source wrapper, redacted when not allowed.
    pub role_source_ref: Option<RoleSourceRef>,
    /// Capability source wrappers allowed by visibility.
    pub capability_source_refs: Vec<CapabilitySourceRef>,
    /// Evidence refs allowed by visibility.
    pub evidence_refs: Vec<CapabilityEvidenceRef>,
    /// Body-free safe summary marker.
    pub safe_summary_ref: Option<RoleCapabilitySafeSummaryRef>,
    /// Stable summary view ref when projection lookup succeeded.
    pub member_summary_view_ref: Option<MemberSummaryViewRef>,
    /// Role/capability safe summary slices from projection.
    pub role_capability_slice_refs: Vec<MemberSummarySliceRef>,
}

/// Public view for one append-only career record.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CareerRecordView {
    /// Career record ref.
    pub career_record_ref: CareerRecordRef,
    /// Member whose career history owns this record.
    pub member_ref: GlobalMemberRef,
    /// Career record state kind.
    pub record_state_kind: CareerRecordStateKind,
    /// Work-owned participation source, when visible.
    pub project_participation_ref: Option<ProjectParticipationRef>,
    /// Work source marker, when visible.
    pub work_source_ref: Option<crate::refs::WorkSourceRef>,
    /// Duplicate source marker, when visible.
    pub source_marker_ref: Option<CareerSourceMarkerRef>,
    /// Redaction-safe career summary marker.
    pub career_summary_ref: Option<CareerSafeSummaryRef>,
    /// Body-free append reason marker, when visible.
    pub append_reason_ref: Option<CareerAppendReasonRef>,
    /// Append time, when visible.
    pub appended_at: Option<IdentityTimestamp>,
    /// Original record explained by this correction.
    pub correction_of_ref: Option<CareerRecordRef>,
    /// Correction record that supersedes this record in interpretation.
    pub superseded_by_ref: Option<CareerRecordRef>,
}

/// Public view for one identity memory/archive reference relation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MemoryReferenceView {
    /// Memory reference relation ref.
    pub memory_reference_ref: MemoryReferenceRef,
    /// Member whose relation owns this reference.
    pub member_ref: GlobalMemberRef,
    /// Current relation state kind.
    pub reference_state_kind: MemoryReferenceStateKind,
    /// External memory carrier ref, when visible.
    pub memory_ref: Option<MemoryRef>,
    /// External archive carrier ref, when visible.
    pub archive_ref: Option<ArchiveRef>,
    /// Archive handoff marker, when visible.
    pub archive_handoff_ref: Option<ArchiveHandoffRef>,
    /// Source marker for the relation state, when visible.
    pub source_ref: Option<MemoryReferenceSourceRef>,
    /// Redaction-safe memory/archive summary marker.
    pub safe_summary_ref: Option<MemorySafeSummaryRef>,
    /// Body-free change reason marker, when visible.
    pub reason_ref: Option<MemoryReferenceReasonRef>,
    /// Last relation change/check time, when visible.
    pub changed_at: Option<IdentityTimestamp>,
}

/// Public redaction-aware trace record view.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityTraceRecordView {
    /// Stable trace record ref.
    pub trace_record_ref: IdentityTraceRecordRef,
    /// Member associated with the change.
    pub member_ref: GlobalMemberRef,
    /// Canonical trace subject.
    pub subject_ref: crate::refs::IdentityTraceSubjectRef,
    /// Canonical audit subject.
    pub audit_subject_ref: IdentityAuditSubjectRef,
    /// Change kind marker.
    pub change_kind_ref: IdentityChangeKindRef,
    /// Committed truth cursor for the accepted change.
    pub source_cursor_ref: IdentityTruthCursor,
    /// Optional body-free reason marker.
    pub reason_ref: Option<IdentityChangeReasonRef>,
    /// Optional body-free source marker.
    pub source_ref: Option<IdentitySourceRef>,
    /// Optional governance basis marker.
    pub basis_ref: Option<GovernanceBasisRef>,
    /// Optional actor or controlled source.
    pub actor_ref: Option<ActorRef>,
    /// Visibility result for a read surface.
    pub visibility_result_ref: VisibilityResultRef,
    /// Optional correction trace that supersedes this record in interpretation.
    pub superseded_by_trace_ref: Option<IdentityTraceRecordRef>,
    /// Material marker used to prevent forbidden bodies.
    pub read_material_marker: IdentityReadMaterialMarker,
    /// Time the accepted change was recorded.
    pub occurred_at: IdentityTimestamp,
}

/// Public redaction-aware audit trail entry view.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuditTrailEntryView {
    /// Stable audit trail ref.
    pub audit_trail_ref: AuditTrailRef,
    /// Canonical audit subject.
    pub audit_subject_ref: IdentityAuditSubjectRef,
    /// Audit scope marker.
    pub audit_scope_ref: AuditScopeRef,
    /// Optional member scope.
    pub member_ref: Option<GlobalMemberRef>,
    /// Trace record included by this entry.
    pub trace_record_ref: IdentityTraceRecordRef,
    /// Change kind marker.
    pub change_kind_ref: IdentityChangeKindRef,
    /// Redaction or visibility result for this entry.
    pub visibility_result_ref: VisibilityResultRef,
    /// Time associated with the trace.
    pub occurred_at: IdentityTimestamp,
}

/// Public body-free projection state view.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProjectionStateView {
    /// Optional stable projection state ref.
    pub projection_state_ref: Option<ProjectionStateRef>,
    /// Projection or derived view being inspected.
    pub projection_ref: IdentityProjectionRef,
    /// Optional member linked to the projection.
    pub member_ref: Option<GlobalMemberRef>,
    /// Optional projection freshness or rebuild state.
    pub state_kind: Option<ProjectionStateKind>,
    /// Optional projection source cursor marker.
    pub source_cursor_ref: Option<IdentityProjectionCursorRef>,
    /// Optional maintenance scope marker.
    pub maintenance_scope_ref: Option<MaintenanceScopeRef>,
    /// Optional safe issue marker.
    pub issue_ref: Option<MaintenanceIssueRef>,
    /// Optional latest checked timestamp.
    pub checked_at: Option<IdentityTimestamp>,
    /// Visibility result for this read surface.
    pub visibility_result_ref: VisibilityResultRef,
}

/// Public body-free sidecar refs for one external reference bundle.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReferenceResolutionSidecarRefsView {
    /// Optional role capability summary safe summary ref.
    pub role_capability_safe_summary_ref: Option<crate::refs::ExternalReferenceSafeSummaryRef>,
    /// Optional career safe summary ref.
    pub career_safe_summary_ref: Option<crate::refs::ExternalReferenceSafeSummaryRef>,
    /// Optional memory safe summary ref.
    pub memory_safe_summary_ref: Option<crate::refs::ExternalReferenceSafeSummaryRef>,
    /// Optional governance basis summary ref.
    pub governance_basis_summary_ref: Option<crate::refs::ExternalReferenceSafeSummaryRef>,
    /// Optional evidence summary ref.
    pub evidence_summary_ref: Option<crate::refs::ExternalReferenceSafeSummaryRef>,
    /// Optional external source version ref.
    pub source_version_ref: Option<crate::refs::ExternalSourceVersionRef>,
}

/// Public body-free external reference resolution view.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReferenceResolutionStateView {
    /// Optional stable resolution state ref.
    pub resolution_state_ref: Option<ReferenceResolutionStateRef>,
    /// External reference bundle being inspected.
    pub external_reference_ref: crate::refs::ExternalReferenceRef,
    /// Optional local owner of the external reference.
    pub owner_ref: Option<crate::refs::IdentityReferenceOwnerRef>,
    /// Optional stored resolution state.
    pub state_kind: Option<ReferenceResolutionStateKind>,
    /// Optional external source version marker.
    pub source_version_ref: Option<crate::refs::ExternalSourceVersionRef>,
    /// Optional body-free safe summary marker.
    pub safe_summary_ref: Option<crate::refs::ExternalReferenceSafeSummaryRef>,
    /// Optional same-bundle typed sidecar refs.
    pub sidecar_refs: Option<ReferenceResolutionSidecarRefsView>,
    /// Optional safe issue marker.
    pub issue_ref: Option<MaintenanceIssueRef>,
    /// Optional latest checked timestamp.
    pub checked_at: Option<IdentityTimestamp>,
    /// Visibility result for this read surface.
    pub visibility_result_ref: VisibilityResultRef,
}

/// Public body-free reconciliation report view.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReconciliationReportView {
    /// Stable report ref.
    pub report_ref: ReconciliationReportRef,
    /// Maintenance scope covered by the report.
    pub maintenance_scope_ref: MaintenanceScopeRef,
    /// Projection, reference, or report targets checked by the report.
    pub target_refs: Vec<IdentityMaintenanceTargetRef>,
    /// Body-free finding refs.
    pub finding_refs: Vec<ReconciliationFindingRef>,
    /// Safe issue refs.
    pub issue_refs: Vec<MaintenanceIssueRef>,
    /// Public report-only state kind.
    pub report_state: ReconciliationReportStateKind,
    /// Optional generator actor or system marker.
    pub generated_by_ref: Option<ActorRef>,
    /// Time the report was generated.
    pub generated_at: IdentityTimestamp,
    /// Visibility result for this read surface.
    pub visibility_result_ref: VisibilityResultRef,
}

/// Public body-free outbox record view.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityOutboxRecordView {
    /// Stable outbox record ref.
    pub outbox_record_ref: IdentityOutboxRecordRef,
    /// Member whose accepted change owns this outbox record.
    pub member_ref: GlobalMemberRef,
    /// Canonical outbound subject marker.
    pub subject_ref: IdentityOutboxSubjectRef,
    /// Accepted change kind marker.
    pub change_kind_ref: IdentityChangeKindRef,
    /// Body-free payload marker.
    pub payload_marker_ref: IdentityOutboxPayloadMarkerRef,
    /// Topic binding marker.
    pub topic_key_ref: TopicKeyRef,
    /// Accepted trace record marker.
    pub trace_record_ref: IdentityTraceRecordRef,
    /// Current outbox publish state.
    pub outbox_state_kind: OutboxStateKind,
    /// Optional publish attempt marker.
    pub attempt_ref: Option<OutboxDeliveryAttemptRef>,
    /// Optional safe issue marker.
    pub issue_ref: Option<OutboxDeliveryIssueRef>,
    /// Create timestamp.
    pub created_at: IdentityTimestamp,
    /// Last update timestamp.
    pub updated_at: IdentityTimestamp,
    /// Visibility result for this read surface.
    pub visibility_result_ref: VisibilityResultRef,
}

/// Public body-free outbox state view.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityOutboxStateView {
    /// Stable outbox record ref.
    pub outbox_record_ref: IdentityOutboxRecordRef,
    /// Canonical outbound subject marker.
    pub subject_ref: IdentityOutboxSubjectRef,
    /// Topic binding marker.
    pub topic_key_ref: TopicKeyRef,
    /// Accepted trace record marker.
    pub trace_record_ref: IdentityTraceRecordRef,
    /// Current outbox publish state.
    pub outbox_state_kind: OutboxStateKind,
    /// Optional publish attempt marker.
    pub attempt_ref: Option<OutboxDeliveryAttemptRef>,
    /// Optional safe issue marker.
    pub issue_ref: Option<OutboxDeliveryIssueRef>,
    /// Body-free payload marker.
    pub payload_marker_ref: IdentityOutboxPayloadMarkerRef,
    /// Latest state change timestamp.
    pub changed_at: IdentityTimestamp,
    /// Visibility result for this read surface.
    pub visibility_result_ref: VisibilityResultRef,
}

/// Public body-free trace handoff state view.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TraceHandoffStateView {
    /// Stable handoff intent ref.
    pub handoff_intent_ref: crate::receipts::TraceHandoffIntentRef,
    /// Member that owns the handoff intent.
    pub member_ref: GlobalMemberRef,
    /// Accepted trace records included in the handoff.
    pub trace_record_refs: Vec<IdentityTraceRecordRef>,
    /// Optional canonical audit trail marker.
    pub audit_trail_ref: Option<AuditTrailRef>,
    /// Handoff target boundary marker.
    pub handoff_target_ref: HandoffTargetRef,
    /// Handoff scope boundary marker.
    pub handoff_scope_ref: HandoffScopeRef,
    /// Body-free handoff material marker.
    pub safe_material_ref: TraceHandoffSafeMaterialRef,
    /// Current handoff lifecycle state.
    pub handoff_state_kind: HandoffStateKind,
    /// Optional delivery attempt marker.
    pub attempt_ref: Option<HandoffAttemptRef>,
    /// Optional delivery receipt marker.
    pub receipt_ref: Option<HandoffReceiptRef>,
    /// Optional safe issue marker.
    pub issue_ref: Option<HandoffIssueRef>,
    /// Create timestamp.
    pub created_at: IdentityTimestamp,
    /// Latest intent update timestamp.
    pub updated_at: IdentityTimestamp,
    /// Latest handoff state change timestamp.
    pub changed_at: IdentityTimestamp,
    /// Visibility result for this read surface.
    pub visibility_result_ref: VisibilityResultRef,
}
