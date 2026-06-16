//! Public read-model and visibility support shells.

use core_contracts::actor::ActorRef;
use serde::{Deserialize, Serialize};

use crate::errors::ContractError;
use crate::refs::{
    ConsumerRef, GlobalMemberRef, IdentityReadSubjectRef, IdentityReadSurfaceKind,
    IdentitySourceRef, IdentityTruthCursor, MemberSummaryViewRef, RedactionProfileRef,
    VisibilityContextRef, VisibilityResultRef, VisibilityScopeRef,
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
    /// Body-free result marker.
    pub visibility_result_ref: VisibilityResultRef,
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
