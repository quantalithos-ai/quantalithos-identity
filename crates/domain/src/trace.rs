//! Accepted trace material helpers.

use core_contracts::actor::ActorRef;
use identity_contracts::refs::{
    GlobalMemberRef, GovernanceBasisRef, IdentityAuditSubjectRef, IdentityChangeKindRef,
    IdentityChangeReasonRef, IdentitySourceRef, IdentityTimestamp, IdentityTraceRecordRef,
    IdentityTraceSubjectRef, IdentityTruthCursor, VisibilityResultRef,
};
use identity_contracts::views::IdentityReadMaterialMarker;

use crate::errors::IdentityDomainError;

/// Persistent append-only trace material for an accepted identity change.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdentityTraceRecord {
    /// Stable trace record ref.
    pub trace_record_ref: IdentityTraceRecordRef,
    /// Member associated with the change.
    pub member_ref: GlobalMemberRef,
    /// Canonical trace subject.
    pub subject_ref: IdentityTraceSubjectRef,
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
    pub visibility_result_ref: Option<VisibilityResultRef>,
    /// Optional correction trace that supersedes this record in interpretation.
    pub superseded_by_trace_ref: Option<IdentityTraceRecordRef>,
    /// Material marker used to prevent forbidden bodies.
    pub read_material_marker: IdentityReadMaterialMarker,
    /// Time the accepted change was recorded.
    pub occurred_at: IdentityTimestamp,
}

impl IdentityTraceRecord {
    /// Creates a new accepted trace record from formal accepted change material.
    #[allow(clippy::too_many_arguments)]
    pub fn from_accepted_change(
        trace_record_ref: IdentityTraceRecordRef,
        member_ref: GlobalMemberRef,
        subject_ref: IdentityTraceSubjectRef,
        audit_subject_ref: IdentityAuditSubjectRef,
        change_kind_ref: IdentityChangeKindRef,
        source_cursor_ref: IdentityTruthCursor,
        reason_ref: Option<IdentityChangeReasonRef>,
        source_ref: Option<IdentitySourceRef>,
        basis_ref: Option<GovernanceBasisRef>,
        actor_ref: Option<ActorRef>,
        read_material_marker: IdentityReadMaterialMarker,
        occurred_at: IdentityTimestamp,
    ) -> Result<Self, IdentityDomainError> {
        if !read_material_marker.is_body_free() {
            return Err(IdentityDomainError::invalid_input(
                "read_material_marker",
                "trace material must remain body-free",
            ));
        }

        Ok(Self {
            trace_record_ref,
            member_ref,
            subject_ref,
            audit_subject_ref,
            change_kind_ref,
            source_cursor_ref,
            reason_ref,
            source_ref,
            basis_ref,
            actor_ref,
            visibility_result_ref: None,
            superseded_by_trace_ref: None,
            read_material_marker,
            occurred_at,
        })
    }

    /// Creates a redacted read surface clone of the trace record.
    pub fn redacted_read_surface(&self, visibility_result_ref: VisibilityResultRef) -> Self {
        let mut cloned = self.clone();
        cloned.visibility_result_ref = Some(visibility_result_ref);
        cloned
    }

    /// Marks the trace as superseded by a formal correction trace.
    pub fn mark_superseded_by_correction(
        &mut self,
        correction_trace_ref: IdentityTraceRecordRef,
    ) -> Result<(), IdentityDomainError> {
        if self.trace_record_ref == correction_trace_ref {
            return Err(IdentityDomainError::invalid_input(
                "correction_trace_ref",
                "correction trace must differ from the original trace",
            ));
        }

        self.superseded_by_trace_ref = Some(correction_trace_ref);
        Ok(())
    }

    /// Returns whether the trace belongs to the provided member.
    pub fn belongs_to(&self, member_ref: &GlobalMemberRef) -> bool {
        &self.member_ref == member_ref
    }

    /// Returns whether the trace matches the provided trace subject.
    pub fn matches_subject(&self, subject_ref: &IdentityTraceSubjectRef) -> bool {
        &self.subject_ref == subject_ref
    }

    /// Returns whether the trace matches the provided audit subject.
    pub fn matches_audit_subject(&self, audit_subject_ref: &IdentityAuditSubjectRef) -> bool {
        &self.audit_subject_ref == audit_subject_ref
    }

    /// Returns whether the trace has been superseded by a correction trace.
    pub fn is_superseded(&self) -> bool {
        self.superseded_by_trace_ref.is_some()
    }

    /// Asserts that the trace remains body-free.
    pub fn assert_body_free(&self) -> Result<(), IdentityDomainError> {
        if self.read_material_marker.is_body_free() {
            return Ok(());
        }

        Err(IdentityDomainError::invalid_input(
            "read_material_marker",
            "trace material must remain body-free",
        ))
    }
}
