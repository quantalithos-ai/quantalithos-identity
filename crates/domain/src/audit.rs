//! Audit trail assembly helpers.

use identity_contracts::refs::{
    AuditCursorRef, AuditScopeRef, AuditTrailRef, GlobalMemberRef, IdentityAuditSubjectRef,
    IdentityChangeKindRef, IdentityReadSurfaceKind, IdentityTimestamp, IdentityTraceRecordRef,
    VisibilityResultRef,
};

use crate::errors::IdentityDomainError;

/// Audit entry value embedded in an audit trail.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuditTrailEntry {
    /// Trace record included by this entry.
    pub trace_record_ref: IdentityTraceRecordRef,
    /// Change kind marker.
    pub change_kind_ref: IdentityChangeKindRef,
    /// Redaction or visibility result for this entry.
    pub visibility_result_ref: VisibilityResultRef,
    /// Time associated with the trace.
    pub occurred_at: IdentityTimestamp,
}

/// Audit trail assembled from identity trace records.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuditTrail {
    /// Stable audit trail ref.
    pub audit_trail_ref: AuditTrailRef,
    /// Canonical audit subject.
    pub audit_subject_ref: IdentityAuditSubjectRef,
    /// Optional member scope.
    pub member_ref: Option<GlobalMemberRef>,
    /// Audit scope marker.
    pub audit_scope_ref: AuditScopeRef,
    /// Audit entries.
    pub entries: Vec<AuditTrailEntry>,
    /// Visibility result for the trail.
    pub visibility_result_ref: VisibilityResultRef,
    /// Public read surface category.
    pub read_surface_kind: IdentityReadSurfaceKind,
    /// Read pagination cursor.
    pub cursor_ref: Option<AuditCursorRef>,
    /// Time the trail was assembled or materialized.
    pub assembled_at: IdentityTimestamp,
}

impl AuditTrail {
    /// Creates a trail for accepted write materialization with one initial body-free entry.
    pub fn from_accepted_write(
        audit_trail_ref: AuditTrailRef,
        audit_subject_ref: IdentityAuditSubjectRef,
        member_ref: Option<GlobalMemberRef>,
        audit_scope_ref: AuditScopeRef,
        initial_entry: AuditTrailEntry,
        visibility_result_ref: VisibilityResultRef,
        assembled_at: IdentityTimestamp,
    ) -> Result<Self, IdentityDomainError> {
        Self::assemble(
            audit_trail_ref,
            audit_subject_ref,
            member_ref,
            audit_scope_ref,
            vec![initial_entry],
            visibility_result_ref,
            None,
            assembled_at,
        )
    }

    /// Assembles an audit trail from body-free entries.
    #[allow(clippy::too_many_arguments)]
    pub fn assemble(
        audit_trail_ref: AuditTrailRef,
        audit_subject_ref: IdentityAuditSubjectRef,
        member_ref: Option<GlobalMemberRef>,
        audit_scope_ref: AuditScopeRef,
        entries: Vec<AuditTrailEntry>,
        visibility_result_ref: VisibilityResultRef,
        cursor_ref: Option<AuditCursorRef>,
        assembled_at: IdentityTimestamp,
    ) -> Result<Self, IdentityDomainError> {
        let has_entries = !entries.is_empty();
        Ok(Self {
            audit_trail_ref,
            audit_subject_ref,
            member_ref,
            audit_scope_ref,
            entries,
            visibility_result_ref,
            read_surface_kind: if cursor_ref.is_some() || has_entries {
                IdentityReadSurfaceKind::Found
            } else {
                IdentityReadSurfaceKind::Empty
            },
            cursor_ref,
            assembled_at,
        })
    }

    /// Creates an empty audit trail surface.
    pub fn empty(
        audit_trail_ref: AuditTrailRef,
        audit_subject_ref: IdentityAuditSubjectRef,
        audit_scope_ref: AuditScopeRef,
        visibility_result_ref: VisibilityResultRef,
        assembled_at: IdentityTimestamp,
    ) -> Self {
        Self {
            audit_trail_ref,
            audit_subject_ref,
            member_ref: None,
            audit_scope_ref,
            entries: Vec::new(),
            visibility_result_ref,
            read_surface_kind: IdentityReadSurfaceKind::Empty,
            cursor_ref: None,
            assembled_at,
        }
    }

    /// Creates a not-visible audit trail surface.
    pub fn not_visible(
        audit_trail_ref: AuditTrailRef,
        audit_subject_ref: IdentityAuditSubjectRef,
        audit_scope_ref: AuditScopeRef,
        visibility_result_ref: VisibilityResultRef,
        assembled_at: IdentityTimestamp,
    ) -> Self {
        Self {
            audit_trail_ref,
            audit_subject_ref,
            member_ref: None,
            audit_scope_ref,
            entries: Vec::new(),
            visibility_result_ref,
            read_surface_kind: IdentityReadSurfaceKind::NotVisible,
            cursor_ref: None,
            assembled_at,
        }
    }

    /// Returns whether the trail contains the provided trace ref.
    pub fn contains_trace(&self, trace_record_ref: &IdentityTraceRecordRef) -> bool {
        self.entries
            .iter()
            .any(|entry| &entry.trace_record_ref == trace_record_ref)
    }

    /// Returns a scope-filtered trail clone.
    pub fn filter_by_scope(&self, audit_scope_ref: &AuditScopeRef) -> Self {
        if &self.audit_scope_ref == audit_scope_ref {
            return self.clone();
        }

        Self {
            audit_scope_ref: audit_scope_ref.clone(),
            entries: Vec::new(),
            read_surface_kind: IdentityReadSurfaceKind::Empty,
            ..self.clone()
        }
    }
}
