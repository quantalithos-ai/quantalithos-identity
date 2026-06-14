//! Projection freshness and rebuild state helpers.

use identity_contracts::receipts::MaintenanceIssueRef;
use identity_contracts::refs::{
    GlobalMemberRef, IdentityOperationChannel, IdentityProjectionCursorRef, IdentityProjectionRef,
    IdentityTimestamp, MaintenanceScopeRef, ProjectionStateRef,
};

use crate::errors::IdentityDomainError;

/// Projection freshness and rebuild state kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProjectionStateKind {
    /// Projection is aligned with a known source cursor.
    Fresh,
    /// Projection is behind or known to be stale.
    Stale,
    /// Projection rebuild has been formally requested.
    RebuildPending,
    /// Projection was rebuilt successfully.
    Rebuilt,
    /// Projection may be served only in degraded form.
    Degraded,
    /// Projection rebuild failed.
    RebuildFailed,
}

/// Projection freshness and rebuild marker for identity-owned derived views.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectionState {
    /// Projection state identity.
    pub projection_state_ref: ProjectionStateRef,
    /// Projection or derived view being tracked.
    pub projection_ref: IdentityProjectionRef,
    /// Member related to this projection when the projection is member-scoped.
    pub member_ref: Option<GlobalMemberRef>,
    /// Current projection state.
    pub state_kind: ProjectionStateKind,
    /// Accepted fact or committed scan cursor that this projection reflects.
    pub source_cursor_ref: Option<IdentityProjectionCursorRef>,
    /// Scope used by the latest maintenance action.
    pub maintenance_scope_ref: Option<MaintenanceScopeRef>,
    /// Safe issue marker when projection is degraded or failed.
    pub issue_ref: Option<MaintenanceIssueRef>,
    /// Latest check or rebuild timestamp.
    pub checked_at: IdentityTimestamp,
}

impl ProjectionState {
    /// Creates a fresh projection state.
    pub fn fresh(
        projection_state_ref: ProjectionStateRef,
        projection_ref: IdentityProjectionRef,
        member_ref: Option<GlobalMemberRef>,
        source_cursor_ref: IdentityProjectionCursorRef,
        checked_at: IdentityTimestamp,
    ) -> Self {
        Self {
            projection_state_ref,
            projection_ref,
            member_ref,
            state_kind: ProjectionStateKind::Fresh,
            source_cursor_ref: Some(source_cursor_ref),
            maintenance_scope_ref: None,
            issue_ref: None,
            checked_at,
        }
    }

    /// Creates a stale projection state.
    pub fn stale(
        projection_state_ref: ProjectionStateRef,
        projection_ref: IdentityProjectionRef,
        member_ref: Option<GlobalMemberRef>,
        source_cursor_ref: IdentityProjectionCursorRef,
        maintenance_scope_ref: MaintenanceScopeRef,
        checked_at: IdentityTimestamp,
    ) -> Self {
        Self {
            projection_state_ref,
            projection_ref,
            member_ref,
            state_kind: ProjectionStateKind::Stale,
            source_cursor_ref: Some(source_cursor_ref),
            maintenance_scope_ref: Some(maintenance_scope_ref),
            issue_ref: None,
            checked_at,
        }
    }

    /// Creates a failed projection state.
    pub fn failed(
        projection_state_ref: ProjectionStateRef,
        projection_ref: IdentityProjectionRef,
        issue_ref: MaintenanceIssueRef,
        maintenance_scope_ref: MaintenanceScopeRef,
        checked_at: IdentityTimestamp,
    ) -> Self {
        Self {
            projection_state_ref,
            projection_ref,
            member_ref: None,
            state_kind: ProjectionStateKind::RebuildFailed,
            source_cursor_ref: None,
            maintenance_scope_ref: Some(maintenance_scope_ref),
            issue_ref: Some(issue_ref),
            checked_at,
        }
    }

    /// Returns whether the projection is fresh.
    pub fn is_fresh(&self) -> bool {
        self.state_kind == ProjectionStateKind::Fresh
    }

    /// Returns whether the projection may be served for read surfaces.
    pub fn can_serve_read(&self) -> bool {
        !matches!(self.state_kind, ProjectionStateKind::RebuildPending)
    }

    /// Returns whether the projection requires an explicit rebuild path.
    pub fn requires_rebuild(&self) -> bool {
        matches!(
            self.state_kind,
            ProjectionStateKind::Stale
                | ProjectionStateKind::Degraded
                | ProjectionStateKind::RebuildFailed
        )
    }

    /// Marks the projection stale.
    pub fn mark_stale(
        &mut self,
        source_cursor_ref: IdentityProjectionCursorRef,
        maintenance_scope_ref: MaintenanceScopeRef,
        checked_at: IdentityTimestamp,
    ) -> Result<(), IdentityDomainError> {
        match self.state_kind {
            ProjectionStateKind::Fresh | ProjectionStateKind::Rebuilt => {
                self.state_kind = ProjectionStateKind::Stale;
                self.source_cursor_ref = Some(source_cursor_ref);
                self.maintenance_scope_ref = Some(maintenance_scope_ref);
                self.issue_ref = None;
                self.checked_at = checked_at;
                Ok(())
            }
            _ => Err(IdentityDomainError::invalid_state_transition(
                "ProjectionState",
                "projection can become stale only from fresh or rebuilt state",
            )),
        }
    }

    /// Marks the projection rebuild pending.
    pub fn mark_rebuild_pending(
        &mut self,
        maintenance_scope_ref: MaintenanceScopeRef,
        checked_at: IdentityTimestamp,
        operation_channel: IdentityOperationChannel,
    ) -> Result<(), IdentityDomainError> {
        if operation_channel != IdentityOperationChannel::Job {
            return Err(IdentityDomainError::write_channel_denied(
                "ProjectionState",
                operation_channel,
                "only maintenance job channel may request projection rebuild",
            ));
        }

        match self.state_kind {
            ProjectionStateKind::Fresh
            | ProjectionStateKind::Stale
            | ProjectionStateKind::Degraded
            | ProjectionStateKind::RebuildFailed => {
                self.state_kind = ProjectionStateKind::RebuildPending;
                self.maintenance_scope_ref = Some(maintenance_scope_ref);
                self.checked_at = checked_at;
                Ok(())
            }
            _ => Err(IdentityDomainError::invalid_state_transition(
                "ProjectionState",
                "projection cannot enter rebuild pending from the current state",
            )),
        }
    }

    /// Marks the projection rebuilt.
    pub fn mark_rebuilt(
        &mut self,
        source_cursor_ref: IdentityProjectionCursorRef,
        checked_at: IdentityTimestamp,
    ) -> Result<(), IdentityDomainError> {
        match self.state_kind {
            ProjectionStateKind::RebuildPending => {
                self.state_kind = ProjectionStateKind::Rebuilt;
                self.source_cursor_ref = Some(source_cursor_ref);
                self.issue_ref = None;
                self.checked_at = checked_at;
                Ok(())
            }
            _ => Err(IdentityDomainError::invalid_state_transition(
                "ProjectionState",
                "projection can be marked rebuilt only from rebuild pending state",
            )),
        }
    }

    /// Marks the projection degraded.
    pub fn mark_degraded(
        &mut self,
        issue_ref: MaintenanceIssueRef,
        checked_at: IdentityTimestamp,
    ) -> Result<(), IdentityDomainError> {
        match self.state_kind {
            ProjectionStateKind::Fresh
            | ProjectionStateKind::Stale
            | ProjectionStateKind::Rebuilt
            | ProjectionStateKind::RebuildPending => {
                self.state_kind = ProjectionStateKind::Degraded;
                self.issue_ref = Some(issue_ref);
                self.checked_at = checked_at;
                Ok(())
            }
            _ => Err(IdentityDomainError::invalid_state_transition(
                "ProjectionState",
                "projection cannot become degraded from the current state",
            )),
        }
    }

    /// Marks the projection rebuild failed.
    pub fn mark_rebuild_failed(
        &mut self,
        issue_ref: MaintenanceIssueRef,
        checked_at: IdentityTimestamp,
    ) -> Result<(), IdentityDomainError> {
        match self.state_kind {
            ProjectionStateKind::RebuildPending => {
                self.state_kind = ProjectionStateKind::RebuildFailed;
                self.issue_ref = Some(issue_ref);
                self.checked_at = checked_at;
                Ok(())
            }
            _ => Err(IdentityDomainError::invalid_state_transition(
                "ProjectionState",
                "projection rebuild can fail only from rebuild pending state",
            )),
        }
    }
}
