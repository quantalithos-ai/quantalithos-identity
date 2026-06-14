//! External reference resolution state helpers.

use identity_contracts::receipts::MaintenanceIssueRef;
use identity_contracts::refs::{
    ExternalReferenceRef, ExternalReferenceSafeSummaryRef, ExternalSourceVersionRef,
    IdentityReferenceOwnerRef, IdentityTimestamp, ReferenceResolutionStateRef,
};

use crate::errors::IdentityDomainError;

/// External reference resolution state kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReferenceResolutionStateKind {
    /// External reference was resolved with safe summary material.
    Resolved,
    /// External source version changed or resolution is stale.
    Stale,
    /// External dependency is unavailable.
    Unavailable,
    /// External reference is not recognized by the formal resolver boundary.
    Unrecognized,
    /// Reference state requires report-only reconciliation.
    PendingReconciliation,
    /// Refresh attempt failed.
    RefreshFailed,
}

/// Resolution state for an external reference used by identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReferenceResolutionState {
    /// Resolution state identity.
    pub resolution_state_ref: ReferenceResolutionStateRef,
    /// External reference being resolved.
    pub external_reference_ref: ExternalReferenceRef,
    /// Local identity owner that uses the external reference.
    pub reference_owner_ref: IdentityReferenceOwnerRef,
    /// Current resolution state.
    pub state_kind: ReferenceResolutionStateKind,
    /// External source version observed by resolver or event mapper.
    pub source_version_ref: Option<ExternalSourceVersionRef>,
    /// Body-free safe summary marker for the resolved external reference.
    pub safe_summary_ref: Option<ExternalReferenceSafeSummaryRef>,
    /// Safe issue marker for unavailable, unrecognized, pending, or failed states.
    pub issue_ref: Option<MaintenanceIssueRef>,
    /// Latest resolution or refresh timestamp.
    pub checked_at: IdentityTimestamp,
}

impl ReferenceResolutionState {
    /// Creates a resolved reference state.
    pub fn resolved(
        resolution_state_ref: ReferenceResolutionStateRef,
        external_reference_ref: ExternalReferenceRef,
        reference_owner_ref: IdentityReferenceOwnerRef,
        source_version_ref: ExternalSourceVersionRef,
        safe_summary_ref: ExternalReferenceSafeSummaryRef,
        checked_at: IdentityTimestamp,
    ) -> Self {
        Self {
            resolution_state_ref,
            external_reference_ref,
            reference_owner_ref,
            state_kind: ReferenceResolutionStateKind::Resolved,
            source_version_ref: Some(source_version_ref),
            safe_summary_ref: Some(safe_summary_ref),
            issue_ref: None,
            checked_at,
        }
    }

    /// Creates an unavailable reference state.
    pub fn unavailable(
        resolution_state_ref: ReferenceResolutionStateRef,
        external_reference_ref: ExternalReferenceRef,
        reference_owner_ref: IdentityReferenceOwnerRef,
        issue_ref: MaintenanceIssueRef,
        checked_at: IdentityTimestamp,
    ) -> Self {
        Self {
            resolution_state_ref,
            external_reference_ref,
            reference_owner_ref,
            state_kind: ReferenceResolutionStateKind::Unavailable,
            source_version_ref: None,
            safe_summary_ref: None,
            issue_ref: Some(issue_ref),
            checked_at,
        }
    }

    /// Creates an unrecognized reference state.
    pub fn unrecognized(
        resolution_state_ref: ReferenceResolutionStateRef,
        external_reference_ref: ExternalReferenceRef,
        reference_owner_ref: IdentityReferenceOwnerRef,
        issue_ref: MaintenanceIssueRef,
        checked_at: IdentityTimestamp,
    ) -> Self {
        Self {
            resolution_state_ref,
            external_reference_ref,
            reference_owner_ref,
            state_kind: ReferenceResolutionStateKind::Unrecognized,
            source_version_ref: None,
            safe_summary_ref: None,
            issue_ref: Some(issue_ref),
            checked_at,
        }
    }

    /// Returns whether the state is usable for accepted truth updates.
    pub fn is_usable_for_truth_update(&self) -> bool {
        self.state_kind == ReferenceResolutionStateKind::Resolved
            && self.source_version_ref.is_some()
            && self.safe_summary_ref.is_some()
    }

    /// Returns whether the state is report-only.
    pub fn is_report_only(&self) -> bool {
        matches!(
            self.state_kind,
            ReferenceResolutionStateKind::Unavailable
                | ReferenceResolutionStateKind::Unrecognized
                | ReferenceResolutionStateKind::PendingReconciliation
                | ReferenceResolutionStateKind::RefreshFailed
        )
    }

    /// Returns whether reconciliation is required.
    pub fn requires_reconciliation(&self) -> bool {
        matches!(
            self.state_kind,
            ReferenceResolutionStateKind::PendingReconciliation
                | ReferenceResolutionStateKind::RefreshFailed
        )
    }

    /// Marks the reference stale.
    pub fn mark_stale(
        &mut self,
        source_version_ref: ExternalSourceVersionRef,
        checked_at: IdentityTimestamp,
    ) -> Result<(), IdentityDomainError> {
        match self.state_kind {
            ReferenceResolutionStateKind::Resolved
            | ReferenceResolutionStateKind::RefreshFailed => {
                self.state_kind = ReferenceResolutionStateKind::Stale;
                self.source_version_ref = Some(source_version_ref);
                self.checked_at = checked_at;
                Ok(())
            }
            _ => Err(IdentityDomainError::invalid_state_transition(
                "ReferenceResolutionState",
                "reference state can become stale only from resolved or refresh failed state",
            )),
        }
    }

    /// Marks the reference unavailable.
    pub fn mark_unavailable(
        &mut self,
        issue_ref: MaintenanceIssueRef,
        checked_at: IdentityTimestamp,
    ) -> Result<(), IdentityDomainError> {
        match self.state_kind {
            ReferenceResolutionStateKind::Resolved
            | ReferenceResolutionStateKind::Stale
            | ReferenceResolutionStateKind::PendingReconciliation => {
                self.state_kind = ReferenceResolutionStateKind::Unavailable;
                self.issue_ref = Some(issue_ref);
                self.safe_summary_ref = None;
                self.checked_at = checked_at;
                Ok(())
            }
            _ => Err(IdentityDomainError::invalid_state_transition(
                "ReferenceResolutionState",
                "reference state cannot become unavailable from the current state",
            )),
        }
    }

    /// Marks the reference pending reconciliation.
    pub fn mark_pending_reconciliation(
        &mut self,
        issue_ref: MaintenanceIssueRef,
        checked_at: IdentityTimestamp,
    ) -> Result<(), IdentityDomainError> {
        match self.state_kind {
            ReferenceResolutionStateKind::Resolved
            | ReferenceResolutionStateKind::Stale
            | ReferenceResolutionStateKind::Unavailable
            | ReferenceResolutionStateKind::Unrecognized
            | ReferenceResolutionStateKind::RefreshFailed => {
                self.state_kind = ReferenceResolutionStateKind::PendingReconciliation;
                self.issue_ref = Some(issue_ref);
                self.checked_at = checked_at;
                Ok(())
            }
            _ => Err(IdentityDomainError::invalid_state_transition(
                "ReferenceResolutionState",
                "reference state cannot enter pending reconciliation from the current state",
            )),
        }
    }

    /// Marks the refresh failed.
    pub fn mark_refresh_failed(
        &mut self,
        issue_ref: MaintenanceIssueRef,
        checked_at: IdentityTimestamp,
    ) -> Result<(), IdentityDomainError> {
        match self.state_kind {
            ReferenceResolutionStateKind::Stale
            | ReferenceResolutionStateKind::Unavailable
            | ReferenceResolutionStateKind::PendingReconciliation => {
                self.state_kind = ReferenceResolutionStateKind::RefreshFailed;
                self.issue_ref = Some(issue_ref);
                self.checked_at = checked_at;
                Ok(())
            }
            _ => Err(IdentityDomainError::invalid_state_transition(
                "ReferenceResolutionState",
                "reference refresh can fail only from stale, unavailable, or pending reconciliation",
            )),
        }
    }
}
