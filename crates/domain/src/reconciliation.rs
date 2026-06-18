//! Report-only maintenance and reconciliation helpers.

use core_contracts::actor::ActorRef;
use identity_contracts::receipts::MaintenanceIssueRef;
use identity_contracts::refs::{
    ExternalReferenceRef, IdentityMaintenanceTargetKind, IdentityMaintenanceTargetRef,
    IdentityOperationChannel, IdentityProjectionRef, IdentityTimestamp, MaintenanceScopeRef,
    ReconciliationFindingIntentRef, ReconciliationFindingMaterial,
    ReconciliationFindingMaterialKind, ReconciliationFindingRef, ReconciliationReportRef,
};

use crate::errors::IdentityDomainError;

/// Maintenance intent category.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdentityMaintenanceIntent {
    /// Rebuild an identity-owned projection.
    RebuildProjection,
    /// Refresh external reference state.
    RefreshReference,
    /// Generate reconciliation report.
    Reconcile,
    /// Attempt to repair identity truth.
    RepairIdentityTruth,
    /// Attempt to repair external truth.
    RepairExternalTruth,
}

/// Report-only reconciliation report state kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReconciliationReportStateKind {
    /// Report was generated and can be read.
    Generated,
    /// No finding was detected in the requested scope.
    NoFinding,
    /// One or more findings were detected.
    FindingDetected,
    /// Report is partial because part of the scope failed or was unavailable.
    Partial,
    /// Report generation failed.
    Failed,
}

/// Guard that keeps identity maintenance report-only.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReconciliationPolicy {
    /// Scope covered by the maintenance operation.
    pub maintenance_scope_ref: MaintenanceScopeRef,
    /// Operation channel attempting maintenance.
    pub operation_channel: IdentityOperationChannel,
    /// Optional actor or system actor for controlled maintenance.
    pub actor_ref: Option<ActorRef>,
    /// Maintenance target.
    pub target_ref: IdentityMaintenanceTargetRef,
    /// Maintenance intent.
    pub maintenance_intent: IdentityMaintenanceIntent,
    /// Optional finding intent.
    pub finding_intent_ref: Option<ReconciliationFindingIntentRef>,
    /// Optional finding material marker.
    pub finding_material: Option<ReconciliationFindingMaterial>,
}

impl ReconciliationPolicy {
    /// Creates a projection rebuild guard.
    pub fn for_projection_rebuild(
        maintenance_scope_ref: MaintenanceScopeRef,
        projection_ref: IdentityProjectionRef,
        actor_ref: Option<ActorRef>,
        operation_channel: IdentityOperationChannel,
    ) -> Self {
        Self {
            maintenance_scope_ref,
            operation_channel,
            actor_ref,
            target_ref: IdentityMaintenanceTargetRef::new(
                IdentityMaintenanceTargetKind::Projection,
                projection_ref.projection_ref,
            ),
            maintenance_intent: IdentityMaintenanceIntent::RebuildProjection,
            finding_intent_ref: None,
            finding_material: None,
        }
    }

    /// Creates a reference refresh guard.
    pub fn for_reference_refresh(
        maintenance_scope_ref: MaintenanceScopeRef,
        external_reference_ref: ExternalReferenceRef,
        actor_ref: Option<ActorRef>,
        operation_channel: IdentityOperationChannel,
    ) -> Self {
        Self {
            maintenance_scope_ref,
            operation_channel,
            actor_ref,
            target_ref: IdentityMaintenanceTargetRef::new(
                IdentityMaintenanceTargetKind::ReferenceResolution,
                external_reference_ref.source_ref,
            ),
            maintenance_intent: IdentityMaintenanceIntent::RefreshReference,
            finding_intent_ref: None,
            finding_material: None,
        }
    }

    /// Creates a reconciliation guard.
    pub fn for_reconciliation(
        maintenance_scope_ref: MaintenanceScopeRef,
        target_ref: IdentityMaintenanceTargetRef,
        finding_intent_ref: ReconciliationFindingIntentRef,
        finding_material: ReconciliationFindingMaterial,
        actor_ref: Option<ActorRef>,
        operation_channel: IdentityOperationChannel,
    ) -> Self {
        Self {
            maintenance_scope_ref,
            operation_channel,
            actor_ref,
            target_ref,
            maintenance_intent: IdentityMaintenanceIntent::Reconcile,
            finding_intent_ref: Some(finding_intent_ref),
            finding_material: Some(finding_material),
        }
    }

    /// Rejects repair intents against identity truth.
    pub fn assert_not_truth_write(&self) -> Result<(), IdentityDomainError> {
        if self.maintenance_intent == IdentityMaintenanceIntent::RepairIdentityTruth {
            return Err(IdentityDomainError::policy_denied(
                "ReconciliationPolicy",
                "maintenance cannot repair identity truth",
            ));
        }

        Ok(())
    }

    /// Rejects repair intents against external truth.
    pub fn assert_not_cross_repo_repair(&self) -> Result<(), IdentityDomainError> {
        if self.maintenance_intent == IdentityMaintenanceIntent::RepairExternalTruth {
            return Err(IdentityDomainError::policy_denied(
                "ReconciliationPolicy",
                "maintenance cannot repair external truth",
            ));
        }

        Ok(())
    }

    /// Rejects refresh and rebuild attempts from query paths.
    pub fn assert_not_query_path_refresh(&self) -> Result<(), IdentityDomainError> {
        if self.operation_channel.is_read_only() {
            return Err(IdentityDomainError::write_channel_denied(
                "ReconciliationPolicy",
                self.operation_channel,
                "query and projection-read channels cannot rebuild, refresh, or reconcile",
            ));
        }

        Ok(())
    }

    /// Rejects forbidden finding material.
    pub fn assert_body_free(&self) -> Result<(), IdentityDomainError> {
        let Some(material) = self.finding_material.as_ref() else {
            return Ok(());
        };

        match material.material_kind {
            ReconciliationFindingMaterialKind::SafeRefsOnly
            | ReconciliationFindingMaterialKind::IssueRefsOnly => Ok(()),
            ReconciliationFindingMaterialKind::ForbiddenExternalBody
            | ReconciliationFindingMaterialKind::ForbiddenDiagnosticBody
            | ReconciliationFindingMaterialKind::ForbiddenSecret => {
                Err(IdentityDomainError::policy_denied(
                    "ReconciliationPolicy",
                    "reconciliation finding material must remain body-free",
                ))
            }
        }
    }

    /// Asserts that the maintenance path is report-only.
    pub fn assert_report_only(&self) -> Result<(), IdentityDomainError> {
        self.assert_not_truth_write()?;
        self.assert_not_cross_repo_repair()?;
        self.assert_not_query_path_refresh()?;
        self.assert_body_free()?;
        Ok(())
    }
}

/// Report-only reconciliation result for identity maintenance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReconciliationReport {
    /// Report identity.
    pub report_ref: ReconciliationReportRef,
    /// Scope covered by this report.
    pub maintenance_scope_ref: MaintenanceScopeRef,
    /// Projection, reference, or report targets checked by this report.
    pub target_refs: Vec<IdentityMaintenanceTargetRef>,
    /// Body-free finding refs.
    pub finding_refs: Vec<ReconciliationFindingRef>,
    /// Safe issue refs for drift, unavailable dependency, partial result, or failure.
    pub issue_refs: Vec<MaintenanceIssueRef>,
    /// Report state.
    pub report_state: ReconciliationReportStateKind,
    /// Optional actor or system actor that generated the report.
    pub generated_by_ref: Option<ActorRef>,
    /// Report generation timestamp.
    pub generated_at: IdentityTimestamp,
}

impl ReconciliationReport {
    /// Creates a report from prepared markers.
    pub fn generated(
        report_ref: ReconciliationReportRef,
        maintenance_scope_ref: MaintenanceScopeRef,
        target_refs: Vec<IdentityMaintenanceTargetRef>,
        finding_refs: Vec<ReconciliationFindingRef>,
        issue_refs: Vec<MaintenanceIssueRef>,
        generated_by_ref: Option<ActorRef>,
        generated_at: IdentityTimestamp,
    ) -> Result<Self, IdentityDomainError> {
        if target_refs.is_empty() {
            return Err(IdentityDomainError::missing_required_field("target_refs"));
        }

        let report_state = if finding_refs.is_empty() && issue_refs.is_empty() {
            ReconciliationReportStateKind::Generated
        } else if finding_refs.is_empty() {
            ReconciliationReportStateKind::Partial
        } else {
            ReconciliationReportStateKind::FindingDetected
        };

        Ok(Self {
            report_ref,
            maintenance_scope_ref,
            target_refs,
            finding_refs,
            issue_refs,
            report_state,
            generated_by_ref,
            generated_at,
        })
    }

    /// Creates an explicit no-finding report.
    pub fn no_finding(
        report_ref: ReconciliationReportRef,
        maintenance_scope_ref: MaintenanceScopeRef,
        target_refs: Vec<IdentityMaintenanceTargetRef>,
        generated_by_ref: Option<ActorRef>,
        generated_at: IdentityTimestamp,
    ) -> Result<Self, IdentityDomainError> {
        Ok(Self {
            report_ref,
            maintenance_scope_ref,
            target_refs,
            finding_refs: Vec::new(),
            issue_refs: Vec::new(),
            report_state: ReconciliationReportStateKind::NoFinding,
            generated_by_ref,
            generated_at,
        })
    }

    /// Creates a failed report.
    pub fn failed(
        report_ref: ReconciliationReportRef,
        maintenance_scope_ref: MaintenanceScopeRef,
        issue_ref: MaintenanceIssueRef,
        generated_by_ref: Option<ActorRef>,
        generated_at: IdentityTimestamp,
    ) -> Result<Self, IdentityDomainError> {
        Ok(Self {
            report_ref,
            maintenance_scope_ref,
            target_refs: Vec::new(),
            finding_refs: Vec::new(),
            issue_refs: vec![issue_ref],
            report_state: ReconciliationReportStateKind::Failed,
            generated_by_ref,
            generated_at,
        })
    }

    /// Appends a finding before persistence.
    pub fn append_finding(
        &mut self,
        finding_ref: ReconciliationFindingRef,
        issue_ref: Option<MaintenanceIssueRef>,
    ) -> Result<(), IdentityDomainError> {
        match self.report_state {
            ReconciliationReportStateKind::Generated
            | ReconciliationReportStateKind::NoFinding
            | ReconciliationReportStateKind::FindingDetected => {
                self.finding_refs.push(finding_ref);
                if let Some(issue_ref) = issue_ref {
                    self.issue_refs.push(issue_ref);
                }
                self.report_state = ReconciliationReportStateKind::FindingDetected;
                Ok(())
            }
            _ => Err(IdentityDomainError::invalid_state_transition(
                "ReconciliationReport",
                "finding may be appended only to generated, no-finding, or finding-detected report",
            )),
        }
    }

    /// Marks the report partial.
    pub fn mark_partial(
        &mut self,
        issue_ref: MaintenanceIssueRef,
        generated_at: IdentityTimestamp,
    ) -> Result<(), IdentityDomainError> {
        match self.report_state {
            ReconciliationReportStateKind::Generated
            | ReconciliationReportStateKind::NoFinding
            | ReconciliationReportStateKind::FindingDetected => {
                self.issue_refs.push(issue_ref);
                self.report_state = ReconciliationReportStateKind::Partial;
                self.generated_at = generated_at;
                Ok(())
            }
            _ => Err(IdentityDomainError::invalid_state_transition(
                "ReconciliationReport",
                "partial marker may be applied only before report becomes failed",
            )),
        }
    }

    /// Marks the report failed.
    pub fn mark_failed(
        &mut self,
        issue_ref: MaintenanceIssueRef,
        generated_at: IdentityTimestamp,
    ) -> Result<(), IdentityDomainError> {
        match self.report_state {
            ReconciliationReportStateKind::Generated
            | ReconciliationReportStateKind::NoFinding
            | ReconciliationReportStateKind::FindingDetected
            | ReconciliationReportStateKind::Partial => {
                self.issue_refs.push(issue_ref);
                self.report_state = ReconciliationReportStateKind::Failed;
                self.generated_at = generated_at;
                Ok(())
            }
            ReconciliationReportStateKind::Failed => {
                Err(IdentityDomainError::invalid_state_transition(
                    "ReconciliationReport",
                    "failed report cannot fail again",
                ))
            }
        }
    }

    /// Returns whether the report has any findings.
    pub fn has_findings(&self) -> bool {
        !self.finding_refs.is_empty()
    }

    /// Returns the finding count.
    pub fn finding_count(&self) -> usize {
        self.finding_refs.len()
    }

    /// Returns whether the report is failed.
    pub fn is_failed(&self) -> bool {
        self.report_state == ReconciliationReportStateKind::Failed
    }

    /// Returns whether the report remains report-only material.
    pub fn assert_report_only(&self) -> Result<(), IdentityDomainError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use core_contracts::actor::{ActorKind, ActorRef};
    use identity_contracts::receipts::MaintenanceIssueRef;
    use identity_contracts::refs::{
        ExternalReferenceKind, ExternalReferenceRef, ExternalReferenceSafeSummaryRef,
        ExternalSourceRef, ExternalSourceVersionRef, GlobalMemberId, GlobalMemberRef,
        IdentityOperationChannel, IdentityProjectionCursorRef, IdentityProjectionRef,
        IdentityReferenceOwnerKind, IdentityReferenceOwnerRef, IdentitySourceOwner,
        IdentitySourceRef, IdentityTimestamp, MaintenanceScopeRef, ProjectionStateId,
        ProjectionStateRef, ReconciliationFindingRef, ReconciliationReportRef,
        ReferenceResolutionStateId, ReferenceResolutionStateRef,
    };

    use super::{
        IdentityMaintenanceIntent, ReconciliationFindingMaterial,
        ReconciliationFindingMaterialKind, ReconciliationPolicy, ReconciliationReport,
        ReconciliationReportStateKind,
    };
    use crate::errors::IdentityDomainError;
    use crate::projection_state::ProjectionState;
    use crate::projection_state::ProjectionStateKind;
    use crate::reference_state::{ReferenceResolutionState, ReferenceResolutionStateKind};

    fn timestamp(value: i64) -> IdentityTimestamp {
        IdentityTimestamp::from_clock(value).expect("valid timestamp")
    }

    fn source(owner: IdentitySourceOwner, token: &str) -> IdentitySourceRef {
        IdentitySourceRef::new(
            owner,
            ExternalSourceRef::new(token.to_owned()).expect("valid external source ref"),
        )
        .expect("valid source ref")
    }

    fn member_ref() -> GlobalMemberRef {
        GlobalMemberRef::from_id(
            GlobalMemberId::new("member-1".to_owned()).expect("valid member id"),
        )
    }

    fn projection_ref() -> IdentityProjectionRef {
        IdentityProjectionRef::new("projection-1")
    }

    fn projection_state_ref() -> ProjectionStateRef {
        ProjectionStateRef::from_id(
            ProjectionStateId::new("projection-state-1".to_owned())
                .expect("valid projection state id"),
        )
    }

    fn projection_cursor_ref() -> IdentityProjectionCursorRef {
        IdentityProjectionCursorRef::new(source(IdentitySourceOwner::Identity, "cursor-1"))
    }

    fn maintenance_scope_ref() -> MaintenanceScopeRef {
        MaintenanceScopeRef::new(source(IdentitySourceOwner::Identity, "scope-1"))
    }

    fn maintenance_issue_ref() -> MaintenanceIssueRef {
        MaintenanceIssueRef::new("maintenance-issue-1")
    }

    fn reference_owner_ref() -> IdentityReferenceOwnerRef {
        IdentityReferenceOwnerRef::new(
            IdentityReferenceOwnerKind::Maintenance,
            source(IdentitySourceOwner::Identity, "owner-1"),
        )
    }

    fn external_reference_ref() -> ExternalReferenceRef {
        ExternalReferenceRef::new(
            ExternalReferenceKind::MethodSource,
            source(IdentitySourceOwner::MethodLibrary, "method-source-1"),
        )
    }

    fn external_source_version_ref() -> ExternalSourceVersionRef {
        ExternalSourceVersionRef::new(source(
            IdentitySourceOwner::MethodLibrary,
            "method-version-1",
        ))
    }

    fn external_safe_summary_ref() -> ExternalReferenceSafeSummaryRef {
        ExternalReferenceSafeSummaryRef::new(
            external_reference_ref(),
            source(IdentitySourceOwner::MethodLibrary, "safe-summary-1"),
        )
    }

    fn reference_state_ref() -> ReferenceResolutionStateRef {
        ReferenceResolutionStateRef::from_id(
            ReferenceResolutionStateId::new("reference-state-1".to_owned())
                .expect("valid reference state id"),
        )
    }

    fn report_ref() -> ReconciliationReportRef {
        ReconciliationReportRef::new("report-1")
    }

    fn actor() -> ActorRef {
        ActorRef::new("actor-1", ActorKind::Human)
    }

    #[test]
    fn query_path_cannot_request_projection_rebuild() {
        let mut state = ProjectionState::stale(
            projection_state_ref(),
            projection_ref(),
            Some(member_ref()),
            projection_cursor_ref(),
            maintenance_scope_ref(),
            timestamp(1),
        );

        let error = state
            .mark_rebuild_pending(
                maintenance_scope_ref(),
                timestamp(2),
                IdentityOperationChannel::Query,
            )
            .expect_err("query channel must not request rebuild");

        assert_eq!(
            error,
            IdentityDomainError::PolicyDenied {
                policy: "ProjectionState",
                message: "query channel cannot mutate identity truth",
            }
        );
        assert_eq!(state.state_kind, ProjectionStateKind::Stale);
    }

    #[test]
    fn projection_rebuild_flow_stays_in_support_state_family() {
        let mut state = ProjectionState::fresh(
            projection_state_ref(),
            projection_ref(),
            Some(member_ref()),
            projection_cursor_ref(),
            timestamp(1),
        );

        state
            .mark_stale(
                projection_cursor_ref(),
                maintenance_scope_ref(),
                timestamp(2),
            )
            .expect("fresh projection can become stale");
        state
            .mark_rebuild_pending(
                maintenance_scope_ref(),
                timestamp(3),
                IdentityOperationChannel::Job,
            )
            .expect("job channel can request rebuild");
        state
            .mark_rebuilt(projection_cursor_ref(), timestamp(4))
            .expect("pending rebuild can complete");

        assert_eq!(state.state_kind, ProjectionStateKind::Rebuilt);
        assert!(state.can_serve_read());
        assert!(!state.requires_rebuild());
    }

    #[test]
    fn reference_unavailable_does_not_stay_usable() {
        let mut state = ReferenceResolutionState::resolved(
            reference_state_ref(),
            external_reference_ref(),
            reference_owner_ref(),
            external_source_version_ref(),
            external_safe_summary_ref(),
            timestamp(1),
        );

        assert!(state.is_usable_for_truth_update());

        state
            .mark_unavailable(maintenance_issue_ref(), timestamp(2))
            .expect("resolved reference can become unavailable");

        assert_eq!(state.state_kind, ReferenceResolutionStateKind::Unavailable);
        assert!(!state.is_usable_for_truth_update());
        assert!(state.is_report_only());
    }

    #[test]
    fn reconciliation_policy_rejects_truth_repair_and_forbidden_material() {
        let policy = ReconciliationPolicy {
            maintenance_scope_ref: maintenance_scope_ref(),
            operation_channel: IdentityOperationChannel::Job,
            actor_ref: Some(actor()),
            target_ref: identity_contracts::refs::IdentityMaintenanceTargetRef::new("target-1"),
            maintenance_intent: IdentityMaintenanceIntent::RepairIdentityTruth,
            finding_intent_ref: None,
            finding_material: Some(ReconciliationFindingMaterial {
                material_kind: ReconciliationFindingMaterialKind::ForbiddenExternalBody,
            }),
        };

        let error = policy
            .assert_report_only()
            .expect_err("repair intent and forbidden material must be rejected");

        assert_eq!(
            error,
            IdentityDomainError::PolicyDenied {
                policy: "ReconciliationPolicy",
                message: "maintenance cannot repair identity truth",
            }
        );
    }

    #[test]
    fn reconciliation_report_stays_report_only() {
        let mut report = ReconciliationReport::generated(
            report_ref(),
            maintenance_scope_ref(),
            vec![identity_contracts::refs::IdentityMaintenanceTargetRef::new(
                "target-1",
            )],
            vec![ReconciliationFindingRef::new("finding-1")],
            vec![],
            Some(actor()),
            timestamp(1),
        )
        .expect("generated report should be valid");

        assert_eq!(
            report.report_state,
            ReconciliationReportStateKind::FindingDetected
        );
        assert!(report.has_findings());

        report
            .mark_partial(maintenance_issue_ref(), timestamp(2))
            .expect("finding report may become partial");
        assert_eq!(report.report_state, ReconciliationReportStateKind::Partial);

        report
            .mark_failed(maintenance_issue_ref(), timestamp(3))
            .expect("partial report may become failed");
        assert!(report.is_failed());
        report
            .assert_report_only()
            .expect("report remains report-only material");
    }
}
