//! Default canonical key mappers and dispatch target catalog shells.

use std::collections::{BTreeMap, BTreeSet};

use identity_contracts::protocol::IdentityJobName;
use identity_contracts::receipts::{MaintenanceIssueRef, TraceHandoffIntentRef};
use identity_contracts::refs::{
    ExternalReferenceRef, GlobalMemberRef, HandoffIssueRef, HandoffReceiptRef,
    IdentityAuditSubjectRef, IdentityConsumerBindingRef, IdentityJobRunRef,
    IdentityOutboxRecordRef, IdentityOutboxSubjectRef, IdentityProjectionRef, IdentitySourceRef,
    IdentityTraceSubjectRef, OutboxDeliveryIssueRef, RoleCapabilitySourceSnapshotRef,
    RoleCapabilitySummaryRef,
};

use crate::errors::ApplicationError;
use crate::ports::{
    IdentityDispatchTargetCatalogPort, IdentityMaintenanceIssueMapper, IdentityMarkerSubjectMapper,
    IdentityTruthChangeSubjectMapper,
};
use crate::support::{
    IdentityAcceptedSubjectRefs, IdentityApiRouteRef, IdentityDispatchTargetRef,
    IdentityEntrySurfaceKind,
};

/// Default mapper that derives accepted trace/audit/outbox subjects from typed truth refs.
#[derive(Clone, Debug, Default)]
pub struct DefaultIdentityTruthChangeSubjectMapper;

impl DefaultIdentityTruthChangeSubjectMapper {
    fn accepted_subjects(kind: &str, id: &str) -> IdentityAcceptedSubjectRefs {
        let key = format!("identity:{kind}:{id}");
        IdentityAcceptedSubjectRefs {
            trace_subject_ref: IdentityTraceSubjectRef::new(key.clone()),
            audit_subject_ref: IdentityAuditSubjectRef::new(key.clone()),
            outbox_subject_ref: IdentityOutboxSubjectRef::new(key),
        }
    }
}

impl IdentityTruthChangeSubjectMapper for DefaultIdentityTruthChangeSubjectMapper {
    fn member_subjects(&self, member_ref: GlobalMemberRef) -> IdentityAcceptedSubjectRefs {
        Self::accepted_subjects("member", member_ref.member_id.as_str())
    }

    fn role_capability_subjects(
        &self,
        summary_ref: RoleCapabilitySummaryRef,
    ) -> IdentityAcceptedSubjectRefs {
        Self::accepted_subjects("role-capability-summary", summary_ref.summary_id.as_str())
    }

    fn role_capability_source_snapshot_subjects(
        &self,
        snapshot_ref: RoleCapabilitySourceSnapshotRef,
    ) -> IdentityAcceptedSubjectRefs {
        Self::accepted_subjects(
            "role-capability-source-snapshot",
            snapshot_ref.snapshot_id.as_str(),
        )
    }

    fn career_record_subjects(
        &self,
        record_ref: identity_contracts::refs::CareerRecordRef,
    ) -> IdentityAcceptedSubjectRefs {
        Self::accepted_subjects("career-record", record_ref.record_id.as_str())
    }

    fn memory_reference_subjects(
        &self,
        reference_ref: identity_contracts::refs::MemoryReferenceRef,
    ) -> IdentityAcceptedSubjectRefs {
        Self::accepted_subjects("memory-reference", reference_ref.reference_id.as_str())
    }

    fn outbox_record_subjects(
        &self,
        outbox_ref: IdentityOutboxRecordRef,
    ) -> IdentityAcceptedSubjectRefs {
        Self::accepted_subjects("outbox-record", outbox_ref.as_str())
    }

    fn handoff_intent_subjects(
        &self,
        intent_ref: TraceHandoffIntentRef,
    ) -> IdentityAcceptedSubjectRefs {
        Self::accepted_subjects("trace-handoff-intent", intent_ref.as_str())
    }
}

/// Default mapper that derives canonical marker trace subjects.
#[derive(Clone, Debug, Default)]
pub struct DefaultIdentityMarkerSubjectMapper;

impl DefaultIdentityMarkerSubjectMapper {
    fn marker_subject(kind: &str, id: &str) -> IdentityTraceSubjectRef {
        IdentityTraceSubjectRef::new(format!("identity:marker:{kind}:{id}"))
    }
}

impl IdentityMarkerSubjectMapper for DefaultIdentityMarkerSubjectMapper {
    fn source_marker_subject(&self, source_ref: IdentitySourceRef) -> IdentityTraceSubjectRef {
        Self::marker_subject("source", source_ref.external_ref.as_str())
    }

    fn external_reference_marker_subject(
        &self,
        reference_ref: ExternalReferenceRef,
    ) -> IdentityTraceSubjectRef {
        Self::marker_subject(
            "external-reference",
            reference_ref.source_ref.external_ref.as_str(),
        )
    }

    fn projection_marker_subject(
        &self,
        projection_ref: IdentityProjectionRef,
    ) -> IdentityTraceSubjectRef {
        Self::marker_subject("projection", projection_ref.as_str())
    }

    fn job_marker_subject(&self, job_run_ref: IdentityJobRunRef) -> IdentityTraceSubjectRef {
        Self::marker_subject("job-run", job_run_ref.as_str())
    }

    fn handoff_receipt_marker_subject(
        &self,
        receipt_ref: HandoffReceiptRef,
    ) -> IdentityTraceSubjectRef {
        Self::marker_subject("handoff-receipt", receipt_ref.as_str())
    }
}

/// Default pure mapper for maintenance and propagation issue refs.
#[derive(Clone, Debug, Default)]
pub struct DefaultIdentityMaintenanceIssueMapper;

impl DefaultIdentityMaintenanceIssueMapper {
    fn issue_from_marker(marker: IdentitySourceRef, suffix: &str) -> MaintenanceIssueRef {
        MaintenanceIssueRef::new(format!("{suffix}:{}", marker.external_ref.as_str()))
    }
}

impl IdentityMaintenanceIssueMapper for DefaultIdentityMaintenanceIssueMapper {
    fn projection_missing_state_issue(
        &self,
        projection_ref: IdentityProjectionRef,
    ) -> MaintenanceIssueRef {
        MaintenanceIssueRef::new(format!(
            "projection-missing-state:{}",
            projection_ref.as_str()
        ))
    }

    fn projection_missing_cursor_issue(
        &self,
        projection_ref: IdentityProjectionRef,
    ) -> MaintenanceIssueRef {
        MaintenanceIssueRef::new(format!(
            "projection-missing-cursor:{}",
            projection_ref.as_str()
        ))
    }

    fn projection_unsupported_writer_issue(
        &self,
        projection_ref: IdentityProjectionRef,
    ) -> MaintenanceIssueRef {
        MaintenanceIssueRef::new(format!(
            "projection-unsupported-writer:{}",
            projection_ref.as_str()
        ))
    }

    fn reference_missing_state_issue(
        &self,
        reference_ref: ExternalReferenceRef,
    ) -> MaintenanceIssueRef {
        Self::issue_from_marker(reference_ref.source_ref, "reference-missing-state")
    }

    fn reference_refresh_failed_issue(
        &self,
        reference_ref: ExternalReferenceRef,
    ) -> MaintenanceIssueRef {
        Self::issue_from_marker(reference_ref.source_ref, "reference-refresh-failed")
    }

    fn outbox_retryable_issue(&self, issue_ref: OutboxDeliveryIssueRef) -> MaintenanceIssueRef {
        Self::issue_from_marker(issue_ref.issue_ref, "outbox-retryable")
    }

    fn outbox_permanent_issue(&self, issue_ref: OutboxDeliveryIssueRef) -> MaintenanceIssueRef {
        Self::issue_from_marker(issue_ref.issue_ref, "outbox-permanent")
    }

    fn outbox_skipped_issue(&self, issue_ref: OutboxDeliveryIssueRef) -> MaintenanceIssueRef {
        Self::issue_from_marker(issue_ref.issue_ref, "outbox-skipped")
    }

    fn outbox_unsupported_topic_issue(
        &self,
        issue_ref: OutboxDeliveryIssueRef,
    ) -> MaintenanceIssueRef {
        Self::issue_from_marker(issue_ref.issue_ref, "outbox-unsupported-topic")
    }

    fn handoff_retryable_issue(&self, issue_ref: HandoffIssueRef) -> MaintenanceIssueRef {
        Self::issue_from_marker(issue_ref.issue_ref, "handoff-retryable")
    }

    fn handoff_permanent_issue(&self, issue_ref: HandoffIssueRef) -> MaintenanceIssueRef {
        Self::issue_from_marker(issue_ref.issue_ref, "handoff-permanent")
    }

    fn handoff_cancelled_issue(&self, issue_ref: HandoffIssueRef) -> MaintenanceIssueRef {
        Self::issue_from_marker(issue_ref.issue_ref, "handoff-cancelled")
    }

    fn handoff_unsupported_target_issue(&self, issue_ref: HandoffIssueRef) -> MaintenanceIssueRef {
        Self::issue_from_marker(issue_ref.issue_ref, "handoff-unsupported-target")
    }
}

/// Default in-memory application dispatch target catalog.
#[derive(Clone, Debug, Default)]
pub struct DefaultIdentityDispatchTargetCatalog {
    api_command_routes: BTreeMap<String, String>,
    api_query_routes: BTreeMap<String, String>,
    worker_consumer_bindings: BTreeMap<String, String>,
    worker_callback_bindings: BTreeMap<String, String>,
    jobs: BTreeMap<String, String>,
    allowed_targets: BTreeSet<String>,
}

impl DefaultIdentityDispatchTargetCatalog {
    /// Creates an empty dispatch target catalog.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers an API command route target.
    pub fn with_api_command_target(
        mut self,
        route_ref: IdentityApiRouteRef,
        target_ref: IdentityDispatchTargetRef,
    ) -> Self {
        self.allowed_targets.insert(target_ref.as_str().to_owned());
        self.api_command_routes
            .insert(route_ref.into_inner(), target_ref.into_inner());
        self
    }

    /// Registers an API query route target.
    pub fn with_api_query_target(
        mut self,
        route_ref: IdentityApiRouteRef,
        target_ref: IdentityDispatchTargetRef,
    ) -> Self {
        self.allowed_targets.insert(target_ref.as_str().to_owned());
        self.api_query_routes
            .insert(route_ref.into_inner(), target_ref.into_inner());
        self
    }

    /// Registers a worker consumer target.
    pub fn with_worker_consumer_target(
        mut self,
        binding_ref: IdentityConsumerBindingRef,
        target_ref: IdentityDispatchTargetRef,
    ) -> Self {
        self.allowed_targets.insert(target_ref.as_str().to_owned());
        self.worker_consumer_bindings
            .insert(binding_ref.into_inner(), target_ref.into_inner());
        self
    }

    /// Registers a worker callback target.
    pub fn with_worker_callback_target(
        mut self,
        binding_ref: IdentityConsumerBindingRef,
        target_ref: IdentityDispatchTargetRef,
    ) -> Self {
        self.allowed_targets.insert(target_ref.as_str().to_owned());
        self.worker_callback_bindings
            .insert(binding_ref.into_inner(), target_ref.into_inner());
        self
    }

    /// Registers an operations job target.
    pub fn with_job_target(
        mut self,
        job_name: IdentityJobName,
        target_ref: IdentityDispatchTargetRef,
    ) -> Self {
        self.allowed_targets.insert(target_ref.as_str().to_owned());
        self.jobs
            .insert(job_name.as_str().to_owned(), target_ref.into_inner());
        self
    }

    fn lookup(
        table: &BTreeMap<String, String>,
        key: &str,
        surface: &str,
    ) -> Result<IdentityDispatchTargetRef, ApplicationError> {
        table
            .get(key)
            .cloned()
            .map(IdentityDispatchTargetRef::new)
            .ok_or_else(|| {
                ApplicationError::not_found(format!(
                    "dispatch target not found for {surface}: {key}"
                ))
            })
    }
}

impl IdentityDispatchTargetCatalogPort for DefaultIdentityDispatchTargetCatalog {
    fn api_command_target(
        &self,
        route_ref: IdentityApiRouteRef,
    ) -> Result<IdentityDispatchTargetRef, ApplicationError> {
        Self::lookup(&self.api_command_routes, route_ref.as_str(), "api_command")
    }

    fn api_query_target(
        &self,
        route_ref: IdentityApiRouteRef,
    ) -> Result<IdentityDispatchTargetRef, ApplicationError> {
        Self::lookup(&self.api_query_routes, route_ref.as_str(), "api_query")
    }

    fn worker_consumer_target(
        &self,
        binding_ref: IdentityConsumerBindingRef,
    ) -> Result<IdentityDispatchTargetRef, ApplicationError> {
        Self::lookup(
            &self.worker_consumer_bindings,
            binding_ref.as_str(),
            "worker_consumer",
        )
    }

    fn worker_callback_target(
        &self,
        binding_ref: IdentityConsumerBindingRef,
    ) -> Result<IdentityDispatchTargetRef, ApplicationError> {
        Self::lookup(
            &self.worker_callback_bindings,
            binding_ref.as_str(),
            "worker_callback",
        )
    }

    fn job_target(
        &self,
        job_name: IdentityJobName,
    ) -> Result<IdentityDispatchTargetRef, ApplicationError> {
        Self::lookup(&self.jobs, job_name.as_str(), "job")
    }

    fn assert_application_target(
        &self,
        _surface_kind: IdentityEntrySurfaceKind,
        target_ref: IdentityDispatchTargetRef,
    ) -> Result<(), ApplicationError> {
        if self.allowed_targets.contains(target_ref.as_str()) {
            Ok(())
        } else {
            Err(ApplicationError::invalid_request(format!(
                "unknown application target: {}",
                target_ref.as_str()
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use identity_contracts::protocol::IdentityJobName;
    use identity_contracts::refs::{
        ExternalReferenceKind, ExternalReferenceRef, ExternalSourceRef, GlobalMemberId,
        GlobalMemberRef, HandoffReceiptRef, IdentityConsumerBindingRef, IdentityJobRunRef,
        IdentityProjectionRef, IdentitySourceOwner, IdentitySourceRef, IdentityTraceSubjectRef,
        RoleCapabilitySourceSnapshotId, RoleCapabilitySummaryId,
    };

    use super::{
        DefaultIdentityDispatchTargetCatalog, DefaultIdentityMarkerSubjectMapper,
        DefaultIdentityTruthChangeSubjectMapper,
    };
    use crate::ports::{
        IdentityDispatchTargetCatalogPort, IdentityMarkerSubjectMapper,
        IdentityTruthChangeSubjectMapper,
    };
    use crate::support::{IdentityApiRouteRef, IdentityDispatchTargetRef};

    #[test]
    fn accepted_subject_mapper_uses_shared_canonical_key() {
        let mapper = DefaultIdentityTruthChangeSubjectMapper;
        let subjects = mapper.member_subjects(GlobalMemberRef::from_id(
            GlobalMemberId::new("member-1".to_owned()).expect("valid member id"),
        ));

        assert_eq!(
            subjects.trace_subject_ref,
            IdentityTraceSubjectRef::new("identity:member:member-1")
        );
        assert_eq!(
            subjects.audit_subject_ref.as_str(),
            "identity:member:member-1"
        );
        assert_eq!(
            subjects.outbox_subject_ref.as_str(),
            "identity:member:member-1"
        );
    }

    #[test]
    fn accepted_subject_mapper_covers_all_truth_families() {
        let mapper = DefaultIdentityTruthChangeSubjectMapper;

        assert_eq!(
            mapper
                .role_capability_subjects(
                    identity_contracts::refs::RoleCapabilitySummaryRef::from_id(
                        RoleCapabilitySummaryId::new("summary-1".to_owned())
                            .expect("valid summary id"),
                    )
                )
                .trace_subject_ref
                .as_str(),
            "identity:role-capability-summary:summary-1"
        );
        assert_eq!(
            mapper
                .role_capability_source_snapshot_subjects(
                    identity_contracts::refs::RoleCapabilitySourceSnapshotRef::from_id(
                        RoleCapabilitySourceSnapshotId::new("snapshot-1".to_owned())
                            .expect("valid snapshot id"),
                    ),
                )
                .trace_subject_ref
                .as_str(),
            "identity:role-capability-source-snapshot:snapshot-1"
        );
    }

    #[test]
    fn marker_subject_mapper_uses_formal_marker_keys() {
        let mapper = DefaultIdentityMarkerSubjectMapper;
        let source_ref = IdentitySourceRef::new(
            IdentitySourceOwner::Work,
            ExternalSourceRef::new("source-1".to_owned()).expect("valid external source"),
        )
        .expect("valid identity source");
        let external_reference_ref =
            ExternalReferenceRef::new(ExternalReferenceKind::WorkParticipation, source_ref.clone());

        assert_eq!(
            mapper.source_marker_subject(source_ref.clone()).as_str(),
            "identity:marker:source:source-1"
        );
        assert_eq!(
            mapper
                .external_reference_marker_subject(external_reference_ref)
                .as_str(),
            "identity:marker:external-reference:source-1"
        );
        assert_eq!(
            mapper
                .projection_marker_subject(IdentityProjectionRef::new("projection-1"))
                .as_str(),
            "identity:marker:projection:projection-1"
        );
        assert_eq!(
            mapper
                .job_marker_subject(IdentityJobRunRef::new("job-run-1"))
                .as_str(),
            "identity:marker:job-run:job-run-1"
        );
        assert_eq!(
            mapper
                .handoff_receipt_marker_subject(HandoffReceiptRef::new("receipt-1"))
                .as_str(),
            "identity:marker:handoff-receipt:receipt-1"
        );
    }

    #[test]
    fn dispatch_target_catalog_returns_registered_targets() {
        let catalog = DefaultIdentityDispatchTargetCatalog::new()
            .with_api_command_target(
                IdentityApiRouteRef::new("api.command.member.establish"),
                IdentityDispatchTargetRef::new("application.command.establish_global_member"),
            )
            .with_api_query_target(
                IdentityApiRouteRef::new("api.query.member.summary"),
                IdentityDispatchTargetRef::new("application.query.read_member_summary"),
            )
            .with_worker_consumer_target(
                IdentityConsumerBindingRef::new("binding.consumer.role-capability.changed"),
                IdentityDispatchTargetRef::new(
                    "application.consumer.role_capability_source_changed",
                ),
            )
            .with_worker_callback_target(
                IdentityConsumerBindingRef::new("binding.callback.trace-handoff"),
                IdentityDispatchTargetRef::new("application.callback.trace_handoff"),
            )
            .with_job_target(
                IdentityJobName::new("RunIdentityReconciliation"),
                IdentityDispatchTargetRef::new("application.job.run_identity_reconciliation"),
            );

        assert_eq!(
            catalog
                .api_command_target(IdentityApiRouteRef::new("api.command.member.establish"))
                .expect("api command target")
                .as_str(),
            "application.command.establish_global_member"
        );
        assert_eq!(
            catalog
                .job_target(IdentityJobName::new("RunIdentityReconciliation"))
                .expect("job target")
                .as_str(),
            "application.job.run_identity_reconciliation"
        );
        assert!(
            catalog
                .assert_application_target(
                    crate::support::IdentityEntrySurfaceKind::ApiCommand,
                    IdentityDispatchTargetRef::new("application.command.establish_global_member"),
                )
                .is_ok()
        );
    }
}
