//! Default canonical key mappers and dispatch target catalog shells.

use std::collections::{BTreeMap, BTreeSet};

use identity_contracts::metadata::IdentityDegradedKind;
use identity_contracts::protocol::IdentityJobName;
use identity_contracts::receipts::{
    MaintenanceIssueKind, MaintenanceIssueRef, TraceHandoffIntentRef,
};
use identity_contracts::refs::{
    AuditScopeRef, AuditTrailRef, CareerRecordRef, ExternalReferenceRef, GlobalMemberRef,
    HandoffIssueRef, HandoffReceiptRef, IdentityAuditSubjectRef, IdentityConsumerBindingRef,
    IdentityDegradedMarkerRef, IdentityJobRunRef, IdentityMaintenanceTargetRef,
    IdentityOutboxRecordRef, IdentityOutboxSubjectRef, IdentityProjectionRef,
    IdentityReferenceOwnerRef, IdentitySourceRef, IdentityTraceRecordRef, IdentityTraceSubjectRef,
    MaintenanceScopeRef, MemoryReferenceRef, OutboxDeliveryIssueRef, ProjectionStateRef,
    ReconciliationReportRef, ReferenceResolutionStateRef, RoleCapabilitySourceSnapshotRef,
    RoleCapabilitySummaryRef, VisibilityResultRef, VisibilityScopeRef,
};
use identity_contracts::refs::{
    IdentityChangeKindRef, IdentityReadSurfaceKind, IdentityTruthCursor,
};
use identity_contracts::views::{IdentityReadMaterialMarker, IdentityVisibilityAccessSummary};

use crate::errors::ApplicationError;
use crate::ports::{
    IdentityAcceptedAuditTrailMarkerMapper, IdentityDispatchTargetCatalogPort,
    IdentityMaintenanceIssueMapper, IdentityMarkerSubjectMapper,
    IdentityQueryMaterialDegradationMapper, IdentityTruthChangeSubjectMapper,
};
use crate::support::{
    IdentityAcceptedAuditTrailMarkers, IdentityAcceptedSubjectRefs, IdentityApiRouteRef,
    IdentityDispatchTargetRef, IdentityEntrySurfaceKind, IdentityOperationContext,
    IdentityQueryMaterialDegradationSummary,
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

/// Default mapper that derives accepted-write audit material markers.
#[derive(Clone, Debug, Default)]
pub struct DefaultIdentityAcceptedAuditTrailMarkerMapper;

impl DefaultIdentityAcceptedAuditTrailMarkerMapper {
    fn marker_key(
        context: &IdentityOperationContext,
        subjects: &IdentityAcceptedSubjectRefs,
        change_kind_ref: &IdentityChangeKindRef,
        source_cursor_ref: &IdentityTruthCursor,
        family: &str,
    ) -> String {
        format!(
            "accepted-audit:{family}:{}:{}:{:?}:{}",
            context.operation_name.as_str(),
            subjects.audit_subject_ref.as_str(),
            change_kind_ref.change_kind,
            source_cursor_ref.as_str(),
        )
    }
}

impl IdentityAcceptedAuditTrailMarkerMapper for DefaultIdentityAcceptedAuditTrailMarkerMapper {
    fn accepted_command_audit_markers(
        &self,
        context: &IdentityOperationContext,
        subjects: &IdentityAcceptedSubjectRefs,
        change_kind_ref: &IdentityChangeKindRef,
        source_cursor_ref: &IdentityTruthCursor,
    ) -> IdentityAcceptedAuditTrailMarkers {
        IdentityAcceptedAuditTrailMarkers {
            audit_scope_ref: AuditScopeRef::new(Self::marker_key(
                context,
                subjects,
                change_kind_ref,
                source_cursor_ref,
                "scope",
            )),
            trail_visibility_result_ref: VisibilityResultRef::new(Self::marker_key(
                context,
                subjects,
                change_kind_ref,
                source_cursor_ref,
                "trail",
            )),
            entry_visibility_result_ref: VisibilityResultRef::new(Self::marker_key(
                context,
                subjects,
                change_kind_ref,
                source_cursor_ref,
                "entry",
            )),
            read_surface_kind: IdentityReadSurfaceKind::Found,
        }
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

/// Default mapper that derives deterministic degraded summaries from typed query material context.
#[derive(Clone, Debug, Default)]
pub struct DefaultIdentityQueryMaterialDegradationMapper;

impl DefaultIdentityQueryMaterialDegradationMapper {
    fn summary(
        access: IdentityVisibilityAccessSummary,
        marker_token: impl Into<String>,
        degraded_kind: IdentityDegradedKind,
    ) -> IdentityQueryMaterialDegradationSummary {
        IdentityQueryMaterialDegradationSummary {
            read_subject_ref: access.read_subject_ref,
            visibility_context_ref: access.visibility_context_ref,
            visibility_scope_ref: access.scope_ref,
            visibility_result_ref: access.visibility_result_ref,
            degraded_marker_ref: IdentityDegradedMarkerRef::new(marker_token),
            degraded_kind,
        }
    }
}

impl IdentityQueryMaterialDegradationMapper for DefaultIdentityQueryMaterialDegradationMapper {
    fn member_summary_view_missing(
        &self,
        access: IdentityVisibilityAccessSummary,
        expected_member_ref: GlobalMemberRef,
        expected_scope_ref: VisibilityScopeRef,
    ) -> IdentityQueryMaterialDegradationSummary {
        Self::summary(
            access,
            format!(
                "query-material:member-summary-view-missing:{}:{}",
                expected_member_ref.member_id.as_str(),
                expected_scope_ref.as_str()
            ),
            IdentityDegradedKind::PartialResult,
        )
    }

    fn member_summary_view_invalid_owner(
        &self,
        access: IdentityVisibilityAccessSummary,
        view_ref: identity_contracts::refs::MemberSummaryViewRef,
        expected_member_ref: GlobalMemberRef,
    ) -> IdentityQueryMaterialDegradationSummary {
        Self::summary(
            access,
            format!(
                "query-material:member-summary-view-invalid-owner:{}:{}",
                view_ref.as_str(),
                expected_member_ref.member_id.as_str()
            ),
            IdentityDegradedKind::MaterialUnsafe,
        )
    }

    fn member_summary_view_scope_mismatch(
        &self,
        access: IdentityVisibilityAccessSummary,
        view_ref: identity_contracts::refs::MemberSummaryViewRef,
        expected_scope_ref: VisibilityScopeRef,
    ) -> IdentityQueryMaterialDegradationSummary {
        Self::summary(
            access,
            format!(
                "query-material:member-summary-view-scope-mismatch:{}:{}",
                view_ref.as_str(),
                expected_scope_ref.as_str()
            ),
            IdentityDegradedKind::MaterialUnsafe,
        )
    }

    fn member_summary_view_missing_freshness(
        &self,
        access: IdentityVisibilityAccessSummary,
        view_ref: identity_contracts::refs::MemberSummaryViewRef,
        expected_member_ref: GlobalMemberRef,
        expected_scope_ref: VisibilityScopeRef,
    ) -> IdentityQueryMaterialDegradationSummary {
        Self::summary(
            access,
            format!(
                "query-material:member-summary-view-missing-freshness:{}:{}:{}",
                view_ref.as_str(),
                expected_member_ref.member_id.as_str(),
                expected_scope_ref.as_str()
            ),
            IdentityDegradedKind::MaterialUnsafe,
        )
    }

    fn forbidden_read_material(
        &self,
        access: IdentityVisibilityAccessSummary,
        read_material_marker: IdentityReadMaterialMarker,
    ) -> IdentityQueryMaterialDegradationSummary {
        Self::summary(
            access,
            format!(
                "query-material:forbidden:{}",
                match read_material_marker.material_kind {
                    identity_contracts::views::IdentityReadMaterialKind::SafeSummaryRefs => {
                        "safe-summary-refs"
                    }
                    identity_contracts::views::IdentityReadMaterialKind::TraceRefsOnly => {
                        "trace-refs-only"
                    }
                    identity_contracts::views::IdentityReadMaterialKind::AuditRefsOnly => {
                        "audit-refs-only"
                    }
                    identity_contracts::views::IdentityReadMaterialKind::RedactedSafeMaterial => {
                        "redacted-safe-material"
                    }
                    identity_contracts::views::IdentityReadMaterialKind::ForbiddenExternalBody => {
                        "forbidden-external-body"
                    }
                    identity_contracts::views::IdentityReadMaterialKind::ForbiddenRawDiagnostic => {
                        "forbidden-raw-diagnostic"
                    }
                    identity_contracts::views::IdentityReadMaterialKind::ForbiddenSecret => {
                        "forbidden-secret"
                    }
                }
            ),
            IdentityDegradedKind::MaterialUnsafe,
        )
    }

    fn career_record_item_missing_after_list(
        &self,
        access: IdentityVisibilityAccessSummary,
        record_ref: CareerRecordRef,
        expected_member_ref: GlobalMemberRef,
    ) -> IdentityQueryMaterialDegradationSummary {
        Self::summary(
            access,
            format!(
                "query-material:career-record-missing:{}:{}",
                record_ref.record_id.as_str(),
                expected_member_ref.member_id.as_str()
            ),
            IdentityDegradedKind::PartialResult,
        )
    }

    fn career_record_item_invalid_member(
        &self,
        access: IdentityVisibilityAccessSummary,
        record_ref: CareerRecordRef,
        expected_member_ref: GlobalMemberRef,
    ) -> IdentityQueryMaterialDegradationSummary {
        Self::summary(
            access,
            format!(
                "query-material:career-record-invalid-member:{}:{}",
                record_ref.record_id.as_str(),
                expected_member_ref.member_id.as_str()
            ),
            IdentityDegradedKind::MaterialUnsafe,
        )
    }

    fn memory_reference_item_missing_after_list(
        &self,
        access: IdentityVisibilityAccessSummary,
        reference_ref: MemoryReferenceRef,
        expected_member_ref: GlobalMemberRef,
    ) -> IdentityQueryMaterialDegradationSummary {
        Self::summary(
            access,
            format!(
                "query-material:memory-reference-missing:{}:{}",
                reference_ref.reference_id.as_str(),
                expected_member_ref.member_id.as_str()
            ),
            IdentityDegradedKind::PartialResult,
        )
    }

    fn memory_reference_item_invalid_member(
        &self,
        access: IdentityVisibilityAccessSummary,
        reference_ref: MemoryReferenceRef,
        expected_member_ref: GlobalMemberRef,
    ) -> IdentityQueryMaterialDegradationSummary {
        Self::summary(
            access,
            format!(
                "query-material:memory-reference-invalid-member:{}:{}",
                reference_ref.reference_id.as_str(),
                expected_member_ref.member_id.as_str()
            ),
            IdentityDegradedKind::MaterialUnsafe,
        )
    }

    fn trace_item_missing_after_list(
        &self,
        access: IdentityVisibilityAccessSummary,
        trace_ref: IdentityTraceRecordRef,
    ) -> IdentityQueryMaterialDegradationSummary {
        Self::summary(
            access,
            format!("query-material:trace-missing:{}", trace_ref.as_str()),
            IdentityDegradedKind::PartialResult,
        )
    }

    fn trace_item_invalid_member(
        &self,
        access: IdentityVisibilityAccessSummary,
        trace_ref: IdentityTraceRecordRef,
        expected_member_ref: GlobalMemberRef,
    ) -> IdentityQueryMaterialDegradationSummary {
        Self::summary(
            access,
            format!(
                "query-material:trace-invalid-member:{}:{}",
                trace_ref.as_str(),
                expected_member_ref.member_id.as_str()
            ),
            IdentityDegradedKind::MaterialUnsafe,
        )
    }

    fn trace_item_subject_mismatch(
        &self,
        access: IdentityVisibilityAccessSummary,
        trace_ref: IdentityTraceRecordRef,
        expected_subject_ref: IdentityTraceSubjectRef,
    ) -> IdentityQueryMaterialDegradationSummary {
        Self::summary(
            access,
            format!(
                "query-material:trace-subject-mismatch:{}:{}",
                trace_ref.as_str(),
                expected_subject_ref.as_str()
            ),
            IdentityDegradedKind::MaterialUnsafe,
        )
    }

    fn audit_item_missing_or_invalid(
        &self,
        access: IdentityVisibilityAccessSummary,
        audit_trail_ref: AuditTrailRef,
        audit_scope_ref: AuditScopeRef,
    ) -> IdentityQueryMaterialDegradationSummary {
        Self::summary(
            access,
            format!(
                "query-material:audit-invalid:{}:{}",
                audit_trail_ref.as_str(),
                audit_scope_ref.as_str()
            ),
            IdentityDegradedKind::PartialResult,
        )
    }

    fn projection_state_ref_mismatch(
        &self,
        access: IdentityVisibilityAccessSummary,
        projection_ref: IdentityProjectionRef,
        requested_state_ref: ProjectionStateRef,
        loaded_state_ref: ProjectionStateRef,
    ) -> IdentityQueryMaterialDegradationSummary {
        Self::summary(
            access,
            format!(
                "query-material:projection-state-ref-mismatch:{}:{}:{}",
                projection_ref.as_str(),
                requested_state_ref.projection_state_id.as_str(),
                loaded_state_ref.projection_state_id.as_str()
            ),
            IdentityDegradedKind::MaterialUnsafe,
        )
    }

    fn reference_state_owner_mismatch(
        &self,
        access: IdentityVisibilityAccessSummary,
        reference_ref: ExternalReferenceRef,
        expected_owner_ref: IdentityReferenceOwnerRef,
        loaded_owner_ref: IdentityReferenceOwnerRef,
    ) -> IdentityQueryMaterialDegradationSummary {
        Self::summary(
            access,
            format!(
                "query-material:reference-owner-mismatch:{:?}:{}:{:?}:{}:{:?}:{}",
                reference_ref.reference_kind,
                reference_ref.source_ref.external_ref.as_str(),
                expected_owner_ref.owner_kind,
                expected_owner_ref.owner_ref.external_ref.as_str(),
                loaded_owner_ref.owner_kind,
                loaded_owner_ref.owner_ref.external_ref.as_str()
            ),
            IdentityDegradedKind::MaterialUnsafe,
        )
    }

    fn reference_sidecar_degraded(
        &self,
        access: IdentityVisibilityAccessSummary,
        reference_ref: ExternalReferenceRef,
        resolution_state_ref: Option<ReferenceResolutionStateRef>,
    ) -> IdentityQueryMaterialDegradationSummary {
        Self::summary(
            access,
            format!(
                "query-material:reference-sidecar-degraded:{:?}:{}:{}",
                reference_ref.reference_kind,
                reference_ref.source_ref.external_ref.as_str(),
                resolution_state_ref
                    .as_ref()
                    .map(|state_ref| state_ref.resolution_state_id.as_str())
                    .unwrap_or("none")
            ),
            IdentityDegradedKind::PartialResult,
        )
    }

    fn reconciliation_report_scope_mismatch(
        &self,
        access: IdentityVisibilityAccessSummary,
        report_ref: ReconciliationReportRef,
        expected_scope_ref: MaintenanceScopeRef,
        loaded_scope_ref: MaintenanceScopeRef,
    ) -> IdentityQueryMaterialDegradationSummary {
        Self::summary(
            access,
            format!(
                "query-material:report-scope-mismatch:{}:{}:{}",
                report_ref.as_str(),
                expected_scope_ref.scope_ref.external_ref.as_str(),
                loaded_scope_ref.scope_ref.external_ref.as_str()
            ),
            IdentityDegradedKind::MaterialUnsafe,
        )
    }

    fn reconciliation_report_item_missing_after_list(
        &self,
        access: IdentityVisibilityAccessSummary,
        report_ref: ReconciliationReportRef,
        expected_scope_ref: MaintenanceScopeRef,
    ) -> IdentityQueryMaterialDegradationSummary {
        Self::summary(
            access,
            format!(
                "query-material:report-missing:{}:{}",
                report_ref.as_str(),
                expected_scope_ref.scope_ref.external_ref.as_str()
            ),
            IdentityDegradedKind::PartialResult,
        )
    }

    fn outbox_record_item_missing_after_list(
        &self,
        access: IdentityVisibilityAccessSummary,
        outbox_ref: IdentityOutboxRecordRef,
    ) -> IdentityQueryMaterialDegradationSummary {
        Self::summary(
            access,
            format!("query-material:outbox-missing:{}", outbox_ref.as_str()),
            IdentityDegradedKind::PartialResult,
        )
    }

    fn outbox_record_selector_mismatch(
        &self,
        access: IdentityVisibilityAccessSummary,
        outbox_ref: IdentityOutboxRecordRef,
    ) -> IdentityQueryMaterialDegradationSummary {
        Self::summary(
            access,
            format!(
                "query-material:outbox-selector-mismatch:{}",
                outbox_ref.as_str()
            ),
            IdentityDegradedKind::MaterialUnsafe,
        )
    }

    fn handoff_intent_empty_trace_refs(
        &self,
        access: IdentityVisibilityAccessSummary,
        intent_ref: TraceHandoffIntentRef,
    ) -> IdentityQueryMaterialDegradationSummary {
        Self::summary(
            access,
            format!(
                "query-material:handoff-empty-trace-refs:{}",
                intent_ref.as_str()
            ),
            IdentityDegradedKind::MaterialUnsafe,
        )
    }

    fn handoff_intent_delivered_without_receipt(
        &self,
        access: IdentityVisibilityAccessSummary,
        intent_ref: TraceHandoffIntentRef,
    ) -> IdentityQueryMaterialDegradationSummary {
        Self::summary(
            access,
            format!(
                "query-material:handoff-delivered-without-receipt:{}",
                intent_ref.as_str()
            ),
            IdentityDegradedKind::MaterialUnsafe,
        )
    }
}

/// Default pure mapper for maintenance and propagation issue refs.
#[derive(Clone, Debug, Default)]
pub struct DefaultIdentityMaintenanceIssueMapper;

impl DefaultIdentityMaintenanceIssueMapper {
    fn issue_from_marker(
        marker: IdentitySourceRef,
        issue_kind: MaintenanceIssueKind,
    ) -> MaintenanceIssueRef {
        MaintenanceIssueRef::new(issue_kind, marker)
    }
}

impl IdentityMaintenanceIssueMapper for DefaultIdentityMaintenanceIssueMapper {
    fn projection_missing_state_issue(
        &self,
        projection_ref: IdentityProjectionRef,
    ) -> MaintenanceIssueRef {
        MaintenanceIssueRef::new(
            MaintenanceIssueKind::Unrecognized,
            projection_ref.projection_ref,
        )
    }

    fn projection_missing_cursor_issue(
        &self,
        projection_ref: IdentityProjectionRef,
    ) -> MaintenanceIssueRef {
        MaintenanceIssueRef::new(MaintenanceIssueKind::Stale, projection_ref.projection_ref)
    }

    fn projection_unsupported_writer_issue(
        &self,
        projection_ref: IdentityProjectionRef,
    ) -> MaintenanceIssueRef {
        MaintenanceIssueRef::new(MaintenanceIssueKind::Failed, projection_ref.projection_ref)
    }

    fn projection_missing_rebuild_input_issue(
        &self,
        projection_ref: IdentityProjectionRef,
    ) -> MaintenanceIssueRef {
        MaintenanceIssueRef::new(MaintenanceIssueKind::Partial, projection_ref.projection_ref)
    }

    fn reference_missing_state_issue(
        &self,
        reference_ref: ExternalReferenceRef,
    ) -> MaintenanceIssueRef {
        Self::issue_from_marker(reference_ref.source_ref, MaintenanceIssueKind::Unrecognized)
    }

    fn reference_refresh_failed_issue(
        &self,
        reference_ref: ExternalReferenceRef,
    ) -> MaintenanceIssueRef {
        Self::issue_from_marker(reference_ref.source_ref, MaintenanceIssueKind::Failed)
    }

    fn maintenance_target_missing_issue(
        &self,
        target_ref: IdentityMaintenanceTargetRef,
    ) -> MaintenanceIssueRef {
        Self::issue_from_marker(target_ref.target_ref, MaintenanceIssueKind::Unrecognized)
    }

    fn outbox_retryable_issue(&self, issue_ref: OutboxDeliveryIssueRef) -> MaintenanceIssueRef {
        Self::issue_from_marker(issue_ref.issue_ref, MaintenanceIssueKind::Unavailable)
    }

    fn outbox_permanent_issue(&self, issue_ref: OutboxDeliveryIssueRef) -> MaintenanceIssueRef {
        Self::issue_from_marker(issue_ref.issue_ref, MaintenanceIssueKind::Failed)
    }

    fn outbox_skipped_issue(&self, issue_ref: OutboxDeliveryIssueRef) -> MaintenanceIssueRef {
        Self::issue_from_marker(issue_ref.issue_ref, MaintenanceIssueKind::Failed)
    }

    fn outbox_unsupported_topic_issue(
        &self,
        issue_ref: OutboxDeliveryIssueRef,
    ) -> MaintenanceIssueRef {
        Self::issue_from_marker(issue_ref.issue_ref, MaintenanceIssueKind::Unrecognized)
    }

    fn handoff_retryable_issue(&self, issue_ref: HandoffIssueRef) -> MaintenanceIssueRef {
        Self::issue_from_marker(issue_ref.issue_ref, MaintenanceIssueKind::Unavailable)
    }

    fn handoff_permanent_issue(&self, issue_ref: HandoffIssueRef) -> MaintenanceIssueRef {
        Self::issue_from_marker(issue_ref.issue_ref, MaintenanceIssueKind::Failed)
    }

    fn handoff_cancelled_issue(&self, issue_ref: HandoffIssueRef) -> MaintenanceIssueRef {
        Self::issue_from_marker(issue_ref.issue_ref, MaintenanceIssueKind::Failed)
    }

    fn handoff_unsupported_target_issue(&self, issue_ref: HandoffIssueRef) -> MaintenanceIssueRef {
        Self::issue_from_marker(issue_ref.issue_ref, MaintenanceIssueKind::Unrecognized)
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
        surface_kind: IdentityEntrySurfaceKind,
        target_ref: IdentityDispatchTargetRef,
    ) -> Result<(), ApplicationError> {
        if !self.allowed_targets.contains(target_ref.as_str()) {
            Err(ApplicationError::invalid_request(format!(
                "unknown application target: {}",
                target_ref.as_str()
            )))
        } else if !IdentityEntrySurfaceKind::is_application_target(&target_ref) {
            Err(ApplicationError::invalid_request(format!(
                "target is not an application service target: {}",
                target_ref.as_str()
            )))
        } else if !surface_kind.matches_application_target(&target_ref) {
            Err(ApplicationError::invalid_request(format!(
                "target {} does not match entry surface {:?}",
                target_ref.as_str(),
                surface_kind
            )))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use identity_contracts::protocol::IdentityJobName;
    use identity_contracts::refs::{
        ExternalReferenceKind, ExternalReferenceRef, ExternalSourceRef, GlobalMemberId,
        GlobalMemberRef, HandoffReceiptRef, IdentityConsumerBindingRef, IdentityJobRunRef,
        IdentityProjectionKind, IdentityProjectionRef, IdentitySourceOwner, IdentitySourceRef,
        IdentityTraceSubjectRef, RoleCapabilitySourceSnapshotId, RoleCapabilitySummaryId,
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
        let work_source_ref = IdentitySourceRef::new(
            IdentitySourceOwner::Work,
            ExternalSourceRef::new("source-1".to_owned()).expect("valid external source"),
        )
        .expect("valid identity source");
        let projection_source_ref = IdentitySourceRef::new(
            IdentitySourceOwner::Identity,
            ExternalSourceRef::new("projection-1".to_owned()).expect("valid external source"),
        )
        .expect("valid projection source");
        let external_reference_ref = ExternalReferenceRef::new(
            ExternalReferenceKind::WorkParticipation,
            work_source_ref.clone(),
        );

        assert_eq!(
            mapper
                .source_marker_subject(work_source_ref.clone())
                .as_str(),
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
                .projection_marker_subject(
                    IdentityProjectionRef::new(
                        IdentityProjectionKind::MemberSummary,
                        projection_source_ref,
                    )
                    .expect("valid projection ref"),
                )
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
            .with_api_command_target(
                IdentityApiRouteRef::new("api.command.lifecycle.update"),
                IdentityDispatchTargetRef::new("application.command.update_global_lifecycle_state"),
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
                .api_command_target(IdentityApiRouteRef::new("api.command.lifecycle.update"))
                .expect("lifecycle command target")
                .as_str(),
            "application.command.update_global_lifecycle_state"
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
        assert!(
            catalog
                .assert_application_target(
                    crate::support::IdentityEntrySurfaceKind::ApiCommand,
                    IdentityDispatchTargetRef::new(
                        "application.command.update_global_lifecycle_state",
                    ),
                )
                .is_ok()
        );
    }

    #[test]
    fn dispatch_target_catalog_rejects_cross_surface_target() {
        let catalog = DefaultIdentityDispatchTargetCatalog::new().with_api_command_target(
            IdentityApiRouteRef::new("api.command.member.establish"),
            IdentityDispatchTargetRef::new("application.command.establish_global_member"),
        );

        let error = catalog
            .assert_application_target(
                crate::support::IdentityEntrySurfaceKind::ApiQuery,
                IdentityDispatchTargetRef::new("application.command.establish_global_member"),
            )
            .expect_err("surface mismatch must fail");

        assert!(error.to_string().contains("does not match entry surface"));
    }
}
