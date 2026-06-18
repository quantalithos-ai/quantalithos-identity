use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use core_contracts::actor::ActorRef;
use identity_application::errors::{ApplicationError, ApplicationErrorKind};
use identity_application::mapper::{
    DefaultIdentityAcceptedAuditTrailMarkerMapper, DefaultIdentityMarkerSubjectMapper,
    DefaultIdentityTruthChangeSubjectMapper,
};
use identity_application::outbound_material::AcceptedOutboundMaterialKind;
use identity_application::ports::{
    CareerRecordRepository, ExternalReferenceResolutionOutcome, ExternalReferenceTypedSidecarRefs,
    GlobalLifecycleRepository, GlobalMemberRepository, HandoffDeliveryOutcome,
    HandoffReceiptResolution, HandoffTargetResolution, IdentityAcceptedAuditTrailMarkerMapper,
    IdentityAdapterAvailabilityPort, IdentityAuditTrailRepository, IdentityClockPort,
    IdentityCommandEffectSummaryRepository, IdentityCursorAssignerPort,
    IdentityExternalReferenceResolverPort, IdentityExternalSourceResolverPort,
    IdentityHandoffDeliveryPort, IdentityHandoffTargetPort, IdentityIdGeneratorPort,
    IdentityIdempotencyRepository, IdentityJobReportRepository,
    IdentityMaintenanceInspectionContext, IdentityMaintenanceRepository,
    IdentityMarkerSubjectMapper, IdentityOperationContextFactoryPort, IdentityOutboxPublisherPort,
    IdentityOutboxRepository, IdentityProjectionRepository, IdentityReadVisibilityRepository,
    IdentityReconciliationReportRepository, IdentityReferenceStateRepository,
    IdentityStoredResultRepository, IdentityTopicBindingPort, IdentityTraceRecordRepository,
    IdentityTruthChangeSubjectMapper, IdentityUnitOfWork, IdentityUnitOfWorkManagerPort,
    MemberSummaryProjectionRebuildPlan, MemoryReferenceRepository, OutboxPublishOutcome,
    RoleCapabilityRepository, TopicBindingResolution, TraceHandoffIntentRepository,
};
use identity_application::support::{
    AuditTrailId, IdempotencyReserveOutcome, IdentityAcceptedAuditTrailMarkers,
    IdentityAcceptedSubjectRefs, IdentityAdapterAvailability, IdentityAdapterModeRef,
    IdentityAdapterRef, IdentityCommandAcceptedResultEnvelope, IdentityCommandEffectSummary,
    IdentityCommandEffectSummaryRef, IdentityCommandRejectedResultEnvelope,
    IdentityConsumerReceiptEnvelope, IdentityIdempotencyKey, IdentityIdempotencyRecord,
    IdentityIdempotencyRecordRef, IdentityJobRunReport, IdentityOperationContext,
    IdentityOperationContextRef, IdentityOperationName, IdentityProjectionRefSet,
    IdentityRepositoryCursor, IdentityRepositoryPage, IdentityRequestDigest,
    IdentityRequestMetadataRef, IdentityStoredResultKind, IdentityStoredSurfaceMarkerRef,
    IdentityTraceRecordId, IdentityTransactionRef, IdentityTruthRef, IdentityVersion,
    IdentityVersionedRef, Page, StoredIdentityOperationResult, Versioned,
};
use identity_contracts::jobs::IdentityJobResultKind;
use identity_contracts::protocol::{
    IdentityJobName, IdentityOutboundEventName, IdentityProtocolSchemaVersionRef,
};
use identity_contracts::receipts::TraceHandoffIntentRef;
use identity_contracts::refs::{
    ArchiveHandoffRef, ArchiveRef, AuditCursorRef, AuditScopeRef, AuditTrailRef, CareerRecordId,
    CareerRecordRef, CareerSourceMarkerRef, ConsumerRef, ExternalReferenceKind,
    ExternalReferenceRef, ExternalSourceRef, ExternalSourceVersionRef, GlobalMemberId,
    GlobalMemberRef, GovernanceBasisRef, GovernanceBasisState, GovernanceBasisSummary,
    HandoffIssueRef, HandoffReceiptRef, HandoffScopeRef, HandoffTargetRef, IdentityAuditSubjectRef,
    IdentityChangeKindRef, IdentityJobRunRef, IdentityMaintenanceTargetRef,
    IdentityOutboxPayloadMarkerRef, IdentityOutboxRecordRef, IdentityOutboxSubjectRef,
    IdentityProjectionCursorRef, IdentityProjectionRef, IdentityReferenceOwnerKind,
    IdentityReferenceOwnerRef, IdentitySourceOwner, IdentitySourceRef, IdentityTimestamp,
    IdentityTraceRecordRef, IdentityTraceSubjectRef, IdentityTruthCursor, LifecycleRiskRef,
    MaintenanceScopeRef, MemberSummaryViewRef, MemoryRef, MemoryReferenceId, MemoryReferenceRef,
    MemoryReferenceSourceState, ProjectParticipationRef, ProjectionStateRef,
    ReconciliationReportRef, ReferenceResolutionStateRef, RoleCapabilitySourceRef,
    RoleCapabilitySourceSnapshotRef, RoleCapabilitySummaryRef, TopicKeyRef,
    TraceHandoffSafeMaterialRef, VisibilityContextRef, VisibilityResultRef, VisibilityScopeRef,
    WorkParticipationSourceState, WorkParticipationSourceSummary,
};
use identity_contracts::views::{IdentityVisibilityAccessSummary, MemberSummaryView};
use identity_domain::audit::{AuditTrail, AuditTrailEntry};
use identity_domain::career::CareerRecord;
use identity_domain::handoff::{HandoffStateKind, TraceHandoffIntent};
use identity_domain::lifecycle::GlobalLifecycleState;
use identity_domain::member_identity::GlobalMember;
use identity_domain::memory_reference::MemoryReference;
use identity_domain::outbox::{IdentityOutboxRecord, OutboxStateKind};
use identity_domain::projection_state::{ProjectionState, ProjectionStateKind};
use identity_domain::reconciliation::{ReconciliationReport, ReconciliationReportStateKind};
use identity_domain::reference_state::{ReferenceResolutionState, ReferenceResolutionStateKind};
use identity_domain::role_capability::{RoleCapabilitySourceSnapshot, RoleCapabilitySummary};
use identity_domain::trace::IdentityTraceRecord;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FaultCase {
    RollbackFails,
    CommitUnknown,
    SaveOutboxRecordFails,
    SaveStoredResultFails,
    SaveReceiptEnvelopeFails,
    SaveJobReportFails,
    CompleteIdempotencyFails,
}

#[derive(Clone, Debug, Default)]
pub struct IdentityInMemoryRuntimeBuilder {
    store: RuntimeStore,
}

impl IdentityInMemoryRuntimeBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn seed_member(mut self, member: GlobalMember, version: IdentityVersion) -> Self {
        self.store.members.insert(
            member_key(&member.member_ref),
            StoredMember { member, version },
        );
        self
    }

    pub fn seed_lifecycle(
        mut self,
        member_ref: GlobalMemberRef,
        lifecycle: GlobalLifecycleState,
        version: IdentityVersion,
    ) -> Self {
        self.store.lifecycles.insert(
            member_key(&member_ref),
            StoredLifecycle {
                member_ref,
                lifecycle,
                version,
            },
        );
        self
    }

    pub fn seed_role_capability_summary(
        mut self,
        summary: RoleCapabilitySummary,
        version: IdentityVersion,
    ) -> Self {
        self.store.role_capability_summary_by_member.insert(
            member_key(&summary.member_ref),
            summary.summary_ref.summary_id.as_str().to_owned(),
        );
        self.store
            .role_capability_summaries_by_member
            .entry(member_key(&summary.member_ref))
            .or_default()
            .push(summary.summary_ref.summary_id.as_str().to_owned());
        self.store.role_capability_summaries.insert(
            role_capability_summary_key(&summary.summary_ref),
            StoredRoleCapabilitySummary { summary, version },
        );
        self
    }

    pub fn seed_role_capability_source_snapshot(
        mut self,
        snapshot: RoleCapabilitySourceSnapshot,
        version: IdentityVersion,
    ) -> Self {
        self.store.role_capability_snapshot_by_source.insert(
            role_capability_source_key(&snapshot.source_ref),
            snapshot.snapshot_ref.snapshot_id.as_str().to_owned(),
        );
        self.store.role_capability_source_snapshots.insert(
            role_capability_snapshot_key(&snapshot.snapshot_ref),
            StoredRoleCapabilitySourceSnapshot { snapshot, version },
        );
        self
    }

    pub fn seed_career_record(mut self, record: CareerRecord, version: IdentityVersion) -> Self {
        let record_key = career_record_key(&record.career_record_ref);
        self.store
            .career_records_by_member
            .entry(member_key(&record.member_ref))
            .or_default()
            .push(record_key.clone());
        self.store.career_records_by_source_marker.insert(
            career_source_marker_key(&record.source_marker_ref),
            record_key.clone(),
        );
        if let Some(original_ref) = record.correction_of_ref.clone() {
            self.store
                .career_corrections_by_original
                .entry(career_record_key(&original_ref))
                .or_default()
                .push(record_key.clone());
        }
        self.store
            .career_records
            .insert(record_key, StoredCareerRecord { record, version });
        self
    }

    pub fn seed_memory_reference(
        mut self,
        reference: MemoryReference,
        version: IdentityVersion,
    ) -> Self {
        let reference_key = memory_reference_key(&reference.memory_reference_ref);
        self.store
            .memory_references_by_member
            .entry(member_key(&reference.member_ref))
            .or_default()
            .push(reference_key.clone());
        if let Some(memory_ref) = reference.memory_ref.clone() {
            self.store.memory_reference_by_memory.insert(
                memory_reference_member_memory_key(&reference.member_ref, &memory_ref),
                reference_key.clone(),
            );
        }
        if let Some(archive_ref) = reference.archive_ref.clone() {
            self.store.memory_reference_by_archive.insert(
                memory_reference_member_archive_key(&reference.member_ref, &archive_ref),
                reference_key.clone(),
            );
        }
        if let Some(handoff_ref) = reference.archive_handoff_ref.clone() {
            self.store
                .memory_reference_by_handoff
                .insert(archive_handoff_key(&handoff_ref), reference_key.clone());
        }
        self.store
            .memory_references
            .insert(reference_key, StoredMemoryReference { reference, version });
        self
    }

    pub fn seed_trace_record(
        mut self,
        trace_record: IdentityTraceRecord,
        version: IdentityVersion,
    ) -> Self {
        let key = trace_record.trace_record_ref.as_str().to_owned();
        self.store
            .trace_subject_index
            .entry(trace_record.subject_ref.as_str().to_owned())
            .or_default()
            .push(key.clone());
        self.store
            .trace_member_index
            .entry(member_key(&trace_record.member_ref))
            .or_default()
            .push(key.clone());
        self.store
            .trace_member_change_kind_index
            .entry(trace_member_change_kind_key(
                &trace_record.member_ref,
                &trace_record.change_kind_ref,
            ))
            .or_default()
            .push(key.clone());
        self.store.trace_records.insert(
            key,
            StoredTraceRecord {
                trace: trace_record,
                version,
            },
        );
        self
    }

    pub fn seed_audit_trail(mut self, trail: AuditTrail, version: IdentityVersion) -> Self {
        let key = trail.audit_trail_ref.as_str().to_owned();
        self.store
            .audit_subject_index
            .insert(trail.audit_subject_ref.as_str().to_owned(), key.clone());
        self.store
            .audit_trails
            .insert(key, StoredAuditTrail { trail, version });
        self
    }

    pub fn seed_member_summary_view(
        mut self,
        view: MemberSummaryView,
        version: IdentityVersion,
    ) -> Self {
        self.store.member_scope_index.insert(
            member_scope_key(&view.member_ref, &view.visibility_scope_ref),
            view.view_ref.as_str().to_owned(),
        );
        self.store.member_summary_views.insert(
            view.view_ref.as_str().to_owned(),
            StoredMemberSummaryView { view, version },
        );
        self
    }

    pub fn seed_member_summary_view_with_lookup_scope(
        mut self,
        lookup_member_ref: GlobalMemberRef,
        lookup_scope_ref: VisibilityScopeRef,
        view: MemberSummaryView,
        version: IdentityVersion,
    ) -> Self {
        self.store.member_scope_index.insert(
            member_scope_key(&lookup_member_ref, &lookup_scope_ref),
            view.view_ref.as_str().to_owned(),
        );
        self.store.member_summary_views.insert(
            view.view_ref.as_str().to_owned(),
            StoredMemberSummaryView { view, version },
        );
        self
    }

    pub fn seed_projection_state(
        mut self,
        state: ProjectionState,
        version: IdentityVersion,
    ) -> Self {
        self.store.projection_states.insert(
            projection_key(&state.projection_ref),
            StoredProjectionState { state, version },
        );
        self
    }

    pub fn seed_member_summary_rebuild_plan(
        mut self,
        plan: MemberSummaryProjectionRebuildPlan,
    ) -> Self {
        self.store
            .member_summary_rebuild_plans
            .insert(projection_key(&plan.projection_ref), plan);
        self
    }

    pub fn seed_reference_state(
        mut self,
        state: ReferenceResolutionState,
        sidecars: ExternalReferenceTypedSidecarRefs,
        version: IdentityVersion,
    ) -> Self {
        let key = external_reference_key(&state.external_reference_ref);
        self.store.reference_states.insert(
            key.clone(),
            StoredReferenceState {
                state: state.clone(),
                sidecars: sidecars.clone(),
                version,
            },
        );
        self.store.reference_resolution_outcomes.insert(
            key,
            ExternalReferenceResolutionOutcome {
                state,
                typed_sidecar_refs: Some(sidecars),
            },
        );
        self
    }

    pub fn seed_external_reference_resolution_outcome(
        mut self,
        outcome: ExternalReferenceResolutionOutcome,
    ) -> Self {
        self.store.reference_resolution_outcomes.insert(
            external_reference_key(&outcome.state.external_reference_ref),
            outcome,
        );
        self
    }

    pub fn seed_reconciliation_report(
        mut self,
        report: ReconciliationReport,
        version: IdentityVersion,
    ) -> Self {
        self.store.reconciliation_reports.insert(
            report.report_ref.as_str().to_owned(),
            StoredReconciliationReport { report, version },
        );
        self
    }

    pub fn seed_handoff_intent(
        mut self,
        intent: TraceHandoffIntent,
        version: IdentityVersion,
    ) -> Self {
        self.store.handoff_intents.insert(
            intent.handoff_intent_ref.as_str().to_owned(),
            StoredHandoffIntent { intent, version },
        );
        self
    }

    pub fn seed_outbox_record(
        mut self,
        record: IdentityOutboxRecord,
        version: IdentityVersion,
    ) -> Self {
        let key = record.outbox_record_ref.as_str().to_owned();
        self.store.outbox_subject_index.insert(
            outbox_subject_key(&record.subject_ref, &record.outbox_record_ref),
            key.clone(),
        );
        self.store.outbox_trace_index.insert(
            outbox_trace_key(&record.trace_record_ref, &record.outbox_record_ref),
            key.clone(),
        );
        seed_outbox_payload_marker_snapshot(&mut self.store, &record)
            .expect("seeded outbox record must use a canonical accepted outbound material");
        self.store
            .outbox_records
            .insert(key, StoredOutboxRecord { record, version });
        self
    }

    pub fn seed_topic_binding_resolution(
        mut self,
        topic_key_ref: TopicKeyRef,
        payload_marker_ref: IdentityOutboxPayloadMarkerRef,
        resolution: TopicBindingResolution,
    ) -> Self {
        self.store.topic_bindings.insert(
            topic_binding_key(&topic_key_ref, &payload_marker_ref),
            resolution,
        );
        self
    }

    pub fn seed_outbox_publish_outcome(
        mut self,
        outbox_ref: IdentityOutboxRecordRef,
        outcome: OutboxPublishOutcome,
    ) -> Self {
        self.store
            .outbox_publish_outcomes
            .insert(outbox_ref.as_str().to_owned(), outcome);
        self
    }

    pub fn seed_handoff_target_resolution(
        mut self,
        target_ref: HandoffTargetRef,
        scope_ref: HandoffScopeRef,
        safe_material_ref: TraceHandoffSafeMaterialRef,
        resolution: HandoffTargetResolution,
    ) -> Self {
        self.store.handoff_target_resolutions.insert(
            handoff_target_resolution_key(&target_ref, &scope_ref, &safe_material_ref),
            resolution,
        );
        self
    }

    pub fn seed_handoff_delivery_outcome(
        mut self,
        intent_ref: TraceHandoffIntentRef,
        outcome: HandoffDeliveryOutcome,
    ) -> Self {
        self.store
            .handoff_delivery_outcomes
            .insert(intent_ref.as_str().to_owned(), outcome);
        self
    }

    pub fn seed_handoff_receipt_resolution(mut self, resolution: HandoffReceiptResolution) -> Self {
        self.store
            .handoff_receipt_resolutions
            .insert(resolution.receipt_ref.as_str().to_owned(), resolution);
        self
    }

    pub fn seed_idempotency_record(
        mut self,
        record: IdentityIdempotencyRecord,
        version: IdentityVersion,
    ) -> Self {
        let key = record.record_ref.as_str().to_owned();
        self.store.idempotency_key_index.insert(
            idempotency_key_key(
                &record.operation_name,
                record.channel,
                &record.idempotency_key,
            ),
            key.clone(),
        );
        self.store
            .idempotency_records
            .insert(key, StoredIdempotencyRecord { record, version });
        self
    }

    pub fn seed_stored_result(mut self, result: StoredIdentityOperationResult) -> Self {
        let key = result.stored_result_ref.as_str().to_owned();
        self.store.stored_result_by_context.insert(
            result.operation_context_ref.as_str().to_owned(),
            key.clone(),
        );
        self.store.stored_results.insert(key, result);
        self
    }

    pub fn seed_consumer_receipt(mut self, envelope: IdentityConsumerReceiptEnvelope) -> Self {
        self.store
            .consumer_receipts
            .insert(envelope.stored_result_ref.as_str().to_owned(), envelope);
        self
    }

    pub fn seed_command_accepted_envelope(
        mut self,
        envelope: IdentityCommandAcceptedResultEnvelope,
    ) -> Self {
        self.store
            .command_accepted_envelopes
            .insert(envelope.stored_result_ref.as_str().to_owned(), envelope);
        self
    }

    pub fn seed_command_rejected_envelope(
        mut self,
        envelope: IdentityCommandRejectedResultEnvelope,
    ) -> Self {
        self.store
            .command_rejected_envelopes
            .insert(envelope.stored_result_ref.as_str().to_owned(), envelope);
        self
    }

    pub fn seed_handoff_callback_receipt(
        mut self,
        envelope: IdentityConsumerReceiptEnvelope,
    ) -> Self {
        self.store
            .handoff_callback_receipts
            .insert(envelope.stored_result_ref.as_str().to_owned(), envelope);
        self
    }

    pub fn seed_effect_summary(mut self, summary: IdentityCommandEffectSummary) -> Self {
        let summary_ref = summary.effect_summary_ref.as_str().to_owned();
        self.store.effects_by_context.insert(
            effect_context_key(&summary.operation_context_ref, &summary.effect_summary_ref),
            summary_ref.clone(),
        );
        self.store.effects_by_truth_ref.insert(
            effect_truth_key(&summary.primary_truth_ref, &summary.effect_summary_ref),
            summary_ref.clone(),
        );
        self.store.effects_after_cursor.insert(
            effect_cursor_key(&summary.accepted_cursor_ref, &summary.effect_summary_ref),
            summary_ref.clone(),
        );
        self.store
            .command_effect_summaries
            .insert(summary_ref, summary);
        self
    }

    pub fn seed_job_report(
        mut self,
        report: IdentityJobRunReport,
        version: IdentityVersion,
    ) -> Self {
        let report_ref = report.report_ref.as_str().to_owned();
        self.store
            .job_report_by_run
            .insert(report.job_run_ref.as_str().to_owned(), report_ref.clone());
        self.store.job_report_by_name.insert(
            job_report_name_key(&report.job_name, &report.report_ref),
            report_ref.clone(),
        );
        self.store.job_report_by_result.insert(
            job_report_result_key(report.result_kind, &report.report_ref),
            report_ref.clone(),
        );
        self.store
            .job_reports
            .insert(report_ref, StoredJobReport { report, version });
        self
    }

    pub fn seed_adapter_availability(mut self, availability: IdentityAdapterAvailability) -> Self {
        self.store
            .adapter_availability
            .insert(availability.adapter_ref.as_str().to_owned(), availability);
        self
    }

    pub fn seed_member_summary_access(
        mut self,
        member_ref: GlobalMemberRef,
        access_summary: IdentityVisibilityAccessSummary,
    ) -> Self {
        self.store
            .member_summary_access
            .insert(member_key(&member_ref), access_summary);
        self
    }

    pub fn seed_trace_read_access(
        mut self,
        subject_ref: IdentityTraceSubjectRef,
        access_summary: IdentityVisibilityAccessSummary,
    ) -> Self {
        self.store
            .trace_read_access
            .insert(subject_ref.as_str().to_owned(), access_summary);
        self
    }

    pub fn seed_trace_member_page_access(
        mut self,
        member_ref: GlobalMemberRef,
        change_kind_ref: Option<IdentityChangeKindRef>,
        access_summary: IdentityVisibilityAccessSummary,
    ) -> Self {
        self.store.trace_member_page_access.insert(
            trace_member_page_access_key(&member_ref, change_kind_ref.as_ref()),
            access_summary,
        );
        self
    }

    pub fn seed_audit_read_access(
        mut self,
        audit_subject_ref: IdentityAuditSubjectRef,
        audit_scope_ref: AuditScopeRef,
        access_summary: IdentityVisibilityAccessSummary,
    ) -> Self {
        self.store.audit_read_access.insert(
            audit_access_key(&audit_subject_ref, &audit_scope_ref),
            access_summary,
        );
        self
    }

    pub fn seed_report_read_access(
        mut self,
        report_ref: ReconciliationReportRef,
        access_summary: IdentityVisibilityAccessSummary,
    ) -> Self {
        self.store
            .report_read_access
            .insert(report_ref.as_str().to_owned(), access_summary);
        self
    }

    pub fn seed_reconciliation_scope_read_access(
        mut self,
        maintenance_scope_ref: MaintenanceScopeRef,
        access_summary: IdentityVisibilityAccessSummary,
    ) -> Self {
        self.store.reconciliation_scope_read_access.insert(
            maintenance_scope_key(&maintenance_scope_ref),
            access_summary,
        );
        self
    }

    pub fn seed_maintenance_targets(
        mut self,
        maintenance_scope_ref: MaintenanceScopeRef,
        target_refs: Vec<IdentityMaintenanceTargetRef>,
    ) -> Self {
        self.store
            .maintenance_targets_by_scope
            .insert(maintenance_scope_key(&maintenance_scope_ref), target_refs);
        self
    }

    pub fn seed_projection_targets_for_rebuild(
        mut self,
        maintenance_scope_ref: MaintenanceScopeRef,
        projection_refs: Vec<IdentityProjectionRef>,
    ) -> Self {
        self.store.projection_targets_by_scope.insert(
            maintenance_scope_key(&maintenance_scope_ref),
            projection_refs,
        );
        self
    }

    pub fn seed_reference_targets_for_refresh(
        mut self,
        maintenance_scope_ref: MaintenanceScopeRef,
        reference_refs: Vec<ExternalReferenceRef>,
    ) -> Self {
        self.store.reference_targets_by_scope.insert(
            maintenance_scope_key(&maintenance_scope_ref),
            reference_refs,
        );
        self
    }

    pub fn seed_report_targets(
        mut self,
        maintenance_scope_ref: MaintenanceScopeRef,
        target_refs: Vec<IdentityMaintenanceTargetRef>,
    ) -> Self {
        self.store
            .report_targets_by_scope
            .insert(maintenance_scope_key(&maintenance_scope_ref), target_refs);
        self
    }

    pub fn seed_maintenance_target_inspection_context(
        mut self,
        context: IdentityMaintenanceInspectionContext,
    ) -> Self {
        self.store
            .maintenance_inspection_contexts
            .insert(maintenance_target_key(&context.target_ref), context);
        self
    }

    pub fn seed_projection_state_read_access(
        mut self,
        projection_ref: IdentityProjectionRef,
        projection_state_ref: Option<ProjectionStateRef>,
        access_summary: IdentityVisibilityAccessSummary,
    ) -> Self {
        self.store.projection_state_read_access.insert(
            projection_state_access_key(&projection_ref, projection_state_ref.as_ref()),
            access_summary,
        );
        self
    }

    pub fn seed_reference_state_read_access(
        mut self,
        external_reference_ref: ExternalReferenceRef,
        owner_ref: Option<IdentityReferenceOwnerRef>,
        access_summary: IdentityVisibilityAccessSummary,
    ) -> Self {
        self.store.reference_state_read_access.insert(
            reference_state_access_key(&external_reference_ref, owner_ref.as_ref()),
            access_summary,
        );
        self
    }

    pub fn seed_outbox_record_read_access(
        mut self,
        outbox_ref: Option<IdentityOutboxRecordRef>,
        subject_ref: Option<IdentityOutboxSubjectRef>,
        topic_key_ref: Option<TopicKeyRef>,
        access_summary: IdentityVisibilityAccessSummary,
    ) -> Self {
        self.store.outbox_record_read_access.insert(
            outbox_record_access_key(
                outbox_ref.as_ref(),
                subject_ref.as_ref(),
                topic_key_ref.as_ref(),
            ),
            access_summary,
        );
        self
    }

    pub fn seed_outbox_trace_page_read_access(
        mut self,
        trace_record_ref: IdentityTraceRecordRef,
        access_summary: IdentityVisibilityAccessSummary,
    ) -> Self {
        self.store
            .outbox_trace_page_read_access
            .insert(trace_record_ref.as_str().to_owned(), access_summary);
        self
    }

    pub fn seed_handoff_intent_read_access(
        mut self,
        handoff_intent_ref: TraceHandoffIntentRef,
        access_summary: IdentityVisibilityAccessSummary,
    ) -> Self {
        self.store
            .handoff_intent_read_access
            .insert(handoff_intent_ref.as_str().to_owned(), access_summary);
        self
    }

    pub fn inject_fault(mut self, fault: FaultCase) -> Self {
        self.store.faults.insert(fault);
        self
    }

    pub fn build(self) -> IdentityInMemoryRuntime {
        IdentityInMemoryRuntime {
            shared: Arc::new(SharedRuntime {
                store: Mutex::new(self.store),
                next_transaction_id: AtomicU64::new(1),
                next_truth_cursor_id: AtomicU64::new(1),
                next_reference_cursor_id: AtomicU64::new(1),
                staged_by_tx: Mutex::new(HashMap::new()),
            }),
        }
    }
}

#[derive(Clone)]
pub struct IdentityInMemoryRuntime {
    shared: Arc<SharedRuntime>,
}

impl IdentityInMemoryRuntime {
    pub fn builder() -> IdentityInMemoryRuntimeBuilder {
        IdentityInMemoryRuntimeBuilder::new()
    }

    pub fn uow_manager(&self) -> &Self {
        self
    }

    pub fn projection_repository(&self) -> &Self {
        self
    }

    pub fn maintenance_repository(&self) -> &Self {
        self
    }

    pub fn reference_state_repository(&self) -> &Self {
        self
    }

    pub fn handoff_intent_repository(&self) -> &Self {
        self
    }

    pub fn adapter_availability_port(&self) -> &Self {
        self
    }

    pub fn handoff_target_port(&self) -> &Self {
        self
    }

    pub fn handoff_delivery_port(&self) -> &Self {
        self
    }

    pub fn read_visibility_repository(&self) -> &Self {
        self
    }

    pub fn active_write_transactions(&self) -> Result<usize, ApplicationError> {
        let staged = self.shared.staged_by_tx.lock().map_err(|_| {
            ApplicationError::consistency_defect("staged transaction map lock poisoned")
        })?;
        Ok(staged.len())
    }

    pub fn staged_write_count(&self) -> Result<usize, ApplicationError> {
        let staged = self.shared.staged_by_tx.lock().map_err(|_| {
            ApplicationError::consistency_defect("staged transaction map lock poisoned")
        })?;
        Ok(staged.values().map(Vec::len).sum())
    }

    fn stage(
        &self,
        transaction_ref: &IdentityTransactionRef,
        op: StagedOp,
    ) -> Result<(), ApplicationError> {
        let mut staged = self.shared.staged_by_tx.lock().map_err(|_| {
            ApplicationError::consistency_defect("staged transaction map lock poisoned")
        })?;
        staged.entry(tx_key(transaction_ref)).or_default().push(op);
        Ok(())
    }

    fn staged_reference_bundle_state(
        &self,
        transaction_ref: &IdentityTransactionRef,
        reference_ref: &ExternalReferenceRef,
    ) -> Result<Option<ReferenceBundleVersionState>, ApplicationError> {
        let committed = {
            let store =
                self.shared.store.lock().map_err(|_| {
                    ApplicationError::consistency_defect("runtime store lock poisoned")
                })?;
            store
                .reference_states
                .get(&external_reference_key(reference_ref))
                .map(|stored| ReferenceBundleVersionState {
                    value_ref: stored.state.resolution_state_ref.clone(),
                    committed_version: Some(stored.version),
                    current_version: stored.version,
                })
        };
        let staged = self.shared.staged_by_tx.lock().map_err(|_| {
            ApplicationError::consistency_defect("staged transaction map lock poisoned")
        })?;
        let mut current = committed;
        if let Some(ops) = staged.get(&tx_key(transaction_ref)) {
            for op in ops {
                match op {
                    StagedOp::SaveReferenceState {
                        state,
                        expected_version,
                    } if state.external_reference_ref == *reference_ref => {
                        let committed_version =
                            current.as_ref().and_then(|value| value.committed_version);
                        current = Some(ReferenceBundleVersionState {
                            value_ref: state.resolution_state_ref.clone(),
                            committed_version,
                            current_version: expected_version
                                .map(|value| IdentityVersion::new(value.get() + 1))
                                .unwrap_or_else(|| IdentityVersion::new(1)),
                        });
                    }
                    StagedOp::SaveTypedSidecars {
                        reference_ref: staged_reference_ref,
                        expected_version,
                        ..
                    } if staged_reference_ref == reference_ref => {
                        if let Some(current_state) = current.as_mut() {
                            if current_state.current_version == *expected_version {
                                current_state.current_version =
                                    IdentityVersion::new(expected_version.get() + 1);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        Ok(current)
    }

    fn predicted_view_version(
        &self,
        view_ref: &MemberSummaryViewRef,
    ) -> Result<IdentityVersion, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(IdentityVersion::new(
            store
                .member_summary_views
                .get(view_ref.as_str())
                .map(|stored| stored.version.get() + 1)
                .unwrap_or(1),
        ))
    }

    fn predicted_member_version(
        &self,
        member_ref: &GlobalMemberRef,
    ) -> Result<IdentityVersion, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(IdentityVersion::new(
            store
                .members
                .get(&member_key(member_ref))
                .map(|stored| stored.version.get() + 1)
                .unwrap_or(1),
        ))
    }

    fn predicted_lifecycle_version(
        &self,
        member_ref: &GlobalMemberRef,
    ) -> Result<IdentityVersion, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(IdentityVersion::new(
            store
                .lifecycles
                .get(&member_key(member_ref))
                .map(|stored| stored.version.get() + 1)
                .unwrap_or(1),
        ))
    }

    fn predicted_role_capability_summary_version(
        &self,
        summary_ref: &RoleCapabilitySummaryRef,
    ) -> Result<IdentityVersion, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(IdentityVersion::new(
            store
                .role_capability_summaries
                .get(&role_capability_summary_key(summary_ref))
                .map(|stored| stored.version.get() + 1)
                .unwrap_or(1),
        ))
    }

    fn predicted_role_capability_snapshot_version(
        &self,
        snapshot_ref: &RoleCapabilitySourceSnapshotRef,
    ) -> Result<IdentityVersion, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(IdentityVersion::new(
            store
                .role_capability_source_snapshots
                .get(&role_capability_snapshot_key(snapshot_ref))
                .map(|stored| stored.version.get() + 1)
                .unwrap_or(1),
        ))
    }

    fn predicted_career_record_version(
        &self,
        record_ref: &CareerRecordRef,
    ) -> Result<IdentityVersion, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(IdentityVersion::new(
            store
                .career_records
                .get(&career_record_key(record_ref))
                .map(|stored| stored.version.get() + 1)
                .unwrap_or(1),
        ))
    }

    fn predicted_memory_reference_version(
        &self,
        reference_ref: &MemoryReferenceRef,
    ) -> Result<IdentityVersion, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(IdentityVersion::new(
            store
                .memory_references
                .get(&memory_reference_key(reference_ref))
                .map(|stored| stored.version.get() + 1)
                .unwrap_or(1),
        ))
    }

    fn predicted_trace_version(
        &self,
        trace_record_ref: &IdentityTraceRecordRef,
    ) -> Result<IdentityVersion, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(IdentityVersion::new(
            store
                .trace_records
                .get(trace_record_ref.as_str())
                .map(|stored| stored.version.get() + 1)
                .unwrap_or(1),
        ))
    }

    fn predicted_audit_version(
        &self,
        audit_trail_ref: &AuditTrailRef,
    ) -> Result<IdentityVersion, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(IdentityVersion::new(
            store
                .audit_trails
                .get(audit_trail_ref.as_str())
                .map(|stored| stored.version.get() + 1)
                .unwrap_or(1),
        ))
    }

    fn predicted_projection_version(
        &self,
        projection_ref: &IdentityProjectionRef,
    ) -> Result<IdentityVersion, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(IdentityVersion::new(
            store
                .projection_states
                .get(&projection_key(projection_ref))
                .map(|stored| stored.version.get() + 1)
                .unwrap_or(1),
        ))
    }

    fn predicted_reference_version(
        &self,
        reference_ref: &ExternalReferenceRef,
    ) -> Result<IdentityVersion, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(IdentityVersion::new(
            store
                .reference_states
                .get(&external_reference_key(reference_ref))
                .map(|stored| stored.version.get() + 1)
                .unwrap_or(1),
        ))
    }

    fn predicted_reconciliation_report_version(
        &self,
        report_ref: &ReconciliationReportRef,
    ) -> Result<IdentityVersion, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(IdentityVersion::new(
            store
                .reconciliation_reports
                .get(report_ref.as_str())
                .map(|stored| stored.version.get() + 1)
                .unwrap_or(1),
        ))
    }

    fn predicted_handoff_version(
        &self,
        intent_ref: &TraceHandoffIntentRef,
    ) -> Result<IdentityVersion, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(IdentityVersion::new(
            store
                .handoff_intents
                .get(intent_ref.as_str())
                .map(|stored| stored.version.get() + 1)
                .unwrap_or(1),
        ))
    }

    fn predicted_outbox_version(
        &self,
        outbox_ref: &IdentityOutboxRecordRef,
    ) -> Result<IdentityVersion, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(IdentityVersion::new(
            store
                .outbox_records
                .get(outbox_ref.as_str())
                .map(|stored| stored.version.get() + 1)
                .unwrap_or(1),
        ))
    }

    fn predicted_job_report_version(
        &self,
        report_ref: &identity_contracts::refs::IdentityJobReportRef,
    ) -> Result<IdentityVersion, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(IdentityVersion::new(
            store
                .job_reports
                .get(report_ref.as_str())
                .map(|stored| stored.version.get() + 1)
                .unwrap_or(1),
        ))
    }
}

struct SharedRuntime {
    store: Mutex<RuntimeStore>,
    next_transaction_id: AtomicU64,
    next_truth_cursor_id: AtomicU64,
    next_reference_cursor_id: AtomicU64,
    staged_by_tx: Mutex<HashMap<String, Vec<StagedOp>>>,
}

#[derive(Clone, Debug)]
struct ReferenceBundleVersionState {
    value_ref: ReferenceResolutionStateRef,
    committed_version: Option<IdentityVersion>,
    current_version: IdentityVersion,
}

#[derive(Clone, Debug, Default)]
struct RuntimeStore {
    members: HashMap<String, StoredMember>,
    lifecycles: HashMap<String, StoredLifecycle>,
    role_capability_summaries: HashMap<String, StoredRoleCapabilitySummary>,
    role_capability_summaries_by_member: HashMap<String, Vec<String>>,
    role_capability_summary_by_member: HashMap<String, String>,
    role_capability_source_snapshots: HashMap<String, StoredRoleCapabilitySourceSnapshot>,
    role_capability_snapshot_by_source: HashMap<String, String>,
    career_records: HashMap<String, StoredCareerRecord>,
    career_records_by_member: HashMap<String, Vec<String>>,
    career_records_by_source_marker: HashMap<String, String>,
    career_corrections_by_original: HashMap<String, Vec<String>>,
    memory_references: HashMap<String, StoredMemoryReference>,
    memory_references_by_member: HashMap<String, Vec<String>>,
    memory_reference_by_memory: HashMap<String, String>,
    memory_reference_by_archive: HashMap<String, String>,
    memory_reference_by_handoff: HashMap<String, String>,
    trace_records: HashMap<String, StoredTraceRecord>,
    trace_subject_index: HashMap<String, Vec<String>>,
    trace_member_index: HashMap<String, Vec<String>>,
    trace_member_change_kind_index: HashMap<String, Vec<String>>,
    audit_trails: HashMap<String, StoredAuditTrail>,
    audit_subject_index: HashMap<String, String>,
    member_summary_views: HashMap<String, StoredMemberSummaryView>,
    member_scope_index: HashMap<String, String>,
    projection_states: HashMap<String, StoredProjectionState>,
    member_summary_rebuild_plans: HashMap<String, MemberSummaryProjectionRebuildPlan>,
    reference_states: HashMap<String, StoredReferenceState>,
    reference_resolution_outcomes: HashMap<String, ExternalReferenceResolutionOutcome>,
    reconciliation_reports: HashMap<String, StoredReconciliationReport>,
    maintenance_targets_by_scope: HashMap<String, Vec<IdentityMaintenanceTargetRef>>,
    projection_targets_by_scope: HashMap<String, Vec<IdentityProjectionRef>>,
    reference_targets_by_scope: HashMap<String, Vec<ExternalReferenceRef>>,
    report_targets_by_scope: HashMap<String, Vec<IdentityMaintenanceTargetRef>>,
    maintenance_inspection_contexts: HashMap<String, IdentityMaintenanceInspectionContext>,
    handoff_intents: HashMap<String, StoredHandoffIntent>,
    outbox_records: HashMap<String, StoredOutboxRecord>,
    outbox_payload_markers: HashMap<String, StoredOutboxPayloadMarker>,
    outbox_subject_index: HashMap<String, String>,
    outbox_trace_index: HashMap<String, String>,
    outbox_trace_event_index: HashMap<String, String>,
    idempotency_records: HashMap<String, StoredIdempotencyRecord>,
    idempotency_key_index: HashMap<String, String>,
    stored_results: HashMap<String, StoredIdentityOperationResult>,
    stored_result_by_context: HashMap<String, String>,
    command_accepted_envelopes: HashMap<String, IdentityCommandAcceptedResultEnvelope>,
    command_rejected_envelopes: HashMap<String, IdentityCommandRejectedResultEnvelope>,
    consumer_receipts: HashMap<String, IdentityConsumerReceiptEnvelope>,
    handoff_callback_receipts: HashMap<String, IdentityConsumerReceiptEnvelope>,
    command_effect_summaries: HashMap<String, IdentityCommandEffectSummary>,
    effects_by_context: HashMap<String, String>,
    effects_by_truth_ref: HashMap<String, String>,
    effects_after_cursor: HashMap<String, String>,
    job_reports: HashMap<String, StoredJobReport>,
    job_report_by_run: HashMap<String, String>,
    job_report_by_name: HashMap<String, String>,
    job_report_by_result: HashMap<String, String>,
    adapter_availability: HashMap<String, IdentityAdapterAvailability>,
    topic_bindings: HashMap<String, TopicBindingResolution>,
    outbox_publish_outcomes: HashMap<String, OutboxPublishOutcome>,
    handoff_target_resolutions: HashMap<String, HandoffTargetResolution>,
    handoff_delivery_outcomes: HashMap<String, HandoffDeliveryOutcome>,
    handoff_receipt_resolutions: HashMap<String, HandoffReceiptResolution>,
    member_summary_access: HashMap<String, IdentityVisibilityAccessSummary>,
    trace_read_access: HashMap<String, IdentityVisibilityAccessSummary>,
    trace_member_page_access: HashMap<String, IdentityVisibilityAccessSummary>,
    audit_read_access: HashMap<String, IdentityVisibilityAccessSummary>,
    report_read_access: HashMap<String, IdentityVisibilityAccessSummary>,
    reconciliation_scope_read_access: HashMap<String, IdentityVisibilityAccessSummary>,
    projection_state_read_access: HashMap<String, IdentityVisibilityAccessSummary>,
    reference_state_read_access: HashMap<String, IdentityVisibilityAccessSummary>,
    outbox_record_read_access: HashMap<String, IdentityVisibilityAccessSummary>,
    outbox_trace_page_read_access: HashMap<String, IdentityVisibilityAccessSummary>,
    handoff_intent_read_access: HashMap<String, IdentityVisibilityAccessSummary>,
    faults: HashSet<FaultCase>,
}

#[derive(Clone, Debug)]
struct StoredMemberSummaryView {
    view: MemberSummaryView,
    version: IdentityVersion,
}

#[derive(Clone, Debug)]
struct StoredMember {
    member: GlobalMember,
    version: IdentityVersion,
}

#[derive(Clone, Debug)]
struct StoredLifecycle {
    member_ref: GlobalMemberRef,
    lifecycle: GlobalLifecycleState,
    version: IdentityVersion,
}

#[derive(Clone, Debug)]
struct StoredRoleCapabilitySummary {
    summary: RoleCapabilitySummary,
    version: IdentityVersion,
}

#[derive(Clone, Debug)]
struct StoredRoleCapabilitySourceSnapshot {
    snapshot: RoleCapabilitySourceSnapshot,
    version: IdentityVersion,
}

#[derive(Clone, Debug)]
struct StoredCareerRecord {
    record: CareerRecord,
    version: IdentityVersion,
}

#[derive(Clone, Debug)]
struct StoredMemoryReference {
    reference: MemoryReference,
    version: IdentityVersion,
}

#[derive(Clone, Debug)]
struct StoredTraceRecord {
    trace: IdentityTraceRecord,
    version: IdentityVersion,
}

#[derive(Clone, Debug)]
struct StoredAuditTrail {
    trail: AuditTrail,
    version: IdentityVersion,
}

#[derive(Clone, Debug)]
struct StoredProjectionState {
    state: ProjectionState,
    version: IdentityVersion,
}

#[derive(Clone, Debug)]
struct StoredReferenceState {
    state: ReferenceResolutionState,
    sidecars: ExternalReferenceTypedSidecarRefs,
    version: IdentityVersion,
}

#[derive(Clone, Debug)]
struct StoredReconciliationReport {
    report: ReconciliationReport,
    version: IdentityVersion,
}

#[derive(Clone, Debug)]
struct StoredHandoffIntent {
    intent: TraceHandoffIntent,
    version: IdentityVersion,
}

#[derive(Clone, Debug)]
struct StoredOutboxRecord {
    record: IdentityOutboxRecord,
    version: IdentityVersion,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct StoredOutboxPayloadMarker {
    payload_marker_ref: IdentityOutboxPayloadMarkerRef,
    event_name: IdentityOutboundEventName,
    schema_version_ref: IdentityProtocolSchemaVersionRef,
    subject_ref: IdentityOutboxSubjectRef,
    trace_record_ref: IdentityTraceRecordRef,
}

#[derive(Clone, Debug)]
struct StoredIdempotencyRecord {
    record: IdentityIdempotencyRecord,
    version: IdentityVersion,
}

#[derive(Clone, Debug)]
struct StoredJobReport {
    report: IdentityJobRunReport,
    version: IdentityVersion,
}

#[derive(Clone, Debug)]
enum StagedOp {
    SaveMember {
        member: GlobalMember,
        expected_version: Option<IdentityVersion>,
    },
    SaveLifecycle {
        member_ref: GlobalMemberRef,
        lifecycle: GlobalLifecycleState,
        expected_version: Option<IdentityVersion>,
    },
    SaveRoleCapabilitySourceSnapshot {
        snapshot: RoleCapabilitySourceSnapshot,
        expected_version: Option<IdentityVersion>,
    },
    SaveRoleCapabilitySummary {
        summary: RoleCapabilitySummary,
        expected_version: Option<IdentityVersion>,
    },
    AppendCareerRecord {
        record: CareerRecord,
    },
    SaveCareerRecordState {
        record: CareerRecord,
        expected_version: IdentityVersion,
    },
    SaveMemoryReference {
        reference: MemoryReference,
        expected_version: Option<IdentityVersion>,
    },
    AppendTraceRecord {
        trace_record: IdentityTraceRecord,
    },
    SaveTraceRecordState {
        trace_record: IdentityTraceRecord,
        expected_version: IdentityVersion,
    },
    SaveAuditTrail {
        trail: AuditTrail,
        expected_version: Option<IdentityVersion>,
    },
    AppendAuditEntry {
        audit_trail_ref: AuditTrailRef,
        entry: AuditTrailEntry,
        expected_version: IdentityVersion,
    },
    SaveMemberSummaryView {
        view: MemberSummaryView,
        expected_version: Option<IdentityVersion>,
    },
    SaveProjectionState {
        state: ProjectionState,
        expected_version: Option<IdentityVersion>,
    },
    SaveReferenceState {
        state: ReferenceResolutionState,
        expected_version: Option<IdentityVersion>,
    },
    SaveReconciliationReport {
        report: ReconciliationReport,
        expected_version: Option<IdentityVersion>,
    },
    SaveTypedSidecars {
        reference_ref: ExternalReferenceRef,
        sidecars: ExternalReferenceTypedSidecarRefs,
        expected_version: IdentityVersion,
    },
    SaveHandoffIntent {
        intent: TraceHandoffIntent,
        expected_version: Option<IdentityVersion>,
    },
    SaveOutboxRecord {
        record: IdentityOutboxRecord,
        expected_version: Option<IdentityVersion>,
    },
    SaveIdempotencyReservation {
        record: IdentityIdempotencyRecord,
    },
    SaveIdempotencyTerminal {
        record: IdentityIdempotencyRecord,
        expected_version: IdentityVersion,
    },
    SaveStoredResult {
        result: StoredIdentityOperationResult,
    },
    SaveCommandAcceptedEnvelope {
        envelope: IdentityCommandAcceptedResultEnvelope,
    },
    SaveCommandRejectedEnvelope {
        envelope: IdentityCommandRejectedResultEnvelope,
    },
    SaveConsumerReceiptEnvelope {
        envelope: IdentityConsumerReceiptEnvelope,
    },
    SaveHandoffCallbackReceiptEnvelope {
        envelope: IdentityConsumerReceiptEnvelope,
    },
    SaveEffectSummary {
        summary: IdentityCommandEffectSummary,
    },
    SaveJobReport {
        report: IdentityJobRunReport,
        expected_version: Option<IdentityVersion>,
    },
}

struct InMemoryUnitOfWork {
    transaction_ref: IdentityTransactionRef,
    shared: Arc<SharedRuntime>,
    truth_cursor: Mutex<Option<IdentityTruthCursor>>,
    reference_cursor: Mutex<Option<IdentityTruthCursor>>,
}

impl IdentityUnitOfWork for InMemoryUnitOfWork {
    fn transaction_ref(&self) -> IdentityTransactionRef {
        self.transaction_ref.clone()
    }

    fn assign_truth_change_cursor(&self) -> Result<IdentityTruthCursor, ApplicationError> {
        let mut truth_cursor = self
            .truth_cursor
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("truth cursor lock poisoned"))?;
        if let Some(existing) = truth_cursor.as_ref() {
            return Ok(existing.clone());
        }

        let next = self
            .shared
            .next_truth_cursor_id
            .fetch_add(1, Ordering::SeqCst);
        let assigned = IdentityTruthCursor::new(format!("truth-cursor-{next}"));
        *truth_cursor = Some(assigned.clone());
        Ok(assigned)
    }

    fn assign_reference_marker_cursor(&self) -> Result<IdentityTruthCursor, ApplicationError> {
        let mut reference_cursor = self
            .reference_cursor
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("reference cursor lock poisoned"))?;
        if let Some(existing) = reference_cursor.as_ref() {
            return Ok(existing.clone());
        }

        let next = self
            .shared
            .next_reference_cursor_id
            .fetch_add(1, Ordering::SeqCst);
        let assigned = IdentityTruthCursor::new(format!("reference-cursor-{next}"));
        *reference_cursor = Some(assigned.clone());
        Ok(assigned)
    }
}

impl IdentityUnitOfWorkManagerPort for IdentityInMemoryRuntime {
    fn begin(&self) -> Result<Box<dyn IdentityUnitOfWork>, ApplicationError> {
        let next = self
            .shared
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(InMemoryUnitOfWork {
            transaction_ref: IdentityTransactionRef::new(format!("tx-{next}")),
            shared: self.shared.clone(),
            truth_cursor: Mutex::new(None),
            reference_cursor: Mutex::new(None),
        }))
    }

    fn commit(&self, uow: Box<dyn IdentityUnitOfWork>) -> Result<(), ApplicationError> {
        let transaction_ref = uow.transaction_ref();
        let tx_key = tx_key(&transaction_ref);
        let staged_ops = {
            let mut staged = self.shared.staged_by_tx.lock().map_err(|_| {
                ApplicationError::consistency_defect("staged transaction map lock poisoned")
            })?;
            staged.remove(&tx_key).unwrap_or_default()
        };

        let mut store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let current = store.clone();

        for op in staged_ops {
            if let Err(error) = apply_op(&mut store, &current, op) {
                *store = current;
                return Err(error);
            }
        }

        if store.faults.contains(&FaultCase::CommitUnknown) {
            return Err(ApplicationError::new(
                ApplicationErrorKind::CommitStatusUnknown,
                "commit status unknown; check stored replay surface before retry",
            ));
        }

        Ok(())
    }

    fn rollback(&self, uow: Box<dyn IdentityUnitOfWork>) -> Result<(), ApplicationError> {
        let transaction_ref = uow.transaction_ref();
        let tx_key = tx_key(&transaction_ref);

        {
            let store =
                self.shared.store.lock().map_err(|_| {
                    ApplicationError::consistency_defect("runtime store lock poisoned")
                })?;
            if store.faults.contains(&FaultCase::RollbackFails) {
                return Err(ApplicationError::new(
                    ApplicationErrorKind::ConsistencyDefect,
                    "rollback failed; manual intervention required",
                ));
            }
        }

        let mut staged = self.shared.staged_by_tx.lock().map_err(|_| {
            ApplicationError::consistency_defect("staged transaction map lock poisoned")
        })?;
        staged.remove(&tx_key);
        Ok(())
    }
}

impl IdentityCursorAssignerPort for IdentityInMemoryRuntime {
    fn assign_truth_change_cursor(
        &self,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityTruthCursor, ApplicationError> {
        uow.assign_truth_change_cursor()
    }

    fn assign_reference_marker_cursor(
        &self,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityTruthCursor, ApplicationError> {
        uow.assign_reference_marker_cursor()
    }
}

impl IdentityClockPort for IdentityInMemoryRuntime {
    fn now(&self) -> Result<IdentityTimestamp, ApplicationError> {
        let next = self
            .shared
            .next_truth_cursor_id
            .fetch_add(1, Ordering::SeqCst) as i64;
        IdentityTimestamp::from_clock(next)
            .map_err(|error| ApplicationError::invalid_request(error.message))
    }
}

impl IdentityIdGeneratorPort for IdentityInMemoryRuntime {
    fn new_global_member_id(&self) -> Result<GlobalMemberId, ApplicationError> {
        let next = self
            .shared
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        GlobalMemberId::new(format!("member-{next}")).map_err(ApplicationError::from)
    }

    fn new_role_capability_summary_id(
        &self,
    ) -> Result<identity_contracts::refs::RoleCapabilitySummaryId, ApplicationError> {
        let next = self
            .shared
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        identity_contracts::refs::RoleCapabilitySummaryId::new(format!("summary-{next}"))
            .map_err(ApplicationError::from)
    }

    fn new_role_capability_source_snapshot_id(
        &self,
    ) -> Result<identity_contracts::refs::RoleCapabilitySourceSnapshotId, ApplicationError> {
        let next = self
            .shared
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        identity_contracts::refs::RoleCapabilitySourceSnapshotId::new(format!("snapshot-{next}"))
            .map_err(ApplicationError::from)
    }

    fn new_career_record_id(&self) -> Result<CareerRecordId, ApplicationError> {
        let next = self
            .shared
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        CareerRecordId::new(format!("career-{next}")).map_err(ApplicationError::from)
    }

    fn new_memory_reference_id(&self) -> Result<MemoryReferenceId, ApplicationError> {
        let next = self
            .shared
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        MemoryReferenceId::new(format!("memory-reference-{next}")).map_err(ApplicationError::from)
    }

    fn new_member_summary_view_id(
        &self,
    ) -> Result<identity_application::support::MemberSummaryViewId, ApplicationError> {
        let next = self
            .shared
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        Ok(identity_application::support::MemberSummaryViewId::new(
            format!("view-{next}"),
        ))
    }

    fn new_identity_trace_record_id(&self) -> Result<IdentityTraceRecordId, ApplicationError> {
        let next = self
            .shared
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        Ok(IdentityTraceRecordId::new(format!("trace-{next}")))
    }

    fn new_audit_trail_id(&self) -> Result<AuditTrailId, ApplicationError> {
        let next = self
            .shared
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        Ok(AuditTrailId::new(format!("audit-{next}")))
    }

    fn new_reconciliation_report_id(
        &self,
    ) -> Result<identity_contracts::refs::ReconciliationReportId, ApplicationError> {
        let next = self
            .shared
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        identity_contracts::refs::ReconciliationReportId::new(format!("report-{next}"))
            .map_err(ApplicationError::from)
    }

    fn new_reconciliation_finding_ref(
        &self,
    ) -> Result<identity_contracts::refs::ReconciliationFindingRef, ApplicationError> {
        let next = self
            .shared
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        Ok(identity_contracts::refs::ReconciliationFindingRef::new(
            identity_source_ref(IdentitySourceOwner::Identity, &format!("finding-{next}")),
        ))
    }

    fn new_identity_outbox_record_ref(&self) -> Result<IdentityOutboxRecordRef, ApplicationError> {
        let next = self
            .shared
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        Ok(IdentityOutboxRecordRef::new(format!("outbox-{next}")))
    }

    fn new_identity_outbox_payload_marker_ref(
        &self,
    ) -> Result<identity_contracts::refs::IdentityOutboxPayloadMarkerRef, ApplicationError> {
        let next = self
            .shared
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        Ok(
            identity_contracts::refs::IdentityOutboxPayloadMarkerRef::new(format!(
                "payload-{next}"
            )),
        )
    }

    fn new_trace_handoff_intent_ref(&self) -> Result<TraceHandoffIntentRef, ApplicationError> {
        let next = self
            .shared
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        Ok(TraceHandoffIntentRef::new(format!("handoff-{next}")))
    }

    fn new_handoff_receipt_ref(&self) -> Result<HandoffReceiptRef, ApplicationError> {
        let next = self
            .shared
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        Ok(HandoffReceiptRef::new(format!("handoff-receipt-{next}")))
    }

    fn new_handoff_issue_ref(&self) -> Result<HandoffIssueRef, ApplicationError> {
        let next = self
            .shared
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        Ok(HandoffIssueRef::new(identity_source_ref(
            IdentitySourceOwner::Identity,
            &format!("handoff-issue-{next}"),
        )))
    }

    fn new_identity_operation_context_ref(
        &self,
    ) -> Result<IdentityOperationContextRef, ApplicationError> {
        let next = self
            .shared
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        Ok(IdentityOperationContextRef::new(format!("context-{next}")))
    }

    fn new_identity_idempotency_record_ref(
        &self,
    ) -> Result<IdentityIdempotencyRecordRef, ApplicationError> {
        let next = self
            .shared
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        Ok(IdentityIdempotencyRecordRef::new(format!(
            "idem-record-{next}"
        )))
    }

    fn new_identity_stored_result_ref(
        &self,
    ) -> Result<identity_contracts::refs::IdentityStoredResultRef, ApplicationError> {
        let next = self
            .shared
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        Ok(identity_contracts::refs::IdentityStoredResultRef::new(
            format!("stored-result-{next}"),
        ))
    }

    fn new_identity_stored_surface_marker_ref(
        &self,
    ) -> Result<IdentityStoredSurfaceMarkerRef, ApplicationError> {
        let next = self
            .shared
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        Ok(IdentityStoredSurfaceMarkerRef::new(format!(
            "surface-{next}"
        )))
    }

    fn new_identity_consumer_receipt_ref(
        &self,
    ) -> Result<identity_contracts::refs::IdentityConsumerReceiptRef, ApplicationError> {
        let next = self
            .shared
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        Ok(identity_contracts::refs::IdentityConsumerReceiptRef::new(
            format!("consumer-receipt-{next}"),
        ))
    }

    fn new_identity_command_effect_summary_ref(
        &self,
    ) -> Result<IdentityCommandEffectSummaryRef, ApplicationError> {
        let next = self
            .shared
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        Ok(IdentityCommandEffectSummaryRef::new(format!(
            "effect-{next}"
        )))
    }

    fn new_identity_visibility_decision_ref(
        &self,
    ) -> Result<identity_contracts::refs::IdentityVisibilityDecisionRef, ApplicationError> {
        let next = self
            .shared
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        Ok(
            identity_contracts::refs::IdentityVisibilityDecisionRef::new(format!(
                "visibility-decision-{next}"
            )),
        )
    }

    fn new_identity_job_run_ref(&self) -> Result<IdentityJobRunRef, ApplicationError> {
        let next = self
            .shared
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        Ok(IdentityJobRunRef::new(format!("job-run-{next}")))
    }

    fn new_identity_job_report_ref(
        &self,
    ) -> Result<identity_contracts::refs::IdentityJobReportRef, ApplicationError> {
        let next = self
            .shared
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        Ok(identity_contracts::refs::IdentityJobReportRef::new(
            format!("job-report-{next}"),
        ))
    }

    fn new_identity_runtime_assembly_ref(
        &self,
    ) -> Result<identity_application::support::IdentityRuntimeAssemblyRef, ApplicationError> {
        let next = self
            .shared
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        Ok(
            identity_application::support::IdentityRuntimeAssemblyRef::new(format!(
                "runtime-{next}"
            )),
        )
    }

    fn new_identity_api_entry_ref(
        &self,
    ) -> Result<identity_application::support::IdentityApiEntryRef, ApplicationError> {
        let next = self
            .shared
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        Ok(identity_application::support::IdentityApiEntryRef::new(
            format!("api-entry-{next}"),
        ))
    }

    fn new_identity_entry_dispatch_ref(
        &self,
    ) -> Result<identity_application::support::IdentityEntryDispatchRef, ApplicationError> {
        let next = self
            .shared
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        Ok(
            identity_application::support::IdentityEntryDispatchRef::new(format!(
                "entry-dispatch-{next}"
            )),
        )
    }

    fn new_identity_worker_entry_ref(
        &self,
    ) -> Result<identity_application::support::IdentityWorkerEntryRef, ApplicationError> {
        let next = self
            .shared
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        Ok(identity_application::support::IdentityWorkerEntryRef::new(
            format!("worker-entry-{next}"),
        ))
    }

    fn new_identity_worker_dispatch_ref(
        &self,
    ) -> Result<identity_application::support::IdentityWorkerDispatchRef, ApplicationError> {
        let next = self
            .shared
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        Ok(
            identity_application::support::IdentityWorkerDispatchRef::new(format!(
                "worker-dispatch-{next}"
            )),
        )
    }

    fn new_identity_job_entry_ref(
        &self,
    ) -> Result<identity_application::support::IdentityJobEntryRef, ApplicationError> {
        let next = self
            .shared
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        Ok(identity_application::support::IdentityJobEntryRef::new(
            format!("job-entry-{next}"),
        ))
    }

    fn new_identity_job_dispatch_ref(
        &self,
    ) -> Result<identity_application::support::IdentityJobDispatchRef, ApplicationError> {
        let next = self
            .shared
            .next_transaction_id
            .fetch_add(1, Ordering::SeqCst);
        Ok(identity_application::support::IdentityJobDispatchRef::new(
            format!("job-dispatch-{next}"),
        ))
    }
}

impl IdentityOperationContextFactoryPort for IdentityInMemoryRuntime {
    fn from_command(
        &self,
        operation_name: IdentityOperationName,
        actor_ref: ActorRef,
        request_metadata_ref: IdentityRequestMetadataRef,
        idempotency_key: Option<IdentityIdempotencyKey>,
        request_digest: IdentityRequestDigest,
        trace_context_ref: Option<identity_contracts::refs::IdentityTraceContextRef>,
        context_ref: IdentityOperationContextRef,
        started_at: IdentityTimestamp,
    ) -> Result<IdentityOperationContext, ApplicationError> {
        Ok(IdentityOperationContext::from_command(
            context_ref,
            operation_name,
            actor_ref,
            request_metadata_ref,
            idempotency_key,
            request_digest,
            trace_context_ref,
            started_at,
        ))
    }

    fn from_query(
        &self,
        operation_name: IdentityOperationName,
        actor_ref: ActorRef,
        request_metadata_ref: IdentityRequestMetadataRef,
        request_digest: IdentityRequestDigest,
        trace_context_ref: Option<identity_contracts::refs::IdentityTraceContextRef>,
        context_ref: IdentityOperationContextRef,
        started_at: IdentityTimestamp,
    ) -> Result<IdentityOperationContext, ApplicationError> {
        Ok(IdentityOperationContext::from_query(
            context_ref,
            operation_name,
            actor_ref,
            request_metadata_ref,
            request_digest,
            trace_context_ref,
            started_at,
        ))
    }

    fn from_inbound_event(
        &self,
        operation_name: IdentityOperationName,
        actor_ref: ActorRef,
        request_metadata_ref: IdentityRequestMetadataRef,
        idempotency_key: IdentityIdempotencyKey,
        request_digest: IdentityRequestDigest,
        trace_context_ref: Option<identity_contracts::refs::IdentityTraceContextRef>,
        source_event_ref: identity_contracts::refs::IdentitySourceEventRef,
        context_ref: IdentityOperationContextRef,
        started_at: IdentityTimestamp,
    ) -> Result<IdentityOperationContext, ApplicationError> {
        Ok(IdentityOperationContext::from_inbound_event(
            context_ref,
            operation_name,
            actor_ref,
            request_metadata_ref,
            idempotency_key,
            request_digest,
            trace_context_ref,
            source_event_ref,
            started_at,
        ))
    }

    fn from_job(
        &self,
        operation_name: IdentityOperationName,
        actor_ref: ActorRef,
        request_metadata_ref: IdentityRequestMetadataRef,
        idempotency_key: IdentityIdempotencyKey,
        request_digest: IdentityRequestDigest,
        trace_context_ref: Option<identity_contracts::refs::IdentityTraceContextRef>,
        job_run_ref: IdentityJobRunRef,
        context_ref: IdentityOperationContextRef,
        started_at: IdentityTimestamp,
    ) -> Result<IdentityOperationContext, ApplicationError> {
        Ok(IdentityOperationContext::from_job(
            context_ref,
            operation_name,
            actor_ref,
            request_metadata_ref,
            idempotency_key,
            request_digest,
            trace_context_ref,
            job_run_ref,
            started_at,
        ))
    }

    fn from_handoff_callback(
        &self,
        operation_name: IdentityOperationName,
        actor_ref: ActorRef,
        request_metadata_ref: IdentityRequestMetadataRef,
        idempotency_key: IdentityIdempotencyKey,
        request_digest: IdentityRequestDigest,
        trace_context_ref: Option<identity_contracts::refs::IdentityTraceContextRef>,
        source_event_ref: identity_contracts::refs::IdentitySourceEventRef,
        context_ref: IdentityOperationContextRef,
        started_at: IdentityTimestamp,
    ) -> Result<IdentityOperationContext, ApplicationError> {
        Ok(IdentityOperationContext::from_handoff_callback(
            context_ref,
            operation_name,
            actor_ref,
            request_metadata_ref,
            idempotency_key,
            request_digest,
            trace_context_ref,
            source_event_ref,
            started_at,
        ))
    }
}

impl GlobalMemberRepository for IdentityInMemoryRuntime {
    fn get_member_with_version(
        &self,
        member_ref: GlobalMemberRef,
    ) -> Result<Option<Versioned<GlobalMember>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .members
            .get(&member_key(&member_ref))
            .map(|stored| Versioned {
                value: stored.member.clone(),
                version: stored.version,
            }))
    }

    fn get_anchor_state(
        &self,
        member_ref: GlobalMemberRef,
    ) -> Result<Option<identity_domain::member_identity::IdentityAnchorState>, ApplicationError>
    {
        Ok(self
            .get_member_with_version(member_ref)?
            .map(|versioned| versioned.value.anchor_state))
    }

    fn list_members(
        &self,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<GlobalMemberRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let mut items: Vec<_> = store
            .members
            .values()
            .map(|stored| IdentityVersionedRef {
                value_ref: stored.member.member_ref.clone(),
                version: stored.version,
            })
            .collect();
        items.sort_by(|left, right| {
            left.value_ref
                .id()
                .as_str()
                .cmp(right.value_ref.id().as_str())
        });
        let (items, next_cursor) = paged(items, page, "member");
        Ok(Page { items, next_cursor })
    }

    fn save_member(
        &self,
        member: GlobalMember,
        expected_version: Option<IdentityVersion>,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<GlobalMemberRef>, ApplicationError> {
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveMember {
                member: member.clone(),
                expected_version,
            },
        )?;
        Ok(IdentityVersionedRef {
            value_ref: member.member_ref.clone(),
            version: self.predicted_member_version(&member.member_ref)?,
        })
    }
}

impl GlobalLifecycleRepository for IdentityInMemoryRuntime {
    fn get_lifecycle_with_version(
        &self,
        member_ref: GlobalMemberRef,
    ) -> Result<Option<Versioned<GlobalLifecycleState>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .lifecycles
            .get(&member_key(&member_ref))
            .map(|stored| Versioned {
                value: stored.lifecycle.clone(),
                version: stored.version,
            }))
    }

    fn list_lifecycles(
        &self,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<GlobalMemberRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let mut items: Vec<_> = store
            .lifecycles
            .values()
            .map(|stored| IdentityVersionedRef {
                value_ref: stored.member_ref.clone(),
                version: stored.version,
            })
            .collect();
        items.sort_by(|left, right| {
            left.value_ref
                .id()
                .as_str()
                .cmp(right.value_ref.id().as_str())
        });
        let (items, next_cursor) = paged(items, page, "lifecycle");
        Ok(Page { items, next_cursor })
    }

    fn save_lifecycle(
        &self,
        member_ref: GlobalMemberRef,
        lifecycle_state: GlobalLifecycleState,
        expected_version: Option<IdentityVersion>,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<GlobalMemberRef>, ApplicationError> {
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveLifecycle {
                member_ref: member_ref.clone(),
                lifecycle: lifecycle_state,
                expected_version,
            },
        )?;
        Ok(IdentityVersionedRef {
            value_ref: member_ref.clone(),
            version: self.predicted_lifecycle_version(&member_ref)?,
        })
    }
}

impl RoleCapabilityRepository for IdentityInMemoryRuntime {
    fn get_summary_with_version(
        &self,
        summary_ref: RoleCapabilitySummaryRef,
    ) -> Result<Option<Versioned<RoleCapabilitySummary>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .role_capability_summaries
            .get(&role_capability_summary_key(&summary_ref))
            .map(|stored| Versioned {
                value: stored.summary.clone(),
                version: stored.version,
            }))
    }

    fn find_current_summary_by_member(
        &self,
        member_ref: GlobalMemberRef,
    ) -> Result<Option<Versioned<RoleCapabilitySummary>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let Some(summary_key) = store
            .role_capability_summary_by_member
            .get(&member_key(&member_ref))
        else {
            return Ok(None);
        };
        Ok(store
            .role_capability_summaries
            .get(summary_key)
            .map(|stored| Versioned {
                value: stored.summary.clone(),
                version: stored.version,
            }))
    }

    fn list_summaries_by_member(
        &self,
        member_ref: GlobalMemberRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<RoleCapabilitySummaryRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let keys = store
            .role_capability_summaries_by_member
            .get(&member_key(&member_ref))
            .cloned()
            .unwrap_or_default();
        let mut items: Vec<_> = keys
            .into_iter()
            .filter_map(|key| store.role_capability_summaries.get(&key))
            .map(|stored| IdentityVersionedRef {
                value_ref: stored.summary.summary_ref.clone(),
                version: stored.version,
            })
            .collect();
        items.sort_by(|left, right| {
            left.value_ref
                .summary_id
                .as_str()
                .cmp(right.value_ref.summary_id.as_str())
        });
        let (items, next_cursor) = paged(items, page, "role-summary");
        Ok(Page { items, next_cursor })
    }

    fn get_source_snapshot_with_version(
        &self,
        snapshot_ref: RoleCapabilitySourceSnapshotRef,
    ) -> Result<Option<Versioned<RoleCapabilitySourceSnapshot>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .role_capability_source_snapshots
            .get(&role_capability_snapshot_key(&snapshot_ref))
            .map(|stored| Versioned {
                value: stored.snapshot.clone(),
                version: stored.version,
            }))
    }

    fn find_source_snapshot_by_source(
        &self,
        source_ref: RoleCapabilitySourceRef,
    ) -> Result<Option<Versioned<RoleCapabilitySourceSnapshot>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let Some(snapshot_key) = store
            .role_capability_snapshot_by_source
            .get(&role_capability_source_key(&source_ref))
        else {
            return Ok(None);
        };
        Ok(store
            .role_capability_source_snapshots
            .get(snapshot_key)
            .map(|stored| Versioned {
                value: stored.snapshot.clone(),
                version: stored.version,
            }))
    }

    fn save_source_snapshot(
        &self,
        snapshot: RoleCapabilitySourceSnapshot,
        expected_version: Option<IdentityVersion>,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<RoleCapabilitySourceSnapshotRef>, ApplicationError> {
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveRoleCapabilitySourceSnapshot {
                snapshot: snapshot.clone(),
                expected_version,
            },
        )?;
        Ok(IdentityVersionedRef {
            value_ref: snapshot.snapshot_ref.clone(),
            version: self.predicted_role_capability_snapshot_version(&snapshot.snapshot_ref)?,
        })
    }

    fn save_summary(
        &self,
        summary: RoleCapabilitySummary,
        expected_version: Option<IdentityVersion>,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<RoleCapabilitySummaryRef>, ApplicationError> {
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveRoleCapabilitySummary {
                summary: summary.clone(),
                expected_version,
            },
        )?;
        Ok(IdentityVersionedRef {
            value_ref: summary.summary_ref.clone(),
            version: self.predicted_role_capability_summary_version(&summary.summary_ref)?,
        })
    }
}

impl CareerRecordRepository for IdentityInMemoryRuntime {
    fn get_career_record(
        &self,
        record_ref: CareerRecordRef,
    ) -> Result<Option<Versioned<CareerRecord>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .career_records
            .get(&career_record_key(&record_ref))
            .map(|stored| Versioned {
                value: stored.record.clone(),
                version: stored.version,
            }))
    }

    fn list_records_by_member(
        &self,
        member_ref: GlobalMemberRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<CareerRecordRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let keys = store
            .career_records_by_member
            .get(&member_key(&member_ref))
            .cloned()
            .unwrap_or_default();
        let mut items: Vec<_> = keys
            .into_iter()
            .filter_map(|key| store.career_records.get(&key))
            .map(|stored| IdentityVersionedRef {
                value_ref: stored.record.career_record_ref.clone(),
                version: stored.version,
            })
            .collect();
        items.sort_by(|left, right| {
            left.value_ref
                .record_id
                .as_str()
                .cmp(right.value_ref.record_id.as_str())
        });
        let (items, next_cursor) = paged(items, page, "career-member");
        Ok(Page { items, next_cursor })
    }

    fn find_records_by_source_marker(
        &self,
        source_marker_ref: CareerSourceMarkerRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<CareerRecordRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let mut items: Vec<_> = store
            .career_records_by_source_marker
            .get(&career_source_marker_key(&source_marker_ref))
            .and_then(|key| store.career_records.get(key))
            .map(|stored| {
                vec![IdentityVersionedRef {
                    value_ref: stored.record.career_record_ref.clone(),
                    version: stored.version,
                }]
            })
            .unwrap_or_default();
        items.sort_by(|left, right| {
            left.value_ref
                .record_id
                .as_str()
                .cmp(right.value_ref.record_id.as_str())
        });
        let (items, next_cursor) = paged(items, page, "career-source");
        Ok(Page { items, next_cursor })
    }

    fn find_duplicate_source_record(
        &self,
        source_marker_ref: CareerSourceMarkerRef,
    ) -> Result<Option<CareerRecordRef>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .career_records_by_source_marker
            .get(&career_source_marker_key(&source_marker_ref))
            .and_then(|key| store.career_records.get(key))
            .map(|stored| stored.record.career_record_ref.clone()))
    }

    fn list_corrections_for_record(
        &self,
        original_record_ref: CareerRecordRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<CareerRecordRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let keys = store
            .career_corrections_by_original
            .get(&career_record_key(&original_record_ref))
            .cloned()
            .unwrap_or_default();
        let mut items: Vec<_> = keys
            .into_iter()
            .filter_map(|key| store.career_records.get(&key))
            .map(|stored| IdentityVersionedRef {
                value_ref: stored.record.career_record_ref.clone(),
                version: stored.version,
            })
            .collect();
        items.sort_by(|left, right| {
            left.value_ref
                .record_id
                .as_str()
                .cmp(right.value_ref.record_id.as_str())
        });
        let (items, next_cursor) = paged(items, page, "career-correction");
        Ok(Page { items, next_cursor })
    }

    fn append_career_record(
        &self,
        record: CareerRecord,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<CareerRecordRef>, ApplicationError> {
        self.stage(
            &uow.transaction_ref(),
            StagedOp::AppendCareerRecord {
                record: record.clone(),
            },
        )?;
        Ok(IdentityVersionedRef {
            value_ref: record.career_record_ref.clone(),
            version: self.predicted_career_record_version(&record.career_record_ref)?,
        })
    }

    fn save_career_record_state(
        &self,
        record: CareerRecord,
        expected_version: IdentityVersion,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<CareerRecordRef>, ApplicationError> {
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveCareerRecordState {
                record: record.clone(),
                expected_version,
            },
        )?;
        Ok(IdentityVersionedRef {
            value_ref: record.career_record_ref.clone(),
            version: IdentityVersion::new(expected_version.get() + 1),
        })
    }
}

impl MemoryReferenceRepository for IdentityInMemoryRuntime {
    fn get_memory_reference_with_version(
        &self,
        reference_ref: MemoryReferenceRef,
    ) -> Result<Option<Versioned<MemoryReference>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .memory_references
            .get(&memory_reference_key(&reference_ref))
            .map(|stored| Versioned {
                value: stored.reference.clone(),
                version: stored.version,
            }))
    }

    fn list_references_by_member(
        &self,
        member_ref: GlobalMemberRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<MemoryReferenceRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let keys = store
            .memory_references_by_member
            .get(&member_key(&member_ref))
            .cloned()
            .unwrap_or_default();
        let mut items: Vec<_> = keys
            .into_iter()
            .filter_map(|key| store.memory_references.get(&key))
            .map(|stored| IdentityVersionedRef {
                value_ref: stored.reference.memory_reference_ref.clone(),
                version: stored.version,
            })
            .collect();
        items.sort_by(|left, right| {
            left.value_ref
                .reference_id
                .as_str()
                .cmp(right.value_ref.reference_id.as_str())
        });
        let (items, next_cursor) = paged(items, page, "memory-member");
        Ok(Page { items, next_cursor })
    }

    fn find_reference_by_memory(
        &self,
        member_ref: GlobalMemberRef,
        memory_ref: MemoryRef,
    ) -> Result<Option<Versioned<MemoryReference>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let Some(reference_key) =
            store
                .memory_reference_by_memory
                .get(&memory_reference_member_memory_key(
                    &member_ref,
                    &memory_ref,
                ))
        else {
            return Ok(None);
        };
        Ok(store
            .memory_references
            .get(reference_key)
            .map(|stored| Versioned {
                value: stored.reference.clone(),
                version: stored.version,
            }))
    }

    fn find_reference_by_archive(
        &self,
        member_ref: GlobalMemberRef,
        archive_ref: ArchiveRef,
    ) -> Result<Option<Versioned<MemoryReference>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let Some(reference_key) =
            store
                .memory_reference_by_archive
                .get(&memory_reference_member_archive_key(
                    &member_ref,
                    &archive_ref,
                ))
        else {
            return Ok(None);
        };
        Ok(store
            .memory_references
            .get(reference_key)
            .map(|stored| Versioned {
                value: stored.reference.clone(),
                version: stored.version,
            }))
    }

    fn find_reference_by_handoff(
        &self,
        handoff_ref: ArchiveHandoffRef,
    ) -> Result<Option<Versioned<MemoryReference>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let Some(reference_key) = store
            .memory_reference_by_handoff
            .get(&archive_handoff_key(&handoff_ref))
        else {
            return Ok(None);
        };
        Ok(store
            .memory_references
            .get(reference_key)
            .map(|stored| Versioned {
                value: stored.reference.clone(),
                version: stored.version,
            }))
    }

    fn find_callback_target_by_handoff(
        &self,
        handoff_ref: ArchiveHandoffRef,
    ) -> Result<Option<MemoryReferenceRef>, ApplicationError> {
        Ok(self
            .find_reference_by_handoff(handoff_ref)?
            .map(|versioned| versioned.value.memory_reference_ref))
    }

    fn save_memory_reference(
        &self,
        reference: MemoryReference,
        expected_version: Option<IdentityVersion>,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<MemoryReferenceRef>, ApplicationError> {
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveMemoryReference {
                reference: reference.clone(),
                expected_version,
            },
        )?;
        Ok(IdentityVersionedRef {
            value_ref: reference.memory_reference_ref.clone(),
            version: self.predicted_memory_reference_version(&reference.memory_reference_ref)?,
        })
    }
}

impl IdentityTraceRecordRepository for IdentityInMemoryRuntime {
    fn get_trace_record(
        &self,
        trace_record_ref: IdentityTraceRecordRef,
    ) -> Result<Option<Versioned<IdentityTraceRecord>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .trace_records
            .get(trace_record_ref.as_str())
            .map(|stored| Versioned {
                value: stored.trace.clone(),
                version: stored.version,
            }))
    }

    fn list_trace_records_by_member(
        &self,
        member_ref: GlobalMemberRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<IdentityTraceRecordRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let keys = store
            .trace_member_index
            .get(&member_key(&member_ref))
            .cloned()
            .unwrap_or_default();
        let mut items: Vec<_> = keys
            .into_iter()
            .map(|key| IdentityVersionedRef {
                value_ref: IdentityTraceRecordRef::new(key.clone()),
                version: store
                    .trace_records
                    .get(&key)
                    .map(|stored| stored.version)
                    .unwrap_or(IdentityVersion::new(1)),
            })
            .collect();
        items.sort_by(|left, right| left.value_ref.as_str().cmp(right.value_ref.as_str()));
        let (items, next_cursor) = paged(items, page, "trace-member");
        Ok(Page { items, next_cursor })
    }

    fn list_trace_records_by_subject(
        &self,
        subject_ref: IdentityTraceSubjectRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<IdentityTraceRecordRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let keys = store
            .trace_subject_index
            .get(subject_ref.as_str())
            .cloned()
            .unwrap_or_default();
        let mut items: Vec<_> = keys
            .into_iter()
            .map(|key| IdentityVersionedRef {
                value_ref: IdentityTraceRecordRef::new(key.clone()),
                version: store
                    .trace_records
                    .get(&key)
                    .map(|stored| stored.version)
                    .unwrap_or(IdentityVersion::new(1)),
            })
            .collect();
        items.sort_by(|left, right| left.value_ref.as_str().cmp(right.value_ref.as_str()));
        let (items, next_cursor) = paged(items, page, "trace-subject");
        Ok(Page { items, next_cursor })
    }

    fn list_trace_records_after_cursor(
        &self,
        subject_ref: IdentityTraceSubjectRef,
        after_cursor: Option<IdentityTruthCursor>,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<IdentityTraceRecordRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let keys = store
            .trace_subject_index
            .get(subject_ref.as_str())
            .cloned()
            .unwrap_or_default();
        let mut items: Vec<_> = keys
            .into_iter()
            .filter_map(|key| store.trace_records.get(&key))
            .filter(|stored| {
                after_cursor
                    .as_ref()
                    .map(|cursor| stored.trace.source_cursor_ref.as_str() > cursor.as_str())
                    .unwrap_or(true)
            })
            .map(|stored| IdentityVersionedRef {
                value_ref: stored.trace.trace_record_ref.clone(),
                version: stored.version,
            })
            .collect();
        items.sort_by(|left, right| left.value_ref.as_str().cmp(right.value_ref.as_str()));
        let (items, next_cursor) = paged(items, page, "trace-cursor");
        Ok(Page { items, next_cursor })
    }

    fn list_trace_records_by_change_kind(
        &self,
        member_ref: GlobalMemberRef,
        change_kind_ref: IdentityChangeKindRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<IdentityTraceRecordRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let keys = store
            .trace_member_change_kind_index
            .get(&trace_member_change_kind_key(&member_ref, &change_kind_ref))
            .cloned()
            .unwrap_or_default();
        let mut items: Vec<_> = keys
            .into_iter()
            .map(|key| IdentityVersionedRef {
                value_ref: IdentityTraceRecordRef::new(key.clone()),
                version: store
                    .trace_records
                    .get(&key)
                    .map(|stored| stored.version)
                    .unwrap_or(IdentityVersion::new(1)),
            })
            .collect();
        items.sort_by(|left, right| left.value_ref.as_str().cmp(right.value_ref.as_str()));
        let (items, next_cursor) = paged(items, page, "trace-kind");
        Ok(Page { items, next_cursor })
    }

    fn append_trace_record(
        &self,
        trace_record: IdentityTraceRecord,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<IdentityTraceRecordRef>, ApplicationError> {
        self.stage(
            &uow.transaction_ref(),
            StagedOp::AppendTraceRecord {
                trace_record: trace_record.clone(),
            },
        )?;
        Ok(IdentityVersionedRef {
            value_ref: trace_record.trace_record_ref.clone(),
            version: self.predicted_trace_version(&trace_record.trace_record_ref)?,
        })
    }

    fn mark_trace_superseded_by_correction(
        &self,
        trace_record: IdentityTraceRecord,
        expected_version: IdentityVersion,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<IdentityTraceRecordRef>, ApplicationError> {
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveTraceRecordState {
                trace_record: trace_record.clone(),
                expected_version,
            },
        )?;
        Ok(IdentityVersionedRef {
            value_ref: trace_record.trace_record_ref.clone(),
            version: IdentityVersion::new(expected_version.get() + 1),
        })
    }
}

impl IdentityAuditTrailRepository for IdentityInMemoryRuntime {
    fn get_audit_trail_with_version(
        &self,
        audit_trail_ref: AuditTrailRef,
    ) -> Result<Option<Versioned<AuditTrail>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .audit_trails
            .get(audit_trail_ref.as_str())
            .map(|stored| Versioned {
                value: stored.trail.clone(),
                version: stored.version,
            }))
    }

    fn find_audit_trail_by_subject(
        &self,
        audit_subject_ref: IdentityAuditSubjectRef,
    ) -> Result<Option<Versioned<AuditTrail>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let Some(key) = store.audit_subject_index.get(audit_subject_ref.as_str()) else {
            return Ok(None);
        };
        Ok(store.audit_trails.get(key).map(|stored| Versioned {
            value: stored.trail.clone(),
            version: stored.version,
        }))
    }

    fn list_audit_entries(
        &self,
        audit_trail_ref: AuditTrailRef,
        audit_scope_ref: AuditScopeRef,
        _cursor_ref: Option<AuditCursorRef>,
        page: IdentityRepositoryPage,
    ) -> Result<Page<AuditTrailEntry>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let Some(stored) = store.audit_trails.get(audit_trail_ref.as_str()) else {
            return Ok(Page {
                items: Vec::new(),
                next_cursor: None,
            });
        };
        let filtered = stored.trail.filter_by_scope(&audit_scope_ref).entries;
        let (items, next_cursor) = paged(filtered, page, "audit-entry");
        Ok(Page { items, next_cursor })
    }

    fn save_audit_trail(
        &self,
        audit_trail: AuditTrail,
        expected_version: Option<IdentityVersion>,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<AuditTrailRef>, ApplicationError> {
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveAuditTrail {
                trail: audit_trail.clone(),
                expected_version,
            },
        )?;
        Ok(IdentityVersionedRef {
            value_ref: audit_trail.audit_trail_ref.clone(),
            version: self.predicted_audit_version(&audit_trail.audit_trail_ref)?,
        })
    }

    fn append_audit_entry(
        &self,
        audit_trail_ref: AuditTrailRef,
        entry: AuditTrailEntry,
        expected_version: IdentityVersion,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<AuditTrailRef>, ApplicationError> {
        self.stage(
            &uow.transaction_ref(),
            StagedOp::AppendAuditEntry {
                audit_trail_ref: audit_trail_ref.clone(),
                entry,
                expected_version,
            },
        )?;
        Ok(IdentityVersionedRef {
            value_ref: audit_trail_ref,
            version: IdentityVersion::new(expected_version.get() + 1),
        })
    }
}

impl IdentityTruthChangeSubjectMapper for IdentityInMemoryRuntime {
    fn member_subjects(&self, member_ref: GlobalMemberRef) -> IdentityAcceptedSubjectRefs {
        DefaultIdentityTruthChangeSubjectMapper.member_subjects(member_ref)
    }

    fn role_capability_subjects(
        &self,
        summary_ref: identity_contracts::refs::RoleCapabilitySummaryRef,
    ) -> IdentityAcceptedSubjectRefs {
        DefaultIdentityTruthChangeSubjectMapper.role_capability_subjects(summary_ref)
    }

    fn role_capability_source_snapshot_subjects(
        &self,
        snapshot_ref: identity_contracts::refs::RoleCapabilitySourceSnapshotRef,
    ) -> IdentityAcceptedSubjectRefs {
        DefaultIdentityTruthChangeSubjectMapper
            .role_capability_source_snapshot_subjects(snapshot_ref)
    }

    fn career_record_subjects(
        &self,
        record_ref: identity_contracts::refs::CareerRecordRef,
    ) -> IdentityAcceptedSubjectRefs {
        DefaultIdentityTruthChangeSubjectMapper.career_record_subjects(record_ref)
    }

    fn memory_reference_subjects(
        &self,
        reference_ref: identity_contracts::refs::MemoryReferenceRef,
    ) -> IdentityAcceptedSubjectRefs {
        DefaultIdentityTruthChangeSubjectMapper.memory_reference_subjects(reference_ref)
    }

    fn outbox_record_subjects(
        &self,
        outbox_ref: IdentityOutboxRecordRef,
    ) -> IdentityAcceptedSubjectRefs {
        DefaultIdentityTruthChangeSubjectMapper.outbox_record_subjects(outbox_ref)
    }

    fn handoff_intent_subjects(
        &self,
        intent_ref: TraceHandoffIntentRef,
    ) -> IdentityAcceptedSubjectRefs {
        DefaultIdentityTruthChangeSubjectMapper.handoff_intent_subjects(intent_ref)
    }
}

impl IdentityAcceptedAuditTrailMarkerMapper for IdentityInMemoryRuntime {
    fn accepted_command_audit_markers(
        &self,
        context: &IdentityOperationContext,
        subjects: &IdentityAcceptedSubjectRefs,
        change_kind_ref: &IdentityChangeKindRef,
        source_cursor_ref: &IdentityTruthCursor,
    ) -> IdentityAcceptedAuditTrailMarkers {
        DefaultIdentityAcceptedAuditTrailMarkerMapper.accepted_command_audit_markers(
            context,
            subjects,
            change_kind_ref,
            source_cursor_ref,
        )
    }
}

impl IdentityMarkerSubjectMapper for IdentityInMemoryRuntime {
    fn source_marker_subject(
        &self,
        source_ref: identity_contracts::refs::IdentitySourceRef,
    ) -> identity_contracts::refs::IdentityTraceSubjectRef {
        DefaultIdentityMarkerSubjectMapper.source_marker_subject(source_ref)
    }

    fn external_reference_marker_subject(
        &self,
        reference_ref: ExternalReferenceRef,
    ) -> identity_contracts::refs::IdentityTraceSubjectRef {
        DefaultIdentityMarkerSubjectMapper.external_reference_marker_subject(reference_ref)
    }

    fn projection_marker_subject(
        &self,
        projection_ref: IdentityProjectionRef,
    ) -> identity_contracts::refs::IdentityTraceSubjectRef {
        DefaultIdentityMarkerSubjectMapper.projection_marker_subject(projection_ref)
    }

    fn job_marker_subject(
        &self,
        job_run_ref: IdentityJobRunRef,
    ) -> identity_contracts::refs::IdentityTraceSubjectRef {
        DefaultIdentityMarkerSubjectMapper.job_marker_subject(job_run_ref)
    }

    fn handoff_receipt_marker_subject(
        &self,
        receipt_ref: HandoffReceiptRef,
    ) -> identity_contracts::refs::IdentityTraceSubjectRef {
        DefaultIdentityMarkerSubjectMapper.handoff_receipt_marker_subject(receipt_ref)
    }
}

impl IdentityExternalSourceResolverPort for IdentityInMemoryRuntime {
    fn resolve_governance_basis(
        &self,
        basis_ref: GovernanceBasisRef,
        risk_ref: Option<LifecycleRiskRef>,
    ) -> Result<GovernanceBasisSummary, ApplicationError> {
        let state = if basis_ref.external_ref.as_str().contains("unavailable") {
            GovernanceBasisState::Unavailable
        } else if basis_ref.external_ref.as_str().contains("stale") {
            GovernanceBasisState::Stale
        } else if basis_ref.external_ref.as_str().contains("invalid") {
            GovernanceBasisState::InvalidForAction
        } else if basis_ref.external_ref.as_str().contains("missing") {
            GovernanceBasisState::NotFound
        } else {
            GovernanceBasisState::Valid
        };
        Ok(GovernanceBasisSummary::from_resolver(
            basis_ref, state, risk_ref,
        ))
    }

    fn resolve_role_capability_source(
        &self,
        source_ref: identity_contracts::refs::RoleCapabilitySourceRef,
    ) -> Result<identity_application::ports::RoleCapabilitySourceResolution, ApplicationError> {
        let evidence_ref = identity_contracts::refs::CapabilityEvidenceRef::new(
            identity_contracts::refs::CapabilityEvidenceKind::MethodArtifact,
            source_ref.source_ref.clone(),
        )?;
        Ok(
            identity_application::ports::RoleCapabilitySourceResolution {
                source_ref: source_ref.clone(),
                source_state:
                    identity_domain::role_capability::RoleCapabilitySourceStateKind::SourceResolved,
                source_version_ref: Some(
                    identity_contracts::refs::RoleCapabilitySourceVersionRef::new(
                        source_ref.clone(),
                        "v1",
                    )?,
                ),
                safe_summary_ref: Some(
                    identity_contracts::refs::RoleCapabilitySafeSummaryRef::new(
                        source_ref.clone(),
                        "safe-summary-1",
                    )?,
                ),
                evidence_refs: vec![evidence_ref],
                material_marker: identity_contracts::refs::RoleCapabilityChangeMaterialMarker::new(
                    identity_contracts::refs::RoleCapabilityChangeMaterialKind::SafeSummaryMarker,
                    Some(source_ref.source_ref.clone()),
                ),
            },
        )
    }

    fn resolve_capability_evidence(
        &self,
        evidence_ref: identity_contracts::refs::CapabilityEvidenceRef,
    ) -> Result<identity_application::ports::CapabilityEvidenceResolution, ApplicationError> {
        Ok(identity_application::ports::CapabilityEvidenceResolution {
            evidence_ref: evidence_ref.clone(),
            evidence_state: ReferenceResolutionStateKind::Resolved,
            safe_summary_ref: Some(
                identity_contracts::refs::ExternalReferenceSafeSummaryRef::new(
                    ExternalReferenceRef::new(
                        ExternalReferenceKind::MethodSource,
                        evidence_ref.source_ref.clone(),
                    ),
                    evidence_ref.source_ref.clone(),
                ),
            ),
            source_version_ref: Some(ExternalSourceVersionRef::new(
                evidence_ref.source_ref.clone(),
            )),
        })
    }

    fn resolve_work_participation(
        &self,
        source_ref: identity_contracts::refs::WorkSourceRef,
    ) -> Result<identity_contracts::refs::WorkParticipationSourceSummary, ApplicationError> {
        let source_token = source_ref.source_ref.external_ref.as_str();
        let member_token = source_token
            .split("::")
            .next()
            .filter(|value| !value.is_empty())
            .unwrap_or("member-1");
        let marker_token = format!("marker-{source_token}");
        let safe_summary_token = format!("safe-{source_token}");
        let state = if source_ref
            .source_ref
            .external_ref
            .as_str()
            .contains("unavailable")
        {
            WorkParticipationSourceState::Unavailable
        } else if source_ref
            .source_ref
            .external_ref
            .as_str()
            .contains("unresolved")
        {
            WorkParticipationSourceState::Unresolved
        } else if source_ref
            .source_ref
            .external_ref
            .as_str()
            .contains("untrusted")
        {
            WorkParticipationSourceState::Untrusted
        } else if source_ref.is_pending_review_marker()
            || source_ref
                .source_ref
                .external_ref
                .as_str()
                .contains("pending-review")
        {
            WorkParticipationSourceState::PendingReview
        } else {
            WorkParticipationSourceState::Trusted
        };
        let project_participation_ref =
            ProjectParticipationRef::from_work_source(source_ref.source_ref.clone())?;
        let member_ref = GlobalMemberRef::from_id(GlobalMemberId::new(member_token.to_owned())?);
        let source_marker_ref =
            CareerSourceMarkerRef::new(member_ref, source_ref.clone(), marker_token)?;
        let safe_summary_ref = match state {
            WorkParticipationSourceState::Trusted | WorkParticipationSourceState::PendingReview => {
                Some(identity_contracts::refs::CareerSafeSummaryRef::new(
                    source_ref.clone(),
                    safe_summary_token,
                )?)
            }
            _ => None,
        };
        Ok(WorkParticipationSourceSummary::from_resolver(
            project_participation_ref,
            source_ref,
            source_marker_ref,
            safe_summary_ref,
            state,
        ))
    }

    fn resolve_memory_reference_source(
        &self,
        source_ref: identity_contracts::refs::MemoryReferenceSourceRef,
    ) -> Result<identity_contracts::refs::MemoryReferenceSourceSummary, ApplicationError> {
        let token = source_ref.source_ref.external_ref.as_str();
        let memory_ref = if token.contains("archive-only") {
            None
        } else {
            Some(MemoryRef::from_source(identity_source_ref(
                IdentitySourceOwner::MemoryArchive,
                &format!("memory-{token}"),
            ))?)
        };
        let archive_ref = if token.contains("archive") || source_ref.is_handoff_result() {
            Some(ArchiveRef::from_source(identity_source_ref(
                IdentitySourceOwner::MemoryArchive,
                &format!("archive-{token}"),
            ))?)
        } else {
            None
        };
        let safe_summary_ref = Some(identity_contracts::refs::MemorySafeSummaryRef::new(
            source_ref.clone(),
            format!("safe-{token}"),
        )?);
        let state = if token.contains("stale") {
            MemoryReferenceSourceState::Stale
        } else if token.contains("unavailable") {
            MemoryReferenceSourceState::Unavailable
        } else if token.contains("pending") {
            MemoryReferenceSourceState::PendingVerification
        } else if token.contains("untrusted") {
            MemoryReferenceSourceState::Untrusted
        } else {
            MemoryReferenceSourceState::Trusted
        };
        Ok(
            identity_contracts::refs::MemoryReferenceSourceSummary::from_resolver(
                source_ref,
                memory_ref,
                archive_ref,
                None,
                safe_summary_ref,
                state,
            ),
        )
    }

    fn resolve_archive_handoff_source(
        &self,
        handoff_ref: identity_contracts::refs::ArchiveHandoffRef,
    ) -> Result<identity_contracts::refs::MemoryReferenceSourceSummary, ApplicationError> {
        let source_ref = identity_contracts::refs::MemoryReferenceSourceRef::new(
            identity_contracts::refs::MemoryReferenceSourceKind::ArchiveHandoffResult,
            handoff_ref.source_ref.clone(),
        )?;
        let archive_ref = if handoff_ref
            .source_ref
            .external_ref
            .as_str()
            .contains("failed")
        {
            None
        } else {
            Some(ArchiveRef::from_source(identity_source_ref(
                IdentitySourceOwner::MemoryArchive,
                &format!("archive-{}", handoff_ref.handoff_token),
            ))?)
        };
        let state = if handoff_ref
            .source_ref
            .external_ref
            .as_str()
            .contains("failed")
        {
            MemoryReferenceSourceState::HandoffResultFailed
        } else {
            MemoryReferenceSourceState::HandoffResultAccepted
        };
        Ok(
            identity_contracts::refs::MemoryReferenceSourceSummary::from_resolver(
                source_ref,
                None,
                archive_ref,
                Some(handoff_ref),
                None,
                state,
            ),
        )
    }
}

impl IdentityProjectionRepository for IdentityInMemoryRuntime {
    fn find_member_summary_view_ref(
        &self,
        member_ref: GlobalMemberRef,
        visibility_scope_ref: VisibilityScopeRef,
    ) -> Result<Option<MemberSummaryViewRef>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .member_scope_index
            .get(&member_scope_key(&member_ref, &visibility_scope_ref))
            .cloned()
            .map(MemberSummaryViewRef::new))
    }

    fn get_member_summary_view(
        &self,
        view_ref: MemberSummaryViewRef,
    ) -> Result<Option<MemberSummaryView>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .member_summary_views
            .get(view_ref.as_str())
            .map(|stored| stored.view.clone()))
    }

    fn get_projection_state_with_version(
        &self,
        projection_ref: IdentityProjectionRef,
    ) -> Result<Option<Versioned<ProjectionState>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .projection_states
            .get(&projection_key(&projection_ref))
            .map(|stored| Versioned {
                value: stored.state.clone(),
                version: stored.version,
            }))
    }

    fn find_projection_state_ref(
        &self,
        projection_ref: IdentityProjectionRef,
    ) -> Result<Option<ProjectionStateRef>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .projection_states
            .get(&projection_key(&projection_ref))
            .map(|stored| stored.state.projection_state_ref.clone()))
    }

    fn list_projection_states(
        &self,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<ProjectionStateRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(project_projection_page(
            store.projection_states.values().collect(),
            page,
            |_| true,
        ))
    }

    fn list_stale_projection_states(
        &self,
        maintenance_scope_ref: identity_contracts::refs::MaintenanceScopeRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<ProjectionStateRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(project_projection_page(
            store.projection_states.values().collect(),
            page,
            |stored| {
                stored.state.state_kind == ProjectionStateKind::Stale
                    && stored.state.maintenance_scope_ref.as_ref() == Some(&maintenance_scope_ref)
            },
        ))
    }

    fn get_projection_source_cursor(
        &self,
        projection_ref: IdentityProjectionRef,
    ) -> Result<Option<IdentityProjectionCursorRef>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .projection_states
            .get(&projection_key(&projection_ref))
            .and_then(|stored| stored.state.source_cursor_ref.clone()))
    }

    fn get_member_summary_rebuild_plan(
        &self,
        projection_ref: IdentityProjectionRef,
    ) -> Result<Option<MemberSummaryProjectionRebuildPlan>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .member_summary_rebuild_plans
            .get(&projection_key(&projection_ref))
            .cloned())
    }

    fn expand_affected_projection_refs(
        &self,
        _subject_refs: identity_application::support::IdentityAcceptedSubjectRefs,
    ) -> Result<IdentityProjectionRefSet, ApplicationError> {
        Ok(IdentityProjectionRefSet {
            projection_refs: Vec::new(),
        })
    }

    fn save_member_summary_view(
        &self,
        view: MemberSummaryView,
        expected_version: Option<IdentityVersion>,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<MemberSummaryViewRef>, ApplicationError> {
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveMemberSummaryView {
                view: view.clone(),
                expected_version,
            },
        )?;
        Ok(IdentityVersionedRef {
            value_ref: view.view_ref.clone(),
            version: self.predicted_view_version(&view.view_ref)?,
        })
    }

    fn save_projection_state(
        &self,
        state: ProjectionState,
        expected_version: Option<IdentityVersion>,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<ProjectionStateRef>, ApplicationError> {
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveProjectionState {
                state: state.clone(),
                expected_version,
            },
        )?;
        Ok(IdentityVersionedRef {
            value_ref: state.projection_state_ref.clone(),
            version: self.predicted_projection_version(&state.projection_ref)?,
        })
    }

    fn mark_projection_stale(
        &self,
        projection_ref: IdentityProjectionRef,
        state: ProjectionState,
        expected_version: IdentityVersion,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<ProjectionStateRef>, ApplicationError> {
        if state.projection_ref != projection_ref {
            return Err(ApplicationError::invalid_request(
                "projection ref does not match projection state",
            ));
        }
        self.save_projection_state(state, Some(expected_version), uow)
    }
}

impl IdentityExternalReferenceResolverPort for IdentityInMemoryRuntime {
    fn resolve_external_reference(
        &self,
        reference_ref: ExternalReferenceRef,
        owner_ref: IdentityReferenceOwnerRef,
    ) -> Result<ExternalReferenceResolutionOutcome, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        if let Some(outcome) = store
            .reference_resolution_outcomes
            .get(&external_reference_key(&reference_ref))
        {
            if outcome.state.reference_owner_ref != owner_ref {
                return Err(ApplicationError::not_found(
                    "external reference resolution outcome owner mismatch",
                ));
            }
            return Ok(outcome.clone());
        }
        Err(ApplicationError::not_found(format!(
            "external reference resolution outcome {} is not seeded",
            reference_ref.source_ref.external_ref.as_str()
        )))
    }

    fn map_role_capability_owner(
        &self,
        summary_ref: RoleCapabilitySummaryRef,
    ) -> Result<IdentityReferenceOwnerRef, ApplicationError> {
        runtime_reference_owner_ref(
            IdentityReferenceOwnerKind::RoleCapability,
            format!(
                "role-capability-summary:{}",
                summary_ref.summary_id.as_str()
            ),
        )
    }

    fn map_career_owner(
        &self,
        record_ref: CareerRecordRef,
    ) -> Result<IdentityReferenceOwnerRef, ApplicationError> {
        runtime_reference_owner_ref(
            IdentityReferenceOwnerKind::CareerRecord,
            format!("career-record:{}", record_ref.record_id.as_str()),
        )
    }

    fn map_memory_owner(
        &self,
        reference_ref: MemoryReferenceRef,
    ) -> Result<IdentityReferenceOwnerRef, ApplicationError> {
        runtime_reference_owner_ref(
            IdentityReferenceOwnerKind::MemoryReference,
            format!("memory-reference:{}", reference_ref.reference_id.as_str()),
        )
    }

    fn map_lifecycle_basis_owner(
        &self,
        member_ref: GlobalMemberRef,
        basis_ref: GovernanceBasisRef,
    ) -> Result<IdentityReferenceOwnerRef, ApplicationError> {
        runtime_reference_owner_ref(
            IdentityReferenceOwnerKind::LifecycleBasis,
            format!(
                "lifecycle-basis:{}:{:?}:{}",
                member_ref.id().as_str(),
                basis_ref.basis_kind,
                basis_ref.external_ref.as_str(),
            ),
        )
    }
}

impl IdentityReferenceStateRepository for IdentityInMemoryRuntime {
    fn get_reference_state_with_version(
        &self,
        reference_ref: ExternalReferenceRef,
    ) -> Result<Option<Versioned<ReferenceResolutionState>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .reference_states
            .get(&external_reference_key(&reference_ref))
            .map(|stored| Versioned {
                value: stored.state.clone(),
                version: stored.version,
            }))
    }

    fn find_reference_state_ref(
        &self,
        reference_ref: ExternalReferenceRef,
    ) -> Result<Option<ReferenceResolutionStateRef>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .reference_states
            .get(&external_reference_key(&reference_ref))
            .map(|stored| stored.state.resolution_state_ref.clone()))
    }

    fn list_reference_states_by_owner(
        &self,
        owner_ref: IdentityReferenceOwnerRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<ExternalReferenceRef>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(project_reference_page(
            store.reference_states.values().collect(),
            page,
            |stored| stored.state.reference_owner_ref == owner_ref,
        ))
    }

    fn list_reference_states_by_kind(
        &self,
        reference_kind: ExternalReferenceKind,
        page: IdentityRepositoryPage,
    ) -> Result<Page<ExternalReferenceRef>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(project_reference_page(
            store.reference_states.values().collect(),
            page,
            |stored| stored.state.external_reference_ref.reference_kind == reference_kind,
        ))
    }

    fn list_stale_reference_states(
        &self,
        _maintenance_scope_ref: identity_contracts::refs::MaintenanceScopeRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<ExternalReferenceRef>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(project_reference_page(
            store.reference_states.values().collect(),
            page,
            |stored| {
                matches!(
                    stored.state.state_kind,
                    ReferenceResolutionStateKind::Stale
                        | ReferenceResolutionStateKind::Unavailable
                        | ReferenceResolutionStateKind::Unrecognized
                        | ReferenceResolutionStateKind::PendingReconciliation
                        | ReferenceResolutionStateKind::RefreshFailed
                )
            },
        ))
    }

    fn get_typed_sidecar_refs(
        &self,
        reference_ref: ExternalReferenceRef,
    ) -> Result<ExternalReferenceTypedSidecarRefs, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .reference_states
            .get(&external_reference_key(&reference_ref))
            .map(|stored| stored.sidecars.clone())
            .unwrap_or_else(empty_sidecars))
    }

    fn save_reference_state(
        &self,
        state: ReferenceResolutionState,
        expected_version: Option<IdentityVersion>,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<ReferenceResolutionStateRef>, ApplicationError> {
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveReferenceState {
                state: state.clone(),
                expected_version,
            },
        )?;
        Ok(IdentityVersionedRef {
            value_ref: state.resolution_state_ref.clone(),
            version: self.predicted_reference_version(&state.external_reference_ref)?,
        })
    }

    fn save_typed_sidecar_refs(
        &self,
        reference_ref: ExternalReferenceRef,
        sidecar_refs: ExternalReferenceTypedSidecarRefs,
        expected_version: IdentityVersion,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<ReferenceResolutionStateRef>, ApplicationError> {
        let current = self
            .staged_reference_bundle_state(&uow.transaction_ref(), &reference_ref)?
            .ok_or_else(|| ApplicationError::not_found("reference bundle not found"))?;
        let current_matches_loaded = current.current_version == expected_version;
        let staged_state_already_used_loaded_version = current.committed_version
            == Some(expected_version)
            && current.current_version == IdentityVersion::new(expected_version.get() + 1);
        if !current_matches_loaded && !staged_state_already_used_loaded_version {
            return Err(ApplicationError::optimistic_version_conflict(
                "reference bundle version mismatch",
            ));
        }
        let predicted_version = if current_matches_loaded {
            IdentityVersion::new(expected_version.get() + 1)
        } else {
            current.current_version
        };

        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveTypedSidecars {
                reference_ref,
                sidecars: sidecar_refs,
                expected_version,
            },
        )?;

        Ok(IdentityVersionedRef {
            value_ref: current.value_ref,
            version: predicted_version,
        })
    }
}

impl IdentityMaintenanceRepository for IdentityInMemoryRuntime {
    fn expand_maintenance_targets(
        &self,
        maintenance_scope_ref: MaintenanceScopeRef,
    ) -> Result<Page<IdentityMaintenanceTargetRef>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let (items, next_cursor) = paged(
            store
                .maintenance_targets_by_scope
                .get(&maintenance_scope_key(&maintenance_scope_ref))
                .cloned()
                .unwrap_or_default(),
            IdentityRepositoryPage::new(None, u32::MAX),
            "maintenance-target",
        );
        Ok(Page { items, next_cursor })
    }

    fn list_projection_targets_for_rebuild(
        &self,
        maintenance_scope_ref: MaintenanceScopeRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityProjectionRef>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let (items, next_cursor) = paged(
            store
                .projection_targets_by_scope
                .get(&maintenance_scope_key(&maintenance_scope_ref))
                .cloned()
                .unwrap_or_default(),
            page,
            "maintenance-projection",
        );
        Ok(Page { items, next_cursor })
    }

    fn list_reference_targets_for_refresh(
        &self,
        maintenance_scope_ref: MaintenanceScopeRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<ExternalReferenceRef>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let (items, next_cursor) = paged(
            store
                .reference_targets_by_scope
                .get(&maintenance_scope_key(&maintenance_scope_ref))
                .cloned()
                .unwrap_or_default(),
            page,
            "maintenance-reference",
        );
        Ok(Page { items, next_cursor })
    }

    fn list_report_targets(
        &self,
        maintenance_scope_ref: MaintenanceScopeRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityMaintenanceTargetRef>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let (items, next_cursor) = paged(
            store
                .report_targets_by_scope
                .get(&maintenance_scope_key(&maintenance_scope_ref))
                .cloned()
                .unwrap_or_default(),
            page,
            "maintenance-report",
        );
        Ok(Page { items, next_cursor })
    }

    fn load_maintenance_target_inspection_context(
        &self,
        target_ref: IdentityMaintenanceTargetRef,
    ) -> Result<Option<IdentityMaintenanceInspectionContext>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .maintenance_inspection_contexts
            .get(&maintenance_target_key(&target_ref))
            .cloned())
    }
}

impl IdentityReconciliationReportRepository for IdentityInMemoryRuntime {
    fn get_report_with_version(
        &self,
        report_ref: ReconciliationReportRef,
    ) -> Result<Option<Versioned<ReconciliationReport>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .reconciliation_reports
            .get(report_ref.as_str())
            .map(|stored| Versioned {
                value: stored.report.clone(),
                version: stored.version,
            }))
    }

    fn list_reports_by_scope(
        &self,
        maintenance_scope_ref: MaintenanceScopeRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<ReconciliationReportRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(project_reconciliation_report_page(
            store.reconciliation_reports.values().collect(),
            page,
            |stored| stored.report.maintenance_scope_ref == maintenance_scope_ref,
        ))
    }

    fn list_reports_by_target(
        &self,
        target_ref: IdentityMaintenanceTargetRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<ReconciliationReportRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(project_reconciliation_report_page(
            store.reconciliation_reports.values().collect(),
            page,
            |stored| stored.report.target_refs.contains(&target_ref),
        ))
    }

    fn list_reports_by_state(
        &self,
        state_kind: ReconciliationReportStateKind,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<ReconciliationReportRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(project_reconciliation_report_page(
            store.reconciliation_reports.values().collect(),
            page,
            |stored| stored.report.report_state == state_kind,
        ))
    }

    fn save_report(
        &self,
        report: ReconciliationReport,
        expected_version: Option<IdentityVersion>,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<ReconciliationReportRef>, ApplicationError> {
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveReconciliationReport {
                report: report.clone(),
                expected_version,
            },
        )?;
        Ok(IdentityVersionedRef {
            value_ref: report.report_ref.clone(),
            version: self.predicted_reconciliation_report_version(&report.report_ref)?,
        })
    }
}

impl TraceHandoffIntentRepository for IdentityInMemoryRuntime {
    fn get_handoff_intent_with_version(
        &self,
        intent_ref: TraceHandoffIntentRef,
    ) -> Result<Option<Versioned<TraceHandoffIntent>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .handoff_intents
            .get(intent_ref.as_str())
            .map(|stored| Versioned {
                value: stored.intent.clone(),
                version: stored.version,
            }))
    }

    fn list_handoff_intents_by_member(
        &self,
        member_ref: GlobalMemberRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<TraceHandoffIntentRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(project_handoff_page(
            store.handoff_intents.values().collect(),
            page,
            |stored| stored.intent.member_ref == member_ref,
        ))
    }

    fn list_handoff_intents_by_trace(
        &self,
        trace_record_ref: IdentityTraceRecordRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<TraceHandoffIntentRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(project_handoff_page(
            store.handoff_intents.values().collect(),
            page,
            |stored| stored.intent.trace_record_refs.contains(&trace_record_ref),
        ))
    }

    fn list_handoff_intents_by_audit_trail(
        &self,
        audit_trail_ref: AuditTrailRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<TraceHandoffIntentRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(project_handoff_page(
            store.handoff_intents.values().collect(),
            page,
            |stored| stored.intent.audit_trail_ref.as_ref() == Some(&audit_trail_ref),
        ))
    }

    fn list_handoff_intents_by_target(
        &self,
        target_ref: HandoffTargetRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<TraceHandoffIntentRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(project_handoff_page(
            store.handoff_intents.values().collect(),
            page,
            |stored| stored.intent.handoff_target_ref == target_ref,
        ))
    }

    fn list_retryable_handoff_intents(
        &self,
        target_ref: Option<HandoffTargetRef>,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<TraceHandoffIntentRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(project_handoff_page(
            store.handoff_intents.values().collect(),
            page,
            |stored| {
                stored.intent.is_retryable()
                    && target_ref
                        .as_ref()
                        .map(|target| &stored.intent.handoff_target_ref == target)
                        .unwrap_or(true)
            },
        ))
    }

    fn save_handoff_intent(
        &self,
        intent: TraceHandoffIntent,
        expected_version: Option<IdentityVersion>,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<TraceHandoffIntentRef>, ApplicationError> {
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveHandoffIntent {
                intent: intent.clone(),
                expected_version,
            },
        )?;
        Ok(IdentityVersionedRef {
            value_ref: intent.handoff_intent_ref.clone(),
            version: self.predicted_handoff_version(&intent.handoff_intent_ref)?,
        })
    }
}

impl IdentityAdapterAvailabilityPort for IdentityInMemoryRuntime {
    fn get_adapter_availability(
        &self,
        adapter_ref: IdentityAdapterRef,
    ) -> Result<IdentityAdapterAvailability, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        store
            .adapter_availability
            .get(adapter_ref.as_str())
            .cloned()
            .ok_or_else(|| ApplicationError::not_found("adapter availability not found"))
    }

    fn list_adapter_availability(
        &self,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityAdapterAvailability>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let entries: Vec<_> = store.adapter_availability.values().cloned().collect();
        let (items, next_cursor) = paged(entries, page, "adapter");
        Ok(Page { items, next_cursor })
    }

    fn assert_adapter_attempt_allowed(
        &self,
        adapter_ref: IdentityAdapterRef,
        required_mode: Option<IdentityAdapterModeRef>,
    ) -> Result<IdentityAdapterAvailability, ApplicationError> {
        let availability = self.get_adapter_availability(adapter_ref)?;
        if let Some(required_mode) = required_mode {
            if availability.adapter_mode_ref != required_mode {
                return Err(ApplicationError::dependency_unavailable(
                    "adapter mode mismatch",
                ));
            }
        }
        Ok(availability)
    }
}

impl IdentityTopicBindingPort for IdentityInMemoryRuntime {
    fn resolve_topic_binding(
        &self,
        topic_key_ref: TopicKeyRef,
        payload_marker_ref: IdentityOutboxPayloadMarkerRef,
    ) -> Result<TopicBindingResolution, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        store
            .topic_bindings
            .get(&topic_binding_key(&topic_key_ref, &payload_marker_ref))
            .cloned()
            .ok_or_else(|| ApplicationError::dependency_unavailable("topic binding not seeded"))
    }
}

impl IdentityOutboxPublisherPort for IdentityInMemoryRuntime {
    fn publish_outbox_record(
        &self,
        record_ref: IdentityOutboxRecordRef,
        topic_binding: TopicBindingResolution,
        payload_marker_ref: IdentityOutboxPayloadMarkerRef,
    ) -> Result<OutboxPublishOutcome, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let stored = store
            .outbox_records
            .get(record_ref.as_str())
            .ok_or_else(|| ApplicationError::not_found("outbox record not found"))?;
        if stored.record.topic_key_ref != topic_binding.topic_key_ref {
            return Err(ApplicationError::invalid_request("outbox topic mismatch"));
        }
        if stored.record.payload_marker_ref != payload_marker_ref {
            return Err(ApplicationError::invalid_request(
                "outbox payload marker mismatch",
            ));
        }
        Ok(store
            .outbox_publish_outcomes
            .get(record_ref.as_str())
            .cloned()
            .unwrap_or_else(|| OutboxPublishOutcome::UnsupportedTopic {
                issue_ref: identity_contracts::refs::OutboxDeliveryIssueRef::new(
                    identity_source_ref(IdentitySourceOwner::Identity, "outbox-unsupported"),
                ),
            }))
    }
}

impl IdentityHandoffTargetPort for IdentityInMemoryRuntime {
    fn resolve_handoff_target(
        &self,
        target_ref: HandoffTargetRef,
        scope_ref: HandoffScopeRef,
        safe_material_ref: TraceHandoffSafeMaterialRef,
    ) -> Result<HandoffTargetResolution, ApplicationError> {
        let availability = {
            let store =
                self.shared.store.lock().map_err(|_| {
                    ApplicationError::consistency_defect("runtime store lock poisoned")
                })?;
            if let Some(resolution) =
                store
                    .handoff_target_resolutions
                    .get(&handoff_target_resolution_key(
                        &target_ref,
                        &scope_ref,
                        &safe_material_ref,
                    ))
            {
                return Ok(resolution.clone());
            }
            store
                .adapter_availability
                .values()
                .next()
                .cloned()
                .ok_or_else(|| {
                    ApplicationError::dependency_unavailable("no adapter availability seeded")
                })?
        };

        Ok(HandoffTargetResolution {
            target_ref,
            scope_ref,
            adapter_ref: availability.adapter_ref,
            adapter_mode_ref: availability.adapter_mode_ref,
        })
    }
}

impl IdentityHandoffDeliveryPort for IdentityInMemoryRuntime {
    fn deliver_handoff(
        &self,
        intent_ref: TraceHandoffIntentRef,
        target_resolution: HandoffTargetResolution,
        safe_material_ref: TraceHandoffSafeMaterialRef,
    ) -> Result<HandoffDeliveryOutcome, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let stored = store
            .handoff_intents
            .get(intent_ref.as_str())
            .ok_or_else(|| ApplicationError::not_found("handoff intent not found"))?;
        if stored.intent.handoff_target_ref != target_resolution.target_ref {
            return Err(ApplicationError::invalid_request("handoff target mismatch"));
        }
        if stored.intent.safe_material_ref != safe_material_ref {
            return Err(ApplicationError::invalid_request(
                "handoff material mismatch",
            ));
        }
        Ok(store
            .handoff_delivery_outcomes
            .get(intent_ref.as_str())
            .cloned()
            .unwrap_or_else(|| HandoffDeliveryOutcome::UnsupportedTarget {
                issue_ref: HandoffIssueRef::new(identity_source_ref(
                    IdentitySourceOwner::Identity,
                    "handoff-unsupported",
                )),
            }))
    }

    fn resolve_handoff_receipt(
        &self,
        receipt_ref: HandoffReceiptRef,
    ) -> Result<HandoffReceiptResolution, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .handoff_receipt_resolutions
            .get(receipt_ref.as_str())
            .cloned()
            .unwrap_or(HandoffReceiptResolution {
                receipt_ref,
                receipt_state: ReferenceResolutionStateKind::Resolved,
                issue_ref: None,
            }))
    }
}

impl IdentityReadVisibilityRepository for IdentityInMemoryRuntime {
    fn resolve_member_summary_read(
        &self,
        member_ref: GlobalMemberRef,
        _view_ref: Option<MemberSummaryViewRef>,
        _consumer_ref: ConsumerRef,
        _visibility_context_ref: VisibilityContextRef,
    ) -> Result<Option<IdentityVisibilityAccessSummary>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .member_summary_access
            .get(&member_key(&member_ref))
            .cloned())
    }

    fn resolve_trace_read(
        &self,
        subject_ref: IdentityTraceSubjectRef,
        _consumer_ref: ConsumerRef,
        _visibility_context_ref: VisibilityContextRef,
    ) -> Result<Option<IdentityVisibilityAccessSummary>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store.trace_read_access.get(subject_ref.as_str()).cloned())
    }

    fn resolve_trace_member_page_read(
        &self,
        member_ref: GlobalMemberRef,
        change_kind_ref: Option<IdentityChangeKindRef>,
        _consumer_ref: ConsumerRef,
        _visibility_context_ref: VisibilityContextRef,
    ) -> Result<Option<IdentityVisibilityAccessSummary>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .trace_member_page_access
            .get(&trace_member_page_access_key(
                &member_ref,
                change_kind_ref.as_ref(),
            ))
            .cloned())
    }

    fn resolve_audit_read(
        &self,
        audit_subject_ref: IdentityAuditSubjectRef,
        audit_scope_ref: AuditScopeRef,
        _consumer_ref: ConsumerRef,
        _visibility_context_ref: VisibilityContextRef,
    ) -> Result<Option<IdentityVisibilityAccessSummary>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .audit_read_access
            .get(&audit_access_key(&audit_subject_ref, &audit_scope_ref))
            .cloned())
    }

    fn resolve_report_read(
        &self,
        report_ref: identity_contracts::refs::ReconciliationReportRef,
        _consumer_ref: ConsumerRef,
        _visibility_context_ref: VisibilityContextRef,
    ) -> Result<Option<IdentityVisibilityAccessSummary>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store.report_read_access.get(report_ref.as_str()).cloned())
    }

    fn resolve_reconciliation_scope_read(
        &self,
        maintenance_scope_ref: identity_contracts::refs::MaintenanceScopeRef,
        _consumer_ref: ConsumerRef,
        _visibility_context_ref: VisibilityContextRef,
    ) -> Result<Option<IdentityVisibilityAccessSummary>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .reconciliation_scope_read_access
            .get(&maintenance_scope_key(&maintenance_scope_ref))
            .cloned())
    }

    fn resolve_projection_state_read(
        &self,
        projection_ref: IdentityProjectionRef,
        projection_state_ref: Option<ProjectionStateRef>,
        _consumer_ref: ConsumerRef,
        _visibility_context_ref: VisibilityContextRef,
    ) -> Result<Option<IdentityVisibilityAccessSummary>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .projection_state_read_access
            .get(&projection_state_access_key(
                &projection_ref,
                projection_state_ref.as_ref(),
            ))
            .cloned()
            .or_else(|| {
                store
                    .projection_state_read_access
                    .get(&projection_state_access_key(&projection_ref, None))
                    .cloned()
            }))
    }

    fn resolve_reference_state_read(
        &self,
        external_reference_ref: ExternalReferenceRef,
        owner_ref: Option<IdentityReferenceOwnerRef>,
        _consumer_ref: ConsumerRef,
        _visibility_context_ref: VisibilityContextRef,
    ) -> Result<Option<IdentityVisibilityAccessSummary>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .reference_state_read_access
            .get(&reference_state_access_key(
                &external_reference_ref,
                owner_ref.as_ref(),
            ))
            .cloned()
            .or_else(|| {
                store
                    .reference_state_read_access
                    .get(&reference_state_access_key(&external_reference_ref, None))
                    .cloned()
            }))
    }

    fn resolve_outbox_record_read(
        &self,
        outbox_ref: Option<IdentityOutboxRecordRef>,
        subject_ref: Option<IdentityOutboxSubjectRef>,
        topic_key_ref: Option<TopicKeyRef>,
        _consumer_ref: ConsumerRef,
        _visibility_context_ref: VisibilityContextRef,
    ) -> Result<Option<IdentityVisibilityAccessSummary>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .outbox_record_read_access
            .get(&outbox_record_access_key(
                outbox_ref.as_ref(),
                subject_ref.as_ref(),
                topic_key_ref.as_ref(),
            ))
            .cloned()
            .or_else(|| {
                store
                    .outbox_record_read_access
                    .get(&outbox_record_access_key(outbox_ref.as_ref(), None, None))
                    .cloned()
            }))
    }

    fn resolve_outbox_trace_page_read(
        &self,
        trace_record_ref: IdentityTraceRecordRef,
        _consumer_ref: ConsumerRef,
        _visibility_context_ref: VisibilityContextRef,
    ) -> Result<Option<IdentityVisibilityAccessSummary>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .outbox_trace_page_read_access
            .get(trace_record_ref.as_str())
            .cloned())
    }

    fn resolve_handoff_intent_read(
        &self,
        intent_ref: TraceHandoffIntentRef,
        _consumer_ref: ConsumerRef,
        _visibility_context_ref: VisibilityContextRef,
    ) -> Result<Option<IdentityVisibilityAccessSummary>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .handoff_intent_read_access
            .get(intent_ref.as_str())
            .cloned())
    }

    fn get_visibility_decision(
        &self,
        _visibility_result_ref: VisibilityResultRef,
    ) -> Result<Option<identity_application::support::IdentityVisibilityDecision>, ApplicationError>
    {
        Ok(None)
    }

    fn save_visibility_decision(
        &self,
        _decision: identity_application::support::IdentityVisibilityDecision,
        _expected_version: Option<IdentityVersion>,
        _uow: &dyn IdentityUnitOfWork,
    ) -> Result<
        IdentityVersionedRef<identity_contracts::refs::IdentityVisibilityDecisionRef>,
        ApplicationError,
    > {
        Err(ApplicationError::consistency_defect(
            "visibility decision persistence is not part of commit-03-b fake runtime skeleton",
        ))
    }
}

impl IdentityOutboxRepository for IdentityInMemoryRuntime {
    fn get_outbox_record_with_version(
        &self,
        outbox_ref: IdentityOutboxRecordRef,
    ) -> Result<Option<Versioned<IdentityOutboxRecord>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .outbox_records
            .get(outbox_ref.as_str())
            .map(|stored| Versioned {
                value: stored.record.clone(),
                version: stored.version,
            }))
    }

    fn list_pending_outbox_records(
        &self,
        topic_key_ref: Option<TopicKeyRef>,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<IdentityOutboxRecordRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(project_outbox_page(
            store.outbox_records.values().collect(),
            page,
            |stored| {
                stored.record.outbox_state.state_kind == OutboxStateKind::PendingPublish
                    && topic_key_ref
                        .as_ref()
                        .map(|topic| &stored.record.topic_key_ref == topic)
                        .unwrap_or(true)
            },
        ))
    }

    fn list_retryable_outbox_records(
        &self,
        topic_key_ref: Option<TopicKeyRef>,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<IdentityOutboxRecordRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(project_outbox_page(
            store.outbox_records.values().collect(),
            page,
            |stored| {
                stored.record.is_retryable()
                    && topic_key_ref
                        .as_ref()
                        .map(|topic| &stored.record.topic_key_ref == topic)
                        .unwrap_or(true)
            },
        ))
    }

    fn list_outbox_records_by_subject(
        &self,
        subject_ref: IdentityOutboxSubjectRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<IdentityOutboxRecordRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let mut items: Vec<_> = store
            .outbox_subject_index
            .iter()
            .filter(|(key, _)| key.starts_with(&format!("{}::", subject_ref.as_str())))
            .map(|(_, value)| IdentityVersionedRef {
                value_ref: IdentityOutboxRecordRef::new(value.clone()),
                version: store
                    .outbox_records
                    .get(value)
                    .map(|stored| stored.version)
                    .unwrap_or(IdentityVersion::new(1)),
            })
            .collect();
        items.sort_by(|left, right| left.value_ref.as_str().cmp(right.value_ref.as_str()));
        let (items, next_cursor) = paged(items, page, "outbox-subject");
        Ok(Page { items, next_cursor })
    }

    fn find_outbox_records_by_trace(
        &self,
        trace_record_ref: IdentityTraceRecordRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<IdentityOutboxRecordRef>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let mut items: Vec<_> = store
            .outbox_trace_index
            .iter()
            .filter(|(key, _)| key.starts_with(&format!("{}::", trace_record_ref.as_str())))
            .map(|(_, value)| IdentityVersionedRef {
                value_ref: IdentityOutboxRecordRef::new(value.clone()),
                version: store
                    .outbox_records
                    .get(value)
                    .map(|stored| stored.version)
                    .unwrap_or(IdentityVersion::new(1)),
            })
            .collect();
        items.sort_by(|left, right| left.value_ref.as_str().cmp(right.value_ref.as_str()));
        let (items, next_cursor) = paged(items, page, "outbox-trace");
        Ok(Page { items, next_cursor })
    }

    fn save_outbox_record(
        &self,
        record: IdentityOutboxRecord,
        expected_version: Option<IdentityVersion>,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<IdentityOutboxRecordRef>, ApplicationError> {
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveOutboxRecord {
                record: record.clone(),
                expected_version,
            },
        )?;
        Ok(IdentityVersionedRef {
            value_ref: record.outbox_record_ref.clone(),
            version: self.predicted_outbox_version(&record.outbox_record_ref)?,
        })
    }

    fn update_outbox_state(
        &self,
        record: IdentityOutboxRecord,
        expected_version: IdentityVersion,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<IdentityOutboxRecordRef>, ApplicationError> {
        self.save_outbox_record(record, Some(expected_version), uow)
    }
}

impl IdentityIdempotencyRepository for IdentityInMemoryRuntime {
    fn get_by_key(
        &self,
        operation_name: IdentityOperationName,
        channel: identity_contracts::refs::IdentityOperationChannel,
        idempotency_key: IdentityIdempotencyKey,
    ) -> Result<Option<Versioned<IdentityIdempotencyRecord>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let Some(record_ref) = store.idempotency_key_index.get(&idempotency_key_key(
            &operation_name,
            channel,
            &idempotency_key,
        )) else {
            return Ok(None);
        };
        Ok(store
            .idempotency_records
            .get(record_ref)
            .map(|stored| Versioned {
                value: stored.record.clone(),
                version: stored.version,
            }))
    }

    fn reserve(
        &self,
        context: IdentityOperationContext,
        record_ref: IdentityIdempotencyRecordRef,
        reserved_at: identity_contracts::refs::IdentityTimestamp,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdempotencyReserveOutcome, ApplicationError> {
        let idempotency_key = context
            .idempotency_key
            .clone()
            .ok_or_else(|| ApplicationError::invalid_request("idempotency key is required"))?;
        if let Some(existing) = self.get_by_key(
            context.operation_name.clone(),
            context.channel,
            idempotency_key,
        )? {
            let record = existing.value.clone();
            if record.can_replay(&context.request_digest) {
                let stored_result_ref = record.stored_result_ref.clone().ok_or_else(|| {
                    ApplicationError::new(
                        ApplicationErrorKind::DuplicateReplayConsistencyDefect,
                        "completed idempotency record missing stored result",
                    )
                })?;
                return Ok(IdempotencyReserveOutcome::ReplayAvailable {
                    record: existing,
                    stored_result_ref,
                });
            }
            if record
                .request_digest
                .conflicts_with(&context.request_digest)
            {
                return Ok(IdempotencyReserveOutcome::Conflict(existing));
            }
            return Ok(IdempotencyReserveOutcome::InFlight(existing));
        }

        let record = IdentityIdempotencyRecord::reserve(record_ref, &context, reserved_at)
            .ok_or_else(|| ApplicationError::invalid_request("idempotency key is required"))?;
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveIdempotencyReservation {
                record: record.clone(),
            },
        )?;
        Ok(IdempotencyReserveOutcome::Reserved(Versioned {
            value: record,
            version: IdentityVersion::new(1),
        }))
    }

    fn complete_with_stored_result(
        &self,
        record: IdentityIdempotencyRecord,
        stored_result_ref: identity_contracts::refs::IdentityStoredResultRef,
        completed_at: identity_contracts::refs::IdentityTimestamp,
        expected_version: IdentityVersion,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<IdentityIdempotencyRecordRef>, ApplicationError> {
        let next = record.complete(stored_result_ref, completed_at);
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveIdempotencyTerminal {
                record: next.clone(),
                expected_version,
            },
        )?;
        Ok(IdentityVersionedRef {
            value_ref: next.record_ref,
            version: IdentityVersion::new(expected_version.get() + 1),
        })
    }

    fn complete_rejected_with_stored_result(
        &self,
        record: IdentityIdempotencyRecord,
        stored_result_ref: identity_contracts::refs::IdentityStoredResultRef,
        completed_at: identity_contracts::refs::IdentityTimestamp,
        expected_version: IdentityVersion,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<IdentityIdempotencyRecordRef>, ApplicationError> {
        let next = record.complete_rejected(stored_result_ref, completed_at);
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveIdempotencyTerminal {
                record: next.clone(),
                expected_version,
            },
        )?;
        Ok(IdentityVersionedRef {
            value_ref: next.record_ref,
            version: IdentityVersion::new(expected_version.get() + 1),
        })
    }

    fn mark_conflict(
        &self,
        record: IdentityIdempotencyRecord,
        _conflicted_at: identity_contracts::refs::IdentityTimestamp,
        expected_version: IdentityVersion,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<IdentityIdempotencyRecordRef>, ApplicationError> {
        let next = record.mark_conflict();
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveIdempotencyTerminal {
                record: next.clone(),
                expected_version,
            },
        )?;
        Ok(IdentityVersionedRef {
            value_ref: next.record_ref,
            version: IdentityVersion::new(expected_version.get() + 1),
        })
    }
}

impl IdentityStoredResultRepository for IdentityInMemoryRuntime {
    fn get_stored_result(
        &self,
        stored_result_ref: identity_contracts::refs::IdentityStoredResultRef,
    ) -> Result<Option<StoredIdentityOperationResult>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .stored_results
            .get(stored_result_ref.as_str())
            .cloned())
    }

    fn find_by_operation_context(
        &self,
        context_ref: IdentityOperationContextRef,
    ) -> Result<Option<StoredIdentityOperationResult>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let Some(result_ref) = store.stored_result_by_context.get(context_ref.as_str()) else {
            return Ok(None);
        };
        Ok(store.stored_results.get(result_ref).cloned())
    }

    fn save_command_accepted_result(
        &self,
        result: StoredIdentityOperationResult,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<identity_contracts::refs::IdentityStoredResultRef, ApplicationError> {
        validate_stored_result_kind(&result, IdentityStoredResultKind::CommandAccepted)?;
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveStoredResult {
                result: result.clone(),
            },
        )?;
        Ok(result.stored_result_ref)
    }

    fn get_command_accepted_result(
        &self,
        stored_result_ref: identity_contracts::refs::IdentityStoredResultRef,
    ) -> Result<Option<IdentityCommandAcceptedResultEnvelope>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .command_accepted_envelopes
            .get(stored_result_ref.as_str())
            .cloned())
    }

    fn save_command_accepted_envelope(
        &self,
        envelope: IdentityCommandAcceptedResultEnvelope,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<identity_contracts::refs::IdentityStoredResultRef, ApplicationError> {
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveCommandAcceptedEnvelope {
                envelope: envelope.clone(),
            },
        )?;
        Ok(envelope.stored_result_ref)
    }

    fn save_command_rejected_result(
        &self,
        result: StoredIdentityOperationResult,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<identity_contracts::refs::IdentityStoredResultRef, ApplicationError> {
        validate_stored_result_kind(&result, IdentityStoredResultKind::CommandRejected)?;
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveStoredResult {
                result: result.clone(),
            },
        )?;
        Ok(result.stored_result_ref)
    }

    fn get_command_rejected_result(
        &self,
        stored_result_ref: identity_contracts::refs::IdentityStoredResultRef,
    ) -> Result<Option<IdentityCommandRejectedResultEnvelope>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .command_rejected_envelopes
            .get(stored_result_ref.as_str())
            .cloned())
    }

    fn save_command_rejected_envelope(
        &self,
        envelope: IdentityCommandRejectedResultEnvelope,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<identity_contracts::refs::IdentityStoredResultRef, ApplicationError> {
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveCommandRejectedEnvelope {
                envelope: envelope.clone(),
            },
        )?;
        Ok(envelope.stored_result_ref)
    }

    fn save_consumer_receipt_result(
        &self,
        result: StoredIdentityOperationResult,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<identity_contracts::refs::IdentityStoredResultRef, ApplicationError> {
        validate_stored_result_kind(&result, IdentityStoredResultKind::ConsumerReceipt)?;
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveStoredResult {
                result: result.clone(),
            },
        )?;
        Ok(result.stored_result_ref)
    }

    fn get_consumer_receipt(
        &self,
        stored_result_ref: identity_contracts::refs::IdentityStoredResultRef,
    ) -> Result<Option<IdentityConsumerReceiptEnvelope>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .consumer_receipts
            .get(stored_result_ref.as_str())
            .cloned())
    }

    fn save_consumer_receipt(
        &self,
        envelope: IdentityConsumerReceiptEnvelope,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<identity_contracts::refs::IdentityStoredResultRef, ApplicationError> {
        validate_receipt_envelope_kind(&envelope, IdentityStoredResultKind::ConsumerReceipt)?;
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveConsumerReceiptEnvelope {
                envelope: envelope.clone(),
            },
        )?;
        Ok(envelope.stored_result_ref)
    }

    fn save_job_report_result(
        &self,
        result: StoredIdentityOperationResult,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<identity_contracts::refs::IdentityStoredResultRef, ApplicationError> {
        validate_stored_result_kind(&result, IdentityStoredResultKind::JobReport)?;
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveStoredResult {
                result: result.clone(),
            },
        )?;
        Ok(result.stored_result_ref)
    }

    fn save_handoff_callback_receipt_result(
        &self,
        result: StoredIdentityOperationResult,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<identity_contracts::refs::IdentityStoredResultRef, ApplicationError> {
        validate_stored_result_kind(&result, IdentityStoredResultKind::HandoffCallbackReceipt)?;
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveStoredResult {
                result: result.clone(),
            },
        )?;
        Ok(result.stored_result_ref)
    }

    fn get_handoff_callback_receipt(
        &self,
        stored_result_ref: identity_contracts::refs::IdentityStoredResultRef,
    ) -> Result<Option<IdentityConsumerReceiptEnvelope>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .handoff_callback_receipts
            .get(stored_result_ref.as_str())
            .cloned())
    }

    fn save_handoff_callback_receipt(
        &self,
        envelope: IdentityConsumerReceiptEnvelope,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<identity_contracts::refs::IdentityStoredResultRef, ApplicationError> {
        validate_receipt_envelope_kind(
            &envelope,
            IdentityStoredResultKind::HandoffCallbackReceipt,
        )?;
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveHandoffCallbackReceiptEnvelope {
                envelope: envelope.clone(),
            },
        )?;
        Ok(envelope.stored_result_ref)
    }
}

impl IdentityCommandEffectSummaryRepository for IdentityInMemoryRuntime {
    fn get_effect_summary(
        &self,
        effect_summary_ref: IdentityCommandEffectSummaryRef,
    ) -> Result<Option<IdentityCommandEffectSummary>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .command_effect_summaries
            .get(effect_summary_ref.as_str())
            .cloned())
    }

    fn list_effects_by_operation_context(
        &self,
        context_ref: IdentityOperationContextRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityCommandEffectSummaryRef>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let mut items: Vec<_> = store
            .command_effect_summaries
            .values()
            .filter(|summary| summary.operation_context_ref == context_ref)
            .map(|summary| summary.effect_summary_ref.clone())
            .collect();
        items.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        let (items, next_cursor) = paged(items, page, "effect-context");
        Ok(Page { items, next_cursor })
    }

    fn list_effects_by_truth_ref(
        &self,
        truth_ref: IdentityTruthRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityCommandEffectSummaryRef>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let mut items: Vec<_> = store
            .command_effect_summaries
            .values()
            .filter(|summary| summary.primary_truth_ref == truth_ref)
            .map(|summary| summary.effect_summary_ref.clone())
            .collect();
        items.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        let (items, next_cursor) = paged(items, page, "effect-truth");
        Ok(Page { items, next_cursor })
    }

    fn list_effects_after_cursor(
        &self,
        after_cursor: Option<IdentityTruthCursor>,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityCommandEffectSummaryRef>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let mut items: Vec<_> = store
            .command_effect_summaries
            .values()
            .filter(|summary| {
                after_cursor
                    .as_ref()
                    .map(|cursor| summary.accepted_cursor_ref.as_str() > cursor.as_str())
                    .unwrap_or(true)
            })
            .map(|summary| summary.effect_summary_ref.clone())
            .collect();
        items.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        let (items, next_cursor) = paged(items, page, "effect-cursor");
        Ok(Page { items, next_cursor })
    }

    fn save_effect_summary(
        &self,
        summary: IdentityCommandEffectSummary,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityCommandEffectSummaryRef, ApplicationError> {
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveEffectSummary {
                summary: summary.clone(),
            },
        )?;
        Ok(summary.effect_summary_ref)
    }
}

impl IdentityJobReportRepository for IdentityInMemoryRuntime {
    fn get_job_report_with_version(
        &self,
        report_ref: identity_contracts::refs::IdentityJobReportRef,
    ) -> Result<Option<Versioned<IdentityJobRunReport>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(store
            .job_reports
            .get(report_ref.as_str())
            .map(|stored| Versioned {
                value: stored.report.clone(),
                version: stored.version,
            }))
    }

    fn find_job_report_by_run(
        &self,
        job_run_ref: identity_contracts::refs::IdentityJobRunRef,
    ) -> Result<Option<Versioned<IdentityJobRunReport>>, ApplicationError> {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        let Some(report_ref) = store.job_report_by_run.get(job_run_ref.as_str()) else {
            return Ok(None);
        };
        Ok(store.job_reports.get(report_ref).map(|stored| Versioned {
            value: stored.report.clone(),
            version: stored.version,
        }))
    }

    fn list_job_reports_by_name(
        &self,
        job_name: IdentityJobName,
        page: IdentityRepositoryPage,
    ) -> Result<
        Page<IdentityVersionedRef<identity_contracts::refs::IdentityJobReportRef>>,
        ApplicationError,
    > {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(project_job_report_page(
            store.job_reports.values().collect(),
            page,
            |stored| stored.report.job_name == job_name,
        ))
    }

    fn list_job_reports_by_result(
        &self,
        result_kind: IdentityJobResultKind,
        page: IdentityRepositoryPage,
    ) -> Result<
        Page<IdentityVersionedRef<identity_contracts::refs::IdentityJobReportRef>>,
        ApplicationError,
    > {
        let store = self
            .shared
            .store
            .lock()
            .map_err(|_| ApplicationError::consistency_defect("runtime store lock poisoned"))?;
        Ok(project_job_report_page(
            store.job_reports.values().collect(),
            page,
            |stored| stored.report.result_kind == result_kind,
        ))
    }

    fn save_job_report(
        &self,
        report: IdentityJobRunReport,
        expected_version: Option<IdentityVersion>,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<
        IdentityVersionedRef<identity_contracts::refs::IdentityJobReportRef>,
        ApplicationError,
    > {
        self.stage(
            &uow.transaction_ref(),
            StagedOp::SaveJobReport {
                report: report.clone(),
                expected_version,
            },
        )?;
        Ok(IdentityVersionedRef {
            value_ref: report.report_ref.clone(),
            version: self.predicted_job_report_version(&report.report_ref)?,
        })
    }
}

fn apply_op(
    store: &mut RuntimeStore,
    baseline: &RuntimeStore,
    op: StagedOp,
) -> Result<(), ApplicationError> {
    match op {
        StagedOp::SaveMember {
            member,
            expected_version,
        } => apply_save_member(store, member, expected_version),
        StagedOp::SaveLifecycle {
            member_ref,
            lifecycle,
            expected_version,
        } => apply_save_lifecycle(store, member_ref, lifecycle, expected_version),
        StagedOp::SaveRoleCapabilitySourceSnapshot {
            snapshot,
            expected_version,
        } => apply_save_role_capability_source_snapshot(store, snapshot, expected_version),
        StagedOp::SaveRoleCapabilitySummary {
            summary,
            expected_version,
        } => apply_save_role_capability_summary(store, summary, expected_version),
        StagedOp::AppendCareerRecord { record } => apply_append_career_record(store, record),
        StagedOp::SaveCareerRecordState {
            record,
            expected_version,
        } => apply_save_career_record_state(store, record, expected_version),
        StagedOp::SaveMemoryReference {
            reference,
            expected_version,
        } => apply_save_memory_reference(store, reference, expected_version),
        StagedOp::AppendTraceRecord { trace_record } => {
            apply_append_trace_record(store, trace_record)
        }
        StagedOp::SaveTraceRecordState {
            trace_record,
            expected_version,
        } => apply_save_trace_record_state(store, trace_record, expected_version),
        StagedOp::SaveAuditTrail {
            trail,
            expected_version,
        } => apply_save_audit_trail(store, trail, expected_version),
        StagedOp::AppendAuditEntry {
            audit_trail_ref,
            entry,
            expected_version,
        } => apply_append_audit_entry(store, audit_trail_ref, entry, expected_version),
        StagedOp::SaveMemberSummaryView {
            view,
            expected_version,
        } => apply_save_member_summary_view(store, view, expected_version),
        StagedOp::SaveProjectionState {
            state,
            expected_version,
        } => apply_save_projection_state(store, baseline, state, expected_version),
        StagedOp::SaveReferenceState {
            state,
            expected_version,
        } => apply_save_reference_state(store, state, expected_version),
        StagedOp::SaveReconciliationReport {
            report,
            expected_version,
        } => apply_save_reconciliation_report(store, report, expected_version),
        StagedOp::SaveTypedSidecars {
            reference_ref,
            sidecars,
            expected_version,
        } => apply_save_typed_sidecars(store, baseline, reference_ref, sidecars, expected_version),
        StagedOp::SaveHandoffIntent {
            intent,
            expected_version,
        } => apply_save_handoff_intent(store, intent, expected_version),
        StagedOp::SaveOutboxRecord {
            record,
            expected_version,
        } => apply_save_outbox_record(store, record, expected_version),
        StagedOp::SaveIdempotencyReservation { record } => {
            apply_save_idempotency_reservation(store, record)
        }
        StagedOp::SaveIdempotencyTerminal {
            record,
            expected_version,
        } => apply_save_idempotency_terminal(store, record, expected_version),
        StagedOp::SaveStoredResult { result } => apply_save_stored_result(store, result),
        StagedOp::SaveCommandAcceptedEnvelope { envelope } => {
            apply_save_command_accepted_envelope(store, envelope)
        }
        StagedOp::SaveCommandRejectedEnvelope { envelope } => {
            apply_save_command_rejected_envelope(store, envelope)
        }
        StagedOp::SaveConsumerReceiptEnvelope { envelope } => {
            apply_save_consumer_receipt(store, envelope)
        }
        StagedOp::SaveHandoffCallbackReceiptEnvelope { envelope } => {
            apply_save_handoff_callback_receipt(store, envelope)
        }
        StagedOp::SaveEffectSummary { summary } => apply_save_effect_summary(store, summary),
        StagedOp::SaveJobReport {
            report,
            expected_version,
        } => apply_save_job_report(store, report, expected_version),
    }
}

fn apply_save_member(
    store: &mut RuntimeStore,
    member: GlobalMember,
    expected_version: Option<IdentityVersion>,
) -> Result<(), ApplicationError> {
    let key = member_key(&member.member_ref);
    match (store.members.get(&key), expected_version) {
        (None, None) => {
            store.members.insert(
                key,
                StoredMember {
                    member,
                    version: IdentityVersion::new(1),
                },
            );
            Ok(())
        }
        (Some(existing), Some(expected)) if existing.version == expected => {
            store.members.insert(
                key,
                StoredMember {
                    member,
                    version: IdentityVersion::new(expected.get() + 1),
                },
            );
            Ok(())
        }
        (None, Some(_)) => Err(ApplicationError::not_found(
            "member truth not found for update",
        )),
        _ => Err(ApplicationError::optimistic_version_conflict(
            "member truth version mismatch",
        )),
    }
}

fn apply_save_lifecycle(
    store: &mut RuntimeStore,
    member_ref: GlobalMemberRef,
    lifecycle: GlobalLifecycleState,
    expected_version: Option<IdentityVersion>,
) -> Result<(), ApplicationError> {
    let key = member_key(&member_ref);
    match (store.lifecycles.get(&key), expected_version) {
        (None, None) => {
            store.lifecycles.insert(
                key,
                StoredLifecycle {
                    member_ref,
                    lifecycle,
                    version: IdentityVersion::new(1),
                },
            );
            Ok(())
        }
        (Some(existing), Some(expected)) if existing.version == expected => {
            store.lifecycles.insert(
                key,
                StoredLifecycle {
                    member_ref,
                    lifecycle,
                    version: IdentityVersion::new(expected.get() + 1),
                },
            );
            Ok(())
        }
        (None, Some(_)) => Err(ApplicationError::not_found(
            "lifecycle truth not found for update",
        )),
        _ => Err(ApplicationError::optimistic_version_conflict(
            "lifecycle truth version mismatch",
        )),
    }
}

fn apply_save_role_capability_source_snapshot(
    store: &mut RuntimeStore,
    snapshot: RoleCapabilitySourceSnapshot,
    expected_version: Option<IdentityVersion>,
) -> Result<(), ApplicationError> {
    let key = role_capability_snapshot_key(&snapshot.snapshot_ref);
    match (
        store.role_capability_source_snapshots.get(&key),
        expected_version,
    ) {
        (None, None) => {
            store.role_capability_snapshot_by_source.insert(
                role_capability_source_key(&snapshot.source_ref),
                key.clone(),
            );
            store.role_capability_source_snapshots.insert(
                key,
                StoredRoleCapabilitySourceSnapshot {
                    snapshot,
                    version: IdentityVersion::new(1),
                },
            );
            Ok(())
        }
        (Some(existing), Some(expected)) if existing.version == expected => {
            store.role_capability_snapshot_by_source.insert(
                role_capability_source_key(&snapshot.source_ref),
                key.clone(),
            );
            store.role_capability_source_snapshots.insert(
                key,
                StoredRoleCapabilitySourceSnapshot {
                    snapshot,
                    version: IdentityVersion::new(expected.get() + 1),
                },
            );
            Ok(())
        }
        (None, Some(_)) => Err(ApplicationError::not_found(
            "role capability source snapshot not found for update",
        )),
        _ => Err(ApplicationError::optimistic_version_conflict(
            "role capability source snapshot version mismatch",
        )),
    }
}

fn apply_save_role_capability_summary(
    store: &mut RuntimeStore,
    summary: RoleCapabilitySummary,
    expected_version: Option<IdentityVersion>,
) -> Result<(), ApplicationError> {
    let key = role_capability_summary_key(&summary.summary_ref);
    match (store.role_capability_summaries.get(&key), expected_version) {
        (None, None) => {
            store
                .role_capability_summary_by_member
                .insert(member_key(&summary.member_ref), key.clone());
            store
                .role_capability_summaries_by_member
                .entry(member_key(&summary.member_ref))
                .or_default()
                .push(key.clone());
            store.role_capability_summaries.insert(
                key,
                StoredRoleCapabilitySummary {
                    summary,
                    version: IdentityVersion::new(1),
                },
            );
            Ok(())
        }
        (Some(existing), Some(expected)) if existing.version == expected => {
            store
                .role_capability_summary_by_member
                .insert(member_key(&summary.member_ref), key.clone());
            let member_entries = store
                .role_capability_summaries_by_member
                .entry(member_key(&summary.member_ref))
                .or_default();
            if !member_entries.contains(&key) {
                member_entries.push(key.clone());
            }
            store.role_capability_summaries.insert(
                key,
                StoredRoleCapabilitySummary {
                    summary,
                    version: IdentityVersion::new(expected.get() + 1),
                },
            );
            Ok(())
        }
        (None, Some(_)) => Err(ApplicationError::not_found(
            "role capability summary not found for update",
        )),
        _ => Err(ApplicationError::optimistic_version_conflict(
            "role capability summary version mismatch",
        )),
    }
}

fn apply_append_career_record(
    store: &mut RuntimeStore,
    record: CareerRecord,
) -> Result<(), ApplicationError> {
    let key = career_record_key(&record.career_record_ref);
    if store.career_records.contains_key(&key) {
        return Err(ApplicationError::new(
            ApplicationErrorKind::FormalUniqueConflict,
            "career record already exists",
        ));
    }
    if store
        .career_records_by_source_marker
        .contains_key(&career_source_marker_key(&record.source_marker_ref))
        && record.correction_of_ref.is_none()
    {
        return Err(ApplicationError::new(
            ApplicationErrorKind::FormalUniqueConflict,
            "career source marker already exists",
        ));
    }
    store
        .career_records_by_member
        .entry(member_key(&record.member_ref))
        .or_default()
        .push(key.clone());
    store.career_records_by_source_marker.insert(
        career_source_marker_key(&record.source_marker_ref),
        key.clone(),
    );
    if let Some(original_ref) = record.correction_of_ref.clone() {
        store
            .career_corrections_by_original
            .entry(career_record_key(&original_ref))
            .or_default()
            .push(key.clone());
    }
    store.career_records.insert(
        key,
        StoredCareerRecord {
            record,
            version: IdentityVersion::new(1),
        },
    );
    Ok(())
}

fn apply_save_career_record_state(
    store: &mut RuntimeStore,
    record: CareerRecord,
    expected_version: IdentityVersion,
) -> Result<(), ApplicationError> {
    let key = career_record_key(&record.career_record_ref);
    let Some(existing) = store.career_records.get(&key) else {
        return Err(ApplicationError::not_found(
            "career record not found for update",
        ));
    };
    if existing.version != expected_version {
        return Err(ApplicationError::optimistic_version_conflict(
            "career record version mismatch",
        ));
    }
    store.career_records.insert(
        key,
        StoredCareerRecord {
            record,
            version: IdentityVersion::new(expected_version.get() + 1),
        },
    );
    Ok(())
}

fn apply_save_memory_reference(
    store: &mut RuntimeStore,
    reference: MemoryReference,
    expected_version: Option<IdentityVersion>,
) -> Result<(), ApplicationError> {
    let key = memory_reference_key(&reference.memory_reference_ref);
    match (store.memory_references.get(&key), expected_version) {
        (None, None) => {
            store
                .memory_references_by_member
                .entry(member_key(&reference.member_ref))
                .or_default()
                .push(key.clone());
            if let Some(memory_ref) = reference.memory_ref.clone() {
                store.memory_reference_by_memory.insert(
                    memory_reference_member_memory_key(&reference.member_ref, &memory_ref),
                    key.clone(),
                );
            }
            if let Some(archive_ref) = reference.archive_ref.clone() {
                store.memory_reference_by_archive.insert(
                    memory_reference_member_archive_key(&reference.member_ref, &archive_ref),
                    key.clone(),
                );
            }
            if let Some(handoff_ref) = reference.archive_handoff_ref.clone() {
                store
                    .memory_reference_by_handoff
                    .insert(archive_handoff_key(&handoff_ref), key.clone());
            }
            store.memory_references.insert(
                key,
                StoredMemoryReference {
                    reference,
                    version: IdentityVersion::new(1),
                },
            );
            Ok(())
        }
        (Some(existing), Some(expected)) if existing.version == expected => {
            store
                .memory_reference_by_memory
                .retain(|_, value| value != &key);
            store
                .memory_reference_by_archive
                .retain(|_, value| value != &key);
            store
                .memory_reference_by_handoff
                .retain(|_, value| value != &key);
            if let Some(memory_ref) = reference.memory_ref.clone() {
                store.memory_reference_by_memory.insert(
                    memory_reference_member_memory_key(&reference.member_ref, &memory_ref),
                    key.clone(),
                );
            }
            if let Some(archive_ref) = reference.archive_ref.clone() {
                store.memory_reference_by_archive.insert(
                    memory_reference_member_archive_key(&reference.member_ref, &archive_ref),
                    key.clone(),
                );
            }
            if let Some(handoff_ref) = reference.archive_handoff_ref.clone() {
                store
                    .memory_reference_by_handoff
                    .insert(archive_handoff_key(&handoff_ref), key.clone());
            }
            let member_entries = store
                .memory_references_by_member
                .entry(member_key(&reference.member_ref))
                .or_default();
            if !member_entries.contains(&key) {
                member_entries.push(key.clone());
            }
            store.memory_references.insert(
                key,
                StoredMemoryReference {
                    reference,
                    version: IdentityVersion::new(expected.get() + 1),
                },
            );
            Ok(())
        }
        (None, Some(_)) => Err(ApplicationError::not_found(
            "memory reference not found for update",
        )),
        _ => Err(ApplicationError::optimistic_version_conflict(
            "memory reference version mismatch",
        )),
    }
}

fn apply_append_trace_record(
    store: &mut RuntimeStore,
    trace_record: IdentityTraceRecord,
) -> Result<(), ApplicationError> {
    let key = trace_record.trace_record_ref.as_str().to_owned();
    if store.trace_records.contains_key(&key) {
        return Err(ApplicationError::new(
            ApplicationErrorKind::FormalUniqueConflict,
            "trace record already exists",
        ));
    }
    store
        .trace_subject_index
        .entry(trace_record.subject_ref.as_str().to_owned())
        .or_default()
        .push(key.clone());
    store
        .trace_member_index
        .entry(member_key(&trace_record.member_ref))
        .or_default()
        .push(key.clone());
    store.trace_records.insert(
        key,
        StoredTraceRecord {
            trace: trace_record,
            version: IdentityVersion::new(1),
        },
    );
    Ok(())
}

fn apply_save_trace_record_state(
    store: &mut RuntimeStore,
    trace_record: IdentityTraceRecord,
    expected_version: IdentityVersion,
) -> Result<(), ApplicationError> {
    let key = trace_record.trace_record_ref.as_str().to_owned();
    let Some(existing) = store.trace_records.get(&key) else {
        return Err(ApplicationError::not_found(
            "trace record not found for update",
        ));
    };
    if existing.version != expected_version {
        return Err(ApplicationError::optimistic_version_conflict(
            "trace record version mismatch",
        ));
    }
    store.trace_records.insert(
        key,
        StoredTraceRecord {
            trace: trace_record,
            version: IdentityVersion::new(expected_version.get() + 1),
        },
    );
    Ok(())
}

fn apply_save_audit_trail(
    store: &mut RuntimeStore,
    trail: AuditTrail,
    expected_version: Option<IdentityVersion>,
) -> Result<(), ApplicationError> {
    let key = trail.audit_trail_ref.as_str().to_owned();
    match (store.audit_trails.get(&key), expected_version) {
        (None, None) => {
            store
                .audit_subject_index
                .insert(trail.audit_subject_ref.as_str().to_owned(), key.clone());
            store.audit_trails.insert(
                key,
                StoredAuditTrail {
                    trail,
                    version: IdentityVersion::new(1),
                },
            );
            Ok(())
        }
        (Some(existing), Some(expected)) if existing.version == expected => {
            store
                .audit_subject_index
                .insert(trail.audit_subject_ref.as_str().to_owned(), key.clone());
            store.audit_trails.insert(
                key,
                StoredAuditTrail {
                    trail,
                    version: IdentityVersion::new(expected.get() + 1),
                },
            );
            Ok(())
        }
        (None, Some(_)) => Err(ApplicationError::not_found(
            "audit trail not found for update",
        )),
        _ => Err(ApplicationError::optimistic_version_conflict(
            "audit trail version mismatch",
        )),
    }
}

fn apply_append_audit_entry(
    store: &mut RuntimeStore,
    audit_trail_ref: AuditTrailRef,
    entry: AuditTrailEntry,
    expected_version: IdentityVersion,
) -> Result<(), ApplicationError> {
    let key = audit_trail_ref.as_str().to_owned();
    let Some(existing) = store.audit_trails.get(&key) else {
        return Err(ApplicationError::not_found(
            "audit trail not found for append",
        ));
    };
    if existing.version != expected_version {
        return Err(ApplicationError::optimistic_version_conflict(
            "audit trail version mismatch",
        ));
    }
    let mut next = existing.trail.clone();
    next.entries.push(entry);
    store.audit_trails.insert(
        key,
        StoredAuditTrail {
            trail: next,
            version: IdentityVersion::new(expected_version.get() + 1),
        },
    );
    Ok(())
}

fn apply_save_member_summary_view(
    store: &mut RuntimeStore,
    view: MemberSummaryView,
    expected_version: Option<IdentityVersion>,
) -> Result<(), ApplicationError> {
    match (
        store.member_summary_views.get(view.view_ref.as_str()),
        expected_version,
    ) {
        (None, None) => {
            store.member_scope_index.insert(
                member_scope_key(&view.member_ref, &view.visibility_scope_ref),
                view.view_ref.as_str().to_owned(),
            );
            store.member_summary_views.insert(
                view.view_ref.as_str().to_owned(),
                StoredMemberSummaryView {
                    view,
                    version: IdentityVersion::new(1),
                },
            );
            Ok(())
        }
        (Some(existing), Some(expected)) if existing.version == expected => {
            store.member_scope_index.insert(
                member_scope_key(&view.member_ref, &view.visibility_scope_ref),
                view.view_ref.as_str().to_owned(),
            );
            store.member_summary_views.insert(
                view.view_ref.as_str().to_owned(),
                StoredMemberSummaryView {
                    view,
                    version: IdentityVersion::new(expected.get() + 1),
                },
            );
            Ok(())
        }
        (None, Some(_)) => Err(ApplicationError::not_found(
            "member summary view not found for update",
        )),
        _ => Err(ApplicationError::optimistic_version_conflict(
            "member summary view version mismatch",
        )),
    }
}

fn apply_save_projection_state(
    store: &mut RuntimeStore,
    _baseline: &RuntimeStore,
    state: ProjectionState,
    expected_version: Option<IdentityVersion>,
) -> Result<(), ApplicationError> {
    let key = projection_key(&state.projection_ref);
    match (store.projection_states.get(&key), expected_version) {
        (None, None) => {
            store.projection_states.insert(
                key,
                StoredProjectionState {
                    state,
                    version: IdentityVersion::new(1),
                },
            );
            Ok(())
        }
        (Some(existing), Some(expected)) if existing.version == expected => {
            let existing_cursor = existing
                .state
                .source_cursor_ref
                .as_ref()
                .map(|cursor| cursor.source_cursor_ref.external_ref.as_str());
            let next_cursor = state
                .source_cursor_ref
                .as_ref()
                .map(|cursor| cursor.source_cursor_ref.external_ref.as_str());
            if existing_cursor.is_some() && next_cursor.is_some() && next_cursor < existing_cursor {
                return Err(ApplicationError::optimistic_version_conflict(
                    "newer projection state already exists",
                ));
            }
            store.projection_states.insert(
                key,
                StoredProjectionState {
                    state,
                    version: IdentityVersion::new(expected.get() + 1),
                },
            );
            Ok(())
        }
        (None, Some(_)) => Err(ApplicationError::not_found(
            "projection state not found for update",
        )),
        _ => Err(ApplicationError::optimistic_version_conflict(
            "projection state version mismatch",
        )),
    }
}

fn apply_save_reference_state(
    store: &mut RuntimeStore,
    state: ReferenceResolutionState,
    expected_version: Option<IdentityVersion>,
) -> Result<(), ApplicationError> {
    let key = external_reference_key(&state.external_reference_ref);
    match (store.reference_states.get(&key), expected_version) {
        (None, None) => {
            let next_state = state;
            store.reference_states.insert(
                key,
                StoredReferenceState {
                    state: next_state.clone(),
                    sidecars: empty_sidecars(),
                    version: IdentityVersion::new(1),
                },
            );
            if let Some(outcome) = store
                .reference_resolution_outcomes
                .get_mut(&external_reference_key(&next_state.external_reference_ref))
            {
                outcome.state = next_state;
            }
            Ok(())
        }
        (Some(existing), Some(expected)) if existing.version == expected => {
            let mut next_state = state;
            if matches!(
                next_state.state_kind,
                ReferenceResolutionStateKind::Unavailable
            ) && next_state.safe_summary_ref.is_none()
            {
                next_state.safe_summary_ref = existing.state.safe_summary_ref.clone();
                next_state.source_version_ref = existing.state.source_version_ref.clone();
            }
            store.reference_states.insert(
                key,
                StoredReferenceState {
                    state: next_state.clone(),
                    sidecars: existing.sidecars.clone(),
                    version: IdentityVersion::new(expected.get() + 1),
                },
            );
            if let Some(outcome) = store
                .reference_resolution_outcomes
                .get_mut(&external_reference_key(&next_state.external_reference_ref))
            {
                outcome.state = next_state;
            }
            Ok(())
        }
        (None, Some(_)) => Err(ApplicationError::not_found(
            "reference state not found for update",
        )),
        _ => Err(ApplicationError::optimistic_version_conflict(
            "reference state version mismatch",
        )),
    }
}

fn apply_save_typed_sidecars(
    store: &mut RuntimeStore,
    baseline: &RuntimeStore,
    reference_ref: ExternalReferenceRef,
    sidecars: ExternalReferenceTypedSidecarRefs,
    expected_version: IdentityVersion,
) -> Result<(), ApplicationError> {
    let key = external_reference_key(&reference_ref);
    let existing = store
        .reference_states
        .get(&key)
        .ok_or_else(|| ApplicationError::not_found("reference state not found for sidecar save"))?;
    let keep_current_version = baseline
        .reference_states
        .get(&key)
        .map(|stored| stored.version == expected_version)
        .unwrap_or(false)
        && existing.version == IdentityVersion::new(expected_version.get() + 1);
    if existing.version != expected_version && !keep_current_version {
        return Err(ApplicationError::optimistic_version_conflict(
            "reference bundle version mismatch",
        ));
    }
    let state = existing.state.clone();
    store.reference_states.insert(
        key,
        StoredReferenceState {
            state,
            sidecars: sidecars.clone(),
            version: if keep_current_version {
                existing.version
            } else {
                IdentityVersion::new(expected_version.get() + 1)
            },
        },
    );
    if let Some(outcome) = store
        .reference_resolution_outcomes
        .get_mut(&external_reference_key(&reference_ref))
    {
        outcome.typed_sidecar_refs = Some(sidecars);
    }
    Ok(())
}

fn apply_save_reconciliation_report(
    store: &mut RuntimeStore,
    report: ReconciliationReport,
    expected_version: Option<IdentityVersion>,
) -> Result<(), ApplicationError> {
    match (
        store.reconciliation_reports.get(report.report_ref.as_str()),
        expected_version,
    ) {
        (None, None) => {
            store.reconciliation_reports.insert(
                report.report_ref.as_str().to_owned(),
                StoredReconciliationReport {
                    report,
                    version: IdentityVersion::new(1),
                },
            );
            Ok(())
        }
        (Some(existing), Some(expected)) if existing.version == expected => {
            store.reconciliation_reports.insert(
                report.report_ref.as_str().to_owned(),
                StoredReconciliationReport {
                    report,
                    version: IdentityVersion::new(expected.get() + 1),
                },
            );
            Ok(())
        }
        (None, Some(_)) => Err(ApplicationError::not_found(
            "reconciliation report not found for update",
        )),
        _ => Err(ApplicationError::optimistic_version_conflict(
            "reconciliation report version mismatch",
        )),
    }
}

fn apply_save_handoff_intent(
    store: &mut RuntimeStore,
    intent: TraceHandoffIntent,
    expected_version: Option<IdentityVersion>,
) -> Result<(), ApplicationError> {
    match (
        store
            .handoff_intents
            .get(intent.handoff_intent_ref.as_str()),
        expected_version,
    ) {
        (None, None) => {
            store.handoff_intents.insert(
                intent.handoff_intent_ref.as_str().to_owned(),
                StoredHandoffIntent {
                    intent,
                    version: IdentityVersion::new(1),
                },
            );
            Ok(())
        }
        (Some(existing), Some(expected)) if existing.version == expected => {
            if intent.handoff_state.state_kind == HandoffStateKind::Delivered
                && intent.handoff_state.receipt_ref.is_none()
            {
                return Err(ApplicationError::domain_rejected(
                    "delivered handoff requires formal receipt marker",
                ));
            }
            store.handoff_intents.insert(
                intent.handoff_intent_ref.as_str().to_owned(),
                StoredHandoffIntent {
                    intent,
                    version: IdentityVersion::new(expected.get() + 1),
                },
            );
            Ok(())
        }
        (None, Some(_)) => Err(ApplicationError::not_found(
            "handoff intent not found for update",
        )),
        _ => Err(ApplicationError::optimistic_version_conflict(
            "handoff intent version mismatch",
        )),
    }
}

fn apply_save_outbox_record(
    store: &mut RuntimeStore,
    record: IdentityOutboxRecord,
    expected_version: Option<IdentityVersion>,
) -> Result<(), ApplicationError> {
    if store.faults.contains(&FaultCase::SaveOutboxRecordFails) {
        return Err(ApplicationError::dependency_unavailable(
            "outbox record persistence unavailable",
        ));
    }

    let material_kind = outbox_material_kind(&record)?;

    match (
        store.outbox_records.get(record.outbox_record_ref.as_str()),
        expected_version,
    ) {
        (None, None) => {
            if store
                .outbox_records
                .contains_key(record.outbox_record_ref.as_str())
            {
                return Err(ApplicationError::new(
                    ApplicationErrorKind::FormalUniqueConflict,
                    "outbox record already exists",
                ));
            }
            if !store
                .trace_records
                .contains_key(record.trace_record_ref.as_str())
            {
                return Err(ApplicationError::consistency_defect(
                    "accepted outbox requires a persisted accepted trace record",
                ));
            }
            let key = record.outbox_record_ref.as_str().to_owned();
            let trace_event_key = outbox_trace_event_key(&record.trace_record_ref, material_kind);
            if let Some(existing) = store.outbox_trace_event_index.get(&trace_event_key) {
                if existing != &key {
                    return Err(ApplicationError::new(
                        ApplicationErrorKind::FormalUniqueConflict,
                        "accepted outbox trace and event kind must remain unique",
                    ));
                }
            }
            store.outbox_subject_index.insert(
                outbox_subject_key(&record.subject_ref, &record.outbox_record_ref),
                key.clone(),
            );
            store.outbox_trace_index.insert(
                outbox_trace_key(&record.trace_record_ref, &record.outbox_record_ref),
                key.clone(),
            );
            store
                .outbox_trace_event_index
                .insert(trace_event_key, key.clone());
            upsert_outbox_payload_marker_snapshot(store, &record, material_kind)?;
            store.outbox_records.insert(
                key,
                StoredOutboxRecord {
                    record,
                    version: IdentityVersion::new(1),
                },
            );
            Ok(())
        }
        (Some(existing), Some(expected)) if existing.version == expected => {
            assert_outbox_material_immutable(&existing.record, &record)?;
            let key = record.outbox_record_ref.as_str().to_owned();
            store.outbox_subject_index.retain(|_, value| value != &key);
            store.outbox_trace_index.retain(|_, value| value != &key);
            store.outbox_subject_index.insert(
                outbox_subject_key(&record.subject_ref, &record.outbox_record_ref),
                key.clone(),
            );
            store.outbox_trace_index.insert(
                outbox_trace_key(&record.trace_record_ref, &record.outbox_record_ref),
                key.clone(),
            );
            upsert_outbox_payload_marker_snapshot(store, &record, material_kind)?;
            store.outbox_records.insert(
                key,
                StoredOutboxRecord {
                    record,
                    version: IdentityVersion::new(expected.get() + 1),
                },
            );
            Ok(())
        }
        (None, Some(_)) => Err(ApplicationError::not_found(
            "outbox record not found for update",
        )),
        _ => Err(ApplicationError::optimistic_version_conflict(
            "outbox record version mismatch",
        )),
    }
}

fn outbox_material_kind(
    record: &IdentityOutboxRecord,
) -> Result<AcceptedOutboundMaterialKind, ApplicationError> {
    AcceptedOutboundMaterialKind::from_topic_key_ref(&record.topic_key_ref).ok_or_else(|| {
        ApplicationError::consistency_defect(
            "accepted outbox record must use a canonical accepted outbound topic key",
        )
    })
}

fn upsert_outbox_payload_marker_snapshot(
    store: &mut RuntimeStore,
    record: &IdentityOutboxRecord,
    material_kind: AcceptedOutboundMaterialKind,
) -> Result<(), ApplicationError> {
    let snapshot = StoredOutboxPayloadMarker {
        payload_marker_ref: record.payload_marker_ref.clone(),
        event_name: material_kind.event_name(),
        schema_version_ref: material_kind.schema_version_ref(),
        subject_ref: record.subject_ref.clone(),
        trace_record_ref: record.trace_record_ref.clone(),
    };
    match store
        .outbox_payload_markers
        .get(record.payload_marker_ref.as_str())
    {
        Some(existing) if existing != &snapshot => Err(ApplicationError::consistency_defect(
            "outbox payload marker snapshot is immutable once saved",
        )),
        Some(_) => Ok(()),
        None => {
            store
                .outbox_payload_markers
                .insert(record.payload_marker_ref.as_str().to_owned(), snapshot);
            Ok(())
        }
    }
}

fn seed_outbox_payload_marker_snapshot(
    store: &mut RuntimeStore,
    record: &IdentityOutboxRecord,
) -> Result<(), ApplicationError> {
    let material_kind = outbox_material_kind(record)?;
    store.outbox_trace_event_index.insert(
        outbox_trace_event_key(&record.trace_record_ref, material_kind),
        record.outbox_record_ref.as_str().to_owned(),
    );
    upsert_outbox_payload_marker_snapshot(store, record, material_kind)
}

fn assert_outbox_material_immutable(
    existing: &IdentityOutboxRecord,
    next: &IdentityOutboxRecord,
) -> Result<(), ApplicationError> {
    if existing.member_ref != next.member_ref
        || existing.subject_ref != next.subject_ref
        || existing.change_kind_ref != next.change_kind_ref
        || existing.payload_marker_ref != next.payload_marker_ref
        || existing.topic_key_ref != next.topic_key_ref
        || existing.trace_record_ref != next.trace_record_ref
        || existing.created_at != next.created_at
    {
        return Err(ApplicationError::consistency_defect(
            "outbox accepted material must remain immutable after creation",
        ));
    }
    Ok(())
}

fn apply_save_idempotency_reservation(
    store: &mut RuntimeStore,
    record: IdentityIdempotencyRecord,
) -> Result<(), ApplicationError> {
    let namespace_key = idempotency_key_key(
        &record.operation_name,
        record.channel,
        &record.idempotency_key,
    );
    if store.idempotency_key_index.contains_key(&namespace_key) {
        return Err(ApplicationError::new(
            ApplicationErrorKind::FormalUniqueConflict,
            "idempotency namespace already reserved",
        ));
    }

    let key = record.record_ref.as_str().to_owned();
    store
        .idempotency_key_index
        .insert(namespace_key, key.clone());
    store.idempotency_records.insert(
        key,
        StoredIdempotencyRecord {
            record,
            version: IdentityVersion::new(1),
        },
    );
    Ok(())
}

fn apply_save_idempotency_terminal(
    store: &mut RuntimeStore,
    record: IdentityIdempotencyRecord,
    expected_version: IdentityVersion,
) -> Result<(), ApplicationError> {
    if store.faults.contains(&FaultCase::CompleteIdempotencyFails) {
        return Err(ApplicationError::dependency_unavailable(
            "idempotency completion unavailable",
        ));
    }

    let key = record.record_ref.as_str().to_owned();
    let Some(existing) = store.idempotency_records.get(&key) else {
        return Err(ApplicationError::not_found(
            "idempotency record not found for update",
        ));
    };
    if existing.version != expected_version {
        return Err(ApplicationError::optimistic_version_conflict(
            "idempotency record version mismatch",
        ));
    }
    store.idempotency_records.insert(
        key,
        StoredIdempotencyRecord {
            record,
            version: IdentityVersion::new(expected_version.get() + 1),
        },
    );
    Ok(())
}

fn apply_save_stored_result(
    store: &mut RuntimeStore,
    result: StoredIdentityOperationResult,
) -> Result<(), ApplicationError> {
    if store.faults.contains(&FaultCase::SaveStoredResultFails) {
        return Err(ApplicationError::dependency_unavailable(
            "stored result persistence unavailable",
        ));
    }
    let stored_result_ref = result.stored_result_ref.as_str().to_owned();
    if store.stored_results.contains_key(&stored_result_ref) {
        return Err(ApplicationError::new(
            ApplicationErrorKind::FormalUniqueConflict,
            "stored result already exists",
        ));
    }
    if let Some(existing_ref) = store
        .stored_result_by_context
        .get(result.operation_context_ref.as_str())
    {
        if existing_ref != &stored_result_ref {
            return Err(ApplicationError::new(
                ApplicationErrorKind::FormalUniqueConflict,
                "operation context already has a stored result",
            ));
        }
    }
    store.stored_result_by_context.insert(
        result.operation_context_ref.as_str().to_owned(),
        stored_result_ref.clone(),
    );
    store.stored_results.insert(stored_result_ref, result);
    Ok(())
}

fn apply_save_command_accepted_envelope(
    store: &mut RuntimeStore,
    envelope: IdentityCommandAcceptedResultEnvelope,
) -> Result<(), ApplicationError> {
    let stored_result_ref = envelope.stored_result_ref.as_str().to_owned();
    if store
        .command_accepted_envelopes
        .contains_key(&stored_result_ref)
    {
        return Err(ApplicationError::new(
            ApplicationErrorKind::FormalUniqueConflict,
            "command accepted envelope already exists",
        ));
    }
    store
        .command_accepted_envelopes
        .insert(stored_result_ref, envelope);
    Ok(())
}

fn apply_save_command_rejected_envelope(
    store: &mut RuntimeStore,
    envelope: IdentityCommandRejectedResultEnvelope,
) -> Result<(), ApplicationError> {
    let stored_result_ref = envelope.stored_result_ref.as_str().to_owned();
    if store
        .command_rejected_envelopes
        .contains_key(&stored_result_ref)
    {
        return Err(ApplicationError::new(
            ApplicationErrorKind::FormalUniqueConflict,
            "command rejected envelope already exists",
        ));
    }
    store
        .command_rejected_envelopes
        .insert(stored_result_ref, envelope);
    Ok(())
}

fn apply_save_consumer_receipt(
    store: &mut RuntimeStore,
    envelope: IdentityConsumerReceiptEnvelope,
) -> Result<(), ApplicationError> {
    if store.faults.contains(&FaultCase::SaveReceiptEnvelopeFails) {
        return Err(ApplicationError::dependency_unavailable(
            "consumer receipt persistence unavailable",
        ));
    }
    let stored_result_ref = envelope.stored_result_ref.as_str().to_owned();
    if store.consumer_receipts.contains_key(&stored_result_ref) {
        return Err(ApplicationError::new(
            ApplicationErrorKind::FormalUniqueConflict,
            "consumer receipt envelope already exists",
        ));
    }
    store.consumer_receipts.insert(stored_result_ref, envelope);
    Ok(())
}

fn apply_save_handoff_callback_receipt(
    store: &mut RuntimeStore,
    envelope: IdentityConsumerReceiptEnvelope,
) -> Result<(), ApplicationError> {
    if store.faults.contains(&FaultCase::SaveReceiptEnvelopeFails) {
        return Err(ApplicationError::dependency_unavailable(
            "handoff callback receipt persistence unavailable",
        ));
    }
    let stored_result_ref = envelope.stored_result_ref.as_str().to_owned();
    if store
        .handoff_callback_receipts
        .contains_key(&stored_result_ref)
    {
        return Err(ApplicationError::new(
            ApplicationErrorKind::FormalUniqueConflict,
            "handoff callback receipt envelope already exists",
        ));
    }
    store
        .handoff_callback_receipts
        .insert(stored_result_ref, envelope);
    Ok(())
}

fn apply_save_effect_summary(
    store: &mut RuntimeStore,
    summary: IdentityCommandEffectSummary,
) -> Result<(), ApplicationError> {
    let key = summary.effect_summary_ref.as_str().to_owned();
    if store.command_effect_summaries.contains_key(&key) {
        return Err(ApplicationError::new(
            ApplicationErrorKind::FormalUniqueConflict,
            "command effect summary already exists",
        ));
    }
    store.effects_by_context.insert(
        effect_context_key(&summary.operation_context_ref, &summary.effect_summary_ref),
        key.clone(),
    );
    store.effects_by_truth_ref.insert(
        effect_truth_key(&summary.primary_truth_ref, &summary.effect_summary_ref),
        key.clone(),
    );
    store.effects_after_cursor.insert(
        effect_cursor_key(&summary.accepted_cursor_ref, &summary.effect_summary_ref),
        key.clone(),
    );
    store.command_effect_summaries.insert(key, summary);
    Ok(())
}

fn apply_save_job_report(
    store: &mut RuntimeStore,
    report: IdentityJobRunReport,
    expected_version: Option<IdentityVersion>,
) -> Result<(), ApplicationError> {
    if store.faults.contains(&FaultCase::SaveJobReportFails) {
        return Err(ApplicationError::dependency_unavailable(
            "job report persistence unavailable",
        ));
    }

    match (
        store.job_reports.get(report.report_ref.as_str()),
        expected_version,
    ) {
        (None, None) => {
            if let Some(existing) = store.job_report_by_run.get(report.job_run_ref.as_str()) {
                if existing != report.report_ref.as_str() {
                    return Err(ApplicationError::new(
                        ApplicationErrorKind::FormalUniqueConflict,
                        "job run already has a report",
                    ));
                }
            }
            let key = report.report_ref.as_str().to_owned();
            store
                .job_report_by_run
                .insert(report.job_run_ref.as_str().to_owned(), key.clone());
            store.job_report_by_name.insert(
                job_report_name_key(&report.job_name, &report.report_ref),
                key.clone(),
            );
            store.job_report_by_result.insert(
                job_report_result_key(report.result_kind, &report.report_ref),
                key.clone(),
            );
            store.job_reports.insert(
                key,
                StoredJobReport {
                    report,
                    version: IdentityVersion::new(1),
                },
            );
            Ok(())
        }
        (Some(existing), Some(expected)) if existing.version == expected => {
            let key = report.report_ref.as_str().to_owned();
            store.job_report_by_name.retain(|_, value| value != &key);
            store.job_report_by_result.retain(|_, value| value != &key);
            store
                .job_report_by_run
                .insert(report.job_run_ref.as_str().to_owned(), key.clone());
            store.job_report_by_name.insert(
                job_report_name_key(&report.job_name, &report.report_ref),
                key.clone(),
            );
            store.job_report_by_result.insert(
                job_report_result_key(report.result_kind, &report.report_ref),
                key.clone(),
            );
            store.job_reports.insert(
                key,
                StoredJobReport {
                    report,
                    version: IdentityVersion::new(expected.get() + 1),
                },
            );
            Ok(())
        }
        (None, Some(_)) => Err(ApplicationError::not_found(
            "job report not found for update",
        )),
        _ => Err(ApplicationError::optimistic_version_conflict(
            "job report version mismatch",
        )),
    }
}

fn project_projection_page<F>(
    entries: Vec<&StoredProjectionState>,
    page: IdentityRepositoryPage,
    predicate: F,
) -> Page<IdentityVersionedRef<ProjectionStateRef>>
where
    F: Fn(&StoredProjectionState) -> bool,
{
    let filtered: Vec<_> = entries
        .into_iter()
        .filter(|entry| predicate(entry))
        .collect();
    let start = parse_page_cursor(page.cursor.as_ref(), "projection");
    let items: Vec<_> = filtered
        .iter()
        .skip(start)
        .take(page.limit as usize)
        .map(|entry| IdentityVersionedRef {
            value_ref: entry.state.projection_state_ref.clone(),
            version: entry.version,
        })
        .collect();
    let next_cursor = if start + items.len() < filtered.len() {
        Some(IdentityRepositoryCursor::new(format!(
            "projection:{}",
            start + items.len()
        )))
    } else {
        None
    };
    Page { items, next_cursor }
}

fn project_outbox_page<F>(
    entries: Vec<&StoredOutboxRecord>,
    page: IdentityRepositoryPage,
    predicate: F,
) -> Page<IdentityVersionedRef<IdentityOutboxRecordRef>>
where
    F: Fn(&StoredOutboxRecord) -> bool,
{
    let filtered: Vec<_> = entries
        .into_iter()
        .filter(|entry| predicate(entry))
        .collect();
    let start = parse_page_cursor(page.cursor.as_ref(), "outbox");
    let items: Vec<_> = filtered
        .iter()
        .skip(start)
        .take(page.limit as usize)
        .map(|entry| IdentityVersionedRef {
            value_ref: entry.record.outbox_record_ref.clone(),
            version: entry.version,
        })
        .collect();
    let next_cursor = if start + items.len() < filtered.len() {
        Some(IdentityRepositoryCursor::new(format!(
            "outbox:{}",
            start + items.len()
        )))
    } else {
        None
    };
    Page { items, next_cursor }
}

fn project_job_report_page<F>(
    entries: Vec<&StoredJobReport>,
    page: IdentityRepositoryPage,
    predicate: F,
) -> Page<IdentityVersionedRef<identity_contracts::refs::IdentityJobReportRef>>
where
    F: Fn(&StoredJobReport) -> bool,
{
    let filtered: Vec<_> = entries
        .into_iter()
        .filter(|entry| predicate(entry))
        .collect();
    let start = parse_page_cursor(page.cursor.as_ref(), "job-report");
    let items: Vec<_> = filtered
        .iter()
        .skip(start)
        .take(page.limit as usize)
        .map(|entry| IdentityVersionedRef {
            value_ref: entry.report.report_ref.clone(),
            version: entry.version,
        })
        .collect();
    let next_cursor = if start + items.len() < filtered.len() {
        Some(IdentityRepositoryCursor::new(format!(
            "job-report:{}",
            start + items.len()
        )))
    } else {
        None
    };
    Page { items, next_cursor }
}

fn project_reconciliation_report_page<F>(
    entries: Vec<&StoredReconciliationReport>,
    page: IdentityRepositoryPage,
    predicate: F,
) -> Page<IdentityVersionedRef<ReconciliationReportRef>>
where
    F: Fn(&StoredReconciliationReport) -> bool,
{
    let filtered: Vec<_> = entries
        .into_iter()
        .filter(|entry| predicate(entry))
        .collect();
    let start = parse_page_cursor(page.cursor.as_ref(), "report");
    let items: Vec<_> = filtered
        .iter()
        .skip(start)
        .take(page.limit as usize)
        .map(|entry| IdentityVersionedRef {
            value_ref: entry.report.report_ref.clone(),
            version: entry.version,
        })
        .collect();
    let next_cursor = if start + items.len() < filtered.len() {
        Some(IdentityRepositoryCursor::new(format!(
            "report:{}",
            start + items.len()
        )))
    } else {
        None
    };
    Page { items, next_cursor }
}

fn project_reference_page<F>(
    entries: Vec<&StoredReferenceState>,
    page: IdentityRepositoryPage,
    predicate: F,
) -> Page<ExternalReferenceRef>
where
    F: Fn(&StoredReferenceState) -> bool,
{
    let filtered: Vec<_> = entries
        .into_iter()
        .filter(|entry| predicate(entry))
        .collect();
    let start = parse_page_cursor(page.cursor.as_ref(), "reference");
    let items: Vec<_> = filtered
        .iter()
        .skip(start)
        .take(page.limit as usize)
        .map(|entry| entry.state.external_reference_ref.clone())
        .collect();
    let next_cursor = if start + items.len() < filtered.len() {
        Some(IdentityRepositoryCursor::new(format!(
            "reference:{}",
            start + items.len()
        )))
    } else {
        None
    };
    Page { items, next_cursor }
}

fn project_handoff_page<F>(
    entries: Vec<&StoredHandoffIntent>,
    page: IdentityRepositoryPage,
    predicate: F,
) -> Page<IdentityVersionedRef<TraceHandoffIntentRef>>
where
    F: Fn(&StoredHandoffIntent) -> bool,
{
    let filtered: Vec<_> = entries
        .into_iter()
        .filter(|entry| predicate(entry))
        .collect();
    let start = parse_page_cursor(page.cursor.as_ref(), "handoff");
    let items: Vec<_> = filtered
        .iter()
        .skip(start)
        .take(page.limit as usize)
        .map(|entry| IdentityVersionedRef {
            value_ref: entry.intent.handoff_intent_ref.clone(),
            version: entry.version,
        })
        .collect();
    let next_cursor = if start + items.len() < filtered.len() {
        Some(IdentityRepositoryCursor::new(format!(
            "handoff:{}",
            start + items.len()
        )))
    } else {
        None
    };
    Page { items, next_cursor }
}

fn paged<T>(
    items: Vec<T>,
    page: IdentityRepositoryPage,
    prefix: &str,
) -> (Vec<T>, Option<IdentityRepositoryCursor>)
where
    T: Clone,
{
    let start = parse_page_cursor(page.cursor.as_ref(), prefix);
    let page_items: Vec<_> = items
        .iter()
        .skip(start)
        .take(page.limit as usize)
        .cloned()
        .collect();
    let next_cursor = if start + page_items.len() < items.len() {
        Some(IdentityRepositoryCursor::new(format!(
            "{prefix}:{}",
            start + page_items.len()
        )))
    } else {
        None
    };
    (page_items, next_cursor)
}

fn parse_page_cursor(cursor: Option<&IdentityRepositoryCursor>, prefix: &str) -> usize {
    cursor
        .and_then(|cursor| cursor.as_str().strip_prefix(&format!("{prefix}:")))
        .and_then(|index| index.parse::<usize>().ok())
        .unwrap_or(0)
}

fn tx_key(transaction_ref: &IdentityTransactionRef) -> String {
    transaction_ref.as_str().to_owned()
}

fn member_key(member_ref: &GlobalMemberRef) -> String {
    member_ref.id().as_str().to_owned()
}

fn member_scope_key(member_ref: &GlobalMemberRef, scope_ref: &VisibilityScopeRef) -> String {
    format!("{}::{}", member_ref.id().as_str(), scope_ref.as_str())
}

fn trace_member_page_access_key(
    member_ref: &GlobalMemberRef,
    change_kind_ref: Option<&IdentityChangeKindRef>,
) -> String {
    match change_kind_ref {
        Some(change_kind_ref) => format!(
            "{}::{}",
            member_ref.id().as_str(),
            trace_change_kind_key(change_kind_ref),
        ),
        None => format!("{}::page", member_ref.id().as_str()),
    }
}

fn trace_member_change_kind_key(
    member_ref: &GlobalMemberRef,
    change_kind_ref: &IdentityChangeKindRef,
) -> String {
    format!(
        "{}::{}",
        member_ref.id().as_str(),
        trace_change_kind_key(change_kind_ref),
    )
}

fn trace_change_kind_key(change_kind_ref: &IdentityChangeKindRef) -> String {
    match &change_kind_ref.source_ref {
        Some(source_ref) => format!(
            "{:?}::{:?}::{}",
            change_kind_ref.change_kind,
            source_ref.owner(),
            source_ref.external_ref.as_str(),
        ),
        None => format!("{:?}::none", change_kind_ref.change_kind),
    }
}

fn audit_access_key(
    audit_subject_ref: &IdentityAuditSubjectRef,
    audit_scope_ref: &AuditScopeRef,
) -> String {
    format!(
        "{}::{}",
        audit_subject_ref.as_str(),
        audit_scope_ref.as_str()
    )
}

fn maintenance_scope_key(maintenance_scope_ref: &MaintenanceScopeRef) -> String {
    identity_source_key(&maintenance_scope_ref.scope_ref)
}

fn maintenance_target_key(target_ref: &IdentityMaintenanceTargetRef) -> String {
    format!(
        "{:?}::{}",
        target_ref.target_kind,
        identity_source_key(&target_ref.target_ref)
    )
}

fn projection_state_access_key(
    projection_ref: &IdentityProjectionRef,
    projection_state_ref: Option<&ProjectionStateRef>,
) -> String {
    match projection_state_ref {
        Some(projection_state_ref) => format!(
            "{}::{}",
            projection_ref.as_str(),
            projection_state_ref.projection_state_id.as_str()
        ),
        None => format!("{}::none", projection_ref.as_str()),
    }
}

fn reference_state_access_key(
    external_reference_ref: &ExternalReferenceRef,
    owner_ref: Option<&IdentityReferenceOwnerRef>,
) -> String {
    match owner_ref {
        Some(owner_ref) => format!(
            "{}::{}::{:?}",
            external_reference_key(external_reference_ref),
            identity_source_key(&owner_ref.owner_ref),
            owner_ref.owner_kind
        ),
        None => format!("{}::none", external_reference_key(external_reference_ref)),
    }
}

fn outbox_record_access_key(
    outbox_ref: Option<&IdentityOutboxRecordRef>,
    subject_ref: Option<&IdentityOutboxSubjectRef>,
    topic_key_ref: Option<&TopicKeyRef>,
) -> String {
    let outbox_token = outbox_ref.map(|value| value.as_str()).unwrap_or("none");
    let subject_token = subject_ref.map(|value| value.as_str()).unwrap_or("none");
    let topic_token = topic_key_ref.map(|value| value.as_str()).unwrap_or("none");
    format!("{outbox_token}::{subject_token}::{topic_token}")
}

fn topic_binding_key(
    topic_key_ref: &TopicKeyRef,
    payload_marker_ref: &IdentityOutboxPayloadMarkerRef,
) -> String {
    format!(
        "{}::{}",
        topic_key_ref.as_str(),
        payload_marker_ref.as_str()
    )
}

fn outbox_trace_event_key(
    trace_record_ref: &IdentityTraceRecordRef,
    material_kind: AcceptedOutboundMaterialKind,
) -> String {
    format!(
        "{}::{}",
        trace_record_ref.as_str(),
        material_kind.event_name().as_str()
    )
}

fn handoff_target_resolution_key(
    target_ref: &HandoffTargetRef,
    scope_ref: &HandoffScopeRef,
    safe_material_ref: &TraceHandoffSafeMaterialRef,
) -> String {
    format!(
        "{}::{}::{}",
        target_ref.as_str(),
        scope_ref.as_str(),
        safe_material_ref.as_str()
    )
}

fn identity_source_key(source_ref: &IdentitySourceRef) -> String {
    format!(
        "{:?}::{}",
        source_ref.source_owner,
        source_ref.external_ref.as_str()
    )
}

fn role_capability_summary_key(summary_ref: &RoleCapabilitySummaryRef) -> String {
    summary_ref.summary_id.as_str().to_owned()
}

fn role_capability_snapshot_key(snapshot_ref: &RoleCapabilitySourceSnapshotRef) -> String {
    snapshot_ref.snapshot_id.as_str().to_owned()
}

fn role_capability_source_key(source_ref: &RoleCapabilitySourceRef) -> String {
    format!(
        "{:?}::{}",
        source_ref.source_kind,
        source_ref.source_ref.external_ref.as_str()
    )
}

fn career_record_key(record_ref: &CareerRecordRef) -> String {
    record_ref.record_id.as_str().to_owned()
}

fn career_source_marker_key(source_marker_ref: &CareerSourceMarkerRef) -> String {
    format!(
        "{}::{}::{:?}::{}::{}",
        source_marker_ref.member_ref.id().as_str(),
        source_marker_ref.work_source_ref.source_ref.owner() as u8,
        source_marker_ref.work_source_ref.source_kind,
        source_marker_ref
            .work_source_ref
            .source_ref
            .external_ref
            .as_str(),
        source_marker_ref.marker_token
    )
}

fn memory_reference_key(reference_ref: &MemoryReferenceRef) -> String {
    reference_ref.reference_id.as_str().to_owned()
}

fn memory_reference_member_memory_key(
    member_ref: &GlobalMemberRef,
    memory_ref: &MemoryRef,
) -> String {
    format!(
        "{}::memory::{}",
        member_ref.id().as_str(),
        memory_ref.source_ref.external_ref.as_str()
    )
}

fn memory_reference_member_archive_key(
    member_ref: &GlobalMemberRef,
    archive_ref: &ArchiveRef,
) -> String {
    format!(
        "{}::archive::{}",
        member_ref.id().as_str(),
        archive_ref.source_ref.external_ref.as_str()
    )
}

fn archive_handoff_key(handoff_ref: &ArchiveHandoffRef) -> String {
    format!(
        "{}::{}",
        handoff_ref.source_ref.external_ref.as_str(),
        handoff_ref.handoff_token
    )
}

fn outbox_subject_key(
    subject_ref: &IdentityOutboxSubjectRef,
    outbox_ref: &IdentityOutboxRecordRef,
) -> String {
    format!("{}::{}", subject_ref.as_str(), outbox_ref.as_str())
}

fn outbox_trace_key(
    trace_ref: &IdentityTraceRecordRef,
    outbox_ref: &IdentityOutboxRecordRef,
) -> String {
    format!("{}::{}", trace_ref.as_str(), outbox_ref.as_str())
}

fn projection_key(projection_ref: &IdentityProjectionRef) -> String {
    projection_ref.as_str().to_owned()
}

fn external_reference_key(reference_ref: &ExternalReferenceRef) -> String {
    format!(
        "{:?}::{}::{}",
        reference_ref.reference_kind,
        reference_ref.source_ref.owner() as u8,
        reference_ref.source_ref.external_ref.as_str()
    )
}

fn idempotency_key_key(
    operation_name: &IdentityOperationName,
    channel: identity_contracts::refs::IdentityOperationChannel,
    idempotency_key: &IdentityIdempotencyKey,
) -> String {
    format!(
        "{}::{channel:?}::{}",
        operation_name.as_str(),
        idempotency_key.as_public().as_str()
    )
}

fn effect_context_key(
    context_ref: &IdentityOperationContextRef,
    effect_ref: &IdentityCommandEffectSummaryRef,
) -> String {
    format!("{}::{}", context_ref.as_str(), effect_ref.as_str())
}

fn effect_truth_key(
    truth_ref: &IdentityTruthRef,
    effect_ref: &IdentityCommandEffectSummaryRef,
) -> String {
    format!("{}::{}", truth_ref_key(truth_ref), effect_ref.as_str())
}

fn effect_cursor_key(
    cursor_ref: &IdentityTruthCursor,
    effect_ref: &IdentityCommandEffectSummaryRef,
) -> String {
    format!("{}::{}", cursor_ref.as_str(), effect_ref.as_str())
}

fn truth_ref_key(truth_ref: &IdentityTruthRef) -> String {
    match truth_ref {
        IdentityTruthRef::GlobalMember(value) => format!("global_member:{}", value.id().as_str()),
        IdentityTruthRef::RoleCapabilitySummary(value) => {
            format!("role_capability_summary:{}", value.summary_id.as_str())
        }
        IdentityTruthRef::RoleCapabilitySourceSnapshot(value) => {
            format!(
                "role_capability_source_snapshot:{}",
                value.snapshot_id.as_str()
            )
        }
        IdentityTruthRef::CareerRecord(value) => {
            format!("career_record:{}", value.record_id.as_str())
        }
        IdentityTruthRef::MemoryReference(value) => {
            format!("memory_reference:{}", value.reference_id.as_str())
        }
        IdentityTruthRef::TraceHandoffIntent(value) => {
            format!("trace_handoff_intent:{}", value.as_str())
        }
    }
}

fn job_report_name_key(
    job_name: &IdentityJobName,
    report_ref: &identity_contracts::refs::IdentityJobReportRef,
) -> String {
    format!("{}::{}", job_name.as_str(), report_ref.as_str())
}

fn job_report_result_key(
    result_kind: IdentityJobResultKind,
    report_ref: &identity_contracts::refs::IdentityJobReportRef,
) -> String {
    format!("{result_kind:?}::{}", report_ref.as_str())
}

fn validate_stored_result_kind(
    result: &StoredIdentityOperationResult,
    expected: IdentityStoredResultKind,
) -> Result<(), ApplicationError> {
    if result.result_kind != expected {
        return Err(ApplicationError::invalid_request(format!(
            "stored result kind mismatch: expected {expected:?}"
        )));
    }
    Ok(())
}

fn validate_receipt_envelope_kind(
    envelope: &IdentityConsumerReceiptEnvelope,
    expected: IdentityStoredResultKind,
) -> Result<(), ApplicationError> {
    if envelope.result_kind != expected {
        return Err(ApplicationError::invalid_request(format!(
            "receipt envelope kind mismatch: expected {expected:?}"
        )));
    }
    Ok(())
}

fn empty_sidecars() -> ExternalReferenceTypedSidecarRefs {
    ExternalReferenceTypedSidecarRefs {
        role_capability_safe_summary_ref: None,
        career_safe_summary_ref: None,
        memory_safe_summary_ref: None,
        governance_basis_summary_ref: None,
        evidence_summary_ref: None,
        source_version_ref: None,
    }
}

fn runtime_identity_source_ref(
    owner: IdentitySourceOwner,
    token: impl Into<String>,
) -> Result<IdentitySourceRef, ApplicationError> {
    let external_ref =
        ExternalSourceRef::new(token.into()).map_err(contract_error_to_application_error)?;
    IdentitySourceRef::new(owner, external_ref).map_err(contract_error_to_application_error)
}

fn runtime_reference_owner_ref(
    owner_kind: IdentityReferenceOwnerKind,
    token: impl Into<String>,
) -> Result<IdentityReferenceOwnerRef, ApplicationError> {
    Ok(IdentityReferenceOwnerRef::new(
        owner_kind,
        runtime_identity_source_ref(IdentitySourceOwner::Identity, token)?,
    ))
}

fn contract_error_to_application_error(
    err: identity_contracts::errors::ContractError,
) -> ApplicationError {
    ApplicationError::invalid_request(format!("{}: {}", err.field, err.message))
}

fn identity_source_ref(owner: IdentitySourceOwner, token: &str) -> IdentitySourceRef {
    IdentitySourceRef::new(
        owner,
        ExternalSourceRef::new(token.to_owned()).expect("valid external source ref"),
    )
    .expect("valid source ref")
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use core_contracts::actor::{ActorKind, ActorRef};
    use identity_application::command::{IdentityCommandService, IdentityCommandServiceDeps};
    use identity_application::consumer::{IdentityConsumerService, IdentityConsumerServiceDeps};
    use identity_application::jobs::{
        IdentityJobExecution, IdentityJobService, IdentityJobServiceDeps,
    };
    use identity_application::mapper::DefaultIdentityMaintenanceIssueMapper;
    use identity_application::outbound_material::AcceptedOutboundMaterialKind;
    use identity_application::ports::{
        ExternalReferenceResolutionOutcome, ExternalReferenceTypedSidecarRefs,
        MemberSummaryProjectionRebuildPlan, MemberSummaryProjectionRebuildViewInput,
    };
    use identity_contracts::commands::{
        AppendCareerRecordRequest, EstablishGlobalMemberRequest, IdentityCommandOutcome,
        IdentityCommandRequest, MaintainMemoryReferenceRequest,
        MaintainRoleCapabilitySummaryRequest, PrepareTraceHandoffRequest,
        UpdateGlobalLifecycleStateRequest,
    };
    use identity_contracts::events::{
        ArchiveHandoffResultPayload, IdentityConsumerOutcome, IdentityConsumerReceipt,
        IdentityInboundEventEnvelope, MemoryReferenceSourceStateChangedPayload,
        RoleCapabilitySourceChangedPayload, TraceHandoffResultKind, TraceHandoffResultPayload,
        WorkParticipationAcceptedPayload,
    };
    use identity_contracts::jobs::{
        DeliverTraceHandoffJobInput, IdentityExternalReferenceRefreshScopeDto,
        IdentityHandoffDeliveryScopeDto, IdentityJobRequest, IdentityJobRunDisposition,
        IdentityProjectionRebuildScopeDto, IdentityPropagationRetryScopeDto,
        IdentityReconciliationTargetScopeDto, PublishIdentityOutboxJobInput,
        RebuildIdentityProjectionJobInput, RefreshExternalReferenceStateJobInput,
        RetryIdentityPropagationFailuresJobInput, RunIdentityReconciliationJobInput,
    };
    use identity_contracts::metadata::{
        IdentityCommandMetadata, IdentityDegradedKind, IdentityQueryDisposition,
        IdentityQueryMetadata, IdentityRequestDigestMarker,
    };
    use identity_contracts::protocol::{
        IdentityCommandName, IdentityDigestAlgorithmMarkerRef, IdentityInboundConsumerName,
        IdentityJobName, IdentityProtocolSchemaVersionRef, IdentityQueryName,
    };
    use identity_contracts::queries::{
        GetGlobalLifecycleSummaryRequest, GetGlobalMemberAnchorRequest,
        GetIdentityOutboxStateRequest, GetProjectionStateRequest,
        GetReferenceResolutionStateRequest, GetRoleCapabilitySummaryRequest,
        GetTraceHandoffStateRequest, IdentityOutboxListSelector, IdentityPublicPageRequest,
        IdentityQueryRequest, IdentityTraceReadSelector, ListCareerRecordsRequest,
        ListMemoryReferencesRequest, ListPendingIdentityOutboxRequest, ReadAuditTrailRequest,
        ReadIdentityTraceRequest, ReadMemberSummaryRequest, ReadReconciliationReportRequest,
    };
    use identity_contracts::receipts::{MaintenanceIssueKind, MaintenanceIssueRef};
    use identity_contracts::refs::{
        ArchiveHandoffRef, ArchiveRef, CapabilityEvidenceKind, CapabilityEvidenceRef,
        CapabilitySourceRef, CareerAppendMaterialKind, CareerAppendMaterialMarker,
        CareerAppendReasonKind, CareerAppendReasonRef, CareerRecordChangeIntent,
        CareerRecordStateKind as PublicCareerRecordStateKind, CareerSafeSummaryRef,
        ExternalReferenceKind, ExternalReferenceSafeSummaryRef, ExternalSourceVersionRef,
        GlobalLifecycleStateKind as PublicLifecycleStateKind, HandoffAttemptRef, HandoffReasonRef,
        HandoffReceiptRef, HandoffScopeRef, HandoffStateKind as PublicHandoffStateKind,
        HandoffTargetRef, IdentityApiRequestMarkerRef, IdentityCanonicalRequestMarkerRef,
        IdentityChangeKind, IdentityConsumerBindingRef, IdentityConsumerReceiptRef,
        IdentityDegradedMarkerRef, IdentityEventEnvelopeMarkerRef, IdentityJobReportRef,
        IdentityJobRunRef, IdentityJobScopeMarkerRef, IdentityMaintenanceTargetKind,
        IdentityMaintenanceTargetRef, IdentityOperationChannel, IdentityOutboxPayloadMarkerRef,
        IdentityProjectionKind, IdentityReadSubjectRef, IdentityReadSurfaceKind,
        IdentityRedactionMarkerRef, IdentityRequestDigestValue, IdentitySourceEventRef,
        IdentityStoredResultRef, IdentityTimestamp, LifecycleReasonKind, LifecycleReasonRef,
        MemoryRef, MemoryReferenceChangeIntent, MemoryReferenceChangeMaterialKind,
        MemoryReferenceChangeMaterialMarker, MemoryReferenceReasonKind, MemoryReferenceReasonRef,
        MemoryReferenceRef, MemoryReferenceSourceKind, MemoryReferenceSourceRef,
        MemoryReferenceStateKind as PublicMemoryReferenceStateKind,
        OutboxStateKind as PublicOutboxStateKind, ProjectParticipationRef,
        ProjectionFreshnessMarkerRef, ProjectionStateKind as PublicProjectionStateKind,
        ReconciliationFindingMaterialKind, ReconciliationFindingRef, ReconciliationReportId,
        ReconciliationReportRef,
        ReferenceResolutionStateKind as PublicReferenceResolutionStateKind,
        RoleCapabilityChangeMaterialKind, RoleCapabilityChangeMaterialMarker,
        RoleCapabilityChangeReasonKind, RoleCapabilityChangeReasonRef,
        RoleCapabilitySafeSummaryRef, RoleCapabilitySourceKind, RoleCapabilitySourceVersionRef,
        RoleCapabilitySummaryStateKind as PublicRoleCapabilitySummaryStateKind, RoleSourceRef,
        TraceHandoffSafeMaterialRef, VisibilityContextRef, WorkSourceKind, WorkSourceRef,
    };
    use identity_contracts::views::{
        IdentityReadMaterialKind, IdentityReadMaterialMarker, IdentityVisibilityAccessState,
        MemberSummarySliceKind, MemberSummarySliceRef,
    };
    use identity_domain::career::CareerRecord;
    use identity_domain::handoff::HandoffState;
    use identity_domain::member_identity::GlobalMember;
    use identity_domain::memory_reference::MemoryReference;
    use identity_domain::outbox::{IdentityOutboxRecord, OutboxState};

    use super::*;
    use identity_application::query::{IdentityQueryService, IdentityQueryServiceDeps};

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct TestJobOutput {
        disposition: &'static str,
        changed_count: u32,
        failed_count: u32,
    }

    fn timestamp(value: i64) -> IdentityTimestamp {
        IdentityTimestamp::from_clock(value).expect("valid timestamp")
    }

    fn member_ref(token: &str) -> GlobalMemberRef {
        identity_contracts::refs::GlobalMemberRef::from_id(
            identity_contracts::refs::GlobalMemberId::new(token.to_owned())
                .expect("valid member id"),
        )
    }

    fn scope_ref(token: &str) -> VisibilityScopeRef {
        VisibilityScopeRef::new(token)
    }

    fn visibility_result(token: &str) -> VisibilityResultRef {
        VisibilityResultRef::new(token)
    }

    fn projection_ref(token: &str) -> IdentityProjectionRef {
        IdentityProjectionRef::new(
            IdentityProjectionKind::MemberSummary,
            identity_source_ref(IdentitySourceOwner::Identity, token),
        )
        .expect("projection ref")
    }

    fn projection_cursor(token: &str) -> IdentityProjectionCursorRef {
        IdentityProjectionCursorRef::new(identity_source_ref(IdentitySourceOwner::Identity, token))
    }

    fn maintenance_scope(token: &str) -> identity_contracts::refs::MaintenanceScopeRef {
        identity_contracts::refs::MaintenanceScopeRef::new(identity_source_ref(
            IdentitySourceOwner::Identity,
            token,
        ))
    }

    fn maintenance_issue_ref(token: &str, issue_kind: MaintenanceIssueKind) -> MaintenanceIssueRef {
        MaintenanceIssueRef::new(
            issue_kind,
            identity_source_ref(IdentitySourceOwner::Identity, token),
        )
    }

    fn maintenance_target_ref(
        token: &str,
        target_kind: IdentityMaintenanceTargetKind,
    ) -> IdentityMaintenanceTargetRef {
        IdentityMaintenanceTargetRef::new(
            target_kind,
            identity_source_ref(IdentitySourceOwner::Identity, token),
        )
    }

    fn reconciliation_finding_ref(token: &str) -> ReconciliationFindingRef {
        ReconciliationFindingRef::new(identity_source_ref(IdentitySourceOwner::Identity, token))
    }

    fn reconciliation_report_ref(token: &str) -> ReconciliationReportRef {
        ReconciliationReportRef::from_id(
            ReconciliationReportId::new(token.to_owned()).expect("report id"),
        )
    }

    fn summary_view(scope: &str) -> MemberSummaryView {
        let member = member_ref("member-1");
        let source = identity_source_ref(IdentitySourceOwner::Identity, "summary-source-1");
        MemberSummaryView::from_projection(
            MemberSummaryViewRef::new(format!("view-{scope}")),
            member.clone(),
            scope_ref(scope),
            MemberSummarySliceRef::new(
                MemberSummarySliceKind::Anchor,
                member.clone(),
                source.clone(),
            ),
            MemberSummarySliceRef::new(MemberSummarySliceKind::Lifecycle, member.clone(), source),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            visibility_result(&format!("visibility-{scope}")),
            IdentityReadSurfaceKind::Found,
            Some(IdentityTruthCursor::new("truth-cursor-1")),
            Some(ProjectionFreshnessMarkerRef {
                projection_ref: projection_ref("projection-1"),
                state_kind: "stale".into(),
            }),
            IdentityReadMaterialMarker::new(IdentityReadMaterialKind::SafeSummaryRefs, None),
        )
        .expect("summary view")
    }

    fn member_summary_rebuild_plan(scope: &str) -> MemberSummaryProjectionRebuildPlan {
        let member = member_ref("member-1");
        let source = identity_source_ref(IdentitySourceOwner::Identity, "summary-source-1");
        MemberSummaryProjectionRebuildPlan {
            projection_ref: projection_ref("projection-1"),
            member_ref: member.clone(),
            view_inputs: vec![MemberSummaryProjectionRebuildViewInput {
                view_ref: MemberSummaryViewRef::new(format!("view-{scope}")),
                member_ref: member.clone(),
                visibility_scope_ref: scope_ref(scope),
                anchor_slice_ref: MemberSummarySliceRef::new(
                    MemberSummarySliceKind::Anchor,
                    member.clone(),
                    source.clone(),
                ),
                lifecycle_slice_ref: MemberSummarySliceRef::new(
                    MemberSummarySliceKind::Lifecycle,
                    member.clone(),
                    source.clone(),
                ),
                role_capability_slice_refs: Vec::new(),
                career_slice_refs: Vec::new(),
                memory_slice_refs: Vec::new(),
                visibility_result_ref: visibility_result(&format!("visibility-{scope}")),
                read_surface_kind: IdentityReadSurfaceKind::Found,
                source_cursor_ref: Some(IdentityTruthCursor::new("truth-cursor-1")),
                projection_freshness_ref: Some(ProjectionFreshnessMarkerRef {
                    projection_ref: projection_ref("projection-1"),
                    state_kind: "stale".into(),
                }),
                read_material_marker: IdentityReadMaterialMarker::new(
                    IdentityReadMaterialKind::SafeSummaryRefs,
                    None,
                ),
            }],
        }
    }

    fn projection_state(cursor: &str) -> ProjectionState {
        ProjectionState::stale(
            ProjectionStateRef::from_id(
                identity_contracts::refs::ProjectionStateId::new(format!(
                    "projection-state-{cursor}"
                ))
                .expect("projection state id"),
            ),
            projection_ref("projection-1"),
            Some(member_ref("member-1")),
            projection_cursor(cursor),
            maintenance_scope("scope-1"),
            timestamp(1),
        )
    }

    fn external_reference() -> ExternalReferenceRef {
        ExternalReferenceRef::new(
            ExternalReferenceKind::MethodSource,
            identity_source_ref(IdentitySourceOwner::MethodLibrary, "method-source-1"),
        )
    }

    fn reference_owner() -> IdentityReferenceOwnerRef {
        IdentityReferenceOwnerRef::new(
            identity_contracts::refs::IdentityReferenceOwnerKind::Maintenance,
            identity_source_ref(IdentitySourceOwner::Identity, "owner-1"),
        )
    }

    fn reference_state_resolved() -> ReferenceResolutionState {
        let reference_ref = external_reference();
        ReferenceResolutionState::resolved(
            ReferenceResolutionStateRef::from_id(
                identity_contracts::refs::ReferenceResolutionStateId::new(
                    "reference-state-1".to_owned(),
                )
                .expect("reference state id"),
            ),
            reference_ref.clone(),
            reference_owner(),
            ExternalSourceVersionRef::new(identity_source_ref(
                IdentitySourceOwner::MethodLibrary,
                "source-version-1",
            )),
            ExternalReferenceSafeSummaryRef::new(
                reference_ref,
                identity_source_ref(IdentitySourceOwner::MethodLibrary, "safe-summary-1"),
            ),
            timestamp(1),
        )
    }

    fn refreshed_reference_outcome() -> ExternalReferenceResolutionOutcome {
        let reference_ref = external_reference();
        ExternalReferenceResolutionOutcome {
            state: ReferenceResolutionState::resolved(
                ReferenceResolutionStateRef::from_id(
                    identity_contracts::refs::ReferenceResolutionStateId::new(
                        "reference-state-1".to_owned(),
                    )
                    .expect("reference state id"),
                ),
                reference_ref.clone(),
                reference_owner(),
                ExternalSourceVersionRef::new(identity_source_ref(
                    IdentitySourceOwner::MethodLibrary,
                    "source-version-2",
                )),
                ExternalReferenceSafeSummaryRef::new(
                    reference_ref.clone(),
                    identity_source_ref(IdentitySourceOwner::MethodLibrary, "safe-summary-2"),
                ),
                timestamp(2),
            ),
            typed_sidecar_refs: Some(ExternalReferenceTypedSidecarRefs {
                role_capability_safe_summary_ref: Some(ExternalReferenceSafeSummaryRef::new(
                    reference_ref,
                    identity_source_ref(IdentitySourceOwner::MethodLibrary, "role-summary-2"),
                )),
                career_safe_summary_ref: None,
                memory_safe_summary_ref: None,
                governance_basis_summary_ref: None,
                evidence_summary_ref: None,
                source_version_ref: Some(ExternalSourceVersionRef::new(identity_source_ref(
                    IdentitySourceOwner::MethodLibrary,
                    "sidecar-version-2",
                ))),
            }),
        }
    }

    fn handoff_intent() -> TraceHandoffIntent {
        TraceHandoffIntent {
            handoff_intent_ref: TraceHandoffIntentRef::new("handoff-1"),
            member_ref: member_ref("member-1"),
            trace_record_refs: vec![IdentityTraceRecordRef::new("trace-1")],
            audit_trail_ref: Some(AuditTrailRef::new("audit-1")),
            handoff_target_ref: HandoffTargetRef::new("target-1"),
            handoff_scope_ref: HandoffScopeRef::new("scope-1"),
            safe_material_ref: TraceHandoffSafeMaterialRef::new("material-1"),
            handoff_state: HandoffState::pending(timestamp(1)),
            created_at: timestamp(1),
            updated_at: timestamp(1),
        }
    }

    fn adapter_availability() -> IdentityAdapterAvailability {
        IdentityAdapterAvailability::available(
            IdentityAdapterRef::new("adapter-1"),
            IdentityAdapterModeRef::new("fake"),
            timestamp(1),
        )
    }

    fn topic_binding_resolution(topic_key_ref: TopicKeyRef) -> TopicBindingResolution {
        TopicBindingResolution {
            topic_key_ref,
            adapter_ref: IdentityAdapterRef::new("adapter-1"),
            adapter_mode_ref: IdentityAdapterModeRef::new("fake"),
            publish_scope_ref: identity_source_ref(
                IdentitySourceOwner::Identity,
                "publish-scope-1",
            ),
        }
    }

    fn outbox_attempt_ref(token: &str) -> identity_contracts::refs::OutboxDeliveryAttemptRef {
        identity_contracts::refs::OutboxDeliveryAttemptRef::new(identity_source_ref(
            IdentitySourceOwner::Identity,
            token,
        ))
    }

    fn outbox_issue_ref(token: &str) -> identity_contracts::refs::OutboxDeliveryIssueRef {
        identity_contracts::refs::OutboxDeliveryIssueRef::new(identity_source_ref(
            IdentitySourceOwner::Identity,
            token,
        ))
    }

    fn handoff_attempt_ref(token: &str) -> HandoffAttemptRef {
        HandoffAttemptRef::new(identity_source_ref(IdentitySourceOwner::Identity, token))
    }

    fn handoff_receipt_ref(token: &str) -> HandoffReceiptRef {
        HandoffReceiptRef::new(format!("handoff-receipt-{token}"))
    }

    fn request_digest(token: &str) -> identity_application::support::IdentityRequestDigest {
        identity_application::support::IdentityRequestDigest::from_canonical_marker(
            IdentityCanonicalRequestMarkerRef::new(format!("canonical-{token}")),
            IdentityRequestDigestValue::new(format!("digest-{token}")),
            IdentityProtocolSchemaVersionRef::new("identity.command.v1"),
            IdentityDigestAlgorithmMarkerRef::new("sha256-v1"),
        )
    }

    fn consumer_request_digest(
        token: &str,
    ) -> identity_application::support::IdentityRequestDigest {
        identity_application::support::IdentityRequestDigest::from_canonical_marker(
            IdentityCanonicalRequestMarkerRef::new(format!("canonical-consumer-{token}")),
            IdentityRequestDigestValue::new(format!("digest-consumer-{token}")),
            IdentityProtocolSchemaVersionRef::new("identity.consumer.v1"),
            IdentityDigestAlgorithmMarkerRef::new("sha256-v1"),
        )
    }

    fn request_digest_marker(token: &str) -> IdentityRequestDigestMarker {
        IdentityRequestDigestMarker {
            canonical_marker_ref: IdentityCanonicalRequestMarkerRef::new(format!(
                "canonical-{token}"
            )),
            digest_value: IdentityRequestDigestValue::new(format!("digest-{token}")),
            schema_version_ref: IdentityProtocolSchemaVersionRef::new("identity.command.v1"),
            algorithm_marker_ref: IdentityDigestAlgorithmMarkerRef::new("sha256-v1"),
        }
    }

    fn lifecycle_reason(token: &str, kind: LifecycleReasonKind) -> LifecycleReasonRef {
        LifecycleReasonRef::new(
            kind,
            identity_source_ref(IdentitySourceOwner::Identity, token),
        )
        .expect("lifecycle reason")
    }

    fn command_service<'a>(runtime: &'a IdentityInMemoryRuntime) -> IdentityCommandService<'a> {
        IdentityCommandService::new(IdentityCommandServiceDeps {
            unit_of_work_manager: runtime,
            clock: runtime,
            id_generator: runtime,
            cursor_assigner: runtime,
            operation_context_factory: runtime,
            idempotency_repository: runtime,
            stored_result_repository: runtime,
            effect_summary_repository: runtime,
            truth_change_subject_mapper: runtime,
            accepted_audit_trail_marker_mapper: runtime,
            member_repository: runtime,
            lifecycle_repository: runtime,
            role_capability_repository: runtime,
            career_record_repository: runtime,
            memory_reference_repository: runtime,
            trace_record_repository: runtime,
            audit_trail_repository: runtime,
            outbox_repository: runtime,
            projection_repository: runtime,
            handoff_intent_repository: runtime,
            handoff_target_port: runtime,
            external_source_resolver: runtime,
        })
    }

    fn consumer_service<'a>(runtime: &'a IdentityInMemoryRuntime) -> IdentityConsumerService<'a> {
        IdentityConsumerService::new(IdentityConsumerServiceDeps {
            unit_of_work_manager: runtime,
            clock: runtime,
            id_generator: runtime,
            cursor_assigner: runtime,
            operation_context_factory: runtime,
            idempotency_repository: runtime,
            stored_result_repository: runtime,
            truth_change_subject_mapper: runtime,
            marker_subject_mapper: runtime,
            accepted_audit_trail_marker_mapper: runtime,
            member_repository: runtime,
            role_capability_repository: runtime,
            career_record_repository: runtime,
            memory_reference_repository: runtime,
            reference_state_repository: runtime,
            external_reference_resolver: runtime,
            trace_record_repository: runtime,
            audit_trail_repository: runtime,
            outbox_repository: runtime,
            projection_repository: runtime,
            handoff_intent_repository: runtime,
        })
    }

    fn job_service<'a>(
        runtime: &'a IdentityInMemoryRuntime,
        maintenance_issue_mapper: &'a DefaultIdentityMaintenanceIssueMapper,
    ) -> IdentityJobService<'a> {
        IdentityJobService::new(IdentityJobServiceDeps {
            unit_of_work_manager: runtime,
            clock: runtime,
            id_generator: runtime,
            idempotency_repository: runtime,
            stored_result_repository: runtime,
            job_report_repository: runtime,
            projection_repository: runtime,
            maintenance_repository: runtime,
            reference_state_repository: runtime,
            external_reference_resolver: runtime,
            reconciliation_report_repository: runtime,
            outbox_repository: runtime,
            topic_binding_port: runtime,
            outbox_publisher_port: runtime,
            handoff_intent_repository: runtime,
            handoff_target_port: runtime,
            handoff_delivery_port: runtime,
            maintenance_issue_mapper,
        })
    }

    fn establish_request(
        token: &str,
        requested_member_ref: Option<GlobalMemberRef>,
    ) -> IdentityCommandRequest<EstablishGlobalMemberRequest> {
        IdentityCommandRequest {
            actor_ref: ActorRef::new("actor-1", ActorKind::Human),
            command_name: IdentityCommandName::new("EstablishGlobalMember"),
            metadata: IdentityCommandMetadata {
                idempotency_key: format!("idem-{token}").into(),
                request_marker_ref: IdentityApiRequestMarkerRef::new(format!("request-{token}")),
                schema_version_ref: IdentityProtocolSchemaVersionRef::new("identity.command.v1"),
                trace_context_ref: None,
            },
            digest: request_digest_marker(token),
            body: EstablishGlobalMemberRequest {
                requested_member_ref,
                source_ref: identity_source_ref(IdentitySourceOwner::Identity, "member-source-1"),
                anchor_reason_ref: None,
                initial_lifecycle_reason_ref: lifecycle_reason(
                    "member-source-1",
                    LifecycleReasonKind::InitialProvisioned,
                ),
            },
        }
    }

    fn establish_context(token: &str) -> IdentityOperationContext {
        IdentityOperationContext::from_command(
            IdentityOperationContextRef::new(format!("context-establish-{token}")),
            IdentityOperationName::new("EstablishGlobalMember"),
            ActorRef::new("actor-1", ActorKind::Human),
            identity_application::support::IdentityRequestMetadataRef::new(format!(
                "metadata-establish-{token}"
            )),
            Some(IdentityIdempotencyKey::new(format!("idem-{token}"))),
            request_digest(token),
            None,
            timestamp(1),
        )
    }

    fn update_lifecycle_request(
        token: &str,
        member_ref: GlobalMemberRef,
        target_state: PublicLifecycleStateKind,
    ) -> IdentityCommandRequest<UpdateGlobalLifecycleStateRequest> {
        IdentityCommandRequest {
            actor_ref: ActorRef::new("actor-1", ActorKind::Human),
            command_name: IdentityCommandName::new("UpdateGlobalLifecycleState"),
            metadata: IdentityCommandMetadata {
                idempotency_key: format!("idem-lifecycle-{token}").into(),
                request_marker_ref: IdentityApiRequestMarkerRef::new(format!(
                    "request-lifecycle-{token}"
                )),
                schema_version_ref: IdentityProtocolSchemaVersionRef::new("identity.command.v1"),
                trace_context_ref: None,
            },
            digest: request_digest_marker(&format!("lifecycle-{token}")),
            body: UpdateGlobalLifecycleStateRequest {
                member_ref,
                target_state,
                reason_ref: lifecycle_reason(
                    "lifecycle-source-1",
                    LifecycleReasonKind::ManualPause,
                ),
                basis_ref: None,
                action_risk_ref: None,
            },
        }
    }

    fn update_lifecycle_context(token: &str) -> IdentityOperationContext {
        IdentityOperationContext::from_command(
            IdentityOperationContextRef::new(format!("context-lifecycle-{token}")),
            IdentityOperationName::new("UpdateGlobalLifecycleState"),
            ActorRef::new("actor-1", ActorKind::Human),
            identity_application::support::IdentityRequestMetadataRef::new(format!(
                "metadata-lifecycle-{token}"
            )),
            Some(IdentityIdempotencyKey::new(format!(
                "idem-lifecycle-{token}"
            ))),
            request_digest(&format!("lifecycle-{token}")),
            None,
            timestamp(1),
        )
    }

    fn command_context(
        operation_name: &str,
        idempotency_key: &str,
        digest_token: &str,
    ) -> IdentityOperationContext {
        IdentityOperationContext::from_command(
            IdentityOperationContextRef::new(format!("context-{operation_name}-{idempotency_key}")),
            IdentityOperationName::new(operation_name),
            ActorRef::new("actor-1", ActorKind::Human),
            identity_application::support::IdentityRequestMetadataRef::new(format!(
                "metadata-{operation_name}"
            )),
            Some(IdentityIdempotencyKey::new(idempotency_key.to_owned())),
            request_digest(digest_token),
            None,
            timestamp(1),
        )
    }

    fn role_capability_source_ref(
        token: &str,
    ) -> identity_contracts::refs::RoleCapabilitySourceRef {
        identity_contracts::refs::RoleCapabilitySourceRef::new(
            RoleCapabilitySourceKind::RoleCapabilityBundle,
            identity_source_ref(IdentitySourceOwner::MethodLibrary, token),
        )
        .expect("role capability source")
    }

    fn role_change_reason(token: &str) -> RoleCapabilityChangeReasonRef {
        RoleCapabilityChangeReasonRef::new(
            RoleCapabilityChangeReasonKind::ManualSummaryMaintenance,
            identity_source_ref(IdentitySourceOwner::Identity, token),
        )
        .expect("role change reason")
    }

    fn career_append_reason(token: &str, kind: CareerAppendReasonKind) -> CareerAppendReasonRef {
        CareerAppendReasonRef::new(
            kind,
            identity_source_ref(IdentitySourceOwner::Identity, token),
        )
        .expect("career append reason")
    }

    fn work_source(token: &str, kind: WorkSourceKind) -> WorkSourceRef {
        WorkSourceRef::new(kind, identity_source_ref(IdentitySourceOwner::Work, token))
            .expect("work source")
    }

    fn project_participation(token: &str) -> ProjectParticipationRef {
        ProjectParticipationRef::from_work_source(identity_source_ref(
            IdentitySourceOwner::Work,
            token,
        ))
        .expect("project participation")
    }

    fn career_source_marker(
        member_ref: &GlobalMemberRef,
        work_source_ref: &WorkSourceRef,
        token: &str,
    ) -> identity_contracts::refs::CareerSourceMarkerRef {
        identity_contracts::refs::CareerSourceMarkerRef::new(
            member_ref.clone(),
            work_source_ref.clone(),
            token.to_owned(),
        )
        .expect("career source marker")
    }

    fn memory_source_ref(token: &str, kind: MemoryReferenceSourceKind) -> MemoryReferenceSourceRef {
        let owner = match kind {
            MemoryReferenceSourceKind::ManualCommand
            | MemoryReferenceSourceKind::ReferenceRefreshMarker => IdentitySourceOwner::Identity,
            MemoryReferenceSourceKind::MemorySourceEvent
            | MemoryReferenceSourceKind::MigrationImport
            | MemoryReferenceSourceKind::ArchiveHandoffResult => IdentitySourceOwner::MemoryArchive,
        };
        MemoryReferenceSourceRef::new(kind, identity_source_ref(owner, token))
            .expect("memory source ref")
    }

    fn memory_reason(token: &str, kind: MemoryReferenceReasonKind) -> MemoryReferenceReasonRef {
        MemoryReferenceReasonRef::new(
            kind,
            identity_source_ref(IdentitySourceOwner::Identity, token),
        )
        .expect("memory reason")
    }

    fn role_source_version(
        source_ref: &identity_contracts::refs::RoleCapabilitySourceRef,
        token: &str,
    ) -> RoleCapabilitySourceVersionRef {
        RoleCapabilitySourceVersionRef::new(source_ref.clone(), token).expect("role source version")
    }

    fn memory_reference_ref(token: &str) -> MemoryReferenceRef {
        MemoryReferenceRef::from_id(
            identity_contracts::refs::MemoryReferenceId::new(token.to_owned())
                .expect("memory reference id"),
        )
    }

    fn linked_memory_reference(
        reference_token: &str,
        member_ref: GlobalMemberRef,
        source_token: &str,
    ) -> MemoryReference {
        let source_ref =
            memory_source_ref(source_token, MemoryReferenceSourceKind::MemorySourceEvent);
        let summary = identity_contracts::refs::MemoryReferenceSourceSummary::from_resolver(
            source_ref.clone(),
            Some(
                MemoryRef::from_source(identity_source_ref(
                    IdentitySourceOwner::MemoryArchive,
                    &format!("memory-{source_token}"),
                ))
                .expect("memory ref"),
            ),
            None,
            None,
            Some(
                identity_contracts::refs::MemorySafeSummaryRef::new(
                    source_ref.clone(),
                    format!("safe-{source_token}"),
                )
                .expect("memory safe summary"),
            ),
            identity_contracts::refs::MemoryReferenceSourceState::Trusted,
        );
        MemoryReference::link_for_member(
            memory_reference_ref(reference_token),
            member_ref,
            summary,
            memory_reason(
                "memory-link-reason-1",
                MemoryReferenceReasonKind::ManualMaintain,
            ),
            ActorRef::new("actor-1", ActorKind::Human),
            timestamp(1),
        )
        .expect("linked memory reference")
    }

    fn archived_memory_reference(
        reference_token: &str,
        member_ref: GlobalMemberRef,
        source_token: &str,
        handoff_token: &str,
    ) -> MemoryReference {
        let source_ref = memory_source_ref(
            source_token,
            MemoryReferenceSourceKind::ArchiveHandoffResult,
        );
        let archive_handoff_ref = ArchiveHandoffRef::new(
            identity_source_ref(IdentitySourceOwner::MemoryArchive, handoff_token),
            handoff_token,
        )
        .expect("archive handoff ref");
        let summary = identity_contracts::refs::MemoryReferenceSourceSummary::from_resolver(
            source_ref.clone(),
            Some(
                MemoryRef::from_source(identity_source_ref(
                    IdentitySourceOwner::MemoryArchive,
                    &format!("memory-{source_token}"),
                ))
                .expect("memory ref"),
            ),
            Some(
                ArchiveRef::from_source(identity_source_ref(
                    IdentitySourceOwner::MemoryArchive,
                    &format!("archive-{source_token}"),
                ))
                .expect("archive ref"),
            ),
            Some(archive_handoff_ref),
            None,
            identity_contracts::refs::MemoryReferenceSourceState::HandoffResultAccepted,
        );
        MemoryReference::from_archive_handoff(
            memory_reference_ref(reference_token),
            member_ref,
            summary,
            memory_reason(
                "memory-handoff-reason-1",
                MemoryReferenceReasonKind::ArchiveHandoffResult,
            ),
            ActorRef::new("actor-1", ActorKind::Human),
            timestamp(1),
        )
        .expect("archived memory reference")
    }

    fn job_context(
        operation_name: &str,
        idempotency_key: &str,
        digest_token: &str,
        job_run_ref: &str,
    ) -> IdentityOperationContext {
        IdentityOperationContext::from_job(
            IdentityOperationContextRef::new(format!("context-{operation_name}-{idempotency_key}")),
            IdentityOperationName::new(operation_name),
            ActorRef::system("job-system"),
            identity_application::support::IdentityRequestMetadataRef::new(format!(
                "metadata-{operation_name}"
            )),
            IdentityIdempotencyKey::new(idempotency_key.to_owned()),
            request_digest(digest_token),
            None,
            IdentityJobRunRef::new(job_run_ref),
            timestamp(1),
        )
    }

    fn job_request(
        job_name: &str,
        idempotency_key: &str,
        job_run_ref: &str,
        token: &str,
    ) -> IdentityJobRequest<String> {
        IdentityJobRequest {
            job_name: IdentityJobName::new(job_name),
            job_run_ref: IdentityJobRunRef::new(job_run_ref),
            run_metadata_ref: identity_contracts::refs::IdentityJobRunMetadataRef::new(format!(
                "job-metadata-{token}"
            )),
            scope_marker_ref: IdentityJobScopeMarkerRef::new(format!("job-scope-{token}")),
            idempotency_key: idempotency_key.to_owned().into(),
            input_cursor_ref: Some(identity_contracts::refs::IdentityJobCursorRef::new(
                format!("job-input-cursor-{token}"),
            )),
            schema_version_ref: IdentityProtocolSchemaVersionRef::new("identity.job.v1"),
            system_actor_ref: ActorRef::system("job-system"),
            input: format!("job-input-{token}"),
        }
    }

    fn typed_job_request<T>(
        job_name: &str,
        idempotency_key: &str,
        job_run_ref: &str,
        token: &str,
        input: T,
    ) -> IdentityJobRequest<T> {
        IdentityJobRequest {
            job_name: IdentityJobName::new(job_name),
            job_run_ref: IdentityJobRunRef::new(job_run_ref),
            run_metadata_ref: identity_contracts::refs::IdentityJobRunMetadataRef::new(format!(
                "job-metadata-{token}"
            )),
            scope_marker_ref: IdentityJobScopeMarkerRef::new(format!("job-scope-{token}")),
            idempotency_key: idempotency_key.to_owned().into(),
            input_cursor_ref: Some(identity_contracts::refs::IdentityJobCursorRef::new(
                format!("job-input-cursor-{token}"),
            )),
            schema_version_ref: IdentityProtocolSchemaVersionRef::new("identity.job.v1"),
            system_actor_ref: ActorRef::system("job-system"),
            input,
        }
    }

    fn job_page(limit: u32) -> IdentityPublicPageRequest {
        IdentityPublicPageRequest {
            cursor: None,
            limit,
        }
    }

    fn stored_result_ref(token: &str) -> IdentityStoredResultRef {
        IdentityStoredResultRef::new(format!("stored-result-{token}"))
    }

    fn command_stored_result(
        token: &str,
        context_ref: &IdentityOperationContextRef,
    ) -> StoredIdentityOperationResult {
        StoredIdentityOperationResult::command_accepted(
            stored_result_ref(token),
            context_ref.clone(),
            identity_application::support::IdentityStoredSurfaceMarkerRef::new(format!(
                "surface-{token}"
            )),
            timestamp(2),
        )
    }

    fn trace_record(token: &str, member_ref: GlobalMemberRef) -> IdentityTraceRecord {
        IdentityTraceRecord::from_accepted_change(
            IdentityTraceRecordRef::new(format!("trace-{token}")),
            member_ref,
            identity_contracts::refs::IdentityTraceSubjectRef::new(format!(
                "trace-subject-{token}"
            )),
            identity_contracts::refs::IdentityAuditSubjectRef::new(format!(
                "audit-subject-{token}"
            )),
            IdentityChangeKindRef::new(
                IdentityChangeKind::DerivedMarkerChanged,
                Some(identity_source_ref(
                    IdentitySourceOwner::Identity,
                    "trace-change-source-1",
                )),
            ),
            IdentityTruthCursor::new(format!("cursor-{token}")),
            Some(identity_contracts::refs::IdentityChangeReasonRef::new(
                identity_source_ref(IdentitySourceOwner::Identity, "trace-reason-1"),
            )),
            Some(identity_source_ref(
                IdentitySourceOwner::Identity,
                "trace-source-1",
            )),
            None,
            Some(ActorRef::new("actor-1", ActorKind::Human)),
            IdentityReadMaterialMarker::new(IdentityReadMaterialKind::TraceRefsOnly, None),
            timestamp(1),
        )
        .expect("trace record")
    }

    fn audit_trail(token: &str, member_ref: Option<GlobalMemberRef>) -> AuditTrail {
        AuditTrail::from_accepted_write(
            AuditTrailRef::new(format!("audit-{token}")),
            identity_contracts::refs::IdentityAuditSubjectRef::new(format!(
                "audit-subject-{token}"
            )),
            member_ref,
            identity_contracts::refs::AuditScopeRef::new(format!("audit-scope-{token}")),
            AuditTrailEntry {
                trace_record_ref: IdentityTraceRecordRef::new(format!("trace-{token}")),
                change_kind_ref: IdentityChangeKindRef::new(
                    IdentityChangeKind::DerivedMarkerChanged,
                    Some(identity_source_ref(
                        IdentitySourceOwner::Identity,
                        "audit-change-source-1",
                    )),
                ),
                visibility_result_ref: VisibilityResultRef::new(format!(
                    "audit-visibility-{token}"
                )),
                occurred_at: timestamp(1),
            },
            VisibilityResultRef::new(format!("audit-trail-visibility-{token}")),
            timestamp(1),
        )
        .expect("audit trail")
    }

    fn handoff_reason(token: &str) -> HandoffReasonRef {
        HandoffReasonRef::new(identity_source_ref(IdentitySourceOwner::Identity, token))
            .expect("handoff reason")
    }

    fn prepare_handoff_request(
        token: &str,
        member_ref: GlobalMemberRef,
        trace_refs: Vec<IdentityTraceRecordRef>,
        audit_trail_ref: Option<AuditTrailRef>,
        requested_handoff_intent_ref: Option<TraceHandoffIntentRef>,
    ) -> IdentityCommandRequest<PrepareTraceHandoffRequest> {
        prepare_handoff_request_with_digest(
            token,
            token,
            member_ref,
            trace_refs,
            audit_trail_ref,
            requested_handoff_intent_ref,
        )
    }

    fn prepare_handoff_request_with_digest(
        token: &str,
        digest_token: &str,
        member_ref: GlobalMemberRef,
        trace_refs: Vec<IdentityTraceRecordRef>,
        audit_trail_ref: Option<AuditTrailRef>,
        requested_handoff_intent_ref: Option<TraceHandoffIntentRef>,
    ) -> IdentityCommandRequest<PrepareTraceHandoffRequest> {
        IdentityCommandRequest {
            actor_ref: ActorRef::new("actor-1", ActorKind::Human),
            command_name: IdentityCommandName::new("PrepareTraceHandoff"),
            metadata: IdentityCommandMetadata {
                idempotency_key: format!("idem-handoff-{token}").into(),
                request_marker_ref: IdentityApiRequestMarkerRef::new(format!(
                    "request-handoff-{token}"
                )),
                schema_version_ref: IdentityProtocolSchemaVersionRef::new("identity.command.v1"),
                trace_context_ref: None,
            },
            digest: request_digest_marker(&format!("handoff-{digest_token}")),
            body: PrepareTraceHandoffRequest {
                member_ref,
                requested_handoff_intent_ref,
                trace_record_refs: trace_refs,
                audit_trail_ref,
                handoff_target_ref: HandoffTargetRef::new("target-1"),
                handoff_scope_ref: HandoffScopeRef::new("scope-1"),
                safe_material_ref: TraceHandoffSafeMaterialRef::new("material-1"),
                visibility_context_ref: VisibilityContextRef::new("visibility-context-1"),
                handoff_reason_ref: handoff_reason("handoff-reason-1"),
            },
        }
    }

    fn maintain_role_request(
        token: &str,
        member_ref: GlobalMemberRef,
        source_token: &str,
    ) -> IdentityCommandRequest<MaintainRoleCapabilitySummaryRequest> {
        let source_ref = role_capability_source_ref(source_token);
        let evidence_ref = CapabilityEvidenceRef::new(
            CapabilityEvidenceKind::MethodArtifact,
            source_ref.source_ref.clone(),
        )
        .expect("evidence ref");
        IdentityCommandRequest {
            actor_ref: ActorRef::new("actor-1", ActorKind::Human),
            command_name: IdentityCommandName::new("MaintainRoleCapabilitySummary"),
            metadata: IdentityCommandMetadata {
                idempotency_key: format!("idem-role-{token}").into(),
                request_marker_ref: IdentityApiRequestMarkerRef::new(format!(
                    "request-role-{token}"
                )),
                schema_version_ref: IdentityProtocolSchemaVersionRef::new("identity.command.v1"),
                trace_context_ref: None,
            },
            digest: request_digest_marker(&format!("role-{token}")),
            body: MaintainRoleCapabilitySummaryRequest {
                member_ref,
                requested_summary_ref: None,
                source_ref: source_ref.clone(),
                role_source_ref: Some(
                    RoleSourceRef::from_source(source_ref.clone()).expect("role source"),
                ),
                capability_source_refs: vec![
                    CapabilitySourceRef::from_source(source_ref.clone())
                        .expect("capability source"),
                ],
                evidence_refs: vec![evidence_ref],
                safe_summary_ref: Some(
                    identity_contracts::refs::RoleCapabilitySafeSummaryRef::new(
                        source_ref.clone(),
                        "safe-summary-1",
                    )
                    .expect("safe summary"),
                ),
                change_reason_ref: role_change_reason("role-change-1"),
                change_material_marker: RoleCapabilityChangeMaterialMarker::new(
                    RoleCapabilityChangeMaterialKind::SafeSummaryMarker,
                    Some(source_ref.source_ref.clone()),
                ),
            },
        }
    }

    fn append_career_request(
        token: &str,
        member_ref: GlobalMemberRef,
        work_token: &str,
        work_kind: WorkSourceKind,
        change_intent: CareerRecordChangeIntent,
        original_record_ref: Option<CareerRecordRef>,
    ) -> IdentityCommandRequest<AppendCareerRecordRequest> {
        let resolver_token = format!("{}::{work_token}", member_ref.id().as_str());
        let work_source_ref = work_source(&resolver_token, work_kind);
        let source_marker_token = format!("marker-{resolver_token}");
        IdentityCommandRequest {
            actor_ref: ActorRef::new("actor-1", ActorKind::Human),
            command_name: IdentityCommandName::new("AppendCareerRecord"),
            metadata: IdentityCommandMetadata {
                idempotency_key: format!("idem-career-{token}").into(),
                request_marker_ref: IdentityApiRequestMarkerRef::new(format!(
                    "request-career-{token}"
                )),
                schema_version_ref: IdentityProtocolSchemaVersionRef::new("identity.command.v1"),
                trace_context_ref: None,
            },
            digest: request_digest_marker(&format!("career-{token}")),
            body: AppendCareerRecordRequest {
                member_ref: member_ref.clone(),
                requested_career_record_ref: None,
                change_intent,
                project_participation_ref: project_participation(&resolver_token),
                work_source_ref: work_source_ref.clone(),
                source_marker_ref: career_source_marker(
                    &member_ref,
                    &work_source_ref,
                    &source_marker_token,
                ),
                career_summary_ref: Some(
                    identity_contracts::refs::CareerSafeSummaryRef::new(
                        work_source_ref.clone(),
                        format!("safe-{work_token}"),
                    )
                    .expect("career safe summary"),
                ),
                append_reason_ref: career_append_reason(
                    "career-reason-1",
                    match change_intent {
                        CareerRecordChangeIntent::AppendCorrection => {
                            CareerAppendReasonKind::CorrectionAppend
                        }
                        CareerRecordChangeIntent::MarkSourcePendingReview => {
                            CareerAppendReasonKind::SourcePendingReview
                        }
                        _ => CareerAppendReasonKind::ManualAppend,
                    },
                ),
                original_record_ref,
                append_material_marker: CareerAppendMaterialMarker {
                    material_kind: match change_intent {
                        CareerRecordChangeIntent::AppendCorrection => {
                            CareerAppendMaterialKind::CorrectionMarkerOnly
                        }
                        CareerRecordChangeIntent::MarkSourcePendingReview => {
                            CareerAppendMaterialKind::SourceMarkerOnly
                        }
                        _ => CareerAppendMaterialKind::SafeSummaryMarker,
                    },
                    source_ref: Some(work_source_ref.source_ref.clone()),
                },
            },
        }
    }

    fn maintain_memory_request(
        token: &str,
        member_ref: GlobalMemberRef,
        source_token: &str,
        source_kind: MemoryReferenceSourceKind,
        change_intent: MemoryReferenceChangeIntent,
        archive_handoff_ref: Option<ArchiveHandoffRef>,
    ) -> IdentityCommandRequest<MaintainMemoryReferenceRequest> {
        let source_ref = memory_source_ref(source_token, source_kind);
        let memory_ref = Some(
            MemoryRef::from_source(identity_source_ref(
                IdentitySourceOwner::MemoryArchive,
                &format!("memory-{source_token}"),
            ))
            .expect("memory ref"),
        );
        let archive_ref = if matches!(
            change_intent,
            MemoryReferenceChangeIntent::AttachArchive
                | MemoryReferenceChangeIntent::RecordArchiveHandoffResult
        ) {
            Some(
                ArchiveRef::from_source(identity_source_ref(
                    IdentitySourceOwner::MemoryArchive,
                    &format!("archive-{source_token}"),
                ))
                .expect("archive ref"),
            )
        } else {
            None
        };
        IdentityCommandRequest {
            actor_ref: ActorRef::new("actor-1", ActorKind::Human),
            command_name: IdentityCommandName::new("MaintainMemoryReference"),
            metadata: IdentityCommandMetadata {
                idempotency_key: format!("idem-memory-{token}").into(),
                request_marker_ref: IdentityApiRequestMarkerRef::new(format!(
                    "request-memory-{token}"
                )),
                schema_version_ref: IdentityProtocolSchemaVersionRef::new("identity.command.v1"),
                trace_context_ref: None,
            },
            digest: request_digest_marker(&format!("memory-{token}")),
            body: MaintainMemoryReferenceRequest {
                member_ref,
                requested_memory_reference_ref: None,
                change_intent,
                memory_ref,
                archive_ref,
                archive_handoff_ref,
                source_ref: source_ref.clone(),
                safe_summary_ref: Some(
                    identity_contracts::refs::MemorySafeSummaryRef::new(
                        source_ref.clone(),
                        format!("safe-{source_token}"),
                    )
                    .expect("memory safe summary"),
                ),
                reason_ref: memory_reason(
                    "memory-reason-1",
                    match change_intent {
                        MemoryReferenceChangeIntent::RecordArchiveHandoffResult => {
                            MemoryReferenceReasonKind::ArchiveHandoffResult
                        }
                        MemoryReferenceChangeIntent::MarkPendingVerification => {
                            MemoryReferenceReasonKind::SourcePendingVerification
                        }
                        _ => MemoryReferenceReasonKind::ManualMaintain,
                    },
                ),
                change_material_marker: MemoryReferenceChangeMaterialMarker {
                    material_kind: match change_intent {
                        MemoryReferenceChangeIntent::RecordArchiveHandoffResult => {
                            MemoryReferenceChangeMaterialKind::HandoffMarkerOnly
                        }
                        _ => MemoryReferenceChangeMaterialKind::ReferenceMarkersOnly,
                    },
                    source_ref: Some(source_ref.source_ref.clone()),
                },
            },
        }
    }

    fn consumer_receipt_envelope(
        token: &str,
        context_ref: &IdentityOperationContextRef,
    ) -> IdentityConsumerReceiptEnvelope {
        let fresh = IdentityConsumerReceipt {
            receipt_ref: IdentityConsumerReceiptRef::new(format!("receipt-{token}")),
            consumer_name: IdentityInboundConsumerName::new("HandleRoleCapabilitySourceChanged"),
            outcome: IdentityConsumerOutcome::Accepted,
            stored_result_ref: stored_result_ref(token),
            trace_refs: vec![IdentityTraceRecordRef::new(format!("trace-{token}"))],
            outbox_refs: vec![IdentityOutboxRecordRef::new(format!("outbox-{token}"))],
            issue_refs: Vec::new(),
        };

        IdentityConsumerReceiptEnvelope::consumer_receipt(
            context_ref.clone(),
            identity_application::support::IdentityStoredSurfaceMarkerRef::new(format!(
                "surface-{token}"
            )),
            IdentityConsumerService::duplicate_replay_receipt(&fresh),
            timestamp(2),
        )
    }

    fn handoff_callback_receipt_envelope(
        token: &str,
        context_ref: &IdentityOperationContextRef,
    ) -> IdentityConsumerReceiptEnvelope {
        let fresh = IdentityConsumerReceipt {
            receipt_ref: IdentityConsumerReceiptRef::new(format!("receipt-{token}")),
            consumer_name: IdentityInboundConsumerName::new("HandleTraceHandoffResult"),
            outcome: IdentityConsumerOutcome::Accepted,
            stored_result_ref: stored_result_ref(token),
            trace_refs: vec![IdentityTraceRecordRef::new(format!("trace-{token}"))],
            outbox_refs: vec![IdentityOutboxRecordRef::new(format!("outbox-{token}"))],
            issue_refs: Vec::new(),
        };

        IdentityConsumerReceiptEnvelope::handoff_callback_receipt(
            context_ref.clone(),
            identity_application::support::IdentityStoredSurfaceMarkerRef::new(format!(
                "surface-{token}"
            )),
            IdentityConsumerService::duplicate_replay_receipt(&fresh),
            timestamp(2),
        )
    }

    fn inbound_event_envelope(
        token: &str,
        consumer_name: &str,
        schema_version: &str,
    ) -> IdentityInboundEventEnvelope<()> {
        IdentityInboundEventEnvelope {
            consumer_name: IdentityInboundConsumerName::new(consumer_name),
            envelope_marker_ref: IdentityEventEnvelopeMarkerRef::new(format!("envelope-{token}")),
            consumer_binding_ref: IdentityConsumerBindingRef::new(format!("binding-{token}")),
            source_event_ref: IdentitySourceEventRef::new(format!("source-event-{token}")),
            idempotency_key: format!("idem-{token}").into(),
            schema_version_ref: IdentityProtocolSchemaVersionRef::new(schema_version),
            occurred_at: None,
            received_at: timestamp(1),
            trace_context_ref: None,
            payload: (),
        }
    }

    fn inbound_event_envelope_with_payload<T>(
        token: &str,
        consumer_name: &str,
        payload: T,
    ) -> IdentityInboundEventEnvelope<T> {
        IdentityInboundEventEnvelope {
            consumer_name: IdentityInboundConsumerName::new(consumer_name),
            envelope_marker_ref: IdentityEventEnvelopeMarkerRef::new(format!("envelope-{token}")),
            consumer_binding_ref: IdentityConsumerBindingRef::new(format!("binding-{token}")),
            source_event_ref: IdentitySourceEventRef::new(format!("source-event-{token}")),
            idempotency_key: format!("idem-{token}").into(),
            schema_version_ref: IdentityProtocolSchemaVersionRef::new("identity.consumer.v1"),
            occurred_at: None,
            received_at: timestamp(1),
            trace_context_ref: None,
            payload,
        }
    }

    fn inbound_context(token: &str, operation_name: &str) -> IdentityOperationContext {
        IdentityOperationContext::from_inbound_event(
            IdentityOperationContextRef::new(format!("context-{token}")),
            IdentityOperationName::new(operation_name),
            ActorRef::system("worker"),
            identity_application::support::IdentityRequestMetadataRef::new(format!(
                "metadata-{token}"
            )),
            IdentityIdempotencyKey::new(format!("idem-{token}")),
            consumer_request_digest(token),
            None,
            IdentitySourceEventRef::new(format!("source-event-{token}")),
            timestamp(1),
        )
    }

    fn callback_context(token: &str, operation_name: &str) -> IdentityOperationContext {
        IdentityOperationContext::from_handoff_callback(
            IdentityOperationContextRef::new(format!("context-{token}")),
            IdentityOperationName::new(operation_name),
            ActorRef::system("worker"),
            identity_application::support::IdentityRequestMetadataRef::new(format!(
                "metadata-{token}"
            )),
            IdentityIdempotencyKey::new(format!("idem-{token}")),
            consumer_request_digest(token),
            None,
            IdentitySourceEventRef::new(format!("source-event-{token}")),
            timestamp(1),
        )
    }

    fn job_report(
        token: &str,
        stored_result_ref: Option<IdentityStoredResultRef>,
    ) -> IdentityJobRunReport {
        IdentityJobRunReport::start(
            IdentityJobReportRef::new(format!("job-report-{token}")),
            IdentityJobRunRef::new(format!("job-run-{token}")),
            IdentityJobName::new("RunIdentityReconciliation"),
            IdentityJobScopeMarkerRef::new(format!("job-scope-{token}")),
            Some(identity_contracts::refs::IdentityJobCursorRef::new(
                format!("job-input-cursor-{token}"),
            )),
            timestamp(1),
        )
        .partial(
            vec![maintenance_issue_ref(
                &format!("issue-{token}"),
                MaintenanceIssueKind::Partial,
            )],
            Some(identity_contracts::refs::IdentityJobCursorRef::new(
                format!("job-output-cursor-{token}"),
            )),
            stored_result_ref,
            timestamp(2),
        )
        .expect("partial report")
    }

    fn outbox_record(token: &str, state: OutboxState) -> IdentityOutboxRecord {
        IdentityOutboxRecord {
            outbox_record_ref: IdentityOutboxRecordRef::new(format!("outbox-{token}")),
            member_ref: member_ref("member-1"),
            subject_ref: IdentityOutboxSubjectRef::new(format!("subject-{token}")),
            change_kind_ref: identity_contracts::refs::IdentityChangeKindRef::new(
                IdentityChangeKind::DerivedMarkerChanged,
                None,
            ),
            payload_marker_ref: IdentityOutboxPayloadMarkerRef::new(format!("payload-{token}")),
            topic_key_ref: identity_application::outbound_material::AcceptedOutboundMaterialKind::GlobalLifecycleChanged
                .topic_key_ref(),
            trace_record_ref: IdentityTraceRecordRef::new(format!("trace-{token}")),
            outbox_state: state,
            created_at: timestamp(1),
            updated_at: timestamp(1),
        }
    }

    fn outbox_payload_marker_count(runtime: &IdentityInMemoryRuntime) -> usize {
        runtime
            .shared
            .store
            .lock()
            .expect("lock runtime store")
            .outbox_payload_markers
            .len()
    }

    fn assert_outbox_material_snapshot(
        runtime: &IdentityInMemoryRuntime,
        outbox_ref: &IdentityOutboxRecordRef,
        expected_kind: AcceptedOutboundMaterialKind,
    ) {
        let store = runtime.shared.store.lock().expect("lock runtime store");
        let stored = store
            .outbox_records
            .get(outbox_ref.as_str())
            .expect("stored outbox record");
        let snapshot = store
            .outbox_payload_markers
            .get(stored.record.payload_marker_ref.as_str())
            .expect("stored outbox payload marker");
        let trace = store
            .trace_records
            .get(stored.record.trace_record_ref.as_str())
            .expect("stored trace record");

        assert_eq!(stored.record.topic_key_ref, expected_kind.topic_key_ref());
        assert_eq!(snapshot.event_name, expected_kind.event_name());
        assert_eq!(
            snapshot.schema_version_ref,
            expected_kind.schema_version_ref()
        );
        assert_eq!(snapshot.subject_ref, stored.record.subject_ref);
        assert_eq!(snapshot.trace_record_ref, stored.record.trace_record_ref);
        assert!(!trace.trace.source_cursor_ref.as_str().is_empty());
    }

    #[test]
    fn projection_rebuild_race_preserves_newer_state() {
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_projection_state(projection_state("cursor-new"), IdentityVersion::new(3))
            .build();

        let uow = runtime.begin().expect("uow");
        let older_state = projection_state("cursor-old");
        runtime
            .save_projection_state(older_state, Some(IdentityVersion::new(2)), uow.as_ref())
            .expect("stage");
        let error = runtime
            .commit(uow)
            .expect_err("older rebuild snapshot must be rejected");
        assert_eq!(error.kind, ApplicationErrorKind::OptimisticVersionConflict);

        let persisted = runtime
            .get_projection_state_with_version(projection_ref("projection-1"))
            .expect("load")
            .expect("state");
        assert_eq!(persisted.version, IdentityVersion::new(3));
        assert_eq!(
            persisted.value.source_cursor_ref,
            Some(projection_cursor("cursor-new"))
        );
    }

    #[test]
    fn reference_refresh_preserves_last_good_snapshot() {
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_reference_state(
                reference_state_resolved(),
                ExternalReferenceTypedSidecarRefs {
                    role_capability_safe_summary_ref: Some(ExternalReferenceSafeSummaryRef::new(
                        external_reference(),
                        identity_source_ref(IdentitySourceOwner::MethodLibrary, "safe-summary-1"),
                    )),
                    career_safe_summary_ref: None,
                    memory_safe_summary_ref: None,
                    governance_basis_summary_ref: None,
                    evidence_summary_ref: None,
                    source_version_ref: Some(ExternalSourceVersionRef::new(identity_source_ref(
                        IdentitySourceOwner::MethodLibrary,
                        "source-version-1",
                    ))),
                },
                IdentityVersion::new(4),
            )
            .build();

        let unavailable = ReferenceResolutionState::unavailable(
            ReferenceResolutionStateRef::from_id(
                identity_contracts::refs::ReferenceResolutionStateId::new(
                    "reference-state-1".to_owned(),
                )
                .expect("reference state id"),
            ),
            external_reference(),
            reference_owner(),
            maintenance_issue_ref("reference-unavailable", MaintenanceIssueKind::Unavailable),
            timestamp(2),
        );

        let uow = runtime.begin().expect("uow");
        runtime
            .save_reference_state(unavailable, Some(IdentityVersion::new(4)), uow.as_ref())
            .expect("stage");
        runtime.commit(uow).expect("commit");

        let persisted = runtime
            .get_reference_state_with_version(external_reference())
            .expect("load")
            .expect("state");
        assert_eq!(persisted.version, IdentityVersion::new(5));
        assert_eq!(
            persisted.value.state_kind,
            ReferenceResolutionStateKind::Unavailable
        );
        assert!(
            runtime
                .get_typed_sidecar_refs(external_reference())
                .expect("sidecars")
                .role_capability_safe_summary_ref
                .is_some()
        );
    }

    #[test]
    fn save_typed_sidecars_sees_staged_reference_bundle() {
        let runtime = IdentityInMemoryRuntime::builder().build();
        let uow = runtime.begin().expect("uow");
        let bundle = reference_state_resolved();
        let staged = runtime
            .save_reference_state(bundle.clone(), None, uow.as_ref())
            .expect("stage reference bundle");

        let updated = runtime
            .save_typed_sidecar_refs(
                bundle.external_reference_ref.clone(),
                ExternalReferenceTypedSidecarRefs {
                    role_capability_safe_summary_ref: Some(ExternalReferenceSafeSummaryRef::new(
                        bundle.external_reference_ref.clone(),
                        identity_source_ref(
                            IdentitySourceOwner::MethodLibrary,
                            "role-summary-sidecar-1",
                        ),
                    )),
                    career_safe_summary_ref: None,
                    memory_safe_summary_ref: None,
                    governance_basis_summary_ref: None,
                    evidence_summary_ref: None,
                    source_version_ref: Some(ExternalSourceVersionRef::new(identity_source_ref(
                        IdentitySourceOwner::MethodLibrary,
                        "sidecar-version-1",
                    ))),
                },
                staged.version,
                uow.as_ref(),
            )
            .expect("stage sidecars");
        assert_eq!(updated.value_ref, staged.value_ref);
        assert_eq!(updated.version, IdentityVersion::new(2));

        runtime.commit(uow).expect("commit");

        let persisted = runtime
            .get_reference_state_with_version(bundle.external_reference_ref.clone())
            .expect("load bundle")
            .expect("bundle present");
        assert_eq!(persisted.version, IdentityVersion::new(2));
        assert_eq!(
            persisted.value.resolution_state_ref,
            bundle.resolution_state_ref
        );
        assert_eq!(
            runtime
                .get_typed_sidecar_refs(bundle.external_reference_ref)
                .expect("load sidecars")
                .source_version_ref,
            Some(ExternalSourceVersionRef::new(identity_source_ref(
                IdentitySourceOwner::MethodLibrary,
                "sidecar-version-1",
            )))
        );
    }

    #[test]
    fn external_reference_resolver_returns_seeded_outcome_and_rejects_missing_seed() {
        let seeded = reference_state_resolved();
        let seeded_outcome = ExternalReferenceResolutionOutcome {
            state: seeded.clone(),
            typed_sidecar_refs: Some(empty_sidecars()),
        };
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_external_reference_resolution_outcome(seeded_outcome.clone())
            .build();

        let loaded = runtime
            .resolve_external_reference(seeded.external_reference_ref.clone(), reference_owner())
            .expect("seeded bundle");
        assert_eq!(loaded, seeded_outcome);

        let err = runtime
            .resolve_external_reference(
                ExternalReferenceRef::new(
                    ExternalReferenceKind::MethodSource,
                    identity_source_ref(IdentitySourceOwner::MethodLibrary, "reference-missing-1"),
                ),
                reference_owner(),
            )
            .expect_err("missing seed");
        assert_eq!(err.kind, ApplicationErrorKind::NotFound);
    }

    #[test]
    fn handoff_delivered_requires_formal_receipt() {
        let mut intent = handoff_intent();
        intent.handoff_state = HandoffState {
            state_kind: HandoffStateKind::Delivered,
            attempt_ref: Some(HandoffAttemptRef::new(identity_source_ref(
                IdentitySourceOwner::Identity,
                "attempt-1",
            ))),
            receipt_ref: None,
            issue_ref: None,
            changed_at: timestamp(2),
        };

        let runtime = IdentityInMemoryRuntime::builder()
            .seed_handoff_intent(handoff_intent(), IdentityVersion::new(2))
            .seed_adapter_availability(adapter_availability())
            .build();

        let uow = runtime.begin().expect("uow");
        runtime
            .save_handoff_intent(intent, Some(IdentityVersion::new(2)), uow.as_ref())
            .expect("stage");
        let error = runtime
            .commit(uow)
            .expect_err("missing receipt must reject delivered");
        assert_eq!(error.kind, ApplicationErrorKind::DomainRejected);

        let persisted = runtime
            .get_handoff_intent_with_version(TraceHandoffIntentRef::new("handoff-1"))
            .expect("load")
            .expect("intent");
        assert_eq!(persisted.version, IdentityVersion::new(2));
        assert_eq!(
            persisted.value.handoff_state.state_kind,
            HandoffStateKind::PendingHandoff
        );
    }

    #[test]
    fn rollback_failure_surfaces_manual_intervention_without_hidden_writes() {
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_projection_state(projection_state("cursor-1"), IdentityVersion::new(1))
            .inject_fault(FaultCase::RollbackFails)
            .build();

        let mut degraded = projection_state("cursor-1");
        degraded
            .mark_degraded(
                maintenance_issue_ref("projection-issue", MaintenanceIssueKind::Failed),
                timestamp(2),
            )
            .expect("mark degraded");

        let uow = runtime.begin().expect("uow");
        runtime
            .save_projection_state(degraded, Some(IdentityVersion::new(1)), uow.as_ref())
            .expect("stage");
        let error = runtime.rollback(uow).expect_err("rollback must fail");
        assert_eq!(error.kind, ApplicationErrorKind::ConsistencyDefect);
        assert!(error.message.contains("manual intervention"));

        let persisted = runtime
            .get_projection_state_with_version(projection_ref("projection-1"))
            .expect("load")
            .expect("state");
        assert_eq!(persisted.version, IdentityVersion::new(1));
        assert_eq!(persisted.value.state_kind, ProjectionStateKind::Stale);
    }

    #[test]
    fn idempotency_namespace_isolated_by_operation_and_channel() {
        let command_context = command_context("establish_member", "idem-shared", "same");
        let consumer_context = IdentityOperationContext::from_inbound_event(
            IdentityOperationContextRef::new("context-consumer"),
            IdentityOperationName::new("handle_role_source_changed"),
            ActorRef::system("worker"),
            identity_application::support::IdentityRequestMetadataRef::new("metadata-consumer"),
            IdentityIdempotencyKey::new("idem-shared"),
            request_digest("same"),
            None,
            identity_contracts::refs::IdentitySourceEventRef::new("source-event-1"),
            timestamp(1),
        );
        let job_context = job_context("refresh_reference", "idem-shared", "same", "job-run-1");

        let runtime = IdentityInMemoryRuntime::builder().build();

        let command_uow = runtime.begin().expect("command uow");
        let command_result = runtime
            .reserve(
                command_context.clone(),
                IdentityIdempotencyRecordRef::new("idem-record-command"),
                timestamp(1),
                command_uow.as_ref(),
            )
            .expect("reserve command");
        runtime.commit(command_uow).expect("commit command");
        assert!(matches!(
            command_result,
            IdempotencyReserveOutcome::Reserved(_)
        ));

        let consumer_uow = runtime.begin().expect("consumer uow");
        let consumer_result = runtime
            .reserve(
                consumer_context.clone(),
                IdentityIdempotencyRecordRef::new("idem-record-consumer"),
                timestamp(1),
                consumer_uow.as_ref(),
            )
            .expect("reserve consumer");
        runtime.commit(consumer_uow).expect("commit consumer");
        assert!(matches!(
            consumer_result,
            IdempotencyReserveOutcome::Reserved(_)
        ));

        let job_uow = runtime.begin().expect("job uow");
        let job_result = runtime
            .reserve(
                job_context.clone(),
                IdentityIdempotencyRecordRef::new("idem-record-job"),
                timestamp(1),
                job_uow.as_ref(),
            )
            .expect("reserve job");
        runtime.commit(job_uow).expect("commit job");
        assert!(matches!(job_result, IdempotencyReserveOutcome::Reserved(_)));

        assert!(
            runtime
                .get_by_key(
                    command_context.operation_name,
                    IdentityOperationChannel::Command,
                    IdentityIdempotencyKey::new("idem-shared"),
                )
                .expect("load command")
                .is_some()
        );
        assert!(
            runtime
                .get_by_key(
                    consumer_context.operation_name,
                    IdentityOperationChannel::Consumer,
                    IdentityIdempotencyKey::new("idem-shared"),
                )
                .expect("load consumer")
                .is_some()
        );
        assert!(
            runtime
                .get_by_key(
                    job_context.operation_name,
                    IdentityOperationChannel::Job,
                    IdentityIdempotencyKey::new("idem-shared"),
                )
                .expect("load job")
                .is_some()
        );
    }

    #[test]
    fn duplicate_missing_stored_result_does_not_recompute() {
        let context = command_context("establish_member", "idem-1", "same");
        let record = IdentityIdempotencyRecord {
            record_ref: IdentityIdempotencyRecordRef::new("idem-record-1"),
            operation_name: context.operation_name.clone(),
            channel: IdentityOperationChannel::Command,
            idempotency_key: IdentityIdempotencyKey::new("idem-1"),
            request_digest: context.request_digest.clone(),
            state: identity_application::support::IdentityIdempotencyStateKind::Completed,
            stored_result_ref: Some(stored_result_ref("missing")),
            reserved_at: timestamp(1),
            completed_at: Some(timestamp(2)),
        };
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_idempotency_record(record, IdentityVersion::new(2))
            .build();

        let uow = runtime.begin().expect("uow");
        let outcome = runtime
            .reserve(
                context,
                IdentityIdempotencyRecordRef::new("idem-record-new"),
                timestamp(3),
                uow.as_ref(),
            )
            .expect("reserve");
        match outcome {
            IdempotencyReserveOutcome::ReplayAvailable {
                record,
                stored_result_ref,
            } => {
                assert_eq!(record.version, IdentityVersion::new(2));
                assert_eq!(
                    stored_result_ref,
                    IdentityStoredResultRef::new("stored-result-missing")
                );
            }
            other => panic!("unexpected reserve outcome: {other:?}"),
        }
        assert!(
            runtime
                .get_stored_result(IdentityStoredResultRef::new("stored-result-missing"))
                .expect("lookup")
                .is_none()
        );
        runtime.rollback(uow).expect("rollback");
    }

    #[test]
    fn consumer_duplicate_replays_typed_receipt() {
        let context = IdentityOperationContext::from_inbound_event(
            IdentityOperationContextRef::new("context-consumer-1"),
            IdentityOperationName::new("handle_role_source_changed"),
            ActorRef::system("worker"),
            identity_application::support::IdentityRequestMetadataRef::new("metadata-consumer"),
            IdentityIdempotencyKey::new("idem-consumer-1"),
            request_digest("consumer"),
            None,
            identity_contracts::refs::IdentitySourceEventRef::new("source-event-1"),
            timestamp(1),
        );
        let record = IdentityIdempotencyRecord {
            record_ref: IdentityIdempotencyRecordRef::new("idem-record-consumer"),
            operation_name: context.operation_name.clone(),
            channel: IdentityOperationChannel::Consumer,
            idempotency_key: IdentityIdempotencyKey::new("idem-consumer-1"),
            request_digest: context.request_digest.clone(),
            state: identity_application::support::IdentityIdempotencyStateKind::Completed,
            stored_result_ref: Some(stored_result_ref("consumer-1")),
            reserved_at: timestamp(1),
            completed_at: Some(timestamp(2)),
        };
        let stored = StoredIdentityOperationResult::consumer_receipt(
            stored_result_ref("consumer-1"),
            context.context_ref.clone(),
            identity_application::support::IdentityStoredSurfaceMarkerRef::new(
                "surface-consumer-1",
            ),
            timestamp(2),
        );
        let envelope = consumer_receipt_envelope("consumer-1", &context.context_ref);

        let runtime = IdentityInMemoryRuntime::builder()
            .seed_idempotency_record(record, IdentityVersion::new(2))
            .seed_stored_result(stored)
            .seed_consumer_receipt(envelope.clone())
            .build();

        let uow = runtime.begin().expect("uow");
        let outcome = runtime
            .reserve(
                context,
                IdentityIdempotencyRecordRef::new("idem-record-consumer-new"),
                timestamp(3),
                uow.as_ref(),
            )
            .expect("reserve");
        let replay_ref = match outcome {
            IdempotencyReserveOutcome::ReplayAvailable {
                stored_result_ref, ..
            } => stored_result_ref,
            other => panic!("unexpected outcome: {other:?}"),
        };
        let replay = runtime
            .get_consumer_receipt(replay_ref)
            .expect("load receipt")
            .expect("receipt");
        assert_eq!(replay, envelope);
        runtime.rollback(uow).expect("rollback");
    }

    #[test]
    fn inbound_consumer_scaffold_duplicate_replays_without_running_handler() {
        let context = inbound_context("consumer-scaffold-1", "HandleRoleCapabilitySourceChanged");
        let stored = StoredIdentityOperationResult::consumer_receipt(
            stored_result_ref("consumer-scaffold-1"),
            context.context_ref.clone(),
            identity_application::support::IdentityStoredSurfaceMarkerRef::new(
                "surface-consumer-scaffold-1",
            ),
            timestamp(2),
        );
        let record = IdentityIdempotencyRecord {
            record_ref: IdentityIdempotencyRecordRef::new("idem-record-consumer-scaffold-1"),
            operation_name: context.operation_name.clone(),
            channel: IdentityOperationChannel::Consumer,
            idempotency_key: IdentityIdempotencyKey::new("idem-consumer-scaffold-1"),
            request_digest: context.request_digest.clone(),
            state: identity_application::support::IdentityIdempotencyStateKind::Completed,
            stored_result_ref: Some(stored_result_ref("consumer-scaffold-1")),
            reserved_at: timestamp(1),
            completed_at: Some(timestamp(2)),
        };
        let stored_receipt = consumer_receipt_envelope("consumer-scaffold-1", &context.context_ref);
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_idempotency_record(record, IdentityVersion::new(2))
            .seed_stored_result(stored)
            .seed_consumer_receipt(stored_receipt.clone())
            .build();
        let service = consumer_service(&runtime);
        let handler_calls = Cell::new(0usize);
        let envelope = inbound_event_envelope(
            "consumer-scaffold-1",
            "HandleRoleCapabilitySourceChanged",
            "identity.consumer.v1",
        );

        let receipt = service
            .dispatch_inbound_event_scaffold(
                context,
                &envelope,
                &IdentityProtocolSchemaVersionRef::new("identity.consumer.v1"),
                |_, _, _| {
                    handler_calls.set(handler_calls.get() + 1);
                    unreachable!("duplicate replay must not dispatch payload handler");
                },
            )
            .expect("duplicate replay");

        assert_eq!(handler_calls.get(), 0);
        assert_eq!(receipt, stored_receipt.receipt);
        assert_eq!(receipt.outcome, IdentityConsumerOutcome::DuplicateReplayed);
    }

    #[test]
    fn unsupported_schema_scaffold_returns_fresh_receipt_and_persists_replay_surface() {
        let runtime = IdentityInMemoryRuntime::builder().build();
        let service = consumer_service(&runtime);
        let context = inbound_context("unsupported-schema-1", "HandleRoleCapabilitySourceChanged");
        let envelope = inbound_event_envelope(
            "unsupported-schema-1",
            "HandleRoleCapabilitySourceChanged",
            "identity.consumer.v0",
        );
        let handler_calls = Cell::new(0usize);

        let first = service
            .dispatch_inbound_event_scaffold(
                context.clone(),
                &envelope,
                &IdentityProtocolSchemaVersionRef::new("identity.consumer.v1"),
                |_, _, _| {
                    handler_calls.set(handler_calls.get() + 1);
                    unreachable!("unsupported schema must not dispatch payload handler");
                },
            )
            .expect("unsupported receipt");

        assert_eq!(handler_calls.get(), 0);
        assert_eq!(first.outcome, IdentityConsumerOutcome::UnsupportedVersion);
        assert_eq!(first.trace_refs, Vec::<IdentityTraceRecordRef>::new());
        assert_eq!(first.outbox_refs, Vec::<IdentityOutboxRecordRef>::new());
        assert_eq!(first.issue_refs.len(), 1);
        assert_eq!(
            first.issue_refs[0],
            identity_contracts::metadata::IdentityProtocolValidationIssueRef::new(
                "unsupported-schema:identity.consumer.v0",
            )
        );

        let stored = runtime
            .get_stored_result(first.stored_result_ref.clone())
            .expect("load stored shell")
            .expect("stored shell");
        assert_eq!(
            stored.result_kind,
            identity_application::support::IdentityStoredResultKind::ConsumerReceipt
        );

        let replay_envelope = runtime
            .get_consumer_receipt(first.stored_result_ref.clone())
            .expect("load stored receipt")
            .expect("stored receipt");
        assert_eq!(
            replay_envelope.receipt.outcome,
            IdentityConsumerOutcome::DuplicateReplayed
        );
        assert_eq!(replay_envelope.receipt.issue_refs, first.issue_refs);

        let replay = service
            .dispatch_inbound_event_scaffold(
                context,
                &envelope,
                &IdentityProtocolSchemaVersionRef::new("identity.consumer.v1"),
                |_, _, _| {
                    handler_calls.set(handler_calls.get() + 1);
                    unreachable!("duplicate replay must not dispatch payload handler");
                },
            )
            .expect("replayed unsupported receipt");

        assert_eq!(handler_calls.get(), 0);
        assert_eq!(replay.outcome, IdentityConsumerOutcome::DuplicateReplayed);
        assert_eq!(replay.stored_result_ref, first.stored_result_ref);
        assert_eq!(replay.issue_refs, first.issue_refs);
    }

    #[test]
    fn callback_scaffold_duplicate_replays_handoff_callback_receipt_without_handler() {
        let context = callback_context("callback-scaffold-1", "HandleTraceHandoffResult");
        let stored = StoredIdentityOperationResult::handoff_callback_receipt(
            stored_result_ref("callback-scaffold-1"),
            context.context_ref.clone(),
            identity_application::support::IdentityStoredSurfaceMarkerRef::new(
                "surface-callback-scaffold-1",
            ),
            timestamp(2),
        );
        let record = IdentityIdempotencyRecord {
            record_ref: IdentityIdempotencyRecordRef::new("idem-record-callback-scaffold-1"),
            operation_name: context.operation_name.clone(),
            channel: IdentityOperationChannel::HandoffCallback,
            idempotency_key: IdentityIdempotencyKey::new("idem-callback-scaffold-1"),
            request_digest: context.request_digest.clone(),
            state: identity_application::support::IdentityIdempotencyStateKind::Completed,
            stored_result_ref: Some(stored_result_ref("callback-scaffold-1")),
            reserved_at: timestamp(1),
            completed_at: Some(timestamp(2)),
        };
        let stored_receipt =
            handoff_callback_receipt_envelope("callback-scaffold-1", &context.context_ref);
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_idempotency_record(record, IdentityVersion::new(2))
            .seed_stored_result(stored)
            .seed_handoff_callback_receipt(stored_receipt.clone())
            .build();
        let service = consumer_service(&runtime);
        let handler_calls = Cell::new(0usize);
        let envelope = inbound_event_envelope(
            "callback-scaffold-1",
            "HandleTraceHandoffResult",
            "identity.consumer.v1",
        );

        let receipt = service
            .dispatch_callback_scaffold(
                context,
                &envelope,
                &IdentityProtocolSchemaVersionRef::new("identity.consumer.v1"),
                |_, _, _| {
                    handler_calls.set(handler_calls.get() + 1);
                    unreachable!("duplicate callback replay must not dispatch payload handler");
                },
            )
            .expect("callback duplicate replay");

        assert_eq!(handler_calls.get(), 0);
        assert_eq!(receipt, stored_receipt.receipt);
        assert_eq!(receipt.outcome, IdentityConsumerOutcome::DuplicateReplayed);
        assert_eq!(
            runtime
                .get_handoff_callback_receipt(stored_result_ref("callback-scaffold-1"))
                .expect("load callback envelope")
                .expect("callback envelope"),
            stored_receipt
        );
    }

    #[test]
    fn handle_role_capability_source_changed_accepts_and_replays() {
        let member_ref = member_ref("member-role-consumer-1");
        let member = GlobalMember::establish(
            member_ref.clone(),
            identity_source_ref(
                IdentitySourceOwner::Identity,
                "member-source-role-consumer-1",
            ),
            ActorRef::new("actor-1", ActorKind::Human),
            timestamp(1),
        )
        .expect("member");
        let source_ref = role_capability_source_ref("role-consumer-source-1");
        let payload = RoleCapabilitySourceChangedPayload {
            member_ref: member_ref.clone(),
            source_ref: source_ref.clone(),
            source_version_ref: role_source_version(&source_ref, "version-1"),
            source_state_kind:
                identity_contracts::refs::RoleCapabilitySourceStateKind::SourceResolved,
            safe_summary_ref: Some(
                RoleCapabilitySafeSummaryRef::new(source_ref.clone(), "safe-role-consumer-1")
                    .expect("role safe summary"),
            ),
            evidence_refs: vec![
                CapabilityEvidenceRef::new(
                    CapabilityEvidenceKind::MethodArtifact,
                    source_ref.source_ref.clone(),
                )
                .expect("evidence ref"),
            ],
            external_reference_ref: None,
            reference_owner_ref: None,
            change_reason_ref: Some(role_change_reason("role-consumer-reason-1")),
            material_marker: RoleCapabilityChangeMaterialMarker::new(
                RoleCapabilityChangeMaterialKind::SafeSummaryMarker,
                Some(source_ref.source_ref.clone()),
            ),
        };
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_member(member, IdentityVersion::new(1))
            .build();
        let service = consumer_service(&runtime);

        let first = service
            .handle_role_capability_source_changed(
                inbound_event_envelope_with_payload(
                    "role-consumer-1",
                    "HandleRoleCapabilitySourceChanged",
                    payload.clone(),
                ),
                inbound_context("role-consumer-1", "HandleRoleCapabilitySourceChanged"),
            )
            .expect("accepted");
        assert_eq!(first.outcome, IdentityConsumerOutcome::Accepted);
        assert_eq!(first.trace_refs.len(), 1);
        assert_eq!(first.outbox_refs.len(), 1);

        let snapshot = runtime
            .find_source_snapshot_by_source(source_ref.clone())
            .expect("load snapshot")
            .expect("snapshot");
        assert_eq!(
            snapshot.value.source_state,
            identity_domain::role_capability::RoleCapabilitySourceStateKind::SourceResolved
        );
        assert_eq!(
            snapshot.value.source_version_ref,
            payload.source_version_ref
        );

        let replay = service
            .handle_role_capability_source_changed(
                inbound_event_envelope_with_payload(
                    "role-consumer-1",
                    "HandleRoleCapabilitySourceChanged",
                    payload,
                ),
                inbound_context("role-consumer-1", "HandleRoleCapabilitySourceChanged"),
            )
            .expect("replay");
        assert_eq!(replay.outcome, IdentityConsumerOutcome::DuplicateReplayed);
        assert_eq!(replay.stored_result_ref, first.stored_result_ref);
        assert_eq!(
            runtime
                .list_trace_records_by_member(member_ref, IdentityRepositoryPage::new(None, 16))
                .expect("list traces")
                .items
                .len(),
            1
        );
    }

    #[test]
    fn handle_work_participation_accepted_source_duplicate_returns_noop() {
        let member_ref = member_ref("member-work-consumer-1");
        let member = GlobalMember::establish(
            member_ref.clone(),
            identity_source_ref(
                IdentitySourceOwner::Identity,
                "member-source-work-consumer-1",
            ),
            ActorRef::new("actor-1", ActorKind::Human),
            timestamp(1),
        )
        .expect("member");
        let work_token = format!("{}::{}", member_ref.id().as_str(), "work-consumer-source-1");
        let work_source_ref =
            work_source(&work_token, WorkSourceKind::ProjectParticipationAccepted);
        let project_participation_ref = project_participation(&work_token);
        let career_source_marker_ref = career_source_marker(
            &member_ref,
            &work_source_ref,
            "career-source-marker-consumer-1",
        );
        let safe_summary_ref =
            CareerSafeSummaryRef::new(work_source_ref.clone(), "career-safe-consumer-1")
                .expect("career safe summary");
        let existing = CareerRecord::append_from_work_source(
            identity_contracts::refs::CareerRecordRef::from_id(
                identity_contracts::refs::CareerRecordId::new(
                    "career-existing-consumer-1".to_owned(),
                )
                .expect("career record id"),
            ),
            member_ref.clone(),
            identity_contracts::refs::WorkParticipationSourceSummary::from_resolver(
                project_participation_ref.clone(),
                work_source_ref.clone(),
                career_source_marker_ref.clone(),
                Some(safe_summary_ref.clone()),
                identity_contracts::refs::WorkParticipationSourceState::Trusted,
            ),
            CareerAppendReasonRef::new(
                CareerAppendReasonKind::WorkParticipationAccepted,
                work_source_ref.source_ref.clone(),
            )
            .expect("append reason"),
            ActorRef::new("actor-1", ActorKind::Human),
            timestamp(1),
        )
        .expect("career record");
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_member(member, IdentityVersion::new(1))
            .seed_career_record(existing, IdentityVersion::new(1))
            .build();
        let service = consumer_service(&runtime);

        let receipt = service
            .handle_work_participation_accepted(
                inbound_event_envelope_with_payload(
                    "work-consumer-1",
                    "HandleWorkParticipationAccepted",
                    WorkParticipationAcceptedPayload {
                        member_ref: member_ref.clone(),
                        project_participation_ref,
                        work_source_ref: work_source_ref.clone(),
                        career_source_marker_ref: career_source_marker_ref.clone(),
                        safe_summary_ref,
                        append_reason_ref: Some(
                            CareerAppendReasonRef::new(
                                CareerAppendReasonKind::WorkParticipationAccepted,
                                work_source_ref.source_ref.clone(),
                            )
                            .expect("append reason"),
                        ),
                        material_marker: CareerAppendMaterialMarker {
                            material_kind: CareerAppendMaterialKind::SafeSummaryMarker,
                            source_ref: Some(work_source_ref.source_ref.clone()),
                        },
                    },
                ),
                inbound_context("work-consumer-1", "HandleWorkParticipationAccepted"),
            )
            .expect("noop");

        assert_eq!(receipt.outcome, IdentityConsumerOutcome::Noop);
        assert!(receipt.trace_refs.is_empty());
        assert!(receipt.outbox_refs.is_empty());
        assert_eq!(
            runtime
                .list_records_by_member(member_ref, IdentityRepositoryPage::new(None, 16))
                .expect("list career records")
                .items
                .len(),
            1
        );
    }

    #[test]
    fn handle_memory_reference_source_state_changed_missing_relation_does_not_create() {
        let member_ref = member_ref("member-memory-consumer-1");
        let member = GlobalMember::establish(
            member_ref.clone(),
            identity_source_ref(
                IdentitySourceOwner::Identity,
                "member-source-memory-consumer-1",
            ),
            ActorRef::new("actor-1", ActorKind::Human),
            timestamp(1),
        )
        .expect("member");
        let source_ref = memory_source_ref(
            "memory-consumer-source-1",
            MemoryReferenceSourceKind::MemorySourceEvent,
        );
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_member(member, IdentityVersion::new(1))
            .build();
        let service = consumer_service(&runtime);

        let receipt = service
            .handle_memory_reference_source_state_changed(
                inbound_event_envelope_with_payload(
                    "memory-consumer-1",
                    "HandleMemoryReferenceSourceStateChanged",
                    MemoryReferenceSourceStateChangedPayload {
                        member_ref: member_ref.clone(),
                        memory_reference_ref: None,
                        source_ref: source_ref.clone(),
                        memory_ref: Some(
                            MemoryRef::from_source(identity_source_ref(
                                IdentitySourceOwner::MemoryArchive,
                                "memory-carrier-consumer-1",
                            ))
                            .expect("memory ref"),
                        ),
                        archive_ref: None,
                        target_state_kind: PublicMemoryReferenceStateKind::Stale,
                        safe_summary_ref: None,
                        external_reference_ref: None,
                        reference_owner_ref: None,
                        reason_ref: Some(memory_reason(
                            "memory-consumer-reason-1",
                            MemoryReferenceReasonKind::SourceStateChanged,
                        )),
                        material_marker: MemoryReferenceChangeMaterialMarker {
                            material_kind: MemoryReferenceChangeMaterialKind::ReferenceMarkersOnly,
                            source_ref: Some(source_ref.source_ref.clone()),
                        },
                    },
                ),
                inbound_context(
                    "memory-consumer-1",
                    "HandleMemoryReferenceSourceStateChanged",
                ),
            )
            .expect("quarantined");

        assert_eq!(receipt.outcome, IdentityConsumerOutcome::Quarantined);
        assert!(
            runtime
                .list_references_by_member(member_ref, IdentityRepositoryPage::new(None, 16))
                .expect("list memory references")
                .items
                .is_empty()
        );
        assert!(
            runtime
                .list_pending_outbox_records(None, IdentityRepositoryPage::new(None, 16))
                .expect("list outbox")
                .items
                .is_empty()
        );
    }

    #[test]
    fn handle_archive_handoff_result_target_mismatch_rejects_without_mutation() {
        let member_ref = member_ref("member-archive-callback-1");
        let member = GlobalMember::establish(
            member_ref.clone(),
            identity_source_ref(
                IdentitySourceOwner::Identity,
                "member-source-archive-callback-1",
            ),
            ActorRef::new("actor-1", ActorKind::Human),
            timestamp(1),
        )
        .expect("member");
        let direct = linked_memory_reference(
            "memory-direct-archive-callback-1",
            member_ref.clone(),
            "memory-direct-source-archive-callback-1",
        );
        let callback_target = archived_memory_reference(
            "memory-target-archive-callback-1",
            member_ref.clone(),
            "archive-target-source-1",
            "archive-callback-handoff-1",
        );
        let archive_ref = callback_target.archive_ref.clone().expect("archive ref");
        let archive_handoff_ref = callback_target
            .archive_handoff_ref
            .clone()
            .expect("archive handoff ref");
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_member(member, IdentityVersion::new(1))
            .seed_memory_reference(direct.clone(), IdentityVersion::new(1))
            .seed_memory_reference(callback_target.clone(), IdentityVersion::new(1))
            .build();
        let service = consumer_service(&runtime);

        let receipt = service
            .handle_archive_handoff_result(
                inbound_event_envelope_with_payload(
                    "archive-callback-mismatch-1",
                    "HandleArchiveHandoffResult",
                    ArchiveHandoffResultPayload {
                        member_ref: member_ref.clone(),
                        memory_reference_ref: Some(direct.memory_reference_ref.clone()),
                        archive_ref,
                        archive_handoff_ref: archive_handoff_ref.clone(),
                        target_state_kind: PublicMemoryReferenceStateKind::Archived,
                        reason_ref: Some(memory_reason(
                            "archive-callback-reason-1",
                            MemoryReferenceReasonKind::ArchiveHandoffResult,
                        )),
                        issue_ref: None,
                        material_marker: MemoryReferenceChangeMaterialMarker {
                            material_kind: MemoryReferenceChangeMaterialKind::HandoffMarkerOnly,
                            source_ref: Some(archive_handoff_ref.source_ref.clone()),
                        },
                    },
                ),
                callback_context("archive-callback-mismatch-1", "HandleArchiveHandoffResult"),
            )
            .expect("rejected");

        assert_eq!(receipt.outcome, IdentityConsumerOutcome::Rejected);
        assert_eq!(
            runtime
                .get_memory_reference_with_version(direct.memory_reference_ref.clone())
                .expect("load direct relation")
                .expect("direct relation")
                .version,
            IdentityVersion::new(1)
        );
        assert_eq!(
            runtime
                .get_memory_reference_with_version(callback_target.memory_reference_ref)
                .expect("load callback target relation")
                .expect("callback target relation")
                .version,
            IdentityVersion::new(1)
        );
        assert!(
            runtime
                .list_pending_outbox_records(None, IdentityRepositoryPage::new(None, 16))
                .expect("list outbox")
                .items
                .is_empty()
        );
    }

    #[test]
    fn handle_trace_handoff_result_delivered_requires_receipt() {
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_handoff_intent(handoff_intent(), IdentityVersion::new(1))
            .build();
        let service = consumer_service(&runtime);

        let receipt = service
            .handle_trace_handoff_result(
                inbound_event_envelope_with_payload(
                    "trace-callback-missing-receipt-1",
                    "HandleTraceHandoffResult",
                    TraceHandoffResultPayload {
                        handoff_intent_ref: TraceHandoffIntentRef::new("handoff-1"),
                        handoff_target_ref: HandoffTargetRef::new("target-1"),
                        handoff_scope_ref: Some(HandoffScopeRef::new("scope-1")),
                        attempt_ref: HandoffAttemptRef::new(identity_source_ref(
                            IdentitySourceOwner::Identity,
                            "attempt-trace-callback-missing-receipt-1",
                        )),
                        result_kind: TraceHandoffResultKind::Delivered,
                        receipt_ref: None,
                        issue_ref: None,
                    },
                ),
                callback_context(
                    "trace-callback-missing-receipt-1",
                    "HandleTraceHandoffResult",
                ),
            )
            .expect("rejected");

        assert_eq!(receipt.outcome, IdentityConsumerOutcome::Rejected);
        let persisted = runtime
            .get_handoff_intent_with_version(TraceHandoffIntentRef::new("handoff-1"))
            .expect("load handoff intent")
            .expect("handoff intent");
        assert_eq!(persisted.version, IdentityVersion::new(1));
        assert_eq!(
            persisted.value.handoff_state.state_kind,
            HandoffStateKind::PendingHandoff
        );
        assert_eq!(
            runtime
                .get_handoff_callback_receipt(receipt.stored_result_ref.clone())
                .expect("load callback receipt")
                .expect("callback receipt")
                .result_kind,
            IdentityStoredResultKind::HandoffCallbackReceipt
        );
        assert!(
            runtime
                .list_pending_outbox_records(None, IdentityRepositoryPage::new(None, 16))
                .expect("list outbox")
                .items
                .is_empty()
        );
    }

    #[test]
    fn handle_trace_handoff_result_delivered_replays_stored_receipt() {
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_handoff_intent(handoff_intent(), IdentityVersion::new(1))
            .build();
        let service = consumer_service(&runtime);
        let payload = TraceHandoffResultPayload {
            handoff_intent_ref: TraceHandoffIntentRef::new("handoff-1"),
            handoff_target_ref: HandoffTargetRef::new("target-1"),
            handoff_scope_ref: Some(HandoffScopeRef::new("scope-1")),
            attempt_ref: HandoffAttemptRef::new(identity_source_ref(
                IdentitySourceOwner::Identity,
                "attempt-trace-callback-accepted-1",
            )),
            result_kind: TraceHandoffResultKind::Delivered,
            receipt_ref: Some(HandoffReceiptRef::new("handoff-receipt-consumer-1")),
            issue_ref: None,
        };

        let first = service
            .handle_trace_handoff_result(
                inbound_event_envelope_with_payload(
                    "trace-callback-accepted-1",
                    "HandleTraceHandoffResult",
                    payload.clone(),
                ),
                callback_context("trace-callback-accepted-1", "HandleTraceHandoffResult"),
            )
            .expect("accepted");
        assert_eq!(first.outcome, IdentityConsumerOutcome::Accepted);
        assert_eq!(first.trace_refs.len(), 2);
        assert_eq!(first.outbox_refs.len(), 1);
        assert_eq!(outbox_payload_marker_count(&runtime), 1);
        assert_outbox_material_snapshot(
            &runtime,
            &first.outbox_refs[0],
            AcceptedOutboundMaterialKind::MemoryArchiveHandoffStateChanged,
        );

        let persisted = runtime
            .get_handoff_intent_with_version(TraceHandoffIntentRef::new("handoff-1"))
            .expect("load handoff intent")
            .expect("handoff intent");
        assert_eq!(persisted.version, IdentityVersion::new(2));
        assert_eq!(
            persisted.value.handoff_state.state_kind,
            HandoffStateKind::Delivered
        );
        assert_eq!(
            persisted.value.handoff_state.receipt_ref,
            Some(HandoffReceiptRef::new("handoff-receipt-consumer-1"))
        );
        assert_eq!(
            runtime
                .get_handoff_callback_receipt(first.stored_result_ref.clone())
                .expect("load stored callback receipt")
                .expect("stored callback receipt")
                .result_kind,
            IdentityStoredResultKind::HandoffCallbackReceipt
        );
        assert_eq!(
            runtime
                .list_trace_records_by_member(
                    member_ref("member-1"),
                    IdentityRepositoryPage::new(None, 16)
                )
                .expect("list traces")
                .items
                .len(),
            2
        );
        assert_eq!(
            runtime
                .list_pending_outbox_records(None, IdentityRepositoryPage::new(None, 16))
                .expect("list outbox")
                .items
                .len(),
            1
        );

        let replay = service
            .handle_trace_handoff_result(
                inbound_event_envelope_with_payload(
                    "trace-callback-accepted-1",
                    "HandleTraceHandoffResult",
                    payload,
                ),
                callback_context("trace-callback-accepted-1", "HandleTraceHandoffResult"),
            )
            .expect("replay");
        assert_eq!(replay.outcome, IdentityConsumerOutcome::DuplicateReplayed);
        assert_eq!(replay.stored_result_ref, first.stored_result_ref);
        assert_eq!(
            runtime
                .get_handoff_intent_with_version(TraceHandoffIntentRef::new("handoff-1"))
                .expect("reload handoff intent")
                .expect("handoff intent")
                .version,
            IdentityVersion::new(2)
        );
        assert_eq!(
            runtime
                .list_trace_records_by_member(
                    member_ref("member-1"),
                    IdentityRepositoryPage::new(None, 16)
                )
                .expect("list traces after replay")
                .items
                .len(),
            2
        );
        assert_eq!(
            runtime
                .list_pending_outbox_records(None, IdentityRepositoryPage::new(None, 16))
                .expect("list outbox after replay")
                .items
                .len(),
            1
        );
        assert_eq!(outbox_payload_marker_count(&runtime), 1);
    }

    #[test]
    fn job_duplicate_replays_stored_report() {
        let context = job_context("run_reconciliation", "idem-job-1", "job", "job-run-job-1");
        let stored = StoredIdentityOperationResult::job_report(
            stored_result_ref("job-1"),
            context.context_ref.clone(),
            identity_application::support::IdentityStoredSurfaceMarkerRef::new("surface-job-1"),
            timestamp(2),
        );
        let report = job_report("job-1", Some(stored_result_ref("job-1")));
        let record = IdentityIdempotencyRecord {
            record_ref: IdentityIdempotencyRecordRef::new("idem-record-job-1"),
            operation_name: context.operation_name.clone(),
            channel: IdentityOperationChannel::Job,
            idempotency_key: IdentityIdempotencyKey::new("idem-job-1"),
            request_digest: context.request_digest.clone(),
            state: identity_application::support::IdentityIdempotencyStateKind::Completed,
            stored_result_ref: Some(stored_result_ref("job-1")),
            reserved_at: timestamp(1),
            completed_at: Some(timestamp(2)),
        };
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_idempotency_record(record, IdentityVersion::new(2))
            .seed_stored_result(stored)
            .seed_job_report(report.clone(), IdentityVersion::new(1))
            .build();

        let outcome = runtime
            .reserve(
                context,
                IdentityIdempotencyRecordRef::new("idem-record-job-1-new"),
                timestamp(3),
                runtime.begin().expect("uow").as_ref(),
            )
            .expect("reserve");
        let replay_ref = match outcome {
            IdempotencyReserveOutcome::ReplayAvailable {
                stored_result_ref, ..
            } => stored_result_ref,
            other => panic!("unexpected outcome: {other:?}"),
        };
        let stored = runtime
            .get_stored_result(replay_ref)
            .expect("load stored")
            .expect("stored");
        assert_eq!(stored.result_kind, IdentityStoredResultKind::JobReport);
        let persisted = runtime
            .find_job_report_by_run(IdentityJobRunRef::new("job-run-job-1"))
            .expect("lookup")
            .expect("report");
        assert_eq!(persisted.value, report);
    }

    #[test]
    fn job_dispatch_duplicate_replay_does_not_run_handler() {
        let context = job_context(
            "RunIdentityReconciliation",
            "idem-job-dup",
            "job",
            "job-run-dup",
        );
        let stored = StoredIdentityOperationResult::job_report(
            stored_result_ref("job-dup"),
            context.context_ref.clone(),
            identity_application::support::IdentityStoredSurfaceMarkerRef::new("surface-job-dup"),
            timestamp(2),
        );
        let report = job_report("dup", Some(stored_result_ref("job-dup")));
        let record = IdentityIdempotencyRecord {
            record_ref: IdentityIdempotencyRecordRef::new("idem-record-job-dup"),
            operation_name: context.operation_name.clone(),
            channel: IdentityOperationChannel::Job,
            idempotency_key: IdentityIdempotencyKey::new("idem-job-dup"),
            request_digest: context.request_digest.clone(),
            state: identity_application::support::IdentityIdempotencyStateKind::Completed,
            stored_result_ref: Some(stored_result_ref("job-dup")),
            reserved_at: timestamp(1),
            completed_at: Some(timestamp(2)),
        };
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_idempotency_record(record, IdentityVersion::new(2))
            .seed_stored_result(stored)
            .seed_job_report(report.clone(), IdentityVersion::new(1))
            .build();
        let mapper = DefaultIdentityMaintenanceIssueMapper;
        let service = job_service(&runtime, &mapper);

        let response = service
            .dispatch_job_scaffold(
                context,
                job_request(
                    "RunIdentityReconciliation",
                    "idem-job-dup",
                    "job-run-dup",
                    "dup",
                ),
                |loaded_report| {
                    Ok(TestJobOutput {
                        disposition: "duplicate_replayed",
                        changed_count: loaded_report.rebuilt_projection_refs.len() as u32,
                        failed_count: loaded_report.issue_refs.len() as u32,
                    })
                },
                |_, _, _, _, _| unreachable!("duplicate replay must not dispatch a job body"),
            )
            .expect("duplicate replay");

        assert_eq!(response.report_ref, report.report_ref);
        assert_eq!(response.stored_result_ref, stored_result_ref("job-dup"));
        assert_eq!(
            response.output,
            TestJobOutput {
                disposition: "duplicate_replayed",
                changed_count: 0,
                failed_count: 1,
            }
        );
        assert_eq!(response.report, report.to_surface());
    }

    #[test]
    fn job_dispatch_replay_missing_report_is_consistency_defect() {
        let context = job_context(
            "RunIdentityReconciliation",
            "idem-job-missing-report",
            "job",
            "job-run-missing-report",
        );
        let stored = StoredIdentityOperationResult::job_report(
            stored_result_ref("job-missing-report"),
            context.context_ref.clone(),
            identity_application::support::IdentityStoredSurfaceMarkerRef::new(
                "surface-job-missing-report",
            ),
            timestamp(2),
        );
        let record = IdentityIdempotencyRecord {
            record_ref: IdentityIdempotencyRecordRef::new("idem-record-job-missing-report"),
            operation_name: context.operation_name.clone(),
            channel: IdentityOperationChannel::Job,
            idempotency_key: IdentityIdempotencyKey::new("idem-job-missing-report"),
            request_digest: context.request_digest.clone(),
            state: identity_application::support::IdentityIdempotencyStateKind::Completed,
            stored_result_ref: Some(stored_result_ref("job-missing-report")),
            reserved_at: timestamp(1),
            completed_at: Some(timestamp(2)),
        };
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_idempotency_record(record, IdentityVersion::new(2))
            .seed_stored_result(stored)
            .build();
        let mapper = DefaultIdentityMaintenanceIssueMapper;
        let service = job_service(&runtime, &mapper);

        let err = service
            .dispatch_job_scaffold(
                context,
                job_request(
                    "RunIdentityReconciliation",
                    "idem-job-missing-report",
                    "job-run-missing-report",
                    "job-missing-report",
                ),
                |_| -> Result<TestJobOutput, ApplicationError> {
                    unreachable!("replay output must not run without a stored report")
                },
                |_, _, _, _, _| unreachable!("duplicate replay must not dispatch a job body"),
            )
            .expect_err("missing report should fail");

        assert_eq!(
            err.kind,
            ApplicationErrorKind::DuplicateReplayConsistencyDefect
        );
    }

    #[test]
    fn job_dispatch_replay_wrong_stored_kind_is_consistency_defect() {
        let context = job_context(
            "RunIdentityReconciliation",
            "idem-job-wrong-kind",
            "job",
            "job-run-wrong-kind",
        );
        let record = IdentityIdempotencyRecord {
            record_ref: IdentityIdempotencyRecordRef::new("idem-record-job-wrong-kind"),
            operation_name: context.operation_name.clone(),
            channel: IdentityOperationChannel::Job,
            idempotency_key: IdentityIdempotencyKey::new("idem-job-wrong-kind"),
            request_digest: context.request_digest.clone(),
            state: identity_application::support::IdentityIdempotencyStateKind::Completed,
            stored_result_ref: Some(stored_result_ref("job-wrong-kind")),
            reserved_at: timestamp(1),
            completed_at: Some(timestamp(2)),
        };
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_idempotency_record(record, IdentityVersion::new(2))
            .seed_stored_result(command_stored_result(
                "job-wrong-kind",
                &context.context_ref,
            ))
            .seed_job_report(
                job_report("job-wrong-kind", Some(stored_result_ref("job-wrong-kind"))),
                IdentityVersion::new(1),
            )
            .build();
        let mapper = DefaultIdentityMaintenanceIssueMapper;
        let service = job_service(&runtime, &mapper);

        let err = service
            .dispatch_job_scaffold(
                context,
                job_request(
                    "RunIdentityReconciliation",
                    "idem-job-wrong-kind",
                    "job-run-wrong-kind",
                    "job-wrong-kind",
                ),
                |_| -> Result<TestJobOutput, ApplicationError> {
                    unreachable!("replay output must not run on wrong stored kind")
                },
                |_, _, _, _, _| unreachable!("duplicate replay must not dispatch a job body"),
            )
            .expect_err("wrong kind should fail");

        assert_eq!(
            err.kind,
            ApplicationErrorKind::DuplicateReplayConsistencyDefect
        );
    }

    #[test]
    fn job_dispatch_saves_report_before_idempotency_completion() {
        let runtime = IdentityInMemoryRuntime::builder()
            .inject_fault(FaultCase::CompleteIdempotencyFails)
            .build();
        let mapper = DefaultIdentityMaintenanceIssueMapper;
        let service = job_service(&runtime, &mapper);
        let context = job_context(
            "RunIdentityReconciliation",
            "idem-job-save-order",
            "job",
            "job-run-save-order",
        );

        let err = service
            .dispatch_job_scaffold(
                context.clone(),
                job_request(
                    "RunIdentityReconciliation",
                    "idem-job-save-order",
                    "job-run-save-order",
                    "job-save-order",
                ),
                |_| unreachable!("first run should not replay"),
                |_, _, now, report, _| {
                    let mut report = report.succeed(
                        Some(identity_contracts::refs::IdentityJobCursorRef::new(
                            "job-output-cursor-save-order",
                        )),
                        None,
                        now,
                    );
                    report
                        .rebuilt_projection_refs
                        .push(projection_ref("projection-save-order"));
                    Ok(IdentityJobExecution::new(
                        TestJobOutput {
                            disposition: "completed",
                            changed_count: 1,
                            failed_count: 0,
                        },
                        report,
                    ))
                },
            )
            .expect_err("completion fault");

        assert_eq!(err.kind, ApplicationErrorKind::DependencyUnavailable);
        assert!(
            runtime
                .find_job_report_by_run(IdentityJobRunRef::new("job-run-save-order"))
                .expect("lookup")
                .is_none()
        );
        assert!(
            runtime
                .find_by_operation_context(context.context_ref.clone())
                .expect("stored result lookup")
                .is_none()
        );
        assert!(
            runtime
                .get_by_key(
                    IdentityOperationName::new("RunIdentityReconciliation"),
                    IdentityOperationChannel::Job,
                    IdentityIdempotencyKey::new("idem-job-save-order"),
                )
                .expect("idempotency lookup")
                .is_none()
        );
    }

    #[test]
    fn rebuild_projection_job_saves_view_and_marks_projection_rebuilt() {
        let member = GlobalMember::establish(
            member_ref("member-1"),
            identity_source_ref(IdentitySourceOwner::Identity, "member-source-job-rebuild-1"),
            ActorRef::new("actor-1", ActorKind::Human),
            timestamp(1),
        )
        .expect("member");
        let plan = member_summary_rebuild_plan("scope-job-rebuild-1");
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_member(member.clone(), IdentityVersion::new(1))
            .seed_projection_state(projection_state("cursor-1"), IdentityVersion::new(1))
            .seed_member_summary_rebuild_plan(plan.clone())
            .build();
        let mapper = DefaultIdentityMaintenanceIssueMapper;
        let service = job_service(&runtime, &mapper);

        let response = service
            .rebuild_identity_projection(
                typed_job_request(
                    "RebuildIdentityProjection",
                    "idem-job-rebuild-1",
                    "job-run-rebuild-1",
                    "job-rebuild-1",
                    RebuildIdentityProjectionJobInput {
                        rebuild_scope: IdentityProjectionRebuildScopeDto::ExplicitProjectionRefs(
                            vec![projection_ref("projection-1")],
                        ),
                        maintenance_scope_ref: maintenance_scope("scope-1"),
                        page: job_page(10),
                    },
                ),
                job_context(
                    "RebuildIdentityProjection",
                    "idem-job-rebuild-1",
                    "job-rebuild-1",
                    "job-run-rebuild-1",
                ),
            )
            .expect("rebuild job");

        assert_eq!(
            response.output.disposition,
            IdentityJobRunDisposition::Completed
        );
        assert_eq!(
            response.output.rebuilt_projection_refs,
            vec![projection_ref("projection-1")]
        );
        assert!(response.output.failed_projection_refs.is_empty());
        assert!(response.output.issue_refs.is_empty());
        let view_ref = runtime
            .find_member_summary_view_ref(
                member.member_ref.clone(),
                scope_ref("scope-job-rebuild-1"),
            )
            .expect("lookup")
            .expect("view ref");
        assert_eq!(view_ref, plan.view_inputs[0].view_ref);
        let view = runtime
            .get_member_summary_view(view_ref)
            .expect("load view")
            .expect("stored view");
        assert_eq!(view.read_surface_kind, IdentityReadSurfaceKind::Found);
        let rebuilt_state = runtime
            .get_projection_state_with_version(projection_ref("projection-1"))
            .expect("load state")
            .expect("projection state");
        assert_eq!(
            rebuilt_state.value.state_kind,
            identity_domain::projection_state::ProjectionStateKind::Rebuilt
        );
        let persisted_member = runtime
            .get_member_with_version(member.member_ref.clone())
            .expect("load member")
            .expect("member");
        assert_eq!(persisted_member.value, member);
    }

    #[test]
    fn refresh_reference_job_by_owner_uses_loaded_bundle_version_for_state_and_sidecars() {
        let member = GlobalMember::establish(
            member_ref("member-1"),
            identity_source_ref(IdentitySourceOwner::Identity, "member-source-job-refresh-1"),
            ActorRef::new("actor-1", ActorKind::Human),
            timestamp(1),
        )
        .expect("member");
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_member(member.clone(), IdentityVersion::new(1))
            .seed_reference_state(
                reference_state_resolved(),
                empty_sidecars(),
                IdentityVersion::new(1),
            )
            .seed_external_reference_resolution_outcome(refreshed_reference_outcome())
            .build();
        let mapper = DefaultIdentityMaintenanceIssueMapper;
        let service = job_service(&runtime, &mapper);

        let response = service
            .refresh_external_reference_state(
                typed_job_request(
                    "RefreshExternalReferenceState",
                    "idem-job-refresh-1",
                    "job-run-refresh-1",
                    "job-refresh-1",
                    RefreshExternalReferenceStateJobInput {
                        refresh_scope: IdentityExternalReferenceRefreshScopeDto::ByOwner(
                            reference_owner(),
                        ),
                        maintenance_scope_ref: maintenance_scope("scope-1"),
                        page: job_page(10),
                    },
                ),
                job_context(
                    "RefreshExternalReferenceState",
                    "idem-job-refresh-1",
                    "job-refresh-1",
                    "job-run-refresh-1",
                ),
            )
            .expect("refresh job");

        assert_eq!(
            response.output.disposition,
            IdentityJobRunDisposition::Completed
        );
        assert_eq!(
            response.output.refreshed_reference_refs,
            vec![external_reference()]
        );
        assert!(response.output.failed_reference_refs.is_empty());
        let persisted = runtime
            .get_reference_state_with_version(external_reference())
            .expect("load bundle")
            .expect("bundle");
        assert_eq!(persisted.version, IdentityVersion::new(2));
        assert_eq!(
            persisted.value.safe_summary_ref,
            Some(ExternalReferenceSafeSummaryRef::new(
                external_reference(),
                identity_source_ref(IdentitySourceOwner::MethodLibrary, "safe-summary-2"),
            ))
        );
        assert_eq!(
            runtime
                .get_typed_sidecar_refs(external_reference())
                .expect("load sidecars")
                .source_version_ref,
            Some(ExternalSourceVersionRef::new(identity_source_ref(
                IdentitySourceOwner::MethodLibrary,
                "sidecar-version-2",
            )))
        );
        let persisted_member = runtime
            .get_member_with_version(member.member_ref.clone())
            .expect("load member")
            .expect("member");
        assert_eq!(persisted_member.value, member);
    }

    #[test]
    fn rebuild_projection_job_duplicate_replay_returns_typed_output_without_body_rerun() {
        let context = job_context(
            "RebuildIdentityProjection",
            "idem-job-rebuild-dup",
            "job-rebuild-dup",
            "job-run-rebuild-dup",
        );
        let stored_result = StoredIdentityOperationResult::job_report(
            stored_result_ref("job-rebuild-dup"),
            context.context_ref.clone(),
            identity_application::support::IdentityStoredSurfaceMarkerRef::new(
                "surface-job-rebuild-dup",
            ),
            timestamp(2),
        );
        let mut report = IdentityJobRunReport::start(
            IdentityJobReportRef::new("job-report-rebuild-dup"),
            IdentityJobRunRef::new("job-run-rebuild-dup"),
            IdentityJobName::new("RebuildIdentityProjection"),
            IdentityJobScopeMarkerRef::new("job-scope-job-rebuild-dup"),
            Some(identity_contracts::refs::IdentityJobCursorRef::new(
                "job-input-cursor-rebuild-dup",
            )),
            timestamp(1),
        )
        .succeed(
            None,
            Some(stored_result_ref("job-rebuild-dup")),
            timestamp(2),
        );
        report
            .affected_projection_refs
            .push(projection_ref("projection-1"));
        report
            .rebuilt_projection_refs
            .push(projection_ref("projection-1"));
        let record = IdentityIdempotencyRecord {
            record_ref: IdentityIdempotencyRecordRef::new("idem-record-job-rebuild-dup"),
            operation_name: context.operation_name.clone(),
            channel: IdentityOperationChannel::Job,
            idempotency_key: IdentityIdempotencyKey::new("idem-job-rebuild-dup"),
            request_digest: context.request_digest.clone(),
            state: identity_application::support::IdentityIdempotencyStateKind::Completed,
            stored_result_ref: Some(stored_result_ref("job-rebuild-dup")),
            reserved_at: timestamp(1),
            completed_at: Some(timestamp(2)),
        };
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_idempotency_record(record, IdentityVersion::new(2))
            .seed_stored_result(stored_result)
            .seed_job_report(report.clone(), IdentityVersion::new(1))
            .build();
        let mapper = DefaultIdentityMaintenanceIssueMapper;
        let service = job_service(&runtime, &mapper);

        let response = service
            .rebuild_identity_projection(
                typed_job_request(
                    "RebuildIdentityProjection",
                    "idem-job-rebuild-dup",
                    "job-run-rebuild-dup",
                    "job-rebuild-dup",
                    RebuildIdentityProjectionJobInput {
                        rebuild_scope: IdentityProjectionRebuildScopeDto::ExplicitProjectionRefs(
                            vec![projection_ref("projection-1")],
                        ),
                        maintenance_scope_ref: maintenance_scope("scope-1"),
                        page: job_page(10),
                    },
                ),
                context,
            )
            .expect("duplicate replay");

        assert_eq!(
            response.output.disposition,
            IdentityJobRunDisposition::DuplicateReplayed
        );
        assert_eq!(
            response.output.rebuilt_projection_refs,
            vec![projection_ref("projection-1")]
        );
        assert_eq!(response.report, report.to_surface());
    }

    #[test]
    fn reconciliation_job_missing_inspection_context_returns_partial_report_only() {
        let member = GlobalMember::establish(
            member_ref("member-1"),
            identity_source_ref(
                IdentitySourceOwner::Identity,
                "member-source-job-reconcile-1",
            ),
            ActorRef::new("actor-1", ActorKind::Human),
            timestamp(1),
        )
        .expect("member");
        let target_ref = maintenance_target_ref(
            "target-reconcile-1",
            IdentityMaintenanceTargetKind::Projection,
        );
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_member(member.clone(), IdentityVersion::new(1))
            .build();
        let mapper = DefaultIdentityMaintenanceIssueMapper;
        let service = job_service(&runtime, &mapper);

        let response = service
            .run_identity_reconciliation(
                typed_job_request(
                    "RunIdentityReconciliation",
                    "idem-job-reconcile-1",
                    "job-run-reconcile-1",
                    "job-reconcile-1",
                    RunIdentityReconciliationJobInput {
                        maintenance_scope_ref: maintenance_scope("scope-1"),
                        target_scope: IdentityReconciliationTargetScopeDto::ExplicitTargets(vec![
                            target_ref.clone(),
                        ]),
                        finding_intent_ref:
                            identity_contracts::refs::ReconciliationFindingIntentRef::new(
                                identity_source_ref(
                                    IdentitySourceOwner::Identity,
                                    "finding-intent-1",
                                ),
                            ),
                        finding_material: identity_contracts::refs::ReconciliationFindingMaterial {
                            material_kind: ReconciliationFindingMaterialKind::SafeRefsOnly,
                            source_ref: Some(identity_source_ref(
                                IdentitySourceOwner::Identity,
                                "finding-material-1",
                            )),
                        },
                        page: job_page(10),
                    },
                ),
                job_context(
                    "RunIdentityReconciliation",
                    "idem-job-reconcile-1",
                    "job-reconcile-1",
                    "job-run-reconcile-1",
                ),
            )
            .expect("reconciliation job");

        assert_eq!(
            response.output.disposition,
            IdentityJobRunDisposition::Partial
        );
        assert_eq!(
            response.output.inspected_target_refs,
            vec![target_ref.clone()]
        );
        assert_eq!(response.output.issue_refs.len(), 1);
        let saved_report = runtime
            .get_report_with_version(response.output.report_refs[0].clone())
            .expect("load report")
            .expect("report");
        assert_eq!(
            saved_report.value.report_state,
            identity_domain::reconciliation::ReconciliationReportStateKind::Partial
        );
        assert_eq!(saved_report.value.target_refs, vec![target_ref]);
        let persisted_member = runtime
            .get_member_with_version(member.member_ref.clone())
            .expect("load member")
            .expect("member");
        assert_eq!(persisted_member.value, member);
    }

    #[test]
    fn reconciliation_job_rejects_forbidden_finding_material() {
        let runtime = IdentityInMemoryRuntime::builder().build();
        let mapper = DefaultIdentityMaintenanceIssueMapper;
        let service = job_service(&runtime, &mapper);

        let err = service
            .run_identity_reconciliation(
                typed_job_request(
                    "RunIdentityReconciliation",
                    "idem-job-reconcile-forbidden",
                    "job-run-reconcile-forbidden",
                    "job-reconcile-forbidden",
                    RunIdentityReconciliationJobInput {
                        maintenance_scope_ref: maintenance_scope("scope-1"),
                        target_scope: IdentityReconciliationTargetScopeDto::ExplicitTargets(vec![
                            maintenance_target_ref(
                                "target-reconcile-forbidden",
                                IdentityMaintenanceTargetKind::Projection,
                            ),
                        ]),
                        finding_intent_ref:
                            identity_contracts::refs::ReconciliationFindingIntentRef::new(
                                identity_source_ref(
                                    IdentitySourceOwner::Identity,
                                    "finding-intent-forbidden",
                                ),
                            ),
                        finding_material: identity_contracts::refs::ReconciliationFindingMaterial {
                            material_kind: ReconciliationFindingMaterialKind::ForbiddenExternalBody,
                            source_ref: Some(identity_source_ref(
                                IdentitySourceOwner::Identity,
                                "finding-material-forbidden",
                            )),
                        },
                        page: job_page(10),
                    },
                ),
                job_context(
                    "RunIdentityReconciliation",
                    "idem-job-reconcile-forbidden",
                    "job-reconcile-forbidden",
                    "job-run-reconcile-forbidden",
                ),
            )
            .expect_err("forbidden finding material");

        assert_eq!(err.kind, ApplicationErrorKind::DomainRejected);
        assert!(
            runtime
                .list_reports_by_scope(
                    maintenance_scope("scope-1"),
                    IdentityRepositoryPage::new(None, 10)
                )
                .expect("list reports")
                .items
                .is_empty()
        );
    }

    #[test]
    fn publish_outbox_job_updates_state_and_preserves_member_truth() {
        let member = GlobalMember::establish(
            member_ref("member-publish-1"),
            identity_source_ref(IdentitySourceOwner::Identity, "member-source-publish-1"),
            ActorRef::new("actor-1", ActorKind::Human),
            timestamp(1),
        )
        .expect("member");
        let mut record = outbox_record("publish-success", OutboxState::pending(timestamp(1)));
        record.member_ref = member.member_ref.clone();

        let runtime = IdentityInMemoryRuntime::builder()
            .seed_member(member.clone(), IdentityVersion::new(1))
            .seed_outbox_record(record.clone(), IdentityVersion::new(1))
            .seed_topic_binding_resolution(
                record.topic_key_ref.clone(),
                record.payload_marker_ref.clone(),
                topic_binding_resolution(record.topic_key_ref.clone()),
            )
            .seed_outbox_publish_outcome(
                record.outbox_record_ref.clone(),
                OutboxPublishOutcome::Published {
                    attempt_ref: outbox_attempt_ref("outbox-attempt-publish-1"),
                },
            )
            .build();
        let mapper = DefaultIdentityMaintenanceIssueMapper;
        let service = job_service(&runtime, &mapper);

        let response = service
            .publish_identity_outbox(
                typed_job_request(
                    "PublishIdentityOutbox",
                    "idem-job-publish-1",
                    "job-run-publish-1",
                    "job-publish-1",
                    PublishIdentityOutboxJobInput {
                        topic_key_ref: Some(record.topic_key_ref.clone()),
                        page: job_page(10),
                    },
                ),
                job_context(
                    "PublishIdentityOutbox",
                    "idem-job-publish-1",
                    "job-publish-1",
                    "job-run-publish-1",
                ),
            )
            .expect("publish job");

        assert_eq!(
            response.output.disposition,
            IdentityJobRunDisposition::Completed
        );
        assert_eq!(
            response.output.scanned_outbox_refs,
            vec![record.outbox_record_ref.clone()]
        );
        assert_eq!(
            response.output.published_outbox_refs,
            vec![record.outbox_record_ref.clone()]
        );
        assert!(response.output.failed_outbox_refs.is_empty());
        assert!(response.output.issue_refs.is_empty());

        let persisted = runtime
            .get_outbox_record_with_version(record.outbox_record_ref.clone())
            .expect("load outbox")
            .expect("outbox");
        assert_eq!(persisted.version, IdentityVersion::new(2));
        assert_eq!(
            persisted.value.outbox_state.state_kind,
            OutboxStateKind::Published
        );
        assert_eq!(
            persisted.value.outbox_state.attempt_ref,
            Some(outbox_attempt_ref("outbox-attempt-publish-1"))
        );
        let persisted_member = runtime
            .get_member_with_version(member.member_ref.clone())
            .expect("load member")
            .expect("member");
        assert_eq!(persisted_member.value, member);
    }

    #[test]
    fn publish_outbox_job_maps_retryable_and_unsupported_outcomes() {
        let published = outbox_record("publish-mixed", OutboxState::pending(timestamp(1)));
        let retryable = outbox_record("retryable-mixed", OutboxState::pending(timestamp(1)));
        let unsupported = outbox_record("unsupported-mixed", OutboxState::pending(timestamp(1)));

        let runtime = IdentityInMemoryRuntime::builder()
            .seed_outbox_record(published.clone(), IdentityVersion::new(1))
            .seed_outbox_record(retryable.clone(), IdentityVersion::new(1))
            .seed_outbox_record(unsupported.clone(), IdentityVersion::new(1))
            .seed_topic_binding_resolution(
                published.topic_key_ref.clone(),
                published.payload_marker_ref.clone(),
                topic_binding_resolution(published.topic_key_ref.clone()),
            )
            .seed_topic_binding_resolution(
                retryable.topic_key_ref.clone(),
                retryable.payload_marker_ref.clone(),
                topic_binding_resolution(retryable.topic_key_ref.clone()),
            )
            .seed_topic_binding_resolution(
                unsupported.topic_key_ref.clone(),
                unsupported.payload_marker_ref.clone(),
                topic_binding_resolution(unsupported.topic_key_ref.clone()),
            )
            .seed_outbox_publish_outcome(
                published.outbox_record_ref.clone(),
                OutboxPublishOutcome::Published {
                    attempt_ref: outbox_attempt_ref("outbox-attempt-mixed"),
                },
            )
            .seed_outbox_publish_outcome(
                retryable.outbox_record_ref.clone(),
                OutboxPublishOutcome::RetryableFailed {
                    attempt_ref: Some(outbox_attempt_ref("outbox-attempt-retryable")),
                    issue_ref: outbox_issue_ref("outbox-issue-retryable"),
                },
            )
            .seed_outbox_publish_outcome(
                unsupported.outbox_record_ref.clone(),
                OutboxPublishOutcome::UnsupportedTopic {
                    issue_ref: outbox_issue_ref("outbox-issue-unsupported"),
                },
            )
            .build();
        let mapper = DefaultIdentityMaintenanceIssueMapper;
        let service = job_service(&runtime, &mapper);

        let response = service
            .publish_identity_outbox(
                typed_job_request(
                    "PublishIdentityOutbox",
                    "idem-job-publish-mixed",
                    "job-run-publish-mixed",
                    "job-publish-mixed",
                    PublishIdentityOutboxJobInput {
                        topic_key_ref: None,
                        page: job_page(10),
                    },
                ),
                job_context(
                    "PublishIdentityOutbox",
                    "idem-job-publish-mixed",
                    "job-publish-mixed",
                    "job-run-publish-mixed",
                ),
            )
            .expect("publish mixed job");

        assert_eq!(
            response.output.disposition,
            IdentityJobRunDisposition::Partial
        );
        assert_eq!(response.output.scanned_outbox_refs.len(), 3);
        assert_eq!(
            response.output.published_outbox_refs,
            vec![published.outbox_record_ref.clone()]
        );
        let mut failed_outbox_refs = response.output.failed_outbox_refs.clone();
        failed_outbox_refs.sort_by(|left, right| left.as_str().cmp(right.as_str()));
        assert_eq!(
            failed_outbox_refs,
            vec![
                retryable.outbox_record_ref.clone(),
                unsupported.outbox_record_ref.clone(),
            ]
        );
        assert_eq!(response.output.issue_refs.len(), 2);
        assert_eq!(
            runtime
                .get_outbox_record_with_version(retryable.outbox_record_ref.clone())
                .expect("load retryable")
                .expect("retryable")
                .value
                .outbox_state
                .state_kind,
            OutboxStateKind::RetryableFailed
        );
        assert_eq!(
            runtime
                .get_outbox_record_with_version(unsupported.outbox_record_ref.clone())
                .expect("load unsupported")
                .expect("unsupported")
                .value
                .outbox_state
                .state_kind,
            OutboxStateKind::Failed
        );
    }

    #[test]
    fn publish_outbox_job_duplicate_replay_returns_stored_report_without_rerun() {
        let context = job_context(
            "PublishIdentityOutbox",
            "idem-job-publish-dup",
            "job-publish-dup",
            "job-run-publish-dup",
        );
        let stored_result = StoredIdentityOperationResult::job_report(
            stored_result_ref("job-publish-dup"),
            context.context_ref.clone(),
            identity_application::support::IdentityStoredSurfaceMarkerRef::new(
                "surface-job-publish-dup",
            ),
            timestamp(2),
        );
        let outbox_ref = IdentityOutboxRecordRef::new("outbox-publish-dup");
        let mut report = IdentityJobRunReport::start(
            IdentityJobReportRef::new("job-report-publish-dup"),
            IdentityJobRunRef::new("job-run-publish-dup"),
            IdentityJobName::new("PublishIdentityOutbox"),
            IdentityJobScopeMarkerRef::new("job-scope-job-publish-dup"),
            Some(identity_contracts::refs::IdentityJobCursorRef::new(
                "job-input-cursor-publish-dup",
            )),
            timestamp(1),
        )
        .succeed(
            None,
            Some(stored_result_ref("job-publish-dup")),
            timestamp(2),
        );
        report.outbox_record_refs.push(outbox_ref.clone());
        report.published_outbox_refs.push(outbox_ref.clone());

        let record = IdentityIdempotencyRecord {
            record_ref: IdentityIdempotencyRecordRef::new("idem-record-job-publish-dup"),
            operation_name: context.operation_name.clone(),
            channel: IdentityOperationChannel::Job,
            idempotency_key: IdentityIdempotencyKey::new("idem-job-publish-dup"),
            request_digest: context.request_digest.clone(),
            state: identity_application::support::IdentityIdempotencyStateKind::Completed,
            stored_result_ref: Some(stored_result_ref("job-publish-dup")),
            reserved_at: timestamp(1),
            completed_at: Some(timestamp(2)),
        };
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_idempotency_record(record, IdentityVersion::new(2))
            .seed_stored_result(stored_result)
            .seed_job_report(report.clone(), IdentityVersion::new(1))
            .build();
        let mapper = DefaultIdentityMaintenanceIssueMapper;
        let service = job_service(&runtime, &mapper);

        let response = service
            .publish_identity_outbox(
                typed_job_request(
                    "PublishIdentityOutbox",
                    "idem-job-publish-dup",
                    "job-run-publish-dup",
                    "job-publish-dup",
                    PublishIdentityOutboxJobInput {
                        topic_key_ref: None,
                        page: job_page(10),
                    },
                ),
                context,
            )
            .expect("duplicate replay");

        assert_eq!(
            response.output.disposition,
            IdentityJobRunDisposition::DuplicateReplayed
        );
        assert_eq!(
            response.output.scanned_outbox_refs,
            vec![outbox_ref.clone()]
        );
        assert_eq!(response.output.published_outbox_refs, vec![outbox_ref]);
        assert_eq!(response.report, report.to_surface());
    }

    #[test]
    fn deliver_trace_handoff_job_delivers_with_formal_receipt() {
        let member = GlobalMember::establish(
            member_ref("member-handoff-1"),
            identity_source_ref(IdentitySourceOwner::Identity, "member-source-handoff-1"),
            ActorRef::new("actor-1", ActorKind::Human),
            timestamp(1),
        )
        .expect("member");
        let mut intent = handoff_intent();
        intent.member_ref = member.member_ref.clone();

        let runtime = IdentityInMemoryRuntime::builder()
            .seed_member(member.clone(), IdentityVersion::new(1))
            .seed_handoff_intent(intent.clone(), IdentityVersion::new(1))
            .seed_adapter_availability(adapter_availability())
            .seed_handoff_delivery_outcome(
                intent.handoff_intent_ref.clone(),
                HandoffDeliveryOutcome::Delivered {
                    attempt_ref: handoff_attempt_ref("handoff-attempt-1"),
                    receipt_ref: handoff_receipt_ref("handoff-receipt-1"),
                },
            )
            .build();
        let mapper = DefaultIdentityMaintenanceIssueMapper;
        let service = job_service(&runtime, &mapper);

        let response = service
            .deliver_trace_handoff(
                typed_job_request(
                    "DeliverTraceHandoff",
                    "idem-job-handoff-1",
                    "job-run-handoff-1",
                    "job-handoff-1",
                    DeliverTraceHandoffJobInput {
                        delivery_scope: IdentityHandoffDeliveryScopeDto::ExplicitIntentRefs(vec![
                            intent.handoff_intent_ref.clone(),
                        ]),
                        page: job_page(10),
                    },
                ),
                job_context(
                    "DeliverTraceHandoff",
                    "idem-job-handoff-1",
                    "job-handoff-1",
                    "job-run-handoff-1",
                ),
            )
            .expect("deliver handoff job");

        assert_eq!(
            response.output.disposition,
            IdentityJobRunDisposition::Completed
        );
        assert_eq!(
            response.output.delivered_handoff_intent_refs,
            vec![intent.handoff_intent_ref.clone()]
        );
        assert_eq!(
            response.output.receipt_refs,
            vec![handoff_receipt_ref("handoff-receipt-1")]
        );
        assert!(response.output.failed_handoff_intent_refs.is_empty());
        let persisted = runtime
            .get_handoff_intent_with_version(intent.handoff_intent_ref.clone())
            .expect("load handoff")
            .expect("handoff");
        assert_eq!(persisted.version, IdentityVersion::new(2));
        assert_eq!(
            persisted.value.handoff_state.state_kind,
            HandoffStateKind::Delivered
        );
        assert_eq!(
            persisted.value.handoff_state.receipt_ref,
            Some(handoff_receipt_ref("handoff-receipt-1"))
        );
        let persisted_member = runtime
            .get_member_with_version(member.member_ref.clone())
            .expect("load member")
            .expect("member");
        assert_eq!(persisted_member.value, member);
    }

    #[test]
    fn deliver_trace_handoff_job_default_fake_does_not_synthesize_success() {
        let intent = handoff_intent();
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_handoff_intent(intent.clone(), IdentityVersion::new(1))
            .seed_adapter_availability(adapter_availability())
            .build();
        let mapper = DefaultIdentityMaintenanceIssueMapper;
        let service = job_service(&runtime, &mapper);

        let response = service
            .deliver_trace_handoff(
                typed_job_request(
                    "DeliverTraceHandoff",
                    "idem-job-handoff-default",
                    "job-run-handoff-default",
                    "job-handoff-default",
                    DeliverTraceHandoffJobInput {
                        delivery_scope: IdentityHandoffDeliveryScopeDto::ByTarget(
                            intent.handoff_target_ref.clone(),
                        ),
                        page: job_page(10),
                    },
                ),
                job_context(
                    "DeliverTraceHandoff",
                    "idem-job-handoff-default",
                    "job-handoff-default",
                    "job-run-handoff-default",
                ),
            )
            .expect("deliver handoff default");

        assert_eq!(
            response.output.disposition,
            IdentityJobRunDisposition::Failed
        );
        assert!(response.output.delivered_handoff_intent_refs.is_empty());
        assert_eq!(
            response.output.failed_handoff_intent_refs,
            vec![intent.handoff_intent_ref.clone()]
        );
        assert_eq!(
            runtime
                .get_handoff_intent_with_version(intent.handoff_intent_ref.clone())
                .expect("load handoff")
                .expect("handoff")
                .value
                .handoff_state
                .state_kind,
            HandoffStateKind::Cancelled
        );
    }

    #[test]
    fn deliver_trace_handoff_job_maps_retryable_failure_with_attempt() {
        let intent = handoff_intent();
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_handoff_intent(intent.clone(), IdentityVersion::new(1))
            .seed_adapter_availability(adapter_availability())
            .seed_handoff_delivery_outcome(
                intent.handoff_intent_ref.clone(),
                HandoffDeliveryOutcome::RetryableFailed {
                    attempt_ref: handoff_attempt_ref("handoff-attempt-retryable"),
                    issue_ref: HandoffIssueRef::new(identity_source_ref(
                        IdentitySourceOwner::Identity,
                        "handoff-issue-retryable",
                    )),
                },
            )
            .build();
        let mapper = DefaultIdentityMaintenanceIssueMapper;
        let service = job_service(&runtime, &mapper);

        let response = service
            .deliver_trace_handoff(
                typed_job_request(
                    "DeliverTraceHandoff",
                    "idem-job-handoff-retryable",
                    "job-run-handoff-retryable",
                    "job-handoff-retryable",
                    DeliverTraceHandoffJobInput {
                        delivery_scope: IdentityHandoffDeliveryScopeDto::ExplicitIntentRefs(vec![
                            intent.handoff_intent_ref.clone(),
                        ]),
                        page: job_page(10),
                    },
                ),
                job_context(
                    "DeliverTraceHandoff",
                    "idem-job-handoff-retryable",
                    "job-handoff-retryable",
                    "job-run-handoff-retryable",
                ),
            )
            .expect("deliver retryable handoff");

        assert_eq!(
            response.output.disposition,
            IdentityJobRunDisposition::RetryableFailed
        );
        assert!(response.output.delivered_handoff_intent_refs.is_empty());
        assert_eq!(
            response.output.failed_handoff_intent_refs,
            vec![intent.handoff_intent_ref.clone()]
        );
        assert_eq!(
            runtime
                .get_handoff_intent_with_version(intent.handoff_intent_ref.clone())
                .expect("load handoff")
                .expect("handoff")
                .value
                .handoff_state
                .state_kind,
            HandoffStateKind::RetryableFailed
        );
        assert!(
            runtime
                .get_handoff_intent_with_version(intent.handoff_intent_ref.clone())
                .expect("load handoff")
                .expect("handoff")
                .value
                .handoff_state
                .attempt_ref
                .is_some()
        );
    }

    #[test]
    fn retry_propagation_job_retries_only_retryable_outbox_family() {
        let retryable = outbox_record(
            "retry-family",
            OutboxState::retryable_failed(
                outbox_issue_ref("outbox-issue-retry-family"),
                timestamp(1),
            ),
        );
        let terminal = outbox_record(
            "terminal-family",
            OutboxState::published(outbox_attempt_ref("outbox-attempt-terminal"), timestamp(1)),
        );

        let runtime = IdentityInMemoryRuntime::builder()
            .seed_outbox_record(retryable.clone(), IdentityVersion::new(1))
            .seed_outbox_record(terminal.clone(), IdentityVersion::new(1))
            .seed_topic_binding_resolution(
                retryable.topic_key_ref.clone(),
                retryable.payload_marker_ref.clone(),
                topic_binding_resolution(retryable.topic_key_ref.clone()),
            )
            .seed_outbox_publish_outcome(
                retryable.outbox_record_ref.clone(),
                OutboxPublishOutcome::Published {
                    attempt_ref: outbox_attempt_ref("outbox-attempt-retry-family"),
                },
            )
            .build();
        let mapper = DefaultIdentityMaintenanceIssueMapper;
        let service = job_service(&runtime, &mapper);

        let response = service
            .retry_identity_propagation_failures(
                typed_job_request(
                    "RetryIdentityPropagationFailures",
                    "idem-job-retry-family",
                    "job-run-retry-family",
                    "job-retry-family",
                    RetryIdentityPropagationFailuresJobInput {
                        retry_scope: IdentityPropagationRetryScopeDto::OutboxRetryable {
                            topic_key_ref: Some(retryable.topic_key_ref.clone()),
                        },
                        page: job_page(10),
                    },
                ),
                job_context(
                    "RetryIdentityPropagationFailures",
                    "idem-job-retry-family",
                    "job-retry-family",
                    "job-run-retry-family",
                ),
            )
            .expect("retry propagation job");

        assert_eq!(
            response.output.disposition,
            IdentityJobRunDisposition::Completed
        );
        assert_eq!(
            response.output.retried_outbox_refs,
            vec![retryable.outbox_record_ref.clone()]
        );
        assert_eq!(
            response.output.published_outbox_refs,
            vec![retryable.outbox_record_ref.clone()]
        );
        assert!(response.output.failed_outbox_refs.is_empty());
        assert!(response.output.retried_handoff_intent_refs.is_empty());
        assert_eq!(
            runtime
                .get_outbox_record_with_version(retryable.outbox_record_ref.clone())
                .expect("load retried outbox")
                .expect("retried outbox")
                .value
                .outbox_state
                .state_kind,
            OutboxStateKind::Published
        );
        assert_eq!(
            runtime
                .get_outbox_record_with_version(terminal.outbox_record_ref.clone())
                .expect("load terminal outbox")
                .expect("terminal outbox")
                .value
                .outbox_state
                .state_kind,
            OutboxStateKind::Published
        );
    }

    #[test]
    fn same_key_different_digest_conflicts() {
        let context = command_context("establish_member", "idem-conflict", "same");
        let existing = IdentityIdempotencyRecord {
            record_ref: IdentityIdempotencyRecordRef::new("idem-record-conflict"),
            operation_name: context.operation_name.clone(),
            channel: IdentityOperationChannel::Command,
            idempotency_key: IdentityIdempotencyKey::new("idem-conflict"),
            request_digest: request_digest("original"),
            state: identity_application::support::IdentityIdempotencyStateKind::Reserved,
            stored_result_ref: None,
            reserved_at: timestamp(1),
            completed_at: None,
        };
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_idempotency_record(existing, IdentityVersion::new(1))
            .build();

        let uow = runtime.begin().expect("uow");
        let outcome = runtime
            .reserve(
                context,
                IdentityIdempotencyRecordRef::new("idem-record-conflict-new"),
                timestamp(2),
                uow.as_ref(),
            )
            .expect("reserve");
        assert!(matches!(outcome, IdempotencyReserveOutcome::Conflict(_)));
        runtime.rollback(uow).expect("rollback");
    }

    #[test]
    fn same_key_same_digest_in_flight_visible() {
        let context = command_context("establish_member", "idem-flight", "flight");
        let existing = IdentityIdempotencyRecord {
            record_ref: IdentityIdempotencyRecordRef::new("idem-record-flight"),
            operation_name: context.operation_name.clone(),
            channel: IdentityOperationChannel::Command,
            idempotency_key: IdentityIdempotencyKey::new("idem-flight"),
            request_digest: context.request_digest.clone(),
            state: identity_application::support::IdentityIdempotencyStateKind::Reserved,
            stored_result_ref: None,
            reserved_at: timestamp(1),
            completed_at: None,
        };
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_idempotency_record(existing, IdentityVersion::new(1))
            .build();

        let uow = runtime.begin().expect("uow");
        let outcome = runtime
            .reserve(
                context,
                IdentityIdempotencyRecordRef::new("idem-record-flight-new"),
                timestamp(2),
                uow.as_ref(),
            )
            .expect("reserve");
        assert!(matches!(outcome, IdempotencyReserveOutcome::InFlight(_)));
        runtime.rollback(uow).expect("rollback");
    }

    #[test]
    fn stored_result_saved_before_idempotency_complete() {
        let context = command_context("establish_member", "idem-complete", "complete");
        let runtime = IdentityInMemoryRuntime::builder()
            .inject_fault(FaultCase::CompleteIdempotencyFails)
            .build();

        let reserve_uow = runtime.begin().expect("reserve uow");
        let reserved = runtime
            .reserve(
                context.clone(),
                IdentityIdempotencyRecordRef::new("idem-record-complete"),
                timestamp(1),
                reserve_uow.as_ref(),
            )
            .expect("reserve");
        runtime.commit(reserve_uow).expect("commit reserve");

        let reserved_record = match reserved {
            IdempotencyReserveOutcome::Reserved(record) => record,
            other => panic!("unexpected reserve outcome: {other:?}"),
        };

        let complete_uow = runtime.begin().expect("complete uow");
        let stored = command_stored_result("complete", &context.context_ref);
        runtime
            .save_command_accepted_result(stored.clone(), complete_uow.as_ref())
            .expect("save stored");
        runtime
            .complete_with_stored_result(
                reserved_record.value,
                stored.stored_result_ref.clone(),
                timestamp(2),
                reserved_record.version,
                complete_uow.as_ref(),
            )
            .expect("stage complete");
        let error = runtime
            .commit(complete_uow)
            .expect_err("complete failure must abort");
        assert_eq!(error.kind, ApplicationErrorKind::DependencyUnavailable);

        let persisted_record = runtime
            .get_by_key(
                context.operation_name.clone(),
                IdentityOperationChannel::Command,
                IdentityIdempotencyKey::new("idem-complete"),
            )
            .expect("load")
            .expect("record");
        assert_eq!(
            persisted_record.value.state,
            identity_application::support::IdentityIdempotencyStateKind::Reserved
        );
        assert!(
            runtime
                .find_by_operation_context(context.context_ref)
                .expect("lookup stored")
                .is_none()
        );
    }

    #[test]
    fn outbox_state_lists_and_updates_are_formal() {
        let pending = outbox_record("pending", OutboxState::pending(timestamp(1)));
        let retryable = outbox_record(
            "retryable",
            OutboxState::retryable_failed(
                identity_contracts::refs::OutboxDeliveryIssueRef::new(identity_source_ref(
                    IdentitySourceOwner::Identity,
                    "outbox-issue-1",
                )),
                timestamp(2),
            ),
        );
        let published = outbox_record(
            "published",
            OutboxState::published(
                identity_contracts::refs::OutboxDeliveryAttemptRef::new(identity_source_ref(
                    IdentitySourceOwner::Identity,
                    "outbox-attempt-1",
                )),
                timestamp(3),
            ),
        );

        let runtime = IdentityInMemoryRuntime::builder()
            .seed_outbox_record(pending.clone(), IdentityVersion::new(1))
            .seed_outbox_record(retryable.clone(), IdentityVersion::new(1))
            .seed_outbox_record(published.clone(), IdentityVersion::new(1))
            .build();

        let pending_page = runtime
            .list_pending_outbox_records(None, IdentityRepositoryPage::new(None, 10))
            .expect("list pending");
        assert_eq!(pending_page.items.len(), 1);
        assert_eq!(
            pending_page.items[0].value_ref,
            IdentityOutboxRecordRef::new("outbox-pending")
        );

        let retryable_page = runtime
            .list_retryable_outbox_records(None, IdentityRepositoryPage::new(None, 10))
            .expect("list retryable");
        assert_eq!(retryable_page.items.len(), 1);
        assert_eq!(
            retryable_page.items[0].value_ref,
            IdentityOutboxRecordRef::new("outbox-retryable")
        );

        let by_trace = runtime
            .find_outbox_records_by_trace(
                IdentityTraceRecordRef::new("trace-pending"),
                IdentityRepositoryPage::new(None, 10),
            )
            .expect("list by trace");
        assert_eq!(by_trace.items.len(), 1);

        let mut updated = pending.clone();
        updated
            .mark_published(OutboxState::published(
                identity_contracts::refs::OutboxDeliveryAttemptRef::new(identity_source_ref(
                    IdentitySourceOwner::Identity,
                    "outbox-attempt-2",
                )),
                timestamp(4),
            ))
            .expect("mark published");
        let uow = runtime.begin().expect("uow");
        runtime
            .update_outbox_state(updated, IdentityVersion::new(1), uow.as_ref())
            .expect("stage update");
        runtime.commit(uow).expect("commit");

        let persisted = runtime
            .get_outbox_record_with_version(IdentityOutboxRecordRef::new("outbox-pending"))
            .expect("load")
            .expect("record");
        assert_eq!(persisted.version, IdentityVersion::new(2));
        assert_eq!(
            persisted.value.outbox_state.state_kind,
            OutboxStateKind::Published
        );
    }

    #[test]
    fn scoped_lookup_uses_persisted_member_scope_index() {
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_member_summary_view(summary_view("scope-a"), IdentityVersion::new(1))
            .seed_member_summary_view(summary_view("scope-b"), IdentityVersion::new(1))
            .build();

        let found = runtime
            .find_member_summary_view_ref(member_ref("member-1"), scope_ref("scope-b"))
            .expect("lookup");
        assert_eq!(found, Some(MemberSummaryViewRef::new("view-scope-b")));
    }

    #[test]
    fn summary_view_scope_guard_matches_design() {
        let view = summary_view("scope-a");
        assert!(view.belongs_to(&member_ref("member-1")));
        assert!(view.matches_visibility_scope(&scope_ref("scope-a")));
        assert!(!view.matches_visibility_scope(&scope_ref("scope-b")));
        assert!(view.assert_body_free().is_ok());
    }

    #[test]
    fn read_visibility_repository_returns_formal_scope() {
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_member_summary_access(
                member_ref("member-1"),
                IdentityVisibilityAccessSummary {
                    read_subject_ref: identity_contracts::refs::IdentityReadSubjectRef::new(
                        "read-subject:member-1",
                    ),
                    consumer_ref: ConsumerRef::new("consumer-1"),
                    actor_ref: Some(ActorRef::new("actor-1", ActorKind::Human)),
                    visibility_context_ref: VisibilityContextRef::new("context-1"),
                    scope_ref: scope_ref("scope-a"),
                    access_state: identity_contracts::views::IdentityVisibilityAccessState::Visible,
                    redaction_profile_ref: None,
                    redaction_marker_ref: None,
                    visibility_result_ref: visibility_result("visibility-a"),
                    degraded_marker_ref: None,
                    degraded_kind: None,
                },
            )
            .build();

        let access = runtime
            .resolve_member_summary_read(
                member_ref("member-1"),
                None,
                ConsumerRef::new("consumer-1"),
                VisibilityContextRef::new("context-1"),
            )
            .expect("resolve")
            .expect("summary access");
        assert_eq!(access.scope_ref, scope_ref("scope-a"));
        assert_eq!(
            access.read_subject_ref,
            IdentityReadSubjectRef::new("read-subject:member-1")
        );
    }

    fn query_service<'a>(runtime: &'a IdentityInMemoryRuntime) -> IdentityQueryService<'a> {
        IdentityQueryService::new(IdentityQueryServiceDeps {
            clock: runtime,
            id_generator: runtime,
            operation_context_factory: runtime,
            read_visibility_repository: runtime,
            projection_repository: runtime,
            member_repository: runtime,
            lifecycle_repository: runtime,
            role_capability_repository: runtime,
            career_record_repository: runtime,
            memory_reference_repository: runtime,
            trace_record_repository: runtime,
            audit_trail_repository: runtime,
            reference_state_repository: runtime,
            reconciliation_report_repository: runtime,
            outbox_repository: runtime,
            handoff_intent_repository: runtime,
            truth_change_subject_mapper: runtime,
            degradation_mapper:
                &identity_application::DefaultIdentityQueryMaterialDegradationMapper,
            unit_of_work_manager: runtime,
        })
    }

    fn query_request() -> IdentityQueryRequest<()> {
        IdentityQueryRequest {
            actor_ref: ActorRef::new("actor-1", ActorKind::Human),
            query_name: IdentityQueryName::new("ReadMemberSummary"),
            metadata: IdentityQueryMetadata {
                request_marker_ref: IdentityApiRequestMarkerRef::new("query-request-1"),
                schema_version_ref: IdentityProtocolSchemaVersionRef::new("identity.query.v1"),
                visibility_context_ref: VisibilityContextRef::new("context-1"),
                trace_context_ref: None,
            },
            page: None,
            body: (),
        }
    }

    fn query_context() -> IdentityOperationContext {
        IdentityOperationContext::from_query(
            IdentityOperationContextRef::new("query-context-1"),
            IdentityOperationName::new("ReadMemberSummary"),
            ActorRef::new("actor-1", ActorKind::Human),
            identity_application::support::IdentityRequestMetadataRef::new("query-metadata-1"),
            IdentityRequestDigest::from_canonical_marker(
                IdentityCanonicalRequestMarkerRef::new("canonical-query-1"),
                IdentityRequestDigestValue::new("digest-query-1"),
                IdentityProtocolSchemaVersionRef::new("identity.query.v1"),
                IdentityDigestAlgorithmMarkerRef::new("sha256-v1"),
            ),
            None,
            timestamp(1),
        )
    }

    fn trace_query_request(
        selector: IdentityTraceReadSelector,
    ) -> IdentityQueryRequest<ReadIdentityTraceRequest> {
        IdentityQueryRequest {
            actor_ref: ActorRef::new("actor-1", ActorKind::Human),
            query_name: IdentityQueryName::new("ReadIdentityTrace"),
            metadata: IdentityQueryMetadata {
                request_marker_ref: IdentityApiRequestMarkerRef::new("trace-query-request-1"),
                schema_version_ref: IdentityProtocolSchemaVersionRef::new("identity.query.v1"),
                visibility_context_ref: VisibilityContextRef::new("context-1"),
                trace_context_ref: None,
            },
            page: Some(IdentityPublicPageRequest {
                cursor: None,
                limit: 10,
            }),
            body: ReadIdentityTraceRequest {
                selector,
                consumer_ref: ConsumerRef::new("consumer-1"),
            },
        }
    }

    fn trace_query_context() -> IdentityOperationContext {
        IdentityOperationContext::from_query(
            IdentityOperationContextRef::new("trace-query-context-1"),
            IdentityOperationName::new("ReadIdentityTrace"),
            ActorRef::new("actor-1", ActorKind::Human),
            identity_application::support::IdentityRequestMetadataRef::new(
                "trace-query-metadata-1",
            ),
            IdentityRequestDigest::from_canonical_marker(
                IdentityCanonicalRequestMarkerRef::new("canonical-trace-query-1"),
                IdentityRequestDigestValue::new("digest-trace-query-1"),
                IdentityProtocolSchemaVersionRef::new("identity.query.v1"),
                IdentityDigestAlgorithmMarkerRef::new("sha256-v1"),
            ),
            None,
            timestamp(1),
        )
    }

    fn single_query_request<T>(query_name: &str, body: T) -> IdentityQueryRequest<T> {
        IdentityQueryRequest {
            actor_ref: ActorRef::new("actor-1", ActorKind::Human),
            query_name: IdentityQueryName::new(query_name),
            metadata: IdentityQueryMetadata {
                request_marker_ref: IdentityApiRequestMarkerRef::new(format!(
                    "query-request-{query_name}"
                )),
                schema_version_ref: IdentityProtocolSchemaVersionRef::new("identity.query.v1"),
                visibility_context_ref: VisibilityContextRef::new("context-1"),
                trace_context_ref: None,
            },
            page: None,
            body,
        }
    }

    fn paged_query_request<T>(query_name: &str, body: T) -> IdentityQueryRequest<T> {
        IdentityQueryRequest {
            page: Some(IdentityPublicPageRequest {
                cursor: None,
                limit: 10,
            }),
            ..single_query_request(query_name, body)
        }
    }

    fn named_query_context(query_name: &str, token: &str) -> IdentityOperationContext {
        IdentityOperationContext::from_query(
            IdentityOperationContextRef::new(format!("query-context-{query_name}-{token}")),
            IdentityOperationName::new(query_name),
            ActorRef::new("actor-1", ActorKind::Human),
            identity_application::support::IdentityRequestMetadataRef::new(format!(
                "query-metadata-{query_name}-{token}"
            )),
            IdentityRequestDigest::from_canonical_marker(
                IdentityCanonicalRequestMarkerRef::new(format!(
                    "canonical-query-{query_name}-{token}"
                )),
                IdentityRequestDigestValue::new(format!("digest-query-{query_name}-{token}")),
                IdentityProtocolSchemaVersionRef::new("identity.query.v1"),
                IdentityDigestAlgorithmMarkerRef::new("sha256-v1"),
            ),
            None,
            timestamp(1),
        )
    }

    fn member_access_summary(
        token: &str,
        access_state: IdentityVisibilityAccessState,
    ) -> IdentityVisibilityAccessSummary {
        IdentityVisibilityAccessSummary {
            read_subject_ref: IdentityReadSubjectRef::new(format!("member-read-subject:{token}")),
            consumer_ref: ConsumerRef::new("consumer-1"),
            actor_ref: Some(ActorRef::new("actor-1", ActorKind::Human)),
            visibility_context_ref: VisibilityContextRef::new("context-1"),
            scope_ref: scope_ref("scope-a"),
            access_state,
            redaction_profile_ref: None,
            redaction_marker_ref: matches!(
                access_state,
                IdentityVisibilityAccessState::Redacted | IdentityVisibilityAccessState::NotVisible
            )
            .then(|| IdentityRedactionMarkerRef::new(format!("member-redaction:{token}"))),
            visibility_result_ref: visibility_result(&format!("member-visibility-{token}")),
            degraded_marker_ref: None,
            degraded_kind: None,
        }
    }

    fn trace_access_summary(
        token: &str,
        access_state: IdentityVisibilityAccessState,
    ) -> IdentityVisibilityAccessSummary {
        IdentityVisibilityAccessSummary {
            read_subject_ref: IdentityReadSubjectRef::new(format!("trace-read-subject:{token}")),
            consumer_ref: ConsumerRef::new("consumer-1"),
            actor_ref: Some(ActorRef::new("actor-1", ActorKind::Human)),
            visibility_context_ref: VisibilityContextRef::new("context-1"),
            scope_ref: scope_ref("scope-a"),
            access_state,
            redaction_profile_ref: None,
            redaction_marker_ref: matches!(
                access_state,
                IdentityVisibilityAccessState::Redacted | IdentityVisibilityAccessState::NotVisible
            )
            .then(|| IdentityRedactionMarkerRef::new(format!("trace-redaction:{token}"))),
            visibility_result_ref: visibility_result(&format!("trace-visibility-{token}")),
            degraded_marker_ref: None,
            degraded_kind: None,
        }
    }

    fn operations_access_summary(
        token: &str,
        access_state: IdentityVisibilityAccessState,
    ) -> IdentityVisibilityAccessSummary {
        IdentityVisibilityAccessSummary {
            read_subject_ref: IdentityReadSubjectRef::new(format!("ops-read-subject:{token}")),
            consumer_ref: ConsumerRef::new("consumer-1"),
            actor_ref: Some(ActorRef::new("actor-1", ActorKind::Human)),
            visibility_context_ref: VisibilityContextRef::new("context-1"),
            scope_ref: scope_ref("ops-scope-a"),
            access_state,
            redaction_profile_ref: None,
            redaction_marker_ref: matches!(
                access_state,
                IdentityVisibilityAccessState::Redacted | IdentityVisibilityAccessState::NotVisible
            )
            .then(|| IdentityRedactionMarkerRef::new(format!("ops-redaction:{token}"))),
            visibility_result_ref: visibility_result(&format!("ops-visibility-{token}")),
            degraded_marker_ref: matches!(
                access_state,
                IdentityVisibilityAccessState::Degraded
                    | IdentityVisibilityAccessState::Unavailable
            )
            .then(|| IdentityDegradedMarkerRef::new(format!("ops-degraded:{token}"))),
            degraded_kind: matches!(
                access_state,
                IdentityVisibilityAccessState::Degraded
                    | IdentityVisibilityAccessState::Unavailable
            )
            .then_some(IdentityDegradedKind::DependencyUnavailable),
        }
    }

    fn reconciliation_report(
        token: &str,
        scope_ref: identity_contracts::refs::MaintenanceScopeRef,
        report_state: identity_domain::reconciliation::ReconciliationReportStateKind,
    ) -> ReconciliationReport {
        let target_refs = vec![maintenance_target_ref(
            &format!("target-{token}"),
            IdentityMaintenanceTargetKind::Projection,
        )];
        let finding_refs = matches!(
            report_state,
            identity_domain::reconciliation::ReconciliationReportStateKind::FindingDetected
        )
        .then(|| vec![reconciliation_finding_ref(&format!("finding-{token}"))])
        .unwrap_or_default();
        let issue_refs = matches!(
            report_state,
            identity_domain::reconciliation::ReconciliationReportStateKind::Partial
                | identity_domain::reconciliation::ReconciliationReportStateKind::Failed
        )
        .then(|| {
            vec![maintenance_issue_ref(
                &format!("issue-{token}"),
                MaintenanceIssueKind::Failed,
            )]
        })
        .unwrap_or_default();
        ReconciliationReport {
            report_ref: reconciliation_report_ref(&format!("report-{token}")),
            maintenance_scope_ref: scope_ref,
            target_refs,
            finding_refs,
            issue_refs,
            report_state,
            generated_by_ref: Some(ActorRef::new("actor-1", ActorKind::Human)),
            generated_at: timestamp(1),
        }
    }

    #[test]
    fn query_context_assertion_rejects_write_channel() {
        let request = query_request();
        let context = query_context();
        IdentityQueryService::assert_query_context(&request, &context)
            .expect("query context should pass");

        let mismatched = IdentityOperationContext::from_command(
            IdentityOperationContextRef::new("wrong-context-1"),
            IdentityOperationName::new("ReadMemberSummary"),
            ActorRef::new("actor-1", ActorKind::Human),
            identity_application::support::IdentityRequestMetadataRef::new("query-metadata-1"),
            None,
            IdentityRequestDigest::from_canonical_marker(
                IdentityCanonicalRequestMarkerRef::new("canonical-query-1"),
                IdentityRequestDigestValue::new("digest-query-1"),
                IdentityProtocolSchemaVersionRef::new("identity.query.v1"),
                IdentityDigestAlgorithmMarkerRef::new("sha256-v1"),
            ),
            None,
            timestamp(1),
        );
        let error = IdentityQueryService::assert_query_context(&request, &mismatched)
            .expect_err("command context must fail query validation");
        assert_eq!(error.kind, ApplicationErrorKind::InvalidRequest);
    }

    #[test]
    fn member_summary_preflight_uses_formal_subject_scope_and_stable_lookup() {
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_member_summary_view(summary_view("scope-a"), IdentityVersion::new(1))
            .seed_member_summary_access(
                member_ref("member-1"),
                IdentityVisibilityAccessSummary {
                    read_subject_ref: IdentityReadSubjectRef::new("read-subject:member-1"),
                    consumer_ref: ConsumerRef::new("consumer-1"),
                    actor_ref: Some(ActorRef::new("actor-1", ActorKind::Human)),
                    visibility_context_ref: VisibilityContextRef::new("context-1"),
                    scope_ref: scope_ref("scope-a"),
                    access_state: IdentityVisibilityAccessState::Visible,
                    redaction_profile_ref: None,
                    redaction_marker_ref: None,
                    visibility_result_ref: visibility_result("visibility-a"),
                    degraded_marker_ref: None,
                    degraded_kind: None,
                },
            )
            .build();
        let service = query_service(&runtime);

        let prepared = service
            .prepare_member_summary_read(
                member_ref("member-1"),
                ConsumerRef::new("consumer-1"),
                VisibilityContextRef::new("context-1"),
            )
            .expect("preflight");

        assert_eq!(
            prepared.access_summary.read_subject_ref,
            IdentityReadSubjectRef::new("read-subject:member-1")
        );
        assert_eq!(prepared.access_summary.scope_ref, scope_ref("scope-a"));
        assert_eq!(prepared.view_ref, MemberSummaryViewRef::new("view-scope-a"));
    }

    #[test]
    fn member_summary_preflight_is_no_write_and_rejects_scope_mismatch() {
        let mismatched_view = MemberSummaryView::from_projection(
            MemberSummaryViewRef::new("view-scope-a"),
            member_ref("member-1"),
            scope_ref("scope-b"),
            MemberSummarySliceRef::new(
                MemberSummarySliceKind::Anchor,
                member_ref("member-1"),
                identity_source_ref(IdentitySourceOwner::Identity, "summary-source-1"),
            ),
            MemberSummarySliceRef::new(
                MemberSummarySliceKind::Lifecycle,
                member_ref("member-1"),
                identity_source_ref(IdentitySourceOwner::Identity, "summary-source-1"),
            ),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            visibility_result("visibility-a"),
            IdentityReadSurfaceKind::Found,
            Some(IdentityTruthCursor::new("truth-cursor-1")),
            Some(ProjectionFreshnessMarkerRef {
                projection_ref: projection_ref("projection-1"),
                state_kind: "stale".into(),
            }),
            IdentityReadMaterialMarker::new(IdentityReadMaterialKind::SafeSummaryRefs, None),
        )
        .expect("view");

        let runtime = IdentityInMemoryRuntime::builder()
            .seed_member_summary_view_with_lookup_scope(
                member_ref("member-1"),
                scope_ref("scope-a"),
                mismatched_view,
                IdentityVersion::new(1),
            )
            .seed_member_summary_access(
                member_ref("member-1"),
                IdentityVisibilityAccessSummary {
                    read_subject_ref: IdentityReadSubjectRef::new("read-subject:member-1"),
                    consumer_ref: ConsumerRef::new("consumer-1"),
                    actor_ref: Some(ActorRef::new("actor-1", ActorKind::Human)),
                    visibility_context_ref: VisibilityContextRef::new("context-1"),
                    scope_ref: scope_ref("scope-a"),
                    access_state: IdentityVisibilityAccessState::Visible,
                    redaction_profile_ref: None,
                    redaction_marker_ref: None,
                    visibility_result_ref: visibility_result("visibility-a"),
                    degraded_marker_ref: None,
                    degraded_kind: None,
                },
            )
            .build();
        let service = query_service(&runtime);

        let active_before = runtime.active_write_transactions().expect("active writes");
        let staged_before = runtime.staged_write_count().expect("staged writes");
        let error = service
            .prepare_member_summary_read(
                member_ref("member-1"),
                ConsumerRef::new("consumer-1"),
                VisibilityContextRef::new("context-1"),
            )
            .expect_err("scope mismatch must surface as consistency defect");
        let active_after = runtime.active_write_transactions().expect("active writes");
        let staged_after = runtime.staged_write_count().expect("staged writes");

        assert_eq!(error.kind, ApplicationErrorKind::ConsistencyDefect);
        assert_eq!(active_before, 0);
        assert_eq!(active_after, 0);
        assert_eq!(staged_before, 0);
        assert_eq!(staged_after, 0);
    }

    #[test]
    fn get_global_member_anchor_missing_returns_missing_without_create() {
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_member_summary_access(
                member_ref("member-anchor-missing-1"),
                member_access_summary("anchor-missing", IdentityVisibilityAccessState::Visible),
            )
            .build();
        let service = query_service(&runtime);
        let active_before = runtime.active_write_transactions().expect("active writes");
        let staged_before = runtime.staged_write_count().expect("staged writes");

        let response = service
            .get_global_member_anchor(
                single_query_request(
                    "GetGlobalMemberAnchor",
                    GetGlobalMemberAnchorRequest {
                        member_ref: member_ref("member-anchor-missing-1"),
                        consumer_ref: ConsumerRef::new("consumer-1"),
                    },
                ),
                named_query_context("GetGlobalMemberAnchor", "missing"),
            )
            .expect("anchor query");

        let active_after = runtime.active_write_transactions().expect("active writes");
        let staged_after = runtime.staged_write_count().expect("staged writes");

        assert_eq!(
            response.surface.disposition,
            IdentityQueryDisposition::Missing
        );
        assert!(response.body.is_none());
        assert!(
            runtime
                .get_member_with_version(member_ref("member-anchor-missing-1"))
                .expect("load member")
                .is_none()
        );
        assert_eq!(active_before, 0);
        assert_eq!(active_after, 0);
        assert_eq!(staged_before, 0);
        assert_eq!(staged_after, 0);
    }

    #[test]
    fn core_member_queries_return_body_free_material_without_write() {
        let member = GlobalMember::establish(
            member_ref("member-query-core-1"),
            identity_source_ref(IdentitySourceOwner::Identity, "member-source-query-core-1"),
            ActorRef::new("actor-1", ActorKind::Human),
            timestamp(1),
        )
        .expect("member");
        let lifecycle = identity_domain::lifecycle::GlobalLifecycleState::initial_available(
            ActorRef::new("actor-1", ActorKind::Human),
            lifecycle_reason(
                "member-source-query-core-1",
                LifecycleReasonKind::InitialProvisioned,
            ),
            timestamp(1),
        );
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_member(member.clone(), IdentityVersion::new(1))
            .seed_lifecycle(
                member.member_ref.clone(),
                lifecycle,
                IdentityVersion::new(1),
            )
            .seed_member_summary_access(
                member.member_ref.clone(),
                member_access_summary("core-visible", IdentityVisibilityAccessState::Visible),
            )
            .build();
        let command = command_service(&runtime);
        command
            .maintain_role_capability_summary(
                maintain_role_request(
                    "query-core-role-1",
                    member.member_ref.clone(),
                    "query-core-role-source-1",
                ),
                command_context(
                    "MaintainRoleCapabilitySummary",
                    "idem-role-query-core-role-1",
                    "role-query-core-role-1",
                ),
            )
            .expect("seed role summary");
        command
            .append_career_record(
                append_career_request(
                    "query-core-career-1",
                    member.member_ref.clone(),
                    "query-core-work-source-1",
                    WorkSourceKind::ProjectParticipationAccepted,
                    CareerRecordChangeIntent::AppendNew,
                    None,
                ),
                command_context(
                    "AppendCareerRecord",
                    "idem-career-query-core-career-1",
                    "career-query-core-career-1",
                ),
            )
            .expect("seed career record");
        command
            .maintain_memory_reference(
                maintain_memory_request(
                    "query-core-memory-1",
                    member.member_ref.clone(),
                    "query-core-memory-source-1",
                    MemoryReferenceSourceKind::MemorySourceEvent,
                    MemoryReferenceChangeIntent::LinkMemory,
                    None,
                ),
                command_context(
                    "MaintainMemoryReference",
                    "idem-memory-query-core-memory-1",
                    "memory-query-core-memory-1",
                ),
            )
            .expect("seed memory reference");

        let service = query_service(&runtime);
        let active_before = runtime.active_write_transactions().expect("active writes");
        let staged_before = runtime.staged_write_count().expect("staged writes");

        let anchor = service
            .get_global_member_anchor(
                single_query_request(
                    "GetGlobalMemberAnchor",
                    GetGlobalMemberAnchorRequest {
                        member_ref: member.member_ref.clone(),
                        consumer_ref: ConsumerRef::new("consumer-1"),
                    },
                ),
                named_query_context("GetGlobalMemberAnchor", "visible"),
            )
            .expect("anchor query");
        assert_eq!(
            anchor.surface.disposition,
            IdentityQueryDisposition::Visible
        );
        assert_eq!(
            anchor.body.expect("anchor view").member_ref,
            member.member_ref.clone()
        );

        let lifecycle_summary = service
            .get_global_lifecycle_summary(
                single_query_request(
                    "GetGlobalLifecycleSummary",
                    GetGlobalLifecycleSummaryRequest {
                        member_ref: member.member_ref.clone(),
                        consumer_ref: ConsumerRef::new("consumer-1"),
                    },
                ),
                named_query_context("GetGlobalLifecycleSummary", "visible"),
            )
            .expect("lifecycle query");
        assert_eq!(
            lifecycle_summary
                .body
                .expect("lifecycle view")
                .lifecycle_state_kind,
            PublicLifecycleStateKind::Available
        );

        let role_summary = service
            .get_role_capability_summary(
                single_query_request(
                    "GetRoleCapabilitySummary",
                    GetRoleCapabilitySummaryRequest {
                        member_ref: member.member_ref.clone(),
                        consumer_ref: ConsumerRef::new("consumer-1"),
                        summary_ref: None,
                    },
                ),
                named_query_context("GetRoleCapabilitySummary", "visible"),
            )
            .expect("role query");
        assert_eq!(
            role_summary.surface.disposition,
            IdentityQueryDisposition::Visible
        );
        let role_view = role_summary.body.expect("role view");
        assert_eq!(role_view.member_ref, member.member_ref.clone());
        assert!(role_view.safe_summary_ref.is_some());

        let careers = service
            .list_career_records(
                paged_query_request(
                    "ListCareerRecords",
                    ListCareerRecordsRequest {
                        member_ref: member.member_ref.clone(),
                        consumer_ref: ConsumerRef::new("consumer-1"),
                    },
                ),
                named_query_context("ListCareerRecords", "visible"),
            )
            .expect("career query");
        assert_eq!(
            careers.surface.disposition,
            IdentityQueryDisposition::Visible
        );
        assert_eq!(careers.items.len(), 1);

        let memories = service
            .list_memory_references(
                paged_query_request(
                    "ListMemoryReferences",
                    ListMemoryReferencesRequest {
                        member_ref: member.member_ref.clone(),
                        consumer_ref: ConsumerRef::new("consumer-1"),
                    },
                ),
                named_query_context("ListMemoryReferences", "visible"),
            )
            .expect("memory query");
        assert_eq!(
            memories.surface.disposition,
            IdentityQueryDisposition::Visible
        );
        assert_eq!(memories.items.len(), 1);

        let active_after = runtime.active_write_transactions().expect("active writes");
        let staged_after = runtime.staged_write_count().expect("staged writes");
        assert_eq!(active_before, 0);
        assert_eq!(active_after, 0);
        assert_eq!(staged_before, 0);
        assert_eq!(staged_after, 0);
    }

    #[test]
    fn read_member_summary_missing_freshness_returns_material_degraded_surface() {
        let mut view = MemberSummaryView::from_projection(
            MemberSummaryViewRef::new("view-scope-a"),
            member_ref("member-1"),
            scope_ref("scope-a"),
            MemberSummarySliceRef::new(
                MemberSummarySliceKind::Anchor,
                member_ref("member-1"),
                identity_source_ref(IdentitySourceOwner::Identity, "summary-source-1"),
            ),
            MemberSummarySliceRef::new(
                MemberSummarySliceKind::Lifecycle,
                member_ref("member-1"),
                identity_source_ref(IdentitySourceOwner::Identity, "summary-source-1"),
            ),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            visibility_result("summary-visibility-1"),
            IdentityReadSurfaceKind::Found,
            Some(IdentityTruthCursor::new("truth-cursor-1")),
            None,
            IdentityReadMaterialMarker::new(IdentityReadMaterialKind::SafeSummaryRefs, None),
        )
        .expect("view");
        view.read_surface_kind = IdentityReadSurfaceKind::Stale;
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_member_summary_view_with_lookup_scope(
                member_ref("member-1"),
                scope_ref("scope-a"),
                view,
                IdentityVersion::new(1),
            )
            .seed_member_summary_access(
                member_ref("member-1"),
                member_access_summary("summary-visible", IdentityVisibilityAccessState::Visible),
            )
            .build();
        let service = query_service(&runtime);

        let response = service
            .read_member_summary(
                single_query_request(
                    "ReadMemberSummary",
                    ReadMemberSummaryRequest {
                        member_ref: member_ref("member-1"),
                        consumer_ref: ConsumerRef::new("consumer-1"),
                    },
                ),
                named_query_context("ReadMemberSummary", "missing-freshness"),
            )
            .expect("member summary query");

        assert_eq!(
            response.surface.disposition,
            IdentityQueryDisposition::Degraded
        );
        assert_eq!(
            response
                .surface
                .degraded
                .as_ref()
                .expect("degraded marker")
                .degraded_kind,
            IdentityDegradedKind::MaterialUnsafe
        );
        assert!(response.body.is_none());
    }

    #[test]
    fn read_identity_trace_by_member_empty_copies_page_access_without_write() {
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_trace_member_page_access(
                member_ref("member-trace-empty-1"),
                None,
                trace_access_summary("member-empty", IdentityVisibilityAccessState::Visible),
            )
            .build();
        let service = query_service(&runtime);
        let active_before = runtime.active_write_transactions().expect("active writes");
        let staged_before = runtime.staged_write_count().expect("staged writes");

        let response = service
            .read_identity_trace(
                trace_query_request(IdentityTraceReadSelector::ByMember {
                    member_ref: member_ref("member-trace-empty-1"),
                }),
                trace_query_context(),
            )
            .expect("trace query");

        let active_after = runtime.active_write_transactions().expect("active writes");
        let staged_after = runtime.staged_write_count().expect("staged writes");

        assert_eq!(
            response.surface.disposition,
            IdentityQueryDisposition::Empty
        );
        assert_eq!(
            response.surface.visibility.visibility_result_ref,
            visibility_result("trace-visibility-member-empty")
        );
        assert_eq!(
            response.surface.visibility.read_surface_kind,
            IdentityReadSurfaceKind::Empty
        );
        assert!(response.items.is_empty());
        assert_eq!(active_before, 0);
        assert_eq!(active_after, 0);
        assert_eq!(staged_before, 0);
        assert_eq!(staged_after, 0);
    }

    #[test]
    fn read_identity_trace_by_member_first_missing_uses_page_access_degradation() {
        let trace = trace_record("member-missing-1", member_ref("member-trace-missing-1"));
        let missing_trace_ref = trace.trace_record_ref.clone();
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_trace_record(trace, IdentityVersion::new(1))
            .seed_trace_member_page_access(
                member_ref("member-trace-missing-1"),
                None,
                trace_access_summary("member-missing", IdentityVisibilityAccessState::Visible),
            )
            .build();
        runtime
            .shared
            .store
            .lock()
            .expect("lock runtime store")
            .trace_records
            .remove(missing_trace_ref.as_str());
        let service = query_service(&runtime);

        let response = service
            .read_identity_trace(
                trace_query_request(IdentityTraceReadSelector::ByMember {
                    member_ref: member_ref("member-trace-missing-1"),
                }),
                trace_query_context(),
            )
            .expect("trace query");

        assert_eq!(
            response.surface.disposition,
            IdentityQueryDisposition::Degraded
        );
        assert_eq!(
            response.surface.visibility.visibility_result_ref,
            visibility_result("trace-visibility-member-missing")
        );
        assert_eq!(
            response
                .surface
                .degraded
                .as_ref()
                .expect("degraded marker")
                .degraded_kind,
            IdentityDegradedKind::PartialResult
        );
        assert!(response.items.is_empty());
    }

    #[test]
    fn read_identity_trace_by_subject_redacts_item_fields_and_copies_visibility_result() {
        let trace = trace_record("subject-redacted-1", member_ref("member-trace-redacted-1"));
        let subject_ref = trace.subject_ref.clone();
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_trace_record(trace, IdentityVersion::new(1))
            .seed_trace_read_access(
                subject_ref.clone(),
                trace_access_summary("subject-redacted", IdentityVisibilityAccessState::Redacted),
            )
            .build();
        let service = query_service(&runtime);

        let response = service
            .read_identity_trace(
                trace_query_request(IdentityTraceReadSelector::BySubject {
                    member_ref: member_ref("member-trace-redacted-1"),
                    subject_ref,
                    after_cursor_ref: None,
                }),
                trace_query_context(),
            )
            .expect("trace query");

        assert_eq!(
            response.surface.disposition,
            IdentityQueryDisposition::Redacted
        );
        assert_eq!(response.items.len(), 1);
        assert_eq!(
            response.items[0].visibility_result_ref,
            visibility_result("trace-visibility-subject-redacted")
        );
        assert_eq!(response.items[0].reason_ref, None);
        assert_eq!(response.items[0].source_ref, None);
        assert_eq!(response.items[0].actor_ref, None);
    }

    #[test]
    fn read_audit_trail_uses_member_canonical_subject_and_stays_read_only() {
        let requested_member = member_ref("member-audit-query-1");
        let runtime = IdentityInMemoryRuntime::builder().build();
        let command = command_service(&runtime);
        command
            .establish_global_member(
                establish_request("audit-query-1", Some(requested_member.clone())),
                establish_context("audit-query-1"),
            )
            .expect("establish member");

        let subjects = identity_application::DefaultIdentityTruthChangeSubjectMapper
            .member_subjects(requested_member.clone());
        let trail = runtime
            .find_audit_trail_by_subject(subjects.audit_subject_ref.clone())
            .expect("find audit trail")
            .expect("member audit trail")
            .value;
        runtime
            .shared
            .store
            .lock()
            .expect("lock runtime store")
            .audit_read_access
            .insert(
                audit_access_key(&subjects.audit_subject_ref, &trail.audit_scope_ref),
                member_access_summary("audit-visible", IdentityVisibilityAccessState::Visible),
            );

        let service = query_service(&runtime);
        let active_before = runtime.active_write_transactions().expect("active writes");
        let staged_before = runtime.staged_write_count().expect("staged writes");

        let response = service
            .read_audit_trail(
                paged_query_request(
                    "ReadAuditTrail",
                    ReadAuditTrailRequest {
                        member_ref: requested_member,
                        audit_scope_ref: trail.audit_scope_ref.clone(),
                        audit_cursor_ref: None,
                        consumer_ref: ConsumerRef::new("consumer-1"),
                    },
                ),
                named_query_context("ReadAuditTrail", "visible"),
            )
            .expect("audit query");

        let active_after = runtime.active_write_transactions().expect("active writes");
        let staged_after = runtime.staged_write_count().expect("staged writes");

        assert_eq!(
            response.surface.disposition,
            IdentityQueryDisposition::Visible
        );
        assert!(!response.items.is_empty());
        assert_eq!(response.items[0].audit_trail_ref, trail.audit_trail_ref);
        assert_eq!(active_before, 0);
        assert_eq!(active_after, 0);
        assert_eq!(staged_before, 0);
        assert_eq!(staged_after, 0);
    }

    #[test]
    fn get_projection_state_stale_returns_freshness_marker_without_write() {
        let state = projection_state("ops-stale-1");
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_projection_state(state.clone(), IdentityVersion::new(1))
            .seed_projection_state_read_access(
                state.projection_ref.clone(),
                None,
                operations_access_summary(
                    "projection-stale-initial",
                    IdentityVisibilityAccessState::Visible,
                ),
            )
            .seed_projection_state_read_access(
                state.projection_ref.clone(),
                Some(state.projection_state_ref.clone()),
                operations_access_summary(
                    "projection-stale-final",
                    IdentityVisibilityAccessState::Visible,
                ),
            )
            .build();
        let service = query_service(&runtime);
        let active_before = runtime.active_write_transactions().expect("active writes");
        let staged_before = runtime.staged_write_count().expect("staged writes");

        let response = service
            .get_projection_state(
                single_query_request(
                    "GetProjectionState",
                    GetProjectionStateRequest {
                        projection_ref: state.projection_ref.clone(),
                        projection_state_ref: None,
                        consumer_ref: ConsumerRef::new("consumer-1"),
                    },
                ),
                named_query_context("GetProjectionState", "stale"),
            )
            .expect("projection query");

        let active_after = runtime.active_write_transactions().expect("active writes");
        let staged_after = runtime.staged_write_count().expect("staged writes");

        assert_eq!(
            response.surface.disposition,
            IdentityQueryDisposition::StaleVisible
        );
        assert_eq!(
            response
                .surface
                .projection_freshness_ref
                .as_ref()
                .expect("freshness marker")
                .state_kind,
            "stale"
        );
        assert_eq!(
            response.body.expect("projection state view").state_kind,
            Some(PublicProjectionStateKind::Stale)
        );
        assert_eq!(active_before, 0);
        assert_eq!(active_after, 0);
        assert_eq!(staged_before, 0);
        assert_eq!(staged_after, 0);
    }

    #[test]
    fn get_reference_resolution_state_returns_bundle_without_write() {
        let state = reference_state_resolved();
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_reference_state(
                state.clone(),
                ExternalReferenceTypedSidecarRefs {
                    role_capability_safe_summary_ref: None,
                    career_safe_summary_ref: None,
                    memory_safe_summary_ref: None,
                    governance_basis_summary_ref: None,
                    evidence_summary_ref: None,
                    source_version_ref: Some(ExternalSourceVersionRef::new(identity_source_ref(
                        IdentitySourceOwner::MethodLibrary,
                        "query-reference-sidecar-version-1",
                    ))),
                },
                IdentityVersion::new(1),
            )
            .seed_reference_state_read_access(
                state.external_reference_ref.clone(),
                None,
                operations_access_summary(
                    "reference-visible",
                    IdentityVisibilityAccessState::Visible,
                ),
            )
            .build();
        let service = query_service(&runtime);
        let active_before = runtime.active_write_transactions().expect("active writes");
        let staged_before = runtime.staged_write_count().expect("staged writes");

        let response = service
            .get_reference_resolution_state(
                single_query_request(
                    "GetReferenceResolutionState",
                    GetReferenceResolutionStateRequest {
                        external_reference_ref: state.external_reference_ref.clone(),
                        owner_ref: None,
                        consumer_ref: ConsumerRef::new("consumer-1"),
                    },
                ),
                named_query_context("GetReferenceResolutionState", "visible"),
            )
            .expect("reference query");

        let active_after = runtime.active_write_transactions().expect("active writes");
        let staged_after = runtime.staged_write_count().expect("staged writes");
        let body = response.body.expect("reference state view");

        assert_eq!(
            response.surface.disposition,
            IdentityQueryDisposition::Visible
        );
        assert_eq!(
            body.state_kind,
            Some(PublicReferenceResolutionStateKind::Resolved)
        );
        assert!(body.sidecar_refs.is_some());
        assert_eq!(active_before, 0);
        assert_eq!(active_after, 0);
        assert_eq!(staged_before, 0);
        assert_eq!(staged_after, 0);
    }

    #[test]
    fn read_reconciliation_report_exact_visible_stays_read_only() {
        let scope = maintenance_scope("report-query-scope-1");
        let report = reconciliation_report(
            "report-query-visible-1",
            scope.clone(),
            identity_domain::reconciliation::ReconciliationReportStateKind::Generated,
        );
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_reconciliation_report(report.clone(), IdentityVersion::new(1))
            .seed_reconciliation_scope_read_access(
                scope.clone(),
                operations_access_summary(
                    "report-scope-visible",
                    IdentityVisibilityAccessState::Visible,
                ),
            )
            .seed_report_read_access(
                report.report_ref.clone(),
                operations_access_summary(
                    "report-item-visible",
                    IdentityVisibilityAccessState::Visible,
                ),
            )
            .build();
        let service = query_service(&runtime);
        let active_before = runtime.active_write_transactions().expect("active writes");
        let staged_before = runtime.staged_write_count().expect("staged writes");

        let response = service
            .read_reconciliation_report(
                single_query_request(
                    "ReadReconciliationReport",
                    ReadReconciliationReportRequest {
                        maintenance_scope_ref: scope,
                        report_ref: Some(report.report_ref.clone()),
                        consumer_ref: ConsumerRef::new("consumer-1"),
                    },
                ),
                named_query_context("ReadReconciliationReport", "exact-visible"),
            )
            .expect("report query");

        let active_after = runtime.active_write_transactions().expect("active writes");
        let staged_after = runtime.staged_write_count().expect("staged writes");

        assert_eq!(
            response.surface.disposition,
            IdentityQueryDisposition::Visible
        );
        assert_eq!(response.items.len(), 1);
        assert_eq!(response.items[0].report_ref, report.report_ref);
        assert_eq!(active_before, 0);
        assert_eq!(active_after, 0);
        assert_eq!(staged_before, 0);
        assert_eq!(staged_after, 0);
    }

    #[test]
    fn list_pending_identity_outbox_by_trace_empty_copies_page_access_without_write() {
        let trace_record_ref = IdentityTraceRecordRef::new("trace-outbox-empty-1");
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_outbox_trace_page_read_access(
                trace_record_ref.clone(),
                operations_access_summary(
                    "outbox-trace-empty",
                    IdentityVisibilityAccessState::Visible,
                ),
            )
            .build();
        let service = query_service(&runtime);
        let active_before = runtime.active_write_transactions().expect("active writes");
        let staged_before = runtime.staged_write_count().expect("staged writes");

        let response = service
            .list_pending_identity_outbox(
                paged_query_request(
                    "ListPendingIdentityOutbox",
                    ListPendingIdentityOutboxRequest {
                        selector: IdentityOutboxListSelector::ByTrace { trace_record_ref },
                        consumer_ref: ConsumerRef::new("consumer-1"),
                    },
                ),
                named_query_context("ListPendingIdentityOutbox", "trace-empty"),
            )
            .expect("outbox page query");

        let active_after = runtime.active_write_transactions().expect("active writes");
        let staged_after = runtime.staged_write_count().expect("staged writes");

        assert_eq!(
            response.surface.disposition,
            IdentityQueryDisposition::Empty
        );
        assert_eq!(
            response.surface.visibility.visibility_result_ref,
            visibility_result("ops-visibility-outbox-trace-empty")
        );
        assert!(response.items.is_empty());
        assert_eq!(active_before, 0);
        assert_eq!(active_after, 0);
        assert_eq!(staged_before, 0);
        assert_eq!(staged_after, 0);
    }

    #[test]
    fn list_pending_identity_outbox_by_trace_missing_item_uses_item_degradation() {
        let record = outbox_record("trace-missing-1", OutboxState::pending(timestamp(1)));
        let outbox_ref = record.outbox_record_ref.clone();
        let trace_record_ref = record.trace_record_ref.clone();
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_outbox_record(record, IdentityVersion::new(1))
            .seed_outbox_trace_page_read_access(
                trace_record_ref.clone(),
                operations_access_summary(
                    "outbox-trace-page",
                    IdentityVisibilityAccessState::Visible,
                ),
            )
            .seed_outbox_record_read_access(
                Some(outbox_ref.clone()),
                None,
                None,
                operations_access_summary(
                    "outbox-trace-ref",
                    IdentityVisibilityAccessState::Visible,
                ),
            )
            .build();
        runtime
            .shared
            .store
            .lock()
            .expect("lock runtime store")
            .outbox_records
            .remove(outbox_ref.as_str());
        let service = query_service(&runtime);

        let response = service
            .list_pending_identity_outbox(
                paged_query_request(
                    "ListPendingIdentityOutbox",
                    ListPendingIdentityOutboxRequest {
                        selector: IdentityOutboxListSelector::ByTrace { trace_record_ref },
                        consumer_ref: ConsumerRef::new("consumer-1"),
                    },
                ),
                named_query_context("ListPendingIdentityOutbox", "trace-missing"),
            )
            .expect("outbox page query");

        assert_eq!(
            response.surface.disposition,
            IdentityQueryDisposition::Degraded
        );
        assert_eq!(
            response
                .surface
                .degraded
                .as_ref()
                .expect("degraded marker")
                .degraded_kind,
            IdentityDegradedKind::PartialResult
        );
        assert!(response.items.is_empty());
    }

    #[test]
    fn get_identity_outbox_state_returns_body_free_state_without_write() {
        let record = outbox_record(
            "state-visible-1",
            OutboxState::retryable_failed(
                identity_contracts::refs::OutboxDeliveryIssueRef::new(identity_source_ref(
                    IdentitySourceOwner::Identity,
                    "outbox-issue-state-visible-1",
                )),
                timestamp(2),
            ),
        );
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_outbox_record(record.clone(), IdentityVersion::new(1))
            .seed_outbox_record_read_access(
                Some(record.outbox_record_ref.clone()),
                None,
                None,
                operations_access_summary(
                    "outbox-state-initial",
                    IdentityVisibilityAccessState::Visible,
                ),
            )
            .seed_outbox_record_read_access(
                Some(record.outbox_record_ref.clone()),
                Some(record.subject_ref.clone()),
                Some(record.topic_key_ref.clone()),
                operations_access_summary(
                    "outbox-state-final",
                    IdentityVisibilityAccessState::Visible,
                ),
            )
            .build();
        let service = query_service(&runtime);
        let active_before = runtime.active_write_transactions().expect("active writes");
        let staged_before = runtime.staged_write_count().expect("staged writes");

        let response = service
            .get_identity_outbox_state(
                single_query_request(
                    "GetIdentityOutboxState",
                    GetIdentityOutboxStateRequest {
                        outbox_record_ref: record.outbox_record_ref.clone(),
                        consumer_ref: ConsumerRef::new("consumer-1"),
                    },
                ),
                named_query_context("GetIdentityOutboxState", "visible"),
            )
            .expect("outbox state query");

        let active_after = runtime.active_write_transactions().expect("active writes");
        let staged_after = runtime.staged_write_count().expect("staged writes");
        let body = response.body.expect("outbox state view");

        assert_eq!(
            response.surface.disposition,
            IdentityQueryDisposition::Visible
        );
        assert_eq!(
            body.outbox_state_kind,
            PublicOutboxStateKind::RetryableFailed
        );
        assert_eq!(active_before, 0);
        assert_eq!(active_after, 0);
        assert_eq!(staged_before, 0);
        assert_eq!(staged_after, 0);
    }

    #[test]
    fn get_trace_handoff_state_delivered_without_receipt_returns_degraded_surface() {
        let mut intent = handoff_intent();
        intent.handoff_state = HandoffState {
            state_kind: identity_domain::handoff::HandoffStateKind::Delivered,
            attempt_ref: Some(HandoffAttemptRef::new(identity_source_ref(
                IdentitySourceOwner::Identity,
                "handoff-attempt-invalid-1",
            ))),
            receipt_ref: None,
            issue_ref: None,
            changed_at: timestamp(2),
        };
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_handoff_intent(intent.clone(), IdentityVersion::new(1))
            .seed_handoff_intent_read_access(
                intent.handoff_intent_ref.clone(),
                operations_access_summary(
                    "handoff-invalid",
                    IdentityVisibilityAccessState::Visible,
                ),
            )
            .build();
        let service = query_service(&runtime);

        let response = service
            .get_trace_handoff_state(
                single_query_request(
                    "GetTraceHandoffState",
                    GetTraceHandoffStateRequest {
                        handoff_intent_ref: intent.handoff_intent_ref.clone(),
                        consumer_ref: ConsumerRef::new("consumer-1"),
                    },
                ),
                named_query_context("GetTraceHandoffState", "invalid-delivered"),
            )
            .expect("handoff query");

        assert_eq!(
            response.surface.disposition,
            IdentityQueryDisposition::Degraded
        );
        assert_eq!(
            response
                .surface
                .degraded
                .as_ref()
                .expect("degraded marker")
                .degraded_kind,
            IdentityDegradedKind::MaterialUnsafe
        );
        assert!(response.body.is_none());
    }

    #[test]
    fn establish_member_persists_member_lifecycle_trace_audit_and_replay() {
        let runtime = IdentityInMemoryRuntime::builder().build();
        let service = command_service(&runtime);
        let requested_member = member_ref("member-establish-1");

        let accepted = service
            .establish_global_member(
                establish_request("establish-1", Some(requested_member.clone())),
                establish_context("establish-1"),
            )
            .expect("accepted");
        let accepted_response = match accepted {
            IdentityCommandOutcome::Accepted(response) => response,
            other => panic!("unexpected outcome: {other:?}"),
        };
        assert_eq!(accepted_response.result.member_ref, requested_member);
        assert_eq!(
            accepted_response.result.lifecycle_state_kind,
            PublicLifecycleStateKind::Available
        );
        assert_eq!(accepted_response.effect.trace_refs.len(), 1);
        assert_eq!(accepted_response.effect.audit_subject_refs.len(), 1);
        assert_eq!(accepted_response.effect.outbox_refs.len(), 2);
        assert_eq!(outbox_payload_marker_count(&runtime), 2);
        assert_outbox_material_snapshot(
            &runtime,
            &accepted_response.effect.outbox_refs[0],
            AcceptedOutboundMaterialKind::GlobalMemberEstablished,
        );
        assert_outbox_material_snapshot(
            &runtime,
            &accepted_response.effect.outbox_refs[1],
            AcceptedOutboundMaterialKind::IdentityAnchorChanged,
        );

        let persisted_member = runtime
            .get_member_with_version(requested_member.clone())
            .expect("load member")
            .expect("member");
        assert_eq!(persisted_member.value.member_ref, requested_member);
        let persisted_lifecycle = runtime
            .get_lifecycle_with_version(member_ref("member-establish-1"))
            .expect("load lifecycle")
            .expect("lifecycle");
        assert_eq!(
            persisted_lifecycle.value.state_kind,
            identity_domain::lifecycle::GlobalLifecycleStateKind::Available
        );
        let audit = runtime
            .find_audit_trail_by_subject(accepted_response.effect.audit_subject_refs[0].clone())
            .expect("find audit")
            .expect("audit");
        assert_eq!(audit.value.entries.len(), 1);

        let replay = service
            .establish_global_member(
                establish_request("establish-1", Some(member_ref("member-establish-1"))),
                establish_context("establish-1"),
            )
            .expect("replay");
        let replay_response = match replay {
            IdentityCommandOutcome::Accepted(response) => response,
            other => panic!("unexpected replay outcome: {other:?}"),
        };
        assert_eq!(replay_response.result_ref, accepted_response.result_ref);
        assert_eq!(replay_response.effect, accepted_response.effect);
        assert_eq!(outbox_payload_marker_count(&runtime), 2);
    }

    #[test]
    fn establish_member_rolls_back_when_outbox_save_fails() {
        let runtime = IdentityInMemoryRuntime::builder()
            .inject_fault(FaultCase::SaveOutboxRecordFails)
            .build();
        let service = command_service(&runtime);
        let requested_member = member_ref("member-establish-fail-1");

        let error = service
            .establish_global_member(
                establish_request("establish-fail-1", Some(requested_member.clone())),
                establish_context("establish-fail-1"),
            )
            .expect_err("outbox save should fail");
        assert_eq!(error.kind, ApplicationErrorKind::DependencyUnavailable);
        assert!(
            runtime
                .get_member_with_version(requested_member.clone())
                .expect("load member")
                .is_none()
        );
        assert!(
            runtime
                .get_lifecycle_with_version(requested_member.clone())
                .expect("load lifecycle")
                .is_none()
        );
        assert_eq!(outbox_payload_marker_count(&runtime), 0);
        assert_eq!(
            runtime.active_write_transactions().expect("active writes"),
            0
        );
        assert_eq!(runtime.staged_write_count().expect("staged writes"), 0);
    }

    #[test]
    fn update_lifecycle_uses_member_key_and_replays_from_stored_envelope() {
        let member = GlobalMember::establish(
            member_ref("member-lifecycle-1"),
            identity_source_ref(IdentitySourceOwner::Identity, "member-source-1"),
            ActorRef::new("actor-1", ActorKind::Human),
            timestamp(1),
        )
        .expect("member");
        let lifecycle = identity_domain::lifecycle::GlobalLifecycleState::initial_available(
            ActorRef::new("actor-1", ActorKind::Human),
            lifecycle_reason("member-source-1", LifecycleReasonKind::InitialProvisioned),
            timestamp(1),
        );
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_member(member, IdentityVersion::new(1))
            .seed_lifecycle(
                member_ref("member-lifecycle-1"),
                lifecycle,
                IdentityVersion::new(1),
            )
            .build();
        let service = command_service(&runtime);

        let accepted = service
            .update_global_lifecycle_state(
                update_lifecycle_request(
                    "pause-1",
                    member_ref("member-lifecycle-1"),
                    PublicLifecycleStateKind::Paused,
                ),
                update_lifecycle_context("pause-1"),
            )
            .expect("accepted");
        let accepted_response = match accepted {
            IdentityCommandOutcome::Accepted(response) => response,
            other => panic!("unexpected outcome: {other:?}"),
        };
        assert_eq!(
            accepted_response.result.lifecycle_state_kind,
            PublicLifecycleStateKind::Paused
        );
        assert_eq!(accepted_response.effect.outbox_refs.len(), 2);
        assert_outbox_material_snapshot(
            &runtime,
            &accepted_response.effect.outbox_refs[0],
            AcceptedOutboundMaterialKind::GlobalLifecycleChanged,
        );
        assert_outbox_material_snapshot(
            &runtime,
            &accepted_response.effect.outbox_refs[1],
            AcceptedOutboundMaterialKind::GlobalMemberAvailabilityChanged,
        );

        let persisted = runtime
            .get_lifecycle_with_version(member_ref("member-lifecycle-1"))
            .expect("load lifecycle")
            .expect("lifecycle");
        assert_eq!(persisted.version, IdentityVersion::new(2));
        assert_eq!(
            persisted.value.state_kind,
            identity_domain::lifecycle::GlobalLifecycleStateKind::Paused
        );

        let replay = service
            .update_global_lifecycle_state(
                update_lifecycle_request(
                    "pause-1",
                    member_ref("member-lifecycle-1"),
                    PublicLifecycleStateKind::Paused,
                ),
                update_lifecycle_context("pause-1"),
            )
            .expect("replay");
        let replay_response = match replay {
            IdentityCommandOutcome::Accepted(response) => response,
            other => panic!("unexpected replay outcome: {other:?}"),
        };
        assert_eq!(replay_response.result_ref, accepted_response.result_ref);
    }

    #[test]
    fn maintain_role_capability_summary_accepts_and_replays() {
        let member = GlobalMember::establish(
            member_ref("member-role-1"),
            identity_source_ref(IdentitySourceOwner::Identity, "member-source-role-1"),
            ActorRef::new("actor-1", ActorKind::Human),
            timestamp(1),
        )
        .expect("member");
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_member(member, IdentityVersion::new(1))
            .build();
        let service = command_service(&runtime);

        let accepted = service
            .maintain_role_capability_summary(
                maintain_role_request("accept-1", member_ref("member-role-1"), "role-source-1"),
                command_context(
                    "MaintainRoleCapabilitySummary",
                    "idem-role-accept-1",
                    "role-accept-1",
                ),
            )
            .expect("accepted");
        let accepted_response = match accepted {
            IdentityCommandOutcome::Accepted(response) => response,
            other => panic!("unexpected outcome: {other:?}"),
        };
        assert_eq!(
            accepted_response.result.summary_state_kind,
            PublicRoleCapabilitySummaryStateKind::Active
        );
        assert_eq!(accepted_response.effect.outbox_refs.len(), 2);
        assert_outbox_material_snapshot(
            &runtime,
            &accepted_response.effect.outbox_refs[0],
            AcceptedOutboundMaterialKind::RoleCapabilitySummaryChanged,
        );
        assert_outbox_material_snapshot(
            &runtime,
            &accepted_response.effect.outbox_refs[1],
            AcceptedOutboundMaterialKind::RoleCapabilitySourceStateChanged,
        );

        let persisted = runtime
            .find_current_summary_by_member(member_ref("member-role-1"))
            .expect("load summary")
            .expect("summary");
        assert_eq!(
            persisted.value.summary_ref,
            accepted_response.result.summary_ref
        );

        let replay = service
            .maintain_role_capability_summary(
                maintain_role_request("accept-1", member_ref("member-role-1"), "role-source-1"),
                command_context(
                    "MaintainRoleCapabilitySummary",
                    "idem-role-accept-1",
                    "role-accept-1",
                ),
            )
            .expect("replay");
        let replay_response = match replay {
            IdentityCommandOutcome::Accepted(response) => response,
            other => panic!("unexpected replay outcome: {other:?}"),
        };
        assert_eq!(replay_response.result_ref, accepted_response.result_ref);
        assert_eq!(replay_response.effect, accepted_response.effect);
    }

    #[test]
    fn append_career_record_handles_append_correction_and_duplicate_conflict() {
        let member = GlobalMember::establish(
            member_ref("member-career-1"),
            identity_source_ref(IdentitySourceOwner::Identity, "member-source-career-1"),
            ActorRef::new("actor-1", ActorKind::Human),
            timestamp(1),
        )
        .expect("member");
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_member(member, IdentityVersion::new(1))
            .build();
        let service = command_service(&runtime);

        let append = service
            .append_career_record(
                append_career_request(
                    "append-1",
                    member_ref("member-career-1"),
                    "work-source-1",
                    WorkSourceKind::ProjectParticipationAccepted,
                    CareerRecordChangeIntent::AppendNew,
                    None,
                ),
                command_context(
                    "AppendCareerRecord",
                    "idem-career-append-1",
                    "career-append-1",
                ),
            )
            .expect("append");
        let appended = match append {
            IdentityCommandOutcome::Accepted(response) => response,
            other => panic!("unexpected append outcome: {other:?}"),
        };
        assert_eq!(
            appended.result.record_state_kind,
            PublicCareerRecordStateKind::Appended
        );
        assert_eq!(appended.effect.outbox_refs.len(), 1);
        assert_outbox_material_snapshot(
            &runtime,
            &appended.effect.outbox_refs[0],
            AcceptedOutboundMaterialKind::CareerRecordAppended,
        );

        let duplicate = service
            .append_career_record(
                append_career_request(
                    "duplicate-1",
                    member_ref("member-career-1"),
                    "work-source-1",
                    WorkSourceKind::ProjectParticipationAccepted,
                    CareerRecordChangeIntent::AppendNew,
                    None,
                ),
                command_context(
                    "AppendCareerRecord",
                    "idem-career-duplicate-1",
                    "career-duplicate-1",
                ),
            )
            .expect("duplicate outcome");
        assert!(matches!(
            duplicate,
            IdentityCommandOutcome::Rejected(rejection)
                if rejection.rejection_kind == identity_contracts::metadata::IdentityProtocolRejectionKind::Conflict
        ));

        let correction = service
            .append_career_record(
                append_career_request(
                    "correction-1",
                    member_ref("member-career-1"),
                    "work-source-2",
                    WorkSourceKind::WorkCorrection,
                    CareerRecordChangeIntent::AppendCorrection,
                    Some(appended.result.career_record_ref.clone()),
                ),
                command_context(
                    "AppendCareerRecord",
                    "idem-career-correction-1",
                    "career-correction-1",
                ),
            )
            .expect("correction");
        let corrected = match correction {
            IdentityCommandOutcome::Accepted(response) => response,
            other => panic!("unexpected correction outcome: {other:?}"),
        };
        assert_eq!(
            corrected.result.record_state_kind,
            PublicCareerRecordStateKind::CorrectionAppended
        );
        assert_eq!(corrected.effect.outbox_refs.len(), 1);
        assert_outbox_material_snapshot(
            &runtime,
            &corrected.effect.outbox_refs[0],
            AcceptedOutboundMaterialKind::CareerCorrectionAppended,
        );
        let original = runtime
            .get_career_record(appended.result.career_record_ref.clone())
            .expect("load original")
            .expect("original");
        assert_eq!(
            original.value.record_state,
            identity_domain::career::CareerRecordStateKind::SupersededByCorrection
        );
    }

    #[test]
    fn append_career_record_pending_review_accepts_without_outbox() {
        let member = GlobalMember::establish(
            member_ref("member-career-review-1"),
            identity_source_ref(
                IdentitySourceOwner::Identity,
                "member-source-career-review-1",
            ),
            ActorRef::new("actor-1", ActorKind::Human),
            timestamp(1),
        )
        .expect("member");
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_member(member, IdentityVersion::new(1))
            .build();
        let service = command_service(&runtime);

        let accepted = service
            .append_career_record(
                append_career_request(
                    "review-1",
                    member_ref("member-career-review-1"),
                    "pending-review-source-1",
                    WorkSourceKind::PendingReviewMarker,
                    CareerRecordChangeIntent::MarkSourcePendingReview,
                    None,
                ),
                command_context(
                    "AppendCareerRecord",
                    "idem-career-review-1",
                    "career-review-1",
                ),
            )
            .expect("pending review");
        let response = match accepted {
            IdentityCommandOutcome::Accepted(response) => response,
            other => panic!("unexpected review outcome: {other:?}"),
        };
        assert_eq!(
            response.result.record_state_kind,
            PublicCareerRecordStateKind::SourcePendingReview
        );
        assert!(response.effect.outbox_refs.is_empty());
    }

    #[test]
    fn maintain_memory_reference_link_archive_handoff_and_replay() {
        let member = GlobalMember::establish(
            member_ref("member-memory-1"),
            identity_source_ref(IdentitySourceOwner::Identity, "member-source-memory-1"),
            ActorRef::new("actor-1", ActorKind::Human),
            timestamp(1),
        )
        .expect("member");
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_member(member, IdentityVersion::new(1))
            .build();
        let service = command_service(&runtime);

        let linked = service
            .maintain_memory_reference(
                maintain_memory_request(
                    "link-1",
                    member_ref("member-memory-1"),
                    "memory-source-1",
                    MemoryReferenceSourceKind::MemorySourceEvent,
                    MemoryReferenceChangeIntent::LinkMemory,
                    None,
                ),
                command_context(
                    "MaintainMemoryReference",
                    "idem-memory-link-1",
                    "memory-link-1",
                ),
            )
            .expect("linked");
        let linked_response = match linked {
            IdentityCommandOutcome::Accepted(response) => response,
            other => panic!("unexpected memory link outcome: {other:?}"),
        };
        assert_eq!(
            linked_response.result.reference_state_kind,
            PublicMemoryReferenceStateKind::Linked
        );
        assert_eq!(linked_response.effect.outbox_refs.len(), 1);
        assert_outbox_material_snapshot(
            &runtime,
            &linked_response.effect.outbox_refs[0],
            AcceptedOutboundMaterialKind::MemoryReferenceChanged,
        );

        let handoff_ref = ArchiveHandoffRef::new(
            identity_source_ref(IdentitySourceOwner::MemoryArchive, "handoff-source-1"),
            "handoff-1",
        )
        .expect("handoff ref");
        let archived = service
            .maintain_memory_reference(
                maintain_memory_request(
                    "handoff-1",
                    member_ref("member-memory-1"),
                    "handoff-source-1",
                    MemoryReferenceSourceKind::ArchiveHandoffResult,
                    MemoryReferenceChangeIntent::RecordArchiveHandoffResult,
                    Some(handoff_ref.clone()),
                ),
                command_context(
                    "MaintainMemoryReference",
                    "idem-memory-handoff-1",
                    "memory-handoff-1",
                ),
            )
            .expect("archived");
        let archived_response = match archived {
            IdentityCommandOutcome::Accepted(response) => response,
            other => panic!("unexpected memory handoff outcome: {other:?}"),
        };
        assert_eq!(
            archived_response.result.reference_state_kind,
            PublicMemoryReferenceStateKind::Archived
        );
        let persisted = runtime
            .find_reference_by_handoff(handoff_ref)
            .expect("lookup by handoff")
            .expect("reference");
        assert_eq!(
            persisted.value.memory_reference_ref,
            archived_response.result.memory_reference_ref
        );

        let replay = service
            .maintain_memory_reference(
                maintain_memory_request(
                    "handoff-1",
                    member_ref("member-memory-1"),
                    "handoff-source-1",
                    MemoryReferenceSourceKind::ArchiveHandoffResult,
                    MemoryReferenceChangeIntent::RecordArchiveHandoffResult,
                    Some(
                        ArchiveHandoffRef::new(
                            identity_source_ref(
                                IdentitySourceOwner::MemoryArchive,
                                "handoff-source-1",
                            ),
                            "handoff-1",
                        )
                        .expect("handoff ref"),
                    ),
                ),
                command_context(
                    "MaintainMemoryReference",
                    "idem-memory-handoff-1",
                    "memory-handoff-1",
                ),
            )
            .expect("replay");
        let replay_response = match replay {
            IdentityCommandOutcome::Accepted(response) => response,
            other => panic!("unexpected memory replay outcome: {other:?}"),
        };
        assert_eq!(replay_response.result_ref, archived_response.result_ref);
    }

    #[test]
    fn prepare_trace_handoff_accepts_pending_intent_without_delivery() {
        let member = GlobalMember::establish(
            member_ref("member-handoff-1"),
            identity_source_ref(IdentitySourceOwner::Identity, "member-source-handoff-1"),
            ActorRef::new("actor-1", ActorKind::Human),
            timestamp(1),
        )
        .expect("member");
        let trace = trace_record("handoff-1", member_ref("member-handoff-1"));
        let audit = audit_trail("handoff-1", Some(member_ref("member-handoff-1")));
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_member(member, IdentityVersion::new(1))
            .seed_trace_record(trace, IdentityVersion::new(1))
            .seed_audit_trail(audit, IdentityVersion::new(1))
            .seed_adapter_availability(adapter_availability())
            .build();
        let service = command_service(&runtime);

        let accepted = service
            .prepare_trace_handoff(
                prepare_handoff_request(
                    "accept-1",
                    member_ref("member-handoff-1"),
                    vec![IdentityTraceRecordRef::new("trace-handoff-1")],
                    Some(AuditTrailRef::new("audit-handoff-1")),
                    None,
                ),
                command_context(
                    "PrepareTraceHandoff",
                    "idem-handoff-accept-1",
                    "handoff-accept-1",
                ),
            )
            .expect("accepted");
        let response = match accepted {
            IdentityCommandOutcome::Accepted(response) => response,
            other => panic!("unexpected outcome: {other:?}"),
        };
        assert_eq!(
            response.result.handoff_state_kind,
            PublicHandoffStateKind::PendingHandoff
        );
        assert!(response.effect.outbox_refs.is_empty());

        let persisted = runtime
            .get_handoff_intent_with_version(response.result.handoff_intent_ref.clone())
            .expect("load intent")
            .expect("intent");
        assert_eq!(
            persisted.value.handoff_state.state_kind,
            HandoffStateKind::PendingHandoff
        );
        assert_eq!(persisted.value.trace_record_refs.len(), 1);
    }

    #[test]
    fn prepare_trace_handoff_rejects_empty_trace_refs() {
        let member = GlobalMember::establish(
            member_ref("member-handoff-2"),
            identity_source_ref(IdentitySourceOwner::Identity, "member-source-handoff-2"),
            ActorRef::new("actor-1", ActorKind::Human),
            timestamp(1),
        )
        .expect("member");
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_member(member, IdentityVersion::new(1))
            .seed_adapter_availability(adapter_availability())
            .build();
        let service = command_service(&runtime);

        let outcome = service
            .prepare_trace_handoff(
                prepare_handoff_request(
                    "reject-1",
                    member_ref("member-handoff-2"),
                    Vec::new(),
                    None,
                    None,
                ),
                command_context(
                    "PrepareTraceHandoff",
                    "idem-handoff-reject-1",
                    "handoff-reject-1",
                ),
            )
            .expect("rejected");

        match outcome {
            IdentityCommandOutcome::Rejected(rejection) => {
                assert_eq!(
                    rejection.rejection_kind,
                    identity_contracts::metadata::IdentityProtocolRejectionKind::PolicyDenied
                );
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }

    #[test]
    fn prepare_trace_handoff_duplicate_replays_stored_envelope() {
        let member = GlobalMember::establish(
            member_ref("member-handoff-3"),
            identity_source_ref(IdentitySourceOwner::Identity, "member-source-handoff-3"),
            ActorRef::new("actor-1", ActorKind::Human),
            timestamp(1),
        )
        .expect("member");
        let trace = trace_record("handoff-3", member_ref("member-handoff-3"));
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_member(member, IdentityVersion::new(1))
            .seed_trace_record(trace, IdentityVersion::new(1))
            .seed_adapter_availability(adapter_availability())
            .build();
        let service = command_service(&runtime);

        let first = service
            .prepare_trace_handoff(
                prepare_handoff_request(
                    "dup-1",
                    member_ref("member-handoff-3"),
                    vec![IdentityTraceRecordRef::new("trace-handoff-3")],
                    None,
                    None,
                ),
                command_context("PrepareTraceHandoff", "idem-handoff-dup-1", "handoff-dup-1"),
            )
            .expect("accepted");
        let first_response = match first {
            IdentityCommandOutcome::Accepted(response) => response,
            other => panic!("unexpected outcome: {other:?}"),
        };

        let replay = service
            .prepare_trace_handoff(
                prepare_handoff_request(
                    "dup-1",
                    member_ref("member-handoff-3"),
                    vec![IdentityTraceRecordRef::new("trace-handoff-3")],
                    None,
                    None,
                ),
                command_context("PrepareTraceHandoff", "idem-handoff-dup-1", "handoff-dup-1"),
            )
            .expect("replay");
        let replay_response = match replay {
            IdentityCommandOutcome::Accepted(response) => response,
            other => panic!("unexpected replay outcome: {other:?}"),
        };
        assert_eq!(replay_response.result_ref, first_response.result_ref);
        assert_eq!(replay_response.effect, first_response.effect);
    }

    #[test]
    fn prepare_trace_handoff_different_digest_returns_duplicate_conflict() {
        let member = GlobalMember::establish(
            member_ref("member-handoff-5"),
            identity_source_ref(IdentitySourceOwner::Identity, "member-source-handoff-5"),
            ActorRef::new("actor-1", ActorKind::Human),
            timestamp(1),
        )
        .expect("member");
        let trace = trace_record("handoff-5", member_ref("member-handoff-5"));
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_member(member, IdentityVersion::new(1))
            .seed_trace_record(trace, IdentityVersion::new(1))
            .seed_adapter_availability(adapter_availability())
            .build();
        let service = command_service(&runtime);

        let accepted = service
            .prepare_trace_handoff(
                prepare_handoff_request(
                    "dup-conflict-1",
                    member_ref("member-handoff-5"),
                    vec![IdentityTraceRecordRef::new("trace-handoff-5")],
                    None,
                    None,
                ),
                command_context(
                    "PrepareTraceHandoff",
                    "idem-handoff-dup-conflict-1",
                    "handoff-dup-conflict-1",
                ),
            )
            .expect("accepted");
        let accepted_response = match accepted {
            IdentityCommandOutcome::Accepted(response) => response,
            other => panic!("unexpected accepted outcome: {other:?}"),
        };

        let conflict = service
            .prepare_trace_handoff(
                prepare_handoff_request_with_digest(
                    "dup-conflict-1",
                    "dup-conflict-2",
                    member_ref("member-handoff-5"),
                    vec![IdentityTraceRecordRef::new("trace-handoff-5")],
                    None,
                    None,
                ),
                command_context(
                    "PrepareTraceHandoff",
                    "idem-handoff-dup-conflict-1",
                    "handoff-dup-conflict-2",
                ),
            )
            .expect("conflict outcome");

        match conflict {
            IdentityCommandOutcome::Rejected(rejection) => {
                assert_eq!(
                    rejection.rejection_kind,
                    identity_contracts::metadata::IdentityProtocolRejectionKind::DuplicateConflict
                );
            }
            other => panic!("unexpected conflict outcome: {other:?}"),
        }

        let idempotency = runtime
            .get_by_key(
                IdentityOperationName::new("PrepareTraceHandoff"),
                IdentityOperationChannel::Command,
                IdentityIdempotencyKey::new("idem-handoff-dup-conflict-1"),
            )
            .expect("load idempotency")
            .expect("idempotency record");
        assert_eq!(
            idempotency.value.state,
            identity_application::support::IdentityIdempotencyStateKind::Conflict
        );
        assert_eq!(
            idempotency.value.stored_result_ref,
            Some(accepted_response.result_ref.clone())
        );

        let replay = runtime
            .get_command_accepted_result(accepted_response.result_ref.clone())
            .expect("load accepted envelope")
            .expect("accepted envelope");
        assert!(matches!(
            replay.result,
            identity_application::support::IdentityCommandTypedResult::TraceHandoff(ref result)
                if result == &accepted_response.result
        ));
    }

    #[test]
    fn prepare_trace_handoff_rolls_back_when_idempotency_complete_fails() {
        let member = GlobalMember::establish(
            member_ref("member-handoff-6"),
            identity_source_ref(IdentitySourceOwner::Identity, "member-source-handoff-6"),
            ActorRef::new("actor-1", ActorKind::Human),
            timestamp(1),
        )
        .expect("member");
        let trace = trace_record("handoff-6", member_ref("member-handoff-6"));
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_member(member, IdentityVersion::new(1))
            .seed_trace_record(trace, IdentityVersion::new(1))
            .seed_adapter_availability(adapter_availability())
            .inject_fault(FaultCase::CompleteIdempotencyFails)
            .build();
        let service = command_service(&runtime);
        let request = prepare_handoff_request(
            "rollback-1",
            member_ref("member-handoff-6"),
            vec![IdentityTraceRecordRef::new("trace-handoff-6")],
            None,
            None,
        );
        let context = command_context(
            "PrepareTraceHandoff",
            "idem-handoff-rollback-1",
            "handoff-rollback-1",
        );

        let error = service
            .prepare_trace_handoff(request, context.clone())
            .expect_err("idempotency complete failure must abort handoff write");
        assert_eq!(error.kind, ApplicationErrorKind::DependencyUnavailable);

        assert!(
            runtime
                .get_by_key(
                    IdentityOperationName::new("PrepareTraceHandoff"),
                    IdentityOperationChannel::Command,
                    IdentityIdempotencyKey::new("idem-handoff-rollback-1"),
                )
                .expect("load idempotency")
                .is_none()
        );

        assert!(
            runtime
                .find_by_operation_context(context.context_ref.clone())
                .expect("lookup stored result")
                .is_none()
        );
        assert!(
            runtime
                .list_effects_by_operation_context(
                    context.context_ref,
                    IdentityRepositoryPage::new(None, 32),
                )
                .expect("list effects")
                .items
                .is_empty()
        );
        assert!(
            runtime
                .list_handoff_intents_by_member(
                    member_ref("member-handoff-6"),
                    IdentityRepositoryPage::new(None, 32),
                )
                .expect("list intents")
                .items
                .is_empty()
        );
        assert_eq!(
            runtime
                .list_trace_records_by_member(
                    member_ref("member-handoff-6"),
                    IdentityRepositoryPage::new(None, 32),
                )
                .expect("list traces")
                .items
                .len(),
            1
        );
        assert!(
            runtime
                .find_audit_trail_by_subject(IdentityAuditSubjectRef::new(
                    "trace-handoff-intent:handoff-1",
                ))
                .expect("find audit trail")
                .is_none()
        );
    }

    #[test]
    fn prepare_trace_handoff_conflicts_on_reused_requested_intent_ref() {
        let member = GlobalMember::establish(
            member_ref("member-handoff-4"),
            identity_source_ref(IdentitySourceOwner::Identity, "member-source-handoff-4"),
            ActorRef::new("actor-1", ActorKind::Human),
            timestamp(1),
        )
        .expect("member");
        let trace = trace_record("handoff-4", member_ref("member-handoff-4"));
        let existing_intent = TraceHandoffIntent {
            handoff_intent_ref: TraceHandoffIntentRef::new("handoff-1"),
            member_ref: member_ref("member-handoff-4"),
            trace_record_refs: vec![IdentityTraceRecordRef::new("trace-handoff-4")],
            audit_trail_ref: None,
            handoff_target_ref: HandoffTargetRef::new("target-1"),
            handoff_scope_ref: HandoffScopeRef::new("scope-1"),
            safe_material_ref: TraceHandoffSafeMaterialRef::new("material-1"),
            handoff_state: HandoffState::pending(timestamp(1)),
            created_at: timestamp(1),
            updated_at: timestamp(1),
        };
        let runtime = IdentityInMemoryRuntime::builder()
            .seed_member(member, IdentityVersion::new(1))
            .seed_trace_record(trace, IdentityVersion::new(1))
            .seed_handoff_intent(existing_intent, IdentityVersion::new(1))
            .seed_adapter_availability(adapter_availability())
            .build();
        let service = command_service(&runtime);

        let outcome = service
            .prepare_trace_handoff(
                prepare_handoff_request(
                    "conflict-1",
                    member_ref("member-handoff-4"),
                    vec![IdentityTraceRecordRef::new("trace-handoff-4")],
                    None,
                    Some(TraceHandoffIntentRef::new("handoff-1")),
                ),
                command_context(
                    "PrepareTraceHandoff",
                    "idem-handoff-conflict-1",
                    "handoff-conflict-1",
                ),
            )
            .expect("conflict outcome");

        match outcome {
            IdentityCommandOutcome::Rejected(rejection) => {
                assert_eq!(
                    rejection.rejection_kind,
                    identity_contracts::metadata::IdentityProtocolRejectionKind::Conflict
                );
            }
            other => panic!("unexpected outcome: {other:?}"),
        }
    }
}
