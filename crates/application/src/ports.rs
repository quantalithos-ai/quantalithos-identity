//! Application-local port traits for shared helpers and runtime foundations.

use core_contracts::actor::ActorRef;
use identity_contracts::protocol::IdentityJobName;
use identity_contracts::receipts::MaintenanceIssueRef;
use identity_contracts::receipts::TraceHandoffIntentRef;
use identity_contracts::refs::{
    ExternalReferenceRef, GlobalMemberId, GlobalMemberRef, HandoffIssueRef, HandoffReceiptRef,
    IdentityConsumerBindingRef, IdentityJobRunRef, IdentityOutboxPayloadMarkerRef,
    IdentityOutboxRecordRef, IdentityProjectionRef, IdentitySourceEventRef,
    IdentityStoredResultRef, IdentityTraceSubjectRef, IdentityTruthCursor,
    RoleCapabilitySourceSnapshotId, RoleCapabilitySummaryId,
};

use crate::errors::ApplicationError;
use crate::support::{
    AuditTrailId, IdentityAcceptedSubjectRefs, IdentityApiEntryRef, IdentityApiRouteRef,
    IdentityCommandEffectSummaryRef, IdentityDispatchTargetRef, IdentityEntryDispatchRef,
    IdentityEntrySurfaceKind, IdentityIdempotencyKey, IdentityIdempotencyRecordRef,
    IdentityJobDispatchRef, IdentityJobEntryRef, IdentityOperationContext,
    IdentityOperationContextRef, IdentityOperationName, IdentityRequestDigest,
    IdentityRequestMetadataRef, IdentityRuntimeAssemblyRef, IdentityStoredSurfaceMarkerRef,
    IdentityTraceRecordId, IdentityTransactionRef, IdentityWorkerDispatchRef,
    IdentityWorkerEntryRef, MemberSummaryViewId, ReconciliationReportId,
};

/// Shared unit-of-work handle used by all write-side application flows.
pub trait IdentityUnitOfWork {
    /// Returns a stable transaction reference for logs and fake runtime assertions.
    fn transaction_ref(&self) -> IdentityTransactionRef;

    /// Assigns the accepted truth cursor after write-side truth changes are staged.
    fn assign_truth_change_cursor(&self) -> Result<IdentityTruthCursor, ApplicationError>;

    /// Assigns the reference marker cursor after marker-only writes are staged.
    fn assign_reference_marker_cursor(&self) -> Result<IdentityTruthCursor, ApplicationError>;
}

/// Begins and closes application write transactions.
pub trait IdentityUnitOfWorkManagerPort {
    /// Begins a new application write transaction.
    fn begin(&self) -> Result<Box<dyn IdentityUnitOfWork>, ApplicationError>;

    /// Commits a previously opened transaction.
    fn commit(&self, uow: Box<dyn IdentityUnitOfWork>) -> Result<(), ApplicationError>;

    /// Rolls a previously opened transaction back.
    fn rollback(&self, uow: Box<dyn IdentityUnitOfWork>) -> Result<(), ApplicationError>;
}

/// Provides trusted application timestamps.
pub trait IdentityClockPort {
    /// Returns the current trusted identity timestamp.
    fn now(&self) -> Result<identity_contracts::refs::IdentityTimestamp, ApplicationError>;
}

/// Generates stable identity-owned ids and refs required by shared application helpers.
pub trait IdentityIdGeneratorPort {
    /// Generates a new global member id.
    fn new_global_member_id(&self) -> Result<GlobalMemberId, ApplicationError>;

    /// Generates a new role capability summary id.
    fn new_role_capability_summary_id(&self) -> Result<RoleCapabilitySummaryId, ApplicationError>;

    /// Generates a new role capability source snapshot id.
    fn new_role_capability_source_snapshot_id(
        &self,
    ) -> Result<RoleCapabilitySourceSnapshotId, ApplicationError>;

    /// Generates a new member summary view id.
    fn new_member_summary_view_id(&self) -> Result<MemberSummaryViewId, ApplicationError>;

    /// Generates a new trace record id.
    fn new_identity_trace_record_id(&self) -> Result<IdentityTraceRecordId, ApplicationError>;

    /// Generates a new audit trail id.
    fn new_audit_trail_id(&self) -> Result<AuditTrailId, ApplicationError>;

    /// Generates a new reconciliation report id.
    fn new_reconciliation_report_id(&self) -> Result<ReconciliationReportId, ApplicationError>;

    /// Generates a new outbox record ref.
    fn new_identity_outbox_record_ref(&self) -> Result<IdentityOutboxRecordRef, ApplicationError>;

    /// Generates a new outbox payload marker ref.
    fn new_identity_outbox_payload_marker_ref(
        &self,
    ) -> Result<IdentityOutboxPayloadMarkerRef, ApplicationError>;

    /// Generates a new trace handoff intent ref.
    fn new_trace_handoff_intent_ref(&self) -> Result<TraceHandoffIntentRef, ApplicationError>;

    /// Generates a new handoff receipt ref.
    fn new_handoff_receipt_ref(&self) -> Result<HandoffReceiptRef, ApplicationError>;

    /// Generates a new handoff issue ref.
    fn new_handoff_issue_ref(&self) -> Result<HandoffIssueRef, ApplicationError>;

    /// Generates a new operation context ref.
    fn new_identity_operation_context_ref(
        &self,
    ) -> Result<IdentityOperationContextRef, ApplicationError>;

    /// Generates a new idempotency record ref.
    fn new_identity_idempotency_record_ref(
        &self,
    ) -> Result<IdentityIdempotencyRecordRef, ApplicationError>;

    /// Generates a new stored result ref.
    fn new_identity_stored_result_ref(&self) -> Result<IdentityStoredResultRef, ApplicationError>;

    /// Generates a new stored surface marker ref.
    fn new_identity_stored_surface_marker_ref(
        &self,
    ) -> Result<IdentityStoredSurfaceMarkerRef, ApplicationError>;

    /// Generates a new command effect summary ref.
    fn new_identity_command_effect_summary_ref(
        &self,
    ) -> Result<IdentityCommandEffectSummaryRef, ApplicationError>;

    /// Generates a new public job run ref.
    fn new_identity_job_run_ref(&self) -> Result<IdentityJobRunRef, ApplicationError>;

    /// Generates a new public job report ref.
    fn new_identity_job_report_ref(
        &self,
    ) -> Result<identity_contracts::refs::IdentityJobReportRef, ApplicationError>;

    /// Generates a new runtime assembly ref.
    fn new_identity_runtime_assembly_ref(
        &self,
    ) -> Result<IdentityRuntimeAssemblyRef, ApplicationError>;

    /// Generates a new API entry ref.
    fn new_identity_api_entry_ref(&self) -> Result<IdentityApiEntryRef, ApplicationError>;

    /// Generates a new entry dispatch ref.
    fn new_identity_entry_dispatch_ref(&self)
    -> Result<IdentityEntryDispatchRef, ApplicationError>;

    /// Generates a new worker entry ref.
    fn new_identity_worker_entry_ref(&self) -> Result<IdentityWorkerEntryRef, ApplicationError>;

    /// Generates a new worker dispatch ref.
    fn new_identity_worker_dispatch_ref(
        &self,
    ) -> Result<IdentityWorkerDispatchRef, ApplicationError>;

    /// Generates a new job entry ref.
    fn new_identity_job_entry_ref(&self) -> Result<IdentityJobEntryRef, ApplicationError>;

    /// Generates a new job dispatch ref.
    fn new_identity_job_dispatch_ref(&self) -> Result<IdentityJobDispatchRef, ApplicationError>;
}

/// Explicit facade for accepted truth cursor and reference marker cursor assignment.
pub trait IdentityCursorAssignerPort {
    /// Assigns the accepted truth cursor from the active unit of work.
    fn assign_truth_change_cursor(
        &self,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityTruthCursor, ApplicationError>;

    /// Assigns the reference marker cursor from the active unit of work.
    fn assign_reference_marker_cursor(
        &self,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityTruthCursor, ApplicationError>;
}

/// Builds operation contexts from body-free entry metadata.
pub trait IdentityOperationContextFactoryPort {
    /// Builds a command operation context.
    #[allow(clippy::too_many_arguments)]
    fn from_command(
        &self,
        operation_name: IdentityOperationName,
        actor_ref: ActorRef,
        request_metadata_ref: IdentityRequestMetadataRef,
        idempotency_key: Option<IdentityIdempotencyKey>,
        request_digest: IdentityRequestDigest,
        trace_context_ref: Option<identity_contracts::refs::IdentityTraceContextRef>,
        context_ref: IdentityOperationContextRef,
        started_at: identity_contracts::refs::IdentityTimestamp,
    ) -> Result<IdentityOperationContext, ApplicationError>;

    /// Builds a query operation context.
    #[allow(clippy::too_many_arguments)]
    fn from_query(
        &self,
        operation_name: IdentityOperationName,
        actor_ref: ActorRef,
        request_metadata_ref: IdentityRequestMetadataRef,
        request_digest: IdentityRequestDigest,
        trace_context_ref: Option<identity_contracts::refs::IdentityTraceContextRef>,
        context_ref: IdentityOperationContextRef,
        started_at: identity_contracts::refs::IdentityTimestamp,
    ) -> Result<IdentityOperationContext, ApplicationError>;

    /// Builds an inbound-event operation context.
    #[allow(clippy::too_many_arguments)]
    fn from_inbound_event(
        &self,
        operation_name: IdentityOperationName,
        actor_ref: ActorRef,
        request_metadata_ref: IdentityRequestMetadataRef,
        idempotency_key: IdentityIdempotencyKey,
        request_digest: IdentityRequestDigest,
        trace_context_ref: Option<identity_contracts::refs::IdentityTraceContextRef>,
        source_event_ref: IdentitySourceEventRef,
        context_ref: IdentityOperationContextRef,
        started_at: identity_contracts::refs::IdentityTimestamp,
    ) -> Result<IdentityOperationContext, ApplicationError>;

    /// Builds a job operation context.
    #[allow(clippy::too_many_arguments)]
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
        started_at: identity_contracts::refs::IdentityTimestamp,
    ) -> Result<IdentityOperationContext, ApplicationError>;

    /// Builds a handoff-callback operation context.
    #[allow(clippy::too_many_arguments)]
    fn from_handoff_callback(
        &self,
        operation_name: IdentityOperationName,
        actor_ref: ActorRef,
        request_metadata_ref: IdentityRequestMetadataRef,
        idempotency_key: IdentityIdempotencyKey,
        request_digest: IdentityRequestDigest,
        trace_context_ref: Option<identity_contracts::refs::IdentityTraceContextRef>,
        source_event_ref: IdentitySourceEventRef,
        context_ref: IdentityOperationContextRef,
        started_at: identity_contracts::refs::IdentityTimestamp,
    ) -> Result<IdentityOperationContext, ApplicationError>;
}

/// Maps accepted truth refs to canonical trace, audit, and outbox subjects.
pub trait IdentityTruthChangeSubjectMapper {
    /// Returns accepted subject refs for a member truth.
    fn member_subjects(&self, member_ref: GlobalMemberRef) -> IdentityAcceptedSubjectRefs;

    /// Returns accepted subject refs for a role capability summary truth.
    fn role_capability_subjects(
        &self,
        summary_ref: identity_contracts::refs::RoleCapabilitySummaryRef,
    ) -> IdentityAcceptedSubjectRefs;

    /// Returns accepted subject refs for a role capability source snapshot truth.
    fn role_capability_source_snapshot_subjects(
        &self,
        snapshot_ref: identity_contracts::refs::RoleCapabilitySourceSnapshotRef,
    ) -> IdentityAcceptedSubjectRefs;

    /// Returns accepted subject refs for a career record truth.
    fn career_record_subjects(
        &self,
        record_ref: identity_contracts::refs::CareerRecordRef,
    ) -> IdentityAcceptedSubjectRefs;

    /// Returns accepted subject refs for a memory reference truth.
    fn memory_reference_subjects(
        &self,
        reference_ref: identity_contracts::refs::MemoryReferenceRef,
    ) -> IdentityAcceptedSubjectRefs;

    /// Returns accepted subject refs for an outbox record.
    fn outbox_record_subjects(
        &self,
        outbox_ref: IdentityOutboxRecordRef,
    ) -> IdentityAcceptedSubjectRefs;

    /// Returns accepted subject refs for a handoff intent.
    fn handoff_intent_subjects(
        &self,
        intent_ref: TraceHandoffIntentRef,
    ) -> IdentityAcceptedSubjectRefs;
}

/// Maps body-free identity marker refs into trace subject refs.
pub trait IdentityMarkerSubjectMapper {
    /// Returns the trace subject for a source marker.
    fn source_marker_subject(
        &self,
        source_ref: identity_contracts::refs::IdentitySourceRef,
    ) -> IdentityTraceSubjectRef;

    /// Returns the trace subject for an external reference marker.
    fn external_reference_marker_subject(
        &self,
        reference_ref: ExternalReferenceRef,
    ) -> IdentityTraceSubjectRef;

    /// Returns the trace subject for a projection marker.
    fn projection_marker_subject(
        &self,
        projection_ref: IdentityProjectionRef,
    ) -> IdentityTraceSubjectRef;

    /// Returns the trace subject for a job run marker.
    fn job_marker_subject(&self, job_run_ref: IdentityJobRunRef) -> IdentityTraceSubjectRef;

    /// Returns the trace subject for a handoff receipt marker.
    fn handoff_receipt_marker_subject(
        &self,
        receipt_ref: HandoffReceiptRef,
    ) -> IdentityTraceSubjectRef;
}

/// Maps maintenance and propagation issues into safe maintenance issue refs.
pub trait IdentityMaintenanceIssueMapper {
    /// Converts a missing projection state into a safe maintenance issue ref.
    fn projection_missing_state_issue(
        &self,
        projection_ref: IdentityProjectionRef,
    ) -> MaintenanceIssueRef;

    /// Converts a missing projection cursor into a safe maintenance issue ref.
    fn projection_missing_cursor_issue(
        &self,
        projection_ref: IdentityProjectionRef,
    ) -> MaintenanceIssueRef;

    /// Converts an unsupported projection writer into a safe maintenance issue ref.
    fn projection_unsupported_writer_issue(
        &self,
        projection_ref: IdentityProjectionRef,
    ) -> MaintenanceIssueRef;

    /// Converts a missing external reference state into a safe maintenance issue ref.
    fn reference_missing_state_issue(
        &self,
        reference_ref: ExternalReferenceRef,
    ) -> MaintenanceIssueRef;

    /// Converts a failed external reference refresh into a safe maintenance issue ref.
    fn reference_refresh_failed_issue(
        &self,
        reference_ref: ExternalReferenceRef,
    ) -> MaintenanceIssueRef;

    /// Converts an outbox retryable issue into a safe maintenance issue ref.
    fn outbox_retryable_issue(
        &self,
        issue_ref: identity_contracts::refs::OutboxDeliveryIssueRef,
    ) -> MaintenanceIssueRef;

    /// Converts an outbox permanent issue into a safe maintenance issue ref.
    fn outbox_permanent_issue(
        &self,
        issue_ref: identity_contracts::refs::OutboxDeliveryIssueRef,
    ) -> MaintenanceIssueRef;

    /// Converts an outbox skipped issue into a safe maintenance issue ref.
    fn outbox_skipped_issue(
        &self,
        issue_ref: identity_contracts::refs::OutboxDeliveryIssueRef,
    ) -> MaintenanceIssueRef;

    /// Converts an outbox unsupported-topic issue into a safe maintenance issue ref.
    fn outbox_unsupported_topic_issue(
        &self,
        issue_ref: identity_contracts::refs::OutboxDeliveryIssueRef,
    ) -> MaintenanceIssueRef;

    /// Converts a retryable handoff issue into a safe maintenance issue ref.
    fn handoff_retryable_issue(&self, issue_ref: HandoffIssueRef) -> MaintenanceIssueRef;

    /// Converts a permanent handoff issue into a safe maintenance issue ref.
    fn handoff_permanent_issue(&self, issue_ref: HandoffIssueRef) -> MaintenanceIssueRef;

    /// Converts a cancelled handoff issue into a safe maintenance issue ref.
    fn handoff_cancelled_issue(&self, issue_ref: HandoffIssueRef) -> MaintenanceIssueRef;

    /// Converts an unsupported-target handoff issue into a safe maintenance issue ref.
    fn handoff_unsupported_target_issue(&self, issue_ref: HandoffIssueRef) -> MaintenanceIssueRef;
}

/// Returns application service targets for API, worker, and job entry guards.
pub trait IdentityDispatchTargetCatalogPort {
    /// Returns the application command target for the given API route.
    fn api_command_target(
        &self,
        route_ref: IdentityApiRouteRef,
    ) -> Result<IdentityDispatchTargetRef, ApplicationError>;

    /// Returns the application query target for the given API route.
    fn api_query_target(
        &self,
        route_ref: IdentityApiRouteRef,
    ) -> Result<IdentityDispatchTargetRef, ApplicationError>;

    /// Returns the worker consumer target for the given consumer binding.
    fn worker_consumer_target(
        &self,
        binding_ref: IdentityConsumerBindingRef,
    ) -> Result<IdentityDispatchTargetRef, ApplicationError>;

    /// Returns the worker callback target for the given consumer binding.
    fn worker_callback_target(
        &self,
        binding_ref: IdentityConsumerBindingRef,
    ) -> Result<IdentityDispatchTargetRef, ApplicationError>;

    /// Returns the job target for the given public job name.
    fn job_target(
        &self,
        job_name: IdentityJobName,
    ) -> Result<IdentityDispatchTargetRef, ApplicationError>;

    /// Asserts that the provided target is a valid application service target for the surface.
    fn assert_application_target(
        &self,
        surface_kind: IdentityEntrySurfaceKind,
        target_ref: IdentityDispatchTargetRef,
    ) -> Result<(), ApplicationError>;
}
