//! Application-local port traits for shared helpers and runtime foundations.

use core_contracts::actor::ActorRef;
use identity_contracts::protocol::IdentityJobName;
use identity_contracts::receipts::MaintenanceIssueRef;
use identity_contracts::receipts::TraceHandoffIntentRef;
use identity_contracts::refs::{
    ArchiveHandoffRef, ArchiveRef, AuditCursorRef, AuditScopeRef, AuditTrailRef,
    CapabilityEvidenceRef, CareerRecordRef, CareerSourceMarkerRef, ConsumerRef,
    ExternalReferenceRef, ExternalReferenceSafeSummaryRef, ExternalSourceVersionRef,
    GlobalMemberId, GlobalMemberRef, GovernanceBasisRef, GovernanceBasisSummary, HandoffAttemptRef,
    HandoffIssueRef, HandoffReceiptRef, HandoffScopeRef, HandoffTargetRef, IdentityAuditSubjectRef,
    IdentityChangeKindRef, IdentityConsumerBindingRef, IdentityJobRunRef,
    IdentityMaintenanceTargetRef, IdentityOutboxPayloadMarkerRef, IdentityOutboxRecordRef,
    IdentityOutboxSubjectRef, IdentityProjectionCursorRef, IdentityProjectionRef,
    IdentityReferenceOwnerRef, IdentitySourceEventRef, IdentitySourceRef, IdentityStoredResultRef,
    IdentityTraceRecordRef, IdentityTraceSubjectRef, IdentityTruthCursor,
    IdentityVisibilityDecisionRef, LifecycleRiskRef, MaintenanceScopeRef, MemberSummaryViewRef,
    MemoryRef, MemoryReferenceRef, MemoryReferenceSourceRef, OutboxDeliveryAttemptRef,
    OutboxDeliveryIssueRef, ProjectionStateRef, ReconciliationReportRef,
    ReferenceResolutionStateRef, RoleCapabilitySafeSummaryRef, RoleCapabilitySourceRef,
    RoleCapabilitySourceSnapshotId, RoleCapabilitySourceSnapshotRef,
    RoleCapabilitySourceVersionRef, RoleCapabilitySummaryId, RoleCapabilitySummaryRef, TopicKeyRef,
    TraceHandoffSafeMaterialRef, VisibilityContextRef, VisibilityResultRef, VisibilityScopeRef,
    WorkParticipationSourceSummary, WorkSourceRef,
};
use identity_contracts::views::{IdentityVisibilityAccessSummary, MemberSummaryView};
use identity_domain::audit::{AuditTrail, AuditTrailEntry};
use identity_domain::career::CareerRecord;
use identity_domain::handoff::TraceHandoffIntent;
use identity_domain::lifecycle::GlobalLifecycleState;
use identity_domain::member_identity::{GlobalMember, IdentityAnchorState};
use identity_domain::memory_reference::MemoryReference;
use identity_domain::outbox::IdentityOutboxRecord;
use identity_domain::projection_state::ProjectionState;
use identity_domain::reconciliation::{ReconciliationReport, ReconciliationReportStateKind};
use identity_domain::reference_state::ReferenceResolutionState;
use identity_domain::role_capability::{
    RoleCapabilitySourceSnapshot, RoleCapabilitySourceStateKind, RoleCapabilitySummary,
};
use identity_domain::trace::IdentityTraceRecord;

use crate::errors::ApplicationError;
use crate::support::{
    AuditTrailId, IdempotencyReserveOutcome, IdentityAcceptedSubjectRefs,
    IdentityAdapterAvailability, IdentityAdapterModeRef, IdentityAdapterRef, IdentityApiEntryRef,
    IdentityApiRouteRef, IdentityCommandEffectSummaryRef, IdentityConsumerReceiptEnvelope,
    IdentityDispatchTargetRef, IdentityEntryDispatchRef, IdentityEntrySurfaceKind,
    IdentityIdempotencyKey, IdentityIdempotencyRecordRef, IdentityJobDispatchRef,
    IdentityJobEntryRef, IdentityOperationContext, IdentityOperationContextRef,
    IdentityOperationName, IdentityProjectionRefSet, IdentityRepositoryPage, IdentityRequestDigest,
    IdentityRequestMetadataRef, IdentityRuntimeAssemblyRef, IdentityStoredSurfaceMarkerRef,
    IdentityTraceRecordId, IdentityTransactionRef, IdentityVersion, IdentityVersionedRef,
    IdentityWorkerDispatchRef, IdentityWorkerEntryRef, MemberSummaryViewId, Page,
    ReconciliationReportId, StoredIdentityOperationResult, Versioned,
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

/// Repository for identity global member anchor truth.
pub trait GlobalMemberRepository {
    /// Loads a member truth and its optimistic version.
    fn get_member_with_version(
        &self,
        member_ref: GlobalMemberRef,
    ) -> Result<Option<Versioned<GlobalMember>>, ApplicationError>;

    /// Loads the anchor state without implicitly creating the member.
    fn get_anchor_state(
        &self,
        member_ref: GlobalMemberRef,
    ) -> Result<Option<IdentityAnchorState>, ApplicationError>;

    /// Lists member refs in deterministic repository order.
    fn list_members(
        &self,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<GlobalMemberRef>>, ApplicationError>;

    /// Saves a member truth using an optional expected version.
    fn save_member(
        &self,
        member: GlobalMember,
        expected_version: Option<IdentityVersion>,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<GlobalMemberRef>, ApplicationError>;
}

/// Repository for identity-owned global lifecycle state.
pub trait GlobalLifecycleRepository {
    /// Loads lifecycle state and its optimistic version.
    fn get_lifecycle_with_version(
        &self,
        member_ref: GlobalMemberRef,
    ) -> Result<Option<Versioned<GlobalLifecycleState>>, ApplicationError>;

    /// Lists lifecycle refs in deterministic repository order.
    fn list_lifecycles(
        &self,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<GlobalMemberRef>>, ApplicationError>;

    /// Saves lifecycle state using an optional expected version.
    fn save_lifecycle(
        &self,
        lifecycle_state: GlobalLifecycleState,
        expected_version: Option<IdentityVersion>,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<GlobalMemberRef>, ApplicationError>;
}

/// Repository for role and capability summaries and source snapshots.
pub trait RoleCapabilityRepository {
    /// Loads a summary truth and version.
    fn get_summary_with_version(
        &self,
        summary_ref: RoleCapabilitySummaryRef,
    ) -> Result<Option<Versioned<RoleCapabilitySummary>>, ApplicationError>;

    /// Finds the current summary for a member.
    fn find_current_summary_by_member(
        &self,
        member_ref: GlobalMemberRef,
    ) -> Result<Option<Versioned<RoleCapabilitySummary>>, ApplicationError>;

    /// Lists summaries by member.
    fn list_summaries_by_member(
        &self,
        member_ref: GlobalMemberRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<RoleCapabilitySummaryRef>>, ApplicationError>;

    /// Loads a source snapshot and version.
    fn get_source_snapshot_with_version(
        &self,
        snapshot_ref: RoleCapabilitySourceSnapshotRef,
    ) -> Result<Option<Versioned<RoleCapabilitySourceSnapshot>>, ApplicationError>;

    /// Finds a source snapshot by typed source ref.
    fn find_source_snapshot_by_source(
        &self,
        source_ref: RoleCapabilitySourceRef,
    ) -> Result<Option<Versioned<RoleCapabilitySourceSnapshot>>, ApplicationError>;

    /// Saves a source snapshot using an optional expected version.
    fn save_source_snapshot(
        &self,
        snapshot: RoleCapabilitySourceSnapshot,
        expected_version: Option<IdentityVersion>,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<RoleCapabilitySourceSnapshotRef>, ApplicationError>;

    /// Saves a role summary using an optional expected version.
    fn save_summary(
        &self,
        summary: RoleCapabilitySummary,
        expected_version: Option<IdentityVersion>,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<RoleCapabilitySummaryRef>, ApplicationError>;
}

/// Repository for append-only career records.
pub trait CareerRecordRepository {
    /// Loads a career record and version.
    fn get_career_record(
        &self,
        record_ref: CareerRecordRef,
    ) -> Result<Option<Versioned<CareerRecord>>, ApplicationError>;

    /// Lists records by member.
    fn list_records_by_member(
        &self,
        member_ref: GlobalMemberRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<CareerRecordRef>>, ApplicationError>;

    /// Lists records by formal source marker.
    fn find_records_by_source_marker(
        &self,
        source_marker_ref: CareerSourceMarkerRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<CareerRecordRef>>, ApplicationError>;

    /// Finds the current duplicate record for a source marker.
    fn find_duplicate_source_record(
        &self,
        source_marker_ref: CareerSourceMarkerRef,
    ) -> Result<Option<CareerRecordRef>, ApplicationError>;

    /// Lists correction records for an original record.
    fn list_corrections_for_record(
        &self,
        original_record_ref: CareerRecordRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<CareerRecordRef>>, ApplicationError>;

    /// Appends a new career record.
    fn append_career_record(
        &self,
        record: CareerRecord,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<CareerRecordRef>, ApplicationError>;

    /// Saves an explanatory career record state update.
    fn save_career_record_state(
        &self,
        record: CareerRecord,
        expected_version: IdentityVersion,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<CareerRecordRef>, ApplicationError>;
}

/// Repository for memory and archive reference relations.
pub trait MemoryReferenceRepository {
    /// Loads a memory relation and version.
    fn get_memory_reference_with_version(
        &self,
        reference_ref: MemoryReferenceRef,
    ) -> Result<Option<Versioned<MemoryReference>>, ApplicationError>;

    /// Lists references by member.
    fn list_references_by_member(
        &self,
        member_ref: GlobalMemberRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<MemoryReferenceRef>>, ApplicationError>;

    /// Finds a relation by member and memory ref.
    fn find_reference_by_memory(
        &self,
        member_ref: GlobalMemberRef,
        memory_ref: MemoryRef,
    ) -> Result<Option<Versioned<MemoryReference>>, ApplicationError>;

    /// Finds a relation by member and archive ref.
    fn find_reference_by_archive(
        &self,
        member_ref: GlobalMemberRef,
        archive_ref: ArchiveRef,
    ) -> Result<Option<Versioned<MemoryReference>>, ApplicationError>;

    /// Finds a relation by archive handoff ref.
    fn find_reference_by_handoff(
        &self,
        handoff_ref: ArchiveHandoffRef,
    ) -> Result<Option<Versioned<MemoryReference>>, ApplicationError>;

    /// Finds the callback target relation by archive handoff ref.
    fn find_callback_target_by_handoff(
        &self,
        handoff_ref: ArchiveHandoffRef,
    ) -> Result<Option<MemoryReferenceRef>, ApplicationError>;

    /// Saves a memory relation using an optional expected version.
    fn save_memory_reference(
        &self,
        reference: MemoryReference,
        expected_version: Option<IdentityVersion>,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<MemoryReferenceRef>, ApplicationError>;
}

/// Append-only repository for accepted trace records.
pub trait IdentityTraceRecordRepository {
    /// Loads a trace record and version.
    fn get_trace_record(
        &self,
        trace_record_ref: IdentityTraceRecordRef,
    ) -> Result<Option<Versioned<IdentityTraceRecord>>, ApplicationError>;

    /// Lists trace records by member.
    fn list_trace_records_by_member(
        &self,
        member_ref: GlobalMemberRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<IdentityTraceRecordRef>>, ApplicationError>;

    /// Lists trace records by formal subject.
    fn list_trace_records_by_subject(
        &self,
        subject_ref: IdentityTraceSubjectRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<IdentityTraceRecordRef>>, ApplicationError>;

    /// Lists trace records after a formal truth cursor.
    fn list_trace_records_after_cursor(
        &self,
        subject_ref: IdentityTraceSubjectRef,
        after_cursor: Option<IdentityTruthCursor>,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<IdentityTraceRecordRef>>, ApplicationError>;

    /// Lists trace records by member and change kind.
    fn list_trace_records_by_change_kind(
        &self,
        member_ref: GlobalMemberRef,
        change_kind_ref: IdentityChangeKindRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<IdentityTraceRecordRef>>, ApplicationError>;

    /// Appends a new trace record.
    fn append_trace_record(
        &self,
        trace_record: IdentityTraceRecord,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<IdentityTraceRecordRef>, ApplicationError>;

    /// Marks an older trace superseded by a formal correction trace.
    fn mark_trace_superseded_by_correction(
        &self,
        trace_record: IdentityTraceRecord,
        expected_version: IdentityVersion,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<IdentityTraceRecordRef>, ApplicationError>;
}

/// Repository for audit timeline aggregates and body-free audit entries.
pub trait IdentityAuditTrailRepository {
    /// Loads an audit trail and version.
    fn get_audit_trail_with_version(
        &self,
        audit_trail_ref: AuditTrailRef,
    ) -> Result<Option<Versioned<AuditTrail>>, ApplicationError>;

    /// Finds an audit trail by canonical audit subject.
    fn find_audit_trail_by_subject(
        &self,
        audit_subject_ref: IdentityAuditSubjectRef,
    ) -> Result<Option<Versioned<AuditTrail>>, ApplicationError>;

    /// Lists audit entries by scope and cursor.
    fn list_audit_entries(
        &self,
        audit_trail_ref: AuditTrailRef,
        audit_scope_ref: AuditScopeRef,
        cursor_ref: Option<AuditCursorRef>,
        page: IdentityRepositoryPage,
    ) -> Result<Page<AuditTrailEntry>, ApplicationError>;

    /// Saves an audit trail using an optional expected version.
    fn save_audit_trail(
        &self,
        audit_trail: AuditTrail,
        expected_version: Option<IdentityVersion>,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<AuditTrailRef>, ApplicationError>;

    /// Appends an audit entry to an existing trail.
    fn append_audit_entry(
        &self,
        audit_trail_ref: AuditTrailRef,
        entry: AuditTrailEntry,
        expected_version: IdentityVersion,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<AuditTrailRef>, ApplicationError>;
}

/// Read-only history facade over formal trace and audit material.
pub trait IdentityTraceHistoryRepository {
    /// Lists history by member.
    fn list_history_by_member(
        &self,
        member_ref: GlobalMemberRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<IdentityTraceRecordRef>>, ApplicationError>;

    /// Lists history by subject.
    fn list_history_by_subject(
        &self,
        subject_ref: IdentityTraceSubjectRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<IdentityTraceRecordRef>>, ApplicationError>;

    /// Lists history between two formal truth cursors.
    fn list_history_between_cursors(
        &self,
        subject_ref: IdentityTraceSubjectRef,
        after_cursor: Option<IdentityTruthCursor>,
        before_cursor: Option<IdentityTruthCursor>,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<IdentityTraceRecordRef>>, ApplicationError>;

    /// Lists a supersession chain for a trace record.
    fn list_supersession_chain(
        &self,
        trace_record_ref: IdentityTraceRecordRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<IdentityTraceRecordRef>>, ApplicationError>;
}

/// Repository for trace and audit handoff intents.
pub trait TraceHandoffIntentRepository {
    /// Loads a handoff intent and version.
    fn get_handoff_intent_with_version(
        &self,
        intent_ref: TraceHandoffIntentRef,
    ) -> Result<Option<Versioned<TraceHandoffIntent>>, ApplicationError>;

    /// Lists handoff intents by member.
    fn list_handoff_intents_by_member(
        &self,
        member_ref: GlobalMemberRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<TraceHandoffIntentRef>>, ApplicationError>;

    /// Lists handoff intents by trace ref.
    fn list_handoff_intents_by_trace(
        &self,
        trace_record_ref: IdentityTraceRecordRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<TraceHandoffIntentRef>>, ApplicationError>;

    /// Lists handoff intents by audit trail ref.
    fn list_handoff_intents_by_audit_trail(
        &self,
        audit_trail_ref: AuditTrailRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<TraceHandoffIntentRef>>, ApplicationError>;

    /// Lists handoff intents by target.
    fn list_handoff_intents_by_target(
        &self,
        target_ref: HandoffTargetRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<TraceHandoffIntentRef>>, ApplicationError>;

    /// Lists retryable handoff intents.
    fn list_retryable_handoff_intents(
        &self,
        target_ref: Option<HandoffTargetRef>,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<TraceHandoffIntentRef>>, ApplicationError>;

    /// Saves a handoff intent using an optional expected version.
    fn save_handoff_intent(
        &self,
        intent: TraceHandoffIntent,
        expected_version: Option<IdentityVersion>,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<TraceHandoffIntentRef>, ApplicationError>;
}

/// Repository for projections, stable view lookup, and projection freshness state.
pub trait IdentityProjectionRepository {
    /// Finds a stable member summary view ref by member and scope.
    fn find_member_summary_view_ref(
        &self,
        member_ref: GlobalMemberRef,
        visibility_scope_ref: VisibilityScopeRef,
    ) -> Result<Option<MemberSummaryViewRef>, ApplicationError>;

    /// Loads a member summary view by stable ref.
    fn get_member_summary_view(
        &self,
        view_ref: MemberSummaryViewRef,
    ) -> Result<Option<MemberSummaryView>, ApplicationError>;

    /// Loads a projection state and version.
    fn get_projection_state_with_version(
        &self,
        projection_ref: IdentityProjectionRef,
    ) -> Result<Option<Versioned<ProjectionState>>, ApplicationError>;

    /// Finds a lightweight projection state ref.
    fn find_projection_state_ref(
        &self,
        projection_ref: IdentityProjectionRef,
    ) -> Result<Option<ProjectionStateRef>, ApplicationError>;

    /// Lists projection states.
    fn list_projection_states(
        &self,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<ProjectionStateRef>>, ApplicationError>;

    /// Lists stale projection states for a maintenance scope.
    fn list_stale_projection_states(
        &self,
        maintenance_scope_ref: MaintenanceScopeRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<ProjectionStateRef>>, ApplicationError>;

    /// Loads a projection source cursor by projection ref.
    fn get_projection_source_cursor(
        &self,
        projection_ref: IdentityProjectionRef,
    ) -> Result<Option<IdentityProjectionCursorRef>, ApplicationError>;

    /// Expands affected projection refs from formal accepted subject refs.
    fn expand_affected_projection_refs(
        &self,
        subject_refs: IdentityAcceptedSubjectRefs,
    ) -> Result<IdentityProjectionRefSet, ApplicationError>;

    /// Saves a member summary view using an optional expected version.
    fn save_member_summary_view(
        &self,
        view: MemberSummaryView,
        expected_version: Option<IdentityVersion>,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<MemberSummaryViewRef>, ApplicationError>;

    /// Saves a projection state using an optional expected version.
    fn save_projection_state(
        &self,
        state: ProjectionState,
        expected_version: Option<IdentityVersion>,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<ProjectionStateRef>, ApplicationError>;

    /// Marks a projection stale using a loaded expected version.
    fn mark_projection_stale(
        &self,
        projection_ref: IdentityProjectionRef,
        state: ProjectionState,
        expected_version: IdentityVersion,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<ProjectionStateRef>, ApplicationError>;
}

/// Read-side port for visibility mapping and optional visibility decision material.
pub trait IdentityReadVisibilityRepository {
    /// Resolves a member summary read request into prepared visibility input.
    fn resolve_member_summary_read(
        &self,
        member_ref: GlobalMemberRef,
        view_ref: Option<MemberSummaryViewRef>,
        consumer_ref: ConsumerRef,
        visibility_context_ref: VisibilityContextRef,
    ) -> Result<Option<IdentityVisibilityAccessSummary>, ApplicationError>;

    /// Resolves a trace read request into prepared visibility input.
    fn resolve_trace_read(
        &self,
        subject_ref: IdentityTraceSubjectRef,
        consumer_ref: ConsumerRef,
        visibility_context_ref: VisibilityContextRef,
    ) -> Result<Option<IdentityVisibilityAccessSummary>, ApplicationError>;

    /// Resolves an audit trail read request into prepared visibility input.
    fn resolve_audit_read(
        &self,
        audit_subject_ref: IdentityAuditSubjectRef,
        audit_scope_ref: AuditScopeRef,
        consumer_ref: ConsumerRef,
        visibility_context_ref: VisibilityContextRef,
    ) -> Result<Option<IdentityVisibilityAccessSummary>, ApplicationError>;

    /// Resolves a reconciliation report read request into prepared visibility input.
    fn resolve_report_read(
        &self,
        report_ref: ReconciliationReportRef,
        consumer_ref: ConsumerRef,
        visibility_context_ref: VisibilityContextRef,
    ) -> Result<Option<IdentityVisibilityAccessSummary>, ApplicationError>;

    /// Resolves a maintenance scope report read request into prepared visibility input.
    fn resolve_reconciliation_scope_read(
        &self,
        maintenance_scope_ref: MaintenanceScopeRef,
        consumer_ref: ConsumerRef,
        visibility_context_ref: VisibilityContextRef,
    ) -> Result<Option<IdentityVisibilityAccessSummary>, ApplicationError>;

    /// Resolves a projection state read request into prepared visibility input.
    fn resolve_projection_state_read(
        &self,
        projection_ref: IdentityProjectionRef,
        projection_state_ref: Option<ProjectionStateRef>,
        consumer_ref: ConsumerRef,
        visibility_context_ref: VisibilityContextRef,
    ) -> Result<Option<IdentityVisibilityAccessSummary>, ApplicationError>;

    /// Resolves a reference state read request into prepared visibility input.
    fn resolve_reference_state_read(
        &self,
        external_reference_ref: ExternalReferenceRef,
        owner_ref: Option<IdentityReferenceOwnerRef>,
        consumer_ref: ConsumerRef,
        visibility_context_ref: VisibilityContextRef,
    ) -> Result<Option<IdentityVisibilityAccessSummary>, ApplicationError>;

    /// Resolves an outbox read request into prepared visibility input.
    fn resolve_outbox_record_read(
        &self,
        outbox_ref: Option<IdentityOutboxRecordRef>,
        subject_ref: Option<IdentityOutboxSubjectRef>,
        topic_key_ref: Option<TopicKeyRef>,
        consumer_ref: ConsumerRef,
        visibility_context_ref: VisibilityContextRef,
    ) -> Result<Option<IdentityVisibilityAccessSummary>, ApplicationError>;

    /// Resolves a handoff intent read request into prepared visibility input.
    fn resolve_handoff_intent_read(
        &self,
        intent_ref: TraceHandoffIntentRef,
        consumer_ref: ConsumerRef,
        visibility_context_ref: VisibilityContextRef,
    ) -> Result<Option<IdentityVisibilityAccessSummary>, ApplicationError>;

    /// Loads an optional stored visibility decision.
    fn get_visibility_decision(
        &self,
        visibility_result_ref: VisibilityResultRef,
    ) -> Result<Option<crate::support::IdentityVisibilityDecision>, ApplicationError>;

    /// Saves a visibility decision using an optional expected version.
    fn save_visibility_decision(
        &self,
        decision: crate::support::IdentityVisibilityDecision,
        expected_version: Option<IdentityVersion>,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<IdentityVisibilityDecisionRef>, ApplicationError>;
}

/// Body-free typed sidecar refs attached to a single external reference bundle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalReferenceTypedSidecarRefs {
    /// Optional role capability summary safe summary ref.
    pub role_capability_safe_summary_ref: Option<ExternalReferenceSafeSummaryRef>,
    /// Optional career safe summary ref.
    pub career_safe_summary_ref: Option<ExternalReferenceSafeSummaryRef>,
    /// Optional memory safe summary ref.
    pub memory_safe_summary_ref: Option<ExternalReferenceSafeSummaryRef>,
    /// Optional governance basis safe summary ref.
    pub governance_basis_summary_ref: Option<ExternalReferenceSafeSummaryRef>,
    /// Optional evidence summary ref.
    pub evidence_summary_ref: Option<ExternalReferenceSafeSummaryRef>,
    /// Optional source version ref.
    pub source_version_ref: Option<ExternalSourceVersionRef>,
}

/// Repository for external reference bundles and typed sidecar refs.
pub trait IdentityReferenceStateRepository {
    /// Loads a reference state and version.
    fn get_reference_state_with_version(
        &self,
        reference_ref: ExternalReferenceRef,
    ) -> Result<Option<Versioned<ReferenceResolutionState>>, ApplicationError>;

    /// Finds a lightweight reference state ref.
    fn find_reference_state_ref(
        &self,
        reference_ref: ExternalReferenceRef,
    ) -> Result<Option<ReferenceResolutionStateRef>, ApplicationError>;

    /// Lists reference states by owner.
    fn list_reference_states_by_owner(
        &self,
        owner_ref: IdentityReferenceOwnerRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<ReferenceResolutionStateRef>>, ApplicationError>;

    /// Lists reference states by external reference kind.
    fn list_reference_states_by_kind(
        &self,
        reference_kind: identity_contracts::refs::ExternalReferenceKind,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<ReferenceResolutionStateRef>>, ApplicationError>;

    /// Lists stale reference states for a maintenance scope.
    fn list_stale_reference_states(
        &self,
        maintenance_scope_ref: MaintenanceScopeRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<ReferenceResolutionStateRef>>, ApplicationError>;

    /// Loads typed sidecar refs for a bundle.
    fn get_typed_sidecar_refs(
        &self,
        reference_ref: ExternalReferenceRef,
    ) -> Result<ExternalReferenceTypedSidecarRefs, ApplicationError>;

    /// Saves a reference state using an optional expected version.
    fn save_reference_state(
        &self,
        state: ReferenceResolutionState,
        expected_version: Option<IdentityVersion>,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<ReferenceResolutionStateRef>, ApplicationError>;

    /// Saves typed sidecar refs using the loaded bundle version.
    fn save_typed_sidecar_refs(
        &self,
        reference_ref: ExternalReferenceRef,
        sidecar_refs: ExternalReferenceTypedSidecarRefs,
        expected_version: IdentityVersion,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<ReferenceResolutionStateRef>, ApplicationError>;
}

/// Repository for maintenance scope expansion and target scans.
pub trait IdentityMaintenanceRepository {
    /// Expands maintenance targets for a scope.
    fn expand_maintenance_targets(
        &self,
        maintenance_scope_ref: MaintenanceScopeRef,
    ) -> Result<Page<IdentityMaintenanceTargetRef>, ApplicationError>;

    /// Lists projection rebuild targets for a scope.
    fn list_projection_targets_for_rebuild(
        &self,
        maintenance_scope_ref: MaintenanceScopeRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityProjectionRef>, ApplicationError>;

    /// Lists reference refresh targets for a scope.
    fn list_reference_targets_for_refresh(
        &self,
        maintenance_scope_ref: MaintenanceScopeRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<ExternalReferenceRef>, ApplicationError>;

    /// Lists report targets for a scope.
    fn list_report_targets(
        &self,
        maintenance_scope_ref: MaintenanceScopeRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityMaintenanceTargetRef>, ApplicationError>;
}

/// Repository for report-only reconciliation reports.
pub trait IdentityReconciliationReportRepository {
    /// Loads a report and version.
    fn get_report_with_version(
        &self,
        report_ref: ReconciliationReportRef,
    ) -> Result<Option<Versioned<ReconciliationReport>>, ApplicationError>;

    /// Lists reports by scope.
    fn list_reports_by_scope(
        &self,
        maintenance_scope_ref: MaintenanceScopeRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<ReconciliationReportRef>>, ApplicationError>;

    /// Lists reports by maintenance target.
    fn list_reports_by_target(
        &self,
        target_ref: IdentityMaintenanceTargetRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<ReconciliationReportRef>>, ApplicationError>;

    /// Lists reports by report state.
    fn list_reports_by_state(
        &self,
        state_kind: ReconciliationReportStateKind,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<ReconciliationReportRef>>, ApplicationError>;

    /// Saves a report using an optional expected version.
    fn save_report(
        &self,
        report: ReconciliationReport,
        expected_version: Option<IdentityVersion>,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<ReconciliationReportRef>, ApplicationError>;
}

/// Repository for accepted outbox records and publish state transitions.
pub trait IdentityOutboxRepository {
    /// Loads an outbox record and version.
    fn get_outbox_record_with_version(
        &self,
        outbox_ref: IdentityOutboxRecordRef,
    ) -> Result<Option<Versioned<IdentityOutboxRecord>>, ApplicationError>;

    /// Lists pending outbox records, optionally filtered by topic.
    fn list_pending_outbox_records(
        &self,
        topic_key_ref: Option<TopicKeyRef>,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<IdentityOutboxRecordRef>>, ApplicationError>;

    /// Lists retryable outbox records, optionally filtered by topic.
    fn list_retryable_outbox_records(
        &self,
        topic_key_ref: Option<TopicKeyRef>,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<IdentityOutboxRecordRef>>, ApplicationError>;

    /// Lists outbox records by formal outbox subject.
    fn list_outbox_records_by_subject(
        &self,
        subject_ref: IdentityOutboxSubjectRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<IdentityOutboxRecordRef>>, ApplicationError>;

    /// Lists outbox records related to a trace record.
    fn find_outbox_records_by_trace(
        &self,
        trace_record_ref: IdentityTraceRecordRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityVersionedRef<IdentityOutboxRecordRef>>, ApplicationError>;

    /// Saves an outbox record using an optional expected version.
    fn save_outbox_record(
        &self,
        record: IdentityOutboxRecord,
        expected_version: Option<IdentityVersion>,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<IdentityOutboxRecordRef>, ApplicationError>;

    /// Updates the publish state of a loaded outbox record.
    fn update_outbox_state(
        &self,
        record: IdentityOutboxRecord,
        expected_version: IdentityVersion,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<IdentityOutboxRecordRef>, ApplicationError>;
}

/// Repository for operation idempotency reserve, conflict, completion, and replay lookup.
pub trait IdentityIdempotencyRepository {
    /// Loads an idempotency record by operation namespace and key.
    fn get_by_key(
        &self,
        operation_name: IdentityOperationName,
        channel: identity_contracts::refs::IdentityOperationChannel,
        idempotency_key: IdentityIdempotencyKey,
    ) -> Result<Option<Versioned<crate::support::IdentityIdempotencyRecord>>, ApplicationError>;

    /// Reserves a new idempotency record or returns the existing same-key outcome.
    fn reserve(
        &self,
        context: IdentityOperationContext,
        record_ref: IdentityIdempotencyRecordRef,
        reserved_at: identity_contracts::refs::IdentityTimestamp,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdempotencyReserveOutcome, ApplicationError>;

    /// Completes a loaded record with a replayable accepted stored result.
    fn complete_with_stored_result(
        &self,
        record: crate::support::IdentityIdempotencyRecord,
        stored_result_ref: IdentityStoredResultRef,
        completed_at: identity_contracts::refs::IdentityTimestamp,
        expected_version: IdentityVersion,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<IdentityIdempotencyRecordRef>, ApplicationError>;

    /// Completes a loaded record with a replayable rejected stored result.
    fn complete_rejected_with_stored_result(
        &self,
        record: crate::support::IdentityIdempotencyRecord,
        stored_result_ref: IdentityStoredResultRef,
        completed_at: identity_contracts::refs::IdentityTimestamp,
        expected_version: IdentityVersion,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<IdentityIdempotencyRecordRef>, ApplicationError>;

    /// Marks a loaded record conflicting for same-key different-digest input.
    fn mark_conflict(
        &self,
        record: crate::support::IdentityIdempotencyRecord,
        conflicted_at: identity_contracts::refs::IdentityTimestamp,
        expected_version: IdentityVersion,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityVersionedRef<IdentityIdempotencyRecordRef>, ApplicationError>;
}

/// Repository for replayable stored result shells and typed receipt envelopes.
pub trait IdentityStoredResultRepository {
    /// Loads a generic stored result shell by stored result ref.
    fn get_stored_result(
        &self,
        stored_result_ref: IdentityStoredResultRef,
    ) -> Result<Option<StoredIdentityOperationResult>, ApplicationError>;

    /// Finds a generic stored result shell by operation context ref.
    fn find_by_operation_context(
        &self,
        context_ref: IdentityOperationContextRef,
    ) -> Result<Option<StoredIdentityOperationResult>, ApplicationError>;

    /// Saves a stored accepted command result shell.
    fn save_command_accepted_result(
        &self,
        result: StoredIdentityOperationResult,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityStoredResultRef, ApplicationError>;

    /// Saves a stored rejected command result shell.
    fn save_command_rejected_result(
        &self,
        result: StoredIdentityOperationResult,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityStoredResultRef, ApplicationError>;

    /// Saves a generic stored consumer receipt result shell.
    fn save_consumer_receipt_result(
        &self,
        result: StoredIdentityOperationResult,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityStoredResultRef, ApplicationError>;

    /// Loads a typed consumer receipt envelope by stored result ref.
    fn get_consumer_receipt(
        &self,
        stored_result_ref: IdentityStoredResultRef,
    ) -> Result<Option<IdentityConsumerReceiptEnvelope>, ApplicationError>;

    /// Saves a typed consumer receipt envelope.
    fn save_consumer_receipt(
        &self,
        envelope: IdentityConsumerReceiptEnvelope,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityStoredResultRef, ApplicationError>;

    /// Saves a stored job report result shell.
    fn save_job_report_result(
        &self,
        result: StoredIdentityOperationResult,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityStoredResultRef, ApplicationError>;

    /// Saves a generic stored handoff callback receipt result shell.
    fn save_handoff_callback_receipt_result(
        &self,
        result: StoredIdentityOperationResult,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityStoredResultRef, ApplicationError>;

    /// Loads a typed handoff callback receipt envelope by stored result ref.
    fn get_handoff_callback_receipt(
        &self,
        stored_result_ref: IdentityStoredResultRef,
    ) -> Result<Option<IdentityConsumerReceiptEnvelope>, ApplicationError>;

    /// Saves a typed handoff callback receipt envelope.
    fn save_handoff_callback_receipt(
        &self,
        envelope: IdentityConsumerReceiptEnvelope,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityStoredResultRef, ApplicationError>;
}

/// Repository for accepted command effect summaries.
pub trait IdentityCommandEffectSummaryRepository {
    /// Loads an effect summary by stable ref.
    fn get_effect_summary(
        &self,
        effect_summary_ref: IdentityCommandEffectSummaryRef,
    ) -> Result<Option<crate::support::IdentityCommandEffectSummary>, ApplicationError>;

    /// Lists effect summaries by operation context.
    fn list_effects_by_operation_context(
        &self,
        context_ref: IdentityOperationContextRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityCommandEffectSummaryRef>, ApplicationError>;

    /// Lists effect summaries by typed accepted truth ref.
    fn list_effects_by_truth_ref(
        &self,
        truth_ref: crate::support::IdentityTruthRef,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityCommandEffectSummaryRef>, ApplicationError>;

    /// Lists effect summaries after a truth cursor.
    fn list_effects_after_cursor(
        &self,
        after_cursor: Option<IdentityTruthCursor>,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityCommandEffectSummaryRef>, ApplicationError>;

    /// Saves an immutable accepted command effect summary.
    fn save_effect_summary(
        &self,
        summary: crate::support::IdentityCommandEffectSummary,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityCommandEffectSummaryRef, ApplicationError>;
}

/// Repository for job run reports and duplicate replay lookups.
pub trait IdentityJobReportRepository {
    /// Loads a job report and version.
    fn get_job_report_with_version(
        &self,
        report_ref: identity_contracts::refs::IdentityJobReportRef,
    ) -> Result<Option<Versioned<crate::support::IdentityJobRunReport>>, ApplicationError>;

    /// Finds a job report by formal job run ref.
    fn find_job_report_by_run(
        &self,
        job_run_ref: IdentityJobRunRef,
    ) -> Result<Option<Versioned<crate::support::IdentityJobRunReport>>, ApplicationError>;

    /// Lists job reports by formal job name.
    fn list_job_reports_by_name(
        &self,
        job_name: IdentityJobName,
        page: IdentityRepositoryPage,
    ) -> Result<
        Page<IdentityVersionedRef<identity_contracts::refs::IdentityJobReportRef>>,
        ApplicationError,
    >;

    /// Lists job reports by result kind.
    fn list_job_reports_by_result(
        &self,
        result_kind: identity_contracts::jobs::IdentityJobResultKind,
        page: IdentityRepositoryPage,
    ) -> Result<
        Page<IdentityVersionedRef<identity_contracts::refs::IdentityJobReportRef>>,
        ApplicationError,
    >;

    /// Saves a job report using an optional expected version.
    fn save_job_report(
        &self,
        report: crate::support::IdentityJobRunReport,
        expected_version: Option<IdentityVersion>,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<
        IdentityVersionedRef<identity_contracts::refs::IdentityJobReportRef>,
        ApplicationError,
    >;
}

/// Body-free role capability source resolution result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoleCapabilitySourceResolution {
    /// Typed source ref.
    pub source_ref: RoleCapabilitySourceRef,
    /// Resolved source state.
    pub source_state: RoleCapabilitySourceStateKind,
    /// Optional source version ref.
    pub source_version_ref: Option<RoleCapabilitySourceVersionRef>,
    /// Optional safe summary ref.
    pub safe_summary_ref: Option<RoleCapabilitySafeSummaryRef>,
    /// Optional evidence refs.
    pub evidence_refs: Vec<CapabilityEvidenceRef>,
    /// Formal material marker.
    pub material_marker: identity_contracts::refs::RoleCapabilityChangeMaterialMarker,
}

/// Body-free capability evidence resolution result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapabilityEvidenceResolution {
    /// Typed evidence ref.
    pub evidence_ref: CapabilityEvidenceRef,
    /// Formal evidence state.
    pub evidence_state: identity_domain::reference_state::ReferenceResolutionStateKind,
    /// Optional safe summary ref.
    pub safe_summary_ref: Option<ExternalReferenceSafeSummaryRef>,
    /// Optional external source version ref.
    pub source_version_ref: Option<ExternalSourceVersionRef>,
}

/// Resolves external business sources into body-free safe summaries.
pub trait IdentityExternalSourceResolverPort {
    /// Resolves a governance basis into a formal safe summary.
    fn resolve_governance_basis(
        &self,
        basis_ref: GovernanceBasisRef,
        risk_ref: Option<LifecycleRiskRef>,
    ) -> Result<GovernanceBasisSummary, ApplicationError>;

    /// Resolves a role capability source into a formal source summary.
    fn resolve_role_capability_source(
        &self,
        source_ref: RoleCapabilitySourceRef,
    ) -> Result<RoleCapabilitySourceResolution, ApplicationError>;

    /// Resolves a capability evidence marker into a body-free evidence summary.
    fn resolve_capability_evidence(
        &self,
        evidence_ref: CapabilityEvidenceRef,
    ) -> Result<CapabilityEvidenceResolution, ApplicationError>;

    /// Resolves a work participation source into a body-free summary.
    fn resolve_work_participation(
        &self,
        source_ref: WorkSourceRef,
    ) -> Result<WorkParticipationSourceSummary, ApplicationError>;

    /// Resolves a memory or archive source into a body-free summary.
    fn resolve_memory_reference_source(
        &self,
        source_ref: MemoryReferenceSourceRef,
    ) -> Result<identity_contracts::refs::MemoryReferenceSourceSummary, ApplicationError>;

    /// Resolves an archive handoff source into a body-free summary.
    fn resolve_archive_handoff_source(
        &self,
        handoff_ref: ArchiveHandoffRef,
    ) -> Result<identity_contracts::refs::MemoryReferenceSourceSummary, ApplicationError>;
}

/// Resolves external reference bundles and local owner mappings.
pub trait IdentityExternalReferenceResolverPort {
    /// Resolves a formal external reference bundle for a local owner.
    fn resolve_external_reference(
        &self,
        reference_ref: ExternalReferenceRef,
        owner_ref: IdentityReferenceOwnerRef,
    ) -> Result<ReferenceResolutionState, ApplicationError>;

    /// Maps a role capability summary to a formal local owner ref.
    fn map_role_capability_owner(
        &self,
        summary_ref: RoleCapabilitySummaryRef,
    ) -> Result<IdentityReferenceOwnerRef, ApplicationError>;

    /// Maps a career record to a formal local owner ref.
    fn map_career_owner(
        &self,
        record_ref: CareerRecordRef,
    ) -> Result<IdentityReferenceOwnerRef, ApplicationError>;

    /// Maps a memory reference to a formal local owner ref.
    fn map_memory_owner(
        &self,
        reference_ref: MemoryReferenceRef,
    ) -> Result<IdentityReferenceOwnerRef, ApplicationError>;

    /// Maps a member and governance basis to a formal local owner ref.
    fn map_lifecycle_basis_owner(
        &self,
        member_ref: GlobalMemberRef,
        basis_ref: GovernanceBasisRef,
    ) -> Result<IdentityReferenceOwnerRef, ApplicationError>;
}

/// Provides configured adapter availability without executing business operations.
pub trait IdentityAdapterAvailabilityPort {
    /// Loads a single adapter availability snapshot.
    fn get_adapter_availability(
        &self,
        adapter_ref: IdentityAdapterRef,
    ) -> Result<IdentityAdapterAvailability, ApplicationError>;

    /// Lists adapter availability snapshots.
    fn list_adapter_availability(
        &self,
        page: IdentityRepositoryPage,
    ) -> Result<Page<IdentityAdapterAvailability>, ApplicationError>;

    /// Asserts that an adapter attempt is allowed for the required mode.
    fn assert_adapter_attempt_allowed(
        &self,
        adapter_ref: IdentityAdapterRef,
        required_mode: Option<IdentityAdapterModeRef>,
    ) -> Result<IdentityAdapterAvailability, ApplicationError>;
}

/// Resolved topic binding used for body-free publish attempts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TopicBindingResolution {
    /// Formal topic key ref.
    pub topic_key_ref: TopicKeyRef,
    /// Target adapter ref.
    pub adapter_ref: IdentityAdapterRef,
    /// Target adapter mode ref.
    pub adapter_mode_ref: IdentityAdapterModeRef,
    /// Publish scope marker.
    pub publish_scope_ref: IdentitySourceRef,
}

/// Publisher outcome classification for outbox delivery.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutboxPublishOutcome {
    /// Publisher boundary accepted the record.
    Published {
        /// Formal publish attempt marker.
        attempt_ref: OutboxDeliveryAttemptRef,
    },
    /// Publish failed and may be retried.
    RetryableFailed {
        /// Optional formal publish attempt marker.
        attempt_ref: Option<OutboxDeliveryAttemptRef>,
        /// Safe issue marker.
        issue_ref: OutboxDeliveryIssueRef,
    },
    /// Publish failed terminally.
    PermanentlyFailed {
        /// Optional formal publish attempt marker.
        attempt_ref: Option<OutboxDeliveryAttemptRef>,
        /// Safe issue marker.
        issue_ref: OutboxDeliveryIssueRef,
    },
    /// Publish skipped by policy.
    SkippedByPolicy {
        /// Safe issue marker.
        issue_ref: OutboxDeliveryIssueRef,
    },
    /// Topic binding is unsupported.
    UnsupportedTopic {
        /// Safe issue marker.
        issue_ref: OutboxDeliveryIssueRef,
    },
}

/// Resolves topic keys into publish boundary targets.
pub trait IdentityTopicBindingPort {
    /// Resolves a topic binding for a body-free payload marker.
    fn resolve_topic_binding(
        &self,
        topic_key_ref: TopicKeyRef,
        payload_marker_ref: IdentityOutboxPayloadMarkerRef,
    ) -> Result<TopicBindingResolution, ApplicationError>;
}

/// Publishes body-free outbox material to an outbound boundary.
pub trait IdentityOutboxPublisherPort {
    /// Publishes a formal outbox record via a resolved topic binding.
    fn publish_outbox_record(
        &self,
        record_ref: IdentityOutboxRecordRef,
        topic_binding: TopicBindingResolution,
        payload_marker_ref: IdentityOutboxPayloadMarkerRef,
    ) -> Result<OutboxPublishOutcome, ApplicationError>;
}

/// Resolved handoff target used for body-free delivery attempts.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HandoffTargetResolution {
    /// Formal target ref.
    pub target_ref: HandoffTargetRef,
    /// Formal scope ref.
    pub scope_ref: HandoffScopeRef,
    /// Target adapter ref.
    pub adapter_ref: IdentityAdapterRef,
    /// Target adapter mode ref.
    pub adapter_mode_ref: IdentityAdapterModeRef,
}

/// Handoff delivery outcome classification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HandoffDeliveryOutcome {
    /// Handoff delivered with a formal receipt marker.
    Delivered {
        /// Formal attempt marker.
        attempt_ref: HandoffAttemptRef,
        /// Formal receipt marker.
        receipt_ref: HandoffReceiptRef,
    },
    /// Handoff failed and may be retried.
    RetryableFailed {
        /// Formal attempt marker.
        attempt_ref: HandoffAttemptRef,
        /// Safe issue marker.
        issue_ref: HandoffIssueRef,
    },
    /// Handoff failed terminally.
    PermanentlyFailed {
        /// Formal attempt marker.
        attempt_ref: HandoffAttemptRef,
        /// Safe issue marker.
        issue_ref: HandoffIssueRef,
    },
    /// Handoff was cancelled by policy.
    CancelledByPolicy {
        /// Safe issue marker.
        issue_ref: HandoffIssueRef,
    },
    /// Target binding is unsupported.
    UnsupportedTarget {
        /// Safe issue marker.
        issue_ref: HandoffIssueRef,
    },
}

/// Handoff receipt resolution result.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HandoffReceiptResolution {
    /// Formal receipt ref.
    pub receipt_ref: HandoffReceiptRef,
    /// Formal receipt state.
    pub receipt_state: identity_domain::reference_state::ReferenceResolutionStateKind,
    /// Optional safe issue marker.
    pub issue_ref: Option<HandoffIssueRef>,
}

/// Resolves handoff targets into delivery boundaries.
pub trait IdentityHandoffTargetPort {
    /// Resolves a handoff target and scope into a delivery boundary.
    fn resolve_handoff_target(
        &self,
        target_ref: HandoffTargetRef,
        scope_ref: HandoffScopeRef,
        safe_material_ref: TraceHandoffSafeMaterialRef,
    ) -> Result<HandoffTargetResolution, ApplicationError>;
}

/// Delivers body-free trace, audit, or archive handoff material.
pub trait IdentityHandoffDeliveryPort {
    /// Delivers a handoff intent to the resolved boundary.
    fn deliver_handoff(
        &self,
        intent_ref: TraceHandoffIntentRef,
        target_resolution: HandoffTargetResolution,
        safe_material_ref: TraceHandoffSafeMaterialRef,
    ) -> Result<HandoffDeliveryOutcome, ApplicationError>;

    /// Resolves a formal handoff receipt marker.
    fn resolve_handoff_receipt(
        &self,
        receipt_ref: HandoffReceiptRef,
    ) -> Result<HandoffReceiptResolution, ApplicationError>;
}
