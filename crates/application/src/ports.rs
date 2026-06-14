//! Application-local port traits for shared helpers and runtime foundations.

use core_contracts::actor::ActorRef;
use identity_contracts::receipts::TraceHandoffIntentRef;
use identity_contracts::refs::{
    GlobalMemberId, HandoffIssueRef, HandoffReceiptRef, IdentityJobRunRef,
    IdentityOutboxPayloadMarkerRef, IdentityOutboxRecordRef, IdentitySourceEventRef,
    IdentityStoredResultRef, IdentityTruthCursor, RoleCapabilitySourceSnapshotId,
    RoleCapabilitySummaryId,
};

use crate::errors::ApplicationError;
use crate::support::{
    AuditTrailId, IdentityApiEntryRef, IdentityCommandEffectSummaryRef, IdentityEntryDispatchRef,
    IdentityIdempotencyKey, IdentityIdempotencyRecordRef, IdentityJobDispatchRef,
    IdentityJobEntryRef, IdentityOperationContext, IdentityOperationContextRef,
    IdentityOperationName, IdentityRequestDigest, IdentityRequestMetadataRef,
    IdentityRuntimeAssemblyRef, IdentityStoredSurfaceMarkerRef, IdentityTraceRecordId,
    IdentityTransactionRef, IdentityWorkerDispatchRef, IdentityWorkerEntryRef, MemberSummaryViewId,
    ReconciliationReportId,
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
