//! Entry-visible application facade that routes all entry surfaces through shared services.

use identity_contracts::commands::{
    AppendCareerRecordRequest, CareerRecordCommandResult, EstablishGlobalMemberRequest,
    GlobalLifecycleCommandResult, GlobalMemberCommandResult, IdentityCommandOutcome,
    IdentityCommandRequest, MaintainMemoryReferenceRequest, MaintainRoleCapabilitySummaryRequest,
    MemoryReferenceCommandResult, PrepareTraceHandoffRequest, RoleCapabilityCommandResult,
    TraceHandoffCommandResult, UpdateGlobalLifecycleStateRequest,
};
use identity_contracts::events::{
    ArchiveHandoffResultPayload, IdentityConsumerReceipt, IdentityInboundEventEnvelope,
    MemoryReferenceSourceStateChangedPayload, RoleCapabilitySourceChangedPayload,
    TraceHandoffResultPayload, WorkParticipationAcceptedPayload,
};
use identity_contracts::jobs::{
    DeliverTraceHandoffJobInput, DeliverTraceHandoffJobOutput, IdentityJobRequest,
    IdentityJobResponse, PublishIdentityOutboxJobInput, PublishIdentityOutboxJobOutput,
    RebuildIdentityProjectionJobInput, RebuildIdentityProjectionJobOutput,
    RefreshExternalReferenceStateJobInput, RefreshExternalReferenceStateJobOutput,
    RetryIdentityPropagationFailuresJobInput, RetryIdentityPropagationFailuresJobOutput,
    RunIdentityReconciliationJobInput, RunIdentityReconciliationJobOutput,
};
use identity_contracts::queries::{
    GetGlobalLifecycleSummaryRequest, GetGlobalMemberAnchorRequest, GetIdentityOutboxStateRequest,
    GetProjectionStateRequest, GetReferenceResolutionStateRequest, GetRoleCapabilitySummaryRequest,
    GetTraceHandoffStateRequest, IdentityPageResponse, IdentityQueryRequest, IdentityQueryResponse,
    ListCareerRecordsRequest, ListMemoryReferencesRequest, ListPendingIdentityOutboxRequest,
    ReadAuditTrailRequest, ReadIdentityTraceRequest, ReadMemberSummaryRequest,
    ReadReconciliationReportRequest,
};
use identity_contracts::views::{
    AuditTrailEntryView, CareerRecordView, GlobalLifecycleSummaryView, GlobalMemberAnchorView,
    IdentityOutboxRecordView, IdentityOutboxStateView, IdentityTraceRecordView, MemberSummaryView,
    MemoryReferenceView, ProjectionStateView, ReconciliationReportView,
    ReferenceResolutionStateView, RoleCapabilitySummaryView, TraceHandoffStateView,
};

use crate::command::IdentityCommandService;
use crate::consumer::IdentityConsumerService;
use crate::errors::ApplicationError;
use crate::jobs::IdentityJobService;
use crate::query::IdentityQueryService;
use crate::support::IdentityOperationContext;

/// Entry-visible typed command dispatch input.
pub enum IdentityApplicationCommandRequest {
    EstablishGlobalMember(IdentityCommandRequest<EstablishGlobalMemberRequest>),
    UpdateGlobalLifecycleState(IdentityCommandRequest<UpdateGlobalLifecycleStateRequest>),
    MaintainRoleCapabilitySummary(IdentityCommandRequest<MaintainRoleCapabilitySummaryRequest>),
    AppendCareerRecord(IdentityCommandRequest<AppendCareerRecordRequest>),
    MaintainMemoryReference(IdentityCommandRequest<MaintainMemoryReferenceRequest>),
    PrepareTraceHandoff(IdentityCommandRequest<PrepareTraceHandoffRequest>),
}

/// Entry-visible typed command dispatch result.
pub enum IdentityApplicationCommandResponse {
    EstablishGlobalMember(IdentityCommandOutcome<GlobalMemberCommandResult>),
    UpdateGlobalLifecycleState(IdentityCommandOutcome<GlobalLifecycleCommandResult>),
    MaintainRoleCapabilitySummary(IdentityCommandOutcome<RoleCapabilityCommandResult>),
    AppendCareerRecord(IdentityCommandOutcome<CareerRecordCommandResult>),
    MaintainMemoryReference(IdentityCommandOutcome<MemoryReferenceCommandResult>),
    PrepareTraceHandoff(IdentityCommandOutcome<TraceHandoffCommandResult>),
}

/// Entry-visible typed query dispatch input.
pub enum IdentityApplicationQueryRequest {
    GetGlobalMemberAnchor(IdentityQueryRequest<GetGlobalMemberAnchorRequest>),
    GetGlobalLifecycleSummary(IdentityQueryRequest<GetGlobalLifecycleSummaryRequest>),
    GetRoleCapabilitySummary(IdentityQueryRequest<GetRoleCapabilitySummaryRequest>),
    ListCareerRecords(IdentityQueryRequest<ListCareerRecordsRequest>),
    ListMemoryReferences(IdentityQueryRequest<ListMemoryReferencesRequest>),
    ReadMemberSummary(IdentityQueryRequest<ReadMemberSummaryRequest>),
    ReadIdentityTrace(IdentityQueryRequest<ReadIdentityTraceRequest>),
    ReadAuditTrail(IdentityQueryRequest<ReadAuditTrailRequest>),
    GetProjectionState(IdentityQueryRequest<GetProjectionStateRequest>),
    GetReferenceResolutionState(IdentityQueryRequest<GetReferenceResolutionStateRequest>),
    ReadReconciliationReport(IdentityQueryRequest<ReadReconciliationReportRequest>),
    ListPendingIdentityOutbox(IdentityQueryRequest<ListPendingIdentityOutboxRequest>),
    GetIdentityOutboxState(IdentityQueryRequest<GetIdentityOutboxStateRequest>),
    GetTraceHandoffState(IdentityQueryRequest<GetTraceHandoffStateRequest>),
}

/// Entry-visible typed query dispatch result.
pub enum IdentityApplicationQueryResponse {
    GetGlobalMemberAnchor(IdentityQueryResponse<GlobalMemberAnchorView>),
    GetGlobalLifecycleSummary(IdentityQueryResponse<GlobalLifecycleSummaryView>),
    GetRoleCapabilitySummary(IdentityQueryResponse<RoleCapabilitySummaryView>),
    ListCareerRecords(IdentityPageResponse<CareerRecordView>),
    ListMemoryReferences(IdentityPageResponse<MemoryReferenceView>),
    ReadMemberSummary(IdentityQueryResponse<MemberSummaryView>),
    ReadIdentityTrace(IdentityPageResponse<IdentityTraceRecordView>),
    ReadAuditTrail(IdentityPageResponse<AuditTrailEntryView>),
    GetProjectionState(IdentityQueryResponse<ProjectionStateView>),
    GetReferenceResolutionState(IdentityQueryResponse<ReferenceResolutionStateView>),
    ReadReconciliationReport(IdentityPageResponse<ReconciliationReportView>),
    ListPendingIdentityOutbox(IdentityPageResponse<IdentityOutboxRecordView>),
    GetIdentityOutboxState(IdentityQueryResponse<IdentityOutboxStateView>),
    GetTraceHandoffState(IdentityQueryResponse<TraceHandoffStateView>),
}

/// Entry-visible typed inbound-event dispatch input.
pub enum IdentityApplicationInboundEventRequest {
    HandleRoleCapabilitySourceChanged(
        IdentityInboundEventEnvelope<RoleCapabilitySourceChangedPayload>,
    ),
    HandleWorkParticipationAccepted(IdentityInboundEventEnvelope<WorkParticipationAcceptedPayload>),
    HandleMemoryReferenceSourceStateChanged(
        IdentityInboundEventEnvelope<MemoryReferenceSourceStateChangedPayload>,
    ),
}

/// Entry-visible typed callback dispatch input.
pub enum IdentityApplicationCallbackRequest {
    HandleArchiveHandoffResult(IdentityInboundEventEnvelope<ArchiveHandoffResultPayload>),
    HandleTraceHandoffResult(IdentityInboundEventEnvelope<TraceHandoffResultPayload>),
}

/// Entry-visible typed job dispatch input.
pub enum IdentityApplicationJobRequest {
    RebuildIdentityProjection(IdentityJobRequest<RebuildIdentityProjectionJobInput>),
    RefreshExternalReferenceState(IdentityJobRequest<RefreshExternalReferenceStateJobInput>),
    RunIdentityReconciliation(IdentityJobRequest<RunIdentityReconciliationJobInput>),
    PublishIdentityOutbox(IdentityJobRequest<PublishIdentityOutboxJobInput>),
    DeliverTraceHandoff(IdentityJobRequest<DeliverTraceHandoffJobInput>),
    RetryIdentityPropagationFailures(IdentityJobRequest<RetryIdentityPropagationFailuresJobInput>),
}

/// Entry-visible typed job dispatch result.
pub enum IdentityApplicationJobResponse {
    RebuildIdentityProjection(IdentityJobResponse<RebuildIdentityProjectionJobOutput>),
    RefreshExternalReferenceState(IdentityJobResponse<RefreshExternalReferenceStateJobOutput>),
    RunIdentityReconciliation(IdentityJobResponse<RunIdentityReconciliationJobOutput>),
    PublishIdentityOutbox(IdentityJobResponse<PublishIdentityOutboxJobOutput>),
    DeliverTraceHandoff(IdentityJobResponse<DeliverTraceHandoffJobOutput>),
    RetryIdentityPropagationFailures(
        IdentityJobResponse<RetryIdentityPropagationFailuresJobOutput>,
    ),
}

/// Entry-visible facade for identity application use cases.
pub struct IdentityApplicationFacade<'a> {
    command_service: IdentityCommandService<'a>,
    query_service: Option<IdentityQueryService<'a>>,
    consumer_service: Option<IdentityConsumerService<'a>>,
    job_service: Option<IdentityJobService<'a>>,
}

impl<'a> IdentityApplicationFacade<'a> {
    /// Creates a facade with the mandatory command service.
    pub fn new(command_service: IdentityCommandService<'a>) -> Self {
        Self {
            command_service,
            query_service: None,
            consumer_service: None,
            job_service: None,
        }
    }

    /// Attaches the shared query service.
    pub fn with_query_service(mut self, query_service: IdentityQueryService<'a>) -> Self {
        self.query_service = Some(query_service);
        self
    }

    /// Attaches the shared consumer/callback service.
    pub fn with_consumer_service(mut self, consumer_service: IdentityConsumerService<'a>) -> Self {
        self.consumer_service = Some(consumer_service);
        self
    }

    /// Attaches the shared operations-job service.
    pub fn with_job_service(mut self, job_service: IdentityJobService<'a>) -> Self {
        self.job_service = Some(job_service);
        self
    }

    /// Returns the command service used by the facade shell.
    pub fn command_service(&self) -> &IdentityCommandService<'a> {
        &self.command_service
    }

    /// Returns the configured query service, if this boundary wired one.
    pub fn query_service(&self) -> Option<&IdentityQueryService<'a>> {
        self.query_service.as_ref()
    }

    /// Returns the configured consumer/callback service, if this boundary wired one.
    pub fn consumer_service(&self) -> Option<&IdentityConsumerService<'a>> {
        self.consumer_service.as_ref()
    }

    /// Returns the configured job service, if this boundary wired one.
    pub fn job_service(&self) -> Option<&IdentityJobService<'a>> {
        self.job_service.as_ref()
    }

    /// Dispatches one command through the shared command service.
    pub fn dispatch_command(
        &self,
        context: IdentityOperationContext,
        request: IdentityApplicationCommandRequest,
    ) -> Result<IdentityApplicationCommandResponse, ApplicationError> {
        match request {
            IdentityApplicationCommandRequest::EstablishGlobalMember(request) => self
                .command_service
                .establish_global_member(request, context)
                .map(IdentityApplicationCommandResponse::EstablishGlobalMember),
            IdentityApplicationCommandRequest::UpdateGlobalLifecycleState(request) => self
                .command_service
                .update_global_lifecycle_state(request, context)
                .map(IdentityApplicationCommandResponse::UpdateGlobalLifecycleState),
            IdentityApplicationCommandRequest::MaintainRoleCapabilitySummary(request) => self
                .command_service
                .maintain_role_capability_summary(request, context)
                .map(IdentityApplicationCommandResponse::MaintainRoleCapabilitySummary),
            IdentityApplicationCommandRequest::AppendCareerRecord(request) => self
                .command_service
                .append_career_record(request, context)
                .map(IdentityApplicationCommandResponse::AppendCareerRecord),
            IdentityApplicationCommandRequest::MaintainMemoryReference(request) => self
                .command_service
                .maintain_memory_reference(request, context)
                .map(IdentityApplicationCommandResponse::MaintainMemoryReference),
            IdentityApplicationCommandRequest::PrepareTraceHandoff(request) => self
                .command_service
                .prepare_trace_handoff(request, context)
                .map(IdentityApplicationCommandResponse::PrepareTraceHandoff),
        }
    }

    /// Dispatches one query through the shared query service.
    pub fn dispatch_query(
        &self,
        context: IdentityOperationContext,
        request: IdentityApplicationQueryRequest,
    ) -> Result<IdentityApplicationQueryResponse, ApplicationError> {
        let service = self
            .query_service
            .as_ref()
            .ok_or_else(|| ApplicationError::invalid_request("query service is not configured"))?;
        match request {
            IdentityApplicationQueryRequest::GetGlobalMemberAnchor(request) => service
                .get_global_member_anchor(request, context)
                .map(IdentityApplicationQueryResponse::GetGlobalMemberAnchor),
            IdentityApplicationQueryRequest::GetGlobalLifecycleSummary(request) => service
                .get_global_lifecycle_summary(request, context)
                .map(IdentityApplicationQueryResponse::GetGlobalLifecycleSummary),
            IdentityApplicationQueryRequest::GetRoleCapabilitySummary(request) => service
                .get_role_capability_summary(request, context)
                .map(IdentityApplicationQueryResponse::GetRoleCapabilitySummary),
            IdentityApplicationQueryRequest::ListCareerRecords(request) => service
                .list_career_records(request, context)
                .map(IdentityApplicationQueryResponse::ListCareerRecords),
            IdentityApplicationQueryRequest::ListMemoryReferences(request) => service
                .list_memory_references(request, context)
                .map(IdentityApplicationQueryResponse::ListMemoryReferences),
            IdentityApplicationQueryRequest::ReadMemberSummary(request) => service
                .read_member_summary(request, context)
                .map(IdentityApplicationQueryResponse::ReadMemberSummary),
            IdentityApplicationQueryRequest::ReadIdentityTrace(request) => service
                .read_identity_trace(request, context)
                .map(IdentityApplicationQueryResponse::ReadIdentityTrace),
            IdentityApplicationQueryRequest::ReadAuditTrail(request) => service
                .read_audit_trail(request, context)
                .map(IdentityApplicationQueryResponse::ReadAuditTrail),
            IdentityApplicationQueryRequest::GetProjectionState(request) => service
                .get_projection_state(request, context)
                .map(IdentityApplicationQueryResponse::GetProjectionState),
            IdentityApplicationQueryRequest::GetReferenceResolutionState(request) => service
                .get_reference_resolution_state(request, context)
                .map(IdentityApplicationQueryResponse::GetReferenceResolutionState),
            IdentityApplicationQueryRequest::ReadReconciliationReport(request) => service
                .read_reconciliation_report(request, context)
                .map(IdentityApplicationQueryResponse::ReadReconciliationReport),
            IdentityApplicationQueryRequest::ListPendingIdentityOutbox(request) => service
                .list_pending_identity_outbox(request, context)
                .map(IdentityApplicationQueryResponse::ListPendingIdentityOutbox),
            IdentityApplicationQueryRequest::GetIdentityOutboxState(request) => service
                .get_identity_outbox_state(request, context)
                .map(IdentityApplicationQueryResponse::GetIdentityOutboxState),
            IdentityApplicationQueryRequest::GetTraceHandoffState(request) => service
                .get_trace_handoff_state(request, context)
                .map(IdentityApplicationQueryResponse::GetTraceHandoffState),
        }
    }

    /// Dispatches one inbound consumer event through the shared consumer service.
    pub fn dispatch_inbound_event(
        &self,
        context: IdentityOperationContext,
        request: IdentityApplicationInboundEventRequest,
    ) -> Result<IdentityConsumerReceipt, ApplicationError> {
        let service = self.consumer_service.as_ref().ok_or_else(|| {
            ApplicationError::invalid_request("consumer/callback service is not configured")
        })?;
        match request {
            IdentityApplicationInboundEventRequest::HandleRoleCapabilitySourceChanged(request) => {
                service.handle_role_capability_source_changed(request, context)
            }
            IdentityApplicationInboundEventRequest::HandleWorkParticipationAccepted(request) => {
                service.handle_work_participation_accepted(request, context)
            }
            IdentityApplicationInboundEventRequest::HandleMemoryReferenceSourceStateChanged(
                request,
            ) => service.handle_memory_reference_source_state_changed(request, context),
        }
    }

    /// Dispatches one callback event through the shared consumer/callback service.
    pub fn dispatch_callback(
        &self,
        context: IdentityOperationContext,
        request: IdentityApplicationCallbackRequest,
    ) -> Result<IdentityConsumerReceipt, ApplicationError> {
        let service = self.consumer_service.as_ref().ok_or_else(|| {
            ApplicationError::invalid_request("consumer/callback service is not configured")
        })?;
        match request {
            IdentityApplicationCallbackRequest::HandleArchiveHandoffResult(request) => {
                service.handle_archive_handoff_result(request, context)
            }
            IdentityApplicationCallbackRequest::HandleTraceHandoffResult(request) => {
                service.handle_trace_handoff_result(request, context)
            }
        }
    }

    /// Dispatches one operations job through the shared job service.
    pub fn dispatch_job(
        &self,
        context: IdentityOperationContext,
        request: IdentityApplicationJobRequest,
    ) -> Result<IdentityApplicationJobResponse, ApplicationError> {
        let service = self
            .job_service
            .as_ref()
            .ok_or_else(|| ApplicationError::invalid_request("job service is not configured"))?;
        match request {
            IdentityApplicationJobRequest::RebuildIdentityProjection(request) => service
                .rebuild_identity_projection(request, context)
                .map(IdentityApplicationJobResponse::RebuildIdentityProjection),
            IdentityApplicationJobRequest::RefreshExternalReferenceState(request) => service
                .refresh_external_reference_state(request, context)
                .map(IdentityApplicationJobResponse::RefreshExternalReferenceState),
            IdentityApplicationJobRequest::RunIdentityReconciliation(request) => service
                .run_identity_reconciliation(request, context)
                .map(IdentityApplicationJobResponse::RunIdentityReconciliation),
            IdentityApplicationJobRequest::PublishIdentityOutbox(request) => service
                .publish_identity_outbox(request, context)
                .map(IdentityApplicationJobResponse::PublishIdentityOutbox),
            IdentityApplicationJobRequest::DeliverTraceHandoff(request) => service
                .deliver_trace_handoff(request, context)
                .map(IdentityApplicationJobResponse::DeliverTraceHandoff),
            IdentityApplicationJobRequest::RetryIdentityPropagationFailures(request) => service
                .retry_identity_propagation_failures(request, context)
                .map(IdentityApplicationJobResponse::RetryIdentityPropagationFailures),
        }
    }
}
