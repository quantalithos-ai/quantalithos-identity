//! Shared query foundation helpers for visibility-first, read-only flows.

use core_contracts::actor::ActorRef;
use identity_contracts::metadata::{
    IdentityDegradedKind, IdentityQueryDisposition, IdentityQuerySurface,
};
use identity_contracts::protocol::IdentityQueryName;
use identity_contracts::queries::{
    GetGlobalLifecycleSummaryRequest, GetGlobalMemberAnchorRequest, GetIdentityOutboxStateRequest,
    GetProjectionStateRequest, GetReferenceResolutionStateRequest, GetRoleCapabilitySummaryRequest,
    GetTraceHandoffStateRequest, IdentityOutboxListSelector, IdentityPageResponse,
    IdentityPublicPageCursor, IdentityPublicPageInfo, IdentityPublicPageRequest,
    IdentityQueryRequest, IdentityQueryResponse, IdentityTraceReadSelector,
    ListCareerRecordsRequest, ListMemoryReferencesRequest, ListPendingIdentityOutboxRequest,
    ReadAuditTrailRequest, ReadIdentityTraceRequest, ReadMemberSummaryRequest,
    ReadReconciliationReportRequest,
};
use identity_contracts::refs::{
    CareerRecordStateKind as PublicCareerRecordStateKind, ConsumerRef,
    GlobalLifecycleStateKind as PublicLifecycleStateKind, GlobalMemberRef,
    HandoffStateKind as PublicHandoffStateKind, IdentityAnchorStateKind as PublicAnchorStateKind,
    IdentityReadSurfaceKind, MemoryReferenceStateKind as PublicMemoryReferenceStateKind,
    OutboxStateKind as PublicOutboxStateKind, ProjectionStateKind as PublicProjectionStateKind,
    ReconciliationReportStateKind as PublicReconciliationReportStateKind,
    ReferenceResolutionStateKind as PublicReferenceResolutionStateKind,
    RoleCapabilitySourceStateKind as PublicRoleCapabilitySourceStateKind,
    RoleCapabilitySummaryStateKind as PublicRoleCapabilitySummaryStateKind, VisibilityContextRef,
};
use identity_contracts::views::{
    AuditTrailEntryView, CareerRecordView, GlobalLifecycleSummaryView, GlobalMemberAnchorView,
    IdentityOutboxRecordView, IdentityOutboxStateView, IdentityReadMaterialKind,
    IdentityReadMaterialMarker, IdentityTraceRecordView, IdentityVisibilityAccessState,
    IdentityVisibilityAccessSummary, MemberSummaryView, MemoryReferenceView, ProjectionStateView,
    ReconciliationReportView, ReferenceResolutionSidecarRefsView, ReferenceResolutionStateView,
    RoleCapabilitySummaryView, TraceHandoffStateView,
};
use identity_domain::career::CareerRecord;
use identity_domain::handoff::{HandoffStateKind, TraceHandoffIntent};
use identity_domain::member_identity::IdentityAnchorState;
use identity_domain::memory_reference::{MemoryReference, MemoryReferenceStateKind};
use identity_domain::outbox::{IdentityOutboxRecord, OutboxStateKind};
use identity_domain::projection_state::{ProjectionState, ProjectionStateKind};
use identity_domain::reconciliation::{ReconciliationReport, ReconciliationReportStateKind};
use identity_domain::reference_state::{ReferenceResolutionState, ReferenceResolutionStateKind};
use identity_domain::role_capability::{
    RoleCapabilitySourceSnapshot, RoleCapabilitySourceStateKind, RoleCapabilitySummaryStateKind,
};
use identity_domain::trace::IdentityTraceRecord;

use crate::errors::{ApplicationError, ApplicationErrorKind};
use crate::ports::{
    CareerRecordRepository, GlobalLifecycleRepository, GlobalMemberRepository,
    IdentityAuditTrailRepository, IdentityClockPort, IdentityIdGeneratorPort,
    IdentityOperationContextFactoryPort, IdentityOutboxRepository, IdentityProjectionRepository,
    IdentityQueryMaterialDegradationMapper, IdentityReadVisibilityRepository,
    IdentityReconciliationReportRepository, IdentityReferenceStateRepository,
    IdentityTraceRecordRepository, IdentityTruthChangeSubjectMapper, IdentityUnitOfWorkManagerPort,
    MemoryReferenceRepository, RoleCapabilityRepository, TraceHandoffIntentRepository,
};
use crate::support::{
    IdentityOperationContext, IdentityOperationName, IdentityQueryMaterialDegradationSummary,
    IdentityReadDispositionKind, IdentityRepositoryCursor, IdentityRepositoryPage,
    IdentityRequestDigest, IdentityRequestMetadataRef, IdentityVisibilityDecision, Page,
};

/// Shared dependencies for read-only query flows.
pub struct IdentityQueryServiceDeps<'a> {
    /// Trusted clock used to time read decisions.
    pub clock: &'a dyn IdentityClockPort,
    /// Stable id generator for read-side decision refs and context ids.
    pub id_generator: &'a dyn IdentityIdGeneratorPort,
    /// Entry metadata to operation-context builder.
    pub operation_context_factory: &'a dyn IdentityOperationContextFactoryPort,
    /// Prepared visibility resolver for read requests.
    pub read_visibility_repository: &'a dyn IdentityReadVisibilityRepository,
    /// Stable projection lookup/read repository.
    pub projection_repository: &'a dyn IdentityProjectionRepository,
    /// Core truth repositories in scope for commit-05-b.
    pub member_repository: &'a dyn GlobalMemberRepository,
    pub lifecycle_repository: &'a dyn GlobalLifecycleRepository,
    pub role_capability_repository: &'a dyn RoleCapabilityRepository,
    pub career_record_repository: &'a dyn CareerRecordRepository,
    pub memory_reference_repository: &'a dyn MemoryReferenceRepository,
    pub trace_record_repository: &'a dyn IdentityTraceRecordRepository,
    pub audit_trail_repository: &'a dyn IdentityAuditTrailRepository,
    /// Operations read repositories in scope for commit-05-c.
    pub reference_state_repository: &'a dyn IdentityReferenceStateRepository,
    pub reconciliation_report_repository: &'a dyn IdentityReconciliationReportRepository,
    pub outbox_repository: &'a dyn IdentityOutboxRepository,
    pub handoff_intent_repository: &'a dyn TraceHandoffIntentRepository,
    pub truth_change_subject_mapper: &'a dyn IdentityTruthChangeSubjectMapper,
    /// Formal mapper for query material degradation after a valid access summary exists.
    pub degradation_mapper: &'a dyn IdentityQueryMaterialDegradationMapper,
    /// Queries are no-write; keep the write manager only for explicit test assertions via deps.
    pub unit_of_work_manager: &'a dyn IdentityUnitOfWorkManagerPort,
}

/// Shared query service skeleton for visibility-first read flows.
pub struct IdentityQueryService<'a> {
    deps: IdentityQueryServiceDeps<'a>,
}

impl<'a> IdentityQueryService<'a> {
    /// Creates a query service from formal shared dependencies.
    pub fn new(deps: IdentityQueryServiceDeps<'a>) -> Self {
        Self { deps }
    }

    /// Returns the shared query dependencies for boundary-local helpers and tests.
    pub fn deps(&self) -> &IdentityQueryServiceDeps<'a> {
        &self.deps
    }

    /// Shared helper that keeps the public query request envelope aligned with the context.
    pub fn assert_query_context<T>(
        request: &IdentityQueryRequest<T>,
        context: &IdentityOperationContext,
    ) -> Result<(), ApplicationError> {
        if context.channel != identity_contracts::refs::IdentityOperationChannel::Query {
            return Err(ApplicationError::invalid_request(
                "query context must use the query channel",
            ));
        }

        if context.operation_name.as_str() != request.query_name.as_str() {
            return Err(ApplicationError::invalid_request(format!(
                "operation name {} does not match query {}",
                context.operation_name.as_str(),
                request.query_name.as_str(),
            )));
        }

        if context.idempotency_key.is_some() {
            return Err(ApplicationError::invalid_request(
                "query context must not carry an idempotency key",
            ));
        }

        Ok(())
    }

    /// Builds a query operation context from entry-owned body-free metadata.
    pub fn build_query_context(
        &self,
        query_name: &IdentityQueryName,
        actor_ref: ActorRef,
        request_metadata_ref: IdentityRequestMetadataRef,
        request_digest: IdentityRequestDigest,
        trace_context_ref: Option<identity_contracts::refs::IdentityTraceContextRef>,
    ) -> Result<IdentityOperationContext, ApplicationError> {
        let context_ref = self
            .deps
            .id_generator
            .new_identity_operation_context_ref()?;
        let started_at = self.deps.clock.now()?;
        self.deps.operation_context_factory.from_query(
            IdentityOperationName::new(query_name.as_str()),
            actor_ref,
            request_metadata_ref,
            request_digest,
            trace_context_ref,
            context_ref,
            started_at,
        )
    }

    /// Resolves visibility-first member-summary access without constructing query DTO bodies.
    pub fn prepare_member_summary_read(
        &self,
        member_ref: GlobalMemberRef,
        consumer_ref: ConsumerRef,
        visibility_context_ref: VisibilityContextRef,
    ) -> Result<IdentityMemberSummaryPreflight, ApplicationError> {
        let access_summary = self
            .deps
            .read_visibility_repository
            .resolve_member_summary_read(
                member_ref.clone(),
                None,
                consumer_ref.clone(),
                visibility_context_ref.clone(),
            )?
            .ok_or_else(|| {
                ApplicationError::invalid_request(
                    "member summary read visibility could not form a canonical subject",
                )
            })?;

        let view_ref = self
            .deps
            .projection_repository
            .find_member_summary_view_ref(member_ref.clone(), access_summary.scope_ref.clone())?
            .ok_or_else(|| ApplicationError::not_found("member summary view not found"))?;

        let view_access_summary = self
            .deps
            .read_visibility_repository
            .resolve_member_summary_read(
                member_ref.clone(),
                Some(view_ref.clone()),
                consumer_ref,
                visibility_context_ref,
            )?
            .ok_or_else(|| {
                ApplicationError::invalid_request(
                    "member summary view read visibility could not form a canonical subject",
                )
            })?;

        let view = self
            .deps
            .projection_repository
            .get_member_summary_view(view_ref.clone())?
            .ok_or_else(|| ApplicationError::not_found("member summary view material not found"))?;

        if !view.belongs_to(&member_ref) {
            return Err(ApplicationError::consistency_defect(
                "member summary view does not belong to the requested member",
            ));
        }

        if !view.matches_visibility_scope(&access_summary.scope_ref) {
            return Err(ApplicationError::consistency_defect(
                "member summary view visibility scope does not match the stable lookup scope",
            ));
        }

        view.assert_body_free()
            .map_err(|err| ApplicationError::consistency_defect(err.message))?;

        Ok(IdentityMemberSummaryPreflight {
            access_summary,
            view_access_summary,
            view_ref,
            view,
        })
    }

    /// Classifies the stable disposition from the formal access summary and loaded view material.
    pub fn classify_member_summary_disposition(
        &self,
        access_summary: &IdentityVisibilityAccessSummary,
        view: &MemberSummaryView,
    ) -> IdentityReadDispositionKind {
        match access_summary.access_state {
            IdentityVisibilityAccessState::Visible => {
                if matches!(view.read_surface_kind, IdentityReadSurfaceKind::Stale) {
                    IdentityReadDispositionKind::StaleVisible
                } else if matches!(view.read_surface_kind, IdentityReadSurfaceKind::Degraded) {
                    IdentityReadDispositionKind::Degraded
                } else {
                    IdentityReadDispositionKind::Visible
                }
            }
            IdentityVisibilityAccessState::Redacted => {
                if view.is_stale_or_degraded() {
                    IdentityReadDispositionKind::StaleVisible
                } else {
                    IdentityReadDispositionKind::Redacted
                }
            }
            IdentityVisibilityAccessState::NotVisible => IdentityReadDispositionKind::NotVisible,
            IdentityVisibilityAccessState::Degraded
            | IdentityVisibilityAccessState::Unavailable => IdentityReadDispositionKind::Degraded,
        }
    }

    /// Commit-05-b flow: read a member anchor slice from truth and optional summary projection.
    pub fn get_global_member_anchor(
        &self,
        request: IdentityQueryRequest<GetGlobalMemberAnchorRequest>,
        context: IdentityOperationContext,
    ) -> Result<IdentityQueryResponse<GlobalMemberAnchorView>, ApplicationError> {
        Self::assert_query_context(&request, &context)?;
        let access = self.resolve_member_access(
            request.body.member_ref.clone(),
            request.body.consumer_ref,
            request.metadata.visibility_context_ref.clone(),
        )?;

        match self.surface_for_access(&access, None, false)? {
            AccessSurfaceOutcome::Visible => {}
            AccessSurfaceOutcome::Return(surface) => {
                return Ok(IdentityQueryResponse {
                    query_name: request.query_name,
                    surface,
                    body: None,
                });
            }
        }

        let member = match self
            .deps
            .member_repository
            .get_member_with_version(request.body.member_ref.clone())?
        {
            Some(member) => member.value,
            None => {
                return Ok(IdentityQueryResponse {
                    query_name: request.query_name,
                    surface: self.missing_surface(&access)?,
                    body: None,
                });
            }
        };

        let projection_slice =
            self.load_optional_member_summary_slice(&access, request.body.member_ref.clone())?;
        if let Some(surface) = projection_slice.surface {
            return Ok(IdentityQueryResponse {
                query_name: request.query_name,
                surface,
                body: None,
            });
        }

        let body = GlobalMemberAnchorView {
            member_ref: member.member_ref.clone(),
            anchor_state_kind: map_anchor_state_kind(&member.anchor_state),
            anchor_reason_ref: member.anchor_state.reason_ref.clone(),
            anchor_changed_at: member.anchor_state.changed_at,
            source_ref: maybe_hide_on_redaction(&access, Some(member.source_ref.clone())),
            member_summary_view_ref: projection_slice.view_ref.clone(),
            anchor_slice_ref: projection_slice
                .view
                .as_ref()
                .map(|view| view.anchor_slice_ref.clone()),
        };

        let surface = self.surface_for_truth_query(
            &access,
            projection_slice.view.as_ref(),
            body.member_summary_view_ref.is_some(),
        )?;

        Ok(IdentityQueryResponse {
            query_name: request.query_name,
            surface,
            body: Some(body),
        })
    }

    /// Commit-05-b flow: read a member lifecycle slice from truth and optional summary projection.
    pub fn get_global_lifecycle_summary(
        &self,
        request: IdentityQueryRequest<GetGlobalLifecycleSummaryRequest>,
        context: IdentityOperationContext,
    ) -> Result<IdentityQueryResponse<GlobalLifecycleSummaryView>, ApplicationError> {
        Self::assert_query_context(&request, &context)?;
        let access = self.resolve_member_access(
            request.body.member_ref.clone(),
            request.body.consumer_ref,
            request.metadata.visibility_context_ref.clone(),
        )?;

        match self.surface_for_access(&access, None, false)? {
            AccessSurfaceOutcome::Visible => {}
            AccessSurfaceOutcome::Return(surface) => {
                return Ok(IdentityQueryResponse {
                    query_name: request.query_name,
                    surface,
                    body: None,
                });
            }
        }

        if self
            .deps
            .member_repository
            .get_member_with_version(request.body.member_ref.clone())?
            .is_none()
        {
            return Ok(IdentityQueryResponse {
                query_name: request.query_name,
                surface: self.missing_surface(&access)?,
                body: None,
            });
        }

        let lifecycle = match self
            .deps
            .lifecycle_repository
            .get_lifecycle_with_version(request.body.member_ref.clone())?
        {
            Some(state) => state.value,
            None => {
                return Ok(IdentityQueryResponse {
                    query_name: request.query_name,
                    surface: self.missing_surface(&access)?,
                    body: None,
                });
            }
        };

        let projection_slice =
            self.load_optional_member_summary_slice(&access, request.body.member_ref.clone())?;
        if let Some(surface) = projection_slice.surface {
            return Ok(IdentityQueryResponse {
                query_name: request.query_name,
                surface,
                body: None,
            });
        }

        let body = GlobalLifecycleSummaryView {
            member_ref: request.body.member_ref.clone(),
            lifecycle_state_kind: map_lifecycle_state_kind(lifecycle.state_kind),
            reason_ref: maybe_hide_on_redaction(&access, Some(lifecycle.reason_ref.clone())),
            basis_ref: maybe_hide_on_redaction(&access, lifecycle.basis_ref.clone()),
            changed_by_ref: maybe_hide_on_redaction(
                &access,
                Some(lifecycle.changed_by_ref.clone()),
            ),
            changed_at: lifecycle.changed_at,
            member_summary_view_ref: projection_slice.view_ref.clone(),
            lifecycle_slice_ref: projection_slice
                .view
                .as_ref()
                .map(|view| view.lifecycle_slice_ref.clone()),
        };

        let surface = self.surface_for_truth_query(
            &access,
            projection_slice.view.as_ref(),
            body.member_summary_view_ref.is_some(),
        )?;

        Ok(IdentityQueryResponse {
            query_name: request.query_name,
            surface,
            body: Some(body),
        })
    }

    /// Commit-05-b flow: read a role/capability summary and optional summary projection slices.
    pub fn get_role_capability_summary(
        &self,
        request: IdentityQueryRequest<GetRoleCapabilitySummaryRequest>,
        context: IdentityOperationContext,
    ) -> Result<IdentityQueryResponse<RoleCapabilitySummaryView>, ApplicationError> {
        Self::assert_query_context(&request, &context)?;
        let access = self.resolve_member_access(
            request.body.member_ref.clone(),
            request.body.consumer_ref,
            request.metadata.visibility_context_ref.clone(),
        )?;

        match self.surface_for_access(&access, None, false)? {
            AccessSurfaceOutcome::Visible => {}
            AccessSurfaceOutcome::Return(surface) => {
                return Ok(IdentityQueryResponse {
                    query_name: request.query_name,
                    surface,
                    body: None,
                });
            }
        }

        if self
            .deps
            .member_repository
            .get_member_with_version(request.body.member_ref.clone())?
            .is_none()
        {
            return Ok(IdentityQueryResponse {
                query_name: request.query_name,
                surface: self.missing_surface(&access)?,
                body: None,
            });
        }

        let summary = match request.body.summary_ref.clone() {
            Some(summary_ref) => self
                .deps
                .role_capability_repository
                .get_summary_with_version(summary_ref)?
                .map(|value| value.value),
            None => self
                .deps
                .role_capability_repository
                .find_current_summary_by_member(request.body.member_ref.clone())?
                .map(|value| value.value),
        };

        let summary = match summary {
            Some(summary) => summary,
            None => {
                return Ok(IdentityQueryResponse {
                    query_name: request.query_name,
                    surface: self.missing_surface(&access)?,
                    body: None,
                });
            }
        };

        if !summary.belongs_to(request.body.member_ref.clone()) {
            let degradation = self.deps.degradation_mapper.forbidden_read_material(
                access.clone(),
                IdentityReadMaterialMarker::new(IdentityReadMaterialKind::SafeSummaryRefs, None),
            );
            return Ok(IdentityQueryResponse {
                query_name: request.query_name,
                surface: self.degraded_surface_from_material(degradation)?,
                body: None,
            });
        }

        let snapshot = self
            .deps
            .role_capability_repository
            .get_source_snapshot_with_version(summary.source_snapshot_ref.clone())?
            .map(|value| value.value);

        let snapshot = match snapshot {
            Some(snapshot) => snapshot,
            None => {
                let degradation = self.deps.degradation_mapper.forbidden_read_material(
                    access.clone(),
                    IdentityReadMaterialMarker::new(
                        IdentityReadMaterialKind::SafeSummaryRefs,
                        None,
                    ),
                );
                return Ok(IdentityQueryResponse {
                    query_name: request.query_name,
                    surface: self.degraded_surface_from_material(degradation)?,
                    body: None,
                });
            }
        };

        let projection_slice =
            self.load_optional_member_summary_slice(&access, request.body.member_ref.clone())?;
        if let Some(surface) = projection_slice.surface {
            return Ok(IdentityQueryResponse {
                query_name: request.query_name,
                surface,
                body: None,
            });
        }

        let body = RoleCapabilitySummaryView {
            member_ref: summary.member_ref.clone(),
            summary_ref: summary.summary_ref.clone(),
            summary_state_kind: map_role_summary_state_kind(summary.summary_state),
            source_snapshot_ref: summary.source_snapshot_ref.clone(),
            source_state_kind: Some(map_role_source_state_kind(snapshot.source_state)),
            role_source_ref: maybe_hide_on_redaction(&access, summary.role_source_ref.clone()),
            capability_source_refs: maybe_hide_vec_on_redaction(
                &access,
                summary.capability_source_refs.clone(),
            ),
            evidence_refs: maybe_hide_vec_on_redaction(&access, summary.evidence_refs.clone()),
            safe_summary_ref: maybe_hide_on_redaction(
                &access,
                Some(summary.safe_summary_ref.clone()),
            ),
            member_summary_view_ref: projection_slice.view_ref.clone(),
            role_capability_slice_refs: projection_slice
                .view
                .as_ref()
                .map(|view| view.role_capability_slice_refs.clone())
                .unwrap_or_default(),
        };

        let surface = if role_snapshot_is_stale_or_unavailable(&snapshot)
            || matches!(
                summary.summary_state,
                RoleCapabilitySummaryStateKind::Stale
                    | RoleCapabilitySummaryStateKind::Unavailable
                    | RoleCapabilitySummaryStateKind::PendingReconciliation
            ) {
            self.degraded_surface_from_access(
                &access,
                IdentityReadSurfaceKind::Stale,
                if matches!(
                    snapshot.source_state,
                    RoleCapabilitySourceStateKind::SourceUnavailable
                ) || matches!(
                    summary.summary_state,
                    RoleCapabilitySummaryStateKind::Unavailable
                ) {
                    IdentityDegradedKind::SourceUnavailable
                } else {
                    IdentityDegradedKind::ProjectionStale
                },
            )?
        } else {
            self.surface_for_truth_query(
                &access,
                projection_slice.view.as_ref(),
                body.member_summary_view_ref.is_some(),
            )?
        };

        Ok(IdentityQueryResponse {
            query_name: request.query_name,
            surface,
            body: Some(body),
        })
    }

    /// Commit-05-b flow: list career records with visibility-first no-write discipline.
    pub fn list_career_records(
        &self,
        request: IdentityQueryRequest<ListCareerRecordsRequest>,
        context: IdentityOperationContext,
    ) -> Result<IdentityPageResponse<CareerRecordView>, ApplicationError> {
        Self::assert_query_context(&request, &context)?;
        let page = require_page(&request.page)?;
        let access = self.resolve_member_access(
            request.body.member_ref.clone(),
            request.body.consumer_ref,
            request.metadata.visibility_context_ref,
        )?;

        match self.surface_for_access(&access, None, false)? {
            AccessSurfaceOutcome::Visible => {}
            AccessSurfaceOutcome::Return(surface) => {
                return Ok(empty_page_response(request.query_name, surface));
            }
        }

        if self
            .deps
            .member_repository
            .get_member_with_version(request.body.member_ref.clone())?
            .is_none()
        {
            return Ok(empty_page_response(
                request.query_name,
                self.missing_surface(&access)?,
            ));
        }

        let listed = self
            .deps
            .career_record_repository
            .list_records_by_member(request.body.member_ref.clone(), page)?;

        if listed.items.is_empty() {
            return Ok(empty_page_response(
                request.query_name,
                self.empty_surface(&access)?,
            ));
        }

        let mut items = Vec::new();
        let mut degraded_surface = None;
        for item in &listed.items {
            let loaded = match self
                .deps
                .career_record_repository
                .get_career_record(item.value_ref.clone())?
            {
                Some(record) => record.value,
                None => {
                    let degradation = self
                        .deps
                        .degradation_mapper
                        .career_record_item_missing_after_list(
                            access.clone(),
                            item.value_ref.clone(),
                            request.body.member_ref.clone(),
                        );
                    degraded_surface = Some(self.degraded_surface_from_material(degradation)?);
                    break;
                }
            };

            if loaded.member_ref != request.body.member_ref {
                let degradation = self
                    .deps
                    .degradation_mapper
                    .career_record_item_invalid_member(
                        access.clone(),
                        item.value_ref.clone(),
                        request.body.member_ref.clone(),
                    );
                degraded_surface = Some(self.degraded_surface_from_material(degradation)?);
                break;
            }

            items.push(self.career_record_view(&access, loaded));
        }

        if let Some(surface) = degraded_surface {
            return Ok(IdentityPageResponse {
                query_name: request.query_name,
                surface,
                page_info: page_info(&listed),
                items,
            });
        }

        Ok(IdentityPageResponse {
            query_name: request.query_name,
            surface: self.surface_for_list_success(&access, listed.items.is_empty(), false)?,
            page_info: page_info(&listed),
            items,
        })
    }

    /// Commit-05-b flow: list memory references with visibility-first no-write discipline.
    pub fn list_memory_references(
        &self,
        request: IdentityQueryRequest<ListMemoryReferencesRequest>,
        context: IdentityOperationContext,
    ) -> Result<IdentityPageResponse<MemoryReferenceView>, ApplicationError> {
        Self::assert_query_context(&request, &context)?;
        let page = require_page(&request.page)?;
        let access = self.resolve_member_access(
            request.body.member_ref.clone(),
            request.body.consumer_ref,
            request.metadata.visibility_context_ref,
        )?;

        match self.surface_for_access(&access, None, false)? {
            AccessSurfaceOutcome::Visible => {}
            AccessSurfaceOutcome::Return(surface) => {
                return Ok(empty_page_response(request.query_name, surface));
            }
        }

        if self
            .deps
            .member_repository
            .get_member_with_version(request.body.member_ref.clone())?
            .is_none()
        {
            return Ok(empty_page_response(
                request.query_name,
                self.missing_surface(&access)?,
            ));
        }

        let listed = self
            .deps
            .memory_reference_repository
            .list_references_by_member(request.body.member_ref.clone(), page)?;

        if listed.items.is_empty() {
            return Ok(empty_page_response(
                request.query_name,
                self.empty_surface(&access)?,
            ));
        }

        let mut items = Vec::new();
        let mut degraded_surface = None;
        for item in &listed.items {
            let loaded = match self
                .deps
                .memory_reference_repository
                .get_memory_reference_with_version(item.value_ref.clone())?
            {
                Some(reference) => reference.value,
                None => {
                    let degradation = self
                        .deps
                        .degradation_mapper
                        .memory_reference_item_missing_after_list(
                            access.clone(),
                            item.value_ref.clone(),
                            request.body.member_ref.clone(),
                        );
                    degraded_surface = Some(self.degraded_surface_from_material(degradation)?);
                    break;
                }
            };

            if !loaded.belongs_to(&request.body.member_ref) {
                let degradation = self
                    .deps
                    .degradation_mapper
                    .memory_reference_item_invalid_member(
                        access.clone(),
                        item.value_ref.clone(),
                        request.body.member_ref.clone(),
                    );
                degraded_surface = Some(self.degraded_surface_from_material(degradation)?);
                break;
            }

            if loaded.has_external_body() {
                let degradation = self.deps.degradation_mapper.forbidden_read_material(
                    access.clone(),
                    IdentityReadMaterialMarker::new(
                        IdentityReadMaterialKind::ForbiddenExternalBody,
                        None,
                    ),
                );
                degraded_surface = Some(self.degraded_surface_from_material(degradation)?);
                break;
            }

            items.push(self.memory_reference_view(&access, loaded));
        }

        if let Some(surface) = degraded_surface {
            return Ok(IdentityPageResponse {
                query_name: request.query_name,
                surface,
                page_info: page_info(&listed),
                items,
            });
        }

        Ok(IdentityPageResponse {
            query_name: request.query_name,
            surface: self.surface_for_list_success(&access, listed.items.is_empty(), false)?,
            page_info: page_info(&listed),
            items,
        })
    }

    /// Commit-05-b flow: read one stable member summary view without touching projection state.
    pub fn read_member_summary(
        &self,
        request: IdentityQueryRequest<ReadMemberSummaryRequest>,
        context: IdentityOperationContext,
    ) -> Result<IdentityQueryResponse<MemberSummaryView>, ApplicationError> {
        Self::assert_query_context(&request, &context)?;
        let access = self
            .deps
            .read_visibility_repository
            .resolve_member_summary_read(
                request.body.member_ref.clone(),
                None,
                request.body.consumer_ref.clone(),
                request.metadata.visibility_context_ref.clone(),
            )?
            .ok_or_else(|| {
                ApplicationError::invalid_request(
                    "member summary read visibility could not form a canonical subject",
                )
            })?;

        match self.surface_for_access(&access, None, false)? {
            AccessSurfaceOutcome::Visible => {}
            AccessSurfaceOutcome::Return(surface) => {
                return Ok(IdentityQueryResponse {
                    query_name: request.query_name,
                    surface,
                    body: None,
                });
            }
        }

        let view_ref = match self
            .deps
            .projection_repository
            .find_member_summary_view_ref(
                request.body.member_ref.clone(),
                access.scope_ref.clone(),
            )? {
            Some(view_ref) => view_ref,
            None => {
                return Ok(IdentityQueryResponse {
                    query_name: request.query_name,
                    surface: self.missing_surface(&access)?,
                    body: None,
                });
            }
        };

        let view_access = self
            .deps
            .read_visibility_repository
            .resolve_member_summary_read(
                request.body.member_ref.clone(),
                Some(view_ref.clone()),
                request.body.consumer_ref,
                request.metadata.visibility_context_ref,
            )?
            .ok_or_else(|| {
                ApplicationError::invalid_request(
                    "member summary view read visibility could not form a canonical subject",
                )
            })?;

        match self.surface_for_access(&view_access, None, false)? {
            AccessSurfaceOutcome::Visible => {}
            AccessSurfaceOutcome::Return(surface) => {
                return Ok(IdentityQueryResponse {
                    query_name: request.query_name,
                    surface,
                    body: None,
                });
            }
        }

        let view = match self
            .deps
            .projection_repository
            .get_member_summary_view(view_ref.clone())?
        {
            Some(view) => view,
            None => {
                let degradation = self.deps.degradation_mapper.member_summary_view_missing(
                    access.clone(),
                    request.body.member_ref.clone(),
                    access.scope_ref.clone(),
                );
                return Ok(IdentityQueryResponse {
                    query_name: request.query_name,
                    surface: self.degraded_surface_from_material(degradation)?,
                    body: None,
                });
            }
        };

        if !view.belongs_to(&request.body.member_ref) {
            let degradation = self
                .deps
                .degradation_mapper
                .member_summary_view_invalid_owner(
                    access.clone(),
                    view_ref,
                    request.body.member_ref.clone(),
                );
            return Ok(IdentityQueryResponse {
                query_name: request.query_name,
                surface: self.degraded_surface_from_material(degradation)?,
                body: None,
            });
        }

        if !view.matches_visibility_scope(&access.scope_ref) {
            let degradation = self
                .deps
                .degradation_mapper
                .member_summary_view_scope_mismatch(
                    access.clone(),
                    view.view_ref.clone(),
                    access.scope_ref.clone(),
                );
            return Ok(IdentityQueryResponse {
                query_name: request.query_name,
                surface: self.degraded_surface_from_material(degradation)?,
                body: None,
            });
        }

        if view.assert_body_free().is_err() {
            let degradation = self
                .deps
                .degradation_mapper
                .forbidden_read_material(access.clone(), view.read_material_marker.clone());
            return Ok(IdentityQueryResponse {
                query_name: request.query_name,
                surface: self.degraded_surface_from_material(degradation)?,
                body: None,
            });
        }

        let disposition = self.classify_member_summary_disposition(&view_access, &view);
        if matches!(disposition, IdentityReadDispositionKind::StaleVisible)
            && view.projection_freshness_ref.is_none()
        {
            let degradation = self
                .deps
                .degradation_mapper
                .member_summary_view_missing_freshness(
                    access.clone(),
                    view.view_ref.clone(),
                    request.body.member_ref.clone(),
                    access.scope_ref.clone(),
                );
            return Ok(IdentityQueryResponse {
                query_name: request.query_name,
                surface: self.degraded_surface_from_material(degradation)?,
                body: None,
            });
        }

        let surface = self.surface_from_access(
            &view_access,
            disposition,
            view.projection_freshness_ref.clone(),
            true,
        )?;

        Ok(IdentityQueryResponse {
            query_name: request.query_name,
            surface,
            body: Some(view),
        })
    }

    /// Commit-05-b flow: read append-only trace material with per-item visibility.
    pub fn read_identity_trace(
        &self,
        request: IdentityQueryRequest<ReadIdentityTraceRequest>,
        context: IdentityOperationContext,
    ) -> Result<IdentityPageResponse<IdentityTraceRecordView>, ApplicationError> {
        Self::assert_query_context(&request, &context)?;
        let page = require_page(&request.page)?;

        let (member_ref, selector_access, listed) = match &request.body.selector {
            IdentityTraceReadSelector::ByMember { member_ref } => {
                let access = self
                    .deps
                    .read_visibility_repository
                    .resolve_trace_member_page_read(
                        member_ref.clone(),
                        None,
                        request.body.consumer_ref.clone(),
                        request.metadata.visibility_context_ref.clone(),
                    )?
                    .ok_or_else(|| {
                        ApplicationError::invalid_request(
                            "trace page visibility could not form a canonical member page subject",
                        )
                    })?;

                match self.surface_for_access(&access, None, false)? {
                    AccessSurfaceOutcome::Visible => {}
                    AccessSurfaceOutcome::Return(surface) => {
                        return Ok(empty_page_response(request.query_name, surface));
                    }
                }

                (
                    member_ref.clone(),
                    access,
                    self.deps
                        .trace_record_repository
                        .list_trace_records_by_member(member_ref.clone(), page)?,
                )
            }
            IdentityTraceReadSelector::BySubject {
                member_ref,
                subject_ref,
                after_cursor_ref,
            } => {
                let access = self
                    .deps
                    .read_visibility_repository
                    .resolve_trace_read(
                        subject_ref.clone(),
                        request.body.consumer_ref.clone(),
                        request.metadata.visibility_context_ref.clone(),
                    )?
                    .ok_or_else(|| {
                        ApplicationError::invalid_request(
                            "trace read visibility could not form a canonical subject",
                        )
                    })?;

                match self.surface_for_access(&access, None, false)? {
                    AccessSurfaceOutcome::Visible => {}
                    AccessSurfaceOutcome::Return(surface) => {
                        return Ok(empty_page_response(request.query_name, surface));
                    }
                }

                (
                    member_ref.clone(),
                    access,
                    self.deps
                        .trace_record_repository
                        .list_trace_records_after_cursor(
                            subject_ref.clone(),
                            after_cursor_ref.clone(),
                            page,
                        )?,
                )
            }
            IdentityTraceReadSelector::ByMemberAndChangeKind {
                member_ref,
                change_kind_ref,
            } => {
                let access = self
                    .deps
                    .read_visibility_repository
                    .resolve_trace_member_page_read(
                        member_ref.clone(),
                        Some(change_kind_ref.clone()),
                        request.body.consumer_ref.clone(),
                        request.metadata.visibility_context_ref.clone(),
                    )?
                    .ok_or_else(|| {
                        ApplicationError::invalid_request(
                            "trace page visibility could not form a canonical member page subject",
                        )
                    })?;

                match self.surface_for_access(&access, None, false)? {
                    AccessSurfaceOutcome::Visible => {}
                    AccessSurfaceOutcome::Return(surface) => {
                        return Ok(empty_page_response(request.query_name, surface));
                    }
                }

                (
                    member_ref.clone(),
                    access,
                    self.deps
                        .trace_record_repository
                        .list_trace_records_by_change_kind(
                            member_ref.clone(),
                            change_kind_ref.clone(),
                            page,
                        )?,
                )
            }
        };

        if listed.items.is_empty() {
            return Ok(empty_page_response(
                request.query_name,
                self.empty_surface(&selector_access)?,
            ));
        }

        let mut items = Vec::new();
        let mut denied_count = 0usize;
        let mut degraded_surface = None;
        let mut partial_redaction_access = None;
        let mut denied_access = None;
        let mut last_resolved_item_access = None;
        for item in &listed.items {
            let loaded = match self
                .deps
                .trace_record_repository
                .get_trace_record(item.value_ref.clone())?
            {
                Some(trace) => trace.value,
                None => {
                    let access = last_resolved_item_access
                        .clone()
                        .unwrap_or_else(|| selector_access.clone());
                    let degradation = self
                        .deps
                        .degradation_mapper
                        .trace_item_missing_after_list(access, item.value_ref.clone());
                    degraded_surface = Some(self.degraded_surface_from_material(degradation)?);
                    break;
                }
            };

            if !loaded.belongs_to(&member_ref) {
                let degradation = self.deps.degradation_mapper.trace_item_invalid_member(
                    selector_access.clone(),
                    item.value_ref.clone(),
                    member_ref.clone(),
                );
                degraded_surface = Some(self.degraded_surface_from_material(degradation)?);
                break;
            }

            if let IdentityTraceReadSelector::BySubject { subject_ref, .. } = &request.body.selector
            {
                if !loaded.matches_subject(subject_ref) {
                    let degradation = self.deps.degradation_mapper.trace_item_subject_mismatch(
                        selector_access.clone(),
                        item.value_ref.clone(),
                        subject_ref.clone(),
                    );
                    degraded_surface = Some(self.degraded_surface_from_material(degradation)?);
                    break;
                }
            }

            let access = self
                .deps
                .read_visibility_repository
                .resolve_trace_read(
                    loaded.subject_ref.clone(),
                    request.body.consumer_ref.clone(),
                    request.metadata.visibility_context_ref.clone(),
                )?
                .ok_or_else(|| {
                    ApplicationError::invalid_request(
                        "trace item visibility could not form a canonical subject",
                    )
                })?;
            last_resolved_item_access = Some(access.clone());

            match self.surface_for_access(&access, None, true)? {
                AccessSurfaceOutcome::Visible => {
                    if matches!(access.access_state, IdentityVisibilityAccessState::Redacted) {
                        partial_redaction_access.get_or_insert_with(|| access.clone());
                    }
                }
                AccessSurfaceOutcome::Return(surface) => match access.access_state {
                    IdentityVisibilityAccessState::NotVisible => {
                        denied_count += 1;
                        denied_access.get_or_insert_with(|| access.clone());
                        if access.redaction_marker_ref.is_some() {
                            partial_redaction_access.get_or_insert_with(|| access.clone());
                        }
                        continue;
                    }
                    _ => {
                        degraded_surface = Some(surface);
                        break;
                    }
                },
            }

            if loaded.assert_body_free().is_err() {
                let degradation = self
                    .deps
                    .degradation_mapper
                    .forbidden_read_material(access, loaded.read_material_marker.clone());
                degraded_surface = Some(self.degraded_surface_from_material(degradation)?);
                break;
            }

            items.push(self.trace_record_view(&access, loaded));
        }

        if let Some(surface) = degraded_surface {
            return Ok(IdentityPageResponse {
                query_name: request.query_name,
                surface,
                page_info: page_info(&listed),
                items,
            });
        }

        let surface = if items.is_empty() && denied_count > 0 {
            self.not_visible_surface(denied_access.as_ref().unwrap_or(&selector_access))?
        } else if denied_count > 0 || partial_redaction_access.is_some() {
            self.redacted_surface(
                partial_redaction_access
                    .as_ref()
                    .unwrap_or(&selector_access),
                true,
            )?
        } else {
            self.surface_for_list_success(&selector_access, false, false)?
        };

        Ok(IdentityPageResponse {
            query_name: request.query_name,
            surface,
            page_info: page_info(&listed),
            items,
        })
    }

    /// Commit-05-b flow: read the canonical member audit trail without creating trail material.
    pub fn read_audit_trail(
        &self,
        request: IdentityQueryRequest<ReadAuditTrailRequest>,
        context: IdentityOperationContext,
    ) -> Result<IdentityPageResponse<AuditTrailEntryView>, ApplicationError> {
        Self::assert_query_context(&request, &context)?;
        let page = require_page(&request.page)?;
        let subjects = self
            .deps
            .truth_change_subject_mapper
            .member_subjects(request.body.member_ref.clone());
        let access = self
            .deps
            .read_visibility_repository
            .resolve_audit_read(
                subjects.audit_subject_ref.clone(),
                request.body.audit_scope_ref.clone(),
                request.body.consumer_ref,
                request.metadata.visibility_context_ref,
            )?
            .ok_or_else(|| {
                ApplicationError::invalid_request(
                    "audit read visibility could not form a canonical subject",
                )
            })?;

        match self.surface_for_access(&access, None, false)? {
            AccessSurfaceOutcome::Visible => {}
            AccessSurfaceOutcome::Return(surface) => {
                return Ok(empty_page_response(request.query_name, surface));
            }
        }

        let trail = match self
            .deps
            .audit_trail_repository
            .find_audit_trail_by_subject(subjects.audit_subject_ref.clone())?
        {
            Some(trail) => trail.value,
            None => {
                return Ok(empty_page_response(
                    request.query_name,
                    self.empty_surface(&access)?,
                ));
            }
        };

        if trail.audit_subject_ref != subjects.audit_subject_ref
            || trail
                .member_ref
                .as_ref()
                .is_some_and(|member| member != &request.body.member_ref)
        {
            let degradation = self.deps.degradation_mapper.audit_item_missing_or_invalid(
                access.clone(),
                trail.audit_trail_ref.clone(),
                request.body.audit_scope_ref.clone(),
            );
            return Ok(empty_page_response(
                request.query_name,
                self.degraded_surface_from_material(degradation)?,
            ));
        }

        let entries = self.deps.audit_trail_repository.list_audit_entries(
            trail.audit_trail_ref.clone(),
            request.body.audit_scope_ref.clone(),
            request.body.audit_cursor_ref.clone(),
            page,
        )?;

        if entries.items.is_empty() {
            return Ok(empty_page_response(
                request.query_name,
                self.empty_surface(&access)?,
            ));
        }

        let items = entries
            .items
            .iter()
            .map(|entry| AuditTrailEntryView {
                audit_trail_ref: trail.audit_trail_ref.clone(),
                audit_subject_ref: trail.audit_subject_ref.clone(),
                audit_scope_ref: request.body.audit_scope_ref.clone(),
                member_ref: Some(request.body.member_ref.clone()),
                trace_record_ref: entry.trace_record_ref.clone(),
                change_kind_ref: entry.change_kind_ref.clone(),
                visibility_result_ref: entry.visibility_result_ref.clone(),
                occurred_at: entry.occurred_at,
            })
            .collect();

        Ok(IdentityPageResponse {
            query_name: request.query_name,
            surface: self.surface_for_list_success(&access, false, false)?,
            page_info: page_info(&entries),
            items,
        })
    }

    /// Commit-05-c flow: read one projection state without rebuild side effects.
    pub fn get_projection_state(
        &self,
        request: IdentityQueryRequest<GetProjectionStateRequest>,
        context: IdentityOperationContext,
    ) -> Result<IdentityQueryResponse<ProjectionStateView>, ApplicationError> {
        Self::assert_query_context(&request, &context)?;
        let access = self
            .deps
            .read_visibility_repository
            .resolve_projection_state_read(
                request.body.projection_ref.clone(),
                request.body.projection_state_ref.clone(),
                request.body.consumer_ref.clone(),
                request.metadata.visibility_context_ref.clone(),
            )?
            .ok_or_else(|| {
                ApplicationError::invalid_request(
                    "projection state read visibility could not form a canonical subject",
                )
            })?;

        match self.surface_for_access(&access, None, false)? {
            AccessSurfaceOutcome::Visible => {}
            AccessSurfaceOutcome::Return(surface) => {
                return Ok(IdentityQueryResponse {
                    query_name: request.query_name,
                    surface,
                    body: None,
                });
            }
        }

        let Some(loaded) = self
            .deps
            .projection_repository
            .get_projection_state_with_version(request.body.projection_ref.clone())?
            .map(|value| value.value)
        else {
            return Ok(IdentityQueryResponse {
                query_name: request.query_name,
                surface: self.missing_surface(&access)?,
                body: None,
            });
        };

        if let Some(requested_state_ref) = request.body.projection_state_ref.clone() {
            if requested_state_ref != loaded.projection_state_ref {
                let degradation = self.deps.degradation_mapper.projection_state_ref_mismatch(
                    access.clone(),
                    request.body.projection_ref,
                    requested_state_ref,
                    loaded.projection_state_ref.clone(),
                );
                return Ok(IdentityQueryResponse {
                    query_name: request.query_name,
                    surface: self.degraded_surface_from_material(degradation)?,
                    body: None,
                });
            }
        }

        let final_access = self
            .deps
            .read_visibility_repository
            .resolve_projection_state_read(
                loaded.projection_ref.clone(),
                Some(loaded.projection_state_ref.clone()),
                request.body.consumer_ref,
                request.metadata.visibility_context_ref,
            )?
            .ok_or_else(|| {
                ApplicationError::invalid_request(
                    "projection state read visibility could not form a canonical loaded subject",
                )
            })?;

        match self.surface_for_access(&final_access, None, false)? {
            AccessSurfaceOutcome::Visible => {}
            AccessSurfaceOutcome::Return(surface) => {
                return Ok(IdentityQueryResponse {
                    query_name: request.query_name,
                    surface,
                    body: None,
                });
            }
        }

        let body = self.projection_state_view(&final_access, &loaded);
        let (surface, body) = match loaded.state_kind {
            ProjectionStateKind::Fresh | ProjectionStateKind::Rebuilt => (
                self.visible_surface_for_operations(&final_access, true)?,
                Some(body),
            ),
            ProjectionStateKind::Stale => (
                self.surface_from_access(
                    &final_access,
                    IdentityReadDispositionKind::StaleVisible,
                    Some(self.projection_freshness_marker(&loaded)),
                    true,
                )?,
                Some(body),
            ),
            ProjectionStateKind::RebuildPending => (self.rebuilding_surface(&final_access)?, None),
            ProjectionStateKind::Degraded | ProjectionStateKind::RebuildFailed => (
                self.degraded_operations_surface(&final_access, true)?,
                Some(body),
            ),
        };

        Ok(IdentityQueryResponse {
            query_name: request.query_name,
            surface,
            body,
        })
    }

    /// Commit-05-c flow: read one stored external reference bundle state without refresh.
    pub fn get_reference_resolution_state(
        &self,
        request: IdentityQueryRequest<GetReferenceResolutionStateRequest>,
        context: IdentityOperationContext,
    ) -> Result<IdentityQueryResponse<ReferenceResolutionStateView>, ApplicationError> {
        Self::assert_query_context(&request, &context)?;
        let access = self
            .deps
            .read_visibility_repository
            .resolve_reference_state_read(
                request.body.external_reference_ref.clone(),
                request.body.owner_ref.clone(),
                request.body.consumer_ref,
                request.metadata.visibility_context_ref,
            )?
            .ok_or_else(|| {
                ApplicationError::invalid_request(
                    "reference state read visibility could not form a canonical subject",
                )
            })?;

        match self.surface_for_access(&access, None, false)? {
            AccessSurfaceOutcome::Visible => {}
            AccessSurfaceOutcome::Return(surface) => {
                return Ok(IdentityQueryResponse {
                    query_name: request.query_name,
                    surface,
                    body: None,
                });
            }
        }

        let Some(loaded) = self
            .deps
            .reference_state_repository
            .get_reference_state_with_version(request.body.external_reference_ref.clone())?
            .map(|value| value.value)
        else {
            return Ok(IdentityQueryResponse {
                query_name: request.query_name,
                surface: self.missing_surface(&access)?,
                body: None,
            });
        };

        if loaded.external_reference_ref != request.body.external_reference_ref {
            return Err(ApplicationError::consistency_defect(
                "loaded reference state does not match the requested external reference",
            ));
        }

        if let Some(expected_owner_ref) = request.body.owner_ref {
            if expected_owner_ref != loaded.reference_owner_ref {
                let degradation = self.deps.degradation_mapper.reference_state_owner_mismatch(
                    access.clone(),
                    loaded.external_reference_ref.clone(),
                    expected_owner_ref,
                    loaded.reference_owner_ref.clone(),
                );
                return Ok(IdentityQueryResponse {
                    query_name: request.query_name,
                    surface: self.degraded_surface_from_material(degradation)?,
                    body: None,
                });
            }
        }

        let sidecar_refs = match self
            .deps
            .reference_state_repository
            .get_typed_sidecar_refs(loaded.external_reference_ref.clone())
        {
            Ok(sidecar_refs) => sidecar_refs,
            Err(error) if error.kind == ApplicationErrorKind::DependencyUnavailable => {
                let degradation = self.deps.degradation_mapper.reference_sidecar_degraded(
                    access.clone(),
                    loaded.external_reference_ref.clone(),
                    Some(loaded.resolution_state_ref.clone()),
                );
                return Ok(IdentityQueryResponse {
                    query_name: request.query_name,
                    surface: self.degraded_surface_from_material(degradation)?,
                    body: None,
                });
            }
            Err(error) => return Err(error),
        };

        let body = self.reference_resolution_state_view(&access, &loaded, sidecar_refs);
        let surface = match loaded.state_kind {
            ReferenceResolutionStateKind::Resolved => {
                self.visible_surface_for_operations(&access, true)?
            }
            ReferenceResolutionStateKind::Stale => self.surface_from_access(
                &access,
                IdentityReadDispositionKind::StaleVisible,
                None,
                true,
            )?,
            ReferenceResolutionStateKind::Unavailable
            | ReferenceResolutionStateKind::Unrecognized
            | ReferenceResolutionStateKind::PendingReconciliation
            | ReferenceResolutionStateKind::RefreshFailed => {
                self.degraded_operations_surface(&access, true)?
            }
        };

        Ok(IdentityQueryResponse {
            query_name: request.query_name,
            surface,
            body: Some(body),
        })
    }

    /// Commit-05-c flow: read report-only reconciliation material without regeneration.
    pub fn read_reconciliation_report(
        &self,
        request: IdentityQueryRequest<ReadReconciliationReportRequest>,
        context: IdentityOperationContext,
    ) -> Result<IdentityPageResponse<ReconciliationReportView>, ApplicationError> {
        Self::assert_query_context(&request, &context)?;
        let scope_access = self
            .deps
            .read_visibility_repository
            .resolve_reconciliation_scope_read(
                request.body.maintenance_scope_ref.clone(),
                request.body.consumer_ref.clone(),
                request.metadata.visibility_context_ref.clone(),
            )?
            .ok_or_else(|| {
                ApplicationError::invalid_request(
                    "reconciliation report scope visibility could not form a canonical subject",
                )
            })?;

        match self.surface_for_access(&scope_access, None, false)? {
            AccessSurfaceOutcome::Visible => {}
            AccessSurfaceOutcome::Return(surface) => {
                return Ok(empty_page_response(request.query_name, surface));
            }
        }

        if let Some(report_ref) = request.body.report_ref.clone() {
            let Some(report) = self
                .deps
                .reconciliation_report_repository
                .get_report_with_version(report_ref.clone())?
                .map(|value| value.value)
            else {
                return Ok(empty_page_response(
                    request.query_name,
                    self.missing_surface(&scope_access)?,
                ));
            };

            if report.maintenance_scope_ref != request.body.maintenance_scope_ref {
                let degradation = self
                    .deps
                    .degradation_mapper
                    .reconciliation_report_scope_mismatch(
                        scope_access.clone(),
                        report_ref,
                        request.body.maintenance_scope_ref,
                        report.maintenance_scope_ref.clone(),
                    );
                return Ok(empty_page_response(
                    request.query_name,
                    self.degraded_surface_from_material(degradation)?,
                ));
            }

            let item_access = self
                .deps
                .read_visibility_repository
                .resolve_report_read(
                    report.report_ref.clone(),
                    request.body.consumer_ref,
                    request.metadata.visibility_context_ref,
                )?
                .ok_or_else(|| {
                    ApplicationError::invalid_request(
                        "reconciliation report item visibility could not form a canonical subject",
                    )
                })?;

            match self.surface_for_access(&item_access, None, false)? {
                AccessSurfaceOutcome::Visible => {}
                AccessSurfaceOutcome::Return(surface) => {
                    return Ok(empty_page_response(request.query_name, surface));
                }
            }

            let view = self.reconciliation_report_view(&item_access, &report);
            let surface = self.visible_surface_for_operations(&item_access, true)?;
            return Ok(IdentityPageResponse {
                query_name: request.query_name,
                surface,
                page_info: IdentityPublicPageInfo {
                    next_cursor: None,
                    has_more: false,
                    item_count: 1,
                },
                items: vec![view],
            });
        }

        let page = require_page(&request.page)?;
        let listed = self
            .deps
            .reconciliation_report_repository
            .list_reports_by_scope(request.body.maintenance_scope_ref.clone(), page)?;

        if listed.items.is_empty() {
            return Ok(empty_page_response(
                request.query_name,
                self.empty_surface(&scope_access)?,
            ));
        }

        let mut items = Vec::new();
        let mut degraded_surface = None;
        let mut denied_count = 0usize;
        let mut denied_access = None;
        let mut partial_redaction_access = None;

        for listed_report in &listed.items {
            let item_access = self
                .deps
                .read_visibility_repository
                .resolve_report_read(
                    listed_report.value_ref.clone(),
                    request.body.consumer_ref.clone(),
                    request.metadata.visibility_context_ref.clone(),
                )?
                .ok_or_else(|| {
                    ApplicationError::invalid_request(
                        "reconciliation report item visibility could not form a canonical subject",
                    )
                })?;

            match item_access.access_state {
                IdentityVisibilityAccessState::NotVisible => {
                    denied_count += 1;
                    denied_access.get_or_insert(item_access);
                    continue;
                }
                IdentityVisibilityAccessState::Degraded
                | IdentityVisibilityAccessState::Unavailable => {
                    degraded_surface = Some(self.degraded_operations_surface(&item_access, false)?);
                    break;
                }
                IdentityVisibilityAccessState::Redacted => {
                    partial_redaction_access.get_or_insert(item_access.clone());
                }
                IdentityVisibilityAccessState::Visible => {}
            }

            let loaded = match self
                .deps
                .reconciliation_report_repository
                .get_report_with_version(listed_report.value_ref.clone())?
            {
                Some(report) => report.value,
                None => {
                    let degradation = self
                        .deps
                        .degradation_mapper
                        .reconciliation_report_item_missing_after_list(
                            item_access.clone(),
                            listed_report.value_ref.clone(),
                            request.body.maintenance_scope_ref.clone(),
                        );
                    degraded_surface = Some(self.degraded_surface_from_material(degradation)?);
                    break;
                }
            };

            if loaded.maintenance_scope_ref != request.body.maintenance_scope_ref {
                let degradation = self
                    .deps
                    .degradation_mapper
                    .reconciliation_report_scope_mismatch(
                        item_access.clone(),
                        listed_report.value_ref.clone(),
                        request.body.maintenance_scope_ref.clone(),
                        loaded.maintenance_scope_ref.clone(),
                    );
                degraded_surface = Some(self.degraded_surface_from_material(degradation)?);
                break;
            }

            items.push(self.reconciliation_report_view(&item_access, &loaded));
        }

        if let Some(surface) = degraded_surface {
            return Ok(IdentityPageResponse {
                query_name: request.query_name,
                surface,
                page_info: page_info(&listed),
                items,
            });
        }

        let surface = if items.is_empty() && denied_count > 0 {
            self.not_visible_surface(denied_access.as_ref().unwrap_or(&scope_access))?
        } else if denied_count > 0 || partial_redaction_access.is_some() {
            self.redacted_surface(
                partial_redaction_access
                    .as_ref()
                    .or(denied_access.as_ref())
                    .unwrap_or(&scope_access),
                true,
            )?
        } else {
            self.surface_for_list_success(&scope_access, false, false)?
        };

        Ok(IdentityPageResponse {
            query_name: request.query_name,
            surface,
            page_info: page_info(&listed),
            items,
        })
    }

    /// Commit-05-c flow: list stored outbox state without publish or retry side effects.
    pub fn list_pending_identity_outbox(
        &self,
        request: IdentityQueryRequest<ListPendingIdentityOutboxRequest>,
        context: IdentityOperationContext,
    ) -> Result<IdentityPageResponse<IdentityOutboxRecordView>, ApplicationError> {
        Self::assert_query_context(&request, &context)?;
        let page = require_page(&request.page)?;

        let (selector_access, listed, subject_filter, topic_filter, trace_filter, selector_kind) =
            match &request.body.selector {
                IdentityOutboxListSelector::Pending { topic_key_ref } => {
                    let access = self
                        .deps
                        .read_visibility_repository
                        .resolve_outbox_record_read(
                            None,
                            None,
                            topic_key_ref.clone(),
                            request.body.consumer_ref.clone(),
                            request.metadata.visibility_context_ref.clone(),
                        )?
                        .ok_or_else(|| {
                            ApplicationError::invalid_request(
                                "pending outbox visibility could not form a canonical subject",
                            )
                        })?;
                    let listed = self
                        .deps
                        .outbox_repository
                        .list_pending_outbox_records(topic_key_ref.clone(), page)?;
                    (access, listed, None, topic_key_ref.clone(), None, "pending")
                }
                IdentityOutboxListSelector::Retryable { topic_key_ref } => {
                    let access = self
                        .deps
                        .read_visibility_repository
                        .resolve_outbox_record_read(
                            None,
                            None,
                            topic_key_ref.clone(),
                            request.body.consumer_ref.clone(),
                            request.metadata.visibility_context_ref.clone(),
                        )?
                        .ok_or_else(|| {
                            ApplicationError::invalid_request(
                                "retryable outbox visibility could not form a canonical subject",
                            )
                        })?;
                    let listed = self
                        .deps
                        .outbox_repository
                        .list_retryable_outbox_records(topic_key_ref.clone(), page)?;
                    (
                        access,
                        listed,
                        None,
                        topic_key_ref.clone(),
                        None,
                        "retryable",
                    )
                }
                IdentityOutboxListSelector::BySubject { subject_ref } => {
                    let access = self
                        .deps
                        .read_visibility_repository
                        .resolve_outbox_record_read(
                            None,
                            Some(subject_ref.clone()),
                            None,
                            request.body.consumer_ref.clone(),
                            request.metadata.visibility_context_ref.clone(),
                        )?
                        .ok_or_else(|| {
                            ApplicationError::invalid_request(
                                "outbox subject visibility could not form a canonical subject",
                            )
                        })?;
                    let listed = self
                        .deps
                        .outbox_repository
                        .list_outbox_records_by_subject(subject_ref.clone(), page)?;
                    (
                        access,
                        listed,
                        Some(subject_ref.clone()),
                        None,
                        None,
                        "subject",
                    )
                }
                IdentityOutboxListSelector::ByMember { member_ref } => {
                    let subject_ref = self
                        .deps
                        .truth_change_subject_mapper
                        .member_subjects(member_ref.clone())
                        .outbox_subject_ref;
                    let access = self
                        .deps
                        .read_visibility_repository
                        .resolve_outbox_record_read(
                            None,
                            Some(subject_ref.clone()),
                            None,
                            request.body.consumer_ref.clone(),
                            request.metadata.visibility_context_ref.clone(),
                        )?
                        .ok_or_else(|| {
                            ApplicationError::invalid_request(
                                "member outbox visibility could not form a canonical subject",
                            )
                        })?;
                    let listed = self
                        .deps
                        .outbox_repository
                        .list_outbox_records_by_subject(subject_ref.clone(), page)?;
                    (access, listed, Some(subject_ref), None, None, "member")
                }
                IdentityOutboxListSelector::ByTrace { trace_record_ref } => {
                    let access = self
                        .deps
                        .read_visibility_repository
                        .resolve_outbox_trace_page_read(
                            trace_record_ref.clone(),
                            request.body.consumer_ref.clone(),
                            request.metadata.visibility_context_ref.clone(),
                        )?
                        .ok_or_else(|| {
                            ApplicationError::invalid_request(
                                "outbox trace page visibility could not form a canonical subject",
                            )
                        })?;
                    let listed = self
                        .deps
                        .outbox_repository
                        .find_outbox_records_by_trace(trace_record_ref.clone(), page)?;
                    (
                        access,
                        listed,
                        None,
                        None,
                        Some(trace_record_ref.clone()),
                        "trace",
                    )
                }
            };

        match self.surface_for_access(&selector_access, None, false)? {
            AccessSurfaceOutcome::Visible => {}
            AccessSurfaceOutcome::Return(surface) => {
                return Ok(empty_page_response(request.query_name, surface));
            }
        }

        if listed.items.is_empty() {
            return Ok(empty_page_response(
                request.query_name,
                self.empty_surface(&selector_access)?,
            ));
        }

        let mut items = Vec::new();
        let mut degraded_surface = None;
        let mut denied_count = 0usize;
        let mut denied_access = None;
        let mut partial_redaction_access = None;

        for listed_outbox in &listed.items {
            let ref_access = self
                .deps
                .read_visibility_repository
                .resolve_outbox_record_read(
                    Some(listed_outbox.value_ref.clone()),
                    None,
                    None,
                    request.body.consumer_ref.clone(),
                    request.metadata.visibility_context_ref.clone(),
                )?
                .ok_or_else(|| {
                    ApplicationError::invalid_request(
                        "outbox item visibility could not form a canonical subject",
                    )
                })?;

            match ref_access.access_state {
                IdentityVisibilityAccessState::NotVisible => {
                    denied_count += 1;
                    denied_access.get_or_insert(ref_access);
                    continue;
                }
                IdentityVisibilityAccessState::Degraded
                | IdentityVisibilityAccessState::Unavailable => {
                    degraded_surface = Some(self.degraded_operations_surface(&ref_access, false)?);
                    break;
                }
                IdentityVisibilityAccessState::Redacted => {
                    partial_redaction_access.get_or_insert(ref_access.clone());
                }
                IdentityVisibilityAccessState::Visible => {}
            }

            let loaded = match self
                .deps
                .outbox_repository
                .get_outbox_record_with_version(listed_outbox.value_ref.clone())?
            {
                Some(record) => record.value,
                None => {
                    let degradation = self
                        .deps
                        .degradation_mapper
                        .outbox_record_item_missing_after_list(
                            ref_access.clone(),
                            listed_outbox.value_ref.clone(),
                        );
                    degraded_surface = Some(self.degraded_surface_from_material(degradation)?);
                    break;
                }
            };

            let selector_matches = match selector_kind {
                "pending" => {
                    loaded.outbox_state.state_kind == OutboxStateKind::PendingPublish
                        && topic_filter
                            .as_ref()
                            .map(|topic| &loaded.topic_key_ref == topic)
                            .unwrap_or(true)
                }
                "retryable" => {
                    loaded.is_retryable()
                        && topic_filter
                            .as_ref()
                            .map(|topic| &loaded.topic_key_ref == topic)
                            .unwrap_or(true)
                }
                "subject" | "member" => subject_filter
                    .as_ref()
                    .map(|subject| loaded.subject_ref == *subject)
                    .unwrap_or(false),
                "trace" => trace_filter
                    .as_ref()
                    .map(|trace_ref| loaded.trace_record_ref == *trace_ref)
                    .unwrap_or(false),
                _ => false,
            };

            if !selector_matches {
                let degradation = self
                    .deps
                    .degradation_mapper
                    .outbox_record_selector_mismatch(
                        ref_access.clone(),
                        listed_outbox.value_ref.clone(),
                    );
                degraded_surface = Some(self.degraded_surface_from_material(degradation)?);
                break;
            }

            let item_access = self
                .deps
                .read_visibility_repository
                .resolve_outbox_record_read(
                    Some(loaded.outbox_record_ref.clone()),
                    Some(loaded.subject_ref.clone()),
                    Some(loaded.topic_key_ref.clone()),
                    request.body.consumer_ref.clone(),
                    request.metadata.visibility_context_ref.clone(),
                )?
                .ok_or_else(|| {
                    ApplicationError::invalid_request(
                        "loaded outbox visibility could not form a canonical subject",
                    )
                })?;

            match item_access.access_state {
                IdentityVisibilityAccessState::NotVisible => {
                    denied_count += 1;
                    denied_access.get_or_insert(item_access);
                    continue;
                }
                IdentityVisibilityAccessState::Degraded
                | IdentityVisibilityAccessState::Unavailable => {
                    degraded_surface = Some(self.degraded_operations_surface(&item_access, false)?);
                    break;
                }
                IdentityVisibilityAccessState::Redacted => {
                    partial_redaction_access.get_or_insert(item_access.clone());
                }
                IdentityVisibilityAccessState::Visible => {}
            }

            items.push(self.identity_outbox_record_view(&item_access, &loaded));
        }

        if let Some(surface) = degraded_surface {
            return Ok(IdentityPageResponse {
                query_name: request.query_name,
                surface,
                page_info: page_info(&listed),
                items,
            });
        }

        let surface = if items.is_empty() && denied_count > 0 {
            self.not_visible_surface(denied_access.as_ref().unwrap_or(&selector_access))?
        } else if denied_count > 0 || partial_redaction_access.is_some() {
            self.redacted_surface(
                partial_redaction_access
                    .as_ref()
                    .or(denied_access.as_ref())
                    .unwrap_or(&selector_access),
                true,
            )?
        } else {
            self.surface_for_list_success(&selector_access, false, false)?
        };

        Ok(IdentityPageResponse {
            query_name: request.query_name,
            surface,
            page_info: page_info(&listed),
            items,
        })
    }

    /// Commit-05-c flow: read one stored outbox state without publisher interaction.
    pub fn get_identity_outbox_state(
        &self,
        request: IdentityQueryRequest<GetIdentityOutboxStateRequest>,
        context: IdentityOperationContext,
    ) -> Result<IdentityQueryResponse<IdentityOutboxStateView>, ApplicationError> {
        Self::assert_query_context(&request, &context)?;
        let access = self
            .deps
            .read_visibility_repository
            .resolve_outbox_record_read(
                Some(request.body.outbox_record_ref.clone()),
                None,
                None,
                request.body.consumer_ref.clone(),
                request.metadata.visibility_context_ref.clone(),
            )?
            .ok_or_else(|| {
                ApplicationError::invalid_request(
                    "outbox state visibility could not form a canonical subject",
                )
            })?;

        match self.surface_for_access(&access, None, false)? {
            AccessSurfaceOutcome::Visible => {}
            AccessSurfaceOutcome::Return(surface) => {
                return Ok(IdentityQueryResponse {
                    query_name: request.query_name,
                    surface,
                    body: None,
                });
            }
        }

        let Some(record) = self
            .deps
            .outbox_repository
            .get_outbox_record_with_version(request.body.outbox_record_ref.clone())?
            .map(|value| value.value)
        else {
            return Ok(IdentityQueryResponse {
                query_name: request.query_name,
                surface: self.missing_surface(&access)?,
                body: None,
            });
        };

        let final_access = self
            .deps
            .read_visibility_repository
            .resolve_outbox_record_read(
                Some(record.outbox_record_ref.clone()),
                Some(record.subject_ref.clone()),
                Some(record.topic_key_ref.clone()),
                request.body.consumer_ref,
                request.metadata.visibility_context_ref,
            )?
            .ok_or_else(|| {
                ApplicationError::invalid_request(
                    "loaded outbox state visibility could not form a canonical subject",
                )
            })?;

        match self.surface_for_access(&final_access, None, false)? {
            AccessSurfaceOutcome::Visible => {}
            AccessSurfaceOutcome::Return(surface) => {
                return Ok(IdentityQueryResponse {
                    query_name: request.query_name,
                    surface,
                    body: None,
                });
            }
        }

        Ok(IdentityQueryResponse {
            query_name: request.query_name,
            surface: self.visible_surface_for_operations(&final_access, true)?,
            body: Some(self.identity_outbox_state_view(&final_access, &record)),
        })
    }

    /// Commit-05-c flow: read one stored handoff state without delivery side effects.
    pub fn get_trace_handoff_state(
        &self,
        request: IdentityQueryRequest<GetTraceHandoffStateRequest>,
        context: IdentityOperationContext,
    ) -> Result<IdentityQueryResponse<TraceHandoffStateView>, ApplicationError> {
        Self::assert_query_context(&request, &context)?;
        let access = self
            .deps
            .read_visibility_repository
            .resolve_handoff_intent_read(
                request.body.handoff_intent_ref.clone(),
                request.body.consumer_ref,
                request.metadata.visibility_context_ref,
            )?
            .ok_or_else(|| {
                ApplicationError::invalid_request(
                    "handoff state visibility could not form a canonical subject",
                )
            })?;

        match self.surface_for_access(&access, None, false)? {
            AccessSurfaceOutcome::Visible => {}
            AccessSurfaceOutcome::Return(surface) => {
                return Ok(IdentityQueryResponse {
                    query_name: request.query_name,
                    surface,
                    body: None,
                });
            }
        }

        let Some(intent) = self
            .deps
            .handoff_intent_repository
            .get_handoff_intent_with_version(request.body.handoff_intent_ref.clone())?
            .map(|value| value.value)
        else {
            return Ok(IdentityQueryResponse {
                query_name: request.query_name,
                surface: self.missing_surface(&access)?,
                body: None,
            });
        };

        if intent.trace_record_refs.is_empty() {
            let degradation = self
                .deps
                .degradation_mapper
                .handoff_intent_empty_trace_refs(access.clone(), request.body.handoff_intent_ref);
            return Ok(IdentityQueryResponse {
                query_name: request.query_name,
                surface: self.degraded_surface_from_material(degradation)?,
                body: None,
            });
        }

        if intent.handoff_state.state_kind == HandoffStateKind::Delivered
            && intent.handoff_state.receipt_ref.is_none()
        {
            let degradation = self
                .deps
                .degradation_mapper
                .handoff_intent_delivered_without_receipt(
                    access.clone(),
                    intent.handoff_intent_ref.clone(),
                );
            return Ok(IdentityQueryResponse {
                query_name: request.query_name,
                surface: self.degraded_surface_from_material(degradation)?,
                body: None,
            });
        }

        Ok(IdentityQueryResponse {
            query_name: request.query_name,
            surface: self.visible_surface_for_operations(&access, true)?,
            body: Some(self.trace_handoff_state_view(&access, &intent)),
        })
    }

    fn resolve_member_access(
        &self,
        member_ref: GlobalMemberRef,
        consumer_ref: ConsumerRef,
        visibility_context_ref: VisibilityContextRef,
    ) -> Result<IdentityVisibilityAccessSummary, ApplicationError> {
        self.deps
            .read_visibility_repository
            .resolve_member_summary_read(member_ref, None, consumer_ref, visibility_context_ref)?
            .ok_or_else(|| {
                ApplicationError::invalid_request(
                    "member-scoped query visibility could not form a canonical subject",
                )
            })
    }

    fn load_optional_member_summary_slice(
        &self,
        access: &IdentityVisibilityAccessSummary,
        member_ref: GlobalMemberRef,
    ) -> Result<OptionalMemberSummarySlice, ApplicationError> {
        let Some(view_ref) = self
            .deps
            .projection_repository
            .find_member_summary_view_ref(member_ref.clone(), access.scope_ref.clone())?
        else {
            return Ok(OptionalMemberSummarySlice::missing_lookup());
        };

        let Some(view) = self
            .deps
            .projection_repository
            .get_member_summary_view(view_ref.clone())?
        else {
            let degradation = self.deps.degradation_mapper.member_summary_view_missing(
                access.clone(),
                member_ref,
                access.scope_ref.clone(),
            );
            return Ok(OptionalMemberSummarySlice::from_surface(
                self.degraded_surface_from_material(degradation)?,
            ));
        };

        if !view.belongs_to(&member_ref) {
            let degradation = self
                .deps
                .degradation_mapper
                .member_summary_view_invalid_owner(access.clone(), view_ref, member_ref);
            return Ok(OptionalMemberSummarySlice::from_surface(
                self.degraded_surface_from_material(degradation)?,
            ));
        }

        if !view.matches_visibility_scope(&access.scope_ref) {
            let degradation = self
                .deps
                .degradation_mapper
                .member_summary_view_scope_mismatch(
                    access.clone(),
                    view.view_ref.clone(),
                    access.scope_ref.clone(),
                );
            return Ok(OptionalMemberSummarySlice::from_surface(
                self.degraded_surface_from_material(degradation)?,
            ));
        }

        if view.assert_body_free().is_err() {
            let degradation = self
                .deps
                .degradation_mapper
                .forbidden_read_material(access.clone(), view.read_material_marker.clone());
            return Ok(OptionalMemberSummarySlice::from_surface(
                self.degraded_surface_from_material(degradation)?,
            ));
        }

        Ok(OptionalMemberSummarySlice {
            surface: None,
            view_ref: Some(view.view_ref.clone()),
            view: Some(view),
        })
    }

    fn visible_surface_for_operations(
        &self,
        access: &IdentityVisibilityAccessSummary,
        found: bool,
    ) -> Result<IdentityQuerySurface, ApplicationError> {
        let disposition = if matches!(access.access_state, IdentityVisibilityAccessState::Redacted)
        {
            IdentityReadDispositionKind::Redacted
        } else {
            IdentityReadDispositionKind::Visible
        };
        self.surface_from_access(access, disposition, None, found)
    }

    fn degraded_operations_surface(
        &self,
        access: &IdentityVisibilityAccessSummary,
        found: bool,
    ) -> Result<IdentityQuerySurface, ApplicationError> {
        let mut surface = self.visible_surface_for_operations(access, found)?;
        surface.disposition = IdentityQueryDisposition::Degraded;
        surface.visibility.read_surface_kind = IdentityReadSurfaceKind::Degraded;
        surface.projection_freshness_ref = None;
        Ok(surface)
    }

    fn rebuilding_surface(
        &self,
        access: &IdentityVisibilityAccessSummary,
    ) -> Result<IdentityQuerySurface, ApplicationError> {
        let mut surface = self.visible_surface_for_operations(access, false)?;
        surface.disposition = IdentityQueryDisposition::Rebuilding;
        surface.visibility.read_surface_kind = IdentityReadSurfaceKind::Degraded;
        surface.projection_freshness_ref = None;
        surface.degraded = None;
        Ok(surface)
    }

    fn projection_freshness_marker(
        &self,
        state: &ProjectionState,
    ) -> identity_contracts::refs::ProjectionFreshnessMarkerRef {
        identity_contracts::refs::ProjectionFreshnessMarkerRef {
            projection_ref: state.projection_ref.clone(),
            state_kind: self
                .public_projection_state_token(state.state_kind)
                .to_owned(),
        }
    }

    fn public_projection_state_token(&self, state: ProjectionStateKind) -> &'static str {
        match state {
            ProjectionStateKind::Fresh => "fresh",
            ProjectionStateKind::Stale => "stale",
            ProjectionStateKind::RebuildPending => "rebuild_pending",
            ProjectionStateKind::Rebuilt => "rebuilt",
            ProjectionStateKind::Degraded => "degraded",
            ProjectionStateKind::RebuildFailed => "rebuild_failed",
        }
    }

    fn projection_state_view(
        &self,
        access: &IdentityVisibilityAccessSummary,
        state: &ProjectionState,
    ) -> ProjectionStateView {
        ProjectionStateView {
            projection_state_ref: Some(state.projection_state_ref.clone()),
            projection_ref: state.projection_ref.clone(),
            member_ref: state.member_ref.clone(),
            state_kind: Some(map_projection_state_kind(state.state_kind)),
            source_cursor_ref: state.source_cursor_ref.clone(),
            maintenance_scope_ref: state.maintenance_scope_ref.clone(),
            issue_ref: state.issue_ref.clone(),
            checked_at: Some(state.checked_at),
            visibility_result_ref: access.visibility_result_ref.clone(),
        }
    }

    fn reference_sidecar_refs_view(
        &self,
        sidecars: &crate::ports::ExternalReferenceTypedSidecarRefs,
    ) -> Option<ReferenceResolutionSidecarRefsView> {
        let view = ReferenceResolutionSidecarRefsView {
            role_capability_safe_summary_ref: sidecars.role_capability_safe_summary_ref.clone(),
            career_safe_summary_ref: sidecars.career_safe_summary_ref.clone(),
            memory_safe_summary_ref: sidecars.memory_safe_summary_ref.clone(),
            governance_basis_summary_ref: sidecars.governance_basis_summary_ref.clone(),
            evidence_summary_ref: sidecars.evidence_summary_ref.clone(),
            source_version_ref: sidecars.source_version_ref.clone(),
        };
        if view.role_capability_safe_summary_ref.is_none()
            && view.career_safe_summary_ref.is_none()
            && view.memory_safe_summary_ref.is_none()
            && view.governance_basis_summary_ref.is_none()
            && view.evidence_summary_ref.is_none()
            && view.source_version_ref.is_none()
        {
            None
        } else {
            Some(view)
        }
    }

    fn reference_resolution_state_view(
        &self,
        access: &IdentityVisibilityAccessSummary,
        state: &ReferenceResolutionState,
        sidecars: crate::ports::ExternalReferenceTypedSidecarRefs,
    ) -> ReferenceResolutionStateView {
        ReferenceResolutionStateView {
            resolution_state_ref: Some(state.resolution_state_ref.clone()),
            external_reference_ref: state.external_reference_ref.clone(),
            owner_ref: Some(state.reference_owner_ref.clone()),
            state_kind: Some(map_reference_state_kind(state.state_kind)),
            source_version_ref: state.source_version_ref.clone(),
            safe_summary_ref: state.safe_summary_ref.clone(),
            sidecar_refs: self.reference_sidecar_refs_view(&sidecars),
            issue_ref: state.issue_ref.clone(),
            checked_at: Some(state.checked_at),
            visibility_result_ref: access.visibility_result_ref.clone(),
        }
    }

    fn reconciliation_report_view(
        &self,
        access: &IdentityVisibilityAccessSummary,
        report: &ReconciliationReport,
    ) -> ReconciliationReportView {
        ReconciliationReportView {
            report_ref: report.report_ref.clone(),
            maintenance_scope_ref: report.maintenance_scope_ref.clone(),
            target_refs: report.target_refs.clone(),
            finding_refs: report.finding_refs.clone(),
            issue_refs: report.issue_refs.clone(),
            report_state: map_reconciliation_report_state_kind(report.report_state),
            generated_by_ref: report.generated_by_ref.clone(),
            generated_at: report.generated_at,
            visibility_result_ref: access.visibility_result_ref.clone(),
        }
    }

    fn identity_outbox_record_view(
        &self,
        access: &IdentityVisibilityAccessSummary,
        record: &IdentityOutboxRecord,
    ) -> IdentityOutboxRecordView {
        IdentityOutboxRecordView {
            outbox_record_ref: record.outbox_record_ref.clone(),
            member_ref: record.member_ref.clone(),
            subject_ref: record.subject_ref.clone(),
            change_kind_ref: record.change_kind_ref.clone(),
            payload_marker_ref: record.payload_marker_ref.clone(),
            topic_key_ref: record.topic_key_ref.clone(),
            trace_record_ref: record.trace_record_ref.clone(),
            outbox_state_kind: map_outbox_state_kind(record.outbox_state.state_kind),
            attempt_ref: record.outbox_state.attempt_ref.clone(),
            issue_ref: record.outbox_state.issue_ref.clone(),
            created_at: record.created_at,
            updated_at: record.updated_at,
            visibility_result_ref: access.visibility_result_ref.clone(),
        }
    }

    fn identity_outbox_state_view(
        &self,
        access: &IdentityVisibilityAccessSummary,
        record: &IdentityOutboxRecord,
    ) -> IdentityOutboxStateView {
        IdentityOutboxStateView {
            outbox_record_ref: record.outbox_record_ref.clone(),
            subject_ref: record.subject_ref.clone(),
            topic_key_ref: record.topic_key_ref.clone(),
            trace_record_ref: record.trace_record_ref.clone(),
            outbox_state_kind: map_outbox_state_kind(record.outbox_state.state_kind),
            attempt_ref: record.outbox_state.attempt_ref.clone(),
            issue_ref: record.outbox_state.issue_ref.clone(),
            payload_marker_ref: record.payload_marker_ref.clone(),
            changed_at: record.outbox_state.changed_at,
            visibility_result_ref: access.visibility_result_ref.clone(),
        }
    }

    fn trace_handoff_state_view(
        &self,
        access: &IdentityVisibilityAccessSummary,
        intent: &TraceHandoffIntent,
    ) -> TraceHandoffStateView {
        TraceHandoffStateView {
            handoff_intent_ref: intent.handoff_intent_ref.clone(),
            member_ref: intent.member_ref.clone(),
            trace_record_refs: intent.trace_record_refs.clone(),
            audit_trail_ref: intent.audit_trail_ref.clone(),
            handoff_target_ref: intent.handoff_target_ref.clone(),
            handoff_scope_ref: intent.handoff_scope_ref.clone(),
            safe_material_ref: intent.safe_material_ref.clone(),
            handoff_state_kind: map_handoff_state_kind(intent.handoff_state.state_kind),
            attempt_ref: intent.handoff_state.attempt_ref.clone(),
            receipt_ref: intent.handoff_state.receipt_ref.clone(),
            issue_ref: intent.handoff_state.issue_ref.clone(),
            created_at: intent.created_at,
            updated_at: intent.updated_at,
            changed_at: intent.handoff_state.changed_at,
            visibility_result_ref: access.visibility_result_ref.clone(),
        }
    }

    fn surface_for_access(
        &self,
        access: &IdentityVisibilityAccessSummary,
        projection_freshness_ref: Option<identity_contracts::refs::ProjectionFreshnessMarkerRef>,
        found: bool,
    ) -> Result<AccessSurfaceOutcome, ApplicationError> {
        match access.access_state {
            IdentityVisibilityAccessState::Visible => Ok(AccessSurfaceOutcome::Visible),
            IdentityVisibilityAccessState::Redacted => Ok(AccessSurfaceOutcome::Visible),
            IdentityVisibilityAccessState::NotVisible => Ok(AccessSurfaceOutcome::Return(
                self.not_visible_surface(access)?,
            )),
            IdentityVisibilityAccessState::Degraded
            | IdentityVisibilityAccessState::Unavailable => {
                Ok(AccessSurfaceOutcome::Return(self.surface_from_access(
                    access,
                    IdentityReadDispositionKind::Degraded,
                    projection_freshness_ref,
                    found,
                )?))
            }
        }
    }

    fn surface_for_truth_query(
        &self,
        access: &IdentityVisibilityAccessSummary,
        view: Option<&MemberSummaryView>,
        found: bool,
    ) -> Result<IdentityQuerySurface, ApplicationError> {
        if let Some(view) = view {
            if view.is_stale_or_degraded() {
                if let Some(marker) = view.projection_freshness_ref.clone() {
                    return self.surface_from_access(
                        access,
                        IdentityReadDispositionKind::StaleVisible,
                        Some(marker),
                        found,
                    );
                }

                let degradation = self
                    .deps
                    .degradation_mapper
                    .member_summary_view_missing_freshness(
                        access.clone(),
                        view.view_ref.clone(),
                        view.member_ref.clone(),
                        access.scope_ref.clone(),
                    );
                return self.degraded_surface_from_material(degradation);
            }
        }

        let disposition = if matches!(access.access_state, IdentityVisibilityAccessState::Redacted)
        {
            IdentityReadDispositionKind::Redacted
        } else {
            IdentityReadDispositionKind::Visible
        };
        self.surface_from_access(access, disposition, None, found)
    }

    fn surface_for_list_success(
        &self,
        access: &IdentityVisibilityAccessSummary,
        empty: bool,
        _partial_redacted: bool,
    ) -> Result<IdentityQuerySurface, ApplicationError> {
        if empty {
            return self.empty_surface(access);
        }

        let disposition = if matches!(access.access_state, IdentityVisibilityAccessState::Redacted)
        {
            IdentityReadDispositionKind::Redacted
        } else {
            IdentityReadDispositionKind::Visible
        };
        self.surface_from_access(access, disposition, None, true)
    }

    fn missing_surface(
        &self,
        access: &IdentityVisibilityAccessSummary,
    ) -> Result<IdentityQuerySurface, ApplicationError> {
        let mut surface = self.surface_from_access(
            access,
            if matches!(access.access_state, IdentityVisibilityAccessState::Redacted) {
                IdentityReadDispositionKind::Redacted
            } else {
                IdentityReadDispositionKind::Visible
            },
            None,
            false,
        )?;
        surface.disposition = IdentityQueryDisposition::Missing;
        surface.visibility.read_surface_kind = IdentityReadSurfaceKind::NotFound;
        surface.projection_freshness_ref = None;
        surface.degraded = None;
        Ok(surface)
    }

    fn empty_surface(
        &self,
        access: &IdentityVisibilityAccessSummary,
    ) -> Result<IdentityQuerySurface, ApplicationError> {
        let mut surface = self.surface_from_access(
            access,
            if matches!(access.access_state, IdentityVisibilityAccessState::Redacted) {
                IdentityReadDispositionKind::Redacted
            } else {
                IdentityReadDispositionKind::Visible
            },
            None,
            false,
        )?;
        surface.disposition = IdentityQueryDisposition::Empty;
        surface.visibility.read_surface_kind = IdentityReadSurfaceKind::Empty;
        surface.projection_freshness_ref = None;
        surface.degraded = None;
        Ok(surface)
    }

    fn not_visible_surface(
        &self,
        access: &IdentityVisibilityAccessSummary,
    ) -> Result<IdentityQuerySurface, ApplicationError> {
        self.surface_from_access(access, IdentityReadDispositionKind::NotVisible, None, false)
    }

    fn redacted_surface(
        &self,
        access: &IdentityVisibilityAccessSummary,
        found: bool,
    ) -> Result<IdentityQuerySurface, ApplicationError> {
        self.surface_from_access(access, IdentityReadDispositionKind::Redacted, None, found)
    }

    fn degraded_surface_from_access(
        &self,
        access: &IdentityVisibilityAccessSummary,
        read_surface_kind: IdentityReadSurfaceKind,
        degraded_kind: IdentityDegradedKind,
    ) -> Result<IdentityQuerySurface, ApplicationError> {
        let surface = self.surface_from_access(
            access,
            IdentityReadDispositionKind::Degraded,
            None,
            read_surface_kind == IdentityReadSurfaceKind::Found,
        )?;
        let mut surface = surface;
        surface.visibility.read_surface_kind = read_surface_kind;
        if let Some(degraded) = surface.degraded.as_mut() {
            degraded.degraded_kind = degraded_kind;
        }
        Ok(surface)
    }

    fn degraded_surface_from_material(
        &self,
        degradation: IdentityQueryMaterialDegradationSummary,
    ) -> Result<IdentityQuerySurface, ApplicationError> {
        let decision_ref = self
            .deps
            .id_generator
            .new_identity_visibility_decision_ref()?;
        let decided_at = self.deps.clock.now()?;
        let decision = IdentityVisibilityDecision::degraded(
            decision_ref,
            degradation.read_subject_ref,
            degradation.visibility_context_ref,
            degradation.visibility_scope_ref,
            degradation.visibility_result_ref,
            IdentityReadSurfaceKind::Degraded,
            degradation.degraded_marker_ref,
            degradation.degraded_kind,
            decided_at,
        );
        self.surface_from_decision(decision, None)
    }

    fn surface_from_access(
        &self,
        access: &IdentityVisibilityAccessSummary,
        disposition: IdentityReadDispositionKind,
        projection_freshness_ref: Option<identity_contracts::refs::ProjectionFreshnessMarkerRef>,
        found: bool,
    ) -> Result<IdentityQuerySurface, ApplicationError> {
        let decision = self.decision_from_access(access, disposition, found)?;
        self.surface_from_decision(decision, projection_freshness_ref)
    }

    fn surface_from_decision(
        &self,
        decision: IdentityVisibilityDecision,
        projection_freshness_ref: Option<identity_contracts::refs::ProjectionFreshnessMarkerRef>,
    ) -> Result<IdentityQuerySurface, ApplicationError> {
        Ok(IdentityQuerySurface {
            disposition: map_query_disposition(decision.disposition),
            visibility: decision.visibility_marker(),
            degraded: decision.degraded_marker(),
            projection_freshness_ref,
            decision_ref: Some(decision.decision_ref),
        })
    }

    fn decision_from_access(
        &self,
        access: &IdentityVisibilityAccessSummary,
        disposition: IdentityReadDispositionKind,
        found: bool,
    ) -> Result<IdentityVisibilityDecision, ApplicationError> {
        let decision_ref = self
            .deps
            .id_generator
            .new_identity_visibility_decision_ref()?;
        let decided_at = self.deps.clock.now()?;
        let surface_kind = map_surface_kind(disposition, found);
        Ok(match disposition {
            IdentityReadDispositionKind::Visible => IdentityVisibilityDecision::visible(
                decision_ref,
                access.read_subject_ref.clone(),
                access.visibility_context_ref.clone(),
                access.scope_ref.clone(),
                access.visibility_result_ref.clone(),
                surface_kind,
                decided_at,
            ),
            IdentityReadDispositionKind::Redacted => IdentityVisibilityDecision::redacted(
                decision_ref,
                access.read_subject_ref.clone(),
                access.visibility_context_ref.clone(),
                access.scope_ref.clone(),
                access.visibility_result_ref.clone(),
                surface_kind,
                access.redaction_marker_ref.clone().ok_or_else(|| {
                    ApplicationError::consistency_defect(
                        "redacted query surface requires a formal redaction marker",
                    )
                })?,
                decided_at,
            ),
            IdentityReadDispositionKind::NotVisible => IdentityVisibilityDecision::not_visible(
                decision_ref,
                access.read_subject_ref.clone(),
                access.visibility_context_ref.clone(),
                access.scope_ref.clone(),
                access.visibility_result_ref.clone(),
                surface_kind,
                access.redaction_marker_ref.clone(),
                decided_at,
            ),
            IdentityReadDispositionKind::Degraded => IdentityVisibilityDecision::degraded(
                decision_ref,
                access.read_subject_ref.clone(),
                access.visibility_context_ref.clone(),
                access.scope_ref.clone(),
                access.visibility_result_ref.clone(),
                surface_kind,
                access.degraded_marker_ref.clone().ok_or_else(|| {
                    ApplicationError::consistency_defect(
                        "degraded query surface requires a formal degraded marker",
                    )
                })?,
                access.degraded_kind.ok_or_else(|| {
                    ApplicationError::consistency_defect(
                        "degraded query surface requires a formal degraded kind",
                    )
                })?,
                decided_at,
            ),
            IdentityReadDispositionKind::StaleVisible => IdentityVisibilityDecision::stale_visible(
                decision_ref,
                access.read_subject_ref.clone(),
                access.visibility_context_ref.clone(),
                access.scope_ref.clone(),
                access.visibility_result_ref.clone(),
                surface_kind,
                access.degraded_marker_ref.clone(),
                access.degraded_kind,
                decided_at,
            ),
        })
    }

    fn career_record_view(
        &self,
        access: &IdentityVisibilityAccessSummary,
        record: CareerRecord,
    ) -> CareerRecordView {
        let redacted = matches!(access.access_state, IdentityVisibilityAccessState::Redacted);
        CareerRecordView {
            career_record_ref: record.career_record_ref.clone(),
            member_ref: record.member_ref,
            record_state_kind: map_career_state_kind(record.record_state),
            project_participation_ref: hide_on_redaction(
                redacted,
                Some(record.project_participation_ref),
            ),
            work_source_ref: hide_on_redaction(redacted, Some(record.work_source_ref)),
            source_marker_ref: hide_on_redaction(redacted, Some(record.source_marker_ref)),
            career_summary_ref: hide_on_redaction(redacted, record.career_summary_ref),
            append_reason_ref: hide_on_redaction(redacted, Some(record.append_reason_ref)),
            appended_at: hide_on_redaction(redacted, Some(record.appended_at)),
            correction_of_ref: record.correction_of_ref,
            superseded_by_ref: record.superseded_by_ref,
        }
    }

    fn memory_reference_view(
        &self,
        access: &IdentityVisibilityAccessSummary,
        reference: MemoryReference,
    ) -> MemoryReferenceView {
        let redacted = matches!(access.access_state, IdentityVisibilityAccessState::Redacted);
        MemoryReferenceView {
            memory_reference_ref: reference.memory_reference_ref,
            member_ref: reference.member_ref,
            reference_state_kind: map_memory_state_kind(reference.reference_state.state_kind),
            memory_ref: hide_on_redaction(redacted, reference.memory_ref),
            archive_ref: hide_on_redaction(redacted, reference.archive_ref),
            archive_handoff_ref: hide_on_redaction(redacted, reference.archive_handoff_ref),
            source_ref: hide_on_redaction(redacted, Some(reference.source_ref)),
            safe_summary_ref: hide_on_redaction(redacted, reference.safe_summary_ref),
            reason_ref: hide_on_redaction(redacted, Some(reference.change_reason_ref)),
            changed_at: hide_on_redaction(redacted, Some(reference.changed_at)),
        }
    }

    fn trace_record_view(
        &self,
        access: &IdentityVisibilityAccessSummary,
        record: IdentityTraceRecord,
    ) -> IdentityTraceRecordView {
        let redacted = matches!(access.access_state, IdentityVisibilityAccessState::Redacted);
        IdentityTraceRecordView {
            trace_record_ref: record.trace_record_ref,
            member_ref: record.member_ref,
            subject_ref: record.subject_ref,
            audit_subject_ref: record.audit_subject_ref,
            change_kind_ref: record.change_kind_ref,
            source_cursor_ref: record.source_cursor_ref,
            reason_ref: hide_on_redaction(redacted, record.reason_ref),
            source_ref: hide_on_redaction(redacted, record.source_ref),
            basis_ref: hide_on_redaction(redacted, record.basis_ref),
            actor_ref: hide_on_redaction(redacted, record.actor_ref),
            visibility_result_ref: access.visibility_result_ref.clone(),
            superseded_by_trace_ref: record.superseded_by_trace_ref,
            read_material_marker: record.read_material_marker,
            occurred_at: record.occurred_at,
        }
    }
}

/// Internal foundation result for stable member-summary lookup and guard checks.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdentityMemberSummaryPreflight {
    /// Visibility-first access summary used for subject/scope/result copying.
    pub access_summary: IdentityVisibilityAccessSummary,
    /// Optional view-specific visibility recheck output.
    pub view_access_summary: IdentityVisibilityAccessSummary,
    /// Stable view identity loaded from the persisted `(member_ref, scope_ref)` index.
    pub view_ref: identity_contracts::refs::MemberSummaryViewRef,
    /// Loaded body-free view material.
    pub view: MemberSummaryView,
}

enum AccessSurfaceOutcome {
    Visible,
    Return(IdentityQuerySurface),
}

struct OptionalMemberSummarySlice {
    surface: Option<IdentityQuerySurface>,
    view_ref: Option<identity_contracts::refs::MemberSummaryViewRef>,
    view: Option<MemberSummaryView>,
}

impl OptionalMemberSummarySlice {
    fn missing_lookup() -> Self {
        Self {
            surface: None,
            view_ref: None,
            view: None,
        }
    }

    fn from_surface(surface: IdentityQuerySurface) -> Self {
        Self {
            surface: Some(surface),
            view_ref: None,
            view: None,
        }
    }
}

fn require_page(
    page: &Option<IdentityPublicPageRequest>,
) -> Result<IdentityRepositoryPage, ApplicationError> {
    let Some(page) = page.as_ref() else {
        return Err(ApplicationError::invalid_request(
            "paged query request must include page information",
        ));
    };
    Ok(IdentityRepositoryPage::new(
        page.cursor
            .as_ref()
            .map(|cursor| IdentityRepositoryCursor::new(cursor.as_str())),
        page.limit,
    ))
}

fn page_info<T>(page: &Page<T>) -> IdentityPublicPageInfo {
    IdentityPublicPageInfo {
        next_cursor: page
            .next_cursor
            .as_ref()
            .map(|cursor| IdentityPublicPageCursor::new(cursor.as_str())),
        has_more: page.next_cursor.is_some(),
        item_count: page.items.len() as u32,
    }
}

fn empty_page_response<T>(
    query_name: IdentityQueryName,
    surface: IdentityQuerySurface,
) -> IdentityPageResponse<T> {
    IdentityPageResponse {
        query_name,
        surface,
        page_info: IdentityPublicPageInfo {
            next_cursor: None,
            has_more: false,
            item_count: 0,
        },
        items: Vec::new(),
    }
}

fn map_surface_kind(
    disposition: IdentityReadDispositionKind,
    found: bool,
) -> IdentityReadSurfaceKind {
    match disposition {
        IdentityReadDispositionKind::Visible => {
            if found {
                IdentityReadSurfaceKind::Found
            } else {
                IdentityReadSurfaceKind::Empty
            }
        }
        IdentityReadDispositionKind::Redacted => IdentityReadSurfaceKind::Redacted,
        IdentityReadDispositionKind::NotVisible => IdentityReadSurfaceKind::NotVisible,
        IdentityReadDispositionKind::Degraded => IdentityReadSurfaceKind::Degraded,
        IdentityReadDispositionKind::StaleVisible => IdentityReadSurfaceKind::Stale,
    }
}

fn map_query_disposition(disposition: IdentityReadDispositionKind) -> IdentityQueryDisposition {
    match disposition {
        IdentityReadDispositionKind::Visible => IdentityQueryDisposition::Visible,
        IdentityReadDispositionKind::Redacted => IdentityQueryDisposition::Redacted,
        IdentityReadDispositionKind::NotVisible => IdentityQueryDisposition::NotVisible,
        IdentityReadDispositionKind::Degraded => IdentityQueryDisposition::Degraded,
        IdentityReadDispositionKind::StaleVisible => IdentityQueryDisposition::StaleVisible,
    }
}

fn map_anchor_state_kind(state: &IdentityAnchorState) -> PublicAnchorStateKind {
    match state.state_kind {
        identity_domain::member_identity::IdentityAnchorStateKind::Established => {
            PublicAnchorStateKind::Established
        }
        identity_domain::member_identity::IdentityAnchorStateKind::RetiredHeld => {
            PublicAnchorStateKind::RetiredHeld
        }
        identity_domain::member_identity::IdentityAnchorStateKind::TombstoneHeld => {
            PublicAnchorStateKind::TombstoneHeld
        }
    }
}

fn map_lifecycle_state_kind(
    state: identity_domain::lifecycle::GlobalLifecycleStateKind,
) -> PublicLifecycleStateKind {
    match state {
        identity_domain::lifecycle::GlobalLifecycleStateKind::Available => {
            PublicLifecycleStateKind::Available
        }
        identity_domain::lifecycle::GlobalLifecycleStateKind::Paused => {
            PublicLifecycleStateKind::Paused
        }
        identity_domain::lifecycle::GlobalLifecycleStateKind::Retired => {
            PublicLifecycleStateKind::Retired
        }
        identity_domain::lifecycle::GlobalLifecycleStateKind::Tombstoned => {
            PublicLifecycleStateKind::Tombstoned
        }
    }
}

fn map_role_summary_state_kind(
    state: RoleCapabilitySummaryStateKind,
) -> PublicRoleCapabilitySummaryStateKind {
    match state {
        RoleCapabilitySummaryStateKind::Active => PublicRoleCapabilitySummaryStateKind::Active,
        RoleCapabilitySummaryStateKind::Stale => PublicRoleCapabilitySummaryStateKind::Stale,
        RoleCapabilitySummaryStateKind::Unavailable => {
            PublicRoleCapabilitySummaryStateKind::Unavailable
        }
        RoleCapabilitySummaryStateKind::PendingReconciliation => {
            PublicRoleCapabilitySummaryStateKind::PendingReconciliation
        }
        RoleCapabilitySummaryStateKind::Superseded => {
            PublicRoleCapabilitySummaryStateKind::Superseded
        }
    }
}

fn map_role_source_state_kind(
    state: RoleCapabilitySourceStateKind,
) -> PublicRoleCapabilitySourceStateKind {
    match state {
        RoleCapabilitySourceStateKind::SourceResolved => {
            PublicRoleCapabilitySourceStateKind::SourceResolved
        }
        RoleCapabilitySourceStateKind::SourceStale => {
            PublicRoleCapabilitySourceStateKind::SourceStale
        }
        RoleCapabilitySourceStateKind::SourceUnavailable => {
            PublicRoleCapabilitySourceStateKind::SourceUnavailable
        }
        RoleCapabilitySourceStateKind::SourceUnrecognized => {
            PublicRoleCapabilitySourceStateKind::SourceUnrecognized
        }
        RoleCapabilitySourceStateKind::SourceSuperseded => {
            PublicRoleCapabilitySourceStateKind::SourceSuperseded
        }
    }
}

fn map_career_state_kind(
    state: identity_domain::career::CareerRecordStateKind,
) -> PublicCareerRecordStateKind {
    match state {
        identity_domain::career::CareerRecordStateKind::Appended => {
            PublicCareerRecordStateKind::Appended
        }
        identity_domain::career::CareerRecordStateKind::CorrectionAppended => {
            PublicCareerRecordStateKind::CorrectionAppended
        }
        identity_domain::career::CareerRecordStateKind::SupersededByCorrection => {
            PublicCareerRecordStateKind::SupersededByCorrection
        }
        identity_domain::career::CareerRecordStateKind::SourcePendingReview => {
            PublicCareerRecordStateKind::SourcePendingReview
        }
    }
}

fn map_memory_state_kind(state: MemoryReferenceStateKind) -> PublicMemoryReferenceStateKind {
    match state {
        MemoryReferenceStateKind::Linked => PublicMemoryReferenceStateKind::Linked,
        MemoryReferenceStateKind::PendingVerification => {
            PublicMemoryReferenceStateKind::PendingVerification
        }
        MemoryReferenceStateKind::Stale => PublicMemoryReferenceStateKind::Stale,
        MemoryReferenceStateKind::Unavailable => PublicMemoryReferenceStateKind::Unavailable,
        MemoryReferenceStateKind::Migrated => PublicMemoryReferenceStateKind::Migrated,
        MemoryReferenceStateKind::Archived => PublicMemoryReferenceStateKind::Archived,
        MemoryReferenceStateKind::HandoffPending => PublicMemoryReferenceStateKind::HandoffPending,
        MemoryReferenceStateKind::HandoffFailed => PublicMemoryReferenceStateKind::HandoffFailed,
    }
}

fn map_projection_state_kind(state: ProjectionStateKind) -> PublicProjectionStateKind {
    match state {
        ProjectionStateKind::Fresh => PublicProjectionStateKind::Fresh,
        ProjectionStateKind::Stale => PublicProjectionStateKind::Stale,
        ProjectionStateKind::RebuildPending => PublicProjectionStateKind::RebuildPending,
        ProjectionStateKind::Rebuilt => PublicProjectionStateKind::Rebuilt,
        ProjectionStateKind::Degraded => PublicProjectionStateKind::Degraded,
        ProjectionStateKind::RebuildFailed => PublicProjectionStateKind::RebuildFailed,
    }
}

fn map_reference_state_kind(
    state: ReferenceResolutionStateKind,
) -> PublicReferenceResolutionStateKind {
    match state {
        ReferenceResolutionStateKind::Resolved => PublicReferenceResolutionStateKind::Resolved,
        ReferenceResolutionStateKind::Stale => PublicReferenceResolutionStateKind::Stale,
        ReferenceResolutionStateKind::Unavailable => {
            PublicReferenceResolutionStateKind::Unavailable
        }
        ReferenceResolutionStateKind::Unrecognized => {
            PublicReferenceResolutionStateKind::Unrecognized
        }
        ReferenceResolutionStateKind::PendingReconciliation => {
            PublicReferenceResolutionStateKind::PendingReconciliation
        }
        ReferenceResolutionStateKind::RefreshFailed => {
            PublicReferenceResolutionStateKind::RefreshFailed
        }
    }
}

fn map_reconciliation_report_state_kind(
    state: ReconciliationReportStateKind,
) -> PublicReconciliationReportStateKind {
    match state {
        ReconciliationReportStateKind::Generated => PublicReconciliationReportStateKind::Generated,
        ReconciliationReportStateKind::NoFinding => PublicReconciliationReportStateKind::NoFinding,
        ReconciliationReportStateKind::FindingDetected => {
            PublicReconciliationReportStateKind::FindingDetected
        }
        ReconciliationReportStateKind::Partial => PublicReconciliationReportStateKind::Partial,
        ReconciliationReportStateKind::Failed => PublicReconciliationReportStateKind::Failed,
    }
}

fn map_outbox_state_kind(state: OutboxStateKind) -> PublicOutboxStateKind {
    match state {
        OutboxStateKind::PendingPublish => PublicOutboxStateKind::PendingPublish,
        OutboxStateKind::Published => PublicOutboxStateKind::Published,
        OutboxStateKind::RetryableFailed => PublicOutboxStateKind::RetryableFailed,
        OutboxStateKind::Failed => PublicOutboxStateKind::Failed,
        OutboxStateKind::SkippedByPolicy => PublicOutboxStateKind::SkippedByPolicy,
    }
}

fn map_handoff_state_kind(state: HandoffStateKind) -> PublicHandoffStateKind {
    match state {
        HandoffStateKind::PendingHandoff => PublicHandoffStateKind::PendingHandoff,
        HandoffStateKind::Delivered => PublicHandoffStateKind::Delivered,
        HandoffStateKind::RetryableFailed => PublicHandoffStateKind::RetryableFailed,
        HandoffStateKind::Failed => PublicHandoffStateKind::Failed,
        HandoffStateKind::Cancelled => PublicHandoffStateKind::Cancelled,
    }
}

fn role_snapshot_is_stale_or_unavailable(snapshot: &RoleCapabilitySourceSnapshot) -> bool {
    matches!(
        snapshot.source_state,
        RoleCapabilitySourceStateKind::SourceStale
            | RoleCapabilitySourceStateKind::SourceUnavailable
            | RoleCapabilitySourceStateKind::SourceUnrecognized
    )
}

fn hide_on_redaction<T>(redacted: bool, value: Option<T>) -> Option<T> {
    if redacted { None } else { value }
}

fn maybe_hide_on_redaction<T: Clone>(
    access: &IdentityVisibilityAccessSummary,
    value: Option<T>,
) -> Option<T> {
    hide_on_redaction(
        matches!(access.access_state, IdentityVisibilityAccessState::Redacted),
        value,
    )
}

fn maybe_hide_vec_on_redaction<T: Clone>(
    access: &IdentityVisibilityAccessSummary,
    value: Vec<T>,
) -> Vec<T> {
    if matches!(access.access_state, IdentityVisibilityAccessState::Redacted) {
        Vec::new()
    } else {
        value
    }
}
