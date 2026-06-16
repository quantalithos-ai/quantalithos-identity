//! Shared query foundation helpers for visibility-first, read-only flows.

use core_contracts::actor::ActorRef;
use identity_contracts::metadata::{
    IdentityDegradedKind, IdentityQueryDisposition, IdentityQuerySurface,
};
use identity_contracts::protocol::IdentityQueryName;
use identity_contracts::queries::{
    GetGlobalLifecycleSummaryRequest, GetGlobalMemberAnchorRequest,
    GetRoleCapabilitySummaryRequest, IdentityPageResponse, IdentityPublicPageCursor,
    IdentityPublicPageInfo, IdentityPublicPageRequest, IdentityQueryRequest, IdentityQueryResponse,
    IdentityTraceReadSelector, ListCareerRecordsRequest, ListMemoryReferencesRequest,
    ReadAuditTrailRequest, ReadIdentityTraceRequest, ReadMemberSummaryRequest,
};
use identity_contracts::refs::{
    CareerRecordStateKind as PublicCareerRecordStateKind, ConsumerRef,
    GlobalLifecycleStateKind as PublicLifecycleStateKind, GlobalMemberRef,
    IdentityAnchorStateKind as PublicAnchorStateKind, IdentityReadSurfaceKind,
    MemoryReferenceStateKind as PublicMemoryReferenceStateKind,
    RoleCapabilitySourceStateKind as PublicRoleCapabilitySourceStateKind,
    RoleCapabilitySummaryStateKind as PublicRoleCapabilitySummaryStateKind, VisibilityContextRef,
};
use identity_contracts::views::{
    AuditTrailEntryView, CareerRecordView, GlobalLifecycleSummaryView, GlobalMemberAnchorView,
    IdentityReadMaterialKind, IdentityReadMaterialMarker, IdentityTraceRecordView,
    IdentityVisibilityAccessState, IdentityVisibilityAccessSummary, MemberSummaryView,
    MemoryReferenceView, RoleCapabilitySummaryView,
};
use identity_domain::career::CareerRecord;
use identity_domain::member_identity::IdentityAnchorState;
use identity_domain::memory_reference::{MemoryReference, MemoryReferenceStateKind};
use identity_domain::role_capability::{
    RoleCapabilitySourceSnapshot, RoleCapabilitySourceStateKind, RoleCapabilitySummaryStateKind,
};
use identity_domain::trace::IdentityTraceRecord;

use crate::errors::ApplicationError;
use crate::ports::{
    CareerRecordRepository, GlobalLifecycleRepository, GlobalMemberRepository,
    IdentityAuditTrailRepository, IdentityClockPort, IdentityIdGeneratorPort,
    IdentityOperationContextFactoryPort, IdentityProjectionRepository,
    IdentityQueryMaterialDegradationMapper, IdentityReadVisibilityRepository,
    IdentityTraceRecordRepository, IdentityTruthChangeSubjectMapper, IdentityUnitOfWorkManagerPort,
    MemoryReferenceRepository, RoleCapabilityRepository,
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
                access.degraded_marker_ref.clone().ok_or_else(|| {
                    ApplicationError::consistency_defect(
                        "stale-visible query surface requires a formal degraded marker",
                    )
                })?,
                access
                    .degraded_kind
                    .unwrap_or(IdentityDegradedKind::ProjectionStale),
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
