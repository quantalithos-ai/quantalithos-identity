//! Shared query foundation helpers for visibility-first, read-only flows.

use core_contracts::actor::ActorRef;
use identity_contracts::protocol::IdentityQueryName;
use identity_contracts::queries::IdentityQueryRequest;
use identity_contracts::refs::{
    ConsumerRef, GlobalMemberRef, IdentityReadSurfaceKind, VisibilityContextRef,
};
use identity_contracts::views::{IdentityVisibilityAccessState, IdentityVisibilityAccessSummary};

use crate::errors::ApplicationError;
use crate::ports::{
    IdentityClockPort, IdentityIdGeneratorPort, IdentityOperationContextFactoryPort,
    IdentityProjectionRepository, IdentityReadVisibilityRepository,
};
use crate::support::{
    IdentityOperationContext, IdentityOperationName, IdentityReadDispositionKind,
    IdentityRequestDigest, IdentityRequestMetadataRef,
};

/// Shared dependencies for read-only query foundation helpers.
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
}

/// Shared query service skeleton for visibility-first read helpers.
pub struct IdentityQueryService<'a> {
    deps: IdentityQueryServiceDeps<'a>,
}

impl<'a> IdentityQueryService<'a> {
    /// Creates a query foundation service from formal shared dependencies.
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
        let access = self
            .deps
            .read_visibility_repository
            .resolve_member_summary_read(
                member_ref.clone(),
                None,
                consumer_ref.clone(),
                visibility_context_ref.clone(),
            )?
            .ok_or_else(|| {
                ApplicationError::dependency_unavailable(
                    "member summary visibility access summary is unavailable",
                )
            })?;

        let view_ref = self
            .deps
            .projection_repository
            .find_member_summary_view_ref(member_ref.clone(), access.scope_ref.clone())?
            .ok_or_else(|| ApplicationError::not_found("member summary view not found"))?;

        let view_access = self
            .deps
            .read_visibility_repository
            .resolve_member_summary_read(
                member_ref.clone(),
                Some(view_ref.clone()),
                consumer_ref,
                visibility_context_ref,
            )?
            .ok_or_else(|| {
                ApplicationError::dependency_unavailable(
                    "member summary view visibility access summary is unavailable",
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

        if !view.matches_visibility_scope(&access.scope_ref) {
            return Err(ApplicationError::consistency_defect(
                "member summary view visibility scope does not match the stable lookup scope",
            ));
        }

        view.assert_body_free()
            .map_err(|err| ApplicationError::consistency_defect(err.message))?;

        Ok(IdentityMemberSummaryPreflight {
            access_summary: access,
            view_access_summary: view_access,
            view_ref,
            view,
        })
    }

    /// Classifies the stable disposition from the formal access summary and loaded view material.
    pub fn classify_member_summary_disposition(
        &self,
        access_summary: &IdentityVisibilityAccessSummary,
        view: &identity_contracts::views::MemberSummaryView,
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
            IdentityVisibilityAccessState::Redacted => IdentityReadDispositionKind::Redacted,
            IdentityVisibilityAccessState::NotVisible => IdentityReadDispositionKind::NotVisible,
            IdentityVisibilityAccessState::Degraded
            | IdentityVisibilityAccessState::Unavailable => IdentityReadDispositionKind::Degraded,
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
    pub view: identity_contracts::views::MemberSummaryView,
}
