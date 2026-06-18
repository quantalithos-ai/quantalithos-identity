//! API entry wiring for the identity workspace.

use identity_application::ports::{
    IdentityClockPort, IdentityDispatchTargetCatalogPort, IdentityIdGeneratorPort,
    IdentityOperationContextFactoryPort,
};
use identity_application::support::{
    IdentityApiDispatchResult, IdentityApiEntryContext, IdentityApiEntryValidation,
    IdentityEntryDispatchGuard, IdentityEntryValidationIssueRef, IdentityOperationName,
    IdentityRequestDigest, IdentityRuntimeAssemblyState,
};
use identity_application::{
    ApplicationError, DefaultIdentityDispatchTargetCatalog, IdentityApplicationCommandRequest,
    IdentityApplicationCommandResponse, IdentityApplicationFacade, IdentityApplicationQueryRequest,
    IdentityApplicationQueryResponse,
};

/// Result of one API entry dispatch attempt.
pub enum IdentityApiDispatchOutcome<T> {
    /// Validation failed before a target could be dispatched.
    PreDispatchFailure {
        validation: IdentityApiEntryValidation,
    },
    /// Runtime wiring exists but is not dispatchable yet.
    RuntimeUnavailable {
        validation: IdentityApiEntryValidation,
        dispatch: IdentityApiDispatchResult,
    },
    /// Dispatch preparation failed before the application facade was called.
    DispatchFailed {
        validation: IdentityApiEntryValidation,
        dispatch: Option<IdentityApiDispatchResult>,
        error: ApplicationError,
    },
    /// The application facade was called.
    Dispatched {
        validation: IdentityApiEntryValidation,
        dispatch: IdentityApiDispatchResult,
        response: Result<T, ApplicationError>,
    },
}

/// Command-specific API entry dispatch outcome.
pub type IdentityApiCommandDispatchOutcome =
    IdentityApiDispatchOutcome<IdentityApplicationCommandResponse>;

/// Query-specific API entry dispatch outcome.
pub type IdentityApiQueryDispatchOutcome =
    IdentityApiDispatchOutcome<IdentityApplicationQueryResponse>;

/// Minimal API entry adapter that maps entry metadata to the application facade.
pub struct IdentityApiEntryAdapter<'a> {
    facade: IdentityApplicationFacade<'a>,
    runtime_state: &'a IdentityRuntimeAssemblyState,
    clock: &'a dyn IdentityClockPort,
    id_generator: &'a dyn IdentityIdGeneratorPort,
    operation_context_factory: &'a dyn IdentityOperationContextFactoryPort,
    dispatch_target_catalog: &'a dyn IdentityDispatchTargetCatalogPort,
}

impl<'a> IdentityApiEntryAdapter<'a> {
    /// Creates one API entry adapter over the formal facade and entry support ports.
    pub fn new(
        facade: IdentityApplicationFacade<'a>,
        runtime_state: &'a IdentityRuntimeAssemblyState,
        clock: &'a dyn IdentityClockPort,
        id_generator: &'a dyn IdentityIdGeneratorPort,
        operation_context_factory: &'a dyn IdentityOperationContextFactoryPort,
        dispatch_target_catalog: &'a dyn IdentityDispatchTargetCatalogPort,
    ) -> Self {
        Self {
            facade,
            runtime_state,
            clock,
            id_generator,
            operation_context_factory,
            dispatch_target_catalog,
        }
    }

    /// Handles one formal command request through the shared application facade.
    pub fn handle_command(
        &self,
        entry_context: IdentityApiEntryContext,
        request: IdentityApplicationCommandRequest,
    ) -> IdentityApiCommandDispatchOutcome {
        let validation = match self.validate_command(&entry_context, &request) {
            Ok(validation) => validation,
            Err(validation) => {
                return IdentityApiDispatchOutcome::PreDispatchFailure { validation };
            }
        };
        let target_ref = match self
            .dispatch_target_catalog
            .api_command_target(entry_context.route_ref.clone())
        {
            Ok(target_ref) => target_ref,
            Err(_) => {
                return IdentityApiDispatchOutcome::PreDispatchFailure {
                    validation: IdentityApiEntryValidation::not_routable(
                        entry_context.api_entry_ref,
                        entry_context.route_ref,
                        vec![issue("api-command-not-routable")],
                    ),
                };
            }
        };
        let guard = IdentityEntryDispatchGuard::for_api(
            &entry_context,
            self.runtime_state,
            target_ref.clone(),
        );
        if let Err(issue_refs) = guard.validate() {
            if guard.assert_runtime_dispatchable().is_err() {
                return self.runtime_unavailable(validation, entry_context, target_ref, issue_refs);
            }
            return IdentityApiDispatchOutcome::PreDispatchFailure {
                validation: IdentityApiEntryValidation::rejected_at_entry(
                    entry_context.api_entry_ref,
                    entry_context.route_ref,
                    issue_refs,
                ),
            };
        }

        let dispatch_ref = match self.id_generator.new_identity_entry_dispatch_ref() {
            Ok(dispatch_ref) => dispatch_ref,
            Err(error) => {
                return IdentityApiDispatchOutcome::DispatchFailed {
                    validation,
                    dispatch: None,
                    error,
                };
            }
        };
        let context_ref = match self.id_generator.new_identity_operation_context_ref() {
            Ok(context_ref) => context_ref,
            Err(error) => {
                return IdentityApiDispatchOutcome::DispatchFailed {
                    validation,
                    dispatch: Some(IdentityApiDispatchResult::failed_before_application(
                        dispatch_ref,
                        entry_context.api_entry_ref,
                        target_ref,
                        vec![issue("api-command-context-ref-failed")],
                    )),
                    error,
                };
            }
        };
        let started_at = match self.clock.now() {
            Ok(started_at) => started_at,
            Err(error) => {
                return IdentityApiDispatchOutcome::DispatchFailed {
                    validation,
                    dispatch: Some(IdentityApiDispatchResult::failed_before_application(
                        dispatch_ref,
                        entry_context.api_entry_ref,
                        target_ref,
                        vec![issue("api-command-clock-failed")],
                    )),
                    error,
                };
            }
        };

        let command_name = command_name(&request);
        let trace_context_ref = command_trace_context_ref(&request);
        let idempotency_key = entry_context.idempotency_key.clone();
        let request_digest = command_digest(&request);
        let operation_context = match self.operation_context_factory.from_command(
            IdentityOperationName::new(command_name.as_str()),
            entry_context.actor_ref.clone(),
            entry_context.request_metadata_ref.clone(),
            idempotency_key,
            request_digest,
            trace_context_ref,
            context_ref,
            started_at,
        ) {
            Ok(context) => context,
            Err(error) => {
                return IdentityApiDispatchOutcome::DispatchFailed {
                    validation,
                    dispatch: Some(IdentityApiDispatchResult::failed_before_application(
                        dispatch_ref,
                        entry_context.api_entry_ref,
                        target_ref,
                        vec![issue("api-command-context-build-failed")],
                    )),
                    error,
                };
            }
        };

        let dispatch = IdentityApiDispatchResult::dispatched(
            dispatch_ref,
            entry_context.api_entry_ref,
            target_ref,
        );
        let response = self.facade.dispatch_command(operation_context, request);
        IdentityApiDispatchOutcome::Dispatched {
            validation,
            dispatch,
            response,
        }
    }

    /// Handles one formal query request through the shared application facade.
    pub fn handle_query(
        &self,
        entry_context: IdentityApiEntryContext,
        request: IdentityApplicationQueryRequest,
    ) -> IdentityApiQueryDispatchOutcome {
        let validation = match self.validate_query(&entry_context, &request) {
            Ok(validation) => validation,
            Err(validation) => {
                return IdentityApiDispatchOutcome::PreDispatchFailure { validation };
            }
        };
        let target_ref = match self
            .dispatch_target_catalog
            .api_query_target(entry_context.route_ref.clone())
        {
            Ok(target_ref) => target_ref,
            Err(_) => {
                return IdentityApiDispatchOutcome::PreDispatchFailure {
                    validation: IdentityApiEntryValidation::not_routable(
                        entry_context.api_entry_ref,
                        entry_context.route_ref,
                        vec![issue("api-query-not-routable")],
                    ),
                };
            }
        };
        let guard = IdentityEntryDispatchGuard::for_api(
            &entry_context,
            self.runtime_state,
            target_ref.clone(),
        );
        if let Err(issue_refs) = guard.validate() {
            if guard.assert_runtime_dispatchable().is_err() {
                return self.runtime_unavailable(validation, entry_context, target_ref, issue_refs);
            }
            return IdentityApiDispatchOutcome::PreDispatchFailure {
                validation: IdentityApiEntryValidation::rejected_at_entry(
                    entry_context.api_entry_ref,
                    entry_context.route_ref,
                    issue_refs,
                ),
            };
        }

        let dispatch_ref = match self.id_generator.new_identity_entry_dispatch_ref() {
            Ok(dispatch_ref) => dispatch_ref,
            Err(error) => {
                return IdentityApiDispatchOutcome::DispatchFailed {
                    validation,
                    dispatch: None,
                    error,
                };
            }
        };
        let context_ref = match self.id_generator.new_identity_operation_context_ref() {
            Ok(context_ref) => context_ref,
            Err(error) => {
                return IdentityApiDispatchOutcome::DispatchFailed {
                    validation,
                    dispatch: Some(IdentityApiDispatchResult::failed_before_application(
                        dispatch_ref,
                        entry_context.api_entry_ref,
                        target_ref,
                        vec![issue("api-query-context-ref-failed")],
                    )),
                    error,
                };
            }
        };
        let started_at = match self.clock.now() {
            Ok(started_at) => started_at,
            Err(error) => {
                return IdentityApiDispatchOutcome::DispatchFailed {
                    validation,
                    dispatch: Some(IdentityApiDispatchResult::failed_before_application(
                        dispatch_ref,
                        entry_context.api_entry_ref,
                        target_ref,
                        vec![issue("api-query-clock-failed")],
                    )),
                    error,
                };
            }
        };

        let query_name = query_name(&request);
        let trace_context_ref = query_trace_context_ref(&request);
        let request_digest = match query_digest(&request) {
            Ok(request_digest) => request_digest,
            Err(error) => {
                return IdentityApiDispatchOutcome::DispatchFailed {
                    validation,
                    dispatch: Some(IdentityApiDispatchResult::failed_before_application(
                        dispatch_ref,
                        entry_context.api_entry_ref,
                        target_ref,
                        vec![issue("api-query-digest-build-failed")],
                    )),
                    error,
                };
            }
        };
        let operation_context = match self.operation_context_factory.from_query(
            IdentityOperationName::new(query_name.as_str()),
            entry_context.actor_ref.clone(),
            entry_context.request_metadata_ref.clone(),
            request_digest,
            trace_context_ref,
            context_ref,
            started_at,
        ) {
            Ok(context) => context,
            Err(error) => {
                return IdentityApiDispatchOutcome::DispatchFailed {
                    validation,
                    dispatch: Some(IdentityApiDispatchResult::failed_before_application(
                        dispatch_ref,
                        entry_context.api_entry_ref,
                        target_ref,
                        vec![issue("api-query-context-build-failed")],
                    )),
                    error,
                };
            }
        };

        let dispatch = IdentityApiDispatchResult::dispatched(
            dispatch_ref,
            entry_context.api_entry_ref,
            target_ref,
        );
        let response = self.facade.dispatch_query(operation_context, request);
        IdentityApiDispatchOutcome::Dispatched {
            validation,
            dispatch,
            response,
        }
    }

    fn validate_command(
        &self,
        entry_context: &IdentityApiEntryContext,
        request: &IdentityApplicationCommandRequest,
    ) -> Result<IdentityApiEntryValidation, IdentityApiEntryValidation> {
        let mut issue_refs = Vec::new();
        let command_name = command_name(request);
        if entry_context.surface_kind
            != identity_application::support::IdentityEntrySurfaceKind::ApiCommand
        {
            issue_refs.push(issue("api-command-surface-mismatch"));
        }
        if entry_context.route_ref
            != DefaultIdentityDispatchTargetCatalog::api_command_route_ref(&command_name)
        {
            issue_refs.push(issue("api-command-route-mismatch"));
        }
        if command_expected_name(request) != command_name.as_str() {
            issue_refs.push(issue("api-command-name-mismatch"));
        }
        if command_actor_ref(request) != &entry_context.actor_ref {
            issue_refs.push(issue("api-command-actor-mismatch"));
        }
        if command_request_marker_ref(request) != &entry_context.request_marker_ref {
            issue_refs.push(issue("api-command-request-marker-mismatch"));
        }
        match &entry_context.idempotency_key {
            Some(key) if key.as_public() == command_idempotency_key(request) => {}
            Some(_) => issue_refs.push(issue("api-command-idempotency-key-mismatch")),
            None => issue_refs.push(issue("api-command-missing-idempotency-key")),
        }
        if command_schema_version_ref(request) != command_digest_schema_version_ref(request) {
            issue_refs.push(issue("api-command-schema-version-mismatch"));
        }
        if issue_refs.is_empty() {
            Ok(IdentityApiEntryValidation::dispatchable(
                entry_context.api_entry_ref.clone(),
                entry_context.route_ref.clone(),
            ))
        } else {
            Err(IdentityApiEntryValidation::rejected_at_entry(
                entry_context.api_entry_ref.clone(),
                entry_context.route_ref.clone(),
                issue_refs,
            ))
        }
    }

    fn validate_query(
        &self,
        entry_context: &IdentityApiEntryContext,
        request: &IdentityApplicationQueryRequest,
    ) -> Result<IdentityApiEntryValidation, IdentityApiEntryValidation> {
        let mut issue_refs = Vec::new();
        let query_name = query_name(request);
        if entry_context.surface_kind
            != identity_application::support::IdentityEntrySurfaceKind::ApiQuery
        {
            issue_refs.push(issue("api-query-surface-mismatch"));
        }
        if entry_context.route_ref
            != DefaultIdentityDispatchTargetCatalog::api_query_route_ref(&query_name)
        {
            issue_refs.push(issue("api-query-route-mismatch"));
        }
        if query_expected_name(request) != query_name.as_str() {
            issue_refs.push(issue("api-query-name-mismatch"));
        }
        if query_actor_ref(request) != &entry_context.actor_ref {
            issue_refs.push(issue("api-query-actor-mismatch"));
        }
        if query_request_marker_ref(request) != &entry_context.request_marker_ref {
            issue_refs.push(issue("api-query-request-marker-mismatch"));
        }
        match &entry_context.visibility_context_ref {
            Some(visibility_context_ref)
                if visibility_context_ref == query_visibility_context_ref(request) => {}
            Some(_) => issue_refs.push(issue("api-query-visibility-context-mismatch")),
            None => issue_refs.push(issue("api-query-missing-visibility-context")),
        }
        if query_requires_page(request) && query_page(request).is_none() {
            issue_refs.push(issue("api-query-missing-page"));
        }
        if issue_refs.is_empty() {
            Ok(IdentityApiEntryValidation::dispatchable(
                entry_context.api_entry_ref.clone(),
                entry_context.route_ref.clone(),
            ))
        } else {
            Err(IdentityApiEntryValidation::rejected_at_entry(
                entry_context.api_entry_ref.clone(),
                entry_context.route_ref.clone(),
                issue_refs,
            ))
        }
    }

    fn runtime_unavailable<T>(
        &self,
        _validation: IdentityApiEntryValidation,
        entry_context: IdentityApiEntryContext,
        target_ref: identity_application::support::IdentityDispatchTargetRef,
        issue_refs: Vec<IdentityEntryValidationIssueRef>,
    ) -> IdentityApiDispatchOutcome<T> {
        let validation = IdentityApiEntryValidation::runtime_unavailable(
            entry_context.api_entry_ref.clone(),
            entry_context.route_ref.clone(),
            issue_refs.clone(),
        );
        let dispatch = match self.id_generator.new_identity_entry_dispatch_ref() {
            Ok(dispatch_ref) => IdentityApiDispatchResult::skipped_runtime_unavailable(
                dispatch_ref,
                entry_context.api_entry_ref,
                target_ref,
                issue_refs,
            ),
            Err(error) => {
                return IdentityApiDispatchOutcome::DispatchFailed {
                    validation,
                    dispatch: None,
                    error,
                };
            }
        };
        IdentityApiDispatchOutcome::RuntimeUnavailable {
            validation,
            dispatch,
        }
    }
}

macro_rules! with_command_request {
    ($request:expr, $binding:ident => $body:expr) => {
        match $request {
            IdentityApplicationCommandRequest::EstablishGlobalMember($binding) => $body,
            IdentityApplicationCommandRequest::UpdateGlobalLifecycleState($binding) => $body,
            IdentityApplicationCommandRequest::MaintainRoleCapabilitySummary($binding) => $body,
            IdentityApplicationCommandRequest::AppendCareerRecord($binding) => $body,
            IdentityApplicationCommandRequest::MaintainMemoryReference($binding) => $body,
            IdentityApplicationCommandRequest::PrepareTraceHandoff($binding) => $body,
        }
    };
}

macro_rules! with_query_request {
    ($request:expr, $binding:ident => $body:expr) => {
        match $request {
            IdentityApplicationQueryRequest::GetGlobalMemberAnchor($binding) => $body,
            IdentityApplicationQueryRequest::GetGlobalLifecycleSummary($binding) => $body,
            IdentityApplicationQueryRequest::GetRoleCapabilitySummary($binding) => $body,
            IdentityApplicationQueryRequest::ListCareerRecords($binding) => $body,
            IdentityApplicationQueryRequest::ListMemoryReferences($binding) => $body,
            IdentityApplicationQueryRequest::ReadMemberSummary($binding) => $body,
            IdentityApplicationQueryRequest::ReadIdentityTrace($binding) => $body,
            IdentityApplicationQueryRequest::ReadAuditTrail($binding) => $body,
            IdentityApplicationQueryRequest::GetProjectionState($binding) => $body,
            IdentityApplicationQueryRequest::GetReferenceResolutionState($binding) => $body,
            IdentityApplicationQueryRequest::ReadReconciliationReport($binding) => $body,
            IdentityApplicationQueryRequest::ListPendingIdentityOutbox($binding) => $body,
            IdentityApplicationQueryRequest::GetIdentityOutboxState($binding) => $body,
            IdentityApplicationQueryRequest::GetTraceHandoffState($binding) => $body,
        }
    };
}

fn command_name(
    request: &IdentityApplicationCommandRequest,
) -> identity_contracts::protocol::IdentityCommandName {
    with_command_request!(request, inner => inner.command_name.clone())
}

fn command_expected_name(request: &IdentityApplicationCommandRequest) -> &'static str {
    match request {
        IdentityApplicationCommandRequest::EstablishGlobalMember(_) => "EstablishGlobalMember",
        IdentityApplicationCommandRequest::UpdateGlobalLifecycleState(_) => {
            "UpdateGlobalLifecycleState"
        }
        IdentityApplicationCommandRequest::MaintainRoleCapabilitySummary(_) => {
            "MaintainRoleCapabilitySummary"
        }
        IdentityApplicationCommandRequest::AppendCareerRecord(_) => "AppendCareerRecord",
        IdentityApplicationCommandRequest::MaintainMemoryReference(_) => "MaintainMemoryReference",
        IdentityApplicationCommandRequest::PrepareTraceHandoff(_) => "PrepareTraceHandoff",
    }
}

fn command_actor_ref(
    request: &IdentityApplicationCommandRequest,
) -> &core_contracts::actor::ActorRef {
    with_command_request!(request, inner => &inner.actor_ref)
}

fn command_request_marker_ref(
    request: &IdentityApplicationCommandRequest,
) -> &identity_contracts::refs::IdentityApiRequestMarkerRef {
    with_command_request!(request, inner => &inner.metadata.request_marker_ref)
}

fn command_idempotency_key(
    request: &IdentityApplicationCommandRequest,
) -> &core_contracts::metadata::IdempotencyKey {
    with_command_request!(request, inner => &inner.metadata.idempotency_key)
}

fn command_schema_version_ref(
    request: &IdentityApplicationCommandRequest,
) -> &identity_contracts::protocol::IdentityProtocolSchemaVersionRef {
    with_command_request!(request, inner => &inner.metadata.schema_version_ref)
}

fn command_digest_schema_version_ref(
    request: &IdentityApplicationCommandRequest,
) -> &identity_contracts::protocol::IdentityProtocolSchemaVersionRef {
    with_command_request!(request, inner => &inner.digest.schema_version_ref)
}

fn command_trace_context_ref(
    request: &IdentityApplicationCommandRequest,
) -> Option<identity_contracts::refs::IdentityTraceContextRef> {
    with_command_request!(request, inner => inner.metadata.trace_context_ref.clone())
}

fn command_digest(request: &IdentityApplicationCommandRequest) -> IdentityRequestDigest {
    with_command_request!(request, inner => IdentityRequestDigest::from_canonical_marker(
        inner.digest.canonical_marker_ref.clone(),
        inner.digest.digest_value.clone(),
        inner.digest.schema_version_ref.clone(),
        inner.digest.algorithm_marker_ref.clone(),
    ))
}

fn query_name(
    request: &IdentityApplicationQueryRequest,
) -> identity_contracts::protocol::IdentityQueryName {
    with_query_request!(request, inner => inner.query_name.clone())
}

fn query_expected_name(request: &IdentityApplicationQueryRequest) -> &'static str {
    match request {
        IdentityApplicationQueryRequest::GetGlobalMemberAnchor(_) => "GetGlobalMemberAnchor",
        IdentityApplicationQueryRequest::GetGlobalLifecycleSummary(_) => {
            "GetGlobalLifecycleSummary"
        }
        IdentityApplicationQueryRequest::GetRoleCapabilitySummary(_) => "GetRoleCapabilitySummary",
        IdentityApplicationQueryRequest::ListCareerRecords(_) => "ListCareerRecords",
        IdentityApplicationQueryRequest::ListMemoryReferences(_) => "ListMemoryReferences",
        IdentityApplicationQueryRequest::ReadMemberSummary(_) => "ReadMemberSummary",
        IdentityApplicationQueryRequest::ReadIdentityTrace(_) => "ReadIdentityTrace",
        IdentityApplicationQueryRequest::ReadAuditTrail(_) => "ReadAuditTrail",
        IdentityApplicationQueryRequest::GetProjectionState(_) => "GetProjectionState",
        IdentityApplicationQueryRequest::GetReferenceResolutionState(_) => {
            "GetReferenceResolutionState"
        }
        IdentityApplicationQueryRequest::ReadReconciliationReport(_) => "ReadReconciliationReport",
        IdentityApplicationQueryRequest::ListPendingIdentityOutbox(_) => {
            "ListPendingIdentityOutbox"
        }
        IdentityApplicationQueryRequest::GetIdentityOutboxState(_) => "GetIdentityOutboxState",
        IdentityApplicationQueryRequest::GetTraceHandoffState(_) => "GetTraceHandoffState",
    }
}

fn query_actor_ref(request: &IdentityApplicationQueryRequest) -> &core_contracts::actor::ActorRef {
    with_query_request!(request, inner => &inner.actor_ref)
}

fn query_request_marker_ref(
    request: &IdentityApplicationQueryRequest,
) -> &identity_contracts::refs::IdentityApiRequestMarkerRef {
    with_query_request!(request, inner => &inner.metadata.request_marker_ref)
}

fn query_visibility_context_ref(
    request: &IdentityApplicationQueryRequest,
) -> &identity_contracts::refs::VisibilityContextRef {
    with_query_request!(request, inner => &inner.metadata.visibility_context_ref)
}

fn query_page(
    request: &IdentityApplicationQueryRequest,
) -> Option<&identity_contracts::queries::IdentityPublicPageRequest> {
    with_query_request!(request, inner => inner.page.as_ref())
}

fn query_requires_page(request: &IdentityApplicationQueryRequest) -> bool {
    matches!(
        request,
        IdentityApplicationQueryRequest::ListCareerRecords(_)
            | IdentityApplicationQueryRequest::ListMemoryReferences(_)
            | IdentityApplicationQueryRequest::ReadIdentityTrace(_)
            | IdentityApplicationQueryRequest::ReadAuditTrail(_)
            | IdentityApplicationQueryRequest::ReadReconciliationReport(_)
            | IdentityApplicationQueryRequest::ListPendingIdentityOutbox(_)
    )
}

fn query_trace_context_ref(
    request: &IdentityApplicationQueryRequest,
) -> Option<identity_contracts::refs::IdentityTraceContextRef> {
    with_query_request!(request, inner => inner.metadata.trace_context_ref.clone())
}

fn query_digest(
    request: &IdentityApplicationQueryRequest,
) -> Result<IdentityRequestDigest, ApplicationError> {
    with_query_request!(request, inner => IdentityRequestDigest::from_entry_canonical_material(
        &format!("api-query:{}", inner.query_name.as_str()),
        inner.metadata.schema_version_ref.clone(),
        &(
            inner.query_name.as_str(),
            &inner.actor_ref,
            inner.metadata.schema_version_ref.as_str(),
            inner.page.as_ref(),
            &inner.body,
        ),
    ))
}

fn issue(code: &str) -> IdentityEntryValidationIssueRef {
    IdentityEntryValidationIssueRef::new(format!("api-entry:{code}"))
}

#[cfg(test)]
mod tests {
    use core_contracts::actor::{ActorKind, ActorRef};
    use core_contracts::metadata::IdempotencyKey;
    use identity_application::support::{
        IdentityApiEntryContext, IdentityApiEntryRef, IdentityIdempotencyKey,
        IdentityRequestMetadataRef,
    };
    use identity_application::{
        DefaultIdentityDispatchTargetCatalog, IdentityApplicationCommandResponse,
    };
    use identity_contracts::commands::{
        EstablishGlobalMemberRequest, IdentityCommandOutcome, IdentityCommandRequest,
    };
    use identity_contracts::metadata::{IdentityCommandMetadata, IdentityRequestDigestMarker};
    use identity_contracts::protocol::{IdentityCommandName, IdentityProtocolSchemaVersionRef};
    use identity_contracts::refs::{
        ExternalSourceRef, IdentityApiRequestMarkerRef, IdentityCanonicalRequestMarkerRef,
        IdentityRequestDigestValue, IdentitySourceOwner, IdentitySourceRef, IdentityTimestamp,
        LifecycleReasonKind,
    };
    use identity_infra::config::{
        IdentityConfigSourceKind, IdentityRuntimeConfigSources, IdentityRuntimeStartupConfig,
    };
    use identity_infra::{
        IdentityInMemoryRuntimeAssemblyBuilder, IdentityInMemoryRuntimeBuildOutcome,
    };

    use super::{IdentityApiCommandDispatchOutcome, IdentityApiEntryAdapter};

    fn build_assembly() -> identity_infra::IdentityInMemoryRuntimeAssembly {
        let defaults = IdentityRuntimeStartupConfig::from_strict_json(
            r#"{
              "profile": { "name": "ci-test", "adapter_mode_policy": "p0-safe", "allow_test_override": true },
              "store": { "mode": "in-memory", "dsn_ref": null, "migration": { "required_version": "identity-schema-p0" }, "transaction_mode": "single-uow", "idempotency": { "enabled": true }, "dead_letter": { "retention_days": 30 } },
              "actor_context": { "required": true, "require_trace_id": true, "trusted_context_profile": "trusted-upstream", "idempotency_key_required": true },
              "role_catalog": { "source_mode": "fake", "snapshot_ref": null, "fixture_ref": "fixture://identity/roles/p0", "fingerprint_required": true, "unknown_role_strategy": "reject-write" },
              "bus": { "publisher_mode": "fake", "endpoint_ref": null, "topic_map_ref": "fixture://identity/topic-map/p0", "require_known_event_kind": true },
              "outbox": { "store_name": "identity-outbox", "publish": { "batch_size": 50, "max_attempts": 5, "backoff_policy_ref": "retry:identity-outbox:p0", "failure_mode": "mark-failed-no-rollback" } },
              "projection": { "store_name": "member-summary-projection", "checkpoint_name": "member-summary", "rebuild": { "batch_size": 100 }, "query": { "not_ready_strategy": "return-not-ready" } },
              "operations": { "run_id_required": true, "replay": { "report_root_ref": null, "input_root_ref": null }, "propagation_retry": { "enabled": true } },
              "external_refs": {
                "artifact_evidence": { "mode": "disabled", "endpoint_ref": null },
                "memory_archive": { "mode": "disabled", "endpoint_ref": null },
                "governance_basis": { "mode": "disabled", "endpoint_ref": null },
                "work_source": { "mode": "disabled" },
                "trace_handoff": { "target_ref": null }
              },
              "audit": { "sink_mode": "captured", "sink_ref": null, "compensation_enabled": true, "redaction_profile": "identity-safe" },
              "redline": { "no_auth_in_identity": true, "ref_only_guard": true, "projection_no_write_guard": true, "outbox_no_event_creation_guard": true, "stored_replay_guard": true },
              "fixture": { "clock_mode": "fixed", "id_sequence_mode": "deterministic", "seed_ref": "fixture://identity/seeds/p0" }
            }"#,
            IdentityConfigSourceKind::CodeDefaults,
        )
        .expect("valid startup config");
        let builder = IdentityInMemoryRuntimeAssemblyBuilder::new(IdentityRuntimeConfigSources {
            code_defaults: defaults,
            config_file_json: None,
            environment_json: None,
        });
        match builder.build() {
            IdentityInMemoryRuntimeBuildOutcome::Ready(assembly) => assembly,
            IdentityInMemoryRuntimeBuildOutcome::Failed(state) => {
                panic!("runtime assembly failed: {:?}", state.state_kind)
            }
        }
    }

    fn source_ref() -> IdentitySourceRef {
        IdentitySourceRef::new(
            IdentitySourceOwner::Identity,
            ExternalSourceRef::new("source-1".to_owned()).expect("valid external source ref"),
        )
        .expect("valid source ref")
    }

    fn command_request() -> IdentityCommandRequest<EstablishGlobalMemberRequest> {
        IdentityCommandRequest {
            actor_ref: ActorRef::new("actor-1", ActorKind::Human),
            command_name: IdentityCommandName::new("EstablishGlobalMember"),
            metadata: IdentityCommandMetadata {
                idempotency_key: IdempotencyKey::new("idem-1".to_owned()),
                request_marker_ref: IdentityApiRequestMarkerRef::new("request-marker-1"),
                schema_version_ref: IdentityProtocolSchemaVersionRef::new("identity.command.v1"),
                trace_context_ref: None,
            },
            digest: IdentityRequestDigestMarker {
                canonical_marker_ref: IdentityCanonicalRequestMarkerRef::new("canonical-1"),
                digest_value: IdentityRequestDigestValue::new("digest-1"),
                schema_version_ref: IdentityProtocolSchemaVersionRef::new("identity.command.v1"),
                algorithm_marker_ref:
                    identity_contracts::protocol::IdentityDigestAlgorithmMarkerRef::new("sha256-v1"),
            },
            body: EstablishGlobalMemberRequest {
                requested_member_ref: None,
                source_ref: source_ref(),
                anchor_reason_ref: None,
                initial_lifecycle_reason_ref: identity_contracts::refs::LifecycleReasonRef::new(
                    LifecycleReasonKind::InitialProvisioned,
                    source_ref(),
                )
                .expect("valid lifecycle reason"),
            },
        }
    }

    fn command_entry_context() -> IdentityApiEntryContext {
        IdentityApiEntryContext {
            api_entry_ref: IdentityApiEntryRef::new("api-entry-1"),
            route_ref: DefaultIdentityDispatchTargetCatalog::api_command_route_ref(
                &IdentityCommandName::new("EstablishGlobalMember"),
            ),
            surface_kind: identity_application::support::IdentityEntrySurfaceKind::ApiCommand,
            request_marker_ref: IdentityApiRequestMarkerRef::new("request-marker-1"),
            actor_ref: ActorRef::new("actor-1", ActorKind::Human),
            request_metadata_ref: IdentityRequestMetadataRef::new("request-metadata-1"),
            idempotency_key: Some(IdentityIdempotencyKey::new(IdempotencyKey::new(
                "idem-1".to_owned(),
            ))),
            visibility_context_ref: None,
            received_at: IdentityTimestamp::from_clock(1).expect("valid timestamp"),
        }
    }

    #[test]
    fn command_entry_dispatches_through_the_application_facade() {
        let assembly = build_assembly();
        let catalog = DefaultIdentityDispatchTargetCatalog::identity_default();
        let adapter = IdentityApiEntryAdapter::new(
            assembly.application_facade(),
            assembly.assembly_state(),
            assembly.runtime(),
            assembly.runtime(),
            assembly.runtime(),
            &catalog,
        );

        let outcome = adapter.handle_command(
            command_entry_context(),
            identity_application::IdentityApplicationCommandRequest::EstablishGlobalMember(
                command_request(),
            ),
        );

        match outcome {
            IdentityApiCommandDispatchOutcome::Dispatched { response, .. } => {
                assert!(matches!(
                    response,
                    Ok(IdentityApplicationCommandResponse::EstablishGlobalMember(
                        IdentityCommandOutcome::Accepted(_)
                    ))
                ));
            }
            _ => panic!("expected dispatched command outcome"),
        }
    }

    #[test]
    fn command_entry_rejects_missing_idempotency_before_dispatch() {
        let assembly = build_assembly();
        let catalog = DefaultIdentityDispatchTargetCatalog::identity_default();
        let adapter = IdentityApiEntryAdapter::new(
            assembly.application_facade(),
            assembly.assembly_state(),
            assembly.runtime(),
            assembly.runtime(),
            assembly.runtime(),
            &catalog,
        );
        let mut entry_context = command_entry_context();
        entry_context.idempotency_key = None;

        let outcome = adapter.handle_command(
            entry_context,
            identity_application::IdentityApplicationCommandRequest::EstablishGlobalMember(
                command_request(),
            ),
        );

        match outcome {
            IdentityApiCommandDispatchOutcome::PreDispatchFailure { validation } => {
                assert_eq!(
                    validation.validation_kind,
                    identity_application::support::IdentityEntryValidationKind::RejectedAtEntry
                );
            }
            _ => panic!("expected pre-dispatch failure"),
        }
    }
}
