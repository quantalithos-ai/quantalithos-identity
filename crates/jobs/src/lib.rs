//! Operations job entry wiring for the identity workspace.

use identity_application::ports::{
    IdentityClockPort, IdentityDispatchTargetCatalogPort, IdentityIdGeneratorPort,
    IdentityOperationContextFactoryPort,
};
use identity_application::support::{
    IdentityEntryDispatchGuard, IdentityEntryValidationIssueRef, IdentityJobDispatchResult,
    IdentityJobEntryContext, IdentityJobEntryValidation, IdentityOperationName,
    IdentityRequestDigest, IdentityRequestMetadataRef, IdentityRuntimeAssemblyState,
};
use identity_application::{
    ApplicationError, IdentityApplicationFacade, IdentityApplicationJobRequest,
    IdentityApplicationJobResponse,
};

/// Result of one job entry dispatch attempt.
pub enum IdentityJobDispatchOutcome<T> {
    /// Validation failed before a target could be dispatched.
    PreDispatchFailure {
        validation: IdentityJobEntryValidation,
    },
    /// Runtime wiring exists but is not dispatchable yet.
    RuntimeUnavailable {
        validation: IdentityJobEntryValidation,
        dispatch: IdentityJobDispatchResult,
    },
    /// Dispatch preparation failed before the application facade was called.
    DispatchFailed {
        validation: IdentityJobEntryValidation,
        dispatch: Option<IdentityJobDispatchResult>,
        error: ApplicationError,
    },
    /// The application facade was called.
    Dispatched {
        validation: IdentityJobEntryValidation,
        dispatch: IdentityJobDispatchResult,
        response: Result<T, ApplicationError>,
    },
}

/// Job-runner dispatch outcome.
pub type IdentityOperationsJobDispatchOutcome =
    IdentityJobDispatchOutcome<IdentityApplicationJobResponse>;

/// Minimal operations-job entry adapter that maps entry metadata to the application facade.
pub struct IdentityJobsEntryAdapter<'a> {
    facade: IdentityApplicationFacade<'a>,
    runtime_state: &'a IdentityRuntimeAssemblyState,
    clock: &'a dyn IdentityClockPort,
    id_generator: &'a dyn IdentityIdGeneratorPort,
    operation_context_factory: &'a dyn IdentityOperationContextFactoryPort,
    dispatch_target_catalog: &'a dyn IdentityDispatchTargetCatalogPort,
}

impl<'a> IdentityJobsEntryAdapter<'a> {
    /// Creates one jobs entry adapter over the formal facade and entry support ports.
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

    /// Handles one formal operations job request.
    pub fn handle_job(
        &self,
        entry_context: IdentityJobEntryContext,
        request: IdentityApplicationJobRequest,
    ) -> IdentityOperationsJobDispatchOutcome {
        let validation = match self.validate_job(&entry_context, &request) {
            Ok(validation) => validation,
            Err(validation) => {
                return IdentityJobDispatchOutcome::PreDispatchFailure { validation };
            }
        };
        let target_ref = match self
            .dispatch_target_catalog
            .job_target(entry_context.job_name.clone())
        {
            Ok(target_ref) => target_ref,
            Err(_) => {
                return IdentityJobDispatchOutcome::PreDispatchFailure {
                    validation: IdentityJobEntryValidation::unknown_job(
                        entry_context.job_entry_ref,
                        entry_context.job_name,
                        vec![issue("jobs-not-routable")],
                    ),
                };
            }
        };
        let guard = IdentityEntryDispatchGuard::for_job(
            &entry_context,
            self.runtime_state,
            target_ref.clone(),
        );
        if let Err(issue_refs) = guard.validate() {
            if guard.assert_runtime_dispatchable().is_err() {
                return self.runtime_unavailable(validation, entry_context, target_ref, issue_refs);
            }
            return IdentityJobDispatchOutcome::PreDispatchFailure {
                validation: IdentityJobEntryValidation::invalid_scope(
                    entry_context.job_entry_ref,
                    entry_context.job_name,
                    issue_refs,
                ),
            };
        }

        let dispatch_ref = match self.id_generator.new_identity_job_dispatch_ref() {
            Ok(dispatch_ref) => dispatch_ref,
            Err(error) => {
                return IdentityJobDispatchOutcome::DispatchFailed {
                    validation,
                    dispatch: None,
                    error,
                };
            }
        };
        let context_ref = match self.id_generator.new_identity_operation_context_ref() {
            Ok(context_ref) => context_ref,
            Err(error) => {
                return IdentityJobDispatchOutcome::DispatchFailed {
                    validation,
                    dispatch: Some(IdentityJobDispatchResult::failed_before_application(
                        dispatch_ref,
                        entry_context.job_entry_ref,
                        target_ref,
                        vec![issue("jobs-context-ref-failed")],
                    )),
                    error,
                };
            }
        };
        let started_at = match self.clock.now() {
            Ok(started_at) => started_at,
            Err(error) => {
                return IdentityJobDispatchOutcome::DispatchFailed {
                    validation,
                    dispatch: Some(IdentityJobDispatchResult::failed_before_application(
                        dispatch_ref,
                        entry_context.job_entry_ref,
                        target_ref,
                        vec![issue("jobs-clock-failed")],
                    )),
                    error,
                };
            }
        };

        let request_metadata_ref = IdentityRequestMetadataRef::from_entry_marker(
            "job-run",
            entry_context.run_metadata_ref.as_str(),
        );
        let request_digest = match job_digest(&request) {
            Ok(request_digest) => request_digest,
            Err(error) => {
                return IdentityJobDispatchOutcome::DispatchFailed {
                    validation,
                    dispatch: Some(IdentityJobDispatchResult::failed_before_application(
                        dispatch_ref,
                        entry_context.job_entry_ref,
                        target_ref,
                        vec![issue("jobs-digest-build-failed")],
                    )),
                    error,
                };
            }
        };
        let operation_context = match self.operation_context_factory.from_job(
            IdentityOperationName::new(job_name(&request).as_str()),
            entry_context.system_actor_ref.clone(),
            request_metadata_ref,
            entry_context.idempotency_key.clone(),
            request_digest,
            None,
            entry_context.job_run_ref.clone(),
            context_ref,
            started_at,
        ) {
            Ok(context) => context,
            Err(error) => {
                return IdentityJobDispatchOutcome::DispatchFailed {
                    validation,
                    dispatch: Some(IdentityJobDispatchResult::failed_before_application(
                        dispatch_ref,
                        entry_context.job_entry_ref,
                        target_ref,
                        vec![issue("jobs-context-build-failed")],
                    )),
                    error,
                };
            }
        };

        let dispatch = IdentityJobDispatchResult::dispatched(
            dispatch_ref,
            entry_context.job_entry_ref,
            target_ref,
        );
        let response = self.facade.dispatch_job(operation_context, request);
        IdentityJobDispatchOutcome::Dispatched {
            validation,
            dispatch,
            response,
        }
    }

    fn validate_job(
        &self,
        entry_context: &IdentityJobEntryContext,
        request: &IdentityApplicationJobRequest,
    ) -> Result<IdentityJobEntryValidation, IdentityJobEntryValidation> {
        let mut issue_refs = Vec::new();
        if entry_context.surface_kind()
            != identity_application::support::IdentityEntrySurfaceKind::OperationsJob
        {
            issue_refs.push(issue("jobs-surface-mismatch"));
        }
        if entry_context.job_name != job_name(request) {
            issue_refs.push(issue("jobs-name-mismatch"));
        }
        if job_expected_name(request) != entry_context.job_name.as_str() {
            issue_refs.push(issue("jobs-public-name-mismatch"));
        }
        if entry_context.job_run_ref != job_run_ref(request) {
            issue_refs.push(issue("jobs-run-ref-mismatch"));
        }
        if entry_context.run_metadata_ref != job_run_metadata_ref(request) {
            issue_refs.push(issue("jobs-run-metadata-mismatch"));
        }
        if entry_context.scope_marker_ref != job_scope_marker_ref(request) {
            issue_refs.push(issue("jobs-scope-mismatch"));
        }
        if entry_context.input_cursor_ref != job_input_cursor_ref(request) {
            issue_refs.push(issue("jobs-cursor-mismatch"));
        }
        if entry_context.idempotency_key.as_public() != job_idempotency_key(request) {
            issue_refs.push(issue("jobs-idempotency-key-mismatch"));
        }
        if entry_context.system_actor_ref != job_system_actor_ref(request) {
            issue_refs.push(issue("jobs-system-actor-mismatch"));
        }
        if issue_refs.is_empty() {
            Ok(IdentityJobEntryValidation::dispatchable(
                entry_context.job_entry_ref.clone(),
                entry_context.job_name.clone(),
            ))
        } else if entry_context.input_cursor_ref != job_input_cursor_ref(request) {
            Err(IdentityJobEntryValidation::invalid_cursor(
                entry_context.job_entry_ref.clone(),
                entry_context.job_name.clone(),
                issue_refs,
            ))
        } else {
            Err(IdentityJobEntryValidation::invalid_scope(
                entry_context.job_entry_ref.clone(),
                entry_context.job_name.clone(),
                issue_refs,
            ))
        }
    }

    fn runtime_unavailable<T>(
        &self,
        _validation: IdentityJobEntryValidation,
        entry_context: IdentityJobEntryContext,
        target_ref: identity_application::support::IdentityDispatchTargetRef,
        issue_refs: Vec<IdentityEntryValidationIssueRef>,
    ) -> IdentityJobDispatchOutcome<T> {
        let validation = IdentityJobEntryValidation::runtime_unavailable(
            entry_context.job_entry_ref.clone(),
            entry_context.job_name.clone(),
            issue_refs.clone(),
        );
        let dispatch = match self.id_generator.new_identity_job_dispatch_ref() {
            Ok(dispatch_ref) => IdentityJobDispatchResult::skipped_runtime_unavailable(
                dispatch_ref,
                entry_context.job_entry_ref,
                target_ref,
                issue_refs,
            ),
            Err(error) => {
                return IdentityJobDispatchOutcome::DispatchFailed {
                    validation,
                    dispatch: None,
                    error,
                };
            }
        };
        IdentityJobDispatchOutcome::RuntimeUnavailable {
            validation,
            dispatch,
        }
    }
}

macro_rules! with_job_request {
    ($request:expr, $binding:ident => $body:expr) => {
        match $request {
            IdentityApplicationJobRequest::RebuildIdentityProjection($binding) => $body,
            IdentityApplicationJobRequest::RefreshExternalReferenceState($binding) => $body,
            IdentityApplicationJobRequest::RunIdentityReconciliation($binding) => $body,
            IdentityApplicationJobRequest::PublishIdentityOutbox($binding) => $body,
            IdentityApplicationJobRequest::DeliverTraceHandoff($binding) => $body,
            IdentityApplicationJobRequest::RetryIdentityPropagationFailures($binding) => $body,
        }
    };
}

fn job_name(
    request: &IdentityApplicationJobRequest,
) -> identity_contracts::protocol::IdentityJobName {
    with_job_request!(request, inner => inner.job_name.clone())
}

fn job_expected_name(request: &IdentityApplicationJobRequest) -> &'static str {
    match request {
        IdentityApplicationJobRequest::RebuildIdentityProjection(_) => "RebuildIdentityProjection",
        IdentityApplicationJobRequest::RefreshExternalReferenceState(_) => {
            "RefreshExternalReferenceState"
        }
        IdentityApplicationJobRequest::RunIdentityReconciliation(_) => "RunIdentityReconciliation",
        IdentityApplicationJobRequest::PublishIdentityOutbox(_) => "PublishIdentityOutbox",
        IdentityApplicationJobRequest::DeliverTraceHandoff(_) => "DeliverTraceHandoff",
        IdentityApplicationJobRequest::RetryIdentityPropagationFailures(_) => {
            "RetryIdentityPropagationFailures"
        }
    }
}

fn job_run_ref(
    request: &IdentityApplicationJobRequest,
) -> identity_contracts::refs::IdentityJobRunRef {
    with_job_request!(request, inner => inner.job_run_ref.clone())
}

fn job_run_metadata_ref(
    request: &IdentityApplicationJobRequest,
) -> identity_contracts::refs::IdentityJobRunMetadataRef {
    with_job_request!(request, inner => inner.run_metadata_ref.clone())
}

fn job_scope_marker_ref(
    request: &IdentityApplicationJobRequest,
) -> identity_contracts::refs::IdentityJobScopeMarkerRef {
    with_job_request!(request, inner => inner.scope_marker_ref.clone())
}

fn job_input_cursor_ref(
    request: &IdentityApplicationJobRequest,
) -> Option<identity_contracts::refs::IdentityJobCursorRef> {
    with_job_request!(request, inner => inner.input_cursor_ref.clone())
}

fn job_idempotency_key(
    request: &IdentityApplicationJobRequest,
) -> &core_contracts::metadata::IdempotencyKey {
    with_job_request!(request, inner => &inner.idempotency_key)
}

fn job_system_actor_ref(
    request: &IdentityApplicationJobRequest,
) -> core_contracts::actor::ActorRef {
    with_job_request!(request, inner => inner.system_actor_ref.clone())
}

fn job_digest(
    request: &IdentityApplicationJobRequest,
) -> Result<IdentityRequestDigest, ApplicationError> {
    with_job_request!(request, inner => IdentityRequestDigest::from_entry_canonical_material(
        &format!("operations-job:{}", inner.job_name.as_str()),
        inner.schema_version_ref.clone(),
        &(
            inner.job_name.as_str(),
            &inner.system_actor_ref,
            &inner.scope_marker_ref,
            inner.input_cursor_ref.as_ref(),
            inner.schema_version_ref.as_str(),
            &inner.input,
        ),
    ))
}

fn issue(code: &str) -> IdentityEntryValidationIssueRef {
    IdentityEntryValidationIssueRef::new(format!("jobs-entry:{code}"))
}

trait JobEntrySurfaceKindExt {
    fn surface_kind(&self) -> identity_application::support::IdentityEntrySurfaceKind;
}

impl JobEntrySurfaceKindExt for IdentityJobEntryContext {
    fn surface_kind(&self) -> identity_application::support::IdentityEntrySurfaceKind {
        identity_application::support::IdentityEntrySurfaceKind::OperationsJob
    }
}

#[cfg(test)]
mod tests {
    use core_contracts::actor::ActorRef;
    use core_contracts::metadata::IdempotencyKey;
    use identity_application::DefaultIdentityDispatchTargetCatalog;
    use identity_application::IdentityApplicationJobRequest;
    use identity_application::support::{
        IdentityIdempotencyKey, IdentityJobEntryContext, IdentityJobEntryRef,
    };
    use identity_contracts::jobs::{IdentityJobRequest, PublishIdentityOutboxJobInput};
    use identity_contracts::protocol::{IdentityJobName, IdentityProtocolSchemaVersionRef};
    use identity_contracts::queries::IdentityPublicPageRequest;
    use identity_contracts::refs::{
        IdentityJobRunMetadataRef, IdentityJobRunRef, IdentityJobScopeMarkerRef, IdentityTimestamp,
    };
    use identity_infra::config::{
        IdentityConfigSourceKind, IdentityRuntimeConfigSources, IdentityRuntimeStartupConfig,
    };
    use identity_infra::{
        IdentityInMemoryRuntimeAssemblyBuilder, IdentityInMemoryRuntimeBuildOutcome,
    };

    use super::{IdentityJobsEntryAdapter, IdentityOperationsJobDispatchOutcome};

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

    fn job_request() -> IdentityJobRequest<PublishIdentityOutboxJobInput> {
        IdentityJobRequest {
            job_name: IdentityJobName::new("PublishIdentityOutbox"),
            job_run_ref: IdentityJobRunRef::new("job-run-1"),
            run_metadata_ref: IdentityJobRunMetadataRef::new("job-run-metadata-1"),
            scope_marker_ref: IdentityJobScopeMarkerRef::new("job-scope-1"),
            idempotency_key: IdempotencyKey::new("job-idem-1".to_owned()),
            input_cursor_ref: None,
            schema_version_ref: IdentityProtocolSchemaVersionRef::new("identity.job.v1"),
            system_actor_ref: ActorRef::system("identity-job"),
            input: PublishIdentityOutboxJobInput {
                topic_key_ref: None,
                page: IdentityPublicPageRequest {
                    cursor: None,
                    limit: 20,
                },
            },
        }
    }

    fn job_entry_context() -> IdentityJobEntryContext {
        IdentityJobEntryContext {
            job_entry_ref: IdentityJobEntryRef::new("job-entry-1"),
            job_name: IdentityJobName::new("PublishIdentityOutbox"),
            job_run_ref: IdentityJobRunRef::new("job-run-1"),
            run_metadata_ref: IdentityJobRunMetadataRef::new("job-run-metadata-1"),
            scope_marker_ref: IdentityJobScopeMarkerRef::new("job-scope-1"),
            input_cursor_ref: None,
            system_actor_ref: ActorRef::system("identity-job"),
            idempotency_key: IdentityIdempotencyKey::new(IdempotencyKey::new(
                "job-idem-1".to_owned(),
            )),
            started_at: IdentityTimestamp::from_clock(1).expect("valid timestamp"),
        }
    }

    #[test]
    fn jobs_entry_dispatches_through_the_application_facade() {
        let assembly = build_assembly();
        let catalog = DefaultIdentityDispatchTargetCatalog::identity_default();
        let adapter = IdentityJobsEntryAdapter::new(
            assembly.application_facade(),
            assembly.assembly_state(),
            assembly.runtime(),
            assembly.runtime(),
            assembly.runtime(),
            &catalog,
        );

        let outcome = adapter.handle_job(
            job_entry_context(),
            IdentityApplicationJobRequest::PublishIdentityOutbox(job_request()),
        );

        match outcome {
            IdentityOperationsJobDispatchOutcome::Dispatched { response, .. } => {
                assert!(matches!(
                    response,
                    Ok(
                        identity_application::IdentityApplicationJobResponse::PublishIdentityOutbox(
                            _
                        )
                    )
                ));
            }
            _ => panic!("expected dispatched job outcome"),
        }
    }

    #[test]
    fn jobs_entry_rejects_scope_mismatch_before_dispatch() {
        let assembly = build_assembly();
        let catalog = DefaultIdentityDispatchTargetCatalog::identity_default();
        let adapter = IdentityJobsEntryAdapter::new(
            assembly.application_facade(),
            assembly.assembly_state(),
            assembly.runtime(),
            assembly.runtime(),
            assembly.runtime(),
            &catalog,
        );
        let mut context = job_entry_context();
        context.scope_marker_ref = IdentityJobScopeMarkerRef::new("wrong-scope");

        let outcome = adapter.handle_job(
            context,
            IdentityApplicationJobRequest::PublishIdentityOutbox(job_request()),
        );

        match outcome {
            IdentityOperationsJobDispatchOutcome::PreDispatchFailure { validation } => {
                assert_eq!(
                    validation.validation_kind,
                    identity_application::support::IdentityJobEntryValidationKind::InvalidScope
                );
            }
            _ => panic!("expected pre-dispatch failure"),
        }
    }
}
