//! Worker consumer and callback entry wiring for the identity workspace.

use core_contracts::actor::ActorRef;
use identity_application::ports::{
    IdentityClockPort, IdentityDispatchTargetCatalogPort, IdentityIdGeneratorPort,
    IdentityOperationContextFactoryPort,
};
use identity_application::support::{
    IdentityEntryDispatchGuard, IdentityEntryValidationIssueRef, IdentityOperationName,
    IdentityRequestDigest, IdentityRequestMetadataRef, IdentityRuntimeAssemblyState,
    IdentityWorkerDispatchResult, IdentityWorkerEntryContext, IdentityWorkerEntryValidation,
};
use identity_application::{
    ApplicationError, DefaultIdentityDispatchTargetCatalog, IdentityApplicationCallbackRequest,
    IdentityApplicationFacade, IdentityApplicationInboundEventRequest,
};
use identity_contracts::events::IdentityConsumerReceipt;

/// Result of one worker entry dispatch attempt.
pub enum IdentityWorkerDispatchOutcome<T> {
    /// Validation failed before a target could be dispatched.
    PreDispatchFailure {
        validation: IdentityWorkerEntryValidation,
    },
    /// Runtime wiring exists but is not dispatchable yet.
    RuntimeUnavailable {
        validation: IdentityWorkerEntryValidation,
        dispatch: IdentityWorkerDispatchResult,
    },
    /// Dispatch preparation failed before the application facade was called.
    DispatchFailed {
        validation: IdentityWorkerEntryValidation,
        dispatch: Option<IdentityWorkerDispatchResult>,
        error: ApplicationError,
    },
    /// The application facade was called.
    Dispatched {
        validation: IdentityWorkerEntryValidation,
        dispatch: IdentityWorkerDispatchResult,
        response: Result<T, ApplicationError>,
    },
}

/// Inbound-event dispatch outcome.
pub type IdentityInboundEventDispatchOutcome =
    IdentityWorkerDispatchOutcome<IdentityConsumerReceipt>;

/// Callback dispatch outcome.
pub type IdentityCallbackDispatchOutcome = IdentityWorkerDispatchOutcome<IdentityConsumerReceipt>;

/// Minimal worker entry adapter that maps validated envelopes to the application facade.
pub struct IdentityWorkerEntryAdapter<'a> {
    facade: IdentityApplicationFacade<'a>,
    runtime_state: &'a IdentityRuntimeAssemblyState,
    clock: &'a dyn IdentityClockPort,
    id_generator: &'a dyn IdentityIdGeneratorPort,
    operation_context_factory: &'a dyn IdentityOperationContextFactoryPort,
    dispatch_target_catalog: &'a dyn IdentityDispatchTargetCatalogPort,
}

impl<'a> IdentityWorkerEntryAdapter<'a> {
    /// Creates one worker entry adapter over the formal facade and entry support ports.
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

    /// Handles one formal inbound-event envelope.
    pub fn handle_inbound_event(
        &self,
        entry_context: IdentityWorkerEntryContext,
        request: IdentityApplicationInboundEventRequest,
    ) -> IdentityInboundEventDispatchOutcome {
        let validation = match self.validate_inbound_event(&entry_context, &request) {
            Ok(validation) => validation,
            Err(validation) => {
                return IdentityWorkerDispatchOutcome::PreDispatchFailure { validation };
            }
        };
        let target_ref = match self
            .dispatch_target_catalog
            .worker_consumer_target(entry_context.consumer_binding_ref.clone())
        {
            Ok(target_ref) => target_ref,
            Err(_) => {
                return IdentityWorkerDispatchOutcome::PreDispatchFailure {
                    validation: IdentityWorkerEntryValidation::unrecognized_binding(
                        entry_context.worker_entry_ref,
                        entry_context.consumer_binding_ref,
                        vec![issue("worker-consumer-not-routable")],
                    ),
                };
            }
        };
        let guard = IdentityEntryDispatchGuard::for_worker(
            &entry_context,
            self.runtime_state,
            target_ref.clone(),
        );
        if let Err(issue_refs) = guard.validate() {
            if guard.assert_runtime_dispatchable().is_err() {
                return self.runtime_unavailable(validation, entry_context, target_ref, issue_refs);
            }
            return IdentityWorkerDispatchOutcome::PreDispatchFailure {
                validation: IdentityWorkerEntryValidation::invalid_envelope_marker(
                    entry_context.worker_entry_ref,
                    entry_context.consumer_binding_ref,
                    issue_refs,
                ),
            };
        }

        let dispatch_ref = match self.id_generator.new_identity_worker_dispatch_ref() {
            Ok(dispatch_ref) => dispatch_ref,
            Err(error) => {
                return IdentityWorkerDispatchOutcome::DispatchFailed {
                    validation,
                    dispatch: None,
                    error,
                };
            }
        };
        let context_ref = match self.id_generator.new_identity_operation_context_ref() {
            Ok(context_ref) => context_ref,
            Err(error) => {
                return IdentityWorkerDispatchOutcome::DispatchFailed {
                    validation,
                    dispatch: Some(IdentityWorkerDispatchResult::failed_before_application(
                        dispatch_ref,
                        entry_context.worker_entry_ref,
                        target_ref,
                        vec![issue("worker-consumer-context-ref-failed")],
                    )),
                    error,
                };
            }
        };
        let started_at = match self.clock.now() {
            Ok(started_at) => started_at,
            Err(error) => {
                return IdentityWorkerDispatchOutcome::DispatchFailed {
                    validation,
                    dispatch: Some(IdentityWorkerDispatchResult::failed_before_application(
                        dispatch_ref,
                        entry_context.worker_entry_ref,
                        target_ref,
                        vec![issue("worker-consumer-clock-failed")],
                    )),
                    error,
                };
            }
        };

        let request_metadata_ref = IdentityRequestMetadataRef::from_entry_marker(
            "worker-envelope",
            entry_context.envelope_marker_ref.as_str(),
        );
        let request_digest = match inbound_event_digest(&request) {
            Ok(request_digest) => request_digest,
            Err(error) => {
                return IdentityWorkerDispatchOutcome::DispatchFailed {
                    validation,
                    dispatch: Some(IdentityWorkerDispatchResult::failed_before_application(
                        dispatch_ref,
                        entry_context.worker_entry_ref,
                        target_ref,
                        vec![issue("worker-consumer-digest-build-failed")],
                    )),
                    error,
                };
            }
        };
        let operation_context = match self.operation_context_factory.from_inbound_event(
            IdentityOperationName::new(inbound_event_name(&request).as_str()),
            worker_actor(identity_application::support::IdentityEntrySurfaceKind::WorkerConsumer),
            request_metadata_ref,
            entry_context.idempotency_key.clone(),
            request_digest,
            entry_context.trace_context_ref.clone(),
            entry_context.source_event_ref.clone(),
            context_ref,
            started_at,
        ) {
            Ok(context) => context,
            Err(error) => {
                return IdentityWorkerDispatchOutcome::DispatchFailed {
                    validation,
                    dispatch: Some(IdentityWorkerDispatchResult::failed_before_application(
                        dispatch_ref,
                        entry_context.worker_entry_ref,
                        target_ref,
                        vec![issue("worker-consumer-context-build-failed")],
                    )),
                    error,
                };
            }
        };

        let dispatch = IdentityWorkerDispatchResult::dispatched(
            dispatch_ref,
            entry_context.worker_entry_ref,
            target_ref,
        );
        let response = self
            .facade
            .dispatch_inbound_event(operation_context, request);
        IdentityWorkerDispatchOutcome::Dispatched {
            validation,
            dispatch,
            response,
        }
    }

    /// Handles one formal callback envelope.
    pub fn handle_callback(
        &self,
        entry_context: IdentityWorkerEntryContext,
        request: IdentityApplicationCallbackRequest,
    ) -> IdentityCallbackDispatchOutcome {
        let validation = match self.validate_callback(&entry_context, &request) {
            Ok(validation) => validation,
            Err(validation) => {
                return IdentityWorkerDispatchOutcome::PreDispatchFailure { validation };
            }
        };
        let target_ref = match self
            .dispatch_target_catalog
            .worker_callback_target(entry_context.consumer_binding_ref.clone())
        {
            Ok(target_ref) => target_ref,
            Err(_) => {
                return IdentityWorkerDispatchOutcome::PreDispatchFailure {
                    validation: IdentityWorkerEntryValidation::unrecognized_binding(
                        entry_context.worker_entry_ref,
                        entry_context.consumer_binding_ref,
                        vec![issue("worker-callback-not-routable")],
                    ),
                };
            }
        };
        let guard = IdentityEntryDispatchGuard::for_worker(
            &entry_context,
            self.runtime_state,
            target_ref.clone(),
        );
        if let Err(issue_refs) = guard.validate() {
            if guard.assert_runtime_dispatchable().is_err() {
                return self.runtime_unavailable(validation, entry_context, target_ref, issue_refs);
            }
            return IdentityWorkerDispatchOutcome::PreDispatchFailure {
                validation: IdentityWorkerEntryValidation::invalid_envelope_marker(
                    entry_context.worker_entry_ref,
                    entry_context.consumer_binding_ref,
                    issue_refs,
                ),
            };
        }

        let dispatch_ref = match self.id_generator.new_identity_worker_dispatch_ref() {
            Ok(dispatch_ref) => dispatch_ref,
            Err(error) => {
                return IdentityWorkerDispatchOutcome::DispatchFailed {
                    validation,
                    dispatch: None,
                    error,
                };
            }
        };
        let context_ref = match self.id_generator.new_identity_operation_context_ref() {
            Ok(context_ref) => context_ref,
            Err(error) => {
                return IdentityWorkerDispatchOutcome::DispatchFailed {
                    validation,
                    dispatch: Some(IdentityWorkerDispatchResult::failed_before_application(
                        dispatch_ref,
                        entry_context.worker_entry_ref,
                        target_ref,
                        vec![issue("worker-callback-context-ref-failed")],
                    )),
                    error,
                };
            }
        };
        let started_at = match self.clock.now() {
            Ok(started_at) => started_at,
            Err(error) => {
                return IdentityWorkerDispatchOutcome::DispatchFailed {
                    validation,
                    dispatch: Some(IdentityWorkerDispatchResult::failed_before_application(
                        dispatch_ref,
                        entry_context.worker_entry_ref,
                        target_ref,
                        vec![issue("worker-callback-clock-failed")],
                    )),
                    error,
                };
            }
        };

        let request_metadata_ref = IdentityRequestMetadataRef::from_entry_marker(
            "worker-envelope",
            entry_context.envelope_marker_ref.as_str(),
        );
        let request_digest = match callback_digest(&request) {
            Ok(request_digest) => request_digest,
            Err(error) => {
                return IdentityWorkerDispatchOutcome::DispatchFailed {
                    validation,
                    dispatch: Some(IdentityWorkerDispatchResult::failed_before_application(
                        dispatch_ref,
                        entry_context.worker_entry_ref,
                        target_ref,
                        vec![issue("worker-callback-digest-build-failed")],
                    )),
                    error,
                };
            }
        };
        let operation_context = match self.operation_context_factory.from_handoff_callback(
            IdentityOperationName::new(callback_name(&request).as_str()),
            worker_actor(identity_application::support::IdentityEntrySurfaceKind::WorkerCallback),
            request_metadata_ref,
            entry_context.idempotency_key.clone(),
            request_digest,
            entry_context.trace_context_ref.clone(),
            entry_context.source_event_ref.clone(),
            context_ref,
            started_at,
        ) {
            Ok(context) => context,
            Err(error) => {
                return IdentityWorkerDispatchOutcome::DispatchFailed {
                    validation,
                    dispatch: Some(IdentityWorkerDispatchResult::failed_before_application(
                        dispatch_ref,
                        entry_context.worker_entry_ref,
                        target_ref,
                        vec![issue("worker-callback-context-build-failed")],
                    )),
                    error,
                };
            }
        };

        let dispatch = IdentityWorkerDispatchResult::dispatched(
            dispatch_ref,
            entry_context.worker_entry_ref,
            target_ref,
        );
        let response = self.facade.dispatch_callback(operation_context, request);
        IdentityWorkerDispatchOutcome::Dispatched {
            validation,
            dispatch,
            response,
        }
    }

    fn validate_inbound_event(
        &self,
        entry_context: &IdentityWorkerEntryContext,
        request: &IdentityApplicationInboundEventRequest,
    ) -> Result<IdentityWorkerEntryValidation, IdentityWorkerEntryValidation> {
        self.validate_worker_request(
            entry_context,
            inbound_event_name(request).as_str(),
            inbound_event_binding_ref(request),
            inbound_event_envelope_marker_ref(request),
            inbound_event_source_event_ref(request),
            inbound_event_idempotency_key(request),
            inbound_event_trace_context_ref(request),
            identity_application::support::IdentityEntrySurfaceKind::WorkerConsumer,
        )
    }

    fn validate_callback(
        &self,
        entry_context: &IdentityWorkerEntryContext,
        request: &IdentityApplicationCallbackRequest,
    ) -> Result<IdentityWorkerEntryValidation, IdentityWorkerEntryValidation> {
        self.validate_worker_request(
            entry_context,
            callback_name(request).as_str(),
            callback_binding_ref(request),
            callback_envelope_marker_ref(request),
            callback_source_event_ref(request),
            callback_idempotency_key(request),
            callback_trace_context_ref(request),
            identity_application::support::IdentityEntrySurfaceKind::WorkerCallback,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn validate_worker_request(
        &self,
        entry_context: &IdentityWorkerEntryContext,
        expected_name: &str,
        binding_ref: &identity_contracts::refs::IdentityConsumerBindingRef,
        envelope_marker_ref: &identity_contracts::refs::IdentityEventEnvelopeMarkerRef,
        source_event_ref: &identity_contracts::refs::IdentitySourceEventRef,
        idempotency_key: &core_contracts::metadata::IdempotencyKey,
        trace_context_ref: &Option<identity_contracts::refs::IdentityTraceContextRef>,
        expected_surface: identity_application::support::IdentityEntrySurfaceKind,
    ) -> Result<IdentityWorkerEntryValidation, IdentityWorkerEntryValidation> {
        let mut issue_refs = Vec::new();
        if entry_context.surface_kind != expected_surface {
            issue_refs.push(issue("worker-surface-mismatch"));
        }
        if entry_context.consumer_binding_ref != *binding_ref {
            issue_refs.push(issue("worker-binding-mismatch"));
        }
        if entry_context.envelope_marker_ref != *envelope_marker_ref {
            issue_refs.push(issue("worker-envelope-marker-mismatch"));
        }
        if entry_context.source_event_ref != *source_event_ref {
            issue_refs.push(issue("worker-source-event-mismatch"));
        }
        if entry_context.idempotency_key.as_public() != idempotency_key {
            issue_refs.push(issue("worker-idempotency-key-mismatch"));
        }
        if &entry_context.trace_context_ref != trace_context_ref {
            issue_refs.push(issue("worker-trace-context-mismatch"));
        }
        let expected_binding_ref = match expected_surface {
            identity_application::support::IdentityEntrySurfaceKind::WorkerConsumer => {
                DefaultIdentityDispatchTargetCatalog::worker_consumer_binding_ref(
                    &identity_contracts::protocol::IdentityInboundConsumerName::new(expected_name),
                )
            }
            identity_application::support::IdentityEntrySurfaceKind::WorkerCallback => {
                DefaultIdentityDispatchTargetCatalog::worker_callback_binding_ref(
                    &identity_contracts::protocol::IdentityInboundConsumerName::new(expected_name),
                )
            }
            _ => unreachable!(),
        };
        if entry_context.consumer_binding_ref != expected_binding_ref {
            issue_refs.push(issue("worker-binding-catalog-mismatch"));
        }
        if issue_refs.is_empty() {
            Ok(IdentityWorkerEntryValidation::dispatchable(
                entry_context.worker_entry_ref.clone(),
                entry_context.consumer_binding_ref.clone(),
            ))
        } else {
            Err(IdentityWorkerEntryValidation::invalid_envelope_marker(
                entry_context.worker_entry_ref.clone(),
                entry_context.consumer_binding_ref.clone(),
                issue_refs,
            ))
        }
    }

    fn runtime_unavailable<T>(
        &self,
        _validation: IdentityWorkerEntryValidation,
        entry_context: IdentityWorkerEntryContext,
        target_ref: identity_application::support::IdentityDispatchTargetRef,
        issue_refs: Vec<IdentityEntryValidationIssueRef>,
    ) -> IdentityWorkerDispatchOutcome<T> {
        let validation = IdentityWorkerEntryValidation::runtime_unavailable(
            entry_context.worker_entry_ref.clone(),
            entry_context.consumer_binding_ref.clone(),
            issue_refs.clone(),
        );
        let dispatch = match self.id_generator.new_identity_worker_dispatch_ref() {
            Ok(dispatch_ref) => IdentityWorkerDispatchResult::skipped_runtime_unavailable(
                dispatch_ref,
                entry_context.worker_entry_ref,
                target_ref,
                issue_refs,
            ),
            Err(error) => {
                return IdentityWorkerDispatchOutcome::DispatchFailed {
                    validation,
                    dispatch: None,
                    error,
                };
            }
        };
        IdentityWorkerDispatchOutcome::RuntimeUnavailable {
            validation,
            dispatch,
        }
    }
}

macro_rules! with_inbound_event_request {
    ($request:expr, $binding:ident => $body:expr) => {
        match $request {
            IdentityApplicationInboundEventRequest::HandleRoleCapabilitySourceChanged($binding) => {
                $body
            }
            IdentityApplicationInboundEventRequest::HandleWorkParticipationAccepted($binding) => {
                $body
            }
            IdentityApplicationInboundEventRequest::HandleMemoryReferenceSourceStateChanged(
                $binding,
            ) => $body,
        }
    };
}

macro_rules! with_callback_request {
    ($request:expr, $binding:ident => $body:expr) => {
        match $request {
            IdentityApplicationCallbackRequest::HandleArchiveHandoffResult($binding) => $body,
            IdentityApplicationCallbackRequest::HandleTraceHandoffResult($binding) => $body,
        }
    };
}

fn worker_actor(surface_kind: identity_application::support::IdentityEntrySurfaceKind) -> ActorRef {
    match surface_kind {
        identity_application::support::IdentityEntrySurfaceKind::WorkerConsumer => {
            ActorRef::system("identity-worker-consumer")
        }
        identity_application::support::IdentityEntrySurfaceKind::WorkerCallback => {
            ActorRef::system("identity-worker-callback")
        }
        _ => ActorRef::system("identity-worker"),
    }
}

fn inbound_event_name(
    request: &IdentityApplicationInboundEventRequest,
) -> identity_contracts::protocol::IdentityInboundConsumerName {
    with_inbound_event_request!(request, inner => inner.consumer_name.clone())
}

fn inbound_event_binding_ref(
    request: &IdentityApplicationInboundEventRequest,
) -> &identity_contracts::refs::IdentityConsumerBindingRef {
    with_inbound_event_request!(request, inner => &inner.consumer_binding_ref)
}

fn inbound_event_envelope_marker_ref(
    request: &IdentityApplicationInboundEventRequest,
) -> &identity_contracts::refs::IdentityEventEnvelopeMarkerRef {
    with_inbound_event_request!(request, inner => &inner.envelope_marker_ref)
}

fn inbound_event_source_event_ref(
    request: &IdentityApplicationInboundEventRequest,
) -> &identity_contracts::refs::IdentitySourceEventRef {
    with_inbound_event_request!(request, inner => &inner.source_event_ref)
}

fn inbound_event_idempotency_key(
    request: &IdentityApplicationInboundEventRequest,
) -> &core_contracts::metadata::IdempotencyKey {
    with_inbound_event_request!(request, inner => &inner.idempotency_key)
}

fn inbound_event_trace_context_ref(
    request: &IdentityApplicationInboundEventRequest,
) -> &Option<identity_contracts::refs::IdentityTraceContextRef> {
    with_inbound_event_request!(request, inner => &inner.trace_context_ref)
}

fn inbound_event_digest(
    request: &IdentityApplicationInboundEventRequest,
) -> Result<IdentityRequestDigest, ApplicationError> {
    with_inbound_event_request!(request, inner => IdentityRequestDigest::from_entry_canonical_material(
        &format!("worker-consumer:{}", inner.consumer_name.as_str()),
        inner.schema_version_ref.clone(),
        &(
            inner.consumer_name.as_str(),
            &inner.consumer_binding_ref,
            &inner.source_event_ref,
            inner.schema_version_ref.as_str(),
            &inner.payload,
        ),
    ))
}

fn callback_name(
    request: &IdentityApplicationCallbackRequest,
) -> identity_contracts::protocol::IdentityInboundConsumerName {
    with_callback_request!(request, inner => inner.consumer_name.clone())
}

fn callback_binding_ref(
    request: &IdentityApplicationCallbackRequest,
) -> &identity_contracts::refs::IdentityConsumerBindingRef {
    with_callback_request!(request, inner => &inner.consumer_binding_ref)
}

fn callback_envelope_marker_ref(
    request: &IdentityApplicationCallbackRequest,
) -> &identity_contracts::refs::IdentityEventEnvelopeMarkerRef {
    with_callback_request!(request, inner => &inner.envelope_marker_ref)
}

fn callback_source_event_ref(
    request: &IdentityApplicationCallbackRequest,
) -> &identity_contracts::refs::IdentitySourceEventRef {
    with_callback_request!(request, inner => &inner.source_event_ref)
}

fn callback_idempotency_key(
    request: &IdentityApplicationCallbackRequest,
) -> &core_contracts::metadata::IdempotencyKey {
    with_callback_request!(request, inner => &inner.idempotency_key)
}

fn callback_trace_context_ref(
    request: &IdentityApplicationCallbackRequest,
) -> &Option<identity_contracts::refs::IdentityTraceContextRef> {
    with_callback_request!(request, inner => &inner.trace_context_ref)
}

fn callback_digest(
    request: &IdentityApplicationCallbackRequest,
) -> Result<IdentityRequestDigest, ApplicationError> {
    with_callback_request!(request, inner => IdentityRequestDigest::from_entry_canonical_material(
        &format!("worker-callback:{}", inner.consumer_name.as_str()),
        inner.schema_version_ref.clone(),
        &(
            inner.consumer_name.as_str(),
            &inner.consumer_binding_ref,
            &inner.source_event_ref,
            inner.schema_version_ref.as_str(),
            &inner.payload,
        ),
    ))
}

fn issue(code: &str) -> IdentityEntryValidationIssueRef {
    IdentityEntryValidationIssueRef::new(format!("worker-entry:{code}"))
}

#[cfg(test)]
mod tests {
    use identity_application::DefaultIdentityDispatchTargetCatalog;
    use identity_application::IdentityApplicationInboundEventRequest;
    use identity_application::support::{
        IdentityEntrySurfaceKind, IdentityIdempotencyKey, IdentityWorkerEntryContext,
        IdentityWorkerEntryRef,
    };
    use identity_contracts::events::{
        IdentityConsumerOutcome, IdentityInboundEventEnvelope, RoleCapabilitySourceChangedPayload,
    };
    use identity_contracts::protocol::{
        IdentityInboundConsumerName, IdentityProtocolSchemaVersionRef,
    };
    use identity_contracts::refs::{
        ExternalSourceRef, GlobalMemberId, GlobalMemberRef, IdentityEventEnvelopeMarkerRef,
        IdentitySourceEventRef, IdentitySourceOwner, IdentityTimestamp,
        RoleCapabilityChangeMaterialKind, RoleCapabilityChangeMaterialMarker,
        RoleCapabilitySourceKind, RoleCapabilitySourceRef, RoleCapabilitySourceStateKind,
        RoleCapabilitySourceVersionRef,
    };
    use identity_infra::config::{
        IdentityConfigSourceKind, IdentityRuntimeConfigSources, IdentityRuntimeStartupConfig,
    };
    use identity_infra::{
        IdentityInMemoryRuntimeAssemblyBuilder, IdentityInMemoryRuntimeBuildOutcome,
    };

    use super::{IdentityInboundEventDispatchOutcome, IdentityWorkerEntryAdapter};

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

    fn member_ref() -> GlobalMemberRef {
        GlobalMemberRef::from_id(
            GlobalMemberId::new("member-1".to_owned()).expect("valid member id"),
        )
    }

    fn method_source_ref() -> identity_contracts::refs::IdentitySourceRef {
        identity_contracts::refs::IdentitySourceRef::new(
            IdentitySourceOwner::MethodLibrary,
            ExternalSourceRef::new("method-source-1".to_owned()).expect("valid method source"),
        )
        .expect("valid method identity source ref")
    }

    fn role_source_ref() -> RoleCapabilitySourceRef {
        RoleCapabilitySourceRef::new(
            RoleCapabilitySourceKind::RoleCapabilityBundle,
            method_source_ref(),
        )
        .expect("valid role source ref")
    }

    fn consumer_envelope() -> IdentityInboundEventEnvelope<RoleCapabilitySourceChangedPayload> {
        let source_ref = role_source_ref();
        IdentityInboundEventEnvelope {
            consumer_name: IdentityInboundConsumerName::new("HandleRoleCapabilitySourceChanged"),
            envelope_marker_ref: IdentityEventEnvelopeMarkerRef::new("envelope-1"),
            consumer_binding_ref: DefaultIdentityDispatchTargetCatalog::worker_consumer_binding_ref(
                &IdentityInboundConsumerName::new("HandleRoleCapabilitySourceChanged"),
            ),
            source_event_ref: IdentitySourceEventRef::new("source-event-1"),
            idempotency_key: core_contracts::metadata::IdempotencyKey::new(
                "consumer-idem-1".to_owned(),
            ),
            schema_version_ref: IdentityProtocolSchemaVersionRef::new("identity.consumer.v1"),
            occurred_at: None,
            received_at: IdentityTimestamp::from_clock(1).expect("valid timestamp"),
            trace_context_ref: None,
            payload: RoleCapabilitySourceChangedPayload {
                member_ref: member_ref(),
                source_ref: source_ref.clone(),
                source_version_ref: RoleCapabilitySourceVersionRef::new(
                    source_ref.clone(),
                    "source-version-1",
                )
                .expect("valid source version"),
                source_state_kind: RoleCapabilitySourceStateKind::SourceUnavailable,
                safe_summary_ref: None,
                evidence_refs: Vec::new(),
                external_reference_ref: None,
                reference_owner_ref: None,
                change_reason_ref: None,
                material_marker: RoleCapabilityChangeMaterialMarker::new(
                    RoleCapabilityChangeMaterialKind::SourceVersionMarker,
                    Some(source_ref.source_ref.clone()),
                ),
            },
        }
    }

    fn worker_context() -> IdentityWorkerEntryContext {
        IdentityWorkerEntryContext {
            worker_entry_ref: IdentityWorkerEntryRef::new("worker-entry-1"),
            surface_kind: IdentityEntrySurfaceKind::WorkerConsumer,
            consumer_binding_ref: DefaultIdentityDispatchTargetCatalog::worker_consumer_binding_ref(
                &IdentityInboundConsumerName::new("HandleRoleCapabilitySourceChanged"),
            ),
            envelope_marker_ref: IdentityEventEnvelopeMarkerRef::new("envelope-1"),
            source_event_ref: IdentitySourceEventRef::new("source-event-1"),
            idempotency_key: IdentityIdempotencyKey::new(
                core_contracts::metadata::IdempotencyKey::new("consumer-idem-1".to_owned()),
            ),
            trace_context_ref: None,
            received_at: IdentityTimestamp::from_clock(1).expect("valid timestamp"),
        }
    }

    #[test]
    fn worker_entry_dispatches_inbound_events_through_the_facade() {
        let assembly = build_assembly();
        let catalog = DefaultIdentityDispatchTargetCatalog::identity_default();
        let adapter = IdentityWorkerEntryAdapter::new(
            assembly.application_facade(),
            assembly.assembly_state(),
            assembly.runtime(),
            assembly.runtime(),
            assembly.runtime(),
            &catalog,
        );

        let outcome = adapter.handle_inbound_event(
            worker_context(),
            IdentityApplicationInboundEventRequest::HandleRoleCapabilitySourceChanged(
                consumer_envelope(),
            ),
        );

        match outcome {
            IdentityInboundEventDispatchOutcome::Dispatched { response, .. } => {
                assert!(matches!(
                    response,
                    Ok(receipt) if matches!(
                        receipt.outcome,
                        IdentityConsumerOutcome::Rejected
                            | IdentityConsumerOutcome::DelayedRetry
                            | IdentityConsumerOutcome::Noop
                            | IdentityConsumerOutcome::Quarantined
                            | IdentityConsumerOutcome::Accepted
                            | IdentityConsumerOutcome::DuplicateReplayed
                    )
                ));
            }
            _ => panic!("expected dispatched worker outcome"),
        }
    }

    #[test]
    fn worker_entry_rejects_binding_mismatch_before_dispatch() {
        let assembly = build_assembly();
        let catalog = DefaultIdentityDispatchTargetCatalog::identity_default();
        let adapter = IdentityWorkerEntryAdapter::new(
            assembly.application_facade(),
            assembly.assembly_state(),
            assembly.runtime(),
            assembly.runtime(),
            assembly.runtime(),
            &catalog,
        );
        let mut context = worker_context();
        context.consumer_binding_ref =
            identity_contracts::refs::IdentityConsumerBindingRef::new("worker.consumer.wrong");

        let outcome = adapter.handle_inbound_event(
            context,
            IdentityApplicationInboundEventRequest::HandleRoleCapabilitySourceChanged(
                consumer_envelope(),
            ),
        );

        match outcome {
            IdentityInboundEventDispatchOutcome::PreDispatchFailure { validation } => {
                assert_eq!(
                    validation.validation_kind,
                    identity_application::support::IdentityWorkerEntryValidationKind::InvalidEnvelopeMarker
                );
            }
            _ => panic!("expected pre-dispatch failure"),
        }
    }
}
