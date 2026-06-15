//! Shared command-service skeleton and accepted-response assembly helpers.

use identity_contracts::commands::{
    EstablishGlobalMemberRequest, GlobalLifecycleCommandResult, GlobalMemberCommandResult,
    IdentityCommandEffectPublicSummary, IdentityCommandOutcome, IdentityCommandRequest,
    IdentityCommandResponse, UpdateGlobalLifecycleStateRequest,
};
use identity_contracts::metadata::IdentityRequestDigestMarker;
use identity_contracts::protocol::IdentityCommandName;
use identity_contracts::refs::{
    IdentityAuditSubjectRef, IdentityOperationChannel, IdentityStoredResultRef, IdentityTimestamp,
};

use crate::errors::ApplicationError;
use crate::ports::{
    GlobalLifecycleRepository, GlobalMemberRepository, IdentityAuditTrailRepository,
    IdentityClockPort, IdentityCommandEffectSummaryRepository, IdentityCursorAssignerPort,
    IdentityExternalSourceResolverPort, IdentityIdGeneratorPort, IdentityIdempotencyRepository,
    IdentityOperationContextFactoryPort, IdentityOutboxRepository, IdentityProjectionRepository,
    IdentityStoredResultRepository, IdentityTraceRecordRepository,
    IdentityTruthChangeSubjectMapper, IdentityUnitOfWork, IdentityUnitOfWorkManagerPort,
};
use crate::support::{
    IdempotencyReserveOutcome, IdentityCommandEffectSummary, IdentityOperationContext,
    IdentityRequestDigest,
};

/// Shared dependencies for command write-path orchestration.
pub struct IdentityCommandServiceDeps<'a> {
    /// Command write transaction manager.
    pub unit_of_work_manager: &'a dyn IdentityUnitOfWorkManagerPort,
    /// Trusted clock used by accepted and replay decisions.
    pub clock: &'a dyn IdentityClockPort,
    /// Stable id and marker generator.
    pub id_generator: &'a dyn IdentityIdGeneratorPort,
    /// Explicit truth cursor assigner.
    pub cursor_assigner: &'a dyn IdentityCursorAssignerPort,
    /// Entry metadata to operation-context builder.
    pub operation_context_factory: &'a dyn IdentityOperationContextFactoryPort,
    /// Duplicate replay reserve and completion repository.
    pub idempotency_repository: &'a dyn IdentityIdempotencyRepository,
    /// Stored replay shell repository.
    pub stored_result_repository: &'a dyn IdentityStoredResultRepository,
    /// Accepted effect summary repository.
    pub effect_summary_repository: &'a dyn IdentityCommandEffectSummaryRepository,
    /// Canonical accepted subject mapper.
    pub truth_change_subject_mapper: &'a dyn IdentityTruthChangeSubjectMapper,
    /// Member truth repository.
    pub member_repository: &'a dyn GlobalMemberRepository,
    /// Lifecycle truth repository.
    pub lifecycle_repository: &'a dyn GlobalLifecycleRepository,
    /// Accepted trace append repository.
    pub trace_record_repository: &'a dyn IdentityTraceRecordRepository,
    /// Audit trail repository.
    pub audit_trail_repository: &'a dyn IdentityAuditTrailRepository,
    /// Accepted outbox repository.
    pub outbox_repository: &'a dyn IdentityOutboxRepository,
    /// Projection lookup and stale-marker repository.
    pub projection_repository: &'a dyn IdentityProjectionRepository,
    /// External source and governance-basis resolver port.
    pub external_source_resolver: &'a dyn IdentityExternalSourceResolverPort,
}

/// Shared command service skeleton for command write-path vertical slices.
pub struct IdentityCommandService<'a> {
    deps: IdentityCommandServiceDeps<'a>,
}

impl<'a> IdentityCommandService<'a> {
    /// Creates a command service from formal shared command dependencies.
    pub fn new(deps: IdentityCommandServiceDeps<'a>) -> Self {
        Self { deps }
    }

    /// Returns the shared command dependencies for vertical-slice implementations.
    pub fn deps(&self) -> &IdentityCommandServiceDeps<'a> {
        &self.deps
    }

    /// Shared helper that copies the public request digest marker into the application digest.
    pub fn request_digest_from_marker(
        marker: &IdentityRequestDigestMarker,
    ) -> IdentityRequestDigest {
        IdentityRequestDigest::from_canonical_marker(
            marker.canonical_marker_ref.clone(),
            marker.digest_value.clone(),
            marker.schema_version_ref.clone(),
            marker.algorithm_marker_ref.clone(),
        )
    }

    /// Shared command precheck that keeps the public request envelope aligned with the context.
    pub fn assert_command_context<T>(
        request: &IdentityCommandRequest<T>,
        context: &IdentityOperationContext,
    ) -> Result<(), ApplicationError> {
        if context.channel != IdentityOperationChannel::Command {
            return Err(ApplicationError::invalid_request(
                "command context must use the command channel",
            ));
        }

        if context.operation_name.as_str() != request.command_name.as_str() {
            return Err(ApplicationError::invalid_request(format!(
                "operation name {} does not match command {}",
                context.operation_name.as_str(),
                request.command_name.as_str(),
            )));
        }

        let Some(idempotency_key) = context.idempotency_key.as_ref() else {
            return Err(ApplicationError::invalid_request(
                "command context must carry an idempotency key",
            ));
        };

        if idempotency_key.as_public() != &request.metadata.idempotency_key {
            return Err(ApplicationError::invalid_request(
                "command context idempotency key does not match the public request metadata",
            ));
        }

        let request_digest = Self::request_digest_from_marker(&request.digest);
        if context.request_digest.conflicts_with(&request_digest) {
            return Err(ApplicationError::invalid_request(
                "command context digest does not match the public request digest marker",
            ));
        }

        Ok(())
    }

    /// Shared helper that reserves command idempotency inside the active write transaction.
    pub fn reserve_idempotency(
        &self,
        context: &IdentityOperationContext,
        reserved_at: IdentityTimestamp,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdempotencyReserveOutcome, ApplicationError> {
        let record_ref = self
            .deps
            .id_generator
            .new_identity_idempotency_record_ref()?;
        self.deps
            .idempotency_repository
            .reserve(context.clone(), record_ref, reserved_at, uow)
    }

    /// Shared helper that maps an accepted application effect summary into the public envelope surface.
    pub fn public_effect_from_summary(
        summary: &IdentityCommandEffectSummary,
        audit_subject_refs: Vec<IdentityAuditSubjectRef>,
    ) -> IdentityCommandEffectPublicSummary {
        IdentityCommandEffectPublicSummary {
            accepted_cursor_ref: summary.accepted_cursor_ref.clone(),
            trace_refs: summary.trace_record_refs.clone(),
            audit_subject_refs,
            outbox_refs: summary.outbox_record_refs.clone(),
            stale_projection_refs: summary.stale_projection_refs.clone(),
        }
    }

    /// Shared helper that assembles the public accepted command response envelope.
    pub fn accepted_response<T>(
        command_name: IdentityCommandName,
        result_ref: IdentityStoredResultRef,
        result: T,
        effect: IdentityCommandEffectPublicSummary,
    ) -> IdentityCommandResponse<T> {
        IdentityCommandResponse {
            command_name,
            result_ref,
            result,
            effect,
        }
    }

    /// Shared helper that assembles the public accepted response from an effect summary.
    pub fn accepted_response_from_summary<T>(
        command_name: IdentityCommandName,
        result: T,
        summary: &IdentityCommandEffectSummary,
        audit_subject_refs: Vec<IdentityAuditSubjectRef>,
    ) -> IdentityCommandResponse<T> {
        let effect = Self::public_effect_from_summary(summary, audit_subject_refs);
        Self::accepted_response(
            command_name,
            summary.stored_result_ref.clone(),
            result,
            effect,
        )
    }

    /// Shared skeleton for the member-establish command vertical slice.
    pub fn establish_global_member(
        &self,
        request: IdentityCommandRequest<EstablishGlobalMemberRequest>,
        context: IdentityOperationContext,
    ) -> Result<IdentityCommandOutcome<GlobalMemberCommandResult>, ApplicationError> {
        Self::assert_command_context(&request, &context)?;
        Err(ApplicationError::invalid_request(
            "EstablishGlobalMember flow is not implemented in this shared skeleton",
        ))
    }

    /// Shared skeleton for the lifecycle-update command vertical slice.
    pub fn update_global_lifecycle_state(
        &self,
        request: IdentityCommandRequest<UpdateGlobalLifecycleStateRequest>,
        context: IdentityOperationContext,
    ) -> Result<IdentityCommandOutcome<GlobalLifecycleCommandResult>, ApplicationError> {
        Self::assert_command_context(&request, &context)?;
        Err(ApplicationError::invalid_request(
            "UpdateGlobalLifecycleState flow is not implemented in this shared skeleton",
        ))
    }
}

/// Application-facade shell that will route command entrypoints through the shared command service.
pub struct IdentityApplicationFacade<'a> {
    command_service: IdentityCommandService<'a>,
}

impl<'a> IdentityApplicationFacade<'a> {
    /// Creates an application facade from the shared command service.
    pub fn new(command_service: IdentityCommandService<'a>) -> Self {
        Self { command_service }
    }

    /// Returns the command service used by the facade shell.
    pub fn command_service(&self) -> &IdentityCommandService<'a> {
        &self.command_service
    }
}

#[cfg(test)]
mod tests {
    use core_contracts::actor::{ActorKind, ActorRef};

    use super::IdentityCommandService;
    use crate::support::{
        IdentityAcceptedEffectKind, IdentityCommandEffectSummary, IdentityCommandEffectSummaryRef,
        IdentityIdempotencyKey, IdentityOperationContext, IdentityOperationContextRef,
        IdentityOperationName, IdentityRequestMetadataRef, IdentityTruthRef,
    };
    use identity_contracts::commands::{
        EstablishGlobalMemberRequest, GlobalMemberCommandResult, IdentityCommandRequest,
    };
    use identity_contracts::metadata::{IdentityCommandMetadata, IdentityRequestDigestMarker};
    use identity_contracts::protocol::{
        IdentityCommandName, IdentityDigestAlgorithmMarkerRef, IdentityProtocolSchemaVersionRef,
    };
    use identity_contracts::refs::{
        ExternalSourceRef, GlobalLifecycleStateKind, GlobalMemberId, GlobalMemberRef,
        IdentityApiRequestMarkerRef, IdentityAuditSubjectRef, IdentityCanonicalRequestMarkerRef,
        IdentityRequestDigestValue, IdentitySourceOwner, IdentitySourceRef,
        IdentityStoredResultRef, IdentityTimestamp, IdentityTruthCursor, LifecycleReasonKind,
        LifecycleReasonRef,
    };

    fn actor_ref() -> ActorRef {
        ActorRef::new("actor-1", ActorKind::Human)
    }

    fn member_ref() -> GlobalMemberRef {
        GlobalMemberRef::from_id(
            GlobalMemberId::new("member-1".to_owned()).expect("valid member id"),
        )
    }

    fn source_ref() -> IdentitySourceRef {
        IdentitySourceRef::new(
            IdentitySourceOwner::Identity,
            ExternalSourceRef::new("source-1".to_owned()).expect("valid external source ref"),
        )
        .expect("valid source ref")
    }

    fn lifecycle_reason_ref() -> LifecycleReasonRef {
        LifecycleReasonRef::new(LifecycleReasonKind::InitialProvisioned, source_ref())
            .expect("valid lifecycle reason")
    }

    fn digest_marker(token: &str) -> IdentityRequestDigestMarker {
        IdentityRequestDigestMarker {
            canonical_marker_ref: IdentityCanonicalRequestMarkerRef::new(format!(
                "canonical-{token}"
            )),
            digest_value: IdentityRequestDigestValue::new(format!("digest-{token}")),
            schema_version_ref: IdentityProtocolSchemaVersionRef::new("identity.command.v1"),
            algorithm_marker_ref: IdentityDigestAlgorithmMarkerRef::new("sha256-v1"),
        }
    }

    fn command_request(token: &str) -> IdentityCommandRequest<EstablishGlobalMemberRequest> {
        IdentityCommandRequest {
            actor_ref: actor_ref(),
            command_name: IdentityCommandName::new("EstablishGlobalMember"),
            metadata: IdentityCommandMetadata {
                idempotency_key: format!("idem-{token}").into(),
                request_marker_ref: IdentityApiRequestMarkerRef::new(format!("request-{token}")),
                schema_version_ref: IdentityProtocolSchemaVersionRef::new("identity.command.v1"),
                trace_context_ref: None,
            },
            digest: digest_marker(token),
            body: EstablishGlobalMemberRequest {
                requested_member_ref: Some(member_ref()),
                source_ref: source_ref(),
                anchor_reason_ref: None,
                initial_lifecycle_reason_ref: lifecycle_reason_ref(),
            },
        }
    }

    fn command_context(token: &str) -> IdentityOperationContext {
        IdentityOperationContext::from_command(
            IdentityOperationContextRef::new(format!("context-{token}")),
            IdentityOperationName::new("EstablishGlobalMember"),
            actor_ref(),
            IdentityRequestMetadataRef::new(format!("metadata-{token}")),
            Some(IdentityIdempotencyKey::new(format!("idem-{token}"))),
            IdentityCommandService::request_digest_from_marker(&digest_marker(token)),
            None,
            IdentityTimestamp::from_clock(1).expect("valid timestamp"),
        )
    }

    #[test]
    fn request_digest_is_copied_from_public_marker() {
        let marker = digest_marker("copy");
        let digest = IdentityCommandService::request_digest_from_marker(&marker);

        assert_eq!(digest.canonical_marker_ref, marker.canonical_marker_ref);
        assert_eq!(digest.digest_value, marker.digest_value);
        assert_eq!(digest.schema_version_ref, marker.schema_version_ref);
        assert_eq!(digest.algorithm_ref, marker.algorithm_marker_ref);
    }

    #[test]
    fn command_context_must_match_public_request() {
        let request = command_request("ok");
        let context = command_context("ok");

        IdentityCommandService::assert_command_context(&request, &context)
            .expect("matching context should pass");

        let mismatched = command_context("different");
        let error = IdentityCommandService::assert_command_context(&request, &mismatched)
            .expect_err("different idempotency key and digest must fail");
        assert_eq!(
            error.kind,
            crate::errors::ApplicationErrorKind::InvalidRequest
        );
    }

    #[test]
    fn accepted_response_carries_effect_on_envelope() {
        let summary = IdentityCommandEffectSummary::from_accepted_change(
            IdentityCommandEffectSummaryRef::new("effect-1"),
            IdentityOperationContextRef::new("context-1"),
            IdentityAcceptedEffectKind::GlobalMemberCommandResult,
            IdentityTruthRef::GlobalMember(member_ref()),
            IdentityTruthCursor::new("cursor-1"),
            vec!["trace-1".into()],
            None,
            vec!["outbox-1".into()],
            vec!["projection-1".into()],
            IdentityStoredResultRef::new("stored-result-1"),
        );
        let result = GlobalMemberCommandResult {
            member_ref: member_ref(),
            anchor_state_kind: identity_contracts::refs::IdentityAnchorStateKind::Established,
            lifecycle_state_kind: GlobalLifecycleStateKind::Available,
            source_ref: source_ref(),
        };

        let response = IdentityCommandService::accepted_response_from_summary(
            IdentityCommandName::new("EstablishGlobalMember"),
            result,
            &summary,
            vec![IdentityAuditSubjectRef::new("audit-subject-1")],
        );

        assert_eq!(
            response.result_ref,
            IdentityStoredResultRef::new("stored-result-1")
        );
        assert_eq!(
            response.effect.accepted_cursor_ref,
            IdentityTruthCursor::new("cursor-1")
        );
        assert_eq!(
            response.effect.audit_subject_refs,
            vec![IdentityAuditSubjectRef::new("audit-subject-1")]
        );
        assert_eq!(
            response.result.anchor_state_kind,
            identity_contracts::refs::IdentityAnchorStateKind::Established
        );
    }
}
