//! Shared command-service skeleton and accepted-response assembly helpers.

use core_contracts::actor::ActorRef;
use identity_contracts::commands::{
    AppendCareerRecordRequest, CareerRecordCommandResult, EstablishGlobalMemberRequest,
    GlobalLifecycleCommandResult, GlobalMemberCommandResult, IdentityCommandEffectPublicSummary,
    IdentityCommandOutcome, IdentityCommandRequest, IdentityCommandResponse,
    MaintainMemoryReferenceRequest, MaintainRoleCapabilitySummaryRequest,
    MemoryReferenceCommandResult, PrepareTraceHandoffRequest, RoleCapabilityCommandResult,
    TraceHandoffCommandResult, UpdateGlobalLifecycleStateRequest,
};
use identity_contracts::metadata::{
    IdentityDegradedKind, IdentityDegradedMarker, IdentityProtocolRejection,
    IdentityProtocolRejectionKind, IdentityProtocolValidationIssueRef,
    IdentityProtocolValidationIssueRefSet, IdentityRequestDigestMarker,
};
use identity_contracts::protocol::{IdentityCommandName, IdentityProtocolSurfaceRef};
use identity_contracts::refs::{
    AuditTrailRef, ExternalSourceRef, GlobalLifecycleStateKind as PublicLifecycleStateKind,
    GlobalMemberRef, GovernanceBasisRef, HandoffStateKind as PublicHandoffStateKind,
    IdentityAnchorReasonKind, IdentityAnchorReasonRef, IdentityAnchorStateKind,
    IdentityAuditSubjectRef, IdentityChangeKind, IdentityChangeKindRef, IdentityChangeReasonRef,
    IdentityDegradedMarkerRef, IdentityOperationChannel, IdentityProjectionRef,
    IdentitySourceOwner, IdentitySourceRef, IdentityStoredResultRef, IdentityTimestamp,
    IdentityTraceRecordRef, IdentityTruthCursor, MaintenanceScopeRef, MemoryReferenceSourceState,
    MemoryReferenceStateKind as PublicMemoryReferenceStateKind, RoleCapabilitySourceStateKind,
    RoleCapabilitySummaryStateKind,
};
use identity_contracts::views::{IdentityReadMaterialKind, IdentityReadMaterialMarker};
use identity_domain::audit::{AuditTrail, AuditTrailEntry};
use identity_domain::career::{CareerAppendPolicy, CareerRecord, CareerRecordStateKind};
use identity_domain::handoff::{
    HandoffPolicy, HandoffPolicyArgs, HandoffState, TraceHandoffIntent,
    TraceHandoffIntentPrepareArgs,
};
use identity_domain::lifecycle::{
    GlobalLifecycleState, GlobalLifecycleStateKind, HighRiskLifecycleGuard,
    LifecycleTransitionPolicy,
};
use identity_domain::member_identity::{GlobalMember, IdentityAnchorPolicy, IdentityAnchorState};
use identity_domain::memory_reference::{
    MemoryReference, MemoryReferencePolicy, MemoryReferenceState, MemoryReferenceStateKind,
};
use identity_domain::outbox::IdentityOutboxRecord;
use identity_domain::role_capability::{
    RoleCapabilitySourcePolicy, RoleCapabilitySourceSnapshot, RoleCapabilitySummary,
};
use identity_domain::trace::IdentityTraceRecord;

use crate::errors::{ApplicationError, ApplicationErrorKind};
use crate::outbound_material::AcceptedOutboundMaterialKind;
use crate::ports::{
    CareerRecordRepository, GlobalLifecycleRepository, GlobalMemberRepository,
    IdentityAcceptedAuditTrailMarkerMapper, IdentityAuditTrailRepository, IdentityClockPort,
    IdentityCommandEffectSummaryRepository, IdentityCursorAssignerPort,
    IdentityExternalSourceResolverPort, IdentityHandoffTargetPort, IdentityIdGeneratorPort,
    IdentityIdempotencyRepository, IdentityOperationContextFactoryPort, IdentityOutboxRepository,
    IdentityProjectionRepository, IdentityStoredResultRepository, IdentityTraceRecordRepository,
    IdentityTruthChangeSubjectMapper, IdentityUnitOfWork, IdentityUnitOfWorkManagerPort,
    MemoryReferenceRepository, RoleCapabilityRepository, TraceHandoffIntentRepository,
};
use crate::support::{
    IdempotencyReserveOutcome, IdentityAcceptedEffectKind, IdentityAcceptedSubjectRefs,
    IdentityCommandAcceptedResultEnvelope, IdentityCommandEffectSummary,
    IdentityCommandRejectedResultEnvelope, IdentityCommandTypedResult, IdentityOperationContext,
    IdentityRequestDigest, IdentityTruthRef, StoredIdentityOperationResult, Versioned,
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
    /// Canonical accepted audit marker mapper.
    pub accepted_audit_trail_marker_mapper: &'a dyn IdentityAcceptedAuditTrailMarkerMapper,
    /// Member truth repository.
    pub member_repository: &'a dyn GlobalMemberRepository,
    /// Lifecycle truth repository.
    pub lifecycle_repository: &'a dyn GlobalLifecycleRepository,
    /// Role capability summary and source snapshot repository.
    pub role_capability_repository: &'a dyn RoleCapabilityRepository,
    /// Append-only career record repository.
    pub career_record_repository: &'a dyn CareerRecordRepository,
    /// Memory reference relation repository.
    pub memory_reference_repository: &'a dyn MemoryReferenceRepository,
    /// Accepted trace append repository.
    pub trace_record_repository: &'a dyn IdentityTraceRecordRepository,
    /// Audit trail repository.
    pub audit_trail_repository: &'a dyn IdentityAuditTrailRepository,
    /// Accepted outbox repository.
    pub outbox_repository: &'a dyn IdentityOutboxRepository,
    /// Projection lookup and stale-marker repository.
    pub projection_repository: &'a dyn IdentityProjectionRepository,
    /// Trace handoff intent repository.
    pub handoff_intent_repository: &'a dyn TraceHandoffIntentRepository,
    /// Handoff target resolver port.
    pub handoff_target_port: &'a dyn IdentityHandoffTargetPort,
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

    fn protocol_rejection(
        command_name: &IdentityCommandName,
        rejection_kind: IdentityProtocolRejectionKind,
        issue: impl Into<String>,
    ) -> IdentityProtocolRejection {
        IdentityProtocolRejection {
            surface_ref: IdentityProtocolSurfaceRef::new(format!(
                "command:{}",
                command_name.as_str()
            )),
            rejection_kind,
            issue_refs: IdentityProtocolValidationIssueRefSet(vec![
                IdentityProtocolValidationIssueRef::new(issue),
            ]),
            degraded: None,
        }
    }

    fn adapter_unavailable_rejection(
        command_name: &IdentityCommandName,
        issue: impl Into<String>,
    ) -> IdentityProtocolRejection {
        IdentityProtocolRejection {
            surface_ref: IdentityProtocolSurfaceRef::new(format!(
                "command:{}",
                command_name.as_str()
            )),
            rejection_kind: IdentityProtocolRejectionKind::AdapterUnavailable,
            issue_refs: IdentityProtocolValidationIssueRefSet(vec![
                IdentityProtocolValidationIssueRef::new(issue),
            ]),
            degraded: Some(IdentityDegradedMarker {
                degraded_marker_ref: IdentityDegradedMarkerRef::new(format!(
                    "degraded:{}:{}",
                    command_name.as_str(),
                    "dependency-unavailable"
                )),
                degraded_kind: IdentityDegradedKind::DependencyUnavailable,
            }),
        }
    }

    fn rejection_from_error(
        command_name: &IdentityCommandName,
        error: &ApplicationError,
    ) -> Option<IdentityProtocolRejection> {
        let rejection = match error.kind {
            ApplicationErrorKind::InvalidRequest => Self::protocol_rejection(
                command_name,
                IdentityProtocolRejectionKind::InvalidRequest,
                error.message.clone(),
            ),
            ApplicationErrorKind::NotFound => Self::protocol_rejection(
                command_name,
                IdentityProtocolRejectionKind::NotFound,
                error.message.clone(),
            ),
            ApplicationErrorKind::DomainRejected => Self::protocol_rejection(
                command_name,
                if error.message.contains("transition") || error.message.contains("version") {
                    IdentityProtocolRejectionKind::Conflict
                } else {
                    IdentityProtocolRejectionKind::PolicyDenied
                },
                error.message.clone(),
            ),
            ApplicationErrorKind::OptimisticVersionConflict
            | ApplicationErrorKind::FormalUniqueConflict => Self::protocol_rejection(
                command_name,
                IdentityProtocolRejectionKind::Conflict,
                error.message.clone(),
            ),
            ApplicationErrorKind::IdempotencyConflict => Self::protocol_rejection(
                command_name,
                IdentityProtocolRejectionKind::DuplicateConflict,
                error.message.clone(),
            ),
            ApplicationErrorKind::DependencyUnavailable => {
                Self::adapter_unavailable_rejection(command_name, error.message.clone())
            }
            _ => return None,
        };
        Some(rejection)
    }

    fn rollback_quietly(&self, uow: Box<dyn IdentityUnitOfWork>) {
        let _ = self.deps.unit_of_work_manager.rollback(uow);
    }

    fn duplicate_replay_consistency_error(&self, message: impl Into<String>) -> ApplicationError {
        ApplicationError::new(
            ApplicationErrorKind::DuplicateReplayConsistencyDefect,
            message,
        )
    }

    fn map_runtime_unavailable(
        &self,
        command_name: &IdentityCommandName,
        error: ApplicationError,
    ) -> Result<IdentityProtocolRejection, ApplicationError> {
        match error.kind {
            ApplicationErrorKind::IdempotencyConflict => Ok(Self::protocol_rejection(
                command_name,
                IdentityProtocolRejectionKind::DuplicateConflict,
                error.message,
            )),
            ApplicationErrorKind::IdempotencyInFlight
            | ApplicationErrorKind::DependencyUnavailable
            | ApplicationErrorKind::CommitStatusUnknown => Ok(Self::adapter_unavailable_rejection(
                command_name,
                error.message,
            )),
            _ => Err(error),
        }
    }

    fn map_public_anchor_state(state: &IdentityAnchorState) -> IdentityAnchorStateKind {
        match state.state_kind {
            identity_domain::member_identity::IdentityAnchorStateKind::Established => {
                IdentityAnchorStateKind::Established
            }
            identity_domain::member_identity::IdentityAnchorStateKind::RetiredHeld => {
                IdentityAnchorStateKind::RetiredHeld
            }
            identity_domain::member_identity::IdentityAnchorStateKind::TombstoneHeld => {
                IdentityAnchorStateKind::TombstoneHeld
            }
        }
    }

    fn map_public_lifecycle_state(state: GlobalLifecycleStateKind) -> PublicLifecycleStateKind {
        match state {
            GlobalLifecycleStateKind::Available => PublicLifecycleStateKind::Available,
            GlobalLifecycleStateKind::Paused => PublicLifecycleStateKind::Paused,
            GlobalLifecycleStateKind::Retired => PublicLifecycleStateKind::Retired,
            GlobalLifecycleStateKind::Tombstoned => PublicLifecycleStateKind::Tombstoned,
        }
    }

    fn anchor_reason_from_lifecycle(
        reason_ref: &identity_contracts::refs::LifecycleReasonRef,
        target_state: PublicLifecycleStateKind,
    ) -> Result<IdentityAnchorReasonRef, ApplicationError> {
        let reason_kind = match target_state {
            PublicLifecycleStateKind::Retired => IdentityAnchorReasonKind::Retired,
            PublicLifecycleStateKind::Tombstoned => IdentityAnchorReasonKind::Tombstoned,
            _ => {
                return Err(ApplicationError::invalid_request(
                    "non-terminal lifecycle state does not produce an anchor hold reason",
                ));
            }
        };
        IdentityAnchorReasonRef::new(reason_kind, reason_ref.source_ref.clone())
            .map_err(ApplicationError::from)
    }

    fn accepted_change_reason(source_ref: &IdentitySourceRef) -> IdentityChangeReasonRef {
        IdentityChangeReasonRef::new(source_ref.clone())
    }

    fn command_trace_record(
        &self,
        member_ref: GlobalMemberRef,
        subjects: &IdentityAcceptedSubjectRefs,
        change_kind_ref: IdentityChangeKindRef,
        accepted_cursor_ref: IdentityTruthCursor,
        reason_ref: Option<IdentityChangeReasonRef>,
        source_ref: Option<IdentitySourceRef>,
        basis_ref: Option<GovernanceBasisRef>,
        actor_ref: ActorRef,
        occurred_at: IdentityTimestamp,
    ) -> Result<IdentityTraceRecord, ApplicationError> {
        let trace_record_ref = self.deps.id_generator.new_identity_trace_record_id()?;
        IdentityTraceRecord::from_accepted_change(
            IdentityTraceRecordRef::new(trace_record_ref.as_str()),
            member_ref,
            subjects.trace_subject_ref.clone(),
            subjects.audit_subject_ref.clone(),
            change_kind_ref,
            accepted_cursor_ref,
            reason_ref,
            source_ref,
            basis_ref,
            Some(actor_ref),
            IdentityReadMaterialMarker::new(IdentityReadMaterialKind::TraceRefsOnly, None),
            occurred_at,
        )
        .map_err(ApplicationError::from)
    }

    fn append_accepted_audit(
        &self,
        context: &IdentityOperationContext,
        member_ref: Option<GlobalMemberRef>,
        subjects: &IdentityAcceptedSubjectRefs,
        change_kind_ref: &IdentityChangeKindRef,
        accepted_cursor_ref: &IdentityTruthCursor,
        trace_record: &IdentityTraceRecord,
        now: IdentityTimestamp,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<AuditTrailRef, ApplicationError> {
        let markers = self
            .deps
            .accepted_audit_trail_marker_mapper
            .accepted_command_audit_markers(
                context,
                subjects,
                change_kind_ref,
                accepted_cursor_ref,
            );
        let entry = AuditTrailEntry {
            trace_record_ref: trace_record.trace_record_ref.clone(),
            change_kind_ref: change_kind_ref.clone(),
            visibility_result_ref: markers.entry_visibility_result_ref.clone(),
            occurred_at: trace_record.occurred_at,
        };
        if let Some(existing) = self
            .deps
            .audit_trail_repository
            .find_audit_trail_by_subject(subjects.audit_subject_ref.clone())?
        {
            self.deps.audit_trail_repository.append_audit_entry(
                existing.value.audit_trail_ref.clone(),
                entry,
                existing.version,
                uow,
            )?;
            return Ok(existing.value.audit_trail_ref);
        }

        let audit_trail_ref =
            AuditTrailRef::new(self.deps.id_generator.new_audit_trail_id()?.as_str());
        let trail = AuditTrail::from_accepted_write(
            audit_trail_ref.clone(),
            subjects.audit_subject_ref.clone(),
            member_ref,
            markers.audit_scope_ref,
            entry,
            markers.trail_visibility_result_ref,
            now,
        )
        .map_err(ApplicationError::from)?;
        self.deps
            .audit_trail_repository
            .save_audit_trail(trail, None, uow)?;
        Ok(audit_trail_ref)
    }

    fn outbox_record(
        &self,
        member_ref: GlobalMemberRef,
        subject_ref: identity_contracts::refs::IdentityOutboxSubjectRef,
        change_kind_ref: IdentityChangeKindRef,
        material_kind: AcceptedOutboundMaterialKind,
        trace_record_ref: IdentityTraceRecordRef,
        now: IdentityTimestamp,
    ) -> Result<IdentityOutboxRecord, ApplicationError> {
        material_kind.build_outbox_record(
            self.deps.id_generator,
            member_ref,
            subject_ref,
            change_kind_ref,
            trace_record_ref,
            now,
        )
    }

    fn save_projection_stale_marks(
        &self,
        subjects: &IdentityAcceptedSubjectRefs,
        now: IdentityTimestamp,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<Vec<IdentityProjectionRef>, ApplicationError> {
        let affected = self
            .deps
            .projection_repository
            .expand_affected_projection_refs(subjects.clone())?;
        let mut stale_refs = Vec::new();
        for projection_ref in affected.projection_refs {
            let Some(versioned) = self
                .deps
                .projection_repository
                .get_projection_state_with_version(projection_ref.clone())?
            else {
                continue;
            };
            let Some(source_cursor_ref) = versioned.value.source_cursor_ref.clone() else {
                continue;
            };
            let mut next = versioned.value.clone();
            let maintenance_scope_ref =
                Self::maintenance_scope_ref(&format!("accepted-stale:{}", projection_ref.as_str()));
            if next
                .mark_stale(source_cursor_ref, maintenance_scope_ref, now)
                .is_ok()
            {
                self.deps.projection_repository.mark_projection_stale(
                    projection_ref.clone(),
                    next,
                    versioned.version,
                    uow,
                )?;
                stale_refs.push(projection_ref);
            }
        }
        Ok(stale_refs)
    }

    fn load_replayed_command_outcome<T>(
        &self,
        command_name: IdentityCommandName,
        stored_result_ref: IdentityStoredResultRef,
        accepted_variant: fn(IdentityCommandTypedResult) -> Option<T>,
    ) -> Result<IdentityCommandOutcome<T>, ApplicationError> {
        let stored = self
            .deps
            .stored_result_repository
            .get_stored_result(stored_result_ref.clone())?
            .ok_or_else(|| {
                self.duplicate_replay_consistency_error(format!(
                    "stored command result {} is missing",
                    stored_result_ref.as_str()
                ))
            })?;
        match stored.result_kind {
            crate::support::IdentityStoredResultKind::CommandAccepted => {
                let envelope = self
                    .deps
                    .stored_result_repository
                    .get_command_accepted_result(stored_result_ref.clone())?
                    .ok_or_else(|| {
                        self.duplicate_replay_consistency_error(format!(
                            "accepted command envelope {} is missing",
                            stored_result_ref.as_str()
                        ))
                    })?;
                if envelope.command_name != command_name {
                    return Err(self.duplicate_replay_consistency_error(
                        "accepted command envelope command name mismatch",
                    ));
                }
                let result = accepted_variant(envelope.result.clone()).ok_or_else(|| {
                    self.duplicate_replay_consistency_error(
                        "accepted command envelope result variant mismatch",
                    )
                })?;
                Ok(IdentityCommandOutcome::Accepted(Self::accepted_response(
                    envelope.command_name,
                    envelope.stored_result_ref,
                    result,
                    envelope.effect,
                )))
            }
            crate::support::IdentityStoredResultKind::CommandRejected => {
                let envelope = self
                    .deps
                    .stored_result_repository
                    .get_command_rejected_result(stored_result_ref)?
                    .ok_or_else(|| {
                        self.duplicate_replay_consistency_error(
                            "rejected command envelope is missing",
                        )
                    })?;
                if envelope.command_name != command_name {
                    return Err(self.duplicate_replay_consistency_error(
                        "rejected command envelope command name mismatch",
                    ));
                }
                Ok(IdentityCommandOutcome::Rejected(envelope.rejection))
            }
            other => Err(self.duplicate_replay_consistency_error(format!(
                "stored result kind {other:?} cannot replay a command outcome"
            ))),
        }
    }

    fn map_domain_lifecycle_state(state: PublicLifecycleStateKind) -> GlobalLifecycleStateKind {
        match state {
            PublicLifecycleStateKind::Available => GlobalLifecycleStateKind::Available,
            PublicLifecycleStateKind::Paused => GlobalLifecycleStateKind::Paused,
            PublicLifecycleStateKind::Retired => GlobalLifecycleStateKind::Retired,
            PublicLifecycleStateKind::Tombstoned => GlobalLifecycleStateKind::Tombstoned,
        }
    }

    fn maintenance_scope_ref(token: &str) -> MaintenanceScopeRef {
        MaintenanceScopeRef::new(
            IdentitySourceRef::new(
                IdentitySourceOwner::Identity,
                ExternalSourceRef::new(token.to_owned()).expect("valid maintenance scope token"),
            )
            .expect("valid maintenance scope ref"),
        )
    }

    fn save_replayable_rejected<T>(
        &self,
        command_name: &IdentityCommandName,
        typed_request: &IdentityCommandRequest<T>,
        context: &IdentityOperationContext,
        rejection: IdentityProtocolRejection,
        reserved: Versioned<crate::support::IdentityIdempotencyRecord>,
        now: IdentityTimestamp,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityCommandOutcome<GlobalMemberCommandResult>, ApplicationError> {
        let stored_result_ref = self.deps.id_generator.new_identity_stored_result_ref()?;
        let surface_marker_ref = self
            .deps
            .id_generator
            .new_identity_stored_surface_marker_ref()?;
        let stored = StoredIdentityOperationResult::command_rejected(
            stored_result_ref.clone(),
            context.context_ref.clone(),
            surface_marker_ref.clone(),
            now,
        );
        self.deps
            .stored_result_repository
            .save_command_rejected_result(stored, uow)?;
        let envelope = IdentityCommandRejectedResultEnvelope::new(
            stored_result_ref.clone(),
            context.context_ref.clone(),
            command_name.clone(),
            surface_marker_ref,
            rejection.clone(),
            now,
        );
        self.deps
            .stored_result_repository
            .save_command_rejected_envelope(envelope, uow)?;
        self.deps
            .idempotency_repository
            .complete_rejected_with_stored_result(
                reserved.value,
                stored_result_ref,
                now,
                reserved.version,
                uow,
            )?;
        let _ = typed_request;
        Ok(IdentityCommandOutcome::Rejected(rejection))
    }

    fn save_replayable_rejected_outcome<TRequest, TResult>(
        &self,
        command_name: &IdentityCommandName,
        typed_request: &IdentityCommandRequest<TRequest>,
        context: &IdentityOperationContext,
        rejection: IdentityProtocolRejection,
        reserved: Versioned<crate::support::IdentityIdempotencyRecord>,
        now: IdentityTimestamp,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityCommandOutcome<TResult>, ApplicationError> {
        let stored_result_ref = self.deps.id_generator.new_identity_stored_result_ref()?;
        let surface_marker_ref = self
            .deps
            .id_generator
            .new_identity_stored_surface_marker_ref()?;
        let stored = StoredIdentityOperationResult::command_rejected(
            stored_result_ref.clone(),
            context.context_ref.clone(),
            surface_marker_ref.clone(),
            now,
        );
        self.deps
            .stored_result_repository
            .save_command_rejected_result(stored, uow)?;
        let envelope = IdentityCommandRejectedResultEnvelope::new(
            stored_result_ref.clone(),
            context.context_ref.clone(),
            command_name.clone(),
            surface_marker_ref,
            rejection.clone(),
            now,
        );
        self.deps
            .stored_result_repository
            .save_command_rejected_envelope(envelope, uow)?;
        self.deps
            .idempotency_repository
            .complete_rejected_with_stored_result(
                reserved.value,
                stored_result_ref,
                now,
                reserved.version,
                uow,
            )?;
        let _ = typed_request;
        Ok(IdentityCommandOutcome::Rejected(rejection))
    }

    fn command_not_found_rejection(
        &self,
        command_name: &IdentityCommandName,
        issue: impl Into<String>,
    ) -> IdentityProtocolRejection {
        Self::protocol_rejection(command_name, IdentityProtocolRejectionKind::NotFound, issue)
    }

    fn map_public_role_summary_state(
        state: identity_domain::role_capability::RoleCapabilitySummaryStateKind,
    ) -> RoleCapabilitySummaryStateKind {
        match state {
            identity_domain::role_capability::RoleCapabilitySummaryStateKind::Active => {
                RoleCapabilitySummaryStateKind::Active
            }
            identity_domain::role_capability::RoleCapabilitySummaryStateKind::Stale => {
                RoleCapabilitySummaryStateKind::Stale
            }
            identity_domain::role_capability::RoleCapabilitySummaryStateKind::Unavailable => {
                RoleCapabilitySummaryStateKind::Unavailable
            }
            identity_domain::role_capability::RoleCapabilitySummaryStateKind::PendingReconciliation => {
                RoleCapabilitySummaryStateKind::PendingReconciliation
            }
            identity_domain::role_capability::RoleCapabilitySummaryStateKind::Superseded => {
                RoleCapabilitySummaryStateKind::Superseded
            }
        }
    }

    fn map_public_role_source_state(
        state: identity_domain::role_capability::RoleCapabilitySourceStateKind,
    ) -> RoleCapabilitySourceStateKind {
        match state {
            identity_domain::role_capability::RoleCapabilitySourceStateKind::SourceResolved => {
                RoleCapabilitySourceStateKind::SourceResolved
            }
            identity_domain::role_capability::RoleCapabilitySourceStateKind::SourceStale => {
                RoleCapabilitySourceStateKind::SourceStale
            }
            identity_domain::role_capability::RoleCapabilitySourceStateKind::SourceUnavailable => {
                RoleCapabilitySourceStateKind::SourceUnavailable
            }
            identity_domain::role_capability::RoleCapabilitySourceStateKind::SourceUnrecognized => {
                RoleCapabilitySourceStateKind::SourceUnrecognized
            }
            identity_domain::role_capability::RoleCapabilitySourceStateKind::SourceSuperseded => {
                RoleCapabilitySourceStateKind::SourceSuperseded
            }
        }
    }

    fn map_public_career_record_state(
        state: CareerRecordStateKind,
    ) -> identity_contracts::refs::CareerRecordStateKind {
        match state {
            CareerRecordStateKind::Appended => {
                identity_contracts::refs::CareerRecordStateKind::Appended
            }
            CareerRecordStateKind::CorrectionAppended => {
                identity_contracts::refs::CareerRecordStateKind::CorrectionAppended
            }
            CareerRecordStateKind::SupersededByCorrection => {
                identity_contracts::refs::CareerRecordStateKind::SupersededByCorrection
            }
            CareerRecordStateKind::SourcePendingReview => {
                identity_contracts::refs::CareerRecordStateKind::SourcePendingReview
            }
        }
    }

    fn map_public_memory_reference_state(
        state: MemoryReferenceStateKind,
    ) -> PublicMemoryReferenceStateKind {
        match state {
            MemoryReferenceStateKind::Linked => PublicMemoryReferenceStateKind::Linked,
            MemoryReferenceStateKind::PendingVerification => {
                PublicMemoryReferenceStateKind::PendingVerification
            }
            MemoryReferenceStateKind::Stale => PublicMemoryReferenceStateKind::Stale,
            MemoryReferenceStateKind::Unavailable => PublicMemoryReferenceStateKind::Unavailable,
            MemoryReferenceStateKind::Migrated => PublicMemoryReferenceStateKind::Migrated,
            MemoryReferenceStateKind::Archived => PublicMemoryReferenceStateKind::Archived,
            MemoryReferenceStateKind::HandoffPending => {
                PublicMemoryReferenceStateKind::HandoffPending
            }
            MemoryReferenceStateKind::HandoffFailed => {
                PublicMemoryReferenceStateKind::HandoffFailed
            }
        }
    }

    fn map_public_handoff_state(
        state: identity_domain::handoff::HandoffStateKind,
    ) -> PublicHandoffStateKind {
        match state {
            identity_domain::handoff::HandoffStateKind::PendingHandoff => {
                PublicHandoffStateKind::PendingHandoff
            }
            identity_domain::handoff::HandoffStateKind::Delivered => {
                PublicHandoffStateKind::Delivered
            }
            identity_domain::handoff::HandoffStateKind::RetryableFailed => {
                PublicHandoffStateKind::RetryableFailed
            }
            identity_domain::handoff::HandoffStateKind::Failed => PublicHandoffStateKind::Failed,
            identity_domain::handoff::HandoffStateKind::Cancelled => {
                PublicHandoffStateKind::Cancelled
            }
        }
    }

    /// Shared skeleton for the member-establish command vertical slice.
    pub fn establish_global_member(
        &self,
        request: IdentityCommandRequest<EstablishGlobalMemberRequest>,
        context: IdentityOperationContext,
    ) -> Result<IdentityCommandOutcome<GlobalMemberCommandResult>, ApplicationError> {
        Self::assert_command_context(&request, &context)?;
        let command_name = request.command_name.clone();
        let now = self.deps.clock.now()?;
        let uow = self.deps.unit_of_work_manager.begin()?;
        let reserve = match self.reserve_idempotency(&context, now, uow.as_ref()) {
            Ok(outcome) => outcome,
            Err(error) => {
                self.rollback_quietly(uow);
                return self
                    .map_runtime_unavailable(&command_name, error)
                    .map(IdentityCommandOutcome::Rejected);
            }
        };

        match reserve {
            IdempotencyReserveOutcome::ReplayAvailable {
                stored_result_ref, ..
            } => {
                self.rollback_quietly(uow);
                return self.load_replayed_command_outcome(
                    command_name,
                    stored_result_ref,
                    |typed| match typed {
                        IdentityCommandTypedResult::GlobalMember(result) => Some(result),
                        _ => None,
                    },
                );
            }
            IdempotencyReserveOutcome::Conflict(record) => {
                let rejection = Self::protocol_rejection(
                    &command_name,
                    IdentityProtocolRejectionKind::DuplicateConflict,
                    "same idempotency key is already bound to different canonical material",
                );
                self.deps.idempotency_repository.mark_conflict(
                    record.value,
                    now,
                    record.version,
                    uow.as_ref(),
                )?;
                self.deps.unit_of_work_manager.commit(uow)?;
                return Ok(IdentityCommandOutcome::Rejected(rejection));
            }
            IdempotencyReserveOutcome::InFlight(_) => {
                self.rollback_quietly(uow);
                return Ok(IdentityCommandOutcome::Rejected(
                    Self::adapter_unavailable_rejection(
                        &command_name,
                        "same command idempotency key and digest is still in flight",
                    ),
                ));
            }
            IdempotencyReserveOutcome::Reserved(record) => {
                let member_ref = match request.body.requested_member_ref.clone() {
                    Some(existing) => existing,
                    None => {
                        GlobalMemberRef::from_id(self.deps.id_generator.new_global_member_id()?)
                    }
                };

                let anchor_state = self
                    .deps
                    .member_repository
                    .get_anchor_state(member_ref.clone())?;
                let policy = IdentityAnchorPolicy::for_create(
                    member_ref.clone(),
                    request.body.source_ref.clone(),
                    context.actor_ref.clone(),
                    anchor_state,
                    context.channel,
                );
                if let Err(error) = policy.assert_can_establish() {
                    let app_error = ApplicationError::from(error);
                    let rejection = Self::rejection_from_error(&command_name, &app_error)
                        .ok_or_else(|| app_error.clone())?;
                    let outcome = self.save_replayable_rejected(
                        &command_name,
                        &request,
                        &context,
                        rejection,
                        record,
                        now,
                        uow.as_ref(),
                    )?;
                    self.deps.unit_of_work_manager.commit(uow)?;
                    return Ok(outcome);
                }

                if self
                    .deps
                    .lifecycle_repository
                    .get_lifecycle_with_version(member_ref.clone())?
                    .is_some()
                {
                    let rejection = Self::protocol_rejection(
                        &command_name,
                        IdentityProtocolRejectionKind::Conflict,
                        "lifecycle already exists for the requested member ref",
                    );
                    let outcome = self.save_replayable_rejected(
                        &command_name,
                        &request,
                        &context,
                        rejection,
                        record,
                        now,
                        uow.as_ref(),
                    )?;
                    self.deps.unit_of_work_manager.commit(uow)?;
                    return Ok(outcome);
                }

                let member = GlobalMember::establish(
                    member_ref.clone(),
                    request.body.source_ref.clone(),
                    context.actor_ref.clone(),
                    now,
                )?;
                let lifecycle = GlobalLifecycleState::initial_available(
                    context.actor_ref.clone(),
                    request.body.initial_lifecycle_reason_ref.clone(),
                    now,
                );
                self.deps
                    .member_repository
                    .save_member(member.clone(), None, uow.as_ref())?;
                self.deps.lifecycle_repository.save_lifecycle(
                    member_ref.clone(),
                    lifecycle.clone(),
                    None,
                    uow.as_ref(),
                )?;

                let accepted_cursor_ref = self
                    .deps
                    .cursor_assigner
                    .assign_truth_change_cursor(uow.as_ref())?;
                let subjects = self
                    .deps
                    .truth_change_subject_mapper
                    .member_subjects(member_ref.clone());
                let change_kind_ref = IdentityChangeKindRef::new(
                    IdentityChangeKind::MemberAnchorChanged,
                    Some(request.body.source_ref.clone()),
                );
                let trace = self.command_trace_record(
                    member_ref.clone(),
                    &subjects,
                    change_kind_ref.clone(),
                    accepted_cursor_ref.clone(),
                    Some(Self::accepted_change_reason(&request.body.source_ref)),
                    Some(request.body.source_ref.clone()),
                    None,
                    context.actor_ref.clone(),
                    now,
                )?;
                self.deps
                    .trace_record_repository
                    .append_trace_record(trace.clone(), uow.as_ref())?;
                let audit_trail_ref = self.append_accepted_audit(
                    &context,
                    Some(member_ref.clone()),
                    &subjects,
                    &change_kind_ref,
                    &accepted_cursor_ref,
                    &trace,
                    now,
                    uow.as_ref(),
                )?;

                let established_outbox = self.outbox_record(
                    member_ref.clone(),
                    subjects.outbox_subject_ref.clone(),
                    change_kind_ref.clone(),
                    AcceptedOutboundMaterialKind::GlobalMemberEstablished,
                    trace.trace_record_ref.clone(),
                    now,
                )?;
                let anchor_outbox = self.outbox_record(
                    member_ref.clone(),
                    subjects.outbox_subject_ref.clone(),
                    change_kind_ref.clone(),
                    AcceptedOutboundMaterialKind::IdentityAnchorChanged,
                    trace.trace_record_ref.clone(),
                    now,
                )?;
                let established_outbox_ref = established_outbox.outbox_record_ref.clone();
                let anchor_outbox_ref = anchor_outbox.outbox_record_ref.clone();
                if let Err(error) = self.deps.outbox_repository.save_outbox_record(
                    established_outbox,
                    None,
                    uow.as_ref(),
                ) {
                    self.rollback_quietly(uow);
                    return Err(error);
                }
                if let Err(error) = self.deps.outbox_repository.save_outbox_record(
                    anchor_outbox,
                    None,
                    uow.as_ref(),
                ) {
                    self.rollback_quietly(uow);
                    return Err(error);
                }

                let stale_projection_refs =
                    self.save_projection_stale_marks(&subjects, now, uow.as_ref())?;
                let stored_result_ref = self.deps.id_generator.new_identity_stored_result_ref()?;
                let surface_marker_ref = self
                    .deps
                    .id_generator
                    .new_identity_stored_surface_marker_ref()?;
                let effect_summary_ref = self
                    .deps
                    .id_generator
                    .new_identity_command_effect_summary_ref()?;
                let summary = IdentityCommandEffectSummary::from_accepted_change(
                    effect_summary_ref,
                    context.context_ref.clone(),
                    IdentityAcceptedEffectKind::GlobalMemberCommandResult,
                    IdentityTruthRef::GlobalMember(member_ref.clone()),
                    accepted_cursor_ref.clone(),
                    vec![trace.trace_record_ref.clone()],
                    Some(audit_trail_ref),
                    vec![established_outbox_ref.clone(), anchor_outbox_ref.clone()],
                    stale_projection_refs.clone(),
                    stored_result_ref.clone(),
                );
                let result = GlobalMemberCommandResult {
                    member_ref: member_ref.clone(),
                    anchor_state_kind: Self::map_public_anchor_state(&member.anchor_state),
                    lifecycle_state_kind: Self::map_public_lifecycle_state(lifecycle.state_kind),
                    source_ref: member.source_ref.clone(),
                };
                let effect = Self::public_effect_from_summary(
                    &summary,
                    vec![subjects.audit_subject_ref.clone()],
                );
                let stored = StoredIdentityOperationResult::command_accepted(
                    stored_result_ref.clone(),
                    context.context_ref.clone(),
                    surface_marker_ref.clone(),
                    now,
                );
                self.deps
                    .stored_result_repository
                    .save_command_accepted_result(stored, uow.as_ref())?;
                self.deps
                    .effect_summary_repository
                    .save_effect_summary(summary.clone(), uow.as_ref())?;
                self.deps
                    .stored_result_repository
                    .save_command_accepted_envelope(
                        IdentityCommandAcceptedResultEnvelope::new(
                            stored_result_ref.clone(),
                            context.context_ref.clone(),
                            command_name.clone(),
                            surface_marker_ref,
                            IdentityCommandTypedResult::GlobalMember(result.clone()),
                            effect.clone(),
                            now,
                        ),
                        uow.as_ref(),
                    )?;
                self.deps
                    .idempotency_repository
                    .complete_with_stored_result(
                        record.value,
                        stored_result_ref.clone(),
                        now,
                        record.version,
                        uow.as_ref(),
                    )?;
                self.deps.unit_of_work_manager.commit(uow)?;
                Ok(IdentityCommandOutcome::Accepted(Self::accepted_response(
                    command_name,
                    stored_result_ref,
                    result,
                    effect,
                )))
            }
        }
    }

    /// Shared skeleton for the lifecycle-update command vertical slice.
    pub fn update_global_lifecycle_state(
        &self,
        request: IdentityCommandRequest<UpdateGlobalLifecycleStateRequest>,
        context: IdentityOperationContext,
    ) -> Result<IdentityCommandOutcome<GlobalLifecycleCommandResult>, ApplicationError> {
        Self::assert_command_context(&request, &context)?;
        let command_name = request.command_name.clone();
        let now = self.deps.clock.now()?;
        let uow = self.deps.unit_of_work_manager.begin()?;
        let reserve = match self.reserve_idempotency(&context, now, uow.as_ref()) {
            Ok(outcome) => outcome,
            Err(error) => {
                self.rollback_quietly(uow);
                return self
                    .map_runtime_unavailable(&command_name, error)
                    .map(IdentityCommandOutcome::Rejected);
            }
        };

        match reserve {
            IdempotencyReserveOutcome::ReplayAvailable {
                stored_result_ref, ..
            } => {
                self.rollback_quietly(uow);
                return self.load_replayed_command_outcome(
                    command_name,
                    stored_result_ref,
                    |typed| match typed {
                        IdentityCommandTypedResult::GlobalLifecycle(result) => Some(result),
                        _ => None,
                    },
                );
            }
            IdempotencyReserveOutcome::Conflict(record) => {
                let rejection = Self::protocol_rejection(
                    &command_name,
                    IdentityProtocolRejectionKind::DuplicateConflict,
                    "same idempotency key is already bound to different canonical material",
                );
                self.deps.idempotency_repository.mark_conflict(
                    record.value,
                    now,
                    record.version,
                    uow.as_ref(),
                )?;
                self.deps.unit_of_work_manager.commit(uow)?;
                Ok(IdentityCommandOutcome::Rejected(rejection))
            }
            IdempotencyReserveOutcome::InFlight(_) => {
                self.rollback_quietly(uow);
                Ok(IdentityCommandOutcome::Rejected(
                    Self::adapter_unavailable_rejection(
                        &command_name,
                        "same command idempotency key and digest is still in flight",
                    ),
                ))
            }
            IdempotencyReserveOutcome::Reserved(record) => {
                let member_v = match self
                    .deps
                    .member_repository
                    .get_member_with_version(request.body.member_ref.clone())?
                {
                    Some(value) => value,
                    None => {
                        let rejection = Self::protocol_rejection(
                            &command_name,
                            IdentityProtocolRejectionKind::NotFound,
                            "member does not exist",
                        );
                        let stored_result_ref =
                            self.deps.id_generator.new_identity_stored_result_ref()?;
                        let surface_marker_ref = self
                            .deps
                            .id_generator
                            .new_identity_stored_surface_marker_ref()?;
                        let stored = StoredIdentityOperationResult::command_rejected(
                            stored_result_ref.clone(),
                            context.context_ref.clone(),
                            surface_marker_ref.clone(),
                            now,
                        );
                        self.deps
                            .stored_result_repository
                            .save_command_rejected_result(stored, uow.as_ref())?;
                        self.deps
                            .stored_result_repository
                            .save_command_rejected_envelope(
                                IdentityCommandRejectedResultEnvelope::new(
                                    stored_result_ref.clone(),
                                    context.context_ref.clone(),
                                    command_name.clone(),
                                    surface_marker_ref,
                                    rejection.clone(),
                                    now,
                                ),
                                uow.as_ref(),
                            )?;
                        self.deps
                            .idempotency_repository
                            .complete_rejected_with_stored_result(
                                record.value,
                                stored_result_ref,
                                now,
                                record.version,
                                uow.as_ref(),
                            )?;
                        self.deps.unit_of_work_manager.commit(uow)?;
                        return Ok(IdentityCommandOutcome::Rejected(rejection));
                    }
                };
                let lifecycle_v = match self
                    .deps
                    .lifecycle_repository
                    .get_lifecycle_with_version(request.body.member_ref.clone())?
                {
                    Some(value) => value,
                    None => {
                        let rejection = Self::protocol_rejection(
                            &command_name,
                            IdentityProtocolRejectionKind::NotFound,
                            "lifecycle does not exist for the member",
                        );
                        let stored_result_ref =
                            self.deps.id_generator.new_identity_stored_result_ref()?;
                        let surface_marker_ref = self
                            .deps
                            .id_generator
                            .new_identity_stored_surface_marker_ref()?;
                        let stored = StoredIdentityOperationResult::command_rejected(
                            stored_result_ref.clone(),
                            context.context_ref.clone(),
                            surface_marker_ref.clone(),
                            now,
                        );
                        self.deps
                            .stored_result_repository
                            .save_command_rejected_result(stored, uow.as_ref())?;
                        self.deps
                            .stored_result_repository
                            .save_command_rejected_envelope(
                                IdentityCommandRejectedResultEnvelope::new(
                                    stored_result_ref.clone(),
                                    context.context_ref.clone(),
                                    command_name.clone(),
                                    surface_marker_ref,
                                    rejection.clone(),
                                    now,
                                ),
                                uow.as_ref(),
                            )?;
                        self.deps
                            .idempotency_repository
                            .complete_rejected_with_stored_result(
                                record.value,
                                stored_result_ref,
                                now,
                                record.version,
                                uow.as_ref(),
                            )?;
                        self.deps.unit_of_work_manager.commit(uow)?;
                        return Ok(IdentityCommandOutcome::Rejected(rejection));
                    }
                };

                let transition = LifecycleTransitionPolicy::for_transition(
                    lifecycle_v.value.clone(),
                    Self::map_domain_lifecycle_state(request.body.target_state),
                    request.body.reason_ref.clone(),
                    context.actor_ref.clone(),
                    context.channel,
                );
                if let Err(error) = transition
                    .assert_explicit_command()
                    .and_then(|_| transition.assert_allowed_transition())
                    .and_then(|_| transition.assert_not_project_or_runtime_state())
                {
                    let app_error = ApplicationError::from(error);
                    let rejection = Self::rejection_from_error(&command_name, &app_error)
                        .ok_or(app_error.clone())?;
                    let stored_result_ref =
                        self.deps.id_generator.new_identity_stored_result_ref()?;
                    let surface_marker_ref = self
                        .deps
                        .id_generator
                        .new_identity_stored_surface_marker_ref()?;
                    let stored = StoredIdentityOperationResult::command_rejected(
                        stored_result_ref.clone(),
                        context.context_ref.clone(),
                        surface_marker_ref.clone(),
                        now,
                    );
                    self.deps
                        .stored_result_repository
                        .save_command_rejected_result(stored, uow.as_ref())?;
                    self.deps
                        .stored_result_repository
                        .save_command_rejected_envelope(
                            IdentityCommandRejectedResultEnvelope::new(
                                stored_result_ref.clone(),
                                context.context_ref.clone(),
                                command_name.clone(),
                                surface_marker_ref,
                                rejection.clone(),
                                now,
                            ),
                            uow.as_ref(),
                        )?;
                    self.deps
                        .idempotency_repository
                        .complete_rejected_with_stored_result(
                            record.value,
                            stored_result_ref,
                            now,
                            record.version,
                            uow.as_ref(),
                        )?;
                    self.deps.unit_of_work_manager.commit(uow)?;
                    return Ok(IdentityCommandOutcome::Rejected(rejection));
                }

                let mut basis_ref = request.body.basis_ref.clone();
                if let Some(risk_ref) = request.body.action_risk_ref.clone() {
                    let guard = HighRiskLifecycleGuard::for_action(
                        Self::map_domain_lifecycle_state(request.body.target_state),
                        risk_ref.clone(),
                        basis_ref.clone(),
                        context.actor_ref.clone(),
                    );
                    if let Err(error) = guard.assert_basis_present() {
                        let app_error = ApplicationError::from(error);
                        let rejection = Self::rejection_from_error(&command_name, &app_error)
                            .ok_or(app_error.clone())?;
                        let stored_result_ref =
                            self.deps.id_generator.new_identity_stored_result_ref()?;
                        let surface_marker_ref = self
                            .deps
                            .id_generator
                            .new_identity_stored_surface_marker_ref()?;
                        let stored = StoredIdentityOperationResult::command_rejected(
                            stored_result_ref.clone(),
                            context.context_ref.clone(),
                            surface_marker_ref.clone(),
                            now,
                        );
                        self.deps
                            .stored_result_repository
                            .save_command_rejected_result(stored, uow.as_ref())?;
                        self.deps
                            .stored_result_repository
                            .save_command_rejected_envelope(
                                IdentityCommandRejectedResultEnvelope::new(
                                    stored_result_ref.clone(),
                                    context.context_ref.clone(),
                                    command_name.clone(),
                                    surface_marker_ref,
                                    rejection.clone(),
                                    now,
                                ),
                                uow.as_ref(),
                            )?;
                        self.deps
                            .idempotency_repository
                            .complete_rejected_with_stored_result(
                                record.value,
                                stored_result_ref,
                                now,
                                record.version,
                                uow.as_ref(),
                            )?;
                        self.deps.unit_of_work_manager.commit(uow)?;
                        return Ok(IdentityCommandOutcome::Rejected(rejection));
                    }
                    if let Some(basis_ref_value) = basis_ref.clone() {
                        let basis_summary = self
                            .deps
                            .external_source_resolver
                            .resolve_governance_basis(basis_ref_value.clone(), Some(risk_ref))?;
                        if let Err(error) = guard.assert_basis_matches_action(&basis_summary) {
                            let app_error = ApplicationError::from(error);
                            let rejection = Self::rejection_from_error(&command_name, &app_error)
                                .ok_or(app_error.clone())?;
                            let stored_result_ref =
                                self.deps.id_generator.new_identity_stored_result_ref()?;
                            let surface_marker_ref = self
                                .deps
                                .id_generator
                                .new_identity_stored_surface_marker_ref()?;
                            let stored = StoredIdentityOperationResult::command_rejected(
                                stored_result_ref.clone(),
                                context.context_ref.clone(),
                                surface_marker_ref.clone(),
                                now,
                            );
                            self.deps
                                .stored_result_repository
                                .save_command_rejected_result(stored, uow.as_ref())?;
                            self.deps
                                .stored_result_repository
                                .save_command_rejected_envelope(
                                    IdentityCommandRejectedResultEnvelope::new(
                                        stored_result_ref.clone(),
                                        context.context_ref.clone(),
                                        command_name.clone(),
                                        surface_marker_ref,
                                        rejection.clone(),
                                        now,
                                    ),
                                    uow.as_ref(),
                                )?;
                            self.deps
                                .idempotency_repository
                                .complete_rejected_with_stored_result(
                                    record.value,
                                    stored_result_ref,
                                    now,
                                    record.version,
                                    uow.as_ref(),
                                )?;
                            self.deps.unit_of_work_manager.commit(uow)?;
                            return Ok(IdentityCommandOutcome::Rejected(rejection));
                        }
                        basis_ref = Some(basis_summary.basis_ref);
                    }
                }

                let new_lifecycle = lifecycle_v.value.transition_to(
                    Self::map_domain_lifecycle_state(request.body.target_state),
                    request.body.reason_ref.clone(),
                    context.actor_ref.clone(),
                    now,
                    basis_ref.clone(),
                )?;
                self.deps.lifecycle_repository.save_lifecycle(
                    request.body.member_ref.clone(),
                    new_lifecycle.clone(),
                    Some(lifecycle_v.version),
                    uow.as_ref(),
                )?;

                let mut updated_member = member_v.value.clone();
                let mut anchor_state_kind = None;
                let change_kind_ref = IdentityChangeKindRef::new(
                    IdentityChangeKind::LifecycleChanged,
                    Some(request.body.reason_ref.source_ref.clone()),
                );
                if matches!(
                    request.body.target_state,
                    PublicLifecycleStateKind::Retired | PublicLifecycleStateKind::Tombstoned
                ) {
                    let anchor_reason = Self::anchor_reason_from_lifecycle(
                        &request.body.reason_ref,
                        request.body.target_state,
                    )?;
                    let next_anchor = match request.body.target_state {
                        PublicLifecycleStateKind::Retired => {
                            IdentityAnchorState::retired_held(anchor_reason, now)
                        }
                        PublicLifecycleStateKind::Tombstoned => {
                            IdentityAnchorState::tombstone_held(anchor_reason, now)
                        }
                        _ => unreachable!(),
                    };
                    updated_member.hold_anchor(next_anchor, context.actor_ref.clone())?;
                    anchor_state_kind =
                        Some(Self::map_public_anchor_state(&updated_member.anchor_state));
                    self.deps.member_repository.save_member(
                        updated_member.clone(),
                        Some(member_v.version),
                        uow.as_ref(),
                    )?;
                }

                let accepted_cursor_ref = self
                    .deps
                    .cursor_assigner
                    .assign_truth_change_cursor(uow.as_ref())?;
                let subjects = self
                    .deps
                    .truth_change_subject_mapper
                    .member_subjects(request.body.member_ref.clone());
                let trace = self.command_trace_record(
                    request.body.member_ref.clone(),
                    &subjects,
                    change_kind_ref.clone(),
                    accepted_cursor_ref.clone(),
                    Some(Self::accepted_change_reason(
                        &request.body.reason_ref.source_ref,
                    )),
                    Some(request.body.reason_ref.source_ref.clone()),
                    basis_ref.clone(),
                    context.actor_ref.clone(),
                    now,
                )?;
                self.deps
                    .trace_record_repository
                    .append_trace_record(trace.clone(), uow.as_ref())?;
                let audit_trail_ref = self.append_accepted_audit(
                    &context,
                    Some(request.body.member_ref.clone()),
                    &subjects,
                    &change_kind_ref,
                    &accepted_cursor_ref,
                    &trace,
                    now,
                    uow.as_ref(),
                )?;

                let lifecycle_outbox = self.outbox_record(
                    request.body.member_ref.clone(),
                    subjects.outbox_subject_ref.clone(),
                    change_kind_ref.clone(),
                    AcceptedOutboundMaterialKind::GlobalLifecycleChanged,
                    trace.trace_record_ref.clone(),
                    now,
                )?;
                let lifecycle_outbox_ref = lifecycle_outbox.outbox_record_ref.clone();
                if let Err(error) = self.deps.outbox_repository.save_outbox_record(
                    lifecycle_outbox,
                    None,
                    uow.as_ref(),
                ) {
                    self.rollback_quietly(uow);
                    return Err(error);
                }

                let mut outbox_refs = vec![lifecycle_outbox_ref];
                if anchor_state_kind.is_some() {
                    let anchor_outbox = self.outbox_record(
                        request.body.member_ref.clone(),
                        subjects.outbox_subject_ref.clone(),
                        IdentityChangeKindRef::new(
                            IdentityChangeKind::MemberAnchorChanged,
                            Some(request.body.reason_ref.source_ref.clone()),
                        ),
                        AcceptedOutboundMaterialKind::IdentityAnchorChanged,
                        trace.trace_record_ref.clone(),
                        now,
                    )?;
                    let anchor_ref = anchor_outbox.outbox_record_ref.clone();
                    if let Err(error) = self.deps.outbox_repository.save_outbox_record(
                        anchor_outbox,
                        None,
                        uow.as_ref(),
                    ) {
                        self.rollback_quietly(uow);
                        return Err(error);
                    }
                    outbox_refs.push(anchor_ref);
                }
                if lifecycle_v.value.is_available() != new_lifecycle.is_available() {
                    let availability_outbox = self.outbox_record(
                        request.body.member_ref.clone(),
                        subjects.outbox_subject_ref.clone(),
                        change_kind_ref.clone(),
                        AcceptedOutboundMaterialKind::GlobalMemberAvailabilityChanged,
                        trace.trace_record_ref.clone(),
                        now,
                    )?;
                    let availability_ref = availability_outbox.outbox_record_ref.clone();
                    if let Err(error) = self.deps.outbox_repository.save_outbox_record(
                        availability_outbox,
                        None,
                        uow.as_ref(),
                    ) {
                        self.rollback_quietly(uow);
                        return Err(error);
                    }
                    outbox_refs.push(availability_ref);
                }

                let stale_projection_refs =
                    self.save_projection_stale_marks(&subjects, now, uow.as_ref())?;
                let stored_result_ref = self.deps.id_generator.new_identity_stored_result_ref()?;
                let surface_marker_ref = self
                    .deps
                    .id_generator
                    .new_identity_stored_surface_marker_ref()?;
                let effect_summary_ref = self
                    .deps
                    .id_generator
                    .new_identity_command_effect_summary_ref()?;
                let summary = IdentityCommandEffectSummary::from_accepted_change(
                    effect_summary_ref,
                    context.context_ref.clone(),
                    IdentityAcceptedEffectKind::GlobalLifecycleCommandResult,
                    IdentityTruthRef::GlobalMember(request.body.member_ref.clone()),
                    accepted_cursor_ref,
                    vec![trace.trace_record_ref.clone()],
                    Some(audit_trail_ref),
                    outbox_refs.clone(),
                    stale_projection_refs,
                    stored_result_ref.clone(),
                );
                let effect = Self::public_effect_from_summary(
                    &summary,
                    vec![subjects.audit_subject_ref.clone()],
                );
                let result = GlobalLifecycleCommandResult {
                    member_ref: request.body.member_ref.clone(),
                    lifecycle_state_kind: Self::map_public_lifecycle_state(
                        new_lifecycle.state_kind,
                    ),
                    reason_ref: request.body.reason_ref.clone(),
                    basis_ref,
                    anchor_state_kind,
                };
                let stored = StoredIdentityOperationResult::command_accepted(
                    stored_result_ref.clone(),
                    context.context_ref.clone(),
                    surface_marker_ref.clone(),
                    now,
                );
                self.deps
                    .stored_result_repository
                    .save_command_accepted_result(stored, uow.as_ref())?;
                self.deps
                    .effect_summary_repository
                    .save_effect_summary(summary, uow.as_ref())?;
                self.deps
                    .stored_result_repository
                    .save_command_accepted_envelope(
                        IdentityCommandAcceptedResultEnvelope::new(
                            stored_result_ref.clone(),
                            context.context_ref.clone(),
                            command_name.clone(),
                            surface_marker_ref,
                            IdentityCommandTypedResult::GlobalLifecycle(result.clone()),
                            effect.clone(),
                            now,
                        ),
                        uow.as_ref(),
                    )?;
                self.deps
                    .idempotency_repository
                    .complete_with_stored_result(
                        record.value,
                        stored_result_ref.clone(),
                        now,
                        record.version,
                        uow.as_ref(),
                    )?;
                self.deps.unit_of_work_manager.commit(uow)?;
                Ok(IdentityCommandOutcome::Accepted(Self::accepted_response(
                    command_name,
                    stored_result_ref,
                    result,
                    effect,
                )))
            }
        }
    }

    /// Shared skeleton for the role capability summary command vertical slice.
    pub fn maintain_role_capability_summary(
        &self,
        request: IdentityCommandRequest<MaintainRoleCapabilitySummaryRequest>,
        context: IdentityOperationContext,
    ) -> Result<IdentityCommandOutcome<RoleCapabilityCommandResult>, ApplicationError> {
        Self::assert_command_context(&request, &context)?;
        let command_name = request.command_name.clone();
        let now = self.deps.clock.now()?;
        let uow = self.deps.unit_of_work_manager.begin()?;
        let reserve = match self.reserve_idempotency(&context, now, uow.as_ref()) {
            Ok(outcome) => outcome,
            Err(error) => {
                self.rollback_quietly(uow);
                return self
                    .map_runtime_unavailable(&command_name, error)
                    .map(IdentityCommandOutcome::Rejected);
            }
        };

        match reserve {
            IdempotencyReserveOutcome::ReplayAvailable {
                stored_result_ref, ..
            } => {
                self.rollback_quietly(uow);
                self.load_replayed_command_outcome(command_name, stored_result_ref, |typed| {
                    match typed {
                        IdentityCommandTypedResult::RoleCapability(result) => Some(result),
                        _ => None,
                    }
                })
            }
            IdempotencyReserveOutcome::Conflict(record) => {
                let rejection = Self::protocol_rejection(
                    &command_name,
                    IdentityProtocolRejectionKind::DuplicateConflict,
                    "same idempotency key is already bound to different canonical material",
                );
                self.deps.idempotency_repository.mark_conflict(
                    record.value,
                    now,
                    record.version,
                    uow.as_ref(),
                )?;
                self.deps.unit_of_work_manager.commit(uow)?;
                Ok(IdentityCommandOutcome::Rejected(rejection))
            }
            IdempotencyReserveOutcome::InFlight(_) => {
                self.rollback_quietly(uow);
                Ok(IdentityCommandOutcome::Rejected(
                    Self::adapter_unavailable_rejection(
                        &command_name,
                        "same command idempotency key and digest is still in flight",
                    ),
                ))
            }
            IdempotencyReserveOutcome::Reserved(record) => {
                let member_exists = self
                    .deps
                    .member_repository
                    .get_member_with_version(request.body.member_ref.clone())?
                    .is_some();
                if !member_exists {
                    let rejection =
                        self.command_not_found_rejection(&command_name, "member does not exist");
                    let outcome = self.save_replayable_rejected_outcome(
                        &command_name,
                        &request,
                        &context,
                        rejection,
                        record,
                        now,
                        uow.as_ref(),
                    )?;
                    self.deps.unit_of_work_manager.commit(uow)?;
                    return Ok(outcome);
                }

                let requested_summary_v = match request.body.requested_summary_ref.clone() {
                    Some(summary_ref) => self
                        .deps
                        .role_capability_repository
                        .get_summary_with_version(summary_ref)?,
                    None => None,
                };
                let current_by_member_v = self
                    .deps
                    .role_capability_repository
                    .find_current_summary_by_member(request.body.member_ref.clone())?;

                if let (Some(requested), Some(current)) =
                    (requested_summary_v.as_ref(), current_by_member_v.as_ref())
                {
                    if !requested
                        .value
                        .summary_ref
                        .same_summary(&current.value.summary_ref)
                    {
                        let rejection = Self::protocol_rejection(
                            &command_name,
                            IdentityProtocolRejectionKind::Conflict,
                            "requested summary ref does not match the current summary for member",
                        );
                        let outcome = self.save_replayable_rejected_outcome(
                            &command_name,
                            &request,
                            &context,
                            rejection,
                            record,
                            now,
                            uow.as_ref(),
                        )?;
                        self.deps.unit_of_work_manager.commit(uow)?;
                        return Ok(outcome);
                    }
                }

                let current_summary_v = requested_summary_v.or(current_by_member_v);
                let current_snapshot_v = self
                    .deps
                    .role_capability_repository
                    .find_source_snapshot_by_source(request.body.source_ref.clone())?;

                let source_resolution = self
                    .deps
                    .external_source_resolver
                    .resolve_role_capability_source(request.body.source_ref.clone())?;
                if !source_resolution
                    .source_ref
                    .same_source(&request.body.source_ref)
                {
                    let rejection = Self::protocol_rejection(
                        &command_name,
                        IdentityProtocolRejectionKind::InvalidRequest,
                        "resolved role source does not match request source",
                    );
                    let outcome = self.save_replayable_rejected_outcome(
                        &command_name,
                        &request,
                        &context,
                        rejection,
                        record,
                        now,
                        uow.as_ref(),
                    )?;
                    self.deps.unit_of_work_manager.commit(uow)?;
                    return Ok(outcome);
                }
                if source_resolution.source_state
                    != identity_domain::role_capability::RoleCapabilitySourceStateKind::SourceResolved
                {
                    let rejection = match source_resolution.source_state {
                        identity_domain::role_capability::RoleCapabilitySourceStateKind::SourceUnavailable => {
                            Self::adapter_unavailable_rejection(
                                &command_name,
                                "role capability source is unavailable for active summary maintenance",
                            )
                        }
                        identity_domain::role_capability::RoleCapabilitySourceStateKind::SourceUnrecognized => {
                            Self::protocol_rejection(
                                &command_name,
                                IdentityProtocolRejectionKind::PolicyDenied,
                                "role capability source is unrecognized for active summary maintenance",
                            )
                        }
                        _ => Self::protocol_rejection(
                            &command_name,
                            IdentityProtocolRejectionKind::PolicyDenied,
                            "role capability source is not current for active summary maintenance",
                        ),
                    };
                    let outcome = self.save_replayable_rejected_outcome(
                        &command_name,
                        &request,
                        &context,
                        rejection,
                        record,
                        now,
                        uow.as_ref(),
                    )?;
                    self.deps.unit_of_work_manager.commit(uow)?;
                    return Ok(outcome);
                }
                if !source_resolution.material_marker.is_safe_marker_only() {
                    let rejection = Self::protocol_rejection(
                        &command_name,
                        IdentityProtocolRejectionKind::ForbiddenBody,
                        "role capability resolver returned forbidden material marker",
                    );
                    let outcome = self.save_replayable_rejected_outcome(
                        &command_name,
                        &request,
                        &context,
                        rejection,
                        record,
                        now,
                        uow.as_ref(),
                    )?;
                    self.deps.unit_of_work_manager.commit(uow)?;
                    return Ok(outcome);
                }

                let source_version_ref = match source_resolution.source_version_ref.clone() {
                    Some(value) if value.belongs_to(&request.body.source_ref) => value,
                    _ => {
                        let rejection = Self::adapter_unavailable_rejection(
                            &command_name,
                            "role capability source version is missing or invalid",
                        );
                        let outcome = self.save_replayable_rejected_outcome(
                            &command_name,
                            &request,
                            &context,
                            rejection,
                            record,
                            now,
                            uow.as_ref(),
                        )?;
                        self.deps.unit_of_work_manager.commit(uow)?;
                        return Ok(outcome);
                    }
                };
                let effective_safe_summary_ref = match source_resolution.safe_summary_ref.clone() {
                    Some(value) if value.belongs_to_source(&request.body.source_ref) => value,
                    _ => {
                        let rejection = Self::protocol_rejection(
                            &command_name,
                            IdentityProtocolRejectionKind::PolicyDenied,
                            "role capability source safe summary is missing or invalid",
                        );
                        let outcome = self.save_replayable_rejected_outcome(
                            &command_name,
                            &request,
                            &context,
                            rejection,
                            record,
                            now,
                            uow.as_ref(),
                        )?;
                        self.deps.unit_of_work_manager.commit(uow)?;
                        return Ok(outcome);
                    }
                };
                if let Some(request_safe_summary_ref) = request.body.safe_summary_ref.clone() {
                    if !request_safe_summary_ref.belongs_to_source(&request.body.source_ref)
                        || !request_safe_summary_ref.same_safe_summary(&effective_safe_summary_ref)
                    {
                        let rejection = Self::protocol_rejection(
                            &command_name,
                            IdentityProtocolRejectionKind::PolicyDenied,
                            "request safe summary does not match authoritative role source summary",
                        );
                        let outcome = self.save_replayable_rejected_outcome(
                            &command_name,
                            &request,
                            &context,
                            rejection,
                            record,
                            now,
                            uow.as_ref(),
                        )?;
                        self.deps.unit_of_work_manager.commit(uow)?;
                        return Ok(outcome);
                    }
                }

                let authoritative_evidence_refs = source_resolution.evidence_refs.clone();
                if authoritative_evidence_refs.is_empty() {
                    let rejection = Self::protocol_rejection(
                        &command_name,
                        IdentityProtocolRejectionKind::PolicyDenied,
                        "role capability source must provide authoritative evidence",
                    );
                    let outcome = self.save_replayable_rejected_outcome(
                        &command_name,
                        &request,
                        &context,
                        rejection,
                        record,
                        now,
                        uow.as_ref(),
                    )?;
                    self.deps.unit_of_work_manager.commit(uow)?;
                    return Ok(outcome);
                }

                for evidence_ref in &request.body.evidence_refs {
                    let evidence_resolution = self
                        .deps
                        .external_source_resolver
                        .resolve_capability_evidence(evidence_ref.clone())?;
                    if !evidence_resolution.evidence_ref.same_evidence(evidence_ref) {
                        let rejection = Self::protocol_rejection(
                            &command_name,
                            IdentityProtocolRejectionKind::InvalidRequest,
                            "resolved capability evidence does not match request evidence",
                        );
                        let outcome = self.save_replayable_rejected_outcome(
                            &command_name,
                            &request,
                            &context,
                            rejection,
                            record,
                            now,
                            uow.as_ref(),
                        )?;
                        self.deps.unit_of_work_manager.commit(uow)?;
                        return Ok(outcome);
                    }
                    if evidence_resolution.evidence_state
                        != identity_domain::reference_state::ReferenceResolutionStateKind::Resolved
                    {
                        let rejection = Self::protocol_rejection(
                            &command_name,
                            IdentityProtocolRejectionKind::PolicyDenied,
                            "capability evidence must resolve before accepted role summary write",
                        );
                        let outcome = self.save_replayable_rejected_outcome(
                            &command_name,
                            &request,
                            &context,
                            rejection,
                            record,
                            now,
                            uow.as_ref(),
                        )?;
                        self.deps.unit_of_work_manager.commit(uow)?;
                        return Ok(outcome);
                    }
                    if !authoritative_evidence_refs
                        .iter()
                        .any(|candidate| candidate.same_evidence(evidence_ref))
                    {
                        let rejection = Self::protocol_rejection(
                            &command_name,
                            IdentityProtocolRejectionKind::PolicyDenied,
                            "requested capability evidence is not authoritative for the source",
                        );
                        let outcome = self.save_replayable_rejected_outcome(
                            &command_name,
                            &request,
                            &context,
                            rejection,
                            record,
                            now,
                            uow.as_ref(),
                        )?;
                        self.deps.unit_of_work_manager.commit(uow)?;
                        return Ok(outcome);
                    }
                }

                let snapshot_ref = match current_snapshot_v.as_ref() {
                    Some(value) => value.value.snapshot_ref.clone(),
                    None => identity_contracts::refs::RoleCapabilitySourceSnapshotRef::from_id(
                        self.deps
                            .id_generator
                            .new_role_capability_source_snapshot_id()?,
                    ),
                };
                let snapshot = RoleCapabilitySourceSnapshot::from_resolved_source(
                    snapshot_ref.clone(),
                    request.body.source_ref.clone(),
                    source_version_ref,
                    effective_safe_summary_ref.clone(),
                    authoritative_evidence_refs.clone(),
                    now,
                )?;
                let policy = RoleCapabilitySourcePolicy::for_summary_update(
                    request.body.member_ref.clone(),
                    snapshot.clone(),
                    authoritative_evidence_refs.clone(),
                    request.body.change_reason_ref.clone(),
                    context.actor_ref.clone(),
                    context.channel,
                    request.body.change_material_marker.clone(),
                );
                if let Err(error) = policy
                    .assert_member_exists(member_exists)
                    .and_then(|_| policy.assert_no_forbidden_body())
                    .and_then(|_| policy.assert_not_automatic_scoring())
                    .and_then(|_| policy.assert_source_or_evidence_present())
                    .and_then(|_| policy.assert_source_usable())
                {
                    let app_error = ApplicationError::from(error);
                    let rejection = Self::rejection_from_error(&command_name, &app_error)
                        .ok_or(app_error.clone())?;
                    let outcome = self.save_replayable_rejected_outcome(
                        &command_name,
                        &request,
                        &context,
                        rejection,
                        record,
                        now,
                        uow.as_ref(),
                    )?;
                    self.deps.unit_of_work_manager.commit(uow)?;
                    return Ok(outcome);
                }

                let summary_ref = match current_summary_v.as_ref() {
                    Some(value) => value.value.summary_ref.clone(),
                    None => match request.body.requested_summary_ref.clone() {
                        Some(value) => value,
                        None => identity_contracts::refs::RoleCapabilitySummaryRef::from_id(
                            self.deps.id_generator.new_role_capability_summary_id()?,
                        ),
                    },
                };
                let mut summary = if let Some(current_summary_v) = current_summary_v.as_ref() {
                    if !current_summary_v
                        .value
                        .belongs_to(request.body.member_ref.clone())
                    {
                        let rejection = Self::protocol_rejection(
                            &command_name,
                            IdentityProtocolRejectionKind::Conflict,
                            "current role capability summary belongs to a different member",
                        );
                        let outcome = self.save_replayable_rejected_outcome(
                            &command_name,
                            &request,
                            &context,
                            rejection,
                            record,
                            now,
                            uow.as_ref(),
                        )?;
                        self.deps.unit_of_work_manager.commit(uow)?;
                        return Ok(outcome);
                    }
                    current_summary_v.value.clone()
                } else {
                    RoleCapabilitySummary::create_for_member(
                        summary_ref.clone(),
                        request.body.member_ref.clone(),
                        &snapshot,
                        effective_safe_summary_ref.clone(),
                        authoritative_evidence_refs.clone(),
                        context.actor_ref.clone(),
                        now,
                    )?
                };

                if let Some(role_source_ref) = request.body.role_source_ref.clone() {
                    if !role_source_ref
                        .canonical_source()
                        .same_source(&request.body.source_ref)
                    {
                        let rejection = Self::protocol_rejection(
                            &command_name,
                            IdentityProtocolRejectionKind::InvalidRequest,
                            "role source ref does not match the requested role capability source",
                        );
                        let outcome = self.save_replayable_rejected_outcome(
                            &command_name,
                            &request,
                            &context,
                            rejection,
                            record,
                            now,
                            uow.as_ref(),
                        )?;
                        self.deps.unit_of_work_manager.commit(uow)?;
                        return Ok(outcome);
                    }
                    summary.attach_role_source(
                        role_source_ref,
                        &snapshot,
                        context.actor_ref.clone(),
                        now,
                    )?;
                }
                summary.update_capability_summary(
                    request.body.capability_source_refs.clone(),
                    authoritative_evidence_refs.clone(),
                    effective_safe_summary_ref.clone(),
                    context.actor_ref.clone(),
                    now,
                )?;

                self.deps.role_capability_repository.save_source_snapshot(
                    snapshot.clone(),
                    current_snapshot_v.as_ref().map(|value| value.version),
                    uow.as_ref(),
                )?;
                self.deps.role_capability_repository.save_summary(
                    summary.clone(),
                    current_summary_v.as_ref().map(|value| value.version),
                    uow.as_ref(),
                )?;

                let accepted_cursor_ref = self
                    .deps
                    .cursor_assigner
                    .assign_truth_change_cursor(uow.as_ref())?;
                let subjects = self
                    .deps
                    .truth_change_subject_mapper
                    .role_capability_subjects(summary_ref.clone());
                let change_kind_ref = IdentityChangeKindRef::new(
                    IdentityChangeKind::RoleCapabilitySummaryChanged,
                    Some(request.body.source_ref.source_ref.clone()),
                );
                let trace = self.command_trace_record(
                    request.body.member_ref.clone(),
                    &subjects,
                    change_kind_ref.clone(),
                    accepted_cursor_ref.clone(),
                    Some(Self::accepted_change_reason(
                        &request.body.change_reason_ref.source_ref,
                    )),
                    Some(request.body.source_ref.source_ref.clone()),
                    None,
                    context.actor_ref.clone(),
                    now,
                )?;
                self.deps
                    .trace_record_repository
                    .append_trace_record(trace.clone(), uow.as_ref())?;
                let audit_trail_ref = self.append_accepted_audit(
                    &context,
                    Some(request.body.member_ref.clone()),
                    &subjects,
                    &change_kind_ref,
                    &accepted_cursor_ref,
                    &trace,
                    now,
                    uow.as_ref(),
                )?;

                let summary_outbox = self.outbox_record(
                    request.body.member_ref.clone(),
                    subjects.outbox_subject_ref.clone(),
                    change_kind_ref.clone(),
                    AcceptedOutboundMaterialKind::RoleCapabilitySummaryChanged,
                    trace.trace_record_ref.clone(),
                    now,
                )?;
                let summary_outbox_ref = summary_outbox.outbox_record_ref.clone();
                if let Err(error) = self.deps.outbox_repository.save_outbox_record(
                    summary_outbox,
                    None,
                    uow.as_ref(),
                ) {
                    self.rollback_quietly(uow);
                    return Err(error);
                }
                let source_subjects = self
                    .deps
                    .truth_change_subject_mapper
                    .role_capability_source_snapshot_subjects(snapshot_ref.clone());
                let source_outbox = self.outbox_record(
                    request.body.member_ref.clone(),
                    source_subjects.outbox_subject_ref.clone(),
                    change_kind_ref.clone(),
                    AcceptedOutboundMaterialKind::RoleCapabilitySourceStateChanged,
                    trace.trace_record_ref.clone(),
                    now,
                )?;
                let source_outbox_ref = source_outbox.outbox_record_ref.clone();
                if let Err(error) = self.deps.outbox_repository.save_outbox_record(
                    source_outbox,
                    None,
                    uow.as_ref(),
                ) {
                    self.rollback_quietly(uow);
                    return Err(error);
                }

                let stale_projection_refs =
                    self.save_projection_stale_marks(&subjects, now, uow.as_ref())?;
                let stored_result_ref = self.deps.id_generator.new_identity_stored_result_ref()?;
                let surface_marker_ref = self
                    .deps
                    .id_generator
                    .new_identity_stored_surface_marker_ref()?;
                let effect_summary_ref = self
                    .deps
                    .id_generator
                    .new_identity_command_effect_summary_ref()?;
                let summary_effect = IdentityCommandEffectSummary::from_accepted_change(
                    effect_summary_ref,
                    context.context_ref.clone(),
                    IdentityAcceptedEffectKind::RoleCapabilityCommandResult,
                    IdentityTruthRef::RoleCapabilitySummary(summary_ref.clone()),
                    accepted_cursor_ref.clone(),
                    vec![trace.trace_record_ref.clone()],
                    Some(audit_trail_ref),
                    vec![summary_outbox_ref, source_outbox_ref],
                    stale_projection_refs,
                    stored_result_ref.clone(),
                );
                let result = RoleCapabilityCommandResult {
                    member_ref: request.body.member_ref.clone(),
                    summary_ref: summary_ref.clone(),
                    source_snapshot_ref: snapshot_ref,
                    summary_state_kind: Self::map_public_role_summary_state(summary.summary_state),
                    source_state_kind: Self::map_public_role_source_state(snapshot.source_state),
                    role_source_ref: summary.role_source_ref.clone(),
                    capability_source_refs: summary.capability_source_refs.clone(),
                    evidence_refs: summary.evidence_refs.clone(),
                    safe_summary_ref: Some(effective_safe_summary_ref),
                };
                let effect = Self::public_effect_from_summary(
                    &summary_effect,
                    vec![subjects.audit_subject_ref.clone()],
                );
                let stored = StoredIdentityOperationResult::command_accepted(
                    stored_result_ref.clone(),
                    context.context_ref.clone(),
                    surface_marker_ref.clone(),
                    now,
                );
                self.deps
                    .stored_result_repository
                    .save_command_accepted_result(stored, uow.as_ref())?;
                self.deps
                    .effect_summary_repository
                    .save_effect_summary(summary_effect, uow.as_ref())?;
                self.deps
                    .stored_result_repository
                    .save_command_accepted_envelope(
                        IdentityCommandAcceptedResultEnvelope::new(
                            stored_result_ref.clone(),
                            context.context_ref.clone(),
                            command_name.clone(),
                            surface_marker_ref,
                            IdentityCommandTypedResult::RoleCapability(result.clone()),
                            effect.clone(),
                            now,
                        ),
                        uow.as_ref(),
                    )?;
                self.deps
                    .idempotency_repository
                    .complete_with_stored_result(
                        record.value,
                        stored_result_ref.clone(),
                        now,
                        record.version,
                        uow.as_ref(),
                    )?;
                self.deps.unit_of_work_manager.commit(uow)?;
                Ok(IdentityCommandOutcome::Accepted(Self::accepted_response(
                    command_name,
                    stored_result_ref,
                    result,
                    effect,
                )))
            }
        }
    }

    /// Shared skeleton for the career append command vertical slice.
    pub fn append_career_record(
        &self,
        request: IdentityCommandRequest<AppendCareerRecordRequest>,
        context: IdentityOperationContext,
    ) -> Result<IdentityCommandOutcome<CareerRecordCommandResult>, ApplicationError> {
        Self::assert_command_context(&request, &context)?;
        let command_name = request.command_name.clone();
        let now = self.deps.clock.now()?;
        let uow = self.deps.unit_of_work_manager.begin()?;
        let reserve = match self.reserve_idempotency(&context, now, uow.as_ref()) {
            Ok(outcome) => outcome,
            Err(error) => {
                self.rollback_quietly(uow);
                return self
                    .map_runtime_unavailable(&command_name, error)
                    .map(IdentityCommandOutcome::Rejected);
            }
        };

        match reserve {
            IdempotencyReserveOutcome::ReplayAvailable {
                stored_result_ref, ..
            } => {
                self.rollback_quietly(uow);
                self.load_replayed_command_outcome(command_name, stored_result_ref, |typed| {
                    match typed {
                        IdentityCommandTypedResult::CareerRecord(result) => Some(result),
                        _ => None,
                    }
                })
            }
            IdempotencyReserveOutcome::Conflict(record) => {
                let rejection = Self::protocol_rejection(
                    &command_name,
                    IdentityProtocolRejectionKind::DuplicateConflict,
                    "same idempotency key is already bound to different canonical material",
                );
                self.deps.idempotency_repository.mark_conflict(
                    record.value,
                    now,
                    record.version,
                    uow.as_ref(),
                )?;
                self.deps.unit_of_work_manager.commit(uow)?;
                Ok(IdentityCommandOutcome::Rejected(rejection))
            }
            IdempotencyReserveOutcome::InFlight(_) => {
                self.rollback_quietly(uow);
                Ok(IdentityCommandOutcome::Rejected(
                    Self::adapter_unavailable_rejection(
                        &command_name,
                        "same command idempotency key and digest is still in flight",
                    ),
                ))
            }
            IdempotencyReserveOutcome::Reserved(record) => {
                let member_exists = self
                    .deps
                    .member_repository
                    .get_member_with_version(request.body.member_ref.clone())?
                    .is_some();
                if !member_exists {
                    let rejection =
                        self.command_not_found_rejection(&command_name, "member does not exist");
                    let outcome = self.save_replayable_rejected_outcome(
                        &command_name,
                        &request,
                        &context,
                        rejection,
                        record,
                        now,
                        uow.as_ref(),
                    )?;
                    self.deps.unit_of_work_manager.commit(uow)?;
                    return Ok(outcome);
                }

                let source_summary = self
                    .deps
                    .external_source_resolver
                    .resolve_work_participation(request.body.work_source_ref.clone())?;
                if !source_summary
                    .work_source_ref
                    .same_source(&request.body.work_source_ref)
                    || !source_summary
                        .source_marker_ref
                        .same_marker(&request.body.source_marker_ref)
                {
                    let rejection = Self::protocol_rejection(
                        &command_name,
                        IdentityProtocolRejectionKind::InvalidRequest,
                        "resolved work participation source does not match request markers",
                    );
                    let outcome = self.save_replayable_rejected_outcome(
                        &command_name,
                        &request,
                        &context,
                        rejection,
                        record,
                        now,
                        uow.as_ref(),
                    )?;
                    self.deps.unit_of_work_manager.commit(uow)?;
                    return Ok(outcome);
                }

                let duplicate_ref = self
                    .deps
                    .career_record_repository
                    .find_duplicate_source_record(request.body.source_marker_ref.clone())?;
                let duplicate_refs = duplicate_ref.clone().into_iter().collect::<Vec<_>>();

                let original_record_v = match request.body.original_record_ref.clone() {
                    Some(record_ref) => self
                        .deps
                        .career_record_repository
                        .get_career_record(record_ref)?,
                    None => None,
                };

                let policy = if request.body.change_intent
                    == identity_contracts::refs::CareerRecordChangeIntent::AppendCorrection
                {
                    CareerAppendPolicy::for_correction(
                        request.body.member_ref.clone(),
                        member_exists,
                        source_summary.clone(),
                        Vec::new(),
                        request.body.append_reason_ref.clone(),
                        context.actor_ref.clone(),
                        context.channel,
                        request.body.append_material_marker.clone(),
                    )
                } else {
                    CareerAppendPolicy::for_append(
                        request.body.member_ref.clone(),
                        member_exists,
                        source_summary.clone(),
                        duplicate_refs,
                        request.body.append_reason_ref.clone(),
                        context.actor_ref.clone(),
                        context.channel,
                        request.body.change_intent,
                        request.body.append_material_marker.clone(),
                    )
                };

                if let Err(error) = policy
                    .assert_member_exists()
                    .and_then(|_| policy.assert_not_work_truth_write())
                    .and_then(|_| policy.assert_allowed_write_channel())
                    .and_then(|_| policy.assert_append_only())
                {
                    let app_error = ApplicationError::from(error);
                    let rejection = Self::rejection_from_error(&command_name, &app_error)
                        .ok_or(app_error.clone())?;
                    let outcome = self.save_replayable_rejected_outcome(
                        &command_name,
                        &request,
                        &context,
                        rejection,
                        record,
                        now,
                        uow.as_ref(),
                    )?;
                    self.deps.unit_of_work_manager.commit(uow)?;
                    return Ok(outcome);
                }

                if duplicate_ref.is_some()
                    && request.body.change_intent
                        != identity_contracts::refs::CareerRecordChangeIntent::AppendCorrection
                {
                    let rejection = Self::protocol_rejection(
                        &command_name,
                        IdentityProtocolRejectionKind::Conflict,
                        "duplicate career source marker must not create a second career record",
                    );
                    let outcome = self.save_replayable_rejected_outcome(
                        &command_name,
                        &request,
                        &context,
                        rejection,
                        record,
                        now,
                        uow.as_ref(),
                    )?;
                    self.deps.unit_of_work_manager.commit(uow)?;
                    return Ok(outcome);
                }

                let record_ref = match request.body.requested_career_record_ref.clone() {
                    Some(value) => value,
                    None => identity_contracts::refs::CareerRecordRef::from_id(
                        self.deps.id_generator.new_career_record_id()?,
                    ),
                };
                let (career_record, superseded_record_ref, create_outbox) = match request
                    .body
                    .change_intent
                {
                    identity_contracts::refs::CareerRecordChangeIntent::AppendNew => {
                        policy.assert_not_duplicate()?;
                        policy.assert_source_trusted()?;
                        (
                            CareerRecord::append_from_work_source(
                                record_ref.clone(),
                                request.body.member_ref.clone(),
                                source_summary.clone(),
                                request.body.append_reason_ref.clone(),
                                context.actor_ref.clone(),
                                now,
                            )?,
                            None,
                            Some(AcceptedOutboundMaterialKind::CareerRecordAppended),
                        )
                    }
                    identity_contracts::refs::CareerRecordChangeIntent::AppendCorrection => {
                        policy.assert_source_trusted()?;
                        let mut original_record = match original_record_v.clone() {
                            Some(value) => value,
                            None => {
                                let rejection = self.command_not_found_rejection(
                                    &command_name,
                                    "original career record does not exist",
                                );
                                let outcome = self.save_replayable_rejected_outcome(
                                    &command_name,
                                    &request,
                                    &context,
                                    rejection,
                                    record,
                                    now,
                                    uow.as_ref(),
                                )?;
                                self.deps.unit_of_work_manager.commit(uow)?;
                                return Ok(outcome);
                            }
                        };
                        if !original_record
                            .value
                            .member_ref
                            .same_member(&request.body.member_ref)
                        {
                            let rejection = Self::protocol_rejection(
                                &command_name,
                                IdentityProtocolRejectionKind::PolicyDenied,
                                "original career record belongs to a different member",
                            );
                            let outcome = self.save_replayable_rejected_outcome(
                                &command_name,
                                &request,
                                &context,
                                rejection,
                                record,
                                now,
                                uow.as_ref(),
                            )?;
                            self.deps.unit_of_work_manager.commit(uow)?;
                            return Ok(outcome);
                        }
                        let correction_record = CareerRecord::correction_for_record(
                            record_ref.clone(),
                            original_record.value.career_record_ref.clone(),
                            request.body.member_ref.clone(),
                            source_summary.clone(),
                            request.body.append_reason_ref.clone(),
                            context.actor_ref.clone(),
                            now,
                        )?;
                        original_record.value.mark_superseded_by_correction(
                            record_ref.clone(),
                            context.actor_ref.clone(),
                            now,
                        )?;
                        self.deps
                            .career_record_repository
                            .save_career_record_state(
                                original_record.value.clone(),
                                original_record.version,
                                uow.as_ref(),
                            )?;
                        (
                            correction_record,
                            request.body.original_record_ref.clone(),
                            Some(AcceptedOutboundMaterialKind::CareerCorrectionAppended),
                        )
                    }
                    identity_contracts::refs::CareerRecordChangeIntent::MarkSourcePendingReview => {
                        if !source_summary.requires_review() {
                            let rejection = Self::protocol_rejection(
                                &command_name,
                                IdentityProtocolRejectionKind::PolicyDenied,
                                "pending review intent requires a reviewable work source state",
                            );
                            let outcome = self.save_replayable_rejected_outcome(
                                &command_name,
                                &request,
                                &context,
                                rejection,
                                record,
                                now,
                                uow.as_ref(),
                            )?;
                            self.deps.unit_of_work_manager.commit(uow)?;
                            return Ok(outcome);
                        }
                        (
                            CareerRecord::pending_review(
                                record_ref.clone(),
                                request.body.member_ref.clone(),
                                source_summary.clone(),
                                request.body.append_reason_ref.clone(),
                                context.actor_ref.clone(),
                                now,
                            )?,
                            None,
                            None,
                        )
                    }
                    _ => {
                        let rejection = Self::protocol_rejection(
                            &command_name,
                            IdentityProtocolRejectionKind::PolicyDenied,
                            "career history is append-only",
                        );
                        let outcome = self.save_replayable_rejected_outcome(
                            &command_name,
                            &request,
                            &context,
                            rejection,
                            record,
                            now,
                            uow.as_ref(),
                        )?;
                        self.deps.unit_of_work_manager.commit(uow)?;
                        return Ok(outcome);
                    }
                };

                self.deps
                    .career_record_repository
                    .append_career_record(career_record.clone(), uow.as_ref())?;
                let accepted_cursor_ref = self
                    .deps
                    .cursor_assigner
                    .assign_truth_change_cursor(uow.as_ref())?;
                let subjects = self
                    .deps
                    .truth_change_subject_mapper
                    .career_record_subjects(career_record.career_record_ref.clone());
                let change_kind_ref = IdentityChangeKindRef::new(
                    IdentityChangeKind::CareerRecordChanged,
                    Some(request.body.work_source_ref.source_ref.clone()),
                );
                let trace = self.command_trace_record(
                    request.body.member_ref.clone(),
                    &subjects,
                    change_kind_ref.clone(),
                    accepted_cursor_ref.clone(),
                    Some(Self::accepted_change_reason(
                        &request.body.append_reason_ref.source_ref,
                    )),
                    Some(request.body.work_source_ref.source_ref.clone()),
                    None,
                    context.actor_ref.clone(),
                    now,
                )?;
                self.deps
                    .trace_record_repository
                    .append_trace_record(trace.clone(), uow.as_ref())?;
                let audit_trail_ref = self.append_accepted_audit(
                    &context,
                    Some(request.body.member_ref.clone()),
                    &subjects,
                    &change_kind_ref,
                    &accepted_cursor_ref,
                    &trace,
                    now,
                    uow.as_ref(),
                )?;

                let mut outbox_refs = Vec::new();
                if let Some(material_kind) = create_outbox {
                    let outbox = self.outbox_record(
                        request.body.member_ref.clone(),
                        subjects.outbox_subject_ref.clone(),
                        change_kind_ref.clone(),
                        material_kind,
                        trace.trace_record_ref.clone(),
                        now,
                    )?;
                    let outbox_ref = outbox.outbox_record_ref.clone();
                    if let Err(error) =
                        self.deps
                            .outbox_repository
                            .save_outbox_record(outbox, None, uow.as_ref())
                    {
                        self.rollback_quietly(uow);
                        return Err(error);
                    }
                    outbox_refs.push(outbox_ref);
                }

                let stale_projection_refs =
                    self.save_projection_stale_marks(&subjects, now, uow.as_ref())?;
                let stored_result_ref = self.deps.id_generator.new_identity_stored_result_ref()?;
                let surface_marker_ref = self
                    .deps
                    .id_generator
                    .new_identity_stored_surface_marker_ref()?;
                let effect_summary_ref = self
                    .deps
                    .id_generator
                    .new_identity_command_effect_summary_ref()?;
                let effect_summary = IdentityCommandEffectSummary::from_accepted_change(
                    effect_summary_ref,
                    context.context_ref.clone(),
                    IdentityAcceptedEffectKind::CareerRecordCommandResult,
                    IdentityTruthRef::CareerRecord(career_record.career_record_ref.clone()),
                    accepted_cursor_ref.clone(),
                    vec![trace.trace_record_ref.clone()],
                    Some(audit_trail_ref),
                    outbox_refs,
                    stale_projection_refs,
                    stored_result_ref.clone(),
                );
                let result = CareerRecordCommandResult {
                    member_ref: request.body.member_ref.clone(),
                    career_record_ref: career_record.career_record_ref.clone(),
                    record_state_kind: Self::map_public_career_record_state(
                        career_record.record_state,
                    ),
                    project_participation_ref: career_record.project_participation_ref.clone(),
                    work_source_ref: career_record.work_source_ref.clone(),
                    source_marker_ref: career_record.source_marker_ref.clone(),
                    career_summary_ref: career_record.career_summary_ref.clone(),
                    correction_of_ref: career_record.correction_of_ref.clone(),
                    superseded_record_ref: superseded_record_ref,
                };
                let effect = Self::public_effect_from_summary(
                    &effect_summary,
                    vec![subjects.audit_subject_ref.clone()],
                );
                let stored = StoredIdentityOperationResult::command_accepted(
                    stored_result_ref.clone(),
                    context.context_ref.clone(),
                    surface_marker_ref.clone(),
                    now,
                );
                self.deps
                    .stored_result_repository
                    .save_command_accepted_result(stored, uow.as_ref())?;
                self.deps
                    .effect_summary_repository
                    .save_effect_summary(effect_summary, uow.as_ref())?;
                self.deps
                    .stored_result_repository
                    .save_command_accepted_envelope(
                        IdentityCommandAcceptedResultEnvelope::new(
                            stored_result_ref.clone(),
                            context.context_ref.clone(),
                            command_name.clone(),
                            surface_marker_ref,
                            IdentityCommandTypedResult::CareerRecord(result.clone()),
                            effect.clone(),
                            now,
                        ),
                        uow.as_ref(),
                    )?;
                self.deps
                    .idempotency_repository
                    .complete_with_stored_result(
                        record.value,
                        stored_result_ref.clone(),
                        now,
                        record.version,
                        uow.as_ref(),
                    )?;
                self.deps.unit_of_work_manager.commit(uow)?;
                Ok(IdentityCommandOutcome::Accepted(Self::accepted_response(
                    command_name,
                    stored_result_ref,
                    result,
                    effect,
                )))
            }
        }
    }

    /// Shared skeleton for the memory reference command vertical slice.
    pub fn maintain_memory_reference(
        &self,
        request: IdentityCommandRequest<MaintainMemoryReferenceRequest>,
        context: IdentityOperationContext,
    ) -> Result<IdentityCommandOutcome<MemoryReferenceCommandResult>, ApplicationError> {
        Self::assert_command_context(&request, &context)?;
        let command_name = request.command_name.clone();
        let now = self.deps.clock.now()?;
        let uow = self.deps.unit_of_work_manager.begin()?;
        let reserve = match self.reserve_idempotency(&context, now, uow.as_ref()) {
            Ok(outcome) => outcome,
            Err(error) => {
                self.rollback_quietly(uow);
                return self
                    .map_runtime_unavailable(&command_name, error)
                    .map(IdentityCommandOutcome::Rejected);
            }
        };

        match reserve {
            IdempotencyReserveOutcome::ReplayAvailable {
                stored_result_ref, ..
            } => {
                self.rollback_quietly(uow);
                self.load_replayed_command_outcome(command_name, stored_result_ref, |typed| {
                    match typed {
                        IdentityCommandTypedResult::MemoryReference(result) => Some(result),
                        _ => None,
                    }
                })
            }
            IdempotencyReserveOutcome::Conflict(record) => {
                let rejection = Self::protocol_rejection(
                    &command_name,
                    IdentityProtocolRejectionKind::DuplicateConflict,
                    "same idempotency key is already bound to different canonical material",
                );
                self.deps.idempotency_repository.mark_conflict(
                    record.value,
                    now,
                    record.version,
                    uow.as_ref(),
                )?;
                self.deps.unit_of_work_manager.commit(uow)?;
                Ok(IdentityCommandOutcome::Rejected(rejection))
            }
            IdempotencyReserveOutcome::InFlight(_) => {
                self.rollback_quietly(uow);
                Ok(IdentityCommandOutcome::Rejected(
                    Self::adapter_unavailable_rejection(
                        &command_name,
                        "same command idempotency key and digest is still in flight",
                    ),
                ))
            }
            IdempotencyReserveOutcome::Reserved(record) => {
                let member_exists = self
                    .deps
                    .member_repository
                    .get_member_with_version(request.body.member_ref.clone())?
                    .is_some();
                if !member_exists {
                    let rejection =
                        self.command_not_found_rejection(&command_name, "member does not exist");
                    let outcome = self.save_replayable_rejected_outcome(
                        &command_name,
                        &request,
                        &context,
                        rejection,
                        record,
                        now,
                        uow.as_ref(),
                    )?;
                    self.deps.unit_of_work_manager.commit(uow)?;
                    return Ok(outcome);
                }

                let source_summary = if request.body.change_intent
                    == identity_contracts::refs::MemoryReferenceChangeIntent::RecordArchiveHandoffResult
                    && request.body.archive_handoff_ref.is_some()
                {
                    self.deps.external_source_resolver.resolve_archive_handoff_source(
                        request
                            .body
                            .archive_handoff_ref
                            .clone()
                            .expect("checked is_some"),
                    )?
                } else {
                    self.deps
                        .external_source_resolver
                        .resolve_memory_reference_source(request.body.source_ref.clone())?
                };
                if source_summary.source_ref != request.body.source_ref {
                    let rejection = Self::protocol_rejection(
                        &command_name,
                        IdentityProtocolRejectionKind::InvalidRequest,
                        "resolved memory reference source does not match request source",
                    );
                    let outcome = self.save_replayable_rejected_outcome(
                        &command_name,
                        &request,
                        &context,
                        rejection,
                        record,
                        now,
                        uow.as_ref(),
                    )?;
                    self.deps.unit_of_work_manager.commit(uow)?;
                    return Ok(outcome);
                }
                if !source_summary.has_reference() {
                    let rejection = Self::protocol_rejection(
                        &command_name,
                        IdentityProtocolRejectionKind::InvalidRequest,
                        "memory reference change requires at least one formal marker",
                    );
                    let outcome = self.save_replayable_rejected_outcome(
                        &command_name,
                        &request,
                        &context,
                        rejection,
                        record,
                        now,
                        uow.as_ref(),
                    )?;
                    self.deps.unit_of_work_manager.commit(uow)?;
                    return Ok(outcome);
                }

                let existing_reference_v = if let Some(reference_ref) =
                    request.body.requested_memory_reference_ref.clone()
                {
                    self.deps
                        .memory_reference_repository
                        .get_memory_reference_with_version(reference_ref)?
                } else if let Some(memory_ref) = request.body.memory_ref.clone() {
                    self.deps
                        .memory_reference_repository
                        .find_reference_by_memory(request.body.member_ref.clone(), memory_ref)?
                } else if let Some(archive_ref) = request.body.archive_ref.clone() {
                    self.deps
                        .memory_reference_repository
                        .find_reference_by_archive(request.body.member_ref.clone(), archive_ref)?
                } else if let Some(handoff_ref) = request.body.archive_handoff_ref.clone() {
                    self.deps
                        .memory_reference_repository
                        .find_reference_by_handoff(handoff_ref)?
                } else {
                    None
                };

                let policy = match request.body.change_intent {
                    identity_contracts::refs::MemoryReferenceChangeIntent::LinkMemory => {
                        MemoryReferencePolicy::for_link(
                            request.body.member_ref.clone(),
                            member_exists,
                            source_summary.clone(),
                            request.body.reason_ref.clone(),
                            context.actor_ref.clone(),
                            context.channel,
                            request.body.change_material_marker.clone(),
                        )
                    }
                    identity_contracts::refs::MemoryReferenceChangeIntent::RefreshState
                        => {
                        MemoryReferencePolicy::for_refresh(
                            request.body.member_ref.clone(),
                            member_exists,
                            source_summary.clone(),
                            request.body.reason_ref.clone(),
                            context.actor_ref.clone(),
                            context.channel,
                            request.body.change_material_marker.clone(),
                        )
                    }
                    identity_contracts::refs::MemoryReferenceChangeIntent::MarkPendingVerification => {
                        MemoryReferencePolicy {
                            member_ref: request.body.member_ref.clone(),
                            member_exists,
                            source_summary: source_summary.clone(),
                            reason_ref: request.body.reason_ref.clone(),
                            actor_ref: context.actor_ref.clone(),
                            operation_channel: context.channel,
                            change_intent: identity_contracts::refs::MemoryReferenceChangeIntent::MarkPendingVerification,
                            change_material_marker: request.body.change_material_marker.clone(),
                        }
                    }
                    identity_contracts::refs::MemoryReferenceChangeIntent::AttachArchive => {
                        MemoryReferencePolicy {
                            member_ref: request.body.member_ref.clone(),
                            member_exists,
                            source_summary: source_summary.clone(),
                            reason_ref: request.body.reason_ref.clone(),
                            actor_ref: context.actor_ref.clone(),
                            operation_channel: context.channel,
                            change_intent: identity_contracts::refs::MemoryReferenceChangeIntent::AttachArchive,
                            change_material_marker: request.body.change_material_marker.clone(),
                        }
                    }
                    _ => MemoryReferencePolicy::for_archive_handoff(
                        request.body.member_ref.clone(),
                        member_exists,
                        source_summary.clone(),
                        request.body.reason_ref.clone(),
                        context.actor_ref.clone(),
                        context.channel,
                        request.body.change_material_marker.clone(),
                    ),
                };
                if let Err(error) = policy
                    .assert_member_exists()
                    .and_then(|_| policy.assert_reference_present())
                    .and_then(|_| policy.assert_body_free())
                    .and_then(|_| policy.assert_handoff_marker_body_free())
                    .and_then(|_| policy.assert_not_external_owner_write())
                    .and_then(|_| policy.assert_allowed_write_channel())
                    .and_then(|_| policy.assert_source_trusted())
                {
                    let app_error = ApplicationError::from(error);
                    let rejection = Self::rejection_from_error(&command_name, &app_error)
                        .ok_or(app_error.clone())?;
                    let outcome = self.save_replayable_rejected_outcome(
                        &command_name,
                        &request,
                        &context,
                        rejection,
                        record,
                        now,
                        uow.as_ref(),
                    )?;
                    self.deps.unit_of_work_manager.commit(uow)?;
                    return Ok(outcome);
                }

                let reference_ref = match existing_reference_v.as_ref() {
                    Some(value) => value.value.memory_reference_ref.clone(),
                    None => match request.body.requested_memory_reference_ref.clone() {
                        Some(value) => value,
                        None => identity_contracts::refs::MemoryReferenceRef::from_id(
                            self.deps.id_generator.new_memory_reference_id()?,
                        ),
                    },
                };

                let memory_reference = match request.body.change_intent {
                    identity_contracts::refs::MemoryReferenceChangeIntent::LinkMemory => {
                        if let Some(existing_reference_v) = existing_reference_v.as_ref() {
                            let mut updated = existing_reference_v.value.clone();
                            let memory_ref = source_summary
                                .memory_ref
                                .clone()
                                .ok_or_else(|| {
                                    ApplicationError::invalid_request(
                                        "trusted memory link requires memory ref",
                                    )
                                })?;
                            updated.update_reference_state(
                                MemoryReferenceState::linked(
                                    memory_ref,
                                    request.body.reason_ref.clone(),
                                    now,
                                ),
                                request.body.reason_ref.clone(),
                                context.actor_ref.clone(),
                                now,
                            )?;
                            updated
                        } else {
                            MemoryReference::link_for_member(
                                reference_ref.clone(),
                                request.body.member_ref.clone(),
                                source_summary.clone(),
                                request.body.reason_ref.clone(),
                                context.actor_ref.clone(),
                                now,
                            )?
                        }
                    }
                    identity_contracts::refs::MemoryReferenceChangeIntent::RefreshState => {
                        let mut updated = match existing_reference_v.as_ref() {
                            Some(value) => value.value.clone(),
                            None => {
                                let rejection = self.command_not_found_rejection(
                                    &command_name,
                                    "memory reference relation does not exist for refresh",
                                );
                                let outcome = self.save_replayable_rejected_outcome(
                                    &command_name,
                                    &request,
                                    &context,
                                    rejection,
                                    record,
                                    now,
                                    uow.as_ref(),
                                )?;
                                self.deps.unit_of_work_manager.commit(uow)?;
                                return Ok(outcome);
                            }
                        };
                        let next_state = match source_summary.source_state {
                            MemoryReferenceSourceState::Stale => {
                                let mut state = updated.reference_state.clone();
                                state.mark_stale(request.body.reason_ref.clone(), now)?;
                                state
                            }
                            MemoryReferenceSourceState::Unavailable => {
                                let mut state = updated.reference_state.clone();
                                state.mark_unavailable(request.body.reason_ref.clone(), now)?;
                                state
                            }
                            MemoryReferenceSourceState::PendingVerification
                            | MemoryReferenceSourceState::Untrusted => {
                                MemoryReferenceState::pending_verification(
                                    source_summary.memory_ref.clone(),
                                    source_summary.archive_ref.clone(),
                                    source_summary.archive_handoff_ref.clone(),
                                    request.body.reason_ref.clone(),
                                    now,
                                )
                            }
                            _ => {
                                return Err(ApplicationError::domain_rejected(
                                    "refresh state requires stale, unavailable, or verification source state",
                                ));
                            }
                        };
                        updated.update_reference_state(
                            next_state,
                            request.body.reason_ref.clone(),
                            context.actor_ref.clone(),
                            now,
                        )?;
                        updated
                    }
                    identity_contracts::refs::MemoryReferenceChangeIntent::AttachArchive
                    | identity_contracts::refs::MemoryReferenceChangeIntent::RecordArchiveHandoffResult => {
                        if let Some(existing_reference_v) = existing_reference_v.as_ref() {
                            let mut updated = existing_reference_v.value.clone();
                            let next_state = match source_summary.source_state {
                                MemoryReferenceSourceState::HandoffResultAccepted => {
                                    if let (Some(archive_ref), Some(handoff_ref)) = (
                                        source_summary.archive_ref.clone(),
                                        source_summary.archive_handoff_ref.clone(),
                                    ) {
                                        MemoryReferenceState::archived(
                                            archive_ref,
                                            handoff_ref,
                                            request.body.reason_ref.clone(),
                                            now,
                                        )
                                    } else {
                                        MemoryReferenceState::pending_verification(
                                            source_summary.memory_ref.clone(),
                                            source_summary.archive_ref.clone(),
                                            source_summary.archive_handoff_ref.clone(),
                                            request.body.reason_ref.clone(),
                                            now,
                                        )
                                    }
                                }
                                MemoryReferenceSourceState::HandoffResultFailed => {
                                    MemoryReferenceState::handoff_failed(
                                        source_summary
                                            .archive_handoff_ref
                                            .clone()
                                            .ok_or_else(|| {
                                                ApplicationError::invalid_request(
                                                    "handoff failed state requires handoff marker",
                                                )
                                            })?,
                                        request.body.reason_ref.clone(),
                                        now,
                                    )
                                }
                                _ => {
                                    return Err(ApplicationError::domain_rejected(
                                        "archive handoff update requires accepted or failed handoff source state",
                                    ));
                                }
                            };
                            updated.update_reference_state(
                                next_state,
                                request.body.reason_ref.clone(),
                                context.actor_ref.clone(),
                                now,
                            )?;
                            updated
                        } else {
                            MemoryReference::from_archive_handoff(
                                reference_ref.clone(),
                                request.body.member_ref.clone(),
                                source_summary.clone(),
                                request.body.reason_ref.clone(),
                                context.actor_ref.clone(),
                                now,
                            )?
                        }
                    }
                    identity_contracts::refs::MemoryReferenceChangeIntent::MarkPendingVerification => {
                        if let Some(existing_reference_v) = existing_reference_v.as_ref() {
                            let mut updated = existing_reference_v.value.clone();
                            updated.update_reference_state(
                                MemoryReferenceState::pending_verification(
                                    source_summary.memory_ref.clone(),
                                    source_summary.archive_ref.clone(),
                                    source_summary.archive_handoff_ref.clone(),
                                    request.body.reason_ref.clone(),
                                    now,
                                ),
                                request.body.reason_ref.clone(),
                                context.actor_ref.clone(),
                                now,
                            )?;
                            updated
                        } else {
                            MemoryReference::link_for_member(
                                reference_ref.clone(),
                                request.body.member_ref.clone(),
                                identity_contracts::refs::MemoryReferenceSourceSummary::from_resolver(
                                    source_summary.source_ref.clone(),
                                    source_summary.memory_ref.clone(),
                                    source_summary.archive_ref.clone(),
                                    source_summary.archive_handoff_ref.clone(),
                                    source_summary.safe_summary_ref.clone(),
                                    MemoryReferenceSourceState::PendingVerification,
                                ),
                                request.body.reason_ref.clone(),
                                context.actor_ref.clone(),
                                now,
                            )?
                        }
                    }
                    _ => {
                        let rejection = Self::protocol_rejection(
                            &command_name,
                            IdentityProtocolRejectionKind::PolicyDenied,
                            "memory relation must not write or delete external owner truth",
                        );
                        let outcome = self.save_replayable_rejected_outcome(
                            &command_name,
                            &request,
                            &context,
                            rejection,
                            record,
                            now,
                            uow.as_ref(),
                        )?;
                        self.deps.unit_of_work_manager.commit(uow)?;
                        return Ok(outcome);
                    }
                };

                self.deps
                    .memory_reference_repository
                    .save_memory_reference(
                        memory_reference.clone(),
                        existing_reference_v.as_ref().map(|value| value.version),
                        uow.as_ref(),
                    )?;
                let accepted_cursor_ref = self
                    .deps
                    .cursor_assigner
                    .assign_truth_change_cursor(uow.as_ref())?;
                let subjects = self
                    .deps
                    .truth_change_subject_mapper
                    .memory_reference_subjects(reference_ref.clone());
                let change_kind_ref = IdentityChangeKindRef::new(
                    IdentityChangeKind::MemoryReferenceChanged,
                    Some(request.body.source_ref.source_ref.clone()),
                );
                let trace = self.command_trace_record(
                    request.body.member_ref.clone(),
                    &subjects,
                    change_kind_ref.clone(),
                    accepted_cursor_ref.clone(),
                    Some(Self::accepted_change_reason(
                        &request.body.reason_ref.source_ref,
                    )),
                    Some(request.body.source_ref.source_ref.clone()),
                    None,
                    context.actor_ref.clone(),
                    now,
                )?;
                self.deps
                    .trace_record_repository
                    .append_trace_record(trace.clone(), uow.as_ref())?;
                let audit_trail_ref = self.append_accepted_audit(
                    &context,
                    Some(request.body.member_ref.clone()),
                    &subjects,
                    &change_kind_ref,
                    &accepted_cursor_ref,
                    &trace,
                    now,
                    uow.as_ref(),
                )?;
                let outbox = self.outbox_record(
                    request.body.member_ref.clone(),
                    subjects.outbox_subject_ref.clone(),
                    change_kind_ref.clone(),
                    AcceptedOutboundMaterialKind::MemoryReferenceChanged,
                    trace.trace_record_ref.clone(),
                    now,
                )?;
                let outbox_ref = outbox.outbox_record_ref.clone();
                if let Err(error) =
                    self.deps
                        .outbox_repository
                        .save_outbox_record(outbox, None, uow.as_ref())
                {
                    self.rollback_quietly(uow);
                    return Err(error);
                }
                let stale_projection_refs =
                    self.save_projection_stale_marks(&subjects, now, uow.as_ref())?;
                let stored_result_ref = self.deps.id_generator.new_identity_stored_result_ref()?;
                let surface_marker_ref = self
                    .deps
                    .id_generator
                    .new_identity_stored_surface_marker_ref()?;
                let effect_summary_ref = self
                    .deps
                    .id_generator
                    .new_identity_command_effect_summary_ref()?;
                let effect_summary = IdentityCommandEffectSummary::from_accepted_change(
                    effect_summary_ref,
                    context.context_ref.clone(),
                    IdentityAcceptedEffectKind::MemoryReferenceCommandResult,
                    IdentityTruthRef::MemoryReference(reference_ref.clone()),
                    accepted_cursor_ref,
                    vec![trace.trace_record_ref.clone()],
                    Some(audit_trail_ref),
                    vec![outbox_ref],
                    stale_projection_refs,
                    stored_result_ref.clone(),
                );
                let result = MemoryReferenceCommandResult {
                    member_ref: request.body.member_ref.clone(),
                    memory_reference_ref: reference_ref,
                    reference_state_kind: Self::map_public_memory_reference_state(
                        memory_reference.reference_state.state_kind,
                    ),
                    memory_ref: memory_reference.memory_ref.clone(),
                    archive_ref: memory_reference.archive_ref.clone(),
                    archive_handoff_ref: memory_reference.archive_handoff_ref.clone(),
                    source_ref: memory_reference.source_ref.clone(),
                    safe_summary_ref: memory_reference.safe_summary_ref.clone(),
                    reason_ref: memory_reference.change_reason_ref.clone(),
                };
                let effect = Self::public_effect_from_summary(
                    &effect_summary,
                    vec![subjects.audit_subject_ref.clone()],
                );
                let stored = StoredIdentityOperationResult::command_accepted(
                    stored_result_ref.clone(),
                    context.context_ref.clone(),
                    surface_marker_ref.clone(),
                    now,
                );
                self.deps
                    .stored_result_repository
                    .save_command_accepted_result(stored, uow.as_ref())?;
                self.deps
                    .effect_summary_repository
                    .save_effect_summary(effect_summary, uow.as_ref())?;
                self.deps
                    .stored_result_repository
                    .save_command_accepted_envelope(
                        IdentityCommandAcceptedResultEnvelope::new(
                            stored_result_ref.clone(),
                            context.context_ref.clone(),
                            command_name.clone(),
                            surface_marker_ref,
                            IdentityCommandTypedResult::MemoryReference(result.clone()),
                            effect.clone(),
                            now,
                        ),
                        uow.as_ref(),
                    )?;
                self.deps
                    .idempotency_repository
                    .complete_with_stored_result(
                        record.value,
                        stored_result_ref.clone(),
                        now,
                        record.version,
                        uow.as_ref(),
                    )?;
                self.deps.unit_of_work_manager.commit(uow)?;
                Ok(IdentityCommandOutcome::Accepted(Self::accepted_response(
                    command_name,
                    stored_result_ref,
                    result,
                    effect,
                )))
            }
        }
    }

    /// Shared skeleton for the trace handoff command vertical slice.
    pub fn prepare_trace_handoff(
        &self,
        request: IdentityCommandRequest<PrepareTraceHandoffRequest>,
        context: IdentityOperationContext,
    ) -> Result<IdentityCommandOutcome<TraceHandoffCommandResult>, ApplicationError> {
        Self::assert_command_context(&request, &context)?;
        let command_name = request.command_name.clone();
        let now = self.deps.clock.now()?;
        let uow = self.deps.unit_of_work_manager.begin()?;
        let reserve = match self.reserve_idempotency(&context, now, uow.as_ref()) {
            Ok(outcome) => outcome,
            Err(error) => {
                self.rollback_quietly(uow);
                return self
                    .map_runtime_unavailable(&command_name, error)
                    .map(IdentityCommandOutcome::Rejected);
            }
        };

        match reserve {
            IdempotencyReserveOutcome::ReplayAvailable {
                stored_result_ref, ..
            } => {
                self.rollback_quietly(uow);
                self.load_replayed_command_outcome(command_name, stored_result_ref, |typed| {
                    match typed {
                        IdentityCommandTypedResult::TraceHandoff(result) => Some(result),
                        _ => None,
                    }
                })
            }
            IdempotencyReserveOutcome::Conflict(record) => {
                let rejection = Self::protocol_rejection(
                    &command_name,
                    IdentityProtocolRejectionKind::DuplicateConflict,
                    "same idempotency key is already bound to different canonical material",
                );
                self.deps.idempotency_repository.mark_conflict(
                    record.value,
                    now,
                    record.version,
                    uow.as_ref(),
                )?;
                self.deps.unit_of_work_manager.commit(uow)?;
                Ok(IdentityCommandOutcome::Rejected(rejection))
            }
            IdempotencyReserveOutcome::InFlight(_) => {
                self.rollback_quietly(uow);
                Ok(IdentityCommandOutcome::Rejected(
                    Self::adapter_unavailable_rejection(
                        &command_name,
                        "same command idempotency key and digest is still in flight",
                    ),
                ))
            }
            IdempotencyReserveOutcome::Reserved(record) => {
                let member_exists = self
                    .deps
                    .member_repository
                    .get_member_with_version(request.body.member_ref.clone())?
                    .is_some();
                if !member_exists {
                    let rejection =
                        self.command_not_found_rejection(&command_name, "member does not exist");
                    let outcome = self.save_replayable_rejected_outcome(
                        &command_name,
                        &request,
                        &context,
                        rejection,
                        record,
                        now,
                        uow.as_ref(),
                    )?;
                    self.deps.unit_of_work_manager.commit(uow)?;
                    return Ok(outcome);
                }

                let policy = HandoffPolicy::for_handoff(HandoffPolicyArgs {
                    handoff_target_ref: request.body.handoff_target_ref.clone(),
                    handoff_scope_ref: request.body.handoff_scope_ref.clone(),
                    safe_material_ref: request.body.safe_material_ref.clone(),
                    trace_record_refs: request.body.trace_record_refs.clone(),
                    visibility_context_ref: request.body.visibility_context_ref.clone(),
                })
                .map_err(ApplicationError::from)?;
                if let Err(error) = policy
                    .assert_target_allowed()
                    .and_then(|_| policy.assert_trace_refs_present())
                    .and_then(|_| policy.assert_safe_material_body_free())
                    .and_then(|_| policy.assert_visible_for_handoff())
                {
                    let app_error = ApplicationError::from(error);
                    let rejection = Self::rejection_from_error(&command_name, &app_error)
                        .unwrap_or_else(|| {
                            Self::protocol_rejection(
                                &command_name,
                                IdentityProtocolRejectionKind::PolicyDenied,
                                app_error.message.clone(),
                            )
                        });
                    let outcome = self.save_replayable_rejected_outcome(
                        &command_name,
                        &request,
                        &context,
                        rejection,
                        record,
                        now,
                        uow.as_ref(),
                    )?;
                    self.deps.unit_of_work_manager.commit(uow)?;
                    return Ok(outcome);
                }

                for trace_ref in &request.body.trace_record_refs {
                    let Some(trace_v) = self
                        .deps
                        .trace_record_repository
                        .get_trace_record(trace_ref.clone())?
                    else {
                        let rejection = Self::protocol_rejection(
                            &command_name,
                            IdentityProtocolRejectionKind::NotFound,
                            format!("trace {} does not exist", trace_ref.as_str()),
                        );
                        let outcome = self.save_replayable_rejected_outcome(
                            &command_name,
                            &request,
                            &context,
                            rejection,
                            record,
                            now,
                            uow.as_ref(),
                        )?;
                        self.deps.unit_of_work_manager.commit(uow)?;
                        return Ok(outcome);
                    };
                    if !trace_v.value.belongs_to(&request.body.member_ref) {
                        let rejection = Self::protocol_rejection(
                            &command_name,
                            IdentityProtocolRejectionKind::PolicyDenied,
                            "trace does not belong to the requested member",
                        );
                        let outcome = self.save_replayable_rejected_outcome(
                            &command_name,
                            &request,
                            &context,
                            rejection,
                            record,
                            now,
                            uow.as_ref(),
                        )?;
                        self.deps.unit_of_work_manager.commit(uow)?;
                        return Ok(outcome);
                    }
                    if let Err(error) = trace_v.value.assert_body_free() {
                        let rejection = Self::protocol_rejection(
                            &command_name,
                            IdentityProtocolRejectionKind::ForbiddenBody,
                            ApplicationError::from(error).message,
                        );
                        let outcome = self.save_replayable_rejected_outcome(
                            &command_name,
                            &request,
                            &context,
                            rejection,
                            record,
                            now,
                            uow.as_ref(),
                        )?;
                        self.deps.unit_of_work_manager.commit(uow)?;
                        return Ok(outcome);
                    }
                }

                if let Some(audit_trail_ref) = request.body.audit_trail_ref.clone() {
                    let Some(audit_v) = self
                        .deps
                        .audit_trail_repository
                        .get_audit_trail_with_version(audit_trail_ref)?
                    else {
                        let rejection = Self::protocol_rejection(
                            &command_name,
                            IdentityProtocolRejectionKind::NotFound,
                            "audit trail does not exist",
                        );
                        let outcome = self.save_replayable_rejected_outcome(
                            &command_name,
                            &request,
                            &context,
                            rejection,
                            record,
                            now,
                            uow.as_ref(),
                        )?;
                        self.deps.unit_of_work_manager.commit(uow)?;
                        return Ok(outcome);
                    };
                    if let Some(member_ref) = audit_v.value.member_ref.as_ref() {
                        if member_ref != &request.body.member_ref {
                            let rejection = Self::protocol_rejection(
                                &command_name,
                                IdentityProtocolRejectionKind::PolicyDenied,
                                "audit trail does not belong to the requested member",
                            );
                            let outcome = self.save_replayable_rejected_outcome(
                                &command_name,
                                &request,
                                &context,
                                rejection,
                                record,
                                now,
                                uow.as_ref(),
                            )?;
                            self.deps.unit_of_work_manager.commit(uow)?;
                            return Ok(outcome);
                        }
                    }
                }

                let handoff_intent_ref = if let Some(requested_ref) =
                    request.body.requested_handoff_intent_ref.clone()
                {
                    if self
                        .deps
                        .handoff_intent_repository
                        .get_handoff_intent_with_version(requested_ref.clone())?
                        .is_some()
                    {
                        let rejection = Self::protocol_rejection(
                            &command_name,
                            IdentityProtocolRejectionKind::Conflict,
                            "requested handoff intent ref already exists",
                        );
                        let outcome = self.save_replayable_rejected_outcome(
                            &command_name,
                            &request,
                            &context,
                            rejection,
                            record,
                            now,
                            uow.as_ref(),
                        )?;
                        self.deps.unit_of_work_manager.commit(uow)?;
                        return Ok(outcome);
                    }
                    requested_ref
                } else {
                    self.deps.id_generator.new_trace_handoff_intent_ref()?
                };

                let target_resolution = match self.deps.handoff_target_port.resolve_handoff_target(
                    request.body.handoff_target_ref.clone(),
                    request.body.handoff_scope_ref.clone(),
                    request.body.safe_material_ref.clone(),
                ) {
                    Ok(resolution) => resolution,
                    Err(error) => {
                        let rejection = self.map_runtime_unavailable(&command_name, error)?;
                        let outcome = self.save_replayable_rejected_outcome(
                            &command_name,
                            &request,
                            &context,
                            rejection,
                            record,
                            now,
                            uow.as_ref(),
                        )?;
                        self.deps.unit_of_work_manager.commit(uow)?;
                        return Ok(outcome);
                    }
                };
                if target_resolution.target_ref != request.body.handoff_target_ref
                    || target_resolution.scope_ref != request.body.handoff_scope_ref
                {
                    let rejection = Self::protocol_rejection(
                        &command_name,
                        IdentityProtocolRejectionKind::InvalidRequest,
                        "resolved handoff target does not match request target or scope",
                    );
                    let outcome = self.save_replayable_rejected_outcome(
                        &command_name,
                        &request,
                        &context,
                        rejection,
                        record,
                        now,
                        uow.as_ref(),
                    )?;
                    self.deps.unit_of_work_manager.commit(uow)?;
                    return Ok(outcome);
                }

                let intent = TraceHandoffIntent::prepare(TraceHandoffIntentPrepareArgs {
                    handoff_intent_ref: handoff_intent_ref.clone(),
                    member_ref: request.body.member_ref.clone(),
                    trace_record_refs: request.body.trace_record_refs.clone(),
                    audit_trail_ref: request.body.audit_trail_ref.clone(),
                    handoff_target_ref: request.body.handoff_target_ref.clone(),
                    handoff_scope_ref: request.body.handoff_scope_ref.clone(),
                    safe_material_ref: request.body.safe_material_ref.clone(),
                    handoff_state: HandoffState::pending(now),
                    created_at: now,
                })
                .map_err(ApplicationError::from)?;
                self.deps.handoff_intent_repository.save_handoff_intent(
                    intent.clone(),
                    None,
                    uow.as_ref(),
                )?;

                let accepted_cursor_ref = self
                    .deps
                    .cursor_assigner
                    .assign_truth_change_cursor(uow.as_ref())?;
                let subjects = self
                    .deps
                    .truth_change_subject_mapper
                    .handoff_intent_subjects(handoff_intent_ref.clone());
                let change_kind_ref = IdentityChangeKindRef::new(
                    IdentityChangeKind::DerivedMarkerChanged,
                    Some(request.body.handoff_reason_ref.source_ref.clone()),
                );
                let trace = self.command_trace_record(
                    request.body.member_ref.clone(),
                    &subjects,
                    change_kind_ref.clone(),
                    accepted_cursor_ref.clone(),
                    Some(IdentityChangeReasonRef::new(
                        request.body.handoff_reason_ref.source_ref.clone(),
                    )),
                    Some(request.body.handoff_reason_ref.source_ref.clone()),
                    None,
                    context.actor_ref.clone(),
                    now,
                )?;
                self.deps
                    .trace_record_repository
                    .append_trace_record(trace.clone(), uow.as_ref())?;
                let audit_trail_ref = self.append_accepted_audit(
                    &context,
                    Some(request.body.member_ref.clone()),
                    &subjects,
                    &change_kind_ref,
                    &accepted_cursor_ref,
                    &trace,
                    now,
                    uow.as_ref(),
                )?;
                let stale_projection_refs =
                    self.save_projection_stale_marks(&subjects, now, uow.as_ref())?;
                let stored_result_ref = self.deps.id_generator.new_identity_stored_result_ref()?;
                let surface_marker_ref = self
                    .deps
                    .id_generator
                    .new_identity_stored_surface_marker_ref()?;
                let effect_summary_ref = self
                    .deps
                    .id_generator
                    .new_identity_command_effect_summary_ref()?;
                let effect_summary = IdentityCommandEffectSummary::from_accepted_change(
                    effect_summary_ref,
                    context.context_ref.clone(),
                    IdentityAcceptedEffectKind::TraceHandoffCommandResult,
                    IdentityTruthRef::TraceHandoffIntent(handoff_intent_ref.clone()),
                    accepted_cursor_ref,
                    vec![trace.trace_record_ref.clone()],
                    Some(audit_trail_ref),
                    Vec::new(),
                    stale_projection_refs,
                    stored_result_ref.clone(),
                );
                let result = TraceHandoffCommandResult {
                    member_ref: request.body.member_ref.clone(),
                    handoff_intent_ref: handoff_intent_ref.clone(),
                    handoff_state_kind: Self::map_public_handoff_state(
                        intent.handoff_state.state_kind,
                    ),
                    handoff_target_ref: intent.handoff_target_ref.clone(),
                    handoff_scope_ref: intent.handoff_scope_ref.clone(),
                    trace_record_refs: intent.trace_record_refs.clone(),
                    audit_trail_ref: intent.audit_trail_ref.clone(),
                    safe_material_ref: intent.safe_material_ref.clone(),
                };
                let effect = Self::public_effect_from_summary(
                    &effect_summary,
                    vec![subjects.audit_subject_ref.clone()],
                );
                let stored = StoredIdentityOperationResult::command_accepted(
                    stored_result_ref.clone(),
                    context.context_ref.clone(),
                    surface_marker_ref.clone(),
                    now,
                );
                self.deps
                    .stored_result_repository
                    .save_command_accepted_result(stored, uow.as_ref())?;
                self.deps
                    .effect_summary_repository
                    .save_effect_summary(effect_summary, uow.as_ref())?;
                self.deps
                    .stored_result_repository
                    .save_command_accepted_envelope(
                        IdentityCommandAcceptedResultEnvelope::new(
                            stored_result_ref.clone(),
                            context.context_ref.clone(),
                            command_name.clone(),
                            surface_marker_ref,
                            IdentityCommandTypedResult::TraceHandoff(result.clone()),
                            effect.clone(),
                            now,
                        ),
                        uow.as_ref(),
                    )?;
                self.deps
                    .idempotency_repository
                    .complete_with_stored_result(
                        record.value,
                        stored_result_ref.clone(),
                        now,
                        record.version,
                        uow.as_ref(),
                    )?;
                self.deps.unit_of_work_manager.commit(uow)?;
                Ok(IdentityCommandOutcome::Accepted(Self::accepted_response(
                    command_name,
                    stored_result_ref,
                    result,
                    effect,
                )))
            }
        }
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
        IdentityProjectionKind, IdentityRequestDigestValue, IdentitySourceOwner, IdentitySourceRef,
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

    fn projection_ref() -> identity_contracts::refs::IdentityProjectionRef {
        identity_contracts::refs::IdentityProjectionRef::new(
            IdentityProjectionKind::MemberSummary,
            source_ref(),
        )
        .expect("valid projection ref")
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
            vec![projection_ref()],
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
