//! Shared command-service skeleton and accepted-response assembly helpers.

use core_contracts::actor::ActorRef;
use identity_contracts::commands::{
    EstablishGlobalMemberRequest, GlobalLifecycleCommandResult, GlobalMemberCommandResult,
    IdentityCommandEffectPublicSummary, IdentityCommandOutcome, IdentityCommandRequest,
    IdentityCommandResponse, UpdateGlobalLifecycleStateRequest,
};
use identity_contracts::metadata::{
    IdentityDegradedKind, IdentityDegradedMarker, IdentityProtocolRejection,
    IdentityProtocolRejectionKind, IdentityProtocolValidationIssueRef,
    IdentityProtocolValidationIssueRefSet, IdentityRequestDigestMarker,
};
use identity_contracts::protocol::{IdentityCommandName, IdentityProtocolSurfaceRef};
use identity_contracts::refs::{
    AuditTrailRef, ExternalSourceRef, GlobalLifecycleStateKind as PublicLifecycleStateKind,
    GlobalMemberRef, GovernanceBasisRef, IdentityAnchorReasonKind, IdentityAnchorReasonRef,
    IdentityAnchorStateKind, IdentityAuditSubjectRef, IdentityChangeKind, IdentityChangeKindRef,
    IdentityChangeReasonRef, IdentityDegradedMarkerRef, IdentityOperationChannel,
    IdentityOutboxPayloadMarkerRef, IdentityProjectionRef, IdentitySourceOwner, IdentitySourceRef,
    IdentityStoredResultRef, IdentityTimestamp, IdentityTraceRecordRef, IdentityTruthCursor,
    MaintenanceScopeRef, TopicKeyRef,
};
use identity_contracts::views::{IdentityReadMaterialKind, IdentityReadMaterialMarker};
use identity_domain::audit::{AuditTrail, AuditTrailEntry};
use identity_domain::lifecycle::{
    GlobalLifecycleState, GlobalLifecycleStateKind, HighRiskLifecycleGuard,
    LifecycleTransitionPolicy,
};
use identity_domain::member_identity::{GlobalMember, IdentityAnchorPolicy, IdentityAnchorState};
use identity_domain::outbox::{IdentityOutboxRecord, OutboxState};
use identity_domain::trace::IdentityTraceRecord;

use crate::errors::{ApplicationError, ApplicationErrorKind};
use crate::ports::{
    GlobalLifecycleRepository, GlobalMemberRepository, IdentityAcceptedAuditTrailMarkerMapper,
    IdentityAuditTrailRepository, IdentityClockPort, IdentityCommandEffectSummaryRepository,
    IdentityCursorAssignerPort, IdentityExternalSourceResolverPort, IdentityIdGeneratorPort,
    IdentityIdempotencyRepository, IdentityOperationContextFactoryPort, IdentityOutboxRepository,
    IdentityProjectionRepository, IdentityStoredResultRepository, IdentityTraceRecordRepository,
    IdentityTruthChangeSubjectMapper, IdentityUnitOfWork, IdentityUnitOfWorkManagerPort,
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
        payload_marker: &str,
        topic: &str,
        trace_record_ref: IdentityTraceRecordRef,
        now: IdentityTimestamp,
    ) -> Result<IdentityOutboxRecord, ApplicationError> {
        let outbox_record_ref = self.deps.id_generator.new_identity_outbox_record_ref()?;
        Ok(IdentityOutboxRecord {
            outbox_record_ref,
            member_ref,
            subject_ref,
            change_kind_ref,
            payload_marker_ref: IdentityOutboxPayloadMarkerRef::new(payload_marker),
            topic_key_ref: TopicKeyRef::new(topic),
            trace_record_ref,
            outbox_state: OutboxState::pending(now),
            created_at: now,
            updated_at: now,
        })
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
                    "identity.global-member.established.v1",
                    "identity.global-member.established.v1",
                    trace.trace_record_ref.clone(),
                    now,
                )?;
                let anchor_outbox = self.outbox_record(
                    member_ref.clone(),
                    subjects.outbox_subject_ref.clone(),
                    change_kind_ref.clone(),
                    "identity.anchor.changed.v1",
                    "identity.anchor.changed.v1",
                    trace.trace_record_ref.clone(),
                    now,
                )?;
                let established_outbox_ref = established_outbox.outbox_record_ref.clone();
                let anchor_outbox_ref = anchor_outbox.outbox_record_ref.clone();
                self.deps.outbox_repository.save_outbox_record(
                    established_outbox,
                    None,
                    uow.as_ref(),
                )?;
                self.deps.outbox_repository.save_outbox_record(
                    anchor_outbox,
                    None,
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
                    "identity.lifecycle.changed.v1",
                    "identity.lifecycle.changed.v1",
                    trace.trace_record_ref.clone(),
                    now,
                )?;
                let lifecycle_outbox_ref = lifecycle_outbox.outbox_record_ref.clone();
                self.deps.outbox_repository.save_outbox_record(
                    lifecycle_outbox,
                    None,
                    uow.as_ref(),
                )?;

                let mut outbox_refs = vec![lifecycle_outbox_ref];
                if anchor_state_kind.is_some() {
                    let anchor_outbox = self.outbox_record(
                        request.body.member_ref.clone(),
                        subjects.outbox_subject_ref.clone(),
                        IdentityChangeKindRef::new(
                            IdentityChangeKind::MemberAnchorChanged,
                            Some(request.body.reason_ref.source_ref.clone()),
                        ),
                        "identity.anchor.changed.v1",
                        "identity.anchor.changed.v1",
                        trace.trace_record_ref.clone(),
                        now,
                    )?;
                    let anchor_ref = anchor_outbox.outbox_record_ref.clone();
                    self.deps.outbox_repository.save_outbox_record(
                        anchor_outbox,
                        None,
                        uow.as_ref(),
                    )?;
                    outbox_refs.push(anchor_ref);
                }
                if lifecycle_v.value.is_available() != new_lifecycle.is_available() {
                    let availability_outbox = self.outbox_record(
                        request.body.member_ref.clone(),
                        subjects.outbox_subject_ref.clone(),
                        change_kind_ref.clone(),
                        "identity.global-member.availability.changed.v1",
                        "identity.global-member.availability.changed.v1",
                        trace.trace_record_ref.clone(),
                        now,
                    )?;
                    let availability_ref = availability_outbox.outbox_record_ref.clone();
                    self.deps.outbox_repository.save_outbox_record(
                        availability_outbox,
                        None,
                        uow.as_ref(),
                    )?;
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
