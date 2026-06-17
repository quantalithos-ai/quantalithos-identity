//! Shared consumer/callback scaffold and typed receipt replay helpers.

use core_contracts::actor::ActorRef;
use identity_contracts::events::{
    ArchiveHandoffResultPayload, IdentityConsumerOutcome, IdentityConsumerReceipt,
    IdentityInboundEventEnvelope, MemoryReferenceSourceStateChangedPayload,
    RoleCapabilitySourceChangedPayload, TraceHandoffResultKind, TraceHandoffResultPayload,
    WorkParticipationAcceptedPayload,
};
use identity_contracts::metadata::IdentityProtocolValidationIssueRef;
use identity_contracts::protocol::{IdentityInboundConsumerName, IdentityProtocolSchemaVersionRef};
use identity_contracts::refs::{
    AuditTrailRef, CareerAppendReasonKind, CareerAppendReasonRef, CareerRecordChangeIntent,
    CareerRecordRef, ExternalReferenceRef, GlobalMemberRef, IdentityChangeKind,
    IdentityChangeKindRef, IdentityChangeReasonRef, IdentityOperationChannel,
    IdentityOutboxRecordRef, IdentityProjectionRef, IdentityReferenceOwnerRef, IdentitySourceRef,
    IdentityStoredResultRef, IdentityTimestamp, IdentityTraceRecordRef, IdentityTruthCursor,
    MemoryReferenceReasonKind, MemoryReferenceReasonRef, MemoryReferenceSourceKind,
    MemoryReferenceSourceState, MemoryReferenceStateKind as PublicMemoryReferenceStateKind,
};
use identity_contracts::views::{IdentityReadMaterialKind, IdentityReadMaterialMarker};
use identity_domain::audit::{AuditTrail, AuditTrailEntry};
use identity_domain::career::{CareerAppendPolicy, CareerRecord};
use identity_domain::handoff::{HandoffPolicy, HandoffState};
use identity_domain::memory_reference::{
    MemoryReference, MemoryReferencePolicy, MemoryReferenceState,
};
use identity_domain::outbox::IdentityOutboxRecord;
use identity_domain::projection_state::ProjectionState;
use identity_domain::role_capability::RoleCapabilitySourceSnapshot;
use identity_domain::trace::IdentityTraceRecord;

use crate::errors::{ApplicationError, ApplicationErrorKind};
use crate::outbound_material::AcceptedOutboundMaterialKind;
use crate::ports::{
    CareerRecordRepository, GlobalMemberRepository, IdentityAcceptedAuditTrailMarkerMapper,
    IdentityAuditTrailRepository, IdentityClockPort, IdentityCursorAssignerPort,
    IdentityExternalReferenceResolverPort, IdentityIdGeneratorPort, IdentityIdempotencyRepository,
    IdentityMarkerSubjectMapper, IdentityOperationContextFactoryPort, IdentityOutboxRepository,
    IdentityProjectionRepository, IdentityReferenceStateRepository, IdentityStoredResultRepository,
    IdentityTraceRecordRepository, IdentityTruthChangeSubjectMapper, IdentityUnitOfWork,
    IdentityUnitOfWorkManagerPort, MemoryReferenceRepository, RoleCapabilityRepository,
    TraceHandoffIntentRepository,
};
use crate::support::{
    IdempotencyReserveOutcome, IdentityAcceptedSubjectRefs, IdentityConsumerReceiptEnvelope,
    IdentityIdempotencyRecord, IdentityOperationContext, IdentityStoredResultKind,
    StoredIdentityOperationResult, Versioned,
};

/// Shared dependencies for consumer/callback scaffold flows.
pub struct IdentityConsumerServiceDeps<'a> {
    /// Consumer/callback write transaction manager.
    pub unit_of_work_manager: &'a dyn IdentityUnitOfWorkManagerPort,
    /// Trusted clock used by receipt persistence and replay decisions.
    pub clock: &'a dyn IdentityClockPort,
    /// Stable id and marker generator.
    pub id_generator: &'a dyn IdentityIdGeneratorPort,
    /// Explicit truth or marker cursor assigner.
    pub cursor_assigner: &'a dyn IdentityCursorAssignerPort,
    /// Entry metadata to operation-context builder.
    pub operation_context_factory: &'a dyn IdentityOperationContextFactoryPort,
    /// Duplicate replay reserve and completion repository.
    pub idempotency_repository: &'a dyn IdentityIdempotencyRepository,
    /// Stored replay shell and typed receipt envelope repository.
    pub stored_result_repository: &'a dyn IdentityStoredResultRepository,
    /// Canonical accepted truth subject mapper.
    pub truth_change_subject_mapper: &'a dyn IdentityTruthChangeSubjectMapper,
    /// Canonical marker subject mapper.
    pub marker_subject_mapper: &'a dyn IdentityMarkerSubjectMapper,
    /// Canonical accepted audit marker mapper.
    pub accepted_audit_trail_marker_mapper: &'a dyn IdentityAcceptedAuditTrailMarkerMapper,
    /// Member repository for consumer/callback target checks.
    pub member_repository: &'a dyn GlobalMemberRepository,
    /// Role capability summary/source snapshot repository.
    pub role_capability_repository: &'a dyn RoleCapabilityRepository,
    /// Career record repository.
    pub career_record_repository: &'a dyn CareerRecordRepository,
    /// Memory reference repository.
    pub memory_reference_repository: &'a dyn MemoryReferenceRepository,
    /// External reference bundle repository.
    pub reference_state_repository: &'a dyn IdentityReferenceStateRepository,
    /// External reference resolver for formal bundle state.
    pub external_reference_resolver: &'a dyn IdentityExternalReferenceResolverPort,
    /// Trace append repository.
    pub trace_record_repository: &'a dyn IdentityTraceRecordRepository,
    /// Audit trail repository.
    pub audit_trail_repository: &'a dyn IdentityAuditTrailRepository,
    /// Outbox repository.
    pub outbox_repository: &'a dyn IdentityOutboxRepository,
    /// Projection stale marker repository.
    pub projection_repository: &'a dyn IdentityProjectionRepository,
    /// Trace handoff intent repository.
    pub handoff_intent_repository: &'a dyn TraceHandoffIntentRepository,
}

/// Shared consumer/callback scaffold for receipt replay and body-free unsupported outcomes.
pub struct IdentityConsumerService<'a> {
    deps: IdentityConsumerServiceDeps<'a>,
}

impl<'a> IdentityConsumerService<'a> {
    /// Creates a consumer service from formal shared consumer dependencies.
    pub fn new(deps: IdentityConsumerServiceDeps<'a>) -> Self {
        Self { deps }
    }

    /// Returns the shared consumer dependencies for vertical-slice implementations.
    pub fn deps(&self) -> &IdentityConsumerServiceDeps<'a> {
        &self.deps
    }

    /// Shared precheck that keeps the inbound public envelope aligned with the context.
    pub fn assert_inbound_context<T>(
        envelope: &IdentityInboundEventEnvelope<T>,
        context: &IdentityOperationContext,
    ) -> Result<(), ApplicationError> {
        Self::assert_context_channel(context, IdentityOperationChannel::Consumer)?;
        Self::assert_context_operation_name(envelope.consumer_name.as_str(), context)?;
        Self::assert_context_idempotency(envelope, context)?;
        Self::assert_context_source_event(envelope, context)?;
        Ok(())
    }

    /// Shared precheck that keeps the callback public envelope aligned with the context.
    pub fn assert_callback_context<T>(
        envelope: &IdentityInboundEventEnvelope<T>,
        context: &IdentityOperationContext,
    ) -> Result<(), ApplicationError> {
        Self::assert_context_channel(context, IdentityOperationChannel::HandoffCallback)?;
        Self::assert_context_operation_name(envelope.consumer_name.as_str(), context)?;
        Self::assert_context_idempotency(envelope, context)?;
        Self::assert_context_source_event(envelope, context)?;
        Ok(())
    }

    /// Clones one replayable receipt and marks it as a duplicate replay surface.
    pub fn duplicate_replay_receipt(receipt: &IdentityConsumerReceipt) -> IdentityConsumerReceipt {
        let mut replay = receipt.clone();
        replay.outcome = IdentityConsumerOutcome::DuplicateReplayed;
        replay
    }

    /// Builds one fresh public consumer receipt from body-free refs and issue markers.
    pub fn build_receipt(
        &self,
        consumer_name: identity_contracts::protocol::IdentityInboundConsumerName,
        outcome: IdentityConsumerOutcome,
        stored_result_ref: IdentityStoredResultRef,
        trace_refs: Vec<IdentityTraceRecordRef>,
        outbox_refs: Vec<IdentityOutboxRecordRef>,
        issue_refs: Vec<IdentityProtocolValidationIssueRef>,
    ) -> Result<IdentityConsumerReceipt, ApplicationError> {
        Ok(IdentityConsumerReceipt {
            receipt_ref: self.deps.id_generator.new_identity_consumer_receipt_ref()?,
            consumer_name,
            outcome,
            stored_result_ref,
            trace_refs,
            outbox_refs,
            issue_refs,
        })
    }

    /// Shared helper that reserves consumer/callback idempotency inside the active write transaction.
    pub fn reserve_idempotency(
        &self,
        context: &IdentityOperationContext,
        reserved_at: identity_contracts::refs::IdentityTimestamp,
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

    /// Shared scaffold for inbound consumer flows before specific payload mutation exists.
    pub fn dispatch_inbound_event_scaffold<T, F>(
        &self,
        context: IdentityOperationContext,
        envelope: &IdentityInboundEventEnvelope<T>,
        expected_schema_version_ref: &IdentityProtocolSchemaVersionRef,
        handler: F,
    ) -> Result<IdentityConsumerReceipt, ApplicationError>
    where
        F: FnOnce(
            Versioned<IdentityIdempotencyRecord>,
            identity_contracts::refs::IdentityTimestamp,
            &dyn IdentityUnitOfWork,
        ) -> Result<IdentityConsumerReceipt, ApplicationError>,
    {
        Self::assert_inbound_context(envelope, &context)?;
        self.dispatch_scaffold(
            &context,
            envelope,
            expected_schema_version_ref,
            IdentityStoredResultKind::ConsumerReceipt,
            handler,
        )
    }

    /// Shared scaffold for handoff callback flows before specific payload mutation exists.
    pub fn dispatch_callback_scaffold<T, F>(
        &self,
        context: IdentityOperationContext,
        envelope: &IdentityInboundEventEnvelope<T>,
        expected_schema_version_ref: &IdentityProtocolSchemaVersionRef,
        handler: F,
    ) -> Result<IdentityConsumerReceipt, ApplicationError>
    where
        F: FnOnce(
            Versioned<IdentityIdempotencyRecord>,
            identity_contracts::refs::IdentityTimestamp,
            &dyn IdentityUnitOfWork,
        ) -> Result<IdentityConsumerReceipt, ApplicationError>,
    {
        Self::assert_callback_context(envelope, &context)?;
        self.dispatch_scaffold(
            &context,
            envelope,
            expected_schema_version_ref,
            IdentityStoredResultKind::HandoffCallbackReceipt,
            handler,
        )
    }

    /// Handles one role-capability source change event.
    pub fn handle_role_capability_source_changed(
        &self,
        envelope: IdentityInboundEventEnvelope<RoleCapabilitySourceChangedPayload>,
        context: IdentityOperationContext,
    ) -> Result<IdentityConsumerReceipt, ApplicationError> {
        self.dispatch_inbound_event_scaffold(
            context.clone(),
            &envelope,
            &Self::consumer_schema_version(),
            |reserved, now, uow| {
                let payload = &envelope.payload;
                if !payload.source_version_ref.belongs_to(&payload.source_ref) {
                    return self.persist_consumer_outcome(
                        &context,
                        reserved,
                        envelope.consumer_name.clone(),
                        IdentityConsumerOutcome::Rejected,
                        Vec::new(),
                        Vec::new(),
                        vec![Self::issue("role-source-version-mismatch")],
                        now,
                        uow,
                    );
                }
                if let Some(safe_summary_ref) = payload.safe_summary_ref.as_ref() {
                    if !safe_summary_ref.belongs_to_source(&payload.source_ref) {
                        return self.persist_consumer_outcome(
                            &context,
                            reserved,
                            envelope.consumer_name.clone(),
                            IdentityConsumerOutcome::Rejected,
                            Vec::new(),
                            Vec::new(),
                            vec![Self::issue("role-safe-summary-source-mismatch")],
                            now,
                            uow,
                        );
                    }
                }
                if payload.material_marker.is_forbidden() {
                    return self.persist_consumer_outcome(
                        &context,
                        reserved,
                        envelope.consumer_name.clone(),
                        IdentityConsumerOutcome::Rejected,
                        Vec::new(),
                        Vec::new(),
                        vec![Self::issue("role-source-forbidden-body")],
                        now,
                        uow,
                    );
                }
                if payload.material_marker.material_kind
                    == identity_contracts::refs::RoleCapabilityChangeMaterialKind::ForbiddenAutomaticScoring
                {
                    return self.persist_consumer_outcome(
                        &context,
                        reserved,
                        envelope.consumer_name.clone(),
                        IdentityConsumerOutcome::Rejected,
                        Vec::new(),
                        Vec::new(),
                        vec![Self::issue("role-source-automatic-scoring-forbidden")],
                        now,
                        uow,
                    );
                }
                if payload.external_reference_ref.is_some()
                    != payload.reference_owner_ref.is_some()
                {
                    return self.persist_consumer_outcome(
                        &context,
                        reserved,
                        envelope.consumer_name.clone(),
                        IdentityConsumerOutcome::Rejected,
                        Vec::new(),
                        Vec::new(),
                        vec![Self::issue("role-reference-sidecar-partial-fields")],
                        now,
                        uow,
                    );
                }

                let current_snapshot_v = self
                    .deps
                    .role_capability_repository
                    .find_source_snapshot_by_source(payload.source_ref.clone())?;
                let current_summary_v = self
                    .deps
                    .role_capability_repository
                    .find_current_summary_by_member(payload.member_ref.clone())?;

                let snapshot_ref = current_snapshot_v
                    .as_ref()
                    .map(|versioned| versioned.value.snapshot_ref.clone())
                    .unwrap_or_else(|| {
                        identity_contracts::refs::RoleCapabilitySourceSnapshotRef::from_id(
                            self.deps
                                .id_generator
                                .new_role_capability_source_snapshot_id()
                                .expect("role snapshot id"),
                        )
                    });
                let snapshot = match payload.source_state_kind {
                    identity_contracts::refs::RoleCapabilitySourceStateKind::SourceResolved => {
                        let Some(safe_summary_ref) = payload.safe_summary_ref.clone() else {
                            return self.persist_consumer_outcome(
                                &context,
                                reserved,
                                envelope.consumer_name.clone(),
                                IdentityConsumerOutcome::Rejected,
                                Vec::new(),
                                Vec::new(),
                                vec![Self::issue("role-source-safe-summary-missing")],
                                now,
                                uow,
                            );
                        };
                        RoleCapabilitySourceSnapshot::from_resolved_source(
                            snapshot_ref,
                            payload.source_ref.clone(),
                            payload.source_version_ref.clone(),
                            safe_summary_ref,
                            payload.evidence_refs.clone(),
                            now,
                        )?
                    }
                    identity_contracts::refs::RoleCapabilitySourceStateKind::SourceUnavailable => {
                        if let Some(current) = current_snapshot_v.as_ref() {
                            let mut snapshot = current.value.clone();
                            snapshot.mark_unavailable(now)?;
                            snapshot
                        } else {
                            RoleCapabilitySourceSnapshot::unavailable(
                                snapshot_ref,
                                payload.source_ref.clone(),
                                payload.source_version_ref.clone(),
                                now,
                            )
                        }
                    }
                    identity_contracts::refs::RoleCapabilitySourceStateKind::SourceUnrecognized => {
                        RoleCapabilitySourceSnapshot::unrecognized(
                            snapshot_ref,
                            payload.source_ref.clone(),
                            payload.source_version_ref.clone(),
                            now,
                        )
                    }
                    identity_contracts::refs::RoleCapabilitySourceStateKind::SourceStale => {
                        let Some(current) = current_snapshot_v.as_ref() else {
                            return self.persist_consumer_outcome(
                                &context,
                                reserved,
                                envelope.consumer_name.clone(),
                                IdentityConsumerOutcome::Quarantined,
                                Vec::new(),
                                Vec::new(),
                                vec![Self::issue("role-source-stale-without-snapshot")],
                                now,
                                uow,
                            );
                        };
                        let mut snapshot = current.value.clone();
                        snapshot.mark_stale(payload.source_version_ref.clone(), now)?;
                        snapshot
                    }
                    identity_contracts::refs::RoleCapabilitySourceStateKind::SourceSuperseded => {
                        let Some(current) = current_snapshot_v.as_ref() else {
                            return self.persist_consumer_outcome(
                                &context,
                                reserved,
                                envelope.consumer_name.clone(),
                                IdentityConsumerOutcome::Quarantined,
                                Vec::new(),
                                Vec::new(),
                                vec![Self::issue("role-source-superseded-without-snapshot")],
                                now,
                                uow,
                            );
                        };
                        let mut snapshot = current.value.clone();
                        snapshot.mark_superseded(payload.source_version_ref.clone(), now)?;
                        snapshot
                    }
                };
                self.deps.role_capability_repository.save_source_snapshot(
                    snapshot.clone(),
                    current_snapshot_v.as_ref().map(|value| value.version),
                    uow,
                )?;

                let change_kind_ref = IdentityChangeKindRef::new(
                    IdentityChangeKind::RoleCapabilitySummaryChanged,
                    Some(payload.source_ref.source_ref.clone()),
                );
                let source_subjects = self
                    .deps
                    .truth_change_subject_mapper
                    .role_capability_source_snapshot_subjects(snapshot.snapshot_ref.clone());
                let accepted_cursor_ref =
                    self.deps.cursor_assigner.assign_truth_change_cursor(uow)?;
                let source_reason = payload
                    .change_reason_ref
                    .as_ref()
                    .map(|reason_ref| Self::accepted_change_reason(&reason_ref.source_ref));
                let source_trace = self.trace_record(
                    payload.member_ref.clone(),
                    &source_subjects,
                    change_kind_ref.clone(),
                    accepted_cursor_ref.clone(),
                    source_reason.clone(),
                    Some(payload.source_ref.source_ref.clone()),
                    context.actor_ref.clone(),
                    now,
                )?;
                self.deps
                    .trace_record_repository
                    .append_trace_record(source_trace.clone(), uow)?;
                self.append_accepted_audit(
                    &context,
                    Some(payload.member_ref.clone()),
                    &source_subjects,
                    &change_kind_ref,
                    &accepted_cursor_ref,
                    &source_trace,
                    now,
                    uow,
                )?;

                let source_outbox = self.outbox_record(
                    payload.member_ref.clone(),
                    source_subjects.outbox_subject_ref.clone(),
                    change_kind_ref.clone(),
                    AcceptedOutboundMaterialKind::RoleCapabilitySourceStateChanged,
                    source_trace.trace_record_ref.clone(),
                    now,
                )?;
                let mut trace_refs = vec![source_trace.trace_record_ref.clone()];
                let mut outbox_refs = vec![source_outbox.outbox_record_ref.clone()];
                self.deps
                    .outbox_repository
                    .save_outbox_record(source_outbox, None, uow)?;

                let mut stale_projection_refs =
                    self.save_projection_stale_marks(&source_subjects, now, uow)?;

                if let Some(mut summary_v) = current_summary_v.clone() {
                    let summary_changed = match snapshot.source_state {
                        identity_domain::role_capability::RoleCapabilitySourceStateKind::SourceStale
                        | identity_domain::role_capability::RoleCapabilitySourceStateKind::SourceSuperseded => {
                            summary_v.value.mark_stale(&snapshot, now)?;
                            true
                        }
                        identity_domain::role_capability::RoleCapabilitySourceStateKind::SourceUnavailable
                        | identity_domain::role_capability::RoleCapabilitySourceStateKind::SourceUnrecognized => {
                            summary_v
                                .value
                                .mark_unavailable(payload.source_ref.clone(), now)?;
                            true
                        }
                        identity_domain::role_capability::RoleCapabilitySourceStateKind::SourceResolved => {
                            false
                        }
                    };
                    if summary_changed {
                        self.deps.role_capability_repository.save_summary(
                            summary_v.value.clone(),
                            Some(summary_v.version),
                            uow,
                        )?;
                        let summary_subjects = self
                            .deps
                            .truth_change_subject_mapper
                            .role_capability_subjects(summary_v.value.summary_ref.clone());
                        let summary_trace = self.trace_record(
                            payload.member_ref.clone(),
                            &summary_subjects,
                            change_kind_ref.clone(),
                            accepted_cursor_ref.clone(),
                            source_reason,
                            Some(payload.source_ref.source_ref.clone()),
                            context.actor_ref.clone(),
                            now,
                        )?;
                        self.deps
                            .trace_record_repository
                            .append_trace_record(summary_trace.clone(), uow)?;
                        self.append_accepted_audit(
                            &context,
                            Some(payload.member_ref.clone()),
                            &summary_subjects,
                            &change_kind_ref,
                            &accepted_cursor_ref,
                            &summary_trace,
                            now,
                            uow,
                        )?;
                        let summary_outbox = self.outbox_record(
                            payload.member_ref.clone(),
                            summary_subjects.outbox_subject_ref.clone(),
                            change_kind_ref.clone(),
                            AcceptedOutboundMaterialKind::RoleCapabilitySummaryChanged,
                            summary_trace.trace_record_ref.clone(),
                            now,
                        )?;
                        trace_refs.push(summary_trace.trace_record_ref.clone());
                        outbox_refs.push(summary_outbox.outbox_record_ref.clone());
                        self.deps
                            .outbox_repository
                            .save_outbox_record(summary_outbox, None, uow)?;
                        for projection_ref in
                            self.save_projection_stale_marks(&summary_subjects, now, uow)?
                        {
                            if !stale_projection_refs.contains(&projection_ref) {
                                stale_projection_refs.push(projection_ref);
                            }
                        }
                    }
                }

                if let (Some(reference_ref), Some(owner_ref)) = (
                    payload.external_reference_ref.clone(),
                    payload.reference_owner_ref.clone(),
                ) {
                    self.save_reference_bundle_sidecars(
                        reference_ref,
                        owner_ref,
                        |state| crate::ports::ExternalReferenceTypedSidecarRefs {
                            role_capability_safe_summary_ref: state.safe_summary_ref.clone(),
                            career_safe_summary_ref: None,
                            memory_safe_summary_ref: None,
                            governance_basis_summary_ref: None,
                            evidence_summary_ref: None,
                            source_version_ref: state.source_version_ref.clone(),
                        },
                        uow,
                    )?;
                }

                let _ = stale_projection_refs;
                self.persist_consumer_outcome(
                    &context,
                    reserved,
                    envelope.consumer_name.clone(),
                    IdentityConsumerOutcome::Accepted,
                    trace_refs,
                    outbox_refs,
                    Vec::new(),
                    now,
                    uow,
                )
            },
        )
    }

    /// Handles one work participation accepted event.
    pub fn handle_work_participation_accepted(
        &self,
        envelope: IdentityInboundEventEnvelope<WorkParticipationAcceptedPayload>,
        context: IdentityOperationContext,
    ) -> Result<IdentityConsumerReceipt, ApplicationError> {
        self.dispatch_inbound_event_scaffold(
            context.clone(),
            &envelope,
            &Self::consumer_schema_version(),
            |reserved, now, uow| {
                let payload = &envelope.payload;
                if !payload
                    .safe_summary_ref
                    .belongs_to_source(&payload.work_source_ref)
                {
                    return self.persist_consumer_outcome(
                        &context,
                        reserved,
                        envelope.consumer_name.clone(),
                        IdentityConsumerOutcome::Rejected,
                        Vec::new(),
                        Vec::new(),
                        vec![Self::issue("career-safe-summary-source-mismatch")],
                        now,
                        uow,
                    );
                }
                if !payload
                    .career_source_marker_ref
                    .member_ref
                    .same_member(&payload.member_ref)
                    || !payload
                        .career_source_marker_ref
                        .work_source_ref
                        .same_source(&payload.work_source_ref)
                {
                    return self.persist_consumer_outcome(
                        &context,
                        reserved,
                        envelope.consumer_name.clone(),
                        IdentityConsumerOutcome::Rejected,
                        Vec::new(),
                        Vec::new(),
                        vec![Self::issue("career-source-marker-mismatch")],
                        now,
                        uow,
                    );
                }

                let Some(_member_v) = self
                    .deps
                    .member_repository
                    .get_member_with_version(payload.member_ref.clone())?
                else {
                    return self.persist_consumer_outcome(
                        &context,
                        reserved,
                        envelope.consumer_name.clone(),
                        IdentityConsumerOutcome::Quarantined,
                        Vec::new(),
                        Vec::new(),
                        vec![Self::issue("career-member-missing")],
                        now,
                        uow,
                    );
                };
                if self
                    .deps
                    .career_record_repository
                    .find_duplicate_source_record(payload.career_source_marker_ref.clone())?
                    .is_some()
                {
                    return self.persist_consumer_outcome(
                        &context,
                        reserved,
                        envelope.consumer_name.clone(),
                        IdentityConsumerOutcome::Noop,
                        Vec::new(),
                        Vec::new(),
                        Vec::new(),
                        now,
                        uow,
                    );
                }

                let source_summary =
                    identity_contracts::refs::WorkParticipationSourceSummary::from_resolver(
                        payload.project_participation_ref.clone(),
                        payload.work_source_ref.clone(),
                        payload.career_source_marker_ref.clone(),
                        Some(payload.safe_summary_ref.clone()),
                        identity_contracts::refs::WorkParticipationSourceState::Trusted,
                    );
                let append_reason_ref =
                    payload
                        .append_reason_ref
                        .clone()
                        .unwrap_or(CareerAppendReasonRef::new(
                            CareerAppendReasonKind::WorkParticipationAccepted,
                            payload.work_source_ref.source_ref.clone(),
                        )?);
                let policy = CareerAppendPolicy::for_append(
                    payload.member_ref.clone(),
                    true,
                    source_summary.clone(),
                    Vec::new(),
                    append_reason_ref.clone(),
                    context.actor_ref.clone(),
                    IdentityOperationChannel::Consumer,
                    CareerRecordChangeIntent::AppendNew,
                    payload.material_marker.clone(),
                );
                policy.assert_member_exists()?;
                policy.assert_source_trusted()?;
                policy.assert_not_duplicate()?;
                policy.assert_append_only()?;
                policy.assert_not_work_truth_write()?;
                policy.assert_allowed_write_channel()?;

                let record_ref =
                    CareerRecordRef::from_id(self.deps.id_generator.new_career_record_id()?);
                let record = CareerRecord::append_from_work_source(
                    record_ref.clone(),
                    payload.member_ref.clone(),
                    source_summary,
                    append_reason_ref.clone(),
                    context.actor_ref.clone(),
                    now,
                )?;
                self.deps
                    .career_record_repository
                    .append_career_record(record.clone(), uow)?;
                let accepted_cursor_ref =
                    self.deps.cursor_assigner.assign_truth_change_cursor(uow)?;
                let subjects = self
                    .deps
                    .truth_change_subject_mapper
                    .career_record_subjects(record_ref.clone());
                let change_kind_ref = IdentityChangeKindRef::new(
                    IdentityChangeKind::CareerRecordChanged,
                    Some(payload.work_source_ref.source_ref.clone()),
                );
                let trace = self.trace_record(
                    payload.member_ref.clone(),
                    &subjects,
                    change_kind_ref.clone(),
                    accepted_cursor_ref.clone(),
                    Some(Self::accepted_change_reason(&append_reason_ref.source_ref)),
                    Some(payload.work_source_ref.source_ref.clone()),
                    context.actor_ref.clone(),
                    now,
                )?;
                self.deps
                    .trace_record_repository
                    .append_trace_record(trace.clone(), uow)?;
                self.append_accepted_audit(
                    &context,
                    Some(payload.member_ref.clone()),
                    &subjects,
                    &change_kind_ref,
                    &accepted_cursor_ref,
                    &trace,
                    now,
                    uow,
                )?;
                let outbox = self.outbox_record(
                    payload.member_ref.clone(),
                    subjects.outbox_subject_ref.clone(),
                    change_kind_ref,
                    AcceptedOutboundMaterialKind::CareerRecordAppended,
                    trace.trace_record_ref.clone(),
                    now,
                )?;
                let outbox_ref = outbox.outbox_record_ref.clone();
                self.deps
                    .outbox_repository
                    .save_outbox_record(outbox, None, uow)?;
                let _ = self.save_projection_stale_marks(&subjects, now, uow)?;
                self.persist_consumer_outcome(
                    &context,
                    reserved,
                    envelope.consumer_name.clone(),
                    IdentityConsumerOutcome::Accepted,
                    vec![trace.trace_record_ref],
                    vec![outbox_ref],
                    Vec::new(),
                    now,
                    uow,
                )
            },
        )
    }

    /// Handles one memory reference source-state change event.
    pub fn handle_memory_reference_source_state_changed(
        &self,
        envelope: IdentityInboundEventEnvelope<MemoryReferenceSourceStateChangedPayload>,
        context: IdentityOperationContext,
    ) -> Result<IdentityConsumerReceipt, ApplicationError> {
        self.dispatch_inbound_event_scaffold(
            context.clone(),
            &envelope,
            &Self::consumer_schema_version(),
            |reserved, now, uow| {
                let payload = &envelope.payload;
                if payload.memory_reference_ref.is_none()
                    && payload.memory_ref.is_none()
                    && payload.archive_ref.is_none()
                    && payload.external_reference_ref.is_none()
                {
                    return self.persist_consumer_outcome(
                        &context,
                        reserved,
                        envelope.consumer_name.clone(),
                        IdentityConsumerOutcome::Rejected,
                        Vec::new(),
                        Vec::new(),
                        vec![Self::issue("memory-event-missing-formal-markers")],
                        now,
                        uow,
                    );
                }
                if payload.external_reference_ref.is_some() != payload.reference_owner_ref.is_some()
                {
                    return self.persist_consumer_outcome(
                        &context,
                        reserved,
                        envelope.consumer_name.clone(),
                        IdentityConsumerOutcome::Rejected,
                        Vec::new(),
                        Vec::new(),
                        vec![Self::issue("memory-reference-sidecar-partial-fields")],
                        now,
                        uow,
                    );
                }
                let Some(_member_v) = self
                    .deps
                    .member_repository
                    .get_member_with_version(payload.member_ref.clone())?
                else {
                    return self.persist_consumer_outcome(
                        &context,
                        reserved,
                        envelope.consumer_name.clone(),
                        IdentityConsumerOutcome::Quarantined,
                        Vec::new(),
                        Vec::new(),
                        vec![Self::issue("memory-member-missing")],
                        now,
                        uow,
                    );
                };

                let loaded = if let Some(reference_ref) = payload.memory_reference_ref.clone() {
                    self.deps
                        .memory_reference_repository
                        .get_memory_reference_with_version(reference_ref)?
                } else if let Some(memory_ref) = payload.memory_ref.clone() {
                    self.deps
                        .memory_reference_repository
                        .find_reference_by_memory(payload.member_ref.clone(), memory_ref)?
                } else if let Some(archive_ref) = payload.archive_ref.clone() {
                    self.deps
                        .memory_reference_repository
                        .find_reference_by_archive(payload.member_ref.clone(), archive_ref)?
                } else {
                    None
                };
                let Some(mut relation_v) = loaded else {
                    return self.persist_consumer_outcome(
                        &context,
                        reserved,
                        envelope.consumer_name.clone(),
                        IdentityConsumerOutcome::Quarantined,
                        Vec::new(),
                        Vec::new(),
                        vec![Self::issue("memory-relation-missing")],
                        now,
                        uow,
                    );
                };
                if !relation_v.value.belongs_to(&payload.member_ref) {
                    return self.persist_consumer_outcome(
                        &context,
                        reserved,
                        envelope.consumer_name.clone(),
                        IdentityConsumerOutcome::Rejected,
                        Vec::new(),
                        Vec::new(),
                        vec![Self::issue("memory-relation-member-mismatch")],
                        now,
                        uow,
                    );
                }

                let reason_ref =
                    payload
                        .reason_ref
                        .clone()
                        .unwrap_or(MemoryReferenceReasonRef::new(
                            MemoryReferenceReasonKind::SourceStateChanged,
                            payload.source_ref.source_ref.clone(),
                        )?);
                let source_summary =
                    identity_contracts::refs::MemoryReferenceSourceSummary::from_resolver(
                        payload.source_ref.clone(),
                        payload
                            .memory_ref
                            .clone()
                            .or_else(|| relation_v.value.memory_ref.clone()),
                        payload
                            .archive_ref
                            .clone()
                            .or_else(|| relation_v.value.archive_ref.clone()),
                        relation_v.value.archive_handoff_ref.clone(),
                        payload.safe_summary_ref.clone(),
                        Self::source_state_from_target(payload.target_state_kind),
                    );
                let policy = MemoryReferencePolicy::for_refresh(
                    payload.member_ref.clone(),
                    true,
                    source_summary.clone(),
                    reason_ref.clone(),
                    context.actor_ref.clone(),
                    IdentityOperationChannel::Consumer,
                    payload.material_marker.clone(),
                );
                policy.assert_member_exists()?;
                policy.assert_reference_present()?;
                policy.assert_source_trusted()?;
                policy.assert_body_free()?;
                policy.assert_not_external_owner_write()?;
                policy.assert_allowed_write_channel()?;

                let next_state = self.memory_state_from_source_event(
                    &relation_v.value,
                    payload,
                    reason_ref.clone(),
                    now,
                )?;
                relation_v.value.source_ref = payload.source_ref.clone();
                relation_v.value.safe_summary_ref = payload.safe_summary_ref.clone();
                relation_v.value.update_reference_state(
                    next_state,
                    reason_ref,
                    context.actor_ref.clone(),
                    now,
                )?;
                self.deps
                    .memory_reference_repository
                    .save_memory_reference(
                        relation_v.value.clone(),
                        Some(relation_v.version),
                        uow,
                    )?;

                if let (Some(reference_ref), Some(owner_ref)) = (
                    payload.external_reference_ref.clone(),
                    payload.reference_owner_ref.clone(),
                ) {
                    self.save_reference_bundle_sidecars(
                        reference_ref,
                        owner_ref,
                        |state| crate::ports::ExternalReferenceTypedSidecarRefs {
                            role_capability_safe_summary_ref: None,
                            career_safe_summary_ref: None,
                            memory_safe_summary_ref: state.safe_summary_ref.clone(),
                            governance_basis_summary_ref: None,
                            evidence_summary_ref: None,
                            source_version_ref: state.source_version_ref.clone(),
                        },
                        uow,
                    )?;
                }

                let accepted_cursor_ref =
                    self.deps.cursor_assigner.assign_truth_change_cursor(uow)?;
                let subjects = self
                    .deps
                    .truth_change_subject_mapper
                    .memory_reference_subjects(relation_v.value.memory_reference_ref.clone());
                let change_kind_ref = IdentityChangeKindRef::new(
                    IdentityChangeKind::MemoryReferenceChanged,
                    Some(payload.source_ref.source_ref.clone()),
                );
                let trace = self.trace_record(
                    payload.member_ref.clone(),
                    &subjects,
                    change_kind_ref.clone(),
                    accepted_cursor_ref.clone(),
                    Some(Self::accepted_change_reason(&payload.source_ref.source_ref)),
                    Some(payload.source_ref.source_ref.clone()),
                    context.actor_ref.clone(),
                    now,
                )?;
                self.deps
                    .trace_record_repository
                    .append_trace_record(trace.clone(), uow)?;
                self.append_accepted_audit(
                    &context,
                    Some(payload.member_ref.clone()),
                    &subjects,
                    &change_kind_ref,
                    &accepted_cursor_ref,
                    &trace,
                    now,
                    uow,
                )?;
                let outbox = self.outbox_record(
                    payload.member_ref.clone(),
                    subjects.outbox_subject_ref.clone(),
                    change_kind_ref.clone(),
                    AcceptedOutboundMaterialKind::MemoryReferenceChanged,
                    trace.trace_record_ref.clone(),
                    now,
                )?;
                let mut outbox_refs = vec![outbox.outbox_record_ref.clone()];
                self.deps
                    .outbox_repository
                    .save_outbox_record(outbox, None, uow)?;
                if relation_v.value.archive_handoff_ref.is_some()
                    && matches!(
                        payload.target_state_kind,
                        PublicMemoryReferenceStateKind::Archived
                            | PublicMemoryReferenceStateKind::Migrated
                            | PublicMemoryReferenceStateKind::HandoffPending
                            | PublicMemoryReferenceStateKind::HandoffFailed
                    )
                {
                    let handoff_outbox = self.outbox_record(
                        payload.member_ref.clone(),
                        subjects.outbox_subject_ref.clone(),
                        change_kind_ref,
                        AcceptedOutboundMaterialKind::MemoryArchiveHandoffStateChanged,
                        trace.trace_record_ref.clone(),
                        now,
                    )?;
                    outbox_refs.push(handoff_outbox.outbox_record_ref.clone());
                    self.deps
                        .outbox_repository
                        .save_outbox_record(handoff_outbox, None, uow)?;
                }
                let _ = self.save_projection_stale_marks(&subjects, now, uow)?;
                self.persist_consumer_outcome(
                    &context,
                    reserved,
                    envelope.consumer_name.clone(),
                    IdentityConsumerOutcome::Accepted,
                    vec![trace.trace_record_ref],
                    outbox_refs,
                    Vec::new(),
                    now,
                    uow,
                )
            },
        )
    }

    /// Handles one archive handoff callback result.
    pub fn handle_archive_handoff_result(
        &self,
        envelope: IdentityInboundEventEnvelope<ArchiveHandoffResultPayload>,
        context: IdentityOperationContext,
    ) -> Result<IdentityConsumerReceipt, ApplicationError> {
        self.dispatch_callback_scaffold(
            context.clone(),
            &envelope,
            &Self::consumer_schema_version(),
            |reserved, now, uow| {
                let payload = &envelope.payload;
                let Some(_member_v) = self
                    .deps
                    .member_repository
                    .get_member_with_version(payload.member_ref.clone())?
                else {
                    return self.persist_callback_outcome(
                        &context,
                        reserved,
                        envelope.consumer_name.clone(),
                        IdentityConsumerOutcome::Quarantined,
                        Vec::new(),
                        Vec::new(),
                        vec![Self::issue("archive-callback-member-missing")],
                        now,
                        uow,
                    );
                };

                let direct = if let Some(reference_ref) = payload.memory_reference_ref.clone() {
                    self.deps
                        .memory_reference_repository
                        .get_memory_reference_with_version(reference_ref)?
                } else {
                    None
                };
                let lookup_ref = self
                    .deps
                    .memory_reference_repository
                    .find_callback_target_by_handoff(payload.archive_handoff_ref.clone())?;
                if let (Some(direct_v), Some(found_ref)) = (direct.as_ref(), lookup_ref.as_ref()) {
                    if direct_v.value.memory_reference_ref != *found_ref {
                        return self.persist_callback_outcome(
                            &context,
                            reserved,
                            envelope.consumer_name.clone(),
                            IdentityConsumerOutcome::Rejected,
                            Vec::new(),
                            Vec::new(),
                            vec![Self::issue("archive-callback-target-mismatch")],
                            now,
                            uow,
                        );
                    }
                }
                let Some(mut relation_v) = (match (direct, lookup_ref) {
                    (Some(direct_v), _) => Some(direct_v),
                    (None, Some(reference_ref)) => self
                        .deps
                        .memory_reference_repository
                        .get_memory_reference_with_version(reference_ref)?,
                    (None, None) => None,
                }) else {
                    return self.persist_callback_outcome(
                        &context,
                        reserved,
                        envelope.consumer_name.clone(),
                        IdentityConsumerOutcome::Quarantined,
                        Vec::new(),
                        Vec::new(),
                        vec![Self::issue("archive-callback-target-missing")],
                        now,
                        uow,
                    );
                };
                if !relation_v.value.belongs_to(&payload.member_ref) {
                    return self.persist_callback_outcome(
                        &context,
                        reserved,
                        envelope.consumer_name.clone(),
                        IdentityConsumerOutcome::Rejected,
                        Vec::new(),
                        Vec::new(),
                        vec![Self::issue("archive-callback-member-mismatch")],
                        now,
                        uow,
                    );
                }
                if payload.target_state_kind == PublicMemoryReferenceStateKind::HandoffFailed
                    && payload.issue_ref.is_none()
                {
                    return self.persist_callback_outcome(
                        &context,
                        reserved,
                        envelope.consumer_name.clone(),
                        IdentityConsumerOutcome::Rejected,
                        Vec::new(),
                        Vec::new(),
                        vec![Self::issue("archive-callback-issue-missing")],
                        now,
                        uow,
                    );
                }

                let source_ref = identity_contracts::refs::MemoryReferenceSourceRef::new(
                    MemoryReferenceSourceKind::ArchiveHandoffResult,
                    payload.archive_handoff_ref.source_ref.clone(),
                )?;
                let reason_ref =
                    payload
                        .reason_ref
                        .clone()
                        .unwrap_or(MemoryReferenceReasonRef::new(
                            MemoryReferenceReasonKind::ArchiveHandoffResult,
                            payload.archive_handoff_ref.source_ref.clone(),
                        )?);
                let source_summary =
                    identity_contracts::refs::MemoryReferenceSourceSummary::from_resolver(
                        source_ref.clone(),
                        relation_v.value.memory_ref.clone(),
                        Some(payload.archive_ref.clone()),
                        Some(payload.archive_handoff_ref.clone()),
                        None,
                        Self::archive_source_state_from_target(payload.target_state_kind),
                    );
                let policy = MemoryReferencePolicy::for_archive_handoff(
                    payload.member_ref.clone(),
                    true,
                    source_summary,
                    reason_ref.clone(),
                    context.actor_ref.clone(),
                    IdentityOperationChannel::HandoffCallback,
                    payload.material_marker.clone(),
                );
                policy.assert_member_exists()?;
                policy.assert_reference_present()?;
                policy.assert_source_trusted()?;
                policy.assert_body_free()?;
                policy.assert_handoff_marker_body_free()?;
                policy.assert_allowed_write_channel()?;

                let next_state = self.memory_state_from_archive_callback(
                    &relation_v.value,
                    payload,
                    reason_ref.clone(),
                    now,
                )?;
                relation_v.value.source_ref = source_ref;
                relation_v.value.safe_summary_ref = None;
                relation_v.value.update_reference_state(
                    next_state,
                    reason_ref,
                    context.actor_ref.clone(),
                    now,
                )?;
                self.deps
                    .memory_reference_repository
                    .save_memory_reference(
                        relation_v.value.clone(),
                        Some(relation_v.version),
                        uow,
                    )?;

                let accepted_cursor_ref =
                    self.deps.cursor_assigner.assign_truth_change_cursor(uow)?;
                let subjects = self
                    .deps
                    .truth_change_subject_mapper
                    .memory_reference_subjects(relation_v.value.memory_reference_ref.clone());
                let change_kind_ref = IdentityChangeKindRef::new(
                    IdentityChangeKind::MemoryReferenceChanged,
                    Some(payload.archive_handoff_ref.source_ref.clone()),
                );
                let trace = self.trace_record(
                    payload.member_ref.clone(),
                    &subjects,
                    change_kind_ref.clone(),
                    accepted_cursor_ref.clone(),
                    Some(Self::accepted_change_reason(
                        &payload.archive_handoff_ref.source_ref,
                    )),
                    Some(payload.archive_handoff_ref.source_ref.clone()),
                    context.actor_ref.clone(),
                    now,
                )?;
                self.deps
                    .trace_record_repository
                    .append_trace_record(trace.clone(), uow)?;
                self.append_accepted_audit(
                    &context,
                    Some(payload.member_ref.clone()),
                    &subjects,
                    &change_kind_ref,
                    &accepted_cursor_ref,
                    &trace,
                    now,
                    uow,
                )?;
                let handoff_outbox = self.outbox_record(
                    payload.member_ref.clone(),
                    subjects.outbox_subject_ref.clone(),
                    change_kind_ref.clone(),
                    AcceptedOutboundMaterialKind::MemoryArchiveHandoffStateChanged,
                    trace.trace_record_ref.clone(),
                    now,
                )?;
                let memory_outbox = self.outbox_record(
                    payload.member_ref.clone(),
                    subjects.outbox_subject_ref.clone(),
                    change_kind_ref,
                    AcceptedOutboundMaterialKind::MemoryReferenceChanged,
                    trace.trace_record_ref.clone(),
                    now,
                )?;
                let outbox_refs = vec![
                    handoff_outbox.outbox_record_ref.clone(),
                    memory_outbox.outbox_record_ref.clone(),
                ];
                self.deps
                    .outbox_repository
                    .save_outbox_record(handoff_outbox, None, uow)?;
                self.deps
                    .outbox_repository
                    .save_outbox_record(memory_outbox, None, uow)?;
                let _ = self.save_projection_stale_marks(&subjects, now, uow)?;
                self.persist_callback_outcome(
                    &context,
                    reserved,
                    envelope.consumer_name.clone(),
                    IdentityConsumerOutcome::Accepted,
                    vec![trace.trace_record_ref],
                    outbox_refs,
                    Vec::new(),
                    now,
                    uow,
                )
            },
        )
    }

    /// Handles one trace handoff callback result.
    pub fn handle_trace_handoff_result(
        &self,
        envelope: IdentityInboundEventEnvelope<TraceHandoffResultPayload>,
        context: IdentityOperationContext,
    ) -> Result<IdentityConsumerReceipt, ApplicationError> {
        self.dispatch_callback_scaffold(
            context.clone(),
            &envelope,
            &Self::consumer_schema_version(),
            |reserved, now, uow| {
                let payload = &envelope.payload;
                let Some(mut intent_v) = self
                    .deps
                    .handoff_intent_repository
                    .get_handoff_intent_with_version(payload.handoff_intent_ref.clone())?
                else {
                    return self.persist_callback_outcome(
                        &context,
                        reserved,
                        envelope.consumer_name.clone(),
                        IdentityConsumerOutcome::Quarantined,
                        Vec::new(),
                        Vec::new(),
                        vec![Self::issue("trace-handoff-intent-missing")],
                        now,
                        uow,
                    );
                };
                if !intent_v.value.targets(&payload.handoff_target_ref) {
                    return self.persist_callback_outcome(
                        &context,
                        reserved,
                        envelope.consumer_name.clone(),
                        IdentityConsumerOutcome::Rejected,
                        Vec::new(),
                        Vec::new(),
                        vec![Self::issue("trace-handoff-target-mismatch")],
                        now,
                        uow,
                    );
                }
                if let Some(scope_ref) = payload.handoff_scope_ref.as_ref() {
                    if intent_v.value.handoff_scope_ref != *scope_ref {
                        return self.persist_callback_outcome(
                            &context,
                            reserved,
                            envelope.consumer_name.clone(),
                            IdentityConsumerOutcome::Rejected,
                            Vec::new(),
                            Vec::new(),
                            vec![Self::issue("trace-handoff-scope-mismatch")],
                            now,
                            uow,
                        );
                    }
                }
                if intent_v.value.trace_record_refs.is_empty()
                    || intent_v.value.safe_material_ref.as_str().contains("body")
                {
                    return self.persist_callback_outcome(
                        &context,
                        reserved,
                        envelope.consumer_name.clone(),
                        IdentityConsumerOutcome::Rejected,
                        Vec::new(),
                        Vec::new(),
                        vec![Self::issue("trace-handoff-safe-material-invalid")],
                        now,
                        uow,
                    );
                }

                let next_state = match payload.result_kind {
                    TraceHandoffResultKind::Delivered => {
                        let Some(receipt_ref) = payload.receipt_ref.clone() else {
                            return self.persist_callback_outcome(
                                &context,
                                reserved,
                                envelope.consumer_name.clone(),
                                IdentityConsumerOutcome::Rejected,
                                Vec::new(),
                                Vec::new(),
                                vec![Self::issue("trace-handoff-receipt-missing")],
                                now,
                                uow,
                            );
                        };
                        HandoffPolicy::assert_receipt_is_marker(&receipt_ref)?;
                        HandoffState::delivered(payload.attempt_ref.clone(), receipt_ref, now)
                    }
                    TraceHandoffResultKind::RetryableFailed => {
                        let Some(issue_ref) = payload.issue_ref.clone() else {
                            return self.persist_callback_outcome(
                                &context,
                                reserved,
                                envelope.consumer_name.clone(),
                                IdentityConsumerOutcome::Rejected,
                                Vec::new(),
                                Vec::new(),
                                vec![Self::issue("trace-handoff-issue-missing")],
                                now,
                                uow,
                            );
                        };
                        HandoffState::retryable_failed(payload.attempt_ref.clone(), issue_ref, now)
                    }
                    TraceHandoffResultKind::Failed => {
                        let Some(issue_ref) = payload.issue_ref.clone() else {
                            return self.persist_callback_outcome(
                                &context,
                                reserved,
                                envelope.consumer_name.clone(),
                                IdentityConsumerOutcome::Rejected,
                                Vec::new(),
                                Vec::new(),
                                vec![Self::issue("trace-handoff-issue-missing")],
                                now,
                                uow,
                            );
                        };
                        HandoffState::failed(payload.attempt_ref.clone(), issue_ref, now)
                    }
                    TraceHandoffResultKind::Cancelled => {
                        let Some(issue_ref) = payload.issue_ref.clone() else {
                            return self.persist_callback_outcome(
                                &context,
                                reserved,
                                envelope.consumer_name.clone(),
                                IdentityConsumerOutcome::Rejected,
                                Vec::new(),
                                Vec::new(),
                                vec![Self::issue("trace-handoff-issue-missing")],
                                now,
                                uow,
                            );
                        };
                        HandoffState::cancelled(issue_ref, now)
                    }
                };
                match payload.result_kind {
                    TraceHandoffResultKind::Delivered => {
                        intent_v.value.mark_delivered(next_state)?
                    }
                    TraceHandoffResultKind::RetryableFailed => {
                        intent_v.value.mark_retryable_failed(next_state)?
                    }
                    TraceHandoffResultKind::Failed => intent_v.value.mark_failed(next_state)?,
                    TraceHandoffResultKind::Cancelled => {
                        intent_v.value.mark_cancelled(next_state)?
                    }
                }
                self.deps.handoff_intent_repository.save_handoff_intent(
                    intent_v.value.clone(),
                    Some(intent_v.version),
                    uow,
                )?;

                let accepted_cursor_ref =
                    self.deps.cursor_assigner.assign_truth_change_cursor(uow)?;
                let subjects = self
                    .deps
                    .truth_change_subject_mapper
                    .handoff_intent_subjects(payload.handoff_intent_ref.clone());
                let change_kind_ref = IdentityChangeKindRef::new(
                    IdentityChangeKind::DerivedMarkerChanged,
                    Some(payload.attempt_ref.attempt_ref.clone()),
                );
                let trace = self.trace_record(
                    intent_v.value.member_ref.clone(),
                    &subjects,
                    change_kind_ref.clone(),
                    accepted_cursor_ref.clone(),
                    Some(Self::accepted_change_reason(
                        &payload.attempt_ref.attempt_ref,
                    )),
                    Some(payload.attempt_ref.attempt_ref.clone()),
                    context.actor_ref.clone(),
                    now,
                )?;
                self.deps
                    .trace_record_repository
                    .append_trace_record(trace.clone(), uow)?;
                self.append_accepted_audit(
                    &context,
                    Some(intent_v.value.member_ref.clone()),
                    &subjects,
                    &change_kind_ref,
                    &accepted_cursor_ref,
                    &trace,
                    now,
                    uow,
                )?;

                let mut trace_refs = vec![trace.trace_record_ref.clone()];
                if let Some(receipt_ref) = payload.receipt_ref.clone() {
                    let marker_trace = self.trace_record_with_subject(
                        intent_v.value.member_ref.clone(),
                        self.deps
                            .marker_subject_mapper
                            .handoff_receipt_marker_subject(receipt_ref),
                        subjects.audit_subject_ref.clone(),
                        change_kind_ref.clone(),
                        accepted_cursor_ref.clone(),
                        Some(Self::accepted_change_reason(
                            &payload.attempt_ref.attempt_ref,
                        )),
                        Some(payload.attempt_ref.attempt_ref.clone()),
                        context.actor_ref.clone(),
                        now,
                    )?;
                    self.deps
                        .trace_record_repository
                        .append_trace_record(marker_trace.clone(), uow)?;
                    trace_refs.push(marker_trace.trace_record_ref);
                }

                let outbox = self.outbox_record(
                    intent_v.value.member_ref.clone(),
                    subjects.outbox_subject_ref.clone(),
                    change_kind_ref,
                    AcceptedOutboundMaterialKind::MemoryArchiveHandoffStateChanged,
                    trace.trace_record_ref.clone(),
                    now,
                )?;
                let outbox_ref = outbox.outbox_record_ref.clone();
                self.deps
                    .outbox_repository
                    .save_outbox_record(outbox, None, uow)?;
                let _ = self.save_projection_stale_marks(&subjects, now, uow)?;
                self.persist_callback_outcome(
                    &context,
                    reserved,
                    envelope.consumer_name.clone(),
                    IdentityConsumerOutcome::Accepted,
                    trace_refs,
                    vec![outbox_ref],
                    Vec::new(),
                    now,
                    uow,
                )
            },
        )
    }

    fn dispatch_scaffold<T, F>(
        &self,
        context: &IdentityOperationContext,
        envelope: &IdentityInboundEventEnvelope<T>,
        expected_schema_version_ref: &IdentityProtocolSchemaVersionRef,
        result_kind: IdentityStoredResultKind,
        handler: F,
    ) -> Result<IdentityConsumerReceipt, ApplicationError>
    where
        F: FnOnce(
            Versioned<IdentityIdempotencyRecord>,
            identity_contracts::refs::IdentityTimestamp,
            &dyn IdentityUnitOfWork,
        ) -> Result<IdentityConsumerReceipt, ApplicationError>,
    {
        let now = self.deps.clock.now()?;
        let uow = self.deps.unit_of_work_manager.begin()?;
        let result = match self.reserve_idempotency(context, now, uow.as_ref())? {
            IdempotencyReserveOutcome::ReplayAvailable {
                stored_result_ref, ..
            } => {
                let receipt = self.replay_receipt(stored_result_ref, result_kind);
                self.rollback_quietly(uow);
                return receipt;
            }
            IdempotencyReserveOutcome::Conflict(_) => {
                self.rollback_quietly(uow);
                return Err(ApplicationError::new(
                    ApplicationErrorKind::IdempotencyConflict,
                    "same inbound idempotency key is already bound to different canonical material",
                ));
            }
            IdempotencyReserveOutcome::InFlight(_) => {
                self.rollback_quietly(uow);
                return Err(ApplicationError::new(
                    ApplicationErrorKind::IdempotencyInFlight,
                    "same inbound idempotency key and digest is still in flight",
                ));
            }
            IdempotencyReserveOutcome::Reserved(record) => {
                if envelope.schema_version_ref != *expected_schema_version_ref {
                    let stored_result_ref =
                        self.deps.id_generator.new_identity_stored_result_ref()?;
                    let fresh_receipt = self.build_receipt(
                        envelope.consumer_name.clone(),
                        IdentityConsumerOutcome::UnsupportedVersion,
                        stored_result_ref.clone(),
                        Vec::new(),
                        Vec::new(),
                        vec![IdentityProtocolValidationIssueRef::new(format!(
                            "unsupported-schema:{}",
                            envelope.schema_version_ref.as_str()
                        ))],
                    )?;
                    let replay_receipt = Self::duplicate_replay_receipt(&fresh_receipt);
                    let persist = self.persist_replayable_receipt(
                        result_kind,
                        context,
                        record,
                        replay_receipt,
                        now,
                        uow.as_ref(),
                    );
                    match persist {
                        Ok(_) => match self.deps.unit_of_work_manager.commit(uow) {
                            Ok(()) => Ok(fresh_receipt),
                            Err(err) => Err(err),
                        },
                        Err(err) => {
                            self.rollback_quietly(uow);
                            Err(err)
                        }
                    }
                } else {
                    match handler(record, now, uow.as_ref()) {
                        Ok(receipt) => match self.deps.unit_of_work_manager.commit(uow) {
                            Ok(()) => Ok(receipt),
                            Err(err) => Err(err),
                        },
                        Err(err) => {
                            self.rollback_quietly(uow);
                            Err(err)
                        }
                    }
                }
            }
        };

        result
    }

    fn consumer_schema_version() -> IdentityProtocolSchemaVersionRef {
        IdentityProtocolSchemaVersionRef::new("identity.consumer.v1")
    }

    fn issue(token: impl Into<String>) -> IdentityProtocolValidationIssueRef {
        IdentityProtocolValidationIssueRef::new(token)
    }

    fn accepted_change_reason(source_ref: &IdentitySourceRef) -> IdentityChangeReasonRef {
        IdentityChangeReasonRef::new(source_ref.clone())
    }

    fn trace_record(
        &self,
        member_ref: GlobalMemberRef,
        subjects: &IdentityAcceptedSubjectRefs,
        change_kind_ref: IdentityChangeKindRef,
        accepted_cursor_ref: IdentityTruthCursor,
        reason_ref: Option<IdentityChangeReasonRef>,
        source_ref: Option<IdentitySourceRef>,
        actor_ref: ActorRef,
        occurred_at: IdentityTimestamp,
    ) -> Result<IdentityTraceRecord, ApplicationError> {
        self.trace_record_with_subject(
            member_ref,
            subjects.trace_subject_ref.clone(),
            subjects.audit_subject_ref.clone(),
            change_kind_ref,
            accepted_cursor_ref,
            reason_ref,
            source_ref,
            actor_ref,
            occurred_at,
        )
    }

    fn trace_record_with_subject(
        &self,
        member_ref: GlobalMemberRef,
        subject_ref: identity_contracts::refs::IdentityTraceSubjectRef,
        audit_subject_ref: identity_contracts::refs::IdentityAuditSubjectRef,
        change_kind_ref: IdentityChangeKindRef,
        accepted_cursor_ref: IdentityTruthCursor,
        reason_ref: Option<IdentityChangeReasonRef>,
        source_ref: Option<IdentitySourceRef>,
        actor_ref: ActorRef,
        occurred_at: IdentityTimestamp,
    ) -> Result<IdentityTraceRecord, ApplicationError> {
        let trace_record_ref = self.deps.id_generator.new_identity_trace_record_id()?;
        IdentityTraceRecord::from_accepted_change(
            IdentityTraceRecordRef::new(trace_record_ref.as_str()),
            member_ref,
            subject_ref,
            audit_subject_ref,
            change_kind_ref,
            accepted_cursor_ref,
            reason_ref,
            source_ref,
            None,
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
        )?;
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
            let mut next: ProjectionState = versioned.value.clone();
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

    fn persist_consumer_outcome(
        &self,
        context: &IdentityOperationContext,
        reserved: Versioned<IdentityIdempotencyRecord>,
        consumer_name: IdentityInboundConsumerName,
        outcome: IdentityConsumerOutcome,
        trace_refs: Vec<IdentityTraceRecordRef>,
        outbox_refs: Vec<IdentityOutboxRecordRef>,
        issue_refs: Vec<IdentityProtocolValidationIssueRef>,
        now: IdentityTimestamp,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityConsumerReceipt, ApplicationError> {
        self.persist_receipt_outcome(
            IdentityStoredResultKind::ConsumerReceipt,
            context,
            reserved,
            consumer_name,
            outcome,
            trace_refs,
            outbox_refs,
            issue_refs,
            now,
            uow,
        )
    }

    fn persist_callback_outcome(
        &self,
        context: &IdentityOperationContext,
        reserved: Versioned<IdentityIdempotencyRecord>,
        consumer_name: IdentityInboundConsumerName,
        outcome: IdentityConsumerOutcome,
        trace_refs: Vec<IdentityTraceRecordRef>,
        outbox_refs: Vec<IdentityOutboxRecordRef>,
        issue_refs: Vec<IdentityProtocolValidationIssueRef>,
        now: IdentityTimestamp,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityConsumerReceipt, ApplicationError> {
        self.persist_receipt_outcome(
            IdentityStoredResultKind::HandoffCallbackReceipt,
            context,
            reserved,
            consumer_name,
            outcome,
            trace_refs,
            outbox_refs,
            issue_refs,
            now,
            uow,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn persist_receipt_outcome(
        &self,
        result_kind: IdentityStoredResultKind,
        context: &IdentityOperationContext,
        reserved: Versioned<IdentityIdempotencyRecord>,
        consumer_name: IdentityInboundConsumerName,
        outcome: IdentityConsumerOutcome,
        trace_refs: Vec<IdentityTraceRecordRef>,
        outbox_refs: Vec<IdentityOutboxRecordRef>,
        issue_refs: Vec<IdentityProtocolValidationIssueRef>,
        now: IdentityTimestamp,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityConsumerReceipt, ApplicationError> {
        let stored_result_ref = self.deps.id_generator.new_identity_stored_result_ref()?;
        let fresh_receipt = self.build_receipt(
            consumer_name,
            outcome,
            stored_result_ref,
            trace_refs,
            outbox_refs,
            issue_refs,
        )?;
        let replay_receipt = Self::duplicate_replay_receipt(&fresh_receipt);
        self.persist_replayable_receipt(result_kind, context, reserved, replay_receipt, now, uow)?;
        Ok(fresh_receipt)
    }

    fn source_state_from_target(
        target_state_kind: PublicMemoryReferenceStateKind,
    ) -> MemoryReferenceSourceState {
        match target_state_kind {
            PublicMemoryReferenceStateKind::Linked => MemoryReferenceSourceState::Trusted,
            PublicMemoryReferenceStateKind::PendingVerification => {
                MemoryReferenceSourceState::PendingVerification
            }
            PublicMemoryReferenceStateKind::Stale => MemoryReferenceSourceState::Stale,
            PublicMemoryReferenceStateKind::Unavailable => MemoryReferenceSourceState::Unavailable,
            PublicMemoryReferenceStateKind::Migrated
            | PublicMemoryReferenceStateKind::Archived
            | PublicMemoryReferenceStateKind::HandoffPending => {
                MemoryReferenceSourceState::HandoffResultAccepted
            }
            PublicMemoryReferenceStateKind::HandoffFailed => {
                MemoryReferenceSourceState::HandoffResultFailed
            }
        }
    }

    fn archive_source_state_from_target(
        target_state_kind: PublicMemoryReferenceStateKind,
    ) -> MemoryReferenceSourceState {
        match target_state_kind {
            PublicMemoryReferenceStateKind::HandoffFailed => {
                MemoryReferenceSourceState::HandoffResultFailed
            }
            _ => MemoryReferenceSourceState::HandoffResultAccepted,
        }
    }

    fn memory_state_from_source_event(
        &self,
        relation: &MemoryReference,
        payload: &MemoryReferenceSourceStateChangedPayload,
        reason_ref: MemoryReferenceReasonRef,
        now: IdentityTimestamp,
    ) -> Result<MemoryReferenceState, ApplicationError> {
        match payload.target_state_kind {
            PublicMemoryReferenceStateKind::Linked => {
                let Some(memory_ref) = payload
                    .memory_ref
                    .clone()
                    .or_else(|| relation.memory_ref.clone())
                else {
                    return Err(ApplicationError::invalid_request(
                        "linked memory relation requires memory_ref",
                    ));
                };
                Ok(MemoryReferenceState::linked(memory_ref, reason_ref, now))
            }
            PublicMemoryReferenceStateKind::PendingVerification => {
                Ok(MemoryReferenceState::pending_verification(
                    payload
                        .memory_ref
                        .clone()
                        .or_else(|| relation.memory_ref.clone()),
                    payload
                        .archive_ref
                        .clone()
                        .or_else(|| relation.archive_ref.clone()),
                    relation.archive_handoff_ref.clone(),
                    reason_ref,
                    now,
                ))
            }
            PublicMemoryReferenceStateKind::Stale => {
                let mut state = relation.reference_state.clone();
                state.mark_stale(reason_ref, now)?;
                Ok(state)
            }
            PublicMemoryReferenceStateKind::Unavailable => {
                let mut state = relation.reference_state.clone();
                state.mark_unavailable(reason_ref, now)?;
                Ok(state)
            }
            PublicMemoryReferenceStateKind::Migrated => {
                let Some(handoff_ref) = relation.archive_handoff_ref.clone() else {
                    return Err(ApplicationError::invalid_request(
                        "migrated memory relation requires archive_handoff_ref",
                    ));
                };
                let mut state = relation.reference_state.clone();
                state.mark_migrated(
                    payload
                        .memory_ref
                        .clone()
                        .or_else(|| relation.memory_ref.clone()),
                    payload
                        .archive_ref
                        .clone()
                        .or_else(|| relation.archive_ref.clone()),
                    handoff_ref,
                    reason_ref,
                    now,
                )?;
                Ok(state)
            }
            PublicMemoryReferenceStateKind::Archived => {
                let Some(archive_ref) = payload
                    .archive_ref
                    .clone()
                    .or_else(|| relation.archive_ref.clone())
                else {
                    return Err(ApplicationError::invalid_request(
                        "archived memory relation requires archive_ref",
                    ));
                };
                let Some(handoff_ref) = relation.archive_handoff_ref.clone() else {
                    return Err(ApplicationError::invalid_request(
                        "archived memory relation requires archive_handoff_ref",
                    ));
                };
                Ok(MemoryReferenceState::archived(
                    archive_ref,
                    handoff_ref,
                    reason_ref,
                    now,
                ))
            }
            PublicMemoryReferenceStateKind::HandoffPending => {
                let Some(handoff_ref) = relation.archive_handoff_ref.clone() else {
                    return Err(ApplicationError::invalid_request(
                        "handoff pending state requires archive_handoff_ref",
                    ));
                };
                Ok(MemoryReferenceState {
                    state_kind:
                        identity_domain::memory_reference::MemoryReferenceStateKind::HandoffPending,
                    memory_ref: payload
                        .memory_ref
                        .clone()
                        .or_else(|| relation.memory_ref.clone()),
                    archive_ref: payload
                        .archive_ref
                        .clone()
                        .or_else(|| relation.archive_ref.clone()),
                    handoff_ref: Some(handoff_ref),
                    reason_ref: Some(reason_ref),
                    checked_at: now,
                })
            }
            PublicMemoryReferenceStateKind::HandoffFailed => {
                let Some(handoff_ref) = relation.archive_handoff_ref.clone() else {
                    return Err(ApplicationError::invalid_request(
                        "handoff failed state requires archive_handoff_ref",
                    ));
                };
                Ok(MemoryReferenceState::handoff_failed(
                    handoff_ref,
                    reason_ref,
                    now,
                ))
            }
        }
    }

    fn memory_state_from_archive_callback(
        &self,
        relation: &MemoryReference,
        payload: &ArchiveHandoffResultPayload,
        reason_ref: MemoryReferenceReasonRef,
        now: IdentityTimestamp,
    ) -> Result<MemoryReferenceState, ApplicationError> {
        match payload.target_state_kind {
            PublicMemoryReferenceStateKind::Archived => Ok(MemoryReferenceState::archived(
                payload.archive_ref.clone(),
                payload.archive_handoff_ref.clone(),
                reason_ref,
                now,
            )),
            PublicMemoryReferenceStateKind::Migrated => {
                let mut state = relation.reference_state.clone();
                state.mark_migrated(
                    relation.memory_ref.clone(),
                    Some(payload.archive_ref.clone()),
                    payload.archive_handoff_ref.clone(),
                    reason_ref,
                    now,
                )?;
                Ok(state)
            }
            PublicMemoryReferenceStateKind::HandoffPending => Ok(MemoryReferenceState {
                state_kind:
                    identity_domain::memory_reference::MemoryReferenceStateKind::HandoffPending,
                memory_ref: relation.memory_ref.clone(),
                archive_ref: Some(payload.archive_ref.clone()),
                handoff_ref: Some(payload.archive_handoff_ref.clone()),
                reason_ref: Some(reason_ref),
                checked_at: now,
            }),
            PublicMemoryReferenceStateKind::HandoffFailed => {
                Ok(MemoryReferenceState::handoff_failed(
                    payload.archive_handoff_ref.clone(),
                    reason_ref,
                    now,
                ))
            }
            PublicMemoryReferenceStateKind::Linked => Err(ApplicationError::invalid_request(
                "archive callback cannot mark relation linked without an explicit safe relation marker",
            )),
            other => Err(ApplicationError::invalid_request(format!(
                "unsupported archive callback target state: {other:?}"
            ))),
        }
    }

    fn save_reference_bundle_sidecars<F>(
        &self,
        reference_ref: ExternalReferenceRef,
        owner_ref: IdentityReferenceOwnerRef,
        build_sidecars: F,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<(), ApplicationError>
    where
        F: FnOnce(
            &identity_domain::reference_state::ReferenceResolutionState,
        ) -> crate::ports::ExternalReferenceTypedSidecarRefs,
    {
        let current = self
            .deps
            .reference_state_repository
            .get_reference_state_with_version(reference_ref.clone())?;
        let resolved = self
            .deps
            .external_reference_resolver
            .resolve_external_reference(reference_ref.clone(), owner_ref)?;
        let saved = self.deps.reference_state_repository.save_reference_state(
            resolved.clone(),
            current.as_ref().map(|value| value.version),
            uow,
        )?;
        self.deps
            .reference_state_repository
            .save_typed_sidecar_refs(
                reference_ref,
                build_sidecars(&resolved),
                saved.version,
                uow,
            )?;
        Ok(())
    }

    fn persist_replayable_receipt(
        &self,
        result_kind: IdentityStoredResultKind,
        context: &IdentityOperationContext,
        reserved: Versioned<IdentityIdempotencyRecord>,
        replay_receipt: IdentityConsumerReceipt,
        recorded_at: identity_contracts::refs::IdentityTimestamp,
        uow: &dyn IdentityUnitOfWork,
    ) -> Result<IdentityStoredResultRef, ApplicationError> {
        let surface_marker_ref = self
            .deps
            .id_generator
            .new_identity_stored_surface_marker_ref()?;
        let stored_result_ref = replay_receipt.stored_result_ref.clone();
        let stored_result = match result_kind {
            IdentityStoredResultKind::ConsumerReceipt => {
                StoredIdentityOperationResult::consumer_receipt(
                    stored_result_ref.clone(),
                    context.context_ref.clone(),
                    surface_marker_ref.clone(),
                    recorded_at,
                )
            }
            IdentityStoredResultKind::HandoffCallbackReceipt => {
                StoredIdentityOperationResult::handoff_callback_receipt(
                    stored_result_ref.clone(),
                    context.context_ref.clone(),
                    surface_marker_ref.clone(),
                    recorded_at,
                )
            }
            other => {
                return Err(ApplicationError::invalid_request(format!(
                    "unsupported consumer receipt result kind: {other:?}"
                )));
            }
        };

        match result_kind {
            IdentityStoredResultKind::ConsumerReceipt => {
                self.deps
                    .stored_result_repository
                    .save_consumer_receipt_result(stored_result, uow)?;
                self.deps.stored_result_repository.save_consumer_receipt(
                    IdentityConsumerReceiptEnvelope::consumer_receipt(
                        context.context_ref.clone(),
                        surface_marker_ref,
                        replay_receipt,
                        recorded_at,
                    ),
                    uow,
                )?;
            }
            IdentityStoredResultKind::HandoffCallbackReceipt => {
                self.deps
                    .stored_result_repository
                    .save_handoff_callback_receipt_result(stored_result, uow)?;
                self.deps
                    .stored_result_repository
                    .save_handoff_callback_receipt(
                        IdentityConsumerReceiptEnvelope::handoff_callback_receipt(
                            context.context_ref.clone(),
                            surface_marker_ref,
                            replay_receipt,
                            recorded_at,
                        ),
                        uow,
                    )?;
            }
            _ => unreachable!("guarded above"),
        }

        self.deps
            .idempotency_repository
            .complete_with_stored_result(
                reserved.value,
                stored_result_ref.clone(),
                recorded_at,
                reserved.version,
                uow,
            )?;

        Ok(stored_result_ref)
    }

    fn replay_receipt(
        &self,
        stored_result_ref: IdentityStoredResultRef,
        expected_kind: IdentityStoredResultKind,
    ) -> Result<IdentityConsumerReceipt, ApplicationError> {
        let stored_shell = self
            .deps
            .stored_result_repository
            .get_stored_result(stored_result_ref.clone())?
            .ok_or_else(|| {
                Self::duplicate_replay_consistency_defect(
                    "stored receipt shell is missing for duplicate replay",
                )
            })?;
        if stored_shell.result_kind != expected_kind {
            return Err(Self::duplicate_replay_consistency_defect(format!(
                "stored receipt shell kind mismatch: expected {expected_kind:?}, found {:?}",
                stored_shell.result_kind
            )));
        }

        let envelope = match expected_kind {
            IdentityStoredResultKind::ConsumerReceipt => self
                .deps
                .stored_result_repository
                .get_consumer_receipt(stored_result_ref.clone())?,
            IdentityStoredResultKind::HandoffCallbackReceipt => self
                .deps
                .stored_result_repository
                .get_handoff_callback_receipt(stored_result_ref.clone())?,
            other => {
                return Err(Self::duplicate_replay_consistency_defect(format!(
                    "unsupported replay receipt kind: {other:?}"
                )));
            }
        }
        .ok_or_else(|| {
            Self::duplicate_replay_consistency_defect(
                "typed receipt envelope is missing for duplicate replay",
            )
        })?;

        if envelope.result_kind != expected_kind {
            return Err(Self::duplicate_replay_consistency_defect(format!(
                "typed receipt envelope kind mismatch: expected {expected_kind:?}, found {:?}",
                envelope.result_kind
            )));
        }
        if envelope.stored_result_ref != stored_shell.stored_result_ref {
            return Err(Self::duplicate_replay_consistency_defect(
                "typed receipt envelope stored result ref does not match the generic shell",
            ));
        }
        if envelope.operation_context_ref != stored_shell.operation_context_ref {
            return Err(Self::duplicate_replay_consistency_defect(
                "typed receipt envelope operation context does not match the generic shell",
            ));
        }
        if envelope.surface_marker_ref != stored_shell.surface_marker_ref {
            return Err(Self::duplicate_replay_consistency_defect(
                "typed receipt envelope surface marker does not match the generic shell",
            ));
        }
        if envelope.receipt.stored_result_ref != stored_shell.stored_result_ref {
            return Err(Self::duplicate_replay_consistency_defect(
                "public receipt stored result ref does not match the generic shell",
            ));
        }

        Ok(envelope.receipt)
    }

    fn assert_context_channel(
        context: &IdentityOperationContext,
        expected_channel: IdentityOperationChannel,
    ) -> Result<(), ApplicationError> {
        if context.channel != expected_channel {
            return Err(ApplicationError::invalid_request(format!(
                "consumer context must use the {expected_channel:?} channel"
            )));
        }
        Ok(())
    }

    fn assert_context_operation_name(
        consumer_name: &str,
        context: &IdentityOperationContext,
    ) -> Result<(), ApplicationError> {
        if context.operation_name.as_str() != consumer_name {
            return Err(ApplicationError::invalid_request(format!(
                "operation name {} does not match consumer {}",
                context.operation_name.as_str(),
                consumer_name,
            )));
        }
        Ok(())
    }

    fn assert_context_idempotency<T>(
        envelope: &IdentityInboundEventEnvelope<T>,
        context: &IdentityOperationContext,
    ) -> Result<(), ApplicationError> {
        let Some(idempotency_key) = context.idempotency_key.as_ref() else {
            return Err(ApplicationError::invalid_request(
                "consumer context must carry an idempotency key",
            ));
        };
        if idempotency_key.as_public() != &envelope.idempotency_key {
            return Err(ApplicationError::invalid_request(
                "consumer context idempotency key does not match the public envelope metadata",
            ));
        }
        Ok(())
    }

    fn assert_context_source_event<T>(
        envelope: &IdentityInboundEventEnvelope<T>,
        context: &IdentityOperationContext,
    ) -> Result<(), ApplicationError> {
        let Some(source_event_ref) = context.source_event_ref.as_ref() else {
            return Err(ApplicationError::invalid_request(
                "consumer context must carry a source event reference",
            ));
        };
        if source_event_ref != &envelope.source_event_ref {
            return Err(ApplicationError::invalid_request(
                "consumer context source event ref does not match the public envelope",
            ));
        }
        Ok(())
    }

    fn duplicate_replay_consistency_defect(message: impl Into<String>) -> ApplicationError {
        ApplicationError::new(
            ApplicationErrorKind::DuplicateReplayConsistencyDefect,
            message,
        )
    }

    fn maintenance_scope_ref(token: &str) -> identity_contracts::refs::MaintenanceScopeRef {
        identity_contracts::refs::MaintenanceScopeRef::new(
            IdentitySourceRef::new(
                identity_contracts::refs::IdentitySourceOwner::Identity,
                identity_contracts::refs::ExternalSourceRef::new(token.to_owned())
                    .expect("valid maintenance scope token"),
            )
            .expect("valid maintenance scope ref"),
        )
    }

    fn rollback_quietly(&self, uow: Box<dyn IdentityUnitOfWork>) {
        let _ = self.deps.unit_of_work_manager.rollback(uow);
    }
}

#[cfg(test)]
mod tests {
    use core_contracts::actor::{ActorKind, ActorRef};

    use super::IdentityConsumerService;
    use crate::errors::ApplicationErrorKind;
    use crate::support::{
        IdentityIdempotencyKey, IdentityOperationContext, IdentityOperationContextRef,
        IdentityOperationName, IdentityRequestMetadataRef,
    };
    use identity_contracts::events::{
        IdentityConsumerOutcome, IdentityConsumerReceipt, IdentityInboundEventEnvelope,
    };
    use identity_contracts::protocol::{
        IdentityDigestAlgorithmMarkerRef, IdentityInboundConsumerName,
        IdentityProtocolSchemaVersionRef,
    };
    use identity_contracts::refs::{
        IdentityConsumerBindingRef, IdentityEventEnvelopeMarkerRef, IdentityRequestDigestValue,
        IdentitySourceEventRef, IdentityStoredResultRef, IdentityTimestamp,
    };

    fn actor_ref() -> ActorRef {
        ActorRef::new("worker-actor", ActorKind::System)
    }

    fn request_digest(token: &str) -> crate::support::IdentityRequestDigest {
        crate::support::IdentityRequestDigest::from_canonical_marker(
            identity_contracts::refs::IdentityCanonicalRequestMarkerRef::new(format!(
                "canonical-{token}"
            )),
            IdentityRequestDigestValue::new(format!("digest-{token}")),
            IdentityProtocolSchemaVersionRef::new("identity.consumer.v1"),
            IdentityDigestAlgorithmMarkerRef::new("sha256-v1"),
        )
    }

    fn envelope(token: &str) -> IdentityInboundEventEnvelope<()> {
        IdentityInboundEventEnvelope {
            consumer_name: IdentityInboundConsumerName::new("HandleRoleCapabilitySourceChanged"),
            envelope_marker_ref: IdentityEventEnvelopeMarkerRef::new(format!("envelope-{token}")),
            consumer_binding_ref: IdentityConsumerBindingRef::new(format!("binding-{token}")),
            source_event_ref: IdentitySourceEventRef::new(format!("source-event-{token}")),
            idempotency_key: format!("idem-{token}").into(),
            schema_version_ref: IdentityProtocolSchemaVersionRef::new("identity.consumer.v1"),
            occurred_at: None,
            received_at: IdentityTimestamp::from_clock(1).expect("timestamp"),
            trace_context_ref: None,
            payload: (),
        }
    }

    fn inbound_context(token: &str) -> IdentityOperationContext {
        IdentityOperationContext::from_inbound_event(
            IdentityOperationContextRef::new(format!("context-{token}")),
            IdentityOperationName::new("HandleRoleCapabilitySourceChanged"),
            actor_ref(),
            IdentityRequestMetadataRef::new(format!("metadata-{token}")),
            IdentityIdempotencyKey::new(format!("idem-{token}")),
            request_digest(token),
            None,
            IdentitySourceEventRef::new(format!("source-event-{token}")),
            IdentityTimestamp::from_clock(1).expect("timestamp"),
        )
    }

    fn callback_context(token: &str) -> IdentityOperationContext {
        IdentityOperationContext::from_handoff_callback(
            IdentityOperationContextRef::new(format!("context-{token}")),
            IdentityOperationName::new("HandleArchiveHandoffResult"),
            actor_ref(),
            IdentityRequestMetadataRef::new(format!("metadata-{token}")),
            IdentityIdempotencyKey::new(format!("idem-{token}")),
            request_digest(token),
            None,
            IdentitySourceEventRef::new(format!("source-event-{token}")),
            IdentityTimestamp::from_clock(1).expect("timestamp"),
        )
    }

    #[test]
    fn inbound_context_must_match_public_envelope() {
        let valid_envelope = envelope("ok");
        let valid_context = inbound_context("ok");
        IdentityConsumerService::assert_inbound_context(&valid_envelope, &valid_context)
            .expect("context should match");

        let mismatched = IdentityOperationContext::from_inbound_event(
            IdentityOperationContextRef::new("context-mismatch"),
            IdentityOperationName::new("HandleMemoryReferenceSourceStateChanged"),
            actor_ref(),
            IdentityRequestMetadataRef::new("metadata-mismatch"),
            IdentityIdempotencyKey::new("idem-ok"),
            request_digest("ok"),
            None,
            IdentitySourceEventRef::new("source-event-ok"),
            IdentityTimestamp::from_clock(1).expect("timestamp"),
        );
        let error = IdentityConsumerService::assert_inbound_context(&valid_envelope, &mismatched)
            .expect_err("operation name mismatch must fail");
        assert_eq!(error.kind, ApplicationErrorKind::InvalidRequest);
    }

    #[test]
    fn callback_context_must_match_public_envelope() {
        let valid_envelope = IdentityInboundEventEnvelope {
            consumer_name: IdentityInboundConsumerName::new("HandleArchiveHandoffResult"),
            ..envelope("callback")
        };
        let valid_context = callback_context("callback");
        IdentityConsumerService::assert_callback_context(&valid_envelope, &valid_context)
            .expect("callback context should match");

        let invalid_context = inbound_context("callback");
        let error =
            IdentityConsumerService::assert_callback_context(&valid_envelope, &invalid_context)
                .expect_err("channel mismatch must fail");
        assert_eq!(error.kind, ApplicationErrorKind::InvalidRequest);
    }

    #[test]
    fn duplicate_replay_receipt_only_changes_outcome() {
        let base = IdentityConsumerReceipt {
            receipt_ref: identity_contracts::refs::IdentityConsumerReceiptRef::new("receipt-1"),
            consumer_name: IdentityInboundConsumerName::new("HandleTraceHandoffResult"),
            outcome: IdentityConsumerOutcome::UnsupportedVersion,
            stored_result_ref: IdentityStoredResultRef::new("stored-result-1"),
            trace_refs: vec![identity_contracts::refs::IdentityTraceRecordRef::new(
                "trace-1",
            )],
            outbox_refs: vec![identity_contracts::refs::IdentityOutboxRecordRef::new(
                "outbox-1",
            )],
            issue_refs: vec![
                identity_contracts::metadata::IdentityProtocolValidationIssueRef::new(
                    "unsupported-schema",
                ),
            ],
        };

        let replay = IdentityConsumerService::duplicate_replay_receipt(&base);
        assert_eq!(replay.outcome, IdentityConsumerOutcome::DuplicateReplayed);
        assert_eq!(replay.receipt_ref, base.receipt_ref);
        assert_eq!(replay.stored_result_ref, base.stored_result_ref);
        assert_eq!(replay.trace_refs, base.trace_refs);
        assert_eq!(replay.outbox_refs, base.outbox_refs);
        assert_eq!(replay.issue_refs, base.issue_refs);
    }
}
