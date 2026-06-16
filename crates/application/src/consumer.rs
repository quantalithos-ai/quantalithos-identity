//! Shared consumer/callback scaffold and typed receipt replay helpers.

use identity_contracts::events::{
    IdentityConsumerOutcome, IdentityConsumerReceipt, IdentityInboundEventEnvelope,
};
use identity_contracts::metadata::IdentityProtocolValidationIssueRef;
use identity_contracts::protocol::IdentityProtocolSchemaVersionRef;
use identity_contracts::refs::{
    IdentityOperationChannel, IdentityOutboxRecordRef, IdentityStoredResultRef,
    IdentityTraceRecordRef,
};

use crate::errors::{ApplicationError, ApplicationErrorKind};
use crate::ports::{
    IdentityClockPort, IdentityIdGeneratorPort, IdentityIdempotencyRepository,
    IdentityOperationContextFactoryPort, IdentityStoredResultRepository, IdentityUnitOfWork,
    IdentityUnitOfWorkManagerPort,
};
use crate::support::{
    IdempotencyReserveOutcome, IdentityConsumerReceiptEnvelope, IdentityIdempotencyRecord,
    IdentityOperationContext, IdentityStoredResultKind, StoredIdentityOperationResult, Versioned,
};

/// Shared dependencies for consumer/callback scaffold flows.
pub struct IdentityConsumerServiceDeps<'a> {
    /// Consumer/callback write transaction manager.
    pub unit_of_work_manager: &'a dyn IdentityUnitOfWorkManagerPort,
    /// Trusted clock used by receipt persistence and replay decisions.
    pub clock: &'a dyn IdentityClockPort,
    /// Stable id and marker generator.
    pub id_generator: &'a dyn IdentityIdGeneratorPort,
    /// Entry metadata to operation-context builder.
    pub operation_context_factory: &'a dyn IdentityOperationContextFactoryPort,
    /// Duplicate replay reserve and completion repository.
    pub idempotency_repository: &'a dyn IdentityIdempotencyRepository,
    /// Stored replay shell and typed receipt envelope repository.
    pub stored_result_repository: &'a dyn IdentityStoredResultRepository,
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
