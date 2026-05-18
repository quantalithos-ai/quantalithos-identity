//! Operations jobs that act on already-persisted facts such as outbox rows and projections.

use time::{OffsetDateTime, PrimitiveDateTime};

use crate::application::career_event::CareerEventOutcome;
use crate::application::memory_refs::MemoryArchiveEventOutcome;
use crate::application::persistence::{
    AuditTraceRepository, IdempotencyStore, InboundDeadLetterStore,
    MemberSummaryProjectionRepository, OutboxStore, ProjectionCheckpointRepository,
    RoleCatalogRepository, UnitOfWork, UnitOfWorkFactory,
};
use crate::application::role_catalog_sync::RoleCatalogSyncOutcome;
use crate::application::tombstone_flow::GateDecisionOutcome;
use crate::domain::audit::{AuditResult, AuditTraceEntry};
use crate::domain::dead_letter::InboundDeadLetter;
use crate::domain::idempotency::{IdempotencyScope, IdempotencyStatus};
use crate::domain::outbox::OutboxEvent;
use crate::domain::projection::{MemberSummaryProjection, ProjectionCheckpoint};
use crate::domain::shared::ids::{DeadLetterId, OutboxEventId};
use crate::error::IdentityError;
use crate::inbound::events::{
    InboundEventEnvelope, InboundGateDecisionEvent, InboundMemoryArchiveEvent,
    InboundProcessFactEvent, InboundRoleCatalogEvent, InboundWorkFactEvent, RoleDefinitionSnapshot,
};
use crate::outbound::{BusPublisherPort, MethodLibraryRoleCatalogPort};

/// Summary returned after one publisher pass over the pending outbox batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PublishOutboxEventsSummary {
    /// Number of outbox rows that were selected for the current pass.
    pub scanned: usize,
    /// Number of rows successfully handed off to the external bus.
    pub published: usize,
    /// Number of rows marked failed and scheduled for retry.
    pub failed: usize,
}

/// Summary returned after one projection rebuild pass over the outbox stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RebuildMemberSummaryProjectionSummary {
    /// Number of outbox rows scanned after the checkpoint cursor.
    pub scanned: usize,
    /// Number of rows that produced an upserted member summary projection.
    pub rebuilt: usize,
    /// Number of rows that were known but irrelevant to the member summary projection.
    pub skipped: usize,
}

/// Result returned after resetting one projection checkpoint for a fresh rebuild pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResetProjectionCheckpointResult {
    /// Name of the checkpoint that was reset.
    pub checkpoint_name: String,
}

/// Result returned after clearing one abnormal idempotency record for later retry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClearIdempotencyRecordResult {
    /// Stable idempotency key that was cleared.
    pub idempotency_key: String,
    /// Scope associated with the cleared idempotency key.
    pub scope: IdempotencyScope,
}

/// Result returned after rebuilding one member summary projection row from persisted facts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RebuildMemberProjectionResult {
    /// Member whose projection row was rebuilt.
    pub global_member_id: String,
    /// Number of outbox facts replayed for the rebuilt member projection.
    pub replayed_events: usize,
}

/// Result returned after replaying one dead outbox row manually.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayDeadOutboxEventResult {
    /// Stable outbox event identifier that was replayed.
    pub outbox_event_id: String,
}

/// Summary returned after one replay pass over pending inbound dead letters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReplayInboundDeadLettersSummary {
    /// Number of dead-letter rows that were selected for the current pass.
    pub scanned: usize,
    /// Number of rows successfully replayed and marked replayed.
    pub replayed: usize,
    /// Number of rows that still require later replay or manual review.
    pub still_pending: usize,
}

/// Result returned after one manual dead-letter ignore operation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IgnoreInboundDeadLetterResult {
    /// Identifier of the dead-letter row that was ignored.
    pub dead_letter_id: String,
}

/// Result returned after replaying one inbound dead-letter row manually.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplayInboundDeadLetterResult {
    /// Identifier of the dead-letter row that was replayed successfully.
    pub dead_letter_id: String,
}

/// Summary returned after one role-catalog reconciliation pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconcileRoleCatalogSummary {
    /// Number of authoritative source roles scanned during the pass.
    pub scanned: usize,
    /// Number of local rows inserted because the source role was missing locally.
    pub missing: usize,
    /// Number of local rows refreshed from matching-fingerprint source snapshots.
    pub refreshed: usize,
    /// Number of local rows updated to `deprecated`.
    pub deprecated: usize,
    /// Number of local rows marked `source_drift`.
    pub drifted: usize,
    /// Number of local rows that were not present in the authoritative source snapshot.
    pub local_only: usize,
    /// Number of local rows that already matched the source snapshot.
    pub unchanged: usize,
}

/// Replays one dead-lettered role-catalog event through the application-service boundary.
pub trait RoleCatalogDeadLetterReplayPort {
    /// Replays one stored role-catalog dead letter.
    fn replay_dead_letter(
        &self,
        dead_letter_id: DeadLetterId,
        created_at: PrimitiveDateTime,
        event: InboundRoleCatalogEvent,
    ) -> impl std::future::Future<Output = Result<RoleCatalogSyncOutcome, IdentityError>>;
}

/// Replays one dead-lettered career event through the application-service boundary.
pub trait CareerDeadLetterReplayPort {
    /// Replays one stored work dead letter.
    fn replay_work_dead_letter(
        &self,
        dead_letter_id: DeadLetterId,
        created_at: PrimitiveDateTime,
        event: InboundWorkFactEvent,
    ) -> impl std::future::Future<Output = Result<CareerEventOutcome, IdentityError>>;

    /// Replays one stored process dead letter.
    fn replay_process_dead_letter(
        &self,
        dead_letter_id: DeadLetterId,
        created_at: PrimitiveDateTime,
        event: InboundProcessFactEvent,
    ) -> impl std::future::Future<Output = Result<CareerEventOutcome, IdentityError>>;
}

/// Replays one dead-lettered gate-decision event through the application-service boundary.
pub trait GateDecisionDeadLetterReplayPort {
    /// Replays one stored gate-decision dead letter.
    fn replay_dead_letter(
        &self,
        dead_letter_id: DeadLetterId,
        created_at: PrimitiveDateTime,
        event: InboundGateDecisionEvent,
    ) -> impl std::future::Future<Output = Result<GateDecisionOutcome, IdentityError>>;
}

/// Replays one dead-lettered memory/archive event through the application-service boundary.
pub trait MemoryArchiveDeadLetterReplayPort {
    /// Replays one stored memory/archive dead letter.
    fn replay_dead_letter(
        &self,
        dead_letter_id: DeadLetterId,
        created_at: PrimitiveDateTime,
        event: InboundMemoryArchiveEvent,
    ) -> impl std::future::Future<Output = Result<MemoryArchiveEventOutcome, IdentityError>>;
}

/// Replays pending inbound dead letters through the same application services used in normal flow.
#[derive(Debug, Clone)]
pub struct InboundDeadLetterReplayJob<
    UowFactory,
    RoleCatalogReplayer,
    CareerReplayer,
    MemoryArchiveReplayer,
    GateReplayer,
> {
    unit_of_work_factory: UowFactory,
    role_catalog_replayer: RoleCatalogReplayer,
    career_replayer: CareerReplayer,
    memory_archive_replayer: MemoryArchiveReplayer,
    gate_replayer: GateReplayer,
}

impl<UowFactory, RoleCatalogReplayer, CareerReplayer, MemoryArchiveReplayer, GateReplayer>
    InboundDeadLetterReplayJob<
        UowFactory,
        RoleCatalogReplayer,
        CareerReplayer,
        MemoryArchiveReplayer,
        GateReplayer,
    >
{
    /// Creates a new inbound dead-letter replay job bound to the provided handlers.
    pub fn new(
        unit_of_work_factory: UowFactory,
        role_catalog_replayer: RoleCatalogReplayer,
        career_replayer: CareerReplayer,
        memory_archive_replayer: MemoryArchiveReplayer,
        gate_replayer: GateReplayer,
    ) -> Self {
        Self {
            unit_of_work_factory,
            role_catalog_replayer,
            career_replayer,
            memory_archive_replayer,
            gate_replayer,
        }
    }

    /// Returns a stable operations name for diagnostics and tests.
    pub fn operation_name(&self) -> &'static str {
        "ReplayInboundDeadLetters"
    }
}

/// Marks one inbound dead-letter row as intentionally ignored after manual review.
#[derive(Debug, Clone)]
pub struct IgnoreInboundDeadLetterJob<UowFactory> {
    unit_of_work_factory: UowFactory,
}

impl<UowFactory> IgnoreInboundDeadLetterJob<UowFactory> {
    /// Creates a new manual dead-letter ignore job.
    pub fn new(unit_of_work_factory: UowFactory) -> Self {
        Self {
            unit_of_work_factory,
        }
    }

    /// Returns a stable operations name for diagnostics and tests.
    pub fn operation_name(&self) -> &'static str {
        "IgnoreInboundDeadLetter"
    }
}

impl<UowFactory> IgnoreInboundDeadLetterJob<UowFactory>
where
    UowFactory: UnitOfWorkFactory,
{
    /// Marks one pending inbound dead-letter row as ignored with an explicit operator reason.
    pub async fn ignore_inbound_dead_letter(
        &self,
        dead_letter_id: &DeadLetterId,
        reason: &str,
    ) -> Result<IgnoreInboundDeadLetterResult, IdentityError> {
        if reason.trim().is_empty() {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_INVALID_ARGUMENT",
                message: "ignore reason must not be blank".to_string(),
            });
        }

        let mut uow = self.unit_of_work_factory.begin().await?;
        let mut dead_letter = uow
            .inbound_dead_letters()
            .get(dead_letter_id)
            .await?
            .ok_or_else(|| IdentityError::RuleViolation {
                code: "IDENTITY_DEAD_LETTER_NOT_FOUND",
                message: format!(
                    "inbound dead letter `{}` was not found",
                    dead_letter_id.as_str()
                ),
            })?;

        if dead_letter.replay_status != crate::domain::dead_letter::DeadLetterReplayStatus::Pending
        {
            uow.rollback().await?;
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_DEAD_LETTER_NOT_PENDING",
                message: format!(
                    "inbound dead letter `{}` is not pending replay or review",
                    dead_letter_id.as_str()
                ),
            });
        }

        dead_letter.mark_ignored(reason.trim());
        uow.inbound_dead_letters().save(&dead_letter).await?;
        uow.commit().await?;

        Ok(IgnoreInboundDeadLetterResult {
            dead_letter_id: dead_letter_id.as_str().to_string(),
        })
    }
}

/// Replays one pending inbound dead-letter row manually through the normal application boundary.
#[derive(Debug, Clone)]
pub struct ReplayInboundDeadLetterJob<
    UowFactory,
    RoleCatalogReplayer,
    CareerReplayer,
    MemoryArchiveReplayer,
    GateReplayer,
> {
    unit_of_work_factory: UowFactory,
    role_catalog_replayer: RoleCatalogReplayer,
    career_replayer: CareerReplayer,
    memory_archive_replayer: MemoryArchiveReplayer,
    gate_replayer: GateReplayer,
}

impl<UowFactory, RoleCatalogReplayer, CareerReplayer, MemoryArchiveReplayer, GateReplayer>
    ReplayInboundDeadLetterJob<
        UowFactory,
        RoleCatalogReplayer,
        CareerReplayer,
        MemoryArchiveReplayer,
        GateReplayer,
    >
{
    /// Creates a new single dead-letter replay job bound to the provided handlers.
    pub fn new(
        unit_of_work_factory: UowFactory,
        role_catalog_replayer: RoleCatalogReplayer,
        career_replayer: CareerReplayer,
        memory_archive_replayer: MemoryArchiveReplayer,
        gate_replayer: GateReplayer,
    ) -> Self {
        Self {
            unit_of_work_factory,
            role_catalog_replayer,
            career_replayer,
            memory_archive_replayer,
            gate_replayer,
        }
    }

    /// Returns a stable operations name for diagnostics and tests.
    pub fn operation_name(&self) -> &'static str {
        "ReplayInboundDeadLetter"
    }
}

impl<UowFactory, RoleCatalogReplayer, CareerReplayer, MemoryArchiveReplayer, GateReplayer>
    ReplayInboundDeadLetterJob<
        UowFactory,
        RoleCatalogReplayer,
        CareerReplayer,
        MemoryArchiveReplayer,
        GateReplayer,
    >
where
    UowFactory: UnitOfWorkFactory + Clone,
    RoleCatalogReplayer: RoleCatalogDeadLetterReplayPort,
    CareerReplayer: CareerDeadLetterReplayPort,
    MemoryArchiveReplayer: MemoryArchiveDeadLetterReplayPort,
    GateReplayer: GateDecisionDeadLetterReplayPort,
{
    /// Replays one pending inbound dead-letter row manually.
    pub async fn replay_inbound_dead_letter(
        &self,
        dead_letter_id: &DeadLetterId,
    ) -> Result<ReplayInboundDeadLetterResult, IdentityError> {
        let mut uow = self.unit_of_work_factory.begin().await?;
        let dead_letter = uow
            .inbound_dead_letters()
            .get(dead_letter_id)
            .await?
            .ok_or_else(|| IdentityError::RuleViolation {
                code: "IDENTITY_DEAD_LETTER_NOT_FOUND",
                message: format!(
                    "inbound dead letter `{}` was not found",
                    dead_letter_id.as_str()
                ),
            })?;
        uow.rollback().await?;

        if dead_letter.replay_status != crate::domain::dead_letter::DeadLetterReplayStatus::Pending
        {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_DEAD_LETTER_NOT_PENDING",
                message: format!(
                    "inbound dead letter `{}` is not pending replay or review",
                    dead_letter_id.as_str()
                ),
            });
        }

        let replayed = self.replay_dead_letter(dead_letter).await?;
        if !replayed {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_DEAD_LETTER_REPLAY_FAILED",
                message: format!(
                    "inbound dead letter `{}` replay did not complete successfully",
                    dead_letter_id.as_str()
                ),
            });
        }

        Ok(ReplayInboundDeadLetterResult {
            dead_letter_id: dead_letter_id.as_str().to_string(),
        })
    }

    async fn replay_dead_letter(
        &self,
        mut dead_letter: InboundDeadLetter,
    ) -> Result<bool, IdentityError> {
        let disposition = self.dispatch_dead_letter(&dead_letter).await;
        match disposition {
            Ok(DeadLetterReplayDisposition::Replayed) => dead_letter.mark_replayed(),
            Ok(DeadLetterReplayDisposition::StillPending(reason)) => {
                dead_letter.refresh_failure_reason(reason);
            }
            Err(error) => dead_letter.refresh_failure_reason(error.to_string()),
        }

        let replayed = dead_letter.replay_status
            == crate::domain::dead_letter::DeadLetterReplayStatus::Replayed;
        let mut uow = self.unit_of_work_factory.begin().await?;
        uow.inbound_dead_letters().save(&dead_letter).await?;
        uow.commit().await?;

        Ok(replayed)
    }

    async fn dispatch_dead_letter(
        &self,
        dead_letter: &InboundDeadLetter,
    ) -> Result<DeadLetterReplayDisposition, IdentityError> {
        let source_event_id = match dead_letter.source_event_id.clone() {
            Some(source_event_id) => source_event_id,
            None => {
                return Ok(DeadLetterReplayDisposition::StillPending(
                    "automatic replay requires source_event_id".to_string(),
                ));
            }
        };

        let envelope = InboundEventEnvelope {
            source_event_id,
            source_module: dead_letter.source_module.clone(),
            event_type: dead_letter.event_type.clone(),
            occurred_at: dead_letter.created_at,
            payload_hash: replay_payload_hash(&dead_letter.payload_json),
            payload: dead_letter.payload_json.clone(),
        };

        match dead_letter.source_module.as_str() {
            "method-library" => {
                let outcome = self
                    .role_catalog_replayer
                    .replay_dead_letter(
                        dead_letter.dead_letter_id.clone(),
                        dead_letter.created_at,
                        InboundRoleCatalogEvent { envelope },
                    )
                    .await?;
                Ok(match outcome {
                    RoleCatalogSyncOutcome::Synced { .. }
                    | RoleCatalogSyncOutcome::SkippedDuplicate { .. } => {
                        DeadLetterReplayDisposition::Replayed
                    }
                    RoleCatalogSyncOutcome::DeadLettered => {
                        DeadLetterReplayDisposition::StillPending(
                            "replay still failed and remained dead-lettered".to_string(),
                        )
                    }
                })
            }
            "work" => {
                let outcome = self
                    .career_replayer
                    .replay_work_dead_letter(
                        dead_letter.dead_letter_id.clone(),
                        dead_letter.created_at,
                        InboundWorkFactEvent { envelope },
                    )
                    .await?;
                Ok(match outcome {
                    CareerEventOutcome::Appended { .. }
                    | CareerEventOutcome::SkippedDuplicate { .. } => {
                        DeadLetterReplayDisposition::Replayed
                    }
                    CareerEventOutcome::DeadLettered => DeadLetterReplayDisposition::StillPending(
                        "replay still failed and remained dead-lettered".to_string(),
                    ),
                })
            }
            "process" => {
                let outcome = self
                    .career_replayer
                    .replay_process_dead_letter(
                        dead_letter.dead_letter_id.clone(),
                        dead_letter.created_at,
                        InboundProcessFactEvent { envelope },
                    )
                    .await?;
                Ok(match outcome {
                    CareerEventOutcome::Appended { .. }
                    | CareerEventOutcome::SkippedDuplicate { .. } => {
                        DeadLetterReplayDisposition::Replayed
                    }
                    CareerEventOutcome::DeadLettered => DeadLetterReplayDisposition::StillPending(
                        "replay still failed and remained dead-lettered".to_string(),
                    ),
                })
            }
            "governance" => {
                let outcome = self
                    .gate_replayer
                    .replay_dead_letter(
                        dead_letter.dead_letter_id.clone(),
                        dead_letter.created_at,
                        InboundGateDecisionEvent { envelope },
                    )
                    .await?;
                Ok(match outcome {
                    GateDecisionOutcome::Recorded { .. }
                    | GateDecisionOutcome::SkippedDuplicate { .. }
                    | GateDecisionOutcome::SkippedNoPendingFlow { .. } => {
                        DeadLetterReplayDisposition::Replayed
                    }
                    GateDecisionOutcome::DeadLettered => DeadLetterReplayDisposition::StillPending(
                        "replay still failed and remained dead-lettered".to_string(),
                    ),
                })
            }
            "memory-archive" => {
                let outcome = self
                    .memory_archive_replayer
                    .replay_dead_letter(
                        dead_letter.dead_letter_id.clone(),
                        dead_letter.created_at,
                        InboundMemoryArchiveEvent { envelope },
                    )
                    .await?;
                Ok(match outcome {
                    MemoryArchiveEventOutcome::Updated { .. }
                    | MemoryArchiveEventOutcome::SkippedDuplicate { .. } => {
                        DeadLetterReplayDisposition::Replayed
                    }
                    MemoryArchiveEventOutcome::DeadLettered => {
                        DeadLetterReplayDisposition::StillPending(
                            "replay still failed and remained dead-lettered".to_string(),
                        )
                    }
                })
            }
            other => Ok(DeadLetterReplayDisposition::StillPending(format!(
                "automatic replay is not supported for source_module `{other}`"
            ))),
        }
    }
}

impl<UowFactory, RoleCatalogReplayer, CareerReplayer, MemoryArchiveReplayer, GateReplayer>
    InboundDeadLetterReplayJob<
        UowFactory,
        RoleCatalogReplayer,
        CareerReplayer,
        MemoryArchiveReplayer,
        GateReplayer,
    >
where
    UowFactory: UnitOfWorkFactory,
    RoleCatalogReplayer: RoleCatalogDeadLetterReplayPort,
    CareerReplayer: CareerDeadLetterReplayPort,
    MemoryArchiveReplayer: MemoryArchiveDeadLetterReplayPort,
    GateReplayer: GateDecisionDeadLetterReplayPort,
{
    /// Replays one batch of pending inbound dead letters.
    pub async fn replay_inbound_dead_letters(
        &self,
        batch_size: usize,
    ) -> Result<ReplayInboundDeadLettersSummary, IdentityError> {
        let mut uow = self.unit_of_work_factory.begin().await?;
        let dead_letters = uow.inbound_dead_letters().list_pending(batch_size).await?;
        uow.rollback().await?;

        let mut summary = ReplayInboundDeadLettersSummary {
            scanned: dead_letters.len(),
            replayed: 0,
            still_pending: 0,
        };

        for dead_letter in dead_letters {
            match self.replay_dead_letter(dead_letter).await? {
                true => summary.replayed += 1,
                false => summary.still_pending += 1,
            }
        }

        Ok(summary)
    }

    async fn replay_dead_letter(
        &self,
        mut dead_letter: InboundDeadLetter,
    ) -> Result<bool, IdentityError> {
        let disposition = self.dispatch_dead_letter(&dead_letter).await;
        match disposition {
            Ok(DeadLetterReplayDisposition::Replayed) => dead_letter.mark_replayed(),
            Ok(DeadLetterReplayDisposition::StillPending(reason)) => {
                dead_letter.refresh_failure_reason(reason);
            }
            Err(error) => dead_letter.refresh_failure_reason(error.to_string()),
        }

        let replayed = dead_letter.replay_status
            == crate::domain::dead_letter::DeadLetterReplayStatus::Replayed;
        let mut uow = self.unit_of_work_factory.begin().await?;
        uow.inbound_dead_letters().save(&dead_letter).await?;
        uow.commit().await?;

        Ok(replayed)
    }

    async fn dispatch_dead_letter(
        &self,
        dead_letter: &InboundDeadLetter,
    ) -> Result<DeadLetterReplayDisposition, IdentityError> {
        let source_event_id = match dead_letter.source_event_id.clone() {
            Some(source_event_id) => source_event_id,
            None => {
                return Ok(DeadLetterReplayDisposition::StillPending(
                    "automatic replay requires source_event_id".to_string(),
                ));
            }
        };

        let envelope = InboundEventEnvelope {
            source_event_id,
            source_module: dead_letter.source_module.clone(),
            event_type: dead_letter.event_type.clone(),
            occurred_at: dead_letter.created_at,
            payload_hash: replay_payload_hash(&dead_letter.payload_json),
            payload: dead_letter.payload_json.clone(),
        };

        match dead_letter.source_module.as_str() {
            "method-library" => {
                let outcome = self
                    .role_catalog_replayer
                    .replay_dead_letter(
                        dead_letter.dead_letter_id.clone(),
                        dead_letter.created_at,
                        InboundRoleCatalogEvent { envelope },
                    )
                    .await?;
                Ok(match outcome {
                    RoleCatalogSyncOutcome::Synced { .. }
                    | RoleCatalogSyncOutcome::SkippedDuplicate { .. } => {
                        DeadLetterReplayDisposition::Replayed
                    }
                    RoleCatalogSyncOutcome::DeadLettered => {
                        DeadLetterReplayDisposition::StillPending(
                            "replay still failed and remained dead-lettered".to_string(),
                        )
                    }
                })
            }
            "work" => {
                let outcome = self
                    .career_replayer
                    .replay_work_dead_letter(
                        dead_letter.dead_letter_id.clone(),
                        dead_letter.created_at,
                        InboundWorkFactEvent { envelope },
                    )
                    .await?;
                Ok(match outcome {
                    CareerEventOutcome::Appended { .. }
                    | CareerEventOutcome::SkippedDuplicate { .. } => {
                        DeadLetterReplayDisposition::Replayed
                    }
                    CareerEventOutcome::DeadLettered => DeadLetterReplayDisposition::StillPending(
                        "replay still failed and remained dead-lettered".to_string(),
                    ),
                })
            }
            "process" => {
                let outcome = self
                    .career_replayer
                    .replay_process_dead_letter(
                        dead_letter.dead_letter_id.clone(),
                        dead_letter.created_at,
                        InboundProcessFactEvent { envelope },
                    )
                    .await?;
                Ok(match outcome {
                    CareerEventOutcome::Appended { .. }
                    | CareerEventOutcome::SkippedDuplicate { .. } => {
                        DeadLetterReplayDisposition::Replayed
                    }
                    CareerEventOutcome::DeadLettered => DeadLetterReplayDisposition::StillPending(
                        "replay still failed and remained dead-lettered".to_string(),
                    ),
                })
            }
            "governance" => {
                let outcome = self
                    .gate_replayer
                    .replay_dead_letter(
                        dead_letter.dead_letter_id.clone(),
                        dead_letter.created_at,
                        InboundGateDecisionEvent { envelope },
                    )
                    .await?;
                Ok(match outcome {
                    GateDecisionOutcome::Recorded { .. }
                    | GateDecisionOutcome::SkippedDuplicate { .. }
                    | GateDecisionOutcome::SkippedNoPendingFlow { .. } => {
                        DeadLetterReplayDisposition::Replayed
                    }
                    GateDecisionOutcome::DeadLettered => DeadLetterReplayDisposition::StillPending(
                        "replay still failed and remained dead-lettered".to_string(),
                    ),
                })
            }
            "memory-archive" => {
                let outcome = self
                    .memory_archive_replayer
                    .replay_dead_letter(
                        dead_letter.dead_letter_id.clone(),
                        dead_letter.created_at,
                        InboundMemoryArchiveEvent { envelope },
                    )
                    .await?;
                Ok(match outcome {
                    MemoryArchiveEventOutcome::Updated { .. }
                    | MemoryArchiveEventOutcome::SkippedDuplicate { .. } => {
                        DeadLetterReplayDisposition::Replayed
                    }
                    MemoryArchiveEventOutcome::DeadLettered => {
                        DeadLetterReplayDisposition::StillPending(
                            "replay still failed and remained dead-lettered".to_string(),
                        )
                    }
                })
            }
            other => Ok(DeadLetterReplayDisposition::StillPending(format!(
                "automatic replay is not supported for source_module `{other}`"
            ))),
        }
    }
}

enum DeadLetterReplayDisposition {
    Replayed,
    StillPending(String),
}

impl<UowFactory> RoleCatalogDeadLetterReplayPort
    for crate::application::role_catalog_sync::RoleCatalogSyncService<UowFactory>
where
    UowFactory: UnitOfWorkFactory,
{
    async fn replay_dead_letter(
        &self,
        dead_letter_id: DeadLetterId,
        created_at: PrimitiveDateTime,
        event: InboundRoleCatalogEvent,
    ) -> Result<RoleCatalogSyncOutcome, IdentityError> {
        crate::application::role_catalog_sync::RoleCatalogSyncService::<UowFactory>::replay_dead_letter(
            self,
            dead_letter_id,
            created_at,
            event,
        )
        .await
    }
}

impl<UowFactory> CareerDeadLetterReplayPort
    for crate::application::career_event::CareerEventConsumerService<UowFactory>
where
    UowFactory: UnitOfWorkFactory,
{
    async fn replay_work_dead_letter(
        &self,
        dead_letter_id: DeadLetterId,
        created_at: PrimitiveDateTime,
        event: InboundWorkFactEvent,
    ) -> Result<CareerEventOutcome, IdentityError> {
        crate::application::career_event::CareerEventConsumerService::<UowFactory>::replay_work_dead_letter(
            self,
            dead_letter_id,
            created_at,
            event,
        )
        .await
    }

    async fn replay_process_dead_letter(
        &self,
        dead_letter_id: DeadLetterId,
        created_at: PrimitiveDateTime,
        event: InboundProcessFactEvent,
    ) -> Result<CareerEventOutcome, IdentityError> {
        crate::application::career_event::CareerEventConsumerService::<UowFactory>::replay_process_dead_letter(
            self,
            dead_letter_id,
            created_at,
            event,
        )
        .await
    }
}

impl<UowFactory, Governance, ArchiveRequester> GateDecisionDeadLetterReplayPort
    for crate::application::tombstone_flow::TombstoneFlowService<
        UowFactory,
        Governance,
        ArchiveRequester,
    >
where
    UowFactory: UnitOfWorkFactory,
    Governance: crate::outbound::GovernancePort,
    ArchiveRequester: crate::outbound::ArchiveRequestPort,
{
    async fn replay_dead_letter(
        &self,
        dead_letter_id: DeadLetterId,
        created_at: PrimitiveDateTime,
        event: InboundGateDecisionEvent,
    ) -> Result<GateDecisionOutcome, IdentityError> {
        crate::application::tombstone_flow::TombstoneFlowService::<
            UowFactory,
            Governance,
            ArchiveRequester,
        >::replay_dead_letter(self, dead_letter_id, created_at, event)
        .await
    }
}

impl<UowFactory, MemoryArchiveValidator> MemoryArchiveDeadLetterReplayPort
    for crate::application::memory_refs::MemoryRefsCommandService<
        UowFactory,
        MemoryArchiveValidator,
    >
where
    UowFactory: UnitOfWorkFactory,
    MemoryArchiveValidator: crate::outbound::MemoryArchivePort,
{
    async fn replay_dead_letter(
        &self,
        dead_letter_id: DeadLetterId,
        created_at: PrimitiveDateTime,
        event: InboundMemoryArchiveEvent,
    ) -> Result<MemoryArchiveEventOutcome, IdentityError> {
        crate::application::memory_refs::MemoryRefsCommandService::<
            UowFactory,
            MemoryArchiveValidator,
        >::replay_archive_dead_letter(self, dead_letter_id, created_at, event)
        .await
    }
}

/// Publishes already-persisted outbox rows to the external L0-bus.
#[derive(Debug, Clone)]
pub struct OutboxPublisherJob<UowFactory, BusPublisher> {
    unit_of_work_factory: UowFactory,
    bus_publisher: BusPublisher,
}

impl<UowFactory, BusPublisher> OutboxPublisherJob<UowFactory, BusPublisher> {
    /// Creates a new outbox publisher job bound to the provided persistence and bus ports.
    pub fn new(unit_of_work_factory: UowFactory, bus_publisher: BusPublisher) -> Self {
        Self {
            unit_of_work_factory,
            bus_publisher,
        }
    }

    /// Returns a stable operations name for diagnostics and tests.
    pub fn operation_name(&self) -> &'static str {
        "PublishOutboxEvents"
    }
}

impl<UowFactory, BusPublisher> OutboxPublisherJob<UowFactory, BusPublisher>
where
    UowFactory: UnitOfWorkFactory,
    BusPublisher: BusPublisherPort,
{
    /// Publishes one batch of pending outbox rows without modifying business write models.
    ///
    /// # Errors
    ///
    /// Returns an error only when persistence cannot load or save outbox rows. External bus
    /// publish failures are captured into outbox state and included in the returned summary.
    pub async fn publish_outbox_events(
        &self,
        batch_size: usize,
    ) -> Result<PublishOutboxEventsSummary, IdentityError> {
        let mut uow = self.unit_of_work_factory.begin().await?;
        let events = uow.outbox().list_pending(batch_size).await?;
        uow.rollback().await?;

        let mut summary = PublishOutboxEventsSummary {
            scanned: events.len(),
            published: 0,
            failed: 0,
        };

        for event in events {
            let publish_result = self.bus_publisher.publish(&event).await;
            let mut event_to_save = event.clone();
            let now = current_timestamp();

            match publish_result {
                Ok(()) => {
                    event_to_save.mark_published(now);
                    summary.published += 1;
                }
                Err(error) => {
                    event_to_save.mark_failed(error.to_string(), now);
                    summary.failed += 1;
                }
            }

            let mut uow = self.unit_of_work_factory.begin().await?;
            uow.outbox().save(&event_to_save).await?;
            uow.commit().await?;
        }

        Ok(summary)
    }
}

/// Replays one dead outbox row manually after operator review.
#[derive(Debug, Clone)]
pub struct ReplayDeadOutboxEventJob<UowFactory, BusPublisher> {
    unit_of_work_factory: UowFactory,
    bus_publisher: BusPublisher,
}

impl<UowFactory, BusPublisher> ReplayDeadOutboxEventJob<UowFactory, BusPublisher> {
    /// Creates a new manual dead-outbox replay job.
    pub fn new(unit_of_work_factory: UowFactory, bus_publisher: BusPublisher) -> Self {
        Self {
            unit_of_work_factory,
            bus_publisher,
        }
    }

    /// Returns a stable operations name for diagnostics and tests.
    pub fn operation_name(&self) -> &'static str {
        "ReplayDeadOutboxEvent"
    }
}

impl<UowFactory, BusPublisher> ReplayDeadOutboxEventJob<UowFactory, BusPublisher>
where
    UowFactory: UnitOfWorkFactory,
    BusPublisher: BusPublisherPort,
{
    /// Replays one outbox row that has already reached `dead`.
    pub async fn replay_dead_outbox_event(
        &self,
        outbox_event_id: &OutboxEventId,
    ) -> Result<ReplayDeadOutboxEventResult, IdentityError> {
        let mut uow = self.unit_of_work_factory.begin().await?;
        let event = uow.outbox().get(outbox_event_id).await?.ok_or_else(|| {
            IdentityError::RuleViolation {
                code: "IDENTITY_OUTBOX_EVENT_NOT_FOUND",
                message: format!("outbox event `{}` was not found", outbox_event_id.as_str()),
            }
        })?;
        uow.rollback().await?;

        if event.status != crate::domain::outbox::OutboxStatus::Dead {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_OUTBOX_EVENT_NOT_DEAD",
                message: format!(
                    "outbox event `{}` is not in dead status",
                    outbox_event_id.as_str()
                ),
            });
        }

        let publish_result = self.bus_publisher.publish(&event).await;
        let mut event_to_save = event.clone();
        let now = current_timestamp();

        match &publish_result {
            Ok(()) => event_to_save.mark_published(now),
            Err(error) => {
                event_to_save.status = crate::domain::outbox::OutboxStatus::Dead;
                event_to_save.failure_reason = Some(error.to_string());
                event_to_save.next_retry_at = None;
                event_to_save.published_at = None;
            }
        }

        let mut uow = self.unit_of_work_factory.begin().await?;
        uow.outbox().save(&event_to_save).await?;
        uow.commit().await?;

        publish_result?;
        Ok(ReplayDeadOutboxEventResult {
            outbox_event_id: outbox_event_id.as_str().to_string(),
        })
    }
}

/// Rebuilds member summary projections from already-persisted outbox events.
#[derive(Debug, Clone)]
pub struct ProjectionRebuildJob<UowFactory> {
    unit_of_work_factory: UowFactory,
}

impl<UowFactory> ProjectionRebuildJob<UowFactory> {
    /// Creates a new projection rebuild job bound to the provided persistence factory.
    pub fn new(unit_of_work_factory: UowFactory) -> Self {
        Self {
            unit_of_work_factory,
        }
    }

    /// Returns a stable operations name for diagnostics and tests.
    pub fn operation_name(&self) -> &'static str {
        "RebuildMemberSummaryProjection"
    }
}

/// Resets one projection checkpoint back to the initial idle state for recovery workflows.
#[derive(Debug, Clone)]
pub struct ResetProjectionCheckpointJob<UowFactory> {
    unit_of_work_factory: UowFactory,
}

impl<UowFactory> ResetProjectionCheckpointJob<UowFactory> {
    /// Creates a new checkpoint reset job bound to the provided persistence factory.
    pub fn new(unit_of_work_factory: UowFactory) -> Self {
        Self {
            unit_of_work_factory,
        }
    }

    /// Returns a stable operations name for diagnostics and tests.
    pub fn operation_name(&self) -> &'static str {
        "ResetProjectionCheckpoint"
    }
}

impl<UowFactory> ResetProjectionCheckpointJob<UowFactory>
where
    UowFactory: UnitOfWorkFactory,
{
    /// Resets one persisted projection checkpoint so a later rebuild can restart from scratch.
    pub async fn reset_projection_checkpoint(
        &self,
        checkpoint_name: &str,
    ) -> Result<ResetProjectionCheckpointResult, IdentityError> {
        if checkpoint_name.trim().is_empty() {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_INVALID_ARGUMENT",
                message: "checkpoint_name must not be blank".to_string(),
            });
        }

        let mut uow = self.unit_of_work_factory.begin().await?;
        let mut checkpoint = uow
            .projection_checkpoints()
            .get_or_create(checkpoint_name)
            .await?;
        checkpoint.reset(current_timestamp());
        uow.projection_checkpoints().save(&checkpoint).await?;
        uow.commit().await?;

        Ok(ResetProjectionCheckpointResult {
            checkpoint_name: checkpoint_name.to_string(),
        })
    }
}

/// Clears one stuck or failed idempotency record so operators can safely retry the original flow.
#[derive(Debug, Clone)]
pub struct ClearIdempotencyRecordJob<UowFactory> {
    unit_of_work_factory: UowFactory,
}

impl<UowFactory> ClearIdempotencyRecordJob<UowFactory> {
    /// Creates a new idempotency recovery job.
    pub fn new(unit_of_work_factory: UowFactory) -> Self {
        Self {
            unit_of_work_factory,
        }
    }

    /// Returns a stable operations name for diagnostics and tests.
    pub fn operation_name(&self) -> &'static str {
        "ClearIdempotencyRecord"
    }
}

impl<UowFactory> ClearIdempotencyRecordJob<UowFactory>
where
    UowFactory: UnitOfWorkFactory,
{
    /// Deletes one `processing` or `failed` idempotency record to allow a clean retry.
    pub async fn clear_idempotency_record(
        &self,
        idempotency_key: &str,
        scope: IdempotencyScope,
    ) -> Result<ClearIdempotencyRecordResult, IdentityError> {
        if idempotency_key.trim().is_empty() {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_INVALID_ARGUMENT",
                message: "idempotency_key must not be blank".to_string(),
            });
        }

        let mut uow = self.unit_of_work_factory.begin().await?;
        let record = uow
            .idempotency()
            .get(idempotency_key, scope)
            .await?
            .ok_or_else(|| IdentityError::RuleViolation {
                code: "IDENTITY_IDEMPOTENCY_RECORD_NOT_FOUND",
                message: format!(
                    "idempotency record `{idempotency_key}` in scope `{}` was not found",
                    scope.as_db()
                ),
            })?;

        if record.status == IdempotencyStatus::Succeeded {
            uow.rollback().await?;
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_IDEMPOTENCY_RECORD_NOT_CLEARABLE",
                message: format!(
                    "idempotency record `{idempotency_key}` in scope `{}` has already succeeded and cannot be cleared",
                    scope.as_db()
                ),
            });
        }

        let deleted = uow.idempotency().delete(idempotency_key, scope).await?;
        if !deleted {
            uow.rollback().await?;
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_IDEMPOTENCY_RECORD_NOT_FOUND",
                message: format!(
                    "idempotency record `{idempotency_key}` in scope `{}` was not found",
                    scope.as_db()
                ),
            });
        }

        uow.commit().await?;

        Ok(ClearIdempotencyRecordResult {
            idempotency_key: idempotency_key.to_string(),
            scope,
        })
    }
}

/// Rebuilds one member summary projection row from that member's persisted outbox facts.
#[derive(Debug, Clone)]
pub struct RebuildMemberProjectionJob<UowFactory> {
    unit_of_work_factory: UowFactory,
}

impl<UowFactory> RebuildMemberProjectionJob<UowFactory> {
    /// Creates a new point-rebuild job for member summary projection recovery.
    pub fn new(unit_of_work_factory: UowFactory) -> Self {
        Self {
            unit_of_work_factory,
        }
    }

    /// Returns a stable operations name for diagnostics and tests.
    pub fn operation_name(&self) -> &'static str {
        "RebuildMemberProjection"
    }
}

impl<UowFactory> RebuildMemberProjectionJob<UowFactory>
where
    UowFactory: UnitOfWorkFactory,
{
    /// Rebuilds one member summary projection row entirely from persisted outbox facts.
    pub async fn rebuild_member_projection(
        &self,
        global_member_id: &str,
    ) -> Result<RebuildMemberProjectionResult, IdentityError> {
        if global_member_id.trim().is_empty() {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_INVALID_ARGUMENT",
                message: "global_member_id must not be blank".to_string(),
            });
        }

        let global_member_id = crate::domain::shared::ids::GlobalMemberId::new(global_member_id);
        let mut uow = self.unit_of_work_factory.begin().await?;
        let events = uow
            .outbox()
            .list_for_member_projection(&global_member_id)
            .await?;

        if events.is_empty() {
            uow.rollback().await?;
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_PROJECTION_REBUILD_SOURCE_NOT_FOUND",
                message: format!(
                    "no persisted member projection facts were found for global member `{}`",
                    global_member_id.as_str()
                ),
            });
        }

        uow.member_summary_projection()
            .delete(&global_member_id)
            .await?;

        let mut rebuilt_any_projection = false;
        for event in &events {
            let projection = apply_member_summary_projection_event(&mut uow, event).await?;
            if let Some(projection) = projection.as_ref() {
                uow.member_summary_projection().upsert(projection).await?;
                rebuilt_any_projection = true;
            }
        }

        if !rebuilt_any_projection {
            uow.rollback().await?;
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_PROJECTION_REBUILD_SOURCE_NOT_FOUND",
                message: format!(
                    "no persisted member projection facts were found for global member `{}`",
                    global_member_id.as_str()
                ),
            });
        }

        uow.commit().await?;

        Ok(RebuildMemberProjectionResult {
            global_member_id: global_member_id.as_str().to_string(),
            replayed_events: events.len(),
        })
    }
}

impl<UowFactory> ProjectionRebuildJob<UowFactory>
where
    UowFactory: UnitOfWorkFactory,
{
    /// Rebuilds one batch of member summary projections strictly after the checkpoint cursor.
    ///
    /// # Errors
    ///
    /// Returns an error when a projection event cannot be applied or when persistence fails.
    /// When an event cannot be applied, the checkpoint is marked failed without advancing past
    /// the problematic outbox row.
    pub async fn rebuild_member_summary_projection(
        &self,
        checkpoint_name: &str,
        batch_size: usize,
    ) -> Result<RebuildMemberSummaryProjectionSummary, IdentityError> {
        let mut checkpoint = self.load_or_create_checkpoint(checkpoint_name).await?;
        checkpoint.mark_running(current_timestamp());
        self.save_checkpoint(&checkpoint).await?;

        let events = {
            let mut uow = self.unit_of_work_factory.begin().await?;
            let events = uow
                .outbox()
                .list_after(checkpoint.last_processed_event_id.as_ref(), batch_size)
                .await?;
            uow.rollback().await?;
            events
        };

        let mut summary = RebuildMemberSummaryProjectionSummary {
            scanned: events.len(),
            rebuilt: 0,
            skipped: 0,
        };

        for event in events {
            match self.process_event(&mut checkpoint, &event).await {
                Ok(true) => summary.rebuilt += 1,
                Ok(false) => summary.skipped += 1,
                Err(error) => {
                    checkpoint.mark_failed(error.to_string(), current_timestamp());
                    self.save_checkpoint(&checkpoint).await?;
                    return Err(error);
                }
            }
        }

        checkpoint.mark_idle(current_timestamp());
        self.save_checkpoint(&checkpoint).await?;
        Ok(summary)
    }

    async fn load_or_create_checkpoint(
        &self,
        checkpoint_name: &str,
    ) -> Result<ProjectionCheckpoint, IdentityError> {
        let mut uow = self.unit_of_work_factory.begin().await?;
        let checkpoint = uow
            .projection_checkpoints()
            .get_or_create(checkpoint_name)
            .await?;
        uow.commit().await?;
        Ok(checkpoint)
    }

    async fn save_checkpoint(
        &self,
        checkpoint: &ProjectionCheckpoint,
    ) -> Result<(), IdentityError> {
        let mut uow = self.unit_of_work_factory.begin().await?;
        uow.projection_checkpoints().save(checkpoint).await?;
        uow.commit().await?;
        Ok(())
    }

    async fn process_event(
        &self,
        checkpoint: &mut ProjectionCheckpoint,
        event: &OutboxEvent,
    ) -> Result<bool, IdentityError> {
        let mut uow = self.unit_of_work_factory.begin().await?;
        let projection_result = match apply_member_summary_projection_event(&mut uow, event).await {
            Ok(projection_result) => projection_result,
            Err(error) => {
                uow.rollback().await?;
                return Err(error);
            }
        };

        if let Some(projection) = projection_result.as_ref() {
            let upsert_result = {
                let mut repository = uow.member_summary_projection();
                repository.upsert(projection).await
            };
            if let Err(error) = upsert_result {
                uow.rollback().await?;
                return Err(error);
            }
        }

        checkpoint.advance_to(event.outbox_event_id.clone(), current_timestamp());
        let save_checkpoint_result = {
            let mut repository = uow.projection_checkpoints();
            repository.save(checkpoint).await
        };
        if let Err(error) = save_checkpoint_result {
            uow.rollback().await?;
            return Err(error);
        }
        uow.commit().await?;

        Ok(projection_result.is_some())
    }
}

/// Reconciles the local role-catalog index against the authoritative method-library catalog.
#[derive(Debug, Clone)]
pub struct RoleReconciliationJob<UowFactory, RoleCatalogPort> {
    unit_of_work_factory: UowFactory,
    role_catalog_port: RoleCatalogPort,
}

impl<UowFactory, RoleCatalogPort> RoleReconciliationJob<UowFactory, RoleCatalogPort> {
    /// Creates a new role-catalog reconciliation job.
    pub fn new(unit_of_work_factory: UowFactory, role_catalog_port: RoleCatalogPort) -> Self {
        Self {
            unit_of_work_factory,
            role_catalog_port,
        }
    }

    /// Returns a stable operations name for diagnostics and tests.
    pub fn operation_name(&self) -> &'static str {
        "ReconcileRoleCatalog"
    }
}

impl<UowFactory, RoleCatalogPort> RoleReconciliationJob<UowFactory, RoleCatalogPort>
where
    UowFactory: UnitOfWorkFactory,
    RoleCatalogPort: MethodLibraryRoleCatalogPort,
{
    /// Reconciles local role-index rows against the authoritative method-library catalog.
    ///
    /// CAUTION: This job only repairs the local index and append-only audit evidence.
    /// It must not mutate any external source of truth.
    pub async fn reconcile_role_catalog(
        &self,
    ) -> Result<ReconcileRoleCatalogSummary, IdentityError> {
        let now = current_timestamp();
        let authoritative_roles = match self.role_catalog_port.list_role_catalog().await {
            Ok(snapshots) => snapshots,
            Err(error) => {
                append_failed_reconciliation_audit(
                    &self.unit_of_work_factory,
                    self.operation_name(),
                    now,
                    error.to_string(),
                )
                .await?;
                return Err(error);
            }
        };
        let summary = reconcile_role_catalog_entries(
            &self.unit_of_work_factory,
            &authoritative_roles,
            now,
            self.operation_name(),
        )
        .await?;
        Ok(summary)
    }
}

fn current_timestamp() -> PrimitiveDateTime {
    let now = OffsetDateTime::now_utc();
    PrimitiveDateTime::new(now.date(), now.time())
}

fn replay_payload_hash(payload_json: &serde_json::Value) -> String {
    payload_json.to_string()
}

async fn reconcile_role_catalog_entries<UowFactory>(
    unit_of_work_factory: &UowFactory,
    authoritative_roles: &[RoleDefinitionSnapshot],
    now: PrimitiveDateTime,
    action_name: &str,
) -> Result<ReconcileRoleCatalogSummary, IdentityError>
where
    UowFactory: UnitOfWorkFactory,
{
    let mut uow = unit_of_work_factory.begin().await?;
    let summary =
        reconcile_role_catalog_entries_in_uow(&mut uow, authoritative_roles, now, action_name)
            .await;

    match summary {
        Ok(summary) => {
            uow.commit().await?;
            Ok(summary)
        }
        Err(error) => {
            uow.rollback().await?;
            Err(error)
        }
    }
}

async fn reconcile_role_catalog_entries_in_uow<Uow>(
    uow: &mut Uow,
    authoritative_roles: &[RoleDefinitionSnapshot],
    now: PrimitiveDateTime,
    action_name: &str,
) -> Result<ReconcileRoleCatalogSummary, IdentityError>
where
    Uow: UnitOfWork,
{
    let local_entries = uow.role_catalog().list_all().await?;
    let mut local_by_role_id = std::collections::HashMap::with_capacity(local_entries.len());
    for entry in local_entries {
        local_by_role_id.insert(entry.role_id.as_str().to_string(), entry);
    }

    let mut summary = ReconcileRoleCatalogSummary {
        scanned: authoritative_roles.len(),
        missing: 0,
        refreshed: 0,
        deprecated: 0,
        drifted: 0,
        local_only: 0,
        unchanged: 0,
    };

    for snapshot in authoritative_roles {
        match local_by_role_id.remove(snapshot.role_id.as_str()) {
            None => {
                let entry =
                    crate::domain::role_catalog::RoleCatalogEntry::from_role_definition_snapshot_ref(
                        snapshot, now,
                    )?;
                uow.role_catalog().upsert(&entry).await?;
                summary.missing += 1;
            }
            Some(mut entry) => {
                if entry.matches_snapshot(snapshot)? {
                    summary.unchanged += 1;
                    continue;
                }

                match snapshot.status.as_str() {
                    "deprecated" => {
                        entry.role_version = snapshot.role_version.clone();
                        entry.source_ref_json = snapshot.source_ref.clone();
                        entry.fingerprint = snapshot.fingerprint.clone();
                        entry.mark_deprecated(now);
                        if entry.role_name != snapshot.role_name {
                            entry.rename(snapshot.role_name.clone(), now);
                        }
                        uow.role_catalog().upsert(&entry).await?;
                        summary.deprecated += 1;
                    }
                    "active" | "source_drift" => {
                        if entry.fingerprint != snapshot.fingerprint {
                            entry.mark_source_drift(snapshot.fingerprint.clone(), now);
                            uow.role_catalog().upsert(&entry).await?;
                            summary.drifted += 1;
                        } else {
                            entry.apply_snapshot(snapshot, now)?;
                            uow.role_catalog().upsert(&entry).await?;
                            summary.refreshed += 1;
                        }
                    }
                    other => {
                        return Err(IdentityError::PersistenceData {
                            message: format!(
                                "unknown authoritative role catalog status `{other}` during reconciliation"
                            ),
                        });
                    }
                }
            }
        }
    }
    summary.local_only = local_by_role_id.len();
    let mut local_only_role_ids = local_by_role_id.keys().cloned().collect::<Vec<_>>();
    local_only_role_ids.sort();

    let audit_entry = AuditTraceEntry::for_operations_job(
        build_operations_audit_id(action_name, now),
        action_name,
        build_operations_trace_id(action_name, now),
        Some(serde_json::json!({
            "kind": "role_catalog_reconciliation_report",
            "scanned": summary.scanned,
            "missing": summary.missing,
            "refreshed": summary.refreshed,
            "deprecated": summary.deprecated,
            "drifted": summary.drifted,
            "local_only": summary.local_only,
            "unchanged": summary.unchanged,
            "local_only_role_ids": local_only_role_ids,
        })),
        AuditResult::Success,
        if summary.local_only == 0 {
            None
        } else {
            Some(format!(
                "{} local role catalog entries are absent from the authoritative source snapshot",
                summary.local_only
            ))
        },
        now,
    );
    uow.audit_traces().append(&audit_entry).await?;
    Ok(summary)
}

async fn append_failed_reconciliation_audit<UowFactory>(
    unit_of_work_factory: &UowFactory,
    action_name: &str,
    now: PrimitiveDateTime,
    reason: String,
) -> Result<(), IdentityError>
where
    UowFactory: UnitOfWorkFactory,
{
    let mut uow = unit_of_work_factory.begin().await?;
    let audit_entry = AuditTraceEntry::for_operations_job(
        build_operations_audit_id(action_name, now),
        action_name,
        build_operations_trace_id(action_name, now),
        Some(serde_json::json!({
            "kind": "role_catalog_reconciliation_report",
            "scanned": 0,
            "missing": 0,
            "refreshed": 0,
            "deprecated": 0,
            "drifted": 0,
            "local_only": 0,
            "unchanged": 0,
            "local_only_role_ids": [],
        })),
        AuditResult::Failed,
        Some(reason),
        now,
    );

    let append_result = {
        let mut audit_repository = uow.audit_traces();
        audit_repository.append(&audit_entry).await
    };

    match append_result {
        Ok(()) => uow.commit().await,
        Err(error) => {
            uow.rollback().await?;
            Err(error)
        }
    }
}

fn build_operations_trace_id(action_name: &str, now: PrimitiveDateTime) -> String {
    format!("{action_name}:{}", now.assume_utc().unix_timestamp_nanos())
}

fn build_operations_audit_id(action_name: &str, now: PrimitiveDateTime) -> String {
    format!(
        "audit:{action_name}:{}",
        now.assume_utc().unix_timestamp_nanos()
    )
}

async fn apply_member_summary_projection_event<Uow>(
    uow: &mut Uow,
    event: &OutboxEvent,
) -> Result<Option<MemberSummaryProjection>, IdentityError>
where
    Uow: UnitOfWork,
{
    let existing_projection = if matches!(
        event.event_type.as_str(),
        "identity.member.lifecycle_changed"
            | "identity.member.tombstoned"
            | "identity.capability_profile.updated"
            | "identity.career_history.appended"
            | "identity.memory_refs.updated"
            | "identity.memory_refs.archive_status_changed"
    ) {
        let global_member_id = event
            .payload_json
            .get("global_member_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| IdentityError::PersistenceData {
                message: format!(
                    "projection outbox payload for `{}` is missing `global_member_id`",
                    event.outbox_event_id.as_str()
                ),
            })?;
        uow.member_summary_projection()
            .get(&crate::domain::shared::ids::GlobalMemberId::new(
                global_member_id,
            ))
            .await?
    } else {
        None
    };

    let mut projection = match MemberSummaryProjection::apply_outbox_event(
        event,
        existing_projection,
        current_timestamp(),
    )? {
        Some(projection) => projection,
        None => return Ok(None),
    };

    if let Some(main_role_id) = projection.main_role_id.clone() {
        projection.main_role_name = uow
            .role_catalog()
            .get_active(&main_role_id)
            .await?
            .map(|entry| entry.role_name);
    }

    Ok(Some(projection))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use serde_json::json;
    use sqlx::{Executor, Row, postgres::PgPoolOptions};
    use time::{Duration, OffsetDateTime, PrimitiveDateTime};

    use crate::application::career_event::CareerEventConsumerService;
    use crate::application::memory_refs::MemoryRefsCommandService;
    use crate::application::role_catalog_sync::{RoleCatalogSyncOutcome, RoleCatalogSyncService};
    use crate::application::tombstone_flow::TombstoneFlowService;
    use crate::config::AppConfig;
    use crate::domain::dead_letter::InboundDeadLetter;
    use crate::domain::idempotency::{IdempotencyScope, IdempotencyStatus};
    use crate::domain::memory_refs::{ArchiveRef, ArchiveStatus};
    use crate::domain::outbox::{OutboxEvent, OutboxStatus};
    use crate::domain::shared::context::{ActorContext, ActorKind};
    use crate::domain::shared::ids::{
        DeadLetterId, EventId, GlobalMemberId, OutboxEventId, ProjectId,
    };
    use crate::domain::tombstone::GateDecisionRef;
    use crate::error::IdentityError;
    use crate::inbound::events::RoleDefinitionSnapshot;
    use crate::inbound::events::{
        InboundEventEnvelope, InboundMemoryArchiveEvent, InboundProcessFactEvent,
        InboundRoleCatalogEvent,
    };
    use crate::outbound::{
        ArchiveRequestPort, BusPublisherPort, GovernancePort, MemoryArchivePort,
        MethodLibraryRoleCatalogPort,
    };
    use crate::persistence::database::run_migrations;
    use crate::persistence::test_support::DB_TEST_MUTEX;
    use crate::persistence::unit_of_work::SqlxUnitOfWorkFactory;

    use super::{
        ClearIdempotencyRecordJob, ClearIdempotencyRecordResult, IgnoreInboundDeadLetterJob,
        IgnoreInboundDeadLetterResult, InboundDeadLetterReplayJob, OutboxPublisherJob,
        ProjectionRebuildJob, PublishOutboxEventsSummary, RebuildMemberProjectionJob,
        RebuildMemberProjectionResult, RebuildMemberSummaryProjectionSummary,
        ReconcileRoleCatalogSummary, ReplayDeadOutboxEventJob, ReplayDeadOutboxEventResult,
        ReplayInboundDeadLetterJob, ReplayInboundDeadLetterResult, ReplayInboundDeadLettersSummary,
        ResetProjectionCheckpointJob, ResetProjectionCheckpointResult, RoleReconciliationJob,
    };

    #[derive(Debug, Clone)]
    struct RecordingBusPublisher {
        state: Arc<Mutex<RecordingBusPublisherState>>,
    }

    #[derive(Debug, Default)]
    struct RecordingBusPublisherState {
        published_event_ids: Vec<String>,
        fail_event_ids: Vec<String>,
    }

    impl RecordingBusPublisher {
        fn with_failures(fail_event_ids: &[&str]) -> Self {
            Self {
                state: Arc::new(Mutex::new(RecordingBusPublisherState {
                    published_event_ids: Vec::new(),
                    fail_event_ids: fail_event_ids
                        .iter()
                        .map(|value| value.to_string())
                        .collect(),
                })),
            }
        }

        fn published_event_ids(&self) -> Vec<String> {
            self.state
                .lock()
                .expect("lock publisher state")
                .published_event_ids
                .clone()
        }
    }

    impl BusPublisherPort for RecordingBusPublisher {
        async fn publish(&self, event: &OutboxEvent) -> Result<(), IdentityError> {
            let mut state = self.state.lock().expect("lock publisher state");
            if state
                .fail_event_ids
                .iter()
                .any(|value| value == event.outbox_event_id.as_str())
            {
                return Err(IdentityError::RuleViolation {
                    code: "IDENTITY_OUTBOX_PUBLISH_FAILED",
                    message: format!("failed to publish `{}`", event.outbox_event_id.as_str()),
                });
            }

            state
                .published_event_ids
                .push(event.outbox_event_id.as_str().to_string());
            Ok(())
        }
    }

    #[derive(Debug, Clone, Default)]
    struct NoopGovernancePort;

    impl GovernancePort for NoopGovernancePort {
        async fn require_gate_decision(
            &self,
            _action_name: &str,
            _member: &crate::domain::member::GlobalMember,
            _actor: &ActorContext,
            _reason: &str,
            _supplied_gate_ref: Option<&GateDecisionRef>,
        ) -> Result<GateDecisionRef, IdentityError> {
            Err(IdentityError::PersistenceData {
                message: "noop governance port should not be called in this test".to_string(),
            })
        }
    }

    #[derive(Debug, Clone, Default)]
    struct NoopArchiveRequester;

    impl ArchiveRequestPort for NoopArchiveRequester {
        async fn request_archive(
            &self,
            _global_member_id: &GlobalMemberId,
            _reason: &str,
        ) -> Result<ArchiveRef, IdentityError> {
            Err(IdentityError::PersistenceData {
                message: "noop archive requester should not be called in this test".to_string(),
            })
        }
    }

    #[derive(Debug, Clone, Default)]
    struct NoopMemoryArchivePort;

    impl MemoryArchivePort for NoopMemoryArchivePort {
        async fn validate_ref(
            &self,
            _memory_ref: &crate::domain::memory_refs::MemoryRef,
        ) -> Result<(), IdentityError> {
            Ok(())
        }
    }

    #[derive(Debug, Clone)]
    struct StubMethodLibraryRoleCatalogPort {
        snapshots: Vec<RoleDefinitionSnapshot>,
        error: Option<String>,
    }

    impl StubMethodLibraryRoleCatalogPort {
        fn succeed(snapshots: Vec<RoleDefinitionSnapshot>) -> Self {
            Self {
                snapshots,
                error: None,
            }
        }

        fn fail(message: &str) -> Self {
            Self {
                snapshots: Vec::new(),
                error: Some(message.to_string()),
            }
        }
    }

    impl MethodLibraryRoleCatalogPort for StubMethodLibraryRoleCatalogPort {
        async fn list_role_catalog(&self) -> Result<Vec<RoleDefinitionSnapshot>, IdentityError> {
            if let Some(message) = self.error.as_ref() {
                return Err(IdentityError::RuleViolation {
                    code: "IDENTITY_METHOD_LIBRARY_UNAVAILABLE",
                    message: message.clone(),
                });
            }

            Ok(self.snapshots.clone())
        }
    }

    #[tokio::test]
    async fn publish_outbox_events_marks_rows_published_after_successful_bus_handoff() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;
        seed_outbox_event(&pool, sample_pending_outbox_event("outbox-001")).await;

        let publisher = RecordingBusPublisher::with_failures(&[]);
        let job =
            OutboxPublisherJob::new(SqlxUnitOfWorkFactory::new(pool.clone()), publisher.clone());

        let summary = job
            .publish_outbox_events(10)
            .await
            .expect("publisher pass should succeed");

        let row = sqlx::query(
            "SELECT status, retry_count, next_retry_at, published_at FROM outbox_events WHERE outbox_event_id = $1",
        )
        .bind("outbox-001")
        .fetch_one(&pool)
        .await
        .expect("load outbox row");

        assert_eq!(
            summary,
            PublishOutboxEventsSummary {
                scanned: 1,
                published: 1,
                failed: 0,
            }
        );
        assert_eq!(
            publisher.published_event_ids(),
            vec!["outbox-001".to_string()]
        );
        assert_eq!(row.get::<String, _>("status"), "published");
        assert_eq!(row.get::<i32, _>("retry_count"), 0);
        assert_eq!(
            row.get::<Option<PrimitiveDateTime>, _>("next_retry_at"),
            None
        );
        assert!(
            row.get::<Option<PrimitiveDateTime>, _>("published_at")
                .is_some()
        );
    }

    #[tokio::test]
    async fn publish_outbox_events_marks_rows_failed_and_sets_retry_metadata() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;
        seed_outbox_event(&pool, sample_pending_outbox_event("outbox-002")).await;

        let publisher = RecordingBusPublisher::with_failures(&["outbox-002"]);
        let job = OutboxPublisherJob::new(SqlxUnitOfWorkFactory::new(pool.clone()), publisher);

        let summary = job
            .publish_outbox_events(10)
            .await
            .expect("publisher pass should capture bus failures");

        let row = sqlx::query(
            "SELECT status, retry_count, next_retry_at, published_at, failure_reason FROM outbox_events WHERE outbox_event_id = $1",
        )
        .bind("outbox-002")
        .fetch_one(&pool)
        .await
        .expect("load failed outbox row");

        assert_eq!(
            summary,
            PublishOutboxEventsSummary {
                scanned: 1,
                published: 0,
                failed: 1,
            }
        );
        assert_eq!(row.get::<String, _>("status"), "failed");
        assert_eq!(row.get::<i32, _>("retry_count"), 1);
        assert!(
            row.get::<Option<PrimitiveDateTime>, _>("next_retry_at")
                .is_some()
        );
        assert_eq!(
            row.get::<Option<PrimitiveDateTime>, _>("published_at"),
            None
        );
        assert!(
            row.get::<Option<String>, _>("failure_reason")
                .expect("failure reason should exist")
                .contains("IDENTITY_OUTBOX_PUBLISH_FAILED")
        );
    }

    #[tokio::test]
    async fn publish_outbox_events_only_scans_pending_rows() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;

        seed_outbox_event(&pool, sample_pending_outbox_event("outbox-003")).await;
        let mut published = sample_pending_outbox_event("outbox-004");
        published.status = OutboxStatus::Published;
        published.published_at = Some(now());
        seed_outbox_event(&pool, published).await;

        let publisher = RecordingBusPublisher::with_failures(&[]);
        let job =
            OutboxPublisherJob::new(SqlxUnitOfWorkFactory::new(pool.clone()), publisher.clone());

        let summary = job
            .publish_outbox_events(10)
            .await
            .expect("publisher pass should succeed");

        assert_eq!(
            summary,
            PublishOutboxEventsSummary {
                scanned: 1,
                published: 1,
                failed: 0,
            }
        );
        assert_eq!(
            publisher.published_event_ids(),
            vec!["outbox-003".to_string()]
        );
    }

    #[tokio::test]
    async fn publish_outbox_events_retries_failed_rows_after_backoff_window() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;

        let mut retryable = sample_pending_outbox_event("outbox-005");
        retryable.status = OutboxStatus::Failed;
        retryable.retry_count = 1;
        retryable.next_retry_at = Some(now() - Duration::seconds(1));
        retryable.failure_reason = Some("previous publish failure".to_string());
        seed_outbox_event(&pool, retryable).await;

        let publisher = RecordingBusPublisher::with_failures(&[]);
        let job =
            OutboxPublisherJob::new(SqlxUnitOfWorkFactory::new(pool.clone()), publisher.clone());

        let summary = job
            .publish_outbox_events(10)
            .await
            .expect("publisher pass should retry failed rows");

        let row = sqlx::query(
            "SELECT status, retry_count, next_retry_at, published_at FROM outbox_events WHERE outbox_event_id = $1",
        )
        .bind("outbox-005")
        .fetch_one(&pool)
        .await
        .expect("load retried outbox row");

        assert_eq!(
            summary,
            PublishOutboxEventsSummary {
                scanned: 1,
                published: 1,
                failed: 0,
            }
        );
        assert_eq!(row.get::<String, _>("status"), "published");
        assert_eq!(row.get::<i32, _>("retry_count"), 1);
        assert_eq!(
            publisher.published_event_ids(),
            vec!["outbox-005".to_string()]
        );
    }

    #[tokio::test]
    async fn publish_outbox_events_skips_failed_rows_until_retry_time() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;

        let mut retry_later = sample_pending_outbox_event("outbox-006");
        retry_later.status = OutboxStatus::Failed;
        retry_later.retry_count = 1;
        retry_later.next_retry_at = Some(now() + Duration::seconds(120));
        retry_later.failure_reason = Some("backoff in progress".to_string());
        seed_outbox_event(&pool, retry_later).await;

        let publisher = RecordingBusPublisher::with_failures(&[]);
        let job =
            OutboxPublisherJob::new(SqlxUnitOfWorkFactory::new(pool.clone()), publisher.clone());

        let summary = job
            .publish_outbox_events(10)
            .await
            .expect("publisher pass should skip rows that are still backing off");

        let row =
            sqlx::query("SELECT status, retry_count FROM outbox_events WHERE outbox_event_id = $1")
                .bind("outbox-006")
                .fetch_one(&pool)
                .await
                .expect("load deferred outbox row");

        assert_eq!(
            summary,
            PublishOutboxEventsSummary {
                scanned: 0,
                published: 0,
                failed: 0,
            }
        );
        assert_eq!(row.get::<String, _>("status"), "failed");
        assert_eq!(row.get::<i32, _>("retry_count"), 1);
        assert!(publisher.published_event_ids().is_empty());
    }

    #[tokio::test]
    async fn publish_outbox_events_marks_rows_dead_after_max_retries() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;

        let mut exhausted = sample_pending_outbox_event("outbox-007");
        exhausted.status = OutboxStatus::Failed;
        exhausted.retry_count = 4;
        exhausted.next_retry_at = Some(now() - Duration::seconds(1));
        exhausted.failure_reason = Some("already retried".to_string());
        seed_outbox_event(&pool, exhausted).await;

        let publisher = RecordingBusPublisher::with_failures(&["outbox-007"]);
        let job = OutboxPublisherJob::new(SqlxUnitOfWorkFactory::new(pool.clone()), publisher);

        let summary = job
            .publish_outbox_events(10)
            .await
            .expect("publisher pass should dead-letter exhausted outbox rows");

        let row = sqlx::query(
            "SELECT status, retry_count, next_retry_at, failure_reason FROM outbox_events WHERE outbox_event_id = $1",
        )
        .bind("outbox-007")
        .fetch_one(&pool)
        .await
        .expect("load exhausted outbox row");

        assert_eq!(
            summary,
            PublishOutboxEventsSummary {
                scanned: 1,
                published: 0,
                failed: 1,
            }
        );
        assert_eq!(row.get::<String, _>("status"), "dead");
        assert_eq!(row.get::<i32, _>("retry_count"), 5);
        assert_eq!(
            row.get::<Option<PrimitiveDateTime>, _>("next_retry_at"),
            None
        );
        assert!(
            row.get::<Option<String>, _>("failure_reason")
                .expect("failure reason should exist")
                .contains("IDENTITY_OUTBOX_PUBLISH_FAILED")
        );
    }

    #[tokio::test]
    async fn replay_dead_outbox_event_marks_dead_rows_published_after_manual_success() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;

        let mut dead_event = sample_pending_outbox_event("outbox-dead-001");
        dead_event.status = OutboxStatus::Dead;
        dead_event.retry_count = 5;
        dead_event.failure_reason = Some("manual replay needed".to_string());
        seed_outbox_event(&pool, dead_event).await;

        let publisher = RecordingBusPublisher::with_failures(&[]);
        let job = ReplayDeadOutboxEventJob::new(
            SqlxUnitOfWorkFactory::new(pool.clone()),
            publisher.clone(),
        );
        let result = job
            .replay_dead_outbox_event(&OutboxEventId::new("outbox-dead-001"))
            .await
            .expect("manual replay should publish dead outbox rows");

        let row = sqlx::query(
            "SELECT status, retry_count, next_retry_at, published_at, failure_reason FROM outbox_events WHERE outbox_event_id = $1",
        )
        .bind("outbox-dead-001")
        .fetch_one(&pool)
        .await
        .expect("load replayed dead outbox row");

        assert_eq!(
            result,
            ReplayDeadOutboxEventResult {
                outbox_event_id: "outbox-dead-001".to_string(),
            }
        );
        assert_eq!(
            publisher.published_event_ids(),
            vec!["outbox-dead-001".to_string()]
        );
        assert_eq!(row.get::<String, _>("status"), "published");
        assert_eq!(row.get::<i32, _>("retry_count"), 5);
        assert_eq!(
            row.get::<Option<PrimitiveDateTime>, _>("next_retry_at"),
            None
        );
        assert!(
            row.get::<Option<PrimitiveDateTime>, _>("published_at")
                .is_some()
        );
        assert_eq!(row.get::<Option<String>, _>("failure_reason"), None);
    }

    #[tokio::test]
    async fn replay_dead_outbox_event_rejects_non_dead_rows() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;
        seed_outbox_event(&pool, sample_pending_outbox_event("outbox-not-dead-001")).await;

        let publisher = RecordingBusPublisher::with_failures(&[]);
        let job =
            ReplayDeadOutboxEventJob::new(SqlxUnitOfWorkFactory::new(pool.clone()), publisher);
        let error = job
            .replay_dead_outbox_event(&OutboxEventId::new("outbox-not-dead-001"))
            .await
            .expect_err("non-dead rows should be rejected");

        match error {
            IdentityError::RuleViolation { code, message } => {
                assert_eq!(code, "IDENTITY_OUTBOX_EVENT_NOT_DEAD");
                assert_eq!(
                    message,
                    "outbox event `outbox-not-dead-001` is not in dead status"
                );
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[tokio::test]
    async fn replay_dead_outbox_event_keeps_rows_dead_when_manual_replay_fails() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;

        let mut dead_event = sample_pending_outbox_event("outbox-dead-002");
        dead_event.status = OutboxStatus::Dead;
        dead_event.retry_count = 5;
        dead_event.failure_reason = Some("previous terminal failure".to_string());
        seed_outbox_event(&pool, dead_event).await;

        let publisher = RecordingBusPublisher::with_failures(&["outbox-dead-002"]);
        let job =
            ReplayDeadOutboxEventJob::new(SqlxUnitOfWorkFactory::new(pool.clone()), publisher);
        let error = job
            .replay_dead_outbox_event(&OutboxEventId::new("outbox-dead-002"))
            .await
            .expect_err("manual replay failure should surface to operators");

        let row = sqlx::query(
            "SELECT status, retry_count, next_retry_at, published_at, failure_reason FROM outbox_events WHERE outbox_event_id = $1",
        )
        .bind("outbox-dead-002")
        .fetch_one(&pool)
        .await
        .expect("load failed manual replay row");

        match error {
            IdentityError::RuleViolation { code, .. } => {
                assert_eq!(code, "IDENTITY_OUTBOX_PUBLISH_FAILED");
            }
            other => panic!("unexpected error: {other}"),
        }
        assert_eq!(row.get::<String, _>("status"), "dead");
        assert_eq!(row.get::<i32, _>("retry_count"), 5);
        assert_eq!(
            row.get::<Option<PrimitiveDateTime>, _>("next_retry_at"),
            None
        );
        assert_eq!(
            row.get::<Option<PrimitiveDateTime>, _>("published_at"),
            None
        );
        assert!(
            row.get::<Option<String>, _>("failure_reason")
                .expect("failure reason should exist")
                .contains("IDENTITY_OUTBOX_PUBLISH_FAILED")
        );
    }

    #[tokio::test]
    async fn replay_inbound_dead_letters_replays_career_event_after_member_exists() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let career_service = CareerEventConsumerService::new(factory.clone());
        let role_service = RoleCatalogSyncService::new(factory.clone());
        let memory_archive_service =
            MemoryRefsCommandService::new(factory.clone(), NoopMemoryArchivePort);
        let tombstone_service =
            TombstoneFlowService::new(factory.clone(), NoopGovernancePort, NoopArchiveRequester);

        let dead_letter_outcome = career_service
            .consume_process_event(sample_process_event(
                "career-replay-001",
                "career-replay-hash-001",
                "member-replay-001",
            ))
            .await
            .expect("missing member should dead-letter");
        assert!(matches!(
            dead_letter_outcome,
            crate::application::career_event::CareerEventOutcome::DeadLettered
        ));
        let original_created_at: PrimitiveDateTime =
            sqlx::query("SELECT created_at FROM inbound_dead_letters WHERE source_event_id = $1")
                .bind("career-replay-001")
                .fetch_one(&pool)
                .await
                .expect("load original dead-letter timestamp")
                .get("created_at");

        insert_member(&pool, "member-replay-001", "Replay Member").await;

        let replay_job = InboundDeadLetterReplayJob::new(
            factory.clone(),
            role_service,
            career_service,
            memory_archive_service,
            tombstone_service,
        );
        let summary = replay_job
            .replay_inbound_dead_letters(10)
            .await
            .expect("replay should succeed after member exists");

        let replay_row = sqlx::query(
            "SELECT replay_status, created_at FROM inbound_dead_letters WHERE source_event_id = $1",
        )
        .bind("career-replay-001")
        .fetch_one(&pool)
        .await
        .expect("load replay row");
        let career_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM career_history_entries WHERE source_event_id = $1",
        )
        .bind("career-replay-001")
        .fetch_one(&pool)
        .await
        .expect("count replayed career rows");

        assert_eq!(
            summary,
            ReplayInboundDeadLettersSummary {
                scanned: 1,
                replayed: 1,
                still_pending: 0,
            }
        );
        assert_eq!(replay_row.get::<String, _>("replay_status"), "replayed");
        assert_eq!(
            replay_row.get::<PrimitiveDateTime, _>("created_at"),
            original_created_at
        );
        assert_eq!(career_count, 1);
    }

    #[tokio::test]
    async fn replay_inbound_dead_letters_does_not_duplicate_dead_letters_when_failure_persists() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let role_service = RoleCatalogSyncService::new(factory.clone());
        let career_service = CareerEventConsumerService::new(factory.clone());
        let memory_archive_service =
            MemoryRefsCommandService::new(factory.clone(), NoopMemoryArchivePort);
        let tombstone_service =
            TombstoneFlowService::new(factory.clone(), NoopGovernancePort, NoopArchiveRequester);

        let dead_letter_outcome = role_service
            .sync_role_catalog(InboundRoleCatalogEvent {
                envelope: InboundEventEnvelope {
                    source_event_id: "role-replay-001".into(),
                    source_module: "method-library".to_string(),
                    event_type: "role.definition.updated".to_string(),
                    occurred_at: now(),
                    payload_hash: "role-replay-hash-001".to_string(),
                    payload: json!({
                        "unexpected_field": true
                    }),
                },
            })
            .await
            .expect("invalid payload should dead-letter");
        assert!(matches!(
            dead_letter_outcome,
            RoleCatalogSyncOutcome::DeadLettered
        ));
        let original_created_at: PrimitiveDateTime =
            sqlx::query("SELECT created_at FROM inbound_dead_letters WHERE source_event_id = $1")
                .bind("role-replay-001")
                .fetch_one(&pool)
                .await
                .expect("load original dead-letter timestamp")
                .get("created_at");

        let replay_job = InboundDeadLetterReplayJob::new(
            factory.clone(),
            role_service,
            career_service,
            memory_archive_service,
            tombstone_service,
        );
        let summary = replay_job
            .replay_inbound_dead_letters(10)
            .await
            .expect("replay should leave persistent failures pending");

        let dead_letter_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM inbound_dead_letters")
                .fetch_one(&pool)
                .await
                .expect("count dead letters");
        let replay_row = sqlx::query(
            "SELECT replay_status, created_at FROM inbound_dead_letters WHERE source_event_id = $1",
        )
        .bind("role-replay-001")
        .fetch_one(&pool)
        .await
        .expect("load replay row");

        assert_eq!(
            summary,
            ReplayInboundDeadLettersSummary {
                scanned: 1,
                replayed: 0,
                still_pending: 1,
            }
        );
        assert_eq!(dead_letter_count, 1);
        assert_eq!(replay_row.get::<String, _>("replay_status"), "pending");
        assert_eq!(
            replay_row.get::<PrimitiveDateTime, _>("created_at"),
            original_created_at
        );
    }

    #[tokio::test]
    async fn replay_inbound_dead_letter_replays_one_pending_row_after_dependency_is_restored() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let career_service = CareerEventConsumerService::new(factory.clone());
        let role_service = RoleCatalogSyncService::new(factory.clone());
        let memory_archive_service =
            MemoryRefsCommandService::new(factory.clone(), NoopMemoryArchivePort);
        let tombstone_service =
            TombstoneFlowService::new(factory.clone(), NoopGovernancePort, NoopArchiveRequester);

        let outcome = career_service
            .consume_process_event(sample_process_event(
                "career-single-replay-001",
                "career-single-replay-hash-001",
                "member-single-replay-001",
            ))
            .await
            .expect("missing member should dead-letter");
        assert!(matches!(
            outcome,
            crate::application::career_event::CareerEventOutcome::DeadLettered
        ));
        let dead_letter_id: String = sqlx::query(
            "SELECT dead_letter_id FROM inbound_dead_letters WHERE source_event_id = $1",
        )
        .bind("career-single-replay-001")
        .fetch_one(&pool)
        .await
        .expect("load single-replay dead-letter id")
        .get("dead_letter_id");

        insert_member(&pool, "member-single-replay-001", "Replay One Member").await;

        let replay_job = ReplayInboundDeadLetterJob::new(
            factory.clone(),
            role_service,
            career_service,
            memory_archive_service,
            tombstone_service,
        );
        let result = replay_job
            .replay_inbound_dead_letter(&DeadLetterId::new(dead_letter_id.clone()))
            .await
            .expect("single dead-letter replay should succeed");

        let replay_row =
            sqlx::query("SELECT replay_status FROM inbound_dead_letters WHERE dead_letter_id = $1")
                .bind(dead_letter_id.as_str())
                .fetch_one(&pool)
                .await
                .expect("load replayed dead-letter row");
        let career_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM career_history_entries WHERE source_event_id = $1",
        )
        .bind("career-single-replay-001")
        .fetch_one(&pool)
        .await
        .expect("count replayed career rows");

        assert_eq!(result, ReplayInboundDeadLetterResult { dead_letter_id });
        assert_eq!(replay_row.get::<String, _>("replay_status"), "replayed");
        assert_eq!(career_count, 1);
    }

    #[tokio::test]
    async fn replay_inbound_dead_letter_rejects_non_pending_rows() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;
        seed_dead_letter(
            &pool,
            InboundDeadLetter {
                dead_letter_id: DeadLetterId::new("dead-letter-replayed-001"),
                source_event_id: Some(EventId::new("dead-letter-source-replayed-001")),
                source_module: "work".to_string(),
                event_type: "work.fact.recorded".to_string(),
                payload_json: json!({ "global_member_id": "member-001" }),
                failure_reason: "already replayed".to_string(),
                replay_status: crate::domain::dead_letter::DeadLetterReplayStatus::Replayed,
                created_at: now(),
            },
        )
        .await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let replay_job = ReplayInboundDeadLetterJob::new(
            factory.clone(),
            RoleCatalogSyncService::new(factory.clone()),
            CareerEventConsumerService::new(factory.clone()),
            MemoryRefsCommandService::new(factory.clone(), NoopMemoryArchivePort),
            TombstoneFlowService::new(factory.clone(), NoopGovernancePort, NoopArchiveRequester),
        );
        let error = replay_job
            .replay_inbound_dead_letter(&DeadLetterId::new("dead-letter-replayed-001"))
            .await
            .expect_err("non-pending dead letters should be rejected");

        match error {
            IdentityError::RuleViolation { code, message } => {
                assert_eq!(code, "IDENTITY_DEAD_LETTER_NOT_PENDING");
                assert_eq!(
                    message,
                    "inbound dead letter `dead-letter-replayed-001` is not pending replay or review"
                );
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[tokio::test]
    async fn replay_inbound_dead_letter_surfaces_persistent_replay_failures() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let role_service = RoleCatalogSyncService::new(factory.clone());
        let career_service = CareerEventConsumerService::new(factory.clone());
        let memory_archive_service =
            MemoryRefsCommandService::new(factory.clone(), NoopMemoryArchivePort);
        let tombstone_service =
            TombstoneFlowService::new(factory.clone(), NoopGovernancePort, NoopArchiveRequester);

        let dead_letter_outcome = role_service
            .sync_role_catalog(InboundRoleCatalogEvent {
                envelope: InboundEventEnvelope {
                    source_event_id: "role-single-replay-001".into(),
                    source_module: "method-library".to_string(),
                    event_type: "role.definition.updated".to_string(),
                    occurred_at: now(),
                    payload_hash: "role-single-replay-hash-001".to_string(),
                    payload: json!({
                        "unexpected_field": true
                    }),
                },
            })
            .await
            .expect("invalid payload should dead-letter");
        assert!(matches!(
            dead_letter_outcome,
            RoleCatalogSyncOutcome::DeadLettered
        ));
        let dead_letter_id: String = sqlx::query(
            "SELECT dead_letter_id FROM inbound_dead_letters WHERE source_event_id = $1",
        )
        .bind("role-single-replay-001")
        .fetch_one(&pool)
        .await
        .expect("load persistent-failure dead-letter id")
        .get("dead_letter_id");

        let replay_job = ReplayInboundDeadLetterJob::new(
            factory.clone(),
            role_service,
            career_service,
            memory_archive_service,
            tombstone_service,
        );
        let error = replay_job
            .replay_inbound_dead_letter(&DeadLetterId::new(dead_letter_id.clone()))
            .await
            .expect_err("persistent replay failures should surface");

        let replay_row =
            sqlx::query("SELECT replay_status FROM inbound_dead_letters WHERE dead_letter_id = $1")
                .bind(dead_letter_id.as_str())
                .fetch_one(&pool)
                .await
                .expect("load pending replay row");

        match error {
            IdentityError::RuleViolation { code, message } => {
                assert_eq!(code, "IDENTITY_DEAD_LETTER_REPLAY_FAILED");
                assert_eq!(
                    message,
                    format!(
                        "inbound dead letter `{}` replay did not complete successfully",
                        dead_letter_id
                    )
                );
            }
            other => panic!("unexpected error: {other}"),
        }
        assert_eq!(replay_row.get::<String, _>("replay_status"), "pending");
    }

    #[tokio::test]
    async fn replay_inbound_dead_letters_replays_memory_archive_event_after_memory_refs_exist() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let role_service = RoleCatalogSyncService::new(factory.clone());
        let career_service = CareerEventConsumerService::new(factory.clone());
        let memory_archive_service =
            MemoryRefsCommandService::new(factory.clone(), NoopMemoryArchivePort);
        let tombstone_service =
            TombstoneFlowService::new(factory.clone(), NoopGovernancePort, NoopArchiveRequester);

        let outcome = memory_archive_service
            .handle_archive_event(sample_memory_archive_event(
                "memory-archive-replay-001",
                "memory-archive-replay-hash-001",
                "member-memory-replay-001",
                "archived",
                now() + Duration::seconds(5),
            ))
            .await
            .expect("missing memory refs should dead-letter");
        assert!(matches!(
            outcome,
            crate::application::memory_refs::MemoryArchiveEventOutcome::DeadLettered
        ));
        let original_created_at: PrimitiveDateTime =
            sqlx::query("SELECT created_at FROM inbound_dead_letters WHERE source_event_id = $1")
                .bind("memory-archive-replay-001")
                .fetch_one(&pool)
                .await
                .expect("load archive dead-letter timestamp")
                .get("created_at");

        insert_member(&pool, "member-memory-replay-001", "Memory Replay Member").await;
        insert_memory_refs(&pool, "member-memory-replay-001", ArchiveStatus::None, None).await;

        let replay_job = InboundDeadLetterReplayJob::new(
            factory.clone(),
            role_service,
            career_service,
            memory_archive_service,
            tombstone_service,
        );
        let summary = replay_job
            .replay_inbound_dead_letters(10)
            .await
            .expect("archive replay should succeed after refs exist");

        let replay_row = sqlx::query(
            "SELECT replay_status, created_at FROM inbound_dead_letters WHERE source_event_id = $1",
        )
        .bind("memory-archive-replay-001")
        .fetch_one(&pool)
        .await
        .expect("load archive replay row");
        let memory_refs_row = sqlx::query(
            "SELECT archive_status, archive_ref_json FROM memory_refs WHERE global_member_id = $1",
        )
        .bind("member-memory-replay-001")
        .fetch_one(&pool)
        .await
        .expect("load replayed memory refs row");
        let outbox_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM outbox_events WHERE event_type = 'identity.memory_refs.archive_status_changed' AND idempotency_key = $1",
        )
        .bind("memory-archive-replay-001")
        .fetch_one(&pool)
        .await
        .expect("count archive outbox rows");

        assert_eq!(
            summary,
            ReplayInboundDeadLettersSummary {
                scanned: 1,
                replayed: 1,
                still_pending: 0,
            }
        );
        assert_eq!(replay_row.get::<String, _>("replay_status"), "replayed");
        assert_eq!(
            replay_row.get::<PrimitiveDateTime, _>("created_at"),
            original_created_at
        );
        assert_eq!(
            memory_refs_row.get::<String, _>("archive_status"),
            "archived"
        );
        assert_eq!(
            memory_refs_row
                .get::<Option<serde_json::Value>, _>("archive_ref_json")
                .expect("archive ref should exist"),
            json!({
                "archive_id": "archive-member-memory-replay-001",
                "archive_kind": "member_memory_archive",
                "archive_version": "v1",
            })
        );
        assert_eq!(outbox_count, 1);
    }

    #[tokio::test]
    async fn ignore_inbound_dead_letter_marks_pending_row_ignored() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;
        let created_at = now();
        seed_dead_letter(
            &pool,
            InboundDeadLetter {
                dead_letter_id: DeadLetterId::new("dead-letter-ignore-001"),
                source_event_id: Some(EventId::new("dead-letter-source-001")),
                source_module: "work".to_string(),
                event_type: "work.fact.recorded".to_string(),
                payload_json: json!({ "global_member_id": "member-001" }),
                failure_reason: "member missing".to_string(),
                replay_status: crate::domain::dead_letter::DeadLetterReplayStatus::Pending,
                created_at,
            },
        )
        .await;
        let stored_created_at: PrimitiveDateTime =
            sqlx::query("SELECT created_at FROM inbound_dead_letters WHERE dead_letter_id = $1")
                .bind("dead-letter-ignore-001")
                .fetch_one(&pool)
                .await
                .expect("load stored dead-letter timestamp")
                .get("created_at");

        let job = IgnoreInboundDeadLetterJob::new(SqlxUnitOfWorkFactory::new(pool.clone()));
        let result = job
            .ignore_inbound_dead_letter(
                &DeadLetterId::new("dead-letter-ignore-001"),
                "ignored after manual review",
            )
            .await
            .expect("ignore should succeed for pending dead letters");

        let row = sqlx::query(
            "SELECT replay_status, failure_reason, created_at FROM inbound_dead_letters WHERE dead_letter_id = $1",
        )
        .bind("dead-letter-ignore-001")
        .fetch_one(&pool)
        .await
        .expect("load ignored dead-letter row");

        assert_eq!(
            result,
            IgnoreInboundDeadLetterResult {
                dead_letter_id: "dead-letter-ignore-001".to_string(),
            }
        );
        assert_eq!(row.get::<String, _>("replay_status"), "ignored");
        assert_eq!(
            row.get::<String, _>("failure_reason"),
            "ignored after manual review"
        );
        assert_eq!(
            row.get::<PrimitiveDateTime, _>("created_at"),
            stored_created_at
        );
    }

    #[tokio::test]
    async fn ignore_inbound_dead_letter_rejects_missing_rows() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;

        let job = IgnoreInboundDeadLetterJob::new(SqlxUnitOfWorkFactory::new(pool.clone()));
        let error = job
            .ignore_inbound_dead_letter(
                &DeadLetterId::new("dead-letter-missing-001"),
                "ignored after manual review",
            )
            .await
            .expect_err("missing dead letters should be rejected");

        match error {
            IdentityError::RuleViolation { code, message } => {
                assert_eq!(code, "IDENTITY_DEAD_LETTER_NOT_FOUND");
                assert!(message.contains("dead-letter-missing-001"));
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[tokio::test]
    async fn ignore_inbound_dead_letter_rejects_non_pending_rows() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;
        seed_dead_letter(
            &pool,
            InboundDeadLetter {
                dead_letter_id: DeadLetterId::new("dead-letter-ignore-002"),
                source_event_id: Some(EventId::new("dead-letter-source-002")),
                source_module: "work".to_string(),
                event_type: "work.fact.recorded".to_string(),
                payload_json: json!({ "global_member_id": "member-002" }),
                failure_reason: "already handled".to_string(),
                replay_status: crate::domain::dead_letter::DeadLetterReplayStatus::Replayed,
                created_at: now(),
            },
        )
        .await;

        let job = IgnoreInboundDeadLetterJob::new(SqlxUnitOfWorkFactory::new(pool.clone()));
        let error = job
            .ignore_inbound_dead_letter(
                &DeadLetterId::new("dead-letter-ignore-002"),
                "ignored after manual review",
            )
            .await
            .expect_err("non-pending dead letters should be rejected");

        match error {
            IdentityError::RuleViolation { code, message } => {
                assert_eq!(code, "IDENTITY_DEAD_LETTER_NOT_PENDING");
                assert!(message.contains("dead-letter-ignore-002"));
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[tokio::test]
    async fn ignore_inbound_dead_letter_rejects_blank_reason() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;
        seed_dead_letter(
            &pool,
            InboundDeadLetter {
                dead_letter_id: DeadLetterId::new("dead-letter-ignore-003"),
                source_event_id: Some(EventId::new("dead-letter-source-003")),
                source_module: "work".to_string(),
                event_type: "work.fact.recorded".to_string(),
                payload_json: json!({ "global_member_id": "member-003" }),
                failure_reason: "member missing".to_string(),
                replay_status: crate::domain::dead_letter::DeadLetterReplayStatus::Pending,
                created_at: now(),
            },
        )
        .await;

        let job = IgnoreInboundDeadLetterJob::new(SqlxUnitOfWorkFactory::new(pool.clone()));
        let error = job
            .ignore_inbound_dead_letter(&DeadLetterId::new("dead-letter-ignore-003"), "   ")
            .await
            .expect_err("blank ignore reason should be rejected");

        match error {
            IdentityError::RuleViolation { code, message } => {
                assert_eq!(code, "IDENTITY_INVALID_ARGUMENT");
                assert_eq!(message, "ignore reason must not be blank");
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[tokio::test]
    async fn rebuild_member_summary_projection_applies_member_events_and_advances_checkpoint() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let first_created_at = now();
        let second_created_at = first_created_at + Duration::seconds(1);
        seed_outbox_event(
            &pool,
            sample_role_catalog_synced_outbox_event(
                "outbox-role-001",
                "role.member.operator",
                "Member Operator",
                first_created_at,
            ),
        )
        .await;
        seed_outbox_event(
            &pool,
            sample_member_created_projection_event(
                "outbox-member-001",
                "member-001",
                "Member Zero One",
                "role.member.operator",
                second_created_at,
            ),
        )
        .await;

        let job = ProjectionRebuildJob::new(SqlxUnitOfWorkFactory::new(pool.clone()));
        let summary = job
            .rebuild_member_summary_projection("member-summary-rebuild", 10)
            .await
            .expect("projection rebuild should succeed");

        let projection_row = sqlx::query(
            r#"
            SELECT
                display_name,
                lifecycle,
                main_role_id,
                main_role_name,
                capability_summary_json,
                career_summary_json,
                memory_ref_summary_json,
                projection_version
            FROM member_summary_projection
            WHERE global_member_id = $1
            "#,
        )
        .bind("member-001")
        .fetch_one(&pool)
        .await
        .expect("load member summary projection row");
        let checkpoint_row = sqlx::query(
            r#"
            SELECT last_processed_event_id, status, failure_reason
            FROM projection_checkpoints
            WHERE checkpoint_name = $1
            "#,
        )
        .bind("member-summary-rebuild")
        .fetch_one(&pool)
        .await
        .expect("load projection checkpoint");

        assert_eq!(
            summary,
            RebuildMemberSummaryProjectionSummary {
                scanned: 2,
                rebuilt: 1,
                skipped: 1,
            }
        );
        assert_eq!(
            projection_row.get::<String, _>("display_name"),
            "Member Zero One"
        );
        assert_eq!(projection_row.get::<String, _>("lifecycle"), "hired");
        assert_eq!(
            projection_row.get::<Option<String>, _>("main_role_id"),
            Some("role.member.operator".to_string())
        );
        assert_eq!(
            projection_row.get::<Option<String>, _>("main_role_name"),
            Some("Member Operator".to_string())
        );
        assert_eq!(
            projection_row.get::<serde_json::Value, _>("capability_summary_json"),
            json!({})
        );
        assert_eq!(
            projection_row.get::<serde_json::Value, _>("career_summary_json"),
            json!({})
        );
        assert_eq!(
            projection_row.get::<serde_json::Value, _>("memory_ref_summary_json"),
            json!({})
        );
        assert_eq!(projection_row.get::<i64, _>("projection_version"), 0);
        assert_eq!(
            checkpoint_row.get::<Option<String>, _>("last_processed_event_id"),
            Some("outbox-member-001".to_string())
        );
        assert_eq!(checkpoint_row.get::<String, _>("status"), "idle");
        assert_eq!(
            checkpoint_row.get::<Option<String>, _>("failure_reason"),
            None
        );
    }

    #[tokio::test]
    async fn rebuild_member_summary_projection_resumes_after_checkpoint_cursor() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let first_created_at = now();
        let second_created_at = first_created_at + Duration::seconds(1);
        seed_outbox_event(
            &pool,
            sample_member_created_projection_event(
                "outbox-member-101",
                "member-101",
                "Member One Zero One",
                "role.member.operator",
                first_created_at,
            ),
        )
        .await;
        seed_outbox_event(
            &pool,
            sample_member_created_projection_event(
                "outbox-member-102",
                "member-102",
                "Member One Zero Two",
                "role.member.operator",
                second_created_at,
            ),
        )
        .await;
        insert_checkpoint(&pool, "member-summary-rebuild", Some("outbox-member-101")).await;

        let job = ProjectionRebuildJob::new(SqlxUnitOfWorkFactory::new(pool.clone()));
        let summary = job
            .rebuild_member_summary_projection("member-summary-rebuild", 10)
            .await
            .expect("projection rebuild should resume after checkpoint");

        let first_projection_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM member_summary_projection WHERE global_member_id = $1",
        )
        .bind("member-101")
        .fetch_one(&pool)
        .await
        .expect("count first projection rows");
        let second_projection_row = sqlx::query(
            r#"
            SELECT display_name, main_role_name
            FROM member_summary_projection
            WHERE global_member_id = $1
            "#,
        )
        .bind("member-102")
        .fetch_one(&pool)
        .await
        .expect("load resumed projection row");
        let checkpoint_row = sqlx::query(
            "SELECT last_processed_event_id, status FROM projection_checkpoints WHERE checkpoint_name = $1",
        )
        .bind("member-summary-rebuild")
        .fetch_one(&pool)
        .await
        .expect("load resumed checkpoint");

        assert_eq!(
            summary,
            RebuildMemberSummaryProjectionSummary {
                scanned: 1,
                rebuilt: 1,
                skipped: 0,
            }
        );
        assert_eq!(first_projection_count, 0);
        assert_eq!(
            second_projection_row.get::<String, _>("display_name"),
            "Member One Zero Two"
        );
        assert_eq!(
            second_projection_row.get::<Option<String>, _>("main_role_name"),
            Some("Member Operator".to_string())
        );
        assert_eq!(
            checkpoint_row.get::<Option<String>, _>("last_processed_event_id"),
            Some("outbox-member-102".to_string())
        );
        assert_eq!(checkpoint_row.get::<String, _>("status"), "idle");
    }

    #[tokio::test]
    async fn rebuild_member_summary_projection_merges_capability_updates_into_existing_projection()
    {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let first_created_at = now();
        let second_created_at = first_created_at + Duration::seconds(1);
        seed_outbox_event(
            &pool,
            sample_member_created_projection_event(
                "outbox-member-151",
                "member-151",
                "Member One Five One",
                "role.member.operator",
                first_created_at,
            ),
        )
        .await;

        let job = ProjectionRebuildJob::new(SqlxUnitOfWorkFactory::new(pool.clone()));
        job.rebuild_member_summary_projection("member-summary-rebuild", 10)
            .await
            .expect("initial projection rebuild should succeed");

        sqlx::query(
            r#"
            UPDATE member_summary_projection
            SET
                career_summary_json = $2,
                memory_ref_summary_json = $3
            WHERE global_member_id = $1
            "#,
        )
        .bind("member-151")
        .bind(json!({ "entries": 2 }))
        .bind(json!({ "refs": ["memory-001"] }))
        .execute(&pool)
        .await
        .expect("seed existing projection summaries");

        seed_outbox_event(
            &pool,
            sample_capability_profile_updated_projection_event(
                "outbox-capability-151",
                "capability-profile:member-151",
                "member-151",
                "Member One Five One",
                "role.member.operator",
                second_created_at,
            ),
        )
        .await;

        let summary = job
            .rebuild_member_summary_projection("member-summary-rebuild", 10)
            .await
            .expect("capability projection rebuild should succeed");

        let projection_row = sqlx::query(
            r#"
            SELECT
                display_name,
                main_role_name,
                capability_summary_json,
                career_summary_json,
                memory_ref_summary_json,
                projection_version
            FROM member_summary_projection
            WHERE global_member_id = $1
            "#,
        )
        .bind("member-151")
        .fetch_one(&pool)
        .await
        .expect("load projection after capability update");
        let checkpoint_row = sqlx::query(
            "SELECT last_processed_event_id, status FROM projection_checkpoints WHERE checkpoint_name = $1",
        )
        .bind("member-summary-rebuild")
        .fetch_one(&pool)
        .await
        .expect("load projection checkpoint after capability update");

        assert_eq!(
            summary,
            RebuildMemberSummaryProjectionSummary {
                scanned: 1,
                rebuilt: 1,
                skipped: 0,
            }
        );
        assert_eq!(
            projection_row.get::<String, _>("display_name"),
            "Member One Five One"
        );
        assert_eq!(
            projection_row.get::<Option<String>, _>("main_role_name"),
            Some("Member Operator".to_string())
        );
        assert_eq!(
            projection_row.get::<serde_json::Value, _>("capability_summary_json"),
            json!({
                "capability_profile_id": "capability-profile:member-151",
                "items": [
                    {
                        "capability_id": "capability.rust",
                        "capability_name": "Rust",
                        "proficiency": "advanced",
                        "notes": "systems programming",
                    }
                ],
                "evidence_refs": [
                    {
                        "artifact_id": "artifact-151",
                        "artifact_kind": "evidence",
                        "artifact_version": "v1",
                    }
                ],
                "version": 1,
            })
        );
        assert_eq!(
            projection_row.get::<serde_json::Value, _>("career_summary_json"),
            json!({ "entries": 2 })
        );
        assert_eq!(
            projection_row.get::<serde_json::Value, _>("memory_ref_summary_json"),
            json!({ "refs": ["memory-001"] })
        );
        assert_eq!(projection_row.get::<i64, _>("projection_version"), 1);
        assert_eq!(
            checkpoint_row.get::<Option<String>, _>("last_processed_event_id"),
            Some("outbox-capability-151".to_string())
        );
        assert_eq!(checkpoint_row.get::<String, _>("status"), "idle");
    }

    #[tokio::test]
    async fn rebuild_member_summary_projection_merges_career_updates_into_existing_projection() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let first_created_at = now();
        let second_created_at = first_created_at + Duration::seconds(1);
        seed_outbox_event(
            &pool,
            sample_member_created_projection_event(
                "outbox-member-156",
                "member-156",
                "Member One Five Six",
                "role.member.operator",
                first_created_at,
            ),
        )
        .await;

        let job = ProjectionRebuildJob::new(SqlxUnitOfWorkFactory::new(pool.clone()));
        job.rebuild_member_summary_projection("member-summary-rebuild", 10)
            .await
            .expect("initial projection rebuild should succeed");

        sqlx::query(
            r#"
            UPDATE member_summary_projection
            SET
                capability_summary_json = $2,
                memory_ref_summary_json = $3
            WHERE global_member_id = $1
            "#,
        )
        .bind("member-156")
        .bind(json!({ "items": ["capability.design"] }))
        .bind(json!({ "refs": ["memory-156"] }))
        .execute(&pool)
        .await
        .expect("seed existing projection summaries");

        seed_outbox_event(
            &pool,
            sample_career_history_appended_projection_event(
                "outbox-career-156",
                "member-156",
                "Member One Five Six",
                "role.member.operator",
                second_created_at,
            ),
        )
        .await;

        let summary = job
            .rebuild_member_summary_projection("member-summary-rebuild", 10)
            .await
            .expect("career projection rebuild should succeed");

        let projection_row = sqlx::query(
            r#"
            SELECT
                display_name,
                main_role_name,
                capability_summary_json,
                career_summary_json,
                memory_ref_summary_json,
                projection_version
            FROM member_summary_projection
            WHERE global_member_id = $1
            "#,
        )
        .bind("member-156")
        .fetch_one(&pool)
        .await
        .expect("load projection after career update");
        let checkpoint_row = sqlx::query(
            "SELECT last_processed_event_id, status FROM projection_checkpoints WHERE checkpoint_name = $1",
        )
        .bind("member-summary-rebuild")
        .fetch_one(&pool)
        .await
        .expect("load projection checkpoint after career update");

        assert_eq!(
            summary,
            RebuildMemberSummaryProjectionSummary {
                scanned: 1,
                rebuilt: 1,
                skipped: 0,
            }
        );
        assert_eq!(
            projection_row.get::<String, _>("display_name"),
            "Member One Five Six"
        );
        assert_eq!(
            projection_row.get::<Option<String>, _>("main_role_name"),
            Some("Member Operator".to_string())
        );
        assert_eq!(
            projection_row.get::<serde_json::Value, _>("capability_summary_json"),
            json!({ "items": ["capability.design"] })
        );
        assert_eq!(
            projection_row.get::<serde_json::Value, _>("career_summary_json"),
            json!({
                "global_member_id": "member-156",
                "entry_count": 2,
                "entries": [
                    {
                        "career_entry_id": "career-entry:career-event-156-a",
                        "source_event_id": "career-event-156-a",
                        "source_module": "work",
                        "project_id": "project-156",
                        "work_ref": {
                            "work_id": "work-156-a",
                            "work_kind": "task",
                            "work_version": "v1",
                        },
                        "process_ref": null,
                        "entry_kind": "assigned",
                        "started_at": second_created_at,
                        "ended_at": second_created_at + Duration::seconds(1),
                        "payload_summary": {
                            "title": "Design projection merge",
                        },
                        "created_at": second_created_at,
                    },
                    {
                        "career_entry_id": "career-entry:career-event-156-b",
                        "source_event_id": "career-event-156-b",
                        "source_module": "process",
                        "project_id": "project-156",
                        "work_ref": null,
                        "process_ref": {
                            "process_id": "process-156-b",
                            "process_kind": "review",
                            "process_version": "v2",
                        },
                        "entry_kind": "reviewed",
                        "started_at": second_created_at + Duration::seconds(1),
                        "ended_at": second_created_at + Duration::seconds(16),
                        "payload_summary": {
                            "activity": "Projection QA",
                        },
                        "created_at": second_created_at + Duration::seconds(1),
                    }
                ]
            })
        );
        assert_eq!(
            projection_row.get::<serde_json::Value, _>("memory_ref_summary_json"),
            json!({ "refs": ["memory-156"] })
        );
        assert_eq!(projection_row.get::<i64, _>("projection_version"), 2);
        assert_eq!(
            checkpoint_row.get::<Option<String>, _>("last_processed_event_id"),
            Some("outbox-career-156".to_string())
        );
        assert_eq!(checkpoint_row.get::<String, _>("status"), "idle");
    }

    #[tokio::test]
    async fn rebuild_member_summary_projection_merges_memory_ref_updates_into_existing_projection()
    {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let first_created_at = now();
        let second_created_at = first_created_at + Duration::seconds(1);
        seed_outbox_event(
            &pool,
            sample_member_created_projection_event(
                "outbox-member-161",
                "member-161",
                "Member One Six One",
                "role.member.operator",
                first_created_at,
            ),
        )
        .await;

        let job = ProjectionRebuildJob::new(SqlxUnitOfWorkFactory::new(pool.clone()));
        job.rebuild_member_summary_projection("member-summary-rebuild", 10)
            .await
            .expect("initial projection rebuild should succeed");

        sqlx::query(
            r#"
            UPDATE member_summary_projection
            SET
                capability_summary_json = $2,
                career_summary_json = $3
            WHERE global_member_id = $1
            "#,
        )
        .bind("member-161")
        .bind(json!({ "items": ["capability.rust"] }))
        .bind(json!({ "entries": 4 }))
        .execute(&pool)
        .await
        .expect("seed existing projection summaries");

        seed_outbox_event(
            &pool,
            sample_memory_refs_updated_projection_event(
                "outbox-memory-161",
                "memory-refs:member-161",
                "member-161",
                "Member One Six One",
                "role.member.operator",
                second_created_at,
            ),
        )
        .await;

        let summary = job
            .rebuild_member_summary_projection("member-summary-rebuild", 10)
            .await
            .expect("memory refs projection rebuild should succeed");

        let projection_row = sqlx::query(
            r#"
            SELECT
                display_name,
                main_role_name,
                capability_summary_json,
                career_summary_json,
                memory_ref_summary_json,
                projection_version
            FROM member_summary_projection
            WHERE global_member_id = $1
            "#,
        )
        .bind("member-161")
        .fetch_one(&pool)
        .await
        .expect("load projection after memory refs update");
        let checkpoint_row = sqlx::query(
            "SELECT last_processed_event_id, status FROM projection_checkpoints WHERE checkpoint_name = $1",
        )
        .bind("member-summary-rebuild")
        .fetch_one(&pool)
        .await
        .expect("load projection checkpoint after memory refs update");

        assert_eq!(
            summary,
            RebuildMemberSummaryProjectionSummary {
                scanned: 1,
                rebuilt: 1,
                skipped: 0,
            }
        );
        assert_eq!(
            projection_row.get::<String, _>("display_name"),
            "Member One Six One"
        );
        assert_eq!(
            projection_row.get::<Option<String>, _>("main_role_name"),
            Some("Member Operator".to_string())
        );
        assert_eq!(
            projection_row.get::<serde_json::Value, _>("capability_summary_json"),
            json!({ "items": ["capability.rust"] })
        );
        assert_eq!(
            projection_row.get::<serde_json::Value, _>("career_summary_json"),
            json!({ "entries": 4 })
        );
        assert_eq!(
            projection_row.get::<serde_json::Value, _>("memory_ref_summary_json"),
            json!({
                "memory_refs_id": "memory-refs:member-161",
                "semantic_memory_ref": {
                    "memory_id": "memory-semantic-161",
                    "memory_kind": "semantic",
                    "memory_version": "v1",
                },
                "episodic_memory_refs": [
                    {
                        "memory_id": "memory-episodic-161",
                        "memory_kind": "episodic",
                        "memory_version": "v1",
                    }
                ],
                "archive_ref": null,
                "archive_status": "none",
                "version": 2,
            })
        );
        assert_eq!(projection_row.get::<i64, _>("projection_version"), 2);
        assert_eq!(
            checkpoint_row.get::<Option<String>, _>("last_processed_event_id"),
            Some("outbox-memory-161".to_string())
        );
        assert_eq!(checkpoint_row.get::<String, _>("status"), "idle");
    }

    #[tokio::test]
    async fn rebuild_member_summary_projection_marks_checkpoint_failed_without_advancing_past_bad_event()
     {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let first_created_at = now();
        let second_created_at = first_created_at + Duration::seconds(1);
        seed_outbox_event(
            &pool,
            sample_member_created_projection_event(
                "outbox-member-201",
                "member-201",
                "Member Two Zero One",
                "role.member.operator",
                first_created_at,
            ),
        )
        .await;
        let mut invalid_event = sample_member_created_projection_event(
            "outbox-member-202",
            "member-202",
            "Member Two Zero Two",
            "role.member.operator",
            second_created_at,
        );
        invalid_event.payload_json["lifecycle"] = json!("unknown");
        seed_outbox_event(&pool, invalid_event).await;

        let job = ProjectionRebuildJob::new(SqlxUnitOfWorkFactory::new(pool.clone()));
        let error = job
            .rebuild_member_summary_projection("member-summary-rebuild", 10)
            .await
            .expect_err("projection rebuild should fail on invalid payload");

        let good_projection_row = sqlx::query(
            "SELECT display_name FROM member_summary_projection WHERE global_member_id = $1",
        )
        .bind("member-201")
        .fetch_one(&pool)
        .await
        .expect("load successful projection written before failure");
        let bad_projection_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM member_summary_projection WHERE global_member_id = $1",
        )
        .bind("member-202")
        .fetch_one(&pool)
        .await
        .expect("count invalid projection rows");
        let checkpoint_row = sqlx::query(
            r#"
            SELECT last_processed_event_id, status, failure_reason
            FROM projection_checkpoints
            WHERE checkpoint_name = $1
            "#,
        )
        .bind("member-summary-rebuild")
        .fetch_one(&pool)
        .await
        .expect("load failed checkpoint");

        assert!(
            error
                .to_string()
                .contains("invalid lifecycle `unknown` in member-created outbox payload")
        );
        assert_eq!(
            good_projection_row.get::<String, _>("display_name"),
            "Member Two Zero One"
        );
        assert_eq!(bad_projection_count, 0);
        assert_eq!(
            checkpoint_row.get::<Option<String>, _>("last_processed_event_id"),
            Some("outbox-member-201".to_string())
        );
        assert_eq!(checkpoint_row.get::<String, _>("status"), "failed");
        assert!(
            checkpoint_row
                .get::<Option<String>, _>("failure_reason")
                .expect("failure reason should be recorded")
                .contains("invalid lifecycle `unknown` in member-created outbox payload")
        );
    }

    #[tokio::test]
    async fn rebuild_member_summary_projection_applies_archive_status_changed_events() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let first_created_at = now();
        let second_created_at = first_created_at + Duration::seconds(1);
        seed_outbox_event(
            &pool,
            sample_member_created_projection_event(
                "outbox-member-162",
                "member-162",
                "Member One Six Two",
                "role.member.operator",
                first_created_at,
            ),
        )
        .await;

        let job = ProjectionRebuildJob::new(SqlxUnitOfWorkFactory::new(pool.clone()));
        job.rebuild_member_summary_projection("member-summary-rebuild", 10)
            .await
            .expect("initial projection rebuild should succeed");

        seed_outbox_event(
            &pool,
            sample_memory_archive_status_changed_projection_event(
                "outbox-memory-archive-162",
                "memory-refs:member-162",
                "member-162",
                "Member One Six Two",
                "role.member.operator",
                second_created_at,
            ),
        )
        .await;

        let summary = job
            .rebuild_member_summary_projection("member-summary-rebuild", 10)
            .await
            .expect("archive-status projection rebuild should succeed");

        let projection_row = sqlx::query(
            r#"
            SELECT
                memory_ref_summary_json,
                projection_version
            FROM member_summary_projection
            WHERE global_member_id = $1
            "#,
        )
        .bind("member-162")
        .fetch_one(&pool)
        .await
        .expect("load projection after archive status update");

        assert_eq!(
            summary,
            RebuildMemberSummaryProjectionSummary {
                scanned: 1,
                rebuilt: 1,
                skipped: 0,
            }
        );
        assert_eq!(
            projection_row.get::<serde_json::Value, _>("memory_ref_summary_json"),
            json!({
                "memory_refs_id": "memory-refs:member-162",
                "semantic_memory_ref": {
                    "memory_id": "memory-semantic-162",
                    "memory_kind": "semantic",
                    "memory_version": "v1",
                },
                "episodic_memory_refs": [
                    {
                        "memory_id": "memory-episodic-162",
                        "memory_kind": "episodic",
                        "memory_version": "v1",
                    }
                ],
                "archive_ref": {
                    "archive_id": "archive-member-162",
                    "archive_kind": "member_memory_archive",
                    "archive_version": "v1",
                },
                "archive_status": "archived",
                "version": 3,
            })
        );
        assert_eq!(projection_row.get::<i64, _>("projection_version"), 3);
    }

    #[tokio::test]
    async fn reset_projection_checkpoint_clears_cursor_and_failure_state() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;
        seed_outbox_event(&pool, sample_pending_outbox_event("outbox-member-999")).await;

        sqlx::query(
            r#"
            INSERT INTO projection_checkpoints (
                checkpoint_name,
                last_processed_event_id,
                status,
                failure_reason,
                updated_at
            ) VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind("member-summary-rebuild")
        .bind(Some("outbox-member-999"))
        .bind("failed")
        .bind(Some("projection payload was invalid"))
        .bind(now())
        .execute(&pool)
        .await
        .expect("seed failed checkpoint");

        let job = ResetProjectionCheckpointJob::new(SqlxUnitOfWorkFactory::new(pool.clone()));
        let result = job
            .reset_projection_checkpoint("member-summary-rebuild")
            .await
            .expect("checkpoint reset should succeed");

        let row = sqlx::query(
            "SELECT last_processed_event_id, status, failure_reason FROM projection_checkpoints WHERE checkpoint_name = $1",
        )
        .bind("member-summary-rebuild")
        .fetch_one(&pool)
        .await
        .expect("load reset checkpoint");

        assert_eq!(
            result,
            ResetProjectionCheckpointResult {
                checkpoint_name: "member-summary-rebuild".to_string(),
            }
        );
        assert_eq!(
            row.get::<Option<String>, _>("last_processed_event_id"),
            None
        );
        assert_eq!(row.get::<String, _>("status"), "idle");
        assert_eq!(row.get::<Option<String>, _>("failure_reason"), None);
    }

    #[tokio::test]
    async fn reset_projection_checkpoint_rejects_blank_names() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;

        let job = ResetProjectionCheckpointJob::new(SqlxUnitOfWorkFactory::new(pool.clone()));
        let error = job
            .reset_projection_checkpoint("   ")
            .await
            .expect_err("blank checkpoint names should be rejected");

        match error {
            IdentityError::RuleViolation { code, message } => {
                assert_eq!(code, "IDENTITY_INVALID_ARGUMENT");
                assert_eq!(message, "checkpoint_name must not be blank");
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[tokio::test]
    async fn reconcile_role_catalog_marks_fingerprint_drift_and_writes_audit_report() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let job = RoleReconciliationJob::new(
            SqlxUnitOfWorkFactory::new(pool.clone()),
            StubMethodLibraryRoleCatalogPort::succeed(vec![sample_role_snapshot(
                "role.member.operator",
                "Member Operator",
                "v2",
                "fingerprint-role.member.operator-upstream",
                "active",
            )]),
        );

        let summary = job
            .reconcile_role_catalog()
            .await
            .expect("reconciliation should succeed");

        let role_row = sqlx::query(
            "SELECT fingerprint, status, role_version FROM role_catalog_entries WHERE role_id = $1",
        )
        .bind("role.member.operator")
        .fetch_one(&pool)
        .await
        .expect("load reconciled role row");
        let audit_row = sqlx::query(
            "SELECT action, source_module, result, target_ref_json FROM audit_trace_entries WHERE action = $1",
        )
        .bind("ReconcileRoleCatalog")
        .fetch_one(&pool)
        .await
        .expect("load reconciliation audit trace");

        assert_eq!(
            summary,
            ReconcileRoleCatalogSummary {
                scanned: 1,
                missing: 0,
                refreshed: 0,
                deprecated: 0,
                drifted: 1,
                local_only: 0,
                unchanged: 0,
            }
        );
        assert_eq!(
            role_row.get::<String, _>("fingerprint"),
            "fingerprint-role.member.operator-upstream"
        );
        assert_eq!(role_row.get::<String, _>("status"), "source_drift");
        assert_eq!(role_row.get::<String, _>("role_version"), "v1");
        assert_eq!(audit_row.get::<String, _>("action"), "ReconcileRoleCatalog");
        assert_eq!(
            audit_row.get::<Option<String>, _>("source_module"),
            Some("operations".to_string())
        );
        assert_eq!(audit_row.get::<String, _>("result"), "success");
        assert_eq!(
            audit_row.get::<serde_json::Value, _>("target_ref_json"),
            json!({
                "kind": "role_catalog_reconciliation_report",
                "scanned": 1,
                "missing": 0,
                "refreshed": 0,
                "deprecated": 0,
                "drifted": 1,
                "local_only": 0,
                "unchanged": 0,
                "local_only_role_ids": [],
            })
        );
    }

    #[tokio::test]
    async fn reconcile_role_catalog_inserts_missing_local_index_rows_and_marks_deprecated_roles() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;
        seed_role(&pool, "role.legacy.reviewer", "Legacy Reviewer").await;

        let job = RoleReconciliationJob::new(
            SqlxUnitOfWorkFactory::new(pool.clone()),
            StubMethodLibraryRoleCatalogPort::succeed(vec![
                sample_role_snapshot(
                    "role.legacy.reviewer",
                    "Legacy Reviewer",
                    "v3",
                    "fingerprint-role.legacy.reviewer-upstream",
                    "deprecated",
                ),
                sample_role_snapshot(
                    "role.new.architect",
                    "New Architect",
                    "v1",
                    "fingerprint-role.new.architect",
                    "active",
                ),
            ]),
        );

        let summary = job
            .reconcile_role_catalog()
            .await
            .expect("reconciliation should insert and deprecate rows");

        let deprecated_row = sqlx::query(
            "SELECT status, fingerprint, role_version FROM role_catalog_entries WHERE role_id = $1",
        )
        .bind("role.legacy.reviewer")
        .fetch_one(&pool)
        .await
        .expect("load deprecated role row");
        let inserted_row = sqlx::query(
            "SELECT role_name, fingerprint, status FROM role_catalog_entries WHERE role_id = $1",
        )
        .bind("role.new.architect")
        .fetch_one(&pool)
        .await
        .expect("load inserted role row");

        assert_eq!(
            summary,
            ReconcileRoleCatalogSummary {
                scanned: 2,
                missing: 1,
                refreshed: 0,
                deprecated: 1,
                drifted: 0,
                local_only: 0,
                unchanged: 0,
            }
        );
        assert_eq!(deprecated_row.get::<String, _>("status"), "deprecated");
        assert_eq!(
            deprecated_row.get::<String, _>("fingerprint"),
            "fingerprint-role.legacy.reviewer-upstream"
        );
        assert_eq!(deprecated_row.get::<String, _>("role_version"), "v3");
        assert_eq!(inserted_row.get::<String, _>("role_name"), "New Architect");
        assert_eq!(
            inserted_row.get::<String, _>("fingerprint"),
            "fingerprint-role.new.architect"
        );
        assert_eq!(inserted_row.get::<String, _>("status"), "active");
    }

    #[tokio::test]
    async fn reconcile_role_catalog_reports_local_only_rows_without_mutating_them() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;
        seed_role(&pool, "role.local.orphan", "Local Orphan").await;

        let job = RoleReconciliationJob::new(
            SqlxUnitOfWorkFactory::new(pool.clone()),
            StubMethodLibraryRoleCatalogPort::succeed(vec![]),
        );

        let summary = job
            .reconcile_role_catalog()
            .await
            .expect("reconciliation should still succeed with no source rows");

        let role_row =
            sqlx::query("SELECT fingerprint, status FROM role_catalog_entries WHERE role_id = $1")
                .bind("role.local.orphan")
                .fetch_one(&pool)
                .await
                .expect("load local-only role row");
        let audit_row = sqlx::query(
            "SELECT reason, target_ref_json FROM audit_trace_entries WHERE action = $1",
        )
        .bind("ReconcileRoleCatalog")
        .fetch_one(&pool)
        .await
        .expect("load local-only reconciliation audit");

        assert_eq!(
            summary,
            ReconcileRoleCatalogSummary {
                scanned: 0,
                missing: 0,
                refreshed: 0,
                deprecated: 0,
                drifted: 0,
                local_only: 1,
                unchanged: 0,
            }
        );
        assert_eq!(
            role_row.get::<String, _>("fingerprint"),
            "fingerprint-role.local.orphan"
        );
        assert_eq!(role_row.get::<String, _>("status"), "active");
        assert_eq!(
            audit_row.get::<Option<String>, _>("reason"),
            Some(
                "1 local role catalog entries are absent from the authoritative source snapshot"
                    .to_string()
            )
        );
        assert_eq!(
            audit_row.get::<serde_json::Value, _>("target_ref_json"),
            json!({
                "kind": "role_catalog_reconciliation_report",
                "scanned": 0,
                "missing": 0,
                "refreshed": 0,
                "deprecated": 0,
                "drifted": 0,
                "local_only": 1,
                "unchanged": 0,
                "local_only_role_ids": ["role.local.orphan"],
            })
        );
    }

    #[tokio::test]
    async fn reconcile_role_catalog_refreshes_local_summary_when_fingerprint_matches() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;
        sqlx::query(
            r#"
            INSERT INTO role_catalog_entries (
                role_id,
                role_name,
                role_version,
                source_ref_json,
                fingerprint,
                status,
                updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind("role.refreshed")
        .bind("Old Name")
        .bind("v1")
        .bind(json!({ "module": "method-library", "id": "role.refreshed" }))
        .bind("fingerprint-role.refreshed")
        .bind("active")
        .bind(now())
        .execute(&pool)
        .await
        .expect("seed refreshable role row");

        let job = RoleReconciliationJob::new(
            SqlxUnitOfWorkFactory::new(pool.clone()),
            StubMethodLibraryRoleCatalogPort::succeed(vec![sample_role_snapshot(
                "role.refreshed",
                "Refreshed Name",
                "v9",
                "fingerprint-role.refreshed",
                "active",
            )]),
        );

        let summary = job
            .reconcile_role_catalog()
            .await
            .expect("reconciliation should refresh local summary fields");

        let role_row = sqlx::query(
            "SELECT role_name, role_version, fingerprint, status FROM role_catalog_entries WHERE role_id = $1",
        )
        .bind("role.refreshed")
        .fetch_one(&pool)
        .await
        .expect("load refreshed role row");

        assert_eq!(
            summary,
            ReconcileRoleCatalogSummary {
                scanned: 1,
                missing: 0,
                refreshed: 1,
                deprecated: 0,
                drifted: 0,
                local_only: 0,
                unchanged: 0,
            }
        );
        assert_eq!(role_row.get::<String, _>("role_name"), "Refreshed Name");
        assert_eq!(role_row.get::<String, _>("role_version"), "v9");
        assert_eq!(
            role_row.get::<String, _>("fingerprint"),
            "fingerprint-role.refreshed"
        );
        assert_eq!(role_row.get::<String, _>("status"), "active");
    }

    #[tokio::test]
    async fn reconcile_role_catalog_surfaces_method_library_failures_with_failed_audit() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;

        let job = RoleReconciliationJob::new(
            SqlxUnitOfWorkFactory::new(pool.clone()),
            StubMethodLibraryRoleCatalogPort::fail("method-library catalog is unavailable"),
        );

        let error = job
            .reconcile_role_catalog()
            .await
            .expect_err("upstream failures should surface");

        let audit_row = sqlx::query(
            "SELECT result, reason, target_ref_json FROM audit_trace_entries WHERE action = $1",
        )
        .bind("ReconcileRoleCatalog")
        .fetch_one(&pool)
        .await
        .expect("load failed reconciliation audit");

        match error {
            IdentityError::RuleViolation { code, message } => {
                assert_eq!(code, "IDENTITY_METHOD_LIBRARY_UNAVAILABLE");
                assert_eq!(message, "method-library catalog is unavailable");
            }
            other => panic!("unexpected error: {other}"),
        }
        assert_eq!(audit_row.get::<String, _>("result"), "failed");
        assert_eq!(
            audit_row.get::<Option<String>, _>("reason"),
            Some(
                "IDENTITY_METHOD_LIBRARY_UNAVAILABLE: method-library catalog is unavailable"
                    .to_string()
            )
        );
        assert_eq!(
            audit_row.get::<serde_json::Value, _>("target_ref_json"),
            json!({
                "kind": "role_catalog_reconciliation_report",
                "scanned": 0,
                "missing": 0,
                "refreshed": 0,
                "deprecated": 0,
                "drifted": 0,
                "local_only": 0,
                "unchanged": 0,
                "local_only_role_ids": [],
            })
        );
    }

    #[tokio::test]
    async fn clear_idempotency_record_deletes_failed_record() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;
        seed_idempotency_record(
            &pool,
            "idem-failed-001",
            IdempotencyScope::Command,
            IdempotencyStatus::Failed,
        )
        .await;

        let job = ClearIdempotencyRecordJob::new(SqlxUnitOfWorkFactory::new(pool.clone()));
        let result = job
            .clear_idempotency_record("idem-failed-001", IdempotencyScope::Command)
            .await
            .expect("failed records should be clearable");

        let remaining_count: i64 = sqlx::query(
            "SELECT COUNT(*) AS count FROM idempotency_records WHERE idempotency_key = $1 AND scope = $2",
        )
        .bind("idem-failed-001")
        .bind(IdempotencyScope::Command.as_db())
        .fetch_one(&pool)
        .await
        .expect("count cleared idempotency record")
        .get("count");

        assert_eq!(
            result,
            ClearIdempotencyRecordResult {
                idempotency_key: "idem-failed-001".to_string(),
                scope: IdempotencyScope::Command,
            }
        );
        assert_eq!(remaining_count, 0);
    }

    #[tokio::test]
    async fn clear_idempotency_record_deletes_processing_record() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;
        seed_idempotency_record(
            &pool,
            "idem-processing-001",
            IdempotencyScope::InboundEvent,
            IdempotencyStatus::Processing,
        )
        .await;

        let job = ClearIdempotencyRecordJob::new(SqlxUnitOfWorkFactory::new(pool.clone()));
        let result = job
            .clear_idempotency_record("idem-processing-001", IdempotencyScope::InboundEvent)
            .await
            .expect("processing records should be clearable");

        let remaining_count: i64 = sqlx::query(
            "SELECT COUNT(*) AS count FROM idempotency_records WHERE idempotency_key = $1 AND scope = $2",
        )
        .bind("idem-processing-001")
        .bind(IdempotencyScope::InboundEvent.as_db())
        .fetch_one(&pool)
        .await
        .expect("count cleared idempotency record")
        .get("count");

        assert_eq!(
            result,
            ClearIdempotencyRecordResult {
                idempotency_key: "idem-processing-001".to_string(),
                scope: IdempotencyScope::InboundEvent,
            }
        );
        assert_eq!(remaining_count, 0);
    }

    #[tokio::test]
    async fn clear_idempotency_record_rejects_succeeded_record() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;
        seed_idempotency_record(
            &pool,
            "idem-succeeded-001",
            IdempotencyScope::Command,
            IdempotencyStatus::Succeeded,
        )
        .await;

        let job = ClearIdempotencyRecordJob::new(SqlxUnitOfWorkFactory::new(pool.clone()));
        let error = job
            .clear_idempotency_record("idem-succeeded-001", IdempotencyScope::Command)
            .await
            .expect_err("succeeded records must remain intact");

        let remaining_status: String = sqlx::query(
            "SELECT status FROM idempotency_records WHERE idempotency_key = $1 AND scope = $2",
        )
        .bind("idem-succeeded-001")
        .bind(IdempotencyScope::Command.as_db())
        .fetch_one(&pool)
        .await
        .expect("load succeeded idempotency record")
        .get("status");

        match error {
            IdentityError::RuleViolation { code, message } => {
                assert_eq!(code, "IDENTITY_IDEMPOTENCY_RECORD_NOT_CLEARABLE");
                assert_eq!(
                    message,
                    "idempotency record `idem-succeeded-001` in scope `command` has already succeeded and cannot be cleared"
                );
            }
            other => panic!("unexpected error: {other}"),
        }
        assert_eq!(remaining_status, IdempotencyStatus::Succeeded.as_db());
    }

    #[tokio::test]
    async fn clear_idempotency_record_rejects_missing_record() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;

        let job = ClearIdempotencyRecordJob::new(SqlxUnitOfWorkFactory::new(pool.clone()));
        let error = job
            .clear_idempotency_record("idem-missing-001", IdempotencyScope::OutboxPublish)
            .await
            .expect_err("missing records should be rejected");

        match error {
            IdentityError::RuleViolation { code, message } => {
                assert_eq!(code, "IDENTITY_IDEMPOTENCY_RECORD_NOT_FOUND");
                assert_eq!(
                    message,
                    "idempotency record `idem-missing-001` in scope `outbox_publish` was not found"
                );
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[tokio::test]
    async fn clear_idempotency_record_rejects_blank_keys() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;

        let job = ClearIdempotencyRecordJob::new(SqlxUnitOfWorkFactory::new(pool.clone()));
        let error = job
            .clear_idempotency_record("   ", IdempotencyScope::Command)
            .await
            .expect_err("blank keys should be rejected");

        match error {
            IdentityError::RuleViolation { code, message } => {
                assert_eq!(code, "IDENTITY_INVALID_ARGUMENT");
                assert_eq!(message, "idempotency_key must not be blank");
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[tokio::test]
    async fn rebuild_member_projection_rebuilds_one_member_from_persisted_outbox_facts() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let first_created_at = now();
        let second_created_at = first_created_at + Duration::seconds(1);
        let third_created_at = second_created_at + Duration::seconds(1);
        seed_outbox_event(
            &pool,
            sample_member_created_projection_event(
                "outbox-member-201",
                "member-201",
                "Member Two Zero One",
                "role.member.operator",
                first_created_at,
            ),
        )
        .await;
        seed_outbox_event(
            &pool,
            sample_capability_profile_updated_projection_event(
                "outbox-capability-201",
                "capability-profile:member-201",
                "member-201",
                "Member Two Zero One",
                "role.member.operator",
                second_created_at,
            ),
        )
        .await;
        seed_outbox_event(
            &pool,
            sample_career_history_appended_projection_event(
                "outbox-career-201",
                "member-201",
                "Member Two Zero One",
                "role.member.operator",
                third_created_at,
            ),
        )
        .await;
        seed_outbox_event(
            &pool,
            sample_member_created_projection_event(
                "outbox-member-999",
                "member-999",
                "Unrelated Member",
                "role.member.operator",
                third_created_at + Duration::seconds(1),
            ),
        )
        .await;

        sqlx::query(
            r#"
            INSERT INTO member_summary_projection (
                global_member_id,
                display_name,
                lifecycle,
                main_role_id,
                main_role_name,
                capability_summary_json,
                career_summary_json,
                memory_ref_summary_json,
                projection_version,
                updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#,
        )
        .bind("member-201")
        .bind("Corrupted Projection")
        .bind("paused")
        .bind(Option::<String>::None)
        .bind(Option::<String>::None)
        .bind(json!({ "corrupted": true }))
        .bind(json!({ "entries": ["bad"] }))
        .bind(json!({ "refs": ["bad"] }))
        .bind(99_i64)
        .bind(third_created_at)
        .execute(&pool)
        .await
        .expect("seed corrupted member projection");

        let job = RebuildMemberProjectionJob::new(SqlxUnitOfWorkFactory::new(pool.clone()));
        let result = job
            .rebuild_member_projection("member-201")
            .await
            .expect("point rebuild should succeed");

        let rebuilt_row = sqlx::query(
            r#"
            SELECT
                display_name,
                lifecycle,
                main_role_id,
                main_role_name,
                capability_summary_json,
                career_summary_json,
                memory_ref_summary_json,
                projection_version
            FROM member_summary_projection
            WHERE global_member_id = $1
            "#,
        )
        .bind("member-201")
        .fetch_one(&pool)
        .await
        .expect("load rebuilt member projection");
        let unrelated_row = sqlx::query(
            r#"
            SELECT display_name
            FROM member_summary_projection
            WHERE global_member_id = $1
            "#,
        )
        .bind("member-999")
        .fetch_optional(&pool)
        .await
        .expect("load unrelated projection row");

        assert_eq!(
            result,
            RebuildMemberProjectionResult {
                global_member_id: "member-201".to_string(),
                replayed_events: 3,
            }
        );
        assert_eq!(
            rebuilt_row.get::<String, _>("display_name"),
            "Member Two Zero One"
        );
        assert_eq!(rebuilt_row.get::<String, _>("lifecycle"), "hired");
        assert_eq!(
            rebuilt_row.get::<Option<String>, _>("main_role_name"),
            Some("Member Operator".to_string())
        );
        assert_eq!(rebuilt_row.get::<i64, _>("projection_version"), 2);
        assert_eq!(
            rebuilt_row.get::<serde_json::Value, _>("career_summary_json"),
            json!({
                "global_member_id": "member-201",
                "entry_count": 2,
                "entries": [
                    {
                        "career_entry_id": "career-entry:career-event-156-a",
                        "source_event_id": "career-event-156-a",
                        "source_module": "work",
                        "project_id": "project-156",
                        "work_ref": {
                            "work_id": "work-156-a",
                            "work_kind": "task",
                            "work_version": "v1",
                        },
                        "process_ref": null,
                        "entry_kind": "assigned",
                        "started_at": first_created_at + Duration::seconds(2),
                        "ended_at": first_created_at + Duration::seconds(3),
                        "payload_summary": {
                            "title": "Design projection merge",
                        },
                        "created_at": first_created_at + Duration::seconds(2),
                    },
                    {
                        "career_entry_id": "career-entry:career-event-156-b",
                        "source_event_id": "career-event-156-b",
                        "source_module": "process",
                        "project_id": "project-156",
                        "work_ref": null,
                        "process_ref": {
                            "process_id": "process-156-b",
                            "process_kind": "review",
                            "process_version": "v2",
                        },
                        "entry_kind": "reviewed",
                        "started_at": first_created_at + Duration::seconds(3),
                        "ended_at": first_created_at + Duration::seconds(18),
                        "payload_summary": {
                            "activity": "Projection QA",
                        },
                        "created_at": first_created_at + Duration::seconds(3),
                    }
                ]
            })
        );
        assert_eq!(
            rebuilt_row.get::<serde_json::Value, _>("memory_ref_summary_json"),
            json!({})
        );
        assert!(
            rebuilt_row
                .get::<serde_json::Value, _>("capability_summary_json")
                .get("items")
                .is_some()
        );
        assert!(unrelated_row.is_none());
    }

    #[tokio::test]
    async fn rebuild_member_projection_rejects_blank_member_ids() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;

        let job = RebuildMemberProjectionJob::new(SqlxUnitOfWorkFactory::new(pool.clone()));
        let error = job
            .rebuild_member_projection("   ")
            .await
            .expect_err("blank member ids should be rejected");

        match error {
            IdentityError::RuleViolation { code, message } => {
                assert_eq!(code, "IDENTITY_INVALID_ARGUMENT");
                assert_eq!(message, "global_member_id must not be blank");
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[tokio::test]
    async fn rebuild_member_projection_rejects_members_without_projection_facts() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_outbox(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;
        seed_outbox_event(
            &pool,
            sample_role_catalog_synced_outbox_event(
                "outbox-role-only-301",
                "role.member.operator",
                "Member Operator",
                now(),
            ),
        )
        .await;

        let job = RebuildMemberProjectionJob::new(SqlxUnitOfWorkFactory::new(pool.clone()));
        let error = job
            .rebuild_member_projection("member-301")
            .await
            .expect_err("missing projection facts should be rejected");

        match error {
            IdentityError::RuleViolation { code, message } => {
                assert_eq!(code, "IDENTITY_PROJECTION_REBUILD_SOURCE_NOT_FOUND");
                assert_eq!(
                    message,
                    "no persisted member projection facts were found for global member `member-301`"
                );
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    async fn test_pool() -> sqlx::postgres::PgPool {
        let config = AppConfig {
            listen_addr: "127.0.0.1:8080".to_string(),
            database_url: Some(
                "postgres://postgres:postgres@127.0.0.1:5432/quantalithos_identity".to_string(),
            ),
            database_max_connections: 5,
        };

        let pool = PgPoolOptions::new()
            .max_connections(config.database_max_connections)
            .connect(
                config
                    .database_url
                    .as_deref()
                    .expect("database url should exist"),
            )
            .await
            .expect("connect test pool");
        run_migrations(&pool).await.expect("apply migrations");
        pool
    }

    async fn reset_outbox(pool: &sqlx::postgres::PgPool) {
        pool.execute(
            r#"
            TRUNCATE TABLE
                inbound_dead_letters,
                projection_checkpoints,
                member_summary_projection,
                outbox_events,
                idempotency_records,
                audit_trace_entries,
                career_history_entries,
                lifecycle_history_entries,
                memory_refs,
                capability_profiles,
                global_members,
                role_catalog_entries
            RESTART IDENTITY CASCADE
            "#,
        )
        .await
        .expect("truncate test tables");
    }

    async fn seed_outbox_event(pool: &sqlx::postgres::PgPool, event: OutboxEvent) {
        sqlx::query(
            r#"
            INSERT INTO outbox_events (
                outbox_event_id,
                aggregate_type,
                aggregate_id,
                event_type,
                payload_json,
                idempotency_key,
                status,
                retry_count,
                next_retry_at,
                created_at,
                published_at,
                failure_reason
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            "#,
        )
        .bind(event.outbox_event_id.as_str())
        .bind(event.aggregate_type)
        .bind(event.aggregate_id)
        .bind(event.event_type)
        .bind(event.payload_json)
        .bind(event.idempotency_key)
        .bind(event.status.as_db())
        .bind(event.retry_count)
        .bind(event.next_retry_at)
        .bind(event.created_at)
        .bind(event.published_at)
        .bind(event.failure_reason)
        .execute(pool)
        .await
        .expect("seed outbox event");
    }

    async fn seed_dead_letter(pool: &sqlx::postgres::PgPool, dead_letter: InboundDeadLetter) {
        sqlx::query(
            r#"
            INSERT INTO inbound_dead_letters (
                dead_letter_id,
                source_event_id,
                source_module,
                event_type,
                payload_json,
                failure_reason,
                replay_status,
                created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(dead_letter.dead_letter_id.as_str())
        .bind(
            dead_letter
                .source_event_id
                .as_ref()
                .map(|value| value.as_str()),
        )
        .bind(dead_letter.source_module)
        .bind(dead_letter.event_type)
        .bind(dead_letter.payload_json)
        .bind(dead_letter.failure_reason)
        .bind(dead_letter.replay_status.as_db())
        .bind(dead_letter.created_at)
        .execute(pool)
        .await
        .expect("seed inbound dead-letter");
    }

    async fn seed_idempotency_record(
        pool: &sqlx::postgres::PgPool,
        idempotency_key: &str,
        scope: IdempotencyScope,
        status: IdempotencyStatus,
    ) {
        sqlx::query(
            r#"
            INSERT INTO idempotency_records (
                idempotency_key,
                scope,
                request_hash,
                result_ref_json,
                status,
                created_at,
                updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(idempotency_key)
        .bind(scope.as_db())
        .bind(format!("request-hash-{idempotency_key}"))
        .bind(Some(json!({
            "kind": "test-record",
            "id": idempotency_key,
        })))
        .bind(status.as_db())
        .bind(now())
        .bind(now())
        .execute(pool)
        .await
        .expect("seed idempotency record");
    }

    async fn seed_role(pool: &sqlx::postgres::PgPool, role_id: &str, role_name: &str) {
        sqlx::query(
            r#"
            INSERT INTO role_catalog_entries (
                role_id,
                role_name,
                role_version,
                source_ref_json,
                fingerprint,
                status,
                updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            "#,
        )
        .bind(role_id)
        .bind(role_name)
        .bind("v1")
        .bind(json!({ "kind": "method_library_role", "id": role_id }))
        .bind(format!("fingerprint-{role_id}"))
        .bind("active")
        .bind(now())
        .execute(pool)
        .await
        .expect("seed role catalog entry");
    }

    fn sample_role_snapshot(
        role_id: &str,
        role_name: &str,
        role_version: &str,
        fingerprint: &str,
        status: &str,
    ) -> RoleDefinitionSnapshot {
        RoleDefinitionSnapshot {
            role_id: role_id.into(),
            role_name: role_name.to_string(),
            role_version: role_version.to_string(),
            source_ref: json!({
                "module": "method-library",
                "id": role_id,
            }),
            fingerprint: fingerprint.to_string(),
            status: status.to_string(),
        }
    }

    async fn insert_checkpoint(
        pool: &sqlx::postgres::PgPool,
        checkpoint_name: &str,
        last_processed_event_id: Option<&str>,
    ) {
        sqlx::query(
            r#"
            INSERT INTO projection_checkpoints (
                checkpoint_name,
                last_processed_event_id,
                status,
                failure_reason,
                updated_at
            ) VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(checkpoint_name)
        .bind(last_processed_event_id)
        .bind("idle")
        .bind(Option::<String>::None)
        .bind(now())
        .execute(pool)
        .await
        .expect("insert projection checkpoint");
    }

    async fn insert_member(
        pool: &sqlx::postgres::PgPool,
        global_member_id: &str,
        display_name: &str,
    ) {
        let created_at = now();
        let created_by_json = serde_json::to_value(ActorContext::new(
            "system:operations-test",
            ActorKind::System,
            None,
        ))
        .expect("serialize created_by actor");

        sqlx::query(
            r#"
            INSERT INTO global_members (
                global_member_id,
                display_name,
                lifecycle,
                main_role_id,
                secondary_role_ids_json,
                capability_profile_id,
                memory_refs_id,
                version,
                created_by_json,
                created_at,
                updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            "#,
        )
        .bind(global_member_id)
        .bind(display_name)
        .bind("hired")
        .bind("role.member.operator")
        .bind(json!([]))
        .bind(Option::<String>::None)
        .bind(Option::<String>::None)
        .bind(0_i64)
        .bind(created_by_json)
        .bind(created_at)
        .bind(created_at)
        .execute(pool)
        .await
        .expect("insert member");
    }

    async fn insert_memory_refs(
        pool: &sqlx::postgres::PgPool,
        global_member_id: &str,
        archive_status: ArchiveStatus,
        archive_ref: Option<ArchiveRef>,
    ) {
        sqlx::query(
            r#"
            INSERT INTO memory_refs (
                memory_refs_id,
                global_member_id,
                semantic_memory_ref_json,
                episodic_memory_refs_json,
                archive_ref_json,
                archive_status,
                version,
                updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(format!("memory-refs:{global_member_id}"))
        .bind(global_member_id)
        .bind(json!({
            "memory_id": format!("memory-semantic-{global_member_id}"),
            "memory_kind": "semantic",
            "memory_version": "v1",
        }))
        .bind(json!([
            {
                "memory_id": format!("memory-episodic-{global_member_id}"),
                "memory_kind": "episodic",
                "memory_version": "v1",
            }
        ]))
        .bind(archive_ref.map(|value| json!(value)))
        .bind(archive_status.as_db())
        .bind(1_i64)
        .bind(now())
        .execute(pool)
        .await
        .expect("insert memory refs");

        sqlx::query("UPDATE global_members SET memory_refs_id = $2, updated_at = $3 WHERE global_member_id = $1")
            .bind(global_member_id)
            .bind(format!("memory-refs:{global_member_id}"))
            .bind(now())
            .execute(pool)
            .await
            .expect("link member memory refs");
    }

    fn sample_pending_outbox_event(outbox_event_id: &str) -> OutboxEvent {
        OutboxEvent {
            outbox_event_id: OutboxEventId::new(outbox_event_id),
            aggregate_type: "global_member".to_string(),
            aggregate_id: "member-001".to_string(),
            event_type: "identity.member.created".to_string(),
            payload_json: json!({
                "global_member_id": "member-001",
                "display_name": "Member Zero One",
            }),
            idempotency_key: format!("idem-{outbox_event_id}"),
            status: OutboxStatus::Pending,
            retry_count: 0,
            next_retry_at: None,
            created_at: now(),
            published_at: None,
            failure_reason: None,
        }
    }

    fn sample_process_event(
        source_event_id: &str,
        payload_hash: &str,
        global_member_id: &str,
    ) -> InboundProcessFactEvent {
        InboundProcessFactEvent {
            envelope: InboundEventEnvelope {
                source_event_id: EventId::new(source_event_id),
                source_module: "process".to_string(),
                event_type: "process.activity.completed".to_string(),
                occurred_at: now() + Duration::seconds(1),
                payload_hash: payload_hash.to_string(),
                payload: json!({
                    "global_member_id": GlobalMemberId::new(global_member_id),
                    "project_id": ProjectId::new("project-002"),
                    "process_ref": {
                        "process_id": "process-001",
                        "process_kind": "activity",
                        "process_version": "v2",
                    },
                    "entry_kind": "completed",
                    "started_at": now(),
                    "ended_at": now() + Duration::seconds(45),
                    "payload_summary": {
                        "activity_name": "Career review",
                    }
                }),
            },
        }
    }

    fn sample_memory_archive_event(
        source_event_id: &str,
        payload_hash: &str,
        global_member_id: &str,
        status: &str,
        occurred_at: PrimitiveDateTime,
    ) -> InboundMemoryArchiveEvent {
        InboundMemoryArchiveEvent {
            envelope: InboundEventEnvelope {
                source_event_id: EventId::new(source_event_id),
                source_module: "memory-archive".to_string(),
                event_type: "memory.archive.status.changed".to_string(),
                occurred_at,
                payload_hash: payload_hash.to_string(),
                payload: json!({
                    "archive_status_snapshot": {
                        "global_member_id": global_member_id,
                        "archive_ref": {
                            "archive_id": format!("archive-{global_member_id}"),
                            "archive_kind": "member_memory_archive",
                            "archive_version": "v1",
                        },
                        "status": status,
                        "reason": if status == "failed" {
                            Some("archive validation failed")
                        } else {
                            Option::<&str>::None
                        },
                    }
                }),
            },
        }
    }

    fn sample_role_catalog_synced_outbox_event(
        outbox_event_id: &str,
        role_id: &str,
        role_name: &str,
        created_at: PrimitiveDateTime,
    ) -> OutboxEvent {
        OutboxEvent {
            outbox_event_id: OutboxEventId::new(outbox_event_id),
            aggregate_type: "role_catalog_entry".to_string(),
            aggregate_id: role_id.to_string(),
            event_type: "identity.role_catalog.synced".to_string(),
            payload_json: json!({
                "role_id": role_id,
                "role_name": role_name,
                "role_version": "v1",
                "source_ref": { "kind": "method_library_role", "id": role_id },
                "fingerprint": format!("fingerprint-{role_id}"),
                "status": "active",
                "updated_at": created_at,
            }),
            idempotency_key: format!("idem-{outbox_event_id}"),
            status: OutboxStatus::Pending,
            retry_count: 0,
            next_retry_at: None,
            created_at,
            published_at: None,
            failure_reason: None,
        }
    }

    fn sample_member_created_projection_event(
        outbox_event_id: &str,
        global_member_id: &str,
        display_name: &str,
        main_role_id: &str,
        created_at: PrimitiveDateTime,
    ) -> OutboxEvent {
        OutboxEvent {
            outbox_event_id: OutboxEventId::new(outbox_event_id),
            aggregate_type: "global_member".to_string(),
            aggregate_id: global_member_id.to_string(),
            event_type: "identity.member.created".to_string(),
            payload_json: json!({
                "global_member_id": global_member_id,
                "display_name": display_name,
                "lifecycle": "hired",
                "main_role_id": main_role_id,
                "secondary_role_ids": [],
                "capability_profile_id": null,
                "memory_refs_id": null,
                "version": 0,
                "created_at": created_at,
                "updated_at": created_at,
            }),
            idempotency_key: format!("idem-{outbox_event_id}"),
            status: OutboxStatus::Pending,
            retry_count: 0,
            next_retry_at: None,
            created_at,
            published_at: None,
            failure_reason: None,
        }
    }

    fn sample_capability_profile_updated_projection_event(
        outbox_event_id: &str,
        capability_profile_id: &str,
        global_member_id: &str,
        display_name: &str,
        main_role_id: &str,
        created_at: PrimitiveDateTime,
    ) -> OutboxEvent {
        OutboxEvent {
            outbox_event_id: OutboxEventId::new(outbox_event_id),
            aggregate_type: "capability_profile".to_string(),
            aggregate_id: capability_profile_id.to_string(),
            event_type: "identity.capability_profile.updated".to_string(),
            payload_json: json!({
                "capability_profile_id": capability_profile_id,
                "global_member_id": global_member_id,
                "display_name": display_name,
                "lifecycle": "hired",
                "main_role_id": main_role_id,
                "capability_summary_json": {
                    "capability_profile_id": capability_profile_id,
                    "items": [
                        {
                            "capability_id": "capability.rust",
                            "capability_name": "Rust",
                            "proficiency": "advanced",
                            "notes": "systems programming",
                        }
                    ],
                    "evidence_refs": [
                        {
                            "artifact_id": "artifact-151",
                            "artifact_kind": "evidence",
                            "artifact_version": "v1",
                        }
                    ],
                    "version": 1,
                },
                "version": 1,
                "updated_at": created_at,
            }),
            idempotency_key: format!("idem-{outbox_event_id}"),
            status: OutboxStatus::Pending,
            retry_count: 0,
            next_retry_at: None,
            created_at,
            published_at: None,
            failure_reason: None,
        }
    }

    fn sample_memory_refs_updated_projection_event(
        outbox_event_id: &str,
        memory_refs_id: &str,
        global_member_id: &str,
        display_name: &str,
        main_role_id: &str,
        created_at: PrimitiveDateTime,
    ) -> OutboxEvent {
        OutboxEvent {
            outbox_event_id: OutboxEventId::new(outbox_event_id),
            aggregate_type: "memory_refs".to_string(),
            aggregate_id: memory_refs_id.to_string(),
            event_type: "identity.memory_refs.updated".to_string(),
            payload_json: json!({
                "memory_refs_id": memory_refs_id,
                "global_member_id": global_member_id,
                "display_name": display_name,
                "lifecycle": "hired",
                "main_role_id": main_role_id,
                "memory_ref_summary_json": {
                    "memory_refs_id": memory_refs_id,
                    "semantic_memory_ref": {
                        "memory_id": "memory-semantic-161",
                        "memory_kind": "semantic",
                        "memory_version": "v1",
                    },
                    "episodic_memory_refs": [
                        {
                            "memory_id": "memory-episodic-161",
                            "memory_kind": "episodic",
                            "memory_version": "v1",
                        }
                    ],
                    "archive_ref": null,
                    "archive_status": "none",
                    "version": 2,
                },
                "version": 2,
                "updated_at": created_at,
            }),
            idempotency_key: format!("idem-{outbox_event_id}"),
            status: OutboxStatus::Pending,
            retry_count: 0,
            next_retry_at: None,
            created_at,
            published_at: None,
            failure_reason: None,
        }
    }

    fn sample_memory_archive_status_changed_projection_event(
        outbox_event_id: &str,
        memory_refs_id: &str,
        global_member_id: &str,
        display_name: &str,
        main_role_id: &str,
        created_at: PrimitiveDateTime,
    ) -> OutboxEvent {
        OutboxEvent {
            outbox_event_id: OutboxEventId::new(outbox_event_id),
            aggregate_type: "memory_refs".to_string(),
            aggregate_id: memory_refs_id.to_string(),
            event_type: "identity.memory_refs.archive_status_changed".to_string(),
            payload_json: json!({
                "memory_refs_id": memory_refs_id,
                "global_member_id": global_member_id,
                "display_name": display_name,
                "lifecycle": "hired",
                "main_role_id": main_role_id,
                "memory_ref_summary_json": {
                    "memory_refs_id": memory_refs_id,
                    "semantic_memory_ref": {
                        "memory_id": "memory-semantic-162",
                        "memory_kind": "semantic",
                        "memory_version": "v1",
                    },
                    "episodic_memory_refs": [
                        {
                            "memory_id": "memory-episodic-162",
                            "memory_kind": "episodic",
                            "memory_version": "v1",
                        }
                    ],
                    "archive_ref": {
                        "archive_id": "archive-member-162",
                        "archive_kind": "member_memory_archive",
                        "archive_version": "v1",
                    },
                    "archive_status": "archived",
                    "version": 3,
                },
                "version": 3,
                "updated_at": created_at,
            }),
            idempotency_key: format!("idem-{outbox_event_id}"),
            status: OutboxStatus::Pending,
            retry_count: 0,
            next_retry_at: None,
            created_at,
            published_at: None,
            failure_reason: None,
        }
    }

    fn sample_career_history_appended_projection_event(
        outbox_event_id: &str,
        global_member_id: &str,
        display_name: &str,
        main_role_id: &str,
        created_at: PrimitiveDateTime,
    ) -> OutboxEvent {
        let later_created_at = created_at + Duration::seconds(1);

        OutboxEvent {
            outbox_event_id: OutboxEventId::new(outbox_event_id),
            aggregate_type: "career_history".to_string(),
            aggregate_id: global_member_id.to_string(),
            event_type: "identity.career_history.appended".to_string(),
            payload_json: json!({
                "global_member_id": global_member_id,
                "display_name": display_name,
                "lifecycle": "hired",
                "main_role_id": main_role_id,
                "career_summary_json": {
                    "global_member_id": global_member_id,
                    "entry_count": 2,
                    "entries": [
                        {
                            "career_entry_id": "career-entry:career-event-156-a",
                            "source_event_id": "career-event-156-a",
                            "source_module": "work",
                            "project_id": "project-156",
                            "work_ref": {
                                "work_id": "work-156-a",
                                "work_kind": "task",
                                "work_version": "v1",
                            },
                            "process_ref": null,
                            "entry_kind": "assigned",
                            "started_at": created_at,
                            "ended_at": created_at + Duration::seconds(1),
                            "payload_summary": {
                                "title": "Design projection merge",
                            },
                            "created_at": created_at,
                        },
                        {
                            "career_entry_id": "career-entry:career-event-156-b",
                            "source_event_id": "career-event-156-b",
                            "source_module": "process",
                            "project_id": "project-156",
                            "work_ref": null,
                            "process_ref": {
                                "process_id": "process-156-b",
                                "process_kind": "review",
                                "process_version": "v2",
                            },
                            "entry_kind": "reviewed",
                            "started_at": later_created_at,
                            "ended_at": later_created_at + Duration::seconds(15),
                            "payload_summary": {
                                "activity": "Projection QA",
                            },
                            "created_at": later_created_at,
                        }
                    ]
                },
                "version": 2,
                "updated_at": later_created_at,
            }),
            idempotency_key: format!("idem-{outbox_event_id}"),
            status: OutboxStatus::Pending,
            retry_count: 0,
            next_retry_at: None,
            created_at,
            published_at: None,
            failure_reason: None,
        }
    }

    fn now() -> PrimitiveDateTime {
        let now = OffsetDateTime::now_utc();
        PrimitiveDateTime::new(now.date(), now.time())
    }
}
