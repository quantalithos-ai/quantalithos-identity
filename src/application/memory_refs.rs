//! Application service for explicit memory refs updates.

use serde_json::json;
use time::PrimitiveDateTime;

use crate::application::persistence::{
    AuditTraceRepository, GlobalMemberRepository, IdempotencyStore, InboundDeadLetterStore,
    MemoryRefsRepository, OutboxStore, UnitOfWork, UnitOfWorkFactory,
};
use crate::domain::audit::{AuditResult, AuditTraceEntry};
use crate::domain::dead_letter::{DeadLetterReplayStatus, InboundDeadLetter};
use crate::domain::idempotency::{IdempotencyRecord, IdempotencyScope, IdempotencyStatus};
use crate::domain::memory_refs::{
    ArchiveStatus, MemoryRef, MemoryRefs, MemoryRefsSummary, UpdateMemoryRefsCommand,
};
use crate::domain::outbox::OutboxEvent;
use crate::domain::shared::context::ActorContext;
use crate::domain::shared::ids::{DeadLetterId, GlobalMemberId, OutboxEventId};
use crate::domain::shared::metadata::CommandMetadata;
use crate::error::IdentityError;
use crate::inbound::events::{
    InboundEventEnvelope, InboundMemoryArchiveEvent, MemoryArchiveEventParser,
};
use crate::outbound::MemoryArchivePort;

/// Coordinates memory refs writes over the shared transaction boundary.
#[derive(Debug, Clone)]
pub struct MemoryRefsCommandService<UowFactory, MemoryArchiveValidator> {
    unit_of_work_factory: UowFactory,
    memory_archive_validator: MemoryArchiveValidator,
    archive_event_parser: MemoryArchiveEventParser,
}

impl<UowFactory, MemoryArchiveValidator>
    MemoryRefsCommandService<UowFactory, MemoryArchiveValidator>
{
    /// Creates a new memory refs command service bound to the provided ports.
    pub fn new(
        unit_of_work_factory: UowFactory,
        memory_archive_validator: MemoryArchiveValidator,
    ) -> Self {
        Self {
            unit_of_work_factory,
            memory_archive_validator,
            archive_event_parser: MemoryArchiveEventParser,
        }
    }
}

/// Summarizes the result of handling one inbound memory/archive status event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MemoryArchiveEventOutcome {
    /// One archive status update was applied successfully.
    Updated { memory_refs_id: String },
    /// The inbound event had already been consumed successfully with the same payload hash.
    SkippedDuplicate { memory_refs_id: Option<String> },
    /// The inbound event was retained into dead-letter storage instead of mutating write models.
    DeadLettered,
}

impl<UowFactory, MemoryArchiveValidator>
    MemoryRefsCommandService<UowFactory, MemoryArchiveValidator>
where
    UowFactory: UnitOfWorkFactory,
    MemoryArchiveValidator: MemoryArchivePort,
{
    /// Updates one member memory refs aggregate while retaining only ref-only pointers.
    ///
    /// # Errors
    ///
    /// Returns an error when the member is missing, when a memory ref is invalid, when the
    /// idempotency key conflicts, or when persistence fails.
    pub async fn update_memory_refs(
        &self,
        command: UpdateMemoryRefsCommand,
        actor: ActorContext,
        metadata: CommandMetadata,
    ) -> Result<MemoryRefsSummary, IdentityError> {
        if metadata.idempotency_key().trim().is_empty() {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_INVALID_ARGUMENT",
                message: "idempotency_key must not be blank".to_string(),
            });
        }

        let mut uow = self.unit_of_work_factory.begin().await?;
        let existing_record = uow
            .idempotency()
            .get(metadata.idempotency_key(), IdempotencyScope::Command)
            .await?;

        if let Some(existing_record) = existing_record {
            return self
                .handle_existing_command_record(existing_record, metadata.request_hash(), uow)
                .await;
        }

        let mut member = uow
            .global_members()
            .get_for_update(&command.global_member_id)
            .await?
            .ok_or_else(|| IdentityError::RuleViolation {
                code: "IDENTITY_MEMBER_NOT_FOUND",
                message: format!(
                    "global member `{}` was not found",
                    command.global_member_id.as_str()
                ),
            })?;

        if let Some(semantic_memory_ref) = command.semantic_memory_ref.as_ref() {
            self.memory_archive_validator
                .validate_ref(semantic_memory_ref)
                .await?;
        }
        for episodic_memory_ref in &command.episodic_memory_refs {
            self.memory_archive_validator
                .validate_ref(episodic_memory_ref)
                .await?;
        }

        let existing_memory_refs = uow
            .memory_refs()
            .get_for_update_by_member(&command.global_member_id)
            .await?;
        let (mut memory_refs, expected_version, should_insert) = match existing_memory_refs {
            Some(memory_refs) => {
                let expected_version = memory_refs.version;
                (memory_refs, expected_version, false)
            }
            None => (
                MemoryRefs::create_empty(command.global_member_id.clone()),
                0,
                true,
            ),
        };

        if let Some(semantic_memory_ref) = command.semantic_memory_ref {
            memory_refs.replace_semantic_ref(semantic_memory_ref, &actor)?;
        }
        for episodic_memory_ref in command.episodic_memory_refs {
            memory_refs.add_episodic_ref(episodic_memory_ref, &actor)?;
        }

        if should_insert {
            uow.memory_refs().insert(&memory_refs).await?;
        } else {
            uow.memory_refs()
                .save(&memory_refs, expected_version)
                .await?;
        }

        let member_expected_version = member.version;
        if member.memory_refs_id.as_ref() != Some(&memory_refs.memory_refs_id) {
            member.link_memory_refs(memory_refs.memory_refs_id.clone());
            uow.global_members()
                .save(&member, member_expected_version)
                .await?;
        }

        let summary = memory_refs.summary();
        let audit_entry = AuditTraceEntry::for_memory_refs_command(
            format!("audit:{}", metadata.idempotency_key()),
            &memory_refs,
            &actor,
            metadata.trace_id(),
            memory_refs.updated_at,
        );
        let outbox_event = OutboxEvent::for_memory_refs_updated(
            OutboxEventId::new(format!("outbox:{}", metadata.idempotency_key())),
            &member,
            &memory_refs,
            metadata.trace_id(),
            metadata.idempotency_key(),
            memory_refs.updated_at,
        );

        uow.audit_traces().append(&audit_entry).await?;
        uow.outbox().append(&outbox_event).await?;
        uow.idempotency()
            .record_success(
                &metadata,
                IdempotencyScope::Command,
                json!({
                    "kind": "memory_refs",
                    "id": summary.memory_refs_id.as_str(),
                    "global_member_id": summary.global_member_id.as_str(),
                    "semantic_memory_ref": summary.semantic_memory_ref.clone(),
                    "episodic_memory_refs": summary.episodic_memory_refs.clone(),
                    "archive_ref": summary.archive_ref.clone(),
                    "archive_status": summary.archive_status.as_db(),
                    "version": summary.version,
                    "updated_at": summary.updated_at,
                }),
            )
            .await?;
        uow.commit().await?;

        Ok(summary)
    }

    async fn handle_existing_command_record<Uow>(
        &self,
        existing_record: IdempotencyRecord,
        request_hash: &str,
        uow: Uow,
    ) -> Result<MemoryRefsSummary, IdentityError>
    where
        Uow: UnitOfWork,
    {
        if existing_record.request_hash != request_hash {
            uow.rollback().await?;
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_IDEMPOTENCY_CONFLICT",
                message: format!(
                    "idempotency key `{}` was already used with a different request hash",
                    existing_record.idempotency_key
                ),
            });
        }

        if existing_record.status != IdempotencyStatus::Succeeded {
            uow.rollback().await?;
            return Err(IdentityError::PersistenceData {
                message: format!(
                    "command idempotency record `{}` has non-succeeded status",
                    existing_record.idempotency_key
                ),
            });
        }

        let summary = summary_from_result_ref(existing_record.result_ref_json.as_ref())
            .ok_or_else(|| IdentityError::PersistenceData {
                message: format!(
                    "command idempotency record `{}` is missing the expected result summary",
                    existing_record.idempotency_key
                ),
            })?;
        uow.rollback().await?;
        Ok(summary)
    }

    /// Consumes one trusted archive status event and updates local memory refs state.
    pub async fn handle_archive_event(
        &self,
        event: InboundMemoryArchiveEvent,
    ) -> Result<MemoryArchiveEventOutcome, IdentityError> {
        self.handle_archive_event_internal(event.envelope, DeadLetterRetention::CreateNew)
            .await
    }

    /// Replays one existing dead-letter row through the normal archive-event consumer logic.
    pub async fn replay_archive_dead_letter(
        &self,
        dead_letter_id: DeadLetterId,
        created_at: PrimitiveDateTime,
        event: InboundMemoryArchiveEvent,
    ) -> Result<MemoryArchiveEventOutcome, IdentityError> {
        self.handle_archive_event_internal(
            event.envelope,
            DeadLetterRetention::UpdateExisting {
                dead_letter_id,
                created_at,
            },
        )
        .await
    }

    async fn handle_archive_event_internal(
        &self,
        envelope: InboundEventEnvelope,
        dead_letter_retention: DeadLetterRetention,
    ) -> Result<MemoryArchiveEventOutcome, IdentityError> {
        let mut uow = self.unit_of_work_factory.begin().await?;
        let existing_record = {
            let mut idempotency = uow.idempotency();
            idempotency
                .get(
                    envelope.source_event_id.as_str(),
                    IdempotencyScope::InboundEvent,
                )
                .await?
        };

        if let Some(existing_record) = existing_record {
            return self
                .handle_existing_archive_record(
                    existing_record,
                    &envelope,
                    dead_letter_retention,
                    uow,
                )
                .await;
        }

        let archive_update = match self.archive_event_parser.parse(envelope.payload.clone()) {
            Ok(archive_update) => archive_update,
            Err(error) => {
                retain_archive_dead_letter(
                    &mut uow,
                    &dead_letter_retention,
                    &envelope,
                    error.to_string(),
                )
                .await?;
                uow.commit().await?;
                return Ok(MemoryArchiveEventOutcome::DeadLettered);
            }
        };

        let mut memory_refs = match {
            let mut repository = uow.memory_refs();
            repository
                .get_for_update_by_member(&archive_update.global_member_id)
                .await?
        } {
            Some(memory_refs) => memory_refs,
            None => {
                retain_archive_dead_letter(
                    &mut uow,
                    &dead_letter_retention,
                    &envelope,
                    format!(
                        "IDENTITY_MEMORY_REFS_NOT_FOUND_TO_DEAD_LETTER: memory refs for member `{}` were not found",
                        archive_update.global_member_id.as_str()
                    ),
                )
                .await?;
                uow.commit().await?;
                return Ok(MemoryArchiveEventOutcome::DeadLettered);
            }
        };
        let expected_version = memory_refs.version;

        let next_status = match parse_archive_status(&archive_update.status) {
            Ok(next_status) => next_status,
            Err(error) => {
                retain_archive_dead_letter(
                    &mut uow,
                    &dead_letter_retention,
                    &envelope,
                    error.to_string(),
                )
                .await?;
                uow.commit().await?;
                return Ok(MemoryArchiveEventOutcome::DeadLettered);
            }
        };
        if memory_refs.rejects_archive_update(next_status, envelope.occurred_at) {
            uow.rollback().await?;
            return Ok(MemoryArchiveEventOutcome::SkippedDuplicate {
                memory_refs_id: Some(memory_refs.memory_refs_id.as_str().to_string()),
            });
        }

        match next_status {
            ArchiveStatus::Pending => {
                memory_refs.mark_archive_pending(archive_update.archive_ref)?;
            }
            ArchiveStatus::Archived => {
                memory_refs.mark_archived(archive_update.archive_ref)?;
            }
            ArchiveStatus::Failed => {
                memory_refs.mark_archive_failed(
                    archive_update.archive_ref,
                    archive_update
                        .reason
                        .as_deref()
                        .unwrap_or("archive event reported failure"),
                )?;
            }
            ArchiveStatus::None => {
                retain_archive_dead_letter(
                    &mut uow,
                    &dead_letter_retention,
                    &envelope,
                    "IDENTITY_EVENT_UNSUPPORTED: archive status `none` is not supported for inbound archive updates"
                        .to_string(),
                )
                .await?;
                uow.commit().await?;
                return Ok(MemoryArchiveEventOutcome::DeadLettered);
            }
        }

        let member = {
            let mut repository = uow.global_members();
            repository.get(&archive_update.global_member_id).await?
        }
        .ok_or_else(|| IdentityError::RuleViolation {
            code: "IDENTITY_MEMBER_NOT_FOUND",
            message: format!(
                "global member `{}` was not found",
                archive_update.global_member_id.as_str()
            ),
        })?;

        let now = memory_refs.updated_at;
        let metadata = CommandMetadata::new(
            envelope.source_event_id.as_str(),
            envelope.source_event_id.as_str(),
            envelope.payload_hash.clone(),
        );
        let audit_entry = AuditTraceEntry::for_inbound_event(
            format!("audit:{}", envelope.source_event_id.as_str()),
            "HandleMemoryArchiveEvent",
            envelope.source_module.clone(),
            envelope.source_event_id.as_str(),
            Some(json!({
                "kind": "memory_refs",
                "id": memory_refs.memory_refs_id.as_str(),
                "global_member_id": memory_refs.global_member_id.as_str(),
                "archive_status": memory_refs.archive_status.as_db(),
            })),
            AuditResult::Success,
            archive_update.reason.clone(),
            now,
        );
        let outbox_event = OutboxEvent::for_memory_archive_status_changed(
            OutboxEventId::new(format!("outbox:{}", envelope.source_event_id.as_str())),
            &member,
            &memory_refs,
            envelope.source_event_id.as_str(),
            envelope.source_event_id.as_str(),
            now,
        );
        let summary = memory_refs.summary();

        uow.memory_refs()
            .save(&memory_refs, expected_version)
            .await?;
        uow.audit_traces().append(&audit_entry).await?;
        uow.outbox().append(&outbox_event).await?;
        uow.idempotency()
            .record_success(
                &metadata,
                IdempotencyScope::InboundEvent,
                json!({
                    "kind": "memory_refs",
                    "id": summary.memory_refs_id.as_str(),
                    "global_member_id": summary.global_member_id.as_str(),
                    "archive_ref": summary.archive_ref.clone(),
                    "archive_status": summary.archive_status.as_db(),
                    "version": summary.version,
                    "updated_at": summary.updated_at,
                }),
            )
            .await?;
        uow.commit().await?;

        Ok(MemoryArchiveEventOutcome::Updated {
            memory_refs_id: summary.memory_refs_id.as_str().to_string(),
        })
    }

    async fn handle_existing_archive_record<Uow>(
        &self,
        existing_record: IdempotencyRecord,
        envelope: &InboundEventEnvelope,
        dead_letter_retention: DeadLetterRetention,
        mut uow: Uow,
    ) -> Result<MemoryArchiveEventOutcome, IdentityError>
    where
        Uow: UnitOfWork,
    {
        if existing_record.request_hash != envelope.payload_hash {
            let error = IdentityError::PersistenceData {
                message: format!(
                    "idempotency conflict for inbound event `{}` with different payload hash",
                    envelope.source_event_id.as_str()
                ),
            };
            retain_archive_dead_letter(
                &mut uow,
                &dead_letter_retention,
                envelope,
                error.to_string(),
            )
            .await?;
            uow.commit().await?;
            return Err(error);
        }

        let memory_refs_id = existing_record
            .result_ref_json
            .as_ref()
            .and_then(|value| value.get("id"))
            .and_then(serde_json::Value::as_str)
            .map(ToString::to_string);

        match existing_record.status {
            IdempotencyStatus::Succeeded => {
                uow.rollback().await?;
                Ok(MemoryArchiveEventOutcome::SkippedDuplicate { memory_refs_id })
            }
            IdempotencyStatus::Processing | IdempotencyStatus::Failed => {
                uow.rollback().await?;
                Err(IdentityError::PersistenceData {
                    message: format!(
                        "inbound event `{}` exists with non-succeeded idempotency status",
                        envelope.source_event_id.as_str()
                    ),
                })
            }
        }
    }
}

#[derive(Debug, Clone)]
enum DeadLetterRetention {
    CreateNew,
    UpdateExisting {
        dead_letter_id: DeadLetterId,
        created_at: PrimitiveDateTime,
    },
}

async fn retain_archive_dead_letter<Uow>(
    uow: &mut Uow,
    retention: &DeadLetterRetention,
    envelope: &InboundEventEnvelope,
    failure_reason: String,
) -> Result<(), IdentityError>
where
    Uow: UnitOfWork,
{
    match retention {
        DeadLetterRetention::CreateNew => {
            uow.inbound_dead_letters()
                .append(&InboundDeadLetter {
                    dead_letter_id: DeadLetterId::new(format!(
                        "dead-letter:{}:{}",
                        envelope.source_module,
                        envelope.source_event_id.as_str()
                    )),
                    source_event_id: Some(envelope.source_event_id.clone()),
                    source_module: envelope.source_module.clone(),
                    event_type: envelope.event_type.clone(),
                    payload_json: envelope.payload.clone(),
                    failure_reason,
                    replay_status: DeadLetterReplayStatus::Pending,
                    created_at: current_timestamp(),
                })
                .await
        }
        DeadLetterRetention::UpdateExisting {
            dead_letter_id,
            created_at,
        } => {
            uow.inbound_dead_letters()
                .save(&InboundDeadLetter {
                    dead_letter_id: dead_letter_id.clone(),
                    source_event_id: Some(envelope.source_event_id.clone()),
                    source_module: envelope.source_module.clone(),
                    event_type: envelope.event_type.clone(),
                    payload_json: envelope.payload.clone(),
                    failure_reason,
                    replay_status: DeadLetterReplayStatus::Pending,
                    created_at: *created_at,
                })
                .await
        }
    }
}

fn parse_archive_status(status: &str) -> Result<ArchiveStatus, IdentityError> {
    ArchiveStatus::from_db(status).ok_or(IdentityError::PersistenceData {
        message: format!("unknown archive status `{status}`"),
    })
}

fn summary_from_result_ref(
    result_ref_json: Option<&serde_json::Value>,
) -> Option<MemoryRefsSummary> {
    let result_ref_json = result_ref_json?;
    let memory_refs_id = result_ref_json.get("id")?.as_str()?;
    let global_member_id = result_ref_json.get("global_member_id")?.as_str()?;
    let semantic_memory_ref = result_ref_json
        .get("semantic_memory_ref")
        .and_then(|value| serde_json::from_value::<Option<MemoryRef>>(value.clone()).ok())
        .flatten();
    let episodic_memory_refs: Vec<MemoryRef> =
        serde_json::from_value(result_ref_json.get("episodic_memory_refs")?.clone()).ok()?;
    let archive_ref = result_ref_json
        .get("archive_ref")
        .and_then(|value| serde_json::from_value(value.clone()).ok());
    let archive_status = result_ref_json
        .get("archive_status")?
        .as_str()
        .and_then(crate::domain::memory_refs::ArchiveStatus::from_db)?;
    let version = result_ref_json.get("version")?.as_i64()?;
    let updated_at = result_ref_json
        .get("updated_at")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())?;

    Some(MemoryRefsSummary {
        memory_refs_id: crate::domain::shared::ids::MemoryRefsId::new(memory_refs_id),
        global_member_id: GlobalMemberId::new(global_member_id),
        semantic_memory_ref,
        episodic_memory_refs,
        archive_ref,
        archive_status,
        version,
        updated_at,
    })
}

fn current_timestamp() -> time::PrimitiveDateTime {
    let now = time::OffsetDateTime::now_utc();
    time::PrimitiveDateTime::new(now.date(), now.time())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;
    use sqlx::{Executor, Row, postgres::PgPoolOptions};
    use time::{Duration, PrimitiveDateTime};

    use crate::application::member_lifecycle::MemberLifecycleCommandService;
    use crate::application::query_projection::{GetMemberSummaryQuery, QueryProjectionService};
    use crate::config::AppConfig;
    use crate::domain::member::HireGlobalMemberCommand;
    use crate::domain::memory_refs::{MemoryRef, UpdateMemoryRefsCommand};
    use crate::domain::shared::context::{ActorContext, ActorKind};
    use crate::domain::shared::ids::{DeadLetterId, EventId, RoleId};
    use crate::domain::shared::metadata::CommandMetadata;
    use crate::error::IdentityError;
    use crate::inbound::events::{InboundEventEnvelope, InboundMemoryArchiveEvent};
    use crate::operations::ProjectionRebuildJob;
    use crate::outbound::MemoryArchivePort;
    use crate::persistence::database::run_migrations;
    use crate::persistence::test_support::DB_TEST_MUTEX;
    use crate::persistence::unit_of_work::SqlxUnitOfWorkFactory;

    use super::{MemoryArchiveEventOutcome, MemoryRefsCommandService, current_timestamp};

    #[derive(Debug, Clone, Default)]
    struct StubMemoryArchiveValidator {
        invalid_memory_id: Option<String>,
    }

    impl StubMemoryArchiveValidator {
        fn accepting() -> Self {
            Self::default()
        }

        fn rejecting(memory_id: &str) -> Self {
            Self {
                invalid_memory_id: Some(memory_id.to_string()),
            }
        }
    }

    impl MemoryArchivePort for StubMemoryArchiveValidator {
        async fn validate_ref(&self, memory_ref: &MemoryRef) -> Result<(), IdentityError> {
            if let Some(invalid_memory_id) = self.invalid_memory_id.as_deref() {
                if memory_ref.memory_id == invalid_memory_id {
                    return Err(IdentityError::RuleViolation {
                        code: "IDENTITY_MEMORY_REF_INVALID",
                        message: format!(
                            "memory ref `{invalid_memory_id}` is not valid for identity retention"
                        ),
                    });
                }
            }

            Ok(())
        }
    }

    #[tokio::test]
    async fn update_memory_refs_creates_refs_links_member_and_refreshes_projection() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let hire_service = MemberLifecycleCommandService::new(factory.clone());
        let service =
            MemoryRefsCommandService::new(factory.clone(), StubMemoryArchiveValidator::accepting());
        let rebuild_job = ProjectionRebuildJob::new(factory.clone());
        let query_service = QueryProjectionService::new(factory);
        let actor = ActorContext::new("human/admin-memory-1", ActorKind::HumanUser, None);
        let hire_metadata = CommandMetadata::new(
            "idem-hire-memory-001",
            "trace-hire-memory-001",
            "hash-hire-memory-001",
        );
        let memory_metadata =
            CommandMetadata::new("idem-memory-001", "trace-memory-001", "hash-memory-001");

        let member = hire_service
            .hire_global_member(
                HireGlobalMemberCommand {
                    display_name: "Memory Member".to_string(),
                    main_role_id: RoleId::new("role.member.operator"),
                    secondary_role_ids: Vec::new(),
                },
                actor.clone(),
                hire_metadata.clone(),
            )
            .await
            .expect("hire member for memory refs update");

        let summary = service
            .update_memory_refs(
                UpdateMemoryRefsCommand {
                    global_member_id: member.global_member_id.clone(),
                    semantic_memory_ref: Some(sample_semantic_memory_ref()),
                    episodic_memory_refs: vec![sample_episodic_memory_ref()],
                },
                actor.clone(),
                memory_metadata.clone(),
            )
            .await
            .expect("memory refs update should succeed");

        sqlx::query("UPDATE outbox_events SET created_at = $2 WHERE outbox_event_id = $1")
            .bind(format!("outbox:{}", hire_metadata.idempotency_key()))
            .bind(current_timestamp())
            .execute(&pool)
            .await
            .expect("stabilize hire outbox created_at");
        sqlx::query("UPDATE outbox_events SET created_at = $2 WHERE outbox_event_id = $1")
            .bind(format!("outbox:{}", memory_metadata.idempotency_key()))
            .bind(current_timestamp() + Duration::seconds(1))
            .execute(&pool)
            .await
            .expect("stabilize memory outbox created_at");

        rebuild_job
            .rebuild_member_summary_projection("member-summary-rebuild", 20)
            .await
            .expect("projection rebuild should succeed");
        let query_summary = query_service
            .get_member_summary(
                GetMemberSummaryQuery {
                    global_member_id: member.global_member_id.clone(),
                },
                actor,
            )
            .await
            .expect("member summary query should succeed after rebuild");

        let memory_refs_row = sqlx::query(
            r#"
            SELECT
                memory_refs_id,
                semantic_memory_ref_json,
                episodic_memory_refs_json,
                archive_ref_json,
                archive_status,
                version
            FROM memory_refs
            WHERE global_member_id = $1
            "#,
        )
        .bind(member.global_member_id.as_str())
        .fetch_one(&pool)
        .await
        .expect("load memory refs row");
        let member_row =
            sqlx::query("SELECT memory_refs_id FROM global_members WHERE global_member_id = $1")
                .bind(member.global_member_id.as_str())
                .fetch_one(&pool)
                .await
                .expect("load linked member row");
        let audit_row = sqlx::query(
            "SELECT action, target_ref_json FROM audit_trace_entries WHERE audit_trace_id = $1",
        )
        .bind(format!("audit:{}", memory_metadata.idempotency_key()))
        .fetch_one(&pool)
        .await
        .expect("load memory refs audit row");
        let outbox_row = sqlx::query(
            "SELECT event_type, payload_json FROM outbox_events WHERE outbox_event_id = $1",
        )
        .bind(format!("outbox:{}", memory_metadata.idempotency_key()))
        .fetch_one(&pool)
        .await
        .expect("load memory refs outbox row");
        let idempotency_row = sqlx::query(
            "SELECT status, result_ref_json FROM idempotency_records WHERE idempotency_key = $1",
        )
        .bind(memory_metadata.idempotency_key())
        .fetch_one(&pool)
        .await
        .expect("load memory refs idempotency row");

        let expected_memory_ref_summary_json = json!({
            "memory_refs_id": summary.memory_refs_id.as_str(),
            "semantic_memory_ref": summary.semantic_memory_ref.clone(),
            "episodic_memory_refs": summary.episodic_memory_refs.clone(),
            "archive_ref": summary.archive_ref.clone(),
            "archive_status": summary.archive_status.as_db(),
            "version": summary.version,
        });
        let outbox_payload_json = outbox_row.get::<serde_json::Value, _>("payload_json");
        let idempotency_result_ref_json =
            idempotency_row.get::<serde_json::Value, _>("result_ref_json");

        assert_eq!(summary.global_member_id, member.global_member_id);
        assert_eq!(
            summary.semantic_memory_ref,
            Some(sample_semantic_memory_ref())
        );
        assert_eq!(
            summary.episodic_memory_refs,
            vec![sample_episodic_memory_ref()]
        );
        assert_eq!(
            memory_refs_row.get::<String, _>("memory_refs_id"),
            summary.memory_refs_id.as_str()
        );
        assert_eq!(
            member_row.get::<Option<String>, _>("memory_refs_id"),
            Some(summary.memory_refs_id.as_str().to_string())
        );
        assert_eq!(
            memory_refs_row.get::<serde_json::Value, _>("semantic_memory_ref_json"),
            json!(Some(sample_semantic_memory_ref()))
        );
        assert_eq!(
            memory_refs_row.get::<serde_json::Value, _>("episodic_memory_refs_json"),
            json!(vec![sample_episodic_memory_ref()])
        );
        assert_eq!(
            memory_refs_row.get::<Option<serde_json::Value>, _>("archive_ref_json"),
            None
        );
        assert_eq!(memory_refs_row.get::<String, _>("archive_status"), "none");
        assert_eq!(memory_refs_row.get::<i64, _>("version"), summary.version);
        assert_eq!(audit_row.get::<String, _>("action"), "UpdateMemoryRefs");
        assert_eq!(
            audit_row.get::<serde_json::Value, _>("target_ref_json"),
            json!({
                "kind": "memory_refs",
                "id": summary.memory_refs_id.as_str(),
                "global_member_id": member.global_member_id.as_str(),
            })
        );
        assert_eq!(
            outbox_row.get::<String, _>("event_type"),
            "identity.memory_refs.updated"
        );
        assert_eq!(
            outbox_payload_json.get("memory_ref_summary_json"),
            Some(&expected_memory_ref_summary_json)
        );
        assert_eq!(idempotency_row.get::<String, _>("status"), "succeeded");
        assert_eq!(
            idempotency_result_ref_json
                .get("id")
                .and_then(serde_json::Value::as_str),
            Some(summary.memory_refs_id.as_str())
        );
        assert_eq!(
            query_summary.memory_ref_summary_json,
            expected_memory_ref_summary_json
        );
    }

    #[tokio::test]
    async fn update_memory_refs_rejects_invalid_memory_ref_without_persisting() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let hire_service = MemberLifecycleCommandService::new(factory.clone());
        let service = MemoryRefsCommandService::new(
            factory,
            StubMemoryArchiveValidator::rejecting("memory-bad"),
        );
        let actor = ActorContext::new("human/admin-memory-2", ActorKind::HumanUser, None);

        let member = hire_service
            .hire_global_member(
                HireGlobalMemberCommand {
                    display_name: "Invalid Memory Member".to_string(),
                    main_role_id: RoleId::new("role.member.operator"),
                    secondary_role_ids: Vec::new(),
                },
                actor.clone(),
                CommandMetadata::new(
                    "idem-hire-memory-002",
                    "trace-hire-memory-002",
                    "hash-hire-memory-002",
                ),
            )
            .await
            .expect("hire member before invalid memory ref test");

        let error = service
            .update_memory_refs(
                UpdateMemoryRefsCommand {
                    global_member_id: member.global_member_id.clone(),
                    semantic_memory_ref: Some(MemoryRef {
                        memory_id: "memory-bad".to_string(),
                        memory_kind: "semantic".to_string(),
                        memory_version: Some("v1".to_string()),
                    }),
                    episodic_memory_refs: Vec::new(),
                },
                actor,
                CommandMetadata::new("idem-memory-002", "trace-memory-002", "hash-memory-002"),
            )
            .await
            .expect_err("invalid memory refs should be rejected");

        assert!(matches!(
            error,
            IdentityError::RuleViolation {
                code: "IDENTITY_MEMORY_REF_INVALID",
                ..
            }
        ));

        let memory_refs_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM memory_refs WHERE global_member_id = $1")
                .bind(member.global_member_id.as_str())
                .fetch_one(&pool)
                .await
                .expect("count memory refs rows");
        let audit_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM audit_trace_entries WHERE action = $1")
                .bind("UpdateMemoryRefs")
                .fetch_one(&pool)
                .await
                .expect("count memory refs audits");
        let outbox_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM outbox_events WHERE event_type = 'identity.memory_refs.updated'",
        )
        .fetch_one(&pool)
        .await
        .expect("count memory refs outbox rows");
        let idempotency_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM idempotency_records WHERE idempotency_key = $1",
        )
        .bind("idem-memory-002")
        .fetch_one(&pool)
        .await
        .expect("count memory refs idempotency rows");

        assert_eq!(memory_refs_count, 0);
        assert_eq!(audit_count, 0);
        assert_eq!(outbox_count, 0);
        assert_eq!(idempotency_count, 0);
    }

    #[tokio::test]
    async fn handle_archive_event_updates_status_writes_outbox_and_refreshes_projection() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let hire_service = MemberLifecycleCommandService::new(factory.clone());
        let service =
            MemoryRefsCommandService::new(factory.clone(), StubMemoryArchiveValidator::accepting());
        let rebuild_job = ProjectionRebuildJob::new(factory.clone());
        let query_service = QueryProjectionService::new(factory);
        let actor = ActorContext::new("human/admin-memory-archive-1", ActorKind::HumanUser, None);

        let member = hire_service
            .hire_global_member(
                HireGlobalMemberCommand {
                    display_name: "Archive Member".to_string(),
                    main_role_id: RoleId::new("role.member.operator"),
                    secondary_role_ids: Vec::new(),
                },
                actor.clone(),
                CommandMetadata::new(
                    "idem-hire-memory-archive-001",
                    "trace-hire-memory-archive-001",
                    "hash-hire-memory-archive-001",
                ),
            )
            .await
            .expect("hire member for archive event");

        service
            .update_memory_refs(
                UpdateMemoryRefsCommand {
                    global_member_id: member.global_member_id.clone(),
                    semantic_memory_ref: Some(sample_semantic_memory_ref()),
                    episodic_memory_refs: vec![sample_episodic_memory_ref()],
                },
                actor.clone(),
                CommandMetadata::new(
                    "idem-memory-archive-001",
                    "trace-memory-archive-001",
                    "hash-memory-archive-001",
                ),
            )
            .await
            .expect("seed memory refs before archive event");

        let outcome = service
            .handle_archive_event(sample_memory_archive_event(
                "memory-archive-event-001",
                "memory-archive-event-hash-001",
                member.global_member_id.as_str(),
                "archived",
                current_timestamp() + Duration::seconds(2),
            ))
            .await
            .expect("archive event should succeed");

        sqlx::query("UPDATE outbox_events SET created_at = $2 WHERE outbox_event_id = $1")
            .bind("outbox:idem-hire-memory-archive-001")
            .bind(current_timestamp())
            .execute(&pool)
            .await
            .expect("stabilize hire outbox created_at");
        sqlx::query("UPDATE outbox_events SET created_at = $2 WHERE outbox_event_id = $1")
            .bind("outbox:idem-memory-archive-001")
            .bind(current_timestamp() + Duration::seconds(1))
            .execute(&pool)
            .await
            .expect("stabilize memory refs outbox created_at");
        sqlx::query("UPDATE outbox_events SET created_at = $2 WHERE outbox_event_id = $1")
            .bind("outbox:memory-archive-event-001")
            .bind(current_timestamp() + Duration::seconds(2))
            .execute(&pool)
            .await
            .expect("stabilize archive outbox created_at");

        rebuild_job
            .rebuild_member_summary_projection("member-summary-rebuild", 20)
            .await
            .expect("projection rebuild should succeed");
        let query_summary = query_service
            .get_member_summary(
                GetMemberSummaryQuery {
                    global_member_id: member.global_member_id.clone(),
                },
                actor,
            )
            .await
            .expect("member summary query should succeed after archive rebuild");

        let memory_refs_row = sqlx::query(
            r#"
            SELECT archive_status, archive_ref_json, version
            FROM memory_refs
            WHERE global_member_id = $1
            "#,
        )
        .bind(member.global_member_id.as_str())
        .fetch_one(&pool)
        .await
        .expect("load archived memory refs row");
        let audit_row = sqlx::query(
            "SELECT action, source_module, result, reason FROM audit_trace_entries WHERE audit_trace_id = $1",
        )
        .bind("audit:memory-archive-event-001")
        .fetch_one(&pool)
        .await
        .expect("load archive event audit row");
        let outbox_row = sqlx::query(
            "SELECT event_type, payload_json FROM outbox_events WHERE outbox_event_id = $1",
        )
        .bind("outbox:memory-archive-event-001")
        .fetch_one(&pool)
        .await
        .expect("load archive event outbox row");
        let idempotency_row = sqlx::query(
            "SELECT status, result_ref_json FROM idempotency_records WHERE idempotency_key = $1",
        )
        .bind("memory-archive-event-001")
        .fetch_one(&pool)
        .await
        .expect("load archive idempotency row");

        assert_eq!(
            outcome,
            MemoryArchiveEventOutcome::Updated {
                memory_refs_id: format!("memory-refs:{}", member.global_member_id.as_str()),
            }
        );
        assert_eq!(
            memory_refs_row.get::<String, _>("archive_status"),
            "archived"
        );
        assert_eq!(
            memory_refs_row.get::<Option<serde_json::Value>, _>("archive_ref_json"),
            Some(json!({
                "archive_id": format!("archive-{}", member.global_member_id.as_str()),
                "archive_kind": "member_memory_archive",
                "archive_version": "v1",
            }))
        );
        assert_eq!(
            audit_row.get::<String, _>("action"),
            "HandleMemoryArchiveEvent"
        );
        assert_eq!(
            audit_row.get::<String, _>("source_module"),
            "memory-archive"
        );
        assert_eq!(audit_row.get::<String, _>("result"), "success");
        assert_eq!(audit_row.get::<Option<String>, _>("reason"), None);
        assert_eq!(
            outbox_row.get::<String, _>("event_type"),
            "identity.memory_refs.archive_status_changed"
        );
        assert_eq!(
            outbox_row
                .get::<serde_json::Value, _>("payload_json")
                .get("memory_ref_summary_json")
                .and_then(|value| value.get("archive_status"))
                .and_then(serde_json::Value::as_str),
            Some("archived")
        );
        assert_eq!(idempotency_row.get::<String, _>("status"), "succeeded");
        assert_eq!(
            idempotency_row
                .get::<serde_json::Value, _>("result_ref_json")
                .get("updated_at")
                .is_some(),
            true
        );
        assert_eq!(
            query_summary
                .memory_ref_summary_json
                .get("archive_status")
                .and_then(serde_json::Value::as_str),
            Some("archived")
        );
        assert_eq!(
            query_summary
                .memory_ref_summary_json
                .get("archive_ref")
                .and_then(serde_json::Value::as_object)
                .is_some(),
            true
        );
        assert_eq!(memory_refs_row.get::<i64, _>("version"), 3);
    }

    #[tokio::test]
    async fn handle_archive_event_dead_letters_invalid_status_payload() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let hire_service = MemberLifecycleCommandService::new(factory.clone());
        let service =
            MemoryRefsCommandService::new(factory.clone(), StubMemoryArchiveValidator::accepting());
        let actor = ActorContext::new("human/admin-memory-archive-2", ActorKind::HumanUser, None);

        let member = hire_service
            .hire_global_member(
                HireGlobalMemberCommand {
                    display_name: "Archive Invalid".to_string(),
                    main_role_id: RoleId::new("role.member.operator"),
                    secondary_role_ids: Vec::new(),
                },
                actor.clone(),
                CommandMetadata::new(
                    "idem-hire-memory-archive-002",
                    "trace-hire-memory-archive-002",
                    "hash-hire-memory-archive-002",
                ),
            )
            .await
            .expect("hire member for invalid archive event");

        service
            .update_memory_refs(
                UpdateMemoryRefsCommand {
                    global_member_id: member.global_member_id.clone(),
                    semantic_memory_ref: Some(sample_semantic_memory_ref()),
                    episodic_memory_refs: Vec::new(),
                },
                actor,
                CommandMetadata::new(
                    "idem-memory-archive-002",
                    "trace-memory-archive-002",
                    "hash-memory-archive-002",
                ),
            )
            .await
            .expect("seed memory refs before invalid archive event");

        let outcome = service
            .handle_archive_event(sample_memory_archive_event(
                "memory-archive-event-002",
                "memory-archive-event-hash-002",
                member.global_member_id.as_str(),
                "unknown_status",
                current_timestamp() + Duration::seconds(2),
            ))
            .await
            .expect("invalid archive event should dead-letter");

        let dead_letter_row = sqlx::query(
            "SELECT source_module, event_type, failure_reason, replay_status FROM inbound_dead_letters WHERE source_event_id = $1",
        )
        .bind("memory-archive-event-002")
        .fetch_one(&pool)
        .await
        .expect("load invalid archive dead-letter row");
        let outbox_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM outbox_events WHERE idempotency_key = $1")
                .bind("memory-archive-event-002")
                .fetch_one(&pool)
                .await
                .expect("count invalid archive outbox rows");
        let idempotency_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM idempotency_records WHERE idempotency_key = $1",
        )
        .bind("memory-archive-event-002")
        .fetch_one(&pool)
        .await
        .expect("count invalid archive idempotency rows");

        assert_eq!(outcome, MemoryArchiveEventOutcome::DeadLettered);
        assert_eq!(
            dead_letter_row.get::<String, _>("source_module"),
            "memory-archive"
        );
        assert_eq!(
            dead_letter_row.get::<String, _>("event_type"),
            "memory.archive.status.changed"
        );
        assert_eq!(dead_letter_row.get::<String, _>("replay_status"), "pending");
        assert!(
            dead_letter_row
                .get::<String, _>("failure_reason")
                .contains("unknown archive status `unknown_status`")
        );
        assert_eq!(outbox_count, 0);
        assert_eq!(idempotency_count, 0);
    }

    #[tokio::test]
    async fn handle_archive_event_skips_out_of_order_terminal_regression() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let hire_service = MemberLifecycleCommandService::new(factory.clone());
        let service =
            MemoryRefsCommandService::new(factory.clone(), StubMemoryArchiveValidator::accepting());
        let actor = ActorContext::new("human/admin-memory-archive-3", ActorKind::HumanUser, None);

        let member = hire_service
            .hire_global_member(
                HireGlobalMemberCommand {
                    display_name: "Archive Out Of Order".to_string(),
                    main_role_id: RoleId::new("role.member.operator"),
                    secondary_role_ids: Vec::new(),
                },
                actor.clone(),
                CommandMetadata::new(
                    "idem-hire-memory-archive-003",
                    "trace-hire-memory-archive-003",
                    "hash-hire-memory-archive-003",
                ),
            )
            .await
            .expect("hire member for out-of-order archive test");

        service
            .update_memory_refs(
                UpdateMemoryRefsCommand {
                    global_member_id: member.global_member_id.clone(),
                    semantic_memory_ref: Some(sample_semantic_memory_ref()),
                    episodic_memory_refs: Vec::new(),
                },
                actor,
                CommandMetadata::new(
                    "idem-memory-archive-003",
                    "trace-memory-archive-003",
                    "hash-memory-archive-003",
                ),
            )
            .await
            .expect("seed memory refs before out-of-order archive test");

        let first_outcome = service
            .handle_archive_event(sample_memory_archive_event(
                "memory-archive-event-003a",
                "memory-archive-event-hash-003a",
                member.global_member_id.as_str(),
                "archived",
                current_timestamp() + Duration::seconds(3),
            ))
            .await
            .expect("first archive event should succeed");
        let second_outcome = service
            .handle_archive_event(sample_memory_archive_event(
                "memory-archive-event-003b",
                "memory-archive-event-hash-003b",
                member.global_member_id.as_str(),
                "pending",
                current_timestamp() - Duration::seconds(30),
            ))
            .await
            .expect("out-of-order archive event should be skipped");

        let memory_refs_row =
            sqlx::query("SELECT archive_status FROM memory_refs WHERE global_member_id = $1")
                .bind(member.global_member_id.as_str())
                .fetch_one(&pool)
                .await
                .expect("load memory refs after out-of-order event");
        let outbox_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM outbox_events WHERE event_type = 'identity.memory_refs.archive_status_changed'",
        )
        .fetch_one(&pool)
        .await
        .expect("count archive status outbox rows");

        assert!(matches!(
            first_outcome,
            MemoryArchiveEventOutcome::Updated { .. }
        ));
        assert!(matches!(
            second_outcome,
            MemoryArchiveEventOutcome::SkippedDuplicate { .. }
        ));
        assert_eq!(
            memory_refs_row.get::<String, _>("archive_status"),
            "archived"
        );
        assert_eq!(outbox_count, 1);
    }

    #[tokio::test]
    async fn replay_archive_dead_letter_reuses_existing_row_after_refs_are_restored() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let hire_service = MemberLifecycleCommandService::new(factory.clone());
        let service =
            MemoryRefsCommandService::new(factory.clone(), StubMemoryArchiveValidator::accepting());
        let actor = ActorContext::new("human/admin-memory-archive-4", ActorKind::HumanUser, None);

        let member = hire_service
            .hire_global_member(
                HireGlobalMemberCommand {
                    display_name: "Archive Replay".to_string(),
                    main_role_id: RoleId::new("role.member.operator"),
                    secondary_role_ids: Vec::new(),
                },
                actor.clone(),
                CommandMetadata::new(
                    "idem-hire-memory-archive-004",
                    "trace-hire-memory-archive-004",
                    "hash-hire-memory-archive-004",
                ),
            )
            .await
            .expect("hire member for archive replay");

        let dead_letter_outcome = service
            .handle_archive_event(sample_memory_archive_event(
                "memory-archive-event-004",
                "memory-archive-event-hash-004",
                member.global_member_id.as_str(),
                "failed",
                current_timestamp() + Duration::seconds(2),
            ))
            .await
            .expect("missing memory refs should dead-letter");
        assert_eq!(dead_letter_outcome, MemoryArchiveEventOutcome::DeadLettered);

        let dead_letter_row = sqlx::query(
            "SELECT dead_letter_id, created_at FROM inbound_dead_letters WHERE source_event_id = $1",
        )
        .bind("memory-archive-event-004")
        .fetch_one(&pool)
        .await
        .expect("load archive replay dead-letter row");
        let dead_letter_id = dead_letter_row.get::<String, _>("dead_letter_id");
        let created_at = dead_letter_row.get::<time::PrimitiveDateTime, _>("created_at");

        service
            .update_memory_refs(
                UpdateMemoryRefsCommand {
                    global_member_id: member.global_member_id.clone(),
                    semantic_memory_ref: Some(sample_semantic_memory_ref()),
                    episodic_memory_refs: Vec::new(),
                },
                actor,
                CommandMetadata::new(
                    "idem-memory-archive-004",
                    "trace-memory-archive-004",
                    "hash-memory-archive-004",
                ),
            )
            .await
            .expect("seed memory refs before replay");

        let replay_outcome = service
            .replay_archive_dead_letter(
                DeadLetterId::new(dead_letter_id.clone()),
                created_at,
                sample_memory_archive_event(
                    "memory-archive-event-004",
                    "memory-archive-event-hash-004",
                    member.global_member_id.as_str(),
                    "failed",
                    current_timestamp() + Duration::seconds(4),
                ),
            )
            .await
            .expect("archive replay should succeed");

        let memory_refs_row =
            sqlx::query("SELECT archive_status FROM memory_refs WHERE global_member_id = $1")
                .bind(member.global_member_id.as_str())
                .fetch_one(&pool)
                .await
                .expect("load memory refs after replay");
        let dead_letter_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM inbound_dead_letters WHERE source_event_id = $1",
        )
        .bind("memory-archive-event-004")
        .fetch_one(&pool)
        .await
        .expect("count archive replay dead letters");

        assert!(matches!(
            replay_outcome,
            MemoryArchiveEventOutcome::Updated { .. }
        ));
        assert_eq!(memory_refs_row.get::<String, _>("archive_status"), "failed");
        assert_eq!(dead_letter_count, 1);
    }

    async fn test_pool() -> sqlx::postgres::PgPool {
        let config = AppConfig {
            listen_addr: "127.0.0.1:8080".to_string(),
            database_url: Some(
                "postgres://postgres:postgres@127.0.0.1:5432/quantalithos_identity".to_string(),
            ),
            database_max_connections: 5,
            outbox_publisher_enabled: false,
            outbox_publisher_batch_size: 50,
            outbox_publisher_poll_interval_ms: 1_000,
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

    async fn reset_tables(pool: &sqlx::postgres::PgPool) {
        pool.execute(
            r#"
            TRUNCATE TABLE
                inbound_dead_letters,
                projection_checkpoints,
                member_summary_projection,
                outbox_events,
                idempotency_records,
                audit_trace_entries,
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
        .bind(current_timestamp())
        .execute(pool)
        .await
        .expect("seed role catalog entry");
    }

    fn sample_semantic_memory_ref() -> MemoryRef {
        MemoryRef {
            memory_id: "memory-semantic-001".to_string(),
            memory_kind: "semantic".to_string(),
            memory_version: Some("v1".to_string()),
        }
    }

    fn sample_episodic_memory_ref() -> MemoryRef {
        MemoryRef {
            memory_id: "memory-episodic-001".to_string(),
            memory_kind: "episodic".to_string(),
            memory_version: Some("v1".to_string()),
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
}
