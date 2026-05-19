//! Application service for append-only career history consumption from inbound facts.

use serde_json::json;
use time::{OffsetDateTime, PrimitiveDateTime};

use crate::application::persistence::{
    AuditTraceRepository, CareerHistoryRepository, GlobalMemberRepository, IdempotencyStore,
    InboundDeadLetterStore, OutboxStore, UnitOfWork, UnitOfWorkFactory,
};
use crate::domain::audit::{AuditResult, AuditTraceEntry};
use crate::domain::career_history::{CareerEntry, ProcessFactEvent, WorkFactEvent};
use crate::domain::dead_letter::{DeadLetterReplayStatus, InboundDeadLetter};
use crate::domain::idempotency::{IdempotencyRecord, IdempotencyScope, IdempotencyStatus};
use crate::domain::outbox::OutboxEvent;
use crate::domain::shared::ids::{CareerEntryId, DeadLetterId, OutboxEventId};
use crate::domain::shared::metadata::CommandMetadata;
use crate::error::IdentityError;
use crate::inbound::events::{InboundEventEnvelope, InboundProcessFactEvent, InboundWorkFactEvent};

/// Summarizes the result of handling one inbound career-history event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CareerEventOutcome {
    /// A new career entry was appended successfully.
    Appended { career_entry_id: String },
    /// The inbound event had already been consumed successfully with the same payload hash.
    SkippedDuplicate { career_entry_id: Option<String> },
    /// The inbound event was retained into dead-letter storage instead of mutating write models.
    DeadLettered,
}

/// Coordinates append-only career-history writes behind the shared transaction boundary.
#[derive(Debug, Clone)]
pub struct CareerEventConsumerService<UowFactory> {
    unit_of_work_factory: UowFactory,
}

impl<UowFactory> CareerEventConsumerService<UowFactory> {
    /// Creates a new career event service bound to the provided persistence factory.
    pub fn new(unit_of_work_factory: UowFactory) -> Self {
        Self {
            unit_of_work_factory,
        }
    }
}

impl<UowFactory> CareerEventConsumerService<UowFactory>
where
    UowFactory: UnitOfWorkFactory,
{
    /// Consumes a work-domain fact event and appends one career-history entry when valid.
    pub async fn consume_work_event(
        &self,
        event: InboundWorkFactEvent,
    ) -> Result<CareerEventOutcome, IdentityError> {
        self.consume_event(
            event.envelope,
            build_work_entry,
            DeadLetterRetention::CreateNew,
        )
        .await
    }

    /// Consumes a process-domain fact event and appends one career-history entry when valid.
    pub async fn consume_process_event(
        &self,
        event: InboundProcessFactEvent,
    ) -> Result<CareerEventOutcome, IdentityError> {
        self.consume_event(
            event.envelope,
            build_process_entry,
            DeadLetterRetention::CreateNew,
        )
        .await
    }

    /// Replays one existing dead-letter row through the normal work-event consumer logic.
    pub async fn replay_work_dead_letter(
        &self,
        dead_letter_id: DeadLetterId,
        created_at: PrimitiveDateTime,
        event: InboundWorkFactEvent,
    ) -> Result<CareerEventOutcome, IdentityError> {
        self.consume_event(
            event.envelope,
            build_work_entry,
            DeadLetterRetention::UpdateExisting {
                dead_letter_id,
                created_at,
            },
        )
        .await
    }

    /// Replays one existing dead-letter row through the normal process-event consumer logic.
    pub async fn replay_process_dead_letter(
        &self,
        dead_letter_id: DeadLetterId,
        created_at: PrimitiveDateTime,
        event: InboundProcessFactEvent,
    ) -> Result<CareerEventOutcome, IdentityError> {
        self.consume_event(
            event.envelope,
            build_process_entry,
            DeadLetterRetention::UpdateExisting {
                dead_letter_id,
                created_at,
            },
        )
        .await
    }

    async fn consume_event<EntryBuilder>(
        &self,
        envelope: InboundEventEnvelope,
        entry_builder: EntryBuilder,
        dead_letter_retention: DeadLetterRetention,
    ) -> Result<CareerEventOutcome, IdentityError>
    where
        EntryBuilder:
            FnOnce(&InboundEventEnvelope, PrimitiveDateTime) -> Result<CareerEntry, IdentityError>,
    {
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
                .handle_existing_idempotency_record(
                    existing_record,
                    &envelope,
                    dead_letter_retention,
                    uow,
                )
                .await;
        }

        let now = current_timestamp();
        let entry = match entry_builder(&envelope, now) {
            Ok(entry) => entry,
            Err(error) => {
                retain_dead_letter(
                    &mut uow,
                    &dead_letter_retention,
                    &envelope,
                    error.to_string(),
                )
                .await?;
                uow.commit().await?;
                return Ok(CareerEventOutcome::DeadLettered);
            }
        };

        let member = match {
            let mut repository = uow.global_members();
            repository.get_for_update(&entry.global_member_id).await?
        } {
            Some(member) => member,
            None => {
                retain_dead_letter(
                    &mut uow,
                    &dead_letter_retention,
                    &envelope,
                    format!(
                        "IDENTITY_MEMBER_NOT_FOUND: global member `{}` was not found",
                        entry.global_member_id.as_str()
                    ),
                )
                .await?;
                uow.commit().await?;
                return Ok(CareerEventOutcome::DeadLettered);
            }
        };

        let mut history = uow
            .career_history()
            .get_for_update(&entry.global_member_id)
            .await?;
        if history.contains_source_event(&entry.source_event_id) {
            uow.rollback().await?;
            return Ok(CareerEventOutcome::SkippedDuplicate {
                career_entry_id: Some(entry.career_entry_id.as_str().to_string()),
            });
        }

        if let Err(error) = history.append(entry.clone()) {
            if matches!(
                error,
                IdentityError::RuleViolation {
                    code: "IDENTITY_IDEMPOTENCY_CONFLICT",
                    ..
                }
            ) {
                uow.rollback().await?;
                return Ok(CareerEventOutcome::SkippedDuplicate {
                    career_entry_id: Some(entry.career_entry_id.as_str().to_string()),
                });
            }
            uow.rollback().await?;
            return Err(error);
        }

        let metadata = CommandMetadata::new(
            envelope.source_event_id.as_str(),
            envelope.source_event_id.as_str(),
            envelope.payload_hash.clone(),
        );
        let audit_entry = AuditTraceEntry::for_inbound_event(
            format!("audit:{}", envelope.source_event_id.as_str()),
            "AppendCareerEntry",
            envelope.source_module.clone(),
            envelope.source_event_id.as_str(),
            Some(json!({
                "kind": "career_entry",
                "id": entry.career_entry_id.as_str(),
                "global_member_id": entry.global_member_id.as_str(),
            })),
            AuditResult::Success,
            None,
            now,
        );
        let outbox_event = OutboxEvent::for_career_history_appended(
            OutboxEventId::new(format!("outbox:{}", envelope.source_event_id.as_str())),
            &member,
            &history,
            envelope.source_event_id.as_str(),
            envelope.source_event_id.as_str(),
            now,
        );

        uow.career_history().save(&history).await?;
        uow.audit_traces().append(&audit_entry).await?;
        uow.outbox().append(&outbox_event).await?;
        uow.idempotency()
            .record_success(
                &metadata,
                IdempotencyScope::InboundEvent,
                json!({
                    "kind": "career_entry",
                    "id": entry.career_entry_id.as_str(),
                    "global_member_id": entry.global_member_id.as_str(),
                }),
            )
            .await?;
        uow.commit().await?;

        Ok(CareerEventOutcome::Appended {
            career_entry_id: entry.career_entry_id.as_str().to_string(),
        })
    }

    async fn handle_existing_idempotency_record<Uow>(
        &self,
        existing_record: IdempotencyRecord,
        envelope: &InboundEventEnvelope,
        dead_letter_retention: DeadLetterRetention,
        mut uow: Uow,
    ) -> Result<CareerEventOutcome, IdentityError>
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
            retain_dead_letter(
                &mut uow,
                &dead_letter_retention,
                envelope,
                error.to_string(),
            )
            .await?;
            uow.commit().await?;
            return Err(error);
        }

        let career_entry_id = existing_record
            .result_ref_json
            .as_ref()
            .and_then(|value| value.get("id"))
            .and_then(serde_json::Value::as_str)
            .map(ToString::to_string);

        match existing_record.status {
            IdempotencyStatus::Succeeded => {
                uow.rollback().await?;
                Ok(CareerEventOutcome::SkippedDuplicate { career_entry_id })
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

fn build_work_entry(
    envelope: &InboundEventEnvelope,
    created_at: PrimitiveDateTime,
) -> Result<CareerEntry, IdentityError> {
    let event: WorkFactEvent =
        serde_json::from_value(envelope.payload.clone()).map_err(|error| {
            IdentityError::PersistenceData {
                message: format!("decode work fact payload: {error}"),
            }
        })?;

    CareerEntry::from_work_event(
        career_entry_id_for(envelope),
        envelope.source_event_id.clone(),
        event,
        created_at,
    )
}

fn build_process_entry(
    envelope: &InboundEventEnvelope,
    created_at: PrimitiveDateTime,
) -> Result<CareerEntry, IdentityError> {
    let event: ProcessFactEvent =
        serde_json::from_value(envelope.payload.clone()).map_err(|error| {
            IdentityError::PersistenceData {
                message: format!("decode process fact payload: {error}"),
            }
        })?;

    CareerEntry::from_process_event(
        career_entry_id_for(envelope),
        envelope.source_event_id.clone(),
        event,
        created_at,
    )
}

fn career_entry_id_for(envelope: &InboundEventEnvelope) -> CareerEntryId {
    CareerEntryId::new(format!(
        "career-entry:{}",
        envelope.source_event_id.as_str()
    ))
}

fn current_timestamp() -> PrimitiveDateTime {
    let now = OffsetDateTime::now_utc();
    PrimitiveDateTime::new(now.date(), now.time())
}

fn build_dead_letter(
    envelope: &InboundEventEnvelope,
    failure_reason: impl Into<String>,
) -> InboundDeadLetter {
    let now = OffsetDateTime::now_utc();

    InboundDeadLetter {
        dead_letter_id: DeadLetterId::new(format!(
            "dead-letter:{}:{}",
            envelope.source_event_id.as_str(),
            now.unix_timestamp_nanos(),
        )),
        source_event_id: Some(envelope.source_event_id.clone()),
        source_module: envelope.source_module.clone(),
        event_type: envelope.event_type.clone(),
        payload_json: envelope.payload.clone(),
        failure_reason: failure_reason.into(),
        replay_status: DeadLetterReplayStatus::Pending,
        created_at: PrimitiveDateTime::new(now.date(), now.time()),
    }
}

async fn retain_dead_letter<Uow>(
    uow: &mut Uow,
    dead_letter_retention: &DeadLetterRetention,
    envelope: &InboundEventEnvelope,
    failure_reason: impl Into<String>,
) -> Result<(), IdentityError>
where
    Uow: UnitOfWork,
{
    let failure_reason = failure_reason.into();
    match dead_letter_retention {
        DeadLetterRetention::CreateNew => {
            let dead_letter = build_dead_letter(envelope, failure_reason);
            uow.inbound_dead_letters().append(&dead_letter).await
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;
    use sqlx::{Executor, Row, postgres::PgPoolOptions};
    use time::Duration;

    use crate::application::member_lifecycle::MemberLifecycleCommandService;
    use crate::application::query_projection::{GetMemberSummaryQuery, QueryProjectionService};
    use crate::config::AppConfig;
    use crate::domain::member::HireGlobalMemberCommand;
    use crate::domain::shared::context::{ActorContext, ActorKind};
    use crate::domain::shared::ids::{EventId, GlobalMemberId, ProjectId, RoleId};
    use crate::domain::shared::metadata::CommandMetadata;
    use crate::inbound::event_consumers::CareerEventConsumer;
    use crate::inbound::events::{
        InboundEventEnvelope, InboundProcessFactEvent, InboundWorkFactEvent,
    };
    use crate::operations::ProjectionRebuildJob;
    use crate::persistence::database::run_migrations;
    use crate::persistence::test_support::DB_TEST_MUTEX;
    use crate::persistence::unit_of_work::SqlxUnitOfWorkFactory;

    use super::{CareerEventConsumerService, CareerEventOutcome, current_timestamp};

    #[tokio::test]
    async fn consume_work_event_appends_career_history_and_refreshes_projection() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let hire_service = MemberLifecycleCommandService::new(factory.clone());
        let consumer = CareerEventConsumer::new(CareerEventConsumerService::new(factory.clone()));
        let rebuild_job = ProjectionRebuildJob::new(factory.clone());
        let query_service = QueryProjectionService::new(factory);
        let actor = ActorContext::new("human/admin-career-1", ActorKind::HumanUser, None);
        let hire_metadata = CommandMetadata::new(
            "idem-hire-career-001",
            "trace-hire-career-001",
            "hash-hire-career-001",
        );

        let member = hire_service
            .hire_global_member(
                HireGlobalMemberCommand {
                    display_name: "Career Member".to_string(),
                    main_role_id: RoleId::new("role.member.operator"),
                    secondary_role_ids: Vec::new(),
                },
                actor.clone(),
                hire_metadata.clone(),
            )
            .await
            .expect("hire member for career append");

        let outcome = consumer
            .consume_work_event(sample_work_event(
                "career-event-001",
                "hash-career-event-001",
                member.global_member_id.as_str(),
            ))
            .await
            .expect("career work event should be consumed");

        assert_eq!(
            outcome,
            CareerEventOutcome::Appended {
                career_entry_id: "career-entry:career-event-001".to_string()
            }
        );

        sqlx::query("UPDATE outbox_events SET created_at = $2 WHERE outbox_event_id = $1")
            .bind(format!("outbox:{}", hire_metadata.idempotency_key()))
            .bind(current_timestamp())
            .execute(&pool)
            .await
            .expect("stabilize hire outbox created_at");
        sqlx::query("UPDATE outbox_events SET created_at = $2 WHERE outbox_event_id = $1")
            .bind("outbox:career-event-001")
            .bind(current_timestamp() + Duration::seconds(1))
            .execute(&pool)
            .await
            .expect("stabilize career outbox created_at");

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

        let career_row = sqlx::query(
            r#"
            SELECT
                career_entry_id,
                source_event_id,
                source_module,
                project_id,
                work_ref_json,
                process_ref_json,
                entry_kind,
                payload_summary_json
            FROM career_history_entries
            WHERE global_member_id = $1
            "#,
        )
        .bind(member.global_member_id.as_str())
        .fetch_one(&pool)
        .await
        .expect("load career history row");
        let audit_row = sqlx::query(
            "SELECT action, source_module, target_ref_json FROM audit_trace_entries WHERE audit_trace_id = $1",
        )
        .bind("audit:career-event-001")
        .fetch_one(&pool)
        .await
        .expect("load career audit row");
        let outbox_row = sqlx::query(
            "SELECT event_type, payload_json FROM outbox_events WHERE outbox_event_id = $1",
        )
        .bind("outbox:career-event-001")
        .fetch_one(&pool)
        .await
        .expect("load career outbox row");
        let idempotency_row = sqlx::query(
            "SELECT status, result_ref_json FROM idempotency_records WHERE idempotency_key = $1",
        )
        .bind("career-event-001")
        .fetch_one(&pool)
        .await
        .expect("load career idempotency row");

        let outbox_payload_json = outbox_row.get::<serde_json::Value, _>("payload_json");
        let career_summary_json = outbox_payload_json
            .get("career_summary_json")
            .cloned()
            .expect("career summary json should exist");
        let entry_summaries = career_summary_json
            .get("entries")
            .and_then(serde_json::Value::as_array)
            .expect("career entry summaries should be an array");

        assert_eq!(
            career_row.get::<String, _>("career_entry_id"),
            "career-entry:career-event-001"
        );
        assert_eq!(
            career_row.get::<String, _>("source_event_id"),
            "career-event-001"
        );
        assert_eq!(career_row.get::<String, _>("source_module"), "work");
        assert_eq!(
            career_row.get::<Option<String>, _>("project_id"),
            Some("project-001".to_string())
        );
        assert_eq!(
            career_row.get::<serde_json::Value, _>("work_ref_json"),
            json!({
                "work_id": "work-001",
                "work_kind": "task",
                "work_version": "v1",
            })
        );
        assert_eq!(
            career_row.get::<Option<serde_json::Value>, _>("process_ref_json"),
            None
        );
        assert_eq!(career_row.get::<String, _>("entry_kind"), "assigned");
        assert_eq!(
            career_row.get::<Option<serde_json::Value>, _>("payload_summary_json"),
            Some(json!({
                "title": "Implement career append flow",
                "score": "high",
            }))
        );
        assert_eq!(audit_row.get::<String, _>("action"), "AppendCareerEntry");
        assert_eq!(
            audit_row.get::<Option<String>, _>("source_module"),
            Some("work".to_string())
        );
        assert_eq!(
            audit_row.get::<serde_json::Value, _>("target_ref_json"),
            json!({
                "kind": "career_entry",
                "id": "career-entry:career-event-001",
                "global_member_id": member.global_member_id.as_str(),
            })
        );
        assert_eq!(
            outbox_row.get::<String, _>("event_type"),
            "identity.career_history.appended"
        );
        assert_eq!(
            outbox_payload_json.get("global_member_id"),
            Some(&json!(member.global_member_id.as_str()))
        );
        assert_eq!(outbox_payload_json.get("version"), Some(&json!(1)));
        assert_eq!(career_summary_json.get("entry_count"), Some(&json!(1)));
        assert_eq!(entry_summaries.len(), 1);
        assert_eq!(
            entry_summaries[0].get("source_event_id"),
            Some(&json!("career-event-001"))
        );
        assert_eq!(query_summary.career_summary_json, career_summary_json);
        assert_eq!(idempotency_row.get::<String, _>("status"), "succeeded");
        assert_eq!(
            idempotency_row
                .get::<serde_json::Value, _>("result_ref_json")
                .get("id")
                .and_then(serde_json::Value::as_str),
            Some("career-entry:career-event-001")
        );
    }

    #[tokio::test]
    async fn consume_work_event_is_idempotent_for_same_event_and_hash() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let hire_service = MemberLifecycleCommandService::new(factory.clone());
        let consumer = CareerEventConsumer::new(CareerEventConsumerService::new(factory));
        let actor = ActorContext::new("human/admin-career-2", ActorKind::HumanUser, None);

        let member = hire_service
            .hire_global_member(
                HireGlobalMemberCommand {
                    display_name: "Duplicate Career Member".to_string(),
                    main_role_id: RoleId::new("role.member.operator"),
                    secondary_role_ids: Vec::new(),
                },
                actor,
                CommandMetadata::new(
                    "idem-hire-career-002",
                    "trace-hire-career-002",
                    "hash-hire-career-002",
                ),
            )
            .await
            .expect("hire member for duplicate career event");

        let first_outcome = consumer
            .consume_work_event(sample_work_event(
                "career-event-002",
                "hash-career-event-002",
                member.global_member_id.as_str(),
            ))
            .await
            .expect("first career work event should succeed");
        let second_outcome = consumer
            .consume_work_event(sample_work_event(
                "career-event-002",
                "hash-career-event-002",
                member.global_member_id.as_str(),
            ))
            .await
            .expect("duplicate career work event should be skipped");

        let career_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM career_history_entries WHERE global_member_id = $1",
        )
        .bind(member.global_member_id.as_str())
        .fetch_one(&pool)
        .await
        .expect("count career history rows");
        let outbox_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM outbox_events WHERE event_type = 'identity.career_history.appended'",
        )
        .fetch_one(&pool)
        .await
        .expect("count career outbox rows");

        assert_eq!(
            first_outcome,
            CareerEventOutcome::Appended {
                career_entry_id: "career-entry:career-event-002".to_string()
            }
        );
        assert_eq!(
            second_outcome,
            CareerEventOutcome::SkippedDuplicate {
                career_entry_id: Some("career-entry:career-event-002".to_string())
            }
        );
        assert_eq!(career_count, 1);
        assert_eq!(outbox_count, 1);
    }

    #[tokio::test]
    async fn consume_process_event_dead_letters_when_member_is_missing() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let consumer = CareerEventConsumer::new(CareerEventConsumerService::new(factory));

        let outcome = consumer
            .consume_process_event(sample_process_event(
                "career-event-003",
                "hash-career-event-003",
                "member-missing",
            ))
            .await
            .expect("missing-member career process event should dead-letter");

        let dead_letter_row = sqlx::query(
            "SELECT source_module, event_type, failure_reason FROM inbound_dead_letters WHERE source_event_id = $1",
        )
        .bind("career-event-003")
        .fetch_one(&pool)
        .await
        .expect("load career dead-letter row");
        let career_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM career_history_entries")
            .fetch_one(&pool)
            .await
            .expect("count career history rows");
        let outbox_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM outbox_events WHERE event_type = 'identity.career_history.appended'",
        )
        .fetch_one(&pool)
        .await
        .expect("count career outbox rows");
        let idempotency_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM idempotency_records WHERE idempotency_key = $1",
        )
        .bind("career-event-003")
        .fetch_one(&pool)
        .await
        .expect("count career idempotency rows");

        assert_eq!(outcome, CareerEventOutcome::DeadLettered);
        assert_eq!(dead_letter_row.get::<String, _>("source_module"), "process");
        assert_eq!(
            dead_letter_row.get::<String, _>("event_type"),
            "process.activity.completed"
        );
        assert!(
            dead_letter_row
                .get::<String, _>("failure_reason")
                .contains("IDENTITY_MEMBER_NOT_FOUND")
        );
        assert_eq!(career_count, 0);
        assert_eq!(outbox_count, 0);
        assert_eq!(idempotency_count, 0);
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

    fn sample_work_event(
        source_event_id: &str,
        payload_hash: &str,
        global_member_id: &str,
    ) -> InboundWorkFactEvent {
        InboundWorkFactEvent {
            envelope: InboundEventEnvelope {
                source_event_id: EventId::new(source_event_id),
                source_module: "work".to_string(),
                event_type: "work.fact.recorded".to_string(),
                occurred_at: current_timestamp() + Duration::seconds(1),
                payload_hash: payload_hash.to_string(),
                payload: json!({
                    "global_member_id": GlobalMemberId::new(global_member_id),
                    "project_id": ProjectId::new("project-001"),
                    "work_ref": {
                        "work_id": "work-001",
                        "work_kind": "task",
                        "work_version": "v1",
                    },
                    "entry_kind": "assigned",
                    "started_at": current_timestamp(),
                    "ended_at": current_timestamp() + Duration::seconds(30),
                    "payload_summary": {
                        "title": "Implement career append flow",
                        "score": "high",
                    }
                }),
            },
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
                occurred_at: current_timestamp() + Duration::seconds(1),
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
                    "started_at": current_timestamp(),
                    "ended_at": current_timestamp() + Duration::seconds(45),
                    "payload_summary": {
                        "activity_name": "Career review",
                    }
                }),
            },
        }
    }
}
