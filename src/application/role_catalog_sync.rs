//! Application service for synchronizing local role catalog entries from inbound events.

use serde_json::json;
use time::{OffsetDateTime, PrimitiveDateTime};

use crate::application::persistence::{
    AuditTraceRepository, IdempotencyStore, InboundDeadLetterStore, OutboxStore,
    RoleCatalogRepository, UnitOfWork, UnitOfWorkFactory,
};
use crate::domain::audit::{AuditResult, AuditTraceEntry};
use crate::domain::dead_letter::{DeadLetterReplayStatus, InboundDeadLetter};
use crate::domain::idempotency::{IdempotencyRecord, IdempotencyScope, IdempotencyStatus};
use crate::domain::outbox::OutboxEvent;
use crate::domain::role_catalog::RoleCatalogEntry;
use crate::domain::shared::ids::{DeadLetterId, OutboxEventId};
use crate::error::IdentityError;
use crate::inbound::events::{
    InboundEventEnvelope, InboundRoleCatalogEvent, RoleCatalogEventParser,
};

/// Summarizes the result of handling a role-catalog inbound event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RoleCatalogSyncOutcome {
    /// A new or updated role catalog entry was written successfully.
    Synced { role_id: String },
    /// The event was already consumed successfully with the same payload hash.
    SkippedDuplicate { role_id: Option<String> },
    /// The payload was captured into dead-letter storage due to parse failure.
    DeadLettered,
}

/// Coordinates inbound role-catalog synchronization using the shared transaction boundary.
#[derive(Debug, Clone)]
pub struct RoleCatalogSyncService<UowFactory> {
    unit_of_work_factory: UowFactory,
    parser: RoleCatalogEventParser,
}

impl<UowFactory> RoleCatalogSyncService<UowFactory> {
    /// Creates a new role-catalog sync service with the provided persistence factory.
    pub fn new(unit_of_work_factory: UowFactory) -> Self {
        Self {
            unit_of_work_factory,
            parser: RoleCatalogEventParser,
        }
    }
}

impl<UowFactory> RoleCatalogSyncService<UowFactory>
where
    UowFactory: UnitOfWorkFactory,
{
    /// Consumes a method-library role-catalog event and synchronizes the local role index.
    ///
    /// # Errors
    ///
    /// Returns an error when persistence fails or when duplicate events conflict on payload hash.
    pub async fn sync_role_catalog(
        &self,
        event: InboundRoleCatalogEvent,
    ) -> Result<RoleCatalogSyncOutcome, IdentityError> {
        self.sync_role_catalog_internal(event.envelope, DeadLetterRetention::CreateNew)
            .await
    }

    /// Replays one existing dead-letter row through the normal role-catalog sync logic.
    pub async fn replay_dead_letter(
        &self,
        dead_letter_id: DeadLetterId,
        created_at: PrimitiveDateTime,
        event: InboundRoleCatalogEvent,
    ) -> Result<RoleCatalogSyncOutcome, IdentityError> {
        self.sync_role_catalog_internal(
            event.envelope,
            DeadLetterRetention::UpdateExisting {
                dead_letter_id,
                created_at,
            },
        )
        .await
    }

    async fn sync_role_catalog_internal(
        &self,
        envelope: InboundEventEnvelope,
        dead_letter_retention: DeadLetterRetention,
    ) -> Result<RoleCatalogSyncOutcome, IdentityError> {
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

        let snapshot = match self.parser.parse(envelope.payload.clone()) {
            Ok(snapshot) => snapshot,
            Err(error) => {
                retain_dead_letter(
                    &mut uow,
                    &dead_letter_retention,
                    &envelope,
                    error.to_string(),
                )
                .await?;
                uow.commit().await?;
                return Ok(RoleCatalogSyncOutcome::DeadLettered);
            }
        };

        let now = current_timestamp();
        let entry = match RoleCatalogEntry::from_role_definition_snapshot(snapshot, now) {
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
                return Ok(RoleCatalogSyncOutcome::DeadLettered);
            }
        };
        let outbox_event = OutboxEvent::for_role_catalog_sync(
            OutboxEventId::new(format!("outbox:{}", envelope.source_event_id.as_str())),
            &entry,
            envelope.source_event_id.as_str(),
            now,
        );
        let audit_entry = AuditTraceEntry::for_inbound_event(
            format!("audit:{}", envelope.source_event_id.as_str()),
            "SyncRoleCatalog",
            envelope.source_module.clone(),
            envelope.source_event_id.as_str(),
            Some(json!({
                "kind": "role_catalog_entry",
                "id": entry.role_id.as_str(),
            })),
            AuditResult::Success,
            None,
            now,
        );

        uow.role_catalog().upsert(&entry).await?;
        uow.audit_traces().append(&audit_entry).await?;
        uow.outbox().append(&outbox_event).await?;
        uow.idempotency()
            .record_success(
                &crate::domain::shared::metadata::CommandMetadata::new(
                    envelope.source_event_id.as_str(),
                    envelope.source_event_id.as_str(),
                    envelope.payload_hash,
                ),
                IdempotencyScope::InboundEvent,
                json!({
                    "kind": "role_catalog_entry",
                    "id": entry.role_id.as_str(),
                }),
            )
            .await?;
        uow.commit().await?;

        Ok(RoleCatalogSyncOutcome::Synced {
            role_id: entry.role_id.as_str().to_string(),
        })
    }

    async fn handle_existing_idempotency_record<Uow>(
        &self,
        existing_record: IdempotencyRecord,
        envelope: &InboundEventEnvelope,
        dead_letter_retention: DeadLetterRetention,
        mut uow: Uow,
    ) -> Result<RoleCatalogSyncOutcome, IdentityError>
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

        let role_id = existing_record
            .result_ref_json
            .as_ref()
            .and_then(|value| value.get("id"))
            .and_then(serde_json::Value::as_str)
            .map(ToString::to_string);

        match existing_record.status {
            IdempotencyStatus::Succeeded => {
                uow.rollback().await?;
                Ok(RoleCatalogSyncOutcome::SkippedDuplicate { role_id })
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

fn current_timestamp() -> PrimitiveDateTime {
    let now = OffsetDateTime::now_utc();
    PrimitiveDateTime::new(now.date(), now.time())
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;
    use sqlx::{Executor, Row, postgres::PgPoolOptions};
    use time::{Duration, OffsetDateTime, PrimitiveDateTime};

    use crate::config::AppConfig;
    use crate::domain::shared::ids::RoleId;
    use crate::inbound::event_consumers::RoleCatalogConsumer;
    use crate::inbound::events::{InboundEventEnvelope, InboundRoleCatalogEvent};
    use crate::persistence::database::run_migrations;
    use crate::persistence::test_support::DB_TEST_MUTEX;
    use crate::persistence::unit_of_work::SqlxUnitOfWorkFactory;

    use super::{RoleCatalogSyncOutcome, RoleCatalogSyncService};

    #[tokio::test]
    async fn sync_role_catalog_persists_role_outbox_audit_and_idempotency() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;

        let service = RoleCatalogSyncService::new(SqlxUnitOfWorkFactory::new(pool.clone()));
        let consumer = RoleCatalogConsumer::new(service);

        let outcome = consumer
            .consume(sample_role_event(
                "role-sync-001",
                "payload-hash-001",
                "role.member.operator",
                "fp-role-001",
                "active",
            ))
            .await
            .expect("sync role catalog should succeed");

        assert_eq!(
            outcome,
            RoleCatalogSyncOutcome::Synced {
                role_id: "role.member.operator".to_string()
            }
        );

        let fingerprint: String =
            sqlx::query("SELECT fingerprint FROM role_catalog_entries WHERE role_id = $1")
                .bind("role.member.operator")
                .fetch_one(&pool)
                .await
                .expect("fetch role fingerprint")
                .get("fingerprint");
        let outbox_event_type: String =
            sqlx::query("SELECT event_type FROM outbox_events WHERE aggregate_id = $1")
                .bind("role.member.operator")
                .fetch_one(&pool)
                .await
                .expect("fetch outbox event")
                .get("event_type");
        let audit_action: String =
            sqlx::query("SELECT action FROM audit_trace_entries WHERE trace_id = $1")
                .bind("role-sync-001")
                .fetch_one(&pool)
                .await
                .expect("fetch audit action")
                .get("action");
        let idempotency_status: String =
            sqlx::query("SELECT status FROM idempotency_records WHERE idempotency_key = $1")
                .bind("role-sync-001")
                .fetch_one(&pool)
                .await
                .expect("fetch idempotency status")
                .get("status");

        assert_eq!(fingerprint, "fp-role-001");
        assert_eq!(outbox_event_type, "identity.role_catalog.synced");
        assert_eq!(audit_action, "SyncRoleCatalog");
        assert_eq!(idempotency_status, "succeeded");
    }

    #[tokio::test]
    async fn sync_role_catalog_is_idempotent_for_same_event_and_hash() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;

        let service = RoleCatalogSyncService::new(SqlxUnitOfWorkFactory::new(pool.clone()));

        let first_outcome = service
            .sync_role_catalog(sample_role_event(
                "role-sync-002",
                "payload-hash-002",
                "role.reviewer",
                "fp-role-002",
                "active",
            ))
            .await
            .expect("first sync should succeed");
        let second_outcome = service
            .sync_role_catalog(sample_role_event(
                "role-sync-002",
                "payload-hash-002",
                "role.reviewer",
                "fp-role-002",
                "active",
            ))
            .await
            .expect("duplicate sync should be skipped");

        assert!(matches!(
            first_outcome,
            RoleCatalogSyncOutcome::Synced { ref role_id } if role_id == "role.reviewer"
        ));
        assert_eq!(
            second_outcome,
            RoleCatalogSyncOutcome::SkippedDuplicate {
                role_id: Some("role.reviewer".to_string())
            }
        );

        let role_count: i64 =
            sqlx::query("SELECT COUNT(*) AS count FROM role_catalog_entries WHERE role_id = $1")
                .bind("role.reviewer")
                .fetch_one(&pool)
                .await
                .expect("count roles")
                .get("count");
        let outbox_count: i64 =
            sqlx::query("SELECT COUNT(*) AS count FROM outbox_events WHERE aggregate_id = $1")
                .bind("role.reviewer")
                .fetch_one(&pool)
                .await
                .expect("count outbox rows")
                .get("count");

        assert_eq!(role_count, 1);
        assert_eq!(outbox_count, 1);
    }

    #[tokio::test]
    async fn sync_role_catalog_updates_fingerprint_for_new_role_snapshot_event() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;

        let service = RoleCatalogSyncService::new(SqlxUnitOfWorkFactory::new(pool.clone()));

        service
            .sync_role_catalog(sample_role_event(
                "role-sync-003",
                "payload-hash-003a",
                "role.architect",
                "fp-role-003a",
                "active",
            ))
            .await
            .expect("first sync should succeed");

        let outcome = service
            .sync_role_catalog(sample_role_event(
                "role-sync-003b",
                "payload-hash-003b",
                "role.architect",
                "fp-role-003b",
                "active",
            ))
            .await
            .expect("second sync should update the existing role");

        assert_eq!(
            outcome,
            RoleCatalogSyncOutcome::Synced {
                role_id: "role.architect".to_string()
            }
        );

        let fingerprint: String =
            sqlx::query("SELECT fingerprint FROM role_catalog_entries WHERE role_id = $1")
                .bind("role.architect")
                .fetch_one(&pool)
                .await
                .expect("fetch updated role fingerprint")
                .get("fingerprint");
        let outbox_count: i64 =
            sqlx::query("SELECT COUNT(*) AS count FROM outbox_events WHERE aggregate_id = $1")
                .bind("role.architect")
                .fetch_one(&pool)
                .await
                .expect("count role outbox rows")
                .get("count");

        assert_eq!(fingerprint, "fp-role-003b");
        assert_eq!(outbox_count, 2);
    }

    #[tokio::test]
    async fn sync_role_catalog_dead_letters_idempotency_conflict_when_hash_differs() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;

        let service = RoleCatalogSyncService::new(SqlxUnitOfWorkFactory::new(pool.clone()));

        service
            .sync_role_catalog(sample_role_event(
                "role-sync-004",
                "payload-hash-004a",
                "role.operator",
                "fp-role-004a",
                "active",
            ))
            .await
            .expect("first sync should succeed");

        let error = service
            .sync_role_catalog(sample_role_event(
                "role-sync-004",
                "payload-hash-004b",
                "role.operator",
                "fp-role-004b",
                "active",
            ))
            .await
            .expect_err("different hash should conflict");

        assert!(matches!(
            error,
            crate::error::IdentityError::PersistenceData { .. }
        ));

        let dead_letter_count: i64 =
            sqlx::query("SELECT COUNT(*) AS count FROM inbound_dead_letters")
                .fetch_one(&pool)
                .await
                .expect("count dead letters")
                .get("count");

        assert_eq!(dead_letter_count, 1);
    }

    #[tokio::test]
    async fn sync_role_catalog_dead_letters_invalid_payload() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;

        let service = RoleCatalogSyncService::new(SqlxUnitOfWorkFactory::new(pool.clone()));
        let event = InboundRoleCatalogEvent {
            envelope: InboundEventEnvelope {
                source_event_id: crate::domain::shared::ids::EventId::new("role-sync-004"),
                source_module: "method-library".to_string(),
                event_type: "role.definition.updated".to_string(),
                occurred_at: now(),
                payload_hash: "payload-hash-004".to_string(),
                payload: json!({
                    "unexpected_field": true
                }),
            },
        };

        let outcome = service
            .sync_role_catalog(event)
            .await
            .expect("invalid payload should be dead-lettered");

        assert_eq!(outcome, RoleCatalogSyncOutcome::DeadLettered);

        let dead_letter_count: i64 =
            sqlx::query("SELECT COUNT(*) AS count FROM inbound_dead_letters")
                .fetch_one(&pool)
                .await
                .expect("count dead letters")
                .get("count");
        let role_count: i64 = sqlx::query("SELECT COUNT(*) AS count FROM role_catalog_entries")
            .fetch_one(&pool)
            .await
            .expect("count role rows")
            .get("count");

        assert_eq!(dead_letter_count, 1);
        assert_eq!(role_count, 0);
    }

    #[tokio::test]
    async fn sync_role_catalog_dead_letters_semantically_invalid_snapshot() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;

        let service = RoleCatalogSyncService::new(SqlxUnitOfWorkFactory::new(pool.clone()));

        let outcome = service
            .sync_role_catalog(sample_role_event(
                "role-sync-006",
                "payload-hash-006",
                "role.invalid",
                "fp-role-006",
                "unexpected-status",
            ))
            .await
            .expect("semantically invalid snapshot should be dead-lettered");

        assert_eq!(outcome, RoleCatalogSyncOutcome::DeadLettered);

        let dead_letter_count: i64 =
            sqlx::query("SELECT COUNT(*) AS count FROM inbound_dead_letters")
                .fetch_one(&pool)
                .await
                .expect("count dead letters")
                .get("count");
        let role_count: i64 = sqlx::query("SELECT COUNT(*) AS count FROM role_catalog_entries")
            .fetch_one(&pool)
            .await
            .expect("count role rows")
            .get("count");

        assert_eq!(dead_letter_count, 1);
        assert_eq!(role_count, 0);
    }

    #[tokio::test]
    async fn sync_role_catalog_repeated_invalid_payload_creates_multiple_dead_letters() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;

        let service = RoleCatalogSyncService::new(SqlxUnitOfWorkFactory::new(pool.clone()));
        let event = InboundRoleCatalogEvent {
            envelope: InboundEventEnvelope {
                source_event_id: crate::domain::shared::ids::EventId::new("role-sync-005"),
                source_module: "method-library".to_string(),
                event_type: "role.definition.updated".to_string(),
                occurred_at: now(),
                payload_hash: "payload-hash-005".to_string(),
                payload: json!({
                    "unexpected_field": true
                }),
            },
        };

        let first_outcome = service
            .sync_role_catalog(event.clone())
            .await
            .expect("first invalid payload should be dead-lettered");
        let second_outcome = service
            .sync_role_catalog(event)
            .await
            .expect("second invalid payload should also be dead-lettered");

        assert_eq!(first_outcome, RoleCatalogSyncOutcome::DeadLettered);
        assert_eq!(second_outcome, RoleCatalogSyncOutcome::DeadLettered);

        let dead_letter_count: i64 =
            sqlx::query("SELECT COUNT(*) AS count FROM inbound_dead_letters")
                .fetch_one(&pool)
                .await
                .expect("count dead letters")
                .get("count");

        assert_eq!(dead_letter_count, 2);
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

    fn sample_role_event(
        source_event_id: &str,
        payload_hash: &str,
        role_id: &str,
        fingerprint: &str,
        status: &str,
    ) -> InboundRoleCatalogEvent {
        InboundRoleCatalogEvent {
            envelope: InboundEventEnvelope {
                source_event_id: crate::domain::shared::ids::EventId::new(source_event_id),
                source_module: "method-library".to_string(),
                event_type: "role.definition.updated".to_string(),
                occurred_at: now() + Duration::seconds(1),
                payload_hash: payload_hash.to_string(),
                payload: json!({
                    "role_snapshot": {
                        "role_id": RoleId::new(role_id),
                        "role_name": format!("Name for {role_id}"),
                        "role_version": "2026.05",
                        "source_ref": {
                            "module": "method-library",
                            "id": role_id,
                        },
                        "fingerprint": fingerprint,
                        "status": status,
                    }
                }),
            },
        }
    }

    fn now() -> PrimitiveDateTime {
        let now = OffsetDateTime::now_utc();
        PrimitiveDateTime::new(now.date(), now.time())
    }
}
