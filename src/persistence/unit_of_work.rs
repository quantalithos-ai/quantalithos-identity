//! SQLx-backed unit-of-work implementation for local identity write transactions.

use sqlx::{Postgres, Transaction, postgres::PgPool};

use crate::application::persistence::UnitOfWorkFactory;
use crate::error::IdentityError;
use crate::persistence::repositories::{
    SqlxAuditTraceRepository, SqlxCapabilityProfileRepository, SqlxCareerHistoryRepository,
    SqlxGlobalMemberRepository, SqlxIdempotencyStore, SqlxInboundDeadLetterStore,
    SqlxLifecycleHistoryRepository, SqlxMemberSummaryProjectionRepository,
    SqlxMemoryRefsRepository, SqlxOutboxStore, SqlxProjectionCheckpointRepository,
    SqlxRoleCatalogRepository,
};

/// Creates PostgreSQL transaction-scoped units of work for write-model operations.
#[derive(Debug, Clone)]
pub struct SqlxUnitOfWorkFactory {
    pool: PgPool,
}

impl SqlxUnitOfWorkFactory {
    /// Creates a new factory backed by the provided shared PostgreSQL connection pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }
}

/// Concrete SQLx unit of work that owns a single PostgreSQL transaction.
pub struct SqlxUnitOfWork<'db> {
    transaction: Transaction<'db, Postgres>,
}

impl<'db> SqlxUnitOfWork<'db> {
    /// Creates a new unit of work from an already-open SQL transaction.
    pub fn new(transaction: Transaction<'db, Postgres>) -> Self {
        Self { transaction }
    }
}

impl<'db> crate::application::persistence::UnitOfWork for SqlxUnitOfWork<'db> {
    type GlobalMembers<'a>
        = SqlxGlobalMemberRepository<'a, 'db>
    where
        Self: 'a;
    type RoleCatalog<'a>
        = SqlxRoleCatalogRepository<'a, 'db>
    where
        Self: 'a;
    type CapabilityProfiles<'a>
        = SqlxCapabilityProfileRepository<'a, 'db>
    where
        Self: 'a;
    type MemoryRefs<'a>
        = SqlxMemoryRefsRepository<'a, 'db>
    where
        Self: 'a;
    type CareerHistory<'a>
        = SqlxCareerHistoryRepository<'a, 'db>
    where
        Self: 'a;
    type LifecycleHistory<'a>
        = SqlxLifecycleHistoryRepository<'a, 'db>
    where
        Self: 'a;
    type AuditTraces<'a>
        = SqlxAuditTraceRepository<'a, 'db>
    where
        Self: 'a;
    type Idempotency<'a>
        = SqlxIdempotencyStore<'a, 'db>
    where
        Self: 'a;
    type Outbox<'a>
        = SqlxOutboxStore<'a, 'db>
    where
        Self: 'a;
    type MemberSummaryProjection<'a>
        = SqlxMemberSummaryProjectionRepository<'a, 'db>
    where
        Self: 'a;
    type ProjectionCheckpoints<'a>
        = SqlxProjectionCheckpointRepository<'a, 'db>
    where
        Self: 'a;
    type InboundDeadLetters<'a>
        = SqlxInboundDeadLetterStore<'a, 'db>
    where
        Self: 'a;

    fn global_members(&mut self) -> Self::GlobalMembers<'_> {
        SqlxGlobalMemberRepository::new(&mut self.transaction)
    }

    fn role_catalog(&mut self) -> Self::RoleCatalog<'_> {
        SqlxRoleCatalogRepository::new(&mut self.transaction)
    }

    fn capability_profiles(&mut self) -> Self::CapabilityProfiles<'_> {
        SqlxCapabilityProfileRepository::new(&mut self.transaction)
    }

    fn memory_refs(&mut self) -> Self::MemoryRefs<'_> {
        SqlxMemoryRefsRepository::new(&mut self.transaction)
    }

    fn career_history(&mut self) -> Self::CareerHistory<'_> {
        SqlxCareerHistoryRepository::new(&mut self.transaction)
    }

    fn lifecycle_history(&mut self) -> Self::LifecycleHistory<'_> {
        SqlxLifecycleHistoryRepository::new(&mut self.transaction)
    }

    fn audit_traces(&mut self) -> Self::AuditTraces<'_> {
        SqlxAuditTraceRepository::new(&mut self.transaction)
    }

    fn idempotency(&mut self) -> Self::Idempotency<'_> {
        SqlxIdempotencyStore::new(&mut self.transaction)
    }

    fn outbox(&mut self) -> Self::Outbox<'_> {
        SqlxOutboxStore::new(&mut self.transaction)
    }

    fn member_summary_projection(&mut self) -> Self::MemberSummaryProjection<'_> {
        SqlxMemberSummaryProjectionRepository::new(&mut self.transaction)
    }

    fn projection_checkpoints(&mut self) -> Self::ProjectionCheckpoints<'_> {
        SqlxProjectionCheckpointRepository::new(&mut self.transaction)
    }

    fn inbound_dead_letters(&mut self) -> Self::InboundDeadLetters<'_> {
        SqlxInboundDeadLetterStore::new(&mut self.transaction)
    }

    async fn commit(self) -> Result<(), IdentityError> {
        self.transaction
            .commit()
            .await
            .map_err(IdentityError::DatabasePool)
    }

    async fn rollback(self) -> Result<(), IdentityError> {
        self.transaction
            .rollback()
            .await
            .map_err(IdentityError::DatabasePool)
    }
}

impl UnitOfWorkFactory for SqlxUnitOfWorkFactory {
    type UnitOfWork<'a>
        = SqlxUnitOfWork<'a>
    where
        Self: 'a;

    async fn begin(&self) -> Result<Self::UnitOfWork<'_>, IdentityError> {
        let transaction = self
            .pool
            .begin()
            .await
            .map_err(IdentityError::DatabasePool)?;
        Ok(SqlxUnitOfWork::new(transaction))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;
    use sqlx::{Executor, Row, postgres::PgPoolOptions};
    use time::{OffsetDateTime, PrimitiveDateTime};

    use crate::application::persistence::{
        AuditTraceRepository, CapabilityProfileRepository, GlobalMemberRepository,
        IdempotencyStore, InboundDeadLetterStore, LifecycleHistoryRepository,
        MemberSummaryProjectionRepository, MemoryRefsRepository, OutboxStore,
        ProjectionCheckpointRepository, RoleCatalogRepository, UnitOfWork, UnitOfWorkFactory,
    };
    use crate::config::AppConfig;
    use crate::domain::audit::{AuditResult, AuditTraceEntry};
    use crate::domain::capability_profile::{ArtifactRef, CapabilityItem, CapabilityProfile};
    use crate::domain::dead_letter::{DeadLetterReplayStatus, InboundDeadLetter};
    use crate::domain::idempotency::{IdempotencyScope, IdempotencyStatus};
    use crate::domain::member::{GlobalMember, GlobalMemberLifecycle};
    use crate::domain::memory_refs::{ArchiveStatus, MemoryRef, MemoryRefs};
    use crate::domain::outbox::{OutboxEvent, OutboxStatus};
    use crate::domain::projection::{MemberSummaryProjection, ProjectionCheckpointStatus};
    use crate::domain::role_catalog::{RoleCatalogEntry, RoleCatalogStatus};
    use crate::domain::shared::context::{ActorContext, ActorKind};
    use crate::domain::shared::ids::{
        CapabilityProfileId, DeadLetterId, GlobalMemberId, MemoryRefsId, OutboxEventId, RoleId,
    };
    use crate::domain::shared::metadata::CommandMetadata;
    use crate::domain::timeline::{LifecycleEventType, LifecycleHistoryEntry};
    use crate::error::IdentityError;
    use crate::persistence::database::run_migrations;
    use crate::persistence::test_support::DB_TEST_MUTEX;

    use super::SqlxUnitOfWorkFactory;

    #[tokio::test]
    async fn unit_of_work_commits_member_history_audit_and_idempotency() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool).await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let mut uow = factory.begin().await.expect("begin transaction");

        let actor = ActorContext::new("human/admin-1", ActorKind::HumanUser, None);
        let member = sample_member(actor.clone());
        let metadata = CommandMetadata::new("idem-commit-1", "trace-commit-1", "hash-commit-1");

        uow.global_members()
            .insert(&member)
            .await
            .expect("insert member");
        uow.lifecycle_history()
            .append(&LifecycleHistoryEntry {
                history_entry_id: "history-commit-1".to_string(),
                global_member_id: member.global_member_id.clone(),
                event_type: LifecycleEventType::Created,
                from_lifecycle: None,
                to_lifecycle: GlobalMemberLifecycle::Hired,
                actor: actor.clone(),
                gate_decision_ref_json: None,
                metadata: metadata.clone(),
                created_at: member.created_at,
            })
            .await
            .expect("append history");
        uow.audit_traces()
            .append(&AuditTraceEntry {
                audit_trace_id: "audit-commit-1".to_string(),
                trace_id: metadata.trace_id().to_string(),
                action: "HireGlobalMember".to_string(),
                actor_json: Some(json!(actor)),
                target_ref_json: Some(json!({
                    "kind": "global_member",
                    "id": member.global_member_id.as_str(),
                })),
                source_module: None,
                result: AuditResult::Success,
                reason: None,
                created_at: member.created_at,
            })
            .await
            .expect("append audit");
        uow.idempotency()
            .record_success(
                &metadata,
                IdempotencyScope::Command,
                json!({
                    "kind": "global_member",
                    "id": member.global_member_id.as_str(),
                }),
            )
            .await
            .expect("record idempotency");

        uow.commit().await.expect("commit transaction");

        let member_count: i64 = sqlx::query("SELECT COUNT(*) AS count FROM global_members")
            .fetch_one(&pool)
            .await
            .expect("count members")
            .get("count");
        let history_count: i64 =
            sqlx::query("SELECT COUNT(*) AS count FROM lifecycle_history_entries")
                .fetch_one(&pool)
                .await
                .expect("count history")
                .get("count");
        let audit_count: i64 = sqlx::query("SELECT COUNT(*) AS count FROM audit_trace_entries")
            .fetch_one(&pool)
            .await
            .expect("count audit")
            .get("count");

        assert_eq!(member_count, 1);
        assert_eq!(history_count, 1);
        assert_eq!(audit_count, 1);

        let idempotency_status: String =
            sqlx::query("SELECT status FROM idempotency_records WHERE idempotency_key = $1")
                .bind("idem-commit-1")
                .fetch_one(&pool)
                .await
                .expect("fetch idempotency")
                .get("status");
        assert_eq!(idempotency_status, IdempotencyStatus::Succeeded.as_db());
    }

    #[tokio::test]
    async fn unit_of_work_rollback_discards_all_pending_writes() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool).await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let mut uow = factory.begin().await.expect("begin transaction");

        let member = sample_member(ActorContext::new("system/replay", ActorKind::System, None));
        uow.global_members()
            .insert(&member)
            .await
            .expect("insert member");

        uow.rollback().await.expect("rollback transaction");

        let member_count: i64 = sqlx::query("SELECT COUNT(*) AS count FROM global_members")
            .fetch_one(&pool)
            .await
            .expect("count members")
            .get("count");
        assert_eq!(member_count, 0);
    }

    #[tokio::test]
    async fn global_member_save_uses_optimistic_locking() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool).await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let actor = ActorContext::new("human/admin-2", ActorKind::HumanUser, None);
        let member = sample_member(actor);

        {
            let mut uow = factory.begin().await.expect("begin insert transaction");
            uow.global_members()
                .insert(&member)
                .await
                .expect("insert member");
            uow.commit().await.expect("commit insert");
        }

        let mut uow = factory.begin().await.expect("begin update transaction");
        let mut loaded = uow
            .global_members()
            .get_for_update(&member.global_member_id)
            .await
            .expect("load member")
            .expect("member should exist");
        loaded.display_name = "Renamed Member".to_string();
        loaded.version += 1;
        loaded.updated_at = now();

        let save_result = uow.global_members().save(&loaded, 999).await;
        assert!(matches!(
            save_result,
            Err(IdentityError::VersionConflict { entity }) if entity == "global_member"
        ));

        uow.rollback().await.expect("rollback failed update");
    }

    #[tokio::test]
    async fn capability_profile_store_round_trips_and_uses_optimistic_locking() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool).await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let actor = ActorContext::new("human/admin-3", ActorKind::HumanUser, None);
        let member = sample_member(actor.clone());
        let timestamp = now();

        {
            let mut uow = factory.begin().await.expect("begin insert transaction");
            uow.global_members()
                .insert(&member)
                .await
                .expect("insert member for capability profile");
            uow.capability_profiles()
                .insert(&CapabilityProfile {
                    capability_profile_id: CapabilityProfileId::new(
                        "capability-profile:member-001",
                    ),
                    global_member_id: member.global_member_id.clone(),
                    capabilities: vec![CapabilityItem {
                        capability_id: "capability.rust".to_string(),
                        capability_name: "Rust".to_string(),
                        proficiency: Some("advanced".to_string()),
                        notes: Some("systems programming".to_string()),
                    }],
                    evidence_refs: vec![ArtifactRef {
                        artifact_id: "artifact-001".to_string(),
                        artifact_kind: "evidence".to_string(),
                        artifact_version: Some("v1".to_string()),
                    }],
                    version: 1,
                    updated_at: timestamp,
                })
                .await
                .expect("insert capability profile");
            uow.commit()
                .await
                .expect("commit capability profile insert");
        }

        let mut uow = factory.begin().await.expect("begin load transaction");
        let mut profile = uow
            .capability_profiles()
            .get_for_update_by_member(&member.global_member_id)
            .await
            .expect("load capability profile")
            .expect("capability profile should exist");
        assert_eq!(profile.capabilities.len(), 1);
        assert_eq!(profile.evidence_refs.len(), 1);

        profile.version += 1;
        profile.updated_at = now();
        profile.capabilities.push(CapabilityItem {
            capability_id: "capability.sql".to_string(),
            capability_name: "SQL".to_string(),
            proficiency: Some("intermediate".to_string()),
            notes: None,
        });
        let save_result = uow.capability_profiles().save(&profile, 999).await;
        assert!(matches!(
            save_result,
            Err(IdentityError::VersionConflict { entity }) if entity == "capability_profile"
        ));
        uow.rollback().await.expect("rollback optimistic-lock test");
    }

    #[tokio::test]
    async fn memory_refs_store_round_trips_and_uses_optimistic_locking() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool).await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let actor = ActorContext::new("human/admin-4", ActorKind::HumanUser, None);
        let member = sample_member(actor);
        let timestamp = now();

        {
            let mut uow = factory.begin().await.expect("begin insert transaction");
            uow.global_members()
                .insert(&member)
                .await
                .expect("insert member for memory refs");
            uow.memory_refs()
                .insert(&MemoryRefs {
                    memory_refs_id: MemoryRefsId::new("memory-refs:member-001"),
                    global_member_id: member.global_member_id.clone(),
                    semantic_memory_ref: Some(MemoryRef {
                        memory_id: "memory-semantic-001".to_string(),
                        memory_kind: "semantic".to_string(),
                        memory_version: Some("v1".to_string()),
                    }),
                    episodic_memory_refs: vec![MemoryRef {
                        memory_id: "memory-episodic-001".to_string(),
                        memory_kind: "episodic".to_string(),
                        memory_version: Some("v1".to_string()),
                    }],
                    archive_ref: None,
                    archive_status: ArchiveStatus::None,
                    version: 2,
                    updated_at: timestamp,
                })
                .await
                .expect("insert memory refs");
            uow.commit().await.expect("commit memory refs insert");
        }

        let mut uow = factory.begin().await.expect("begin load transaction");
        let mut memory_refs = uow
            .memory_refs()
            .get_for_update_by_member(&member.global_member_id)
            .await
            .expect("load memory refs")
            .expect("memory refs should exist");
        assert!(memory_refs.semantic_memory_ref.is_some());
        assert_eq!(memory_refs.episodic_memory_refs.len(), 1);

        memory_refs.version += 1;
        memory_refs.updated_at = now();
        memory_refs.episodic_memory_refs.push(MemoryRef {
            memory_id: "memory-episodic-002".to_string(),
            memory_kind: "episodic".to_string(),
            memory_version: Some("v2".to_string()),
        });
        let save_result = uow.memory_refs().save(&memory_refs, 999).await;
        assert!(matches!(
            save_result,
            Err(IdentityError::VersionConflict { entity }) if entity == "memory_refs"
        ));
        uow.rollback().await.expect("rollback optimistic-lock test");
    }

    #[tokio::test]
    async fn role_catalog_round_trip_and_idempotency_lookup_work_inside_unit_of_work() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let now = now();

        {
            let mut uow = factory.begin().await.expect("begin seed transaction");
            uow.role_catalog()
                .upsert(&RoleCatalogEntry {
                    role_id: RoleId::new("role.runtime.index"),
                    role_name: "Runtime Index".to_string(),
                    role_version: "2026.05".to_string(),
                    source_ref_json: json!({
                        "module": "method-library",
                        "id": "role.runtime.index",
                    }),
                    fingerprint: "fp-runtime-index".to_string(),
                    status: RoleCatalogStatus::Active,
                    updated_at: now,
                })
                .await
                .expect("upsert role");
            uow.idempotency()
                .record_success(
                    &CommandMetadata::new("source-event-1", "trace-event-1", "payload-hash-1"),
                    IdempotencyScope::InboundEvent,
                    json!({
                        "kind": "role_catalog_entry",
                        "id": "role.runtime.index",
                    }),
                )
                .await
                .expect("record inbound event idempotency");
            uow.commit().await.expect("commit seed transaction");
        }

        let mut uow = factory.begin().await.expect("begin read transaction");
        let role = uow
            .role_catalog()
            .get_active(&RoleId::new("role.runtime.index"))
            .await
            .expect("load role")
            .expect("role should exist");
        let idempotency_record = uow
            .idempotency()
            .get("source-event-1", IdempotencyScope::InboundEvent)
            .await
            .expect("load idempotency")
            .expect("idempotency should exist");

        assert_eq!(role.role_name, "Runtime Index");
        assert_eq!(role.status, RoleCatalogStatus::Active);
        assert_eq!(idempotency_record.status, IdempotencyStatus::Succeeded);
        uow.rollback().await.expect("rollback read transaction");
    }

    #[tokio::test]
    async fn outbox_store_lists_pending_and_saves_publish_status() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let created_at = now();

        {
            let mut uow = factory.begin().await.expect("begin seed transaction");
            uow.outbox()
                .append(&OutboxEvent {
                    outbox_event_id: OutboxEventId::new("outbox-001"),
                    aggregate_type: "global_member".to_string(),
                    aggregate_id: "member-001".to_string(),
                    event_type: "identity.member.created".to_string(),
                    payload_json: json!({ "global_member_id": "member-001" }),
                    idempotency_key: "idem-outbox-001".to_string(),
                    status: OutboxStatus::Pending,
                    retry_count: 0,
                    next_retry_at: None,
                    created_at,
                    published_at: None,
                    failure_reason: None,
                })
                .await
                .expect("append pending outbox event");
            uow.outbox()
                .append(&OutboxEvent {
                    outbox_event_id: OutboxEventId::new("outbox-002"),
                    aggregate_type: "global_member".to_string(),
                    aggregate_id: "member-002".to_string(),
                    event_type: "identity.member.updated".to_string(),
                    payload_json: json!({ "global_member_id": "member-002" }),
                    idempotency_key: "idem-outbox-002".to_string(),
                    status: OutboxStatus::Published,
                    retry_count: 0,
                    next_retry_at: None,
                    created_at,
                    published_at: Some(created_at),
                    failure_reason: None,
                })
                .await
                .expect("append published outbox event");
            uow.commit().await.expect("commit seed outbox rows");
        }

        {
            let mut uow = factory.begin().await.expect("begin pending scan");
            let pending_events = uow
                .outbox()
                .list_pending(10)
                .await
                .expect("list pending outbox events");

            assert_eq!(pending_events.len(), 1);
            assert_eq!(pending_events[0].outbox_event_id.as_str(), "outbox-001");
            uow.rollback().await.expect("rollback pending scan");
        }

        {
            let mut uow = factory.begin().await.expect("begin save update");
            let mut pending_event = uow
                .outbox()
                .list_pending(10)
                .await
                .expect("list pending before update")
                .into_iter()
                .next()
                .expect("pending event should exist");
            pending_event.status = OutboxStatus::Published;
            pending_event.published_at = Some(created_at);

            uow.outbox()
                .save(&pending_event)
                .await
                .expect("save outbox publish status");
            uow.commit().await.expect("commit outbox update");
        }

        let published_status: String =
            sqlx::query("SELECT status FROM outbox_events WHERE outbox_event_id = $1")
                .bind("outbox-001")
                .fetch_one(&pool)
                .await
                .expect("fetch outbox status")
                .get("status");
        assert_eq!(published_status, "published");
    }

    #[tokio::test]
    async fn outbox_store_lists_rows_after_checkpoint_cursor() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let base_time = now();

        {
            let mut uow = factory.begin().await.expect("begin seed transaction");
            for (event_id, aggregate_id, seconds) in [
                ("outbox-a", "member-a", 0),
                ("outbox-b", "member-b", 1),
                ("outbox-c", "member-c", 2),
            ] {
                let created_at = base_time + time::Duration::seconds(seconds);
                uow.outbox()
                    .append(&OutboxEvent {
                        outbox_event_id: OutboxEventId::new(event_id),
                        aggregate_type: "global_member".to_string(),
                        aggregate_id: aggregate_id.to_string(),
                        event_type: "identity.member.changed".to_string(),
                        payload_json: json!({ "global_member_id": aggregate_id }),
                        idempotency_key: format!("idem-{event_id}"),
                        status: OutboxStatus::Pending,
                        retry_count: 0,
                        next_retry_at: None,
                        created_at,
                        published_at: None,
                        failure_reason: None,
                    })
                    .await
                    .expect("append outbox event");
            }
            uow.commit().await.expect("commit seed outbox events");
        }

        let mut uow = factory.begin().await.expect("begin cursor scan");
        let events_after_cursor = uow
            .outbox()
            .list_after(Some(&OutboxEventId::new("outbox-a")), 10)
            .await
            .expect("list outbox rows after cursor");

        assert_eq!(
            events_after_cursor
                .into_iter()
                .map(|event| event.outbox_event_id.as_str().to_string())
                .collect::<Vec<_>>(),
            vec!["outbox-b".to_string(), "outbox-c".to_string()]
        );
        uow.rollback().await.expect("rollback cursor scan");
    }

    #[tokio::test]
    async fn projection_and_dead_letter_stores_round_trip() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let timestamp = now();

        {
            let mut uow = factory.begin().await.expect("begin seed transaction");
            uow.outbox()
                .append(&OutboxEvent {
                    outbox_event_id: OutboxEventId::new("outbox-proj-001"),
                    aggregate_type: "global_member".to_string(),
                    aggregate_id: "member-proj-001".to_string(),
                    event_type: "identity.member.created".to_string(),
                    payload_json: json!({ "global_member_id": "member-proj-001" }),
                    idempotency_key: "idem-proj-001".to_string(),
                    status: OutboxStatus::Pending,
                    retry_count: 0,
                    next_retry_at: None,
                    created_at: timestamp,
                    published_at: None,
                    failure_reason: None,
                })
                .await
                .expect("append outbox event for checkpoint ref");

            uow.member_summary_projection()
                .upsert(&MemberSummaryProjection {
                    global_member_id: GlobalMemberId::new("member-proj-001"),
                    display_name: "Projection Member".to_string(),
                    lifecycle: GlobalMemberLifecycle::Active,
                    main_role_id: Some(RoleId::new("role.member.operator")),
                    main_role_name: Some("Member Operator".to_string()),
                    capability_summary_json: json!({ "count": 1 }),
                    career_summary_json: json!({ "count": 0 }),
                    memory_ref_summary_json: json!({ "semantic": null }),
                    projection_version: 3,
                    updated_at: timestamp,
                })
                .await
                .expect("upsert member summary projection");

            let mut checkpoint = uow
                .projection_checkpoints()
                .get_or_create("member-summary-rebuild")
                .await
                .expect("get or create checkpoint");
            checkpoint.last_processed_event_id = Some(OutboxEventId::new("outbox-proj-001"));
            checkpoint.status = ProjectionCheckpointStatus::Running;
            checkpoint.updated_at = timestamp;
            uow.projection_checkpoints()
                .save(&checkpoint)
                .await
                .expect("save checkpoint");

            uow.inbound_dead_letters()
                .append(&InboundDeadLetter {
                    dead_letter_id: DeadLetterId::new("dead-letter-001"),
                    source_event_id: Some(crate::domain::shared::ids::EventId::new(
                        "source-event-001",
                    )),
                    source_module: "method-library".to_string(),
                    event_type: "role.definition.updated".to_string(),
                    payload_json: json!({ "role_id": "role.member.operator" }),
                    failure_reason: "payload parse failed".to_string(),
                    replay_status: DeadLetterReplayStatus::Pending,
                    created_at: timestamp,
                })
                .await
                .expect("append dead letter");
            uow.commit().await.expect("commit projection seed");
        }

        let mut uow = factory.begin().await.expect("begin read transaction");
        let projection = uow
            .member_summary_projection()
            .get(&GlobalMemberId::new("member-proj-001"))
            .await
            .expect("load member summary projection")
            .expect("projection should exist");
        let checkpoint = uow
            .projection_checkpoints()
            .get_or_create("member-summary-rebuild")
            .await
            .expect("load existing checkpoint");

        assert_eq!(projection.display_name, "Projection Member");
        assert_eq!(projection.projection_version, 3);
        assert_eq!(
            checkpoint
                .last_processed_event_id
                .as_ref()
                .map(|value| value.as_str()),
            Some("outbox-proj-001")
        );
        assert_eq!(checkpoint.status, ProjectionCheckpointStatus::Running);

        uow.inbound_dead_letters()
            .save(&InboundDeadLetter {
                dead_letter_id: DeadLetterId::new("dead-letter-001"),
                source_event_id: Some(crate::domain::shared::ids::EventId::new("source-event-001")),
                source_module: "method-library".to_string(),
                event_type: "role.definition.updated".to_string(),
                payload_json: json!({ "role_id": "role.member.operator" }),
                failure_reason: "ignored after manual review".to_string(),
                replay_status: DeadLetterReplayStatus::Ignored,
                created_at: timestamp,
            })
            .await
            .expect("update dead letter status");
        uow.commit().await.expect("commit dead-letter update");

        let replay_status: String =
            sqlx::query("SELECT replay_status FROM inbound_dead_letters WHERE dead_letter_id = $1")
                .bind("dead-letter-001")
                .fetch_one(&pool)
                .await
                .expect("fetch dead-letter replay status")
                .get("replay_status");
        assert_eq!(replay_status, "ignored");
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
                idempotency_records,
                audit_trace_entries,
                lifecycle_history_entries,
                memory_refs,
                capability_profiles,
                global_members,
                projection_checkpoints,
                member_summary_projection,
                inbound_dead_letters,
                outbox_events,
                role_catalog_entries
            RESTART IDENTITY CASCADE
            "#,
        )
        .await
        .expect("truncate test tables");
    }

    async fn seed_role(pool: &sqlx::postgres::PgPool) {
        let now = now();
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
            ON CONFLICT (role_id) DO NOTHING
            "#,
        )
        .bind("role.member.operator")
        .bind("Member Operator")
        .bind("2026.05")
        .bind(json!({
            "module": "method-library",
            "id": "role.member.operator",
        }))
        .bind("fp-role-member-operator")
        .bind("active")
        .bind(now)
        .execute(pool)
        .await
        .expect("seed role catalog entry");
    }

    fn sample_member(actor: ActorContext) -> GlobalMember {
        let now = now();

        GlobalMember {
            global_member_id: GlobalMemberId::new("member-001"),
            display_name: "Member Zero One".to_string(),
            lifecycle: GlobalMemberLifecycle::Hired,
            main_role_id: RoleId::new("role.member.operator"),
            secondary_role_ids: vec![RoleId::new("role.runtime.index")],
            capability_profile_id: None,
            memory_refs_id: None,
            version: 0,
            created_by: actor,
            created_at: now,
            updated_at: now,
        }
    }

    fn now() -> PrimitiveDateTime {
        let now = OffsetDateTime::now_utc();
        PrimitiveDateTime::new(now.date(), now.time())
    }
}
