//! SQLx-backed unit-of-work implementation for local identity write transactions.

use sqlx::{Postgres, Transaction, postgres::PgPool};

use crate::application::persistence::UnitOfWorkFactory;
use crate::error::IdentityError;
use crate::persistence::repositories::{
    SqlxAuditTraceRepository, SqlxGlobalMemberRepository, SqlxIdempotencyStore,
    SqlxLifecycleHistoryRepository, SqlxRoleCatalogRepository,
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

    fn global_members(&mut self) -> Self::GlobalMembers<'_> {
        SqlxGlobalMemberRepository::new(&mut self.transaction)
    }

    fn role_catalog(&mut self) -> Self::RoleCatalog<'_> {
        SqlxRoleCatalogRepository::new(&mut self.transaction)
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
    use std::sync::LazyLock;

    use serde_json::json;
    use sqlx::{Executor, Row, postgres::PgPoolOptions};
    use time::{OffsetDateTime, PrimitiveDateTime};
    use tokio::sync::Mutex;

    use crate::application::persistence::{
        AuditTraceRepository, GlobalMemberRepository, IdempotencyStore, LifecycleHistoryRepository,
        RoleCatalogRepository, UnitOfWork, UnitOfWorkFactory,
    };
    use crate::config::AppConfig;
    use crate::domain::audit::{AuditResult, AuditTraceEntry};
    use crate::domain::idempotency::{IdempotencyScope, IdempotencyStatus};
    use crate::domain::member::{GlobalMember, GlobalMemberLifecycle};
    use crate::domain::role_catalog::{RoleCatalogEntry, RoleCatalogStatus};
    use crate::domain::shared::context::{ActorContext, ActorKind};
    use crate::domain::shared::ids::{GlobalMemberId, RoleId};
    use crate::domain::shared::metadata::CommandMetadata;
    use crate::domain::timeline::{LifecycleEventType, LifecycleHistoryEntry};
    use crate::error::IdentityError;
    use crate::persistence::database::run_migrations;

    use super::SqlxUnitOfWorkFactory;

    static DB_TEST_MUTEX: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    #[tokio::test]
    async fn unit_of_work_commits_member_history_audit_and_idempotency() {
        let _guard = DB_TEST_MUTEX.lock().await;
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
        let _guard = DB_TEST_MUTEX.lock().await;
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
        let _guard = DB_TEST_MUTEX.lock().await;
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
    async fn role_catalog_round_trip_and_idempotency_lookup_work_inside_unit_of_work() {
        let _guard = DB_TEST_MUTEX.lock().await;
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
