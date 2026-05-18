//! Application services for explicit global-member creation and lifecycle management.

use serde_json::json;
use time::{OffsetDateTime, PrimitiveDateTime};

use crate::application::persistence::{
    AuditTraceRepository, GlobalMemberRepository, IdempotencyStore, LifecycleHistoryRepository,
    OutboxStore, RoleCatalogRepository, UnitOfWork, UnitOfWorkFactory,
};
use crate::domain::audit::AuditTraceEntry;
use crate::domain::idempotency::{IdempotencyRecord, IdempotencyScope, IdempotencyStatus};
use crate::domain::member::{GlobalMember, GlobalMemberSummary, HireGlobalMemberCommand};
use crate::domain::outbox::OutboxEvent;
use crate::domain::shared::context::ActorContext;
use crate::domain::shared::ids::{GlobalMemberId, OutboxEventId};
use crate::domain::shared::metadata::CommandMetadata;
use crate::domain::timeline::LifecycleHistoryEntry;
use crate::error::IdentityError;

/// Coordinates member lifecycle write flows over the shared transaction boundary.
#[derive(Debug, Clone)]
pub struct MemberLifecycleCommandService<UowFactory> {
    unit_of_work_factory: UowFactory,
}

impl<UowFactory> MemberLifecycleCommandService<UowFactory> {
    /// Creates a new lifecycle command service bound to the provided persistence factory.
    pub fn new(unit_of_work_factory: UowFactory) -> Self {
        Self {
            unit_of_work_factory,
        }
    }
}

impl<UowFactory> MemberLifecycleCommandService<UowFactory>
where
    UowFactory: UnitOfWorkFactory,
{
    /// Explicitly creates a new global member and writes all required transactional side effects.
    ///
    /// # Errors
    ///
    /// Returns an error when the main role is missing, when the idempotency key conflicts with a
    /// different request hash, or when persistence fails.
    pub async fn hire_global_member(
        &self,
        command: HireGlobalMemberCommand,
        actor: ActorContext,
        metadata: CommandMetadata,
    ) -> Result<GlobalMemberSummary, IdentityError> {
        if metadata.idempotency_key().trim().is_empty() {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_INVALID_ARGUMENT",
                message: "idempotency_key must not be blank".to_string(),
            });
        }

        let mut uow = self.unit_of_work_factory.begin().await?;
        let role_entry = uow
            .role_catalog()
            .get_active(&command.main_role_id)
            .await?
            .ok_or_else(|| IdentityError::RuleViolation {
                code: "IDENTITY_ROLE_NOT_FOUND",
                message: format!(
                    "active role catalog entry `{}` was not found",
                    command.main_role_id.as_str()
                ),
            })?;

        let existing_record = uow
            .idempotency()
            .get(metadata.idempotency_key(), IdempotencyScope::Command)
            .await?;

        if let Some(existing_record) = existing_record {
            return self
                .handle_existing_command_record(existing_record, metadata.request_hash(), uow)
                .await;
        }

        let now = current_timestamp();
        let global_member_id = GlobalMemberId::new(format!(
            "member:{}:{}",
            metadata.trace_id(),
            now.assume_utc().unix_timestamp_nanos(),
        ));
        let member =
            GlobalMember::hire(command, &role_entry, actor.clone(), global_member_id, now)?;
        let history_entry = LifecycleHistoryEntry::for_hire(
            format!("history:{}", metadata.idempotency_key()),
            &member,
            actor.clone(),
            metadata.clone(),
        );
        let audit_entry = AuditTraceEntry::for_hire_command(
            format!("audit:{}", metadata.idempotency_key()),
            &member,
            &actor,
            metadata.trace_id(),
            now,
        );
        let outbox_event = OutboxEvent::for_member_hired(
            OutboxEventId::new(format!("outbox:{}", metadata.idempotency_key())),
            &member,
            metadata.idempotency_key(),
            now,
        );

        uow.global_members().insert(&member).await?;
        uow.lifecycle_history().append(&history_entry).await?;
        uow.audit_traces().append(&audit_entry).await?;
        uow.outbox().append(&outbox_event).await?;
        uow.idempotency()
            .record_success(
                &metadata,
                IdempotencyScope::Command,
                json!({
                    "kind": "global_member",
                    "id": member.global_member_id.as_str(),
                    "display_name": member.display_name,
                    "lifecycle": member.lifecycle.as_db(),
                    "main_role_id": member.main_role_id.as_str(),
                    "secondary_role_ids": member.secondary_role_ids.iter().map(|value| value.as_str()).collect::<Vec<_>>(),
                    "capability_profile_id": member.capability_profile_id.as_ref().map(|value| value.as_str()),
                    "memory_refs_id": member.memory_refs_id.as_ref().map(|value| value.as_str()),
                }),
            )
            .await?;
        uow.commit().await?;

        Ok(member.summary())
    }

    async fn handle_existing_command_record<Uow>(
        &self,
        existing_record: IdempotencyRecord,
        request_hash: &str,
        uow: Uow,
    ) -> Result<GlobalMemberSummary, IdentityError>
    where
        Uow: crate::application::persistence::UnitOfWork,
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
}

fn summary_from_result_ref(
    result_ref_json: Option<&serde_json::Value>,
) -> Option<GlobalMemberSummary> {
    let result_ref_json = result_ref_json?;
    let global_member_id = result_ref_json.get("id")?.as_str()?;
    let display_name = result_ref_json.get("display_name")?.as_str()?;
    let lifecycle = result_ref_json.get("lifecycle")?.as_str()?;
    let main_role_id = result_ref_json.get("main_role_id")?.as_str()?;
    let secondary_role_ids = result_ref_json
        .get("secondary_role_ids")
        .and_then(serde_json::Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(serde_json::Value::as_str)
                .map(crate::domain::shared::ids::RoleId::new)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let capability_profile_id = result_ref_json
        .get("capability_profile_id")
        .and_then(serde_json::Value::as_str)
        .map(crate::domain::shared::ids::CapabilityProfileId::new);
    let memory_refs_id = result_ref_json
        .get("memory_refs_id")
        .and_then(serde_json::Value::as_str)
        .map(crate::domain::shared::ids::MemoryRefsId::new);

    Some(GlobalMemberSummary {
        global_member_id: GlobalMemberId::new(global_member_id),
        display_name: display_name.to_string(),
        lifecycle: crate::domain::member::GlobalMemberLifecycle::from_db(lifecycle)?,
        main_role_id: crate::domain::shared::ids::RoleId::new(main_role_id),
        secondary_role_ids,
        capability_profile_id,
        memory_refs_id,
    })
}

fn current_timestamp() -> PrimitiveDateTime {
    let now = OffsetDateTime::now_utc();
    PrimitiveDateTime::new(now.date(), now.time())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;
    use sqlx::{Executor, Row, postgres::PgPoolOptions};
    use time::{OffsetDateTime, PrimitiveDateTime};

    use crate::config::AppConfig;
    use crate::domain::member::{GlobalMemberLifecycle, HireGlobalMemberCommand};
    use crate::domain::shared::context::{ActorContext, ActorKind};
    use crate::domain::shared::ids::RoleId;
    use crate::domain::shared::metadata::CommandMetadata;
    use crate::error::IdentityError;
    use crate::persistence::database::run_migrations;
    use crate::persistence::test_support::DB_TEST_MUTEX;
    use crate::persistence::unit_of_work::SqlxUnitOfWorkFactory;

    use super::MemberLifecycleCommandService;

    #[tokio::test]
    async fn hire_global_member_persists_member_history_audit_outbox_and_idempotency() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator").await;

        let service = MemberLifecycleCommandService::new(SqlxUnitOfWorkFactory::new(pool.clone()));
        let actor = ActorContext::new("human/admin-1", ActorKind::HumanUser, None);
        let metadata = CommandMetadata::new("hire-001", "trace-hire-001", "request-hash-001");

        let summary = service
            .hire_global_member(
                HireGlobalMemberCommand {
                    display_name: "Member Zero One".to_string(),
                    main_role_id: RoleId::new("role.member.operator"),
                    secondary_role_ids: vec![RoleId::new("role.secondary.reviewer")],
                },
                actor,
                metadata,
            )
            .await
            .expect("hire command should succeed");

        assert_eq!(summary.display_name, "Member Zero One");
        assert_eq!(summary.lifecycle, GlobalMemberLifecycle::Hired);
        assert_eq!(summary.main_role_id.as_str(), "role.member.operator");

        let member_row = sqlx::query(
            "SELECT display_name, lifecycle, main_role_id FROM global_members WHERE global_member_id = $1",
        )
        .bind(summary.global_member_id.as_str())
        .fetch_one(&pool)
        .await
        .expect("load member row");
        let history_count: i64 = sqlx::query(
            "SELECT COUNT(*) AS count FROM lifecycle_history_entries WHERE global_member_id = $1",
        )
        .bind(summary.global_member_id.as_str())
        .fetch_one(&pool)
        .await
        .expect("count lifecycle history")
        .get("count");
        let audit_action: String =
            sqlx::query("SELECT action FROM audit_trace_entries WHERE trace_id = $1")
                .bind("trace-hire-001")
                .fetch_one(&pool)
                .await
                .expect("load audit action")
                .get("action");
        let outbox_event_type: String =
            sqlx::query("SELECT event_type FROM outbox_events WHERE aggregate_id = $1")
                .bind(summary.global_member_id.as_str())
                .fetch_one(&pool)
                .await
                .expect("load outbox row")
                .get("event_type");
        let idempotency_status: String =
            sqlx::query("SELECT status FROM idempotency_records WHERE idempotency_key = $1")
                .bind("hire-001")
                .fetch_one(&pool)
                .await
                .expect("load idempotency row")
                .get("status");

        assert_eq!(
            member_row.get::<String, _>("display_name"),
            "Member Zero One"
        );
        assert_eq!(member_row.get::<String, _>("lifecycle"), "hired");
        assert_eq!(
            member_row.get::<String, _>("main_role_id"),
            "role.member.operator"
        );
        assert_eq!(history_count, 1);
        assert_eq!(audit_action, "HireGlobalMember");
        assert_eq!(outbox_event_type, "identity.member.created");
        assert_eq!(idempotency_status, "succeeded");
    }

    #[tokio::test]
    async fn hire_global_member_returns_previous_result_for_same_key_and_hash() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator").await;

        let service = MemberLifecycleCommandService::new(SqlxUnitOfWorkFactory::new(pool.clone()));
        let actor = ActorContext::new("human/admin-2", ActorKind::HumanUser, None);
        let metadata = CommandMetadata::new("hire-002", "trace-hire-002", "request-hash-002");

        let first_summary = service
            .hire_global_member(
                HireGlobalMemberCommand {
                    display_name: "Member Zero Two".to_string(),
                    main_role_id: RoleId::new("role.member.operator"),
                    secondary_role_ids: vec![RoleId::new("role.secondary.reviewer")],
                },
                actor.clone(),
                metadata.clone(),
            )
            .await
            .expect("first hire should succeed");
        let second_summary = service
            .hire_global_member(
                HireGlobalMemberCommand {
                    display_name: "Member Zero Two".to_string(),
                    main_role_id: RoleId::new("role.member.operator"),
                    secondary_role_ids: vec![RoleId::new("role.secondary.reviewer")],
                },
                actor,
                metadata,
            )
            .await
            .expect("duplicate hire should return previous result");

        let member_count: i64 = sqlx::query("SELECT COUNT(*) AS count FROM global_members")
            .fetch_one(&pool)
            .await
            .expect("count members")
            .get("count");

        assert_eq!(
            first_summary.global_member_id,
            second_summary.global_member_id
        );
        assert_eq!(first_summary.secondary_role_ids, second_summary.secondary_role_ids);
        assert_eq!(member_count, 1);
    }

    #[tokio::test]
    async fn hire_global_member_rejects_idempotency_conflict() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator").await;

        let service = MemberLifecycleCommandService::new(SqlxUnitOfWorkFactory::new(pool.clone()));
        let actor = ActorContext::new("human/admin-3", ActorKind::HumanUser, None);

        service
            .hire_global_member(
                HireGlobalMemberCommand {
                    display_name: "Member Zero Three".to_string(),
                    main_role_id: RoleId::new("role.member.operator"),
                    secondary_role_ids: Vec::new(),
                },
                actor.clone(),
                CommandMetadata::new("hire-003", "trace-hire-003a", "request-hash-003a"),
            )
            .await
            .expect("first hire should succeed");

        let error = service
            .hire_global_member(
                HireGlobalMemberCommand {
                    display_name: "Member Zero Three Changed".to_string(),
                    main_role_id: RoleId::new("role.member.operator"),
                    secondary_role_ids: Vec::new(),
                },
                actor,
                CommandMetadata::new("hire-003", "trace-hire-003b", "request-hash-003b"),
            )
            .await
            .expect_err("different request hash should conflict");

        assert!(matches!(
            error,
            IdentityError::RuleViolation {
                code: "IDENTITY_IDEMPOTENCY_CONFLICT",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn hire_global_member_rejects_missing_role_without_persisting_anything() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;

        let service = MemberLifecycleCommandService::new(SqlxUnitOfWorkFactory::new(pool.clone()));
        let actor = ActorContext::new("human/admin-4", ActorKind::HumanUser, None);

        let error = service
            .hire_global_member(
                HireGlobalMemberCommand {
                    display_name: "Member Missing Role".to_string(),
                    main_role_id: RoleId::new("role.missing"),
                    secondary_role_ids: Vec::new(),
                },
                actor,
                CommandMetadata::new("hire-004", "trace-hire-004", "request-hash-004"),
            )
            .await
            .expect_err("missing role should be rejected");

        let member_count: i64 = sqlx::query("SELECT COUNT(*) AS count FROM global_members")
            .fetch_one(&pool)
            .await
            .expect("count members")
            .get("count");

        assert!(matches!(
            error,
            IdentityError::RuleViolation {
                code: "IDENTITY_ROLE_NOT_FOUND",
                ..
            }
        ));
        assert_eq!(member_count, 0);
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
                global_members,
                role_catalog_entries
            RESTART IDENTITY CASCADE
            "#,
        )
        .await
        .expect("truncate test tables");
    }

    async fn seed_role(pool: &sqlx::postgres::PgPool, role_id: &str) {
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
            "#,
        )
        .bind(role_id)
        .bind(format!("Role {role_id}"))
        .bind("2026.05")
        .bind(json!({
            "module": "method-library",
            "id": role_id,
        }))
        .bind(format!("fp-{role_id}"))
        .bind("active")
        .bind(now)
        .execute(pool)
        .await
        .expect("seed active role");
    }

    fn now() -> PrimitiveDateTime {
        let now = OffsetDateTime::now_utc();
        PrimitiveDateTime::new(now.date(), now.time())
    }
}
