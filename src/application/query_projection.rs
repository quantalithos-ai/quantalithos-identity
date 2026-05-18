//! Application query services for projection-backed read APIs.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::application::persistence::{
    GlobalMemberRepository, MemberSummaryProjectionRepository, UnitOfWork, UnitOfWorkFactory,
};
use crate::domain::member::GlobalMemberLifecycle;
use crate::domain::projection::MemberSummaryProjection;
use crate::domain::shared::context::ActorContext;
use crate::domain::shared::ids::{GlobalMemberId, RoleId};
use crate::error::IdentityError;

/// Coordinates projection-backed query flows without mutating write models.
#[derive(Debug, Clone)]
pub struct QueryProjectionService<UowFactory> {
    unit_of_work_factory: UowFactory,
}

impl<UowFactory> QueryProjectionService<UowFactory> {
    /// Creates a new query service bound to the provided persistence factory.
    pub fn new(unit_of_work_factory: UowFactory) -> Self {
        Self {
            unit_of_work_factory,
        }
    }
}

/// Minimal query DTO for member summary lookups.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetMemberSummaryQuery {
    /// Stable global member id used to load the read-optimized projection.
    pub global_member_id: GlobalMemberId,
}

/// Read-facing member summary DTO returned by query flows.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemberSummaryDto {
    /// Stable member identifier.
    pub global_member_id: GlobalMemberId,
    /// User-facing display name from the current projection row.
    pub display_name: String,
    /// Lifecycle snapshot exposed to callers.
    pub lifecycle: GlobalMemberLifecycle,
    /// Optional main role id summary.
    pub main_role_id: Option<RoleId>,
    /// Optional main role display name summary.
    pub main_role_name: Option<String>,
    /// Projection-safe capability summary without external truth bodies.
    pub capability_summary_json: Value,
    /// Projection-safe career summary.
    pub career_summary_json: Value,
    /// Projection-safe memory refs summary without memory bodies.
    pub memory_ref_summary_json: Value,
    /// Projection version useful for diagnostics and client refresh decisions.
    pub projection_version: i64,
}

impl MemberSummaryDto {
    /// Builds the query DTO from the persisted projection row.
    pub fn from_projection(projection: MemberSummaryProjection) -> Self {
        Self {
            global_member_id: projection.global_member_id,
            display_name: projection.display_name,
            lifecycle: projection.lifecycle,
            main_role_id: projection.main_role_id,
            main_role_name: projection.main_role_name,
            capability_summary_json: projection.capability_summary_json,
            career_summary_json: projection.career_summary_json,
            memory_ref_summary_json: projection.memory_ref_summary_json,
            projection_version: projection.projection_version,
        }
    }
}

impl<UowFactory> QueryProjectionService<UowFactory>
where
    UowFactory: UnitOfWorkFactory,
{
    /// Loads the current member summary projection without triggering rebuilds or implicit member creation.
    ///
    /// # Errors
    ///
    /// Returns `IDENTITY_MEMBER_NOT_FOUND` when neither projection nor write model exists.
    /// Returns `IDENTITY_PROJECTION_NOT_READY` when the write model exists but the projection is
    /// still missing.
    pub async fn get_member_summary(
        &self,
        query: GetMemberSummaryQuery,
        actor: ActorContext,
    ) -> Result<MemberSummaryDto, IdentityError> {
        let mut uow = self.unit_of_work_factory.begin().await?;
        let projection = {
            let mut repository = uow.member_summary_projection();
            repository.get(&query.global_member_id).await?
        };

        if let Some(projection) = projection {
            let summary = trim_member_summary(projection, &actor);
            uow.rollback().await?;
            return Ok(summary);
        }

        let member_exists = {
            let mut repository = uow.global_members();
            repository.get(&query.global_member_id).await?.is_some()
        };
        uow.rollback().await?;

        if member_exists {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_PROJECTION_NOT_READY",
                message: format!(
                    "member summary projection for `{}` is not ready",
                    query.global_member_id.as_str()
                ),
            });
        }

        Err(IdentityError::RuleViolation {
            code: "IDENTITY_MEMBER_NOT_FOUND",
            message: format!(
                "global member `{}` was not found",
                query.global_member_id.as_str()
            ),
        })
    }
}

fn trim_member_summary(
    projection: MemberSummaryProjection,
    _actor: &ActorContext,
) -> MemberSummaryDto {
    MemberSummaryDto::from_projection(projection)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;
    use sqlx::{Executor, Row, postgres::PgPoolOptions};

    use crate::application::member_lifecycle::MemberLifecycleCommandService;
    use crate::config::AppConfig;
    use crate::domain::member::{GlobalMemberLifecycle, HireGlobalMemberCommand};
    use crate::domain::shared::context::{ActorContext, ActorKind};
    use crate::domain::shared::ids::RoleId;
    use crate::domain::shared::metadata::CommandMetadata;
    use crate::error::IdentityError;
    use crate::operations::ProjectionRebuildJob;
    use crate::persistence::database::run_migrations;
    use crate::persistence::test_support::DB_TEST_MUTEX;
    use crate::persistence::unit_of_work::SqlxUnitOfWorkFactory;

    use super::{GetMemberSummaryQuery, QueryProjectionService};

    #[tokio::test]
    async fn get_member_summary_returns_projection_when_ready() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let command_service = MemberLifecycleCommandService::new(factory.clone());
        let query_service = QueryProjectionService::new(factory.clone());
        let rebuild_job = ProjectionRebuildJob::new(factory);
        let actor = ActorContext::new("human/admin-1", ActorKind::HumanUser, None);

        let member = command_service
            .hire_global_member(
                HireGlobalMemberCommand {
                    display_name: "Member Zero One".to_string(),
                    main_role_id: RoleId::new("role.member.operator"),
                    secondary_role_ids: Vec::new(),
                },
                actor.clone(),
                CommandMetadata::new("idem-query-001", "trace-query-001", "hash-query-001"),
            )
            .await
            .expect("hire member for query");
        rebuild_job
            .rebuild_member_summary_projection("member-summary-rebuild", 10)
            .await
            .expect("rebuild projection");

        let summary = query_service
            .get_member_summary(
                GetMemberSummaryQuery {
                    global_member_id: member.global_member_id.clone(),
                },
                actor,
            )
            .await
            .expect("query should return the rebuilt projection");

        assert_eq!(summary.global_member_id, member.global_member_id);
        assert_eq!(summary.display_name, "Member Zero One");
        assert_eq!(summary.lifecycle, GlobalMemberLifecycle::Hired);
        assert_eq!(
            summary.main_role_id.as_ref().map(RoleId::as_str),
            Some("role.member.operator")
        );
        assert_eq!(summary.main_role_name.as_deref(), Some("Member Operator"));
        assert_eq!(summary.capability_summary_json, json!({}));
        assert_eq!(summary.career_summary_json, json!({}));
        assert_eq!(summary.memory_ref_summary_json, json!({}));
        assert_eq!(summary.projection_version, 0);
    }

    #[tokio::test]
    async fn get_member_summary_returns_not_ready_when_member_exists_but_projection_is_missing() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let factory = SqlxUnitOfWorkFactory::new(pool);
        let command_service = MemberLifecycleCommandService::new(factory.clone());
        let query_service = QueryProjectionService::new(factory);
        let actor = ActorContext::new("human/admin-2", ActorKind::HumanUser, None);

        let member = command_service
            .hire_global_member(
                HireGlobalMemberCommand {
                    display_name: "Member Zero Two".to_string(),
                    main_role_id: RoleId::new("role.member.operator"),
                    secondary_role_ids: Vec::new(),
                },
                actor.clone(),
                CommandMetadata::new("idem-query-002", "trace-query-002", "hash-query-002"),
            )
            .await
            .expect("hire member without rebuilding projection");

        let error = query_service
            .get_member_summary(
                GetMemberSummaryQuery {
                    global_member_id: member.global_member_id,
                },
                actor,
            )
            .await
            .expect_err("query should report the projection as not ready");

        match error {
            IdentityError::RuleViolation { code, message } => {
                assert_eq!(code, "IDENTITY_PROJECTION_NOT_READY");
                assert!(message.contains("is not ready"));
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[tokio::test]
    async fn get_member_summary_returns_not_found_without_creating_a_member() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;

        let query_service = QueryProjectionService::new(SqlxUnitOfWorkFactory::new(pool.clone()));
        let error = query_service
            .get_member_summary(
                GetMemberSummaryQuery {
                    global_member_id: crate::domain::shared::ids::GlobalMemberId::new(
                        "member-missing-001",
                    ),
                },
                ActorContext::new("human/admin-3", ActorKind::HumanUser, None),
            )
            .await
            .expect_err("query should return not found");

        let member_count: i64 = sqlx::query("SELECT COUNT(*) AS count FROM global_members")
            .fetch_one(&pool)
            .await
            .expect("count members after failed query")
            .get("count");

        match error {
            IdentityError::RuleViolation { code, message } => {
                assert_eq!(code, "IDENTITY_MEMBER_NOT_FOUND");
                assert!(message.contains("member-missing-001"));
            }
            other => panic!("unexpected error: {other}"),
        }
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
            ) VALUES ($1, $2, $3, $4, $5, $6, NOW())
            "#,
        )
        .bind(role_id)
        .bind(role_name)
        .bind("v1")
        .bind(json!({ "kind": "method_library_role", "id": role_id }))
        .bind(format!("fingerprint-{role_id}"))
        .bind("active")
        .execute(pool)
        .await
        .expect("seed role catalog entry");
    }
}
