use std::sync::{Arc, Mutex};

use serde_json::json;
use sqlx::{Executor, Row, postgres::PgPoolOptions};
use time::{Duration, OffsetDateTime, PrimitiveDateTime};

use crate::application::member_lifecycle::MemberLifecycleCommandService;
use crate::application::query_projection::{GetMemberSummaryQuery, QueryProjectionService};
use crate::application::role_catalog_sync::RoleCatalogSyncService;
use crate::config::AppConfig;
use crate::domain::member::{GlobalMemberLifecycle, HireGlobalMemberCommand};
use crate::domain::outbox::OutboxEvent;
use crate::domain::shared::context::{ActorContext, ActorKind};
use crate::domain::shared::ids::{EventId, RoleId};
use crate::domain::shared::metadata::CommandMetadata;
use crate::error::IdentityError;
use crate::inbound::event_consumers::RoleCatalogConsumer;
use crate::inbound::events::{InboundEventEnvelope, InboundRoleCatalogEvent};
use crate::operations::{
    OutboxPublisherJob, ProjectionRebuildJob, PublishOutboxEventsSummary,
    RebuildMemberSummaryProjectionSummary,
};
use crate::outbound::BusPublisherPort;
use crate::persistence::database::run_migrations;
use crate::persistence::test_support::DB_TEST_MUTEX;
use crate::persistence::unit_of_work::SqlxUnitOfWorkFactory;

#[derive(Debug, Clone, Default)]
struct RecordingBusPublisher {
    published_event_ids: Arc<Mutex<Vec<String>>>,
}

impl RecordingBusPublisher {
    fn published_event_ids(&self) -> Vec<String> {
        self.published_event_ids
            .lock()
            .expect("lock publisher state")
            .clone()
    }
}

impl BusPublisherPort for RecordingBusPublisher {
    async fn publish(&self, event: &OutboxEvent) -> Result<(), IdentityError> {
        self.published_event_ids
            .lock()
            .expect("lock publisher state")
            .push(event.outbox_event_id.as_str().to_string());
        Ok(())
    }
}

#[tokio::test]
async fn p0_minimal_flow_smoke_runs_end_to_end() {
    let db_mutex = Arc::clone(&DB_TEST_MUTEX);
    let _guard = db_mutex.lock().await;
    let pool = test_pool().await;
    reset_tables(&pool).await;

    let factory = SqlxUnitOfWorkFactory::new(pool.clone());
    let role_catalog_consumer =
        RoleCatalogConsumer::new(RoleCatalogSyncService::new(factory.clone()));
    let member_lifecycle_service = MemberLifecycleCommandService::new(factory.clone());
    let bus_publisher = RecordingBusPublisher::default();
    let outbox_publisher = OutboxPublisherJob::new(factory.clone(), bus_publisher.clone());
    let projection_rebuild = ProjectionRebuildJob::new(factory.clone());
    let query_service = QueryProjectionService::new(factory);

    role_catalog_consumer
        .consume(sample_role_event(
            "role-sync-smoke-001",
            "payload-hash-smoke-001",
            "role.member.operator",
            "fp-role-smoke-001",
            "active",
        ))
        .await
        .expect("sync role catalog should succeed");

    let actor = ActorContext::new("human/admin-smoke", ActorKind::HumanUser, None);
    let created_member = member_lifecycle_service
        .hire_global_member(
            HireGlobalMemberCommand {
                display_name: "Member Smoke".to_string(),
                main_role_id: RoleId::new("role.member.operator"),
                secondary_role_ids: Vec::new(),
            },
            actor.clone(),
            CommandMetadata::new(
                "idem-hire-smoke-001",
                "trace-hire-smoke-001",
                "hash-hire-smoke-001",
            ),
        )
        .await
        .expect("hire global member should succeed");

    let publish_summary = outbox_publisher
        .publish_outbox_events(10)
        .await
        .expect("outbox publish should succeed");
    let rebuild_summary = projection_rebuild
        .rebuild_member_summary_projection("member-summary-rebuild", 10)
        .await
        .expect("projection rebuild should succeed");
    let query_summary = query_service
        .get_member_summary(
            GetMemberSummaryQuery {
                global_member_id: created_member.global_member_id.clone(),
            },
            actor,
        )
        .await
        .expect("member summary query should succeed");

    let member_count: i64 = sqlx::query("SELECT COUNT(*) AS count FROM global_members")
        .fetch_one(&pool)
        .await
        .expect("count global members")
        .get("count");
    let published_outbox_count: i64 =
        sqlx::query("SELECT COUNT(*) AS count FROM outbox_events WHERE status = 'published'")
            .fetch_one(&pool)
            .await
            .expect("count published outbox rows")
            .get("count");
    let projection_row = sqlx::query(
        r#"
        SELECT display_name, lifecycle, main_role_name, projection_version
        FROM member_summary_projection
        WHERE global_member_id = $1
        "#,
    )
    .bind(created_member.global_member_id.as_str())
    .fetch_one(&pool)
    .await
    .expect("load member projection row");
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
    let role_row =
        sqlx::query("SELECT role_name, fingerprint FROM role_catalog_entries WHERE role_id = $1")
            .bind("role.member.operator")
            .fetch_one(&pool)
            .await
            .expect("load role catalog row");

    assert_eq!(
        publish_summary,
        PublishOutboxEventsSummary {
            scanned: 2,
            published: 2,
            failed: 0,
        }
    );
    assert_eq!(
        rebuild_summary,
        RebuildMemberSummaryProjectionSummary {
            scanned: 2,
            rebuilt: 1,
            skipped: 1,
        }
    );
    assert_eq!(
        bus_publisher.published_event_ids(),
        vec![
            "outbox:role-sync-smoke-001".to_string(),
            "outbox:idem-hire-smoke-001".to_string(),
        ]
    );
    assert_eq!(member_count, 1);
    assert_eq!(published_outbox_count, 2);
    assert_eq!(
        role_row.get::<String, _>("role_name"),
        "Name for role.member.operator"
    );
    assert_eq!(
        role_row.get::<String, _>("fingerprint"),
        "fp-role-smoke-001"
    );
    assert_eq!(
        projection_row.get::<String, _>("display_name"),
        "Member Smoke"
    );
    assert_eq!(projection_row.get::<String, _>("lifecycle"), "hired");
    assert_eq!(
        projection_row.get::<Option<String>, _>("main_role_name"),
        Some("Name for role.member.operator".to_string())
    );
    assert_eq!(projection_row.get::<i64, _>("projection_version"), 0);
    assert_eq!(
        query_summary.global_member_id,
        created_member.global_member_id
    );
    assert_eq!(query_summary.display_name, "Member Smoke");
    assert_eq!(query_summary.lifecycle, GlobalMemberLifecycle::Hired);
    assert_eq!(
        query_summary.main_role_id.as_ref().map(RoleId::as_str),
        Some("role.member.operator")
    );
    assert_eq!(
        query_summary.main_role_name.as_deref(),
        Some("Name for role.member.operator")
    );
    assert_eq!(query_summary.capability_summary_json, json!({}));
    assert_eq!(query_summary.career_summary_json, json!({}));
    assert_eq!(query_summary.memory_ref_summary_json, json!({}));
    assert_eq!(query_summary.projection_version, 0);
    assert_eq!(
        checkpoint_row.get::<Option<String>, _>("last_processed_event_id"),
        Some("outbox:idem-hire-smoke-001".to_string())
    );
    assert_eq!(checkpoint_row.get::<String, _>("status"), "idle");
    assert_eq!(
        checkpoint_row.get::<Option<String>, _>("failure_reason"),
        None
    );
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

fn sample_role_event(
    source_event_id: &str,
    payload_hash: &str,
    role_id: &str,
    fingerprint: &str,
    status: &str,
) -> InboundRoleCatalogEvent {
    InboundRoleCatalogEvent {
        envelope: InboundEventEnvelope {
            source_event_id: EventId::new(source_event_id),
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
