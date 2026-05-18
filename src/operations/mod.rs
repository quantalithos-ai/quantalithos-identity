//! Operations jobs that act on already-persisted facts such as outbox rows and projections.

use time::{OffsetDateTime, PrimitiveDateTime};

use crate::application::persistence::{OutboxStore, UnitOfWork, UnitOfWorkFactory};
use crate::error::IdentityError;
use crate::outbound::BusPublisherPort;

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

/// Placeholder projection rebuild job.
#[derive(Debug, Default)]
pub struct ProjectionRebuildJob;

impl ProjectionRebuildJob {
    /// Returns a stable placeholder operation name for diagnostics.
    pub fn operation_name(&self) -> &'static str {
        "RebuildMemberSummaryProjection"
    }
}

/// Placeholder role reconciliation job.
#[derive(Debug, Default)]
pub struct RoleReconciliationJob;

impl RoleReconciliationJob {
    /// Returns a stable placeholder operation name for diagnostics.
    pub fn operation_name(&self) -> &'static str {
        "ReconcileRoleCatalog"
    }
}

fn current_timestamp() -> PrimitiveDateTime {
    let now = OffsetDateTime::now_utc();
    PrimitiveDateTime::new(now.date(), now.time())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use serde_json::json;
    use sqlx::{Executor, Row, postgres::PgPoolOptions};
    use time::{OffsetDateTime, PrimitiveDateTime};

    use crate::config::AppConfig;
    use crate::domain::outbox::{OutboxEvent, OutboxStatus};
    use crate::domain::shared::ids::OutboxEventId;
    use crate::error::IdentityError;
    use crate::outbound::BusPublisherPort;
    use crate::persistence::database::run_migrations;
    use crate::persistence::test_support::DB_TEST_MUTEX;
    use crate::persistence::unit_of_work::SqlxUnitOfWorkFactory;

    use super::{OutboxPublisherJob, PublishOutboxEventsSummary};

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
                lifecycle_history_entries,
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

    fn now() -> PrimitiveDateTime {
        let now = OffsetDateTime::now_utc();
        PrimitiveDateTime::new(now.date(), now.time())
    }
}
