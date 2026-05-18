//! Operations jobs that act on already-persisted facts such as outbox rows and projections.

use time::{OffsetDateTime, PrimitiveDateTime};

use crate::application::persistence::{
    MemberSummaryProjectionRepository, OutboxStore, ProjectionCheckpointRepository,
    RoleCatalogRepository, UnitOfWork, UnitOfWorkFactory,
};
use crate::domain::outbox::OutboxEvent;
use crate::domain::projection::{MemberSummaryProjection, ProjectionCheckpoint};
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

async fn apply_member_summary_projection_event<Uow>(
    uow: &mut Uow,
    event: &OutboxEvent,
) -> Result<Option<MemberSummaryProjection>, IdentityError>
where
    Uow: UnitOfWork,
{
    let existing_projection = if event.event_type == "identity.capability_profile.updated" {
        let global_member_id = event
            .payload_json
            .get("global_member_id")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| IdentityError::PersistenceData {
                message: format!(
                    "capability-profile outbox payload for `{}` is missing `global_member_id`",
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

    use crate::config::AppConfig;
    use crate::domain::outbox::{OutboxEvent, OutboxStatus};
    use crate::domain::shared::ids::OutboxEventId;
    use crate::error::IdentityError;
    use crate::outbound::BusPublisherPort;
    use crate::persistence::database::run_migrations;
    use crate::persistence::test_support::DB_TEST_MUTEX;
    use crate::persistence::unit_of_work::SqlxUnitOfWorkFactory;

    use super::{
        OutboxPublisherJob, ProjectionRebuildJob, PublishOutboxEventsSummary,
        RebuildMemberSummaryProjectionSummary,
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

    fn now() -> PrimitiveDateTime {
        let now = OffsetDateTime::now_utc();
        PrimitiveDateTime::new(now.date(), now.time())
    }
}
