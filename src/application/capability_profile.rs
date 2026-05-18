//! Application service for explicit capability-profile updates.

use serde_json::json;

use crate::application::persistence::{
    AuditTraceRepository, CapabilityProfileRepository, GlobalMemberRepository, IdempotencyStore,
    OutboxStore, UnitOfWork, UnitOfWorkFactory,
};
use crate::domain::audit::AuditTraceEntry;
use crate::domain::capability_profile::{
    ArtifactRef, CapabilityItem, CapabilityProfile, CapabilityProfileSummary,
    UpdateCapabilityProfileCommand,
};
use crate::domain::idempotency::{IdempotencyRecord, IdempotencyScope, IdempotencyStatus};
use crate::domain::outbox::OutboxEvent;
use crate::domain::shared::context::ActorContext;
use crate::domain::shared::ids::{GlobalMemberId, OutboxEventId};
use crate::domain::shared::metadata::CommandMetadata;
use crate::error::IdentityError;
use crate::outbound::ArtifactPort;

/// Coordinates capability-profile writes over the shared transaction boundary.
#[derive(Debug, Clone)]
pub struct CapabilityProfileCommandService<UowFactory, ArtifactValidator> {
    unit_of_work_factory: UowFactory,
    artifact_validator: ArtifactValidator,
}

impl<UowFactory, ArtifactValidator> CapabilityProfileCommandService<UowFactory, ArtifactValidator> {
    /// Creates a new capability-profile command service bound to the provided ports.
    pub fn new(unit_of_work_factory: UowFactory, artifact_validator: ArtifactValidator) -> Self {
        Self {
            unit_of_work_factory,
            artifact_validator,
        }
    }
}

impl<UowFactory, ArtifactValidator> CapabilityProfileCommandService<UowFactory, ArtifactValidator>
where
    UowFactory: UnitOfWorkFactory,
    ArtifactValidator: ArtifactPort,
{
    /// Replaces one member capability profile while preserving ref-only evidence storage.
    ///
    /// # Errors
    ///
    /// Returns an error when the member is missing, when the member is terminal, when evidence
    /// refs are invalid, when the idempotency key conflicts, or when persistence fails.
    pub async fn update_capability_profile(
        &self,
        command: UpdateCapabilityProfileCommand,
        actor: ActorContext,
        metadata: CommandMetadata,
    ) -> Result<CapabilityProfileSummary, IdentityError> {
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

        if member.lifecycle.is_terminal() {
            uow.rollback().await?;
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_MEMBER_NOT_MUTABLE",
                message: format!(
                    "global member `{}` is terminal and cannot update capability profile",
                    command.global_member_id.as_str()
                ),
            });
        }

        let existing_profile = uow
            .capability_profiles()
            .get_for_update_by_member(&command.global_member_id)
            .await?;
        let (mut profile, expected_version, should_insert) = match existing_profile {
            Some(profile) => {
                let expected_version = command.expected_version.unwrap_or(profile.version);
                (profile, expected_version, false)
            }
            None => {
                let expected_version = command.expected_version.unwrap_or(0);
                if expected_version != 0 {
                    uow.rollback().await?;
                    return Err(IdentityError::VersionConflict {
                        entity: "capability_profile".to_string(),
                    });
                }

                (
                    CapabilityProfile::create_for_member(
                        command.global_member_id.clone(),
                        Vec::new(),
                        &actor,
                    )?,
                    expected_version,
                    true,
                )
            }
        };

        self.artifact_validator
            .validate_refs(&command.evidence_refs)
            .await?;
        profile.replace_capabilities(command.capabilities, command.evidence_refs, &actor)?;

        if should_insert {
            uow.capability_profiles().insert(&profile).await?;
        } else {
            uow.capability_profiles()
                .save(&profile, expected_version)
                .await?;
        }

        let member_expected_version = member.version;
        if member.capability_profile_id.as_ref() != Some(&profile.capability_profile_id) {
            member.link_capability_profile(profile.capability_profile_id.clone());
            uow.global_members()
                .save(&member, member_expected_version)
                .await?;
        }

        let summary = profile.summary();
        let audit_entry = AuditTraceEntry::for_capability_profile_command(
            format!("audit:{}", metadata.idempotency_key()),
            &profile,
            &actor,
            metadata.trace_id(),
            profile.updated_at,
        );
        let outbox_event = OutboxEvent::for_capability_profile_updated(
            OutboxEventId::new(format!("outbox:{}", metadata.idempotency_key())),
            &member,
            &profile,
            metadata.idempotency_key(),
            profile.updated_at,
        );

        uow.audit_traces().append(&audit_entry).await?;
        uow.outbox().append(&outbox_event).await?;
        uow.idempotency()
            .record_success(
                &metadata,
                IdempotencyScope::Command,
                json!({
                    "kind": "capability_profile",
                    "id": summary.capability_profile_id.as_str(),
                    "global_member_id": summary.global_member_id.as_str(),
                    "capabilities": summary.capabilities.clone(),
                    "evidence_refs": summary.evidence_refs.clone(),
                    "version": summary.version,
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
    ) -> Result<CapabilityProfileSummary, IdentityError>
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
}

fn summary_from_result_ref(
    result_ref_json: Option<&serde_json::Value>,
) -> Option<CapabilityProfileSummary> {
    let result_ref_json = result_ref_json?;
    let capability_profile_id = result_ref_json.get("id")?.as_str()?;
    let global_member_id = result_ref_json.get("global_member_id")?.as_str()?;
    let capabilities: Vec<CapabilityItem> =
        serde_json::from_value(result_ref_json.get("capabilities")?.clone()).ok()?;
    let evidence_refs: Vec<ArtifactRef> =
        serde_json::from_value(result_ref_json.get("evidence_refs")?.clone()).ok()?;
    let version = result_ref_json.get("version")?.as_i64()?;

    Some(CapabilityProfileSummary {
        capability_profile_id: crate::domain::shared::ids::CapabilityProfileId::new(
            capability_profile_id,
        ),
        global_member_id: GlobalMemberId::new(global_member_id),
        capabilities,
        evidence_refs,
        version,
    })
}

#[cfg(test)]
fn current_timestamp() -> time::PrimitiveDateTime {
    let now = time::OffsetDateTime::now_utc();
    time::PrimitiveDateTime::new(now.date(), now.time())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;
    use sqlx::{Executor, Row, postgres::PgPoolOptions};

    use crate::application::member_lifecycle::MemberLifecycleCommandService;
    use crate::application::query_projection::{GetMemberSummaryQuery, QueryProjectionService};
    use crate::config::AppConfig;
    use crate::domain::capability_profile::{
        ArtifactRef, CapabilityItem, UpdateCapabilityProfileCommand,
    };
    use crate::domain::member::HireGlobalMemberCommand;
    use crate::domain::shared::context::{ActorContext, ActorKind};
    use crate::domain::shared::ids::RoleId;
    use crate::domain::shared::metadata::CommandMetadata;
    use crate::error::IdentityError;
    use crate::operations::ProjectionRebuildJob;
    use crate::outbound::ArtifactPort;
    use crate::persistence::database::run_migrations;
    use crate::persistence::test_support::DB_TEST_MUTEX;
    use crate::persistence::unit_of_work::SqlxUnitOfWorkFactory;

    use super::{CapabilityProfileCommandService, current_timestamp};

    #[derive(Debug, Clone, Default)]
    struct StubArtifactValidator {
        invalid_artifact_id: Option<String>,
    }

    impl StubArtifactValidator {
        fn accepting() -> Self {
            Self::default()
        }

        fn rejecting(artifact_id: &str) -> Self {
            Self {
                invalid_artifact_id: Some(artifact_id.to_string()),
            }
        }
    }

    impl ArtifactPort for StubArtifactValidator {
        async fn validate_refs(&self, refs: &[ArtifactRef]) -> Result<(), IdentityError> {
            if let Some(invalid_artifact_id) = self.invalid_artifact_id.as_deref() {
                if refs
                    .iter()
                    .any(|artifact_ref| artifact_ref.artifact_id == invalid_artifact_id)
                {
                    return Err(IdentityError::RuleViolation {
                        code: "IDENTITY_ARTIFACT_REF_INVALID",
                        message: format!(
                            "artifact ref `{invalid_artifact_id}` is not valid for identity retention"
                        ),
                    });
                }
            }

            Ok(())
        }
    }

    #[tokio::test]
    async fn update_capability_profile_creates_profile_links_member_and_refreshes_projection() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let hire_service = MemberLifecycleCommandService::new(factory.clone());
        let service = CapabilityProfileCommandService::new(
            factory.clone(),
            StubArtifactValidator::accepting(),
        );
        let rebuild_job = ProjectionRebuildJob::new(factory.clone());
        let query_service = QueryProjectionService::new(factory);
        let actor = ActorContext::new("human/admin-capability-1", ActorKind::HumanUser, None);
        let hire_metadata = CommandMetadata::new(
            "idem-hire-capability-001",
            "trace-hire-capability-001",
            "hash-hire-capability-001",
        );
        let capability_metadata = CommandMetadata::new(
            "idem-capability-001",
            "trace-capability-001",
            "hash-capability-001",
        );

        let member = hire_service
            .hire_global_member(
                HireGlobalMemberCommand {
                    display_name: "Capability Member".to_string(),
                    main_role_id: RoleId::new("role.member.operator"),
                    secondary_role_ids: Vec::new(),
                },
                actor.clone(),
                hire_metadata.clone(),
            )
            .await
            .expect("hire member for capability update");

        let summary = service
            .update_capability_profile(
                UpdateCapabilityProfileCommand {
                    global_member_id: member.global_member_id.clone(),
                    capabilities: sample_capabilities(),
                    evidence_refs: sample_evidence_refs(),
                    expected_version: None,
                },
                actor.clone(),
                capability_metadata.clone(),
            )
            .await
            .expect("capability update should succeed");

        sqlx::query("UPDATE outbox_events SET created_at = $2 WHERE outbox_event_id = $1")
            .bind(format!("outbox:{}", hire_metadata.idempotency_key()))
            .bind(current_timestamp())
            .execute(&pool)
            .await
            .expect("stabilize hire outbox created_at");
        sqlx::query("UPDATE outbox_events SET created_at = $2 WHERE outbox_event_id = $1")
            .bind(format!("outbox:{}", capability_metadata.idempotency_key()))
            .bind(current_timestamp() + time::Duration::seconds(1))
            .execute(&pool)
            .await
            .expect("stabilize capability outbox created_at");

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

        let profile_row = sqlx::query(
            r#"
            SELECT capability_profile_id, capabilities_json, evidence_refs_json, version
            FROM capability_profiles
            WHERE global_member_id = $1
            "#,
        )
        .bind(member.global_member_id.as_str())
        .fetch_one(&pool)
        .await
        .expect("load capability profile row");
        let member_row = sqlx::query(
            "SELECT capability_profile_id FROM global_members WHERE global_member_id = $1",
        )
        .bind(member.global_member_id.as_str())
        .fetch_one(&pool)
        .await
        .expect("load linked member row");
        let audit_row = sqlx::query(
            "SELECT action, target_ref_json FROM audit_trace_entries WHERE audit_trace_id = $1",
        )
        .bind(format!("audit:{}", capability_metadata.idempotency_key()))
        .fetch_one(&pool)
        .await
        .expect("load capability audit row");
        let outbox_row = sqlx::query(
            "SELECT event_type, payload_json FROM outbox_events WHERE outbox_event_id = $1",
        )
        .bind(format!("outbox:{}", capability_metadata.idempotency_key()))
        .fetch_one(&pool)
        .await
        .expect("load capability outbox row");
        let idempotency_row = sqlx::query(
            "SELECT status, result_ref_json FROM idempotency_records WHERE idempotency_key = $1",
        )
        .bind(capability_metadata.idempotency_key())
        .fetch_one(&pool)
        .await
        .expect("load capability idempotency row");

        let expected_capability_summary_json = json!({
            "capability_profile_id": summary.capability_profile_id.as_str(),
            "items": sample_capabilities(),
            "evidence_refs": sample_evidence_refs(),
            "version": 1,
        });
        let outbox_payload_json = outbox_row.get::<serde_json::Value, _>("payload_json");
        let idempotency_result_ref_json =
            idempotency_row.get::<serde_json::Value, _>("result_ref_json");

        assert_eq!(summary.global_member_id, member.global_member_id);
        assert_eq!(summary.version, 1);
        assert_eq!(summary.capabilities, sample_capabilities());
        assert_eq!(summary.evidence_refs, sample_evidence_refs());
        assert_eq!(
            profile_row.get::<String, _>("capability_profile_id"),
            summary.capability_profile_id.as_str()
        );
        assert_eq!(
            member_row.get::<Option<String>, _>("capability_profile_id"),
            Some(summary.capability_profile_id.as_str().to_string())
        );
        assert_eq!(
            profile_row.get::<serde_json::Value, _>("capabilities_json"),
            json!(sample_capabilities())
        );
        assert_eq!(
            profile_row.get::<serde_json::Value, _>("evidence_refs_json"),
            json!(sample_evidence_refs())
        );
        assert_eq!(profile_row.get::<i64, _>("version"), 1);
        assert_eq!(
            audit_row.get::<String, _>("action"),
            "UpdateCapabilityProfile"
        );
        assert_eq!(
            audit_row.get::<serde_json::Value, _>("target_ref_json"),
            json!({
                "kind": "capability_profile",
                "id": summary.capability_profile_id.as_str(),
                "global_member_id": member.global_member_id.as_str(),
            })
        );
        assert_eq!(
            outbox_row.get::<String, _>("event_type"),
            "identity.capability_profile.updated"
        );
        assert_eq!(
            outbox_payload_json.get("capability_summary_json"),
            Some(&expected_capability_summary_json)
        );
        assert_eq!(idempotency_row.get::<String, _>("status"), "succeeded");
        assert_eq!(
            idempotency_result_ref_json
                .get("id")
                .and_then(serde_json::Value::as_str),
            Some(summary.capability_profile_id.as_str())
        );
        assert_eq!(
            query_summary.capability_summary_json,
            expected_capability_summary_json
        );
    }

    #[tokio::test]
    async fn update_capability_profile_rejects_invalid_artifact_refs_without_persisting() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let hire_service = MemberLifecycleCommandService::new(factory.clone());
        let service = CapabilityProfileCommandService::new(
            factory,
            StubArtifactValidator::rejecting("artifact-bad"),
        );
        let actor = ActorContext::new("human/admin-capability-2", ActorKind::HumanUser, None);

        let member = hire_service
            .hire_global_member(
                HireGlobalMemberCommand {
                    display_name: "Artifact Invalid Member".to_string(),
                    main_role_id: RoleId::new("role.member.operator"),
                    secondary_role_ids: Vec::new(),
                },
                actor.clone(),
                CommandMetadata::new(
                    "idem-hire-capability-002",
                    "trace-hire-capability-002",
                    "hash-hire-capability-002",
                ),
            )
            .await
            .expect("hire member before invalid artifact test");

        let error = service
            .update_capability_profile(
                UpdateCapabilityProfileCommand {
                    global_member_id: member.global_member_id.clone(),
                    capabilities: sample_capabilities(),
                    evidence_refs: vec![ArtifactRef {
                        artifact_id: "artifact-bad".to_string(),
                        artifact_kind: "evidence".to_string(),
                        artifact_version: Some("v1".to_string()),
                    }],
                    expected_version: None,
                },
                actor,
                CommandMetadata::new(
                    "idem-capability-002",
                    "trace-capability-002",
                    "hash-capability-002",
                ),
            )
            .await
            .expect_err("invalid artifact refs should be rejected");

        assert!(matches!(
            error,
            IdentityError::RuleViolation {
                code: "IDENTITY_ARTIFACT_REF_INVALID",
                ..
            }
        ));

        let profile_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM capability_profiles WHERE global_member_id = $1",
        )
        .bind(member.global_member_id.as_str())
        .fetch_one(&pool)
        .await
        .expect("count capability profiles");
        let audit_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM audit_trace_entries WHERE action = $1")
                .bind("UpdateCapabilityProfile")
                .fetch_one(&pool)
                .await
                .expect("count capability audits");
        let outbox_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM outbox_events WHERE event_type = 'identity.capability_profile.updated'",
        )
        .fetch_one(&pool)
        .await
        .expect("count capability outbox rows");
        let idempotency_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM idempotency_records WHERE idempotency_key = $1",
        )
        .bind("idem-capability-002")
        .fetch_one(&pool)
        .await
        .expect("count capability idempotency rows");

        assert_eq!(profile_count, 0);
        assert_eq!(audit_count, 0);
        assert_eq!(outbox_count, 0);
        assert_eq!(idempotency_count, 0);
    }

    #[tokio::test]
    async fn update_capability_profile_rejects_terminal_members() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let hire_service = MemberLifecycleCommandService::new(factory.clone());
        let service =
            CapabilityProfileCommandService::new(factory, StubArtifactValidator::accepting());
        let actor = ActorContext::new("human/admin-capability-3", ActorKind::HumanUser, None);

        let member = hire_service
            .hire_global_member(
                HireGlobalMemberCommand {
                    display_name: "Terminal Member".to_string(),
                    main_role_id: RoleId::new("role.member.operator"),
                    secondary_role_ids: Vec::new(),
                },
                actor.clone(),
                CommandMetadata::new(
                    "idem-hire-capability-003",
                    "trace-hire-capability-003",
                    "hash-hire-capability-003",
                ),
            )
            .await
            .expect("hire member before terminal-member test");

        sqlx::query(
            "UPDATE global_members SET lifecycle = 'retired', version = version + 1, updated_at = $2 WHERE global_member_id = $1",
        )
        .bind(member.global_member_id.as_str())
        .bind(current_timestamp())
        .execute(&pool)
        .await
        .expect("mark member retired");

        let error = service
            .update_capability_profile(
                UpdateCapabilityProfileCommand {
                    global_member_id: member.global_member_id.clone(),
                    capabilities: sample_capabilities(),
                    evidence_refs: sample_evidence_refs(),
                    expected_version: None,
                },
                actor,
                CommandMetadata::new(
                    "idem-capability-003",
                    "trace-capability-003",
                    "hash-capability-003",
                ),
            )
            .await
            .expect_err("terminal members should reject capability updates");

        assert!(matches!(
            error,
            IdentityError::RuleViolation {
                code: "IDENTITY_MEMBER_NOT_MUTABLE",
                ..
            }
        ));

        let profile_count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM capability_profiles WHERE global_member_id = $1",
        )
        .bind(member.global_member_id.as_str())
        .fetch_one(&pool)
        .await
        .expect("count capability profiles");
        assert_eq!(profile_count, 0);
    }

    #[tokio::test]
    async fn update_capability_profile_uses_optimistic_locking() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let hire_service = MemberLifecycleCommandService::new(factory.clone());
        let service =
            CapabilityProfileCommandService::new(factory, StubArtifactValidator::accepting());
        let actor = ActorContext::new("human/admin-capability-4", ActorKind::HumanUser, None);

        let member = hire_service
            .hire_global_member(
                HireGlobalMemberCommand {
                    display_name: "Versioned Member".to_string(),
                    main_role_id: RoleId::new("role.member.operator"),
                    secondary_role_ids: Vec::new(),
                },
                actor.clone(),
                CommandMetadata::new(
                    "idem-hire-capability-004",
                    "trace-hire-capability-004",
                    "hash-hire-capability-004",
                ),
            )
            .await
            .expect("hire member before version-conflict test");

        service
            .update_capability_profile(
                UpdateCapabilityProfileCommand {
                    global_member_id: member.global_member_id.clone(),
                    capabilities: sample_capabilities(),
                    evidence_refs: sample_evidence_refs(),
                    expected_version: None,
                },
                actor.clone(),
                CommandMetadata::new(
                    "idem-capability-004a",
                    "trace-capability-004a",
                    "hash-capability-004a",
                ),
            )
            .await
            .expect("initial capability update should succeed");

        let error = service
            .update_capability_profile(
                UpdateCapabilityProfileCommand {
                    global_member_id: member.global_member_id.clone(),
                    capabilities: vec![CapabilityItem {
                        capability_id: "capability.sql".to_string(),
                        capability_name: "SQL".to_string(),
                        proficiency: Some("intermediate".to_string()),
                        notes: None,
                    }],
                    evidence_refs: sample_evidence_refs(),
                    expected_version: Some(0),
                },
                actor,
                CommandMetadata::new(
                    "idem-capability-004b",
                    "trace-capability-004b",
                    "hash-capability-004b",
                ),
            )
            .await
            .expect_err("stale expected_version should fail");

        assert!(matches!(
            error,
            IdentityError::VersionConflict { ref entity } if entity == "capability_profile"
        ));

        let profile_row = sqlx::query(
            "SELECT capabilities_json, version FROM capability_profiles WHERE global_member_id = $1",
        )
        .bind(member.global_member_id.as_str())
        .fetch_one(&pool)
        .await
        .expect("load capability profile after version conflict");
        assert_eq!(
            profile_row.get::<serde_json::Value, _>("capabilities_json"),
            json!(sample_capabilities())
        );
        assert_eq!(profile_row.get::<i64, _>("version"), 1);
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

    fn sample_capabilities() -> Vec<CapabilityItem> {
        vec![CapabilityItem {
            capability_id: "capability.rust".to_string(),
            capability_name: "Rust".to_string(),
            proficiency: Some("advanced".to_string()),
            notes: Some("systems programming".to_string()),
        }]
    }

    fn sample_evidence_refs() -> Vec<ArtifactRef> {
        vec![ArtifactRef {
            artifact_id: "artifact-001".to_string(),
            artifact_kind: "evidence".to_string(),
            artifact_version: Some("v1".to_string()),
        }]
    }
}
