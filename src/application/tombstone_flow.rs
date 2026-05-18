//! Application services for high-risk tombstone command and governance event handling.

use serde_json::json;
use time::{OffsetDateTime, PrimitiveDateTime};

use crate::application::persistence::{
    AuditTraceRepository, GlobalMemberRepository, IdempotencyStore, InboundDeadLetterStore,
    LifecycleHistoryRepository, MemoryRefsRepository, OutboxStore, PendingTombstoneRepository,
    UnitOfWork, UnitOfWorkFactory,
};
use crate::domain::audit::{AuditResult, AuditTraceEntry};
use crate::domain::dead_letter::{DeadLetterReplayStatus, InboundDeadLetter};
use crate::domain::idempotency::{IdempotencyRecord, IdempotencyScope, IdempotencyStatus};
use crate::domain::member::{GlobalMemberLifecycle, GlobalMemberSummary};
use crate::domain::outbox::OutboxEvent;
use crate::domain::shared::context::ActorContext;
use crate::domain::shared::ids::{DeadLetterId, GlobalMemberId, OutboxEventId};
use crate::domain::shared::metadata::CommandMetadata;
use crate::domain::timeline::LifecycleHistoryEntry;
use crate::domain::tombstone::TombstoneMemberCommand;
use crate::error::IdentityError;
use crate::inbound::events::{
    GateDecisionEventParser, InboundEventEnvelope, InboundGateDecisionEvent,
};
use crate::outbound::{ArchiveRequestPort, GovernancePort};

/// Summarizes the result of handling one inbound gate-decision event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GateDecisionOutcome {
    /// Governance evidence was attached to an existing pending flow.
    Recorded { pending_flow_id: String },
    /// The event had already been consumed successfully with the same payload hash.
    SkippedDuplicate { pending_flow_id: Option<String> },
    /// No pending flow matched the gate decision id, so only local evidence was recorded.
    SkippedNoPendingFlow { gate_decision_id: String },
    /// The inbound event was retained into dead-letter storage instead of mutating state.
    DeadLettered,
}

/// Coordinates the explicit tombstone command and governance gate-decision event handling.
#[derive(Debug, Clone)]
pub struct TombstoneFlowService<UowFactory, Governance, ArchiveRequester> {
    unit_of_work_factory: UowFactory,
    governance: Governance,
    archive_requester: ArchiveRequester,
    gate_decision_parser: GateDecisionEventParser,
}

impl<UowFactory, Governance, ArchiveRequester>
    TombstoneFlowService<UowFactory, Governance, ArchiveRequester>
{
    /// Creates a new tombstone flow service bound to the provided persistence and outbound ports.
    pub fn new(
        unit_of_work_factory: UowFactory,
        governance: Governance,
        archive_requester: ArchiveRequester,
    ) -> Self {
        Self {
            unit_of_work_factory,
            governance,
            archive_requester,
            gate_decision_parser: GateDecisionEventParser,
        }
    }
}

impl<UowFactory, Governance, ArchiveRequester>
    TombstoneFlowService<UowFactory, Governance, ArchiveRequester>
where
    UowFactory: UnitOfWorkFactory,
    Governance: GovernancePort,
    ArchiveRequester: ArchiveRequestPort,
{
    /// Tombstones one member after governance approval and archive collaboration are confirmed.
    pub async fn tombstone_member(
        &self,
        command: TombstoneMemberCommand,
        actor: ActorContext,
        metadata: CommandMetadata,
    ) -> Result<GlobalMemberSummary, IdentityError> {
        if metadata.idempotency_key().trim().is_empty() {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_INVALID_ARGUMENT",
                message: "idempotency_key must not be blank".to_string(),
            });
        }
        if command.reason.trim().is_empty() {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_INVALID_ARGUMENT",
                message: "reason must not be blank".to_string(),
            });
        }

        let mut uow = self.unit_of_work_factory.begin().await?;
        let existing_record = {
            let mut idempotency = uow.idempotency();
            idempotency
                .get(metadata.idempotency_key(), IdempotencyScope::Command)
                .await?
        };

        if let Some(existing_record) = existing_record {
            return self
                .handle_existing_command_record(existing_record, metadata.request_hash(), uow)
                .await;
        }

        let mut member = {
            let mut repository = uow.global_members();
            repository.get_for_update(&command.global_member_id).await?
        }
        .ok_or_else(|| IdentityError::RuleViolation {
            code: "IDENTITY_MEMBER_NOT_FOUND",
            message: format!(
                "global member `{}` was not found",
                command.global_member_id.as_str()
            ),
        })?;

        let member_expected_version = command.expected_version.unwrap_or(member.version);
        let gate_decision_ref = self
            .governance
            .require_gate_decision(
                "TombstoneMember",
                &member,
                &actor,
                &command.reason,
                command.gate_decision_ref.as_ref(),
            )
            .await?;
        gate_decision_ref.validate()?;
        if !gate_decision_ref.is_approved() {
            uow.rollback().await?;
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_GATE_REJECTED",
                message: format!(
                    "member `{}` cannot be tombstoned without an approved gate decision",
                    member.global_member_id.as_str()
                ),
            });
        }

        let archive_ref = match self
            .archive_requester
            .request_archive(&member.global_member_id, &command.reason)
            .await
        {
            Ok(archive_ref) => archive_ref,
            Err(error) => {
                uow.rollback().await?;
                return Err(IdentityError::RuleViolation {
                    code: "IDENTITY_MEMORY_ARCHIVE_UNAVAILABLE",
                    message: format!(
                        "memory archive request failed for member `{}`: {error}",
                        member.global_member_id.as_str()
                    ),
                });
            }
        };
        archive_ref.validate()?;

        let mut memory_ref_summary_json = json!({});
        if let Some(mut memory_refs) = {
            let mut repository = uow.memory_refs();
            repository
                .get_for_update_by_member(&member.global_member_id)
                .await?
        } {
            let expected_version = memory_refs.version;
            memory_refs.mark_archive_pending(archive_ref)?;
            memory_ref_summary_json = memory_refs.summary_json();
            uow.memory_refs()
                .save(&memory_refs, expected_version)
                .await?;
        }

        let from_lifecycle = member.lifecycle;
        member.tombstone(gate_decision_ref.clone(), &actor)?;

        let history_entry = LifecycleHistoryEntry::for_tombstone(
            format!("history:{}", metadata.idempotency_key()),
            &member,
            from_lifecycle,
            actor.clone(),
            &gate_decision_ref,
            metadata.clone(),
        );
        let audit_entry = AuditTraceEntry::for_tombstone_command(
            format!("audit:{}", metadata.idempotency_key()),
            &member,
            &actor,
            metadata.trace_id(),
            member.updated_at,
            Some(command.reason.clone()),
        );
        let outbox_event = OutboxEvent::for_member_tombstoned(
            OutboxEventId::new(format!("outbox:{}", metadata.idempotency_key())),
            &member,
            memory_ref_summary_json,
            &gate_decision_ref,
            &command.reason,
            metadata.idempotency_key(),
            member.updated_at,
        );

        uow.global_members()
            .save(&member, member_expected_version)
            .await?;
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

    /// Records governance gate evidence for an existing pending tombstone flow.
    pub async fn handle_gate_decision_event(
        &self,
        event: InboundGateDecisionEvent,
    ) -> Result<GateDecisionOutcome, IdentityError> {
        let envelope = event.envelope;
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
                .handle_existing_gate_event_record(existing_record, &envelope, uow)
                .await;
        }

        let now = current_timestamp();
        let gate_decision_ref = match self.gate_decision_parser.parse(envelope.payload.clone()) {
            Ok(gate_decision_ref) => gate_decision_ref,
            Err(error) => {
                let dead_letter = build_dead_letter(&envelope, error.to_string());
                uow.inbound_dead_letters().append(&dead_letter).await?;
                uow.commit().await?;
                return Ok(GateDecisionOutcome::DeadLettered);
            }
        };
        let metadata = inbound_event_metadata(&envelope);

        let mut pending_flow = match {
            let mut repository = uow.pending_tombstone_flows();
            repository
                .get_by_gate_decision(&gate_decision_ref.gate_decision_id)
                .await?
        } {
            Some(pending_flow) => pending_flow,
            None => {
                let audit_entry = AuditTraceEntry::for_inbound_event(
                    format!("audit:{}", envelope.source_event_id.as_str()),
                    "HandleGateDecisionEvent",
                    envelope.source_module.clone(),
                    envelope.source_event_id.as_str(),
                    Some(json!({
                        "kind": "gate_decision",
                        "id": gate_decision_ref.gate_decision_id.as_str(),
                    })),
                    AuditResult::Skipped,
                    Some("no pending tombstone flow matched the gate decision".to_string()),
                    now,
                );
                uow.audit_traces().append(&audit_entry).await?;
                uow.idempotency()
                    .record_success(
                        &metadata,
                        IdempotencyScope::InboundEvent,
                        json!({
                            "kind": "gate_decision",
                            "id": gate_decision_ref.gate_decision_id.as_str(),
                            "pending_flow_id": null,
                            "status": "skipped_no_pending_flow",
                        }),
                    )
                    .await?;
                uow.commit().await?;
                return Ok(GateDecisionOutcome::SkippedNoPendingFlow {
                    gate_decision_id: gate_decision_ref.gate_decision_id.as_str().to_string(),
                });
            }
        };

        if let Err(error) = pending_flow.attach_gate_decision(gate_decision_ref.clone(), now) {
            let dead_letter = build_dead_letter(&envelope, error.to_string());
            uow.inbound_dead_letters().append(&dead_letter).await?;
            uow.commit().await?;
            return Ok(GateDecisionOutcome::DeadLettered);
        }

        let audit_entry = AuditTraceEntry::for_inbound_event(
            format!("audit:{}", envelope.source_event_id.as_str()),
            "HandleGateDecisionEvent",
            envelope.source_module.clone(),
            envelope.source_event_id.as_str(),
            Some(json!({
                "kind": "pending_tombstone_flow",
                "id": pending_flow.pending_flow_id.as_str(),
                "global_member_id": pending_flow.global_member_id.as_str(),
            })),
            AuditResult::Success,
            None,
            now,
        );
        let outbox_event = OutboxEvent::for_gate_decision_recorded(
            OutboxEventId::new(format!("outbox:{}", envelope.source_event_id.as_str())),
            &pending_flow,
            envelope.source_event_id.as_str(),
            now,
        );

        uow.pending_tombstone_flows().save(&pending_flow).await?;
        uow.audit_traces().append(&audit_entry).await?;
        uow.outbox().append(&outbox_event).await?;
        uow.idempotency()
            .record_success(
                &metadata,
                IdempotencyScope::InboundEvent,
                json!({
                    "kind": "pending_tombstone_flow",
                    "id": pending_flow.pending_flow_id.as_str(),
                    "global_member_id": pending_flow.global_member_id.as_str(),
                    "gate_decision_id": gate_decision_ref.gate_decision_id.as_str(),
                }),
            )
            .await?;
        uow.commit().await?;

        Ok(GateDecisionOutcome::Recorded {
            pending_flow_id: pending_flow.pending_flow_id.as_str().to_string(),
        })
    }

    async fn handle_existing_command_record<Uow>(
        &self,
        existing_record: IdempotencyRecord,
        request_hash: &str,
        uow: Uow,
    ) -> Result<GlobalMemberSummary, IdentityError>
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

    async fn handle_existing_gate_event_record<Uow>(
        &self,
        existing_record: IdempotencyRecord,
        envelope: &InboundEventEnvelope,
        mut uow: Uow,
    ) -> Result<GateDecisionOutcome, IdentityError>
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
            let dead_letter = build_dead_letter(envelope, error.to_string());
            uow.inbound_dead_letters().append(&dead_letter).await?;
            uow.commit().await?;
            return Err(error);
        }

        let pending_flow_id = existing_record
            .result_ref_json
            .as_ref()
            .and_then(|value| value.get("id"))
            .and_then(serde_json::Value::as_str)
            .map(ToString::to_string);

        match existing_record.status {
            IdempotencyStatus::Succeeded => {
                uow.rollback().await?;
                Ok(GateDecisionOutcome::SkippedDuplicate { pending_flow_id })
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
        lifecycle: GlobalMemberLifecycle::from_db(lifecycle)?,
        main_role_id: crate::domain::shared::ids::RoleId::new(main_role_id),
        secondary_role_ids,
        capability_profile_id,
        memory_refs_id,
    })
}

fn inbound_event_metadata(envelope: &InboundEventEnvelope) -> CommandMetadata {
    CommandMetadata::new(
        envelope.source_event_id.as_str(),
        envelope.source_event_id.as_str(),
        envelope.payload_hash.clone(),
    )
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

    use crate::application::member_lifecycle::MemberLifecycleCommandService;
    use crate::application::memory_refs::MemoryRefsCommandService;
    use crate::application::persistence::{
        PendingTombstoneRepository, UnitOfWork, UnitOfWorkFactory,
    };
    use crate::config::AppConfig;
    use crate::domain::member::{GlobalMemberLifecycle, HireGlobalMemberCommand};
    use crate::domain::memory_refs::{ArchiveRef, MemoryRef, UpdateMemoryRefsCommand};
    use crate::domain::shared::context::{ActorContext, ActorKind};
    use crate::domain::shared::ids::{GateDecisionId, GlobalMemberId, PendingFlowId, RoleId};
    use crate::domain::shared::metadata::CommandMetadata;
    use crate::domain::tombstone::{
        GateDecision, GateDecisionRef, PendingTombstoneFlow, TombstoneMemberCommand,
    };
    use crate::error::IdentityError;
    use crate::inbound::event_consumers::GateDecisionConsumer;
    use crate::inbound::events::{InboundEventEnvelope, InboundGateDecisionEvent};
    use crate::operations::ProjectionRebuildJob;
    use crate::outbound::{ArchiveRequestPort, GovernancePort, MemoryArchivePort};
    use crate::persistence::database::run_migrations;
    use crate::persistence::test_support::DB_TEST_MUTEX;
    use crate::persistence::unit_of_work::SqlxUnitOfWorkFactory;

    use super::{GateDecisionOutcome, TombstoneFlowService};

    #[derive(Debug, Clone)]
    struct StubGovernancePort {
        response: GateDecisionRef,
        calls: Arc<Mutex<usize>>,
    }

    impl StubGovernancePort {
        fn new(response: GateDecisionRef) -> Self {
            Self {
                response,
                calls: Arc::new(Mutex::new(0)),
            }
        }

        fn call_count(&self) -> usize {
            *self.calls.lock().expect("lock governance calls")
        }
    }

    impl GovernancePort for StubGovernancePort {
        async fn require_gate_decision(
            &self,
            _action_name: &str,
            _member: &crate::domain::member::GlobalMember,
            _actor: &ActorContext,
            _reason: &str,
            _supplied_gate_ref: Option<&GateDecisionRef>,
        ) -> Result<GateDecisionRef, IdentityError> {
            let mut calls = self.calls.lock().expect("lock governance calls");
            *calls += 1;
            Ok(self.response.clone())
        }
    }

    #[derive(Debug, Clone)]
    struct StubArchiveRequester {
        response: Result<ArchiveRef, String>,
        calls: Arc<Mutex<Vec<(String, String)>>>,
    }

    impl StubArchiveRequester {
        fn succeeding(response: ArchiveRef) -> Self {
            Self {
                response: Ok(response),
                calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn failing(message: &str) -> Self {
            Self {
                response: Err(message.to_string()),
                calls: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn call_count(&self) -> usize {
            self.calls.lock().expect("lock archive calls").len()
        }
    }

    impl ArchiveRequestPort for StubArchiveRequester {
        async fn request_archive(
            &self,
            global_member_id: &GlobalMemberId,
            reason: &str,
        ) -> Result<ArchiveRef, IdentityError> {
            self.calls
                .lock()
                .expect("lock archive calls")
                .push((global_member_id.as_str().to_string(), reason.to_string()));

            match &self.response {
                Ok(archive_ref) => Ok(archive_ref.clone()),
                Err(message) => Err(IdentityError::PersistenceData {
                    message: message.clone(),
                }),
            }
        }
    }

    #[derive(Debug, Clone, Default)]
    struct StubMemoryArchiveValidator;

    impl MemoryArchivePort for StubMemoryArchiveValidator {
        async fn validate_ref(&self, memory_ref: &MemoryRef) -> Result<(), IdentityError> {
            memory_ref.validate()
        }
    }

    #[tokio::test]
    async fn tombstone_member_persists_history_audit_outbox_and_archive_pending() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator").await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let lifecycle_service = MemberLifecycleCommandService::new(factory.clone());
        let memory_refs_service =
            MemoryRefsCommandService::new(factory.clone(), StubMemoryArchiveValidator);
        let governance = StubGovernancePort::new(approved_gate_decision("gate-001"));
        let archive_requester = StubArchiveRequester::succeeding(ArchiveRef {
            archive_id: "archive-001".to_string(),
            archive_kind: "member-memory".to_string(),
            archive_version: Some("v1".to_string()),
        });
        let service = TombstoneFlowService::new(
            factory.clone(),
            governance.clone(),
            archive_requester.clone(),
        );
        let actor = ActorContext::new("human/admin-tombstone-1", ActorKind::HumanUser, None);

        let member = lifecycle_service
            .hire_global_member(
                HireGlobalMemberCommand {
                    display_name: "Member Tombstone One".to_string(),
                    main_role_id: RoleId::new("role.member.operator"),
                    secondary_role_ids: Vec::new(),
                },
                actor.clone(),
                CommandMetadata::new(
                    "hire-tombstone-001",
                    "trace-hire-tombstone-001",
                    "hash-hire-tombstone-001",
                ),
            )
            .await
            .expect("hire member");
        memory_refs_service
            .update_memory_refs(
                UpdateMemoryRefsCommand {
                    global_member_id: member.global_member_id.clone(),
                    semantic_memory_ref: Some(MemoryRef {
                        memory_id: "memory-sem-001".to_string(),
                        memory_kind: "semantic".to_string(),
                        memory_version: Some("m1".to_string()),
                    }),
                    episodic_memory_refs: Vec::new(),
                },
                actor.clone(),
                CommandMetadata::new(
                    "memory-tombstone-001",
                    "trace-memory-tombstone-001",
                    "hash-memory-tombstone-001",
                ),
            )
            .await
            .expect("seed memory refs");

        let summary = service
            .tombstone_member(
                TombstoneMemberCommand {
                    global_member_id: member.global_member_id.clone(),
                    reason: "privacy erase request".to_string(),
                    expected_version: None,
                    gate_decision_ref: Some(approved_gate_decision("gate-001")),
                },
                actor,
                CommandMetadata::new("tombstone-001", "trace-tombstone-001", "hash-tombstone-001"),
            )
            .await
            .expect("tombstone should succeed");

        let member_row = sqlx::query(
            "SELECT lifecycle, version FROM global_members WHERE global_member_id = $1",
        )
        .bind(summary.global_member_id.as_str())
        .fetch_one(&pool)
        .await
        .expect("load member row");
        let history_row = sqlx::query(
            r#"
            SELECT event_type, from_lifecycle, to_lifecycle, gate_decision_ref_json
            FROM lifecycle_history_entries
            WHERE history_entry_id = $1
            "#,
        )
        .bind("history:tombstone-001")
        .fetch_one(&pool)
        .await
        .expect("load lifecycle history row");
        let audit_row =
            sqlx::query("SELECT action, reason FROM audit_trace_entries WHERE trace_id = $1")
                .bind("trace-tombstone-001")
                .fetch_one(&pool)
                .await
                .expect("load audit row");
        let outbox_row = sqlx::query(
            "SELECT event_type, payload_json FROM outbox_events WHERE outbox_event_id = $1",
        )
        .bind("outbox:tombstone-001")
        .fetch_one(&pool)
        .await
        .expect("load outbox row");
        let memory_refs_row = sqlx::query(
            "SELECT archive_status, archive_ref_json FROM memory_refs WHERE global_member_id = $1",
        )
        .bind(summary.global_member_id.as_str())
        .fetch_one(&pool)
        .await
        .expect("load memory refs row");

        assert_eq!(summary.lifecycle, GlobalMemberLifecycle::Tombstoned);
        assert_eq!(member_row.get::<String, _>("lifecycle"), "tombstoned");
        assert_eq!(history_row.get::<String, _>("event_type"), "tombstoned");
        assert_eq!(
            history_row.get::<Option<String>, _>("from_lifecycle"),
            Some("hired".to_string())
        );
        assert_eq!(history_row.get::<String, _>("to_lifecycle"), "tombstoned");
        assert_eq!(
            history_row.get::<serde_json::Value, _>("gate_decision_ref_json")["decision"],
            json!("approved")
        );
        assert_eq!(audit_row.get::<String, _>("action"), "TombstoneMember");
        assert_eq!(
            audit_row.get::<Option<String>, _>("reason"),
            Some("privacy erase request".to_string())
        );
        assert_eq!(
            outbox_row.get::<String, _>("event_type"),
            "identity.member.tombstoned"
        );
        assert_eq!(
            outbox_row.get::<serde_json::Value, _>("payload_json")["gate_decision_ref"]["decision"],
            json!("approved")
        );
        assert_eq!(
            memory_refs_row.get::<String, _>("archive_status"),
            "pending"
        );
        assert_eq!(
            memory_refs_row.get::<serde_json::Value, _>("archive_ref_json")["archive_id"],
            json!("archive-001")
        );
        assert_eq!(governance.call_count(), 1);
        assert_eq!(archive_requester.call_count(), 1);
    }

    #[tokio::test]
    async fn tombstone_member_rejects_rejected_gate_without_persisting_changes() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator").await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let lifecycle_service = MemberLifecycleCommandService::new(factory.clone());
        let governance = StubGovernancePort::new(rejected_gate_decision("gate-002"));
        let archive_requester = StubArchiveRequester::succeeding(ArchiveRef {
            archive_id: "archive-002".to_string(),
            archive_kind: "member-memory".to_string(),
            archive_version: None,
        });
        let service =
            TombstoneFlowService::new(factory.clone(), governance, archive_requester.clone());
        let actor = ActorContext::new("human/admin-tombstone-2", ActorKind::HumanUser, None);

        let member = lifecycle_service
            .hire_global_member(
                HireGlobalMemberCommand {
                    display_name: "Member Tombstone Two".to_string(),
                    main_role_id: RoleId::new("role.member.operator"),
                    secondary_role_ids: Vec::new(),
                },
                actor.clone(),
                CommandMetadata::new(
                    "hire-tombstone-002",
                    "trace-hire-tombstone-002",
                    "hash-hire-tombstone-002",
                ),
            )
            .await
            .expect("hire member");

        let error = service
            .tombstone_member(
                TombstoneMemberCommand {
                    global_member_id: member.global_member_id.clone(),
                    reason: "blocked by governance".to_string(),
                    expected_version: None,
                    gate_decision_ref: None,
                },
                actor,
                CommandMetadata::new("tombstone-002", "trace-tombstone-002", "hash-tombstone-002"),
            )
            .await
            .expect_err("rejected gate should fail");

        let member_row = sqlx::query(
            "SELECT lifecycle, version FROM global_members WHERE global_member_id = $1",
        )
        .bind(member.global_member_id.as_str())
        .fetch_one(&pool)
        .await
        .expect("load unchanged member row");
        let tombstone_history_count: i64 = sqlx::query(
            "SELECT COUNT(*) AS count FROM lifecycle_history_entries WHERE history_entry_id = $1",
        )
        .bind("history:tombstone-002")
        .fetch_one(&pool)
        .await
        .expect("count tombstone history rows")
        .get("count");
        let tombstone_outbox_count: i64 =
            sqlx::query("SELECT COUNT(*) AS count FROM outbox_events WHERE outbox_event_id = $1")
                .bind("outbox:tombstone-002")
                .fetch_one(&pool)
                .await
                .expect("count tombstone outbox rows")
                .get("count");

        assert!(matches!(
            error,
            IdentityError::RuleViolation {
                code: "IDENTITY_GATE_REJECTED",
                ..
            }
        ));
        assert_eq!(member_row.get::<String, _>("lifecycle"), "hired");
        assert_eq!(member_row.get::<i64, _>("version"), 0);
        assert_eq!(tombstone_history_count, 0);
        assert_eq!(tombstone_outbox_count, 0);
        assert_eq!(archive_requester.call_count(), 0);
    }

    #[tokio::test]
    async fn tombstone_member_rejects_archive_unavailable_without_tombstoning_member() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator").await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let lifecycle_service = MemberLifecycleCommandService::new(factory.clone());
        let governance = StubGovernancePort::new(approved_gate_decision("gate-003"));
        let archive_requester = StubArchiveRequester::failing("archive backend unavailable");
        let service =
            TombstoneFlowService::new(factory.clone(), governance, archive_requester.clone());
        let actor = ActorContext::new("human/admin-tombstone-3", ActorKind::HumanUser, None);

        let member = lifecycle_service
            .hire_global_member(
                HireGlobalMemberCommand {
                    display_name: "Member Tombstone Three".to_string(),
                    main_role_id: RoleId::new("role.member.operator"),
                    secondary_role_ids: Vec::new(),
                },
                actor.clone(),
                CommandMetadata::new(
                    "hire-tombstone-003",
                    "trace-hire-tombstone-003",
                    "hash-hire-tombstone-003",
                ),
            )
            .await
            .expect("hire member");

        let error = service
            .tombstone_member(
                TombstoneMemberCommand {
                    global_member_id: member.global_member_id.clone(),
                    reason: "archive dependency missing".to_string(),
                    expected_version: None,
                    gate_decision_ref: None,
                },
                actor,
                CommandMetadata::new("tombstone-003", "trace-tombstone-003", "hash-tombstone-003"),
            )
            .await
            .expect_err("archive failure should abort tombstone");

        let member_row = sqlx::query(
            "SELECT lifecycle, version FROM global_members WHERE global_member_id = $1",
        )
        .bind(member.global_member_id.as_str())
        .fetch_one(&pool)
        .await
        .expect("load unchanged member row");

        assert!(matches!(
            error,
            IdentityError::RuleViolation {
                code: "IDENTITY_MEMORY_ARCHIVE_UNAVAILABLE",
                ..
            }
        ));
        assert_eq!(member_row.get::<String, _>("lifecycle"), "hired");
        assert_eq!(member_row.get::<i64, _>("version"), 0);
        assert_eq!(archive_requester.call_count(), 1);
    }

    #[tokio::test]
    async fn handle_gate_decision_event_records_pending_flow_without_tombstoning_member() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator").await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let lifecycle_service = MemberLifecycleCommandService::new(factory.clone());
        let service = TombstoneFlowService::new(
            factory.clone(),
            StubGovernancePort::new(approved_gate_decision("gate-unused")),
            StubArchiveRequester::succeeding(ArchiveRef {
                archive_id: "archive-unused".to_string(),
                archive_kind: "member-memory".to_string(),
                archive_version: None,
            }),
        );
        let consumer = GateDecisionConsumer::new(service);
        let actor = ActorContext::new("human/admin-tombstone-4", ActorKind::HumanUser, None);

        let member = lifecycle_service
            .hire_global_member(
                HireGlobalMemberCommand {
                    display_name: "Member Tombstone Four".to_string(),
                    main_role_id: RoleId::new("role.member.operator"),
                    secondary_role_ids: Vec::new(),
                },
                actor.clone(),
                CommandMetadata::new(
                    "hire-tombstone-004",
                    "trace-hire-tombstone-004",
                    "hash-hire-tombstone-004",
                ),
            )
            .await
            .expect("hire member");

        let mut uow = factory.begin().await.expect("begin unit of work");
        let flow = PendingTombstoneFlow::open_for_tombstone(
            PendingFlowId::new("pending-flow-001"),
            member.global_member_id.clone(),
            actor,
            "await governance".to_string(),
            Some(GateDecisionId::new("gate-004")),
            now(),
        )
        .expect("open pending flow");
        uow.pending_tombstone_flows()
            .insert(&flow)
            .await
            .expect("insert pending flow");
        uow.commit().await.expect("commit pending flow");

        let outcome = consumer
            .consume(sample_gate_event(
                "gate-event-001",
                "gate-hash-001",
                "gate-004",
                "approved",
            ))
            .await
            .expect("gate event should succeed");

        let flow_row = sqlx::query(
            r#"
            SELECT status, gate_decision_ref_json
            FROM pending_tombstone_flows
            WHERE pending_flow_id = $1
            "#,
        )
        .bind("pending-flow-001")
        .fetch_one(&pool)
        .await
        .expect("load pending flow row");
        let member_row =
            sqlx::query("SELECT lifecycle FROM global_members WHERE global_member_id = $1")
                .bind(member.global_member_id.as_str())
                .fetch_one(&pool)
                .await
                .expect("load member row");
        let outbox_row = sqlx::query(
            "SELECT event_type, payload_json FROM outbox_events WHERE outbox_event_id = $1",
        )
        .bind("outbox:gate-event-001")
        .fetch_one(&pool)
        .await
        .expect("load gate outbox row");

        assert_eq!(
            outcome,
            GateDecisionOutcome::Recorded {
                pending_flow_id: "pending-flow-001".to_string(),
            }
        );
        assert_eq!(flow_row.get::<String, _>("status"), "gate_recorded");
        assert_eq!(
            flow_row.get::<serde_json::Value, _>("gate_decision_ref_json")["decision"],
            json!("approved")
        );
        assert_eq!(member_row.get::<String, _>("lifecycle"), "hired");
        assert_eq!(
            outbox_row.get::<String, _>("event_type"),
            "identity.gate_decision.recorded"
        );
        assert_eq!(
            outbox_row.get::<serde_json::Value, _>("payload_json")["status"],
            json!("gate_recorded")
        );
    }

    #[tokio::test]
    async fn tombstoned_outbox_event_can_rebuild_projection_without_wiping_memory_summary() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator").await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let lifecycle_service = MemberLifecycleCommandService::new(factory.clone());
        let memory_refs_service =
            MemoryRefsCommandService::new(factory.clone(), StubMemoryArchiveValidator);
        let governance = StubGovernancePort::new(approved_gate_decision("gate-005"));
        let archive_requester = StubArchiveRequester::succeeding(ArchiveRef {
            archive_id: "archive-005".to_string(),
            archive_kind: "member-memory".to_string(),
            archive_version: Some("v1".to_string()),
        });
        let service = TombstoneFlowService::new(factory.clone(), governance, archive_requester);
        let actor = ActorContext::new("human/admin-tombstone-5", ActorKind::HumanUser, None);

        let member = lifecycle_service
            .hire_global_member(
                HireGlobalMemberCommand {
                    display_name: "Member Tombstone Five".to_string(),
                    main_role_id: RoleId::new("role.member.operator"),
                    secondary_role_ids: Vec::new(),
                },
                actor.clone(),
                CommandMetadata::new(
                    "hire-tombstone-005",
                    "trace-hire-tombstone-005",
                    "hash-hire-tombstone-005",
                ),
            )
            .await
            .expect("hire member");
        memory_refs_service
            .update_memory_refs(
                UpdateMemoryRefsCommand {
                    global_member_id: member.global_member_id.clone(),
                    semantic_memory_ref: Some(MemoryRef {
                        memory_id: "memory-sem-005".to_string(),
                        memory_kind: "semantic".to_string(),
                        memory_version: Some("m5".to_string()),
                    }),
                    episodic_memory_refs: Vec::new(),
                },
                actor.clone(),
                CommandMetadata::new(
                    "memory-tombstone-005",
                    "trace-memory-tombstone-005",
                    "hash-memory-tombstone-005",
                ),
            )
            .await
            .expect("seed memory refs");

        let rebuild_job = ProjectionRebuildJob::new(factory.clone());
        rebuild_job
            .rebuild_member_summary_projection("member-summary", 50)
            .await
            .expect("rebuild initial projection");

        service
            .tombstone_member(
                TombstoneMemberCommand {
                    global_member_id: member.global_member_id.clone(),
                    reason: "contract sunset".to_string(),
                    expected_version: None,
                    gate_decision_ref: None,
                },
                actor,
                CommandMetadata::new("tombstone-005", "trace-tombstone-005", "hash-tombstone-005"),
            )
            .await
            .expect("tombstone should succeed");

        rebuild_job
            .rebuild_member_summary_projection("member-summary", 50)
            .await
            .expect("rebuild projection after tombstone");

        let projection_row = sqlx::query(
            r#"
            SELECT lifecycle, memory_ref_summary_json
            FROM member_summary_projection
            WHERE global_member_id = $1
            "#,
        )
        .bind(member.global_member_id.as_str())
        .fetch_one(&pool)
        .await
        .expect("load projection row");

        assert_eq!(projection_row.get::<String, _>("lifecycle"), "tombstoned");
        assert_eq!(
            projection_row.get::<serde_json::Value, _>("memory_ref_summary_json")["archive_status"],
            json!("pending")
        );
        assert_eq!(
            projection_row.get::<serde_json::Value, _>("memory_ref_summary_json")["archive_ref"]["archive_id"],
            json!("archive-005")
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

    fn approved_gate_decision(gate_decision_id: &str) -> GateDecisionRef {
        GateDecisionRef {
            gate_decision_id: GateDecisionId::new(gate_decision_id),
            decision: GateDecision::Approved,
            policy_ref_json: json!({
                "policy_id": "policy.high-risk.tombstone",
                "policy_version": "2026.05",
            }),
            decided_at: now(),
        }
    }

    fn rejected_gate_decision(gate_decision_id: &str) -> GateDecisionRef {
        GateDecisionRef {
            gate_decision_id: GateDecisionId::new(gate_decision_id),
            decision: GateDecision::Rejected,
            policy_ref_json: json!({
                "policy_id": "policy.high-risk.tombstone",
                "policy_version": "2026.05",
            }),
            decided_at: now(),
        }
    }

    fn sample_gate_event(
        source_event_id: &str,
        payload_hash: &str,
        gate_decision_id: &str,
        decision: &str,
    ) -> InboundGateDecisionEvent {
        InboundGateDecisionEvent {
            envelope: InboundEventEnvelope {
                source_event_id: source_event_id.into(),
                source_module: "governance".to_string(),
                event_type: "gate_decision_recorded".to_string(),
                occurred_at: now(),
                payload_hash: payload_hash.to_string(),
                payload: json!({
                    "gate_decision_id": gate_decision_id,
                    "decision": decision,
                    "policy_ref": {
                        "policy_id": "policy.high-risk.tombstone",
                        "policy_version": "2026.05",
                    },
                    "decided_at": now(),
                }),
            },
        }
    }

    fn now() -> PrimitiveDateTime {
        let now = OffsetDateTime::now_utc();
        PrimitiveDateTime::new(now.date(), now.time())
    }
}
