//! Outbox records used to persist durable event publication state.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use time::{Duration, PrimitiveDateTime};

use crate::domain::capability_profile::CapabilityProfile;
use crate::domain::career_history::CareerHistory;
use crate::domain::member::GlobalMember;
use crate::domain::memory_refs::MemoryRefs;
use crate::domain::role_catalog::RoleCatalogEntry;
use crate::domain::shared::ids::OutboxEventId;
use crate::domain::tombstone::{GateDecisionRef, PendingTombstoneFlow};

/// Enumerates the lifecycle states allowed for persisted outbox events.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OutboxStatus {
    /// Event is pending publication by an operations worker.
    Pending,
    /// Event was published successfully to the external bus.
    Published,
    /// Event publication failed and may be retried later.
    Failed,
    /// Event exhausted automatic retries and now requires manual intervention.
    Dead,
}

impl OutboxStatus {
    /// Parses the persisted database string into the strongly-typed outbox status enum.
    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "published" => Some(Self::Published),
            "failed" => Some(Self::Failed),
            "dead" => Some(Self::Dead),
            _ => None,
        }
    }

    /// Returns the canonical persisted string for the outbox status enum.
    pub fn as_db(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Published => "published",
            Self::Failed => "failed",
            Self::Dead => "dead",
        }
    }
}

const MAX_AUTOMATIC_RETRY_COUNT: i32 = 5;

/// Represents a durable outbox record stored in `outbox_events`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OutboxEvent {
    /// Stable outbox row id used as both storage primary key and replay cursor.
    pub outbox_event_id: OutboxEventId,
    /// Aggregate type retained for downstream routing and diagnostics.
    pub aggregate_type: String,
    /// Aggregate id associated with the event payload.
    pub aggregate_id: String,
    /// Event type published to the external message bus.
    pub event_type: String,
    /// Canonical event payload persisted inside the local transaction.
    pub payload_json: Value,
    /// Publish-side idempotency key unique across all outbox rows.
    pub idempotency_key: String,
    /// Current durable publication status for the outbox row.
    pub status: OutboxStatus,
    /// Number of publish attempts already performed for this outbox row.
    pub retry_count: i32,
    /// Earliest timestamp when the row may be retried again.
    pub next_retry_at: Option<PrimitiveDateTime>,
    /// Timestamp when the outbox row was created.
    pub created_at: PrimitiveDateTime,
    /// Timestamp when publication succeeded, when applicable.
    pub published_at: Option<PrimitiveDateTime>,
    /// Most recent failure reason captured by the publisher workflow.
    pub failure_reason: Option<String>,
}

impl OutboxEvent {
    /// Marks the outbox event as published after a successful bus handoff.
    pub fn mark_published(&mut self, published_at: PrimitiveDateTime) {
        self.status = OutboxStatus::Published;
        self.published_at = Some(published_at);
        self.next_retry_at = None;
        self.failure_reason = None;
    }

    /// Marks the outbox event as failed and schedules the next retry attempt.
    pub fn mark_failed(&mut self, failure_reason: impl Into<String>, failed_at: PrimitiveDateTime) {
        self.status = OutboxStatus::Failed;
        self.retry_count += 1;
        self.failure_reason = Some(failure_reason.into());
        self.published_at = None;
        if self.retry_count >= MAX_AUTOMATIC_RETRY_COUNT {
            self.status = OutboxStatus::Dead;
            self.next_retry_at = None;
        } else {
            self.next_retry_at = Some(self.next_retry_at(failed_at));
        }
    }

    /// Computes the next retry timestamp using a simple bounded linear backoff.
    pub fn next_retry_at(&self, failed_at: PrimitiveDateTime) -> PrimitiveDateTime {
        let retry_count = i64::from(self.retry_count.max(1));
        let retry_delay_seconds = (retry_count * 30).min(300);
        failed_at + Duration::seconds(retry_delay_seconds)
    }

    /// Creates the outbox record produced by a successful hire command.
    pub fn for_member_hired(
        outbox_event_id: OutboxEventId,
        member: &GlobalMember,
        idempotency_key: &str,
        created_at: PrimitiveDateTime,
    ) -> Self {
        Self {
            outbox_event_id,
            aggregate_type: "global_member".to_string(),
            aggregate_id: member.global_member_id.as_str().to_string(),
            event_type: "identity.member.created".to_string(),
            payload_json: json!({
                "global_member_id": member.global_member_id.as_str(),
                "display_name": member.display_name,
                "lifecycle": member.lifecycle.as_db(),
                "main_role_id": member.main_role_id.as_str(),
                "secondary_role_ids": member.secondary_role_ids.iter().map(|value| value.as_str()).collect::<Vec<_>>(),
                "capability_profile_id": member.capability_profile_id.as_ref().map(|value| value.as_str()),
                "memory_refs_id": member.memory_refs_id.as_ref().map(|value| value.as_str()),
                "version": member.version,
                "created_at": member.created_at,
                "updated_at": member.updated_at,
            }),
            idempotency_key: idempotency_key.to_string(),
            status: OutboxStatus::Pending,
            retry_count: 0,
            next_retry_at: None,
            created_at,
            published_at: None,
            failure_reason: None,
        }
    }

    /// Creates the outbox record produced by a successful lifecycle-change command.
    pub fn for_member_lifecycle_changed(
        outbox_event_id: OutboxEventId,
        member: &GlobalMember,
        from_lifecycle: &str,
        reason: &str,
        idempotency_key: &str,
        created_at: PrimitiveDateTime,
    ) -> Self {
        Self {
            outbox_event_id,
            aggregate_type: "global_member".to_string(),
            aggregate_id: member.global_member_id.as_str().to_string(),
            event_type: "identity.member.lifecycle_changed".to_string(),
            payload_json: json!({
                "global_member_id": member.global_member_id.as_str(),
                "display_name": member.display_name,
                "lifecycle": member.lifecycle.as_db(),
                "from_lifecycle": from_lifecycle,
                "reason": reason,
                "main_role_id": member.main_role_id.as_str(),
                "secondary_role_ids": member.secondary_role_ids.iter().map(|value| value.as_str()).collect::<Vec<_>>(),
                "capability_profile_id": member.capability_profile_id.as_ref().map(|value| value.as_str()),
                "memory_refs_id": member.memory_refs_id.as_ref().map(|value| value.as_str()),
                "version": member.version,
                "created_at": member.created_at,
                "updated_at": member.updated_at,
            }),
            idempotency_key: idempotency_key.to_string(),
            status: OutboxStatus::Pending,
            retry_count: 0,
            next_retry_at: None,
            created_at,
            published_at: None,
            failure_reason: None,
        }
    }

    /// Creates the outbox record produced by a successful tombstone command.
    pub fn for_member_tombstoned(
        outbox_event_id: OutboxEventId,
        member: &GlobalMember,
        memory_ref_summary_json: Value,
        gate_decision_ref: &GateDecisionRef,
        reason: &str,
        idempotency_key: &str,
        created_at: PrimitiveDateTime,
    ) -> Self {
        Self {
            outbox_event_id,
            aggregate_type: "global_member".to_string(),
            aggregate_id: member.global_member_id.as_str().to_string(),
            event_type: "identity.member.tombstoned".to_string(),
            payload_json: json!({
                "global_member_id": member.global_member_id.as_str(),
                "display_name": member.display_name,
                "lifecycle": member.lifecycle.as_db(),
                "reason": reason,
                "main_role_id": member.main_role_id.as_str(),
                "secondary_role_ids": member.secondary_role_ids.iter().map(|value| value.as_str()).collect::<Vec<_>>(),
                "capability_profile_id": member.capability_profile_id.as_ref().map(|value| value.as_str()),
                "memory_refs_id": member.memory_refs_id.as_ref().map(|value| value.as_str()),
                "memory_ref_summary_json": memory_ref_summary_json,
                "gate_decision_ref": gate_decision_ref,
                "version": member.version,
                "created_at": member.created_at,
                "updated_at": member.updated_at,
            }),
            idempotency_key: idempotency_key.to_string(),
            status: OutboxStatus::Pending,
            retry_count: 0,
            next_retry_at: None,
            created_at,
            published_at: None,
            failure_reason: None,
        }
    }

    /// Creates the outbox record produced by a successful capability-profile update command.
    pub fn for_capability_profile_updated(
        outbox_event_id: OutboxEventId,
        member: &GlobalMember,
        profile: &CapabilityProfile,
        idempotency_key: &str,
        created_at: PrimitiveDateTime,
    ) -> Self {
        Self {
            outbox_event_id,
            aggregate_type: "capability_profile".to_string(),
            aggregate_id: profile.capability_profile_id.as_str().to_string(),
            event_type: "identity.capability_profile.updated".to_string(),
            payload_json: json!({
                "capability_profile_id": profile.capability_profile_id.as_str(),
                "global_member_id": profile.global_member_id.as_str(),
                "display_name": member.display_name,
                "lifecycle": member.lifecycle.as_db(),
                "main_role_id": member.main_role_id.as_str(),
                "capability_summary_json": profile.summary_json(),
                "version": profile.version,
                "updated_at": profile.updated_at,
            }),
            idempotency_key: idempotency_key.to_string(),
            status: OutboxStatus::Pending,
            retry_count: 0,
            next_retry_at: None,
            created_at,
            published_at: None,
            failure_reason: None,
        }
    }

    /// Creates the outbox record produced by a successful memory refs update command.
    pub fn for_memory_refs_updated(
        outbox_event_id: OutboxEventId,
        member: &GlobalMember,
        memory_refs: &MemoryRefs,
        idempotency_key: &str,
        created_at: PrimitiveDateTime,
    ) -> Self {
        Self {
            outbox_event_id,
            aggregate_type: "memory_refs".to_string(),
            aggregate_id: memory_refs.memory_refs_id.as_str().to_string(),
            event_type: "identity.memory_refs.updated".to_string(),
            payload_json: json!({
                "memory_refs_id": memory_refs.memory_refs_id.as_str(),
                "global_member_id": memory_refs.global_member_id.as_str(),
                "display_name": member.display_name,
                "lifecycle": member.lifecycle.as_db(),
                "main_role_id": member.main_role_id.as_str(),
                "memory_ref_summary_json": memory_refs.summary_json(),
                "version": memory_refs.version,
                "updated_at": memory_refs.updated_at,
            }),
            idempotency_key: idempotency_key.to_string(),
            status: OutboxStatus::Pending,
            retry_count: 0,
            next_retry_at: None,
            created_at,
            published_at: None,
            failure_reason: None,
        }
    }

    /// Creates the outbox record produced by a successful career-history append.
    pub fn for_career_history_appended(
        outbox_event_id: OutboxEventId,
        member: &GlobalMember,
        history: &CareerHistory,
        idempotency_key: &str,
        created_at: PrimitiveDateTime,
    ) -> Self {
        Self {
            outbox_event_id,
            aggregate_type: "career_history".to_string(),
            aggregate_id: member.global_member_id.as_str().to_string(),
            event_type: "identity.career_history.appended".to_string(),
            payload_json: json!({
                "global_member_id": member.global_member_id.as_str(),
                "display_name": member.display_name,
                "lifecycle": member.lifecycle.as_db(),
                "main_role_id": member.main_role_id.as_str(),
                "career_summary_json": history.summary_json(),
                "version": history.version(),
                "updated_at": history.latest_created_at().unwrap_or(created_at),
            }),
            idempotency_key: idempotency_key.to_string(),
            status: OutboxStatus::Pending,
            retry_count: 0,
            next_retry_at: None,
            created_at,
            published_at: None,
            failure_reason: None,
        }
    }

    /// Creates the outbox record produced by a successful role-catalog synchronization.
    pub fn for_role_catalog_sync(
        outbox_event_id: OutboxEventId,
        entry: &RoleCatalogEntry,
        idempotency_key: &str,
        created_at: PrimitiveDateTime,
    ) -> Self {
        Self {
            outbox_event_id,
            aggregate_type: "role_catalog_entry".to_string(),
            aggregate_id: entry.role_id.as_str().to_string(),
            event_type: "identity.role_catalog.synced".to_string(),
            payload_json: json!({
                "role_id": entry.role_id.as_str(),
                "role_name": entry.role_name,
                "role_version": entry.role_version,
                "source_ref": entry.source_ref_json,
                "fingerprint": entry.fingerprint,
                "status": entry.status.as_db(),
                "updated_at": entry.updated_at,
            }),
            idempotency_key: idempotency_key.to_string(),
            status: OutboxStatus::Pending,
            retry_count: 0,
            next_retry_at: None,
            created_at,
            published_at: None,
            failure_reason: None,
        }
    }

    /// Creates the outbox record produced after recording a governance decision for a pending flow.
    pub fn for_gate_decision_recorded(
        outbox_event_id: OutboxEventId,
        pending_flow: &PendingTombstoneFlow,
        idempotency_key: &str,
        created_at: PrimitiveDateTime,
    ) -> Self {
        Self {
            outbox_event_id,
            aggregate_type: "pending_tombstone_flow".to_string(),
            aggregate_id: pending_flow.pending_flow_id.as_str().to_string(),
            event_type: "identity.gate_decision.recorded".to_string(),
            payload_json: json!({
                "pending_flow_id": pending_flow.pending_flow_id.as_str(),
                "global_member_id": pending_flow.global_member_id.as_str(),
                "action_name": pending_flow.action_name,
                "expected_gate_decision_id": pending_flow.expected_gate_decision_id.as_ref().map(|value| value.as_str()),
                "gate_decision_ref": pending_flow.gate_decision_ref,
                "status": pending_flow.status.as_db(),
                "updated_at": pending_flow.updated_at,
            }),
            idempotency_key: idempotency_key.to_string(),
            status: OutboxStatus::Pending,
            retry_count: 0,
            next_retry_at: None,
            created_at,
            published_at: None,
            failure_reason: None,
        }
    }
}
