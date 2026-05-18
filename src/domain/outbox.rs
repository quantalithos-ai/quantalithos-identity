//! Outbox records used to persist durable event publication state.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::PrimitiveDateTime;

use crate::domain::shared::ids::OutboxEventId;

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
}

impl OutboxStatus {
    /// Parses the persisted database string into the strongly-typed outbox status enum.
    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "pending" => Some(Self::Pending),
            "published" => Some(Self::Published),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }

    /// Returns the canonical persisted string for the outbox status enum.
    pub fn as_db(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Published => "published",
            Self::Failed => "failed",
        }
    }
}

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
