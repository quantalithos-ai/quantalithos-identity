//! Idempotency records and scope enums used by command and inbound-event deduplication.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::PrimitiveDateTime;

/// Enumerates the handling scopes supported by `idempotency_records`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdempotencyScope {
    /// Command-side deduplication for explicit write requests.
    Command,
    /// Inbound event-side deduplication for subscriber handling.
    InboundEvent,
    /// Publisher-side deduplication for outbox publication.
    OutboxPublish,
}

impl IdempotencyScope {
    /// Parses the persisted database string into a typed scope enum.
    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "command" => Some(Self::Command),
            "inbound_event" => Some(Self::InboundEvent),
            "outbox_publish" => Some(Self::OutboxPublish),
            _ => None,
        }
    }

    /// Returns the canonical persisted string for the scope enum.
    pub fn as_db(self) -> &'static str {
        match self {
            Self::Command => "command",
            Self::InboundEvent => "inbound_event",
            Self::OutboxPublish => "outbox_publish",
        }
    }
}

/// Enumerates the states supported by an idempotency record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdempotencyStatus {
    /// The flow is currently being processed and has not yet completed.
    Processing,
    /// The flow completed successfully and may return a previous result.
    Succeeded,
    /// The flow failed and retained failure state for diagnostics.
    Failed,
}

impl IdempotencyStatus {
    /// Parses the persisted database string into a typed status enum.
    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "processing" => Some(Self::Processing),
            "succeeded" => Some(Self::Succeeded),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }

    /// Returns the canonical persisted string for the status enum.
    pub fn as_db(self) -> &'static str {
        match self {
            Self::Processing => "processing",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }
}

/// Represents a durable idempotency record stored in `idempotency_records`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IdempotencyRecord {
    /// Stable idempotency key derived from a command key or inbound event id.
    pub idempotency_key: String,
    /// Scope that determines how the idempotency key is interpreted.
    pub scope: IdempotencyScope,
    /// Stable hash of the inbound request or event payload.
    pub request_hash: String,
    /// Optional weak reference to the previously computed result.
    pub result_ref_json: Option<Value>,
    /// Current handling status for the idempotency key.
    pub status: IdempotencyStatus,
    /// Timestamp when the record was first created.
    pub created_at: PrimitiveDateTime,
    /// Timestamp when the record last changed status or result.
    pub updated_at: PrimitiveDateTime,
}
