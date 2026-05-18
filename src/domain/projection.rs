//! Query projection records and checkpoint rows used by rebuild operations.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::PrimitiveDateTime;

use crate::domain::member::GlobalMemberLifecycle;
use crate::domain::shared::ids::{GlobalMemberId, OutboxEventId, RoleId};

/// Represents the read-optimized member summary projection row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemberSummaryProjection {
    /// Projection primary key matching the underlying global member id.
    pub global_member_id: GlobalMemberId,
    /// User-facing member display name cached for query responses.
    pub display_name: String,
    /// Lifecycle snapshot derived from the write model or outbox event stream.
    pub lifecycle: GlobalMemberLifecycle,
    /// Optional main role id summary for list and detail queries.
    pub main_role_id: Option<RoleId>,
    /// Optional cached main role display name for queries.
    pub main_role_name: Option<String>,
    /// Projection-safe capability summary JSON that excludes external truth bodies.
    pub capability_summary_json: Value,
    /// Projection-safe career summary JSON derived from append-only history.
    pub career_summary_json: Value,
    /// Projection-safe memory summary JSON that excludes memory payload bodies.
    pub memory_ref_summary_json: Value,
    /// Rebuild-controlled projection version for idempotent replay and diagnostics.
    pub projection_version: i64,
    /// Timestamp when the projection row was last refreshed.
    pub updated_at: PrimitiveDateTime,
}

/// Enumerates the states allowed for projection checkpoint rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionCheckpointStatus {
    /// No rebuild worker currently owns or is failing this checkpoint.
    Idle,
    /// A rebuild worker is actively advancing the checkpoint.
    Running,
    /// The most recent rebuild attempt failed and requires recovery.
    Failed,
}

impl ProjectionCheckpointStatus {
    /// Parses the persisted database string into the typed checkpoint status enum.
    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "idle" => Some(Self::Idle),
            "running" => Some(Self::Running),
            "failed" => Some(Self::Failed),
            _ => None,
        }
    }

    /// Returns the canonical persisted string for the checkpoint status enum.
    pub fn as_db(self) -> &'static str {
        match self {
            Self::Idle => "idle",
            Self::Running => "running",
            Self::Failed => "failed",
        }
    }
}

/// Represents a durable rebuild checkpoint stored in `projection_checkpoints`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectionCheckpoint {
    /// Stable checkpoint name, usually the projection or job identifier.
    pub checkpoint_name: String,
    /// Most recent outbox event processed successfully by the rebuild workflow.
    pub last_processed_event_id: Option<OutboxEventId>,
    /// Current rebuild status for the checkpoint row.
    pub status: ProjectionCheckpointStatus,
    /// Optional failure reason for the most recent rebuild error.
    pub failure_reason: Option<String>,
    /// Timestamp when the checkpoint row was last updated.
    pub updated_at: PrimitiveDateTime,
}
