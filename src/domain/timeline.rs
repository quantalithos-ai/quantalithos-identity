//! Append-only lifecycle history records written alongside member state changes.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::PrimitiveDateTime;

use crate::domain::member::GlobalMemberLifecycle;
use crate::domain::shared::context::ActorContext;
use crate::domain::shared::ids::GlobalMemberId;
use crate::domain::shared::metadata::CommandMetadata;

/// Enumerates the lifecycle history event kinds supported by the initial identity write model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LifecycleEventType {
    /// The member was created through an explicit hire command.
    Created,
    /// The member transitioned between non-tombstone lifecycle states.
    LifecycleChanged,
    /// The member entered the tombstoned terminal lifecycle.
    Tombstoned,
}

impl LifecycleEventType {
    /// Parses the persisted database string into the strongly-typed enum.
    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "created" => Some(Self::Created),
            "lifecycle_changed" => Some(Self::LifecycleChanged),
            "tombstoned" => Some(Self::Tombstoned),
            _ => None,
        }
    }

    /// Returns the canonical persisted string for the event type.
    pub fn as_db(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::LifecycleChanged => "lifecycle_changed",
            Self::Tombstoned => "tombstoned",
        }
    }
}

/// Represents a single append-only lifecycle history entry for a global member.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LifecycleHistoryEntry {
    /// Stable history row identifier.
    pub history_entry_id: String,
    /// Global member referenced by this lifecycle change.
    pub global_member_id: GlobalMemberId,
    /// Lifecycle event kind persisted in the append-only history table.
    pub event_type: LifecycleEventType,
    /// Lifecycle state before the change, when applicable.
    pub from_lifecycle: Option<GlobalMemberLifecycle>,
    /// Lifecycle state after the change.
    pub to_lifecycle: GlobalMemberLifecycle,
    /// Trusted actor snapshot responsible for the lifecycle transition.
    pub actor: ActorContext,
    /// Optional governance gate decision reference kept as ref-only JSON.
    pub gate_decision_ref_json: Option<Value>,
    /// Request metadata snapshot captured alongside the history row.
    pub metadata: CommandMetadata,
    /// Timestamp when the history row was created.
    pub created_at: PrimitiveDateTime,
}
