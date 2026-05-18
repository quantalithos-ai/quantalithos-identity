//! Global member write-model aggregate used by repository and command layers.

use serde::{Deserialize, Serialize};
use time::PrimitiveDateTime;

use crate::domain::shared::context::ActorContext;
use crate::domain::shared::ids::{CapabilityProfileId, GlobalMemberId, MemoryRefsId, RoleId};

/// Enumerates the only lifecycle states allowed for a global member write model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GlobalMemberLifecycle {
    /// Initial lifecycle immediately after explicit member creation.
    Hired,
    /// Member is available for normal platform collaboration.
    Active,
    /// Member is temporarily paused from new collaboration.
    Paused,
    /// Member is retired from standard collaboration but still retained as truth.
    Retired,
    /// Member has entered the irreversible tombstoned terminal state.
    Tombstoned,
}

impl GlobalMemberLifecycle {
    /// Parses a persisted lifecycle string into the strongly-typed lifecycle enum.
    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "hired" => Some(Self::Hired),
            "active" => Some(Self::Active),
            "paused" => Some(Self::Paused),
            "retired" => Some(Self::Retired),
            "tombstoned" => Some(Self::Tombstoned),
            _ => None,
        }
    }

    /// Returns the canonical database representation of the lifecycle enum.
    pub fn as_db(self) -> &'static str {
        match self {
            Self::Hired => "hired",
            Self::Active => "active",
            Self::Paused => "paused",
            Self::Retired => "retired",
            Self::Tombstoned => "tombstoned",
        }
    }
}

/// Represents the platform-level member identity truth persisted in `global_members`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GlobalMember {
    /// Stable global member identifier referenced by downstream modules.
    pub global_member_id: GlobalMemberId,
    /// User-facing display name kept in the identity write model.
    pub display_name: String,
    /// Current lifecycle state governed by the domain state machine.
    pub lifecycle: GlobalMemberLifecycle,
    /// Main role reference that must point at a valid role catalog entry.
    pub main_role_id: RoleId,
    /// Secondary role references stored as an ordered JSON array.
    pub secondary_role_ids: Vec<RoleId>,
    /// Optional capability profile reference linked after profile creation.
    pub capability_profile_id: Option<CapabilityProfileId>,
    /// Optional memory refs aggregate reference linked after memory setup.
    pub memory_refs_id: Option<MemoryRefsId>,
    /// Optimistic-lock version incremented on every successful save.
    pub version: i64,
    /// Trusted actor snapshot that originally created the member.
    pub created_by: ActorContext,
    /// Creation timestamp written once when the member is first created.
    pub created_at: PrimitiveDateTime,
    /// Last update timestamp refreshed whenever the write model changes.
    pub updated_at: PrimitiveDateTime,
}
