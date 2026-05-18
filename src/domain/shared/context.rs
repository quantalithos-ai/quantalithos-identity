//! Trusted caller context passed from the gateway into identity.

use serde::{Deserialize, Serialize};

use super::ids::GlobalMemberId;

/// Describes the kind of trusted actor making the current call.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ActorKind {
    /// A human operator such as an owner or administrator.
    HumanUser,
    /// A platform-level AI member represented by a global member record.
    AiMember,
    /// A system-triggered actor such as a scheduled job or replay worker.
    System,
}

/// Represents trusted caller context injected by the gateway or runtime boundary.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorContext {
    /// Stable actor reference for audit traces and created_by snapshots.
    pub actor_ref: String,
    /// Actor category used to drive authorization and audit decisions later.
    pub actor_kind: ActorKind,
    /// Optional global member identity when the actor is an AI member.
    pub global_member_id: Option<GlobalMemberId>,
}

impl ActorContext {
    /// Creates a trusted actor context from already-validated gateway values.
    pub fn new(
        actor_ref: impl Into<String>,
        actor_kind: ActorKind,
        global_member_id: Option<GlobalMemberId>,
    ) -> Self {
        Self {
            actor_ref: actor_ref.into(),
            actor_kind,
            global_member_id,
        }
    }

    /// Returns true when the current actor is a system actor.
    pub fn is_system_actor(&self) -> bool {
        matches!(self.actor_kind, ActorKind::System)
    }

    /// Returns the actor member id when the actor is an AI member.
    pub fn actor_member_id(&self) -> Option<&GlobalMemberId> {
        self.global_member_id.as_ref()
    }
}
