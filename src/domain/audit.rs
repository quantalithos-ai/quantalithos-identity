//! Audit trace records appended by command and event handling flows.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use time::PrimitiveDateTime;

use crate::domain::capability_profile::CapabilityProfile;
use crate::domain::member::GlobalMember;
use crate::domain::memory_refs::MemoryRefs;
use crate::domain::shared::context::ActorContext;

/// Enumerates the supported terminal results for an audit trace entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditResult {
    /// The command or event handling flow completed successfully.
    Success,
    /// The command or event handling flow failed.
    Failed,
    /// The command or event handling flow was intentionally skipped.
    Skipped,
}

impl AuditResult {
    /// Parses the persisted database value into the typed audit result enum.
    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "success" => Some(Self::Success),
            "failed" => Some(Self::Failed),
            "skipped" => Some(Self::Skipped),
            _ => None,
        }
    }

    /// Returns the canonical persisted string for the audit result enum.
    pub fn as_db(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
        }
    }
}

/// Represents a single append-only audit trace row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuditTraceEntry {
    /// Stable audit record identifier.
    pub audit_trace_id: String,
    /// Trace id used to correlate command or event handling across adapters.
    pub trace_id: String,
    /// Action name for the command or event handling flow.
    pub action: String,
    /// Optional trusted actor snapshot, absent for pure system-triggered events.
    pub actor_json: Option<Value>,
    /// Optional target reference snapshot retained as weak JSON ref.
    pub target_ref_json: Option<Value>,
    /// Optional source module name for inbound event or job initiated actions.
    pub source_module: Option<String>,
    /// Terminal result of the action.
    pub result: AuditResult,
    /// Optional reason recorded for failed or skipped actions.
    pub reason: Option<String>,
    /// Timestamp when the audit row was captured.
    pub created_at: PrimitiveDateTime,
}

impl AuditTraceEntry {
    /// Creates an audit trace row for a successful hire command.
    pub fn for_hire_command(
        audit_trace_id: impl Into<String>,
        member: &GlobalMember,
        actor: &ActorContext,
        trace_id: impl Into<String>,
        created_at: PrimitiveDateTime,
    ) -> Self {
        Self::for_member_command(
            audit_trace_id,
            "HireGlobalMember",
            member,
            actor,
            trace_id,
            created_at,
            None,
        )
    }

    /// Creates an audit trace row for a successful lifecycle-change command.
    pub fn for_lifecycle_command(
        audit_trace_id: impl Into<String>,
        member: &GlobalMember,
        actor: &ActorContext,
        trace_id: impl Into<String>,
        created_at: PrimitiveDateTime,
        reason: Option<String>,
    ) -> Self {
        Self::for_member_command(
            audit_trace_id,
            "UpdateLifecycle",
            member,
            actor,
            trace_id,
            created_at,
            reason,
        )
    }

    /// Creates an audit trace row for a successful capability-profile update command.
    pub fn for_capability_profile_command(
        audit_trace_id: impl Into<String>,
        profile: &CapabilityProfile,
        actor: &ActorContext,
        trace_id: impl Into<String>,
        created_at: PrimitiveDateTime,
    ) -> Self {
        Self {
            audit_trace_id: audit_trace_id.into(),
            trace_id: trace_id.into(),
            action: "UpdateCapabilityProfile".to_string(),
            actor_json: Some(json!(actor)),
            target_ref_json: Some(json!({
                "kind": "capability_profile",
                "id": profile.capability_profile_id.as_str(),
                "global_member_id": profile.global_member_id.as_str(),
            })),
            source_module: None,
            result: AuditResult::Success,
            reason: None,
            created_at,
        }
    }

    /// Creates an audit trace row for a successful memory refs update command.
    pub fn for_memory_refs_command(
        audit_trace_id: impl Into<String>,
        memory_refs: &MemoryRefs,
        actor: &ActorContext,
        trace_id: impl Into<String>,
        created_at: PrimitiveDateTime,
    ) -> Self {
        Self {
            audit_trace_id: audit_trace_id.into(),
            trace_id: trace_id.into(),
            action: "UpdateMemoryRefs".to_string(),
            actor_json: Some(json!(actor)),
            target_ref_json: Some(json!({
                "kind": "memory_refs",
                "id": memory_refs.memory_refs_id.as_str(),
                "global_member_id": memory_refs.global_member_id.as_str(),
            })),
            source_module: None,
            result: AuditResult::Success,
            reason: None,
            created_at,
        }
    }

    fn for_member_command(
        audit_trace_id: impl Into<String>,
        action: impl Into<String>,
        member: &GlobalMember,
        actor: &ActorContext,
        trace_id: impl Into<String>,
        created_at: PrimitiveDateTime,
        reason: Option<String>,
    ) -> Self {
        Self {
            audit_trace_id: audit_trace_id.into(),
            trace_id: trace_id.into(),
            action: action.into(),
            actor_json: Some(json!(actor)),
            target_ref_json: Some(json!({
                "kind": "global_member",
                "id": member.global_member_id.as_str(),
            })),
            source_module: None,
            result: AuditResult::Success,
            reason,
            created_at,
        }
    }

    /// Creates an audit trace row for a handled inbound event.
    pub fn for_inbound_event(
        audit_trace_id: impl Into<String>,
        action: impl Into<String>,
        source_module: impl Into<String>,
        trace_id: impl Into<String>,
        target_ref_json: Option<Value>,
        result: AuditResult,
        reason: Option<String>,
        created_at: PrimitiveDateTime,
    ) -> Self {
        Self {
            audit_trace_id: audit_trace_id.into(),
            trace_id: trace_id.into(),
            action: action.into(),
            actor_json: Some(json!({
                "actor_ref": "system/inbound-event",
                "actor_kind": "system",
                "global_member_id": null,
            })),
            target_ref_json,
            source_module: Some(source_module.into()),
            result,
            reason,
            created_at,
        }
    }
}
