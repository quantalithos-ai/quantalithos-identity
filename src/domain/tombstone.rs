//! Tombstone and governance evidence records used by high-risk identity flows.

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use time::PrimitiveDateTime;

use crate::domain::shared::context::ActorContext;
use crate::domain::shared::ids::{GateDecisionId, GlobalMemberId, PendingFlowId};
use crate::error::IdentityError;

/// Enumerates the governance decision outcomes retained by identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateDecision {
    /// Governance explicitly approved the protected action.
    Approved,
    /// Governance explicitly rejected the protected action.
    Rejected,
    /// Governance approval window expired before the action completed.
    Expired,
}

impl GateDecision {
    /// Returns the canonical persisted representation of the decision.
    pub fn as_db(self) -> &'static str {
        match self {
            Self::Approved => "approved",
            Self::Rejected => "rejected",
            Self::Expired => "expired",
        }
    }
}

/// Ref-only governance evidence retained for high-risk identity actions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GateDecisionRef {
    /// Stable governance decision id.
    pub gate_decision_id: GateDecisionId,
    /// Governance decision outcome summary.
    pub decision: GateDecision,
    /// Ref-only policy pointer emitted by governance.
    pub policy_ref_json: Value,
    /// Timestamp when governance recorded the decision.
    pub decided_at: PrimitiveDateTime,
}

impl GateDecisionRef {
    /// Validates that the governance evidence contains the minimum required fields.
    pub fn validate(&self) -> Result<(), IdentityError> {
        if self.gate_decision_id.as_str().trim().is_empty() {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_INVALID_ARGUMENT",
                message: "gate_decision_id must not be blank".to_string(),
            });
        }
        if self.policy_ref_json.is_null() {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_INVALID_ARGUMENT",
                message: "policy_ref must not be null".to_string(),
            });
        }

        Ok(())
    }

    /// Returns true when the referenced governance decision approved the action.
    pub fn is_approved(&self) -> bool {
        self.decision == GateDecision::Approved
    }
}

/// Enumerates the states retained for a pending tombstone flow record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PendingTombstoneFlowStatus {
    /// The flow is waiting for governance gate evidence to arrive.
    WaitingGate,
    /// Governance gate evidence has been recorded for the flow.
    GateRecorded,
    /// The flow has been fully consumed by the primary tombstone path.
    Completed,
    /// The flow was cancelled and should not be resumed automatically.
    Cancelled,
}

impl PendingTombstoneFlowStatus {
    /// Parses a persisted database value into the strongly-typed status enum.
    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "waiting_gate" => Some(Self::WaitingGate),
            "gate_recorded" => Some(Self::GateRecorded),
            "completed" => Some(Self::Completed),
            "cancelled" => Some(Self::Cancelled),
            _ => None,
        }
    }

    /// Returns the canonical persisted representation of the status.
    pub fn as_db(self) -> &'static str {
        match self {
            Self::WaitingGate => "waiting_gate",
            Self::GateRecorded => "gate_recorded",
            Self::Completed => "completed",
            Self::Cancelled => "cancelled",
        }
    }

    /// Returns true when the flow is still active and may accept new evidence.
    pub fn is_active(self) -> bool {
        matches!(self, Self::WaitingGate | Self::GateRecorded)
    }
}

/// Durable record retained while a tombstone flow waits for governance evidence or recovery.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PendingTombstoneFlow {
    /// Stable pending-flow identifier.
    pub pending_flow_id: PendingFlowId,
    /// Member targeted by the high-risk flow.
    pub global_member_id: GlobalMemberId,
    /// High-risk action currently tracked by the flow.
    pub action_name: String,
    /// Trusted actor snapshot that opened the flow.
    pub requested_by: ActorContext,
    /// Human-readable reason retained for audit and archive collaboration.
    pub requested_reason: String,
    /// Governance decision id expected to arrive later, when already known.
    pub expected_gate_decision_id: Option<GateDecisionId>,
    /// Governance evidence recorded after the decision event is consumed.
    pub gate_decision_ref: Option<GateDecisionRef>,
    /// Current flow lifecycle status.
    pub status: PendingTombstoneFlowStatus,
    /// Cancellation reason retained when the flow was explicitly cancelled.
    pub cancel_reason: Option<String>,
    /// Timestamp when the flow was opened.
    pub opened_at: PrimitiveDateTime,
    /// Timestamp when the flow was last updated.
    pub updated_at: PrimitiveDateTime,
}

impl PendingTombstoneFlow {
    /// Opens a new pending flow for tombstone coordination.
    pub fn open_for_tombstone(
        pending_flow_id: PendingFlowId,
        global_member_id: GlobalMemberId,
        actor: ActorContext,
        reason: impl Into<String>,
        expected_gate_decision_id: Option<GateDecisionId>,
        opened_at: PrimitiveDateTime,
    ) -> Result<Self, IdentityError> {
        let reason = reason.into();
        if pending_flow_id.as_str().trim().is_empty() {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_INVALID_ARGUMENT",
                message: "pending_flow_id must not be blank".to_string(),
            });
        }
        if global_member_id.as_str().trim().is_empty() {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_INVALID_ARGUMENT",
                message: "global_member_id must not be blank".to_string(),
            });
        }
        if reason.trim().is_empty() {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_INVALID_ARGUMENT",
                message: "requested_reason must not be blank".to_string(),
            });
        }
        if let Some(gate_decision_id) = expected_gate_decision_id.as_ref() {
            if gate_decision_id.as_str().trim().is_empty() {
                return Err(IdentityError::RuleViolation {
                    code: "IDENTITY_INVALID_ARGUMENT",
                    message: "expected_gate_decision_id must not be blank".to_string(),
                });
            }
        }

        Ok(Self {
            pending_flow_id,
            global_member_id,
            action_name: "TombstoneMember".to_string(),
            requested_by: actor,
            requested_reason: reason,
            expected_gate_decision_id,
            gate_decision_ref: None,
            status: PendingTombstoneFlowStatus::WaitingGate,
            cancel_reason: None,
            opened_at,
            updated_at: opened_at,
        })
    }

    /// Attaches governance evidence to an active flow.
    pub fn attach_gate_decision(
        &mut self,
        gate_ref: GateDecisionRef,
        updated_at: PrimitiveDateTime,
    ) -> Result<(), IdentityError> {
        gate_ref.validate()?;

        if !self.status.is_active() {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_PENDING_TOMBSTONE_FLOW_NOT_ACTIVE",
                message: format!(
                    "pending flow `{}` is `{}` and cannot accept a gate decision",
                    self.pending_flow_id.as_str(),
                    self.status.as_db()
                ),
            });
        }

        if let Some(expected_gate_decision_id) = self.expected_gate_decision_id.as_ref() {
            if expected_gate_decision_id != &gate_ref.gate_decision_id {
                return Err(IdentityError::RuleViolation {
                    code: "IDENTITY_GATE_DECISION_MISMATCH",
                    message: format!(
                        "pending flow `{}` expected gate decision `{}` but received `{}`",
                        self.pending_flow_id.as_str(),
                        expected_gate_decision_id.as_str(),
                        gate_ref.gate_decision_id.as_str()
                    ),
                });
            }
        }

        self.gate_decision_ref = Some(gate_ref.clone());
        if self.expected_gate_decision_id.is_none() {
            self.expected_gate_decision_id = Some(gate_ref.gate_decision_id);
        }
        self.status = PendingTombstoneFlowStatus::GateRecorded;
        self.cancel_reason = None;
        self.updated_at = updated_at;
        Ok(())
    }

    /// Marks the flow as completed after the primary action consumes the evidence.
    pub fn mark_completed(&mut self, updated_at: PrimitiveDateTime) {
        self.status = PendingTombstoneFlowStatus::Completed;
        self.cancel_reason = None;
        self.updated_at = updated_at;
    }

    /// Cancels the flow and records the cancellation reason.
    pub fn mark_cancelled(
        &mut self,
        reason: impl Into<String>,
        updated_at: PrimitiveDateTime,
    ) -> Result<(), IdentityError> {
        let reason = reason.into();
        if reason.trim().is_empty() {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_INVALID_ARGUMENT",
                message: "cancel_reason must not be blank".to_string(),
            });
        }

        self.status = PendingTombstoneFlowStatus::Cancelled;
        self.cancel_reason = Some(reason);
        self.updated_at = updated_at;
        Ok(())
    }

    /// Returns a projection-safe summary JSON for idempotency and outbox payloads.
    pub fn summary_json(&self) -> Value {
        json!({
            "pending_flow_id": self.pending_flow_id.as_str(),
            "global_member_id": self.global_member_id.as_str(),
            "action_name": self.action_name,
            "requested_reason": self.requested_reason,
            "expected_gate_decision_id": self.expected_gate_decision_id.as_ref().map(|value| value.as_str()),
            "gate_decision_ref": self.gate_decision_ref,
            "status": self.status.as_db(),
            "cancel_reason": self.cancel_reason,
            "opened_at": self.opened_at,
            "updated_at": self.updated_at,
        })
    }
}

/// Command payload used by the explicit tombstone command path.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TombstoneMemberCommand {
    /// Stable member id targeted by the command.
    pub global_member_id: GlobalMemberId,
    /// Human-readable reason retained for archive and audit evidence.
    pub reason: String,
    /// Optional optimistic-lock version expected by the caller.
    pub expected_version: Option<i64>,
    /// Optional governance evidence carried by the caller for validation.
    pub gate_decision_ref: Option<GateDecisionRef>,
}
