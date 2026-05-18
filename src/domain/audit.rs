//! Audit trace records appended by command and event handling flows.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::PrimitiveDateTime;

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
