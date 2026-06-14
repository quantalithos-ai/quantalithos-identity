//! Shared contract errors for identity public contracts.

use serde::{Deserialize, Serialize};

/// Minimal caller-safe contract error used by foundational value helpers.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ContractError {
    /// Logical field or marker associated with the failure.
    pub field: String,
    /// Caller-safe message.
    pub message: String,
}

impl ContractError {
    /// Creates an invalid-value contract error.
    pub fn invalid_value(field: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            field: field.into(),
            message: message.into(),
        }
    }
}
