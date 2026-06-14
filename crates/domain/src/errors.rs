//! Shared domain errors for core identity truth and guard logic.

use identity_contracts::refs::{IdentityOperationChannel, IdentitySourceOwner};

/// Domain error used by identity core truth and guard operations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IdentityDomainError {
    /// A required input or marker is missing.
    MissingRequiredField {
        /// Stable field or input name.
        field: &'static str,
    },
    /// An input value or typed combination is invalid.
    InvalidInput {
        /// Stable field or input name.
        field: &'static str,
        /// Caller-safe explanation.
        message: &'static str,
    },
    /// A state transition is not allowed by the formal matrix.
    InvalidStateTransition {
        /// State family that rejected the transition.
        entity: &'static str,
        /// Caller-safe explanation.
        message: &'static str,
    },
    /// A policy or guard rejected the requested operation.
    PolicyDenied {
        /// Stable policy name.
        policy: &'static str,
        /// Caller-safe explanation.
        message: &'static str,
    },
}

impl IdentityDomainError {
    /// Creates a missing-field error.
    pub fn missing_required_field(field: &'static str) -> Self {
        Self::MissingRequiredField { field }
    }

    /// Creates an invalid-input error.
    pub fn invalid_input(field: &'static str, message: &'static str) -> Self {
        Self::InvalidInput { field, message }
    }

    /// Creates an invalid-state-transition error.
    pub fn invalid_state_transition(entity: &'static str, message: &'static str) -> Self {
        Self::InvalidStateTransition { entity, message }
    }

    /// Creates a policy-denied error.
    pub fn policy_denied(policy: &'static str, message: &'static str) -> Self {
        Self::PolicyDenied { policy, message }
    }

    /// Creates a write-channel denial error.
    pub fn write_channel_denied(
        policy: &'static str,
        channel: IdentityOperationChannel,
        message: &'static str,
    ) -> Self {
        let detail = match channel {
            IdentityOperationChannel::Command => message,
            IdentityOperationChannel::Query => "query channel cannot mutate identity truth",
            IdentityOperationChannel::Consumer => message,
            IdentityOperationChannel::Job => "job channel cannot mutate core identity truth",
            IdentityOperationChannel::HandoffCallback => {
                "handoff callback cannot mutate this truth family"
            }
            IdentityOperationChannel::ProjectionMaintenance => {
                "projection maintenance cannot mutate identity truth"
            }
        };
        Self::policy_denied(policy, detail)
    }

    /// Creates an invalid-source-owner error.
    pub fn invalid_source_owner(
        policy: &'static str,
        owner: IdentitySourceOwner,
        message: &'static str,
    ) -> Self {
        let detail = match owner {
            IdentitySourceOwner::Identity => message,
            _ => message,
        };
        Self::policy_denied(policy, detail)
    }
}
