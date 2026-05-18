//! Shared error type definitions for the identity service bootstrap phase.

use thiserror::Error;

/// Represents service-level errors that are safe to construct before business logic exists.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum IdentityError {
    /// Raised when a required or optional configuration value is malformed.
    #[error("invalid configuration for `{key}`: {reason}")]
    InvalidConfiguration { key: String, reason: String },
}
