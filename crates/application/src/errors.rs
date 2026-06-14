//! Application-layer error taxonomy for identity orchestration and ports.

use std::fmt;

use identity_contracts::errors::ContractError;
use identity_domain::errors::IdentityDomainError;

/// Stable application error class used across application services and ports.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ApplicationErrorKind {
    /// Request, metadata, or helper precondition failed at the application layer.
    InvalidRequest,
    /// The requested identity-owned object or required view could not be found.
    NotFound,
    /// The actor or caller is not allowed to perform the requested operation.
    NotVisible,
    /// Domain policy, invariant, or transition rejected the requested operation.
    DomainRejected,
    /// An optimistic version token became stale.
    OptimisticVersionConflict,
    /// A formal unique key collided.
    FormalUniqueConflict,
    /// A same-key different-digest idempotency conflict occurred.
    IdempotencyConflict,
    /// A same-key same-digest operation is still in flight.
    IdempotencyInFlight,
    /// Stored replay state is missing or inconsistent.
    DuplicateReplayConsistencyDefect,
    /// A required repository or external dependency is currently unavailable.
    DependencyUnavailable,
    /// Commit status is unknown and the flow cannot safely decide replay semantics.
    CommitStatusUnknown,
    /// A consistency or layering defect was detected.
    ConsistencyDefect,
}

/// Internal application error carrying a stable taxonomy class and caller-safe message.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationError {
    /// Stable application error class.
    pub kind: ApplicationErrorKind,
    /// Caller-safe message.
    pub message: String,
}

impl ApplicationError {
    /// Creates a new application error from a stable kind and caller-safe message.
    pub fn new(kind: ApplicationErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// Creates an invalid-request application error.
    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self::new(ApplicationErrorKind::InvalidRequest, message)
    }

    /// Creates a not-found application error.
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(ApplicationErrorKind::NotFound, message)
    }

    /// Creates a domain-rejected application error.
    pub fn domain_rejected(message: impl Into<String>) -> Self {
        Self::new(ApplicationErrorKind::DomainRejected, message)
    }

    /// Creates an optimistic-version-conflict error.
    pub fn optimistic_version_conflict(message: impl Into<String>) -> Self {
        Self::new(ApplicationErrorKind::OptimisticVersionConflict, message)
    }

    /// Creates a dependency-unavailable application error.
    pub fn dependency_unavailable(message: impl Into<String>) -> Self {
        Self::new(ApplicationErrorKind::DependencyUnavailable, message)
    }

    /// Creates a consistency-defect application error.
    pub fn consistency_defect(message: impl Into<String>) -> Self {
        Self::new(ApplicationErrorKind::ConsistencyDefect, message)
    }
}

impl fmt::Display for ApplicationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for ApplicationError {}

impl From<ContractError> for ApplicationError {
    fn from(value: ContractError) -> Self {
        Self::invalid_request(format!("{}: {}", value.field, value.message))
    }
}

impl From<IdentityDomainError> for ApplicationError {
    fn from(value: IdentityDomainError) -> Self {
        match value {
            IdentityDomainError::MissingRequiredField { field } => {
                Self::invalid_request(format!("missing required field: {field}"))
            }
            IdentityDomainError::InvalidInput { field, message } => {
                Self::invalid_request(format!("{field}: {message}"))
            }
            IdentityDomainError::InvalidStateTransition { entity, message } => Self::new(
                ApplicationErrorKind::DomainRejected,
                format!("{entity}: {message}"),
            ),
            IdentityDomainError::PolicyDenied { policy, message } => Self::new(
                ApplicationErrorKind::DomainRejected,
                format!("{policy}: {message}"),
            ),
        }
    }
}
