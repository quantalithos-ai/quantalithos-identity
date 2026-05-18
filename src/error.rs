//! Shared error type definitions for the identity service bootstrap phase.

use thiserror::Error;

/// Represents service-level errors that are safe to construct before business logic exists.
#[derive(Debug, Error)]
pub enum IdentityError {
    /// Raised when a required or optional configuration value is malformed.
    #[error("invalid configuration for `{key}`: {reason}")]
    InvalidConfiguration { key: String, reason: String },
    /// Raised when persistence integration requires a database URL but none exists.
    #[error("database configuration is missing `DATABASE_URL`")]
    MissingDatabaseUrl,
    /// Raised when the database pool cannot be created.
    #[error("database pool initialization failed: {0}")]
    DatabasePool(#[source] sqlx::Error),
    /// Raised when SQLx migrations cannot be applied.
    #[error("database migration failed: {0}")]
    DatabaseMigration(#[source] sqlx::migrate::MigrateError),
    /// Raised when repository code encounters malformed persisted data.
    #[error("persistence data is invalid: {message}")]
    PersistenceData { message: String },
    /// Raised when an optimistic-lock save cannot match the expected version.
    #[error("version conflict while saving `{entity}`")]
    VersionConflict { entity: String },
}
