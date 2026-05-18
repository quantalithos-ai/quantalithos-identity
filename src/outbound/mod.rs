//! Outbound ports and adapter placeholders.

use crate::domain::capability_profile::ArtifactRef;
use crate::domain::memory_refs::MemoryRef;
use crate::domain::outbox::OutboxEvent;
use crate::error::IdentityError;

/// Validates external artifact refs without copying artifact bodies into identity.
pub trait ArtifactPort {
    /// Validates that every evidence ref exists and is safe to retain as a ref-only pointer.
    fn validate_refs(
        &self,
        refs: &[ArtifactRef],
    ) -> impl std::future::Future<Output = Result<(), IdentityError>> + Send;
}

/// Validates external memory refs without copying memory bodies into identity.
pub trait MemoryArchivePort {
    /// Validates that one memory ref exists and is safe to retain as a ref-only pointer.
    fn validate_ref(
        &self,
        memory_ref: &MemoryRef,
    ) -> impl std::future::Future<Output = Result<(), IdentityError>> + Send;
}

/// Publishes durable outbox records to the external L0-bus.
pub trait BusPublisherPort {
    /// Publishes a single outbox event to the external bus.
    fn publish(
        &self,
        event: &OutboxEvent,
    ) -> impl std::future::Future<Output = Result<(), IdentityError>> + Send;
}
