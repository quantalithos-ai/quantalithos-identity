//! Outbound ports and adapter placeholders.

use crate::domain::outbox::OutboxEvent;
use crate::error::IdentityError;

/// Publishes durable outbox records to the external L0-bus.
pub trait BusPublisherPort {
    /// Publishes a single outbox event to the external bus.
    fn publish(
        &self,
        event: &OutboxEvent,
    ) -> impl std::future::Future<Output = Result<(), IdentityError>> + Send;
}
