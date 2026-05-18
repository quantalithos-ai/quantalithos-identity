//! Inbound event consumer entrypoints that delegate into application services.

use crate::application::role_catalog_sync::{RoleCatalogSyncOutcome, RoleCatalogSyncService};
use crate::error::IdentityError;
use crate::inbound::events::InboundRoleCatalogEvent;

/// Consumer entrypoint for method-library role-catalog events.
#[derive(Debug, Clone)]
pub struct RoleCatalogConsumer<Service> {
    service: Service,
}

impl<Service> RoleCatalogConsumer<Service> {
    /// Creates a role-catalog consumer bound to the provided application service.
    pub fn new(service: Service) -> Self {
        Self { service }
    }

    /// Returns the stable operation name used for diagnostics and tests.
    pub fn operation_name(&self) -> &'static str {
        "SyncRoleCatalog"
    }
}

impl<UowFactory> RoleCatalogConsumer<RoleCatalogSyncService<UowFactory>>
where
    UowFactory: crate::application::persistence::UnitOfWorkFactory,
{
    /// Consumes a role-catalog inbound event through the application service boundary.
    pub async fn consume(
        &self,
        event: InboundRoleCatalogEvent,
    ) -> Result<RoleCatalogSyncOutcome, IdentityError> {
        self.service.sync_role_catalog(event).await
    }
}

/// Placeholder career event consumer.
#[derive(Debug, Default)]
pub struct CareerEventConsumer;

impl CareerEventConsumer {
    /// Returns a stable placeholder operation name for diagnostics.
    pub fn operation_name(&self) -> &'static str {
        "AppendCareerEntry"
    }
}

/// Placeholder archive event consumer.
#[derive(Debug, Default)]
pub struct MemoryArchiveConsumer;

impl MemoryArchiveConsumer {
    /// Returns a stable placeholder operation name for diagnostics.
    pub fn operation_name(&self) -> &'static str {
        "HandleMemoryArchiveEvent"
    }
}

/// Placeholder gate-decision event consumer.
#[derive(Debug, Default)]
pub struct GateDecisionConsumer;

impl GateDecisionConsumer {
    /// Returns a stable placeholder operation name for diagnostics.
    pub fn operation_name(&self) -> &'static str {
        "HandleGateDecisionEvent"
    }
}
