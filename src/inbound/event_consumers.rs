//! Inbound event consumer entrypoints that delegate into application services.

use crate::application::career_event::{CareerEventConsumerService, CareerEventOutcome};
use crate::application::role_catalog_sync::{RoleCatalogSyncOutcome, RoleCatalogSyncService};
use crate::application::tombstone_flow::{GateDecisionOutcome, TombstoneFlowService};
use crate::error::IdentityError;
use crate::inbound::events::{
    InboundGateDecisionEvent, InboundProcessFactEvent, InboundRoleCatalogEvent,
    InboundWorkFactEvent,
};

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

/// Consumer entrypoint for work/process facts that append career history.
#[derive(Debug, Clone)]
pub struct CareerEventConsumer<Service> {
    service: Service,
}

impl<Service> CareerEventConsumer<Service> {
    /// Creates a career event consumer bound to the provided application service.
    pub fn new(service: Service) -> Self {
        Self { service }
    }

    /// Returns the stable operation name used for diagnostics and tests.
    pub fn operation_name(&self) -> &'static str {
        "AppendCareerEntry"
    }
}

impl<UowFactory> CareerEventConsumer<CareerEventConsumerService<UowFactory>>
where
    UowFactory: crate::application::persistence::UnitOfWorkFactory,
{
    /// Consumes a work fact inbound event through the application service boundary.
    pub async fn consume_work_event(
        &self,
        event: InboundWorkFactEvent,
    ) -> Result<CareerEventOutcome, IdentityError> {
        self.service.consume_work_event(event).await
    }

    /// Consumes a process fact inbound event through the application service boundary.
    pub async fn consume_process_event(
        &self,
        event: InboundProcessFactEvent,
    ) -> Result<CareerEventOutcome, IdentityError> {
        self.service.consume_process_event(event).await
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

/// Consumer entrypoint for governance gate-decision events.
#[derive(Debug, Clone)]
pub struct GateDecisionConsumer<Service> {
    service: Service,
}

impl<Service> GateDecisionConsumer<Service> {
    /// Creates a gate-decision consumer bound to the provided application service.
    pub fn new(service: Service) -> Self {
        Self { service }
    }

    /// Returns a stable operation name for diagnostics and tests.
    pub fn operation_name(&self) -> &'static str {
        "HandleGateDecisionEvent"
    }
}

impl<UowFactory, Governance, ArchiveRequester>
    GateDecisionConsumer<TombstoneFlowService<UowFactory, Governance, ArchiveRequester>>
where
    UowFactory: crate::application::persistence::UnitOfWorkFactory,
    Governance: crate::outbound::GovernancePort,
    ArchiveRequester: crate::outbound::ArchiveRequestPort,
{
    /// Consumes one governance gate-decision event through the application service boundary.
    pub async fn consume(
        &self,
        event: InboundGateDecisionEvent,
    ) -> Result<GateDecisionOutcome, IdentityError> {
        self.service.handle_gate_decision_event(event).await
    }
}
