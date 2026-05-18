//! Compilable inbound event consumer placeholders for the skeleton phase.

/// Placeholder role catalog consumer.
#[derive(Debug, Default)]
pub struct RoleCatalogConsumer;

impl RoleCatalogConsumer {
    /// Returns a stable placeholder operation name for diagnostics.
    pub fn operation_name(&self) -> &'static str {
        "SyncRoleCatalog"
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
