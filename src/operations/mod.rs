//! Operations layer placeholders.

/// Placeholder outbox publisher job.
#[derive(Debug, Default)]
pub struct OutboxPublisherJob;

impl OutboxPublisherJob {
    /// Returns a stable placeholder operation name for diagnostics.
    pub fn operation_name(&self) -> &'static str {
        "PublishOutboxEvents"
    }
}

/// Placeholder projection rebuild job.
#[derive(Debug, Default)]
pub struct ProjectionRebuildJob;

impl ProjectionRebuildJob {
    /// Returns a stable placeholder operation name for diagnostics.
    pub fn operation_name(&self) -> &'static str {
        "RebuildMemberSummaryProjection"
    }
}

/// Placeholder role reconciliation job.
#[derive(Debug, Default)]
pub struct RoleReconciliationJob;

impl RoleReconciliationJob {
    /// Returns a stable placeholder operation name for diagnostics.
    pub fn operation_name(&self) -> &'static str {
        "ReconcileRoleCatalog"
    }
}
