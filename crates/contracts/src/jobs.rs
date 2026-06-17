//! Public operations job shells.

use core_contracts::actor::ActorRef;
use serde::{Deserialize, Serialize};

use crate::protocol::{IdentityJobName, IdentityProtocolSchemaVersionRef};
use crate::queries::IdentityPublicPageRequest;
use crate::receipts::MaintenanceIssueRef;
use crate::refs::{
    ExternalReferenceRef, GlobalMemberRef, HandoffReceiptRef, IdentityJobCursorRef,
    IdentityJobReportRef, IdentityJobRunMetadataRef, IdentityJobRunRef, IdentityJobScopeMarkerRef,
    IdentityMaintenanceTargetRef, IdentityOutboxRecordRef, IdentityProjectionRef,
    IdentityReferenceOwnerRef, IdentityStoredResultRef, IdentityTimestamp, MaintenanceScopeRef,
    ReconciliationFindingIntentRef, ReconciliationFindingMaterial, ReconciliationReportRef,
};

/// Public job request shell.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityJobRequest<T> {
    /// Stable public job name.
    pub job_name: IdentityJobName,
    /// Job run reference.
    pub job_run_ref: IdentityJobRunRef,
    /// Body-free run metadata marker.
    pub run_metadata_ref: IdentityJobRunMetadataRef,
    /// Body-free scope marker.
    pub scope_marker_ref: IdentityJobScopeMarkerRef,
    /// Idempotency key for replay protection.
    pub idempotency_key: core_contracts::metadata::IdempotencyKey,
    /// Optional input cursor marker.
    pub input_cursor_ref: Option<IdentityJobCursorRef>,
    /// Canonical protocol schema version marker.
    pub schema_version_ref: IdentityProtocolSchemaVersionRef,
    /// System actor ref for the job run.
    pub system_actor_ref: ActorRef,
    /// Typed safe job input shell.
    pub input: T,
}

/// Public job response shell.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityJobResponse<T> {
    /// Stable public job name.
    pub job_name: IdentityJobName,
    /// Public job report reference.
    pub report_ref: IdentityJobReportRef,
    /// Stored replay result reference.
    pub stored_result_ref: IdentityStoredResultRef,
    /// Typed safe job output shell.
    pub output: T,
    /// Public job report shell.
    pub report: IdentityJobReportSurface,
}

/// Public job completion disposition.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityJobRunDisposition {
    /// Job completed without failed items.
    Completed,
    /// Job completed with mixed success.
    Partial,
    /// Job failed terminally.
    Failed,
    /// Job failed but may be retried.
    RetryableFailed,
    /// Job found nothing to do.
    Noop,
    /// Duplicate request was replayed from stored material.
    DuplicateReplayed,
    /// Job request was rejected before execution.
    Rejected,
}

/// Public per-run item counters.
#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityJobItemCounts {
    /// Items scanned in the requested scope.
    pub scanned_count: u32,
    /// Items changed or emitted by the job.
    pub changed_count: u32,
    /// Items that failed.
    pub failed_count: u32,
    /// Items skipped without mutation.
    pub skipped_count: u32,
}

/// Projection rebuild target scope.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityProjectionRebuildScopeDto {
    /// Rebuild only the explicitly listed projections.
    ExplicitProjectionRefs(Vec<IdentityProjectionRef>),
    /// Rebuild stale projections within the outer maintenance scope.
    StaleInMaintenanceScope,
}

/// External reference refresh target scope.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityExternalReferenceRefreshScopeDto {
    /// Refresh only the explicitly listed reference bundles.
    ExplicitReferenceRefs(Vec<ExternalReferenceRef>),
    /// Refresh stale reference bundles within the outer maintenance scope.
    StaleInMaintenanceScope,
    /// Refresh bundles owned by one local identity object.
    ByOwner(IdentityReferenceOwnerRef),
    /// Refresh bundles within one external reference category.
    ByKind(crate::refs::ExternalReferenceKind),
}

/// Reconciliation target expansion scope.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityReconciliationTargetScopeDto {
    /// Reconcile only the explicitly listed targets.
    ExplicitTargets(Vec<IdentityMaintenanceTargetRef>),
    /// Expand all targets from the outer maintenance scope.
    ByMaintenanceScope,
}

/// Public rebuild job input.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RebuildIdentityProjectionJobInput {
    /// Projection rebuild target selection.
    pub rebuild_scope: IdentityProjectionRebuildScopeDto,
    /// Outer maintenance scope marker.
    pub maintenance_scope_ref: MaintenanceScopeRef,
    /// Public page request for batched job execution.
    pub page: IdentityPublicPageRequest,
}

/// Public rebuild job output.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RebuildIdentityProjectionJobOutput {
    /// Public run disposition.
    pub disposition: IdentityJobRunDisposition,
    /// Public item counters.
    pub counts: IdentityJobItemCounts,
    /// Projection refs rebuilt during this run.
    pub rebuilt_projection_refs: Vec<IdentityProjectionRef>,
    /// Projection refs that failed during this run.
    pub failed_projection_refs: Vec<IdentityProjectionRef>,
    /// Reconciliation report refs generated during this run.
    pub report_refs: Vec<ReconciliationReportRef>,
    /// Safe issue refs exposed by the run.
    pub issue_refs: Vec<MaintenanceIssueRef>,
}

/// Public reference refresh job input.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RefreshExternalReferenceStateJobInput {
    /// Reference refresh target selection.
    pub refresh_scope: IdentityExternalReferenceRefreshScopeDto,
    /// Outer maintenance scope marker.
    pub maintenance_scope_ref: MaintenanceScopeRef,
    /// Public page request for batched job execution.
    pub page: IdentityPublicPageRequest,
}

/// Public reference refresh job output.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RefreshExternalReferenceStateJobOutput {
    /// Public run disposition.
    pub disposition: IdentityJobRunDisposition,
    /// Public item counters.
    pub counts: IdentityJobItemCounts,
    /// Reference bundles refreshed during this run.
    pub refreshed_reference_refs: Vec<ExternalReferenceRef>,
    /// Reference bundles that failed during this run.
    pub failed_reference_refs: Vec<ExternalReferenceRef>,
    /// Safe issue refs exposed by the run.
    pub issue_refs: Vec<MaintenanceIssueRef>,
}

/// Public reconciliation job input.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RunIdentityReconciliationJobInput {
    /// Outer maintenance scope marker.
    pub maintenance_scope_ref: MaintenanceScopeRef,
    /// Reconciliation target selection.
    pub target_scope: IdentityReconciliationTargetScopeDto,
    /// Finding intent marker.
    pub finding_intent_ref: ReconciliationFindingIntentRef,
    /// Body-free finding material marker.
    pub finding_material: ReconciliationFindingMaterial,
    /// Public page request for batched job execution.
    pub page: IdentityPublicPageRequest,
}

/// Public reconciliation job output.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RunIdentityReconciliationJobOutput {
    /// Public run disposition.
    pub disposition: IdentityJobRunDisposition,
    /// Public item counters.
    pub counts: IdentityJobItemCounts,
    /// Report refs generated during this run.
    pub report_refs: Vec<ReconciliationReportRef>,
    /// Targets inspected during this run.
    pub inspected_target_refs: Vec<IdentityMaintenanceTargetRef>,
    /// Safe issue refs exposed by the run.
    pub issue_refs: Vec<MaintenanceIssueRef>,
}

/// Public job report shell.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityJobReportSurface {
    /// Job run reference.
    pub job_run_ref: IdentityJobRunRef,
    /// Stable job result kind.
    pub result_kind: IdentityJobResultKind,
    /// Affected member refs.
    pub affected_member_refs: Vec<GlobalMemberRef>,
    /// Affected projection refs.
    pub affected_projection_refs: Vec<IdentityProjectionRef>,
    /// Rebuilt projection refs.
    pub rebuilt_projection_refs: Vec<IdentityProjectionRef>,
    /// Failed projection refs.
    pub failed_projection_refs: Vec<IdentityProjectionRef>,
    /// Refreshed external reference refs.
    pub refreshed_reference_refs: Vec<ExternalReferenceRef>,
    /// Failed external reference refs.
    pub failed_reference_refs: Vec<ExternalReferenceRef>,
    /// Inspected maintenance targets.
    pub inspected_target_refs: Vec<IdentityMaintenanceTargetRef>,
    /// Generated report refs.
    pub report_refs: Vec<ReconciliationReportRef>,
    /// Touched outbox record refs.
    pub outbox_record_refs: Vec<IdentityOutboxRecordRef>,
    /// Published outbox record refs.
    pub published_outbox_refs: Vec<IdentityOutboxRecordRef>,
    /// Failed outbox record refs.
    pub failed_outbox_refs: Vec<IdentityOutboxRecordRef>,
    /// Touched handoff intent refs.
    pub handoff_intent_refs: Vec<crate::receipts::TraceHandoffIntentRef>,
    /// Delivered handoff intent refs.
    pub delivered_handoff_refs: Vec<crate::receipts::TraceHandoffIntentRef>,
    /// Failed handoff intent refs.
    pub failed_handoff_refs: Vec<crate::receipts::TraceHandoffIntentRef>,
    /// Formal handoff receipt refs.
    pub handoff_receipt_refs: Vec<HandoffReceiptRef>,
    /// Safe maintenance issue refs.
    pub issue_refs: Vec<MaintenanceIssueRef>,
    /// Optional input cursor marker.
    pub input_cursor_ref: Option<IdentityJobCursorRef>,
    /// Optional output cursor marker.
    pub output_cursor_ref: Option<IdentityJobCursorRef>,
    /// Job start timestamp.
    pub started_at: IdentityTimestamp,
    /// Optional job finish timestamp.
    pub finished_at: Option<IdentityTimestamp>,
}

/// Public job result kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityJobResultKind {
    /// Job succeeded.
    Succeeded,
    /// Job completed with partial issues.
    Partial,
    /// Job failed terminally.
    Failed,
    /// Job made no changes.
    Noop,
    /// Job failed but may be retried.
    RetryableFailed,
}
