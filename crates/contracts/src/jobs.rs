//! Public operations job shells.

use core_contracts::actor::ActorRef;
use serde::{Deserialize, Serialize};

use crate::protocol::{IdentityJobName, IdentityProtocolSchemaVersionRef};
use crate::refs::{
    ExternalReferenceRef, GlobalMemberRef, HandoffReceiptRef, IdentityJobCursorRef,
    IdentityJobReportRef, IdentityJobRunMetadataRef, IdentityJobRunRef, IdentityJobScopeMarkerRef,
    IdentityMaintenanceTargetRef, IdentityOutboxRecordRef, IdentityProjectionRef,
    IdentityStoredResultRef, IdentityTimestamp, ReconciliationReportRef,
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
    pub issue_refs: Vec<crate::receipts::MaintenanceIssueRef>,
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
