//! Public command envelopes and accepted side-effect summary shell.

use core_contracts::actor::ActorRef;
use serde::{Deserialize, Serialize};

use crate::metadata::{
    IdentityCommandMetadata, IdentityProtocolRejection, IdentityRequestDigestMarker,
};
use crate::protocol::IdentityCommandName;
use crate::refs::{
    GlobalLifecycleStateKind, GlobalMemberRef, GovernanceBasisRef, IdentityAnchorReasonRef,
    IdentityAnchorStateKind, IdentityAuditSubjectRef, IdentityOutboxRecordRef,
    IdentityProjectionRef, IdentitySourceRef, IdentityStoredResultRef, IdentityTraceRecordRef,
    IdentityTruthCursor, LifecycleReasonRef, LifecycleRiskRef,
};

/// Public command request envelope.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityCommandRequest<T> {
    /// Caller actor reference extracted at the entry boundary.
    pub actor_ref: ActorRef,
    /// Stable public command name.
    pub command_name: IdentityCommandName,
    /// Public request metadata shell.
    pub metadata: IdentityCommandMetadata,
    /// Canonical digest marker.
    pub digest: IdentityRequestDigestMarker,
    /// Typed command body.
    pub body: T,
}

/// Public accepted command response envelope.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityCommandResponse<T> {
    /// Stable public command name.
    pub command_name: IdentityCommandName,
    /// Stored replay result reference.
    pub result_ref: IdentityStoredResultRef,
    /// Typed accepted result body.
    pub result: T,
    /// Public accepted side-effect summary.
    pub effect: IdentityCommandEffectPublicSummary,
}

/// Public command outcome shell.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum IdentityCommandOutcome<T> {
    /// Accepted response shell.
    Accepted(IdentityCommandResponse<T>),
    /// Rejected response shell.
    Rejected(IdentityProtocolRejection),
}

/// Public accepted side-effect summary for command results.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityCommandEffectPublicSummary {
    /// Accepted truth cursor marker.
    pub accepted_cursor_ref: IdentityTruthCursor,
    /// Appended trace references.
    pub trace_refs: Vec<IdentityTraceRecordRef>,
    /// Audit subject references associated with the accepted write.
    pub audit_subject_refs: Vec<IdentityAuditSubjectRef>,
    /// Outbox record references created by the accepted write.
    pub outbox_refs: Vec<IdentityOutboxRecordRef>,
    /// Projection refs marked stale by the accepted write.
    pub stale_projection_refs: Vec<IdentityProjectionRef>,
}

/// Request body for establishing a platform-level global member identity.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct EstablishGlobalMemberRequest {
    /// Optional caller-proposed member ref.
    pub requested_member_ref: Option<GlobalMemberRef>,
    /// Body-free source marker used to establish the member.
    pub source_ref: IdentitySourceRef,
    /// Optional reason marker used by accepted trace or anchor material.
    pub anchor_reason_ref: Option<IdentityAnchorReasonRef>,
    /// Reason marker for the initial lifecycle state created with the member.
    pub initial_lifecycle_reason_ref: LifecycleReasonRef,
}

/// Accepted command result for `EstablishGlobalMember`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GlobalMemberCommandResult {
    /// Established member ref.
    pub member_ref: GlobalMemberRef,
    /// Final anchor state after establishment.
    pub anchor_state_kind: IdentityAnchorStateKind,
    /// Initial lifecycle state created for the member.
    pub lifecycle_state_kind: GlobalLifecycleStateKind,
    /// Body-free source marker that established the member.
    pub source_ref: IdentitySourceRef,
}

/// Request body for explicitly changing a member global lifecycle state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct UpdateGlobalLifecycleStateRequest {
    /// Member whose lifecycle state will be updated.
    pub member_ref: GlobalMemberRef,
    /// Requested target lifecycle state.
    pub target_state: GlobalLifecycleStateKind,
    /// Body-free lifecycle reason marker.
    pub reason_ref: LifecycleReasonRef,
    /// Optional governance basis marker for high-risk lifecycle changes.
    pub basis_ref: Option<GovernanceBasisRef>,
    /// Lifecycle action risk marker used for high-risk precheck.
    pub action_risk_ref: Option<LifecycleRiskRef>,
}

/// Accepted command result for `UpdateGlobalLifecycleState`.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GlobalLifecycleCommandResult {
    /// Member whose lifecycle state changed.
    pub member_ref: GlobalMemberRef,
    /// Lifecycle state after the command.
    pub lifecycle_state_kind: GlobalLifecycleStateKind,
    /// Body-free lifecycle reason marker.
    pub reason_ref: LifecycleReasonRef,
    /// Governance basis marker persisted for the lifecycle change when present.
    pub basis_ref: Option<GovernanceBasisRef>,
    /// Anchor state after terminal handling when changed by the flow.
    pub anchor_state_kind: Option<IdentityAnchorStateKind>,
}
