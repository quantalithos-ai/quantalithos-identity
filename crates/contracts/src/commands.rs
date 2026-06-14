//! Public command envelopes and accepted side-effect summary shell.

use core_contracts::actor::ActorRef;
use serde::{Deserialize, Serialize};

use crate::metadata::{
    IdentityCommandMetadata, IdentityProtocolRejection, IdentityRequestDigestMarker,
};
use crate::protocol::IdentityCommandName;
use crate::refs::{
    IdentityAuditSubjectRef, IdentityOutboxRecordRef, IdentityProjectionRef,
    IdentityStoredResultRef, IdentityTraceRecordRef, IdentityTruthCursor,
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
