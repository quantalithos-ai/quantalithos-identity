//! Public inbound and outbound event shells.

use serde::{Deserialize, Serialize};

use crate::metadata::IdentityProtocolValidationIssueRef;
use crate::protocol::{
    IdentityInboundConsumerName, IdentityOutboundEventName, IdentityProtocolSchemaVersionRef,
};
use crate::refs::{
    GlobalLifecycleStateKind, GlobalMemberRef, GovernanceBasisRef, IdentityAnchorReasonRef,
    IdentityAnchorStateKind, IdentityConsumerBindingRef, IdentityConsumerReceiptRef,
    IdentityEventEnvelopeMarkerRef, IdentityOutboxPayloadMarkerRef, IdentityOutboxRecordRef,
    IdentityOutboxSubjectRef, IdentitySourceEventRef, IdentitySourceRef, IdentityStoredResultRef,
    IdentityTimestamp, IdentityTraceContextRef, IdentityTraceRecordRef, IdentityTruthCursor,
    LifecycleReasonRef, TopicKeyRef,
};

/// Public inbound event or callback envelope.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityInboundEventEnvelope<T> {
    /// Stable public consumer or callback name.
    pub consumer_name: IdentityInboundConsumerName,
    /// Safe envelope marker.
    pub envelope_marker_ref: IdentityEventEnvelopeMarkerRef,
    /// Consumer binding marker.
    pub consumer_binding_ref: IdentityConsumerBindingRef,
    /// Upstream source event reference.
    pub source_event_ref: IdentitySourceEventRef,
    /// Idempotency key used for replay protection.
    pub idempotency_key: core_contracts::metadata::IdempotencyKey,
    /// Canonical protocol schema version marker.
    pub schema_version_ref: IdentityProtocolSchemaVersionRef,
    /// Optional upstream occurrence timestamp.
    pub occurred_at: Option<IdentityTimestamp>,
    /// Local receive timestamp.
    pub received_at: IdentityTimestamp,
    /// Optional propagated trace context marker.
    pub trace_context_ref: Option<IdentityTraceContextRef>,
    /// Typed safe payload shell.
    pub payload: T,
}

/// Public consumer receipt shell.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityConsumerReceipt {
    /// Public receipt reference.
    pub receipt_ref: IdentityConsumerReceiptRef,
    /// Stable public consumer or callback name.
    pub consumer_name: IdentityInboundConsumerName,
    /// Public consumer outcome.
    pub outcome: IdentityConsumerOutcome,
    /// Stored replay result reference.
    pub stored_result_ref: IdentityStoredResultRef,
    /// Trace refs recorded by the accepted or replayed path.
    pub trace_refs: Vec<IdentityTraceRecordRef>,
    /// Outbox refs created by the accepted or replayed path.
    pub outbox_refs: Vec<IdentityOutboxRecordRef>,
    /// Safe issue markers.
    pub issue_refs: Vec<IdentityProtocolValidationIssueRef>,
}

/// Public consumer outcome shell.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityConsumerOutcome {
    /// Consumer accepted the payload.
    Accepted,
    /// Duplicate receipt replayed a stored result.
    DuplicateReplayed,
    /// Consumer rejected the payload.
    Rejected,
    /// Payload was quarantined.
    Quarantined,
    /// Payload should be retried later.
    DelayedRetry,
    /// Payload produced no mutation.
    Noop,
    /// Schema version was unsupported.
    UnsupportedVersion,
}

/// Public outbound event envelope shell.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityOutboundEventEnvelope<T> {
    /// Stable public outbound event name.
    pub event_name: IdentityOutboundEventName,
    /// Stable outbound event reference.
    pub event_ref: IdentityOutboundEventRef,
    /// Outbox record reference that produced the event.
    pub outbox_record_ref: IdentityOutboxRecordRef,
    /// Canonical topic key marker.
    pub topic_key_ref: TopicKeyRef,
    /// Canonical protocol schema version marker.
    pub schema_version_ref: IdentityProtocolSchemaVersionRef,
    /// Body-free payload marker.
    pub payload_marker_ref: IdentityOutboxPayloadMarkerRef,
    /// Canonical accepted trace reference.
    pub trace_ref: IdentityTraceRecordRef,
    /// Canonical published subject reference.
    pub published_subject_ref: IdentityOutboxSubjectRef,
    /// Typed safe payload shell.
    pub payload: T,
}

/// Public outbound event reference.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IdentityOutboundEventRef(String);

impl IdentityOutboundEventRef {
    /// Creates a new outbound event reference.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

/// Body-free outbound payload emitted when a global member is established.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GlobalMemberEstablishedPayload {
    /// Established member ref.
    pub member_ref: GlobalMemberRef,
    /// Body-free source marker used for establishment.
    pub source_ref: IdentitySourceRef,
    /// Anchor state created by the establish flow.
    pub anchor_state_kind: IdentityAnchorStateKind,
    /// Initial lifecycle state created by the establish flow.
    pub lifecycle_state_kind: GlobalLifecycleStateKind,
    /// Actor that established the member.
    pub created_by_ref: core_contracts::actor::ActorRef,
    /// Timestamp when the member was established.
    pub established_at: IdentityTimestamp,
    /// Accepted truth cursor for the change.
    pub accepted_cursor_ref: IdentityTruthCursor,
}

/// Body-free outbound payload emitted when a member anchor state changes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityAnchorChangedPayload {
    /// Member whose anchor changed.
    pub member_ref: GlobalMemberRef,
    /// Anchor state after the accepted change.
    pub anchor_state_kind: IdentityAnchorStateKind,
    /// Optional anchor hold reason marker.
    pub anchor_reason_ref: Option<IdentityAnchorReasonRef>,
    /// Timestamp when the anchor changed.
    pub changed_at: IdentityTimestamp,
    /// Accepted truth cursor for the change.
    pub accepted_cursor_ref: IdentityTruthCursor,
}

/// Body-free outbound payload emitted when lifecycle truth changes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GlobalLifecycleChangedPayload {
    /// Member whose lifecycle changed.
    pub member_ref: GlobalMemberRef,
    /// Lifecycle state after the accepted change.
    pub lifecycle_state_kind: GlobalLifecycleStateKind,
    /// Body-free lifecycle reason marker.
    pub reason_ref: LifecycleReasonRef,
    /// Optional governance basis marker persisted for the change.
    pub basis_ref: Option<GovernanceBasisRef>,
    /// Actor that changed the lifecycle state.
    pub changed_by_ref: core_contracts::actor::ActorRef,
    /// Timestamp when the lifecycle changed.
    pub changed_at: IdentityTimestamp,
    /// Optional anchor state side effect for terminal transitions.
    pub anchor_state_kind: Option<IdentityAnchorStateKind>,
    /// Accepted truth cursor for the change.
    pub accepted_cursor_ref: IdentityTruthCursor,
}

/// Body-free outbound payload emitted when member availability changes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GlobalMemberAvailabilityChangedPayload {
    /// Member whose availability changed.
    pub member_ref: GlobalMemberRef,
    /// Lifecycle state that determined the availability.
    pub lifecycle_state_kind: GlobalLifecycleStateKind,
    /// Availability derived from the lifecycle state matrix.
    pub is_available: bool,
    /// Body-free lifecycle reason marker.
    pub reason_ref: LifecycleReasonRef,
    /// Timestamp when the availability changed.
    pub changed_at: IdentityTimestamp,
    /// Accepted truth cursor for the change.
    pub accepted_cursor_ref: IdentityTruthCursor,
}
