//! Public inbound and outbound event shells.

use serde::{Deserialize, Serialize};

use crate::metadata::IdentityProtocolValidationIssueRef;
use crate::protocol::{
    IdentityInboundConsumerName, IdentityOutboundEventName, IdentityProtocolSchemaVersionRef,
};
use crate::refs::{
    IdentityConsumerBindingRef, IdentityConsumerReceiptRef, IdentityEventEnvelopeMarkerRef,
    IdentityOutboxPayloadMarkerRef, IdentityOutboxRecordRef, IdentityOutboxSubjectRef,
    IdentitySourceEventRef, IdentityStoredResultRef, IdentityTimestamp, IdentityTraceContextRef,
    IdentityTraceRecordRef, TopicKeyRef,
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
