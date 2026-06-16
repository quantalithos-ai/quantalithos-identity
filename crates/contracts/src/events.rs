//! Public inbound and outbound event shells.

use serde::{Deserialize, Serialize};

use crate::metadata::IdentityProtocolValidationIssueRef;
use crate::protocol::{
    IdentityInboundConsumerName, IdentityOutboundEventName, IdentityProtocolSchemaVersionRef,
};
use crate::receipts::TraceHandoffIntentRef;
use crate::refs::{
    ArchiveHandoffRef, ArchiveRef, CapabilityEvidenceRef, CareerAppendMaterialMarker,
    CareerAppendReasonRef, CareerSafeSummaryRef, CareerSourceMarkerRef, GlobalLifecycleStateKind,
    GlobalMemberRef, GovernanceBasisRef, HandoffAttemptRef, HandoffIssueRef, HandoffReceiptRef,
    HandoffScopeRef, HandoffTargetRef, IdentityAnchorReasonRef, IdentityAnchorStateKind,
    IdentityConsumerBindingRef, IdentityConsumerReceiptRef, IdentityEventEnvelopeMarkerRef,
    IdentityOutboxPayloadMarkerRef, IdentityOutboxRecordRef, IdentityOutboxSubjectRef,
    IdentityReferenceOwnerRef, IdentitySourceEventRef, IdentitySourceRef, IdentityStoredResultRef,
    IdentityTimestamp, IdentityTraceContextRef, IdentityTraceRecordRef, IdentityTruthCursor,
    LifecycleReasonRef, MemoryRef, MemoryReferenceChangeMaterialMarker, MemoryReferenceReasonRef,
    MemoryReferenceRef, MemoryReferenceSourceRef, MemoryReferenceStateKind, MemorySafeSummaryRef,
    ProjectParticipationRef, RoleCapabilityChangeMaterialMarker, RoleCapabilityChangeReasonRef,
    RoleCapabilitySafeSummaryRef, RoleCapabilitySourceRef, RoleCapabilitySourceStateKind,
    RoleCapabilitySourceVersionRef, TopicKeyRef, WorkSourceRef,
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

/// Body-free method-library role/capability source change payload.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RoleCapabilitySourceChangedPayload {
    /// Member whose role-capability source snapshot changed.
    pub member_ref: GlobalMemberRef,
    /// Canonical role-capability source marker.
    pub source_ref: RoleCapabilitySourceRef,
    /// Formal source version marker.
    pub source_version_ref: RoleCapabilitySourceVersionRef,
    /// Source state reported by the upstream change.
    pub source_state_kind: RoleCapabilitySourceStateKind,
    /// Optional redaction-safe summary marker for resolved sources.
    pub safe_summary_ref: Option<RoleCapabilitySafeSummaryRef>,
    /// Evidence refs retained by the source snapshot.
    pub evidence_refs: Vec<CapabilityEvidenceRef>,
    /// Optional external reference bundle marker.
    pub external_reference_ref: Option<crate::refs::ExternalReferenceRef>,
    /// Optional identity owner marker for the external reference bundle.
    pub reference_owner_ref: Option<IdentityReferenceOwnerRef>,
    /// Optional body-free source change reason marker.
    pub change_reason_ref: Option<RoleCapabilityChangeReasonRef>,
    /// Material guard for forbidden method/evidence body.
    pub material_marker: RoleCapabilityChangeMaterialMarker,
}

/// Body-free work participation accepted payload.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkParticipationAcceptedPayload {
    /// Member whose career history may be appended.
    pub member_ref: GlobalMemberRef,
    /// Formal project participation marker.
    pub project_participation_ref: ProjectParticipationRef,
    /// Formal work source marker.
    pub work_source_ref: WorkSourceRef,
    /// Stable duplicate-source marker.
    pub career_source_marker_ref: CareerSourceMarkerRef,
    /// Required body-free career safe summary marker.
    pub safe_summary_ref: CareerSafeSummaryRef,
    /// Optional append reason marker.
    pub append_reason_ref: Option<CareerAppendReasonRef>,
    /// Material guard for forbidden work/project body.
    pub material_marker: CareerAppendMaterialMarker,
}

/// Body-free memory/archive carrier source state payload.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MemoryReferenceSourceStateChangedPayload {
    /// Member that owns the memory/archive relation.
    pub member_ref: GlobalMemberRef,
    /// Optional direct local relation ref.
    pub memory_reference_ref: Option<MemoryReferenceRef>,
    /// Formal memory/archive source marker.
    pub source_ref: MemoryReferenceSourceRef,
    /// Optional external memory carrier ref.
    pub memory_ref: Option<MemoryRef>,
    /// Optional external archive carrier ref.
    pub archive_ref: Option<ArchiveRef>,
    /// Target relation state kind requested by the source event.
    pub target_state_kind: MemoryReferenceStateKind,
    /// Optional body-free summary marker for usable states.
    pub safe_summary_ref: Option<MemorySafeSummaryRef>,
    /// Optional external reference bundle marker.
    pub external_reference_ref: Option<crate::refs::ExternalReferenceRef>,
    /// Optional identity owner marker for the external reference bundle.
    pub reference_owner_ref: Option<IdentityReferenceOwnerRef>,
    /// Optional body-free reason marker.
    pub reason_ref: Option<MemoryReferenceReasonRef>,
    /// Material guard for forbidden memory/archive body.
    pub material_marker: MemoryReferenceChangeMaterialMarker,
}

/// Body-free archive/memory handoff result payload for memory reference state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ArchiveHandoffResultPayload {
    /// Member that owns the memory/archive relation.
    pub member_ref: GlobalMemberRef,
    /// Optional direct local relation ref.
    pub memory_reference_ref: Option<MemoryReferenceRef>,
    /// Formal archive carrier ref.
    pub archive_ref: ArchiveRef,
    /// Formal archive handoff marker.
    pub archive_handoff_ref: ArchiveHandoffRef,
    /// Target relation state kind requested by the callback.
    pub target_state_kind: MemoryReferenceStateKind,
    /// Optional body-free reason marker.
    pub reason_ref: Option<MemoryReferenceReasonRef>,
    /// Optional safe handoff issue marker.
    pub issue_ref: Option<HandoffIssueRef>,
    /// Material guard for forbidden receipt/archive body.
    pub material_marker: MemoryReferenceChangeMaterialMarker,
}

/// Trace handoff callback result classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TraceHandoffResultKind {
    /// Callback confirms formal delivery with a receipt marker.
    Delivered,
    /// Callback reports a retryable failure.
    RetryableFailed,
    /// Callback reports a terminal failure.
    Failed,
    /// Callback reports a policy or target cancellation.
    Cancelled,
}

/// Body-free trace handoff receipt or failure payload.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TraceHandoffResultPayload {
    /// Existing handoff intent ref.
    pub handoff_intent_ref: TraceHandoffIntentRef,
    /// Formal handoff target marker.
    pub handoff_target_ref: HandoffTargetRef,
    /// Optional handoff scope marker for target consistency checks.
    pub handoff_scope_ref: Option<HandoffScopeRef>,
    /// Formal delivery attempt marker.
    pub attempt_ref: HandoffAttemptRef,
    /// Callback result classification.
    pub result_kind: TraceHandoffResultKind,
    /// Formal receipt marker required for delivered results.
    pub receipt_ref: Option<HandoffReceiptRef>,
    /// Safe issue marker required for failed or cancelled results.
    pub issue_ref: Option<HandoffIssueRef>,
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
