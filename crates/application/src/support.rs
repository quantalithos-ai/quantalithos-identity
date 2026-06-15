//! Application-local helper objects shared by identity services and ports.

use core_contracts::actor::ActorRef;
use core_contracts::metadata::IdempotencyKey;
use identity_contracts::commands::{
    CareerRecordCommandResult, GlobalLifecycleCommandResult, GlobalMemberCommandResult,
    IdentityCommandEffectPublicSummary, MemoryReferenceCommandResult, RoleCapabilityCommandResult,
    TraceHandoffCommandResult,
};
use identity_contracts::events::IdentityConsumerReceipt;
use identity_contracts::jobs::IdentityJobResultKind;
use identity_contracts::metadata::{IdentityDegradedKind, IdentityProtocolRejection};
use identity_contracts::protocol::{
    IdentityCommandName, IdentityDigestAlgorithmMarkerRef, IdentityJobName,
    IdentityProtocolSchemaVersionRef,
};
use identity_contracts::receipts::{MaintenanceIssueRef, TraceHandoffIntentRef};
use identity_contracts::refs::{
    AuditScopeRef, AuditTrailRef, ExternalReferenceRef, GlobalMemberRef, HandoffReceiptRef,
    IdentityApiRequestMarkerRef, IdentityConsumerBindingRef, IdentityConsumerReceiptRef,
    IdentityDegradedMarkerRef, IdentityEventEnvelopeMarkerRef, IdentityJobCursorRef,
    IdentityJobReportRef, IdentityJobRunMetadataRef, IdentityJobRunRef, IdentityJobScopeMarkerRef,
    IdentityMaintenanceTargetRef, IdentityOutboxRecordRef, IdentityProjectionRef,
    IdentityRedactionMarkerRef, IdentitySourceEventRef, IdentityStoredResultRef, IdentityTimestamp,
    IdentityTraceContextRef, IdentityTraceRecordRef, IdentityTraceSubjectRef, IdentityTruthCursor,
    ReconciliationReportRef, VisibilityContextRef, VisibilityResultRef, VisibilityScopeRef,
};
use serde::{Deserialize, Serialize};

macro_rules! string_newtype {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Creates a new opaque typed value.
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            /// Returns the wrapped string.
            pub fn as_str(&self) -> &str {
                &self.0
            }

            /// Consumes the wrapper and returns the inner string.
            pub fn into_inner(self) -> String {
                self.0
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self::new(value)
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self::new(value)
            }
        }
    };
}

string_newtype!(
    IdentityTransactionRef,
    "Stable application-local transaction reference."
);
string_newtype!(
    IdentityRepositoryCursor,
    "Opaque application repository page cursor."
);
string_newtype!(
    IdentityOperationContextRef,
    "Single application operation context reference."
);
string_newtype!(IdentityOperationName, "Stable application operation name.");
string_newtype!(
    IdentityRequestMetadataRef,
    "Body-free application request metadata marker."
);
string_newtype!(
    IdentityIdempotencyRecordRef,
    "Application idempotency record reference."
);
string_newtype!(
    IdentityStoredSurfaceMarkerRef,
    "Replayable stored public surface marker."
);
string_newtype!(
    IdentityCommandEffectSummaryRef,
    "Accepted command effect summary reference."
);
string_newtype!(IdentityReadSubjectRef, "Canonical read subject marker.");
string_newtype!(
    IdentityDispatchTargetRef,
    "Application service dispatch target marker."
);
string_newtype!(IdentityApiRouteRef, "API route marker.");
string_newtype!(IdentityRuntimeProfileRef, "Runtime profile marker.");
string_newtype!(IdentityConfigEvidenceRef, "Runtime config evidence marker.");
string_newtype!(
    IdentityApiRouteCatalogRef,
    "API route catalog binding marker."
);
string_newtype!(
    IdentityConsumerBindingCatalogRef,
    "Worker binding catalog marker."
);
string_newtype!(IdentityJobCatalogRef, "Job catalog binding marker.");
string_newtype!(IdentityRuntimeAssemblyRef, "Runtime assembly identity.");
string_newtype!(IdentityApiEntryRef, "API entry identity.");
string_newtype!(IdentityEntryDispatchRef, "API dispatch attempt identity.");
string_newtype!(IdentityWorkerEntryRef, "Worker entry identity.");
string_newtype!(
    IdentityWorkerDispatchRef,
    "Worker dispatch attempt identity."
);
string_newtype!(IdentityJobEntryRef, "Job entry identity.");
string_newtype!(IdentityJobDispatchRef, "Job dispatch attempt identity.");
string_newtype!(
    IdentityConfigIssueRef,
    "Runtime config validation issue marker."
);
string_newtype!(
    IdentityEntryValidationIssueRef,
    "Entry validation issue marker."
);
string_newtype!(IdentityAdapterRef, "Adapter identity marker.");
string_newtype!(IdentityAdapterModeRef, "Adapter mode marker.");
string_newtype!(
    IdentityAdapterAvailabilityIssueRef,
    "Adapter availability issue marker."
);
string_newtype!(
    MemberSummaryViewId,
    "Stable member summary view identifier."
);
string_newtype!(IdentityTraceRecordId, "Stable trace record identifier.");
string_newtype!(AuditTrailId, "Stable audit trail identifier.");
string_newtype!(
    ReconciliationReportId,
    "Stable reconciliation report identifier."
);

/// Stable optimistic version attached to persisted application objects or sidecars.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IdentityVersion(pub u64);

impl IdentityVersion {
    /// Creates a new version token.
    pub fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the wrapped version.
    pub fn get(self) -> u64 {
        self.0
    }
}

/// Application-local page request used by repositories.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityRepositoryPage {
    /// Opaque cursor from a previous page.
    pub cursor: Option<IdentityRepositoryCursor>,
    /// Maximum number of items requested.
    pub limit: u32,
}

impl IdentityRepositoryPage {
    /// Creates a new page request.
    pub fn new(cursor: Option<IdentityRepositoryCursor>, limit: u32) -> Self {
        Self { cursor, limit }
    }
}

/// Persisted value paired with its optimistic version.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Versioned<T> {
    /// Persisted value.
    pub value: T,
    /// Optimistic version token.
    pub version: IdentityVersion,
}

/// Repository page result with an opaque next cursor.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Page<T> {
    /// Returned page items.
    pub items: Vec<T>,
    /// Opaque cursor for the next page.
    pub next_cursor: Option<IdentityRepositoryCursor>,
}

/// Typed ref paired with its optimistic version.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityVersionedRef<TRef> {
    /// Typed ref to the persisted object.
    pub value_ref: TRef,
    /// Optimistic version token for the referenced object.
    pub version: IdentityVersion,
}

/// Deterministic application-local set of member refs.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GlobalMemberRefSet {
    /// Member refs in deterministic order.
    pub member_refs: Vec<GlobalMemberRef>,
}

/// Deterministic application-local set of projection refs.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityProjectionRefSet {
    /// Projection refs in deterministic order.
    pub projection_refs: Vec<IdentityProjectionRef>,
}

/// Deterministic application-local set of external reference refs.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ExternalReferenceRefSet {
    /// External reference refs in deterministic order.
    pub reference_refs: Vec<ExternalReferenceRef>,
}

/// Accepted subject refs that share one canonical identity subject key.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityAcceptedSubjectRefs {
    /// Trace subject used by accepted trace records.
    pub trace_subject_ref: IdentityTraceSubjectRef,
    /// Audit subject used by audit trails.
    pub audit_subject_ref: identity_contracts::refs::IdentityAuditSubjectRef,
    /// Outbox subject used by accepted outbox records.
    pub outbox_subject_ref: identity_contracts::refs::IdentityOutboxSubjectRef,
}

/// Body-free markers required to materialize accepted write audit material.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityAcceptedAuditTrailMarkers {
    /// Accepted-write audit scope marker.
    pub audit_scope_ref: AuditScopeRef,
    /// Trail-level body-free materialized visibility marker.
    pub trail_visibility_result_ref: VisibilityResultRef,
    /// Entry-level body-free materialized visibility marker.
    pub entry_visibility_result_ref: VisibilityResultRef,
    /// Read surface kind carried by the accepted-write trail materialization.
    pub read_surface_kind: identity_contracts::refs::IdentityReadSurfaceKind,
}

/// Application-local entry surface classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityEntrySurfaceKind {
    /// API command entry.
    ApiCommand,
    /// API query entry.
    ApiQuery,
    /// Worker consumer entry.
    WorkerConsumer,
    /// Worker callback entry.
    WorkerCallback,
    /// Operations job entry.
    OperationsJob,
}

/// Stable application-local idempotency key wrapper.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IdentityIdempotencyKey(pub IdempotencyKey);

impl IdentityIdempotencyKey {
    /// Creates a new application idempotency key wrapper.
    pub fn new(value: impl Into<IdempotencyKey>) -> Self {
        Self(value.into())
    }

    /// Returns the underlying public idempotency key.
    pub fn as_public(&self) -> &IdempotencyKey {
        &self.0
    }
}

/// Stable request digest used by duplicate replay checks.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityRequestDigest {
    /// Body-free canonical request material marker.
    pub canonical_marker_ref: identity_contracts::refs::IdentityCanonicalRequestMarkerRef,
    /// Stable digest value.
    pub digest_value: identity_contracts::refs::IdentityRequestDigestValue,
    /// Canonical schema version marker.
    pub schema_version_ref: IdentityProtocolSchemaVersionRef,
    /// Canonical digest algorithm marker.
    pub algorithm_ref: IdentityDigestAlgorithmMarkerRef,
}

impl IdentityRequestDigest {
    /// Creates a new request digest from body-free canonical markers.
    pub fn from_canonical_marker(
        canonical_marker_ref: identity_contracts::refs::IdentityCanonicalRequestMarkerRef,
        digest_value: identity_contracts::refs::IdentityRequestDigestValue,
        schema_version_ref: IdentityProtocolSchemaVersionRef,
        algorithm_ref: IdentityDigestAlgorithmMarkerRef,
    ) -> Self {
        Self {
            canonical_marker_ref,
            digest_value,
            schema_version_ref,
            algorithm_ref,
        }
    }

    /// Returns whether the two digests describe the same canonical material.
    pub fn matches(&self, other: &Self) -> bool {
        self.digest_value == other.digest_value
            && self.schema_version_ref == other.schema_version_ref
            && self.algorithm_ref == other.algorithm_ref
    }

    /// Returns whether the two digests conflict for the same idempotency key.
    pub fn conflicts_with(&self, other: &Self) -> bool {
        !self.matches(other)
    }
}

/// Application operation metadata shared by command, query, consumer, job, and callback flows.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityOperationContext {
    /// Stable context identity.
    pub context_ref: IdentityOperationContextRef,
    /// Stable operation name.
    pub operation_name: IdentityOperationName,
    /// Entry channel that originated the operation.
    pub channel: identity_contracts::refs::IdentityOperationChannel,
    /// Effective actor reference.
    pub actor_ref: ActorRef,
    /// Body-free request metadata marker.
    pub request_metadata_ref: IdentityRequestMetadataRef,
    /// Optional idempotency key for replay-protected flows.
    pub idempotency_key: Option<IdentityIdempotencyKey>,
    /// Stable request digest.
    pub request_digest: IdentityRequestDigest,
    /// Optional propagated trace context marker.
    pub trace_context_ref: Option<IdentityTraceContextRef>,
    /// Optional source event marker for consumer and callback flows.
    pub source_event_ref: Option<IdentitySourceEventRef>,
    /// Optional job run marker for operations job flows.
    pub job_run_ref: Option<IdentityJobRunRef>,
    /// Start timestamp captured from the clock port.
    pub started_at: IdentityTimestamp,
}

impl IdentityOperationContext {
    /// Builds a command context.
    pub fn from_command(
        context_ref: IdentityOperationContextRef,
        operation_name: IdentityOperationName,
        actor_ref: ActorRef,
        request_metadata_ref: IdentityRequestMetadataRef,
        idempotency_key: Option<IdentityIdempotencyKey>,
        request_digest: IdentityRequestDigest,
        trace_context_ref: Option<IdentityTraceContextRef>,
        started_at: IdentityTimestamp,
    ) -> Self {
        Self {
            context_ref,
            operation_name,
            channel: identity_contracts::refs::IdentityOperationChannel::Command,
            actor_ref,
            request_metadata_ref,
            idempotency_key,
            request_digest,
            trace_context_ref,
            source_event_ref: None,
            job_run_ref: None,
            started_at,
        }
    }

    /// Builds a query context.
    pub fn from_query(
        context_ref: IdentityOperationContextRef,
        operation_name: IdentityOperationName,
        actor_ref: ActorRef,
        request_metadata_ref: IdentityRequestMetadataRef,
        request_digest: IdentityRequestDigest,
        trace_context_ref: Option<IdentityTraceContextRef>,
        started_at: IdentityTimestamp,
    ) -> Self {
        Self {
            context_ref,
            operation_name,
            channel: identity_contracts::refs::IdentityOperationChannel::Query,
            actor_ref,
            request_metadata_ref,
            idempotency_key: None,
            request_digest,
            trace_context_ref,
            source_event_ref: None,
            job_run_ref: None,
            started_at,
        }
    }

    /// Builds an inbound consumer context.
    pub fn from_inbound_event(
        context_ref: IdentityOperationContextRef,
        operation_name: IdentityOperationName,
        actor_ref: ActorRef,
        request_metadata_ref: IdentityRequestMetadataRef,
        idempotency_key: IdentityIdempotencyKey,
        request_digest: IdentityRequestDigest,
        trace_context_ref: Option<IdentityTraceContextRef>,
        source_event_ref: IdentitySourceEventRef,
        started_at: IdentityTimestamp,
    ) -> Self {
        Self {
            context_ref,
            operation_name,
            channel: identity_contracts::refs::IdentityOperationChannel::Consumer,
            actor_ref,
            request_metadata_ref,
            idempotency_key: Some(idempotency_key),
            request_digest,
            trace_context_ref,
            source_event_ref: Some(source_event_ref),
            job_run_ref: None,
            started_at,
        }
    }

    /// Builds a job context.
    pub fn from_job(
        context_ref: IdentityOperationContextRef,
        operation_name: IdentityOperationName,
        actor_ref: ActorRef,
        request_metadata_ref: IdentityRequestMetadataRef,
        idempotency_key: IdentityIdempotencyKey,
        request_digest: IdentityRequestDigest,
        trace_context_ref: Option<IdentityTraceContextRef>,
        job_run_ref: IdentityJobRunRef,
        started_at: IdentityTimestamp,
    ) -> Self {
        Self {
            context_ref,
            operation_name,
            channel: identity_contracts::refs::IdentityOperationChannel::Job,
            actor_ref,
            request_metadata_ref,
            idempotency_key: Some(idempotency_key),
            request_digest,
            trace_context_ref,
            source_event_ref: None,
            job_run_ref: Some(job_run_ref),
            started_at,
        }
    }

    /// Builds a handoff-callback context.
    pub fn from_handoff_callback(
        context_ref: IdentityOperationContextRef,
        operation_name: IdentityOperationName,
        actor_ref: ActorRef,
        request_metadata_ref: IdentityRequestMetadataRef,
        idempotency_key: IdentityIdempotencyKey,
        request_digest: IdentityRequestDigest,
        trace_context_ref: Option<IdentityTraceContextRef>,
        source_event_ref: IdentitySourceEventRef,
        started_at: IdentityTimestamp,
    ) -> Self {
        Self {
            context_ref,
            operation_name,
            channel: identity_contracts::refs::IdentityOperationChannel::HandoffCallback,
            actor_ref,
            request_metadata_ref,
            idempotency_key: Some(idempotency_key),
            request_digest,
            trace_context_ref,
            source_event_ref: Some(source_event_ref),
            job_run_ref: None,
            started_at,
        }
    }

    /// Returns whether this operation should carry an idempotency key.
    pub fn requires_idempotency(&self) -> bool {
        self.channel != identity_contracts::refs::IdentityOperationChannel::Query
    }

    /// Returns whether the channel may attempt a write-side application flow.
    pub fn is_write_channel(&self) -> bool {
        !matches!(
            self.channel,
            identity_contracts::refs::IdentityOperationChannel::Query
                | identity_contracts::refs::IdentityOperationChannel::ProjectionMaintenance
        )
    }
}

/// Application idempotency state family.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityIdempotencyStateKind {
    /// The key and digest are reserved but the flow is not complete.
    Reserved,
    /// The flow completed with a replayable accepted result.
    Completed,
    /// The flow completed with a replayable rejected result.
    RejectedStored,
    /// The key is already bound to a different canonical request digest.
    Conflict,
    /// The reservation expired and cannot be replayed.
    Expired,
}

/// Application-local idempotency record.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityIdempotencyRecord {
    /// Stable record identity.
    pub record_ref: IdentityIdempotencyRecordRef,
    /// Stable operation name.
    pub operation_name: IdentityOperationName,
    /// Channel copied from the operation context.
    pub channel: identity_contracts::refs::IdentityOperationChannel,
    /// Stable idempotency key.
    pub idempotency_key: IdentityIdempotencyKey,
    /// Canonical request digest.
    pub request_digest: IdentityRequestDigest,
    /// Current replay state.
    pub state: IdentityIdempotencyStateKind,
    /// Optional stored replay result reference.
    pub stored_result_ref: Option<IdentityStoredResultRef>,
    /// Timestamp when the reservation was created.
    pub reserved_at: IdentityTimestamp,
    /// Timestamp when the record reached a terminal replayable state.
    pub completed_at: Option<IdentityTimestamp>,
}

impl IdentityIdempotencyRecord {
    /// Reserves a new idempotency record from a write-capable operation context.
    pub fn reserve(
        record_ref: IdentityIdempotencyRecordRef,
        context: &IdentityOperationContext,
        now: IdentityTimestamp,
    ) -> Option<Self> {
        let idempotency_key = context.idempotency_key.clone()?;
        Some(Self {
            record_ref,
            operation_name: context.operation_name.clone(),
            channel: context.channel,
            idempotency_key,
            request_digest: context.request_digest.clone(),
            state: IdentityIdempotencyStateKind::Reserved,
            stored_result_ref: None,
            reserved_at: now,
            completed_at: None,
        })
    }

    /// Completes the record with a replayable accepted result.
    pub fn complete(
        mut self,
        stored_result_ref: IdentityStoredResultRef,
        now: IdentityTimestamp,
    ) -> Self {
        self.state = IdentityIdempotencyStateKind::Completed;
        self.stored_result_ref = Some(stored_result_ref);
        self.completed_at = Some(now);
        self
    }

    /// Completes the record with a replayable rejected result.
    pub fn complete_rejected(
        mut self,
        stored_result_ref: IdentityStoredResultRef,
        now: IdentityTimestamp,
    ) -> Self {
        self.state = IdentityIdempotencyStateKind::RejectedStored;
        self.stored_result_ref = Some(stored_result_ref);
        self.completed_at = Some(now);
        self
    }

    /// Marks the record as conflicting for same-key different-digest input.
    pub fn mark_conflict(mut self) -> Self {
        self.state = IdentityIdempotencyStateKind::Conflict;
        self
    }

    /// Returns whether the incoming digest can replay this record.
    pub fn can_replay(&self, digest: &IdentityRequestDigest) -> bool {
        matches!(
            self.state,
            IdentityIdempotencyStateKind::Completed | IdentityIdempotencyStateKind::RejectedStored
        ) && self.request_digest.matches(digest)
            && self.stored_result_ref.is_some()
    }
}

/// Result of attempting to reserve an idempotency key inside a write transaction.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum IdempotencyReserveOutcome {
    /// A fresh reservation was created for the current operation.
    Reserved(Versioned<IdentityIdempotencyRecord>),
    /// A completed same-digest record may replay a stored result.
    ReplayAvailable {
        /// Existing versioned idempotency record.
        record: Versioned<IdentityIdempotencyRecord>,
        /// Stored replay surface referenced by the record.
        stored_result_ref: IdentityStoredResultRef,
    },
    /// The same key is already bound to a different request digest.
    Conflict(Versioned<IdentityIdempotencyRecord>),
    /// The same key and digest are still reserved but not yet replayable.
    InFlight(Versioned<IdentityIdempotencyRecord>),
}

/// Stable stored replay result classification.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityStoredResultKind {
    /// Stored accepted command result.
    CommandAccepted,
    /// Stored rejected command result.
    CommandRejected,
    /// Stored consumer receipt.
    ConsumerReceipt,
    /// Stored job report.
    JobReport,
    /// Stored handoff callback receipt.
    HandoffCallbackReceipt,
}

/// Stored replay snapshot for duplicate-safe application flows.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredIdentityOperationResult {
    /// Stable stored result identity.
    pub stored_result_ref: IdentityStoredResultRef,
    /// Operation context that produced the replayable result.
    pub operation_context_ref: IdentityOperationContextRef,
    /// Replayable stored result kind.
    pub result_kind: IdentityStoredResultKind,
    /// Body-free stored surface marker.
    pub surface_marker_ref: IdentityStoredSurfaceMarkerRef,
    /// Timestamp when the replay surface was recorded.
    pub recorded_at: IdentityTimestamp,
}

impl StoredIdentityOperationResult {
    /// Creates a stored accepted command result.
    pub fn command_accepted(
        stored_result_ref: IdentityStoredResultRef,
        operation_context_ref: IdentityOperationContextRef,
        surface_marker_ref: IdentityStoredSurfaceMarkerRef,
        recorded_at: IdentityTimestamp,
    ) -> Self {
        Self::new(
            stored_result_ref,
            operation_context_ref,
            IdentityStoredResultKind::CommandAccepted,
            surface_marker_ref,
            recorded_at,
        )
    }

    /// Creates a stored rejected command result.
    pub fn command_rejected(
        stored_result_ref: IdentityStoredResultRef,
        operation_context_ref: IdentityOperationContextRef,
        surface_marker_ref: IdentityStoredSurfaceMarkerRef,
        recorded_at: IdentityTimestamp,
    ) -> Self {
        Self::new(
            stored_result_ref,
            operation_context_ref,
            IdentityStoredResultKind::CommandRejected,
            surface_marker_ref,
            recorded_at,
        )
    }

    /// Creates a stored consumer receipt result.
    pub fn consumer_receipt(
        stored_result_ref: IdentityStoredResultRef,
        operation_context_ref: IdentityOperationContextRef,
        surface_marker_ref: IdentityStoredSurfaceMarkerRef,
        recorded_at: IdentityTimestamp,
    ) -> Self {
        Self::new(
            stored_result_ref,
            operation_context_ref,
            IdentityStoredResultKind::ConsumerReceipt,
            surface_marker_ref,
            recorded_at,
        )
    }

    /// Creates a stored job report result.
    pub fn job_report(
        stored_result_ref: IdentityStoredResultRef,
        operation_context_ref: IdentityOperationContextRef,
        surface_marker_ref: IdentityStoredSurfaceMarkerRef,
        recorded_at: IdentityTimestamp,
    ) -> Self {
        Self::new(
            stored_result_ref,
            operation_context_ref,
            IdentityStoredResultKind::JobReport,
            surface_marker_ref,
            recorded_at,
        )
    }

    /// Creates a stored handoff callback receipt result.
    pub fn handoff_callback_receipt(
        stored_result_ref: IdentityStoredResultRef,
        operation_context_ref: IdentityOperationContextRef,
        surface_marker_ref: IdentityStoredSurfaceMarkerRef,
        recorded_at: IdentityTimestamp,
    ) -> Self {
        Self::new(
            stored_result_ref,
            operation_context_ref,
            IdentityStoredResultKind::HandoffCallbackReceipt,
            surface_marker_ref,
            recorded_at,
        )
    }

    fn new(
        stored_result_ref: IdentityStoredResultRef,
        operation_context_ref: IdentityOperationContextRef,
        result_kind: IdentityStoredResultKind,
        surface_marker_ref: IdentityStoredSurfaceMarkerRef,
        recorded_at: IdentityTimestamp,
    ) -> Self {
        Self {
            stored_result_ref,
            operation_context_ref,
            result_kind,
            surface_marker_ref,
            recorded_at,
        }
    }
}

/// Typed stored receipt envelope used as the duplicate replay source.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityConsumerReceiptEnvelope {
    /// Stable stored result identity.
    pub stored_result_ref: IdentityStoredResultRef,
    /// Operation context that produced the replayable receipt.
    pub operation_context_ref: IdentityOperationContextRef,
    /// Replayable stored result kind.
    pub result_kind: IdentityStoredResultKind,
    /// Body-free stored surface marker.
    pub surface_marker_ref: IdentityStoredSurfaceMarkerRef,
    /// Full public receipt shell returned during duplicate replay.
    pub receipt: IdentityConsumerReceipt,
    /// Timestamp when the receipt envelope was recorded.
    pub recorded_at: IdentityTimestamp,
}

impl IdentityConsumerReceiptEnvelope {
    /// Creates a stored consumer receipt envelope.
    pub fn consumer_receipt(
        operation_context_ref: IdentityOperationContextRef,
        surface_marker_ref: IdentityStoredSurfaceMarkerRef,
        receipt: IdentityConsumerReceipt,
        recorded_at: IdentityTimestamp,
    ) -> Self {
        Self {
            stored_result_ref: receipt.stored_result_ref.clone(),
            operation_context_ref,
            result_kind: IdentityStoredResultKind::ConsumerReceipt,
            surface_marker_ref,
            receipt,
            recorded_at,
        }
    }

    /// Creates a stored handoff callback receipt envelope.
    pub fn handoff_callback_receipt(
        operation_context_ref: IdentityOperationContextRef,
        surface_marker_ref: IdentityStoredSurfaceMarkerRef,
        receipt: IdentityConsumerReceipt,
        recorded_at: IdentityTimestamp,
    ) -> Self {
        Self {
            stored_result_ref: receipt.stored_result_ref.clone(),
            operation_context_ref,
            result_kind: IdentityStoredResultKind::HandoffCallbackReceipt,
            surface_marker_ref,
            receipt,
            recorded_at,
        }
    }
}

/// Typed command accepted result union used by duplicate replay envelopes.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum IdentityCommandTypedResult {
    /// `EstablishGlobalMember` accepted result.
    GlobalMember(GlobalMemberCommandResult),
    /// `UpdateGlobalLifecycleState` accepted result.
    GlobalLifecycle(GlobalLifecycleCommandResult),
    /// `MaintainRoleCapabilitySummary` accepted result.
    RoleCapability(RoleCapabilityCommandResult),
    /// `AppendCareerRecord` accepted result.
    CareerRecord(CareerRecordCommandResult),
    /// `MaintainMemoryReference` accepted result.
    MemoryReference(MemoryReferenceCommandResult),
    /// `PrepareTraceHandoff` accepted result.
    TraceHandoff(TraceHandoffCommandResult),
}

/// Typed stored accepted command envelope used as duplicate replay source.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityCommandAcceptedResultEnvelope {
    /// Stable stored result identity.
    pub stored_result_ref: IdentityStoredResultRef,
    /// Operation context that produced the replayable accepted response.
    pub operation_context_ref: IdentityOperationContextRef,
    /// Formal command name.
    pub command_name: IdentityCommandName,
    /// Body-free stored surface marker.
    pub surface_marker_ref: IdentityStoredSurfaceMarkerRef,
    /// Full typed accepted result body.
    pub result: IdentityCommandTypedResult,
    /// Full public accepted effect surface.
    pub effect: IdentityCommandEffectPublicSummary,
    /// Timestamp when the accepted envelope was recorded.
    pub recorded_at: IdentityTimestamp,
}

impl IdentityCommandAcceptedResultEnvelope {
    /// Creates a stored accepted command envelope.
    pub fn new(
        stored_result_ref: IdentityStoredResultRef,
        operation_context_ref: IdentityOperationContextRef,
        command_name: IdentityCommandName,
        surface_marker_ref: IdentityStoredSurfaceMarkerRef,
        result: IdentityCommandTypedResult,
        effect: IdentityCommandEffectPublicSummary,
        recorded_at: IdentityTimestamp,
    ) -> Self {
        Self {
            stored_result_ref,
            operation_context_ref,
            command_name,
            surface_marker_ref,
            result,
            effect,
            recorded_at,
        }
    }
}

/// Typed stored rejected command envelope used as duplicate replay source.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityCommandRejectedResultEnvelope {
    /// Stable stored result identity.
    pub stored_result_ref: IdentityStoredResultRef,
    /// Operation context that produced the replayable rejection.
    pub operation_context_ref: IdentityOperationContextRef,
    /// Formal command name.
    pub command_name: IdentityCommandName,
    /// Body-free stored surface marker.
    pub surface_marker_ref: IdentityStoredSurfaceMarkerRef,
    /// Full public rejection surface.
    pub rejection: IdentityProtocolRejection,
    /// Timestamp when the rejected envelope was recorded.
    pub recorded_at: IdentityTimestamp,
}

impl IdentityCommandRejectedResultEnvelope {
    /// Creates a stored rejected command envelope.
    pub fn new(
        stored_result_ref: IdentityStoredResultRef,
        operation_context_ref: IdentityOperationContextRef,
        command_name: IdentityCommandName,
        surface_marker_ref: IdentityStoredSurfaceMarkerRef,
        rejection: IdentityProtocolRejection,
        recorded_at: IdentityTimestamp,
    ) -> Self {
        Self {
            stored_result_ref,
            operation_context_ref,
            command_name,
            surface_marker_ref,
            rejection,
            recorded_at,
        }
    }
}

/// Accepted command effect family shared by command result assembly and replay.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityAcceptedEffectKind {
    /// `EstablishGlobalMember` accepted path.
    GlobalMemberCommandResult,
    /// `UpdateGlobalLifecycleState` accepted path.
    GlobalLifecycleCommandResult,
    /// `MaintainRoleCapabilitySummary` accepted path.
    RoleCapabilityCommandResult,
    /// `AppendCareerRecord` accepted path.
    CareerRecordCommandResult,
    /// `MaintainMemoryReference` accepted path.
    MemoryReferenceCommandResult,
    /// `PrepareTraceHandoff` accepted path.
    TraceHandoffCommandResult,
}

/// Typed identity-owned ref sum type used by accepted effect summaries.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum IdentityTruthRef {
    /// Global member truth.
    GlobalMember(GlobalMemberRef),
    /// Role capability summary truth.
    RoleCapabilitySummary(identity_contracts::refs::RoleCapabilitySummaryRef),
    /// Role capability source snapshot truth.
    RoleCapabilitySourceSnapshot(identity_contracts::refs::RoleCapabilitySourceSnapshotRef),
    /// Career record truth.
    CareerRecord(identity_contracts::refs::CareerRecordRef),
    /// Memory reference truth.
    MemoryReference(identity_contracts::refs::MemoryReferenceRef),
    /// Trace handoff intent truth.
    TraceHandoffIntent(TraceHandoffIntentRef),
}

/// Application-local summary of an accepted command and its side effects.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityCommandEffectSummary {
    /// Stable effect summary identity.
    pub effect_summary_ref: IdentityCommandEffectSummaryRef,
    /// Source operation context reference.
    pub operation_context_ref: IdentityOperationContextRef,
    /// Accepted effect family.
    pub effect_kind: IdentityAcceptedEffectKind,
    /// Primary accepted truth or intent ref.
    pub primary_truth_ref: IdentityTruthRef,
    /// Accepted truth cursor assigned by the UoW.
    pub accepted_cursor_ref: IdentityTruthCursor,
    /// Trace record refs appended by the accepted flow.
    pub trace_record_refs: Vec<IdentityTraceRecordRef>,
    /// Optional audit trail touched by the accepted flow.
    pub audit_trail_ref: Option<AuditTrailRef>,
    /// Outbox record refs created by the accepted flow.
    pub outbox_record_refs: Vec<IdentityOutboxRecordRef>,
    /// Projection refs marked stale by the accepted flow.
    pub stale_projection_refs: Vec<IdentityProjectionRef>,
    /// Stored result ref used for duplicate replay.
    pub stored_result_ref: IdentityStoredResultRef,
}

impl IdentityCommandEffectSummary {
    /// Creates a new accepted command effect summary.
    #[allow(clippy::too_many_arguments)]
    pub fn from_accepted_change(
        effect_summary_ref: IdentityCommandEffectSummaryRef,
        operation_context_ref: IdentityOperationContextRef,
        effect_kind: IdentityAcceptedEffectKind,
        primary_truth_ref: IdentityTruthRef,
        accepted_cursor_ref: IdentityTruthCursor,
        trace_record_refs: Vec<IdentityTraceRecordRef>,
        audit_trail_ref: Option<AuditTrailRef>,
        outbox_record_refs: Vec<IdentityOutboxRecordRef>,
        stale_projection_refs: Vec<IdentityProjectionRef>,
        stored_result_ref: IdentityStoredResultRef,
    ) -> Self {
        Self {
            effect_summary_ref,
            operation_context_ref,
            effect_kind,
            primary_truth_ref,
            accepted_cursor_ref,
            trace_record_refs,
            audit_trail_ref,
            outbox_record_refs,
            stale_projection_refs,
            stored_result_ref,
        }
    }

    /// Returns whether the accepted flow carries at least one formal trace ref.
    pub fn requires_trace(&self) -> bool {
        !self.trace_record_refs.is_empty()
    }

    /// Returns whether the summary points to a stored replay surface.
    pub fn has_replay_surface(&self) -> bool {
        true
    }

    /// Returns the affected stale projection refs without triggering rebuild.
    pub fn affected_projection_refs(&self) -> &[IdentityProjectionRef] {
        &self.stale_projection_refs
    }
}

/// Stable query read disposition family.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityReadDispositionKind {
    /// Fully visible read result.
    Visible,
    /// Visible result with field redaction.
    Redacted,
    /// Not visible result.
    NotVisible,
    /// Degraded result due to dependency or freshness issues.
    Degraded,
    /// Visible result carrying stale material markers.
    StaleVisible,
}

/// Application-local query visibility decision.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityVisibilityDecision {
    /// Stable visibility decision ref.
    pub decision_ref: identity_contracts::refs::IdentityVisibilityDecisionRef,
    /// Formal read subject marker.
    pub read_subject_ref: IdentityReadSubjectRef,
    /// Visibility context marker copied from the query metadata.
    pub visibility_context_ref: VisibilityContextRef,
    /// Visibility scope marker.
    pub visibility_scope_ref: VisibilityScopeRef,
    /// Visibility result marker.
    pub visibility_result_ref: VisibilityResultRef,
    /// Public read surface kind.
    pub surface_kind: identity_contracts::refs::IdentityReadSurfaceKind,
    /// Read disposition family.
    pub disposition: IdentityReadDispositionKind,
    /// Optional redaction marker.
    pub redaction_marker_ref: Option<IdentityRedactionMarkerRef>,
    /// Optional degraded marker.
    pub degraded_marker_ref: Option<IdentityDegradedMarkerRef>,
    /// Timestamp when the visibility decision was assembled.
    pub decided_at: IdentityTimestamp,
}

impl IdentityVisibilityDecision {
    /// Creates a fully visible read decision.
    pub fn visible(
        decision_ref: identity_contracts::refs::IdentityVisibilityDecisionRef,
        read_subject_ref: IdentityReadSubjectRef,
        visibility_context_ref: VisibilityContextRef,
        visibility_scope_ref: VisibilityScopeRef,
        visibility_result_ref: VisibilityResultRef,
        surface_kind: identity_contracts::refs::IdentityReadSurfaceKind,
        decided_at: IdentityTimestamp,
    ) -> Self {
        Self {
            decision_ref,
            read_subject_ref,
            visibility_context_ref,
            visibility_scope_ref,
            visibility_result_ref,
            surface_kind,
            disposition: IdentityReadDispositionKind::Visible,
            redaction_marker_ref: None,
            degraded_marker_ref: None,
            decided_at,
        }
    }

    /// Creates a redacted read decision.
    #[allow(clippy::too_many_arguments)]
    pub fn redacted(
        decision_ref: identity_contracts::refs::IdentityVisibilityDecisionRef,
        read_subject_ref: IdentityReadSubjectRef,
        visibility_context_ref: VisibilityContextRef,
        visibility_scope_ref: VisibilityScopeRef,
        visibility_result_ref: VisibilityResultRef,
        surface_kind: identity_contracts::refs::IdentityReadSurfaceKind,
        redaction_marker_ref: IdentityRedactionMarkerRef,
        decided_at: IdentityTimestamp,
    ) -> Self {
        Self {
            decision_ref,
            read_subject_ref,
            visibility_context_ref,
            visibility_scope_ref,
            visibility_result_ref,
            surface_kind,
            disposition: IdentityReadDispositionKind::Redacted,
            redaction_marker_ref: Some(redaction_marker_ref),
            degraded_marker_ref: None,
            decided_at,
        }
    }

    /// Creates a not-visible read decision.
    #[allow(clippy::too_many_arguments)]
    pub fn not_visible(
        decision_ref: identity_contracts::refs::IdentityVisibilityDecisionRef,
        read_subject_ref: IdentityReadSubjectRef,
        visibility_context_ref: VisibilityContextRef,
        visibility_scope_ref: VisibilityScopeRef,
        visibility_result_ref: VisibilityResultRef,
        surface_kind: identity_contracts::refs::IdentityReadSurfaceKind,
        redaction_marker_ref: Option<IdentityRedactionMarkerRef>,
        decided_at: IdentityTimestamp,
    ) -> Self {
        Self {
            decision_ref,
            read_subject_ref,
            visibility_context_ref,
            visibility_scope_ref,
            visibility_result_ref,
            surface_kind,
            disposition: IdentityReadDispositionKind::NotVisible,
            redaction_marker_ref,
            degraded_marker_ref: None,
            decided_at,
        }
    }

    /// Creates a degraded read decision.
    #[allow(clippy::too_many_arguments)]
    pub fn degraded(
        decision_ref: identity_contracts::refs::IdentityVisibilityDecisionRef,
        read_subject_ref: IdentityReadSubjectRef,
        visibility_context_ref: VisibilityContextRef,
        visibility_scope_ref: VisibilityScopeRef,
        visibility_result_ref: VisibilityResultRef,
        surface_kind: identity_contracts::refs::IdentityReadSurfaceKind,
        degraded_marker_ref: IdentityDegradedMarkerRef,
        decided_at: IdentityTimestamp,
    ) -> Self {
        Self {
            decision_ref,
            read_subject_ref,
            visibility_context_ref,
            visibility_scope_ref,
            visibility_result_ref,
            surface_kind,
            disposition: IdentityReadDispositionKind::Degraded,
            redaction_marker_ref: None,
            degraded_marker_ref: Some(degraded_marker_ref),
            decided_at,
        }
    }

    /// Creates a stale-visible read decision.
    #[allow(clippy::too_many_arguments)]
    pub fn stale_visible(
        decision_ref: identity_contracts::refs::IdentityVisibilityDecisionRef,
        read_subject_ref: IdentityReadSubjectRef,
        visibility_context_ref: VisibilityContextRef,
        visibility_scope_ref: VisibilityScopeRef,
        visibility_result_ref: VisibilityResultRef,
        surface_kind: identity_contracts::refs::IdentityReadSurfaceKind,
        degraded_marker_ref: IdentityDegradedMarkerRef,
        decided_at: IdentityTimestamp,
    ) -> Self {
        Self {
            decision_ref,
            read_subject_ref,
            visibility_context_ref,
            visibility_scope_ref,
            visibility_result_ref,
            surface_kind,
            disposition: IdentityReadDispositionKind::StaleVisible,
            redaction_marker_ref: None,
            degraded_marker_ref: Some(degraded_marker_ref),
            decided_at,
        }
    }

    /// Returns whether a safe body-free result body may be surfaced.
    pub fn allows_body_material(&self) -> bool {
        matches!(
            self.disposition,
            IdentityReadDispositionKind::Visible | IdentityReadDispositionKind::StaleVisible
        )
    }

    /// Converts the safe degraded marker into the public degraded shell when needed.
    pub fn degraded_marker(&self) -> Option<identity_contracts::metadata::IdentityDegradedMarker> {
        self.degraded_marker_ref
            .as_ref()
            .map(
                |degraded_marker_ref| identity_contracts::metadata::IdentityDegradedMarker {
                    degraded_marker_ref: degraded_marker_ref.clone(),
                    degraded_kind: IdentityDegradedKind::DependencyUnavailable,
                },
            )
    }

    /// Converts the safe visibility markers into the public visibility shell.
    pub fn visibility_marker(&self) -> identity_contracts::metadata::IdentityVisibilityMarker {
        identity_contracts::metadata::IdentityVisibilityMarker {
            visibility_result_ref: self.visibility_result_ref.clone(),
            read_surface_kind: self.surface_kind,
            redaction_marker_ref: self.redaction_marker_ref.clone(),
        }
    }
}

/// Application-local job report assembly object.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityJobRunReport {
    /// Public report reference.
    pub report_ref: IdentityJobReportRef,
    /// Stable job run reference.
    pub job_run_ref: IdentityJobRunRef,
    /// Stable public job name.
    pub job_name: IdentityJobName,
    /// Body-free job scope marker.
    pub job_scope_ref: IdentityJobScopeMarkerRef,
    /// Optional input cursor marker.
    pub input_cursor_ref: Option<IdentityJobCursorRef>,
    /// Optional output cursor marker.
    pub output_cursor_ref: Option<IdentityJobCursorRef>,
    /// Stable public job result kind.
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
    /// Inspected maintenance target refs.
    pub inspected_target_refs: Vec<IdentityMaintenanceTargetRef>,
    /// Generated reconciliation report refs.
    pub report_refs: Vec<ReconciliationReportRef>,
    /// Touched outbox record refs.
    pub outbox_record_refs: Vec<IdentityOutboxRecordRef>,
    /// Successfully published outbox refs.
    pub published_outbox_refs: Vec<IdentityOutboxRecordRef>,
    /// Failed outbox refs.
    pub failed_outbox_refs: Vec<IdentityOutboxRecordRef>,
    /// Touched handoff intent refs.
    pub handoff_intent_refs: Vec<TraceHandoffIntentRef>,
    /// Delivered handoff intent refs.
    pub delivered_handoff_refs: Vec<TraceHandoffIntentRef>,
    /// Failed handoff intent refs.
    pub failed_handoff_refs: Vec<TraceHandoffIntentRef>,
    /// Formal handoff receipt refs.
    pub handoff_receipt_refs: Vec<HandoffReceiptRef>,
    /// Safe maintenance issue refs.
    pub issue_refs: Vec<MaintenanceIssueRef>,
    /// Optional stored result ref used for duplicate replay.
    pub stored_result_ref: Option<IdentityStoredResultRef>,
    /// Job start timestamp.
    pub started_at: IdentityTimestamp,
    /// Optional finish timestamp.
    pub finished_at: Option<IdentityTimestamp>,
}

impl IdentityJobRunReport {
    /// Creates the initial report object for a job run.
    pub fn start(
        report_ref: IdentityJobReportRef,
        job_run_ref: IdentityJobRunRef,
        job_name: IdentityJobName,
        job_scope_ref: IdentityJobScopeMarkerRef,
        input_cursor_ref: Option<IdentityJobCursorRef>,
        started_at: IdentityTimestamp,
    ) -> Self {
        Self {
            report_ref,
            job_run_ref,
            job_name,
            job_scope_ref,
            input_cursor_ref,
            output_cursor_ref: None,
            result_kind: IdentityJobResultKind::Noop,
            affected_member_refs: Vec::new(),
            affected_projection_refs: Vec::new(),
            rebuilt_projection_refs: Vec::new(),
            failed_projection_refs: Vec::new(),
            refreshed_reference_refs: Vec::new(),
            failed_reference_refs: Vec::new(),
            inspected_target_refs: Vec::new(),
            report_refs: Vec::new(),
            outbox_record_refs: Vec::new(),
            published_outbox_refs: Vec::new(),
            failed_outbox_refs: Vec::new(),
            handoff_intent_refs: Vec::new(),
            delivered_handoff_refs: Vec::new(),
            failed_handoff_refs: Vec::new(),
            handoff_receipt_refs: Vec::new(),
            issue_refs: Vec::new(),
            stored_result_ref: None,
            started_at,
            finished_at: None,
        }
    }

    /// Marks the report as succeeded.
    pub fn succeed(
        mut self,
        output_cursor_ref: Option<IdentityJobCursorRef>,
        stored_result_ref: Option<IdentityStoredResultRef>,
        finished_at: IdentityTimestamp,
    ) -> Self {
        self.result_kind = IdentityJobResultKind::Succeeded;
        self.output_cursor_ref = output_cursor_ref;
        self.stored_result_ref = stored_result_ref;
        self.finished_at = Some(finished_at);
        self
    }

    /// Marks the report as partial.
    pub fn partial(
        mut self,
        issue_refs: Vec<MaintenanceIssueRef>,
        output_cursor_ref: Option<IdentityJobCursorRef>,
        stored_result_ref: Option<IdentityStoredResultRef>,
        finished_at: IdentityTimestamp,
    ) -> Self {
        self.result_kind = IdentityJobResultKind::Partial;
        self.issue_refs = issue_refs;
        self.output_cursor_ref = output_cursor_ref;
        self.stored_result_ref = stored_result_ref;
        self.finished_at = Some(finished_at);
        self
    }

    /// Marks the report as failed.
    pub fn fail(
        mut self,
        issue_refs: Vec<MaintenanceIssueRef>,
        finished_at: IdentityTimestamp,
    ) -> Self {
        self.result_kind = IdentityJobResultKind::Failed;
        self.issue_refs = issue_refs;
        self.finished_at = Some(finished_at);
        self
    }

    /// Marks the report as retryably failed.
    pub fn retryable_fail(
        mut self,
        issue_refs: Vec<MaintenanceIssueRef>,
        finished_at: IdentityTimestamp,
    ) -> Self {
        self.result_kind = IdentityJobResultKind::RetryableFailed;
        self.issue_refs = issue_refs;
        self.finished_at = Some(finished_at);
        self
    }

    /// Marks the report as noop.
    pub fn noop(
        mut self,
        output_cursor_ref: Option<IdentityJobCursorRef>,
        stored_result_ref: Option<IdentityStoredResultRef>,
        finished_at: IdentityTimestamp,
    ) -> Self {
        self.result_kind = IdentityJobResultKind::Noop;
        self.output_cursor_ref = output_cursor_ref;
        self.stored_result_ref = stored_result_ref;
        self.finished_at = Some(finished_at);
        self
    }
}

/// Validation state of a body-free runtime config shell.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityConfigValidationStateKind {
    /// Config has been validated and is ready for assembly.
    Validated,
    /// Config is degraded but may still assemble in a controlled mode.
    Degraded,
    /// Config is invalid and must not assemble.
    Invalid,
}

/// Validated runtime config shell used by runtime assembly and fake runtime tests.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityRuntimeConfigShell {
    /// Formal runtime profile marker.
    pub profile_ref: IdentityRuntimeProfileRef,
    /// Body-free config evidence marker.
    pub config_evidence_ref: IdentityConfigEvidenceRef,
    /// Configured adapter modes.
    pub adapter_mode_refs: Vec<IdentityAdapterModeRef>,
    /// Optional API binding catalog marker.
    pub api_binding_ref: Option<IdentityApiRouteCatalogRef>,
    /// Optional worker binding catalog marker.
    pub worker_binding_ref: Option<IdentityConsumerBindingCatalogRef>,
    /// Optional job binding catalog marker.
    pub job_binding_ref: Option<IdentityJobCatalogRef>,
    /// Safe config issue refs.
    pub issue_refs: Vec<IdentityConfigIssueRef>,
    /// Validation state.
    pub validation_state: IdentityConfigValidationStateKind,
}

impl IdentityRuntimeConfigShell {
    /// Creates a validated config shell.
    pub fn validated(
        profile_ref: IdentityRuntimeProfileRef,
        config_evidence_ref: IdentityConfigEvidenceRef,
        adapter_mode_refs: Vec<IdentityAdapterModeRef>,
        api_binding_ref: Option<IdentityApiRouteCatalogRef>,
        worker_binding_ref: Option<IdentityConsumerBindingCatalogRef>,
        job_binding_ref: Option<IdentityJobCatalogRef>,
    ) -> Self {
        Self {
            profile_ref,
            config_evidence_ref,
            adapter_mode_refs,
            api_binding_ref,
            worker_binding_ref,
            job_binding_ref,
            issue_refs: Vec::new(),
            validation_state: IdentityConfigValidationStateKind::Validated,
        }
    }

    /// Creates a degraded config shell.
    pub fn degraded(
        profile_ref: IdentityRuntimeProfileRef,
        config_evidence_ref: IdentityConfigEvidenceRef,
        adapter_mode_refs: Vec<IdentityAdapterModeRef>,
        api_binding_ref: Option<IdentityApiRouteCatalogRef>,
        worker_binding_ref: Option<IdentityConsumerBindingCatalogRef>,
        job_binding_ref: Option<IdentityJobCatalogRef>,
        issue_refs: Vec<IdentityConfigIssueRef>,
    ) -> Self {
        Self {
            profile_ref,
            config_evidence_ref,
            adapter_mode_refs,
            api_binding_ref,
            worker_binding_ref,
            job_binding_ref,
            issue_refs,
            validation_state: IdentityConfigValidationStateKind::Degraded,
        }
    }

    /// Creates an invalid config shell.
    pub fn invalid(
        profile_ref: IdentityRuntimeProfileRef,
        config_evidence_ref: IdentityConfigEvidenceRef,
        issue_refs: Vec<IdentityConfigIssueRef>,
    ) -> Self {
        Self {
            profile_ref,
            config_evidence_ref,
            adapter_mode_refs: Vec::new(),
            api_binding_ref: None,
            worker_binding_ref: None,
            job_binding_ref: None,
            issue_refs,
            validation_state: IdentityConfigValidationStateKind::Invalid,
        }
    }

    /// Returns whether the shell allows the given entry surface to be wired.
    pub fn allows_entry(&self, surface_kind: IdentityEntrySurfaceKind) -> bool {
        if self.validation_state == IdentityConfigValidationStateKind::Invalid {
            return false;
        }

        match surface_kind {
            IdentityEntrySurfaceKind::ApiCommand | IdentityEntrySurfaceKind::ApiQuery => {
                self.api_binding_ref.is_some()
            }
            IdentityEntrySurfaceKind::WorkerConsumer | IdentityEntrySurfaceKind::WorkerCallback => {
                self.worker_binding_ref.is_some()
            }
            IdentityEntrySurfaceKind::OperationsJob => self.job_binding_ref.is_some(),
        }
    }
}

/// Runtime assembly state kind for entry readiness and fake runtime wiring.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityRuntimeAssemblyStateKind {
    /// Runtime has not started assembly.
    NotStarted,
    /// Config has been validated but ports are not wired yet.
    ConfigValidated,
    /// Runtime is assembled and ready for dispatch.
    Assembled,
    /// Runtime is assembled but degraded.
    Degraded,
    /// Runtime assembly failed.
    Failed,
}

/// Body-free runtime assembly state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityRuntimeAssemblyState {
    /// Stable runtime assembly ref.
    pub assembly_ref: IdentityRuntimeAssemblyRef,
    /// Runtime profile marker.
    pub profile_ref: IdentityRuntimeProfileRef,
    /// Assembly lifecycle state.
    pub state_kind: IdentityRuntimeAssemblyStateKind,
    /// Adapter refs checked during assembly.
    pub adapter_availability_refs: Vec<IdentityAdapterRef>,
    /// Safe issue refs.
    pub issue_refs: Vec<IdentityConfigIssueRef>,
    /// Optional assembly timestamp.
    pub assembled_at: Option<IdentityTimestamp>,
}

impl IdentityRuntimeAssemblyState {
    /// Creates a not-started runtime state.
    pub fn not_started(
        assembly_ref: IdentityRuntimeAssemblyRef,
        profile_ref: IdentityRuntimeProfileRef,
    ) -> Self {
        Self {
            assembly_ref,
            profile_ref,
            state_kind: IdentityRuntimeAssemblyStateKind::NotStarted,
            adapter_availability_refs: Vec::new(),
            issue_refs: Vec::new(),
            assembled_at: None,
        }
    }

    /// Creates a config-validated runtime state.
    pub fn config_validated(
        assembly_ref: IdentityRuntimeAssemblyRef,
        config_shell: &IdentityRuntimeConfigShell,
    ) -> Self {
        Self {
            assembly_ref,
            profile_ref: config_shell.profile_ref.clone(),
            state_kind: IdentityRuntimeAssemblyStateKind::ConfigValidated,
            adapter_availability_refs: Vec::new(),
            issue_refs: config_shell.issue_refs.clone(),
            assembled_at: None,
        }
    }

    /// Creates an assembled runtime state.
    pub fn assembled(
        assembly_ref: IdentityRuntimeAssemblyRef,
        config_shell: &IdentityRuntimeConfigShell,
        adapter_refs: Vec<IdentityAdapterRef>,
        assembled_at: IdentityTimestamp,
    ) -> Self {
        Self {
            assembly_ref,
            profile_ref: config_shell.profile_ref.clone(),
            state_kind: IdentityRuntimeAssemblyStateKind::Assembled,
            adapter_availability_refs: adapter_refs,
            issue_refs: Vec::new(),
            assembled_at: Some(assembled_at),
        }
    }

    /// Creates a degraded runtime state.
    pub fn degraded(
        assembly_ref: IdentityRuntimeAssemblyRef,
        config_shell: &IdentityRuntimeConfigShell,
        adapter_refs: Vec<IdentityAdapterRef>,
        issue_refs: Vec<IdentityConfigIssueRef>,
        assembled_at: IdentityTimestamp,
    ) -> Self {
        Self {
            assembly_ref,
            profile_ref: config_shell.profile_ref.clone(),
            state_kind: IdentityRuntimeAssemblyStateKind::Degraded,
            adapter_availability_refs: adapter_refs,
            issue_refs,
            assembled_at: Some(assembled_at),
        }
    }

    /// Creates a failed runtime state.
    pub fn failed(
        assembly_ref: IdentityRuntimeAssemblyRef,
        config_shell: &IdentityRuntimeConfigShell,
        issue_refs: Vec<IdentityConfigIssueRef>,
    ) -> Self {
        Self {
            assembly_ref,
            profile_ref: config_shell.profile_ref.clone(),
            state_kind: IdentityRuntimeAssemblyStateKind::Failed,
            adapter_availability_refs: Vec::new(),
            issue_refs,
            assembled_at: None,
        }
    }

    /// Returns whether the runtime may attempt dispatch for an entry surface.
    pub fn can_dispatch(&self, _surface_kind: IdentityEntrySurfaceKind) -> bool {
        matches!(
            self.state_kind,
            IdentityRuntimeAssemblyStateKind::Assembled
                | IdentityRuntimeAssemblyStateKind::Degraded
        )
    }
}

/// Adapter availability kind for runtime assembly and controlled/fake wiring.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityAdapterAvailabilityKind {
    /// Adapter is available.
    Available,
    /// Adapter is degraded but still visible to callers.
    Degraded,
    /// Adapter is unavailable.
    Unavailable,
    /// Adapter is disabled by configuration.
    Disabled,
}

/// Body-free adapter availability snapshot.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityAdapterAvailability {
    /// Adapter identity marker.
    pub adapter_ref: IdentityAdapterRef,
    /// Adapter mode marker.
    pub adapter_mode_ref: IdentityAdapterModeRef,
    /// Availability kind.
    pub availability_kind: IdentityAdapterAvailabilityKind,
    /// Optional availability issue marker.
    pub issue_ref: Option<IdentityAdapterAvailabilityIssueRef>,
    /// Availability check timestamp.
    pub checked_at: IdentityTimestamp,
}

impl IdentityAdapterAvailability {
    /// Creates an available adapter snapshot.
    pub fn available(
        adapter_ref: IdentityAdapterRef,
        adapter_mode_ref: IdentityAdapterModeRef,
        checked_at: IdentityTimestamp,
    ) -> Self {
        Self {
            adapter_ref,
            adapter_mode_ref,
            availability_kind: IdentityAdapterAvailabilityKind::Available,
            issue_ref: None,
            checked_at,
        }
    }

    /// Creates a degraded adapter snapshot.
    pub fn degraded(
        adapter_ref: IdentityAdapterRef,
        adapter_mode_ref: IdentityAdapterModeRef,
        issue_ref: IdentityAdapterAvailabilityIssueRef,
        checked_at: IdentityTimestamp,
    ) -> Self {
        Self {
            adapter_ref,
            adapter_mode_ref,
            availability_kind: IdentityAdapterAvailabilityKind::Degraded,
            issue_ref: Some(issue_ref),
            checked_at,
        }
    }

    /// Creates an unavailable adapter snapshot.
    pub fn unavailable(
        adapter_ref: IdentityAdapterRef,
        adapter_mode_ref: IdentityAdapterModeRef,
        issue_ref: IdentityAdapterAvailabilityIssueRef,
        checked_at: IdentityTimestamp,
    ) -> Self {
        Self {
            adapter_ref,
            adapter_mode_ref,
            availability_kind: IdentityAdapterAvailabilityKind::Unavailable,
            issue_ref: Some(issue_ref),
            checked_at,
        }
    }

    /// Creates a disabled adapter snapshot.
    pub fn disabled(
        adapter_ref: IdentityAdapterRef,
        adapter_mode_ref: IdentityAdapterModeRef,
        issue_ref: IdentityAdapterAvailabilityIssueRef,
        checked_at: IdentityTimestamp,
    ) -> Self {
        Self {
            adapter_ref,
            adapter_mode_ref,
            availability_kind: IdentityAdapterAvailabilityKind::Disabled,
            issue_ref: Some(issue_ref),
            checked_at,
        }
    }

    /// Returns whether the runtime may attempt this adapter.
    pub fn allows_attempt(&self) -> bool {
        matches!(
            self.availability_kind,
            IdentityAdapterAvailabilityKind::Available | IdentityAdapterAvailabilityKind::Degraded
        )
    }
}

/// Entry validation kind shared by API, worker, and job entry shells.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityEntryValidationKind {
    /// Entry is dispatchable.
    Dispatchable,
    /// Entry is rejected before application dispatch.
    RejectedAtEntry,
    /// Entry is not routable.
    NotRoutable,
    /// Runtime is unavailable.
    RuntimeUnavailable,
}

/// Entry dispatch attempt kind shared by API, worker, and job entry shells.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityEntryDispatchKind {
    /// Entry has been dispatched to the application facade.
    Dispatched,
    /// Entry was skipped because it was rejected at the entry boundary.
    SkippedRejectedAtEntry,
    /// Entry was skipped because runtime was unavailable.
    SkippedRuntimeUnavailable,
    /// Dispatch failed before the application facade was called.
    FailedBeforeApplication,
}

/// Body-free API entry context.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityApiEntryContext {
    /// Stable API entry ref.
    pub api_entry_ref: IdentityApiEntryRef,
    /// Formal route ref.
    pub route_ref: IdentityApiRouteRef,
    /// Entry surface kind.
    pub surface_kind: IdentityEntrySurfaceKind,
    /// Body-free request marker.
    pub request_marker_ref: IdentityApiRequestMarkerRef,
    /// Actor extracted at the boundary.
    pub actor_ref: ActorRef,
    /// Body-free request metadata marker.
    pub request_metadata_ref: IdentityRequestMetadataRef,
    /// Optional idempotency key for mutation routes.
    pub idempotency_key: Option<IdentityIdempotencyKey>,
    /// Optional visibility context for query routes.
    pub visibility_context_ref: Option<VisibilityContextRef>,
    /// Entry receive timestamp.
    pub received_at: IdentityTimestamp,
}

/// Body-free API entry validation result.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityApiEntryValidation {
    /// API entry ref.
    pub api_entry_ref: IdentityApiEntryRef,
    /// Formal route ref.
    pub route_ref: IdentityApiRouteRef,
    /// Validation kind.
    pub validation_kind: IdentityEntryValidationKind,
    /// Safe issue refs.
    pub issue_refs: Vec<IdentityEntryValidationIssueRef>,
}

/// Body-free API dispatch result.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityApiDispatchResult {
    /// Dispatch attempt ref.
    pub dispatch_ref: IdentityEntryDispatchRef,
    /// API entry ref.
    pub api_entry_ref: IdentityApiEntryRef,
    /// Application dispatch target ref.
    pub target_ref: IdentityDispatchTargetRef,
    /// Dispatch attempt kind.
    pub dispatch_kind: IdentityEntryDispatchKind,
    /// Safe issue refs.
    pub issue_refs: Vec<IdentityEntryValidationIssueRef>,
}

/// Worker entry validation kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityWorkerEntryValidationKind {
    /// Entry is dispatchable.
    Dispatchable,
    /// Consumer binding is unrecognized.
    UnrecognizedBinding,
    /// Dedupe key is missing.
    MissingDedupeKey,
    /// Envelope marker is invalid.
    InvalidEnvelopeMarker,
    /// Runtime is unavailable.
    RuntimeUnavailable,
}

/// Body-free worker entry context.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityWorkerEntryContext {
    /// Stable worker entry ref.
    pub worker_entry_ref: IdentityWorkerEntryRef,
    /// Entry surface kind.
    pub surface_kind: IdentityEntrySurfaceKind,
    /// Consumer binding ref.
    pub consumer_binding_ref: IdentityConsumerBindingRef,
    /// Body-free envelope marker.
    pub envelope_marker_ref: IdentityEventEnvelopeMarkerRef,
    /// Upstream source event ref.
    pub source_event_ref: IdentitySourceEventRef,
    /// Formal idempotency key.
    pub idempotency_key: IdentityIdempotencyKey,
    /// Optional propagated trace context marker.
    pub trace_context_ref: Option<IdentityTraceContextRef>,
    /// Entry receive timestamp.
    pub received_at: IdentityTimestamp,
}

/// Body-free worker entry validation result.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityWorkerEntryValidation {
    /// Worker entry ref.
    pub worker_entry_ref: IdentityWorkerEntryRef,
    /// Consumer binding ref.
    pub consumer_binding_ref: IdentityConsumerBindingRef,
    /// Validation kind.
    pub validation_kind: IdentityWorkerEntryValidationKind,
    /// Safe issue refs.
    pub issue_refs: Vec<IdentityEntryValidationIssueRef>,
}

/// Body-free worker dispatch result.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityWorkerDispatchResult {
    /// Dispatch attempt ref.
    pub dispatch_ref: IdentityWorkerDispatchRef,
    /// Worker entry ref.
    pub worker_entry_ref: IdentityWorkerEntryRef,
    /// Application dispatch target ref.
    pub target_ref: IdentityDispatchTargetRef,
    /// Dispatch attempt kind.
    pub dispatch_kind: IdentityEntryDispatchKind,
    /// Safe issue refs.
    pub issue_refs: Vec<IdentityEntryValidationIssueRef>,
}

/// Job entry validation kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityJobEntryValidationKind {
    /// Entry is dispatchable.
    Dispatchable,
    /// Job name is unknown.
    UnknownJob,
    /// Scope marker is invalid.
    InvalidScope,
    /// Cursor marker is invalid.
    InvalidCursor,
    /// Idempotency key is missing.
    MissingIdempotencyKey,
    /// Runtime is unavailable.
    RuntimeUnavailable,
}

/// Body-free job entry context.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityJobEntryContext {
    /// Stable job entry ref.
    pub job_entry_ref: IdentityJobEntryRef,
    /// Formal job name.
    pub job_name: IdentityJobName,
    /// Formal job run ref.
    pub job_run_ref: IdentityJobRunRef,
    /// Body-free job run metadata marker.
    pub run_metadata_ref: IdentityJobRunMetadataRef,
    /// Body-free job scope marker.
    pub scope_marker_ref: IdentityJobScopeMarkerRef,
    /// Optional input cursor marker.
    pub input_cursor_ref: Option<IdentityJobCursorRef>,
    /// System actor for the run.
    pub system_actor_ref: ActorRef,
    /// Formal idempotency key.
    pub idempotency_key: IdentityIdempotencyKey,
    /// Job start timestamp.
    pub started_at: IdentityTimestamp,
}

/// Body-free job entry validation result.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityJobEntryValidation {
    /// Job entry ref.
    pub job_entry_ref: IdentityJobEntryRef,
    /// Formal job name.
    pub job_name: IdentityJobName,
    /// Validation kind.
    pub validation_kind: IdentityJobEntryValidationKind,
    /// Safe issue refs.
    pub issue_refs: Vec<IdentityEntryValidationIssueRef>,
}

/// Body-free job dispatch result.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityJobDispatchResult {
    /// Dispatch attempt ref.
    pub dispatch_ref: IdentityJobDispatchRef,
    /// Job entry ref.
    pub job_entry_ref: IdentityJobEntryRef,
    /// Application dispatch target ref.
    pub target_ref: IdentityDispatchTargetRef,
    /// Dispatch attempt kind.
    pub dispatch_kind: IdentityEntryDispatchKind,
    /// Safe issue refs.
    pub issue_refs: Vec<IdentityEntryValidationIssueRef>,
}

/// Convenience alias for API request markers that already exist in shared contracts.
pub type IdentityPublicRequestMarkerRef = IdentityApiRequestMarkerRef;
/// Convenience alias for public consumer binding refs that already exist in shared contracts.
pub type IdentityPublicConsumerBindingRef = IdentityConsumerBindingRef;
/// Convenience alias for public consumer receipt refs that already exist in shared contracts.
pub type IdentityPublicConsumerReceiptRef = IdentityConsumerReceiptRef;
/// Convenience alias for public job metadata refs that already exist in shared contracts.
pub type IdentityPublicJobRunMetadataRef = IdentityJobRunMetadataRef;
