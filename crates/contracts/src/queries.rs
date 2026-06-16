//! Public query envelopes, page shells, and typed query DTOs.

use core_contracts::actor::ActorRef;
use serde::{Deserialize, Serialize};

use crate::metadata::{IdentityQueryMetadata, IdentityQuerySurface};
use crate::protocol::IdentityQueryName;
use crate::receipts::TraceHandoffIntentRef;
use crate::refs::{
    AuditCursorRef, AuditScopeRef, ConsumerRef, ExternalReferenceRef, GlobalMemberRef,
    IdentityChangeKindRef, IdentityOutboxRecordRef, IdentityOutboxSubjectRef,
    IdentityProjectionRef, IdentityReferenceOwnerRef, IdentityTraceRecordRef,
    IdentityTraceSubjectRef, IdentityTruthCursor, MaintenanceScopeRef, ProjectionStateRef,
    ReconciliationReportRef, RoleCapabilitySummaryRef, TopicKeyRef,
};

/// Public query request envelope.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityQueryRequest<T> {
    /// Caller actor reference extracted at the entry boundary.
    pub actor_ref: ActorRef,
    /// Stable public query name.
    pub query_name: IdentityQueryName,
    /// Public query metadata shell.
    pub metadata: IdentityQueryMetadata,
    /// Optional paging request.
    pub page: Option<IdentityPublicPageRequest>,
    /// Typed query body.
    pub body: T,
}

/// Public single-object query response.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityQueryResponse<T> {
    /// Stable public query name.
    pub query_name: IdentityQueryName,
    /// Public read surface shell.
    pub surface: IdentityQuerySurface,
    /// Optional typed response body.
    pub body: Option<T>,
}

/// Public paged query response.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityPageResponse<T> {
    /// Stable public query name.
    pub query_name: IdentityQueryName,
    /// Public read surface shell.
    pub surface: IdentityQuerySurface,
    /// Public page information shell.
    pub page_info: IdentityPublicPageInfo,
    /// Typed page items.
    pub items: Vec<T>,
}

/// Public paging request shell.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityPublicPageRequest {
    /// Optional public page cursor.
    pub cursor: Option<IdentityPublicPageCursor>,
    /// Requested item limit.
    pub limit: u32,
}

/// Public paging information shell.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityPublicPageInfo {
    /// Optional next page cursor.
    pub next_cursor: Option<IdentityPublicPageCursor>,
    /// Indicates whether additional items remain.
    pub has_more: bool,
    /// Number of items returned in the page.
    pub item_count: u32,
}

/// Public page cursor shell.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IdentityPublicPageCursor(String);

impl IdentityPublicPageCursor {
    /// Creates a new public page cursor.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the wrapped cursor string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Request body for reading the platform-level anchor state of a global member.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GetGlobalMemberAnchorRequest {
    /// Member whose anchor state is being read.
    pub member_ref: GlobalMemberRef,
    /// Boundary consumer requesting the anchor material.
    pub consumer_ref: ConsumerRef,
}

/// Request body for reading a member global lifecycle summary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GetGlobalLifecycleSummaryRequest {
    /// Member whose lifecycle state is being read.
    pub member_ref: GlobalMemberRef,
    /// Boundary consumer requesting lifecycle material.
    pub consumer_ref: ConsumerRef,
}

/// Request body for reading a member role/capability summary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GetRoleCapabilitySummaryRequest {
    /// Member whose role/capability summary is being read.
    pub member_ref: GlobalMemberRef,
    /// Boundary consumer requesting role/capability material.
    pub consumer_ref: ConsumerRef,
    /// Optional explicit summary ref. When absent, the current member summary is read.
    pub summary_ref: Option<RoleCapabilitySummaryRef>,
}

/// Request body for listing append-only career records of a member.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ListCareerRecordsRequest {
    /// Member whose career records are being listed.
    pub member_ref: GlobalMemberRef,
    /// Boundary consumer requesting career material.
    pub consumer_ref: ConsumerRef,
}

/// Request body for listing memory/archive references of a member.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ListMemoryReferencesRequest {
    /// Member whose memory references are being listed.
    pub member_ref: GlobalMemberRef,
    /// Boundary consumer requesting memory reference material.
    pub consumer_ref: ConsumerRef,
}

/// Request body for reading the unified body-free member summary.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReadMemberSummaryRequest {
    /// Member whose identity summary is being read.
    pub member_ref: GlobalMemberRef,
    /// Boundary consumer requesting the summary material.
    pub consumer_ref: ConsumerRef,
}

/// Supported trace read selectors.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityTraceReadSelector {
    /// Read trace records by member.
    ByMember { member_ref: GlobalMemberRef },
    /// Read trace records by trace subject, optionally after a committed truth cursor.
    BySubject {
        member_ref: GlobalMemberRef,
        subject_ref: IdentityTraceSubjectRef,
        after_cursor_ref: Option<IdentityTruthCursor>,
    },
    /// Read trace records for a member and change kind.
    ByMemberAndChangeKind {
        member_ref: GlobalMemberRef,
        change_kind_ref: IdentityChangeKindRef,
    },
}

/// Request body for reading body-free identity trace material.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReadIdentityTraceRequest {
    /// Trace selector mapped to a Step 7 repository read surface.
    pub selector: IdentityTraceReadSelector,
    /// Boundary consumer requesting trace material.
    pub consumer_ref: ConsumerRef,
}

/// Request body for reading a member canonical audit trail.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReadAuditTrailRequest {
    /// Member whose canonical audit subject timeline is being read.
    pub member_ref: GlobalMemberRef,
    /// Audit scope requested by the caller.
    pub audit_scope_ref: AuditScopeRef,
    /// Optional audit cursor. This is not a truth cursor and not a page cursor.
    pub audit_cursor_ref: Option<AuditCursorRef>,
    /// Boundary consumer requesting audit material.
    pub consumer_ref: ConsumerRef,
}

/// Request body for reading projection freshness without triggering rebuild.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GetProjectionStateRequest {
    /// Projection or derived view being inspected.
    pub projection_ref: IdentityProjectionRef,
    /// Optional stable projection state ref supplied by a previous lookup or result.
    pub projection_state_ref: Option<ProjectionStateRef>,
    /// Boundary consumer requesting operations state.
    pub consumer_ref: ConsumerRef,
}

/// Request body for reading stored external reference resolution state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GetReferenceResolutionStateRequest {
    /// External reference bundle being inspected.
    pub external_reference_ref: ExternalReferenceRef,
    /// Optional expected local owner for the reference.
    pub owner_ref: Option<IdentityReferenceOwnerRef>,
    /// Boundary consumer requesting reference state.
    pub consumer_ref: ConsumerRef,
}

/// Request body for reading report-only reconciliation reports.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReadReconciliationReportRequest {
    /// Maintenance scope whose reports are being read.
    pub maintenance_scope_ref: MaintenanceScopeRef,
    /// Optional exact report ref. When present the response page contains at most one item.
    pub report_ref: Option<ReconciliationReportRef>,
    /// Boundary consumer requesting report material.
    pub consumer_ref: ConsumerRef,
}

/// Supported outbox list selectors.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityOutboxListSelector {
    /// List pending publish records, optionally by topic.
    Pending { topic_key_ref: Option<TopicKeyRef> },
    /// List retryable failed records, optionally by topic.
    Retryable { topic_key_ref: Option<TopicKeyRef> },
    /// List records by formal outbox subject.
    BySubject {
        subject_ref: IdentityOutboxSubjectRef,
    },
    /// List member records through the formal accepted subject mapper.
    ByMember { member_ref: GlobalMemberRef },
    /// List outbox records linked to an accepted trace record.
    ByTrace {
        trace_record_ref: IdentityTraceRecordRef,
    },
}

/// Request body for listing body-free identity outbox state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ListPendingIdentityOutboxRequest {
    /// Selector mapped to a Step 7 outbox repository read surface.
    pub selector: IdentityOutboxListSelector,
    /// Boundary consumer requesting outbox material.
    pub consumer_ref: ConsumerRef,
}

/// Request body for reading one outbox record state.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GetIdentityOutboxStateRequest {
    /// Outbox record being inspected.
    pub outbox_record_ref: IdentityOutboxRecordRef,
    /// Boundary consumer requesting outbox state.
    pub consumer_ref: ConsumerRef,
}

/// Request body for reading trace handoff state without delivery side effects.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GetTraceHandoffStateRequest {
    /// Handoff intent being inspected.
    pub handoff_intent_ref: TraceHandoffIntentRef,
    /// Boundary consumer requesting handoff state.
    pub consumer_ref: ConsumerRef,
}
