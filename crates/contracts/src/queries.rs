//! Public query envelopes, page shells, and typed query DTOs.

use core_contracts::actor::ActorRef;
use serde::{Deserialize, Serialize};

use crate::metadata::{IdentityQueryMetadata, IdentityQuerySurface};
use crate::protocol::IdentityQueryName;
use crate::refs::{
    AuditCursorRef, AuditScopeRef, ConsumerRef, GlobalMemberRef, IdentityChangeKindRef,
    IdentityTraceSubjectRef, IdentityTruthCursor, RoleCapabilitySummaryRef,
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
