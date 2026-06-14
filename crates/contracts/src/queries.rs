//! Public query envelopes and page shells.

use core_contracts::actor::ActorRef;
use serde::{Deserialize, Serialize};

use crate::metadata::{IdentityQueryMetadata, IdentityQuerySurface};
use crate::protocol::IdentityQueryName;

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
}
