//! Shared metadata shells reused by command, query, event, and job contracts.

use serde::{Deserialize, Serialize};

use crate::protocol::IdentityProtocolSurfaceRef;
use crate::protocol::{IdentityDigestAlgorithmMarkerRef, IdentityProtocolSchemaVersionRef};
use crate::refs::{
    IdentityApiRequestMarkerRef, IdentityCanonicalRequestMarkerRef, IdentityDegradedMarkerRef,
    IdentityReadSurfaceKind, IdentityRedactionMarkerRef, IdentityRequestDigestValue,
    IdentityTraceContextRef, IdentityVisibilityDecisionRef, ProjectionFreshnessMarkerRef,
    VisibilityContextRef, VisibilityResultRef,
};

/// Public metadata carried by command requests.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityCommandMetadata {
    /// Public idempotency key for the write request.
    pub idempotency_key: core_contracts::metadata::IdempotencyKey,
    /// Body-free request marker.
    pub request_marker_ref: IdentityApiRequestMarkerRef,
    /// Canonical protocol schema version marker.
    pub schema_version_ref: IdentityProtocolSchemaVersionRef,
    /// Optional propagated trace context marker.
    pub trace_context_ref: Option<IdentityTraceContextRef>,
}

/// Public metadata carried by query requests.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityQueryMetadata {
    /// Body-free request marker.
    pub request_marker_ref: IdentityApiRequestMarkerRef,
    /// Canonical protocol schema version marker.
    pub schema_version_ref: IdentityProtocolSchemaVersionRef,
    /// Visibility context marker extracted at the entry boundary.
    pub visibility_context_ref: VisibilityContextRef,
    /// Optional propagated trace context marker.
    pub trace_context_ref: Option<IdentityTraceContextRef>,
}

/// Public digest marker produced by entry canonicalization.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityRequestDigestMarker {
    /// Body-free canonical material marker.
    pub canonical_marker_ref: IdentityCanonicalRequestMarkerRef,
    /// Stable digest value for duplicate and conflict checks.
    pub digest_value: IdentityRequestDigestValue,
    /// Canonical schema version marker.
    pub schema_version_ref: IdentityProtocolSchemaVersionRef,
    /// Digest algorithm binding marker.
    pub algorithm_marker_ref: IdentityDigestAlgorithmMarkerRef,
}

/// Public visibility marker copied from a body-free visibility decision.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityVisibilityMarker {
    /// Visibility result marker copied from the application decision.
    pub visibility_result_ref: VisibilityResultRef,
    /// Public read surface kind.
    pub read_surface_kind: IdentityReadSurfaceKind,
    /// Optional redaction marker for field-level safe redaction.
    pub redaction_marker_ref: Option<IdentityRedactionMarkerRef>,
}

/// Public degraded marker copied from safe resolver or dependency summaries.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityDegradedMarker {
    /// Safe degraded marker reference.
    pub degraded_marker_ref: IdentityDegradedMarkerRef,
    /// Public degraded category.
    pub degraded_kind: IdentityDegradedKind,
}

/// Public degraded category.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityDegradedKind {
    /// A required dependency is unavailable.
    DependencyUnavailable,
    /// A required source is unavailable.
    SourceUnavailable,
    /// The backing projection is stale.
    ProjectionStale,
    /// The backing projection is rebuilding.
    ProjectionRebuilding,
    /// The requested material is unsafe to expose.
    MaterialUnsafe,
    /// The result is partial.
    PartialResult,
    /// A required adapter is unavailable.
    AdapterUnavailable,
    /// The read or entry surface is disabled.
    Disabled,
}

/// Public query surface shared by single and paged read responses.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityQuerySurface {
    /// High-level read disposition.
    pub disposition: IdentityQueryDisposition,
    /// Public visibility marker.
    pub visibility: IdentityVisibilityMarker,
    /// Optional degraded marker.
    pub degraded: Option<IdentityDegradedMarker>,
    /// Optional projection freshness marker.
    pub projection_freshness_ref: Option<ProjectionFreshnessMarkerRef>,
    /// Optional visibility decision reference.
    pub decision_ref: Option<IdentityVisibilityDecisionRef>,
}

/// Public read disposition category.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityQueryDisposition {
    /// Visible response with full data.
    Visible,
    /// Visible response with field-level redaction.
    Redacted,
    /// Not visible response.
    NotVisible,
    /// Degraded response.
    Degraded,
    /// Visible but stale response.
    StaleVisible,
    /// Empty successful response.
    Empty,
    /// Missing target response.
    Missing,
    /// Rebuilding response.
    Rebuilding,
    /// Disabled surface response.
    Disabled,
}

/// Safe protocol validation issue reference.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IdentityProtocolValidationIssueRef(String);

impl IdentityProtocolValidationIssueRef {
    /// Creates a new issue marker.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }
}

/// Group of validation issue references.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct IdentityProtocolValidationIssueRefSet(pub Vec<IdentityProtocolValidationIssueRef>);

/// Public rejection shell returned by command handlers and duplicate replay.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdentityProtocolRejection {
    /// Protocol surface being rejected.
    pub surface_ref: IdentityProtocolSurfaceRef,
    /// Stable rejection kind.
    pub rejection_kind: IdentityProtocolRejectionKind,
    /// Safe issue markers explaining the rejection.
    pub issue_refs: IdentityProtocolValidationIssueRefSet,
    /// Optional degraded marker when the rejection is caused by safe dependency failure.
    pub degraded: Option<IdentityDegradedMarker>,
}

/// Stable public rejection kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityProtocolRejectionKind {
    /// Request validation failed.
    InvalidRequest,
    /// Forbidden body or material was detected.
    ForbiddenBody,
    /// Policy rejected the request.
    PolicyDenied,
    /// Target was not found.
    NotFound,
    /// Write precondition or state conflicted.
    Conflict,
    /// Duplicate request conflicted with existing canonical material.
    DuplicateConflict,
    /// Schema version is unsupported.
    UnsupportedVersion,
    /// Required adapter is unavailable.
    AdapterUnavailable,
    /// Surface is disabled.
    Disabled,
}
