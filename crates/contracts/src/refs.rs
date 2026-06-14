//! Typed refs and shared markers for identity public contracts.

use std::fmt;

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

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }
    };
}

string_newtype!(
    GlobalMemberRef,
    "Stable opaque reference for a global member."
);
string_newtype!(
    IdentityApiRequestMarkerRef,
    "Body-free API request material marker."
);
string_newtype!(
    IdentityAuditSubjectRef,
    "Canonical audit subject reference."
);
string_newtype!(
    IdentityCanonicalRequestMarkerRef,
    "Canonical request material marker."
);
string_newtype!(
    IdentityConsumerBindingRef,
    "Inbound consumer binding marker."
);
string_newtype!(
    IdentityConsumerReceiptRef,
    "Public consumer receipt reference."
);
string_newtype!(IdentityDegradedMarkerRef, "Safe degraded marker reference.");
string_newtype!(
    IdentityEventEnvelopeMarkerRef,
    "Inbound event envelope marker."
);
string_newtype!(IdentityJobCursorRef, "Operations job cursor marker.");
string_newtype!(IdentityJobReportRef, "Public job report reference.");
string_newtype!(
    IdentityJobRunMetadataRef,
    "Body-free job run metadata marker."
);
string_newtype!(IdentityJobRunRef, "Operations job run reference.");
string_newtype!(IdentityJobScopeMarkerRef, "Operations job scope marker.");
string_newtype!(IdentityMaintenanceTargetRef, "Maintenance target marker.");
string_newtype!(
    IdentityOutboxPayloadMarkerRef,
    "Body-free outbound payload marker."
);
string_newtype!(IdentityOutboxRecordRef, "Identity outbox record reference.");
string_newtype!(
    IdentityOutboxSubjectRef,
    "Canonical outbound subject reference."
);
string_newtype!(IdentityProjectionRef, "Projection reference.");
string_newtype!(
    IdentityRedactionMarkerRef,
    "Safe redaction marker reference."
);
string_newtype!(
    IdentityRequestDigestValue,
    "Canonical request digest value."
);
string_newtype!(IdentitySourceEventRef, "Inbound source event reference.");
string_newtype!(IdentityStoredResultRef, "Stored replay surface reference.");
string_newtype!(IdentityTraceContextRef, "Runtime trace context marker.");
string_newtype!(IdentityTraceRecordRef, "Identity trace record reference.");
string_newtype!(
    IdentityTruthCursor,
    "Committed identity truth cursor marker."
);
string_newtype!(ReconciliationReportRef, "Reconciliation report reference.");
string_newtype!(TopicKeyRef, "Topic binding key marker.");
string_newtype!(VisibilityContextRef, "Visibility context marker.");
string_newtype!(VisibilityResultRef, "Visibility result marker.");
string_newtype!(VisibilityScopeRef, "Visibility scope marker.");
string_newtype!(
    IdentityVisibilityDecisionRef,
    "Public visibility decision reference."
);
string_newtype!(HandoffReceiptRef, "Formal handoff receipt reference.");

/// Identity-side timestamp captured from the configured clock source.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct IdentityTimestamp {
    /// Milliseconds since Unix epoch from the configured clock source.
    pub epoch_millis: i64,
}

impl IdentityTimestamp {
    /// Builds a timestamp from a validated clock value.
    pub fn from_clock(epoch_millis: i64) -> Result<Self, crate::errors::ContractError> {
        if epoch_millis < 0 {
            return Err(crate::errors::ContractError::invalid_value(
                "identity_timestamp",
                "epoch_millis must be non-negative",
            ));
        }

        Ok(Self { epoch_millis })
    }

    /// Returns whether two timestamps refer to the same instant.
    pub fn same_instant(&self, other: &IdentityTimestamp) -> bool {
        self.epoch_millis == other.epoch_millis
    }

    /// Returns whether this timestamp is after the other timestamp.
    pub fn is_after(&self, other: &IdentityTimestamp) -> bool {
        self.epoch_millis > other.epoch_millis
    }
}

/// Public read surface kind used by query visibility and read shells.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IdentityReadSurfaceKind {
    /// Member summary read surface.
    Summary,
    /// Identity trace read surface.
    Trace,
    /// Audit trail read surface.
    Audit,
    /// Projection state read surface.
    Projection,
    /// Reference resolution read surface.
    Reference,
    /// Reconciliation report read surface.
    Report,
    /// Outbox read surface.
    Outbox,
    /// Handoff read surface.
    Handoff,
}

/// Public projection freshness marker reused by query and job shells.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ProjectionFreshnessMarkerRef {
    /// Projection being described.
    pub projection_ref: IdentityProjectionRef,
    /// Public freshness state copied from projection state.
    pub state_kind: String,
}
