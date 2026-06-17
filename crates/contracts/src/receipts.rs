//! Public receipt and report support refs.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::refs::IdentitySourceRef;

macro_rules! string_newtype {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Creates a new typed marker.
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            /// Returns the wrapped string.
            pub fn as_str(&self) -> &str {
                &self.0
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

/// Maintenance issue category.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceIssueKind {
    /// Projection or reference is stale.
    Stale,
    /// External dependency is unavailable.
    Unavailable,
    /// External reference cannot be recognized.
    Unrecognized,
    /// Derived material is incomplete.
    Partial,
    /// Drift was detected.
    DriftDetected,
    /// Maintenance execution failed.
    Failed,
    /// Forbidden material was detected.
    ForbiddenBody,
}

/// Body-free maintenance issue marker.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct MaintenanceIssueRef {
    /// Issue category.
    pub issue_kind: MaintenanceIssueKind,
    /// Opaque issue source marker.
    pub issue_ref: IdentitySourceRef,
}

impl MaintenanceIssueRef {
    /// Creates a new body-free maintenance issue marker.
    pub fn new(issue_kind: MaintenanceIssueKind, issue_ref: IdentitySourceRef) -> Self {
        Self {
            issue_kind,
            issue_ref,
        }
    }
}

string_newtype!(TraceHandoffIntentRef, "Trace handoff intent reference.");
