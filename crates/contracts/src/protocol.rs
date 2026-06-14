//! Shared protocol naming and canonicalization markers.

use std::fmt;

use serde::{Deserialize, Serialize};

macro_rules! string_newtype {
    ($name:ident, $doc:literal) => {
        #[doc = $doc]
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
        #[serde(transparent)]
        pub struct $name(String);

        impl $name {
            /// Creates a new protocol marker.
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

string_newtype!(
    IdentityCommandName,
    "Public identity command protocol name."
);
string_newtype!(IdentityQueryName, "Public identity query protocol name.");
string_newtype!(
    IdentityInboundConsumerName,
    "Public identity inbound consumer or callback protocol name."
);
string_newtype!(
    IdentityOutboundEventName,
    "Public identity outbound event protocol name."
);
string_newtype!(
    IdentityJobName,
    "Public identity operations job protocol name."
);
string_newtype!(
    IdentityProtocolSurfaceRef,
    "Public protocol surface marker."
);
string_newtype!(
    IdentityProtocolSchemaVersionRef,
    "Canonical public protocol schema version marker."
);
string_newtype!(
    IdentityDigestAlgorithmMarkerRef,
    "Canonical digest algorithm binding marker."
);
