//! Lightweight ID value objects used to keep repository boundaries explicit.

macro_rules! define_id_type {
    ($name:ident) => {
        #[doc = "Stable identifier value object."]
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        pub struct $name(String);

        impl $name {
            /// Creates a new identifier from a validated string value.
            pub fn new(value: impl Into<String>) -> Self {
                Self(value.into())
            }

            /// Returns the identifier as a string slice.
            pub fn as_str(&self) -> &str {
                self.0.as_str()
            }
        }

        impl From<String> for $name {
            fn from(value: String) -> Self {
                Self(value)
            }
        }

        impl From<&str> for $name {
            fn from(value: &str) -> Self {
                Self(value.to_string())
            }
        }
    };
}

define_id_type!(GlobalMemberId);
define_id_type!(CapabilityProfileId);
define_id_type!(MemoryRefsId);
define_id_type!(CareerEntryId);
define_id_type!(PendingFlowId);
define_id_type!(OutboxEventId);
define_id_type!(AuditTraceId);
define_id_type!(DeadLetterId);
define_id_type!(RoleId);
define_id_type!(ProjectId);
define_id_type!(EventId);
define_id_type!(GateDecisionId);
