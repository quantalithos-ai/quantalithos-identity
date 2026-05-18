//! Request-scoped metadata value objects for commands and inbound events.

/// Carries trace, idempotency, and hashing metadata for command or event handling.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandMetadata {
    /// Idempotency key used to deduplicate repeated requests or events.
    pub idempotency_key: String,
    /// Trace identifier used to correlate a handling flow across adapters.
    pub trace_id: String,
    /// Stable hash of the inbound payload for conflict detection.
    pub request_hash: String,
}

impl CommandMetadata {
    /// Creates metadata from trusted caller-provided values.
    pub fn new(
        idempotency_key: impl Into<String>,
        trace_id: impl Into<String>,
        request_hash: impl Into<String>,
    ) -> Self {
        Self {
            idempotency_key: idempotency_key.into(),
            trace_id: trace_id.into(),
            request_hash: request_hash.into(),
        }
    }

    /// Returns the idempotency key for the current request.
    pub fn idempotency_key(&self) -> &str {
        self.idempotency_key.as_str()
    }

    /// Returns the trace identifier for the current request.
    pub fn trace_id(&self) -> &str {
        self.trace_id.as_str()
    }

    /// Returns the request hash used for conflict detection.
    pub fn request_hash(&self) -> &str {
        self.request_hash.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::CommandMetadata;
    use crate::domain::shared::context::{ActorContext, ActorKind};
    use crate::domain::shared::ids::GlobalMemberId;

    #[test]
    fn command_metadata_exposes_all_fields() {
        let metadata = CommandMetadata::new("idem-1", "trace-1", "hash-1");

        assert_eq!(metadata.idempotency_key(), "idem-1");
        assert_eq!(metadata.trace_id(), "trace-1");
        assert_eq!(metadata.request_hash(), "hash-1");
    }

    #[test]
    fn actor_context_returns_member_id_for_ai_members() {
        let member_id = GlobalMemberId::new("member-1");
        let actor = ActorContext::new("actor/member-1", ActorKind::AiMember, Some(member_id));

        assert_eq!(
            actor.actor_member_id().map(|value| value.as_str()),
            Some("member-1")
        );
        assert!(!actor.is_system_actor());
    }

    #[test]
    fn actor_context_marks_system_actors() {
        let actor = ActorContext::new("system/rebuild", ActorKind::System, None);

        assert!(actor.is_system_actor());
        assert_eq!(actor.actor_member_id(), None);
    }
}
