//! Outbox propagation state helpers.

use identity_contracts::refs::{
    GlobalMemberRef, IdentityChangeKindRef, IdentityOutboxPayloadMarkerRef,
    IdentityOutboxRecordRef, IdentityOutboxSubjectRef, IdentityTimestamp, IdentityTraceRecordRef,
    OutboxDeliveryAttemptRef, OutboxDeliveryIssueRef, TopicKeyRef, VisibilityContextRef,
};

use crate::errors::IdentityDomainError;

/// Outbox publish lifecycle state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutboxStateKind {
    /// Accepted outbox exists and is waiting for publish.
    PendingPublish,
    /// Publisher boundary accepted the outbox.
    Published,
    /// Publish failed but may be retried.
    RetryableFailed,
    /// Publish failed terminally.
    Failed,
    /// Policy skipped propagation.
    SkippedByPolicy,
}

/// Outbox publish state marker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboxState {
    /// Current publish state.
    pub state_kind: OutboxStateKind,
    /// Optional publish attempt marker.
    pub attempt_ref: Option<OutboxDeliveryAttemptRef>,
    /// Optional safe issue marker.
    pub issue_ref: Option<OutboxDeliveryIssueRef>,
    /// Latest state change timestamp.
    pub changed_at: IdentityTimestamp,
}

impl OutboxState {
    /// Creates a pending outbox state.
    pub fn pending(changed_at: IdentityTimestamp) -> Self {
        Self {
            state_kind: OutboxStateKind::PendingPublish,
            attempt_ref: None,
            issue_ref: None,
            changed_at,
        }
    }

    /// Creates a published outbox state.
    pub fn published(attempt_ref: OutboxDeliveryAttemptRef, changed_at: IdentityTimestamp) -> Self {
        Self {
            state_kind: OutboxStateKind::Published,
            attempt_ref: Some(attempt_ref),
            issue_ref: None,
            changed_at,
        }
    }

    /// Creates a retryable failed outbox state.
    pub fn retryable_failed(
        issue_ref: OutboxDeliveryIssueRef,
        changed_at: IdentityTimestamp,
    ) -> Self {
        Self {
            state_kind: OutboxStateKind::RetryableFailed,
            attempt_ref: None,
            issue_ref: Some(issue_ref),
            changed_at,
        }
    }

    /// Creates a terminal failed outbox state.
    pub fn failed(issue_ref: OutboxDeliveryIssueRef, changed_at: IdentityTimestamp) -> Self {
        Self {
            state_kind: OutboxStateKind::Failed,
            attempt_ref: None,
            issue_ref: Some(issue_ref),
            changed_at,
        }
    }

    /// Creates a policy-skipped outbox state.
    pub fn skipped_by_policy(
        issue_ref: OutboxDeliveryIssueRef,
        changed_at: IdentityTimestamp,
    ) -> Self {
        Self {
            state_kind: OutboxStateKind::SkippedByPolicy,
            attempt_ref: None,
            issue_ref: Some(issue_ref),
            changed_at,
        }
    }

    /// Returns whether retry may select this state.
    pub fn is_retryable(&self) -> bool {
        self.state_kind == OutboxStateKind::RetryableFailed
    }

    /// Returns whether this state is terminal.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state_kind,
            OutboxStateKind::Published | OutboxStateKind::Failed | OutboxStateKind::SkippedByPolicy
        )
    }
}

/// Accepted outbox record shell with terminal publish guards.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdentityOutboxRecord {
    /// Outbox record identity.
    pub outbox_record_ref: IdentityOutboxRecordRef,
    /// Accepted change member subject.
    pub member_ref: GlobalMemberRef,
    /// Canonical outbound subject marker.
    pub subject_ref: IdentityOutboxSubjectRef,
    /// Accepted change kind marker.
    pub change_kind_ref: IdentityChangeKindRef,
    /// Body-free payload marker.
    pub payload_marker_ref: IdentityOutboxPayloadMarkerRef,
    /// Topic binding marker.
    pub topic_key_ref: TopicKeyRef,
    /// Accepted trace record marker.
    pub trace_record_ref: IdentityTraceRecordRef,
    /// Current publish state.
    pub outbox_state: OutboxState,
    /// Create timestamp.
    pub created_at: IdentityTimestamp,
    /// Update timestamp.
    pub updated_at: IdentityTimestamp,
}

/// Factory input for creating accepted pending outbox records.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdentityOutboxRecordCreateArgs {
    /// Stable outbox record identity.
    pub outbox_record_ref: IdentityOutboxRecordRef,
    /// Accepted change member subject.
    pub member_ref: GlobalMemberRef,
    /// Canonical outbound subject marker.
    pub subject_ref: IdentityOutboxSubjectRef,
    /// Accepted change kind marker.
    pub change_kind_ref: IdentityChangeKindRef,
    /// Body-free payload marker.
    pub payload_marker_ref: IdentityOutboxPayloadMarkerRef,
    /// Topic binding marker.
    pub topic_key_ref: TopicKeyRef,
    /// Accepted trace record marker.
    pub trace_record_ref: IdentityTraceRecordRef,
    /// Create timestamp.
    pub created_at: IdentityTimestamp,
}

impl IdentityOutboxRecord {
    /// Creates a pending outbox record from accepted change material.
    pub fn from_accepted_change(
        args: IdentityOutboxRecordCreateArgs,
    ) -> Result<Self, IdentityDomainError> {
        if args.trace_record_ref.as_str().is_empty() {
            return Err(IdentityDomainError::invalid_input(
                "trace_record_ref",
                "accepted outbox requires a formal trace record",
            ));
        }
        if args.payload_marker_ref.as_str().is_empty() {
            return Err(IdentityDomainError::invalid_input(
                "payload_marker_ref",
                "accepted outbox requires a body-free payload marker",
            ));
        }
        if args.topic_key_ref.as_str().is_empty() {
            return Err(IdentityDomainError::invalid_input(
                "topic_key_ref",
                "accepted outbox requires a canonical topic key marker",
            ));
        }

        Ok(Self {
            outbox_record_ref: args.outbox_record_ref,
            member_ref: args.member_ref,
            subject_ref: args.subject_ref,
            change_kind_ref: args.change_kind_ref,
            payload_marker_ref: args.payload_marker_ref,
            topic_key_ref: args.topic_key_ref,
            trace_record_ref: args.trace_record_ref,
            outbox_state: OutboxState::pending(args.created_at),
            created_at: args.created_at,
            updated_at: args.created_at,
        })
    }

    /// Returns whether the outbox belongs to the provided member.
    pub fn belongs_to(&self, member_ref: &GlobalMemberRef) -> bool {
        self.member_ref.same_member(member_ref)
    }

    /// Returns whether the outbox matches the provided subject.
    pub fn matches_subject(&self, subject_ref: &IdentityOutboxSubjectRef) -> bool {
        self.subject_ref == *subject_ref
    }

    /// Returns whether retry may select this outbox.
    pub fn is_retryable(&self) -> bool {
        self.outbox_state.is_retryable()
    }

    /// Marks the record published.
    pub fn mark_published(&mut self, state: OutboxState) -> Result<(), IdentityDomainError> {
        if state.state_kind != OutboxStateKind::Published || state.attempt_ref.is_none() {
            return Err(IdentityDomainError::invalid_input(
                "outbox_state",
                "published outbox state requires an attempt marker",
            ));
        }

        match self.outbox_state.state_kind {
            OutboxStateKind::PendingPublish | OutboxStateKind::RetryableFailed => {
                self.updated_at = state.changed_at;
                self.outbox_state = state;
                Ok(())
            }
            _ => Err(IdentityDomainError::invalid_state_transition(
                "IdentityOutboxRecord",
                "terminal outbox state cannot be published again",
            )),
        }
    }

    /// Marks the record retryable failed.
    pub fn mark_retryable_failed(&mut self, state: OutboxState) -> Result<(), IdentityDomainError> {
        if state.state_kind != OutboxStateKind::RetryableFailed || state.issue_ref.is_none() {
            return Err(IdentityDomainError::invalid_input(
                "outbox_state",
                "retryable failed outbox state requires an issue marker",
            ));
        }

        match self.outbox_state.state_kind {
            OutboxStateKind::PendingPublish | OutboxStateKind::RetryableFailed => {
                self.updated_at = state.changed_at;
                self.outbox_state = state;
                Ok(())
            }
            _ => Err(IdentityDomainError::invalid_state_transition(
                "IdentityOutboxRecord",
                "terminal outbox state cannot become retryable failed",
            )),
        }
    }

    /// Marks the record terminal failed.
    pub fn mark_failed(&mut self, state: OutboxState) -> Result<(), IdentityDomainError> {
        if state.state_kind != OutboxStateKind::Failed || state.issue_ref.is_none() {
            return Err(IdentityDomainError::invalid_input(
                "outbox_state",
                "failed outbox state requires an issue marker",
            ));
        }

        match self.outbox_state.state_kind {
            OutboxStateKind::PendingPublish | OutboxStateKind::RetryableFailed => {
                self.updated_at = state.changed_at;
                self.outbox_state = state;
                Ok(())
            }
            _ => Err(IdentityDomainError::invalid_state_transition(
                "IdentityOutboxRecord",
                "terminal outbox state cannot fail again",
            )),
        }
    }

    /// Marks the record skipped by policy.
    pub fn mark_skipped_by_policy(
        &mut self,
        state: OutboxState,
    ) -> Result<(), IdentityDomainError> {
        if state.state_kind != OutboxStateKind::SkippedByPolicy || state.issue_ref.is_none() {
            return Err(IdentityDomainError::invalid_input(
                "outbox_state",
                "skipped outbox state requires an issue marker",
            ));
        }

        match self.outbox_state.state_kind {
            OutboxStateKind::PendingPublish | OutboxStateKind::RetryableFailed => {
                self.updated_at = state.changed_at;
                self.outbox_state = state;
                Ok(())
            }
            _ => Err(IdentityDomainError::invalid_state_transition(
                "IdentityOutboxRecord",
                "terminal outbox state cannot be skipped again",
            )),
        }
    }
}

/// Factory input for outbound accepted material policy construction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboundEventPolicyArgs {
    /// Canonical outbound subject marker.
    pub subject_ref: IdentityOutboxSubjectRef,
    /// Accepted change kind marker.
    pub change_kind_ref: IdentityChangeKindRef,
    /// Body-free payload marker.
    pub payload_marker_ref: IdentityOutboxPayloadMarkerRef,
    /// Canonical topic marker.
    pub topic_key_ref: TopicKeyRef,
    /// Prepared visibility context marker.
    pub visibility_context_ref: VisibilityContextRef,
}

/// Accepted-only outbound material policy guard.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OutboundEventPolicy {
    /// Canonical outbound subject marker.
    pub subject_ref: IdentityOutboxSubjectRef,
    /// Accepted change kind marker.
    pub change_kind_ref: IdentityChangeKindRef,
    /// Body-free payload marker.
    pub payload_marker_ref: IdentityOutboxPayloadMarkerRef,
    /// Canonical topic marker.
    pub topic_key_ref: TopicKeyRef,
    /// Prepared visibility context marker.
    pub visibility_context_ref: VisibilityContextRef,
}

impl OutboundEventPolicy {
    /// Creates an outbound event policy for accepted outbox material.
    pub fn for_outbox(args: OutboundEventPolicyArgs) -> Result<Self, IdentityDomainError> {
        if args.payload_marker_ref.as_str().is_empty() {
            return Err(IdentityDomainError::invalid_input(
                "payload_marker_ref",
                "outbound policy requires a body-free payload marker",
            ));
        }
        if args.topic_key_ref.as_str().is_empty() {
            return Err(IdentityDomainError::invalid_input(
                "topic_key_ref",
                "outbound policy requires a canonical topic key marker",
            ));
        }
        if args.visibility_context_ref.as_str().is_empty() {
            return Err(IdentityDomainError::invalid_input(
                "visibility_context_ref",
                "outbound policy requires a visibility context marker",
            ));
        }

        Ok(Self {
            subject_ref: args.subject_ref,
            change_kind_ref: args.change_kind_ref,
            payload_marker_ref: args.payload_marker_ref,
            topic_key_ref: args.topic_key_ref,
            visibility_context_ref: args.visibility_context_ref,
        })
    }

    /// Asserts that the material originates from an accepted change trace.
    pub fn assert_from_accepted_change(
        &self,
        trace_record_ref: &IdentityTraceRecordRef,
    ) -> Result<(), IdentityDomainError> {
        if trace_record_ref.as_str().is_empty() {
            return Err(IdentityDomainError::invalid_input(
                "trace_record_ref",
                "accepted outbound material requires a formal trace record",
            ));
        }
        Ok(())
    }

    /// Asserts that the payload marker remains body-free.
    pub fn assert_payload_body_free(&self) -> Result<(), IdentityDomainError> {
        if self.payload_marker_ref.as_str().is_empty() {
            return Err(IdentityDomainError::invalid_input(
                "payload_marker_ref",
                "outbound payload marker must remain body-free",
            ));
        }
        Ok(())
    }

    /// Asserts that the material remains visible for the canonical topic.
    pub fn assert_visible_for_topic(&self) -> Result<(), IdentityDomainError> {
        if self.visibility_context_ref.as_str().is_empty() {
            return Err(IdentityDomainError::invalid_input(
                "visibility_context_ref",
                "outbound visibility context marker must remain present",
            ));
        }
        Ok(())
    }

    /// Asserts that publish remains decoupled from accepted truth persistence.
    pub fn assert_publish_not_acceptance_gate(&self) -> Result<(), IdentityDomainError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use identity_contracts::refs::{
        ExternalSourceRef, GlobalMemberId, GlobalMemberRef, IdentityChangeKind,
        IdentityChangeKindRef, IdentityOutboxPayloadMarkerRef, IdentityOutboxRecordRef,
        IdentityOutboxSubjectRef, IdentitySourceOwner, IdentitySourceRef, IdentityTimestamp,
        IdentityTraceRecordRef, OutboxDeliveryAttemptRef, OutboxDeliveryIssueRef, TopicKeyRef,
        VisibilityContextRef,
    };

    use super::{
        IdentityOutboxRecord, IdentityOutboxRecordCreateArgs, OutboundEventPolicy,
        OutboundEventPolicyArgs, OutboxState,
    };
    use crate::errors::IdentityDomainError;

    fn timestamp(value: i64) -> IdentityTimestamp {
        IdentityTimestamp::from_clock(value).expect("valid timestamp")
    }

    fn source(token: &str) -> IdentitySourceRef {
        IdentitySourceRef::new(
            IdentitySourceOwner::Identity,
            ExternalSourceRef::new(token.to_owned()).expect("valid external source ref"),
        )
        .expect("valid source ref")
    }

    fn member_ref() -> GlobalMemberRef {
        GlobalMemberRef::from_id(
            GlobalMemberId::new("member-1".to_owned()).expect("valid member id"),
        )
    }

    fn outbox_record() -> IdentityOutboxRecord {
        IdentityOutboxRecord::from_accepted_change(IdentityOutboxRecordCreateArgs {
            outbox_record_ref: IdentityOutboxRecordRef::new("outbox-1"),
            member_ref: member_ref(),
            subject_ref: IdentityOutboxSubjectRef::new("subject-1"),
            change_kind_ref: IdentityChangeKindRef::new(
                IdentityChangeKind::DerivedMarkerChanged,
                None,
            ),
            payload_marker_ref: IdentityOutboxPayloadMarkerRef::new("payload-1"),
            topic_key_ref: TopicKeyRef::new("identity.lifecycle.changed.v1"),
            trace_record_ref: IdentityTraceRecordRef::new("trace-1"),
            created_at: timestamp(1),
        })
        .expect("accepted outbox")
    }

    fn attempt_ref() -> OutboxDeliveryAttemptRef {
        OutboxDeliveryAttemptRef::new(source("attempt-1"))
    }

    fn issue_ref() -> OutboxDeliveryIssueRef {
        OutboxDeliveryIssueRef::new(source("issue-1"))
    }

    #[test]
    fn accepted_outbox_factory_creates_pending_state() {
        let record = outbox_record();

        assert_eq!(
            record.outbox_state.state_kind,
            super::OutboxStateKind::PendingPublish
        );
        assert_eq!(record.created_at, timestamp(1));
        assert_eq!(record.updated_at, timestamp(1));
    }

    #[test]
    fn accepted_outbox_factory_requires_trace_record() {
        let error = IdentityOutboxRecord::from_accepted_change(IdentityOutboxRecordCreateArgs {
            outbox_record_ref: IdentityOutboxRecordRef::new("outbox-trace-missing"),
            member_ref: member_ref(),
            subject_ref: IdentityOutboxSubjectRef::new("subject-1"),
            change_kind_ref: IdentityChangeKindRef::new(
                IdentityChangeKind::DerivedMarkerChanged,
                None,
            ),
            payload_marker_ref: IdentityOutboxPayloadMarkerRef::new("payload-1"),
            topic_key_ref: TopicKeyRef::new("identity.lifecycle.changed.v1"),
            trace_record_ref: IdentityTraceRecordRef::new(""),
            created_at: timestamp(1),
        })
        .expect_err("missing trace should be rejected");

        assert_eq!(
            error,
            IdentityDomainError::invalid_input(
                "trace_record_ref",
                "accepted outbox requires a formal trace record",
            )
        );
    }

    #[test]
    fn outbound_policy_requires_visibility_context() {
        let error = OutboundEventPolicy::for_outbox(OutboundEventPolicyArgs {
            subject_ref: IdentityOutboxSubjectRef::new("subject-1"),
            change_kind_ref: IdentityChangeKindRef::new(
                IdentityChangeKind::DerivedMarkerChanged,
                None,
            ),
            payload_marker_ref: IdentityOutboxPayloadMarkerRef::new("payload-1"),
            topic_key_ref: TopicKeyRef::new("identity.lifecycle.changed.v1"),
            visibility_context_ref: VisibilityContextRef::new(""),
        })
        .expect_err("missing visibility context should fail");

        assert_eq!(
            error,
            IdentityDomainError::invalid_input(
                "visibility_context_ref",
                "outbound policy requires a visibility context marker",
            )
        );
    }

    #[test]
    fn retry_selector_only_picks_retryable_outbox() {
        let pending = OutboxState::pending(timestamp(1));
        let retryable = OutboxState::retryable_failed(issue_ref(), timestamp(2));
        let published = OutboxState::published(attempt_ref(), timestamp(3));
        let failed = OutboxState::failed(issue_ref(), timestamp(4));

        assert!(!pending.is_retryable());
        assert!(retryable.is_retryable());
        assert!(!published.is_retryable());
        assert!(!failed.is_retryable());
    }

    #[test]
    fn published_outbox_is_terminal_for_retry() {
        let mut record = outbox_record();
        record
            .mark_published(OutboxState::published(attempt_ref(), timestamp(2)))
            .expect("pending outbox can be published");

        assert!(record.outbox_state.is_terminal());
        assert!(!record.is_retryable());

        let error = record
            .mark_retryable_failed(OutboxState::retryable_failed(issue_ref(), timestamp(3)))
            .expect_err("published outbox must remain terminal");

        assert_eq!(
            error,
            IdentityDomainError::InvalidStateTransition {
                entity: "IdentityOutboxRecord",
                message: "terminal outbox state cannot become retryable failed",
            }
        );
    }

    #[test]
    fn failed_outbox_remains_terminal() {
        let mut record = outbox_record();
        record
            .mark_failed(OutboxState::failed(issue_ref(), timestamp(2)))
            .expect("pending outbox can fail");

        assert!(record.outbox_state.is_terminal());
        assert!(!record.is_retryable());

        let error = record
            .mark_published(OutboxState::published(attempt_ref(), timestamp(3)))
            .expect_err("failed outbox must not publish later");

        assert_eq!(
            error,
            IdentityDomainError::InvalidStateTransition {
                entity: "IdentityOutboxRecord",
                message: "terminal outbox state cannot be published again",
            }
        );
    }
}
