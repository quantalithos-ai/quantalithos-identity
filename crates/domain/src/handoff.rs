//! Handoff propagation state helpers.

use identity_contracts::receipts::TraceHandoffIntentRef;
use identity_contracts::refs::{
    AuditTrailRef, GlobalMemberRef, HandoffAttemptRef, HandoffIssueRef, HandoffReceiptRef,
    HandoffScopeRef, HandoffTargetRef, IdentityTimestamp, IdentityTraceRecordRef,
    TraceHandoffSafeMaterialRef, VisibilityContextRef,
};

use crate::errors::IdentityDomainError;

/// Handoff delivery lifecycle state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HandoffStateKind {
    /// Handoff exists and is waiting for delivery.
    PendingHandoff,
    /// Formal delivery receipt was accepted.
    Delivered,
    /// Delivery failed but may be retried.
    RetryableFailed,
    /// Delivery failed terminally.
    Failed,
    /// Delivery was cancelled before completion.
    Cancelled,
}

/// Constructor arguments for a pending handoff intent.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TraceHandoffIntentPrepareArgs {
    /// Handoff identity.
    pub handoff_intent_ref: TraceHandoffIntentRef,
    /// Member that owns the handoff subject.
    pub member_ref: GlobalMemberRef,
    /// Trace refs included in the handoff.
    pub trace_record_refs: Vec<IdentityTraceRecordRef>,
    /// Optional audit trail ref included in the handoff.
    pub audit_trail_ref: Option<AuditTrailRef>,
    /// Handoff target marker.
    pub handoff_target_ref: HandoffTargetRef,
    /// Handoff scope marker.
    pub handoff_scope_ref: HandoffScopeRef,
    /// Safe handoff material marker.
    pub safe_material_ref: TraceHandoffSafeMaterialRef,
    /// Pending handoff state.
    pub handoff_state: HandoffState,
    /// Create timestamp.
    pub created_at: IdentityTimestamp,
}

/// Guard arguments for preparing a trace handoff.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HandoffPolicyArgs {
    /// Handoff target marker.
    pub handoff_target_ref: HandoffTargetRef,
    /// Handoff scope marker.
    pub handoff_scope_ref: HandoffScopeRef,
    /// Safe handoff material marker.
    pub safe_material_ref: TraceHandoffSafeMaterialRef,
    /// Trace refs included in the handoff.
    pub trace_record_refs: Vec<IdentityTraceRecordRef>,
    /// Visibility context marker.
    pub visibility_context_ref: VisibilityContextRef,
}

/// Handoff delivery state marker.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HandoffState {
    /// Current delivery state.
    pub state_kind: HandoffStateKind,
    /// Optional handoff attempt marker.
    pub attempt_ref: Option<HandoffAttemptRef>,
    /// Optional formal receipt marker.
    pub receipt_ref: Option<HandoffReceiptRef>,
    /// Optional safe issue marker.
    pub issue_ref: Option<HandoffIssueRef>,
    /// Latest state change timestamp.
    pub changed_at: IdentityTimestamp,
}

impl HandoffState {
    /// Creates a pending handoff state.
    pub fn pending(changed_at: IdentityTimestamp) -> Self {
        Self {
            state_kind: HandoffStateKind::PendingHandoff,
            attempt_ref: None,
            receipt_ref: None,
            issue_ref: None,
            changed_at,
        }
    }

    /// Creates a delivered handoff state.
    pub fn delivered(
        attempt_ref: HandoffAttemptRef,
        receipt_ref: HandoffReceiptRef,
        changed_at: IdentityTimestamp,
    ) -> Self {
        Self {
            state_kind: HandoffStateKind::Delivered,
            attempt_ref: Some(attempt_ref),
            receipt_ref: Some(receipt_ref),
            issue_ref: None,
            changed_at,
        }
    }

    /// Creates a retryable failed handoff state.
    pub fn retryable_failed(
        attempt_ref: HandoffAttemptRef,
        issue_ref: HandoffIssueRef,
        changed_at: IdentityTimestamp,
    ) -> Self {
        Self {
            state_kind: HandoffStateKind::RetryableFailed,
            attempt_ref: Some(attempt_ref),
            receipt_ref: None,
            issue_ref: Some(issue_ref),
            changed_at,
        }
    }

    /// Creates a terminal failed handoff state.
    pub fn failed(
        attempt_ref: HandoffAttemptRef,
        issue_ref: HandoffIssueRef,
        changed_at: IdentityTimestamp,
    ) -> Self {
        Self {
            state_kind: HandoffStateKind::Failed,
            attempt_ref: Some(attempt_ref),
            receipt_ref: None,
            issue_ref: Some(issue_ref),
            changed_at,
        }
    }

    /// Creates a cancelled handoff state.
    pub fn cancelled(issue_ref: HandoffIssueRef, changed_at: IdentityTimestamp) -> Self {
        Self {
            state_kind: HandoffStateKind::Cancelled,
            attempt_ref: None,
            receipt_ref: None,
            issue_ref: Some(issue_ref),
            changed_at,
        }
    }

    /// Returns whether retry may select this state.
    pub fn is_retryable(&self) -> bool {
        self.state_kind == HandoffStateKind::RetryableFailed
    }

    /// Returns whether this state is terminal.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state_kind,
            HandoffStateKind::Delivered | HandoffStateKind::Failed | HandoffStateKind::Cancelled
        )
    }
}

/// Trace handoff intent shell with terminal delivery guards.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TraceHandoffIntent {
    /// Handoff identity.
    pub handoff_intent_ref: TraceHandoffIntentRef,
    /// Member that owns the handoff subject.
    pub member_ref: GlobalMemberRef,
    /// Trace refs included in the handoff.
    pub trace_record_refs: Vec<IdentityTraceRecordRef>,
    /// Optional audit trail ref included in the handoff.
    pub audit_trail_ref: Option<AuditTrailRef>,
    /// Handoff target marker.
    pub handoff_target_ref: HandoffTargetRef,
    /// Handoff scope marker.
    pub handoff_scope_ref: HandoffScopeRef,
    /// Safe handoff material marker.
    pub safe_material_ref: TraceHandoffSafeMaterialRef,
    /// Current handoff state.
    pub handoff_state: HandoffState,
    /// Create timestamp.
    pub created_at: IdentityTimestamp,
    /// Update timestamp.
    pub updated_at: IdentityTimestamp,
}

impl TraceHandoffIntent {
    /// Creates a pending handoff intent.
    pub fn prepare(args: TraceHandoffIntentPrepareArgs) -> Result<Self, IdentityDomainError> {
        if args.trace_record_refs.is_empty() {
            return Err(IdentityDomainError::missing_required_field(
                "trace_record_refs",
            ));
        }
        if args.handoff_state.state_kind != HandoffStateKind::PendingHandoff {
            return Err(IdentityDomainError::invalid_input(
                "handoff_state",
                "prepare requires a pending handoff state",
            ));
        }
        if args.handoff_state.attempt_ref.is_some()
            || args.handoff_state.receipt_ref.is_some()
            || args.handoff_state.issue_ref.is_some()
        {
            return Err(IdentityDomainError::invalid_input(
                "handoff_state",
                "pending handoff state must not carry attempt, receipt, or issue markers",
            ));
        }
        if args.safe_material_ref.as_str().trim().is_empty() {
            return Err(IdentityDomainError::missing_required_field(
                "safe_material_ref",
            ));
        }
        if args.handoff_target_ref.as_str().trim().is_empty() {
            return Err(IdentityDomainError::missing_required_field(
                "handoff_target_ref",
            ));
        }
        if args.handoff_scope_ref.as_str().trim().is_empty() {
            return Err(IdentityDomainError::missing_required_field(
                "handoff_scope_ref",
            ));
        }

        Ok(Self {
            handoff_intent_ref: args.handoff_intent_ref,
            member_ref: args.member_ref,
            trace_record_refs: args.trace_record_refs,
            audit_trail_ref: args.audit_trail_ref,
            handoff_target_ref: args.handoff_target_ref,
            handoff_scope_ref: args.handoff_scope_ref,
            safe_material_ref: args.safe_material_ref,
            handoff_state: args.handoff_state,
            created_at: args.created_at,
            updated_at: args.created_at,
        })
    }

    /// Returns whether the intent targets the provided handoff target.
    pub fn targets(&self, target_ref: &HandoffTargetRef) -> bool {
        self.handoff_target_ref == *target_ref
    }

    /// Returns whether the intent contains the provided trace ref.
    pub fn contains_trace(&self, trace_record_ref: &IdentityTraceRecordRef) -> bool {
        self.trace_record_refs.contains(trace_record_ref)
    }

    /// Returns whether retry may select this handoff.
    pub fn is_retryable(&self) -> bool {
        self.handoff_state.is_retryable()
    }

    /// Marks the handoff delivered.
    pub fn mark_delivered(&mut self, state: HandoffState) -> Result<(), IdentityDomainError> {
        if state.state_kind != HandoffStateKind::Delivered
            || state.attempt_ref.is_none()
            || state.receipt_ref.is_none()
        {
            return Err(IdentityDomainError::invalid_input(
                "handoff_state",
                "delivered handoff state requires attempt and receipt markers",
            ));
        }

        match self.handoff_state.state_kind {
            HandoffStateKind::PendingHandoff | HandoffStateKind::RetryableFailed => {
                self.updated_at = state.changed_at;
                self.handoff_state = state;
                Ok(())
            }
            _ => Err(IdentityDomainError::invalid_state_transition(
                "TraceHandoffIntent",
                "terminal handoff state cannot be delivered again",
            )),
        }
    }

    /// Marks the handoff retryable failed.
    pub fn mark_retryable_failed(
        &mut self,
        state: HandoffState,
    ) -> Result<(), IdentityDomainError> {
        if state.state_kind != HandoffStateKind::RetryableFailed
            || state.attempt_ref.is_none()
            || state.issue_ref.is_none()
        {
            return Err(IdentityDomainError::invalid_input(
                "handoff_state",
                "retryable failed handoff state requires attempt and issue markers",
            ));
        }

        match self.handoff_state.state_kind {
            HandoffStateKind::PendingHandoff | HandoffStateKind::RetryableFailed => {
                self.updated_at = state.changed_at;
                self.handoff_state = state;
                Ok(())
            }
            _ => Err(IdentityDomainError::invalid_state_transition(
                "TraceHandoffIntent",
                "terminal handoff state cannot become retryable failed",
            )),
        }
    }

    /// Marks the handoff terminal failed.
    pub fn mark_failed(&mut self, state: HandoffState) -> Result<(), IdentityDomainError> {
        if state.state_kind != HandoffStateKind::Failed
            || state.attempt_ref.is_none()
            || state.issue_ref.is_none()
        {
            return Err(IdentityDomainError::invalid_input(
                "handoff_state",
                "failed handoff state requires attempt and issue markers",
            ));
        }

        match self.handoff_state.state_kind {
            HandoffStateKind::PendingHandoff | HandoffStateKind::RetryableFailed => {
                self.updated_at = state.changed_at;
                self.handoff_state = state;
                Ok(())
            }
            _ => Err(IdentityDomainError::invalid_state_transition(
                "TraceHandoffIntent",
                "terminal handoff state cannot fail again",
            )),
        }
    }

    /// Marks the handoff cancelled.
    pub fn mark_cancelled(&mut self, state: HandoffState) -> Result<(), IdentityDomainError> {
        if state.state_kind != HandoffStateKind::Cancelled || state.issue_ref.is_none() {
            return Err(IdentityDomainError::invalid_input(
                "handoff_state",
                "cancelled handoff state requires an issue marker",
            ));
        }

        match self.handoff_state.state_kind {
            HandoffStateKind::PendingHandoff | HandoffStateKind::RetryableFailed => {
                self.updated_at = state.changed_at;
                self.handoff_state = state;
                Ok(())
            }
            _ => Err(IdentityDomainError::invalid_state_transition(
                "TraceHandoffIntent",
                "terminal handoff state cannot be cancelled again",
            )),
        }
    }
}

/// Guard for trace handoff preparation and delivery result integrity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HandoffPolicy {
    /// Handoff target marker.
    pub handoff_target_ref: HandoffTargetRef,
    /// Handoff scope marker.
    pub handoff_scope_ref: HandoffScopeRef,
    /// Safe handoff material marker.
    pub safe_material_ref: TraceHandoffSafeMaterialRef,
    /// Trace refs included in the handoff.
    pub trace_record_refs: Vec<IdentityTraceRecordRef>,
    /// Visibility context marker.
    pub visibility_context_ref: VisibilityContextRef,
}

impl HandoffPolicy {
    /// Creates a handoff guard from formal request markers.
    pub fn for_handoff(args: HandoffPolicyArgs) -> Result<Self, IdentityDomainError> {
        Ok(Self {
            handoff_target_ref: args.handoff_target_ref,
            handoff_scope_ref: args.handoff_scope_ref,
            safe_material_ref: args.safe_material_ref,
            trace_record_refs: args.trace_record_refs,
            visibility_context_ref: args.visibility_context_ref,
        })
    }

    /// Asserts that target and scope markers are present.
    pub fn assert_target_allowed(&self) -> Result<(), IdentityDomainError> {
        if self.handoff_target_ref.as_str().trim().is_empty() {
            return Err(IdentityDomainError::missing_required_field(
                "handoff_target_ref",
            ));
        }
        if self.handoff_scope_ref.as_str().trim().is_empty() {
            return Err(IdentityDomainError::missing_required_field(
                "handoff_scope_ref",
            ));
        }
        Ok(())
    }

    /// Asserts that at least one trace ref is present.
    pub fn assert_trace_refs_present(&self) -> Result<(), IdentityDomainError> {
        if self.trace_record_refs.is_empty() {
            return Err(IdentityDomainError::policy_denied(
                "HandoffPolicy",
                "handoff requires at least one trace ref",
            ));
        }
        Ok(())
    }

    /// Asserts that the material marker remains body-free.
    pub fn assert_safe_material_body_free(&self) -> Result<(), IdentityDomainError> {
        let marker = self.safe_material_ref.as_str().to_ascii_lowercase();
        if marker.trim().is_empty() {
            return Err(IdentityDomainError::missing_required_field(
                "safe_material_ref",
            ));
        }
        if marker.contains("body")
            || marker.contains("package")
            || marker.contains("raw")
            || marker.contains("receipt")
        {
            return Err(IdentityDomainError::policy_denied(
                "HandoffPolicy",
                "handoff material marker must remain body-free",
            ));
        }
        Ok(())
    }

    /// Asserts that the handoff keeps a formal visibility context marker.
    pub fn assert_visible_for_handoff(&self) -> Result<(), IdentityDomainError> {
        if self.visibility_context_ref.as_str().trim().is_empty() {
            return Err(IdentityDomainError::missing_required_field(
                "visibility_context_ref",
            ));
        }
        Ok(())
    }

    /// Asserts that a delivered state carries only a formal receipt marker.
    pub fn assert_receipt_is_marker(
        receipt_ref: &HandoffReceiptRef,
    ) -> Result<(), IdentityDomainError> {
        if receipt_ref.as_str().trim().is_empty() {
            return Err(IdentityDomainError::missing_required_field("receipt_ref"));
        }
        Ok(())
    }

    /// Asserts that delivered state always carries a formal receipt marker.
    pub fn assert_delivered_requires_receipt(
        state: &HandoffState,
    ) -> Result<(), IdentityDomainError> {
        if state.state_kind == HandoffStateKind::Delivered && state.receipt_ref.is_none() {
            return Err(IdentityDomainError::policy_denied(
                "HandoffPolicy",
                "delivered handoff requires a formal receipt marker",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use identity_contracts::receipts::TraceHandoffIntentRef;
    use identity_contracts::refs::{
        AuditTrailRef, ExternalSourceRef, GlobalMemberId, GlobalMemberRef, HandoffAttemptRef,
        HandoffIssueRef, HandoffReceiptRef, HandoffScopeRef, HandoffTargetRef, IdentitySourceOwner,
        IdentitySourceRef, IdentityTimestamp, IdentityTraceRecordRef, TraceHandoffSafeMaterialRef,
    };

    use super::{HandoffState, TraceHandoffIntent};
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

    fn handoff_intent() -> TraceHandoffIntent {
        TraceHandoffIntent {
            handoff_intent_ref: TraceHandoffIntentRef::new("handoff-1"),
            member_ref: member_ref(),
            trace_record_refs: vec![IdentityTraceRecordRef::new("trace-1")],
            audit_trail_ref: Some(AuditTrailRef::new("audit-1")),
            handoff_target_ref: HandoffTargetRef::new("target-1"),
            handoff_scope_ref: HandoffScopeRef::new("scope-1"),
            safe_material_ref: TraceHandoffSafeMaterialRef::new("material-1"),
            handoff_state: HandoffState::pending(timestamp(1)),
            created_at: timestamp(1),
            updated_at: timestamp(1),
        }
    }

    fn attempt_ref() -> HandoffAttemptRef {
        HandoffAttemptRef::new(source("attempt-1"))
    }

    fn receipt_ref() -> HandoffReceiptRef {
        HandoffReceiptRef::new("receipt-1")
    }

    fn issue_ref() -> HandoffIssueRef {
        HandoffIssueRef::new(source("issue-1"))
    }

    #[test]
    fn retry_selector_only_picks_retryable_handoff() {
        let pending = HandoffState::pending(timestamp(1));
        let retryable = HandoffState::retryable_failed(attempt_ref(), issue_ref(), timestamp(2));
        let delivered = HandoffState::delivered(attempt_ref(), receipt_ref(), timestamp(3));
        let cancelled = HandoffState::cancelled(issue_ref(), timestamp(4));

        assert!(!pending.is_retryable());
        assert!(retryable.is_retryable());
        assert!(!delivered.is_retryable());
        assert!(!cancelled.is_retryable());
    }

    #[test]
    fn delivered_handoff_is_terminal_for_retry() {
        let mut intent = handoff_intent();
        intent
            .mark_delivered(HandoffState::delivered(
                attempt_ref(),
                receipt_ref(),
                timestamp(2),
            ))
            .expect("pending handoff can be delivered");

        assert!(intent.handoff_state.is_terminal());
        assert!(!intent.is_retryable());

        let error = intent
            .mark_retryable_failed(HandoffState::retryable_failed(
                attempt_ref(),
                issue_ref(),
                timestamp(3),
            ))
            .expect_err("delivered handoff must remain terminal");

        assert_eq!(
            error,
            IdentityDomainError::InvalidStateTransition {
                entity: "TraceHandoffIntent",
                message: "terminal handoff state cannot become retryable failed",
            }
        );
    }

    #[test]
    fn cancelled_handoff_remains_terminal() {
        let mut intent = handoff_intent();
        intent
            .mark_cancelled(HandoffState::cancelled(issue_ref(), timestamp(2)))
            .expect("pending handoff can be cancelled");

        assert!(intent.handoff_state.is_terminal());
        assert!(!intent.is_retryable());

        let error = intent
            .mark_delivered(HandoffState::delivered(
                attempt_ref(),
                receipt_ref(),
                timestamp(3),
            ))
            .expect_err("cancelled handoff must not deliver later");

        assert_eq!(
            error,
            IdentityDomainError::InvalidStateTransition {
                entity: "TraceHandoffIntent",
                message: "terminal handoff state cannot be delivered again",
            }
        );
    }
}
