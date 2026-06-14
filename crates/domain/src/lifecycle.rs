//! Core lifecycle truth and high-risk guards.

use core_contracts::actor::ActorRef;
use identity_contracts::refs::{
    GovernanceBasisRef, GovernanceBasisSummary, IdentityOperationChannel, IdentityTimestamp,
    LifecycleReasonRef, LifecycleRiskRef,
};

use crate::errors::IdentityDomainError;

/// Global lifecycle truth state kind owned by the domain layer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GlobalLifecycleStateKind {
    /// Member is available platform-wide.
    Available,
    /// Member is explicitly paused.
    Paused,
    /// Member is retired and may only tombstone next.
    Retired,
    /// Member is tombstoned and terminal.
    Tombstoned,
}

/// Global lifecycle truth state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GlobalLifecycleState {
    /// Lifecycle state kind.
    pub state_kind: GlobalLifecycleStateKind,
    /// Latest lifecycle reason.
    pub reason_ref: LifecycleReasonRef,
    /// Latest actor.
    pub changed_by_ref: ActorRef,
    /// Latest change timestamp.
    pub changed_at: IdentityTimestamp,
    /// Optional governance basis for high-risk actions.
    pub basis_ref: Option<GovernanceBasisRef>,
}

impl GlobalLifecycleState {
    /// Creates the initial available lifecycle state.
    pub fn initial_available(
        actor_ref: ActorRef,
        reason_ref: LifecycleReasonRef,
        changed_at: IdentityTimestamp,
    ) -> Self {
        Self {
            state_kind: GlobalLifecycleStateKind::Available,
            reason_ref,
            changed_by_ref: actor_ref,
            changed_at,
            basis_ref: None,
        }
    }

    /// Creates a new lifecycle state from a formal transition.
    pub fn from_transition(
        current_state: &GlobalLifecycleState,
        target_state: GlobalLifecycleStateKind,
        reason_ref: LifecycleReasonRef,
        actor_ref: ActorRef,
        changed_at: IdentityTimestamp,
        basis_ref: Option<GovernanceBasisRef>,
    ) -> Result<Self, IdentityDomainError> {
        if !current_state.can_transition_to(target_state) {
            return Err(IdentityDomainError::invalid_state_transition(
                "GlobalLifecycleState",
                "lifecycle transition is not allowed",
            ));
        }

        Ok(Self {
            state_kind: target_state,
            reason_ref,
            changed_by_ref: actor_ref,
            changed_at,
            basis_ref,
        })
    }

    /// Returns whether the target state is a formal transition candidate.
    pub fn can_transition_to(&self, target_state: GlobalLifecycleStateKind) -> bool {
        matches!(
            (self.state_kind, target_state),
            (
                GlobalLifecycleStateKind::Available,
                GlobalLifecycleStateKind::Paused
            ) | (
                GlobalLifecycleStateKind::Available,
                GlobalLifecycleStateKind::Retired
            ) | (
                GlobalLifecycleStateKind::Available,
                GlobalLifecycleStateKind::Tombstoned
            ) | (
                GlobalLifecycleStateKind::Paused,
                GlobalLifecycleStateKind::Available
            ) | (
                GlobalLifecycleStateKind::Paused,
                GlobalLifecycleStateKind::Retired
            ) | (
                GlobalLifecycleStateKind::Paused,
                GlobalLifecycleStateKind::Tombstoned
            ) | (
                GlobalLifecycleStateKind::Retired,
                GlobalLifecycleStateKind::Tombstoned
            )
        )
    }

    /// Builds a new lifecycle state from the current state.
    pub fn transition_to(
        &self,
        target_state: GlobalLifecycleStateKind,
        reason_ref: LifecycleReasonRef,
        actor_ref: ActorRef,
        changed_at: IdentityTimestamp,
        basis_ref: Option<GovernanceBasisRef>,
    ) -> Result<Self, IdentityDomainError> {
        Self::from_transition(
            self,
            target_state,
            reason_ref,
            actor_ref,
            changed_at,
            basis_ref,
        )
    }

    /// Returns whether the member is platform-available.
    pub fn is_available(&self) -> bool {
        self.state_kind == GlobalLifecycleStateKind::Available
    }

    /// Returns whether the state is terminal or terminal-candidate.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self.state_kind,
            GlobalLifecycleStateKind::Retired | GlobalLifecycleStateKind::Tombstoned
        )
    }

    /// Returns the stored governance basis ref.
    pub fn basis_ref(&self) -> Option<GovernanceBasisRef> {
        self.basis_ref.clone()
    }
}

/// Guard for explicit lifecycle transitions.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LifecycleTransitionPolicy {
    /// Loaded current lifecycle state.
    pub current_state: GlobalLifecycleState,
    /// Requested target state.
    pub target_state: GlobalLifecycleStateKind,
    /// Transition reason.
    pub reason_ref: LifecycleReasonRef,
    /// Request actor.
    pub actor_ref: ActorRef,
    /// Current operation channel.
    pub operation_channel: IdentityOperationChannel,
}

impl LifecycleTransitionPolicy {
    /// Creates a lifecycle transition policy.
    pub fn for_transition(
        current_state: GlobalLifecycleState,
        target_state: GlobalLifecycleStateKind,
        reason_ref: LifecycleReasonRef,
        actor_ref: ActorRef,
        operation_channel: IdentityOperationChannel,
    ) -> Self {
        Self {
            current_state,
            target_state,
            reason_ref,
            actor_ref,
            operation_channel,
        }
    }

    /// Asserts that the lifecycle change comes from an explicit write path.
    pub fn assert_explicit_command(&self) -> Result<(), IdentityDomainError> {
        match self.operation_channel {
            IdentityOperationChannel::Command => Ok(()),
            _ => Err(IdentityDomainError::write_channel_denied(
                "LifecycleTransitionPolicy",
                self.operation_channel,
                "only command channel may mutate lifecycle truth",
            )),
        }
    }

    /// Asserts that the target lifecycle transition is allowed.
    pub fn assert_allowed_transition(&self) -> Result<(), IdentityDomainError> {
        if self.current_state.can_transition_to(self.target_state) {
            return Ok(());
        }

        Err(IdentityDomainError::invalid_state_transition(
            "GlobalLifecycleState",
            "lifecycle transition is not allowed",
        ))
    }

    /// Rejects mixed external state families.
    pub fn assert_not_project_or_runtime_state(&self) -> Result<(), IdentityDomainError> {
        match self.target_state {
            GlobalLifecycleStateKind::Available
            | GlobalLifecycleStateKind::Paused
            | GlobalLifecycleStateKind::Retired
            | GlobalLifecycleStateKind::Tombstoned => Ok(()),
        }
    }
}

/// Guard for high-risk lifecycle actions that require governance basis validation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HighRiskLifecycleGuard {
    /// Requested target state.
    pub target_state: GlobalLifecycleStateKind,
    /// Formal lifecycle risk marker.
    pub action_risk_ref: LifecycleRiskRef,
    /// Optional governance basis ref supplied with the action.
    pub basis_ref: Option<GovernanceBasisRef>,
    /// Request actor.
    pub actor_ref: ActorRef,
}

impl HighRiskLifecycleGuard {
    /// Creates a high-risk lifecycle guard.
    pub fn for_action(
        target_state: GlobalLifecycleStateKind,
        action_risk_ref: LifecycleRiskRef,
        basis_ref: Option<GovernanceBasisRef>,
        actor_ref: ActorRef,
    ) -> Self {
        Self {
            target_state,
            action_risk_ref,
            basis_ref,
            actor_ref,
        }
    }

    /// Returns whether the action requires a governance basis.
    pub fn requires_basis(&self) -> bool {
        self.action_risk_ref.requires_governance_basis()
    }

    /// Asserts that a required basis is present.
    pub fn assert_basis_present(&self) -> Result<(), IdentityDomainError> {
        if !self.requires_basis() || self.basis_ref.is_some() {
            return Ok(());
        }

        Err(IdentityDomainError::policy_denied(
            "HighRiskLifecycleGuard",
            "high-risk lifecycle action requires a governance basis",
        ))
    }

    /// Asserts that the resolved basis summary authorizes the requested action.
    pub fn assert_basis_matches_action(
        &self,
        basis_summary: &GovernanceBasisSummary,
    ) -> Result<(), IdentityDomainError> {
        self.assert_basis_present()?;

        if !basis_summary.is_valid_for(&self.action_risk_ref) {
            return Err(IdentityDomainError::policy_denied(
                "HighRiskLifecycleGuard",
                "governance basis is not valid for the lifecycle action",
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use core_contracts::actor::{ActorKind, ActorRef};
    use identity_contracts::refs::{
        ExternalSourceRef, GovernanceBasisKind, GovernanceBasisRef, GovernanceBasisState,
        GovernanceBasisSummary, IdentityOperationChannel, IdentitySourceOwner, IdentitySourceRef,
        IdentityTimestamp, LifecycleReasonKind, LifecycleReasonRef, LifecycleRiskKind,
        LifecycleRiskRef,
    };

    use super::{
        GlobalLifecycleState, GlobalLifecycleStateKind, HighRiskLifecycleGuard,
        LifecycleTransitionPolicy,
    };
    use crate::errors::IdentityDomainError;

    fn actor() -> ActorRef {
        ActorRef::new("actor-1", ActorKind::Human)
    }

    fn timestamp(value: i64) -> IdentityTimestamp {
        IdentityTimestamp::from_clock(value).expect("valid timestamp")
    }

    fn source(owner: IdentitySourceOwner, token: &str) -> IdentitySourceRef {
        IdentitySourceRef::new(
            owner,
            ExternalSourceRef::new(token.to_owned()).expect("valid external source ref"),
        )
        .expect("valid source ref")
    }

    fn lifecycle_reason(kind: LifecycleReasonKind) -> LifecycleReasonRef {
        LifecycleReasonRef::new(
            kind,
            source(IdentitySourceOwner::Identity, "identity-source-1"),
        )
        .expect("valid lifecycle reason")
    }

    fn lifecycle_state(
        kind: GlobalLifecycleStateKind,
        reason_kind: LifecycleReasonKind,
    ) -> GlobalLifecycleState {
        GlobalLifecycleState {
            state_kind: kind,
            reason_ref: lifecycle_reason(reason_kind),
            changed_by_ref: actor(),
            changed_at: timestamp(1),
            basis_ref: None,
        }
    }

    fn risk(kind: LifecycleRiskKind) -> LifecycleRiskRef {
        LifecycleRiskRef::new(
            kind,
            source(IdentitySourceOwner::Governance, "governance-source-1"),
        )
        .expect("valid lifecycle risk")
    }

    #[test]
    fn lifecycle_allows_only_formal_transitions() {
        let available = lifecycle_state(
            GlobalLifecycleStateKind::Available,
            LifecycleReasonKind::InitialProvisioned,
        );
        assert!(available.can_transition_to(GlobalLifecycleStateKind::Paused));
        assert!(available.can_transition_to(GlobalLifecycleStateKind::Retired));
        assert!(!available.can_transition_to(GlobalLifecycleStateKind::Available));

        let retired = lifecycle_state(
            GlobalLifecycleStateKind::Retired,
            LifecycleReasonKind::Retirement,
        );
        assert!(retired.can_transition_to(GlobalLifecycleStateKind::Tombstoned));
        assert!(!retired.can_transition_to(GlobalLifecycleStateKind::Available));
    }

    #[test]
    fn tombstoned_is_terminal() {
        let tombstoned = lifecycle_state(
            GlobalLifecycleStateKind::Tombstoned,
            LifecycleReasonKind::Tombstone,
        );
        assert!(tombstoned.is_terminal());
        assert!(!tombstoned.can_transition_to(GlobalLifecycleStateKind::Paused));
    }

    #[test]
    fn high_risk_guard_requires_and_validates_basis_summary() {
        let basis_ref = GovernanceBasisRef::new(
            GovernanceBasisKind::Approval,
            ExternalSourceRef::new("basis-1".to_owned()).expect("valid basis ref"),
        )
        .expect("valid governance basis ref");
        let risk_ref = risk(LifecycleRiskKind::Critical);
        let guard = HighRiskLifecycleGuard::for_action(
            GlobalLifecycleStateKind::Tombstoned,
            risk_ref.clone(),
            Some(basis_ref.clone()),
            actor(),
        );

        let missing_basis_guard = HighRiskLifecycleGuard::for_action(
            GlobalLifecycleStateKind::Tombstoned,
            risk_ref.clone(),
            None,
            actor(),
        );
        assert!(matches!(
            missing_basis_guard.assert_basis_present(),
            Err(IdentityDomainError::PolicyDenied { .. })
        ));

        let invalid_summary = GovernanceBasisSummary::from_resolver(
            basis_ref.clone(),
            GovernanceBasisState::InvalidForAction,
            Some(risk_ref.clone()),
        );
        assert!(matches!(
            guard.assert_basis_matches_action(&invalid_summary),
            Err(IdentityDomainError::PolicyDenied { .. })
        ));

        let valid_summary = GovernanceBasisSummary::from_resolver(
            basis_ref,
            GovernanceBasisState::Valid,
            Some(risk_ref),
        );
        guard
            .assert_basis_matches_action(&valid_summary)
            .expect("valid basis summary should pass");
    }

    #[test]
    fn lifecycle_transition_policy_rejects_non_command_channels() {
        let policy = LifecycleTransitionPolicy::for_transition(
            lifecycle_state(
                GlobalLifecycleStateKind::Available,
                LifecycleReasonKind::InitialProvisioned,
            ),
            GlobalLifecycleStateKind::Paused,
            lifecycle_reason(LifecycleReasonKind::ManualPause),
            actor(),
            IdentityOperationChannel::Job,
        );

        assert!(matches!(
            policy.assert_explicit_command(),
            Err(IdentityDomainError::PolicyDenied { .. })
        ));
    }
}
