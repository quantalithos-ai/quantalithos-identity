//! Core member identity truth and anchor guards.

use core_contracts::actor::ActorRef;
use identity_contracts::refs::{
    GlobalMemberRef, IdentityAnchorReasonRef, IdentityOperationChannel, IdentitySourceOwner,
    IdentitySourceRef, IdentityTimestamp,
};

use crate::errors::IdentityDomainError;

/// Anchor state kind for a global member ref.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdentityAnchorStateKind {
    /// The member ref has been established and is occupied.
    Established,
    /// The member ref is permanently held after retirement.
    RetiredHeld,
    /// The member ref is permanently held after tombstoning.
    TombstoneHeld,
}

/// Anchor state for a global member ref.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdentityAnchorState {
    /// Current anchor state kind.
    pub state_kind: IdentityAnchorStateKind,
    /// Hold reason for non-initial states.
    pub reason_ref: Option<IdentityAnchorReasonRef>,
    /// Last change timestamp.
    pub changed_at: IdentityTimestamp,
}

impl IdentityAnchorState {
    /// Creates the initial established anchor state.
    pub fn established(changed_at: IdentityTimestamp) -> Self {
        Self {
            state_kind: IdentityAnchorStateKind::Established,
            reason_ref: None,
            changed_at,
        }
    }

    /// Creates a retired-hold anchor state.
    pub fn retired_held(
        reason_ref: IdentityAnchorReasonRef,
        changed_at: IdentityTimestamp,
    ) -> Self {
        Self {
            state_kind: IdentityAnchorStateKind::RetiredHeld,
            reason_ref: Some(reason_ref),
            changed_at,
        }
    }

    /// Creates a tombstone-hold anchor state.
    pub fn tombstone_held(
        reason_ref: IdentityAnchorReasonRef,
        changed_at: IdentityTimestamp,
    ) -> Self {
        Self {
            state_kind: IdentityAnchorStateKind::TombstoneHeld,
            reason_ref: Some(reason_ref),
            changed_at,
        }
    }

    /// Returns whether the ref may be reused.
    pub fn is_reusable(&self) -> bool {
        false
    }

    /// Returns whether the anchor is tombstone-held.
    pub fn is_tombstone_held(&self) -> bool {
        self.state_kind == IdentityAnchorStateKind::TombstoneHeld
    }
}

/// Global member truth aggregate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GlobalMember {
    /// Stable member ref.
    pub member_ref: GlobalMemberRef,
    /// Current anchor state.
    pub anchor_state: IdentityAnchorState,
    /// Body-free creation source.
    pub source_ref: IdentitySourceRef,
    /// Actor that established the member truth.
    pub created_by_ref: ActorRef,
    /// Creation timestamp.
    pub created_at: IdentityTimestamp,
}

impl GlobalMember {
    /// Establishes a new global member truth.
    pub fn establish(
        member_ref: GlobalMemberRef,
        source_ref: IdentitySourceRef,
        actor_ref: ActorRef,
        created_at: IdentityTimestamp,
    ) -> Result<Self, IdentityDomainError> {
        Ok(Self {
            member_ref,
            anchor_state: IdentityAnchorState::established(created_at),
            source_ref,
            created_by_ref: actor_ref,
            created_at,
        })
    }

    /// Returns the stable member ref.
    pub fn to_ref(&self) -> GlobalMemberRef {
        self.member_ref.clone()
    }

    /// Asserts that the provided ref matches this truth subject.
    pub fn assert_same_ref(&self, member_ref: GlobalMemberRef) -> Result<(), IdentityDomainError> {
        if self.member_ref.same_member(&member_ref) {
            return Ok(());
        }

        Err(IdentityDomainError::invalid_input(
            "member_ref",
            "member ref does not match loaded member truth",
        ))
    }

    /// Updates the anchor hold state after lifecycle terminal handling.
    pub fn hold_anchor(
        &mut self,
        anchor_state: IdentityAnchorState,
        _actor_ref: ActorRef,
    ) -> Result<(), IdentityDomainError> {
        match (&self.anchor_state.state_kind, &anchor_state.state_kind) {
            (IdentityAnchorStateKind::Established, IdentityAnchorStateKind::RetiredHeld)
            | (IdentityAnchorStateKind::Established, IdentityAnchorStateKind::TombstoneHeld)
            | (IdentityAnchorStateKind::RetiredHeld, IdentityAnchorStateKind::TombstoneHeld) => {
                if anchor_state.reason_ref.is_none() {
                    return Err(IdentityDomainError::missing_required_field("reason_ref"));
                }
                self.anchor_state = anchor_state;
                Ok(())
            }
            _ => Err(IdentityDomainError::invalid_state_transition(
                "IdentityAnchorState",
                "anchor hold transition is not allowed",
            )),
        }
    }
}

/// Guard for member establishment and read-only no-create boundaries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdentityAnchorPolicy {
    /// Candidate member ref.
    pub candidate_member_ref: GlobalMemberRef,
    /// Optional creation source.
    pub source_ref: Option<IdentitySourceRef>,
    /// Optional actor.
    pub actor_ref: Option<ActorRef>,
    /// Existing anchor state if the ref is already occupied.
    pub existing_anchor_state: Option<IdentityAnchorState>,
    /// Current operation channel.
    pub operation_channel: IdentityOperationChannel,
}

impl IdentityAnchorPolicy {
    /// Creates a policy for explicit member establishment.
    pub fn for_create(
        member_ref: GlobalMemberRef,
        source_ref: IdentitySourceRef,
        actor_ref: ActorRef,
        existing_anchor_state: Option<IdentityAnchorState>,
        operation_channel: IdentityOperationChannel,
    ) -> Self {
        Self {
            candidate_member_ref: member_ref,
            source_ref: Some(source_ref),
            actor_ref: Some(actor_ref),
            existing_anchor_state,
            operation_channel,
        }
    }

    /// Creates a policy for read-only anchor access.
    pub fn for_read(
        member_ref: GlobalMemberRef,
        operation_channel: IdentityOperationChannel,
    ) -> Self {
        Self {
            candidate_member_ref: member_ref,
            source_ref: None,
            actor_ref: None,
            existing_anchor_state: None,
            operation_channel,
        }
    }

    /// Runs all establishment guards.
    pub fn assert_can_establish(&self) -> Result<(), IdentityDomainError> {
        self.assert_ref_not_reused()?;
        self.assert_query_does_not_create()?;
        self.assert_not_external_account_truth()?;

        if self.source_ref.is_none() {
            return Err(IdentityDomainError::missing_required_field("source_ref"));
        }
        if self.actor_ref.is_none() {
            return Err(IdentityDomainError::missing_required_field("actor_ref"));
        }

        Ok(())
    }

    /// Rejects any attempt to reuse an occupied member ref.
    pub fn assert_ref_not_reused(&self) -> Result<(), IdentityDomainError> {
        if self.existing_anchor_state.is_some() {
            return Err(IdentityDomainError::policy_denied(
                "IdentityAnchorPolicy",
                "member ref is already occupied and cannot be reused",
            ));
        }

        Ok(())
    }

    /// Rejects create-like behavior on read-only channels.
    pub fn assert_query_does_not_create(&self) -> Result<(), IdentityDomainError> {
        match self.operation_channel {
            IdentityOperationChannel::Command | IdentityOperationChannel::Consumer => Ok(()),
            _ => Err(IdentityDomainError::write_channel_denied(
                "IdentityAnchorPolicy",
                self.operation_channel,
                "only command or consumer channels may establish a member truth",
            )),
        }
    }

    /// Rejects external truth owners that are not allowed to create member truth.
    pub fn assert_not_external_account_truth(&self) -> Result<(), IdentityDomainError> {
        let Some(source_ref) = self.source_ref.as_ref() else {
            return Ok(());
        };

        match source_ref.owner() {
            IdentitySourceOwner::Runtime | IdentitySourceOwner::Work => {
                Err(IdentityDomainError::invalid_source_owner(
                    "IdentityAnchorPolicy",
                    source_ref.owner(),
                    "runtime or work truth cannot establish member identity truth",
                ))
            }
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use core_contracts::actor::{ActorKind, ActorRef};
    use identity_contracts::refs::{
        ExternalSourceRef, GlobalMemberId, IdentityAnchorReasonKind, IdentityAnchorReasonRef,
        IdentitySourceRef,
    };

    use super::{GlobalMember, IdentityAnchorPolicy, IdentityAnchorState, IdentityAnchorStateKind};
    use crate::errors::IdentityDomainError;

    fn actor() -> ActorRef {
        ActorRef::new("actor-1", ActorKind::Human)
    }

    fn member_ref(id: &str) -> identity_contracts::refs::GlobalMemberRef {
        identity_contracts::refs::GlobalMemberRef::from_id(
            GlobalMemberId::new(id.to_owned()).expect("valid member id"),
        )
    }

    fn timestamp(value: i64) -> identity_contracts::refs::IdentityTimestamp {
        identity_contracts::refs::IdentityTimestamp::from_clock(value).expect("valid timestamp")
    }

    fn source(
        owner: identity_contracts::refs::IdentitySourceOwner,
        token: &str,
    ) -> IdentitySourceRef {
        IdentitySourceRef::new(
            owner,
            ExternalSourceRef::new(token.to_owned()).expect("valid external source ref"),
        )
        .expect("valid source ref")
    }

    fn retired_reason() -> IdentityAnchorReasonRef {
        IdentityAnchorReasonRef::new(
            IdentityAnchorReasonKind::Retired,
            source(
                identity_contracts::refs::IdentitySourceOwner::Identity,
                "identity-source-1",
            ),
        )
        .expect("valid retired reason")
    }

    #[test]
    fn ref_reuse_is_rejected() {
        let policy = IdentityAnchorPolicy::for_create(
            member_ref("member-1"),
            source(
                identity_contracts::refs::IdentitySourceOwner::Account,
                "account-1",
            ),
            actor(),
            Some(IdentityAnchorState::established(timestamp(1))),
            identity_contracts::refs::IdentityOperationChannel::Command,
        );

        assert_eq!(
            policy.assert_ref_not_reused(),
            Err(IdentityDomainError::policy_denied(
                "IdentityAnchorPolicy",
                "member ref is already occupied and cannot be reused",
            ))
        );
    }

    #[test]
    fn query_channel_cannot_create_member_truth() {
        let policy = IdentityAnchorPolicy::for_create(
            member_ref("member-1"),
            source(
                identity_contracts::refs::IdentitySourceOwner::Account,
                "account-1",
            ),
            actor(),
            None,
            identity_contracts::refs::IdentityOperationChannel::Query,
        );

        assert!(matches!(
            policy.assert_query_does_not_create(),
            Err(IdentityDomainError::PolicyDenied { .. })
        ));
    }

    #[test]
    fn hold_anchor_only_allows_formal_hold_transitions() {
        let mut member = GlobalMember::establish(
            member_ref("member-1"),
            source(
                identity_contracts::refs::IdentitySourceOwner::Account,
                "account-1",
            ),
            actor(),
            timestamp(1),
        )
        .expect("member established");

        member
            .hold_anchor(
                IdentityAnchorState::retired_held(retired_reason(), timestamp(2)),
                actor(),
            )
            .expect("retired hold allowed");
        assert_eq!(
            member.anchor_state.state_kind,
            IdentityAnchorStateKind::RetiredHeld
        );

        member
            .hold_anchor(
                IdentityAnchorState::tombstone_held(
                    IdentityAnchorReasonRef::new(
                        IdentityAnchorReasonKind::Tombstoned,
                        source(
                            identity_contracts::refs::IdentitySourceOwner::Identity,
                            "identity-source-2",
                        ),
                    )
                    .expect("valid tombstone reason"),
                    timestamp(3),
                ),
                actor(),
            )
            .expect("retired to tombstone hold allowed");
        assert_eq!(
            member.anchor_state.state_kind,
            IdentityAnchorStateKind::TombstoneHeld
        );

        let result = member.hold_anchor(IdentityAnchorState::established(timestamp(4)), actor());
        assert!(matches!(
            result,
            Err(IdentityDomainError::InvalidStateTransition { .. })
        ));
    }
}
