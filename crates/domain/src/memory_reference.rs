//! Memory reference relation truth and guards.

use core_contracts::actor::ActorRef;
use identity_contracts::refs::{
    ArchiveHandoffRef, ArchiveRef, GlobalMemberRef, IdentityOperationChannel, IdentityTimestamp,
    MemoryRef, MemoryReferenceChangeIntent, MemoryReferenceChangeMaterialKind,
    MemoryReferenceChangeMaterialMarker, MemoryReferenceReasonRef, MemoryReferenceRef,
    MemoryReferenceSourceState, MemoryReferenceSourceSummary, MemorySafeSummaryRef,
};

use crate::errors::IdentityDomainError;

/// Memory reference relation state kind.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MemoryReferenceStateKind {
    /// Linked to a trusted memory ref.
    Linked,
    /// Requires formal verification before trusted mainline use.
    PendingVerification,
    /// External reference may be stale.
    Stale,
    /// External carrier is unavailable.
    Unavailable,
    /// Relation migrated to new marker(s).
    Migrated,
    /// Relation points to archive/cold-storage marker(s).
    Archived,
    /// Archive or migration handoff is pending.
    HandoffPending,
    /// Archive or migration handoff failed.
    HandoffFailed,
}

/// Memory reference relation state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryReferenceState {
    /// State category.
    pub state_kind: MemoryReferenceStateKind,
    /// Optional memory ref.
    pub memory_ref: Option<MemoryRef>,
    /// Optional archive ref.
    pub archive_ref: Option<ArchiveRef>,
    /// Optional handoff marker.
    pub handoff_ref: Option<ArchiveHandoffRef>,
    /// Optional state reason.
    pub reason_ref: Option<MemoryReferenceReasonRef>,
    /// State timestamp.
    pub checked_at: IdentityTimestamp,
}

impl MemoryReferenceState {
    /// Creates a linked state.
    pub fn linked(
        memory_ref: MemoryRef,
        reason_ref: MemoryReferenceReasonRef,
        checked_at: IdentityTimestamp,
    ) -> Self {
        Self {
            state_kind: MemoryReferenceStateKind::Linked,
            memory_ref: Some(memory_ref),
            archive_ref: None,
            handoff_ref: None,
            reason_ref: Some(reason_ref),
            checked_at,
        }
    }

    /// Creates a pending-verification state.
    pub fn pending_verification(
        memory_ref: Option<MemoryRef>,
        archive_ref: Option<ArchiveRef>,
        handoff_ref: Option<ArchiveHandoffRef>,
        reason_ref: MemoryReferenceReasonRef,
        checked_at: IdentityTimestamp,
    ) -> Self {
        Self {
            state_kind: MemoryReferenceStateKind::PendingVerification,
            memory_ref,
            archive_ref,
            handoff_ref,
            reason_ref: Some(reason_ref),
            checked_at,
        }
    }

    /// Creates an archived state.
    pub fn archived(
        archive_ref: ArchiveRef,
        handoff_ref: ArchiveHandoffRef,
        reason_ref: MemoryReferenceReasonRef,
        checked_at: IdentityTimestamp,
    ) -> Self {
        Self {
            state_kind: MemoryReferenceStateKind::Archived,
            memory_ref: None,
            archive_ref: Some(archive_ref),
            handoff_ref: Some(handoff_ref),
            reason_ref: Some(reason_ref),
            checked_at,
        }
    }

    /// Creates a handoff-failed state.
    pub fn handoff_failed(
        handoff_ref: ArchiveHandoffRef,
        reason_ref: MemoryReferenceReasonRef,
        checked_at: IdentityTimestamp,
    ) -> Self {
        Self {
            state_kind: MemoryReferenceStateKind::HandoffFailed,
            memory_ref: None,
            archive_ref: None,
            handoff_ref: Some(handoff_ref),
            reason_ref: Some(reason_ref),
            checked_at,
        }
    }

    /// Returns whether the state may be used for a safe summary.
    pub fn is_usable_for_summary(&self) -> bool {
        matches!(
            self.state_kind,
            MemoryReferenceStateKind::Linked
                | MemoryReferenceStateKind::Archived
                | MemoryReferenceStateKind::Migrated
        )
    }

    /// Returns whether the relation requires refresh or verification.
    pub fn requires_refresh(&self) -> bool {
        matches!(
            self.state_kind,
            MemoryReferenceStateKind::PendingVerification
                | MemoryReferenceStateKind::Stale
                | MemoryReferenceStateKind::Unavailable
                | MemoryReferenceStateKind::HandoffFailed
        )
    }

    /// Returns whether the handoff relation state is terminal for this batch.
    pub fn is_handoff_terminal(&self) -> bool {
        matches!(
            self.state_kind,
            MemoryReferenceStateKind::Archived | MemoryReferenceStateKind::HandoffFailed
        )
    }

    /// Marks the state stale while preserving formal refs.
    pub fn mark_stale(
        &mut self,
        reason_ref: MemoryReferenceReasonRef,
        checked_at: IdentityTimestamp,
    ) -> Result<(), IdentityDomainError> {
        self.state_kind = MemoryReferenceStateKind::Stale;
        self.reason_ref = Some(reason_ref);
        self.checked_at = checked_at;
        Ok(())
    }

    /// Marks the state unavailable while preserving formal refs.
    pub fn mark_unavailable(
        &mut self,
        reason_ref: MemoryReferenceReasonRef,
        checked_at: IdentityTimestamp,
    ) -> Result<(), IdentityDomainError> {
        self.state_kind = MemoryReferenceStateKind::Unavailable;
        self.reason_ref = Some(reason_ref);
        self.checked_at = checked_at;
        Ok(())
    }

    /// Marks the relation migrated with new formal markers.
    pub fn mark_migrated(
        &mut self,
        memory_ref: Option<MemoryRef>,
        archive_ref: Option<ArchiveRef>,
        handoff_ref: ArchiveHandoffRef,
        reason_ref: MemoryReferenceReasonRef,
        checked_at: IdentityTimestamp,
    ) -> Result<(), IdentityDomainError> {
        self.state_kind = MemoryReferenceStateKind::Migrated;
        self.memory_ref = memory_ref;
        self.archive_ref = archive_ref;
        self.handoff_ref = Some(handoff_ref);
        self.reason_ref = Some(reason_ref);
        self.checked_at = checked_at;
        Ok(())
    }
}

/// Identity-owned memory reference relation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryReference {
    /// Stable relation ref.
    pub memory_reference_ref: MemoryReferenceRef,
    /// Member that owns the relation.
    pub member_ref: GlobalMemberRef,
    /// Optional memory ref.
    pub memory_ref: Option<MemoryRef>,
    /// Optional archive ref.
    pub archive_ref: Option<ArchiveRef>,
    /// Optional handoff marker.
    pub archive_handoff_ref: Option<ArchiveHandoffRef>,
    /// Source ref that drove the current relation state.
    pub source_ref: identity_contracts::refs::MemoryReferenceSourceRef,
    /// Optional safe summary ref.
    pub safe_summary_ref: Option<MemorySafeSummaryRef>,
    /// Current relation state.
    pub reference_state: MemoryReferenceState,
    /// Latest change reason.
    pub change_reason_ref: MemoryReferenceReasonRef,
    /// Latest actor.
    pub changed_by_ref: ActorRef,
    /// Latest change time.
    pub changed_at: IdentityTimestamp,
}

impl MemoryReference {
    /// Creates a linked or pending relation for a member.
    pub fn link_for_member(
        memory_reference_ref: MemoryReferenceRef,
        member_ref: GlobalMemberRef,
        source_summary: MemoryReferenceSourceSummary,
        reason_ref: MemoryReferenceReasonRef,
        actor_ref: ActorRef,
        changed_at: IdentityTimestamp,
    ) -> Result<Self, IdentityDomainError> {
        let reference_state = match source_summary.source_state {
            MemoryReferenceSourceState::Trusted => {
                let memory_ref = source_summary
                    .memory_ref
                    .clone()
                    .ok_or_else(|| IdentityDomainError::missing_required_field("memory_ref"))?;
                MemoryReferenceState::linked(memory_ref, reason_ref.clone(), changed_at)
            }
            MemoryReferenceSourceState::PendingVerification
            | MemoryReferenceSourceState::Untrusted => MemoryReferenceState::pending_verification(
                source_summary.memory_ref.clone(),
                source_summary.archive_ref.clone(),
                source_summary.archive_handoff_ref.clone(),
                reason_ref.clone(),
                changed_at,
            ),
            _ => {
                return Err(IdentityDomainError::policy_denied(
                    "MemoryReference",
                    "source summary cannot create linked relation in the current branch",
                ));
            }
        };

        Ok(Self {
            memory_reference_ref,
            member_ref,
            memory_ref: source_summary.memory_ref.clone(),
            archive_ref: source_summary.archive_ref.clone(),
            archive_handoff_ref: source_summary.archive_handoff_ref.clone(),
            source_ref: source_summary.source_ref,
            safe_summary_ref: source_summary.safe_summary_ref,
            reference_state,
            change_reason_ref: reason_ref,
            changed_by_ref: actor_ref,
            changed_at,
        })
    }

    /// Creates an archive or handoff-driven relation.
    pub fn from_archive_handoff(
        memory_reference_ref: MemoryReferenceRef,
        member_ref: GlobalMemberRef,
        source_summary: MemoryReferenceSourceSummary,
        reason_ref: MemoryReferenceReasonRef,
        actor_ref: ActorRef,
        changed_at: IdentityTimestamp,
    ) -> Result<Self, IdentityDomainError> {
        let reference_state = match source_summary.source_state {
            MemoryReferenceSourceState::HandoffResultAccepted => {
                if let (Some(archive_ref), Some(handoff_ref)) = (
                    source_summary.archive_ref.clone(),
                    source_summary.archive_handoff_ref.clone(),
                ) {
                    MemoryReferenceState::archived(
                        archive_ref,
                        handoff_ref,
                        reason_ref.clone(),
                        changed_at,
                    )
                } else if source_summary.archive_handoff_ref.is_some() {
                    MemoryReferenceState {
                        state_kind: MemoryReferenceStateKind::HandoffPending,
                        memory_ref: source_summary.memory_ref.clone(),
                        archive_ref: source_summary.archive_ref.clone(),
                        handoff_ref: source_summary.archive_handoff_ref.clone(),
                        reason_ref: Some(reason_ref.clone()),
                        checked_at: changed_at,
                    }
                } else {
                    return Err(IdentityDomainError::missing_required_field(
                        "archive_handoff_ref",
                    ));
                }
            }
            MemoryReferenceSourceState::HandoffResultFailed => {
                let handoff_ref = source_summary.archive_handoff_ref.clone().ok_or_else(|| {
                    IdentityDomainError::missing_required_field("archive_handoff_ref")
                })?;
                MemoryReferenceState::handoff_failed(handoff_ref, reason_ref.clone(), changed_at)
            }
            _ => {
                return Err(IdentityDomainError::policy_denied(
                    "MemoryReference",
                    "archive handoff relation requires a formal handoff result source state",
                ));
            }
        };

        Ok(Self {
            memory_reference_ref,
            member_ref,
            memory_ref: source_summary.memory_ref.clone(),
            archive_ref: source_summary.archive_ref.clone(),
            archive_handoff_ref: source_summary.archive_handoff_ref.clone(),
            source_ref: source_summary.source_ref,
            safe_summary_ref: source_summary.safe_summary_ref,
            reference_state,
            change_reason_ref: reason_ref,
            changed_by_ref: actor_ref,
            changed_at,
        })
    }

    /// Returns whether the relation belongs to the given member.
    pub fn belongs_to(&self, member_ref: &GlobalMemberRef) -> bool {
        self.member_ref.same_member(member_ref)
    }

    /// Attaches archive and handoff refs to an existing relation.
    pub fn attach_archive_ref(
        &mut self,
        archive_ref: ArchiveRef,
        archive_handoff_ref: ArchiveHandoffRef,
        reason_ref: MemoryReferenceReasonRef,
        actor_ref: ActorRef,
        changed_at: IdentityTimestamp,
    ) -> Result<(), IdentityDomainError> {
        self.archive_ref = Some(archive_ref.clone());
        self.archive_handoff_ref = Some(archive_handoff_ref.clone());
        self.reference_state = MemoryReferenceState::archived(
            archive_ref,
            archive_handoff_ref,
            reason_ref.clone(),
            changed_at,
        );
        self.change_reason_ref = reason_ref;
        self.changed_by_ref = actor_ref;
        self.changed_at = changed_at;
        Ok(())
    }

    /// Replaces the relation state after policy validation.
    pub fn update_reference_state(
        &mut self,
        reference_state: MemoryReferenceState,
        reason_ref: MemoryReferenceReasonRef,
        actor_ref: ActorRef,
        changed_at: IdentityTimestamp,
    ) -> Result<(), IdentityDomainError> {
        self.memory_ref = reference_state.memory_ref.clone();
        self.archive_ref = reference_state.archive_ref.clone();
        self.archive_handoff_ref = reference_state.handoff_ref.clone();
        self.reference_state = reference_state;
        self.change_reason_ref = reason_ref;
        self.changed_by_ref = actor_ref;
        self.changed_at = changed_at;
        Ok(())
    }

    /// Returns whether the relation requires verification.
    pub fn requires_verification(&self) -> bool {
        self.reference_state.requires_refresh()
    }

    /// Returns whether the relation carries any external body.
    pub fn has_external_body(&self) -> bool {
        false
    }
}

/// Guard for memory reference relation changes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemoryReferencePolicy {
    /// Member that owns the relation.
    pub member_ref: GlobalMemberRef,
    /// Whether the member exists in loaded truth.
    pub member_exists: bool,
    /// Body-free source summary.
    pub source_summary: MemoryReferenceSourceSummary,
    /// Change reason.
    pub reason_ref: MemoryReferenceReasonRef,
    /// Change actor.
    pub actor_ref: ActorRef,
    /// Operation channel.
    pub operation_channel: IdentityOperationChannel,
    /// Requested change intent.
    pub change_intent: MemoryReferenceChangeIntent,
    /// Body-free material marker.
    pub change_material_marker: MemoryReferenceChangeMaterialMarker,
}

impl MemoryReferencePolicy {
    /// Creates a policy for trusted linking.
    pub fn for_link(
        member_ref: GlobalMemberRef,
        member_exists: bool,
        source_summary: MemoryReferenceSourceSummary,
        reason_ref: MemoryReferenceReasonRef,
        actor_ref: ActorRef,
        operation_channel: IdentityOperationChannel,
        change_material_marker: MemoryReferenceChangeMaterialMarker,
    ) -> Self {
        Self {
            member_ref,
            member_exists,
            source_summary,
            reason_ref,
            actor_ref,
            operation_channel,
            change_intent: MemoryReferenceChangeIntent::LinkMemory,
            change_material_marker,
        }
    }

    /// Creates a policy for state refresh.
    pub fn for_refresh(
        member_ref: GlobalMemberRef,
        member_exists: bool,
        source_summary: MemoryReferenceSourceSummary,
        reason_ref: MemoryReferenceReasonRef,
        actor_ref: ActorRef,
        operation_channel: IdentityOperationChannel,
        change_material_marker: MemoryReferenceChangeMaterialMarker,
    ) -> Self {
        Self {
            member_ref,
            member_exists,
            source_summary,
            reason_ref,
            actor_ref,
            operation_channel,
            change_intent: MemoryReferenceChangeIntent::RefreshState,
            change_material_marker,
        }
    }

    /// Creates a policy for archive or handoff result handling.
    pub fn for_archive_handoff(
        member_ref: GlobalMemberRef,
        member_exists: bool,
        source_summary: MemoryReferenceSourceSummary,
        reason_ref: MemoryReferenceReasonRef,
        actor_ref: ActorRef,
        operation_channel: IdentityOperationChannel,
        change_material_marker: MemoryReferenceChangeMaterialMarker,
    ) -> Self {
        Self {
            member_ref,
            member_exists,
            source_summary,
            reason_ref,
            actor_ref,
            operation_channel,
            change_intent: MemoryReferenceChangeIntent::RecordArchiveHandoffResult,
            change_material_marker,
        }
    }

    /// Asserts that the member exists.
    pub fn assert_member_exists(&self) -> Result<(), IdentityDomainError> {
        if self.member_exists {
            return Ok(());
        }

        Err(IdentityDomainError::policy_denied(
            "MemoryReferencePolicy",
            "memory reference change requires an established member",
        ))
    }

    /// Asserts that at least one formal ref or marker is present.
    pub fn assert_reference_present(&self) -> Result<(), IdentityDomainError> {
        if self.source_summary.has_reference() {
            return Ok(());
        }

        Err(IdentityDomainError::policy_denied(
            "MemoryReferencePolicy",
            "memory reference change requires at least one formal ref or handoff marker",
        ))
    }

    /// Asserts that the source branch is trusted for the requested intent.
    pub fn assert_source_trusted(&self) -> Result<(), IdentityDomainError> {
        match self.change_intent {
            MemoryReferenceChangeIntent::LinkMemory => {
                if self.source_summary.source_state == MemoryReferenceSourceState::Trusted {
                    return Ok(());
                }
            }
            MemoryReferenceChangeIntent::RefreshState => {
                if matches!(
                    self.source_summary.source_state,
                    MemoryReferenceSourceState::Stale
                        | MemoryReferenceSourceState::Unavailable
                        | MemoryReferenceSourceState::PendingVerification
                        | MemoryReferenceSourceState::Untrusted
                ) {
                    return Ok(());
                }
            }
            MemoryReferenceChangeIntent::AttachArchive
            | MemoryReferenceChangeIntent::RecordArchiveHandoffResult => {
                if matches!(
                    self.source_summary.source_state,
                    MemoryReferenceSourceState::HandoffResultAccepted
                        | MemoryReferenceSourceState::HandoffResultFailed
                ) {
                    return Ok(());
                }
            }
            MemoryReferenceChangeIntent::MarkPendingVerification => {
                if self.source_summary.requires_verification() {
                    return Ok(());
                }
            }
            _ => {}
        }

        Err(IdentityDomainError::policy_denied(
            "MemoryReferencePolicy",
            "source state is not allowed for the requested relation change",
        ))
    }

    /// Rejects forbidden body material.
    pub fn assert_body_free(&self) -> Result<(), IdentityDomainError> {
        match self.change_material_marker.material_kind {
            MemoryReferenceChangeMaterialKind::SafeSummaryMarker
            | MemoryReferenceChangeMaterialKind::ReferenceMarkersOnly
            | MemoryReferenceChangeMaterialKind::HandoffMarkerOnly => Ok(()),
            _ => Err(IdentityDomainError::policy_denied(
                "MemoryReferencePolicy",
                "memory, embedding, archive, or receipt body cannot enter identity truth",
            )),
        }
    }

    /// Rejects non-body-free handoff markers.
    pub fn assert_handoff_marker_body_free(&self) -> Result<(), IdentityDomainError> {
        if self.source_summary.archive_handoff_ref.is_some()
            && self.change_material_marker.material_kind
                != MemoryReferenceChangeMaterialKind::ForbiddenExternalBody
        {
            return Ok(());
        }

        Err(IdentityDomainError::policy_denied(
            "MemoryReferencePolicy",
            "handoff marker must remain body-free",
        ))
    }

    /// Rejects attempts to write or delete external owner truth.
    pub fn assert_not_external_owner_write(&self) -> Result<(), IdentityDomainError> {
        match self.change_intent {
            MemoryReferenceChangeIntent::ForbiddenExternalOwnerWrite
            | MemoryReferenceChangeIntent::ForbiddenExternalBodyDelete => {
                Err(IdentityDomainError::policy_denied(
                    "MemoryReferencePolicy",
                    "identity relation must not write or delete external carrier truth",
                ))
            }
            _ => Ok(()),
        }
    }

    /// Restricts writes to command or callback channels for this batch.
    pub fn assert_allowed_write_channel(&self) -> Result<(), IdentityDomainError> {
        match self.operation_channel {
            IdentityOperationChannel::Command
            | IdentityOperationChannel::Consumer
            | IdentityOperationChannel::HandoffCallback => Ok(()),
            _ => Err(IdentityDomainError::write_channel_denied(
                "MemoryReferencePolicy",
                self.operation_channel,
                "only command, consumer, or handoff callback channels may write memory relations",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use core_contracts::actor::{ActorKind, ActorRef};
    use identity_contracts::refs::{
        ArchiveHandoffRef, ArchiveRef, ExternalSourceRef, GlobalMemberId, GlobalMemberRef,
        IdentityOperationChannel, IdentitySourceOwner, IdentitySourceRef, IdentityTimestamp,
        MemoryRef, MemoryReferenceChangeMaterialKind, MemoryReferenceChangeMaterialMarker,
        MemoryReferenceId, MemoryReferenceReasonKind, MemoryReferenceReasonRef, MemoryReferenceRef,
        MemoryReferenceSourceKind, MemoryReferenceSourceState, MemoryReferenceSourceSummary,
        MemorySafeSummaryRef,
    };

    use super::{MemoryReference, MemoryReferencePolicy, MemoryReferenceStateKind};
    use crate::errors::IdentityDomainError;

    fn actor() -> ActorRef {
        ActorRef::new("actor-1", ActorKind::Human)
    }

    fn timestamp(value: i64) -> IdentityTimestamp {
        IdentityTimestamp::from_clock(value).expect("valid timestamp")
    }

    fn member_ref(id: &str) -> GlobalMemberRef {
        GlobalMemberRef::from_id(GlobalMemberId::new(id.to_owned()).expect("valid member id"))
    }

    fn memory_source(token: &str) -> IdentitySourceRef {
        IdentitySourceRef::new(
            IdentitySourceOwner::MemoryArchive,
            ExternalSourceRef::new(token.to_owned()).expect("valid external source ref"),
        )
        .expect("valid source ref")
    }

    fn relation_ref(id: &str) -> MemoryReferenceRef {
        MemoryReferenceRef::from_id(
            MemoryReferenceId::new(id.to_owned()).expect("valid memory reference id"),
        )
    }

    fn reason(kind: MemoryReferenceReasonKind) -> MemoryReferenceReasonRef {
        MemoryReferenceReasonRef::new(kind, memory_source("reason-source-1"))
            .expect("valid memory reference reason")
    }

    fn trusted_summary() -> MemoryReferenceSourceSummary {
        let source_ref = identity_contracts::refs::MemoryReferenceSourceRef::new(
            MemoryReferenceSourceKind::MemorySourceEvent,
            memory_source("memory-source-1"),
        )
        .expect("valid source ref");
        let memory_ref =
            MemoryRef::from_source(memory_source("memory-carrier-1")).expect("valid memory ref");

        MemoryReferenceSourceSummary::from_resolver(
            source_ref.clone(),
            Some(memory_ref),
            None,
            None,
            Some(
                MemorySafeSummaryRef::new(source_ref, "memory-summary-1")
                    .expect("valid safe summary"),
            ),
            MemoryReferenceSourceState::Trusted,
        )
    }

    fn handoff_summary(state: MemoryReferenceSourceState) -> MemoryReferenceSourceSummary {
        let source_ref = identity_contracts::refs::MemoryReferenceSourceRef::new(
            MemoryReferenceSourceKind::ArchiveHandoffResult,
            memory_source("handoff-source-1"),
        )
        .expect("valid handoff source ref");
        let archive_ref =
            ArchiveRef::from_source(memory_source("archive-carrier-1")).expect("valid archive ref");
        let handoff_ref = ArchiveHandoffRef::new(memory_source("handoff-marker-1"), "handoff-1")
            .expect("valid handoff ref");

        MemoryReferenceSourceSummary::from_resolver(
            source_ref,
            None,
            Some(archive_ref),
            Some(handoff_ref),
            None,
            state,
        )
    }

    #[test]
    fn forbidden_body_is_rejected() {
        let policy = MemoryReferencePolicy::for_link(
            member_ref("member-1"),
            true,
            trusted_summary(),
            reason(MemoryReferenceReasonKind::ManualMaintain),
            actor(),
            IdentityOperationChannel::Command,
            MemoryReferenceChangeMaterialMarker {
                material_kind: MemoryReferenceChangeMaterialKind::ForbiddenMemoryBody,
                source_ref: None,
            },
        );

        assert!(matches!(
            policy.assert_body_free(),
            Err(IdentityDomainError::PolicyDenied { .. })
        ));
    }

    #[test]
    fn missing_refs_are_rejected() {
        let empty_source_ref = identity_contracts::refs::MemoryReferenceSourceRef::new(
            MemoryReferenceSourceKind::ReferenceRefreshMarker,
            IdentitySourceRef::new(
                IdentitySourceOwner::Identity,
                ExternalSourceRef::new("identity-source-1".to_owned())
                    .expect("valid external source ref"),
            )
            .expect("valid source ref"),
        )
        .expect("valid source ref");
        let summary = MemoryReferenceSourceSummary::from_resolver(
            empty_source_ref,
            None,
            None,
            None,
            None,
            MemoryReferenceSourceState::PendingVerification,
        );
        let policy = MemoryReferencePolicy::for_refresh(
            member_ref("member-1"),
            true,
            summary,
            reason(MemoryReferenceReasonKind::SourcePendingVerification),
            actor(),
            IdentityOperationChannel::Command,
            MemoryReferenceChangeMaterialMarker {
                material_kind: MemoryReferenceChangeMaterialKind::SafeSummaryMarker,
                source_ref: None,
            },
        );

        assert!(matches!(
            policy.assert_reference_present(),
            Err(IdentityDomainError::PolicyDenied { .. })
        ));
    }

    #[test]
    fn pending_verification_and_handoff_rules_are_preserved() {
        let trusted_relation = MemoryReference::link_for_member(
            relation_ref("relation-1"),
            member_ref("member-1"),
            trusted_summary(),
            reason(MemoryReferenceReasonKind::ManualMaintain),
            actor(),
            timestamp(1),
        )
        .expect("trusted link should create relation");
        assert_eq!(
            trusted_relation.reference_state.state_kind,
            MemoryReferenceStateKind::Linked
        );

        let handoff_relation = MemoryReference::from_archive_handoff(
            relation_ref("relation-2"),
            member_ref("member-1"),
            handoff_summary(MemoryReferenceSourceState::HandoffResultAccepted),
            reason(MemoryReferenceReasonKind::ArchiveHandoffResult),
            actor(),
            timestamp(2),
        )
        .expect("accepted handoff should create relation");
        assert!(matches!(
            handoff_relation.reference_state.state_kind,
            MemoryReferenceStateKind::Archived | MemoryReferenceStateKind::HandoffPending
        ));

        let failed_handoff_relation = MemoryReference::from_archive_handoff(
            relation_ref("relation-3"),
            member_ref("member-1"),
            handoff_summary(MemoryReferenceSourceState::HandoffResultFailed),
            reason(MemoryReferenceReasonKind::ArchiveHandoffResult),
            actor(),
            timestamp(3),
        )
        .expect("failed handoff should create relation");
        assert_eq!(
            failed_handoff_relation.reference_state.state_kind,
            MemoryReferenceStateKind::HandoffFailed
        );
    }
}
