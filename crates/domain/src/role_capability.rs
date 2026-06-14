//! Role capability summary truth, source snapshots, and guards.

use core_contracts::actor::ActorRef;
use identity_contracts::refs::{
    CapabilityEvidenceRef, CapabilitySourceRef, GlobalMemberRef, IdentityOperationChannel,
    IdentityTimestamp, RoleCapabilityChangeMaterialKind, RoleCapabilityChangeMaterialMarker,
    RoleCapabilityChangeReasonKind, RoleCapabilityChangeReasonRef, RoleCapabilitySafeSummaryRef,
    RoleCapabilitySourceRef, RoleCapabilitySourceSnapshotRef, RoleCapabilitySourceVersionRef,
    RoleCapabilitySummaryRef, RoleSourceRef,
};

use crate::errors::IdentityDomainError;

/// Role capability source snapshot state owned by the domain layer.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RoleCapabilitySourceStateKind {
    /// Source is resolved and usable for active summary writes.
    SourceResolved,
    /// Source version changed or snapshot is otherwise stale.
    SourceStale,
    /// Source is currently unavailable.
    SourceUnavailable,
    /// Source marker cannot map to a recognized source.
    SourceUnrecognized,
    /// Snapshot has been replaced by a newer version.
    SourceSuperseded,
}

/// Role capability summary state.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RoleCapabilitySummaryStateKind {
    /// The summary is active and usable.
    Active,
    /// The summary is stale and requires refresh.
    Stale,
    /// The summary source is unavailable.
    Unavailable,
    /// The summary requires formal reconciliation.
    PendingReconciliation,
    /// The summary has been superseded.
    Superseded,
}

/// Identity-owned role capability summary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoleCapabilitySummary {
    /// Stable summary ref.
    pub summary_ref: RoleCapabilitySummaryRef,
    /// Member that owns the summary.
    pub member_ref: GlobalMemberRef,
    /// Optional role source ref.
    pub role_source_ref: Option<RoleSourceRef>,
    /// Capability source refs.
    pub capability_source_refs: Vec<CapabilitySourceRef>,
    /// Evidence refs.
    pub evidence_refs: Vec<CapabilityEvidenceRef>,
    /// Safe summary marker.
    pub safe_summary_ref: RoleCapabilitySafeSummaryRef,
    /// Current source snapshot ref.
    pub source_snapshot_ref: RoleCapabilitySourceSnapshotRef,
    /// Summary state.
    pub summary_state: RoleCapabilitySummaryStateKind,
    /// Latest actor that changed the summary.
    pub changed_by_ref: ActorRef,
    /// Latest change time.
    pub changed_at: IdentityTimestamp,
}

impl RoleCapabilitySummary {
    /// Creates a role capability summary for a member from a usable snapshot.
    pub fn create_for_member(
        summary_ref: RoleCapabilitySummaryRef,
        member_ref: GlobalMemberRef,
        source_snapshot: &RoleCapabilitySourceSnapshot,
        safe_summary_ref: RoleCapabilitySafeSummaryRef,
        evidence_refs: Vec<CapabilityEvidenceRef>,
        actor_ref: ActorRef,
        changed_at: IdentityTimestamp,
    ) -> Result<Self, IdentityDomainError> {
        if !source_snapshot.is_usable_for_summary() {
            return Err(IdentityDomainError::policy_denied(
                "RoleCapabilitySummary",
                "source snapshot is not usable for an active summary",
            ));
        }
        if !safe_summary_ref.belongs_to_source(&source_snapshot.source_ref) {
            return Err(IdentityDomainError::invalid_input(
                "safe_summary_ref",
                "safe summary does not belong to the source snapshot",
            ));
        }
        if evidence_refs.is_empty() {
            return Err(IdentityDomainError::missing_required_field("evidence_refs"));
        }

        Ok(Self {
            summary_ref,
            member_ref,
            role_source_ref: None,
            capability_source_refs: Vec::new(),
            evidence_refs,
            safe_summary_ref,
            source_snapshot_ref: source_snapshot.snapshot_ref.clone(),
            summary_state: RoleCapabilitySummaryStateKind::Active,
            changed_by_ref: actor_ref,
            changed_at,
        })
    }

    /// Returns whether the summary belongs to the given member.
    pub fn belongs_to(&self, member_ref: GlobalMemberRef) -> bool {
        self.member_ref.same_member(&member_ref)
    }

    /// Attaches a role source to the summary.
    pub fn attach_role_source(
        &mut self,
        role_source_ref: RoleSourceRef,
        source_snapshot: &RoleCapabilitySourceSnapshot,
        actor_ref: ActorRef,
        changed_at: IdentityTimestamp,
    ) -> Result<(), IdentityDomainError> {
        if !source_snapshot.matches_source(role_source_ref.canonical_source().clone()) {
            return Err(IdentityDomainError::invalid_input(
                "role_source_ref",
                "role source does not match the source snapshot",
            ));
        }
        if !source_snapshot.is_usable_for_summary() {
            return Err(IdentityDomainError::policy_denied(
                "RoleCapabilitySummary",
                "source snapshot is not usable for active summary update",
            ));
        }

        self.role_source_ref = Some(role_source_ref);
        self.source_snapshot_ref = source_snapshot.snapshot_ref.clone();
        self.summary_state = RoleCapabilitySummaryStateKind::Active;
        self.changed_by_ref = actor_ref;
        self.changed_at = changed_at;
        Ok(())
    }

    /// Updates the capability summary refs and evidence.
    pub fn update_capability_summary(
        &mut self,
        capability_source_refs: Vec<CapabilitySourceRef>,
        evidence_refs: Vec<CapabilityEvidenceRef>,
        safe_summary_ref: RoleCapabilitySafeSummaryRef,
        actor_ref: ActorRef,
        changed_at: IdentityTimestamp,
    ) -> Result<(), IdentityDomainError> {
        if capability_source_refs.is_empty() {
            return Err(IdentityDomainError::missing_required_field(
                "capability_source_refs",
            ));
        }
        if evidence_refs.is_empty() {
            return Err(IdentityDomainError::missing_required_field("evidence_refs"));
        }

        self.capability_source_refs = capability_source_refs;
        self.evidence_refs = evidence_refs;
        self.safe_summary_ref = safe_summary_ref;
        self.summary_state = RoleCapabilitySummaryStateKind::Active;
        self.changed_by_ref = actor_ref;
        self.changed_at = changed_at;
        Ok(())
    }

    /// Marks the summary stale after a source change.
    pub fn mark_stale(
        &mut self,
        source_snapshot: &RoleCapabilitySourceSnapshot,
        changed_at: IdentityTimestamp,
    ) -> Result<(), IdentityDomainError> {
        if self.summary_state == RoleCapabilitySummaryStateKind::Superseded {
            return Err(IdentityDomainError::invalid_state_transition(
                "RoleCapabilitySummary",
                "superseded summary cannot be reopened",
            ));
        }
        self.source_snapshot_ref = source_snapshot.snapshot_ref.clone();
        self.summary_state = RoleCapabilitySummaryStateKind::Stale;
        self.changed_at = changed_at;
        Ok(())
    }

    /// Marks the summary unavailable for the given source.
    pub fn mark_unavailable(
        &mut self,
        source_ref: RoleCapabilitySourceRef,
        changed_at: IdentityTimestamp,
    ) -> Result<(), IdentityDomainError> {
        if self.summary_state == RoleCapabilitySummaryStateKind::Superseded {
            return Err(IdentityDomainError::invalid_state_transition(
                "RoleCapabilitySummary",
                "superseded summary cannot be reopened",
            ));
        }
        if let Some(role_source_ref) = self.role_source_ref.as_ref() {
            if !role_source_ref.canonical_source().same_source(&source_ref) {
                return Err(IdentityDomainError::invalid_input(
                    "source_ref",
                    "source ref does not match the summary source",
                ));
            }
        }
        self.summary_state = RoleCapabilitySummaryStateKind::Unavailable;
        self.changed_at = changed_at;
        Ok(())
    }

    /// Returns whether the summary needs reconciliation or refresh.
    pub fn requires_reconciliation(&self) -> bool {
        matches!(
            self.summary_state,
            RoleCapabilitySummaryStateKind::Stale
                | RoleCapabilitySummaryStateKind::Unavailable
                | RoleCapabilitySummaryStateKind::PendingReconciliation
        )
    }
}

/// Method-library source snapshot for role capability maintenance.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoleCapabilitySourceSnapshot {
    /// Stable snapshot ref.
    pub snapshot_ref: RoleCapabilitySourceSnapshotRef,
    /// Canonical source ref.
    pub source_ref: RoleCapabilitySourceRef,
    /// Formal source version.
    pub source_version_ref: RoleCapabilitySourceVersionRef,
    /// Current source state.
    pub source_state: RoleCapabilitySourceStateKind,
    /// Optional safe summary marker.
    pub safe_summary_ref: Option<RoleCapabilitySafeSummaryRef>,
    /// Evidence refs.
    pub evidence_refs: Vec<CapabilityEvidenceRef>,
    /// Resolution timestamp.
    pub resolved_at: IdentityTimestamp,
}

impl RoleCapabilitySourceSnapshot {
    /// Creates a resolved source snapshot.
    pub fn from_resolved_source(
        snapshot_ref: RoleCapabilitySourceSnapshotRef,
        source_ref: RoleCapabilitySourceRef,
        source_version_ref: RoleCapabilitySourceVersionRef,
        safe_summary_ref: RoleCapabilitySafeSummaryRef,
        evidence_refs: Vec<CapabilityEvidenceRef>,
        resolved_at: IdentityTimestamp,
    ) -> Result<Self, IdentityDomainError> {
        if !source_version_ref.belongs_to(&source_ref) {
            return Err(IdentityDomainError::invalid_input(
                "source_version_ref",
                "source version ref does not belong to source ref",
            ));
        }
        if !safe_summary_ref.belongs_to_source(&source_ref) {
            return Err(IdentityDomainError::invalid_input(
                "safe_summary_ref",
                "safe summary does not belong to source ref",
            ));
        }

        Ok(Self {
            snapshot_ref,
            source_ref,
            source_version_ref,
            source_state: RoleCapabilitySourceStateKind::SourceResolved,
            safe_summary_ref: Some(safe_summary_ref),
            evidence_refs,
            resolved_at,
        })
    }

    /// Creates an unavailable source snapshot with a formal version marker.
    pub fn unavailable(
        snapshot_ref: RoleCapabilitySourceSnapshotRef,
        source_ref: RoleCapabilitySourceRef,
        source_version_ref: RoleCapabilitySourceVersionRef,
        checked_at: IdentityTimestamp,
    ) -> Self {
        Self {
            snapshot_ref,
            source_ref,
            source_version_ref,
            source_state: RoleCapabilitySourceStateKind::SourceUnavailable,
            safe_summary_ref: None,
            evidence_refs: Vec::new(),
            resolved_at: checked_at,
        }
    }

    /// Creates an unrecognized source snapshot with a formal version marker.
    pub fn unrecognized(
        snapshot_ref: RoleCapabilitySourceSnapshotRef,
        source_ref: RoleCapabilitySourceRef,
        source_version_ref: RoleCapabilitySourceVersionRef,
        checked_at: IdentityTimestamp,
    ) -> Self {
        Self {
            snapshot_ref,
            source_ref,
            source_version_ref,
            source_state: RoleCapabilitySourceStateKind::SourceUnrecognized,
            safe_summary_ref: None,
            evidence_refs: Vec::new(),
            resolved_at: checked_at,
        }
    }

    /// Returns whether the snapshot matches the provided source.
    pub fn matches_source(&self, source_ref: RoleCapabilitySourceRef) -> bool {
        self.source_ref.same_source(&source_ref)
    }

    /// Returns whether the snapshot has evidence refs.
    pub fn has_required_evidence(&self) -> bool {
        !self.evidence_refs.is_empty()
    }

    /// Returns whether the snapshot is usable for active summary writes.
    pub fn is_usable_for_summary(&self) -> bool {
        self.source_state == RoleCapabilitySourceStateKind::SourceResolved
            && self.safe_summary_ref.is_some()
            && self.has_required_evidence()
    }

    /// Marks the snapshot stale with a new formal version marker.
    pub fn mark_stale(
        &mut self,
        new_version_ref: RoleCapabilitySourceVersionRef,
        changed_at: IdentityTimestamp,
    ) -> Result<(), IdentityDomainError> {
        if self.source_state == RoleCapabilitySourceStateKind::SourceSuperseded {
            return Err(IdentityDomainError::invalid_state_transition(
                "RoleCapabilitySourceSnapshot",
                "superseded snapshot cannot transition",
            ));
        }
        if !new_version_ref.belongs_to(&self.source_ref) {
            return Err(IdentityDomainError::invalid_input(
                "source_version_ref",
                "new version ref does not belong to source ref",
            ));
        }
        self.source_version_ref = new_version_ref;
        self.source_state = RoleCapabilitySourceStateKind::SourceStale;
        self.resolved_at = changed_at;
        Ok(())
    }

    /// Marks the snapshot unavailable while preserving the formal source version.
    pub fn mark_unavailable(
        &mut self,
        checked_at: IdentityTimestamp,
    ) -> Result<(), IdentityDomainError> {
        if self.source_state == RoleCapabilitySourceStateKind::SourceSuperseded {
            return Err(IdentityDomainError::invalid_state_transition(
                "RoleCapabilitySourceSnapshot",
                "superseded snapshot cannot transition",
            ));
        }
        self.source_state = RoleCapabilitySourceStateKind::SourceUnavailable;
        self.safe_summary_ref = None;
        self.resolved_at = checked_at;
        Ok(())
    }

    /// Marks the snapshot superseded by a newer formal version marker.
    pub fn mark_superseded(
        &mut self,
        new_version_ref: RoleCapabilitySourceVersionRef,
        changed_at: IdentityTimestamp,
    ) -> Result<(), IdentityDomainError> {
        if !new_version_ref.belongs_to(&self.source_ref) {
            return Err(IdentityDomainError::invalid_input(
                "source_version_ref",
                "new version ref does not belong to source ref",
            ));
        }
        self.source_version_ref = new_version_ref;
        self.source_state = RoleCapabilitySourceStateKind::SourceSuperseded;
        self.resolved_at = changed_at;
        Ok(())
    }

    /// Returns whether the snapshot is terminally superseded.
    pub fn is_superseded(&self) -> bool {
        self.source_state == RoleCapabilitySourceStateKind::SourceSuperseded
    }
}

/// Guard for role capability summary writes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RoleCapabilitySourcePolicy {
    /// Member that owns the summary.
    pub member_ref: GlobalMemberRef,
    /// Source snapshot for this change.
    pub source_snapshot: RoleCapabilitySourceSnapshot,
    /// Evidence refs supplied with the change.
    pub evidence_refs: Vec<CapabilityEvidenceRef>,
    /// Change reason marker.
    pub change_reason_ref: RoleCapabilityChangeReasonRef,
    /// Change actor.
    pub actor_ref: ActorRef,
    /// Current operation channel.
    pub operation_channel: IdentityOperationChannel,
    /// Change material marker.
    pub change_material_marker: RoleCapabilityChangeMaterialMarker,
}

impl RoleCapabilitySourcePolicy {
    /// Creates a policy for direct summary maintenance.
    pub fn for_summary_update(
        member_ref: GlobalMemberRef,
        source_snapshot: RoleCapabilitySourceSnapshot,
        evidence_refs: Vec<CapabilityEvidenceRef>,
        change_reason_ref: RoleCapabilityChangeReasonRef,
        actor_ref: ActorRef,
        operation_channel: IdentityOperationChannel,
        change_material_marker: RoleCapabilityChangeMaterialMarker,
    ) -> Self {
        Self {
            member_ref,
            source_snapshot,
            evidence_refs,
            change_reason_ref,
            actor_ref,
            operation_channel,
            change_material_marker,
        }
    }

    /// Creates a policy for source-driven summary change handling.
    pub fn for_source_change(
        current_summary: &RoleCapabilitySummary,
        source_snapshot: RoleCapabilitySourceSnapshot,
        operation_channel: IdentityOperationChannel,
        change_material_marker: RoleCapabilityChangeMaterialMarker,
    ) -> Self {
        Self {
            member_ref: current_summary.member_ref.clone(),
            source_snapshot,
            evidence_refs: current_summary.evidence_refs.clone(),
            change_reason_ref: RoleCapabilityChangeReasonRef::new(
                RoleCapabilityChangeReasonKind::SourceChanged,
                current_summary
                    .role_source_ref
                    .as_ref()
                    .map(|source_ref| source_ref.canonical_source().source_ref.clone())
                    .unwrap_or_else(|| {
                        current_summary
                            .safe_summary_ref
                            .source_ref
                            .source_ref
                            .clone()
                    }),
            )
            .expect("source-driven reason ref should be valid"),
            actor_ref: ActorRef::system("role-capability-source-change"),
            operation_channel,
            change_material_marker,
        }
    }

    /// Asserts that the member dependency exists in the loaded application context.
    pub fn assert_member_exists(&self, member_exists: bool) -> Result<(), IdentityDomainError> {
        if member_exists {
            return Ok(());
        }

        Err(IdentityDomainError::policy_denied(
            "RoleCapabilitySourcePolicy",
            "role capability summary requires an established member",
        ))
    }

    /// Asserts that at least source material or evidence exists.
    pub fn assert_source_or_evidence_present(&self) -> Result<(), IdentityDomainError> {
        if self.source_snapshot.safe_summary_ref.is_some() || !self.evidence_refs.is_empty() {
            return Ok(());
        }

        Err(IdentityDomainError::policy_denied(
            "RoleCapabilitySourcePolicy",
            "role capability update requires source material or evidence",
        ))
    }

    /// Asserts that the source snapshot is usable for active writes.
    pub fn assert_source_usable(&self) -> Result<(), IdentityDomainError> {
        if self.source_snapshot.is_usable_for_summary() {
            return Ok(());
        }

        Err(IdentityDomainError::policy_denied(
            "RoleCapabilitySourcePolicy",
            "only resolved source snapshots may drive active summary writes",
        ))
    }

    /// Rejects forbidden body material.
    pub fn assert_no_forbidden_body(&self) -> Result<(), IdentityDomainError> {
        if !self.change_material_marker.is_forbidden() {
            return Ok(());
        }

        Err(IdentityDomainError::policy_denied(
            "RoleCapabilitySourcePolicy",
            "forbidden source, method, or evidence body cannot enter identity truth",
        ))
    }

    /// Rejects automatic scoring or performance inference payloads.
    pub fn assert_not_automatic_scoring(&self) -> Result<(), IdentityDomainError> {
        if self.change_material_marker.material_kind
            != RoleCapabilityChangeMaterialKind::ForbiddenAutomaticScoring
        {
            return Ok(());
        }

        Err(IdentityDomainError::policy_denied(
            "RoleCapabilitySourcePolicy",
            "automatic scoring or performance inference cannot enter identity truth",
        ))
    }
}

#[cfg(test)]
mod tests {
    use core_contracts::actor::{ActorKind, ActorRef};
    use identity_contracts::refs::{
        CapabilityEvidenceKind, CapabilityEvidenceRef, ExternalSourceRef, GlobalMemberId,
        GlobalMemberRef, IdentityOperationChannel, IdentitySourceOwner, IdentitySourceRef,
        IdentityTimestamp, RoleCapabilityChangeMaterialKind, RoleCapabilityChangeMaterialMarker,
        RoleCapabilityChangeReasonKind, RoleCapabilityChangeReasonRef,
        RoleCapabilitySafeSummaryRef, RoleCapabilitySourceKind, RoleCapabilitySourceRef,
        RoleCapabilitySourceSnapshotId, RoleCapabilitySourceSnapshotRef,
        RoleCapabilitySourceVersionRef, RoleCapabilitySummaryId, RoleCapabilitySummaryRef,
    };

    use super::{
        RoleCapabilitySourcePolicy, RoleCapabilitySourceSnapshot, RoleCapabilitySourceStateKind,
        RoleCapabilitySummary, RoleCapabilitySummaryStateKind,
    };
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

    fn source_ref(token: &str) -> RoleCapabilitySourceRef {
        RoleCapabilitySourceRef::new(
            RoleCapabilitySourceKind::RoleCapabilityBundle,
            IdentitySourceRef::new(
                IdentitySourceOwner::MethodLibrary,
                ExternalSourceRef::new(token.to_owned()).expect("valid external source ref"),
            )
            .expect("valid source ref"),
        )
        .expect("valid canonical source ref")
    }

    fn source_version(
        source_ref: &RoleCapabilitySourceRef,
        token: &str,
    ) -> RoleCapabilitySourceVersionRef {
        RoleCapabilitySourceVersionRef::new(source_ref.clone(), token.to_owned())
            .expect("valid source version ref")
    }

    fn safe_summary(
        source_ref: &RoleCapabilitySourceRef,
        token: &str,
    ) -> RoleCapabilitySafeSummaryRef {
        RoleCapabilitySafeSummaryRef::new(source_ref.clone(), token.to_owned())
            .expect("valid safe summary ref")
    }

    fn evidence(source_owner: IdentitySourceOwner, token: &str) -> CapabilityEvidenceRef {
        let source_ref = IdentitySourceRef::new(
            source_owner,
            ExternalSourceRef::new(token.to_owned()).expect("valid external source ref"),
        )
        .expect("valid evidence source");

        let kind = match source_owner {
            IdentitySourceOwner::MethodLibrary => CapabilityEvidenceKind::MethodArtifact,
            IdentitySourceOwner::Governance => CapabilityEvidenceKind::GovernanceBasis,
            IdentitySourceOwner::Work => CapabilityEvidenceKind::WorkParticipationSummary,
            IdentitySourceOwner::Identity => CapabilityEvidenceKind::IdentitySafeMarker,
            _ => CapabilityEvidenceKind::IdentitySafeMarker,
        };

        CapabilityEvidenceRef::new(kind, source_ref).expect("valid capability evidence ref")
    }

    fn resolved_snapshot() -> RoleCapabilitySourceSnapshot {
        let source_ref = source_ref("method-source-1");
        RoleCapabilitySourceSnapshot::from_resolved_source(
            RoleCapabilitySourceSnapshotRef::from_id(
                RoleCapabilitySourceSnapshotId::new("snapshot-1".to_owned())
                    .expect("valid snapshot id"),
            ),
            source_ref.clone(),
            source_version(&source_ref, "v1"),
            safe_summary(&source_ref, "summary-1"),
            vec![evidence(
                IdentitySourceOwner::MethodLibrary,
                "method-evidence-1",
            )],
            timestamp(1),
        )
        .expect("resolved snapshot")
    }

    #[test]
    fn active_summary_requires_resolved_usable_snapshot() {
        let snapshot = resolved_snapshot();
        let summary = RoleCapabilitySummary::create_for_member(
            RoleCapabilitySummaryRef::from_id(
                RoleCapabilitySummaryId::new("summary-1".to_owned()).expect("valid summary id"),
            ),
            member_ref("member-1"),
            &snapshot,
            snapshot
                .safe_summary_ref
                .clone()
                .expect("resolved snapshot has safe summary"),
            snapshot.evidence_refs.clone(),
            actor(),
            timestamp(2),
        )
        .expect("active summary should be created");

        assert_eq!(
            summary.summary_state,
            RoleCapabilitySummaryStateKind::Active
        );
    }

    #[test]
    fn non_resolved_snapshot_cannot_create_active_summary() {
        let source_ref = source_ref("method-source-1");
        let snapshot = RoleCapabilitySourceSnapshot::unavailable(
            RoleCapabilitySourceSnapshotRef::from_id(
                RoleCapabilitySourceSnapshotId::new("snapshot-2".to_owned())
                    .expect("valid snapshot id"),
            ),
            source_ref.clone(),
            source_version(&source_ref, "v2"),
            timestamp(2),
        );

        let result = RoleCapabilitySummary::create_for_member(
            RoleCapabilitySummaryRef::from_id(
                RoleCapabilitySummaryId::new("summary-2".to_owned()).expect("valid summary id"),
            ),
            member_ref("member-1"),
            &snapshot,
            safe_summary(&source_ref, "summary-2"),
            vec![evidence(
                IdentitySourceOwner::MethodLibrary,
                "method-evidence-2",
            )],
            actor(),
            timestamp(3),
        );

        assert!(matches!(
            result,
            Err(IdentityDomainError::PolicyDenied { .. })
        ));
    }

    #[test]
    fn forbidden_material_is_rejected() {
        let snapshot = resolved_snapshot();
        let policy = RoleCapabilitySourcePolicy::for_summary_update(
            member_ref("member-1"),
            snapshot,
            vec![evidence(
                IdentitySourceOwner::MethodLibrary,
                "method-evidence-1",
            )],
            RoleCapabilityChangeReasonRef::new(
                RoleCapabilityChangeReasonKind::ManualSummaryMaintenance,
                IdentitySourceRef::new(
                    IdentitySourceOwner::Identity,
                    ExternalSourceRef::new("identity-source-1".to_owned())
                        .expect("valid external source ref"),
                )
                .expect("valid source ref"),
            )
            .expect("valid change reason"),
            actor(),
            IdentityOperationChannel::Command,
            RoleCapabilityChangeMaterialMarker::new(
                RoleCapabilityChangeMaterialKind::ForbiddenDefinitionBody,
                None,
            ),
        );

        assert!(matches!(
            policy.assert_no_forbidden_body(),
            Err(IdentityDomainError::PolicyDenied { .. })
        ));
    }

    #[test]
    fn unavailable_and_unrecognized_snapshots_keep_formal_version() {
        let source_ref = source_ref("method-source-1");
        let version_ref = source_version(&source_ref, "v3");
        let unavailable = RoleCapabilitySourceSnapshot::unavailable(
            RoleCapabilitySourceSnapshotRef::from_id(
                RoleCapabilitySourceSnapshotId::new("snapshot-3".to_owned())
                    .expect("valid snapshot id"),
            ),
            source_ref.clone(),
            version_ref.clone(),
            timestamp(4),
        );
        assert_eq!(unavailable.source_version_ref, version_ref);

        let unrecognized = RoleCapabilitySourceSnapshot::unrecognized(
            RoleCapabilitySourceSnapshotRef::from_id(
                RoleCapabilitySourceSnapshotId::new("snapshot-4".to_owned())
                    .expect("valid snapshot id"),
            ),
            source_ref.clone(),
            source_version(&source_ref, "v4"),
            timestamp(5),
        );
        assert_eq!(
            unrecognized.source_state,
            RoleCapabilitySourceStateKind::SourceUnrecognized
        );
    }
}
