//! Append-only career truth and guards.

use core_contracts::actor::ActorRef;
use identity_contracts::refs::{
    CareerAppendMaterialKind, CareerAppendMaterialMarker, CareerAppendReasonRef,
    CareerRecordChangeIntent, CareerRecordRef, CareerSafeSummaryRef, GlobalMemberRef,
    IdentityOperationChannel, IdentityTimestamp, ProjectParticipationRef,
    WorkParticipationSourceSummary, WorkSourceRef,
};

use crate::errors::IdentityDomainError;

/// Career record state for append-only identity history.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CareerRecordStateKind {
    /// Normal append-only record.
    Appended,
    /// Correction record appended against an existing record.
    CorrectionAppended,
    /// Existing record superseded by a correction record.
    SupersededByCorrection,
    /// Pending review record created from non-trusted source input.
    SourcePendingReview,
}

/// Append-only career record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CareerRecord {
    /// Stable record ref.
    pub career_record_ref: CareerRecordRef,
    /// Member that owns the record.
    pub member_ref: GlobalMemberRef,
    /// Work-owned participation source.
    pub project_participation_ref: ProjectParticipationRef,
    /// Work source ref.
    pub work_source_ref: WorkSourceRef,
    /// Duplicate-prevention marker.
    pub source_marker_ref: identity_contracts::refs::CareerSourceMarkerRef,
    /// Optional safe summary marker.
    pub career_summary_ref: Option<CareerSafeSummaryRef>,
    /// Append reason.
    pub append_reason_ref: CareerAppendReasonRef,
    /// Append actor.
    pub appended_by_ref: ActorRef,
    /// Append timestamp.
    pub appended_at: IdentityTimestamp,
    /// Record state.
    pub record_state: CareerRecordStateKind,
    /// Optional original record ref for corrections.
    pub correction_of_ref: Option<CareerRecordRef>,
    /// Optional correction record ref that supersedes this record.
    pub superseded_by_ref: Option<CareerRecordRef>,
}

impl CareerRecord {
    /// Appends a normal record from a trusted work source.
    pub fn append_from_work_source(
        career_record_ref: CareerRecordRef,
        member_ref: GlobalMemberRef,
        source_summary: WorkParticipationSourceSummary,
        append_reason_ref: CareerAppendReasonRef,
        actor_ref: ActorRef,
        appended_at: IdentityTimestamp,
    ) -> Result<Self, IdentityDomainError> {
        if !source_summary.is_trusted() {
            return Err(IdentityDomainError::policy_denied(
                "CareerRecord",
                "trusted work source is required for mainline append",
            ));
        }

        Ok(Self {
            career_record_ref,
            member_ref,
            project_participation_ref: source_summary.project_participation_ref,
            work_source_ref: source_summary.work_source_ref,
            source_marker_ref: source_summary.source_marker_ref,
            career_summary_ref: source_summary.safe_summary_ref,
            append_reason_ref,
            appended_by_ref: actor_ref,
            appended_at,
            record_state: CareerRecordStateKind::Appended,
            correction_of_ref: None,
            superseded_by_ref: None,
        })
    }

    /// Appends a correction record for an existing career record.
    pub fn correction_for_record(
        career_record_ref: CareerRecordRef,
        original_record_ref: CareerRecordRef,
        member_ref: GlobalMemberRef,
        source_summary: WorkParticipationSourceSummary,
        append_reason_ref: CareerAppendReasonRef,
        actor_ref: ActorRef,
        appended_at: IdentityTimestamp,
    ) -> Result<Self, IdentityDomainError> {
        if !source_summary.is_trusted() {
            return Err(IdentityDomainError::policy_denied(
                "CareerRecord",
                "trusted work source is required for correction append",
            ));
        }

        Ok(Self {
            career_record_ref,
            member_ref,
            project_participation_ref: source_summary.project_participation_ref,
            work_source_ref: source_summary.work_source_ref,
            source_marker_ref: source_summary.source_marker_ref,
            career_summary_ref: source_summary.safe_summary_ref,
            append_reason_ref,
            appended_by_ref: actor_ref,
            appended_at,
            record_state: CareerRecordStateKind::CorrectionAppended,
            correction_of_ref: Some(original_record_ref),
            superseded_by_ref: None,
        })
    }

    /// Appends a pending-review record from formal non-trusted source input.
    pub fn pending_review(
        career_record_ref: CareerRecordRef,
        member_ref: GlobalMemberRef,
        source_summary: WorkParticipationSourceSummary,
        append_reason_ref: CareerAppendReasonRef,
        actor_ref: ActorRef,
        appended_at: IdentityTimestamp,
    ) -> Result<Self, IdentityDomainError> {
        if !source_summary.requires_review() {
            return Err(IdentityDomainError::policy_denied(
                "CareerRecord",
                "pending-review record requires a reviewable source state",
            ));
        }

        Ok(Self {
            career_record_ref,
            member_ref,
            project_participation_ref: source_summary.project_participation_ref,
            work_source_ref: source_summary.work_source_ref,
            source_marker_ref: source_summary.source_marker_ref,
            career_summary_ref: source_summary.safe_summary_ref,
            append_reason_ref,
            appended_by_ref: actor_ref,
            appended_at,
            record_state: CareerRecordStateKind::SourcePendingReview,
            correction_of_ref: None,
            superseded_by_ref: None,
        })
    }

    /// Returns whether the record was created from the given source marker.
    pub fn matches_source_marker(
        &self,
        source_marker_ref: &identity_contracts::refs::CareerSourceMarkerRef,
    ) -> bool {
        self.source_marker_ref.same_marker(source_marker_ref)
    }

    /// Returns whether the record follows append-only rules.
    pub fn is_append_only(&self) -> bool {
        true
    }

    /// Returns whether the record is a correction record.
    pub fn is_correction(&self) -> bool {
        self.record_state == CareerRecordStateKind::CorrectionAppended
    }

    /// Returns whether the record requires source review.
    pub fn requires_source_review(&self) -> bool {
        self.record_state == CareerRecordStateKind::SourcePendingReview
    }

    /// Marks an appended record as superseded by a correction record.
    pub fn mark_superseded_by_correction(
        &mut self,
        correction_record_ref: CareerRecordRef,
        _actor_ref: ActorRef,
        _changed_at: IdentityTimestamp,
    ) -> Result<(), IdentityDomainError> {
        if self.record_state != CareerRecordStateKind::Appended {
            return Err(IdentityDomainError::invalid_state_transition(
                "CareerRecord",
                "only appended records may be superseded by correction",
            ));
        }

        self.record_state = CareerRecordStateKind::SupersededByCorrection;
        self.superseded_by_ref = Some(correction_record_ref);
        Ok(())
    }
}

/// Guard for append-only career writes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CareerAppendPolicy {
    /// Member being appended against.
    pub member_ref: GlobalMemberRef,
    /// Whether the member exists in loaded identity truth.
    pub member_exists: bool,
    /// Body-free work source summary.
    pub source_summary: WorkParticipationSourceSummary,
    /// Existing records for the same source marker.
    pub existing_records_for_source: Vec<CareerRecordRef>,
    /// Append reason.
    pub append_reason_ref: CareerAppendReasonRef,
    /// Append actor.
    pub actor_ref: ActorRef,
    /// Operation channel.
    pub operation_channel: IdentityOperationChannel,
    /// Requested change intent.
    pub change_intent: CareerRecordChangeIntent,
    /// Body-free material marker.
    pub append_material_marker: CareerAppendMaterialMarker,
}

impl CareerAppendPolicy {
    /// Creates a policy for append-like requests.
    pub fn for_append(
        member_ref: GlobalMemberRef,
        member_exists: bool,
        source_summary: WorkParticipationSourceSummary,
        existing_records_for_source: Vec<CareerRecordRef>,
        append_reason_ref: CareerAppendReasonRef,
        actor_ref: ActorRef,
        operation_channel: IdentityOperationChannel,
        change_intent: CareerRecordChangeIntent,
        append_material_marker: CareerAppendMaterialMarker,
    ) -> Self {
        Self {
            member_ref,
            member_exists,
            source_summary,
            existing_records_for_source,
            append_reason_ref,
            actor_ref,
            operation_channel,
            change_intent,
            append_material_marker,
        }
    }

    /// Creates a policy for correction append requests.
    pub fn for_correction(
        member_ref: GlobalMemberRef,
        member_exists: bool,
        source_summary: WorkParticipationSourceSummary,
        existing_records_for_source: Vec<CareerRecordRef>,
        append_reason_ref: CareerAppendReasonRef,
        actor_ref: ActorRef,
        operation_channel: IdentityOperationChannel,
        append_material_marker: CareerAppendMaterialMarker,
    ) -> Self {
        Self::for_append(
            member_ref,
            member_exists,
            source_summary,
            existing_records_for_source,
            append_reason_ref,
            actor_ref,
            operation_channel,
            CareerRecordChangeIntent::AppendCorrection,
            append_material_marker,
        )
    }

    /// Asserts that the member exists.
    pub fn assert_member_exists(&self) -> Result<(), IdentityDomainError> {
        if self.member_exists {
            return Ok(());
        }

        Err(IdentityDomainError::policy_denied(
            "CareerAppendPolicy",
            "career append requires an established member",
        ))
    }

    /// Asserts that the source is trusted for mainline writes.
    pub fn assert_source_trusted(&self) -> Result<(), IdentityDomainError> {
        if self.source_summary.is_trusted() {
            return Ok(());
        }

        Err(IdentityDomainError::policy_denied(
            "CareerAppendPolicy",
            "trusted work source is required for accepted mainline append",
        ))
    }

    /// Asserts that no duplicate source marker exists.
    pub fn assert_not_duplicate(&self) -> Result<(), IdentityDomainError> {
        if self.existing_records_for_source.is_empty() {
            return Ok(());
        }

        Err(IdentityDomainError::policy_denied(
            "CareerAppendPolicy",
            "duplicate work source marker must not create a second career record",
        ))
    }

    /// Asserts append-only change intent.
    pub fn assert_append_only(&self) -> Result<(), IdentityDomainError> {
        match self.change_intent {
            CareerRecordChangeIntent::AppendNew
            | CareerRecordChangeIntent::AppendCorrection
            | CareerRecordChangeIntent::MarkSourcePendingReview => Ok(()),
            _ => Err(IdentityDomainError::policy_denied(
                "CareerAppendPolicy",
                "career history is append-only",
            )),
        }
    }

    /// Rejects forbidden work truth body material.
    pub fn assert_not_work_truth_write(&self) -> Result<(), IdentityDomainError> {
        match self.append_material_marker.material_kind {
            CareerAppendMaterialKind::SafeSummaryMarker
            | CareerAppendMaterialKind::SourceMarkerOnly
            | CareerAppendMaterialKind::CorrectionMarkerOnly => Ok(()),
            _ => Err(IdentityDomainError::policy_denied(
                "CareerAppendPolicy",
                "work truth body cannot enter identity career truth",
            )),
        }
    }

    /// Restricts core truth writes to command or consumer channels.
    pub fn assert_allowed_write_channel(&self) -> Result<(), IdentityDomainError> {
        match self.operation_channel {
            IdentityOperationChannel::Command | IdentityOperationChannel::Consumer => Ok(()),
            _ => Err(IdentityDomainError::write_channel_denied(
                "CareerAppendPolicy",
                self.operation_channel,
                "only command or consumer channels may append career truth",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use core_contracts::actor::{ActorKind, ActorRef};
    use identity_contracts::refs::{
        CareerAppendMaterialKind, CareerAppendMaterialMarker, CareerAppendReasonKind,
        CareerAppendReasonRef, CareerRecordChangeIntent, CareerRecordId, CareerRecordRef,
        CareerSafeSummaryRef, ExternalSourceRef, GlobalMemberId, GlobalMemberRef,
        IdentityOperationChannel, IdentitySourceOwner, IdentitySourceRef, IdentityTimestamp,
        ProjectParticipationRef, WorkParticipationSourceState, WorkParticipationSourceSummary,
        WorkSourceKind, WorkSourceRef,
    };

    use super::{CareerAppendPolicy, CareerRecord, CareerRecordStateKind};
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

    fn source(owner: IdentitySourceOwner, token: &str) -> IdentitySourceRef {
        IdentitySourceRef::new(
            owner,
            ExternalSourceRef::new(token.to_owned()).expect("valid external source ref"),
        )
        .expect("valid source ref")
    }

    fn work_source(token: &str, kind: WorkSourceKind) -> WorkSourceRef {
        WorkSourceRef::new(kind, source(IdentitySourceOwner::Work, token))
            .expect("valid work source")
    }

    fn source_summary(state: WorkParticipationSourceState) -> WorkParticipationSourceSummary {
        let member_ref = member_ref("member-1");
        let work_source_ref = work_source(
            "work-source-1",
            WorkSourceKind::ProjectParticipationAccepted,
        );
        WorkParticipationSourceSummary::from_resolver(
            ProjectParticipationRef::from_work_source(source(
                IdentitySourceOwner::Work,
                "project-participation-1",
            ))
            .expect("valid project participation ref"),
            work_source_ref.clone(),
            identity_contracts::refs::CareerSourceMarkerRef::new(
                member_ref.clone(),
                work_source_ref.clone(),
                "marker-1",
            )
            .expect("valid source marker"),
            if state == WorkParticipationSourceState::Trusted {
                Some(
                    CareerSafeSummaryRef::new(work_source_ref, "safe-summary-1")
                        .expect("valid safe summary"),
                )
            } else {
                None
            },
            state,
        )
    }

    fn append_reason(kind: CareerAppendReasonKind) -> CareerAppendReasonRef {
        CareerAppendReasonRef::new(
            kind,
            source(IdentitySourceOwner::Identity, "identity-source-1"),
        )
        .expect("valid append reason")
    }

    fn record_ref(id: &str) -> CareerRecordRef {
        CareerRecordRef::from_id(CareerRecordId::new(id.to_owned()).expect("valid record id"))
    }

    #[test]
    fn duplicate_source_is_rejected() {
        let policy = CareerAppendPolicy::for_append(
            member_ref("member-1"),
            true,
            source_summary(WorkParticipationSourceState::Trusted),
            vec![record_ref("record-1")],
            append_reason(CareerAppendReasonKind::ManualAppend),
            actor(),
            IdentityOperationChannel::Command,
            CareerRecordChangeIntent::AppendNew,
            CareerAppendMaterialMarker {
                material_kind: CareerAppendMaterialKind::SafeSummaryMarker,
                source_ref: None,
            },
        );

        assert!(matches!(
            policy.assert_not_duplicate(),
            Err(IdentityDomainError::PolicyDenied { .. })
        ));
    }

    #[test]
    fn correction_supersedes_original_record() {
        let trusted_summary = source_summary(WorkParticipationSourceState::Trusted);
        let correction = CareerRecord::correction_for_record(
            record_ref("record-2"),
            record_ref("record-1"),
            member_ref("member-1"),
            trusted_summary.clone(),
            append_reason(CareerAppendReasonKind::CorrectionAppend),
            actor(),
            timestamp(2),
        )
        .expect("correction record created");

        let mut original = CareerRecord::append_from_work_source(
            record_ref("record-1"),
            member_ref("member-1"),
            trusted_summary,
            append_reason(CareerAppendReasonKind::ManualAppend),
            actor(),
            timestamp(1),
        )
        .expect("original append created");

        original
            .mark_superseded_by_correction(
                correction.career_record_ref.clone(),
                actor(),
                timestamp(3),
            )
            .expect("original should be superseded by correction");

        assert_eq!(
            original.record_state,
            CareerRecordStateKind::SupersededByCorrection
        );
    }

    #[test]
    fn pending_review_requires_explicit_review_path() {
        let pending_summary = source_summary(WorkParticipationSourceState::PendingReview);
        let record = CareerRecord::pending_review(
            record_ref("record-3"),
            member_ref("member-1"),
            pending_summary,
            append_reason(CareerAppendReasonKind::SourcePendingReview),
            actor(),
            timestamp(4),
        )
        .expect("pending review record created");
        assert_eq!(
            record.record_state,
            CareerRecordStateKind::SourcePendingReview
        );

        let pending_append_policy = CareerAppendPolicy::for_append(
            member_ref("member-1"),
            true,
            source_summary(WorkParticipationSourceState::PendingReview),
            Vec::new(),
            append_reason(CareerAppendReasonKind::ManualAppend),
            actor(),
            IdentityOperationChannel::Command,
            CareerRecordChangeIntent::AppendNew,
            CareerAppendMaterialMarker {
                material_kind: CareerAppendMaterialKind::SourceMarkerOnly,
                source_ref: None,
            },
        );
        assert!(matches!(
            pending_append_policy.assert_source_trusted(),
            Err(IdentityDomainError::PolicyDenied { .. })
        ));
    }
}
