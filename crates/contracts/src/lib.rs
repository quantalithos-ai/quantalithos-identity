//! Public contracts for the identity workspace.
//!
//! This crate holds the shared public protocol shell and typed markers.

pub mod commands;
pub mod errors;
pub mod events;
pub mod jobs;
pub mod metadata;
pub mod protocol;
pub mod queries;
pub mod receipts;
pub mod refs;
pub mod views;

#[cfg(test)]
mod tests {
    use core_contracts::actor::{ActorKind, ActorRef};
    use serde::Serialize;
    use serde::de::DeserializeOwned;

    use crate::commands::{
        AppendCareerRecordRequest, CareerRecordCommandResult, EstablishGlobalMemberRequest,
        GlobalLifecycleCommandResult, GlobalMemberCommandResult,
        IdentityCommandEffectPublicSummary, IdentityCommandOutcome, IdentityCommandRequest,
        IdentityCommandResponse, MaintainMemoryReferenceRequest,
        MaintainRoleCapabilitySummaryRequest, MemoryReferenceCommandResult,
        PrepareTraceHandoffRequest, RoleCapabilityCommandResult, TraceHandoffCommandResult,
        UpdateGlobalLifecycleStateRequest,
    };
    use crate::events::{
        ArchiveHandoffResultPayload, GlobalLifecycleChangedPayload,
        GlobalMemberAvailabilityChangedPayload, GlobalMemberEstablishedPayload,
        IdentityAnchorChangedPayload, IdentityConsumerOutcome, IdentityConsumerReceipt,
        IdentityInboundEventEnvelope, IdentityOutboundEventEnvelope, IdentityOutboundEventRef,
        MemoryReferenceSourceStateChangedPayload, RoleCapabilitySourceChangedPayload,
        TraceHandoffResultKind, TraceHandoffResultPayload, WorkParticipationAcceptedPayload,
    };
    use crate::jobs::{
        IdentityJobReportSurface, IdentityJobRequest, IdentityJobResponse, IdentityJobResultKind,
    };
    use crate::metadata::{
        IdentityCommandMetadata, IdentityDegradedKind, IdentityDegradedMarker,
        IdentityProtocolRejection, IdentityProtocolRejectionKind,
        IdentityProtocolValidationIssueRef, IdentityProtocolValidationIssueRefSet,
        IdentityQueryDisposition, IdentityQueryMetadata, IdentityQuerySurface,
        IdentityRequestDigestMarker, IdentityVisibilityMarker,
    };
    use crate::protocol::{
        IdentityCommandName, IdentityDigestAlgorithmMarkerRef, IdentityInboundConsumerName,
        IdentityJobName, IdentityOutboundEventName, IdentityProtocolSchemaVersionRef,
        IdentityProtocolSurfaceRef, IdentityQueryName,
    };
    use crate::queries::{
        GetGlobalLifecycleSummaryRequest, GetGlobalMemberAnchorRequest,
        GetIdentityOutboxStateRequest, GetProjectionStateRequest,
        GetReferenceResolutionStateRequest, GetRoleCapabilitySummaryRequest,
        GetTraceHandoffStateRequest, IdentityOutboxListSelector, IdentityPageResponse,
        IdentityPublicPageCursor, IdentityPublicPageInfo, IdentityPublicPageRequest,
        IdentityQueryRequest, IdentityQueryResponse, IdentityTraceReadSelector,
        ListCareerRecordsRequest, ListMemoryReferencesRequest, ListPendingIdentityOutboxRequest,
        ReadAuditTrailRequest, ReadIdentityTraceRequest, ReadMemberSummaryRequest,
        ReadReconciliationReportRequest,
    };
    use crate::receipts::{MaintenanceIssueRef, TraceHandoffIntentRef};
    use crate::refs::{
        ArchiveHandoffRef, ArchiveRef, AuditCursorRef, AuditScopeRef, AuditTrailRef,
        CapabilityEvidenceKind, CapabilityEvidenceRef, CapabilitySourceRef,
        CareerAppendMaterialKind, CareerAppendMaterialMarker, CareerAppendReasonKind,
        CareerAppendReasonRef, CareerRecordChangeIntent, CareerRecordId, CareerRecordRef,
        CareerRecordStateKind, CareerSafeSummaryRef, CareerSourceMarkerRef, ConsumerRef,
        ExternalReferenceKind, ExternalReferenceRef, ExternalReferenceSafeSummaryRef,
        ExternalSourceRef, ExternalSourceVersionRef, GlobalLifecycleStateKind, GlobalMemberId,
        GlobalMemberRef, GovernanceBasisKind, GovernanceBasisRef, HandoffAttemptRef,
        HandoffIssueRef, HandoffReasonRef, HandoffReceiptRef, HandoffScopeRef, HandoffStateKind,
        HandoffTargetRef, IdentityAnchorReasonKind, IdentityAnchorReasonRef,
        IdentityApiRequestMarkerRef, IdentityAuditSubjectRef, IdentityCanonicalRequestMarkerRef,
        IdentityChangeKind, IdentityChangeKindRef, IdentityChangeReasonRef,
        IdentityConsumerBindingRef, IdentityConsumerReceiptRef, IdentityDegradedMarkerRef,
        IdentityJobCursorRef, IdentityJobReportRef, IdentityJobRunMetadataRef, IdentityJobRunRef,
        IdentityJobScopeMarkerRef, IdentityMaintenanceTargetRef, IdentityOutboxPayloadMarkerRef,
        IdentityOutboxRecordRef, IdentityOutboxSubjectRef, IdentityProjectionCursorRef,
        IdentityProjectionRef, IdentityReadSubjectRef, IdentityReadSurfaceKind,
        IdentityRedactionMarkerRef, IdentityReferenceOwnerKind, IdentityReferenceOwnerRef,
        IdentityRequestDigestValue, IdentitySourceEventRef, IdentitySourceOwner,
        IdentityStoredResultRef, IdentityTimestamp, IdentityTraceContextRef,
        IdentityTraceRecordRef, IdentityTraceSubjectRef, IdentityTruthCursor, LifecycleReasonKind,
        LifecycleReasonRef, MaintenanceScopeRef, MemberSummaryViewRef, MemoryRef,
        MemoryReferenceChangeIntent, MemoryReferenceChangeMaterialKind,
        MemoryReferenceChangeMaterialMarker, MemoryReferenceId, MemoryReferenceReasonKind,
        MemoryReferenceReasonRef, MemoryReferenceRef, MemoryReferenceSourceKind,
        MemoryReferenceSourceRef, MemoryReferenceStateKind, MemorySafeSummaryRef,
        OutboxDeliveryAttemptRef, OutboxDeliveryIssueRef, OutboxStateKind, ProjectParticipationRef,
        ProjectionFreshnessMarkerRef, ProjectionStateId, ProjectionStateKind, ProjectionStateRef,
        ReconciliationFindingRef, ReconciliationReportRef, ReconciliationReportStateKind,
        ReferenceResolutionStateId, ReferenceResolutionStateKind, ReferenceResolutionStateRef,
        RoleCapabilityChangeMaterialKind, RoleCapabilityChangeMaterialMarker,
        RoleCapabilityChangeReasonKind, RoleCapabilityChangeReasonRef,
        RoleCapabilitySafeSummaryRef, RoleCapabilitySourceKind, RoleCapabilitySourceRef,
        RoleCapabilitySourceSnapshotId, RoleCapabilitySourceSnapshotRef,
        RoleCapabilitySourceStateKind, RoleCapabilitySourceVersionRef, RoleCapabilitySummaryId,
        RoleCapabilitySummaryRef, RoleCapabilitySummaryStateKind, RoleSourceRef, TopicKeyRef,
        TraceHandoffSafeMaterialRef, VisibilityContextRef, VisibilityResultRef, VisibilityScopeRef,
        WorkSourceKind, WorkSourceRef,
    };
    use crate::views::{
        AuditTrailEntryView, CareerRecordView, GlobalLifecycleSummaryView, GlobalMemberAnchorView,
        IdentityOutboxRecordView, IdentityOutboxStateView, IdentityReadMaterialKind,
        IdentityReadMaterialMarker, IdentityTraceRecordView, IdentityVisibilityAccessState,
        IdentityVisibilityAccessSummary, MemberSummarySliceKind, MemberSummarySliceRef,
        MemberSummaryView, MemoryReferenceView, ProjectionStateView, ReconciliationReportView,
        ReferenceResolutionSidecarRefsView, ReferenceResolutionStateView,
        RoleCapabilitySummaryView, TraceHandoffStateView,
    };

    fn roundtrip<T>(value: &T)
    where
        T: Clone + DeserializeOwned + Eq + Serialize + std::fmt::Debug,
    {
        let encoded = serde_json::to_value(value).expect("value should serialize");
        let decoded: T =
            serde_json::from_value(encoded).expect("value should deserialize after roundtrip");
        assert_eq!(decoded, *value);
    }

    fn sample_actor_ref() -> ActorRef {
        ActorRef::new("actor-1", ActorKind::Human)
    }

    fn sample_member_ref() -> GlobalMemberRef {
        GlobalMemberRef::from_id(
            GlobalMemberId::new("member-1".to_owned()).expect("sample member id should be valid"),
        )
    }

    fn sample_command_metadata() -> IdentityCommandMetadata {
        IdentityCommandMetadata {
            idempotency_key: "idem-1".into(),
            request_marker_ref: IdentityApiRequestMarkerRef::new("api-request-1"),
            schema_version_ref: IdentityProtocolSchemaVersionRef::new("identity.command.v1"),
            trace_context_ref: Some(IdentityTraceContextRef::new("trace-context-1")),
        }
    }

    fn sample_identity_source_ref() -> crate::refs::IdentitySourceRef {
        crate::refs::IdentitySourceRef::new(
            crate::refs::IdentitySourceOwner::Identity,
            crate::refs::ExternalSourceRef::new("source-1".to_owned())
                .expect("sample external source ref should be valid"),
        )
        .expect("sample source ref should be valid")
    }

    fn sample_method_source_ref(token: &str) -> crate::refs::IdentitySourceRef {
        crate::refs::IdentitySourceRef::new(
            IdentitySourceOwner::MethodLibrary,
            ExternalSourceRef::new(token.to_owned()).expect("sample external source ref"),
        )
        .expect("sample method source ref")
    }

    fn sample_work_source_ref(token: &str) -> crate::refs::IdentitySourceRef {
        crate::refs::IdentitySourceRef::new(
            IdentitySourceOwner::Work,
            ExternalSourceRef::new(token.to_owned()).expect("sample external source ref"),
        )
        .expect("sample work source ref")
    }

    fn sample_memory_source_ref(token: &str) -> crate::refs::IdentitySourceRef {
        crate::refs::IdentitySourceRef::new(
            IdentitySourceOwner::MemoryArchive,
            ExternalSourceRef::new(token.to_owned()).expect("sample external source ref"),
        )
        .expect("sample memory source ref")
    }

    fn sample_lifecycle_reason_ref() -> LifecycleReasonRef {
        LifecycleReasonRef::new(
            LifecycleReasonKind::InitialProvisioned,
            sample_identity_source_ref(),
        )
        .expect("sample lifecycle reason should be valid")
    }

    fn sample_consumer_ref() -> ConsumerRef {
        ConsumerRef::new("consumer-1")
    }

    fn sample_visibility_scope_ref() -> VisibilityScopeRef {
        VisibilityScopeRef::new("scope-1")
    }

    fn sample_role_source_ref() -> RoleCapabilitySourceRef {
        RoleCapabilitySourceRef::new(
            RoleCapabilitySourceKind::RoleCapabilityBundle,
            sample_method_source_ref("method-source-1"),
        )
        .expect("sample role source ref")
    }

    fn sample_work_source_ref_typed() -> WorkSourceRef {
        WorkSourceRef::new(
            WorkSourceKind::ProjectParticipationAccepted,
            sample_work_source_ref("work-source-1"),
        )
        .expect("sample work source")
    }

    fn sample_memory_reference_source_ref() -> MemoryReferenceSourceRef {
        MemoryReferenceSourceRef::new(
            MemoryReferenceSourceKind::MemorySourceEvent,
            sample_memory_source_ref("memory-source-1"),
        )
        .expect("sample memory source ref")
    }

    fn sample_external_reference() -> ExternalReferenceRef {
        ExternalReferenceRef::new(
            ExternalReferenceKind::MethodSource,
            sample_method_source_ref("external-reference-1"),
        )
    }

    fn sample_reference_owner() -> IdentityReferenceOwnerRef {
        IdentityReferenceOwnerRef::new(
            IdentityReferenceOwnerKind::RoleCapability,
            sample_identity_source_ref(),
        )
    }

    fn sample_member_summary_slice_ref(kind: MemberSummarySliceKind) -> MemberSummarySliceRef {
        MemberSummarySliceRef::new(kind, sample_member_ref(), sample_identity_source_ref())
    }

    fn sample_member_summary_view() -> MemberSummaryView {
        MemberSummaryView::from_projection(
            MemberSummaryViewRef::new("view-1"),
            sample_member_ref(),
            sample_visibility_scope_ref(),
            sample_member_summary_slice_ref(MemberSummarySliceKind::Anchor),
            sample_member_summary_slice_ref(MemberSummarySliceKind::Lifecycle),
            vec![sample_member_summary_slice_ref(
                MemberSummarySliceKind::RoleCapability,
            )],
            vec![sample_member_summary_slice_ref(
                MemberSummarySliceKind::Career,
            )],
            vec![sample_member_summary_slice_ref(
                MemberSummarySliceKind::MemoryReference,
            )],
            VisibilityResultRef::new("visibility-result-1"),
            IdentityReadSurfaceKind::Found,
            Some(IdentityTruthCursor::new("truth-cursor-1")),
            Some(ProjectionFreshnessMarkerRef {
                projection_ref: IdentityProjectionRef::new("projection-1"),
                state_kind: "stale".into(),
            }),
            IdentityReadMaterialMarker::new(IdentityReadMaterialKind::SafeSummaryRefs, None),
        )
        .expect("sample member summary view")
    }

    fn sample_query_surface() -> IdentityQuerySurface {
        IdentityQuerySurface {
            disposition: IdentityQueryDisposition::Degraded,
            visibility: IdentityVisibilityMarker {
                visibility_result_ref: VisibilityResultRef::new("visibility-result-1"),
                read_surface_kind: IdentityReadSurfaceKind::Found,
                redaction_marker_ref: Some(IdentityRedactionMarkerRef::new("redaction-1")),
            },
            degraded: Some(IdentityDegradedMarker {
                degraded_marker_ref: IdentityDegradedMarkerRef::new("degraded-1"),
                degraded_kind: IdentityDegradedKind::ProjectionStale,
            }),
            projection_freshness_ref: Some(ProjectionFreshnessMarkerRef {
                projection_ref: IdentityProjectionRef::new("projection-1"),
                state_kind: "stale".into(),
            }),
            decision_ref: Some("visibility-decision-1".into()),
        }
    }

    #[test]
    fn command_shell_roundtrips() {
        let request = IdentityCommandRequest {
            actor_ref: sample_actor_ref(),
            command_name: IdentityCommandName::new("EstablishGlobalMember"),
            metadata: sample_command_metadata(),
            digest: IdentityRequestDigestMarker {
                canonical_marker_ref: IdentityCanonicalRequestMarkerRef::new("canonical-1"),
                digest_value: IdentityRequestDigestValue::new("digest-1"),
                schema_version_ref: IdentityProtocolSchemaVersionRef::new("identity.command.v1"),
                algorithm_marker_ref: IdentityDigestAlgorithmMarkerRef::new("sha256-v1"),
            },
            body: "body".to_owned(),
        };

        let response = IdentityCommandResponse {
            command_name: IdentityCommandName::new("EstablishGlobalMember"),
            result_ref: IdentityStoredResultRef::new("stored-result-1"),
            result: "accepted".to_owned(),
            effect: IdentityCommandEffectPublicSummary {
                accepted_cursor_ref: IdentityTruthCursor::new("truth-cursor-1"),
                trace_refs: vec![IdentityTraceRecordRef::new("trace-1")],
                audit_subject_refs: vec![IdentityAuditSubjectRef::new("audit-subject-1")],
                outbox_refs: vec![IdentityOutboxRecordRef::new("outbox-1")],
                stale_projection_refs: vec![IdentityProjectionRef::new("projection-1")],
            },
        };

        roundtrip(&request);
        roundtrip(&response);
        roundtrip(&IdentityCommandOutcome::Accepted(response));
    }

    #[test]
    fn command_member_lifecycle_dtos_roundtrip() {
        let establish_request = EstablishGlobalMemberRequest {
            requested_member_ref: Some(sample_member_ref()),
            source_ref: sample_identity_source_ref(),
            anchor_reason_ref: None,
            initial_lifecycle_reason_ref: sample_lifecycle_reason_ref(),
        };
        let establish_result = GlobalMemberCommandResult {
            member_ref: sample_member_ref(),
            anchor_state_kind: crate::refs::IdentityAnchorStateKind::Established,
            lifecycle_state_kind: crate::refs::GlobalLifecycleStateKind::Available,
            source_ref: sample_identity_source_ref(),
        };
        let lifecycle_request = UpdateGlobalLifecycleStateRequest {
            member_ref: sample_member_ref(),
            target_state: crate::refs::GlobalLifecycleStateKind::Paused,
            reason_ref: LifecycleReasonRef::new(
                LifecycleReasonKind::ManualPause,
                sample_identity_source_ref(),
            )
            .expect("pause reason should be valid"),
            basis_ref: None,
            action_risk_ref: None,
        };
        let lifecycle_result = GlobalLifecycleCommandResult {
            member_ref: sample_member_ref(),
            lifecycle_state_kind: crate::refs::GlobalLifecycleStateKind::Paused,
            reason_ref: LifecycleReasonRef::new(
                LifecycleReasonKind::ManualPause,
                sample_identity_source_ref(),
            )
            .expect("pause reason should be valid"),
            basis_ref: None,
            anchor_state_kind: None,
        };

        roundtrip(&establish_request);
        roundtrip(&establish_result);
        roundtrip(&lifecycle_request);
        roundtrip(&lifecycle_result);
    }

    #[test]
    fn command_role_career_memory_dtos_roundtrip() {
        let role_source = RoleCapabilitySourceRef::new(
            RoleCapabilitySourceKind::RoleCapabilityBundle,
            sample_method_source_ref("method-source-1"),
        )
        .expect("role source");
        let capability_source =
            CapabilitySourceRef::from_source(role_source.clone()).expect("capability source");
        let role_summary_ref = RoleCapabilitySummaryRef::from_id(
            RoleCapabilitySummaryId::new("summary-1".to_owned()).expect("summary id"),
        );
        let snapshot_ref = RoleCapabilitySourceSnapshotRef::from_id(
            RoleCapabilitySourceSnapshotId::new("snapshot-1".to_owned()).expect("snapshot id"),
        );
        let role_request = MaintainRoleCapabilitySummaryRequest {
            member_ref: sample_member_ref(),
            requested_summary_ref: Some(role_summary_ref.clone()),
            source_ref: role_source.clone(),
            role_source_ref: Some(
                RoleSourceRef::from_source(role_source.clone()).expect("role wrapper"),
            ),
            capability_source_refs: vec![capability_source.clone()],
            evidence_refs: vec![
                CapabilityEvidenceRef::new(
                    CapabilityEvidenceKind::MethodArtifact,
                    sample_method_source_ref("method-evidence-1"),
                )
                .expect("evidence ref"),
            ],
            safe_summary_ref: Some(
                RoleCapabilitySafeSummaryRef::new(role_source.clone(), "safe-role-summary-1")
                    .expect("role safe summary"),
            ),
            change_reason_ref: RoleCapabilityChangeReasonRef::new(
                RoleCapabilityChangeReasonKind::ManualSummaryMaintenance,
                sample_identity_source_ref(),
            )
            .expect("role change reason"),
            change_material_marker: RoleCapabilityChangeMaterialMarker::new(
                RoleCapabilityChangeMaterialKind::SafeSummaryMarker,
                None,
            ),
        };
        let role_result = RoleCapabilityCommandResult {
            member_ref: sample_member_ref(),
            summary_ref: role_summary_ref,
            source_snapshot_ref: snapshot_ref,
            summary_state_kind: RoleCapabilitySummaryStateKind::Active,
            source_state_kind: RoleCapabilitySourceStateKind::SourceResolved,
            role_source_ref: Some(
                RoleSourceRef::from_source(role_source.clone()).expect("role wrapper"),
            ),
            capability_source_refs: vec![capability_source],
            evidence_refs: vec![
                CapabilityEvidenceRef::new(
                    CapabilityEvidenceKind::MethodArtifact,
                    sample_method_source_ref("method-evidence-1"),
                )
                .expect("evidence ref"),
            ],
            safe_summary_ref: Some(
                RoleCapabilitySafeSummaryRef::new(role_source, "safe-role-summary-1")
                    .expect("role safe summary"),
            ),
        };

        let work_source = WorkSourceRef::new(
            WorkSourceKind::ProjectParticipationAccepted,
            sample_work_source_ref("work-source-1"),
        )
        .expect("work source");
        let project_participation_ref = ProjectParticipationRef::from_work_source(
            sample_work_source_ref("project-participation-1"),
        )
        .expect("project participation");
        let career_record_ref = CareerRecordRef::from_id(
            CareerRecordId::new("career-record-1".to_owned()).expect("career record id"),
        );
        let career_request = AppendCareerRecordRequest {
            member_ref: sample_member_ref(),
            requested_career_record_ref: Some(career_record_ref.clone()),
            change_intent: CareerRecordChangeIntent::AppendNew,
            project_participation_ref: project_participation_ref.clone(),
            work_source_ref: work_source.clone(),
            source_marker_ref: crate::refs::CareerSourceMarkerRef::new(
                sample_member_ref(),
                work_source.clone(),
                "career-marker-1",
            )
            .expect("career source marker"),
            career_summary_ref: Some(
                CareerSafeSummaryRef::new(work_source.clone(), "career-safe-summary-1")
                    .expect("career safe summary"),
            ),
            append_reason_ref: CareerAppendReasonRef::new(
                CareerAppendReasonKind::ManualAppend,
                sample_identity_source_ref(),
            )
            .expect("career append reason"),
            original_record_ref: None,
            append_material_marker: CareerAppendMaterialMarker {
                material_kind: CareerAppendMaterialKind::SafeSummaryMarker,
                source_ref: None,
            },
        };
        let career_result = CareerRecordCommandResult {
            member_ref: sample_member_ref(),
            career_record_ref: career_record_ref.clone(),
            record_state_kind: CareerRecordStateKind::Appended,
            project_participation_ref,
            work_source_ref: work_source.clone(),
            source_marker_ref: crate::refs::CareerSourceMarkerRef::new(
                sample_member_ref(),
                work_source.clone(),
                "career-marker-1",
            )
            .expect("career source marker"),
            career_summary_ref: Some(
                CareerSafeSummaryRef::new(work_source, "career-safe-summary-1")
                    .expect("career safe summary"),
            ),
            correction_of_ref: None,
            superseded_record_ref: None,
        };

        let memory_source = MemoryReferenceSourceRef::new(
            MemoryReferenceSourceKind::ManualCommand,
            sample_identity_source_ref(),
        )
        .expect("memory source");
        let memory_ref =
            MemoryRef::from_source(sample_memory_source_ref("memory-1")).expect("memory ref");
        let archive_ref =
            ArchiveRef::from_source(sample_memory_source_ref("archive-1")).expect("archive ref");
        let archive_handoff_ref =
            ArchiveHandoffRef::new(sample_identity_source_ref(), "handoff-1").expect("handoff ref");
        let memory_reference_ref = MemoryReferenceRef::from_id(
            MemoryReferenceId::new("memory-reference-1".to_owned()).expect("memory ref id"),
        );
        let memory_request = MaintainMemoryReferenceRequest {
            member_ref: sample_member_ref(),
            requested_memory_reference_ref: Some(memory_reference_ref.clone()),
            change_intent: MemoryReferenceChangeIntent::RecordArchiveHandoffResult,
            memory_ref: Some(memory_ref.clone()),
            archive_ref: Some(archive_ref.clone()),
            archive_handoff_ref: Some(archive_handoff_ref.clone()),
            source_ref: memory_source.clone(),
            safe_summary_ref: Some(
                MemorySafeSummaryRef::new(memory_source.clone(), "memory-safe-summary-1")
                    .expect("memory safe summary"),
            ),
            reason_ref: MemoryReferenceReasonRef::new(
                MemoryReferenceReasonKind::ArchiveHandoffResult,
                sample_identity_source_ref(),
            )
            .expect("memory reason"),
            change_material_marker: MemoryReferenceChangeMaterialMarker {
                material_kind: MemoryReferenceChangeMaterialKind::HandoffMarkerOnly,
                source_ref: None,
            },
        };
        let memory_result = MemoryReferenceCommandResult {
            member_ref: sample_member_ref(),
            memory_reference_ref,
            reference_state_kind: MemoryReferenceStateKind::Archived,
            memory_ref: Some(memory_ref),
            archive_ref: Some(archive_ref),
            archive_handoff_ref: Some(archive_handoff_ref),
            source_ref: memory_source.clone(),
            safe_summary_ref: Some(
                MemorySafeSummaryRef::new(memory_source, "memory-safe-summary-1")
                    .expect("memory safe summary"),
            ),
            reason_ref: MemoryReferenceReasonRef::new(
                MemoryReferenceReasonKind::ArchiveHandoffResult,
                sample_identity_source_ref(),
            )
            .expect("memory reason"),
        };

        roundtrip(&role_request);
        roundtrip(&role_result);
        roundtrip(&career_request);
        roundtrip(&career_result);
        roundtrip(&memory_request);
        roundtrip(&memory_result);
    }

    #[test]
    fn command_trace_handoff_dtos_roundtrip() {
        let request = PrepareTraceHandoffRequest {
            member_ref: sample_member_ref(),
            requested_handoff_intent_ref: Some(TraceHandoffIntentRef::new("handoff-intent-1")),
            trace_record_refs: vec![
                IdentityTraceRecordRef::new("trace-1"),
                IdentityTraceRecordRef::new("trace-2"),
            ],
            audit_trail_ref: Some(crate::refs::AuditTrailRef::new("audit-1")),
            handoff_target_ref: HandoffTargetRef::new("target-1"),
            handoff_scope_ref: HandoffScopeRef::new("scope-1"),
            safe_material_ref: TraceHandoffSafeMaterialRef::new("material-1"),
            visibility_context_ref: VisibilityContextRef::new("visibility-context-1"),
            handoff_reason_ref: HandoffReasonRef::new(sample_identity_source_ref())
                .expect("handoff reason"),
        };
        let result = TraceHandoffCommandResult {
            member_ref: sample_member_ref(),
            handoff_intent_ref: TraceHandoffIntentRef::new("handoff-intent-1"),
            handoff_state_kind: HandoffStateKind::PendingHandoff,
            handoff_target_ref: HandoffTargetRef::new("target-1"),
            handoff_scope_ref: HandoffScopeRef::new("scope-1"),
            trace_record_refs: vec![
                IdentityTraceRecordRef::new("trace-1"),
                IdentityTraceRecordRef::new("trace-2"),
            ],
            audit_trail_ref: Some(crate::refs::AuditTrailRef::new("audit-1")),
            safe_material_ref: TraceHandoffSafeMaterialRef::new("material-1"),
        };

        roundtrip(&request);
        roundtrip(&result);
    }

    #[test]
    fn query_shell_roundtrips() {
        let request = IdentityQueryRequest {
            actor_ref: sample_actor_ref(),
            query_name: IdentityQueryName::new("ReadMemberSummary"),
            metadata: IdentityQueryMetadata {
                request_marker_ref: IdentityApiRequestMarkerRef::new("api-request-1"),
                schema_version_ref: IdentityProtocolSchemaVersionRef::new("identity.query.v1"),
                visibility_context_ref: VisibilityContextRef::new("visibility-context-1"),
                trace_context_ref: Some(IdentityTraceContextRef::new("trace-context-1")),
            },
            page: Some(IdentityPublicPageRequest {
                cursor: Some(IdentityPublicPageCursor::new("cursor-1")),
                limit: 20,
            }),
            body: "query".to_owned(),
        };

        let response = IdentityQueryResponse {
            query_name: IdentityQueryName::new("ReadMemberSummary"),
            surface: sample_query_surface(),
            body: Some("summary".to_owned()),
        };

        let page = IdentityPageResponse {
            query_name: IdentityQueryName::new("ReadIdentityTrace"),
            surface: sample_query_surface(),
            page_info: IdentityPublicPageInfo {
                next_cursor: Some(IdentityPublicPageCursor::new("cursor-2")),
                has_more: true,
                item_count: 1,
            },
            items: vec!["trace".to_owned()],
        };

        roundtrip(&request);
        roundtrip(&response);
        roundtrip(&page);
    }

    #[test]
    fn query_commit_05_b_request_dtos_roundtrip() {
        let role_source_ref = sample_role_source_ref();
        let query_requests = (
            GetGlobalMemberAnchorRequest {
                member_ref: sample_member_ref(),
                consumer_ref: sample_consumer_ref(),
            },
            GetGlobalLifecycleSummaryRequest {
                member_ref: sample_member_ref(),
                consumer_ref: sample_consumer_ref(),
            },
            GetRoleCapabilitySummaryRequest {
                member_ref: sample_member_ref(),
                consumer_ref: sample_consumer_ref(),
                summary_ref: Some(RoleCapabilitySummaryRef::from_id(
                    RoleCapabilitySummaryId::new("summary-1".to_owned())
                        .expect("summary id should be valid"),
                )),
            },
            ListCareerRecordsRequest {
                member_ref: sample_member_ref(),
                consumer_ref: sample_consumer_ref(),
            },
            ListMemoryReferencesRequest {
                member_ref: sample_member_ref(),
                consumer_ref: sample_consumer_ref(),
            },
            ReadMemberSummaryRequest {
                member_ref: sample_member_ref(),
                consumer_ref: sample_consumer_ref(),
            },
            ReadIdentityTraceRequest {
                selector: IdentityTraceReadSelector::BySubject {
                    member_ref: sample_member_ref(),
                    subject_ref: IdentityTraceSubjectRef::new("trace-subject-1"),
                    after_cursor_ref: Some(IdentityTruthCursor::new("truth-cursor-2")),
                },
                consumer_ref: sample_consumer_ref(),
            },
            ReadAuditTrailRequest {
                member_ref: sample_member_ref(),
                audit_scope_ref: AuditScopeRef::new("audit-scope-1"),
                audit_cursor_ref: Some(AuditCursorRef::new("audit-cursor-1")),
                consumer_ref: sample_consumer_ref(),
            },
            GetProjectionStateRequest {
                projection_ref: IdentityProjectionRef::new("projection-1"),
                projection_state_ref: Some(ProjectionStateRef::from_id(
                    ProjectionStateId::new("projection-state-1".to_owned())
                        .expect("projection state id"),
                )),
                consumer_ref: sample_consumer_ref(),
            },
            GetReferenceResolutionStateRequest {
                external_reference_ref: ExternalReferenceRef::new(
                    ExternalReferenceKind::MethodSource,
                    sample_identity_source_ref(),
                ),
                owner_ref: Some(IdentityReferenceOwnerRef::new(
                    IdentityReferenceOwnerKind::Maintenance,
                    sample_identity_source_ref(),
                )),
                consumer_ref: sample_consumer_ref(),
            },
            ReadReconciliationReportRequest {
                maintenance_scope_ref: MaintenanceScopeRef::new(sample_identity_source_ref()),
                report_ref: Some(ReconciliationReportRef::new("report-1")),
                consumer_ref: sample_consumer_ref(),
            },
            ListPendingIdentityOutboxRequest {
                selector: IdentityOutboxListSelector::ByTrace {
                    trace_record_ref: IdentityTraceRecordRef::new("trace-record-1"),
                },
                consumer_ref: sample_consumer_ref(),
            },
            GetIdentityOutboxStateRequest {
                outbox_record_ref: IdentityOutboxRecordRef::new("outbox-record-1"),
                consumer_ref: sample_consumer_ref(),
            },
            GetTraceHandoffStateRequest {
                handoff_intent_ref: TraceHandoffIntentRef::new("handoff-intent-1"),
                consumer_ref: sample_consumer_ref(),
            },
        );

        roundtrip(&query_requests.0);
        roundtrip(&query_requests.1);
        roundtrip(&query_requests.2);
        roundtrip(&query_requests.3);
        roundtrip(&query_requests.4);
        roundtrip(&query_requests.5);
        roundtrip(&query_requests.6);
        roundtrip(&query_requests.7);
        roundtrip(&query_requests.8);
        roundtrip(&query_requests.9);
        roundtrip(&query_requests.10);
        roundtrip(&query_requests.11);
        roundtrip(&query_requests.12);
        roundtrip(&query_requests.13);

        roundtrip(&IdentityVisibilityAccessSummary {
            read_subject_ref: IdentityReadSubjectRef::new("read-subject-1"),
            consumer_ref: sample_consumer_ref(),
            actor_ref: Some(sample_actor_ref()),
            visibility_context_ref: VisibilityContextRef::new("visibility-context-1"),
            scope_ref: sample_visibility_scope_ref(),
            access_state: IdentityVisibilityAccessState::Visible,
            redaction_profile_ref: None,
            redaction_marker_ref: None,
            visibility_result_ref: VisibilityResultRef::new("visibility-result-1"),
            degraded_marker_ref: None,
            degraded_kind: None,
        });

        let capability_source_ref =
            CapabilitySourceRef::from_source(role_source_ref.clone()).expect("capability source");
        let role_source_wrapper =
            RoleSourceRef::from_source(role_source_ref.clone()).expect("role source wrapper");
        let capability_evidence = CapabilityEvidenceRef::new(
            CapabilityEvidenceKind::MethodArtifact,
            sample_method_source_ref("method-evidence-1"),
        )
        .expect("capability evidence");
        let work_source_ref = sample_work_source_ref_typed();
        let project_participation_ref =
            ProjectParticipationRef::from_work_source(sample_work_source_ref("work-pp-1"))
                .expect("project participation ref");
        let source_marker_ref = CareerSourceMarkerRef::new(
            sample_member_ref(),
            work_source_ref.clone(),
            "career-marker-1",
        )
        .expect("career source marker");
        let memory_source_ref = sample_memory_reference_source_ref();

        roundtrip(&GlobalMemberAnchorView {
            member_ref: sample_member_ref(),
            anchor_state_kind: crate::refs::IdentityAnchorStateKind::Established,
            anchor_reason_ref: Some(
                IdentityAnchorReasonRef::new(
                    IdentityAnchorReasonKind::Retired,
                    sample_identity_source_ref(),
                )
                .expect("anchor reason"),
            ),
            anchor_changed_at: IdentityTimestamp::from_clock(10).expect("valid timestamp"),
            source_ref: Some(sample_identity_source_ref()),
            member_summary_view_ref: Some(MemberSummaryViewRef::new("view-1")),
            anchor_slice_ref: Some(sample_member_summary_slice_ref(
                MemberSummarySliceKind::Anchor,
            )),
        });

        roundtrip(&GlobalLifecycleSummaryView {
            member_ref: sample_member_ref(),
            lifecycle_state_kind: GlobalLifecycleStateKind::Available,
            reason_ref: Some(sample_lifecycle_reason_ref()),
            basis_ref: Some(
                GovernanceBasisRef::new(
                    GovernanceBasisKind::GateDecision,
                    ExternalSourceRef::new("basis-1".to_owned()).expect("basis ext ref"),
                )
                .expect("governance basis ref"),
            ),
            changed_by_ref: Some(sample_actor_ref()),
            changed_at: IdentityTimestamp::from_clock(11).expect("valid timestamp"),
            member_summary_view_ref: Some(MemberSummaryViewRef::new("view-1")),
            lifecycle_slice_ref: Some(sample_member_summary_slice_ref(
                MemberSummarySliceKind::Lifecycle,
            )),
        });

        roundtrip(&RoleCapabilitySummaryView {
            member_ref: sample_member_ref(),
            summary_ref: RoleCapabilitySummaryRef::from_id(
                RoleCapabilitySummaryId::new("summary-2".to_owned())
                    .expect("summary id should be valid"),
            ),
            summary_state_kind: RoleCapabilitySummaryStateKind::Active,
            source_snapshot_ref: RoleCapabilitySourceSnapshotRef::from_id(
                RoleCapabilitySourceSnapshotId::new("snapshot-1".to_owned())
                    .expect("snapshot id should be valid"),
            ),
            source_state_kind: Some(RoleCapabilitySourceStateKind::SourceResolved),
            role_source_ref: Some(role_source_wrapper),
            capability_source_refs: vec![capability_source_ref],
            evidence_refs: vec![capability_evidence],
            safe_summary_ref: Some(
                RoleCapabilitySafeSummaryRef::new(role_source_ref.clone(), "safe-summary-1")
                    .expect("safe summary"),
            ),
            member_summary_view_ref: Some(MemberSummaryViewRef::new("view-1")),
            role_capability_slice_refs: vec![sample_member_summary_slice_ref(
                MemberSummarySliceKind::RoleCapability,
            )],
        });

        roundtrip(&CareerRecordView {
            career_record_ref: CareerRecordRef::from_id(
                CareerRecordId::new("career-1".to_owned()).expect("career id should be valid"),
            ),
            member_ref: sample_member_ref(),
            record_state_kind: CareerRecordStateKind::Appended,
            project_participation_ref: Some(project_participation_ref),
            work_source_ref: Some(work_source_ref),
            source_marker_ref: Some(source_marker_ref),
            career_summary_ref: Some(
                CareerSafeSummaryRef::new(sample_work_source_ref_typed(), "career-safe-summary-1")
                    .expect("career safe summary"),
            ),
            append_reason_ref: Some(
                CareerAppendReasonRef::new(
                    CareerAppendReasonKind::ManualAppend,
                    sample_identity_source_ref(),
                )
                .expect("career append reason"),
            ),
            appended_at: Some(IdentityTimestamp::from_clock(12).expect("valid timestamp")),
            correction_of_ref: None,
            superseded_by_ref: None,
        });

        roundtrip(&MemoryReferenceView {
            memory_reference_ref: MemoryReferenceRef::from_id(
                MemoryReferenceId::new("memory-ref-1".to_owned())
                    .expect("memory reference id should be valid"),
            ),
            member_ref: sample_member_ref(),
            reference_state_kind: MemoryReferenceStateKind::Linked,
            memory_ref: Some(
                MemoryRef::from_source(sample_memory_source_ref("memory-carrier-1"))
                    .expect("memory ref"),
            ),
            archive_ref: Some(
                ArchiveRef::from_source(sample_memory_source_ref("archive-carrier-1"))
                    .expect("archive ref"),
            ),
            archive_handoff_ref: Some(
                ArchiveHandoffRef::new(sample_identity_source_ref(), "handoff-1")
                    .expect("archive handoff ref"),
            ),
            source_ref: Some(memory_source_ref.clone()),
            safe_summary_ref: Some(
                MemorySafeSummaryRef::new(memory_source_ref, "memory-safe-summary-1")
                    .expect("memory safe summary"),
            ),
            reason_ref: Some(
                MemoryReferenceReasonRef::new(
                    MemoryReferenceReasonKind::ManualMaintain,
                    sample_identity_source_ref(),
                )
                .expect("memory reason"),
            ),
            changed_at: Some(IdentityTimestamp::from_clock(13).expect("valid timestamp")),
        });

        roundtrip(&sample_member_summary_view());

        roundtrip(&IdentityTraceRecordView {
            trace_record_ref: IdentityTraceRecordRef::new("trace-record-1"),
            member_ref: sample_member_ref(),
            subject_ref: IdentityTraceSubjectRef::new("trace-subject-1"),
            audit_subject_ref: IdentityAuditSubjectRef::new("audit-subject-1"),
            change_kind_ref: IdentityChangeKindRef::new(
                IdentityChangeKind::MemberAnchorChanged,
                Some(sample_identity_source_ref()),
            ),
            source_cursor_ref: IdentityTruthCursor::new("truth-cursor-3"),
            reason_ref: Some(IdentityChangeReasonRef::new(sample_identity_source_ref())),
            source_ref: Some(sample_identity_source_ref()),
            basis_ref: Some(
                GovernanceBasisRef::new(
                    GovernanceBasisKind::GateDecision,
                    ExternalSourceRef::new("basis-2".to_owned()).expect("basis ext ref"),
                )
                .expect("governance basis ref"),
            ),
            actor_ref: Some(sample_actor_ref()),
            visibility_result_ref: VisibilityResultRef::new("visibility-result-2"),
            superseded_by_trace_ref: Some(IdentityTraceRecordRef::new("trace-record-2")),
            read_material_marker: IdentityReadMaterialMarker::new(
                IdentityReadMaterialKind::TraceRefsOnly,
                Some(sample_identity_source_ref()),
            ),
            occurred_at: IdentityTimestamp::from_clock(14).expect("valid timestamp"),
        });

        roundtrip(&AuditTrailEntryView {
            audit_trail_ref: AuditTrailRef::new("audit-trail-1"),
            audit_subject_ref: IdentityAuditSubjectRef::new("audit-subject-1"),
            audit_scope_ref: AuditScopeRef::new("audit-scope-1"),
            member_ref: Some(sample_member_ref()),
            trace_record_ref: IdentityTraceRecordRef::new("trace-record-1"),
            change_kind_ref: IdentityChangeKindRef::new(
                IdentityChangeKind::LifecycleChanged,
                Some(sample_identity_source_ref()),
            ),
            visibility_result_ref: VisibilityResultRef::new("visibility-result-3"),
            occurred_at: IdentityTimestamp::from_clock(15).expect("valid timestamp"),
        });

        roundtrip(&ProjectionStateView {
            projection_state_ref: Some(ProjectionStateRef::from_id(
                ProjectionStateId::new("projection-state-2".to_owned())
                    .expect("projection state id"),
            )),
            projection_ref: IdentityProjectionRef::new("projection-1"),
            member_ref: Some(sample_member_ref()),
            state_kind: Some(ProjectionStateKind::Stale),
            source_cursor_ref: Some(IdentityProjectionCursorRef::new(
                sample_identity_source_ref(),
            )),
            maintenance_scope_ref: Some(MaintenanceScopeRef::new(sample_identity_source_ref())),
            issue_ref: Some(MaintenanceIssueRef::new("maintenance-issue-1")),
            checked_at: Some(IdentityTimestamp::from_clock(16).expect("valid timestamp")),
            visibility_result_ref: VisibilityResultRef::new("visibility-result-4"),
        });

        roundtrip(&ReferenceResolutionSidecarRefsView {
            role_capability_safe_summary_ref: Some(ExternalReferenceSafeSummaryRef::new(
                ExternalReferenceRef::new(
                    ExternalReferenceKind::MethodSource,
                    sample_identity_source_ref(),
                ),
                sample_identity_source_ref(),
            )),
            career_safe_summary_ref: None,
            memory_safe_summary_ref: None,
            governance_basis_summary_ref: None,
            evidence_summary_ref: None,
            source_version_ref: Some(ExternalSourceVersionRef::new(sample_identity_source_ref())),
        });

        roundtrip(&ReferenceResolutionStateView {
            resolution_state_ref: Some(ReferenceResolutionStateRef::from_id(
                ReferenceResolutionStateId::new("reference-state-1".to_owned())
                    .expect("reference state id"),
            )),
            external_reference_ref: ExternalReferenceRef::new(
                ExternalReferenceKind::MethodSource,
                sample_identity_source_ref(),
            ),
            owner_ref: Some(IdentityReferenceOwnerRef::new(
                IdentityReferenceOwnerKind::Maintenance,
                sample_identity_source_ref(),
            )),
            state_kind: Some(ReferenceResolutionStateKind::Resolved),
            source_version_ref: Some(ExternalSourceVersionRef::new(sample_identity_source_ref())),
            safe_summary_ref: Some(ExternalReferenceSafeSummaryRef::new(
                ExternalReferenceRef::new(
                    ExternalReferenceKind::MethodSource,
                    sample_identity_source_ref(),
                ),
                sample_identity_source_ref(),
            )),
            sidecar_refs: Some(ReferenceResolutionSidecarRefsView {
                role_capability_safe_summary_ref: None,
                career_safe_summary_ref: None,
                memory_safe_summary_ref: None,
                governance_basis_summary_ref: None,
                evidence_summary_ref: None,
                source_version_ref: Some(ExternalSourceVersionRef::new(
                    sample_identity_source_ref(),
                )),
            }),
            issue_ref: None,
            checked_at: Some(IdentityTimestamp::from_clock(17).expect("valid timestamp")),
            visibility_result_ref: VisibilityResultRef::new("visibility-result-5"),
        });

        roundtrip(&ReconciliationReportView {
            report_ref: ReconciliationReportRef::new("report-1"),
            maintenance_scope_ref: MaintenanceScopeRef::new(sample_identity_source_ref()),
            target_refs: vec![IdentityMaintenanceTargetRef::new("target-1")],
            finding_refs: vec![ReconciliationFindingRef::new("finding-1")],
            issue_refs: vec![MaintenanceIssueRef::new("maintenance-issue-2")],
            report_state: ReconciliationReportStateKind::FindingDetected,
            generated_by_ref: Some(sample_actor_ref()),
            generated_at: IdentityTimestamp::from_clock(18).expect("valid timestamp"),
            visibility_result_ref: VisibilityResultRef::new("visibility-result-6"),
        });

        roundtrip(&IdentityOutboxRecordView {
            outbox_record_ref: IdentityOutboxRecordRef::new("outbox-record-1"),
            member_ref: sample_member_ref(),
            subject_ref: IdentityOutboxSubjectRef::new("outbox-subject-1"),
            change_kind_ref: IdentityChangeKindRef::new(
                IdentityChangeKind::MemberAnchorChanged,
                Some(sample_identity_source_ref()),
            ),
            payload_marker_ref: IdentityOutboxPayloadMarkerRef::new("payload-marker-1"),
            topic_key_ref: TopicKeyRef::new("topic-key-1"),
            trace_record_ref: IdentityTraceRecordRef::new("trace-record-1"),
            outbox_state_kind: OutboxStateKind::PendingPublish,
            attempt_ref: Some(OutboxDeliveryAttemptRef::new(sample_identity_source_ref())),
            issue_ref: Some(OutboxDeliveryIssueRef::new(sample_identity_source_ref())),
            created_at: IdentityTimestamp::from_clock(19).expect("valid timestamp"),
            updated_at: IdentityTimestamp::from_clock(20).expect("valid timestamp"),
            visibility_result_ref: VisibilityResultRef::new("visibility-result-7"),
        });

        roundtrip(&IdentityOutboxStateView {
            outbox_record_ref: IdentityOutboxRecordRef::new("outbox-record-2"),
            subject_ref: IdentityOutboxSubjectRef::new("outbox-subject-2"),
            topic_key_ref: TopicKeyRef::new("topic-key-2"),
            trace_record_ref: IdentityTraceRecordRef::new("trace-record-2"),
            outbox_state_kind: OutboxStateKind::RetryableFailed,
            attempt_ref: Some(OutboxDeliveryAttemptRef::new(sample_identity_source_ref())),
            issue_ref: Some(OutboxDeliveryIssueRef::new(sample_identity_source_ref())),
            payload_marker_ref: IdentityOutboxPayloadMarkerRef::new("payload-marker-2"),
            changed_at: IdentityTimestamp::from_clock(21).expect("valid timestamp"),
            visibility_result_ref: VisibilityResultRef::new("visibility-result-8"),
        });

        roundtrip(&TraceHandoffStateView {
            handoff_intent_ref: TraceHandoffIntentRef::new("handoff-intent-2"),
            member_ref: sample_member_ref(),
            trace_record_refs: vec![IdentityTraceRecordRef::new("trace-record-3")],
            audit_trail_ref: Some(AuditTrailRef::new("audit-trail-2")),
            handoff_target_ref: HandoffTargetRef::new("handoff-target-1"),
            handoff_scope_ref: HandoffScopeRef::new("handoff-scope-1"),
            safe_material_ref: TraceHandoffSafeMaterialRef::new("safe-material-1"),
            handoff_state_kind: HandoffStateKind::Delivered,
            attempt_ref: Some(HandoffAttemptRef::new(sample_identity_source_ref())),
            receipt_ref: Some(HandoffReceiptRef::new("receipt-1")),
            issue_ref: Some(HandoffIssueRef::new(sample_identity_source_ref())),
            created_at: IdentityTimestamp::from_clock(22).expect("valid timestamp"),
            updated_at: IdentityTimestamp::from_clock(23).expect("valid timestamp"),
            changed_at: IdentityTimestamp::from_clock(24).expect("valid timestamp"),
            visibility_result_ref: VisibilityResultRef::new("visibility-result-9"),
        });
    }

    #[test]
    fn protocol_rejection_roundtrips() {
        roundtrip(&IdentityProtocolRejection {
            surface_ref: IdentityProtocolSurfaceRef::new("surface-1"),
            rejection_kind: IdentityProtocolRejectionKind::UnsupportedVersion,
            issue_refs: IdentityProtocolValidationIssueRefSet(vec![
                IdentityProtocolValidationIssueRef::new("issue-1"),
            ]),
            degraded: Some(IdentityDegradedMarker {
                degraded_marker_ref: IdentityDegradedMarkerRef::new("degraded-1"),
                degraded_kind: IdentityDegradedKind::AdapterUnavailable,
            }),
        });
    }

    #[test]
    fn event_shell_roundtrips() {
        let inbound = IdentityInboundEventEnvelope {
            consumer_name: IdentityInboundConsumerName::new("HandleRoleCapabilitySourceChanged"),
            envelope_marker_ref: "envelope-1".into(),
            consumer_binding_ref: IdentityConsumerBindingRef::new("binding-1"),
            source_event_ref: IdentitySourceEventRef::new("source-event-1"),
            idempotency_key: "idem-1".into(),
            schema_version_ref: IdentityProtocolSchemaVersionRef::new("identity.consumer.v1"),
            occurred_at: Some(IdentityTimestamp::from_clock(1).expect("valid timestamp")),
            received_at: IdentityTimestamp::from_clock(2).expect("valid timestamp"),
            trace_context_ref: Some(IdentityTraceContextRef::new("trace-context-1")),
            payload: "payload".to_owned(),
        };

        let receipt = IdentityConsumerReceipt {
            receipt_ref: IdentityConsumerReceiptRef::new("consumer-receipt-1"),
            consumer_name: IdentityInboundConsumerName::new("HandleRoleCapabilitySourceChanged"),
            outcome: IdentityConsumerOutcome::Accepted,
            stored_result_ref: IdentityStoredResultRef::new("stored-result-1"),
            trace_refs: vec![IdentityTraceRecordRef::new("trace-1")],
            outbox_refs: vec![IdentityOutboxRecordRef::new("outbox-1")],
            issue_refs: vec![IdentityProtocolValidationIssueRef::new("issue-1")],
        };

        let outbound = IdentityOutboundEventEnvelope {
            event_name: IdentityOutboundEventName::new("GlobalLifecycleChanged"),
            event_ref: IdentityOutboundEventRef::new("event-1"),
            outbox_record_ref: IdentityOutboxRecordRef::new("outbox-1"),
            topic_key_ref: TopicKeyRef::new("topic-key-1"),
            schema_version_ref: IdentityProtocolSchemaVersionRef::new("identity.outbound.v1"),
            payload_marker_ref: IdentityOutboxPayloadMarkerRef::new("payload-marker-1"),
            trace_ref: IdentityTraceRecordRef::new("trace-1"),
            published_subject_ref: IdentityOutboxSubjectRef::new("outbox-subject-1"),
            payload: "payload".to_owned(),
        };

        roundtrip(&inbound);
        roundtrip(&receipt);
        roundtrip(&outbound);
    }

    #[test]
    fn member_lifecycle_outbound_payloads_roundtrip() {
        let established = GlobalMemberEstablishedPayload {
            member_ref: sample_member_ref(),
            source_ref: sample_identity_source_ref(),
            anchor_state_kind: crate::refs::IdentityAnchorStateKind::Established,
            lifecycle_state_kind: crate::refs::GlobalLifecycleStateKind::Available,
            created_by_ref: sample_actor_ref(),
            established_at: IdentityTimestamp::from_clock(1).expect("valid timestamp"),
            accepted_cursor_ref: IdentityTruthCursor::new("truth-cursor-1"),
        };
        let anchor_changed = IdentityAnchorChangedPayload {
            member_ref: sample_member_ref(),
            anchor_state_kind: crate::refs::IdentityAnchorStateKind::RetiredHeld,
            anchor_reason_ref: Some(
                crate::refs::IdentityAnchorReasonRef::new(
                    crate::refs::IdentityAnchorReasonKind::Retired,
                    sample_identity_source_ref(),
                )
                .expect("anchor reason should be valid"),
            ),
            changed_at: IdentityTimestamp::from_clock(2).expect("valid timestamp"),
            accepted_cursor_ref: IdentityTruthCursor::new("truth-cursor-2"),
        };
        let lifecycle_changed = GlobalLifecycleChangedPayload {
            member_ref: sample_member_ref(),
            lifecycle_state_kind: crate::refs::GlobalLifecycleStateKind::Retired,
            reason_ref: LifecycleReasonRef::new(
                LifecycleReasonKind::Retirement,
                sample_identity_source_ref(),
            )
            .expect("retirement reason should be valid"),
            basis_ref: None,
            changed_by_ref: sample_actor_ref(),
            changed_at: IdentityTimestamp::from_clock(3).expect("valid timestamp"),
            anchor_state_kind: Some(crate::refs::IdentityAnchorStateKind::RetiredHeld),
            accepted_cursor_ref: IdentityTruthCursor::new("truth-cursor-3"),
        };
        let availability_changed = GlobalMemberAvailabilityChangedPayload {
            member_ref: sample_member_ref(),
            lifecycle_state_kind: crate::refs::GlobalLifecycleStateKind::Paused,
            is_available: false,
            reason_ref: LifecycleReasonRef::new(
                LifecycleReasonKind::ManualPause,
                sample_identity_source_ref(),
            )
            .expect("pause reason should be valid"),
            changed_at: IdentityTimestamp::from_clock(4).expect("valid timestamp"),
            accepted_cursor_ref: IdentityTruthCursor::new("truth-cursor-4"),
        };

        roundtrip(&established);
        roundtrip(&anchor_changed);
        roundtrip(&lifecycle_changed);
        roundtrip(&availability_changed);
    }

    #[test]
    fn inbound_consumer_callback_payloads_roundtrip() {
        let role_source = sample_role_source_ref();
        let work_source = sample_work_source_ref_typed();
        let memory_source = sample_memory_reference_source_ref();
        let archive_handoff_ref =
            ArchiveHandoffRef::new(sample_memory_source_ref("handoff-1"), "handoff-1")
                .expect("archive handoff");

        let role_payload = RoleCapabilitySourceChangedPayload {
            member_ref: sample_member_ref(),
            source_ref: role_source.clone(),
            source_version_ref: RoleCapabilitySourceVersionRef::new(role_source.clone(), "v1")
                .expect("role source version"),
            source_state_kind: RoleCapabilitySourceStateKind::SourceResolved,
            safe_summary_ref: Some(
                RoleCapabilitySafeSummaryRef::new(role_source.clone(), "safe-role-summary-1")
                    .expect("role safe summary"),
            ),
            evidence_refs: vec![
                CapabilityEvidenceRef::new(
                    CapabilityEvidenceKind::MethodArtifact,
                    role_source.source_ref.clone(),
                )
                .expect("role evidence"),
            ],
            external_reference_ref: Some(sample_external_reference()),
            reference_owner_ref: Some(sample_reference_owner()),
            change_reason_ref: Some(
                RoleCapabilityChangeReasonRef::new(
                    RoleCapabilityChangeReasonKind::SourceChanged,
                    sample_identity_source_ref(),
                )
                .expect("role change reason"),
            ),
            material_marker: RoleCapabilityChangeMaterialMarker::new(
                RoleCapabilityChangeMaterialKind::SafeSummaryMarker,
                Some(role_source.source_ref.clone()),
            ),
        };

        let work_payload = WorkParticipationAcceptedPayload {
            member_ref: sample_member_ref(),
            project_participation_ref: ProjectParticipationRef::from_work_source(
                work_source.source_ref.clone(),
            )
            .expect("project participation"),
            work_source_ref: work_source.clone(),
            career_source_marker_ref: CareerSourceMarkerRef::new(
                sample_member_ref(),
                work_source.clone(),
                "career-marker-1",
            )
            .expect("career source marker"),
            safe_summary_ref: CareerSafeSummaryRef::new(work_source.clone(), "career-safe-1")
                .expect("career safe summary"),
            append_reason_ref: Some(
                CareerAppendReasonRef::new(
                    CareerAppendReasonKind::ManualAppend,
                    sample_identity_source_ref(),
                )
                .expect("career reason"),
            ),
            material_marker: CareerAppendMaterialMarker {
                material_kind: CareerAppendMaterialKind::SafeSummaryMarker,
                source_ref: Some(work_source.source_ref.clone()),
            },
        };

        let memory_payload = MemoryReferenceSourceStateChangedPayload {
            member_ref: sample_member_ref(),
            memory_reference_ref: Some(MemoryReferenceRef::from_id(
                MemoryReferenceId::new("memory-reference-1".to_owned())
                    .expect("memory reference id"),
            )),
            source_ref: memory_source.clone(),
            memory_ref: Some(
                MemoryRef::from_source(sample_memory_source_ref("memory-1")).expect("memory ref"),
            ),
            archive_ref: Some(
                ArchiveRef::from_source(sample_memory_source_ref("archive-1"))
                    .expect("archive ref"),
            ),
            target_state_kind: MemoryReferenceStateKind::Archived,
            safe_summary_ref: Some(
                MemorySafeSummaryRef::new(memory_source.clone(), "memory-safe-1")
                    .expect("memory safe summary"),
            ),
            external_reference_ref: Some(sample_external_reference()),
            reference_owner_ref: Some(sample_reference_owner()),
            reason_ref: Some(
                MemoryReferenceReasonRef::new(
                    MemoryReferenceReasonKind::SourceStateChanged,
                    sample_identity_source_ref(),
                )
                .expect("memory reason"),
            ),
            material_marker: MemoryReferenceChangeMaterialMarker {
                material_kind: MemoryReferenceChangeMaterialKind::ReferenceMarkersOnly,
                source_ref: Some(memory_source.source_ref.clone()),
            },
        };

        let archive_payload = ArchiveHandoffResultPayload {
            member_ref: sample_member_ref(),
            memory_reference_ref: None,
            archive_ref: ArchiveRef::from_source(sample_memory_source_ref("archive-2"))
                .expect("archive ref"),
            archive_handoff_ref: archive_handoff_ref.clone(),
            target_state_kind: MemoryReferenceStateKind::HandoffFailed,
            reason_ref: Some(
                MemoryReferenceReasonRef::new(
                    MemoryReferenceReasonKind::ArchiveHandoffResult,
                    sample_identity_source_ref(),
                )
                .expect("archive reason"),
            ),
            issue_ref: Some(HandoffIssueRef::new(sample_identity_source_ref())),
            material_marker: MemoryReferenceChangeMaterialMarker {
                material_kind: MemoryReferenceChangeMaterialKind::HandoffMarkerOnly,
                source_ref: Some(archive_handoff_ref.source_ref.clone()),
            },
        };

        let trace_payload = TraceHandoffResultPayload {
            handoff_intent_ref: TraceHandoffIntentRef::new("handoff-1"),
            handoff_target_ref: HandoffTargetRef::new("target-1"),
            handoff_scope_ref: Some(HandoffScopeRef::new("scope-1")),
            attempt_ref: HandoffAttemptRef::new(sample_identity_source_ref()),
            result_kind: TraceHandoffResultKind::Delivered,
            receipt_ref: Some(HandoffReceiptRef::new("receipt-1")),
            issue_ref: None,
        };

        roundtrip(&role_payload);
        roundtrip(&work_payload);
        roundtrip(&memory_payload);
        roundtrip(&archive_payload);
        roundtrip(&trace_payload);
        roundtrip(&TraceHandoffResultKind::RetryableFailed);
    }

    #[test]
    fn job_shell_roundtrips() {
        let request = IdentityJobRequest {
            job_name: IdentityJobName::new("RunIdentityReconciliation"),
            job_run_ref: IdentityJobRunRef::new("job-run-1"),
            run_metadata_ref: IdentityJobRunMetadataRef::new("job-meta-1"),
            scope_marker_ref: IdentityJobScopeMarkerRef::new("job-scope-1"),
            idempotency_key: "idem-1".into(),
            input_cursor_ref: Some(IdentityJobCursorRef::new("job-cursor-1")),
            schema_version_ref: IdentityProtocolSchemaVersionRef::new("identity.job.v1"),
            system_actor_ref: ActorRef::system("system-job-1"),
            input: "input".to_owned(),
        };

        let response = IdentityJobResponse {
            job_name: IdentityJobName::new("RunIdentityReconciliation"),
            report_ref: IdentityJobReportRef::new("job-report-1"),
            stored_result_ref: IdentityStoredResultRef::new("stored-result-1"),
            output: "output".to_owned(),
            report: IdentityJobReportSurface {
                job_run_ref: IdentityJobRunRef::new("job-run-1"),
                result_kind: IdentityJobResultKind::Partial,
                affected_member_refs: vec![sample_member_ref()],
                affected_projection_refs: vec![IdentityProjectionRef::new("projection-1")],
                rebuilt_projection_refs: vec![],
                failed_projection_refs: vec![],
                refreshed_reference_refs: vec![],
                failed_reference_refs: vec![],
                inspected_target_refs: vec![IdentityMaintenanceTargetRef::new("target-1")],
                report_refs: vec![ReconciliationReportRef::new("report-1")],
                outbox_record_refs: vec![IdentityOutboxRecordRef::new("outbox-1")],
                published_outbox_refs: vec![],
                failed_outbox_refs: vec![],
                handoff_intent_refs: vec![],
                delivered_handoff_refs: vec![],
                failed_handoff_refs: vec![],
                handoff_receipt_refs: vec![HandoffReceiptRef::new("handoff-receipt-1")],
                issue_refs: vec!["maintenance-issue-1".into()],
                input_cursor_ref: Some(IdentityJobCursorRef::new("job-cursor-1")),
                output_cursor_ref: Some(IdentityJobCursorRef::new("job-cursor-2")),
                started_at: IdentityTimestamp::from_clock(1).expect("valid timestamp"),
                finished_at: Some(IdentityTimestamp::from_clock(2).expect("valid timestamp")),
            },
        };

        roundtrip(&request);
        roundtrip(&response);
    }
}
