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
        IdentityCommandEffectPublicSummary, IdentityCommandOutcome, IdentityCommandRequest,
        IdentityCommandResponse,
    };
    use crate::events::{
        IdentityConsumerOutcome, IdentityConsumerReceipt, IdentityInboundEventEnvelope,
        IdentityOutboundEventEnvelope, IdentityOutboundEventRef,
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
        IdentityPageResponse, IdentityPublicPageCursor, IdentityPublicPageInfo,
        IdentityPublicPageRequest, IdentityQueryRequest, IdentityQueryResponse,
    };
    use crate::refs::{
        GlobalMemberId, GlobalMemberRef, HandoffReceiptRef, IdentityApiRequestMarkerRef,
        IdentityAuditSubjectRef, IdentityCanonicalRequestMarkerRef, IdentityConsumerBindingRef,
        IdentityConsumerReceiptRef, IdentityDegradedMarkerRef, IdentityJobCursorRef,
        IdentityJobReportRef, IdentityJobRunMetadataRef, IdentityJobRunRef,
        IdentityJobScopeMarkerRef, IdentityMaintenanceTargetRef, IdentityOutboxPayloadMarkerRef,
        IdentityOutboxRecordRef, IdentityOutboxSubjectRef, IdentityProjectionRef,
        IdentityReadSurfaceKind, IdentityRedactionMarkerRef, IdentityRequestDigestValue,
        IdentitySourceEventRef, IdentityStoredResultRef, IdentityTimestamp,
        IdentityTraceContextRef, IdentityTraceRecordRef, IdentityTruthCursor,
        ProjectionFreshnessMarkerRef, ReconciliationReportRef, TopicKeyRef, VisibilityContextRef,
        VisibilityResultRef,
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

    fn sample_query_surface() -> IdentityQuerySurface {
        IdentityQuerySurface {
            disposition: IdentityQueryDisposition::Degraded,
            visibility: IdentityVisibilityMarker {
                visibility_result_ref: VisibilityResultRef::new("visibility-result-1"),
                read_surface_kind: IdentityReadSurfaceKind::Summary,
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
