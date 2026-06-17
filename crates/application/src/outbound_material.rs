//! Shared accepted outbound material mapping and outbox construction helpers.

use identity_contracts::protocol::{IdentityOutboundEventName, IdentityProtocolSchemaVersionRef};
use identity_contracts::refs::{
    GlobalMemberRef, IdentityChangeKindRef, IdentityOutboxSubjectRef, IdentityTimestamp,
    IdentityTraceRecordRef, TopicKeyRef,
};
use identity_domain::outbox::{IdentityOutboxRecord, IdentityOutboxRecordCreateArgs};

use crate::errors::ApplicationError;
use crate::ports::IdentityIdGeneratorPort;

/// Canonical accepted outbound material kinds defined by Step 8/9.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AcceptedOutboundMaterialKind {
    GlobalMemberEstablished,
    IdentityAnchorChanged,
    GlobalLifecycleChanged,
    GlobalMemberAvailabilityChanged,
    RoleCapabilitySummaryChanged,
    RoleCapabilitySourceStateChanged,
    CareerRecordAppended,
    CareerCorrectionAppended,
    MemoryReferenceChanged,
    MemoryArchiveHandoffStateChanged,
}

impl AcceptedOutboundMaterialKind {
    /// Returns all canonical accepted outbound material kinds.
    pub const fn all() -> [Self; 10] {
        [
            Self::GlobalMemberEstablished,
            Self::IdentityAnchorChanged,
            Self::GlobalLifecycleChanged,
            Self::GlobalMemberAvailabilityChanged,
            Self::RoleCapabilitySummaryChanged,
            Self::RoleCapabilitySourceStateChanged,
            Self::CareerRecordAppended,
            Self::CareerCorrectionAppended,
            Self::MemoryReferenceChanged,
            Self::MemoryArchiveHandoffStateChanged,
        ]
    }

    /// Returns the stable public outbound event name.
    pub fn event_name(self) -> IdentityOutboundEventName {
        IdentityOutboundEventName::new(match self {
            Self::GlobalMemberEstablished => "GlobalMemberEstablished",
            Self::IdentityAnchorChanged => "IdentityAnchorChanged",
            Self::GlobalLifecycleChanged => "GlobalLifecycleChanged",
            Self::GlobalMemberAvailabilityChanged => "GlobalMemberAvailabilityChanged",
            Self::RoleCapabilitySummaryChanged => "RoleCapabilitySummaryChanged",
            Self::RoleCapabilitySourceStateChanged => "RoleCapabilitySourceStateChanged",
            Self::CareerRecordAppended => "CareerRecordAppended",
            Self::CareerCorrectionAppended => "CareerCorrectionAppended",
            Self::MemoryReferenceChanged => "MemoryReferenceChanged",
            Self::MemoryArchiveHandoffStateChanged => "MemoryArchiveHandoffStateChanged",
        })
    }

    /// Returns the canonical topic key marker.
    pub fn topic_key_ref(self) -> TopicKeyRef {
        TopicKeyRef::new(match self {
            Self::GlobalMemberEstablished => "identity.global-member.established.v1",
            Self::IdentityAnchorChanged => "identity.anchor.changed.v1",
            Self::GlobalLifecycleChanged => "identity.lifecycle.changed.v1",
            Self::GlobalMemberAvailabilityChanged => {
                "identity.global-member.availability.changed.v1"
            }
            Self::RoleCapabilitySummaryChanged => "identity.role-capability.summary.changed.v1",
            Self::RoleCapabilitySourceStateChanged => {
                "identity.role-capability.source-state.changed.v1"
            }
            Self::CareerRecordAppended => "identity.career.record.appended.v1",
            Self::CareerCorrectionAppended => "identity.career.correction.appended.v1",
            Self::MemoryReferenceChanged => "identity.memory.reference.changed.v1",
            Self::MemoryArchiveHandoffStateChanged => {
                "identity.memory.archive-handoff-state.changed.v1"
            }
        })
    }

    /// Returns the canonical outbound protocol schema marker.
    pub fn schema_version_ref(self) -> IdentityProtocolSchemaVersionRef {
        IdentityProtocolSchemaVersionRef::new(match self {
            Self::GlobalMemberEstablished => "identity.outbound.global-member-established.v1",
            Self::IdentityAnchorChanged => "identity.outbound.anchor-changed.v1",
            Self::GlobalLifecycleChanged => "identity.outbound.lifecycle-changed.v1",
            Self::GlobalMemberAvailabilityChanged => {
                "identity.outbound.member-availability-changed.v1"
            }
            Self::RoleCapabilitySummaryChanged => {
                "identity.outbound.role-capability-summary-changed.v1"
            }
            Self::RoleCapabilitySourceStateChanged => {
                "identity.outbound.role-capability-source-state-changed.v1"
            }
            Self::CareerRecordAppended => "identity.outbound.career-record-appended.v1",
            Self::CareerCorrectionAppended => "identity.outbound.career-correction-appended.v1",
            Self::MemoryReferenceChanged => "identity.outbound.memory-reference-changed.v1",
            Self::MemoryArchiveHandoffStateChanged => {
                "identity.outbound.memory-archive-handoff-state-changed.v1"
            }
        })
    }

    /// Resolves a canonical material kind from a topic key marker.
    pub fn from_topic_key_ref(topic_key_ref: &TopicKeyRef) -> Option<Self> {
        match topic_key_ref.as_str() {
            "identity.global-member.established.v1" => Some(Self::GlobalMemberEstablished),
            "identity.anchor.changed.v1" => Some(Self::IdentityAnchorChanged),
            "identity.lifecycle.changed.v1" => Some(Self::GlobalLifecycleChanged),
            "identity.global-member.availability.changed.v1" => {
                Some(Self::GlobalMemberAvailabilityChanged)
            }
            "identity.role-capability.summary.changed.v1" => {
                Some(Self::RoleCapabilitySummaryChanged)
            }
            "identity.role-capability.source-state.changed.v1" => {
                Some(Self::RoleCapabilitySourceStateChanged)
            }
            "identity.career.record.appended.v1" => Some(Self::CareerRecordAppended),
            "identity.career.correction.appended.v1" => Some(Self::CareerCorrectionAppended),
            "identity.memory.reference.changed.v1" => Some(Self::MemoryReferenceChanged),
            "identity.memory.archive-handoff-state.changed.v1" => {
                Some(Self::MemoryArchiveHandoffStateChanged)
            }
            _ => None,
        }
    }

    /// Creates an accepted pending outbox record with a fresh payload marker.
    pub fn build_outbox_record(
        self,
        id_generator: &dyn IdentityIdGeneratorPort,
        member_ref: GlobalMemberRef,
        subject_ref: IdentityOutboxSubjectRef,
        change_kind_ref: IdentityChangeKindRef,
        trace_record_ref: IdentityTraceRecordRef,
        created_at: IdentityTimestamp,
    ) -> Result<IdentityOutboxRecord, ApplicationError> {
        IdentityOutboxRecord::from_accepted_change(IdentityOutboxRecordCreateArgs {
            outbox_record_ref: id_generator.new_identity_outbox_record_ref()?,
            member_ref,
            subject_ref,
            change_kind_ref,
            payload_marker_ref: id_generator.new_identity_outbox_payload_marker_ref()?,
            topic_key_ref: self.topic_key_ref(),
            trace_record_ref,
            created_at,
        })
        .map_err(ApplicationError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::AcceptedOutboundMaterialKind;

    #[test]
    fn accepted_outbound_material_mapping_roundtrips_topic_to_schema() {
        for kind in AcceptedOutboundMaterialKind::all() {
            let topic = kind.topic_key_ref();
            let resolved = AcceptedOutboundMaterialKind::from_topic_key_ref(&topic)
                .expect("canonical topic should resolve");
            assert_eq!(resolved, kind);
            assert!(
                kind.event_name().as_str().ends_with("Changed")
                    || matches!(
                        kind,
                        AcceptedOutboundMaterialKind::GlobalMemberEstablished
                            | AcceptedOutboundMaterialKind::CareerRecordAppended
                            | AcceptedOutboundMaterialKind::CareerCorrectionAppended
                    )
            );
            assert!(
                kind.schema_version_ref()
                    .as_str()
                    .starts_with("identity.outbound.")
            );
        }
    }
}
