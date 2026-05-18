//! Outbound ports and adapter placeholders.

use crate::domain::capability_profile::ArtifactRef;
use crate::domain::member::GlobalMember;
use crate::domain::memory_refs::{ArchiveRef, MemoryRef};
use crate::domain::outbox::OutboxEvent;
use crate::domain::shared::context::ActorContext;
use crate::domain::shared::ids::GlobalMemberId;
use crate::domain::tombstone::GateDecisionRef;
use crate::error::IdentityError;
use crate::inbound::events::RoleDefinitionSnapshot;

/// Validates external artifact refs without copying artifact bodies into identity.
pub trait ArtifactPort {
    /// Validates that every evidence ref exists and is safe to retain as a ref-only pointer.
    fn validate_refs(
        &self,
        refs: &[ArtifactRef],
    ) -> impl std::future::Future<Output = Result<(), IdentityError>> + Send;
}

/// Validates external memory refs without copying memory bodies into identity.
pub trait MemoryArchivePort {
    /// Validates that one memory ref exists and is safe to retain as a ref-only pointer.
    fn validate_ref(
        &self,
        memory_ref: &MemoryRef,
    ) -> impl std::future::Future<Output = Result<(), IdentityError>> + Send;
}

/// Validates or retrieves governance evidence for a protected identity action.
pub trait GovernancePort {
    /// Requires governance evidence for the requested protected action.
    fn require_gate_decision(
        &self,
        action_name: &str,
        member: &GlobalMember,
        actor: &ActorContext,
        reason: &str,
        supplied_gate_ref: Option<&GateDecisionRef>,
    ) -> impl std::future::Future<Output = Result<GateDecisionRef, IdentityError>> + Send;
}

/// Requests archive collaboration without copying archive bodies into identity.
pub trait ArchiveRequestPort {
    /// Requests an archive operation and returns the resulting archive ref snapshot.
    fn request_archive(
        &self,
        global_member_id: &GlobalMemberId,
        reason: &str,
    ) -> impl std::future::Future<Output = Result<ArchiveRef, IdentityError>> + Send;
}

/// Publishes durable outbox records to the external L0-bus.
pub trait BusPublisherPort {
    /// Publishes a single outbox event to the external bus.
    fn publish(
        &self,
        event: &OutboxEvent,
    ) -> impl std::future::Future<Output = Result<(), IdentityError>> + Send;
}

/// Reads the authoritative method-library role catalog for reconciliation jobs.
pub trait MethodLibraryRoleCatalogPort {
    /// Lists the authoritative role snapshots that identity may index locally.
    fn list_role_catalog(
        &self,
    ) -> impl std::future::Future<Output = Result<Vec<RoleDefinitionSnapshot>, IdentityError>> + Send;
}
