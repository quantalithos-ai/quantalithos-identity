//! Persistence-facing application ports used by command and event handlers.

use crate::domain::audit::AuditTraceEntry;
use crate::domain::capability_profile::CapabilityProfile;
use crate::domain::dead_letter::InboundDeadLetter;
use crate::domain::idempotency::{IdempotencyRecord, IdempotencyScope};
use crate::domain::member::GlobalMember;
use crate::domain::memory_refs::MemoryRefs;
use crate::domain::outbox::OutboxEvent;
use crate::domain::projection::{MemberSummaryProjection, ProjectionCheckpoint};
use crate::domain::role_catalog::RoleCatalogEntry;
use crate::domain::shared::ids::{GlobalMemberId, OutboxEventId, RoleId};
use crate::domain::shared::metadata::CommandMetadata;
use crate::domain::timeline::LifecycleHistoryEntry;
use crate::error::IdentityError;

/// Exposes write-model persistence for global member aggregates.
pub trait GlobalMemberRepository {
    /// Loads a member by id without taking a database lock.
    fn get(
        &mut self,
        global_member_id: &GlobalMemberId,
    ) -> impl std::future::Future<Output = Result<Option<GlobalMember>, IdentityError>> + Send;

    /// Loads a member by id and locks the selected row for update.
    fn get_for_update(
        &mut self,
        global_member_id: &GlobalMemberId,
    ) -> impl std::future::Future<Output = Result<Option<GlobalMember>, IdentityError>> + Send;

    /// Inserts a newly-created global member aggregate.
    fn insert(
        &mut self,
        member: &GlobalMember,
    ) -> impl std::future::Future<Output = Result<(), IdentityError>> + Send;

    /// Persists an updated member using optimistic locking on the expected version.
    fn save(
        &mut self,
        member: &GlobalMember,
        expected_version: i64,
    ) -> impl std::future::Future<Output = Result<(), IdentityError>> + Send;
}

/// Exposes local role catalog persistence required by member validation and sync flows.
pub trait RoleCatalogRepository {
    /// Loads an active role catalog entry by role id.
    fn get_active(
        &mut self,
        role_id: &RoleId,
    ) -> impl std::future::Future<Output = Result<Option<RoleCatalogEntry>, IdentityError>> + Send;

    /// Inserts or updates a role catalog entry based on its stable role id.
    fn upsert(
        &mut self,
        entry: &RoleCatalogEntry,
    ) -> impl std::future::Future<Output = Result<(), IdentityError>> + Send;
}

/// Exposes capability-profile persistence required by capability update flows.
pub trait CapabilityProfileRepository {
    /// Loads the current capability profile for the provided member, if any.
    fn get_by_member(
        &mut self,
        global_member_id: &GlobalMemberId,
    ) -> impl std::future::Future<Output = Result<Option<CapabilityProfile>, IdentityError>> + Send;

    /// Loads and locks the current capability profile for the provided member, if any.
    fn get_for_update_by_member(
        &mut self,
        global_member_id: &GlobalMemberId,
    ) -> impl std::future::Future<Output = Result<Option<CapabilityProfile>, IdentityError>> + Send;

    /// Inserts a newly-created capability profile.
    fn insert(
        &mut self,
        profile: &CapabilityProfile,
    ) -> impl std::future::Future<Output = Result<(), IdentityError>> + Send;

    /// Persists an updated capability profile using optimistic locking.
    fn save(
        &mut self,
        profile: &CapabilityProfile,
        expected_version: i64,
    ) -> impl std::future::Future<Output = Result<(), IdentityError>> + Send;
}

/// Exposes memory-refs persistence required by memory ref update flows.
pub trait MemoryRefsRepository {
    /// Loads the current memory refs aggregate for the provided member, if any.
    fn get_by_member(
        &mut self,
        global_member_id: &GlobalMemberId,
    ) -> impl std::future::Future<Output = Result<Option<MemoryRefs>, IdentityError>> + Send;

    /// Loads and locks the current memory refs aggregate for the provided member, if any.
    fn get_for_update_by_member(
        &mut self,
        global_member_id: &GlobalMemberId,
    ) -> impl std::future::Future<Output = Result<Option<MemoryRefs>, IdentityError>> + Send;

    /// Inserts a newly-created memory refs aggregate.
    fn insert(
        &mut self,
        memory_refs: &MemoryRefs,
    ) -> impl std::future::Future<Output = Result<(), IdentityError>> + Send;

    /// Persists an updated memory refs aggregate using optimistic locking.
    fn save(
        &mut self,
        memory_refs: &MemoryRefs,
        expected_version: i64,
    ) -> impl std::future::Future<Output = Result<(), IdentityError>> + Send;
}

/// Appends lifecycle history rows generated inside command transactions.
pub trait LifecycleHistoryRepository {
    /// Persists a single lifecycle history entry without overwriting existing rows.
    fn append(
        &mut self,
        entry: &LifecycleHistoryEntry,
    ) -> impl std::future::Future<Output = Result<(), IdentityError>> + Send;
}

/// Appends audit traces produced by command and event handling flows.
pub trait AuditTraceRepository {
    /// Persists a single audit trace row in append-only fashion.
    fn append(
        &mut self,
        entry: &AuditTraceEntry,
    ) -> impl std::future::Future<Output = Result<(), IdentityError>> + Send;
}

/// Provides idempotency lookup and success recording within the local transaction boundary.
pub trait IdempotencyStore {
    /// Looks up the current record, if any, for the requested key and scope.
    fn get(
        &mut self,
        idempotency_key: &str,
        scope: IdempotencyScope,
    ) -> impl std::future::Future<Output = Result<Option<IdempotencyRecord>, IdentityError>> + Send;

    /// Persists a succeeded idempotency record inside the same local transaction as business writes.
    fn record_success(
        &mut self,
        metadata: &CommandMetadata,
        scope: IdempotencyScope,
        result_ref_json: serde_json::Value,
    ) -> impl std::future::Future<Output = Result<(), IdentityError>> + Send;
}

/// Persists and scans durable outbox rows outside of direct business rule evaluation.
pub trait OutboxStore {
    /// Appends a newly-created outbox row inside the current local transaction.
    fn append(
        &mut self,
        event: &OutboxEvent,
    ) -> impl std::future::Future<Output = Result<(), IdentityError>> + Send;

    /// Lists pending outbox rows ordered for publisher processing.
    fn list_pending(
        &mut self,
        batch_size: usize,
    ) -> impl std::future::Future<Output = Result<Vec<OutboxEvent>, IdentityError>> + Send;

    /// Lists outbox rows strictly after the optional replay cursor.
    fn list_after(
        &mut self,
        last_processed_event_id: Option<&OutboxEventId>,
        batch_size: usize,
    ) -> impl std::future::Future<Output = Result<Vec<OutboxEvent>, IdentityError>> + Send;

    /// Persists an updated outbox row after publisher-side status changes.
    fn save(
        &mut self,
        event: &OutboxEvent,
    ) -> impl std::future::Future<Output = Result<(), IdentityError>> + Send;
}

/// Reads and writes the read-optimized member summary projection.
pub trait MemberSummaryProjectionRepository {
    /// Loads the current projection row for a member, if it already exists.
    fn get(
        &mut self,
        global_member_id: &GlobalMemberId,
    ) -> impl std::future::Future<Output = Result<Option<MemberSummaryProjection>, IdentityError>> + Send;

    /// Inserts or updates a member summary projection row.
    fn upsert(
        &mut self,
        projection: &MemberSummaryProjection,
    ) -> impl std::future::Future<Output = Result<(), IdentityError>> + Send;
}

/// Maintains durable rebuild progress for projection and replay workflows.
pub trait ProjectionCheckpointRepository {
    /// Loads the checkpoint row by name or creates the initial idle row when absent.
    fn get_or_create(
        &mut self,
        checkpoint_name: &str,
    ) -> impl std::future::Future<Output = Result<ProjectionCheckpoint, IdentityError>> + Send;

    /// Saves checkpoint progress or failure state after a rebuild step.
    fn save(
        &mut self,
        checkpoint: &ProjectionCheckpoint,
    ) -> impl std::future::Future<Output = Result<(), IdentityError>> + Send;
}

/// Captures and updates failed inbound events retained for replay and diagnosis.
pub trait InboundDeadLetterStore {
    /// Appends a new dead-letter row for a failed inbound event.
    fn append(
        &mut self,
        dead_letter: &InboundDeadLetter,
    ) -> impl std::future::Future<Output = Result<(), IdentityError>> + Send;

    /// Persists replay-status changes for an existing dead-letter row.
    fn save(
        &mut self,
        dead_letter: &InboundDeadLetter,
    ) -> impl std::future::Future<Output = Result<(), IdentityError>> + Send;
}

/// Wraps the local transaction that must atomically commit write model, history, audit, and idempotency data.
pub trait UnitOfWork {
    /// Global member repository bound to the current transaction.
    type GlobalMembers<'a>: GlobalMemberRepository
    where
        Self: 'a;

    /// Role catalog repository bound to the current transaction.
    type RoleCatalog<'a>: RoleCatalogRepository
    where
        Self: 'a;

    /// Capability-profile repository bound to the current transaction.
    type CapabilityProfiles<'a>: CapabilityProfileRepository
    where
        Self: 'a;

    /// Memory-refs repository bound to the current transaction.
    type MemoryRefs<'a>: MemoryRefsRepository
    where
        Self: 'a;

    /// Lifecycle history repository bound to the current transaction.
    type LifecycleHistory<'a>: LifecycleHistoryRepository
    where
        Self: 'a;

    /// Audit trace repository bound to the current transaction.
    type AuditTraces<'a>: AuditTraceRepository
    where
        Self: 'a;

    /// Idempotency store bound to the current transaction.
    type Idempotency<'a>: IdempotencyStore
    where
        Self: 'a;

    /// Outbox store bound to the current transaction.
    type Outbox<'a>: OutboxStore
    where
        Self: 'a;

    /// Member summary projection repository bound to the current transaction.
    type MemberSummaryProjection<'a>: MemberSummaryProjectionRepository
    where
        Self: 'a;

    /// Projection checkpoint repository bound to the current transaction.
    type ProjectionCheckpoints<'a>: ProjectionCheckpointRepository
    where
        Self: 'a;

    /// Inbound dead-letter store bound to the current transaction.
    type InboundDeadLetters<'a>: InboundDeadLetterStore
    where
        Self: 'a;

    /// Returns a repository handle for global member persistence.
    fn global_members(&mut self) -> Self::GlobalMembers<'_>;

    /// Returns a repository handle for local role catalog persistence.
    fn role_catalog(&mut self) -> Self::RoleCatalog<'_>;

    /// Returns a repository handle for capability-profile persistence.
    fn capability_profiles(&mut self) -> Self::CapabilityProfiles<'_>;

    /// Returns a repository handle for memory-refs persistence.
    fn memory_refs(&mut self) -> Self::MemoryRefs<'_>;

    /// Returns a repository handle for lifecycle history writes.
    fn lifecycle_history(&mut self) -> Self::LifecycleHistory<'_>;

    /// Returns a repository handle for audit trace writes.
    fn audit_traces(&mut self) -> Self::AuditTraces<'_>;

    /// Returns a repository handle for idempotency reads and writes.
    fn idempotency(&mut self) -> Self::Idempotency<'_>;

    /// Returns a store handle for outbox persistence and scanning.
    fn outbox(&mut self) -> Self::Outbox<'_>;

    /// Returns a repository handle for member summary projections.
    fn member_summary_projection(&mut self) -> Self::MemberSummaryProjection<'_>;

    /// Returns a repository handle for projection checkpoints.
    fn projection_checkpoints(&mut self) -> Self::ProjectionCheckpoints<'_>;

    /// Returns a store handle for inbound dead-letter persistence.
    fn inbound_dead_letters(&mut self) -> Self::InboundDeadLetters<'_>;

    /// Commits the current local transaction.
    fn commit(self) -> impl std::future::Future<Output = Result<(), IdentityError>> + Send;

    /// Rolls back the current local transaction.
    fn rollback(self) -> impl std::future::Future<Output = Result<(), IdentityError>> + Send;
}

/// Creates transaction-scoped units of work over the shared persistence backend.
pub trait UnitOfWorkFactory {
    /// Concrete unit-of-work type produced by the factory.
    type UnitOfWork<'a>: UnitOfWork
    where
        Self: 'a;

    /// Begins a new local transaction and returns a unit of work bound to it.
    fn begin(
        &self,
    ) -> impl std::future::Future<Output = Result<Self::UnitOfWork<'_>, IdentityError>> + Send;
}
