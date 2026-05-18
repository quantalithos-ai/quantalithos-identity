//! Persistence-facing application ports used by command and event handlers.

use crate::domain::audit::AuditTraceEntry;
use crate::domain::idempotency::{IdempotencyRecord, IdempotencyScope};
use crate::domain::member::GlobalMember;
use crate::domain::role_catalog::RoleCatalogEntry;
use crate::domain::shared::ids::{GlobalMemberId, RoleId};
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

    /// Returns a repository handle for global member persistence.
    fn global_members(&mut self) -> Self::GlobalMembers<'_>;

    /// Returns a repository handle for local role catalog persistence.
    fn role_catalog(&mut self) -> Self::RoleCatalog<'_>;

    /// Returns a repository handle for lifecycle history writes.
    fn lifecycle_history(&mut self) -> Self::LifecycleHistory<'_>;

    /// Returns a repository handle for audit trace writes.
    fn audit_traces(&mut self) -> Self::AuditTraces<'_>;

    /// Returns a repository handle for idempotency reads and writes.
    fn idempotency(&mut self) -> Self::Idempotency<'_>;

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
