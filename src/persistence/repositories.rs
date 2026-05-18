//! SQLx-backed repository implementations bound to a shared transaction.

use serde_json::Value;
use sqlx::{Postgres, Row, Transaction};
use time::{OffsetDateTime, PrimitiveDateTime};

use crate::application::persistence::{
    AuditTraceRepository, CapabilityProfileRepository, CareerHistoryRepository,
    GlobalMemberRepository, IdempotencyStore, InboundDeadLetterStore, LifecycleHistoryRepository,
    MemberSummaryProjectionRepository, MemoryRefsRepository, OutboxStore,
    ProjectionCheckpointRepository, RoleCatalogRepository,
};
use crate::domain::audit::{AuditResult, AuditTraceEntry};
use crate::domain::capability_profile::{ArtifactRef, CapabilityItem, CapabilityProfile};
use crate::domain::career_history::{CareerEntry, CareerHistory, ProcessRef, WorkRef};
use crate::domain::dead_letter::{DeadLetterReplayStatus, InboundDeadLetter};
use crate::domain::idempotency::{IdempotencyRecord, IdempotencyScope, IdempotencyStatus};
use crate::domain::member::{GlobalMember, GlobalMemberLifecycle};
use crate::domain::memory_refs::{ArchiveRef, ArchiveStatus, MemoryRef, MemoryRefs};
use crate::domain::outbox::{OutboxEvent, OutboxStatus};
use crate::domain::projection::{
    MemberSummaryProjection, ProjectionCheckpoint, ProjectionCheckpointStatus,
};
use crate::domain::role_catalog::{RoleCatalogEntry, RoleCatalogStatus};
use crate::domain::shared::context::ActorContext;
use crate::domain::shared::ids::{
    CapabilityProfileId, CareerEntryId, EventId, GlobalMemberId, MemoryRefsId, OutboxEventId,
    ProjectId, RoleId,
};
use crate::domain::shared::metadata::CommandMetadata;
use crate::domain::shared::pagination::NormalizedPageRequest;
use crate::domain::timeline::{LifecycleEventType, LifecycleHistoryEntry};
use crate::error::IdentityError;

/// Global member repository bound to an open SQL transaction.
pub struct SqlxGlobalMemberRepository<'tx, 'db> {
    transaction: &'tx mut Transaction<'db, Postgres>,
}

impl<'tx, 'db> SqlxGlobalMemberRepository<'tx, 'db> {
    /// Creates a repository facade over the provided SQL transaction.
    pub fn new(transaction: &'tx mut Transaction<'db, Postgres>) -> Self {
        Self { transaction }
    }
}

impl GlobalMemberRepository for SqlxGlobalMemberRepository<'_, '_> {
    async fn get(
        &mut self,
        global_member_id: &GlobalMemberId,
    ) -> Result<Option<GlobalMember>, IdentityError> {
        let row = sqlx::query(
            r#"
            SELECT
                global_member_id,
                display_name,
                lifecycle,
                main_role_id,
                secondary_role_ids_json,
                capability_profile_id,
                memory_refs_id,
                version,
                created_by_json,
                created_at,
                updated_at
            FROM global_members
            WHERE global_member_id = $1
            "#,
        )
        .bind(global_member_id.as_str())
        .fetch_optional(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        row.map(map_global_member_row).transpose()
    }

    async fn get_for_update(
        &mut self,
        global_member_id: &GlobalMemberId,
    ) -> Result<Option<GlobalMember>, IdentityError> {
        let row = sqlx::query(
            r#"
            SELECT
                global_member_id,
                display_name,
                lifecycle,
                main_role_id,
                secondary_role_ids_json,
                capability_profile_id,
                memory_refs_id,
                version,
                created_by_json,
                created_at,
                updated_at
            FROM global_members
            WHERE global_member_id = $1
            FOR UPDATE
            "#,
        )
        .bind(global_member_id.as_str())
        .fetch_optional(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        row.map(map_global_member_row).transpose()
    }

    async fn insert(&mut self, member: &GlobalMember) -> Result<(), IdentityError> {
        let secondary_role_ids_json = serde_json::to_value(
            member
                .secondary_role_ids
                .iter()
                .map(RoleId::as_str)
                .collect::<Vec<_>>(),
        )
        .map_err(|error| IdentityError::PersistenceData {
            message: format!("serialize secondary role ids: {error}"),
        })?;
        let created_by_json = serde_json::to_value(&member.created_by).map_err(|error| {
            IdentityError::PersistenceData {
                message: format!("serialize created_by actor context: {error}"),
            }
        })?;

        sqlx::query(
            r#"
            INSERT INTO global_members (
                global_member_id,
                display_name,
                lifecycle,
                main_role_id,
                secondary_role_ids_json,
                capability_profile_id,
                memory_refs_id,
                version,
                created_by_json,
                created_at,
                updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
            "#,
        )
        .bind(member.global_member_id.as_str())
        .bind(member.display_name.as_str())
        .bind(member.lifecycle.as_db())
        .bind(member.main_role_id.as_str())
        .bind(secondary_role_ids_json)
        .bind(
            member
                .capability_profile_id
                .as_ref()
                .map(|value| value.as_str()),
        )
        .bind(member.memory_refs_id.as_ref().map(|value| value.as_str()))
        .bind(member.version)
        .bind(created_by_json)
        .bind(member.created_at)
        .bind(member.updated_at)
        .execute(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        Ok(())
    }

    async fn save(
        &mut self,
        member: &GlobalMember,
        expected_version: i64,
    ) -> Result<(), IdentityError> {
        let secondary_role_ids_json = serde_json::to_value(
            member
                .secondary_role_ids
                .iter()
                .map(RoleId::as_str)
                .collect::<Vec<_>>(),
        )
        .map_err(|error| IdentityError::PersistenceData {
            message: format!("serialize secondary role ids: {error}"),
        })?;
        let created_by_json = serde_json::to_value(&member.created_by).map_err(|error| {
            IdentityError::PersistenceData {
                message: format!("serialize created_by actor context: {error}"),
            }
        })?;

        let result = sqlx::query(
            r#"
            UPDATE global_members
            SET
                display_name = $2,
                lifecycle = $3,
                main_role_id = $4,
                secondary_role_ids_json = $5,
                capability_profile_id = $6,
                memory_refs_id = $7,
                version = $8,
                created_by_json = $9,
                created_at = $10,
                updated_at = $11
            WHERE global_member_id = $1
              AND version = $12
            "#,
        )
        .bind(member.global_member_id.as_str())
        .bind(member.display_name.as_str())
        .bind(member.lifecycle.as_db())
        .bind(member.main_role_id.as_str())
        .bind(secondary_role_ids_json)
        .bind(
            member
                .capability_profile_id
                .as_ref()
                .map(|value| value.as_str()),
        )
        .bind(member.memory_refs_id.as_ref().map(|value| value.as_str()))
        .bind(member.version)
        .bind(created_by_json)
        .bind(member.created_at)
        .bind(member.updated_at)
        .bind(expected_version)
        .execute(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        if result.rows_affected() == 0 {
            return Err(IdentityError::VersionConflict {
                entity: "global_member".to_string(),
            });
        }

        Ok(())
    }
}

/// Role catalog repository bound to an open SQL transaction.
pub struct SqlxRoleCatalogRepository<'tx, 'db> {
    transaction: &'tx mut Transaction<'db, Postgres>,
}

impl<'tx, 'db> SqlxRoleCatalogRepository<'tx, 'db> {
    /// Creates a repository facade over the provided SQL transaction.
    pub fn new(transaction: &'tx mut Transaction<'db, Postgres>) -> Self {
        Self { transaction }
    }
}

impl RoleCatalogRepository for SqlxRoleCatalogRepository<'_, '_> {
    async fn get_active(
        &mut self,
        role_id: &RoleId,
    ) -> Result<Option<RoleCatalogEntry>, IdentityError> {
        let row = sqlx::query(
            r#"
            SELECT
                role_id,
                role_name,
                role_version,
                source_ref_json,
                fingerprint,
                status,
                updated_at
            FROM role_catalog_entries
            WHERE role_id = $1
              AND status = 'active'
            "#,
        )
        .bind(role_id.as_str())
        .fetch_optional(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        row.map(map_role_catalog_row).transpose()
    }

    async fn list_all(&mut self) -> Result<Vec<RoleCatalogEntry>, IdentityError> {
        let rows = sqlx::query(
            r#"
            SELECT
                role_id,
                role_name,
                role_version,
                source_ref_json,
                fingerprint,
                status,
                updated_at
            FROM role_catalog_entries
            ORDER BY role_id
            "#,
        )
        .fetch_all(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        rows.into_iter().map(map_role_catalog_row).collect()
    }

    async fn upsert(&mut self, entry: &RoleCatalogEntry) -> Result<(), IdentityError> {
        sqlx::query(
            r#"
            INSERT INTO role_catalog_entries (
                role_id,
                role_name,
                role_version,
                source_ref_json,
                fingerprint,
                status,
                updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (role_id) DO UPDATE
            SET
                role_name = EXCLUDED.role_name,
                role_version = EXCLUDED.role_version,
                source_ref_json = EXCLUDED.source_ref_json,
                fingerprint = EXCLUDED.fingerprint,
                status = EXCLUDED.status,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(entry.role_id.as_str())
        .bind(entry.role_name.as_str())
        .bind(entry.role_version.as_str())
        .bind(entry.source_ref_json.clone())
        .bind(entry.fingerprint.as_str())
        .bind(entry.status.as_db())
        .bind(entry.updated_at)
        .execute(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        Ok(())
    }
}

/// Capability-profile repository bound to an open SQL transaction.
pub struct SqlxCapabilityProfileRepository<'tx, 'db> {
    transaction: &'tx mut Transaction<'db, Postgres>,
}

impl<'tx, 'db> SqlxCapabilityProfileRepository<'tx, 'db> {
    /// Creates a repository facade over the provided SQL transaction.
    pub fn new(transaction: &'tx mut Transaction<'db, Postgres>) -> Self {
        Self { transaction }
    }
}

impl CapabilityProfileRepository for SqlxCapabilityProfileRepository<'_, '_> {
    async fn get_by_member(
        &mut self,
        global_member_id: &GlobalMemberId,
    ) -> Result<Option<CapabilityProfile>, IdentityError> {
        let row = sqlx::query(
            r#"
            SELECT
                capability_profile_id,
                global_member_id,
                capabilities_json,
                evidence_refs_json,
                version,
                updated_at
            FROM capability_profiles
            WHERE global_member_id = $1
            "#,
        )
        .bind(global_member_id.as_str())
        .fetch_optional(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        row.map(map_capability_profile_row).transpose()
    }

    async fn get_for_update_by_member(
        &mut self,
        global_member_id: &GlobalMemberId,
    ) -> Result<Option<CapabilityProfile>, IdentityError> {
        let row = sqlx::query(
            r#"
            SELECT
                capability_profile_id,
                global_member_id,
                capabilities_json,
                evidence_refs_json,
                version,
                updated_at
            FROM capability_profiles
            WHERE global_member_id = $1
            FOR UPDATE
            "#,
        )
        .bind(global_member_id.as_str())
        .fetch_optional(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        row.map(map_capability_profile_row).transpose()
    }

    async fn insert(&mut self, profile: &CapabilityProfile) -> Result<(), IdentityError> {
        let capabilities_json = serde_json::to_value(&profile.capabilities).map_err(|error| {
            IdentityError::PersistenceData {
                message: format!("serialize capability items: {error}"),
            }
        })?;
        let evidence_refs_json = serde_json::to_value(&profile.evidence_refs).map_err(|error| {
            IdentityError::PersistenceData {
                message: format!("serialize capability evidence refs: {error}"),
            }
        })?;

        sqlx::query(
            r#"
            INSERT INTO capability_profiles (
                capability_profile_id,
                global_member_id,
                capabilities_json,
                evidence_refs_json,
                version,
                updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(profile.capability_profile_id.as_str())
        .bind(profile.global_member_id.as_str())
        .bind(capabilities_json)
        .bind(evidence_refs_json)
        .bind(profile.version)
        .bind(profile.updated_at)
        .execute(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        Ok(())
    }

    async fn save(
        &mut self,
        profile: &CapabilityProfile,
        expected_version: i64,
    ) -> Result<(), IdentityError> {
        let capabilities_json = serde_json::to_value(&profile.capabilities).map_err(|error| {
            IdentityError::PersistenceData {
                message: format!("serialize capability items: {error}"),
            }
        })?;
        let evidence_refs_json = serde_json::to_value(&profile.evidence_refs).map_err(|error| {
            IdentityError::PersistenceData {
                message: format!("serialize capability evidence refs: {error}"),
            }
        })?;

        let result = sqlx::query(
            r#"
            UPDATE capability_profiles
            SET
                capabilities_json = $2,
                evidence_refs_json = $3,
                version = $4,
                updated_at = $5
            WHERE capability_profile_id = $1
              AND version = $6
            "#,
        )
        .bind(profile.capability_profile_id.as_str())
        .bind(capabilities_json)
        .bind(evidence_refs_json)
        .bind(profile.version)
        .bind(profile.updated_at)
        .bind(expected_version)
        .execute(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        if result.rows_affected() == 0 {
            return Err(IdentityError::VersionConflict {
                entity: "capability_profile".to_string(),
            });
        }

        Ok(())
    }
}

/// Memory-refs repository bound to an open SQL transaction.
pub struct SqlxMemoryRefsRepository<'tx, 'db> {
    transaction: &'tx mut Transaction<'db, Postgres>,
}

impl<'tx, 'db> SqlxMemoryRefsRepository<'tx, 'db> {
    /// Creates a repository facade over the provided SQL transaction.
    pub fn new(transaction: &'tx mut Transaction<'db, Postgres>) -> Self {
        Self { transaction }
    }
}

impl MemoryRefsRepository for SqlxMemoryRefsRepository<'_, '_> {
    async fn get_by_member(
        &mut self,
        global_member_id: &GlobalMemberId,
    ) -> Result<Option<MemoryRefs>, IdentityError> {
        let row = sqlx::query(
            r#"
            SELECT
                memory_refs_id,
                global_member_id,
                semantic_memory_ref_json,
                episodic_memory_refs_json,
                archive_ref_json,
                archive_status,
                version,
                updated_at
            FROM memory_refs
            WHERE global_member_id = $1
            "#,
        )
        .bind(global_member_id.as_str())
        .fetch_optional(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        row.map(map_memory_refs_row).transpose()
    }

    async fn get_for_update_by_member(
        &mut self,
        global_member_id: &GlobalMemberId,
    ) -> Result<Option<MemoryRefs>, IdentityError> {
        let row = sqlx::query(
            r#"
            SELECT
                memory_refs_id,
                global_member_id,
                semantic_memory_ref_json,
                episodic_memory_refs_json,
                archive_ref_json,
                archive_status,
                version,
                updated_at
            FROM memory_refs
            WHERE global_member_id = $1
            FOR UPDATE
            "#,
        )
        .bind(global_member_id.as_str())
        .fetch_optional(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        row.map(map_memory_refs_row).transpose()
    }

    async fn insert(&mut self, memory_refs: &MemoryRefs) -> Result<(), IdentityError> {
        let semantic_memory_ref_json = memory_refs
            .semantic_memory_ref
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|error| IdentityError::PersistenceData {
                message: format!("serialize semantic memory ref: {error}"),
            })?;
        let episodic_memory_refs_json = serde_json::to_value(&memory_refs.episodic_memory_refs)
            .map_err(|error| IdentityError::PersistenceData {
                message: format!("serialize episodic memory refs: {error}"),
            })?;
        let archive_ref_json = memory_refs
            .archive_ref
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|error| IdentityError::PersistenceData {
                message: format!("serialize archive ref: {error}"),
            })?;

        sqlx::query(
            r#"
            INSERT INTO memory_refs (
                memory_refs_id,
                global_member_id,
                semantic_memory_ref_json,
                episodic_memory_refs_json,
                archive_ref_json,
                archive_status,
                version,
                updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(memory_refs.memory_refs_id.as_str())
        .bind(memory_refs.global_member_id.as_str())
        .bind(semantic_memory_ref_json)
        .bind(episodic_memory_refs_json)
        .bind(archive_ref_json)
        .bind(memory_refs.archive_status.as_db())
        .bind(memory_refs.version)
        .bind(memory_refs.updated_at)
        .execute(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        Ok(())
    }

    async fn save(
        &mut self,
        memory_refs: &MemoryRefs,
        expected_version: i64,
    ) -> Result<(), IdentityError> {
        let semantic_memory_ref_json = memory_refs
            .semantic_memory_ref
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|error| IdentityError::PersistenceData {
                message: format!("serialize semantic memory ref: {error}"),
            })?;
        let episodic_memory_refs_json = serde_json::to_value(&memory_refs.episodic_memory_refs)
            .map_err(|error| IdentityError::PersistenceData {
                message: format!("serialize episodic memory refs: {error}"),
            })?;
        let archive_ref_json = memory_refs
            .archive_ref
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|error| IdentityError::PersistenceData {
                message: format!("serialize archive ref: {error}"),
            })?;

        let result = sqlx::query(
            r#"
            UPDATE memory_refs
            SET
                semantic_memory_ref_json = $2,
                episodic_memory_refs_json = $3,
                archive_ref_json = $4,
                archive_status = $5,
                version = $6,
                updated_at = $7
            WHERE memory_refs_id = $1
              AND version = $8
            "#,
        )
        .bind(memory_refs.memory_refs_id.as_str())
        .bind(semantic_memory_ref_json)
        .bind(episodic_memory_refs_json)
        .bind(archive_ref_json)
        .bind(memory_refs.archive_status.as_db())
        .bind(memory_refs.version)
        .bind(memory_refs.updated_at)
        .bind(expected_version)
        .execute(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        if result.rows_affected() == 0 {
            return Err(IdentityError::VersionConflict {
                entity: "memory_refs".to_string(),
            });
        }

        Ok(())
    }
}

/// Career-history repository bound to an open SQL transaction.
pub struct SqlxCareerHistoryRepository<'tx, 'db> {
    transaction: &'tx mut Transaction<'db, Postgres>,
}

impl<'tx, 'db> SqlxCareerHistoryRepository<'tx, 'db> {
    /// Creates a repository facade over the provided SQL transaction.
    pub fn new(transaction: &'tx mut Transaction<'db, Postgres>) -> Self {
        Self { transaction }
    }
}

impl CareerHistoryRepository for SqlxCareerHistoryRepository<'_, '_> {
    async fn get_for_update(
        &mut self,
        global_member_id: &GlobalMemberId,
    ) -> Result<CareerHistory, IdentityError> {
        let rows = sqlx::query(
            r#"
            SELECT
                career_entry_id,
                global_member_id,
                source_event_id,
                source_module,
                project_id,
                work_ref_json,
                process_ref_json,
                entry_kind,
                started_at,
                ended_at,
                payload_summary_json,
                created_at
            FROM career_history_entries
            WHERE global_member_id = $1
            ORDER BY created_at ASC, career_entry_id ASC
            FOR UPDATE
            "#,
        )
        .bind(global_member_id.as_str())
        .fetch_all(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        let entries = rows
            .into_iter()
            .map(map_career_entry_row)
            .collect::<Result<Vec<_>, _>>()?;

        CareerHistory::rehydrate(global_member_id.clone(), entries)
    }

    async fn save(&mut self, history: &CareerHistory) -> Result<(), IdentityError> {
        for entry in &history.entries {
            let work_ref_json = entry
                .work_ref
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
                .map_err(|error| IdentityError::PersistenceData {
                    message: format!("serialize career work ref: {error}"),
                })?;
            let process_ref_json = entry
                .process_ref
                .as_ref()
                .map(serde_json::to_value)
                .transpose()
                .map_err(|error| IdentityError::PersistenceData {
                    message: format!("serialize career process ref: {error}"),
                })?;

            sqlx::query(
                r#"
                INSERT INTO career_history_entries (
                    career_entry_id,
                    global_member_id,
                    source_event_id,
                    source_module,
                    project_id,
                    work_ref_json,
                    process_ref_json,
                    entry_kind,
                    started_at,
                    ended_at,
                    payload_summary_json,
                    created_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                ON CONFLICT (career_entry_id) DO NOTHING
                "#,
            )
            .bind(entry.career_entry_id.as_str())
            .bind(entry.global_member_id.as_str())
            .bind(entry.source_event_id.as_str())
            .bind(entry.source_module.as_str())
            .bind(entry.project_id.as_ref().map(ProjectId::as_str))
            .bind(work_ref_json)
            .bind(process_ref_json)
            .bind(entry.entry_kind.as_str())
            .bind(entry.started_at)
            .bind(entry.ended_at)
            .bind(entry.payload_summary.clone())
            .bind(entry.created_at)
            .execute(self.transaction.as_mut())
            .await
            .map_err(IdentityError::DatabasePool)?;
        }

        Ok(())
    }
}

/// Lifecycle history repository bound to an open SQL transaction.
pub struct SqlxLifecycleHistoryRepository<'tx, 'db> {
    transaction: &'tx mut Transaction<'db, Postgres>,
}

impl<'tx, 'db> SqlxLifecycleHistoryRepository<'tx, 'db> {
    /// Creates a repository facade over the provided SQL transaction.
    pub fn new(transaction: &'tx mut Transaction<'db, Postgres>) -> Self {
        Self { transaction }
    }
}

impl LifecycleHistoryRepository for SqlxLifecycleHistoryRepository<'_, '_> {
    async fn append(&mut self, entry: &LifecycleHistoryEntry) -> Result<(), IdentityError> {
        let actor_json =
            serde_json::to_value(&entry.actor).map_err(|error| IdentityError::PersistenceData {
                message: format!("serialize lifecycle actor context: {error}"),
            })?;
        let metadata_json = serde_json::to_value(&entry.metadata).map_err(|error| {
            IdentityError::PersistenceData {
                message: format!("serialize lifecycle command metadata: {error}"),
            }
        })?;

        sqlx::query(
            r#"
            INSERT INTO lifecycle_history_entries (
                history_entry_id,
                global_member_id,
                event_type,
                from_lifecycle,
                to_lifecycle,
                actor_json,
                gate_decision_ref_json,
                metadata_json,
                created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#,
        )
        .bind(entry.history_entry_id.as_str())
        .bind(entry.global_member_id.as_str())
        .bind(entry.event_type.as_db())
        .bind(entry.from_lifecycle.map(GlobalMemberLifecycle::as_db))
        .bind(entry.to_lifecycle.as_db())
        .bind(actor_json)
        .bind(entry.gate_decision_ref_json.clone())
        .bind(metadata_json)
        .bind(entry.created_at)
        .execute(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        Ok(())
    }
}

/// Audit trace repository bound to an open SQL transaction.
pub struct SqlxAuditTraceRepository<'tx, 'db> {
    transaction: &'tx mut Transaction<'db, Postgres>,
}

impl<'tx, 'db> SqlxAuditTraceRepository<'tx, 'db> {
    /// Creates a repository facade over the provided SQL transaction.
    pub fn new(transaction: &'tx mut Transaction<'db, Postgres>) -> Self {
        Self { transaction }
    }
}

impl AuditTraceRepository for SqlxAuditTraceRepository<'_, '_> {
    async fn append(&mut self, entry: &AuditTraceEntry) -> Result<(), IdentityError> {
        sqlx::query(
            r#"
            INSERT INTO audit_trace_entries (
                audit_trace_id,
                trace_id,
                action,
                actor_json,
                target_ref_json,
                source_module,
                result,
                reason,
                created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#,
        )
        .bind(entry.audit_trace_id.as_str())
        .bind(entry.trace_id.as_str())
        .bind(entry.action.as_str())
        .bind(entry.actor_json.clone())
        .bind(entry.target_ref_json.clone())
        .bind(entry.source_module.as_deref())
        .bind(entry.result.as_db())
        .bind(entry.reason.as_deref())
        .bind(entry.created_at)
        .execute(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        Ok(())
    }

    async fn list_by_member(
        &mut self,
        global_member_id: &GlobalMemberId,
        page: &NormalizedPageRequest,
    ) -> Result<Vec<AuditTraceEntry>, IdentityError> {
        let limit = i64::from(page.limit);

        let rows = if let Some(cursor_audit_trace_id) = page.cursor.as_deref() {
            let cursor_row = sqlx::query(
                r#"
                SELECT created_at, audit_trace_id
                FROM audit_trace_entries
                WHERE audit_trace_id = $1
                "#,
            )
            .bind(cursor_audit_trace_id)
            .fetch_optional(self.transaction.as_mut())
            .await
            .map_err(IdentityError::DatabasePool)?;

            let Some(cursor_row) = cursor_row else {
                return Ok(Vec::new());
            };
            let cursor_created_at: PrimitiveDateTime = cursor_row.get("created_at");
            let cursor_audit_trace_id: String = cursor_row.get("audit_trace_id");

            sqlx::query(
                r#"
                SELECT
                    audit_trace_id,
                    trace_id,
                    action,
                    actor_json,
                    target_ref_json,
                    source_module,
                    result,
                    reason,
                    created_at
                FROM audit_trace_entries
                WHERE (
                    target_ref_json ->> 'global_member_id' = $1
                    OR (
                        target_ref_json ->> 'kind' = 'global_member'
                        AND target_ref_json ->> 'id' = $1
                    )
                )
                  AND (
                    created_at < $2
                    OR (created_at = $2 AND audit_trace_id < $3)
                  )
                ORDER BY created_at DESC, audit_trace_id DESC
                LIMIT $4
                "#,
            )
            .bind(global_member_id.as_str())
            .bind(cursor_created_at)
            .bind(cursor_audit_trace_id)
            .bind(limit)
            .fetch_all(self.transaction.as_mut())
            .await
            .map_err(IdentityError::DatabasePool)?
        } else {
            sqlx::query(
                r#"
                SELECT
                    audit_trace_id,
                    trace_id,
                    action,
                    actor_json,
                    target_ref_json,
                    source_module,
                    result,
                    reason,
                    created_at
                FROM audit_trace_entries
                WHERE (
                    target_ref_json ->> 'global_member_id' = $1
                    OR (
                        target_ref_json ->> 'kind' = 'global_member'
                        AND target_ref_json ->> 'id' = $1
                    )
                )
                ORDER BY created_at DESC, audit_trace_id DESC
                LIMIT $2
                "#,
            )
            .bind(global_member_id.as_str())
            .bind(limit)
            .fetch_all(self.transaction.as_mut())
            .await
            .map_err(IdentityError::DatabasePool)?
        };

        rows.into_iter().map(map_audit_trace_row).collect()
    }
}

/// Idempotency store bound to an open SQL transaction.
pub struct SqlxIdempotencyStore<'tx, 'db> {
    transaction: &'tx mut Transaction<'db, Postgres>,
}

impl<'tx, 'db> SqlxIdempotencyStore<'tx, 'db> {
    /// Creates a store facade over the provided SQL transaction.
    pub fn new(transaction: &'tx mut Transaction<'db, Postgres>) -> Self {
        Self { transaction }
    }
}

impl IdempotencyStore for SqlxIdempotencyStore<'_, '_> {
    async fn get(
        &mut self,
        idempotency_key: &str,
        scope: IdempotencyScope,
    ) -> Result<Option<IdempotencyRecord>, IdentityError> {
        let row = sqlx::query(
            r#"
            SELECT
                idempotency_key,
                scope,
                request_hash,
                result_ref_json,
                status,
                created_at,
                updated_at
            FROM idempotency_records
            WHERE idempotency_key = $1
              AND scope = $2
            "#,
        )
        .bind(idempotency_key)
        .bind(scope.as_db())
        .fetch_optional(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        row.map(map_idempotency_row).transpose()
    }

    async fn delete(
        &mut self,
        idempotency_key: &str,
        scope: IdempotencyScope,
    ) -> Result<bool, IdentityError> {
        let result = sqlx::query(
            r#"
            DELETE FROM idempotency_records
            WHERE idempotency_key = $1
              AND scope = $2
            "#,
        )
        .bind(idempotency_key)
        .bind(scope.as_db())
        .execute(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        Ok(result.rows_affected() == 1)
    }

    async fn record_success(
        &mut self,
        metadata: &CommandMetadata,
        scope: IdempotencyScope,
        result_ref_json: Value,
    ) -> Result<(), IdentityError> {
        let now = OffsetDateTime::now_utc().replace_offset(time::UtcOffset::UTC);
        let now = PrimitiveDateTime::new(now.date(), now.time());

        sqlx::query(
            r#"
            INSERT INTO idempotency_records (
                idempotency_key,
                scope,
                request_hash,
                result_ref_json,
                status,
                created_at,
                updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (idempotency_key) DO UPDATE
            SET
                scope = EXCLUDED.scope,
                request_hash = EXCLUDED.request_hash,
                result_ref_json = EXCLUDED.result_ref_json,
                status = EXCLUDED.status,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(metadata.idempotency_key())
        .bind(scope.as_db())
        .bind(metadata.request_hash())
        .bind(result_ref_json)
        .bind(IdempotencyStatus::Succeeded.as_db())
        .bind(now)
        .bind(now)
        .execute(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        Ok(())
    }
}

/// Outbox store bound to an open SQL transaction.
pub struct SqlxOutboxStore<'tx, 'db> {
    transaction: &'tx mut Transaction<'db, Postgres>,
}

impl<'tx, 'db> SqlxOutboxStore<'tx, 'db> {
    /// Creates a store facade over the provided SQL transaction.
    pub fn new(transaction: &'tx mut Transaction<'db, Postgres>) -> Self {
        Self { transaction }
    }
}

impl OutboxStore for SqlxOutboxStore<'_, '_> {
    async fn append(&mut self, event: &OutboxEvent) -> Result<(), IdentityError> {
        sqlx::query(
            r#"
            INSERT INTO outbox_events (
                outbox_event_id,
                aggregate_type,
                aggregate_id,
                event_type,
                payload_json,
                idempotency_key,
                status,
                retry_count,
                next_retry_at,
                created_at,
                published_at,
                failure_reason
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            "#,
        )
        .bind(event.outbox_event_id.as_str())
        .bind(event.aggregate_type.as_str())
        .bind(event.aggregate_id.as_str())
        .bind(event.event_type.as_str())
        .bind(event.payload_json.clone())
        .bind(event.idempotency_key.as_str())
        .bind(event.status.as_db())
        .bind(event.retry_count)
        .bind(event.next_retry_at)
        .bind(event.created_at)
        .bind(event.published_at)
        .bind(event.failure_reason.as_deref())
        .execute(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        Ok(())
    }

    async fn list_pending(&mut self, batch_size: usize) -> Result<Vec<OutboxEvent>, IdentityError> {
        let now = current_timestamp();
        let rows = sqlx::query(
            r#"
            SELECT
                outbox_event_id,
                aggregate_type,
                aggregate_id,
                event_type,
                payload_json,
                idempotency_key,
                status,
                retry_count,
                next_retry_at,
                created_at,
                published_at,
                failure_reason
            FROM outbox_events
            WHERE status = 'pending'
               OR (
                    status = 'failed'
                AND next_retry_at IS NOT NULL
                AND next_retry_at <= $2
               )
            ORDER BY COALESCE(next_retry_at, created_at) ASC, created_at ASC, outbox_event_id ASC
            LIMIT $1
            "#,
        )
        .bind(batch_size as i64)
        .bind(now)
        .fetch_all(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        rows.into_iter().map(map_outbox_row).collect()
    }

    async fn list_after(
        &mut self,
        last_processed_event_id: Option<&OutboxEventId>,
        batch_size: usize,
    ) -> Result<Vec<OutboxEvent>, IdentityError> {
        let rows = if let Some(last_processed_event_id) = last_processed_event_id {
            let cursor_created_at: Option<PrimitiveDateTime> = sqlx::query(
                r#"
                SELECT created_at
                FROM outbox_events
                WHERE outbox_event_id = $1
                "#,
            )
            .bind(last_processed_event_id.as_str())
            .fetch_optional(self.transaction.as_mut())
            .await
            .map_err(IdentityError::DatabasePool)?
            .map(|row| row.get("created_at"));

            if let Some(cursor_created_at) = cursor_created_at {
                sqlx::query(
                    r#"
                    SELECT
                        outbox_event_id,
                        aggregate_type,
                        aggregate_id,
                        event_type,
                        payload_json,
                        idempotency_key,
                        status,
                        retry_count,
                        next_retry_at,
                        created_at,
                        published_at,
                        failure_reason
                    FROM outbox_events
                    WHERE (created_at, outbox_event_id) > ($1, $2)
                    ORDER BY created_at ASC, outbox_event_id ASC
                    LIMIT $3
                    "#,
                )
                .bind(cursor_created_at)
                .bind(last_processed_event_id.as_str())
                .bind(batch_size as i64)
                .fetch_all(self.transaction.as_mut())
                .await
                .map_err(IdentityError::DatabasePool)?
            } else {
                Vec::new()
            }
        } else {
            sqlx::query(
                r#"
                SELECT
                    outbox_event_id,
                    aggregate_type,
                    aggregate_id,
                    event_type,
                    payload_json,
                    idempotency_key,
                    status,
                    retry_count,
                    next_retry_at,
                    created_at,
                    published_at,
                    failure_reason
                FROM outbox_events
                ORDER BY created_at ASC, outbox_event_id ASC
                LIMIT $1
                "#,
            )
            .bind(batch_size as i64)
            .fetch_all(self.transaction.as_mut())
            .await
            .map_err(IdentityError::DatabasePool)?
        };

        rows.into_iter().map(map_outbox_row).collect()
    }

    async fn list_for_member_projection(
        &mut self,
        global_member_id: &GlobalMemberId,
    ) -> Result<Vec<OutboxEvent>, IdentityError> {
        let rows = sqlx::query(
            r#"
            SELECT
                outbox_event_id,
                aggregate_type,
                aggregate_id,
                event_type,
                payload_json,
                idempotency_key,
                status,
                retry_count,
                next_retry_at,
                created_at,
                published_at,
                failure_reason
            FROM outbox_events
            WHERE payload_json->>'global_member_id' = $1
              AND event_type IN (
                    'identity.member.created',
                    'identity.member.lifecycle_changed',
                    'identity.member.tombstoned',
                    'identity.capability_profile.updated',
                    'identity.memory_refs.updated',
                    'identity.career_history.appended'
              )
            ORDER BY created_at ASC, outbox_event_id ASC
            "#,
        )
        .bind(global_member_id.as_str())
        .fetch_all(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        rows.into_iter().map(map_outbox_row).collect()
    }

    async fn get(
        &mut self,
        outbox_event_id: &OutboxEventId,
    ) -> Result<Option<OutboxEvent>, IdentityError> {
        let row = sqlx::query(
            r#"
            SELECT
                outbox_event_id,
                aggregate_type,
                aggregate_id,
                event_type,
                payload_json,
                idempotency_key,
                status,
                retry_count,
                next_retry_at,
                created_at,
                published_at,
                failure_reason
            FROM outbox_events
            WHERE outbox_event_id = $1
            "#,
        )
        .bind(outbox_event_id.as_str())
        .fetch_optional(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        row.map(map_outbox_row).transpose()
    }

    async fn save(&mut self, event: &OutboxEvent) -> Result<(), IdentityError> {
        sqlx::query(
            r#"
            UPDATE outbox_events
            SET
                aggregate_type = $2,
                aggregate_id = $3,
                event_type = $4,
                payload_json = $5,
                idempotency_key = $6,
                status = $7,
                retry_count = $8,
                next_retry_at = $9,
                created_at = $10,
                published_at = $11,
                failure_reason = $12
            WHERE outbox_event_id = $1
            "#,
        )
        .bind(event.outbox_event_id.as_str())
        .bind(event.aggregate_type.as_str())
        .bind(event.aggregate_id.as_str())
        .bind(event.event_type.as_str())
        .bind(event.payload_json.clone())
        .bind(event.idempotency_key.as_str())
        .bind(event.status.as_db())
        .bind(event.retry_count)
        .bind(event.next_retry_at)
        .bind(event.created_at)
        .bind(event.published_at)
        .bind(event.failure_reason.as_deref())
        .execute(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        Ok(())
    }
}

/// Member summary projection repository bound to an open SQL transaction.
pub struct SqlxMemberSummaryProjectionRepository<'tx, 'db> {
    transaction: &'tx mut Transaction<'db, Postgres>,
}

impl<'tx, 'db> SqlxMemberSummaryProjectionRepository<'tx, 'db> {
    /// Creates a repository facade over the provided SQL transaction.
    pub fn new(transaction: &'tx mut Transaction<'db, Postgres>) -> Self {
        Self { transaction }
    }
}

impl MemberSummaryProjectionRepository for SqlxMemberSummaryProjectionRepository<'_, '_> {
    async fn get(
        &mut self,
        global_member_id: &GlobalMemberId,
    ) -> Result<Option<MemberSummaryProjection>, IdentityError> {
        let row = sqlx::query(
            r#"
            SELECT
                global_member_id,
                display_name,
                lifecycle,
                main_role_id,
                main_role_name,
                capability_summary_json,
                career_summary_json,
                memory_ref_summary_json,
                projection_version,
                updated_at
            FROM member_summary_projection
            WHERE global_member_id = $1
            "#,
        )
        .bind(global_member_id.as_str())
        .fetch_optional(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        row.map(map_member_summary_projection_row).transpose()
    }

    async fn upsert(&mut self, projection: &MemberSummaryProjection) -> Result<(), IdentityError> {
        sqlx::query(
            r#"
            INSERT INTO member_summary_projection (
                global_member_id,
                display_name,
                lifecycle,
                main_role_id,
                main_role_name,
                capability_summary_json,
                career_summary_json,
                memory_ref_summary_json,
                projection_version,
                updated_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (global_member_id) DO UPDATE
            SET
                display_name = EXCLUDED.display_name,
                lifecycle = EXCLUDED.lifecycle,
                main_role_id = EXCLUDED.main_role_id,
                main_role_name = EXCLUDED.main_role_name,
                capability_summary_json = EXCLUDED.capability_summary_json,
                career_summary_json = EXCLUDED.career_summary_json,
                memory_ref_summary_json = EXCLUDED.memory_ref_summary_json,
                projection_version = EXCLUDED.projection_version,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(projection.global_member_id.as_str())
        .bind(projection.display_name.as_str())
        .bind(projection.lifecycle.as_db())
        .bind(projection.main_role_id.as_ref().map(|value| value.as_str()))
        .bind(projection.main_role_name.as_deref())
        .bind(projection.capability_summary_json.clone())
        .bind(projection.career_summary_json.clone())
        .bind(projection.memory_ref_summary_json.clone())
        .bind(projection.projection_version)
        .bind(projection.updated_at)
        .execute(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        Ok(())
    }

    async fn delete(&mut self, global_member_id: &GlobalMemberId) -> Result<bool, IdentityError> {
        let result = sqlx::query(
            r#"
            DELETE FROM member_summary_projection
            WHERE global_member_id = $1
            "#,
        )
        .bind(global_member_id.as_str())
        .execute(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        Ok(result.rows_affected() == 1)
    }
}

/// Projection checkpoint repository bound to an open SQL transaction.
pub struct SqlxProjectionCheckpointRepository<'tx, 'db> {
    transaction: &'tx mut Transaction<'db, Postgres>,
}

impl<'tx, 'db> SqlxProjectionCheckpointRepository<'tx, 'db> {
    /// Creates a repository facade over the provided SQL transaction.
    pub fn new(transaction: &'tx mut Transaction<'db, Postgres>) -> Self {
        Self { transaction }
    }
}

impl ProjectionCheckpointRepository for SqlxProjectionCheckpointRepository<'_, '_> {
    async fn get_or_create(
        &mut self,
        checkpoint_name: &str,
    ) -> Result<ProjectionCheckpoint, IdentityError> {
        let now = current_timestamp();

        sqlx::query(
            r#"
            INSERT INTO projection_checkpoints (
                checkpoint_name,
                last_processed_event_id,
                status,
                failure_reason,
                updated_at
            ) VALUES ($1, NULL, 'idle', NULL, $2)
            ON CONFLICT (checkpoint_name) DO NOTHING
            "#,
        )
        .bind(checkpoint_name)
        .bind(now)
        .execute(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        let row = sqlx::query(
            r#"
            SELECT
                checkpoint_name,
                last_processed_event_id,
                status,
                failure_reason,
                updated_at
            FROM projection_checkpoints
            WHERE checkpoint_name = $1
            FOR UPDATE
            "#,
        )
        .bind(checkpoint_name)
        .fetch_one(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        map_projection_checkpoint_row(row)
    }

    async fn save(&mut self, checkpoint: &ProjectionCheckpoint) -> Result<(), IdentityError> {
        sqlx::query(
            r#"
            UPDATE projection_checkpoints
            SET
                last_processed_event_id = $2,
                status = $3,
                failure_reason = $4,
                updated_at = $5
            WHERE checkpoint_name = $1
            "#,
        )
        .bind(checkpoint.checkpoint_name.as_str())
        .bind(
            checkpoint
                .last_processed_event_id
                .as_ref()
                .map(|value| value.as_str()),
        )
        .bind(checkpoint.status.as_db())
        .bind(checkpoint.failure_reason.as_deref())
        .bind(checkpoint.updated_at)
        .execute(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        Ok(())
    }
}

/// Dead-letter store bound to an open SQL transaction.
pub struct SqlxInboundDeadLetterStore<'tx, 'db> {
    transaction: &'tx mut Transaction<'db, Postgres>,
}

impl<'tx, 'db> SqlxInboundDeadLetterStore<'tx, 'db> {
    /// Creates a store facade over the provided SQL transaction.
    pub fn new(transaction: &'tx mut Transaction<'db, Postgres>) -> Self {
        Self { transaction }
    }
}

impl InboundDeadLetterStore for SqlxInboundDeadLetterStore<'_, '_> {
    async fn append(&mut self, dead_letter: &InboundDeadLetter) -> Result<(), IdentityError> {
        sqlx::query(
            r#"
            INSERT INTO inbound_dead_letters (
                dead_letter_id,
                source_event_id,
                source_module,
                event_type,
                payload_json,
                failure_reason,
                replay_status,
                created_at
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            "#,
        )
        .bind(dead_letter.dead_letter_id.as_str())
        .bind(
            dead_letter
                .source_event_id
                .as_ref()
                .map(|value| value.as_str()),
        )
        .bind(dead_letter.source_module.as_str())
        .bind(dead_letter.event_type.as_str())
        .bind(dead_letter.payload_json.clone())
        .bind(dead_letter.failure_reason.as_str())
        .bind(dead_letter.replay_status.as_db())
        .bind(dead_letter.created_at)
        .execute(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        Ok(())
    }

    async fn list_pending(
        &mut self,
        batch_size: usize,
    ) -> Result<Vec<InboundDeadLetter>, IdentityError> {
        let rows = sqlx::query(
            r#"
            SELECT
                dead_letter_id,
                source_event_id,
                source_module,
                event_type,
                payload_json,
                failure_reason,
                replay_status,
                created_at
            FROM inbound_dead_letters
            WHERE replay_status = 'pending'
            ORDER BY created_at ASC, dead_letter_id ASC
            LIMIT $1
            "#,
        )
        .bind(batch_size as i64)
        .fetch_all(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        rows.into_iter().map(map_inbound_dead_letter_row).collect()
    }

    async fn get(
        &mut self,
        dead_letter_id: &crate::domain::shared::ids::DeadLetterId,
    ) -> Result<Option<InboundDeadLetter>, IdentityError> {
        let row = sqlx::query(
            r#"
            SELECT
                dead_letter_id,
                source_event_id,
                source_module,
                event_type,
                payload_json,
                failure_reason,
                replay_status,
                created_at
            FROM inbound_dead_letters
            WHERE dead_letter_id = $1
            "#,
        )
        .bind(dead_letter_id.as_str())
        .fetch_optional(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        row.map(map_inbound_dead_letter_row).transpose()
    }

    async fn save(&mut self, dead_letter: &InboundDeadLetter) -> Result<(), IdentityError> {
        sqlx::query(
            r#"
            UPDATE inbound_dead_letters
            SET
                source_event_id = $2,
                source_module = $3,
                event_type = $4,
                payload_json = $5,
                failure_reason = $6,
                replay_status = $7,
                created_at = $8
            WHERE dead_letter_id = $1
            "#,
        )
        .bind(dead_letter.dead_letter_id.as_str())
        .bind(
            dead_letter
                .source_event_id
                .as_ref()
                .map(|value| value.as_str()),
        )
        .bind(dead_letter.source_module.as_str())
        .bind(dead_letter.event_type.as_str())
        .bind(dead_letter.payload_json.clone())
        .bind(dead_letter.failure_reason.as_str())
        .bind(dead_letter.replay_status.as_db())
        .bind(dead_letter.created_at)
        .execute(self.transaction.as_mut())
        .await
        .map_err(IdentityError::DatabasePool)?;

        Ok(())
    }
}

fn map_global_member_row(row: sqlx::postgres::PgRow) -> Result<GlobalMember, IdentityError> {
    let lifecycle_value: String = row.get("lifecycle");
    let lifecycle =
        GlobalMemberLifecycle::from_db(&lifecycle_value).ok_or(IdentityError::PersistenceData {
            message: format!("unknown global member lifecycle `{lifecycle_value}`"),
        })?;
    let secondary_role_ids_json: Value = row.get("secondary_role_ids_json");
    let secondary_role_ids_raw: Vec<String> = serde_json::from_value(secondary_role_ids_json)
        .map_err(|error| IdentityError::PersistenceData {
            message: format!("decode secondary role ids json: {error}"),
        })?;
    let created_by_json: Value = row.get("created_by_json");
    let created_by: ActorContext = serde_json::from_value(created_by_json).map_err(|error| {
        IdentityError::PersistenceData {
            message: format!("decode created_by actor context: {error}"),
        }
    })?;

    Ok(GlobalMember {
        global_member_id: GlobalMemberId::new(row.get::<String, _>("global_member_id")),
        display_name: row.get("display_name"),
        lifecycle,
        main_role_id: RoleId::new(row.get::<String, _>("main_role_id")),
        secondary_role_ids: secondary_role_ids_raw
            .into_iter()
            .map(RoleId::new)
            .collect(),
        capability_profile_id: row
            .get::<Option<String>, _>("capability_profile_id")
            .map(crate::domain::shared::ids::CapabilityProfileId::new),
        memory_refs_id: row
            .get::<Option<String>, _>("memory_refs_id")
            .map(crate::domain::shared::ids::MemoryRefsId::new),
        version: row.get("version"),
        created_by,
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

fn map_role_catalog_row(row: sqlx::postgres::PgRow) -> Result<RoleCatalogEntry, IdentityError> {
    let status_value: String = row.get("status");
    let status =
        RoleCatalogStatus::from_db(&status_value).ok_or(IdentityError::PersistenceData {
            message: format!("unknown role catalog status `{status_value}`"),
        })?;

    Ok(RoleCatalogEntry {
        role_id: RoleId::new(row.get::<String, _>("role_id")),
        role_name: row.get("role_name"),
        role_version: row.get("role_version"),
        source_ref_json: row.get("source_ref_json"),
        fingerprint: row.get("fingerprint"),
        status,
        updated_at: row.get("updated_at"),
    })
}

fn map_capability_profile_row(
    row: sqlx::postgres::PgRow,
) -> Result<CapabilityProfile, IdentityError> {
    let capabilities_json: Value = row.get("capabilities_json");
    let evidence_refs_json: Value = row.get("evidence_refs_json");
    let capabilities: Vec<CapabilityItem> =
        serde_json::from_value(capabilities_json).map_err(|error| {
            IdentityError::PersistenceData {
                message: format!("decode capability items json: {error}"),
            }
        })?;
    let evidence_refs: Vec<ArtifactRef> =
        serde_json::from_value(evidence_refs_json).map_err(|error| {
            IdentityError::PersistenceData {
                message: format!("decode capability evidence refs json: {error}"),
            }
        })?;

    Ok(CapabilityProfile {
        capability_profile_id: CapabilityProfileId::new(
            row.get::<String, _>("capability_profile_id"),
        ),
        global_member_id: GlobalMemberId::new(row.get::<String, _>("global_member_id")),
        capabilities,
        evidence_refs,
        version: row.get("version"),
        updated_at: row.get("updated_at"),
    })
}

fn map_memory_refs_row(row: sqlx::postgres::PgRow) -> Result<MemoryRefs, IdentityError> {
    let semantic_memory_ref_json: Option<Value> = row.get("semantic_memory_ref_json");
    let episodic_memory_refs_json: Value = row.get("episodic_memory_refs_json");
    let archive_ref_json: Option<Value> = row.get("archive_ref_json");
    let archive_status_value: String = row.get("archive_status");
    let archive_status =
        ArchiveStatus::from_db(&archive_status_value).ok_or(IdentityError::PersistenceData {
            message: format!("unknown archive status `{archive_status_value}`"),
        })?;
    let semantic_memory_ref = semantic_memory_ref_json
        .filter(|value| !value.is_null())
        .map(serde_json::from_value::<MemoryRef>)
        .transpose()
        .map_err(|error| IdentityError::PersistenceData {
            message: format!("decode semantic memory ref json: {error}"),
        })?;
    let episodic_memory_refs: Vec<MemoryRef> = serde_json::from_value(episodic_memory_refs_json)
        .map_err(|error| IdentityError::PersistenceData {
            message: format!("decode episodic memory refs json: {error}"),
        })?;
    let archive_ref = archive_ref_json
        .filter(|value| !value.is_null())
        .map(serde_json::from_value::<ArchiveRef>)
        .transpose()
        .map_err(|error| IdentityError::PersistenceData {
            message: format!("decode archive ref json: {error}"),
        })?;

    Ok(MemoryRefs {
        memory_refs_id: MemoryRefsId::new(row.get::<String, _>("memory_refs_id")),
        global_member_id: GlobalMemberId::new(row.get::<String, _>("global_member_id")),
        semantic_memory_ref,
        episodic_memory_refs,
        archive_ref,
        archive_status,
        version: row.get("version"),
        updated_at: row.get("updated_at"),
    })
}

fn map_career_entry_row(row: sqlx::postgres::PgRow) -> Result<CareerEntry, IdentityError> {
    let work_ref_json: Option<Value> = row.get("work_ref_json");
    let process_ref_json: Option<Value> = row.get("process_ref_json");
    let work_ref = work_ref_json
        .filter(|value| !value.is_null())
        .map(serde_json::from_value::<WorkRef>)
        .transpose()
        .map_err(|error| IdentityError::PersistenceData {
            message: format!("decode career work ref json: {error}"),
        })?;
    let process_ref = process_ref_json
        .filter(|value| !value.is_null())
        .map(serde_json::from_value::<ProcessRef>)
        .transpose()
        .map_err(|error| IdentityError::PersistenceData {
            message: format!("decode career process ref json: {error}"),
        })?;

    if work_ref.is_some() == process_ref.is_some() {
        return Err(IdentityError::PersistenceData {
            message: format!(
                "career entry `{}` must have exactly one source ref",
                row.get::<String, _>("career_entry_id")
            ),
        });
    }

    Ok(CareerEntry {
        career_entry_id: CareerEntryId::new(row.get::<String, _>("career_entry_id")),
        global_member_id: GlobalMemberId::new(row.get::<String, _>("global_member_id")),
        source_event_id: EventId::new(row.get::<String, _>("source_event_id")),
        source_module: row.get("source_module"),
        project_id: row
            .get::<Option<String>, _>("project_id")
            .map(ProjectId::new),
        work_ref,
        process_ref,
        entry_kind: row.get("entry_kind"),
        started_at: row.get("started_at"),
        ended_at: row.get("ended_at"),
        payload_summary: row.get("payload_summary_json"),
        created_at: row.get("created_at"),
    })
}

fn map_idempotency_row(row: sqlx::postgres::PgRow) -> Result<IdempotencyRecord, IdentityError> {
    let scope_value: String = row.get("scope");
    let scope = IdempotencyScope::from_db(&scope_value).ok_or(IdentityError::PersistenceData {
        message: format!("unknown idempotency scope `{scope_value}`"),
    })?;
    let status_value: String = row.get("status");
    let status =
        IdempotencyStatus::from_db(&status_value).ok_or(IdentityError::PersistenceData {
            message: format!("unknown idempotency status `{status_value}`"),
        })?;

    Ok(IdempotencyRecord {
        idempotency_key: row.get("idempotency_key"),
        scope,
        request_hash: row.get("request_hash"),
        result_ref_json: row.get("result_ref_json"),
        status,
        created_at: row.get("created_at"),
        updated_at: row.get("updated_at"),
    })
}

fn map_outbox_row(row: sqlx::postgres::PgRow) -> Result<OutboxEvent, IdentityError> {
    let status_value: String = row.get("status");
    let status = OutboxStatus::from_db(&status_value).ok_or(IdentityError::PersistenceData {
        message: format!("unknown outbox status `{status_value}`"),
    })?;

    Ok(OutboxEvent {
        outbox_event_id: OutboxEventId::new(row.get::<String, _>("outbox_event_id")),
        aggregate_type: row.get("aggregate_type"),
        aggregate_id: row.get("aggregate_id"),
        event_type: row.get("event_type"),
        payload_json: row.get("payload_json"),
        idempotency_key: row.get("idempotency_key"),
        status,
        retry_count: row.get("retry_count"),
        next_retry_at: row.get("next_retry_at"),
        created_at: row.get("created_at"),
        published_at: row.get("published_at"),
        failure_reason: row.get("failure_reason"),
    })
}

fn map_member_summary_projection_row(
    row: sqlx::postgres::PgRow,
) -> Result<MemberSummaryProjection, IdentityError> {
    let lifecycle_value: String = row.get("lifecycle");
    let lifecycle =
        GlobalMemberLifecycle::from_db(&lifecycle_value).ok_or(IdentityError::PersistenceData {
            message: format!("unknown member summary lifecycle `{lifecycle_value}`"),
        })?;

    Ok(MemberSummaryProjection {
        global_member_id: GlobalMemberId::new(row.get::<String, _>("global_member_id")),
        display_name: row.get("display_name"),
        lifecycle,
        main_role_id: row
            .get::<Option<String>, _>("main_role_id")
            .map(RoleId::new),
        main_role_name: row.get("main_role_name"),
        capability_summary_json: row.get("capability_summary_json"),
        career_summary_json: row.get("career_summary_json"),
        memory_ref_summary_json: row.get("memory_ref_summary_json"),
        projection_version: row.get("projection_version"),
        updated_at: row.get("updated_at"),
    })
}

fn map_projection_checkpoint_row(
    row: sqlx::postgres::PgRow,
) -> Result<ProjectionCheckpoint, IdentityError> {
    let status_value: String = row.get("status");
    let status = ProjectionCheckpointStatus::from_db(&status_value).ok_or(
        IdentityError::PersistenceData {
            message: format!("unknown projection checkpoint status `{status_value}`"),
        },
    )?;

    Ok(ProjectionCheckpoint {
        checkpoint_name: row.get("checkpoint_name"),
        last_processed_event_id: row
            .get::<Option<String>, _>("last_processed_event_id")
            .map(OutboxEventId::new),
        status,
        failure_reason: row.get("failure_reason"),
        updated_at: row.get("updated_at"),
    })
}

fn map_inbound_dead_letter_row(
    row: sqlx::postgres::PgRow,
) -> Result<InboundDeadLetter, IdentityError> {
    let replay_status_value: String = row.get("replay_status");
    let replay_status = DeadLetterReplayStatus::from_db(&replay_status_value).ok_or(
        IdentityError::PersistenceData {
            message: format!("unknown dead-letter replay status `{replay_status_value}`"),
        },
    )?;

    Ok(InboundDeadLetter {
        dead_letter_id: crate::domain::shared::ids::DeadLetterId::new(
            row.get::<String, _>("dead_letter_id"),
        ),
        source_event_id: row
            .get::<Option<String>, _>("source_event_id")
            .map(EventId::new),
        source_module: row.get("source_module"),
        event_type: row.get("event_type"),
        payload_json: row.get("payload_json"),
        failure_reason: row.get("failure_reason"),
        replay_status,
        created_at: row.get("created_at"),
    })
}

fn current_timestamp() -> PrimitiveDateTime {
    let now = OffsetDateTime::now_utc();
    PrimitiveDateTime::new(now.date(), now.time())
}

#[allow(dead_code)]
fn _map_lifecycle_history_row(
    row: sqlx::postgres::PgRow,
) -> Result<LifecycleHistoryEntry, IdentityError> {
    let event_type_value: String = row.get("event_type");
    let event_type =
        LifecycleEventType::from_db(&event_type_value).ok_or(IdentityError::PersistenceData {
            message: format!("unknown lifecycle event type `{event_type_value}`"),
        })?;
    let from_lifecycle = row
        .get::<Option<String>, _>("from_lifecycle")
        .map(|value| {
            GlobalMemberLifecycle::from_db(&value).ok_or(IdentityError::PersistenceData {
                message: format!("unknown lifecycle from-state `{value}`"),
            })
        })
        .transpose()?;
    let to_lifecycle_value: String = row.get("to_lifecycle");
    let to_lifecycle = GlobalMemberLifecycle::from_db(&to_lifecycle_value).ok_or(
        IdentityError::PersistenceData {
            message: format!("unknown lifecycle to-state `{to_lifecycle_value}`"),
        },
    )?;
    let actor_json: Value = row.get("actor_json");
    let actor: ActorContext =
        serde_json::from_value(actor_json).map_err(|error| IdentityError::PersistenceData {
            message: format!("decode lifecycle actor context: {error}"),
        })?;
    let metadata_json: Value = row.get("metadata_json");
    let metadata: CommandMetadata =
        serde_json::from_value(metadata_json).map_err(|error| IdentityError::PersistenceData {
            message: format!("decode lifecycle command metadata: {error}"),
        })?;

    Ok(LifecycleHistoryEntry {
        history_entry_id: row.get("history_entry_id"),
        global_member_id: GlobalMemberId::new(row.get::<String, _>("global_member_id")),
        event_type,
        from_lifecycle,
        to_lifecycle,
        actor,
        gate_decision_ref_json: row.get("gate_decision_ref_json"),
        metadata,
        created_at: row.get("created_at"),
    })
}

fn map_audit_trace_row(row: sqlx::postgres::PgRow) -> Result<AuditTraceEntry, IdentityError> {
    let result_value: String = row.get("result");
    let result = AuditResult::from_db(&result_value).ok_or(IdentityError::PersistenceData {
        message: format!("unknown audit result `{result_value}`"),
    })?;

    Ok(AuditTraceEntry {
        audit_trace_id: row.get("audit_trace_id"),
        trace_id: row.get("trace_id"),
        action: row.get("action"),
        actor_json: row.get("actor_json"),
        target_ref_json: row.get("target_ref_json"),
        source_module: row.get("source_module"),
        result,
        reason: row.get("reason"),
        created_at: row.get("created_at"),
    })
}
