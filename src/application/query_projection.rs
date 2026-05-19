//! Application query services for projection-backed read APIs.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use time::PrimitiveDateTime;

use crate::application::persistence::{
    AuditTraceRepository, GlobalMemberRepository, MemberSummaryProjectionRepository,
    RoleCatalogRepository, UnitOfWork, UnitOfWorkFactory,
};
use crate::domain::audit::{AuditResult, AuditTraceEntry};
use crate::domain::member::GlobalMemberLifecycle;
use crate::domain::projection::MemberSummaryProjection;
use crate::domain::role_catalog::{RoleCatalogEntry, RoleCatalogStatus};
use crate::domain::shared::context::{ActorContext, ActorKind};
use crate::domain::shared::ids::{GlobalMemberId, RoleId};
use crate::domain::shared::pagination::{NormalizedPageRequest, PageRequest};
use crate::error::IdentityError;

const DEFAULT_AUDIT_TRACE_PAGE_LIMIT: u32 = 50;
const MAX_AUDIT_TRACE_PAGE_LIMIT: u32 = 100;

/// Coordinates projection-backed query flows without mutating write models.
#[derive(Debug, Clone)]
pub struct QueryProjectionService<UowFactory> {
    unit_of_work_factory: UowFactory,
}

impl<UowFactory> QueryProjectionService<UowFactory> {
    /// Creates a new query service bound to the provided persistence factory.
    pub fn new(unit_of_work_factory: UowFactory) -> Self {
        Self {
            unit_of_work_factory,
        }
    }
}

/// Minimal query DTO for member summary lookups.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetMemberSummaryQuery {
    /// Stable global member id used to load the read-optimized projection.
    pub global_member_id: GlobalMemberId,
}

/// Query DTO for member audit-trace lookups.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GetMemberAuditTraceQuery {
    /// Stable global member id whose audit trail should be listed.
    pub global_member_id: GlobalMemberId,
    /// Optional pagination input controlling the returned slice.
    pub page: Option<PageRequest>,
}

/// Query DTO for local role-catalog summary lookups.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct GetRoleCatalogQuery {
    /// Optional filter controlling which local role rows are returned.
    pub filter: Option<RoleCatalogFilter>,
}

/// Optional filter used by the local role-catalog query API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RoleCatalogFilter {
    /// Optional local status filter: `active`, `deprecated`, or `source_drift`.
    pub status: Option<String>,
    /// Optional explicit role-id subset filter.
    #[serde(default)]
    pub role_ids: Vec<RoleId>,
    /// Optional case-insensitive keyword matched against the cached display name.
    pub keyword: Option<String>,
}

/// Read-facing member summary DTO returned by query flows.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemberSummaryDto {
    /// Stable member identifier.
    pub global_member_id: GlobalMemberId,
    /// User-facing display name from the current projection row.
    pub display_name: String,
    /// Lifecycle snapshot exposed to callers.
    pub lifecycle: GlobalMemberLifecycle,
    /// Optional main role id summary.
    pub main_role_id: Option<RoleId>,
    /// Optional main role display name summary.
    pub main_role_name: Option<String>,
    /// Projection-safe capability summary without external truth bodies.
    pub capability_summary_json: Value,
    /// Projection-safe career summary.
    pub career_summary_json: Value,
    /// Projection-safe memory refs summary without memory bodies.
    pub memory_ref_summary_json: Value,
    /// Projection version useful for diagnostics and client refresh decisions.
    pub projection_version: i64,
}

/// One read-facing audit trace row returned by the audit query API.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuditTraceDto {
    /// Stable audit trace identifier.
    pub audit_trace_id: String,
    /// Stable trace identifier retained for correlation.
    pub trace_id: String,
    /// Action name recorded by the handler that produced the trace.
    pub action: String,
    /// Actor snapshot when visible to the current caller.
    pub actor_json: Option<Value>,
    /// Weak target reference snapshot when visible to the current caller.
    pub target_ref_json: Option<Value>,
    /// Optional source module for inbound-event traces.
    pub source_module: Option<String>,
    /// Terminal result of the audited action.
    pub result: AuditResult,
    /// Optional reason when retained and visible to the current caller.
    pub reason: Option<String>,
    /// Timestamp when the audit row was captured.
    pub created_at: PrimitiveDateTime,
}

/// Paginated audit-trace response returned by the query service.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MemberAuditTracePageDto {
    /// Audit trace rows in reverse chronological order.
    pub traces: Vec<AuditTraceDto>,
    /// Opaque cursor for the next page when more rows are available.
    pub next_cursor: Option<String>,
}

/// One read-facing local role-catalog summary row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RoleCatalogEntryDto {
    /// Stable local role identifier.
    pub role_id: RoleId,
    /// Cached role display name.
    pub role_name: String,
    /// Upstream role version retained for diagnostics and compatibility.
    pub role_version: String,
    /// Ref-only upstream source pointer without role-definition body duplication.
    pub source_ref_json: Value,
    /// Cached upstream fingerprint used for drift diagnostics.
    pub fingerprint: String,
    /// Current local index status.
    pub status: RoleCatalogStatus,
    /// Last successful local synchronization timestamp.
    pub updated_at: PrimitiveDateTime,
}

impl MemberSummaryDto {
    /// Builds the query DTO from the persisted projection row.
    pub fn from_projection(projection: MemberSummaryProjection) -> Self {
        Self {
            global_member_id: projection.global_member_id,
            display_name: projection.display_name,
            lifecycle: projection.lifecycle,
            main_role_id: projection.main_role_id,
            main_role_name: projection.main_role_name,
            capability_summary_json: projection.capability_summary_json,
            career_summary_json: projection.career_summary_json,
            memory_ref_summary_json: projection.memory_ref_summary_json,
            projection_version: projection.projection_version,
        }
    }
}

impl AuditTraceDto {
    fn from_entry(entry: AuditTraceEntry) -> Self {
        Self {
            audit_trace_id: entry.audit_trace_id,
            trace_id: entry.trace_id,
            action: entry.action,
            actor_json: entry.actor_json,
            target_ref_json: entry.target_ref_json,
            source_module: entry.source_module,
            result: entry.result,
            reason: entry.reason,
            created_at: entry.created_at,
        }
    }

    fn from_trimmed_entry(entry: AuditTraceEntry) -> Self {
        Self {
            audit_trace_id: entry.audit_trace_id,
            trace_id: entry.trace_id,
            action: entry.action,
            actor_json: None,
            target_ref_json: None,
            source_module: entry.source_module,
            result: entry.result,
            reason: None,
            created_at: entry.created_at,
        }
    }
}

impl RoleCatalogEntryDto {
    fn from_entry(entry: RoleCatalogEntry) -> Self {
        Self {
            role_id: entry.role_id,
            role_name: entry.role_name,
            role_version: entry.role_version,
            source_ref_json: entry.source_ref_json,
            fingerprint: entry.fingerprint,
            status: entry.status,
            updated_at: entry.updated_at,
        }
    }
}

impl<UowFactory> QueryProjectionService<UowFactory>
where
    UowFactory: UnitOfWorkFactory,
{
    /// Loads the current member summary projection without triggering rebuilds or implicit member creation.
    ///
    /// # Errors
    ///
    /// Returns `IDENTITY_MEMBER_NOT_FOUND` when neither projection nor write model exists.
    /// Returns `IDENTITY_PROJECTION_NOT_READY` when the write model exists but the projection is
    /// still missing.
    pub async fn get_member_summary(
        &self,
        query: GetMemberSummaryQuery,
        actor: ActorContext,
    ) -> Result<MemberSummaryDto, IdentityError> {
        let mut uow = self.unit_of_work_factory.begin().await?;
        let projection = {
            let mut repository = uow.member_summary_projection();
            repository.get(&query.global_member_id).await?
        };

        if let Some(projection) = projection {
            let summary = trim_member_summary(projection, &actor);
            uow.rollback().await?;
            return Ok(summary);
        }

        let member_exists = {
            let mut repository = uow.global_members();
            repository.get(&query.global_member_id).await?.is_some()
        };
        uow.rollback().await?;

        if member_exists {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_PROJECTION_NOT_READY",
                message: format!(
                    "member summary projection for `{}` is not ready",
                    query.global_member_id.as_str()
                ),
            });
        }

        Err(IdentityError::RuleViolation {
            code: "IDENTITY_MEMBER_NOT_FOUND",
            message: format!(
                "global member `{}` was not found",
                query.global_member_id.as_str()
            ),
        })
    }

    /// Lists one member's audit traces without mutating write models or append-only history.
    ///
    /// # Errors
    ///
    /// Returns `IDENTITY_AUDIT_TRACE_NOT_VISIBLE` when the caller is not allowed to view the
    /// requested member's audit trail. Returns `IDENTITY_MEMBER_NOT_FOUND` when the member does
    /// not exist and no audit traces can be found for the requested id.
    pub async fn get_member_audit_trace(
        &self,
        query: GetMemberAuditTraceQuery,
        actor: ActorContext,
    ) -> Result<MemberAuditTracePageDto, IdentityError> {
        if !can_view_member_trace(&actor, &query.global_member_id) {
            return Err(IdentityError::RuleViolation {
                code: "IDENTITY_AUDIT_TRACE_NOT_VISIBLE",
                message: format!(
                    "actor `{}` cannot view audit trace for member `{}`",
                    actor.actor_ref,
                    query.global_member_id.as_str()
                ),
            });
        }

        let normalized_page = query
            .page
            .unwrap_or_default()
            .normalize(DEFAULT_AUDIT_TRACE_PAGE_LIMIT, MAX_AUDIT_TRACE_PAGE_LIMIT)?;
        let repository_page = page_for_fetch(&normalized_page);
        let mut uow = self.unit_of_work_factory.begin().await?;
        let mut traces = {
            let mut repository = uow.audit_traces();
            repository
                .list_by_member(&query.global_member_id, &repository_page)
                .await?
        };

        if traces.is_empty() {
            let member_exists = {
                let mut repository = uow.global_members();
                repository.get(&query.global_member_id).await?.is_some()
            };
            uow.rollback().await?;

            if !member_exists {
                return Err(IdentityError::RuleViolation {
                    code: "IDENTITY_MEMBER_NOT_FOUND",
                    message: format!(
                        "global member `{}` was not found",
                        query.global_member_id.as_str()
                    ),
                });
            }

            return Ok(MemberAuditTracePageDto {
                traces: Vec::new(),
                next_cursor: None,
            });
        }

        uow.rollback().await?;

        let next_cursor = if traces.len() > normalized_page.limit as usize {
            traces.truncate(normalized_page.limit as usize);
            traces.last().map(|trace| trace.audit_trace_id.clone())
        } else {
            None
        };

        Ok(MemberAuditTracePageDto {
            traces: trim_audit_trace(traces, &actor),
            next_cursor,
        })
    }

    /// Lists local role-catalog summary rows without reading upstream role-definition bodies.
    pub async fn get_role_catalog(
        &self,
        query: GetRoleCatalogQuery,
        _actor: ActorContext,
    ) -> Result<Vec<RoleCatalogEntryDto>, IdentityError> {
        let mut uow = self.unit_of_work_factory.begin().await?;
        let entries = {
            let mut repository = uow.role_catalog();
            repository.list().await?
        };
        uow.rollback().await?;

        let filter = normalize_role_catalog_filter(query.filter)?;
        Ok(entries
            .into_iter()
            .filter(|entry| role_catalog_entry_matches(entry, filter.as_ref()))
            .map(RoleCatalogEntryDto::from_entry)
            .collect())
    }
}

fn trim_member_summary(
    projection: MemberSummaryProjection,
    _actor: &ActorContext,
) -> MemberSummaryDto {
    MemberSummaryDto::from_projection(projection)
}

fn trim_audit_trace(traces: Vec<AuditTraceEntry>, actor: &ActorContext) -> Vec<AuditTraceDto> {
    match actor.actor_kind {
        ActorKind::HumanUser => traces.into_iter().map(AuditTraceDto::from_entry).collect(),
        ActorKind::AiMember => traces
            .into_iter()
            .map(AuditTraceDto::from_trimmed_entry)
            .collect(),
        ActorKind::System => Vec::new(),
    }
}

fn can_view_member_trace(actor: &ActorContext, global_member_id: &GlobalMemberId) -> bool {
    match actor.actor_kind {
        ActorKind::HumanUser => true,
        ActorKind::AiMember => actor.actor_member_id() == Some(global_member_id),
        ActorKind::System => false,
    }
}

fn page_for_fetch(page: &NormalizedPageRequest) -> NormalizedPageRequest {
    NormalizedPageRequest {
        limit: page.limit.saturating_add(1),
        cursor: page.cursor.clone(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct NormalizedRoleCatalogFilter {
    status: Option<RoleCatalogStatus>,
    role_ids: Vec<RoleId>,
    keyword: Option<String>,
}

fn normalize_role_catalog_filter(
    filter: Option<RoleCatalogFilter>,
) -> Result<Option<NormalizedRoleCatalogFilter>, IdentityError> {
    let Some(filter) = filter else {
        return Ok(None);
    };

    let status = match filter.status.as_deref().map(str::trim) {
        None | Some("") => None,
        Some(value) => Some(RoleCatalogStatus::from_db(value).ok_or(IdentityError::RuleViolation {
            code: "IDENTITY_INVALID_ARGUMENT",
            message: format!(
                "role_catalog.filter.status must be one of `active`, `deprecated`, or `source_drift`, got `{value}`"
            ),
        })?),
    };

    let role_ids = filter
        .role_ids
        .into_iter()
        .filter(|role_id| !role_id.as_str().trim().is_empty())
        .collect::<Vec<_>>();

    let keyword = match filter.keyword {
        None => None,
        Some(keyword) => {
            let trimmed = keyword.trim().to_ascii_lowercase();
            if trimmed.is_empty() {
                return Err(IdentityError::RuleViolation {
                    code: "IDENTITY_INVALID_ARGUMENT",
                    message: "role_catalog.filter.keyword must not be blank".to_string(),
                });
            }
            Some(trimmed)
        }
    };

    Ok(Some(NormalizedRoleCatalogFilter {
        status,
        role_ids,
        keyword,
    }))
}

fn role_catalog_entry_matches(
    entry: &RoleCatalogEntry,
    filter: Option<&NormalizedRoleCatalogFilter>,
) -> bool {
    let Some(filter) = filter else {
        return true;
    };

    if filter.status.is_some_and(|status| entry.status != status) {
        return false;
    }
    if !filter.role_ids.is_empty()
        && !filter
            .role_ids
            .iter()
            .any(|role_id| role_id == &entry.role_id)
    {
        return false;
    }
    if let Some(keyword) = filter.keyword.as_deref() {
        let role_name = entry.role_name.to_ascii_lowercase();
        if !role_name.contains(keyword) {
            return false;
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde_json::json;
    use sqlx::{Executor, Row, postgres::PgPoolOptions};
    use time::{Duration, OffsetDateTime, PrimitiveDateTime};

    use crate::application::career_event::CareerEventConsumerService;
    use crate::application::member_lifecycle::MemberLifecycleCommandService;
    use crate::application::memory_refs::MemoryRefsCommandService;
    use crate::config::AppConfig;
    use crate::domain::member::{GlobalMemberLifecycle, HireGlobalMemberCommand};
    use crate::domain::memory_refs::{MemoryRef, UpdateMemoryRefsCommand};
    use crate::domain::role_catalog::RoleCatalogStatus;
    use crate::domain::shared::context::{ActorContext, ActorKind};
    use crate::domain::shared::ids::{EventId, GlobalMemberId, ProjectId, RoleId};
    use crate::domain::shared::metadata::CommandMetadata;
    use crate::domain::shared::pagination::PageRequest;
    use crate::error::IdentityError;
    use crate::inbound::event_consumers::CareerEventConsumer;
    use crate::inbound::events::{InboundEventEnvelope, InboundWorkFactEvent};
    use crate::operations::ProjectionRebuildJob;
    use crate::outbound::MemoryArchivePort;
    use crate::persistence::database::run_migrations;
    use crate::persistence::test_support::DB_TEST_MUTEX;
    use crate::persistence::unit_of_work::SqlxUnitOfWorkFactory;

    use super::{
        GetMemberAuditTraceQuery, GetMemberSummaryQuery, GetRoleCatalogQuery,
        QueryProjectionService, RoleCatalogFilter,
    };

    #[derive(Debug, Clone, Default)]
    struct StubMemoryArchiveValidator;

    impl MemoryArchivePort for StubMemoryArchiveValidator {
        async fn validate_ref(&self, _memory_ref: &MemoryRef) -> Result<(), IdentityError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn get_member_summary_returns_projection_when_ready() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let command_service = MemberLifecycleCommandService::new(factory.clone());
        let query_service = QueryProjectionService::new(factory.clone());
        let rebuild_job = ProjectionRebuildJob::new(factory);
        let actor = ActorContext::new("human/admin-1", ActorKind::HumanUser, None);

        let member = command_service
            .hire_global_member(
                HireGlobalMemberCommand {
                    display_name: "Member Zero One".to_string(),
                    main_role_id: RoleId::new("role.member.operator"),
                    secondary_role_ids: Vec::new(),
                },
                actor.clone(),
                CommandMetadata::new("idem-query-001", "trace-query-001", "hash-query-001"),
            )
            .await
            .expect("hire member for query");
        rebuild_job
            .rebuild_member_summary_projection("member-summary-rebuild", 10)
            .await
            .expect("rebuild projection");

        let summary = query_service
            .get_member_summary(
                GetMemberSummaryQuery {
                    global_member_id: member.global_member_id.clone(),
                },
                actor,
            )
            .await
            .expect("query should return the rebuilt projection");

        assert_eq!(summary.global_member_id, member.global_member_id);
        assert_eq!(summary.display_name, "Member Zero One");
        assert_eq!(summary.lifecycle, GlobalMemberLifecycle::Hired);
        assert_eq!(
            summary.main_role_id.as_ref().map(RoleId::as_str),
            Some("role.member.operator")
        );
        assert_eq!(summary.main_role_name.as_deref(), Some("Member Operator"));
        assert_eq!(summary.capability_summary_json, json!({}));
        assert_eq!(summary.career_summary_json, json!({}));
        assert_eq!(summary.memory_ref_summary_json, json!({}));
        assert_eq!(summary.projection_version, 0);
    }

    #[tokio::test]
    async fn get_member_summary_returns_not_ready_when_member_exists_but_projection_is_missing() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let factory = SqlxUnitOfWorkFactory::new(pool);
        let command_service = MemberLifecycleCommandService::new(factory.clone());
        let query_service = QueryProjectionService::new(factory);
        let actor = ActorContext::new("human/admin-2", ActorKind::HumanUser, None);

        let member = command_service
            .hire_global_member(
                HireGlobalMemberCommand {
                    display_name: "Member Zero Two".to_string(),
                    main_role_id: RoleId::new("role.member.operator"),
                    secondary_role_ids: Vec::new(),
                },
                actor.clone(),
                CommandMetadata::new("idem-query-002", "trace-query-002", "hash-query-002"),
            )
            .await
            .expect("hire member without rebuilding projection");

        let error = query_service
            .get_member_summary(
                GetMemberSummaryQuery {
                    global_member_id: member.global_member_id,
                },
                actor,
            )
            .await
            .expect_err("query should report the projection as not ready");

        match error {
            IdentityError::RuleViolation { code, message } => {
                assert_eq!(code, "IDENTITY_PROJECTION_NOT_READY");
                assert!(message.contains("is not ready"));
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[tokio::test]
    async fn get_member_summary_returns_not_found_without_creating_a_member() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;

        let query_service = QueryProjectionService::new(SqlxUnitOfWorkFactory::new(pool.clone()));
        let error = query_service
            .get_member_summary(
                GetMemberSummaryQuery {
                    global_member_id: crate::domain::shared::ids::GlobalMemberId::new(
                        "member-missing-001",
                    ),
                },
                ActorContext::new("human/admin-3", ActorKind::HumanUser, None),
            )
            .await
            .expect_err("query should return not found");

        let member_count: i64 = sqlx::query("SELECT COUNT(*) AS count FROM global_members")
            .fetch_one(&pool)
            .await
            .expect("count members after failed query")
            .get("count");

        match error {
            IdentityError::RuleViolation { code, message } => {
                assert_eq!(code, "IDENTITY_MEMBER_NOT_FOUND");
                assert!(message.contains("member-missing-001"));
            }
            other => panic!("unexpected error: {other}"),
        }
        assert_eq!(member_count, 0);
    }

    #[tokio::test]
    async fn get_member_audit_trace_returns_paginated_traces_for_human_actor() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let command_service = MemberLifecycleCommandService::new(factory.clone());
        let memory_service =
            MemoryRefsCommandService::new(factory.clone(), StubMemoryArchiveValidator);
        let career_consumer = CareerEventConsumer::new(CareerEventConsumerService::new(factory));
        let actor = ActorContext::new("human/admin-audit-1", ActorKind::HumanUser, None);

        let member = command_service
            .hire_global_member(
                HireGlobalMemberCommand {
                    display_name: "Member Audit One".to_string(),
                    main_role_id: RoleId::new("role.member.operator"),
                    secondary_role_ids: Vec::new(),
                },
                actor.clone(),
                CommandMetadata::new(
                    "idem-audit-hire-001",
                    "trace-audit-hire-001",
                    "hash-audit-hire-001",
                ),
            )
            .await
            .expect("hire member for audit query");
        memory_service
            .update_memory_refs(
                UpdateMemoryRefsCommand {
                    global_member_id: member.global_member_id.clone(),
                    semantic_memory_ref: Some(MemoryRef {
                        memory_id: "memory-audit-001".to_string(),
                        memory_kind: "semantic".to_string(),
                        memory_version: Some("v1".to_string()),
                    }),
                    episodic_memory_refs: Vec::new(),
                },
                actor.clone(),
                CommandMetadata::new(
                    "idem-audit-memory-001",
                    "trace-audit-memory-001",
                    "hash-audit-memory-001",
                ),
            )
            .await
            .expect("update memory refs for audit query");
        career_consumer
            .consume_work_event(sample_work_event(
                "audit-career-event-001",
                "audit-career-hash-001",
                member.global_member_id.as_str(),
            ))
            .await
            .expect("append career event for audit query");

        update_audit_created_at(&pool, "audit:idem-audit-hire-001", now()).await;
        update_audit_created_at(
            &pool,
            "audit:idem-audit-memory-001",
            now() + Duration::seconds(1),
        )
        .await;
        update_audit_created_at(
            &pool,
            "audit:audit-career-event-001",
            now() + Duration::seconds(2),
        )
        .await;

        let query_service = QueryProjectionService::new(SqlxUnitOfWorkFactory::new(pool));
        let first_page = query_service
            .get_member_audit_trace(
                GetMemberAuditTraceQuery {
                    global_member_id: member.global_member_id.clone(),
                    page: Some(PageRequest {
                        limit: Some(2),
                        cursor: None,
                    }),
                },
                actor.clone(),
            )
            .await
            .expect("first audit page should succeed");

        assert_eq!(first_page.traces.len(), 2);
        assert_eq!(first_page.traces[0].action, "AppendCareerEntry");
        assert_eq!(first_page.traces[1].action, "UpdateMemoryRefs");
        assert!(first_page.traces[0].actor_json.is_some());
        assert!(first_page.traces[0].target_ref_json.is_some());
        assert_eq!(
            first_page.next_cursor.as_deref(),
            Some("audit:idem-audit-memory-001")
        );

        let second_page = query_service
            .get_member_audit_trace(
                GetMemberAuditTraceQuery {
                    global_member_id: member.global_member_id,
                    page: Some(PageRequest {
                        limit: Some(2),
                        cursor: first_page.next_cursor.clone(),
                    }),
                },
                actor,
            )
            .await
            .expect("second audit page should succeed");

        assert_eq!(second_page.traces.len(), 1);
        assert_eq!(second_page.traces[0].action, "HireGlobalMember");
        assert_eq!(second_page.next_cursor, None);
    }

    #[tokio::test]
    async fn get_member_audit_trace_trims_sensitive_fields_for_same_ai_member() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let member = seed_member_audit_trail(&pool).await;
        let ai_actor = ActorContext::new(
            "ai/member-audit-002",
            ActorKind::AiMember,
            Some(member.global_member_id.clone()),
        );
        let query_service = QueryProjectionService::new(SqlxUnitOfWorkFactory::new(pool));

        let page = query_service
            .get_member_audit_trace(
                GetMemberAuditTraceQuery {
                    global_member_id: member.global_member_id,
                    page: Some(PageRequest {
                        limit: Some(10),
                        cursor: None,
                    }),
                },
                ai_actor,
            )
            .await
            .expect("ai member should view its own trimmed audit trace");

        assert!(!page.traces.is_empty());
        assert_eq!(page.traces[0].action, "AppendCareerEntry");
        assert_eq!(page.traces[0].actor_json, None);
        assert_eq!(page.traces[0].target_ref_json, None);
        assert_eq!(page.traces[0].reason, None);
    }

    #[tokio::test]
    async fn get_member_audit_trace_rejects_invisible_actor() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let member = seed_member_audit_trail(&pool).await;
        let query_service = QueryProjectionService::new(SqlxUnitOfWorkFactory::new(pool));

        let error = query_service
            .get_member_audit_trace(
                GetMemberAuditTraceQuery {
                    global_member_id: member.global_member_id,
                    page: None,
                },
                ActorContext::new("system/query", ActorKind::System, None),
            )
            .await
            .expect_err("system actor should not view member audit traces");

        match error {
            IdentityError::RuleViolation { code, message } => {
                assert_eq!(code, "IDENTITY_AUDIT_TRACE_NOT_VISIBLE");
                assert!(message.contains("cannot view audit trace"));
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    #[tokio::test]
    async fn get_member_audit_trace_rejects_zero_page_limit() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let member = seed_member_audit_trail(&pool).await;
        let query_service = QueryProjectionService::new(SqlxUnitOfWorkFactory::new(pool));

        let error = query_service
            .get_member_audit_trace(
                GetMemberAuditTraceQuery {
                    global_member_id: member.global_member_id,
                    page: Some(PageRequest {
                        limit: Some(0),
                        cursor: None,
                    }),
                },
                ActorContext::new("human/admin-audit-2", ActorKind::HumanUser, None),
            )
            .await
            .expect_err("zero page limit should be rejected");

        assert!(matches!(
            error,
            IdentityError::RuleViolation {
                code: "IDENTITY_INVALID_ARGUMENT",
                ..
            }
        ));
    }

    #[tokio::test]
    async fn get_role_catalog_returns_local_summary_rows_with_filtering() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;
        seed_role(&pool, "role.member.reviewer", "Reviewer").await;
        rename_role_status(
            &pool,
            "role.member.reviewer",
            "Senior Reviewer",
            "source_drift",
        )
        .await;

        let query_service = QueryProjectionService::new(SqlxUnitOfWorkFactory::new(pool));
        let roles = query_service
            .get_role_catalog(
                GetRoleCatalogQuery {
                    filter: Some(RoleCatalogFilter {
                        status: Some("source_drift".to_string()),
                        role_ids: vec![RoleId::new("role.member.reviewer")],
                        keyword: Some("review".to_string()),
                    }),
                },
                ActorContext::new("human/admin-role-query-1", ActorKind::HumanUser, None),
            )
            .await
            .expect("role catalog query should succeed");

        assert_eq!(roles.len(), 1);
        assert_eq!(roles[0].role_id.as_str(), "role.member.reviewer");
        assert_eq!(roles[0].role_name, "Senior Reviewer");
        assert_eq!(roles[0].status, RoleCatalogStatus::SourceDrift);
        assert_eq!(roles[0].source_ref_json["id"], "role.member.reviewer");
    }

    #[tokio::test]
    async fn get_role_catalog_rejects_invalid_status_filter() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;

        let query_service = QueryProjectionService::new(SqlxUnitOfWorkFactory::new(pool));
        let error = query_service
            .get_role_catalog(
                GetRoleCatalogQuery {
                    filter: Some(RoleCatalogFilter {
                        status: Some("unknown".to_string()),
                        role_ids: Vec::new(),
                        keyword: None,
                    }),
                },
                ActorContext::new("human/admin-role-query-2", ActorKind::HumanUser, None),
            )
            .await
            .expect_err("invalid status should be rejected");

        assert!(matches!(
            error,
            IdentityError::RuleViolation {
                code: "IDENTITY_INVALID_ARGUMENT",
                ..
            }
        ));
    }

    async fn test_pool() -> sqlx::postgres::PgPool {
        let config = AppConfig {
            listen_addr: "127.0.0.1:8080".to_string(),
            database_url: Some(
                "postgres://postgres:postgres@127.0.0.1:5432/quantalithos_identity".to_string(),
            ),
            database_max_connections: 5,
        };

        let pool = PgPoolOptions::new()
            .max_connections(config.database_max_connections)
            .connect(
                config
                    .database_url
                    .as_deref()
                    .expect("database url should exist"),
            )
            .await
            .expect("connect test pool");
        run_migrations(&pool).await.expect("apply migrations");
        pool
    }

    async fn reset_tables(pool: &sqlx::postgres::PgPool) {
        pool.execute(
            r#"
            TRUNCATE TABLE
                inbound_dead_letters,
                projection_checkpoints,
                member_summary_projection,
                outbox_events,
                idempotency_records,
                audit_trace_entries,
                career_history_entries,
                lifecycle_history_entries,
                memory_refs,
                capability_profiles,
                global_members,
                role_catalog_entries
            RESTART IDENTITY CASCADE
            "#,
        )
        .await
        .expect("truncate test tables");
    }

    async fn seed_role(pool: &sqlx::postgres::PgPool, role_id: &str, role_name: &str) {
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
            ) VALUES ($1, $2, $3, $4, $5, $6, NOW())
            ON CONFLICT (role_id) DO NOTHING
            "#,
        )
        .bind(role_id)
        .bind(role_name)
        .bind("v1")
        .bind(json!({ "kind": "method_library_role", "id": role_id }))
        .bind(format!("fingerprint-{role_id}"))
        .bind("active")
        .execute(pool)
        .await
        .expect("seed role catalog entry");
    }

    async fn seed_member_audit_trail(
        pool: &sqlx::postgres::PgPool,
    ) -> crate::domain::member::GlobalMemberSummary {
        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let command_service = MemberLifecycleCommandService::new(factory.clone());
        let memory_service =
            MemoryRefsCommandService::new(factory.clone(), StubMemoryArchiveValidator);
        let career_consumer = CareerEventConsumer::new(CareerEventConsumerService::new(factory));
        let actor = ActorContext::new("human/admin-audit-seed", ActorKind::HumanUser, None);

        let member = command_service
            .hire_global_member(
                HireGlobalMemberCommand {
                    display_name: "Member Audit Seed".to_string(),
                    main_role_id: RoleId::new("role.member.operator"),
                    secondary_role_ids: Vec::new(),
                },
                actor.clone(),
                CommandMetadata::new(
                    "idem-audit-seed-hire",
                    "trace-audit-seed-hire",
                    "hash-audit-seed-hire",
                ),
            )
            .await
            .expect("hire member for audit seed");
        memory_service
            .update_memory_refs(
                UpdateMemoryRefsCommand {
                    global_member_id: member.global_member_id.clone(),
                    semantic_memory_ref: Some(MemoryRef {
                        memory_id: "memory-audit-seed".to_string(),
                        memory_kind: "semantic".to_string(),
                        memory_version: Some("v1".to_string()),
                    }),
                    episodic_memory_refs: Vec::new(),
                },
                actor.clone(),
                CommandMetadata::new(
                    "idem-audit-seed-memory",
                    "trace-audit-seed-memory",
                    "hash-audit-seed-memory",
                ),
            )
            .await
            .expect("update memory refs for audit seed");
        career_consumer
            .consume_work_event(sample_work_event(
                "audit-career-seed-event",
                "audit-career-seed-hash",
                member.global_member_id.as_str(),
            ))
            .await
            .expect("append career event for audit seed");

        update_audit_created_at(pool, "audit:idem-audit-seed-hire", now()).await;
        update_audit_created_at(
            pool,
            "audit:idem-audit-seed-memory",
            now() + Duration::seconds(1),
        )
        .await;
        update_audit_created_at(
            pool,
            "audit:audit-career-seed-event",
            now() + Duration::seconds(2),
        )
        .await;

        member
    }

    async fn rename_role_status(
        pool: &sqlx::postgres::PgPool,
        role_id: &str,
        role_name: &str,
        status: &str,
    ) {
        sqlx::query(
            "UPDATE role_catalog_entries SET role_name = $2, status = $3 WHERE role_id = $1",
        )
        .bind(role_id)
        .bind(role_name)
        .bind(status)
        .execute(pool)
        .await
        .expect("update role catalog row");
    }

    async fn update_audit_created_at(
        pool: &sqlx::postgres::PgPool,
        audit_trace_id: &str,
        created_at: PrimitiveDateTime,
    ) {
        sqlx::query("UPDATE audit_trace_entries SET created_at = $2 WHERE audit_trace_id = $1")
            .bind(audit_trace_id)
            .bind(created_at)
            .execute(pool)
            .await
            .expect("update audit trace created_at");
    }

    fn sample_work_event(
        source_event_id: &str,
        payload_hash: &str,
        global_member_id: &str,
    ) -> InboundWorkFactEvent {
        InboundWorkFactEvent {
            envelope: InboundEventEnvelope {
                source_event_id: EventId::new(source_event_id),
                source_module: "work".to_string(),
                event_type: "work.fact.recorded".to_string(),
                occurred_at: now(),
                payload_hash: payload_hash.to_string(),
                payload: json!({
                    "global_member_id": GlobalMemberId::new(global_member_id),
                    "project_id": ProjectId::new("project-audit-001"),
                    "work_ref": {
                        "work_id": "work-audit-001",
                        "work_kind": "task",
                        "work_version": "v1",
                    },
                    "entry_kind": "assigned",
                    "started_at": now(),
                    "ended_at": now() + Duration::seconds(30),
                    "payload_summary": {
                        "title": "Audit query work item",
                    }
                }),
            },
        }
    }

    fn now() -> PrimitiveDateTime {
        let now = OffsetDateTime::now_utc();
        PrimitiveDateTime::new(now.date(), now.time())
    }
}
