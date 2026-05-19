//! HTTP route registration and first real command/query adapters.

#[cfg(test)]
use std::sync::Mutex;

use axum::extract::rejection::{JsonRejection, QueryRejection};
use axum::extract::{FromRequest, FromRequestParts, Path, Query, Request, State};
use axum::http::{HeaderMap, StatusCode, request::Parts};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPool;
use time::{OffsetDateTime, PrimitiveDateTime};
use tokio::runtime::Handle;

use crate::application::capability_profile::CapabilityProfileCommandService;
use crate::application::member_lifecycle::MemberLifecycleCommandService;
use crate::application::memory_refs::MemoryRefsCommandService;
use crate::application::query_projection::{
    GetMemberAuditTraceQuery, GetMemberSummaryQuery, GetRoleCatalogQuery, MemberAuditTracePageDto,
    MemberSummaryDto, QueryProjectionService, RoleCatalogEntryDto, RoleCatalogFilter,
};
use crate::application::tombstone_flow::TombstoneFlowService;
use crate::domain::capability_profile::{
    ArtifactRef, CapabilityItem, CapabilityProfileSummary, UpdateCapabilityProfileCommand,
};
use crate::domain::member::{
    GlobalMemberLifecycle, GlobalMemberSummary, HireGlobalMemberCommand, UpdateLifecycleCommand,
};
use crate::domain::memory_refs::{
    ArchiveRef, MemoryRef, MemoryRefsSummary, UpdateMemoryRefsCommand,
};
use crate::domain::shared::context::{ActorContext, ActorKind};
use crate::domain::shared::ids::{GateDecisionId, GlobalMemberId, RoleId};
use crate::domain::shared::metadata::CommandMetadata;
use crate::domain::tombstone::{GateDecision, GateDecisionRef, TombstoneMemberCommand};
use crate::error::IdentityError;
use crate::outbound::{ArchiveRequestPort, ArtifactPort, GovernancePort, MemoryArchivePort};
use crate::persistence::unit_of_work::SqlxUnitOfWorkFactory;

const HEADER_ACTOR_REF: &str = "x-identity-actor-ref";
const HEADER_ACTOR_KIND: &str = "x-identity-actor-kind";
const HEADER_ACTOR_MEMBER_ID: &str = "x-identity-actor-member-id";
const HEADER_TRACE_ID: &str = "x-trace-id";
const HEADER_IDEMPOTENCY_KEY: &str = "idempotency-key";

/// Shared HTTP application state for the first production adapter slice.
#[derive(Debug, Clone)]
pub struct HttpAppState {
    /// Shared PostgreSQL pool cloned into each request-scoped service bridge.
    pool: PgPool,
}

impl HttpAppState {
    /// Creates one HTTP state bundle from the shared PostgreSQL pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Builds a fresh unit-of-work factory for one request or background bridge call.
    fn unit_of_work_factory(&self) -> SqlxUnitOfWorkFactory {
        SqlxUnitOfWorkFactory::new(self.pool.clone())
    }

    // NOTE: The service bridges below intentionally use `spawn_blocking` plus
    // `Handle::block_on` during the current phase. This keeps the HTTP adapter aligned with the
    // existing transaction-scoped application-service construction path while ensuring request
    // state is moved, not borrowed, across the blocking worker boundary.

    /// Executes the hire-member command service on the existing application boundary.
    ///
    /// # Errors
    ///
    /// Returns command, persistence, or task-join failures as `IdentityError`.
    async fn hire_global_member(
        &self,
        command: HireGlobalMemberCommand,
        actor: ActorContext,
        metadata: CommandMetadata,
    ) -> Result<GlobalMemberSummary, IdentityError> {
        let factory = self.unit_of_work_factory();
        tokio::task::spawn_blocking(move || {
            Handle::current().block_on(async move {
                MemberLifecycleCommandService::new(factory)
                    .hire_global_member(command, actor, metadata)
                    .await
            })
        })
        .await
        .map_err(|error| IdentityError::PersistenceData {
            message: format!("hire_global_member task join failed: {error}"),
        })?
    }

    /// Loads one member summary projection without mutating write-model truth.
    ///
    /// # Errors
    ///
    /// Returns projection query, persistence, or task-join failures as `IdentityError`.
    async fn get_member_summary(
        &self,
        query: GetMemberSummaryQuery,
        actor: ActorContext,
    ) -> Result<MemberSummaryDto, IdentityError> {
        let factory = self.unit_of_work_factory();
        tokio::task::spawn_blocking(move || {
            Handle::current().block_on(async move {
                QueryProjectionService::new(factory)
                    .get_member_summary(query, actor)
                    .await
            })
        })
        .await
        .map_err(|error| IdentityError::PersistenceData {
            message: format!("get_member_summary task join failed: {error}"),
        })?
    }

    /// Loads one member audit-trace page through the existing query service boundary.
    ///
    /// # Errors
    ///
    /// Returns visibility, query, persistence, or task-join failures as `IdentityError`.
    async fn get_member_audit_trace(
        &self,
        query: GetMemberAuditTraceQuery,
        actor: ActorContext,
    ) -> Result<MemberAuditTracePageDto, IdentityError> {
        let factory = self.unit_of_work_factory();
        tokio::task::spawn_blocking(move || {
            Handle::current().block_on(async move {
                QueryProjectionService::new(factory)
                    .get_member_audit_trace(query, actor)
                    .await
            })
        })
        .await
        .map_err(|error| IdentityError::PersistenceData {
            message: format!("get_member_audit_trace task join failed: {error}"),
        })?
    }

    /// Loads local role-catalog summary rows through the projection query service.
    ///
    /// # Errors
    ///
    /// Returns filter-validation, persistence, or task-join failures as `IdentityError`.
    async fn get_role_catalog(
        &self,
        query: GetRoleCatalogQuery,
        actor: ActorContext,
    ) -> Result<Vec<RoleCatalogEntryDto>, IdentityError> {
        let factory = self.unit_of_work_factory();
        tokio::task::spawn_blocking(move || {
            Handle::current().block_on(async move {
                QueryProjectionService::new(factory)
                    .get_role_catalog(query, actor)
                    .await
            })
        })
        .await
        .map_err(|error| IdentityError::PersistenceData {
            message: format!("get_role_catalog task join failed: {error}"),
        })?
    }

    /// Executes a lifecycle transition command on the current application boundary.
    ///
    /// # Errors
    ///
    /// Returns validation, persistence, or task-join failures as `IdentityError`.
    async fn update_lifecycle(
        &self,
        command: UpdateLifecycleCommand,
        actor: ActorContext,
        metadata: CommandMetadata,
    ) -> Result<GlobalMemberSummary, IdentityError> {
        let factory = self.unit_of_work_factory();
        tokio::task::spawn_blocking(move || {
            Handle::current().block_on(async move {
                MemberLifecycleCommandService::new(factory)
                    .update_lifecycle(command, actor, metadata)
                    .await
            })
        })
        .await
        .map_err(|error| IdentityError::PersistenceData {
            message: format!("update_lifecycle task join failed: {error}"),
        })?
    }

    /// Executes full capability-profile replacement using the current local artifact stub.
    ///
    /// # Errors
    ///
    /// Returns validation, persistence, or task-join failures as `IdentityError`.
    async fn update_capability_profile(
        &self,
        command: UpdateCapabilityProfileCommand,
        actor: ActorContext,
        metadata: CommandMetadata,
    ) -> Result<CapabilityProfileSummary, IdentityError> {
        let factory = self.unit_of_work_factory();
        tokio::task::spawn_blocking(move || {
            Handle::current().block_on(async move {
                CapabilityProfileCommandService::new(factory, NoopArtifactPort)
                    .update_capability_profile(command, actor, metadata)
                    .await
            })
        })
        .await
        .map_err(|error| IdentityError::PersistenceData {
            message: format!("update_capability_profile task join failed: {error}"),
        })?
    }

    /// Executes memory-ref replacement using the current local memory-archive stub.
    ///
    /// # Errors
    ///
    /// Returns validation, persistence, or task-join failures as `IdentityError`.
    async fn update_memory_refs(
        &self,
        command: UpdateMemoryRefsCommand,
        actor: ActorContext,
        metadata: CommandMetadata,
    ) -> Result<MemoryRefsSummary, IdentityError> {
        let factory = self.unit_of_work_factory();
        tokio::task::spawn_blocking(move || {
            Handle::current().block_on(async move {
                MemoryRefsCommandService::new(factory, NoopMemoryArchivePort)
                    .update_memory_refs(command, actor, metadata)
                    .await
            })
        })
        .await
        .map_err(|error| IdentityError::PersistenceData {
            message: format!("update_memory_refs task join failed: {error}"),
        })?
    }

    /// Executes the high-risk tombstone flow using the current local governance and archive stubs.
    ///
    /// # Errors
    ///
    /// Returns domain, persistence, or task-join failures as `IdentityError`.
    async fn tombstone_member(
        &self,
        command: TombstoneMemberCommand,
        actor: ActorContext,
        metadata: CommandMetadata,
    ) -> Result<GlobalMemberSummary, IdentityError> {
        let factory = self.unit_of_work_factory();
        tokio::task::spawn_blocking(move || {
            Handle::current().block_on(async move {
                TombstoneFlowService::new(factory, HttpStubGovernancePort, HttpStubArchiveRequester)
                    .tombstone_member(command, actor, metadata)
                    .await
            })
        })
        .await
        .map_err(|error| IdentityError::PersistenceData {
            message: format!("tombstone_member task join failed: {error}"),
        })?
    }
}

/// Builds the HTTP router for the current service state.
pub fn router(state: HttpAppState) -> Router {
    Router::new()
        .route("/health", get(health_check))
        .route("/identity/global-members", post(hire_global_member))
        .route(
            "/identity/global-members/{global_member_id}/lifecycle",
            post(update_lifecycle),
        )
        .route(
            "/identity/global-members/{global_member_id}/capability-profile",
            put(update_capability_profile),
        )
        .route(
            "/identity/global-members/{global_member_id}/memory-refs",
            put(update_memory_refs),
        )
        .route(
            "/identity/global-members/{global_member_id}/tombstone",
            post(tombstone_member),
        )
        .route(
            "/identity/global-members/{global_member_id}/summary",
            get(get_member_summary),
        )
        .route(
            "/identity/global-members/{global_member_id}/audit-trace",
            get(get_member_audit_trace),
        )
        .route("/identity/role-catalog", get(get_role_catalog))
        .with_state(state)
}

/// Reports process liveness for the current phase.
pub async fn health_check() -> impl IntoResponse {
    (StatusCode::OK, Json(HealthResponse { status: "ok" }))
}

/// Trait implemented by command HTTP DTOs that can derive the stable idempotency request hash.
trait CommandTransportPayload: Serialize {
    /// Stable operation label included in the request hash payload.
    const OPERATION: &'static str;

    /// Builds the canonical request-hash payload consumed by `CommandMetadata`.
    fn request_hash_payload(&self) -> Result<String, ApiError> {
        request_hash_payload(Self::OPERATION, self)
    }
}

/// Trusted actor extractor that only consumes gateway-injected headers.
struct TrustedActor(ActorContext);

impl<S> FromRequestParts<S> for TrustedActor
where
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        actor_context_from_headers(&parts.headers).map(Self)
    }
}

/// Transport-local view of command metadata headers before request hashing is complete.
struct CommandMetadataHeaders {
    /// Idempotency key copied from trusted request headers.
    idempotency_key: String,
    /// Trace id copied from trusted request headers.
    trace_id: String,
}

impl CommandMetadataHeaders {
    /// Completes one command metadata object once the request hash is known.
    fn into_metadata(self, request_hash: String) -> CommandMetadata {
        CommandMetadata::new(self.idempotency_key, self.trace_id, request_hash)
    }
}

impl<S> FromRequestParts<S> for CommandMetadataHeaders
where
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, _state: &S) -> Result<Self, Self::Rejection> {
        Ok(Self {
            idempotency_key: required_header(&parts.headers, HEADER_IDEMPOTENCY_KEY)?,
            trace_id: required_header(&parts.headers, HEADER_TRACE_ID)?,
        })
    }
}

/// JSON extractor that maps all body parsing failures into the stable identity error envelope.
struct ApiJson<T>(T);

impl<S, T> FromRequest<S> for ApiJson<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = ApiError;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        let Json(value) = Json::<T>::from_request(request, state)
            .await
            .map_err(ApiError::from)?;
        Ok(Self(value))
    }
}

/// Query-string extractor that maps axum query parsing failures into the stable identity error envelope.
struct ApiQuery<T>(T);

impl<S, T> FromRequestParts<S> for ApiQuery<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let Query(value) = Query::<T>::from_request_parts(parts, state)
            .await
            .map_err(ApiError::from)?;
        Ok(Self(value))
    }
}

/// Command extractor that standardizes trusted actor injection, command metadata parsing, and JSON DTO loading.
struct CommandRequest<T> {
    /// Trusted actor context injected by the gateway.
    actor: ActorContext,
    /// Request metadata built from trusted transport headers plus the request hash.
    metadata: CommandMetadata,
    /// Parsed command DTO.
    payload: T,
}

impl<T> CommandRequest<T> {
    /// Splits the extractor output into the parsed DTO and transport context values.
    fn into_parts(self) -> (T, ActorContext, CommandMetadata) {
        (self.payload, self.actor, self.metadata)
    }
}

impl<S, T> FromRequest<S> for CommandRequest<T>
where
    S: Send + Sync,
    T: CommandTransportPayload + DeserializeOwned,
{
    type Rejection = ApiError;

    async fn from_request(request: Request, state: &S) -> Result<Self, Self::Rejection> {
        let (mut parts, body) = request.into_parts();
        let TrustedActor(actor) = TrustedActor::from_request_parts(&mut parts, state).await?;
        let metadata_headers =
            CommandMetadataHeaders::from_request_parts(&mut parts, state).await?;
        let ApiJson(payload) =
            ApiJson::<T>::from_request(Request::from_parts(parts, body), state).await?;
        let metadata = metadata_headers.into_metadata(payload.request_hash_payload()?);

        Ok(Self {
            actor,
            metadata,
            payload,
        })
    }
}

async fn hire_global_member(
    State(state): State<HttpAppState>,
    command: CommandRequest<HireGlobalMemberRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let (request, actor, metadata) = command.into_parts();
    let summary = state
        .hire_global_member(request.into_command(), actor, metadata)
        .await
        .map_err(ApiError::from)?;

    Ok((StatusCode::CREATED, Json(summary)))
}

/// Handles lifecycle transition requests for one existing member.
///
/// # Errors
///
/// Returns stable transport errors when path/header/body parsing fails or when the application
/// service rejects the requested transition.
async fn update_lifecycle(
    State(state): State<HttpAppState>,
    Path(global_member_id): Path<String>,
    command: CommandRequest<UpdateLifecycleRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let (request, actor, metadata) = command.into_parts();
    let summary = state
        .update_lifecycle(request.into_command(global_member_id), actor, metadata)
        .await
        .map_err(ApiError::from)?;

    Ok((StatusCode::OK, Json(summary)))
}

/// Handles full capability-profile replacement for one member.
///
/// # Errors
///
/// Returns stable transport errors when path/header/body parsing fails or when the application
/// service rejects the new capability snapshot.
async fn update_capability_profile(
    State(state): State<HttpAppState>,
    Path(global_member_id): Path<String>,
    command: CommandRequest<UpdateCapabilityProfileRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let (request, actor, metadata) = command.into_parts();
    let summary = state
        .update_capability_profile(request.into_command(global_member_id), actor, metadata)
        .await
        .map_err(ApiError::from)?;

    Ok((StatusCode::OK, Json(summary)))
}

/// Handles memory-ref replacement for one member.
///
/// # Errors
///
/// Returns stable transport errors when path/header/body parsing fails or when the application
/// service rejects the submitted memory refs.
async fn update_memory_refs(
    State(state): State<HttpAppState>,
    Path(global_member_id): Path<String>,
    command: CommandRequest<UpdateMemoryRefsRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let (request, actor, metadata) = command.into_parts();
    let summary = state
        .update_memory_refs(request.into_command(global_member_id), actor, metadata)
        .await
        .map_err(ApiError::from)?;

    Ok((StatusCode::OK, Json(summary)))
}

/// Handles the tombstone command endpoint for one member.
///
/// # Errors
///
/// Returns stable transport errors when path/header/body parsing fails or when governance,
/// archive, or domain validation rejects the tombstone flow.
async fn tombstone_member(
    State(state): State<HttpAppState>,
    Path(global_member_id): Path<String>,
    command: CommandRequest<TombstoneMemberRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let (request, actor, metadata) = command.into_parts();
    let summary = state
        .tombstone_member(request.into_command(global_member_id), actor, metadata)
        .await
        .map_err(ApiError::from)?;

    Ok((StatusCode::OK, Json(summary)))
}

/// Returns the current projection-backed summary for one member.
///
/// # Errors
///
/// Returns stable transport errors when path/header parsing fails or when the projection query
/// reports `not_found` or `not_ready` semantics.
async fn get_member_summary(
    State(state): State<HttpAppState>,
    Path(global_member_id): Path<String>,
    TrustedActor(actor): TrustedActor,
) -> Result<impl IntoResponse, ApiError> {
    let summary = state
        .get_member_summary(
            GetMemberSummaryQuery {
                global_member_id: GlobalMemberId::new(global_member_id),
            },
            actor,
        )
        .await
        .map_err(ApiError::from)?;

    Ok((StatusCode::OK, Json(summary)))
}

/// Returns one audit-trace page for the requested member.
///
/// # Errors
///
/// Returns stable transport errors when path/header/query parsing fails or when visibility or
/// member existence checks reject the request.
async fn get_member_audit_trace(
    State(state): State<HttpAppState>,
    Path(global_member_id): Path<String>,
    TrustedActor(actor): TrustedActor,
    ApiQuery(request): ApiQuery<GetMemberAuditTraceRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let query = request.into_query(global_member_id)?;
    let result = state
        .get_member_audit_trace(query, actor)
        .await
        .map_err(ApiError::from)?;

    Ok((StatusCode::OK, Json(result)))
}

/// Returns local role-catalog summary rows using normalized query filters.
///
/// # Errors
///
/// Returns stable transport errors when header/query parsing fails or when the query service
/// rejects unsupported filter values.
async fn get_role_catalog(
    State(state): State<HttpAppState>,
    TrustedActor(actor): TrustedActor,
    ApiQuery(request): ApiQuery<GetRoleCatalogRequest>,
) -> Result<impl IntoResponse, ApiError> {
    let result = state
        .get_role_catalog(request.into_query()?, actor)
        .await
        .map_err(ApiError::from)?;

    Ok((StatusCode::OK, Json(result)))
}

/// Builds one trusted actor context snapshot from the identity HTTP headers.
fn actor_context_from_headers(headers: &HeaderMap) -> Result<ActorContext, ApiError> {
    let actor_ref = required_header(headers, HEADER_ACTOR_REF)?;
    let actor_kind = actor_kind_from_header(&required_header(headers, HEADER_ACTOR_KIND)?)?;
    let global_member_id = match actor_kind {
        ActorKind::AiMember => Some(GlobalMemberId::new(required_header(
            headers,
            HEADER_ACTOR_MEMBER_ID,
        )?)),
        ActorKind::HumanUser | ActorKind::System => None,
    };

    Ok(ActorContext::new(actor_ref, actor_kind, global_member_id))
}

/// Builds a stable request hash payload string from the operation name and serialized request.
fn request_hash_payload<T>(operation: &'static str, request: &T) -> Result<String, ApiError>
where
    T: Serialize + ?Sized,
{
    let json = serde_json::to_string(request).map_err(|error| {
        ApiError::bad_request(
            "IDENTITY_INVALID_ARGUMENT",
            format!("request payload for `{operation}` could not be serialized: {error}"),
        )
    })?;

    Ok(format!("{operation}|payload={json}"))
}

/// Parses the canonical transport representation of one actor kind.
fn actor_kind_from_header(value: &str) -> Result<ActorKind, ApiError> {
    match value {
        "human" | "human_user" => Ok(ActorKind::HumanUser),
        "ai_member" => Ok(ActorKind::AiMember),
        "system" => Ok(ActorKind::System),
        _ => Err(ApiError::bad_request(
            "IDENTITY_INVALID_ARGUMENT",
            format!(
                "header `{HEADER_ACTOR_KIND}` must be one of `human`, `ai_member`, or `system`"
            ),
        )),
    }
}

/// Loads one required trusted header and rejects missing, non-UTF8, or blank values.
fn required_header(headers: &HeaderMap, name: &'static str) -> Result<String, ApiError> {
    let value = headers
        .get(name)
        .ok_or_else(|| {
            ApiError::bad_request(
                "IDENTITY_INVALID_ARGUMENT",
                format!("required header `{name}` is missing"),
            )
        })?
        .to_str()
        .map_err(|_| {
            ApiError::bad_request(
                "IDENTITY_INVALID_ARGUMENT",
                format!("header `{name}` must be valid UTF-8"),
            )
        })?
        .trim()
        .to_string();

    if value.is_empty() {
        return Err(ApiError::bad_request(
            "IDENTITY_INVALID_ARGUMENT",
            format!("header `{name}` must not be blank"),
        ));
    }

    Ok(value)
}

/// Builds default approved governance evidence for HTTP-only tombstone adapter tests and stubs.
fn approved_gate_decision_http(gate_decision_id: &str) -> GateDecisionRef {
    GateDecisionRef {
        gate_decision_id: GateDecisionId::new(gate_decision_id),
        decision: GateDecision::Approved,
        policy_ref_json: serde_json::json!({
            "kind": "governance_policy",
            "id": "policy-http-default",
        }),
        decided_at: gate_decision_decided_at_http(),
    }
}

#[cfg(test)]
/// Builds rejected governance evidence for HTTP tombstone transport tests.
fn rejected_gate_decision_http(gate_decision_id: &str) -> GateDecisionRef {
    GateDecisionRef {
        gate_decision_id: GateDecisionId::new(gate_decision_id),
        decision: GateDecision::Rejected,
        policy_ref_json: serde_json::json!({
            "kind": "governance_policy",
            "id": "policy-http-rejected",
        }),
        decided_at: gate_decision_decided_at_http(),
    }
}

/// Returns the current UTC timestamp in the primitive form used by governance evidence refs.
fn gate_decision_decided_at_http() -> PrimitiveDateTime {
    let now = OffsetDateTime::now_utc();
    PrimitiveDateTime::new(now.date(), now.time())
}

/// Transport-local error envelope used to map domain and persistence failures into HTTP.
#[derive(Debug)]
struct ApiError {
    /// Final HTTP status returned to the caller.
    status: StatusCode,
    /// Stable machine-readable error body.
    body: ErrorResponse,
}

impl ApiError {
    /// Creates one bad-request error with the stable identity error response shape.
    fn bad_request(error: &'static str, message: String) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            body: ErrorResponse {
                error: error.to_string(),
                message,
            },
        }
    }
}

impl From<JsonRejection> for ApiError {
    fn from(rejection: JsonRejection) -> Self {
        Self::bad_request(
            "IDENTITY_INVALID_ARGUMENT",
            format!("request body is invalid: {}", rejection.body_text()),
        )
    }
}

impl From<QueryRejection> for ApiError {
    fn from(rejection: QueryRejection) -> Self {
        Self::bad_request(
            "IDENTITY_INVALID_ARGUMENT",
            format!("query string is invalid: {}", rejection.body_text()),
        )
    }
}

impl From<IdentityError> for ApiError {
    fn from(error: IdentityError) -> Self {
        match error {
            IdentityError::RuleViolation { code, message } => Self {
                status: status_for_rule_violation(code),
                body: ErrorResponse {
                    error: code.to_string(),
                    message,
                },
            },
            IdentityError::VersionConflict { entity } => Self {
                status: StatusCode::CONFLICT,
                body: ErrorResponse {
                    error: "IDENTITY_VERSION_CONFLICT".to_string(),
                    message: format!("version conflict while saving `{entity}`"),
                },
            },
            IdentityError::MissingDatabaseUrl => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                body: ErrorResponse {
                    error: "IDENTITY_INTERNAL_ERROR".to_string(),
                    message: "database configuration is missing `DATABASE_URL`".to_string(),
                },
            },
            IdentityError::InvalidConfiguration { key, reason } => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                body: ErrorResponse {
                    error: "IDENTITY_INTERNAL_ERROR".to_string(),
                    message: format!("invalid configuration for `{key}`: {reason}"),
                },
            },
            IdentityError::DatabasePool(source) => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                body: ErrorResponse {
                    error: "IDENTITY_INTERNAL_ERROR".to_string(),
                    message: format!("database pool initialization failed: {source}"),
                },
            },
            IdentityError::DatabaseMigration(source) => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                body: ErrorResponse {
                    error: "IDENTITY_INTERNAL_ERROR".to_string(),
                    message: format!("database migration failed: {source}"),
                },
            },
            IdentityError::PersistenceData { message } => Self {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                body: ErrorResponse {
                    error: "IDENTITY_INTERNAL_ERROR".to_string(),
                    message,
                },
            },
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(self.body)).into_response()
    }
}

/// Maps stable identity rule-violation codes into the documented HTTP status contract.
fn status_for_rule_violation(code: &str) -> StatusCode {
    match code {
        "IDENTITY_MEMBER_NOT_FOUND" | "IDENTITY_ROLE_NOT_FOUND" => StatusCode::NOT_FOUND,
        "IDENTITY_PROJECTION_NOT_READY" => StatusCode::SERVICE_UNAVAILABLE,
        "IDENTITY_MEMORY_ARCHIVE_UNAVAILABLE" => StatusCode::SERVICE_UNAVAILABLE,
        "IDENTITY_GATE_REJECTED" | "IDENTITY_AUDIT_TRACE_NOT_VISIBLE" => StatusCode::FORBIDDEN,
        "IDENTITY_LIFECYCLE_TRANSITION_INVALID"
        | "IDENTITY_USE_TOMBSTONE_COMMAND"
        | "IDENTITY_MEMBER_NOT_MUTABLE"
        | "IDENTITY_IDEMPOTENCY_CONFLICT" => StatusCode::CONFLICT,
        "IDENTITY_INVALID_ARGUMENT"
        | "IDENTITY_ARTIFACT_REF_INVALID"
        | "IDENTITY_MEMORY_REF_INVALID" => StatusCode::BAD_REQUEST,
        _ => StatusCode::UNPROCESSABLE_ENTITY,
    }
}

/// Health-check DTO used by the service.
#[derive(Debug, Clone, Copy, Serialize)]
struct HealthResponse {
    /// Process liveness marker.
    status: &'static str,
}

/// Stable error DTO returned by implemented handlers.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ErrorResponse {
    /// Stable machine-readable error code.
    error: String,
    /// Human-readable message for diagnostics.
    message: String,
}

/// Transport DTO for the first explicit member-create endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct HireGlobalMemberRequest {
    /// User-facing display name for the new member.
    display_name: String,
    /// Main role reference that must exist in the local role catalog.
    main_role_id: String,
    /// Optional ordered secondary role references.
    #[serde(default)]
    secondary_role_ids: Vec<String>,
}

impl HireGlobalMemberRequest {
    /// Converts the transport DTO into the command DTO consumed by the lifecycle service.
    fn into_command(self) -> HireGlobalMemberCommand {
        HireGlobalMemberCommand {
            display_name: self.display_name,
            main_role_id: RoleId::new(self.main_role_id),
            secondary_role_ids: self
                .secondary_role_ids
                .into_iter()
                .map(RoleId::new)
                .collect(),
        }
    }
}

impl CommandTransportPayload for HireGlobalMemberRequest {
    const OPERATION: &'static str = "hire_global_member";
}

/// Transport DTO for lifecycle transitions requested through the HTTP command API.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct UpdateLifecycleRequest {
    /// Target lifecycle requested by the caller.
    target_lifecycle: GlobalMemberLifecycle,
    /// Human-readable reason retained in audit and history.
    reason: String,
    /// Optional optimistic-lock version expected by the caller.
    expected_version: Option<i64>,
}

impl UpdateLifecycleRequest {
    /// Converts the transport DTO into the lifecycle command scoped to one member id.
    fn into_command(self, global_member_id: String) -> UpdateLifecycleCommand {
        UpdateLifecycleCommand {
            global_member_id: GlobalMemberId::new(global_member_id),
            target_lifecycle: self.target_lifecycle,
            reason: self.reason,
            expected_version: self.expected_version,
        }
    }
}

impl CommandTransportPayload for UpdateLifecycleRequest {
    const OPERATION: &'static str = "update_lifecycle";
}

/// Transport DTO for full capability-profile replacement through the HTTP command API.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct UpdateCapabilityProfileRequest {
    /// Full capability snapshot that replaces the current local summary.
    capabilities: Vec<CapabilityItem>,
    /// Evidence refs retained as ref-only artifacts.
    evidence_refs: Vec<ArtifactRef>,
    /// Optional optimistic-lock version expected by the caller.
    expected_version: Option<i64>,
}

impl UpdateCapabilityProfileRequest {
    /// Converts the transport DTO into the capability-profile command for one member.
    fn into_command(self, global_member_id: String) -> UpdateCapabilityProfileCommand {
        UpdateCapabilityProfileCommand {
            global_member_id: GlobalMemberId::new(global_member_id),
            capabilities: self.capabilities,
            evidence_refs: self.evidence_refs,
            expected_version: self.expected_version,
        }
    }
}

impl CommandTransportPayload for UpdateCapabilityProfileRequest {
    const OPERATION: &'static str = "update_capability_profile";
}

/// Transport DTO for memory-ref replacement through the HTTP command API.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct UpdateMemoryRefsRequest {
    /// Optional semantic memory ref snapshot retained for the member.
    semantic_memory_ref: Option<MemoryRef>,
    /// Optional episodic memory ref snapshots retained for the member.
    #[serde(default)]
    episodic_memory_refs: Vec<MemoryRef>,
}

impl UpdateMemoryRefsRequest {
    /// Converts the transport DTO into the memory-ref command for one member.
    fn into_command(self, global_member_id: String) -> UpdateMemoryRefsCommand {
        UpdateMemoryRefsCommand {
            global_member_id: GlobalMemberId::new(global_member_id),
            semantic_memory_ref: self.semantic_memory_ref,
            episodic_memory_refs: self.episodic_memory_refs,
        }
    }
}

impl CommandTransportPayload for UpdateMemoryRefsRequest {
    const OPERATION: &'static str = "update_memory_refs";
}

/// Transport DTO for the high-risk tombstone command endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct TombstoneMemberRequest {
    /// Human-readable reason retained for audit and archive coordination.
    reason: String,
    /// Optional optimistic-lock version expected by the caller.
    expected_version: Option<i64>,
    /// Optional governance evidence supplied by the caller when already available.
    gate_decision_ref: Option<GateDecisionRef>,
}

impl TombstoneMemberRequest {
    /// Converts the transport DTO into the tombstone command scoped to one member id.
    fn into_command(self, global_member_id: String) -> TombstoneMemberCommand {
        TombstoneMemberCommand {
            global_member_id: GlobalMemberId::new(global_member_id),
            reason: self.reason,
            expected_version: self.expected_version,
            gate_decision_ref: self.gate_decision_ref,
        }
    }
}

impl CommandTransportPayload for TombstoneMemberRequest {
    const OPERATION: &'static str = "tombstone_member";
}

/// Query-string DTO for the audit-trace endpoint.
#[derive(Debug, Clone, Deserialize, Default)]
struct GetMemberAuditTraceRequest {
    /// Optional page size override passed as `?limit=<u32>`.
    limit: Option<u32>,
    /// Optional opaque pagination cursor passed as `?cursor=<audit_trace_id>`.
    cursor: Option<String>,
}

impl GetMemberAuditTraceRequest {
    /// Converts raw query-string fields into the shared audit-trace query DTO.
    fn into_query(self, global_member_id: String) -> Result<GetMemberAuditTraceQuery, ApiError> {
        let cursor = match self.cursor {
            None => None,
            Some(cursor) => {
                let trimmed = cursor.trim().to_string();
                if trimmed.is_empty() {
                    return Err(ApiError::bad_request(
                        "IDENTITY_INVALID_ARGUMENT",
                        "query parameter `cursor` must not be blank".to_string(),
                    ));
                }
                Some(trimmed)
            }
        };

        let page = if self.limit.is_none() && cursor.is_none() {
            None
        } else {
            Some(crate::domain::shared::pagination::PageRequest {
                limit: self.limit,
                cursor,
            })
        };

        Ok(GetMemberAuditTraceQuery {
            global_member_id: GlobalMemberId::new(global_member_id),
            page,
        })
    }
}

/// Query-string DTO for the role-catalog endpoint.
#[derive(Debug, Clone, Deserialize, Default)]
struct GetRoleCatalogRequest {
    /// Optional status filter passed as `?status=active|deprecated|source_drift`.
    status: Option<String>,
    /// Optional comma-separated role id subset passed as `?role_ids=id1,id2`.
    role_ids: Option<String>,
    /// Optional case-insensitive display-name keyword passed as `?keyword=value`.
    keyword: Option<String>,
}

impl GetRoleCatalogRequest {
    /// Converts raw query-string filters into the shared role-catalog query DTO.
    fn into_query(self) -> Result<GetRoleCatalogQuery, ApiError> {
        let role_ids = match self.role_ids {
            None => Vec::new(),
            Some(raw) => raw
                .split(',')
                .map(str::trim)
                .map(|value| {
                    if value.is_empty() {
                        Err(ApiError::bad_request(
                            "IDENTITY_INVALID_ARGUMENT",
                            "query parameter `role_ids` must not contain blank role ids"
                                .to_string(),
                        ))
                    } else {
                        Ok(RoleId::new(value))
                    }
                })
                .collect::<Result<Vec<_>, _>>()?,
        };

        Ok(GetRoleCatalogQuery {
            filter: Some(RoleCatalogFilter {
                status: self.status,
                role_ids,
                keyword: self.keyword,
            }),
        })
    }
}

/// Artifact-port stub used by the current HTTP adapter phase.
///
/// NOTE: This stub only validates local artifact-ref shape. It does not prove upstream artifact
/// existence, authorization, or reachability.
#[derive(Debug, Clone, Copy, Default)]
struct NoopArtifactPort;

impl ArtifactPort for NoopArtifactPort {
    async fn validate_refs(&self, refs: &[ArtifactRef]) -> Result<(), IdentityError> {
        for artifact_ref in refs {
            artifact_ref.validate()?;
        }

        Ok(())
    }
}

/// Memory-archive validation stub used by the current HTTP adapter phase.
///
/// NOTE: This stub only validates local memory-ref shape. It does not call the real memory-archive
/// boundary or reserve external archive capacity.
#[derive(Debug, Clone, Copy, Default)]
struct NoopMemoryArchivePort;

impl MemoryArchivePort for NoopMemoryArchivePort {
    async fn validate_ref(&self, memory_ref: &MemoryRef) -> Result<(), IdentityError> {
        memory_ref.validate()?;
        Ok(())
    }
}

/// Governance-port stub used by the tombstone HTTP adapter.
///
/// CAUTION: When the caller omits governance evidence, this stub synthesizes a default approved
/// decision so the local transport flow can be exercised before the real governance integration
/// lands. Successful tombstone requests here therefore do not prove end-to-end governance checks.
#[derive(Debug, Clone, Copy, Default)]
struct HttpStubGovernancePort;

impl GovernancePort for HttpStubGovernancePort {
    async fn require_gate_decision(
        &self,
        _action_name: &str,
        _member: &crate::domain::member::GlobalMember,
        _actor: &ActorContext,
        _reason: &str,
        supplied_gate_ref: Option<&GateDecisionRef>,
    ) -> Result<GateDecisionRef, IdentityError> {
        if let Some(gate_ref) = supplied_gate_ref {
            return Ok(gate_ref.clone());
        }

        Ok(approved_gate_decision_http("gate-http-default"))
    }
}

/// Archive-request stub used by the tombstone HTTP adapter.
///
/// CAUTION: This stub fabricates archive refs locally and only exposes failure injection in tests.
/// It does not prove downstream archive acceptance, persistence, or recovery semantics.
#[derive(Debug, Clone, Copy, Default)]
struct HttpStubArchiveRequester;

impl ArchiveRequestPort for HttpStubArchiveRequester {
    async fn request_archive(
        &self,
        global_member_id: &GlobalMemberId,
        _reason: &str,
    ) -> Result<ArchiveRef, IdentityError> {
        #[cfg(test)]
        if let Some(message) = http_stub_archive_failure_message() {
            return Err(IdentityError::PersistenceData { message });
        }

        Ok(ArchiveRef {
            archive_id: format!("archive:{}", global_member_id.as_str()),
            archive_kind: "memory_archive".to_string(),
            archive_version: Some("v1".to_string()),
        })
    }
}

#[cfg(test)]
static HTTP_STUB_ARCHIVE_FAILURE: Mutex<Option<String>> = Mutex::new(None);

#[cfg(test)]
fn http_stub_archive_failure_message() -> Option<String> {
    HTTP_STUB_ARCHIVE_FAILURE
        .lock()
        .expect("lock archive failure override")
        .clone()
}

#[cfg(test)]
/// Test-only guard that forces the HTTP archive requester stub to fail while it is held.
struct HttpStubArchiveFailureGuard;

#[cfg(test)]
impl HttpStubArchiveFailureGuard {
    /// Enables deterministic archive-request failure injection for the duration of the guard.
    fn failing(message: &str) -> Self {
        *HTTP_STUB_ARCHIVE_FAILURE
            .lock()
            .expect("lock archive failure override") = Some(message.to_string());
        Self
    }
}

#[cfg(test)]
impl Drop for HttpStubArchiveFailureGuard {
    fn drop(&mut self) {
        *HTTP_STUB_ARCHIVE_FAILURE
            .lock()
            .expect("lock archive failure override") = None;
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::body::{Body, to_bytes};
    use axum::http::{HeaderValue, Request, StatusCode};
    use axum::response::Response;
    use serde::de::DeserializeOwned;
    use serde_json::json;
    use sqlx::{Executor, Row, postgres::PgPoolOptions};
    use tower::util::ServiceExt;

    use crate::application::career_event::CareerEventConsumerService;
    use crate::application::member_lifecycle::MemberLifecycleCommandService;
    use crate::application::memory_refs::MemoryRefsCommandService;
    use crate::application::query_projection::MemberSummaryDto;
    use crate::application::query_projection::{MemberAuditTracePageDto, RoleCatalogEntryDto};
    use crate::config::AppConfig;
    use crate::domain::capability_profile::{
        ArtifactRef, CapabilityItem, CapabilityProfileSummary,
    };
    use crate::domain::member::{GlobalMemberLifecycle, GlobalMemberSummary};
    use crate::domain::memory_refs::{MemoryRef, MemoryRefsSummary, UpdateMemoryRefsCommand};
    use crate::domain::role_catalog::RoleCatalogStatus;
    use crate::domain::shared::context::{ActorContext, ActorKind};
    use crate::domain::shared::ids::{EventId, GlobalMemberId, ProjectId, RoleId};
    use crate::domain::shared::metadata::CommandMetadata;
    use crate::error::IdentityError;
    use crate::inbound::event_consumers::CareerEventConsumer;
    use crate::inbound::events::{InboundEventEnvelope, InboundWorkFactEvent};
    use crate::operations::ProjectionRebuildJob;
    use crate::outbound::MemoryArchivePort;
    use crate::persistence::database::run_migrations;
    use crate::persistence::test_support::DB_TEST_MUTEX;
    use crate::persistence::unit_of_work::SqlxUnitOfWorkFactory;
    use time::{Duration, OffsetDateTime, PrimitiveDateTime};

    use super::{
        ErrorResponse, HEADER_ACTOR_KIND, HEADER_ACTOR_MEMBER_ID, HEADER_ACTOR_REF,
        HEADER_IDEMPOTENCY_KEY, HEADER_TRACE_ID, HttpAppState, HttpStubArchiveFailureGuard,
        rejected_gate_decision_http, router,
    };

    #[tokio::test]
    async fn health_endpoint_returns_ok() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;

        let response = router(HttpAppState::new(pool))
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn hire_endpoint_creates_member() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let response = router(HttpAppState::new(pool.clone()))
            .oneshot(hire_request(
                "idem-http-hire-001",
                "trace-http-hire-001",
                json!({
                    "display_name": "Member Http One",
                    "main_role_id": "role.member.operator",
                    "secondary_role_ids": ["role.secondary.reviewer"]
                }),
            ))
            .await
            .expect("router should respond");

        assert_eq!(response.status(), StatusCode::CREATED);
        let summary: GlobalMemberSummary = read_json_body(response).await;
        assert_eq!(summary.display_name, "Member Http One");
        assert_eq!(summary.main_role_id.as_str(), "role.member.operator");

        let persisted_display_name =
            sqlx::query("SELECT display_name FROM global_members WHERE global_member_id = $1")
                .bind(summary.global_member_id.as_str())
                .fetch_one(&pool)
                .await
                .expect("member row should exist")
                .get::<String, _>("display_name");

        assert_eq!(persisted_display_name, "Member Http One");
    }

    #[tokio::test]
    async fn hire_endpoint_reuses_previous_result_for_same_idempotency_key_and_payload() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let app = router(HttpAppState::new(pool.clone()));
        let payload = json!({
            "display_name": "Member Http Two",
            "main_role_id": "role.member.operator",
            "secondary_role_ids": []
        });

        let first = app
            .clone()
            .oneshot(hire_request(
                "idem-http-hire-002",
                "trace-http-hire-002a",
                payload.clone(),
            ))
            .await
            .expect("first request should succeed");
        let second = app
            .oneshot(hire_request(
                "idem-http-hire-002",
                "trace-http-hire-002b",
                payload,
            ))
            .await
            .expect("second request should succeed");

        assert_eq!(first.status(), StatusCode::CREATED);
        assert_eq!(second.status(), StatusCode::CREATED);

        let first_body: GlobalMemberSummary = read_json_body(first).await;
        let second_body: GlobalMemberSummary = read_json_body(second).await;
        assert_eq!(first_body, second_body);
    }

    #[tokio::test]
    async fn hire_endpoint_returns_conflict_for_same_idempotency_key_with_different_payload() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let app = router(HttpAppState::new(pool));
        let first = app
            .clone()
            .oneshot(hire_request(
                "idem-http-hire-003",
                "trace-http-hire-003a",
                json!({
                    "display_name": "Member Http Three",
                    "main_role_id": "role.member.operator",
                    "secondary_role_ids": []
                }),
            ))
            .await
            .expect("first request should succeed");
        let second = app
            .oneshot(hire_request(
                "idem-http-hire-003",
                "trace-http-hire-003b",
                json!({
                    "display_name": "Member Http Three Changed",
                    "main_role_id": "role.member.operator",
                    "secondary_role_ids": []
                }),
            ))
            .await
            .expect("second request should respond");

        assert_eq!(first.status(), StatusCode::CREATED);
        assert_eq!(second.status(), StatusCode::CONFLICT);

        let error: ErrorResponse = read_json_body(second).await;
        assert_eq!(error.error, "IDENTITY_IDEMPOTENCY_CONFLICT");
    }

    #[tokio::test]
    async fn command_endpoints_reject_missing_idempotency_header_with_stable_error_shape() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let response = router(HttpAppState::new(pool))
            .oneshot(json_request_without_idempotency(
                "POST",
                "/identity/global-members",
                "trace-http-hire-missing-idem",
                json!({
                    "display_name": "Member Missing Idem",
                    "main_role_id": "role.member.operator",
                    "secondary_role_ids": []
                }),
            ))
            .await
            .expect("router should respond");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let error: ErrorResponse = read_json_body(response).await;
        assert_eq!(error.error, "IDENTITY_INVALID_ARGUMENT");
        assert!(error.message.contains(HEADER_IDEMPOTENCY_KEY));
    }

    #[tokio::test]
    async fn query_endpoints_reject_ai_actor_without_member_id_with_stable_error_shape() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;

        let response = router(HttpAppState::new(pool))
            .oneshot(summary_request_as_ai_without_member_id(
                "member:missing-ai-header",
            ))
            .await
            .expect("router should respond");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let error: ErrorResponse = read_json_body(response).await;
        assert_eq!(error.error, "IDENTITY_INVALID_ARGUMENT");
        assert!(error.message.contains(HEADER_ACTOR_MEMBER_ID));
    }

    #[tokio::test]
    async fn query_endpoints_map_query_parse_failures_into_stable_error_shape() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;

        let response = router(HttpAppState::new(pool))
            .oneshot(audit_trace_request(
                "member-query-parse-001",
                Some("limit=abc"),
            ))
            .await
            .expect("router should respond");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let error: ErrorResponse = read_json_body(response).await;
        assert_eq!(error.error, "IDENTITY_INVALID_ARGUMENT");
        assert!(error.message.contains("query string is invalid"));
    }

    #[tokio::test]
    async fn command_endpoints_map_json_parse_failures_into_stable_error_shape() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let response = router(HttpAppState::new(pool))
            .oneshot(invalid_json_request(
                "POST",
                "/identity/global-members",
                "idem-http-hire-invalid-json",
                "trace-http-hire-invalid-json",
                "{",
            ))
            .await
            .expect("router should respond");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let error: ErrorResponse = read_json_body(response).await;
        assert_eq!(error.error, "IDENTITY_INVALID_ARGUMENT");
        assert!(error.message.contains("request body is invalid"));
    }

    #[tokio::test]
    async fn get_member_summary_returns_not_found_when_member_is_missing() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;

        let response = router(HttpAppState::new(pool))
            .oneshot(summary_request("member:missing-001"))
            .await
            .expect("router should respond");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let error: ErrorResponse = read_json_body(response).await;
        assert_eq!(error.error, "IDENTITY_MEMBER_NOT_FOUND");
    }

    #[tokio::test]
    async fn get_member_summary_returns_not_ready_when_projection_has_not_caught_up() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let app = router(HttpAppState::new(pool));
        let hire = app
            .clone()
            .oneshot(hire_request(
                "idem-http-hire-004",
                "trace-http-hire-004",
                json!({
                    "display_name": "Member Http Four",
                    "main_role_id": "role.member.operator",
                    "secondary_role_ids": []
                }),
            ))
            .await
            .expect("hire request should succeed");
        let summary: GlobalMemberSummary = read_json_body(hire).await;

        let response = app
            .oneshot(summary_request(summary.global_member_id.as_str()))
            .await
            .expect("summary request should respond");

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let error: ErrorResponse = read_json_body(response).await;
        assert_eq!(error.error, "IDENTITY_PROJECTION_NOT_READY");
    }

    #[tokio::test]
    async fn get_member_summary_returns_projection_after_rebuild() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let app = router(HttpAppState::new(pool.clone()));
        let hire = app
            .clone()
            .oneshot(hire_request(
                "idem-http-hire-005",
                "trace-http-hire-005",
                json!({
                    "display_name": "Member Http Five",
                    "main_role_id": "role.member.operator",
                    "secondary_role_ids": []
                }),
            ))
            .await
            .expect("hire request should succeed");
        let summary: GlobalMemberSummary = read_json_body(hire).await;

        ProjectionRebuildJob::new(factory)
            .rebuild_member_summary_projection("member-summary-http", 20)
            .await
            .expect("projection rebuild should succeed");

        let response = app
            .oneshot(summary_request(summary.global_member_id.as_str()))
            .await
            .expect("summary request should respond");

        assert_eq!(response.status(), StatusCode::OK);
        let summary: MemberSummaryDto = read_json_body(response).await;
        assert_eq!(summary.display_name, "Member Http Five");
        assert_eq!(summary.main_role_name.as_deref(), Some("Member Operator"));
    }

    #[tokio::test]
    async fn update_lifecycle_endpoint_updates_member_state() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let app = router(HttpAppState::new(pool.clone()));
        let hire = app
            .clone()
            .oneshot(hire_request(
                "idem-http-lifecycle-hire-001",
                "trace-http-lifecycle-hire-001",
                json!({
                    "display_name": "Member Lifecycle Http",
                    "main_role_id": "role.member.operator",
                    "secondary_role_ids": []
                }),
            ))
            .await
            .expect("hire request should succeed");
        let member: GlobalMemberSummary = read_json_body(hire).await;

        let response = app
            .oneshot(update_lifecycle_request(
                member.global_member_id.as_str(),
                "idem-http-lifecycle-001",
                "trace-http-lifecycle-001",
                json!({
                    "target_lifecycle": "active",
                    "reason": "activate through http",
                    "expected_version": 0
                }),
            ))
            .await
            .expect("lifecycle update should respond");

        assert_eq!(response.status(), StatusCode::OK);
        let summary: GlobalMemberSummary = read_json_body(response).await;
        assert_eq!(summary.lifecycle, GlobalMemberLifecycle::Active);
    }

    #[tokio::test]
    async fn update_lifecycle_endpoint_rejects_illegal_transition() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let app = router(HttpAppState::new(pool));
        let hire = app
            .clone()
            .oneshot(hire_request(
                "idem-http-lifecycle-hire-002",
                "trace-http-lifecycle-hire-002",
                json!({
                    "display_name": "Member Lifecycle Invalid Http",
                    "main_role_id": "role.member.operator",
                    "secondary_role_ids": []
                }),
            ))
            .await
            .expect("hire request should succeed");
        let member: GlobalMemberSummary = read_json_body(hire).await;

        let response = app
            .oneshot(update_lifecycle_request(
                member.global_member_id.as_str(),
                "idem-http-lifecycle-002",
                "trace-http-lifecycle-002",
                json!({
                    "target_lifecycle": "paused",
                    "reason": "pause before activation",
                    "expected_version": 0
                }),
            ))
            .await
            .expect("lifecycle update should respond");

        assert_eq!(response.status(), StatusCode::CONFLICT);
        let error: ErrorResponse = read_json_body(response).await;
        assert_eq!(error.error, "IDENTITY_LIFECYCLE_TRANSITION_INVALID");
    }

    #[tokio::test]
    async fn update_capability_profile_endpoint_updates_profile() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let app = router(HttpAppState::new(pool));
        let hire = app
            .clone()
            .oneshot(hire_request(
                "idem-http-capability-hire-001",
                "trace-http-capability-hire-001",
                json!({
                    "display_name": "Member Capability Http",
                    "main_role_id": "role.member.operator",
                    "secondary_role_ids": []
                }),
            ))
            .await
            .expect("hire request should succeed");
        let member: GlobalMemberSummary = read_json_body(hire).await;

        let response = app
            .oneshot(update_capability_profile_request(
                member.global_member_id.as_str(),
                "idem-http-capability-001",
                "trace-http-capability-001",
                json!({
                    "capabilities": [{
                        "capability_id": "cap.http.1",
                        "capability_name": "HTTP Capability",
                        "proficiency": "advanced",
                        "notes": "added via http"
                    }],
                    "evidence_refs": [{
                        "artifact_id": "artifact-http-1",
                        "artifact_kind": "evidence",
                        "artifact_version": "v1"
                    }],
                    "expected_version": 0
                }),
            ))
            .await
            .expect("capability update should respond");

        assert_eq!(response.status(), StatusCode::OK);
        let summary: CapabilityProfileSummary = read_json_body(response).await;
        assert_eq!(summary.version, 1);
        assert_eq!(summary.capabilities, sample_capabilities_http());
        assert_eq!(summary.evidence_refs, sample_evidence_refs_http());
    }

    #[tokio::test]
    async fn update_capability_profile_endpoint_rejects_invalid_artifact_ref() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let app = router(HttpAppState::new(pool));
        let hire = app
            .clone()
            .oneshot(hire_request(
                "idem-http-capability-hire-002",
                "trace-http-capability-hire-002",
                json!({
                    "display_name": "Member Capability Invalid Http",
                    "main_role_id": "role.member.operator",
                    "secondary_role_ids": []
                }),
            ))
            .await
            .expect("hire request should succeed");
        let member: GlobalMemberSummary = read_json_body(hire).await;

        let response = app
            .oneshot(update_capability_profile_request(
                member.global_member_id.as_str(),
                "idem-http-capability-002",
                "trace-http-capability-002",
                json!({
                    "capabilities": [{
                        "capability_id": "cap.http.2",
                        "capability_name": "Bad Artifact Capability",
                        "proficiency": null,
                        "notes": null
                    }],
                    "evidence_refs": [{
                        "artifact_id": "   ",
                        "artifact_kind": "evidence",
                        "artifact_version": null
                    }],
                    "expected_version": 0
                }),
            ))
            .await
            .expect("capability update should respond");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let error: ErrorResponse = read_json_body(response).await;
        assert_eq!(error.error, "IDENTITY_INVALID_ARGUMENT");
    }

    #[tokio::test]
    async fn update_memory_refs_endpoint_updates_refs() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let app = router(HttpAppState::new(pool));
        let hire = app
            .clone()
            .oneshot(hire_request(
                "idem-http-memory-hire-001",
                "trace-http-memory-hire-001",
                json!({
                    "display_name": "Member Memory Http",
                    "main_role_id": "role.member.operator",
                    "secondary_role_ids": []
                }),
            ))
            .await
            .expect("hire request should succeed");
        let member: GlobalMemberSummary = read_json_body(hire).await;

        let response = app
            .oneshot(update_memory_refs_request(
                member.global_member_id.as_str(),
                "idem-http-memory-001",
                "trace-http-memory-001",
                json!({
                    "semantic_memory_ref": {
                        "memory_id": "memory-semantic-http-1",
                        "memory_kind": "semantic",
                        "memory_version": "v1"
                    },
                    "episodic_memory_refs": [{
                        "memory_id": "memory-episodic-http-1",
                        "memory_kind": "episodic",
                        "memory_version": "v1"
                    }]
                }),
            ))
            .await
            .expect("memory refs update should respond");

        assert_eq!(response.status(), StatusCode::OK);
        let summary: MemoryRefsSummary = read_json_body(response).await;
        assert_eq!(
            summary.semantic_memory_ref,
            Some(sample_semantic_memory_ref_http())
        );
        assert_eq!(
            summary.episodic_memory_refs,
            vec![sample_episodic_memory_ref_http()]
        );
    }

    #[tokio::test]
    async fn update_memory_refs_endpoint_rejects_invalid_memory_ref() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let app = router(HttpAppState::new(pool));
        let hire = app
            .clone()
            .oneshot(hire_request(
                "idem-http-memory-hire-002",
                "trace-http-memory-hire-002",
                json!({
                    "display_name": "Member Memory Invalid Http",
                    "main_role_id": "role.member.operator",
                    "secondary_role_ids": []
                }),
            ))
            .await
            .expect("hire request should succeed");
        let member: GlobalMemberSummary = read_json_body(hire).await;

        let response = app
            .oneshot(update_memory_refs_request(
                member.global_member_id.as_str(),
                "idem-http-memory-002",
                "trace-http-memory-002",
                json!({
                    "semantic_memory_ref": {
                        "memory_id": "   ",
                        "memory_kind": "semantic",
                        "memory_version": null
                    },
                    "episodic_memory_refs": []
                }),
            ))
            .await
            .expect("memory refs update should respond");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let error: ErrorResponse = read_json_body(response).await;
        assert_eq!(error.error, "IDENTITY_INVALID_ARGUMENT");
    }

    #[tokio::test]
    async fn get_member_audit_trace_endpoint_returns_paginated_rows_for_human_actor() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let member = seed_member_audit_trail_http(&pool).await;
        let response = router(HttpAppState::new(pool))
            .oneshot(audit_trace_request(
                member.global_member_id.as_str(),
                Some("limit=2"),
            ))
            .await
            .expect("audit trace request should respond");

        assert_eq!(response.status(), StatusCode::OK);
        let page: MemberAuditTracePageDto = read_json_body(response).await;
        assert_eq!(page.traces.len(), 2);
        assert_eq!(page.traces[0].action, "AppendCareerEntry");
        assert_eq!(page.traces[1].action, "UpdateMemoryRefs");
        assert!(page.traces[0].actor_json.is_some());
        assert_eq!(
            page.next_cursor.as_deref(),
            Some("audit:idem-audit-http-memory")
        );
    }

    #[tokio::test]
    async fn get_member_audit_trace_endpoint_rejects_invisible_actor() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let member = seed_member_audit_trail_http(&pool).await;
        let response = router(HttpAppState::new(pool))
            .oneshot(audit_trace_request_as_system(
                member.global_member_id.as_str(),
            ))
            .await
            .expect("audit trace request should respond");

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let error: ErrorResponse = read_json_body(response).await;
        assert_eq!(error.error, "IDENTITY_AUDIT_TRACE_NOT_VISIBLE");
    }

    #[tokio::test]
    async fn get_role_catalog_endpoint_returns_filtered_local_summary_rows() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;
        seed_role(&pool, "role.member.reviewer", "Reviewer").await;
        update_role_catalog_row_http(
            &pool,
            "role.member.reviewer",
            "Senior Reviewer",
            "source_drift",
        )
        .await;

        let response = router(HttpAppState::new(pool))
            .oneshot(role_catalog_request(Some(
                "status=source_drift&keyword=review&role_ids=role.member.reviewer",
            )))
            .await
            .expect("role catalog request should respond");

        assert_eq!(response.status(), StatusCode::OK);
        let roles: Vec<RoleCatalogEntryDto> = read_json_body(response).await;
        assert_eq!(roles.len(), 1);
        assert_eq!(roles[0].role_id.as_str(), "role.member.reviewer");
        assert_eq!(roles[0].role_name, "Senior Reviewer");
        assert_eq!(roles[0].status, RoleCatalogStatus::SourceDrift);
        assert_eq!(roles[0].source_ref_json["id"], "role.member.reviewer");
    }

    #[tokio::test]
    async fn get_role_catalog_endpoint_rejects_invalid_status_filter() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;

        let response = router(HttpAppState::new(pool))
            .oneshot(role_catalog_request(Some("status=invalid")))
            .await
            .expect("role catalog request should respond");

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let error: ErrorResponse = read_json_body(response).await;
        assert_eq!(error.error, "IDENTITY_INVALID_ARGUMENT");
    }

    #[tokio::test]
    async fn tombstone_member_endpoint_tombstones_member_without_supplied_gate_ref() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let app = router(HttpAppState::new(pool.clone()));
        let hire = app
            .clone()
            .oneshot(hire_request(
                "idem-http-tombstone-hire-001",
                "trace-http-tombstone-hire-001",
                json!({
                    "display_name": "Member Tombstone Http",
                    "main_role_id": "role.member.operator",
                    "secondary_role_ids": []
                }),
            ))
            .await
            .expect("hire request should succeed");
        let member: GlobalMemberSummary = read_json_body(hire).await;

        let response = app
            .oneshot(tombstone_member_request(
                member.global_member_id.as_str(),
                "idem-http-tombstone-001",
                "trace-http-tombstone-001",
                json!({
                    "reason": "tombstone through http",
                    "expected_version": 0
                }),
            ))
            .await
            .expect("tombstone request should respond");

        assert_eq!(response.status(), StatusCode::OK);
        let summary: GlobalMemberSummary = read_json_body(response).await;
        assert_eq!(summary.lifecycle, GlobalMemberLifecycle::Tombstoned);

        let persisted_lifecycle =
            sqlx::query("SELECT lifecycle FROM global_members WHERE global_member_id = $1")
                .bind(summary.global_member_id.as_str())
                .fetch_one(&pool)
                .await
                .expect("member row should exist after tombstone")
                .get::<String, _>("lifecycle");

        assert_eq!(persisted_lifecycle, "tombstoned");
    }

    #[tokio::test]
    async fn tombstone_member_endpoint_rejects_rejected_gate_decision() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let app = router(HttpAppState::new(pool.clone()));
        let hire = app
            .clone()
            .oneshot(hire_request(
                "idem-http-tombstone-hire-002",
                "trace-http-tombstone-hire-002",
                json!({
                    "display_name": "Member Tombstone Rejected Http",
                    "main_role_id": "role.member.operator",
                    "secondary_role_ids": []
                }),
            ))
            .await
            .expect("hire request should succeed");
        let member: GlobalMemberSummary = read_json_body(hire).await;

        let response = app
            .oneshot(tombstone_member_request(
                member.global_member_id.as_str(),
                "idem-http-tombstone-002",
                "trace-http-tombstone-002",
                json!({
                    "reason": "gate rejected",
                    "expected_version": 0,
                    "gate_decision_ref": rejected_gate_decision_http("gate-http-rejected-001")
                }),
            ))
            .await
            .expect("tombstone request should respond");

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let error: ErrorResponse = read_json_body(response).await;
        assert_eq!(error.error, "IDENTITY_GATE_REJECTED");

        let persisted_lifecycle =
            sqlx::query("SELECT lifecycle FROM global_members WHERE global_member_id = $1")
                .bind(member.global_member_id.as_str())
                .fetch_one(&pool)
                .await
                .expect("member row should remain available")
                .get::<String, _>("lifecycle");

        assert_eq!(persisted_lifecycle, "hired");
    }

    #[tokio::test]
    async fn tombstone_member_endpoint_returns_unavailable_when_archive_request_fails() {
        let db_mutex = Arc::clone(&DB_TEST_MUTEX);
        let _guard = db_mutex.lock().await;
        let pool = test_pool().await;
        reset_tables(&pool).await;
        seed_role(&pool, "role.member.operator", "Member Operator").await;

        let app = router(HttpAppState::new(pool.clone()));
        let hire = app
            .clone()
            .oneshot(hire_request(
                "idem-http-tombstone-hire-003",
                "trace-http-tombstone-hire-003",
                json!({
                    "display_name": "Member Tombstone Archive Failure Http",
                    "main_role_id": "role.member.operator",
                    "secondary_role_ids": []
                }),
            ))
            .await
            .expect("hire request should succeed");
        let member: GlobalMemberSummary = read_json_body(hire).await;
        let _archive_failure = HttpStubArchiveFailureGuard::failing("archive service unavailable");

        let response = app
            .oneshot(tombstone_member_request(
                member.global_member_id.as_str(),
                "idem-http-tombstone-003",
                "trace-http-tombstone-003",
                json!({
                    "reason": "archive unavailable",
                    "expected_version": 0
                }),
            ))
            .await
            .expect("tombstone request should respond");

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let error: ErrorResponse = read_json_body(response).await;
        assert_eq!(error.error, "IDENTITY_MEMORY_ARCHIVE_UNAVAILABLE");

        let persisted_lifecycle =
            sqlx::query("SELECT lifecycle FROM global_members WHERE global_member_id = $1")
                .bind(member.global_member_id.as_str())
                .fetch_one(&pool)
                .await
                .expect("member row should remain available")
                .get::<String, _>("lifecycle");

        assert_eq!(persisted_lifecycle, "hired");
    }

    fn hire_request(
        idempotency_key: &str,
        trace_id: &str,
        payload: serde_json::Value,
    ) -> Request<Body> {
        let body = serde_json::to_vec(&payload).expect("payload should serialize");
        Request::builder()
            .method("POST")
            .uri("/identity/global-members")
            .header("content-type", "application/json")
            .header(HEADER_ACTOR_REF, "human/admin-http")
            .header(HEADER_ACTOR_KIND, "human")
            .header(HEADER_IDEMPOTENCY_KEY, idempotency_key)
            .header(HEADER_TRACE_ID, trace_id)
            .body(Body::from(body))
            .expect("request should build")
    }

    fn summary_request(global_member_id: &str) -> Request<Body> {
        Request::builder()
            .uri(format!(
                "/identity/global-members/{global_member_id}/summary"
            ))
            .header(HEADER_ACTOR_REF, "human/admin-http")
            .header(HEADER_ACTOR_KIND, "human")
            .body(Body::empty())
            .expect("request should build")
    }

    fn summary_request_as_ai_without_member_id(global_member_id: &str) -> Request<Body> {
        Request::builder()
            .uri(format!(
                "/identity/global-members/{global_member_id}/summary"
            ))
            .header(HEADER_ACTOR_REF, "ai/member-http")
            .header(HEADER_ACTOR_KIND, "ai_member")
            .body(Body::empty())
            .expect("request should build")
    }

    fn audit_trace_request(global_member_id: &str, query: Option<&str>) -> Request<Body> {
        let mut uri = format!("/identity/global-members/{global_member_id}/audit-trace");
        if let Some(query) = query {
            uri.push('?');
            uri.push_str(query);
        }

        Request::builder()
            .uri(uri)
            .header(HEADER_ACTOR_REF, "human/admin-http")
            .header(HEADER_ACTOR_KIND, "human")
            .body(Body::empty())
            .expect("request should build")
    }

    fn audit_trace_request_as_system(global_member_id: &str) -> Request<Body> {
        Request::builder()
            .uri(format!(
                "/identity/global-members/{global_member_id}/audit-trace"
            ))
            .header(HEADER_ACTOR_REF, "system/http-query")
            .header(HEADER_ACTOR_KIND, "system")
            .body(Body::empty())
            .expect("request should build")
    }

    fn role_catalog_request(query: Option<&str>) -> Request<Body> {
        let mut uri = "/identity/role-catalog".to_string();
        if let Some(query) = query {
            uri.push('?');
            uri.push_str(query);
        }

        Request::builder()
            .uri(uri)
            .header(HEADER_ACTOR_REF, "human/admin-http")
            .header(HEADER_ACTOR_KIND, "human")
            .body(Body::empty())
            .expect("request should build")
    }

    fn update_lifecycle_request(
        global_member_id: &str,
        idempotency_key: &str,
        trace_id: &str,
        payload: serde_json::Value,
    ) -> Request<Body> {
        json_request(
            "POST",
            &format!("/identity/global-members/{global_member_id}/lifecycle"),
            idempotency_key,
            trace_id,
            payload,
        )
    }

    fn update_capability_profile_request(
        global_member_id: &str,
        idempotency_key: &str,
        trace_id: &str,
        payload: serde_json::Value,
    ) -> Request<Body> {
        json_request(
            "PUT",
            &format!("/identity/global-members/{global_member_id}/capability-profile"),
            idempotency_key,
            trace_id,
            payload,
        )
    }

    fn update_memory_refs_request(
        global_member_id: &str,
        idempotency_key: &str,
        trace_id: &str,
        payload: serde_json::Value,
    ) -> Request<Body> {
        json_request(
            "PUT",
            &format!("/identity/global-members/{global_member_id}/memory-refs"),
            idempotency_key,
            trace_id,
            payload,
        )
    }

    fn tombstone_member_request(
        global_member_id: &str,
        idempotency_key: &str,
        trace_id: &str,
        payload: serde_json::Value,
    ) -> Request<Body> {
        json_request(
            "POST",
            &format!("/identity/global-members/{global_member_id}/tombstone"),
            idempotency_key,
            trace_id,
            payload,
        )
    }

    fn json_request(
        method: &str,
        uri: &str,
        idempotency_key: &str,
        trace_id: &str,
        payload: serde_json::Value,
    ) -> Request<Body> {
        let body = serde_json::to_vec(&payload).expect("payload should serialize");
        Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .header(HEADER_ACTOR_REF, "human/admin-http")
            .header(HEADER_ACTOR_KIND, "human")
            .header(HEADER_IDEMPOTENCY_KEY, idempotency_key)
            .header(HEADER_TRACE_ID, trace_id)
            .body(Body::from(body))
            .expect("request should build")
    }

    fn json_request_without_idempotency(
        method: &str,
        uri: &str,
        trace_id: &str,
        payload: serde_json::Value,
    ) -> Request<Body> {
        let body = serde_json::to_vec(&payload).expect("payload should serialize");
        Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .header(HEADER_ACTOR_REF, "human/admin-http")
            .header(HEADER_ACTOR_KIND, "human")
            .header(HEADER_TRACE_ID, trace_id)
            .body(Body::from(body))
            .expect("request should build")
    }

    fn invalid_json_request(
        method: &str,
        uri: &str,
        idempotency_key: &str,
        trace_id: &str,
        invalid_body: &str,
    ) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .header(HEADER_ACTOR_REF, "human/admin-http")
            .header(HEADER_ACTOR_KIND, "human")
            .header(HEADER_IDEMPOTENCY_KEY, idempotency_key)
            .header(HEADER_TRACE_ID, trace_id)
            .body(Body::from(invalid_body.to_string()))
            .expect("request should build")
    }

    async fn read_json_body<T>(response: Response) -> T
    where
        T: DeserializeOwned,
    {
        let body = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body should read");
        serde_json::from_slice(&body).expect("response body should be valid json")
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

    fn sample_capabilities_http() -> Vec<CapabilityItem> {
        vec![CapabilityItem {
            capability_id: "cap.http.1".to_string(),
            capability_name: "HTTP Capability".to_string(),
            proficiency: Some("advanced".to_string()),
            notes: Some("added via http".to_string()),
        }]
    }

    fn sample_evidence_refs_http() -> Vec<ArtifactRef> {
        vec![ArtifactRef {
            artifact_id: "artifact-http-1".to_string(),
            artifact_kind: "evidence".to_string(),
            artifact_version: Some("v1".to_string()),
        }]
    }

    fn sample_semantic_memory_ref_http() -> MemoryRef {
        MemoryRef {
            memory_id: "memory-semantic-http-1".to_string(),
            memory_kind: "semantic".to_string(),
            memory_version: Some("v1".to_string()),
        }
    }

    fn sample_episodic_memory_ref_http() -> MemoryRef {
        MemoryRef {
            memory_id: "memory-episodic-http-1".to_string(),
            memory_kind: "episodic".to_string(),
            memory_version: Some("v1".to_string()),
        }
    }

    #[derive(Debug, Clone, Default)]
    struct StubMemoryArchiveValidator;

    impl MemoryArchivePort for StubMemoryArchiveValidator {
        async fn validate_ref(&self, memory_ref: &MemoryRef) -> Result<(), IdentityError> {
            memory_ref.validate()
        }
    }

    /// Seeds one member audit trail through application services so HTTP audit queries can verify
    /// pagination and field trimming on realistic append-only rows.
    async fn seed_member_audit_trail_http(
        pool: &sqlx::postgres::PgPool,
    ) -> crate::domain::member::GlobalMemberSummary {
        let factory = SqlxUnitOfWorkFactory::new(pool.clone());
        let command_service = MemberLifecycleCommandService::new(factory.clone());
        let memory_service =
            MemoryRefsCommandService::new(factory.clone(), StubMemoryArchiveValidator);
        let career_consumer = CareerEventConsumer::new(CareerEventConsumerService::new(factory));
        let actor = ActorContext::new("human/admin-audit-http", ActorKind::HumanUser, None);

        let member = command_service
            .hire_global_member(
                crate::domain::member::HireGlobalMemberCommand {
                    display_name: "Member Audit Http".to_string(),
                    main_role_id: RoleId::new("role.member.operator"),
                    secondary_role_ids: Vec::new(),
                },
                actor.clone(),
                CommandMetadata::new(
                    "idem-audit-http-hire",
                    "trace-audit-http-hire",
                    "hash-audit-http-hire",
                ),
            )
            .await
            .expect("hire member for audit http query");
        memory_service
            .update_memory_refs(
                UpdateMemoryRefsCommand {
                    global_member_id: member.global_member_id.clone(),
                    semantic_memory_ref: Some(MemoryRef {
                        memory_id: "memory-audit-http".to_string(),
                        memory_kind: "semantic".to_string(),
                        memory_version: Some("v1".to_string()),
                    }),
                    episodic_memory_refs: Vec::new(),
                },
                actor.clone(),
                CommandMetadata::new(
                    "idem-audit-http-memory",
                    "trace-audit-http-memory",
                    "hash-audit-http-memory",
                ),
            )
            .await
            .expect("update memory refs for audit http query");
        career_consumer
            .consume_work_event(sample_work_event_http(member.global_member_id.as_str()))
            .await
            .expect("append career event for audit http query");

        update_audit_created_at_http(pool, "audit:idem-audit-http-hire", now_http()).await;
        update_audit_created_at_http(
            pool,
            "audit:idem-audit-http-memory",
            now_http() + Duration::seconds(1),
        )
        .await;
        update_audit_created_at_http(
            pool,
            "audit:audit-career-http-event",
            now_http() + Duration::seconds(2),
        )
        .await;

        member
    }

    /// Adjusts one local role-catalog row for HTTP query filter tests.
    async fn update_role_catalog_row_http(
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

    /// Reorders audit rows deterministically so pagination assertions do not depend on runtime timing.
    async fn update_audit_created_at_http(
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

    /// Builds one deterministic work-fact event used to seed HTTP audit-trace tests.
    fn sample_work_event_http(global_member_id: &str) -> InboundWorkFactEvent {
        InboundWorkFactEvent {
            envelope: InboundEventEnvelope {
                source_event_id: EventId::new("audit-career-http-event"),
                source_module: "work".to_string(),
                event_type: "work.fact.recorded".to_string(),
                occurred_at: now_http(),
                payload_hash: "audit-career-http-hash".to_string(),
                payload: json!({
                    "global_member_id": GlobalMemberId::new(global_member_id),
                    "project_id": ProjectId::new("project-audit-http-001"),
                    "work_ref": {
                        "work_id": "work-audit-http-001",
                        "work_kind": "task",
                        "work_version": "v1",
                    },
                    "entry_kind": "assigned",
                    "started_at": now_http(),
                    "ended_at": now_http() + Duration::seconds(30),
                    "payload_summary": {
                        "title": "Audit query work item http",
                    }
                }),
            },
        }
    }

    /// Returns the current UTC timestamp in the primitive form used by HTTP test fixtures.
    fn now_http() -> PrimitiveDateTime {
        let now = OffsetDateTime::now_utc();
        PrimitiveDateTime::new(now.date(), now.time())
    }

    #[test]
    fn trusted_header_constants_are_ascii() {
        for header in [
            HEADER_ACTOR_REF,
            HEADER_ACTOR_KIND,
            HEADER_IDEMPOTENCY_KEY,
            HEADER_TRACE_ID,
        ] {
            let value = HeaderValue::from_str(header).expect("header name should be valid ascii");
            assert_eq!(value.to_str().expect("header should remain utf8"), header);
        }
    }
}
