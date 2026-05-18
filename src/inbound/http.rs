//! HTTP route registration for command and query skeleton endpoints.

use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post, put};
use axum::{Json, Router};
use serde::Serialize;

/// Builds the HTTP router for the skeleton phase.
pub fn router() -> Router {
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
}

/// Reports process liveness for the skeleton phase.
pub async fn health_check() -> impl IntoResponse {
    (StatusCode::OK, Json(HealthResponse { status: "ok" }))
}

async fn hire_global_member() -> impl IntoResponse {
    not_implemented("HireGlobalMember")
}

async fn update_lifecycle(Path(_global_member_id): Path<String>) -> impl IntoResponse {
    not_implemented("UpdateLifecycle")
}

async fn update_capability_profile(Path(_global_member_id): Path<String>) -> impl IntoResponse {
    not_implemented("UpdateCapabilityProfile")
}

async fn update_memory_refs(Path(_global_member_id): Path<String>) -> impl IntoResponse {
    not_implemented("UpdateMemoryRefs")
}

async fn tombstone_member(Path(_global_member_id): Path<String>) -> impl IntoResponse {
    not_implemented("TombstoneMember")
}

async fn get_member_summary(Path(_global_member_id): Path<String>) -> impl IntoResponse {
    not_implemented("GetMemberSummary")
}

async fn get_member_audit_trace(Path(_global_member_id): Path<String>) -> impl IntoResponse {
    not_implemented("GetMemberAuditTrace")
}

async fn get_role_catalog() -> impl IntoResponse {
    not_implemented("GetRoleCatalog")
}

fn not_implemented(operation: &'static str) -> impl IntoResponse {
    (
        StatusCode::NOT_IMPLEMENTED,
        Json(NotImplementedResponse {
            error: "IDENTITY_NOT_IMPLEMENTED",
            operation,
        }),
    )
}

/// Health-check DTO used by the skeleton service.
#[derive(Debug, Clone, Copy, Serialize)]
struct HealthResponse {
    /// Process liveness marker.
    status: &'static str,
}

/// Placeholder error DTO returned by unimplemented skeleton handlers.
#[derive(Debug, Clone, Copy, Serialize)]
struct NotImplementedResponse {
    /// Stable placeholder error code for unimplemented endpoints.
    error: &'static str,
    /// Operation name that has not been implemented yet.
    operation: &'static str,
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::util::ServiceExt;

    use super::router;

    #[tokio::test]
    async fn health_endpoint_returns_ok() {
        let response = router()
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
    async fn command_endpoint_is_registered() {
        let response = router()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/identity/global-members")
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_eq!(response.status(), StatusCode::NOT_IMPLEMENTED);
    }
}
