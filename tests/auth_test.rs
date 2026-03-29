use acp_gateway::{
    app::build_app,
    manager::{AppState, SessionManager},
};
use axum::http::StatusCode;
use axum_test::TestServer;
use std::sync::Arc;
use tempfile::tempdir;

async fn make_server() -> (TestServer, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let manager = SessionManager::new(dir.path().join("sessions.jsonl"))
        .await
        .unwrap();
    let app = build_app(AppState {
        sessions: Arc::new(manager),
    });
    // TestServer::new returns TestServer directly (not Result) in this version
    (TestServer::new(app), dir)
}

/// /health must be reachable without any Authorization header regardless of
/// whether API_KEY is set, because it sits outside the protected sub-router.
#[tokio::test]
async fn test_health_is_unauthenticated() {
    let (server, _dir) = make_server().await;
    let response = server.get("/health").await;
    assert_eq!(response.status_code(), StatusCode::OK);
}

/// When no API_KEY env var is set the middleware passes all requests through
/// (dev mode). Hitting a protected route should NOT return 401.
///
/// Note: API_KEY is cached in a OnceLock on first read. In the test binary
/// API_KEY is not set at process start, so the first call initialises it to
/// None and the middleware stays permissive for the lifetime of the process.
#[tokio::test]
async fn test_protected_route_is_permissive_without_api_key() {
    // API_KEY is not set in the test environment; OnceLock will capture None.
    let (server, _dir) = make_server().await;
    let response = server.get("/api/sessions").await;
    // Anything except 401 confirms the middleware did not block the request.
    assert_ne!(
        response.status_code(),
        StatusCode::UNAUTHORIZED,
        "expected middleware to pass request when API_KEY is unset, got 401"
    );
}
