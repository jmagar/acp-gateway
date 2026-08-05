use acp_gateway::{
    app::build_app,
    manager::{AppState, SessionManager},
};
use axum::http::StatusCode;
use axum_test::TestServer;
use std::{collections::HashMap, sync::Arc};
use tempfile::tempdir;

#[tokio::test]
async fn test_health_check_returns_ok() {
    let dir = tempdir().unwrap();
    let manager = SessionManager::new(dir.path().join("sessions.jsonl"))
        .await
        .unwrap();
    let app = build_app(AppState {
        sessions: Arc::new(manager),
        pools: Arc::new(HashMap::new()),
        default_cwd: std::path::PathBuf::from("/tmp"),
    });
    let server = TestServer::new(app);

    let response = server.get("/health").await;
    assert_eq!(response.status_code(), StatusCode::OK);

    let body: serde_json::Value = response.json();
    assert_eq!(body["status"], "ok");
    assert!(body.get("version").is_some());
}
