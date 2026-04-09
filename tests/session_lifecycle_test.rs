use acp_gateway::{
    app::build_app,
    manager::{ActiveSession, AppState, SessionManager, BROADCAST_CAPACITY},
    types::{SessionMeta, SessionStatus},
};
use axum::http::StatusCode;
use axum_test::TestServer;
use chrono::Utc;
use std::{collections::HashMap, sync::Arc};
use tempfile::tempdir;

async fn make_test_server_with_session(session_id: &str, event_count: usize) -> TestServer {
    let dir = tempdir().unwrap();
    let manager = Arc::new(
        SessionManager::new(dir.path().join("sessions.jsonl"))
            .await
            .unwrap(),
    );

    let (tx, _rx) = tokio::sync::broadcast::channel(BROADCAST_CAPACITY);
    let meta = SessionMeta {
        session_id: session_id.to_string(),
        agent: "claude-code".to_string(),
        cwd: "/tmp".to_string(),
        mcp_servers: vec![],
        status: SessionStatus::Active,
        created_at: Utc::now(),
    };
    manager.insert(session_id.to_string(), ActiveSession::new(meta, tx, None));

    for i in 0..event_count {
        manager
            .push_event(
                session_id,
                "agent_message_chunk".to_string(),
                serde_json::json!({ "i": i }),
            )
            .await;
    }

    TestServer::new(build_app(AppState {
        sessions: manager,
        pools: Arc::new(HashMap::new()),
        default_cwd: std::path::PathBuf::from("/tmp"),
    }))
}

#[tokio::test]
async fn test_events_endpoint_returns_buffered_events() {
    let server = make_test_server_with_session("test-001", 3).await;
    let response = server.get("/api/sessions/test-001/events?from=0&limit=10").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();
    assert_eq!(body["total"], 3);
    assert_eq!(body["events"].as_array().unwrap().len(), 3);
    assert_eq!(body["has_more"], false);
}

#[tokio::test]
async fn test_events_endpoint_pagination() {
    let server = make_test_server_with_session("test-002", 10).await;
    let response = server.get("/api/sessions/test-002/events?from=5&limit=3").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();
    assert_eq!(body["events"].as_array().unwrap().len(), 3);
    assert_eq!(body["has_more"], true);
}

#[tokio::test]
async fn test_events_endpoint_returns_404_for_unknown_session() {
    let server = make_test_server_with_session("test-003", 0).await;
    let response = server.get("/api/sessions/nonexistent/events").await;
    assert_eq!(response.status_code(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_stream_endpoint_returns_410_for_dead_session() {
    let dir = tempdir().unwrap();
    let manager = Arc::new(
        SessionManager::new(dir.path().join("sessions.jsonl"))
            .await
            .unwrap(),
    );
    let meta = SessionMeta {
        session_id: "dead-001".to_string(),
        agent: "claude-code".to_string(),
        cwd: "/tmp".to_string(),
        mcp_servers: vec![],
        status: SessionStatus::Resumable,
        created_at: Utc::now(),
    };
    manager.registry.append(&meta).await.unwrap();

    let server = TestServer::new(build_app(AppState {
        sessions: manager,
        pools: Arc::new(HashMap::new()),
        default_cwd: std::path::PathBuf::from("/tmp"),
    }));
    let response = server.get("/api/sessions/dead-001/stream").await;
    assert_eq!(response.status_code(), StatusCode::GONE);
}

#[tokio::test]
#[ignore = "requires claude binary"]
async fn test_full_session_lifecycle_with_real_agent() {
    let dir = tempdir().unwrap();
    let manager = Arc::new(
        SessionManager::new(dir.path().join("sessions.jsonl"))
            .await
            .unwrap(),
    );
    let server = TestServer::new(build_app(AppState {
        sessions: Arc::clone(&manager),
        pools: Arc::new(HashMap::new()),
        default_cwd: std::path::PathBuf::from("/tmp"),
    }));

    let create_response = server
        .post("/api/sessions")
        .json(&serde_json::json!({ "agent": "claude-code", "cwd": "/tmp", "mcp_servers": [] }))
        .await;
    assert_eq!(create_response.status_code(), StatusCode::CREATED);

    let body: serde_json::Value = create_response.json();
    let session_id = body["session_id"].as_str().unwrap().to_string();

    let prompt_response = server
        .post(&format!("/api/sessions/{session_id}/prompt"))
        .json(&serde_json::json!({ "content": "Reply with exactly: gateway test ok" }))
        .await;
    assert_eq!(prompt_response.status_code(), StatusCode::ACCEPTED);

    tokio::time::sleep(std::time::Duration::from_secs(8)).await;

    let events_response = server
        .get(&format!("/api/sessions/{session_id}/events?from=0&limit=50"))
        .await;
    let events_body: serde_json::Value = events_response.json();
    assert!(events_body["total"].as_u64().unwrap_or(0) > 0);

    let delete_response = server.delete(&format!("/api/sessions/{session_id}")).await;
    assert_eq!(delete_response.status_code(), StatusCode::NO_CONTENT);
    assert!(!manager.contains(&session_id));
}
