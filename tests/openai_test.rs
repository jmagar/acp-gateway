use acp_gateway::{
    app::build_app,
    manager::{AppState, SessionManager},
    agent::known_agents,
    pool::AgentPool,
};
use axum::http::StatusCode;
use axum_test::TestServer;
use std::{collections::HashMap, sync::Arc};
use tempfile::tempdir;

async fn make_server() -> TestServer {
    let dir = tempdir().unwrap();
    let manager = Arc::new(
        SessionManager::new(dir.path().join("sessions.jsonl"))
            .await
            .unwrap(),
    );
    let pools: HashMap<String, AgentPool> = known_agents()
        .into_iter()
        .map(|(name, def)| (name, AgentPool::new(def)))
        .collect();
    TestServer::new(build_app(AppState {
        sessions: manager,
        pools: Arc::new(pools),
        default_cwd: std::path::PathBuf::from("/tmp"),
    }))
}

#[tokio::test]
async fn test_models_lists_known_agents() {
    let server = make_server().await;
    let response = server.get("/v1/models").await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();
    assert_eq!(body["object"], "list");
    let data = body["data"].as_array().unwrap();
    assert!(!data.is_empty());
    assert!(data.iter().any(|model| model["id"] == "claude-code"));
    for model in data {
        assert!(model.get("id").is_some());
        assert!(model.get("object").is_some());
        assert!(model.get("created").is_some());
    }
}

#[tokio::test]
async fn test_chat_completions_unknown_model_returns_400() {
    let server = make_server().await;
    let response = server
        .post("/v1/chat/completions")
        .json(&serde_json::json!({
            "model": "nonexistent-model-xyz",
            "messages": [{ "role": "user", "content": "hello" }],
            "stream": false
        }))
        .await;
    assert_eq!(response.status_code(), StatusCode::BAD_REQUEST);
    let body: serde_json::Value = response.json();
    assert_eq!(body["error"], "AGENT_NOT_FOUND");
}

#[tokio::test]
#[ignore = "requires claude binary"]
async fn test_chat_completions_non_streaming_returns_content() {
    let server = make_server().await;
    let response = server
        .post("/v1/chat/completions")
        .json(&serde_json::json!({
            "model": "claude-code",
            "messages": [{ "role": "user", "content": "Reply with exactly: pong" }],
            "stream": false
        }))
        .await;
    assert_eq!(response.status_code(), StatusCode::OK);
    let body: serde_json::Value = response.json();
    assert_eq!(body["object"], "chat.completion");
    let content = body["choices"][0]["message"]["content"].as_str().unwrap();
    assert!(!content.is_empty());
}
