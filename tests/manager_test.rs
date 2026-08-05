use acp_gateway::manager::{ActiveSession, SessionManager, BROADCAST_CAPACITY};
use acp_gateway::types::{SessionMeta, SessionStatus};
use chrono::Utc;
use tempfile::tempdir;

fn make_meta(id: &str) -> SessionMeta {
    SessionMeta {
        session_id: id.to_string(),
        agent: "claude-code".to_string(),
        cwd: "/tmp".to_string(),
        mcp_servers: vec![],
        status: SessionStatus::Active,
        created_at: Utc::now(),
    }
}

#[tokio::test]
async fn test_manager_insert_and_retrieve_events() {
    let dir = tempdir().unwrap();
    let manager = SessionManager::new(dir.path().join("sessions.jsonl"))
        .await
        .unwrap();

    let (tx, _rx) = tokio::sync::broadcast::channel(BROADCAST_CAPACITY);
    manager.insert("s1".to_string(), ActiveSession::new(make_meta("s1"), tx, None));
    manager
        .push_event(
            "s1",
            "agent_message_chunk".to_string(),
            serde_json::json!({ "text": "hello" }),
        )
        .await;

    let events = manager.get_events("s1", 0, 10).await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].event_type, "agent_message_chunk");
}

#[tokio::test]
async fn test_manager_event_index_monotonically_increases() {
    let dir = tempdir().unwrap();
    let manager = SessionManager::new(dir.path().join("sessions.jsonl"))
        .await
        .unwrap();

    let (tx, _rx) = tokio::sync::broadcast::channel(BROADCAST_CAPACITY);
    manager.insert("s2".to_string(), ActiveSession::new(make_meta("s2"), tx, None));
    for i in 0..5 {
        manager
            .push_event(
                "s2",
                "agent_message_chunk".to_string(),
                serde_json::json!({ "i": i }),
            )
            .await;
    }

    let events = manager.get_events("s2", 2, 10).await;
    assert_eq!(events.len(), 3);
    assert_eq!(events[0].index, 2);
}

#[tokio::test]
async fn test_manager_remove_session() {
    let dir = tempdir().unwrap();
    let manager = SessionManager::new(dir.path().join("sessions.jsonl"))
        .await
        .unwrap();

    let (tx, _rx) = tokio::sync::broadcast::channel(BROADCAST_CAPACITY);
    manager.insert("s3".to_string(), ActiveSession::new(make_meta("s3"), tx, None));
    assert!(manager.contains("s3"));
    let _ = manager.remove("s3");
    assert!(!manager.contains("s3"));
}

#[tokio::test]
async fn test_manager_get_events_returns_empty_for_unknown_session() {
    let dir = tempdir().unwrap();
    let manager = SessionManager::new(dir.path().join("sessions.jsonl"))
        .await
        .unwrap();

    let events = manager.get_events("missing", 0, 10).await;
    assert!(events.is_empty());
}
