use acp_gateway::registry::SessionRegistry;
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
async fn test_registry_create_and_load() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("sessions.jsonl");

    let registry = SessionRegistry::new(&path).await.unwrap();
    registry.append(&make_meta("sess-001")).await.unwrap();

    let sessions = registry.list_async().await;
    assert_eq!(sessions.len(), 1);
    assert_eq!(sessions[0].session_id, "sess-001");
    assert_eq!(sessions[0].status, SessionStatus::Active);
}

#[tokio::test]
async fn test_registry_status_update_latest_wins() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("sessions.jsonl");

    let registry = SessionRegistry::new(&path).await.unwrap();
    registry.append(&make_meta("sess-002")).await.unwrap();
    registry
        .update_status("sess-002", SessionStatus::Completed)
        .await
        .unwrap();

    let sessions = registry.list_async().await;
    assert_eq!(sessions[0].status, SessionStatus::Completed);
}

#[tokio::test]
async fn test_registry_get_by_id() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("sessions.jsonl");

    let registry = SessionRegistry::new(&path).await.unwrap();
    registry.append(&make_meta("sess-003")).await.unwrap();

    assert!(registry.get_async("sess-003").await.is_some());
    assert!(registry.get_async("sess-999").await.is_none());
}

#[tokio::test]
async fn test_registry_startup_marks_active_as_resumable() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("sessions.jsonl");

    let registry = SessionRegistry::new(&path).await.unwrap();
    registry.append(&make_meta("sess-004")).await.unwrap();

    let registry = SessionRegistry::new(&path).await.unwrap();
    let session = registry.get_async("sess-004").await.unwrap();
    assert_eq!(session.status, SessionStatus::Resumable);
}
