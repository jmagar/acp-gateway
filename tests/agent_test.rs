use acp_gateway::agent::{get_agent, known_agents};

#[test]
fn test_known_agents_contains_claude_code() {
    let agents = known_agents();
    assert!(agents.contains_key("claude-code"));
    let claude = &agents["claude-code"];
    assert_eq!(claude.name, "claude-code");
    assert_eq!(claude.command, "./claude-agent-acp");
}

#[test]
fn test_get_agent_returns_some_for_known_agent() {
    assert!(get_agent("claude-code").is_some());
}

#[test]
fn test_get_agent_returns_none_for_unknown() {
    assert!(get_agent("nonexistent-agent-xyz").is_none());
}

#[test]
fn test_list_agents_returns_only_verified_agents() {
    let agents = known_agents();
    for (name, definition) in &agents {
        assert!(!definition.command.is_empty(), "agent {} has empty command", name);
        assert_eq!(&definition.name, name);
    }
}

#[test]
fn test_known_agents_does_not_include_unverified_placeholders() {
    let agents = known_agents();
    for definition in agents.values() {
        assert_ne!(definition.command, "todo");
        assert_ne!(definition.command, "TODO");
    }
}

#[tokio::test]
#[ignore = "requires claude binary"]
async fn test_connect_and_new_session() {
    use acp_gateway::agent::connect_agent;
    use std::path::PathBuf;

    let definition = get_agent("claude-code").unwrap();
    let handle = connect_agent(&definition).await.expect("failed to connect");
    let response = handle
        .new_session(PathBuf::from("/tmp"), vec![])
        .await
        .expect("new_session failed");
    assert!(!response.session_id.0.is_empty());
    let _ = handle.close().await;
}
