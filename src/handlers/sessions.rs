use crate::{
    agent::{connect_agent, get_agent, known_agents, resume_session},
    error::AppError,
    events::spawn_ingestion_loop,
    manager::{ActiveSession, AppState, BROADCAST_CAPACITY},
    types::{CreateSessionRequest, CreateSessionResponse, EventsQuery, McpServerConfig, PromptBody, SessionMeta, SessionStatus},
};
use agent_client_protocol::{McpServer, McpServerHttp, McpServerSse, McpServerStdio, SessionId};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use chrono::Utc;
use serde_json::json;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::broadcast;

pub async fn create(
    State(state): State<AppState>,
    Json(body): Json<CreateSessionRequest>,
) -> Result<(StatusCode, Json<CreateSessionResponse>), AppError> {
    let agent_def = get_agent(&body.agent)
        .ok_or_else(|| AppError::AgentNotFound(body.agent.clone()))?;
    let handle = connect_agent(&agent_def).await.map_err(AppError::Acp)?;
    let mcp_servers = body
        .mcp_servers
        .iter()
        .enumerate()
        .map(|(index, config)| mcp_to_acp(config, index))
        .collect::<Result<Vec<_>, _>>()?;

    let response = handle
        .new_session(PathBuf::from(&body.cwd), mcp_servers)
        .await
        .map_err(AppError::Acp)?;
    let session_id = response.session_id.0.to_string();
    let created_at = Utc::now();

    let meta = SessionMeta {
        session_id: session_id.clone(),
        agent: body.agent.clone(),
        cwd: body.cwd.clone(),
        mcp_servers: body.mcp_servers.clone(),
        status: SessionStatus::Active,
        created_at,
    };

    state.sessions.registry.append(&meta).await?;
    let (tx, _rx) = broadcast::channel(BROADCAST_CAPACITY);
    state.sessions.insert(
        session_id.clone(),
        ActiveSession::new(meta, tx, Some(handle)),
    );
    spawn_ingestion_loop(Arc::clone(&state.sessions), session_id.clone());

    Ok((
        StatusCode::CREATED,
        Json(CreateSessionResponse {
            session_id,
            agent: body.agent,
            status: SessionStatus::Active,
            created_at,
        }),
    ))
}

pub async fn list(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(json!(state.sessions.registry.list_async().await))
}

pub async fn delete_session(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
) -> Result<StatusCode, AppError> {
    let known = state.sessions.registry.get_async(&session_id).await.is_some();
    let removed = state.sessions.remove(&session_id);

    if let Some(session) = removed {
        if let Some(agent) = session.agent {
            let _ = agent.close().await;
        }
    } else if !known {
        return Err(AppError::SessionNotFound(session_id));
    }

    state
        .sessions
        .registry
        .update_status(&session_id, SessionStatus::Deleted)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn send_prompt(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Json(body): Json<PromptBody>,
) -> Result<StatusCode, AppError> {
    if !state.sessions.contains(&session_id) {
        if state.sessions.registry.get_async(&session_id).await.is_some() {
            return Err(AppError::SessionDead(session_id));
        }
        return Err(AppError::SessionNotFound(session_id));
    }

    let handle = state
        .sessions
        .agent_handle(&session_id)
        .ok_or_else(|| AppError::SessionDead(session_id.clone()))?;
    let session_id_for_task = session_id.clone();
    tokio::spawn(async move {
        if let Err(error) = handle
            .prompt_text(SessionId::new(session_id.clone()), body.content)
            .await
        {
            tracing::error!(session_id = %session_id_for_task, error = %error, "prompt failed");
        }
    });

    Ok(StatusCode::ACCEPTED)
}

pub async fn events(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Query(query): Query<EventsQuery>,
) -> Result<Json<serde_json::Value>, AppError> {
    if !state.sessions.contains(&session_id) {
        if state.sessions.registry.get_async(&session_id).await.is_some() {
            return Err(AppError::SessionDead(session_id));
        }
        return Err(AppError::SessionNotFound(session_id));
    }

    let total = state.sessions.event_count(&session_id).await;
    let events = state
        .sessions
        .get_events(&session_id, query.from, query.limit)
        .await;
    let has_more = query.from + events.len() < total;

    Ok(Json(json!({
        "events": events,
        "total": total,
        "has_more": has_more
    })))
}

pub async fn resume(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
) -> Result<Json<serde_json::Value>, AppError> {
    if state.sessions.contains(&session_id) {
        return Ok(Json(json!({
            "session_id": session_id,
            "status": "active",
            "message": "already active"
        })));
    }

    let meta = state
        .sessions
        .registry
        .get_async(&session_id)
        .await
        .ok_or_else(|| AppError::SessionNotFound(session_id.clone()))?;
    let agent_def = get_agent(&meta.agent)
        .ok_or_else(|| AppError::AgentNotFound(meta.agent.clone()))?;
    let handle = connect_agent(&agent_def).await.map_err(AppError::Acp)?;
    let mcp_servers = meta
        .mcp_servers
        .iter()
        .enumerate()
        .map(|(index, config)| mcp_to_acp(config, index))
        .collect::<Result<Vec<_>, _>>()?;

    resume_session(&handle, &session_id, &meta.cwd, mcp_servers)
        .await
        .map_err(AppError::Acp)?;

    let (tx, _rx) = broadcast::channel(BROADCAST_CAPACITY);
    let mut active_meta = meta.clone();
    active_meta.status = SessionStatus::Active;
    state.sessions.insert(
        session_id.clone(),
        ActiveSession::new(active_meta, tx, Some(handle)),
    );
    spawn_ingestion_loop(Arc::clone(&state.sessions), session_id.clone());
    state
        .sessions
        .registry
        .update_status(&session_id, SessionStatus::Active)
        .await?;

    Ok(Json(json!({
        "session_id": session_id,
        "agent": meta.agent,
        "status": "active"
    })))
}

pub async fn list_agents() -> Json<serde_json::Value> {
    let mut agents: Vec<_> = known_agents()
        .values()
        .map(|agent| json!({ "name": agent.name, "title": agent.title }))
        .collect();
    agents.sort_by(|left, right| left["name"].as_str().cmp(&right["name"].as_str()));
    Json(json!(agents))
}

fn validate_mcp_url(url: &str) -> Result<(), AppError> {
    // Permit only http and https schemes.
    if !url.starts_with("http://") && !url.starts_with("https://") {
        return Err(AppError::InvalidRequest(format!(
            "MCP server URL must use http or https scheme: {url}"
        )));
    }

    // Extract the host component (between "scheme://" and the first "/" or ":").
    let after_scheme = url
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    let host = after_scheme
        .split(&['/', ':'] as &[char])
        .next()
        .unwrap_or("");

    // Block known sensitive hostnames before attempting IP parse.
    let host_lower = host.to_lowercase();
    if host_lower == "localhost" || host_lower.ends_with(".local") {
        return Err(AppError::InvalidRequest(format!(
            "MCP server URL targets a local/internal hostname: {url}"
        )));
    }

    // If the host parses as an IP address, check against blocked ranges.
    if let Ok(addr) = host.parse::<std::net::IpAddr>() {
        if is_private_ip(&addr) {
            return Err(AppError::InvalidRequest(format!(
                "MCP server URL targets a private/internal address: {url}"
            )));
        }
    }

    Ok(())
}

fn is_private_ip(addr: &std::net::IpAddr) -> bool {
    match addr {
        std::net::IpAddr::V4(v4) => {
            let o = v4.octets();
            // 127.0.0.0/8  — loopback
            o[0] == 127
            // 0.0.0.0/8
            || o[0] == 0
            // 10.0.0.0/8  — RFC-1918
            || o[0] == 10
            // 172.16.0.0/12  — RFC-1918
            || (o[0] == 172 && o[1] >= 16 && o[1] <= 31)
            // 192.168.0.0/16  — RFC-1918
            || (o[0] == 192 && o[1] == 168)
            // 169.254.0.0/16  — link-local / AWS & Azure IMDS
            || (o[0] == 169 && o[1] == 254)
        }
        std::net::IpAddr::V6(v6) => {
            // ::1  — loopback
            v6.is_loopback()
            // fe80::/10  — link-local
            || v6.segments()[0] & 0xffc0 == 0xfe80
            // fc00::/7  — unique local (ULA)
            || v6.segments()[0] & 0xfe00 == 0xfc00
        }
    }
}

fn validate_stdio_command(command: &str) -> Result<(), AppError> {
    // Reject empty commands.
    if command.trim().is_empty() {
        return Err(AppError::InvalidRequest(
            "MCP stdio command must not be empty".to_string(),
        ));
    }

    // Reject shell metacharacters that indicate injection attempts.
    let shell_metacharacters = [';', '|', '&', '`'];
    for ch in shell_metacharacters {
        if command.contains(ch) {
            return Err(AppError::InvalidRequest(format!(
                "MCP stdio command contains forbidden shell metacharacter: {ch}"
            )));
        }
    }
    if command.contains("$(") || command.contains("${") {
        return Err(AppError::InvalidRequest(
            "MCP stdio command contains forbidden shell substitution".to_string(),
        ));
    }

    // Reject known shell binaries and common escape tools by basename.
    // Using a blocklist because a full allowlist would require enumerating all
    // valid MCP server binary names, which is not feasible.
    let command_basename = std::path::Path::new(command)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(command);
    let blocked_commands = [
        "sh", "bash", "zsh", "fish", "csh", "tcsh", "dash", "ksh",
        "python", "python2", "python3", "perl", "ruby", "node", "nodejs",
        "nc", "netcat", "ncat", "socat",
    ];
    if blocked_commands.contains(&command_basename) {
        return Err(AppError::InvalidRequest(format!(
            "MCP stdio command '{}' is not permitted",
            command_basename
        )));
    }

    Ok(())
}

fn mcp_to_acp(config: &McpServerConfig, index: usize) -> Result<McpServer, AppError> {
    match config {
        McpServerConfig::Sse { url } => {
            validate_mcp_url(url)?;
            Ok(McpServer::Sse(McpServerSse::new(
                format!("mcp-sse-{index}"),
                url.clone(),
            )))
        }
        McpServerConfig::Http { url } => {
            validate_mcp_url(url)?;
            Ok(McpServer::Http(McpServerHttp::new(
                format!("mcp-http-{index}"),
                url.clone(),
            )))
        }
        McpServerConfig::Stdio { command, args } => {
            validate_stdio_command(command)?;
            Ok(McpServer::Stdio(
                McpServerStdio::new(format!("mcp-stdio-{index}"), command.clone()).args(args.clone()),
            ))
        }
    }
}

#[cfg(test)]
mod stdio_tests {
    use super::*;

    #[test]
    fn test_validate_stdio_command_blocks_shells() {
        assert!(validate_stdio_command("bash").is_err());
        assert!(validate_stdio_command("/bin/sh").is_err());
        assert!(validate_stdio_command("/usr/bin/python3").is_err());
        assert!(validate_stdio_command("nc").is_err());
    }

    #[test]
    fn test_validate_stdio_command_blocks_metacharacters() {
        assert!(validate_stdio_command("echo; rm -rf /").is_err());
        assert!(validate_stdio_command("cmd|bash").is_err());
        assert!(validate_stdio_command("$(evil)").is_err());
    }

    #[test]
    fn test_validate_stdio_command_permits_mcp_servers() {
        assert!(validate_stdio_command("npx").is_ok());
        assert!(validate_stdio_command("/usr/local/bin/my-mcp-server").is_ok());
        assert!(validate_stdio_command("./mcp-server").is_ok());
    }

    #[test]
    fn test_validate_stdio_command_rejects_empty() {
        assert!(validate_stdio_command("").is_err());
        assert!(validate_stdio_command("   ").is_err());
    }

    #[test]
    fn test_validate_stdio_command_blocks_shell_substitution() {
        assert!(validate_stdio_command("${PATH}").is_err());
        assert!(validate_stdio_command("$(whoami)").is_err());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_mcp_url_blocks_private_ips() {
        // RFC-1918 and loopback addresses must be rejected.
        assert!(validate_mcp_url("http://10.0.0.1/api").is_err());
        assert!(validate_mcp_url("http://192.168.1.1/api").is_err());
        assert!(validate_mcp_url("http://169.254.169.254/latest/meta-data/").is_err());
        assert!(validate_mcp_url("http://127.0.0.1:8080/api").is_err());
        // Sensitive hostnames must be rejected.
        assert!(validate_mcp_url("http://localhost/api").is_err());
        assert!(validate_mcp_url("http://myservice.local/api").is_err());
        // Non-http schemes must be rejected.
        assert!(validate_mcp_url("ftp://example.com/").is_err());
        assert!(validate_mcp_url("file:///etc/passwd").is_err());
        // Public addresses must be accepted.
        assert!(validate_mcp_url("https://example.com/api").is_ok());
        assert!(validate_mcp_url("http://example.com:3000/api").is_ok());
        assert!(validate_mcp_url("https://api.example.com/v1/mcp").is_ok());
    }

    #[test]
    fn test_validate_mcp_url_blocks_172_16_range() {
        // All 172.16.x.x–172.31.x.x must be blocked.
        assert!(validate_mcp_url("http://172.16.0.1/api").is_err());
        assert!(validate_mcp_url("http://172.31.255.255/api").is_err());
        // 172.15.x.x and 172.32.x.x are public and must be allowed.
        assert!(validate_mcp_url("http://172.15.0.1/api").is_ok());
        assert!(validate_mcp_url("http://172.32.0.1/api").is_ok());
    }
}
