use acp_gateway::{
    agent::known_agents,
    app::build_app,
    config::ServerConfig,
    manager::{AppState, SessionManager},
    pool::AgentPool,
};
use std::{collections::HashMap, net::SocketAddr, sync::Arc};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "acp_gateway=debug,tower_http=debug".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    // Warn if API_KEY is not configured (bead .47)
    acp_gateway::auth::check_api_key_configured();

    // Validate DEFAULT_CWD at startup so misconfiguration fails fast (bead .52)
    let validated_default_cwd = {
        let raw = std::env::var("DEFAULT_CWD").unwrap_or_else(|_| "/tmp".to_string());
        std::fs::canonicalize(&raw)
            .map(|p| {
                if !p.is_dir() {
                    panic!("DEFAULT_CWD '{}' is not a directory", raw);
                }
                p
            })
            .unwrap_or_else(|e| panic!("DEFAULT_CWD '{}' is invalid: {}", raw, e))
    };

    let config = ServerConfig::from_env();
    let manager = SessionManager::new(config.data_dir.join("sessions.jsonl"))
        .await
        .expect("failed to initialize session registry");

    let pools: HashMap<String, AgentPool> = known_agents()
        .into_iter()
        .map(|(name, def)| (name, AgentPool::new(def)))
        .collect();

    let app_state = AppState {
        sessions: Arc::new(manager),
        pools: Arc::new(pools),
        default_cwd: validated_default_cwd,
    };

    // Capture handles for graceful shutdown (bead .16)
    let sessions_for_shutdown = Arc::clone(&app_state.sessions);
    let registry_for_shutdown = Arc::clone(&app_state.sessions.registry);

    let app = build_app(app_state);

    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    tracing::info!(%addr, "listening");
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("failed to bind TCP listener");

    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            tokio::signal::ctrl_c()
                .await
                .expect("failed to listen for ctrl_c");
            tracing::info!("shutdown signal received; closing agent connections");

            // Collect all session handles FIRST (drops DashMap iter guard immediately)
            // so we never .await while holding a DashMap shard read lock (deadlock risk).
            let sessions_snapshot: Vec<(String, Option<acp_gateway::agent::AgentHandle>)> =
                sessions_for_shutdown
                    .active
                    .iter()
                    .map(|e| (e.key().clone(), e.value().agent.clone()))
                    .collect();
            // DashMap lock is released here

            // Now await outside the lock
            for (session_id, agent) in sessions_snapshot {
                if let Some(agent) = agent {
                    let _ = agent.close().await;
                }
                // Mark as resumable so they can be resumed after restart
                let _ = sessions_for_shutdown
                    .registry
                    .update_status(&session_id, acp_gateway::types::SessionStatus::Resumable)
                    .await;
            }

            // Flush registry write buffer
            let _ = registry_for_shutdown.flush().await;
            tracing::info!("shutdown complete");
        })
        .await
        .expect("server exited unexpectedly");
}
