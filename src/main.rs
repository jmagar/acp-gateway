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

    let config = ServerConfig::from_env();
    let manager = SessionManager::new(config.data_dir.join("sessions.jsonl"))
        .await
        .expect("failed to initialize session registry");

    let pools: HashMap<String, AgentPool> = known_agents()
        .into_iter()
        .map(|(name, def)| (name, AgentPool::new(def)))
        .collect();

    let app = build_app(AppState {
        sessions: Arc::new(manager),
        pools: Arc::new(pools),
    });

    let addr = SocketAddr::from(([0, 0, 0, 0], config.port));
    tracing::info!(%addr, "listening");
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .expect("failed to bind TCP listener");
    axum::serve(listener, app)
        .await
        .expect("server exited unexpectedly");
}
