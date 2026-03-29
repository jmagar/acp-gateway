use acp_gateway::{
    app::build_app,
    config::ServerConfig,
    manager::{AppState, SessionManager},
};
use std::{net::SocketAddr, sync::Arc};
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
    let app = build_app(AppState {
        sessions: Arc::new(manager),
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
