use crate::{handlers, manager::AppState};
use axum::{
    routing::{delete, get, post},
    Router,
};
use tower_http::cors::{Any, CorsLayer};

pub fn build_app(state: AppState) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new()
        .route("/health", get(handlers::health::health))
        .route("/api/sessions", post(handlers::sessions::create).get(handlers::sessions::list))
        .route("/api/sessions/{id}", delete(handlers::sessions::delete_session))
        .route("/api/sessions/{id}/prompt", post(handlers::sessions::send_prompt))
        .route("/api/sessions/{id}/events", get(handlers::sessions::events))
        .route("/api/sessions/{id}/stream", get(handlers::stream::stream))
        .route("/api/sessions/{id}/resume", post(handlers::sessions::resume))
        .route("/api/agents", get(handlers::sessions::list_agents))
        .route("/v1/models", get(handlers::openai::models))
        .route("/v1/chat/completions", post(handlers::openai::chat_completions))
        .with_state(state)
        .layer(cors)
}
