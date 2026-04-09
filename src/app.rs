use crate::{auth::require_api_key, handlers, manager::AppState};
use axum::{
    extract::DefaultBodyLimit,
    middleware,
    routing::{delete, get, post},
    Router,
};
use std::time::Duration;
use tower::ServiceBuilder;
use tower_http::{
    cors::CorsLayer,
    timeout::TimeoutLayer,
    trace::TraceLayer,
};

fn build_cors() -> CorsLayer {
    match std::env::var("ALLOWED_ORIGINS") {
        Ok(origins) if !origins.is_empty() => {
            let headers: Vec<axum::http::HeaderValue> = origins
                .split(',')
                .map(|o| o.trim())
                .filter(|o| !o.is_empty())
                .filter_map(|o| o.parse().ok())
                .collect();
            CorsLayer::new()
                .allow_origin(headers)
                .allow_methods([
                    axum::http::Method::GET,
                    axum::http::Method::POST,
                    axum::http::Method::DELETE,
                    axum::http::Method::OPTIONS,
                ])
                .allow_headers([
                    axum::http::header::AUTHORIZATION,
                    axum::http::header::CONTENT_TYPE,
                ])
        }
        _ => {
            // No ALLOWED_ORIGINS set — deny all cross-origin requests
            CorsLayer::new()
        }
    }
}

pub fn build_app(state: AppState) -> Router {
    let cors = build_cors();

    // Protected routes — require a valid Bearer token when API_KEY env var is set
    let protected = Router::new()
        .route("/api/sessions", post(handlers::sessions::create).get(handlers::sessions::list))
        .route("/api/sessions/{id}", delete(handlers::sessions::delete_session))
        .route("/api/sessions/{id}/prompt", post(handlers::sessions::send_prompt))
        .route("/api/sessions/{id}/events", get(handlers::sessions::events))
        .route("/api/sessions/{id}/stream", get(handlers::stream::stream))
        .route("/api/sessions/{id}/resume", post(handlers::sessions::resume))
        .route("/api/agents", get(handlers::sessions::list_agents))
        .route("/v1/models", get(handlers::openai::models))
        .route("/v1/chat/completions", post(handlers::openai::chat_completions))
        .route_layer(middleware::from_fn(require_api_key));

    Router::new()
        .route("/health", get(handlers::health::health))
        .merge(protected)
        .with_state(state)
        .layer(
            ServiceBuilder::new()
                .layer(TraceLayer::new_for_http())
                .layer(TimeoutLayer::with_status_code(
                    axum::http::StatusCode::REQUEST_TIMEOUT,
                    Duration::from_secs(30),
                ))
                .layer(DefaultBodyLimit::max(1024 * 1024)) // 1 MiB
                .layer(cors),
        )
}
