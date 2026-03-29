use axum::{
    body::Body,
    extract::Request,
    http::StatusCode,
    middleware::Next,
    response::Response,
};
use std::sync::OnceLock;

static API_KEY: OnceLock<Option<String>> = OnceLock::new();

fn api_key() -> Option<&'static str> {
    API_KEY.get_or_init(|| std::env::var("API_KEY").ok()).as_deref()
}

pub async fn require_api_key(request: Request<Body>, next: Next) -> Result<Response, StatusCode> {
    if let Some(key) = api_key() {
        let provided = request
            .headers()
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));
        if provided != Some(key) {
            return Err(StatusCode::UNAUTHORIZED);
        }
    }
    Ok(next.run(request).await)
}
