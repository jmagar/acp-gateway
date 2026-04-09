use axum::{body::Body, extract::Request, http::StatusCode, middleware::Next, response::Response};
use std::sync::OnceLock;
use subtle::ConstantTimeEq;

static API_KEY: OnceLock<Option<String>> = OnceLock::new();

fn api_key() -> Option<&'static str> {
    API_KEY.get_or_init(|| std::env::var("API_KEY").ok()).as_deref()
}

/// Call once at startup. Logs a warning if `API_KEY` is not configured.
pub fn check_api_key_configured() {
    if api_key().is_none() {
        tracing::warn!(
            "API_KEY not set — running without authentication; all endpoints are publicly accessible"
        );
    }
}

pub async fn require_api_key(request: Request<Body>, next: Next) -> Result<Response, StatusCode> {
    if let Some(key) = api_key() {
        let provided = request
            .headers()
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "));
        let valid = provided
            .map(|p| {
                let p_bytes = p.as_bytes();
                let k_bytes = key.as_bytes();
                // constant-time length comparison via xor, then content
                let len_ok = (p_bytes.len() ^ k_bytes.len()) == 0;
                let content_ok: bool = p_bytes.ct_eq(k_bytes).into();
                len_ok & content_ok
            })
            .unwrap_or(false);
        if !valid {
            return Err(StatusCode::UNAUTHORIZED);
        }
    }
    Ok(next.run(request).await)
}
