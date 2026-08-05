use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AppError {
    #[error("session not found: {0}")]
    SessionNotFound(String),

    #[error("session not active (dead or not yet resumed): {0}")]
    SessionDead(String),

    #[error("agent not found: {0}")]
    AgentNotFound(String),

    #[error("ACP error: {0}")]
    Acp(#[from] anyhow::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    #[error("invalid request: {0}")]
    InvalidRequest(String),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, code) = match &self {
            Self::SessionNotFound(_) => (StatusCode::NOT_FOUND, "SESSION_NOT_FOUND"),
            Self::SessionDead(_) => (StatusCode::GONE, "SESSION_DEAD"),
            Self::AgentNotFound(_) => (StatusCode::BAD_REQUEST, "AGENT_NOT_FOUND"),
            Self::Acp(_) => (StatusCode::INTERNAL_SERVER_ERROR, "ACP_ERROR"),
            Self::Io(_) => (StatusCode::INTERNAL_SERVER_ERROR, "IO_ERROR"),
            Self::Serde(_) => (StatusCode::INTERNAL_SERVER_ERROR, "SERDE_ERROR"),
            Self::InvalidRequest(_) => (StatusCode::BAD_REQUEST, "INVALID_REQUEST"),
        };

        (status, Json(json!({ "error": code, "message": self.to_string() }))).into_response()
    }
}

pub type Result<T> = std::result::Result<T, AppError>;
