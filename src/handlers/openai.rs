use crate::{
    agent::{connect_agent, get_agent, known_agents, AgentHandle},
    error::AppError,
    events::extract_text_from_notification,
    manager::AppState,
    types::{
        OpenAiChatRequest, OpenAiChatResponse, OpenAiChoice, OpenAiDelta, OpenAiMessageOut,
        OpenAiStreamChunk, OpenAiStreamChoice,
    },
};
use agent_client_protocol::SessionId;
use axum::{
    extract::State,
    response::{
        sse::{Event, Sse},
        IntoResponse, Response,
    },
    Json,
};
use chrono::Utc;
use serde_json::json;
use std::{convert::Infallible, path::PathBuf, time::Duration};
use uuid::Uuid;

pub async fn models() -> Json<serde_json::Value> {
    let data: Vec<_> = known_agents()
        .values()
        .map(|agent| {
            json!({
                "id": agent.name,
                "object": "model",
                "created": Utc::now().timestamp(),
                "owned_by": "acp-gateway"
            })
        })
        .collect();

    Json(json!({ "object": "list", "data": data }))
}

pub async fn chat_completions(
    State(_state): State<AppState>,
    Json(body): Json<OpenAiChatRequest>,
) -> Result<Response, AppError> {
    let agent_def = get_agent(&body.model)
        .ok_or_else(|| AppError::AgentNotFound(body.model.clone()))?;
    let user_message = body
        .messages
        .iter()
        .rev()
        .find(|message| message.role == "user")
        .map(|message| message.content.clone())
        .unwrap_or_default();

    let handle = connect_agent(&agent_def).await.map_err(AppError::Acp)?;
    let session = handle
        .new_session(default_cwd(), vec![])
        .await
        .map_err(AppError::Acp)?;
    let request_id = format!("chatcmpl-{}", Uuid::new_v4().simple());
    let created = Utc::now().timestamp();

    if body.stream {
        streaming_response(handle, session.session_id, user_message, request_id, created, body.model).await
    } else {
        non_streaming_response(handle, session.session_id, user_message, request_id, created, body.model).await
    }
}

async fn non_streaming_response(
    handle: AgentHandle,
    session_id: SessionId,
    user_message: String,
    request_id: String,
    created: i64,
    model: String,
) -> Result<Response, AppError> {
    let mut updates = handle.subscribe_updates();
    handle
        .prompt_text(session_id, user_message)
        .await
        .map_err(AppError::Acp)?;

    tokio::time::sleep(Duration::from_millis(100)).await;
    let mut content = String::new();
    while let Ok(notification) = updates.try_recv() {
        if let Some(text) = extract_text_from_notification(&notification) {
            content.push_str(&text);
        }
    }

    let _ = handle.close().await;

    Ok(Json(OpenAiChatResponse {
        id: request_id,
        object: "chat.completion".to_string(),
        created,
        model,
        choices: vec![OpenAiChoice {
            index: 0,
            message: OpenAiMessageOut {
                role: "assistant".to_string(),
                content,
            },
            finish_reason: "stop".to_string(),
        }],
    })
    .into_response())
}

async fn streaming_response(
    handle: AgentHandle,
    session_id: SessionId,
    user_message: String,
    request_id: String,
    created: i64,
    model: String,
) -> Result<Response, AppError> {
    let mut updates = handle.subscribe_updates();
    let prompt_handle = handle.clone();

    let stream = async_stream::stream! {
        let role_chunk = OpenAiStreamChunk {
            id: request_id.clone(),
            object: "chat.completion.chunk".to_string(),
            created,
            model: model.clone(),
            choices: vec![OpenAiStreamChoice {
                index: 0,
                delta: OpenAiDelta {
                    role: Some("assistant".to_string()),
                    content: None,
                },
                finish_reason: None,
            }],
        };
        yield Ok::<Event, Infallible>(Event::default().data(serde_json::to_string(&role_chunk).unwrap()));

        let mut prompt_task = tokio::spawn(async move {
            prompt_handle.prompt_text(session_id, user_message).await
        });

        loop {
            tokio::select! {
                prompt_result = &mut prompt_task => {
                    if let Ok(Ok(())) = prompt_result.map(|result| result.map(|_| ())) {
                        while let Ok(notification) = updates.try_recv() {
                            if let Some(text) = extract_text_from_notification(&notification) {
                                let chunk = OpenAiStreamChunk {
                                    id: request_id.clone(),
                                    object: "chat.completion.chunk".to_string(),
                                    created,
                                    model: model.clone(),
                                    choices: vec![OpenAiStreamChoice {
                                        index: 0,
                                        delta: OpenAiDelta { role: None, content: Some(text) },
                                        finish_reason: None,
                                    }],
                                };
                                yield Ok(Event::default().data(serde_json::to_string(&chunk).unwrap()));
                            }
                        }
                    }

                    let done_chunk = OpenAiStreamChunk {
                        id: request_id.clone(),
                        object: "chat.completion.chunk".to_string(),
                        created,
                        model: model.clone(),
                        choices: vec![OpenAiStreamChoice {
                            index: 0,
                            delta: OpenAiDelta { role: None, content: None },
                            finish_reason: Some("stop".to_string()),
                        }],
                    };
                    yield Ok(Event::default().data(serde_json::to_string(&done_chunk).unwrap()));
                    yield Ok(Event::default().data("[DONE]"));
                    let _ = handle.close().await;
                    break;
                }
                update = updates.recv() => {
                    match update {
                        Ok(notification) => {
                            if let Some(text) = extract_text_from_notification(&notification) {
                                let chunk = OpenAiStreamChunk {
                                    id: request_id.clone(),
                                    object: "chat.completion.chunk".to_string(),
                                    created,
                                    model: model.clone(),
                                    choices: vec![OpenAiStreamChoice {
                                        index: 0,
                                        delta: OpenAiDelta { role: None, content: Some(text) },
                                        finish_reason: None,
                                    }],
                                };
                                yield Ok(Event::default().data(serde_json::to_string(&chunk).unwrap()));
                            }
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            yield Ok(Event::default().data("[DONE]"));
                            break;
                        }
                    }
                }
            }
        }
    };

    Ok(Sse::new(stream).into_response())
}

fn default_cwd() -> PathBuf {
    std::env::var("DEFAULT_CWD")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("/tmp"))
}
