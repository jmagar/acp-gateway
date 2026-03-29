use crate::manager::SessionManager;
use agent_client_protocol::{ContentBlock, SessionNotification, SessionUpdate};
use std::sync::Arc;

pub fn spawn_ingestion_loop(manager: Arc<SessionManager>, session_id: String) {
    tokio::spawn(async move {
        let mut updates = match manager.subscribe_agent_updates(&session_id) {
            Some(updates) => updates,
            None => {
                tracing::warn!(session_id, "ingestion loop: no agent update stream available");
                return;
            }
        };

        loop {
            match updates.recv().await {
                Ok(notification) => {
                    if notification.session_id.0.as_ref() != session_id {
                        continue;
                    }

                    if let Some((event_type, data)) = event_data_from_notification(&notification) {
                        manager.push_event(&session_id, event_type, data).await;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(skipped)) => {
                    tracing::warn!(session_id, skipped, "ingestion loop lagged behind session updates");
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    tracing::info!(session_id, "ingestion loop closed");
                    break;
                }
            }
        }
    });
}

pub fn event_data_from_notification(notification: &SessionNotification) -> Option<(String, serde_json::Value)> {
    let event_type = event_type_for_update(&notification.update)?;
    let data = serde_json::to_value(&notification.update).ok()?;
    Some((event_type.to_string(), data))
}

pub fn event_type_for_update(update: &SessionUpdate) -> Option<&'static str> {
    match update {
        SessionUpdate::UserMessageChunk(_) => Some("user_message_chunk"),
        SessionUpdate::AgentMessageChunk(_) => Some("agent_message_chunk"),
        SessionUpdate::AgentThoughtChunk(_) => Some("agent_thought_chunk"),
        SessionUpdate::ToolCall(_) => Some("tool_call"),
        SessionUpdate::ToolCallUpdate(_) => Some("tool_call_update"),
        SessionUpdate::Plan(_) => Some("plan"),
        SessionUpdate::AvailableCommandsUpdate(_) => Some("available_commands_update"),
        SessionUpdate::CurrentModeUpdate(_) => Some("current_mode_update"),
        SessionUpdate::ConfigOptionUpdate(_) => Some("config_option_update"),
        _ => None,
    }
}

pub fn extract_text_from_notification(notification: &SessionNotification) -> Option<String> {
    extract_text_from_update(&notification.update)
}

pub fn extract_text_from_update(update: &SessionUpdate) -> Option<String> {
    match update {
        SessionUpdate::AgentMessageChunk(chunk) | SessionUpdate::AgentThoughtChunk(chunk) => {
            match &chunk.content {
                ContentBlock::Text(text) => Some(text.text.clone()),
                _ => None,
            }
        }
        _ => None,
    }
}
