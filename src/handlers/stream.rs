use crate::{error::AppError, manager::AppState, types::StreamQuery};
use axum::{
    extract::{Path, Query, State},
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse,
    },
};
use futures::{stream, StreamExt};
use std::{convert::Infallible, time::Duration};
use tokio_stream::wrappers::BroadcastStream;

pub async fn stream(
    State(state): State<AppState>,
    Path(session_id): Path<String>,
    Query(query): Query<StreamQuery>,
) -> Result<impl IntoResponse, AppError> {
    if !state.sessions.contains(&session_id) {
        if state.sessions.registry.get_async(&session_id).await.is_some() {
            return Err(AppError::SessionDead(session_id));
        }
        return Err(AppError::SessionNotFound(session_id));
    }

    let buffered = state
        .sessions
        .get_events(&session_id, query.from, usize::MAX)
        .await;
    let rx = state
        .sessions
        .subscribe(&session_id)
        .ok_or_else(|| AppError::SessionDead(session_id.clone()))?;

    let replay = stream::iter(buffered.into_iter().map(|event| {
        Ok::<Event, Infallible>(
            Event::default()
                .id(event.index.to_string())
                .event(event.event_type)
                .data(serde_json::to_string(&event.data).unwrap_or_default()),
        )
    }));

    let live = BroadcastStream::new(rx).filter_map(|result| async move {
        result.ok().map(|(_index, event)| {
            Ok::<Event, Infallible>(
                Event::default()
                    .id(event.index.to_string())
                    .event(event.event_type)
                    .data(serde_json::to_string(&event.data).unwrap_or_default()),
            )
        })
    });

    Ok(Sse::new(replay.chain(live)).keep_alive(KeepAlive::new().interval(Duration::from_secs(15))))
}
