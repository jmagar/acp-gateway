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

    // FIX (.54/.19): subscribe FIRST to avoid missing events between snapshot and subscribe
    let rx = state
        .sessions
        .subscribe(&session_id)
        .ok_or_else(|| AppError::SessionDead(session_id.clone()))?;

    // FIX (.20): cap replay at 500 events instead of usize::MAX
    let buffered = state
        .sessions
        .get_events(&session_id, query.from, 500)
        .await;

    // Track the highest index we replayed so live stream can filter duplicates
    let last_replayed = buffered.last().map(|e| e.index);

    let replay = stream::iter(buffered.into_iter().map(|event| {
        Ok::<Event, Infallible>(
            Event::default()
                .id(event.index.to_string())
                .event(event.event_type)
                .data(serde_json::to_string(&event.data).unwrap_or_default()),
        )
    }));

    // FIX (.54/.19): filter live events to only those not already covered by snapshot replay
    let live = BroadcastStream::new(rx).filter_map(move |result| async move {
        result.ok().and_then(|(_index, event)| {
            if let Some(last) = last_replayed {
                if event.index <= last {
                    return None;
                }
            }
            Some(Ok::<Event, Infallible>(
                Event::default()
                    .id(event.index.to_string())
                    .event(event.event_type)
                    .data(serde_json::to_string(&event.data).unwrap_or_default()),
            ))
        })
    });

    Ok(Sse::new(replay.chain(live)).keep_alive(KeepAlive::new().interval(Duration::from_secs(15))))
}
