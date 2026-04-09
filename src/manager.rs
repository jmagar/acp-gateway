use crate::{
    agent::AgentHandle,
    pool::AgentPool,
    registry::SessionRegistry,
    types::{SessionMeta, StoredEvent},
};
use dashmap::DashMap;
use std::{collections::{HashMap, VecDeque}, path::PathBuf, sync::Arc};
use tokio::sync::{broadcast, RwLock};

pub const BROADCAST_CAPACITY: usize = 1024;

/// Maximum number of events kept in memory per session (bead .58/.12).
pub const MAX_EVENTS_PER_SESSION: usize = 10_000;

/// Holds the ring-buffer of stored events together with a monotonically
/// increasing counter so that indices never reset after eviction.
pub struct EventLog {
    pub events: VecDeque<StoredEvent>,
    pub next_index: usize,
}

pub struct ActiveSession {
    pub meta: SessionMeta,
    pub events: Arc<RwLock<EventLog>>,
    pub tx: broadcast::Sender<(usize, StoredEvent)>,
    pub agent: Option<AgentHandle>,
}

impl ActiveSession {
    pub fn new(
        meta: SessionMeta,
        tx: broadcast::Sender<(usize, StoredEvent)>,
        agent: Option<AgentHandle>,
    ) -> Self {
        Self {
            meta,
            events: Arc::new(RwLock::new(EventLog {
                events: VecDeque::new(),
                next_index: 0,
            })),
            tx,
            agent,
        }
    }
}

pub struct SessionManager {
    pub active: Arc<DashMap<String, ActiveSession>>,
    pub registry: Arc<SessionRegistry>,
    /// Base directory used for per-session event log persistence (bead .57).
    pub data_dir: PathBuf,
}

impl SessionManager {
    pub async fn new(registry_path: PathBuf) -> crate::error::Result<Self> {
        let data_dir = registry_path
            .parent()
            .unwrap_or(std::path::Path::new("."))
            .to_path_buf();
        let registry = SessionRegistry::new(&registry_path).await?;
        Ok(Self {
            active: Arc::new(DashMap::new()),
            registry: Arc::new(registry),
            data_dir,
        })
    }

    /// Path of the JSONL event log for a given session (bead .57).
    pub fn session_events_path(&self, session_id: &str) -> PathBuf {
        self.data_dir
            .join("sessions")
            .join(session_id)
            .join("events.jsonl")
    }

    pub fn insert(&self, session_id: String, session: ActiveSession) {
        self.active.insert(session_id, session);
    }

    pub fn remove(&self, session_id: &str) -> Option<ActiveSession> {
        self.active.remove(session_id).map(|(_, session)| session)
    }

    pub fn contains(&self, session_id: &str) -> bool {
        self.active.contains_key(session_id)
    }

    /// Returns the number of currently active sessions (bead .17).
    pub fn active_count(&self) -> usize {
        self.active.len()
    }

    pub async fn push_event(
        &self,
        session_id: &str,
        event_type: String,
        data: serde_json::Value,
    ) -> Option<usize> {
        let maybe_session = self.active.get(session_id).map(|entry| {
            (Arc::clone(&entry.events), entry.tx.clone())
        });

        if let Some((events, tx)) = maybe_session {
            let mut guard = events.write().await;
            let index = guard.next_index; // monotonic: never resets after eviction
            guard.next_index += 1;
            let event = StoredEvent { index, event_type: event_type.clone(), data };
            guard.events.push_back(event.clone());

            // Cap in-memory events at MAX_EVENTS_PER_SESSION (bead .58/.12)
            if guard.events.len() > MAX_EVENTS_PER_SESSION {
                guard.events.pop_front();
            }
            drop(guard);

            let _ = tx.send((index, event.clone()));

            // Persist to disk fire-and-forget (bead .57)
            let events_path = self.session_events_path(session_id);
            let event_json = serde_json::to_string(&event).unwrap_or_default();
            tokio::spawn(async move {
                use tokio::io::AsyncWriteExt;
                if let Some(parent) = events_path.parent() {
                    let _ = tokio::fs::create_dir_all(parent).await;
                }
                if let Ok(mut file) = tokio::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&events_path)
                    .await
                {
                    let _ = file
                        .write_all(format!("{event_json}\n").as_bytes())
                        .await;
                }
            });

            Some(index)
        } else {
            // bead .56: warn when event is dropped because session is absent
            tracing::warn!(
                session_id,
                event_type,
                "event dropped: session not in active map (deleted or not yet created)"
            );
            None
        }
    }

    pub async fn get_events(
        &self,
        session_id: &str,
        from: usize,
        limit: usize,
    ) -> Vec<StoredEvent> {
        let events = match self.active.get(session_id) {
            Some(session) => Arc::clone(&session.events),
            None => return Vec::new(),
        };

        let values = events
            .read()
            .await
            .events
            .iter()
            .filter(|e| e.index >= from)
            .take(limit)
            .cloned()
            .collect();

        values
    }

    pub fn subscribe(
        &self,
        session_id: &str,
    ) -> Option<broadcast::Receiver<(usize, StoredEvent)>> {
        self.active
            .get(session_id)
            .map(|session| session.tx.subscribe())
    }

    pub fn subscribe_agent_updates(
        &self,
        session_id: &str,
    ) -> Option<broadcast::Receiver<agent_client_protocol::SessionNotification>> {
        self.active
            .get(session_id)
            .and_then(|session| session.agent.as_ref().map(AgentHandle::subscribe_updates))
    }

    pub fn agent_handle(&self, session_id: &str) -> Option<AgentHandle> {
        self.active
            .get(session_id)
            .and_then(|session| session.agent.as_ref().cloned())
    }

    pub async fn event_count(&self, session_id: &str) -> usize {
        let events = match self.active.get(session_id) {
            Some(session) => Arc::clone(&session.events),
            None => return 0,
        };

        let len = events.read().await.events.len();
        len
    }
}

#[derive(Clone)]
pub struct AppState {
    pub sessions: Arc<SessionManager>,
    pub pools: Arc<HashMap<String, AgentPool>>,
    /// Validated, canonicalized working directory for OpenAI chat completions (bead .52).
    pub default_cwd: PathBuf,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::SessionMeta;
    use tokio::sync::broadcast;

    fn make_session() -> ActiveSession {
        let meta = SessionMeta {
            session_id: "test-session".to_string(),
            agent: "test-agent".to_string(),
            cwd: "/tmp".to_string(),
            mcp_servers: vec![],
            status: crate::types::SessionStatus::Active,
            created_at: chrono::Utc::now(),
        };
        let (tx, _rx) = broadcast::channel(BROADCAST_CAPACITY);
        ActiveSession::new(meta, tx, None)
    }

    #[tokio::test]
    async fn test_indices_are_monotonic_after_eviction() {
        let session = make_session();
        let total = MAX_EVENTS_PER_SESSION + 100;

        for i in 0..total {
            let mut guard = session.events.write().await;
            let index = guard.next_index;
            guard.next_index += 1;
            let event = StoredEvent {
                index,
                event_type: "test".to_string(),
                data: serde_json::json!(i),
            };
            guard.events.push_back(event);
            if guard.events.len() > MAX_EVENTS_PER_SESSION {
                guard.events.pop_front();
            }
        }

        let guard = session.events.read().await;
        // next_index should equal total (0..total pushed)
        assert_eq!(guard.next_index, total);
        // ring buffer should be capped
        assert_eq!(guard.events.len(), MAX_EVENTS_PER_SESSION);

        // All indices in the buffer must be strictly increasing with no resets
        let indices: Vec<usize> = guard.events.iter().map(|e| e.index).collect();
        for window in indices.windows(2) {
            assert!(
                window[1] == window[0] + 1,
                "non-monotonic indices: {} followed by {}",
                window[0],
                window[1]
            );
        }
        // The first retained index should be 100 (the first 100 were evicted)
        assert_eq!(indices[0], 100);
        assert_eq!(*indices.last().unwrap(), total - 1);
    }

    #[tokio::test]
    async fn test_get_events_filter_by_index_after_eviction() {
        let session = make_session();
        let total = MAX_EVENTS_PER_SESSION + 100;

        for i in 0..total {
            let mut guard = session.events.write().await;
            let index = guard.next_index;
            guard.next_index += 1;
            let event = StoredEvent {
                index,
                event_type: "test".to_string(),
                data: serde_json::json!(i),
            };
            guard.events.push_back(event);
            if guard.events.len() > MAX_EVENTS_PER_SESSION {
                guard.events.pop_front();
            }
        }

        // from = MAX_EVENTS_PER_SESSION - 1 is in the evicted range; the first
        // available index is 100.  filter_by_index should skip nothing from the
        // live buffer and return events starting at index 100.
        let from = MAX_EVENTS_PER_SESSION - 1; // 9999
        let limit = 10;
        let result: Vec<StoredEvent> = session
            .events
            .read()
            .await
            .events
            .iter()
            .filter(|e| e.index >= from)
            .take(limit)
            .cloned()
            .collect();

        // from=9999 is within the live window (indices 100..10099 are retained)
        assert!(!result.is_empty(), "expected events with index >= {from}");
        for e in &result {
            assert!(
                e.index >= from,
                "event index {} is less than from={}",
                e.index,
                from
            );
        }
        assert!(result.len() <= limit);
    }
}
