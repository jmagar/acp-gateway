use crate::{
    agent::AgentHandle,
    registry::SessionRegistry,
    types::{SessionMeta, StoredEvent},
};
use dashmap::DashMap;
use std::{path::PathBuf, sync::Arc};
use tokio::sync::{broadcast, RwLock};

pub const BROADCAST_CAPACITY: usize = 1024;

pub struct ActiveSession {
    pub meta: SessionMeta,
    pub events: Arc<RwLock<Vec<StoredEvent>>>,
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
            events: Arc::new(RwLock::new(Vec::new())),
            tx,
            agent,
        }
    }
}

pub struct SessionManager {
    pub active: Arc<DashMap<String, ActiveSession>>,
    pub registry: Arc<SessionRegistry>,
}

impl SessionManager {
    pub async fn new(registry_path: PathBuf) -> crate::error::Result<Self> {
        let registry = SessionRegistry::new(&registry_path).await?;
        Ok(Self {
            active: Arc::new(DashMap::new()),
            registry: Arc::new(registry),
        })
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

    pub async fn push_event(&self, session_id: &str, event_type: String, data: serde_json::Value) -> Option<usize> {
        let maybe_session = self.active.get(session_id).map(|entry| {
            (Arc::clone(&entry.events), entry.tx.clone())
        });

        if let Some((events, tx)) = maybe_session {
            let mut guard = events.write().await;
            let index = guard.len(); // atomic: assigned under write lock
            let event = StoredEvent { index, event_type, data };
            guard.push(event.clone());
            drop(guard);
            let _ = tx.send((index, event));
            Some(index)
        } else {
            None
        }
    }

    pub async fn get_events(&self, session_id: &str, from: usize, limit: usize) -> Vec<StoredEvent> {
        let events = match self.active.get(session_id) {
            Some(session) => Arc::clone(&session.events),
            None => return Vec::new(),
        };

        let values = events
            .read()
            .await
            .iter()
            .skip(from)
            .take(limit)
            .cloned()
            .collect();

        values
    }

    pub fn subscribe(&self, session_id: &str) -> Option<broadcast::Receiver<(usize, StoredEvent)>> {
        self.active.get(session_id).map(|session| session.tx.subscribe())
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

        let len = events.read().await.len();
        len
    }
}

#[derive(Clone)]
pub struct AppState {
    pub sessions: Arc<SessionManager>,
}
