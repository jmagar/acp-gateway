use crate::agent::{connect_agent, AgentDef, AgentHandle};
use std::sync::Arc;
use tokio::sync::Mutex;

/// A simple pool of pre-connected AgentHandles for a single agent type.
/// Handles are reused across requests; the underlying subprocess stays
/// alive between uses. Callers must release() after use — do NOT call
/// handle.close() before releasing.
pub struct AgentPool {
    def: AgentDef,
    handles: Arc<Mutex<Vec<AgentHandle>>>,
}

impl AgentPool {
    pub fn new(def: AgentDef) -> Self {
        Self {
            def,
            handles: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Acquire a handle from the pool. If empty, spawns a new connection.
    pub async fn acquire(&self) -> anyhow::Result<AgentHandle> {
        let mut guard = self.handles.lock().await;
        if let Some(handle) = guard.pop() {
            return Ok(handle);
        }
        // Pool empty — spawn a fresh connection.
        // Drop the lock before the spawn; connect_agent() takes 100-500ms
        // and holding the lock would serialize all pool misses.
        drop(guard);
        connect_agent(&self.def).await
    }

    /// Return a handle to the pool after use. Do NOT call handle.close() before this.
    pub async fn release(&self, handle: AgentHandle) {
        self.handles.lock().await.push(handle);
    }

    /// Return a handle to the pool, but discard (close) it if the pool already
    /// holds `max_idle` or more idle handles. Prevents unbounded subprocess growth.
    pub async fn release_capped(&self, handle: AgentHandle, max_idle: usize) {
        let mut guard = self.handles.lock().await;
        if guard.len() < max_idle {
            guard.push(handle);
        } else {
            drop(guard);
            let _ = handle.close().await;
        }
    }
}
