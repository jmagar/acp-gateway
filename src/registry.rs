use crate::types::{SessionMeta, SessionStatus};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{io::AsyncWriteExt, sync::Mutex};

pub struct SessionRegistry {
    path: PathBuf,
    sessions: Arc<Mutex<HashMap<String, SessionMeta>>>,
}

impl SessionRegistry {
    pub async fn new(path: &Path) -> crate::error::Result<Self> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }

        let mut sessions = HashMap::new();
        if tokio::fs::try_exists(path).await? {
            let contents = tokio::fs::read_to_string(path).await?;
            for line in contents.lines() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                let meta: SessionMeta = serde_json::from_str(line)?;
                sessions.insert(meta.session_id.clone(), meta);
            }
        }

        for meta in sessions.values_mut() {
            if meta.status == SessionStatus::Active {
                meta.status = SessionStatus::Resumable;
            }
        }

        Ok(Self {
            path: path.to_path_buf(),
            sessions: Arc::new(Mutex::new(sessions)),
        })
    }

    pub async fn append(&self, meta: &SessionMeta) -> crate::error::Result<()> {
        let line = format!("{}\n", serde_json::to_string(meta)?);
        let mut file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await?;
        file.write_all(line.as_bytes()).await?;

        let mut sessions = self.sessions.lock().await;
        sessions.insert(meta.session_id.clone(), meta.clone());
        Ok(())
    }

    pub async fn update_status(
        &self,
        session_id: &str,
        status: SessionStatus,
    ) -> crate::error::Result<()> {
        let updated = {
            let mut sessions = self.sessions.lock().await;
            match sessions.get_mut(session_id) {
                Some(meta) => {
                    meta.status = status;
                    meta.clone()
                }
                None => return Ok(()),
            }
        };

        self.append(&updated).await
    }

    pub async fn list_async(&self) -> Vec<SessionMeta> {
        let sessions = self.sessions.lock().await;
        let mut values: Vec<_> = sessions.values().cloned().collect();
        values.sort_by_key(|session| session.created_at);
        values
    }

    pub async fn get_async(&self, session_id: &str) -> Option<SessionMeta> {
        let sessions = self.sessions.lock().await;
        sessions.get(session_id).cloned()
    }
}
