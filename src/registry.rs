use crate::types::{SessionMeta, SessionStatus};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::{
    io::{AsyncWriteExt, BufWriter},
    sync::RwLock,
};

struct RegistryInner {
    sessions: HashMap<String, SessionMeta>,
    writer: BufWriter<tokio::fs::File>,
}

pub struct SessionRegistry {
    #[allow(dead_code)]
    path: PathBuf,
    inner: Arc<RwLock<RegistryInner>>,
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
                // Last entry per session_id wins (natural JSONL log order)
                sessions.insert(meta.session_id.clone(), meta);
            }
        }

        // Downgrade any sessions that were Active at shutdown → Resumable
        for meta in sessions.values_mut() {
            if meta.status == SessionStatus::Active {
                meta.status = SessionStatus::Resumable;
            }
        }

        // .11 — Compact the file: write one line per session to a temp file,
        // then atomically rename it over the original.
        let tmp_path = path.with_extension("tmp");
        {
            let mut tmp = tokio::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp_path)
                .await?;
            let mut sorted: Vec<_> = sessions.values().collect();
            sorted.sort_by_key(|m| m.created_at);
            for meta in sorted {
                let line = format!("{}\n", serde_json::to_string(meta)?);
                tmp.write_all(line.as_bytes()).await?;
            }
            tmp.flush().await?;
        }
        tokio::fs::rename(&tmp_path, path).await?;

        // .18 — Open a persistent append-mode file handle
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .await?;
        let writer = BufWriter::new(file);

        Ok(Self {
            path: path.to_path_buf(),
            inner: Arc::new(RwLock::new(RegistryInner { sessions, writer })),
        })
    }

    pub async fn append(&self, meta: &SessionMeta) -> crate::error::Result<()> {
        let line = format!("{}\n", serde_json::to_string(meta)?);
        // .13 / .18 — single write lock covers both the map and the file writer
        let mut inner = self.inner.write().await;
        inner.writer.write_all(line.as_bytes()).await?;
        inner.writer.flush().await?;
        inner.sessions.insert(meta.session_id.clone(), meta.clone());
        Ok(())
    }

    pub async fn update_status(
        &self,
        session_id: &str,
        status: SessionStatus,
    ) -> crate::error::Result<()> {
        let updated = {
            let mut inner = self.inner.write().await;
            match inner.sessions.get_mut(session_id) {
                Some(meta) => {
                    meta.status = status;
                    meta.clone()
                }
                None => return Ok(()),
            }
        };
        self.append(&updated).await
    }

    // .13 — read-only paths use a shared read lock
    pub async fn list_async(&self) -> Vec<SessionMeta> {
        let inner = self.inner.read().await;
        let mut values: Vec<_> = inner.sessions.values().cloned().collect();
        values.sort_by_key(|session| session.created_at);
        values
    }

    pub async fn get_async(&self, session_id: &str) -> Option<SessionMeta> {
        let inner = self.inner.read().await;
        inner.sessions.get(session_id).cloned()
    }

    /// Flush the buffered writer — call during graceful shutdown.
    pub async fn flush(&self) -> crate::error::Result<()> {
        let mut inner = self.inner.write().await;
        inner.writer.flush().await?;
        Ok(())
    }
}
