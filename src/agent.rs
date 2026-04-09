use anyhow::{anyhow, Context, Result};
use futures::StreamExt;
use std::{collections::HashMap, path::PathBuf, thread};
use tokio::sync::{broadcast, mpsc, oneshot};

use agent_client_protocol::{
    self as acp, ContentBlock, Implementation, LoadSessionResponse, NewSessionResponse,
    PromptResponse, ProtocolVersion, SessionNotification, SessionId,
};

#[derive(Debug, Clone)]
pub struct AgentDef {
    pub name: String,
    pub title: String,
    pub command: String,
    pub args: Vec<String>,
}

#[derive(Clone)]
pub struct AgentHandle {
    command_tx: mpsc::Sender<AgentCommand>,
    updates_tx: broadcast::Sender<SessionNotification>,
}

enum AgentCommand {
    NewSession {
        cwd: PathBuf,
        mcp_servers: Vec<acp::McpServer>,
        resp: oneshot::Sender<Result<NewSessionResponse>>,
    },
    LoadSession {
        session_id: SessionId,
        cwd: PathBuf,
        mcp_servers: Vec<acp::McpServer>,
        resp: oneshot::Sender<Result<LoadSessionResponse>>,
    },
    Prompt {
        session_id: SessionId,
        prompt: Vec<ContentBlock>,
        resp: oneshot::Sender<Result<PromptResponse>>,
    },
    Close {
        resp: oneshot::Sender<Result<()>>,
    },
}

impl AgentHandle {
    pub fn subscribe_updates(&self) -> broadcast::Receiver<SessionNotification> {
        self.updates_tx.subscribe()
    }

    pub async fn new_session(
        &self,
        cwd: PathBuf,
        mcp_servers: Vec<acp::McpServer>,
    ) -> Result<NewSessionResponse> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.command_tx
            .send(AgentCommand::NewSession {
                cwd,
                mcp_servers,
                resp: resp_tx,
            })
            .await
            .map_err(|_| anyhow!("agent worker is no longer running"))?;
        resp_rx.await.context("agent worker dropped new_session response")?
    }

    pub async fn load_session(
        &self,
        session_id: SessionId,
        cwd: PathBuf,
        mcp_servers: Vec<acp::McpServer>,
    ) -> Result<LoadSessionResponse> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.command_tx
            .send(AgentCommand::LoadSession {
                session_id,
                cwd,
                mcp_servers,
                resp: resp_tx,
            })
            .await
            .map_err(|_| anyhow!("agent worker is no longer running"))?;
        resp_rx.await.context("agent worker dropped load_session response")?
    }

    pub async fn prompt(&self, session_id: SessionId, prompt: Vec<ContentBlock>) -> Result<PromptResponse> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.command_tx
            .send(AgentCommand::Prompt {
                session_id,
                prompt,
                resp: resp_tx,
            })
            .await
            .map_err(|_| anyhow!("agent worker is no longer running"))?;
        resp_rx.await.context("agent worker dropped prompt response")?
    }

    pub async fn prompt_text(&self, session_id: SessionId, content: String) -> Result<PromptResponse> {
        self.prompt(
            session_id,
            vec![ContentBlock::Text(acp::TextContent::new(content))],
        )
        .await
    }

    pub async fn close(&self) -> Result<()> {
        let (resp_tx, resp_rx) = oneshot::channel();
        self.command_tx
            .send(AgentCommand::Close { resp: resp_tx })
            .await
            .map_err(|_| anyhow!("agent worker is no longer running"))?;
        resp_rx.await.context("agent worker dropped close response")?
    }
}

pub fn known_agents() -> HashMap<String, AgentDef> {
    let mut map = HashMap::new();
    map.insert(
        "claude-code".to_string(),
        AgentDef {
            name: "claude-code".to_string(),
            title: "Claude Code".to_string(),
            command: "./claude-agent-acp".to_string(),
            args: vec![],
        },
    );
    map
}

pub fn get_agent(name: &str) -> Option<AgentDef> {
    known_agents().get(name).cloned()
}

pub async fn connect_agent(def: &AgentDef) -> Result<AgentHandle> {
    let (command_tx, mut command_rx) = mpsc::channel(32);
    let (updates_tx, _) = broadcast::channel(1024);
    let (ready_tx, ready_rx) = oneshot::channel();
    let def = def.clone();
    let updates_for_thread = updates_tx.clone();

    thread::spawn(move || {
        let runtime = match tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
        {
            Ok(runtime) => runtime,
            Err(error) => {
                let _ = ready_tx.send(Err(anyhow!(error).context("failed to build ACP runtime")));
                return;
            }
        };

        let local_set = tokio::task::LocalSet::new();
        local_set.block_on(&runtime, async move {
            use acpx::{AgentServer as _, AgentServerMetadata, CommandAgentServer, CommandSpec, RuntimeContext};

            let metadata = AgentServerMetadata::new(
                def.name.clone(),
                def.title.clone(),
                env!("CARGO_PKG_VERSION"),
            );
            let command = CommandSpec::new(def.command.clone()).args(def.args.clone());
            let server = CommandAgentServer::new(metadata, command);
            let runtime = RuntimeContext::new(|task| {
                tokio::task::spawn_local(task);
            });

            let connection = match server
                .connect(&runtime)
                .await
                .context("failed to spawn agent subprocess")
            {
                Ok(connection) => connection,
                Err(error) => {
                    let _ = ready_tx.send(Err(error));
                    return;
                }
            };

            if let Err(error) = connection
                .initialize(
                    acp::InitializeRequest::new(ProtocolVersion::V1).client_info(
                        Implementation::new("acp-gateway", env!("CARGO_PKG_VERSION"))
                            .title("ACP Gateway"),
                    ),
                )
                .await
                .context("ACP initialize failed")
            {
                let _ = ready_tx.send(Err(error));
                let _ = connection.close().await;
                return;
            }

            let mut updates = connection.subscribe_session_updates();
            let updates_tx = updates_for_thread.clone();
            tokio::task::spawn_local(async move {
                while let Some(notification) = updates.next().await {
                    let _ = updates_tx.send(notification);
                }
            });

            let _ = ready_tx.send(Ok(()));

            while let Some(command) = command_rx.recv().await {
                match command {
                    AgentCommand::NewSession {
                        cwd,
                        mcp_servers,
                        resp,
                    } => {
                        let result = connection
                            .new_session(acp::NewSessionRequest::new(cwd).mcp_servers(mcp_servers))
                            .await
                            .map_err(anyhow::Error::from);
                        let _ = resp.send(result);
                    }
                    AgentCommand::LoadSession {
                        session_id,
                        cwd,
                        mcp_servers,
                        resp,
                    } => {
                        let result = connection
                            .load_session(
                                acp::LoadSessionRequest::new(session_id, cwd)
                                    .mcp_servers(mcp_servers),
                            )
                            .await
                            .map_err(anyhow::Error::from);
                        let _ = resp.send(result);
                    }
                    AgentCommand::Prompt {
                        session_id,
                        prompt,
                        resp,
                    } => {
                        let result = connection
                            .prompt(acp::PromptRequest::new(session_id, prompt))
                            .await
                            .map_err(anyhow::Error::from);
                        let _ = resp.send(result);
                    }
                    AgentCommand::Close { resp } => {
                        let result = connection.close().await.map_err(anyhow::Error::from);
                        let _ = resp.send(result);
                        return;
                    }
                }
            }

            let _ = connection.close().await;
        });
    });

    ready_rx
        .await
        .context("agent worker failed before reporting readiness")??;

    Ok(AgentHandle {
        command_tx,
        updates_tx,
    })
}

pub async fn resume_session(
    handle: &AgentHandle,
    session_id: &str,
    cwd: &str,
    mcp_servers: Vec<acp::McpServer>,
) -> Result<()> {
    handle
        .load_session(SessionId::new(session_id.to_string()), PathBuf::from(cwd), mcp_servers)
        .await
        .context("ACP load_session failed")?;
    Ok(())
}
