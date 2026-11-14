//! Provider-specific session control; messaging still goes through the MCP bridge.

use std::collections::{HashMap, VecDeque};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use managed_protocol::{DeliveryState, Runtime};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Serialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum Input {
    Turn { message_id: u64, prompt: String },
    Response { id: String, response: Value },
    Interrupt,
}

#[derive(Debug, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum Event {
    SessionReady {
        session_id: String,
    },
    Started {
        message_id: u64,
        turn_id: String,
    },
    Output {
        message_id: u64,
        text: String,
    },
    Finished {
        message_id: u64,
        state: DeliveryState,
        error: Option<String>,
    },
    Approval {
        message_id: u64,
        id: String,
        request: Value,
    },
    ApprovalResolved {
        id: String,
    },
    Fatal {
        error: String,
    },
}

#[derive(Clone)]
pub struct Settings {
    pub command: Vec<String>,
    pub runtime: Runtime,
    pub cwd: String,
    pub session_id: Option<String>,
    pub model: Option<String>,
    pub instructions: String,
    pub bridge: Value,
}

pub struct Provider {
    input: mpsc::Sender<Input>,
    cancel: CancellationToken,
    task: JoinHandle<Result<()>>,
}

impl Provider {
    pub fn start(settings: Settings, events: mpsc::Sender<Event>) -> Self {
        let (input, commands) = mpsc::channel(16);
        let cancel = CancellationToken::new();
        let cancelled = cancel.clone();
        let task = tokio::spawn(async move {
            let mut process = match Process::spawn(&settings) {
                Ok(process) => process,
                Err(error) => {
                    let _ = events
                        .send(Event::Fatal {
                            error: format!("{error:#}"),
                        })
                        .await;
                    return Ok(());
                }
            };
            let result = tokio::select! {
                _ = cancelled.cancelled() => Ok(()),
                result = run(&mut process, settings, commands, events.clone()) => result,
            };
            if let Err(error) = result {
                let _ = events
                    .send(Event::Fatal {
                        error: format!("{error:#}"),
                    })
                    .await;
            }
            process.shutdown().await
        });
        Self {
            input,
            cancel,
            task,
        }
    }

    pub async fn send(&self, command: Input) -> Result<()> {
        self.input
            .send(command)
            .await
            .context("Runtime process stopped")
    }

    pub async fn stop(self) -> Result<()> {
        let _ = tokio::time::timeout(
            Duration::from_millis(250),
            self.input.send(Input::Interrupt),
        )
        .await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        self.cancel.cancel();
        self.task.await.context("Runtime task panicked")?
    }
}

struct Process {
    child: Child,
    input: ChildStdin,
    lines: Lines<BufReader<ChildStdout>>,
    pending: VecDeque<Value>,
    sequence: u64,
}

impl Process {
    fn spawn(settings: &Settings) -> Result<Self> {
        let (program, args) = settings
            .command
            .split_first()
            .context("Runtime command is empty")?;
        let mut command = Command::new(program);
        command
            .args(args)
            .current_dir(&settings.cwd)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);
        #[cfg(unix)]
        command.process_group(0);
        let mut child = command
            .spawn()
            .with_context(|| format!("Could not start {program}"))?;
        let input = child.stdin.take().context("Runtime stdin is missing")?;
        let output = child.stdout.take().context("Runtime stdout is missing")?;
        Ok(Self {
            child,
            input,
            lines: BufReader::new(output).lines(),
            pending: VecDeque::new(),
            sequence: 0,
        })
    }

    async fn write(&mut self, value: &Value) -> Result<()> {
        let mut bytes = serde_json::to_vec(value)?;
        bytes.push(b'\n');
        self.input.write_all(&bytes).await?;
        self.input.flush().await?;
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<()> {
        #[cfg(unix)]
        if let Some(pid) = self.child.id() {
            // SAFETY: our unreaped child owns this private process group.
            unsafe {
                libc::kill(-(pid as i32), libc::SIGKILL);
            }
        }
        tokio::time::timeout(Duration::from_secs(5), self.child.wait())
            .await
            .context("Runtime did not stop; ownership was not released")??;
        Ok(())
    }

    async fn wire(&mut self) -> Result<Value> {
        let line = self
            .lines
            .next_line()
            .await?
            .context("Runtime closed stdout")?;
        serde_json::from_str(&line).context("Runtime emitted invalid JSON")
    }

    async fn next(&mut self) -> Result<Value> {
        if let Some(value) = self.pending.pop_front() {
            Ok(value)
        } else {
            self.wire().await
        }
    }

    async fn rpc(&mut self, method: &str, params: Value) -> Result<Value> {
        self.sequence += 1;
        let id = self.sequence;
        self.write(&json!({"id":id,"method":method,"params":params}))
            .await?;
        tokio::time::timeout(Duration::from_secs(90), async {
            loop {
                let message = self.wire().await?;
                if message.get("id").and_then(Value::as_u64) == Some(id)
                    && message.get("method").is_none()
                {
                    if let Some(error) = message.get("error") {
                        bail!("{method}: {error}");
                    }
                    break message
                        .get("result")
                        .cloned()
                        .context("RPC response has no result");
                }
                self.pending.push_back(message);
            }
        })
        .await
        .context("Runtime RPC timed out")?
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        // Every runtime starts in its own group. Its unreaped child PID cannot
        // be reused, so this also stops tools and MCP bridges it spawned.
        #[cfg(unix)]
        if let Some(pid) = self.child.id() {
            // SAFETY: this is our child's private process group, not the worker's.
            unsafe {
                libc::kill(-(pid as i32), libc::SIGKILL);
            }
        }
    }
}

async fn emit(events: &mpsc::Sender<Event>, event: Event) -> Result<()> {
    events
        .send(event)
        .await
        .context("Worker stopped receiving runtime events")
}

async fn run(
    process: &mut Process,
    settings: Settings,
    mut commands: mpsc::Receiver<Input>,
    events: mpsc::Sender<Event>,
) -> Result<()> {
    if settings.runtime == Runtime::Claude {
        process
            .write(&json!({
                "type":"init","cwd":settings.cwd,"sessionId":settings.session_id,
                "model":settings.model,"instructions":settings.instructions,"bridge":settings.bridge
            }))
            .await?;
        loop {
            tokio::select! {
                command = commands.recv() => match command {
                    Some(command) => process.write(&serde_json::to_value(command)?).await?,
                    None => break Ok(()),
                },
                value = process.next() => emit(&events, serde_json::from_value(value?)?).await?,
            }
        }
    } else {
        process.rpc("initialize", json!({"clientInfo":{"name":"agent_hub","title":"Agent Hub","version":env!("CARGO_PKG_VERSION")},"capabilities":{}})).await?;
        process.write(&json!({"method":"initialized"})).await?;
        let mut params = json!({
            "cwd":settings.cwd,"approvalPolicy":"on-request","sandbox":"workspace-write",
            "developerInstructions":settings.instructions,
            "config":{"mcp_servers.agent_hub":settings.bridge},
        });
        if let Some(model) = settings.model {
            params["model"] = json!(model);
        }
        let method = if let Some(id) = settings.session_id {
            params["threadId"] = json!(id);
            "thread/resume"
        } else {
            "thread/start"
        };
        let thread = process.rpc(method, params).await?;
        let thread_id = thread["thread"]["id"]
            .as_str()
            .context("Codex returned no thread ID")?
            .to_string();
        emit(
            &events,
            Event::SessionReady {
                session_id: thread_id.clone(),
            },
        )
        .await?;
        let mut active: Option<(u64, String)> = None;
        let mut requests: HashMap<String, Value> = HashMap::new();
        loop {
            tokio::select! {
                command = commands.recv() => match command {
                    Some(Input::Turn {message_id,prompt}) => {
                        if active.is_some() { bail!("Codex already has an active turn"); }
                        let result = process.rpc("turn/start", json!({"threadId":thread_id,"input":[{"type":"text","text":prompt}]})).await?;
                        let turn_id = result["turn"]["id"].as_str().context("Codex returned no turn ID")?.to_string();
                        active = Some((message_id,turn_id.clone()));
                        emit(&events, Event::Started {message_id,turn_id}).await?;
                    }
                    Some(Input::Response {id,response}) => {
                        if let Some(request_id) = requests.remove(&id) {
                            process.write(&json!({"id":request_id,"result":response})).await?;
                            emit(&events, Event::ApprovalResolved {id}).await?;
                        }
                    }
                    Some(Input::Interrupt) => {
                        if let Some((_,turn_id)) = &active {
                            process.rpc("turn/interrupt",json!({"threadId":thread_id,"turnId":turn_id})).await?;
                        }
                    }
                    None => break Ok(()),
                },
                value = process.next() => {
                    let value = value?;
                    let method = value["method"].as_str().unwrap_or("");
                    let params = &value["params"];
                    if let Some(id) = value.get("id") {
                        let (message_id,_) = active.as_ref().context("Codex requested input before a delivery started")?;
                        let key = format!("{message_id}:{id}");
                        requests.insert(key.clone(),id.clone());
                        emit(&events, Event::Approval {message_id:*message_id,id:key,request:json!({"kind":"codex-request","method":method,"params":params})}).await?;
                    } else if method == "item/completed" && params["item"]["type"] == "agentMessage" {
                        if let Some((message_id,turn_id)) = &active
                            && params["threadId"].as_str() == Some(&thread_id)
                            && params["turnId"].as_str() == Some(turn_id) {
                                // Completed items are authoritative, including when token
                                // deltas are suppressed by the provider or client settings.
                                let text = params["item"]["text"].as_str().unwrap_or("");
                                emit(&events, Event::Output {message_id:*message_id,text:format!("{text}\n")}).await?;
                        }
                    } else if method == "turn/completed" {
                        if let Some((message_id,turn_id)) = &active
                            && params["threadId"].as_str() == Some(&thread_id) && params["turn"]["id"].as_str() == Some(turn_id) {
                                let status = params["turn"]["status"].as_str().unwrap_or("failed");
                                let success = status == "completed";
                                emit(&events, Event::Finished {message_id:*message_id,state:if success {DeliveryState::Completed} else {DeliveryState::Failed},error:(!success).then(|| params["turn"].to_string())}).await?;
                                active = None;
                                requests.clear();
                        }
                    } else if method == "mcpServer/startupStatus/updated" && params["name"] == "agent_hub" && params["status"] == "failed" {
                        bail!("The messaging MCP server failed to start: {params}");
                    }
                }
            }
        }
    }
}
