//! Managed provider processes, real hub persistence, and real MCP peer sends.

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use agent_hub::client::Client;
use agent_hub::hub::{self, Hub};
use agent_hub::store::Store;
use managed_protocol::{Command as SessionCommand, DeliveryState, Response};
use serde_json::{Value, json};
use tokio::process::{Child, Command};
use tokio::task::JoinHandle;
use uuid::Uuid;

struct Test {
    hub: String,
    key: String,
    directory: PathBuf,
    server: JoinHandle<()>,
}

impl Test {
    async fn start(agents: &[(&str, &str)]) -> Self {
        let directory = std::env::temp_dir().join(format!("agent-hub-managed-{}", Uuid::new_v4()));
        fs::create_dir_all(&directory).unwrap();
        let store = Store::open(&directory.join("hub.db")).unwrap();
        let key = store.person_key().unwrap();
        let app = hub::router(
            Arc::new(Hub::new(store).unwrap()),
            directory.join("no-dashboard"),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let hub = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let fixture =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/managed-runtime.ts");
        let config = json!({
            "codexCommand":["bun",fixture,"codex",directory.join("trace.jsonl")],
            "claudeCommand":["bun",fixture,"claude",directory.join("trace.jsonl")],
            "agents":agents.iter().map(|(name,runtime)| json!({"name":name,"runtime":runtime,"cwd":directory,"turnTimeoutSeconds":20})).collect::<Vec<_>>()
        });
        fs::write(
            directory.join("worker.json"),
            serde_json::to_vec(&config).unwrap(),
        )
        .unwrap();
        let test = Self {
            hub,
            key,
            directory,
            server,
        };
        test.native(|client| client.register("sender").unwrap())
            .await;
        test
    }

    async fn native<T: Send + 'static>(
        &self,
        operation: impl FnOnce(Client) -> T + Send + 'static,
    ) -> T {
        let hub = self.hub.clone();
        tokio::task::spawn_blocking(move || operation(Client::new(&hub)))
            .await
            .unwrap()
    }

    fn worker(&self) -> Worker {
        let process = Command::new(env!("CARGO_BIN_EXE_agent-hub"))
            .args(["agent-worker", "--config"])
            .arg(self.directory.join("worker.json"))
            .args(["--hub", &self.hub])
            .stdin(Stdio::null())
            .stdout(
                File::options()
                    .create(true)
                    .append(true)
                    .open(self.directory.join("worker.log"))
                    .unwrap(),
            )
            .stderr(
                File::options()
                    .create(true)
                    .append(true)
                    .open(self.directory.join("worker.log"))
                    .unwrap(),
            )
            .spawn()
            .unwrap();
        Worker {
            process: Some(process),
        }
    }

    async fn wait(&self, condition: impl Fn(&[Response]) -> bool) -> Vec<Response> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(25);
        loop {
            let snapshots = self.native(|client| client.sessions().unwrap()).await;
            if condition(&snapshots) {
                break snapshots;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "Timed out. Worker log:\n{}\nSnapshots: {}",
                fs::read_to_string(self.directory.join("worker.log")).unwrap_or_default(),
                serde_json::to_string_pretty(&snapshots).unwrap()
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    async fn registered(&self, count: usize) {
        self.wait(|snapshots| {
            snapshots.len() == count
                && snapshots
                    .iter()
                    .all(|snapshot| snapshot.session.lease_until > hub::now())
        })
        .await;
    }

    async fn send(&self, to: &str, body: &str) -> u64 {
        let to = to.to_string();
        let body = body.to_string();
        self.native(move |client| client.send("sender", &to, &body).unwrap().id)
            .await
    }

    fn trace(&self) -> Vec<Value> {
        fs::read_to_string(self.directory.join("trace.jsonl"))
            .unwrap_or_default()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }
}

impl Drop for Test {
    fn drop(&mut self) {
        self.server.abort();
        let _ = fs::remove_dir_all(&self.directory);
    }
}

struct Worker {
    process: Option<Child>,
}

impl Worker {
    async fn stop(mut self) {
        let mut process = self.process.take().unwrap();
        if let Some(pid) = process.id() {
            // SAFETY: this is the child launched by this fixture.
            unsafe {
                libc::kill(pid as i32, libc::SIGINT);
            }
        }
        let status = tokio::time::timeout(Duration::from_secs(8), process.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(status.success());
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if let Some(pid) = self.process.as_ref().and_then(Child::id) {
            // Give the worker its normal shutdown path, including runtime groups.
            unsafe {
                libc::kill(pid as i32, libc::SIGINT);
            }
        }
    }
}

fn completed(snapshot: &Response) -> bool {
    snapshot
        .delivery
        .as_ref()
        .is_some_and(|delivery| delivery.state == DeliveryState::Completed)
}

#[tokio::test(flavor = "multi_thread")]
async fn incoming_messages_start_both_providers_and_restart_resumes_exact_conversations() {
    let test = Test::start(&[("codex", "codex"), ("claude", "claude")]).await;
    let worker = test.worker();
    test.registered(2).await;
    test.send("codex", "send:claude:send:codex:done").await;
    // Reading through the CLI's HTTP operation must not cancel scheduled work.
    test.native(|client| client.read_inbox("codex", 20).unwrap())
        .await;
    let before = test
        .wait(|snapshots| {
            snapshots.len() == 2
                && snapshots.iter().all(completed)
                && snapshots
                    .iter()
                    .find(|s| s.session.name == "codex")
                    .unwrap()
                    .session
                    .turns
                    == 2
        })
        .await;
    assert_eq!(before.iter().map(|s| s.session.turns).sum::<u32>(), 3);
    worker.stop().await;
    let worker = test.worker();
    test.registered(2).await;
    test.send("codex", "after restart").await;
    test.send("claude", "after restart").await;
    let after = test
        .wait(|snapshots| {
            snapshots.len() == 2
                && snapshots.iter().all(completed)
                && snapshots.iter().map(|s| s.session.turns).sum::<u32>() == 5
        })
        .await;
    for old in before {
        let resumed = after
            .iter()
            .find(|s| s.session.name == old.session.name)
            .unwrap();
        assert_eq!(
            resumed.session.runtime_session_id,
            old.session.runtime_session_id
        );
    }
    let trace = test.trace();
    assert!(trace.iter().any(|event| event["method"] == "thread/resume"));
    assert!(trace.iter().any(|event| event["method"] == "resume"));
    worker.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn approvals_wait_for_the_person_and_busy_sessions_serialize_arrivals() {
    let test = Test::start(&[("codex", "codex"), ("claude", "claude")]).await;
    let worker = test.worker();
    test.registered(2).await;
    test.send("codex", "approval").await;
    test.send("claude", "approval").await;
    let pending = test
        .wait(|snapshots| {
            snapshots.len() == 2 && snapshots.iter().all(|s| s.session.approvals.len() == 1)
        })
        .await;
    for snapshot in pending {
        assert_eq!(snapshot.delivery.unwrap().state, DeliveryState::Running);
        let response = if snapshot.session.name == "codex" {
            json!({"decision":"accept"})
        } else {
            json!({"behavior":"allow"})
        };
        let request = SessionCommand::Respond {
            name: snapshot.session.name,
            generation: snapshot.session.generation,
            id: snapshot.session.approvals[0].id.clone(),
            response,
            key: "wrong".into(),
        };
        assert!(
            test.native(move |client| client.session(&request))
                .await
                .is_err()
        );
    }
    let snapshots = test.native(|client| client.sessions().unwrap()).await;
    for snapshot in snapshots {
        let response = if snapshot.session.name == "codex" {
            json!({"decision":"accept"})
        } else {
            json!({"behavior":"allow"})
        };
        let request = SessionCommand::Respond {
            name: snapshot.session.name,
            generation: snapshot.session.generation,
            id: snapshot.session.approvals[0].id.clone(),
            response,
            key: test.key.clone(),
        };
        test.native(move |client| client.session(&request).unwrap())
            .await;
    }
    test.wait(|snapshots| snapshots.iter().all(completed)).await;
    test.send("codex", "slow first").await;
    test.wait(|snapshots| {
        snapshots.iter().any(|s| {
            s.session.name == "codex"
                && s.delivery
                    .as_ref()
                    .is_some_and(|d| d.state == DeliveryState::Running)
        })
    })
    .await;
    test.send("codex", "second").await;
    test.wait(|snapshots| {
        snapshots
            .iter()
            .find(|s| s.session.name == "codex")
            .is_some_and(|s| s.session.turns == 3 && completed(s))
    })
    .await;
    let trace = test.trace();
    let first_finish = trace
        .iter()
        .position(|e| e["event"] == "finish" && e["body"] == "slow first")
        .unwrap();
    let next_start = trace
        .iter()
        .position(|e| e["event"] == "start" && e["body"] == "second")
        .unwrap();
    assert!(first_finish < next_start);
    worker.stop().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_crashed_runtime_leaves_uncertain_work_and_never_replays_it_automatically() {
    let test = Test::start(&[("codex", "codex")]).await;
    let mut worker = test.worker();
    test.registered(1).await;
    let id = test.send("codex", "crash").await;
    let status = tokio::time::timeout(
        Duration::from_secs(15),
        worker.process.as_mut().unwrap().wait(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(!status.success());
    worker.process = None;
    let worker = test.worker();
    test.registered(1).await;
    test.send("codex", "queued after crash").await;
    tokio::time::sleep(Duration::from_secs(2)).await;
    let snapshot = test
        .native(|client| client.sessions().unwrap().remove(0))
        .await;
    assert_eq!(
        snapshot.delivery.as_ref().unwrap().state,
        DeliveryState::Uncertain
    );
    assert_eq!(
        test.trace()
            .iter()
            .filter(|event| event["event"] == "start")
            .count(),
        1
    );
    worker.stop().await;
    let resolve = SessionCommand::Resolve {
        name: "codex".into(),
        generation: snapshot.session.generation,
        message_id: id,
        state: DeliveryState::Completed,
        key: test.key.clone(),
    };
    test.native(move |client| client.session(&resolve).unwrap())
        .await;
    let worker = test.worker();
    test.registered(1).await;
    test.wait(|snapshots| snapshots[0].session.turns == 2 && completed(&snapshots[0]))
        .await;
    assert_eq!(
        test.trace()
            .iter()
            .filter(|event| event["event"] == "start" && event["body"] == "crash")
            .count(),
        1
    );
    worker.stop().await;
}
